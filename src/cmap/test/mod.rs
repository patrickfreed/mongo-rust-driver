mod event;
mod file;
mod integration;

use std::{collections::HashMap, ops::Deref, sync::Arc, time::Duration};

use futures::future::{BoxFuture, FutureExt};

use tokio::sync::{Mutex, RwLock, RwLockReadGuard};

use self::{
    event::{Event, EventHandler},
    file::{Operation, TestFile},
};

use super::ConnectionPoolV2;
use crate::{
    cmap::{options::ConnectionPoolOptions, Connection, ConnectionPool},
    error::{Error, Result},
    options::TlsOptions,
    runtime::AsyncJoinHandle,
    test::{assert_matches, run_spec_test, EventClient, Matchable, CLIENT_OPTIONS, LOCK},
    RUNTIME,
};

const TEST_DESCRIPTIONS_TO_SKIP: &[&str] = &[
    "must destroy checked in connection if pool has been closed",
    "must throw error if checkOut is called on a closed pool",
];

#[derive(Debug)]
struct Executor {
    description: String,
    operations: Vec<Operation>,
    error: Option<self::file::Error>,
    events: Vec<Event>,
    state: Arc<State>,
    ignored_event_names: Vec<String>,
}

#[derive(Debug)]
struct State {
    handler: Arc<EventHandler>,
    connections: RwLock<HashMap<String, Connection>>,
    unlabeled_connections: Mutex<Vec<Connection>>,
    threads: RwLock<HashMap<String, AsyncJoinHandle<Result<()>>>>,

    // In order to drop the pool when performing a `close` operation, we use an `Option` so that we
    // can replace it with `None`. Since none of the tests should use the pool after its closed
    // (besides the ones we manually skip over), it's fine for us to `unwrap` the pool during these
    // tests, as panicking is sufficient to exit any aberrant test with a failure.
    pool: RwLock<Option<ConnectionPoolV2>>,
}

impl State {
    fn count_all_events(&self) -> usize {
        self.handler.events.read().unwrap().len()
    }

    // Counts the number of events of the given type that have occurred so far.
    fn count_events(&self, event_type: &str) -> usize {
        self.handler
            .events
            .read()
            .unwrap()
            .iter()
            .filter(|cmap_event| cmap_event.name() == event_type)
            .count()
    }
}

impl Executor {
    fn new(mut test_file: TestFile) -> Self {
        let operations = test_file.process_operations();
        let handler = Arc::new(EventHandler::new());
        let error = test_file.error;

        test_file
            .pool_options
            .get_or_insert_with(Default::default)
            .tls_options = CLIENT_OPTIONS.tls_options();

        test_file
            .pool_options
            .get_or_insert_with(Default::default)
            .event_handler = Some(handler.clone());

        let pool = ConnectionPoolV2::new(
            CLIENT_OPTIONS.hosts[0].clone(),
            Default::default(),
            test_file.pool_options,
        );

        let state = State {
            handler,
            pool: RwLock::new(Some(pool)),
            connections: Default::default(),
            threads: Default::default(),
            unlabeled_connections: Mutex::new(Default::default()),
        };

        Self {
            description: test_file.description,
            error,
            events: test_file.events,
            operations,
            state: Arc::new(state),
            ignored_event_names: test_file.ignore,
        }
    }

    async fn execute_test(self) {
        let mut error: Option<Error> = None;
        let operations = self.operations;

        println!("Executing {}", self.description);

        for operation in operations {
            let err = operation.execute(self.state.clone()).await.err();
            if error.is_none() {
                error = err;
            }

            // Some of the CMAP spec tests have some subtle race conditions due assertions about the
            // order that concurrent threads check out connections from the pool. The solution to
            // this is to sleep for a small amount of time between each operation, which in practice
            // allows threads to enter the wait queue in the order that they were started.
            RUNTIME.delay_for(Duration::from_millis(1)).await;
        }

        match (self.error, error) {
            (Some(ref expected), Some(ref actual)) => {
                expected.assert_matches(actual, self.description.as_str())
            }
            (Some(ref expected), None) => {
                panic!("Expected {}, but no error occurred", expected.type_)
            }
            (None, Some(ref actual)) => panic!(
                "Expected no error to occur, but the following error was returned: {:?}",
                actual
            ),
            (None, None) => {}
        }

        // The `Drop` implementation for `Connection` and `ConnectionPool` spawn background async
        // tasks that emit certain events. If the tasks haven't been scheduled yet, we may
        // not see the events here. To account for this, we wait for a small amount of time
        // before checking again.
        if self.state.count_all_events() < self.events.len() {
            RUNTIME.delay_for(Duration::from_millis(250)).await;
        }

        let ignored_event_names = self.ignored_event_names;
        let mut actual_events = self.state.handler.events.write().unwrap();
        actual_events.retain(|e| !ignored_event_names.iter().any(|name| e.name() == name));

        // if actual_events.len() < self.events.len() {
        //     panic!(
        //         "{}: more events expected than were actually received. expected: {:?}, actual: \
        //          {:?}",
        //         self.description,
        //         self.events,
        //         actual_events.deref()
        //     )
        // }

        println!("actual");
        for event in actual_events.iter() {
            println!("{}", event.name());
        }
        for i in 0..self.events.len() {
            assert_matches(
                &actual_events[i],
                &self.events[i],
                Some(self.description.as_str()),
            );
        }
    }
}

impl Operation {
    /// Execute this operation.
    /// async fns currently cannot be recursive, so instead we manually return a BoxFuture here.
    /// See: https://rust-lang.github.io/async-book/07_workarounds/05_recursion.html
    fn execute(self, state: Arc<State>) -> BoxFuture<'static, Result<()>> {
        async move {
            match self {
                Operation::StartHelper { target, operations } => {
                    let state_ref = state.clone();
                    let target_name = target.clone();
                    let task = RUNTIME.spawn(async move {
                        for operation in operations {
                            println!("{}: {:?}", target_name, operation);
                            // If any error occurs during an operation, we halt the thread and yield
                            // that value when `join` is called on the thread.
                            operation.execute(state_ref.clone()).await?;
                            println!("{}: done!", target_name);
                        }
                        Ok(())
                    });

                    state.threads.write().await.insert(target, task.unwrap());
                }
                Operation::Wait { ms } => RUNTIME.delay_for(Duration::from_millis(ms)).await,
                Operation::WaitForThread { target } => state
                    .threads
                    .write()
                    .await
                    .remove(&target)
                    .unwrap()
                    .await
                    .expect("polling the future should not fail")?,
                Operation::WaitForEvent { event, count } => {
                    while state.count_events(&event) < count {
                        println!("waiting for {} {} events", count, event);
                        RUNTIME.delay_for(Duration::from_millis(100)).await;
                    }
                }
                Operation::CheckOut { label } => {
                    if let Some(pool) = state.pool.read().await.deref() {
                        let mut conn = pool.check_out().await?;

                        if let Some(label) = label {
                            state.connections.write().await.insert(label, conn);
                        } else {
                            state.unlabeled_connections.lock().await.push(conn);
                        }
                    }
                }
                Operation::CheckIn { connection } => {
                    let conn = state.connections.write().await.remove(&connection).unwrap();

                    if let Some(pool) = state.pool.read().await.deref() {
                        pool.check_in(conn).await;
                    }
                }
                Operation::Clear => {
                    if let Some(pool) = state.pool.write().await.deref() {
                        pool.clear();
                    }
                }
                Operation::Close => {
                    state.connections.write().await.clear();
                    state.pool.write().await.take();
                }

                // We replace all instances of `Start` with `StartHelper` when we preprocess the
                // events, so this should never actually be found.
                Operation::Start { .. } => unreachable!(),
            }
            Ok(())
        }
        .boxed()
    }
}

impl Matchable for TlsOptions {
    fn content_matches(&self, expected: &TlsOptions) -> bool {
        self.allow_invalid_certificates
            .matches(&expected.allow_invalid_certificates)
            && self.ca_file_path.matches(&expected.ca_file_path)
            && self
                .cert_key_file_path
                .matches(&expected.cert_key_file_path)
    }
}

impl Matchable for ConnectionPoolOptions {
    fn content_matches(&self, expected: &ConnectionPoolOptions) -> bool {
        self.app_name.matches(&expected.app_name)
            && self.connect_timeout.matches(&expected.connect_timeout)
            && self.credential.matches(&expected.credential)
            && self.max_idle_time.matches(&expected.max_idle_time)
            && self.max_pool_size.matches(&expected.max_pool_size)
            && self.min_pool_size.matches(&expected.min_pool_size)
            && self.tls_options.matches(&expected.tls_options)
            && self
                .wait_queue_timeout
                .matches(&expected.wait_queue_timeout)
    }
}

impl Matchable for Event {
    fn content_matches(&self, expected: &Event) -> bool {
        match (self, expected) {
            (Event::ConnectionPoolCreated(actual), Event::ConnectionPoolCreated(ref expected)) => {
                actual.options.matches(&expected.options)
            }
            (Event::ConnectionCreated(actual), Event::ConnectionCreated(ref expected)) => {
                actual.connection_id.matches(&expected.connection_id)
            }
            (Event::ConnectionReady(actual), Event::ConnectionReady(ref expected)) => {
                actual.connection_id.matches(&expected.connection_id)
            }
            (Event::ConnectionClosed(actual), Event::ConnectionClosed(ref expected)) => {
                actual.reason == expected.reason
                    && actual.connection_id.matches(&expected.connection_id)
            }
            (Event::ConnectionCheckedOut(actual), Event::ConnectionCheckedOut(ref expected)) => {
                actual.connection_id.matches(&expected.connection_id)
            }
            (Event::ConnectionCheckedIn(actual), Event::ConnectionCheckedIn(ref expected)) => {
                actual.connection_id.matches(&expected.connection_id)
            }
            (
                Event::ConnectionCheckOutFailed(actual),
                Event::ConnectionCheckOutFailed(ref expected),
            ) => actual.reason == expected.reason,
            (Event::ConnectionCheckOutStarted(_), Event::ConnectionCheckOutStarted(_)) => true,
            (Event::ConnectionPoolCleared(_), Event::ConnectionPoolCleared(_)) => true,
            (Event::ConnectionPoolClosed(_), Event::ConnectionPoolClosed(_)) => true,
            _ => false,
        }
    }
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn cmap_spec_tests() {
    async fn run_cmap_spec_tests(test_file: TestFile) {
        if test_file.description.contains("maxConnecting") {
            return;
        }
        if TEST_DESCRIPTIONS_TO_SKIP.contains(&test_file.description.as_str()) {
            return;
        }

        println!("running {}", test_file.description);

        let _guard: RwLockReadGuard<()> = LOCK.run_concurrently().await;

        // let client = EventClient::new().await;
        // if let Some(ref run_on) = test_file.run_on {
        //     let can_run_on = run_on.iter().any(|run_on| run_on.can_run_on(&client));
        //     if !can_run_on {
        //         println!("Skipping {}", test_file.description);
        //         return;
        //     }
        // }

        // if let Some(ref fail_point) = test_file.fail_point {
        //     println!("fp");
        //     client
        //         .database("admin")
        //         .run_command(fail_point.clone(), None)
        //         .await
        //         .unwrap();
        // }

        let executor = Executor::new(test_file);
        executor.execute_test().await;
    }

    run_spec_test(&["connection-monitoring-and-pooling"], run_cmap_spec_tests).await;
}
