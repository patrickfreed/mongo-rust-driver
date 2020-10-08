#[cfg(test)]
mod test;

mod background;
pub(crate) mod conn;
mod establish;
pub(crate) mod options;
#[allow(dead_code)]
mod wait_queue;
mod worker;

use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::Duration,
};

use derivative::Derivative;
use tokio::sync::{mpsc, oneshot, Mutex};

pub use self::conn::ConnectionInfo;
pub(crate) use self::conn::{Command, CommandResponse, Connection, StreamDescription};
use self::{
    establish::ConnectionEstablisher,
    options::{ConnectionOptions, ConnectionPoolOptions},
};
use crate::{
    error::{ErrorKind, Result},
    event::cmap::{
        CmapEventHandler,
        ConnectionCheckoutFailedEvent,
        ConnectionCheckoutFailedReason,
        ConnectionCheckoutStartedEvent,
        ConnectionClosedReason,
        PoolClearedEvent,
        PoolClosedEvent,
        PoolCreatedEvent,
    },
    options::StreamAddress,
    runtime::HttpClient,
    RUNTIME,
};
use conn::PendingConnection;
use worker::{ConnectionPoolWorker, ConnectionRequestResult, PoolManager};

const DEFAULT_MAX_POOL_SIZE: u32 = 100;

/// A pool of connections implementing the CMAP spec. All state is kept internally in an `Arc`, and
/// internal state that is mutable is additionally wrapped by a lock.
#[derive(Clone, Debug)]
pub(crate) struct ConnectionPool {
    inner: Arc<ConnectionPoolInner>,
}

impl From<Arc<ConnectionPoolInner>> for ConnectionPool {
    fn from(inner: Arc<ConnectionPoolInner>) -> Self {
        Self { inner }
    }
}

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub(crate) struct ConnectionPoolV2 {
    address: StreamAddress,
    manager: PoolManager,
    check_out_sender: mpsc::UnboundedSender<oneshot::Sender<ConnectionRequestResult>>,
    ref_count_sender: mpsc::Sender<()>,
    wait_queue_timeout: Option<Duration>,

    #[derivative(Debug = "ignore")]
    event_handler: Option<Arc<dyn CmapEventHandler>>,
}

impl ConnectionPoolV2 {
    pub(crate) fn new(
        address: StreamAddress,
        http_client: HttpClient,
        options: Option<ConnectionPoolOptions>,
    ) -> Self {
        let establisher = ConnectionEstablisher::new(http_client, options.as_ref());
        let event_handler = options.as_ref().and_then(|opts| opts.event_handler.clone());

        // The CMAP spec indicates that a max idle time of zero means that connections should not be
        // closed due to idleness.
        let mut max_idle_time = options.as_ref().and_then(|opts| opts.max_idle_time);
        if max_idle_time == Some(Duration::from_millis(0)) {
            max_idle_time = None;
        }

        let max_pool_size = options
            .as_ref()
            .and_then(|opts| opts.max_pool_size)
            .unwrap_or(DEFAULT_MAX_POOL_SIZE);

        let min_pool_size = options.as_ref().and_then(|opts| opts.min_pool_size);
        let wait_queue_timeout = options.as_ref().and_then(|opts| opts.wait_queue_timeout);

        let connection_options: Option<ConnectionOptions> = options
            .as_ref()
            .map(|pool_options| ConnectionOptions::from(pool_options.clone()));

        let (check_out_sender, request_receiver) = mpsc::unbounded_channel();
        let (management_sender, management_receiver) = mpsc::unbounded_channel();
        let (ref_count_sender, liveness_receiver) = mpsc::channel(1);

        let inner = ConnectionPoolWorker {
            address: address.clone(),
            event_handler: event_handler.clone(),
            max_idle_time,
            min_pool_size,
            establisher,
            next_connection_id: 1,
            total_connection_count: 0,
            generation: 0,
            connection_options,
            available_connections: VecDeque::new(),
            max_pool_size,
            wait_queue_timeout,
            request_receiver,
            management_receiver,
            management_sender: management_sender.clone(),
            unfinished_check_out: None,
            liveness_receiver,
        };

        inner.start();

        if let Some(ref handler) = event_handler {
            handler.handle_pool_created_event(PoolCreatedEvent {
                address: address.clone(),
                options,
            });
        };

        Self {
            address,
            manager: PoolManager {
                sender: management_sender,
            },
            check_out_sender,
            wait_queue_timeout,
            event_handler,
            ref_count_sender,
        }
    }

    fn emit_event<F>(&self, emit: F)
    where
        F: FnOnce(&Arc<dyn CmapEventHandler>),
    {
        if let Some(ref handler) = self.event_handler {
            emit(handler);
        }
    }

    pub(crate) async fn check_out(&self) -> Result<Connection> {
        let (sender, receiver) = oneshot::channel();

        self.emit_event(|handler| {
            let event = ConnectionCheckoutStartedEvent {
                address: self.address.clone(),
            };

            handler.handle_connection_checkout_started_event(event);
        });

        let conn = match self.check_out_sender.send(sender) {
            Ok(_) => {
                let response = match self.wait_queue_timeout {
                    Some(timeout) => RUNTIME
                        .timeout(timeout, receiver)
                        .await
                        .map(|r| r.unwrap())
                        .map_err(|_| {
                            ErrorKind::WaitQueueTimeoutError {
                                address: self.address.clone(),
                            }
                            .into()
                        }),
                    None => Ok(receiver.await.unwrap()),
                };

                match response {
                    Ok(ConnectionRequestResult::Pooled(c)) => Ok(c),
                    Ok(ConnectionRequestResult::Establishing(task)) => task.await.unwrap(),
                    Err(e) => Err(e),
                }
            }
            Err(e) => panic!("woops {:?}", e),
        };

        match conn {
            Ok(ref conn) => {
                self.emit_event(|handler| {
                    handler.handle_connection_checked_out_event(conn.checked_out_event());
                });
            }
            Err(ref e) => {
                let failure_reason =
                    if let ErrorKind::WaitQueueTimeoutError { .. } = e.kind.as_ref() {
                        ConnectionCheckoutFailedReason::Timeout
                    } else {
                        ConnectionCheckoutFailedReason::ConnectionError
                    };

                self.emit_event(|handler| {
                    handler.handle_connection_checkout_failed_event(ConnectionCheckoutFailedEvent {
                        address: self.address.clone(),
                        reason: failure_reason,
                    })
                });
            }
        }

        conn
    }

    pub(crate) async fn check_in(&self, conn: Connection) {
        self.manager.check_in(conn);
    }

    pub(crate) fn clear(&self) {
        self.manager.clear();
    }
}

/// The internal state of a connection pool.
#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) struct ConnectionPoolInner {
    /// The address the pool's connections will connect to.
    address: StreamAddress,

    /// The total number of connections managed by the pool, including connections which are
    /// currently checked out of the pool or have yet to be established.
    total_connection_count: AtomicU32,

    /// The ID of the next connection created by the pool.
    next_connection_id: AtomicU32,

    /// The current generation of the pool. The generation is incremented whenever the pool is
    /// cleared. Connections belonging to a previous generation are considered stale and will be
    /// closed when checked back in or when popped off of the set of available connections.
    generation: AtomicU32,

    /// The established connections that are currently checked into the pool and awaiting usage in
    /// future operations.
    available_connections: Mutex<VecDeque<Connection>>,

    /// Contains the logic for "establishing" a connection. This includes handshaking and
    /// authenticating a connection when it's first created.
    establisher: ConnectionEstablisher,

    /// The options used to create new connections.
    connection_options: Option<ConnectionOptions>,

    /// The event handler specified by the user to process CMAP events.
    #[derivative(Debug = "ignore")]
    event_handler: Option<Arc<dyn CmapEventHandler>>,

    /// Connections that have been ready for usage in the pool for longer than `max_idle_time` will
    /// be closed either by the background thread or when popped off of the set of available
    /// connections. If `max_idle_time` is `None`, then connections will not be closed due to being
    /// idle.
    max_idle_time: Option<Duration>,

    /// The minimum number of connections that the pool can have at a given time. This includes
    /// connections which are currently checked out of the pool. If fewer than `min_pool_size`
    /// connections are in the pool, the background thread will create more connections and add
    /// them to the pool.
    min_pool_size: Option<u32>,

    max_pool_size: u32,

    wait_queue_sender: mpsc::UnboundedSender<oneshot::Sender<ConnectionRequestResult>>,

    connection_return_sender: mpsc::UnboundedSender<Connection>,

    wait_queue_timeout: Option<Duration>,
}

impl ConnectionPool {
    pub(crate) fn new(
        address: StreamAddress,
        http_client: HttpClient,
        options: Option<ConnectionPoolOptions>,
    ) -> Self {
        let establisher = ConnectionEstablisher::new(http_client, options.as_ref());
        let event_handler = options.as_ref().and_then(|opts| opts.event_handler.clone());

        // The CMAP spec indicates that a max idle time of zero means that connections should not be
        // closed due to idleness.
        let mut max_idle_time = options.as_ref().and_then(|opts| opts.max_idle_time);
        if max_idle_time == Some(Duration::from_millis(0)) {
            max_idle_time = None;
        }

        let max_pool_size = options
            .as_ref()
            .and_then(|opts| opts.max_pool_size)
            .unwrap_or(DEFAULT_MAX_POOL_SIZE);

        let min_pool_size = options.as_ref().and_then(|opts| opts.min_pool_size);
        let wait_queue_timeout = options.as_ref().and_then(|opts| opts.wait_queue_timeout);

        let connection_options: Option<ConnectionOptions> = options
            .as_ref()
            .map(|pool_options| ConnectionOptions::from(pool_options.clone()));

        let (wait_queue_sender, wait_queue_receiver) = mpsc::unbounded_channel();
        let (connection_return_sender, connection_return_receiver) = mpsc::unbounded_channel();

        let inner = ConnectionPoolInner {
            address: address.clone(),
            event_handler,
            max_idle_time,
            min_pool_size,
            establisher,
            next_connection_id: AtomicU32::new(1),
            total_connection_count: AtomicU32::new(0),
            generation: AtomicU32::new(0),
            connection_options,
            available_connections: Mutex::new(VecDeque::new()),
            wait_queue_sender,
            connection_return_sender,
            max_pool_size,
            wait_queue_timeout,
        };

        let pool = Self {
            inner: Arc::new(inner),
        };

        pool.inner.emit_event(move |handler| {
            let event = PoolCreatedEvent { address, options };
            handler.handle_pool_created_event(event);
        });

        background::start_background_task(Arc::downgrade(&pool.inner));
        // worker::start(
        //     wait_queue_receiver,
        //     connection_return_receiver,
        //     Arc::downgrade(&pool.inner),
        // );

        pool
    }

    /// Checks out a connection from the pool. This method will block until this thread is at the
    /// front of the wait queue, and then will block again if no available connections are in the
    /// pool and the total number of connections is not less than the max pool size. If the method
    /// blocks for longer than `wait_queue_timeout`, a `WaitQueueTimeoutError` will be returned.
    pub(crate) async fn check_out(&self) -> Result<Connection> {
        let mut conn = self.inner.check_out().await?;
        conn.mark_as_in_use(Arc::downgrade(&self.inner));
        Ok(conn)
    }

    /// Checks a connection back into the pool and notifies the wait queue that a connection is
    /// ready. If the connection is stale, it will be closed instead of being added to the set of
    /// available connections. The time that the connection is checked in will be marked to
    /// facilitate detecting if the connection becomes idle.
    #[cfg(test)]
    pub(crate) async fn check_in(&self, conn: Connection) {
        self.inner.check_in(conn).await;
    }

    /// Increments the generation of the pool. Rather than eagerly removing stale connections from
    /// the pool, they are left for the background thread to clean up.
    pub(crate) fn clear(&self) {
        self.inner.clear();
    }
}

impl ConnectionPoolInner {
    /// Emits an event from the event handler if one is present, where `emit` is a closure that uses
    /// the event handler.
    fn emit_event<F>(&self, emit: F)
    where
        F: FnOnce(&Arc<dyn CmapEventHandler>),
    {
        if let Some(ref handler) = self.event_handler {
            emit(handler);
        }
    }

    async fn check_in(&self, mut conn: Connection) {
        self.emit_event(|handler| {
            handler.handle_connection_checked_in_event(conn.checked_in_event());
        });

        conn.mark_as_available();

        // Close the connection if it's stale.
        if conn.is_stale(self.generation.load(Ordering::SeqCst)) {
            self.close_connection(conn, ConnectionClosedReason::Stale);
        } else if conn.is_executing() {
            self.close_connection(conn, ConnectionClosedReason::Dropped)
        } else {
            self.connection_return_sender.send(conn).unwrap();
        }
    }

    async fn check_out(&self) -> Result<Connection> {
        let (sender, receiver) = oneshot::channel();
        let wait_queue_sender = self.wait_queue_sender.clone();

        self.emit_event(|handler| {
            let event = ConnectionCheckoutStartedEvent {
                address: self.address.clone(),
            };

            handler.handle_connection_checkout_started_event(event);
        });

        let conn = match wait_queue_sender.send(sender) {
            Ok(_) => {
                let response = match self.wait_queue_timeout {
                    Some(timeout) => RUNTIME
                        .timeout(timeout, receiver)
                        .await
                        .map(|r| r.unwrap())
                        .map_err(|_| {
                            ErrorKind::WaitQueueTimeoutError {
                                address: self.address.clone(),
                            }
                            .into()
                        }),
                    None => Ok(receiver.await.unwrap()),
                };

                match response {
                    Ok(ConnectionRequestResult::Pooled(c)) => Ok(c),
                    Ok(ConnectionRequestResult::Establishing(task)) => task.await.unwrap(),
                    Err(e) => Err(e),
                }
            }
            Err(e) => panic!("woops {:?}", e),
        };

        match conn {
            Ok(ref conn) => {
                self.emit_event(|handler| {
                    handler.handle_connection_checked_out_event(conn.checked_out_event());
                });
            }
            Err(ref e) => {
                let failure_reason =
                    if let ErrorKind::WaitQueueTimeoutError { .. } = e.kind.as_ref() {
                        ConnectionCheckoutFailedReason::Timeout
                    } else {
                        ConnectionCheckoutFailedReason::ConnectionError
                    };

                self.emit_event(|handler| {
                    handler.handle_connection_checkout_failed_event(ConnectionCheckoutFailedEvent {
                        address: self.address.clone(),
                        reason: failure_reason,
                    })
                });
            }
        }

        conn
    }

    /// Create a connection that has not been established, incrementing the total connection count
    /// and emitting the appropriate events. This method performs no I/O.
    ///
    /// This MUST ONLY be called while holding a handle to the wait queue.
    fn create_pending_connection(&self) -> PendingConnection {
        self.total_connection_count.fetch_add(1, Ordering::SeqCst);

        let connection = PendingConnection {
            id: self.next_connection_id.fetch_add(1, Ordering::SeqCst),
            address: self.address.clone(),
            generation: self.generation.load(Ordering::SeqCst),
            options: self.connection_options.clone(),
        };
        self.emit_event(|handler| {
            handler.handle_connection_created_event(connection.created_event())
        });

        connection
    }

    /// Connect and handshake the given pending connection, returning an error if establishment was
    /// unsuccessful.
    async fn establish_connection(
        &self,
        pending_connection: PendingConnection,
    ) -> Result<Connection> {
        let establish_result = self
            .establisher
            .establish_connection(pending_connection)
            .await;

        match establish_result {
            Err(ref e) => {
                if e.is_authentication_error() {
                    // auth spec requires pool is cleared after encountering auth error.
                    self.clear();
                }
                // Establishing a pending connection failed, so that must be reflected in to total
                // connection count.
                self.total_connection_count.fetch_sub(1, Ordering::SeqCst);
            }
            Ok(ref connection) => {
                self.emit_event(|handler| {
                    handler.handle_connection_ready_event(connection.ready_event())
                });
            }
        }

        establish_result
    }

    /// Close a connection, emit the event for it being closed, and decrement the
    /// total connection count.
    fn close_connection(&self, connection: Connection, reason: ConnectionClosedReason) {
        connection.close_and_drop(reason);
        self.total_connection_count.fetch_sub(1, Ordering::SeqCst);
    }

    fn clear(&self) {
        self.generation.fetch_add(1, Ordering::SeqCst);
        self.emit_event(|handler| {
            let event = PoolClearedEvent {
                address: self.address.clone(),
            };

            handler.handle_pool_cleared_event(event);
        });
    }
}

impl Drop for ConnectionPoolInner {
    /// Automatic cleanup for the connection pool. This is defined on `ConnectionPoolInner` rather
    /// than `ConnectionPool` so that it only gets run once all (non-weak) references to the
    /// `ConnectionPoolInner` are dropped.
    fn drop(&mut self) {
        let address = self.address.clone();
        let available_connections =
            std::mem::replace(&mut self.available_connections, Mutex::new(VecDeque::new()));
        let event_handler = self.event_handler.clone();

        RUNTIME.execute(async move {
            // this lock attempt will always immediately succeed.
            let mut available_connections = available_connections.lock().await;
            while let Some(connection) = available_connections.pop_front() {
                connection.close_and_drop(ConnectionClosedReason::PoolClosed);
            }

            if let Some(ref handler) = event_handler {
                handler.handle_pool_closed_event(PoolClosedEvent {
                    address: address.clone(),
                });
            }
        });
    }
}
