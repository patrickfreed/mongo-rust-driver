#[cfg(test)]
mod test;

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
        CmapEventHandler, ConnectionCheckoutFailedEvent, ConnectionCheckoutFailedReason,
        ConnectionCheckoutStartedEvent, ConnectionClosedReason, PoolClearedEvent, PoolClosedEvent,
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
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub(crate) struct ConnectionPool {
    address: StreamAddress,
    manager: PoolManager,
    check_out_sender: mpsc::UnboundedSender<oneshot::Sender<ConnectionRequestResult>>,
    ref_count_sender: mpsc::Sender<()>,
    wait_queue_timeout: Option<Duration>,

    #[derivative(Debug = "ignore")]
    event_handler: Option<Arc<dyn CmapEventHandler>>,
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

    /// Checks out a connection from the pool. This method will block until this thread is at the
    /// front of the wait queue, and then will block again if no available connections are in the
    /// pool and the total number of connections is not less than the max pool size. If the method
    /// blocks for longer than `wait_queue_timeout` waiting for an available connection or to
    /// start establishing a new one, a `WaitQueueTimeoutError` will be returned.
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
    /// Increments the generation of the pool. Rather than eagerly removing stale connections from
    /// the pool, they are left for the background thread to clean up.
    pub(crate) fn clear(&self) {
        self.manager.clear();
    }
}

// impl Drop for ConnectionPoolInner {
//     /// Automatic cleanup for the connection pool. This is defined on `ConnectionPoolInner` rather
//     /// than `ConnectionPool` so that it only gets run once all (non-weak) references to the
//     /// `ConnectionPoolInner` are dropped.
//     fn drop(&mut self) {
//         let address = self.address.clone();
//         let available_connections =
//             std::mem::replace(&mut self.available_connections, Mutex::new(VecDeque::new()));
//         let event_handler = self.event_handler.clone();

//         RUNTIME.execute(async move {
//             // this lock attempt will always immediately succeed.
//             let mut available_connections = available_connections.lock().await;
//             while let Some(connection) = available_connections.pop_front() {
//                 connection.close_and_drop(ConnectionClosedReason::PoolClosed);
//             }

//             if let Some(ref handler) = event_handler {
//                 handler.handle_pool_closed_event(PoolClosedEvent {
//                     address: address.clone(),
//                 });
//             }
//         });
//     }
// }
