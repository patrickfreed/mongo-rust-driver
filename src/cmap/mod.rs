#[cfg(test)]
mod test;

pub(crate) mod conn;
mod connection_requester;
mod establish;
pub(crate) mod options;
#[allow(dead_code)]
mod wait_queue;
mod worker;

use std::{collections::VecDeque, sync::Arc, time::Duration};

use derivative::Derivative;
use tokio::sync::{mpsc, oneshot};

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
        ConnectionCheckoutStartedEvent, PoolCreatedEvent,
    },
    options::StreamAddress,
    runtime::HttpClient,
    RUNTIME,
};
use connection_requester::{ConnectionRequester, RequestedConnection};
use worker::{ConnectionPoolWorker, PoolManager};

const DEFAULT_MAX_POOL_SIZE: u32 = 100;

/// A pool of connections implementing the CMAP spec. All state is kept internally in an `Arc`, and
/// internal state that is mutable is additionally wrapped by a lock.
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub(crate) struct ConnectionPool {
    address: StreamAddress,
    manager: PoolManager,
    connection_requester: ConnectionRequester,
    worker_handle: mpsc::Sender<()>,
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
        let (manager, connection_requester, handle) =
            ConnectionPoolWorker::new(address.clone(), http_client, options.clone());

        let event_handler = options.as_ref().and_then(|opts| opts.event_handler.clone());
        let wait_queue_timeout = options.as_ref().and_then(|opts| opts.wait_queue_timeout);

        if let Some(ref handler) = event_handler {
            handler.handle_pool_created_event(PoolCreatedEvent {
                address: address.clone(),
                options,
            });
        };

        Self {
            address,
            manager,
            connection_requester,
            wait_queue_timeout,
            event_handler,
            worker_handle: handle,
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
        self.emit_event(|handler| {
            let event = ConnectionCheckoutStartedEvent {
                address: self.address.clone(),
            };

            handler.handle_connection_checkout_started_event(event);
        });

        let conn = self
            .connection_requester
            .request(self.wait_queue_timeout)
            .await;
        // let conn = match self.connection_requester.request().await {
        //     Ok(_) => {
        //         let response = match self.wait_queue_timeout {
        //             Some(timeout) => RUNTIME
        //                 .timeout(timeout, receiver)
        //                 .await
        //                 .map(|r| r.unwrap())
        //                 .map_err(|_| {
        //                     ErrorKind::WaitQueueTimeoutError {
        //                         address: self.address.clone(),
        //                     }
        //                     .into()
        //                 }),
        //             None => Ok(receiver.await.unwrap()),
        //         };

        //         match response {
        //             Ok(RequestedConnection::Pooled(c)) => Ok(c),
        //             Ok(RequestedConnection::Establishing(task)) => task.await.unwrap(),
        //             Err(e) => Err(e),
        //         }
        //     }
        //     Err(e) => panic!("woops {:?}", e),
        // };

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
