#[cfg(test)]
pub(crate) mod test;

pub(crate) mod conn;
mod connection_requester;
mod establish;
mod manager;
pub(crate) mod options;
mod status;
mod worker;

use std::{sync::Arc, time::Duration};

use derivative::Derivative;

pub use self::conn::ConnectionInfo;
pub(crate) use self::{
    conn::{Command, CommandResponse, Connection, StreamDescription},
    establish::handshake::{is_master, Handshaker},
    status::PoolGenerationSubscriber,
};
use self::{connection_requester::ConnectionRequestResult, options::ConnectionPoolOptions};
use crate::{
    error::{Error, ErrorKind, Result},
    event::cmap::{
        CmapEventHandler,
        ConnectionCheckoutFailedEvent,
        ConnectionCheckoutFailedReason,
        ConnectionCheckoutStartedEvent,
        PoolCreatedEvent,
    },
    options::StreamAddress,
    runtime::HttpClient,
};
use connection_requester::ConnectionRequester;
use manager::PoolManager;
use worker::ConnectionPoolWorker;

#[cfg(test)]
use self::worker::PoolWorkerHandle;

const DEFAULT_MAX_POOL_SIZE: u32 = 100;

/// A pool of connections implementing the CMAP spec. All state is kept internally in an `Arc`, and
/// internal state that is mutable is additionally wrapped by a lock.
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub(crate) struct ConnectionPool {
    address: StreamAddress,
    manager: PoolManager,
    connection_requester: ConnectionRequester,
    generation_subscriber: PoolGenerationSubscriber,

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
        let (manager, connection_requester, generation_subscriber) =
            ConnectionPoolWorker::start(address.clone(), http_client, options.clone());

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
            generation_subscriber,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_mocked(address: StreamAddress) -> Self {
        let (manager, _) = manager::channel();
        let handle = PoolWorkerHandle::new_mocked();
        let (connection_requester, _) = connection_requester::channel(Default::default(), handle);
        let (_, generation_subscriber) = status::channel();

        Self {
            address,
            manager,
            connection_requester,
            generation_subscriber,
            wait_queue_timeout: None,
            event_handler: None,
        }
    }

    fn emit_event<F>(&self, emit: F)
    where
        F: FnOnce(&Arc<dyn CmapEventHandler>),
    {
        if let Some(ref handler) = self.event_handler {
            emit(handler);
        } else {
            println!("no handler")
        }
    }

    /// Checks out a connection from the pool. This method will yield until this thread is at the
    /// front of the wait queue, and then will block again if no available connections are in the
    /// pool and the total number of connections is not less than the max pool size. If the method
    /// blocks for longer than `wait_queue_timeout` waiting for an available connection or to
    /// start establishing a new one, a `WaitQueueTimeoutError` will be returned.
    pub(crate) async fn check_out(&self) -> Result<Connection> {
        println!("entered ConnectionPool::check_out");
        self.emit_event(|handler| {
            let event = ConnectionCheckoutStartedEvent {
                address: self.address.clone(),
            };

            println!("invoking handler for checkout started event");
            handler.handle_connection_checkout_started_event(event);
        });

        println!("requesting connection");
        let response = self
            .connection_requester
            .request(self.wait_queue_timeout)
            .await;

        println!("got connection request response {:?}", response);

        let conn = match response {
            Some(ConnectionRequestResult::Pooled(c)) => Ok(c),
            Some(ConnectionRequestResult::Establishing(task)) => task.await,
            Some(ConnectionRequestResult::PoolCleared) => {
                Err(Error::pool_cleared_error(&self.address))
            }
            None => Err(ErrorKind::WaitQueueTimeoutError {
                address: self.address.clone(),
            }
            .into()),
        };

        println!("conn={:?}", conn);

        match conn {
            Ok(ref conn) => {
                println!("emitting checked out event");
                self.emit_event(|handler| {
                    println!("invoking handler for checkout checked out event");
                    handler.handle_connection_checked_out_event(conn.checked_out_event());
                });
                println!("event emitted");
            }
            Err(ref e) => {
                let failure_reason =
                    if let ErrorKind::WaitQueueTimeoutError { .. } = e.kind.as_ref() {
                        ConnectionCheckoutFailedReason::Timeout
                    } else {
                        ConnectionCheckoutFailedReason::ConnectionError
                    };

                println!("checkout failed {:?} => {:?}", e, failure_reason);
                println!("about to emit check out failed event");
                self.emit_event(|handler| {
                    println!("inside check out failed event emitter, calling handler");
                    handler.handle_connection_checkout_failed_event(
                        ConnectionCheckoutFailedEvent {
                            address: self.address.clone(),
                            reason: failure_reason,
                        },
                    );
                    println!("handler call compelted");
                });
                println!("check out failed event emitted");
            }
        }

        conn
    }

    /// Increments the generation of the pool. Rather than eagerly removing stale connections from
    /// the pool, they are left for the background thread to clean up.
    pub(crate) fn clear(&self) {
        println!("in ConnectionPool::clear, invoking manager");
        self.manager.clear();
        println!("clear invoked");
    }

    /// Mark the pool as "ready", allowing connections to be created and checked out.
    pub(crate) async fn mark_as_ready(&self) {
        println!("in ConnectionPool::mark_as_ready, invoking manager");
        self.manager.mark_as_ready().await;
        println!("mark as ready done");
    }

    /// Subscribe to updates to the pool's generation.
    ///
    /// This can be used to listen for errors that occur during connection
    /// establishment or to get the current generation of the pool.
    pub(crate) fn subscribe_to_generation_updates(&self) -> PoolGenerationSubscriber {
        self.generation_subscriber.clone()
    }
}
