use derivative::Derivative;

use super::{
    conn::PendingConnection,
    connection_requester::{
        ConnectionRequest, ConnectionRequestReceiver, ConnectionRequester, RequestedConnection,
    },
    establish::ConnectionEstablisher,
    options::{ConnectionOptions, ConnectionPoolOptions},
    Connection, DEFAULT_MAX_POOL_SIZE,
};
use crate::{
    error::{Error, Result},
    event::cmap::{
        CmapEventHandler, ConnectionClosedReason, PoolClearedEvent, PoolClosedEvent,
        PoolCreatedEvent,
    },
    options::StreamAddress,
    runtime::{AsyncJoinHandle, HttpClient},
    RUNTIME,
};

use std::{
    collections::VecDeque,
    sync::{atomic::Ordering, Arc, Weak},
    time::Duration,
};
use tokio::sync::{mpsc, oneshot};

/// The internal state of a connection pool.
#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) struct ConnectionPoolWorker {
    /// The address the pool's connections will connect to.
    address: StreamAddress,

    /// The total number of connections managed by the pool, including connections which are
    /// currently checked out of the pool or have yet to be established.
    total_connection_count: u32,

    /// The ID of the next connection created by the pool.
    next_connection_id: u32,

    /// The current generation of the pool. The generation is incremented whenever the pool is
    /// cleared. Connections belonging to a previous generation are considered stale and will be
    /// closed when checked back in or when popped off of the set of available connections.
    generation: u32,

    /// The established connections that are currently checked into the pool and awaiting usage in
    /// future operations.
    available_connections: VecDeque<Connection>,

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

    /// The maximum number of connections that the pool can manage, including connections checked
    /// out of the pool. If a thread requests a connection and the pool is empty + there are already
    /// max_pool_size connections in use, it will block until one is returned or the wait_queue_timeout
    /// is exceeded.
    max_pool_size: u32,

    /// Receiver used to determine if any threads hold references to this pool. If all the
    /// sender ends of this receiver drop, this worker will be notified and drop too.
    liveness_receiver: mpsc::Receiver<()>,

    request_receiver: ConnectionRequestReceiver,

    management_receiver: mpsc::UnboundedReceiver<PoolManagementRequest>,

    management_sender: mpsc::UnboundedSender<PoolManagementRequest>,

    unfinished_check_out: Option<ConnectionRequest>,
}

impl ConnectionPoolWorker {
    /// Starts a worker and returns a manager and connection requester, as well as a handle
    /// to the worker. Once all handles are dropped, the work will cease execution.
    pub(super) fn new(
        address: StreamAddress,
        http_client: HttpClient,
        options: Option<ConnectionPoolOptions>,
    ) -> (PoolManager, ConnectionRequester, mpsc::Sender<()>) {
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

        let connection_options: Option<ConnectionOptions> = options
            .as_ref()
            .map(|pool_options| ConnectionOptions::from(pool_options.clone()));

        let (connection_requester, request_receiver) = ConnectionRequester::new(address.clone());
        let (management_sender, management_receiver) = mpsc::unbounded_channel();
        let (ref_count_sender, liveness_receiver) = mpsc::channel(1);

        let worker = ConnectionPoolWorker {
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
            request_receiver,
            management_receiver,
            management_sender: management_sender.clone(),
            unfinished_check_out: None,
            liveness_receiver,
        };
        let manager = worker.manage();
        worker.start();

        (manager, connection_requester, ref_count_sender)
    }

    /// Get a manager for this pool which can be used to check in connections or clear the pool.
    pub(super) fn manage(&self) -> PoolManager {
        PoolManager {
            sender: self.management_sender.clone(),
        }
    }

    pub(super) fn start(mut self) {
        RUNTIME.execute(async move {
            let mut maintenance_interval = RUNTIME.interval(Duration::from_millis(500));

            loop {
                println!("worker: waiting for task");
                let task = tokio::select! {
                    Some(result_sender) = self.request_receiver.recv(),
                                          if self.unfinished_check_out.is_none() => PoolTask::CheckOut(result_sender),
                    Some(request) = self.management_receiver.recv() => request.into(),
                    None = self.liveness_receiver.recv() => {
                        // all sender ends of the liveness receiver dropped, meaning this
                        // pool has no more references.
                        break
                    },
                    _ = maintenance_interval.tick() => {
                        PoolTask::Maintenance
                    },
                    else => {
                        println!("worker: breaking worker");
                        break
                    }
                };
                println!("worker: got {:?}", task);

                match task {
                    PoolTask::CheckOut(result_sender) => self.check_out(result_sender).await,
                    PoolTask::CheckIn(connection) => self.check_in(connection),
                    PoolTask::Clear => self.clear(),
                    PoolTask::HandleConnectionFailed(error) => self.handle_connection_failed(error),
                    PoolTask::Maintenance => self.perform_maintenance(),
                    PoolTask::Populate(connection) => self.populate_connection(connection),
                }
            }

            while let Some(connection) = self.available_connections.pop_front() {
                connection.close_and_drop(ConnectionClosedReason::PoolClosed);
            }

            self.emit_event(|handler| {
                handler.handle_pool_closed_event(PoolClosedEvent {
                    address: self.address.clone(),
                });
            });
        })
    }

    async fn check_out(&mut self, request: ConnectionRequest) {
        let mut connection = None;
        while let Some(conn) = self.available_connections.pop_back() {
            // Close the connection if it's stale.
            if conn.is_stale(self.generation) {
                self.close_connection(conn, ConnectionClosedReason::Stale);
                continue;
            }

            // Close the connection if it's idle.
            if conn.is_idle(self.max_idle_time) {
                self.close_connection(conn, ConnectionClosedReason::Idle);
                continue;
            }

            connection = Some(conn);
            break;
        }

        match connection {
            Some(conn) => {
                match request.fulfill(RequestedConnection::Pooled(conn)) {
                    Ok(_) => return,
                    Err(result) => {
                        // thread stopped listening, indicating it hit the WaitQueue
                        // timeout.
                        self.available_connections
                            .push_back(result.unwrap_pooled_connection());
                    }
                }
            }
            None if self.total_connection_count < self.max_pool_size => {
                let event_handler = self.event_handler.clone();
                let establisher = self.establisher.clone();
                let pending_connection = self.create_pending_connection();
                let manager = self.manage();
                let handle = RUNTIME
                    .spawn(async move {
                        let mut establish_result = establish_connection(
                            &establisher,
                            pending_connection,
                            &manager,
                            event_handler.as_ref(),
                        )
                        .await;

                        if let Ok(ref mut c) = establish_result {
                            c.mark_as_in_use(manager.clone());
                        }

                        establish_result
                    })
                    .unwrap();

                // this will fail if the other end stopped listening, in which case
                // we just let the connection establish in the background.
                let _ = request.fulfill(RequestedConnection::Establishing(handle));
            }
            None => self.unfinished_check_out = Some(request),
        }
    }

    fn create_pending_connection(&mut self) -> PendingConnection {
        self.total_connection_count += 1;

        let pending_connection = PendingConnection {
            id: self.next_connection_id,
            address: self.address.clone(),
            generation: self.generation,
            options: self.connection_options.clone(),
        };
        self.next_connection_id += 1;
        self.emit_event(|handler| {
            handler.handle_connection_created_event(pending_connection.created_event())
        });

        pending_connection
    }

    fn handle_connection_failed(&mut self, error: Error) {
        if error.is_authentication_error() {
            // auth spec requires pool is cleared after encountering auth error.
            self.clear();
        }
        // Establishing a pending connection failed, so that must be reflected in to total
        // connection count.
        self.total_connection_count -= 1;
    }

    fn check_in(&mut self, mut conn: Connection) {
        self.emit_event(|handler| {
            handler.handle_connection_checked_in_event(conn.checked_in_event());
        });

        conn.mark_as_available();

        // Close the connection if it's stale.
        if conn.is_stale(self.generation) {
            self.close_connection(conn, ConnectionClosedReason::Stale);
        } else if conn.is_executing() {
            self.close_connection(conn, ConnectionClosedReason::Dropped)
        } else {
            match self.unfinished_check_out.take() {
                Some(result_sender) => {
                    if let Err(conn) = result_sender.fulfill(RequestedConnection::Pooled(conn)) {
                        self.available_connections
                            .push_back(conn.unwrap_pooled_connection());
                    }
                }
                None => {
                    self.available_connections.push_back(conn);
                }
            }
        }
    }

    fn populate_connection(&mut self, mut conn: Connection) {
        conn.mark_as_available();
        self.available_connections.push_back(conn);
    }

    fn clear(&mut self) {
        self.generation += 1;
        self.emit_event(|handler| {
            let event = PoolClearedEvent {
                address: self.address.clone(),
            };

            handler.handle_pool_cleared_event(event);
        });
    }

    fn emit_event<F>(&self, emit: F)
    where
        F: FnOnce(&Arc<dyn CmapEventHandler>),
    {
        if let Some(ref handler) = self.event_handler {
            emit(handler);
        }
    }

    /// Close a connection, emit the event for it being closed, and decrement the
    /// total connection count.
    fn close_connection(&mut self, connection: Connection, reason: ConnectionClosedReason) {
        connection.close_and_drop(reason);
        self.total_connection_count -= 1;
    }

    fn perform_maintenance(&mut self) {
        self.remove_perished_connections();
        self.ensure_min_connections();
    }

    /// Iterate over the connections and remove any that are stale or idle.
    fn remove_perished_connections(&mut self) {
        // re-acquire the lock between loop iterations to allow other threads to use the pool.
        loop {
            if self.available_connections.len() == 0 {
                break;
            }

            let (connection, close_reason) =
                if self.available_connections[0].is_stale(self.generation) {
                    // the following unwrap is okay becaue we asserted the pool was nonempty
                    (
                        self.available_connections.pop_front().unwrap(),
                        ConnectionClosedReason::Stale,
                    )
                } else if self.available_connections[0].is_idle(self.max_idle_time) {
                    // the following unwrap is okay becaue we asserted the pool was nonempty
                    (
                        self.available_connections.pop_front().unwrap(),
                        ConnectionClosedReason::Idle,
                    )
                } else {
                    // All subsequent connections are either not idle or not stale since they were
                    // checked into the pool later, so we can just quit early.
                    break;
                };

            self.close_connection(connection, close_reason);
        }
    }

    fn ensure_min_connections(&mut self) {
        if let Some(min_pool_size) = self.min_pool_size {
            while self.total_connection_count < min_pool_size {
                let pending_connection = self.create_pending_connection();
                let event_handler = self.event_handler.clone();
                let manager = PoolManager {
                    sender: self.management_sender.clone(),
                };
                let establisher = self.establisher.clone();
                RUNTIME.execute(async move {
                    let connection = establish_connection(
                        &establisher,
                        pending_connection,
                        &manager,
                        event_handler.as_ref(),
                    )
                    .await;

                    if let Ok(connection) = connection {
                        manager.populate_connection(connection);
                    }
                });
            }
        }
    }
}

async fn establish_connection(
    establisher: &ConnectionEstablisher,
    pending_connection: PendingConnection,
    manager: &PoolManager,
    event_handler: Option<&Arc<dyn CmapEventHandler>>,
) -> Result<Connection> {
    let mut establish_result = establisher.establish_connection(pending_connection).await;

    match establish_result {
        Err(ref e) => {
            manager.handle_connection_failed(e.clone());
        }
        Ok(ref mut connection) => {
            if let Some(handler) = event_handler {
                handler.handle_connection_ready_event(connection.ready_event())
            };
        }
    }

    establish_result
}

#[derive(Debug)]
enum PoolTask {
    Clear,
    CheckIn(Connection),
    CheckOut(ConnectionRequest),
    Populate(Connection),
    HandleConnectionFailed(Error),
    Maintenance,
}

impl From<PoolManagementRequest> for PoolTask {
    fn from(request: PoolManagementRequest) -> Self {
        match request {
            PoolManagementRequest::CheckIn(c) => PoolTask::CheckIn(c),
            PoolManagementRequest::Clear => PoolTask::Clear,
            PoolManagementRequest::HandleConnectionFailed(error) => {
                PoolTask::HandleConnectionFailed(error)
            }
            PoolManagementRequest::Populate(c) => PoolTask::Populate(c),
        }
    }
}

pub(super) enum PoolManagementRequest {
    Clear,
    Populate(Connection),
    CheckIn(Connection),
    HandleConnectionFailed(Error),
}

#[derive(Clone, Debug)]
pub(super) struct PoolManager {
    pub(super) sender: mpsc::UnboundedSender<PoolManagementRequest>,
}

impl PoolManager {
    pub(super) fn clear(&self) {
        let _ = self.sender.send(PoolManagementRequest::Clear);
    }

    pub(super) fn check_in(&self, connection: Connection) {
        let _ = self.sender.send(PoolManagementRequest::CheckIn(connection));
    }

    fn handle_connection_failed(&self, error: Error) {
        let _ = self
            .sender
            .send(PoolManagementRequest::HandleConnectionFailed(error));
    }

    fn populate_connection(&self, connection: Connection) {
        let _ = self
            .sender
            .send(PoolManagementRequest::Populate(connection));
    }
}
