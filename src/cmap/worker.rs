use super::{Connection, ConnectionPoolInner};
use crate::{
    error::Result, event::cmap::ConnectionClosedReason, runtime::AsyncJoinHandle, RUNTIME,
};

use std::sync::{atomic::Ordering, Weak};
use tokio::sync::{mpsc, oneshot};

pub(super) fn start(
    mut request_receiver: mpsc::UnboundedReceiver<oneshot::Sender<ConnectionRequestResult>>,
    mut connection_receiver: mpsc::UnboundedReceiver<Connection>,
    pool: Weak<ConnectionPoolInner>,
) {
    RUNTIME.execute(async move {
        // fn make_fut<T>(receiver: &mut mpsc::UnboundedReceiver<T>) ->

        // while let Some(task) =
        // let mut request_recv = None;
        // let mut conn_recv = None;
        let mut unfinished_task: Option<oneshot::Sender<ConnectionRequestResult>> = None;

        loop {
            // let request_recv_fut = match request_recv.take() {
            //     Some(fut) => fut,
            //     None => Box::pin(request_receiver.recv()),
            // };
            // // .unwrap_or_else(|| pin_mut!(request_receiver.recv()));
            // let conn_recv_fut = conn_recv
            //     .take()
            //     .unwrap_or_else(|| Box::pin(connection_receiver.recv()));
            // // .unwrap_or_else(|| pin_mut!(connection_receiver.recv()));

            // // pin_mut!(request_recv_fut);
            // // pin_mut!(conn_recv_fut);

            // let task = futures::future::select(request_recv_fut, conn_recv_fut).await;
            // TODO emit event
            println!("worker: waiting for task");
            let task = tokio::select! {
                Some(result_sender) = request_receiver.recv(), if unfinished_task.is_none() => PoolTask::CheckOut(result_sender),
                Some(connection) = connection_receiver.recv() => PoolTask::CheckIn(connection),
                else => {
                    println!("worker: breaking worker");
                    break
                }
            };
            println!("worker: got {:?}", task);

            let pool = match pool.upgrade() {
                Some(p) => p,
                None => return,
            };

            println!("worker: upgraded pool");

            match task {
                PoolTask::CheckOut(result_sender) => {
                    // conn_recv = Some(fut);

                    let mut connection = None;
                    let mut available_connections_lock = pool.available_connections.lock().await;
                    while let Some(conn) = available_connections_lock.pop_back() {
                        // Close the connection if it's stale.
                        if conn.is_stale(pool.generation.load(Ordering::SeqCst)) {
                            pool.close_connection(conn, ConnectionClosedReason::Stale);
                            continue;
                        }

                        // Close the connection if it's idle.
                        if conn.is_idle(pool.max_idle_time) {
                            pool.close_connection(conn, ConnectionClosedReason::Idle);
                            continue;
                        }

                        connection = Some(conn);
                        break;
                    }

                    match connection {
                        Some(conn) => {
                            match result_sender.send(ConnectionRequestResult::Pooled(conn)) {
                                Ok(_) => continue,
                                Err(result) => {
                                    // thread stopped listening, indicating it hit the WaitQueue timeout.
                                    available_connections_lock
                                        .push_back(result.unwrap_pooled_connection());
                                }
                            }
                        }
                        None if pool.total_connection_count.load(Ordering::SeqCst)
                            < pool.max_pool_size =>
                        {
                            let pending_connection = pool.create_pending_connection();
                            drop(available_connections_lock);

                            let handle = RUNTIME
                                .spawn(async move {
                                    pool.establish_connection(pending_connection).await
                                })
                                .unwrap();

                            // this will fail if the other end stopped listening, in which case we just
                            // let the connection establish in the background.
                            let _ =
                                result_sender.send(ConnectionRequestResult::Establishing(handle));
                        },
                        None => unfinished_task = Some(result_sender),
                    }
                }
                PoolTask::CheckIn(connection) => {
                    match unfinished_task.take() {
                        Some(result_sender) => {
                            if let Err(conn) = result_sender.send(ConnectionRequestResult::Pooled(connection)) {
                                let mut available_connections_lock = pool.available_connections.lock().await;
                                available_connections_lock.push_back(conn.unwrap_pooled_connection());
                            }
                        }
                        None => {
                            let mut available_connections_lock = pool.available_connections.lock().await;
                            available_connections_lock.push_back(connection);
                        }
                    }
                    // request_recv = Some(fut);
                }
            }
        }
    })
}

#[derive(Debug)]
enum PoolTask {
    CheckIn(Connection),
    CheckOut(oneshot::Sender<ConnectionRequestResult>),
}

#[derive(Debug)]
pub(super) enum ConnectionRequestResult {
    Pooled(Connection),
    Establishing(AsyncJoinHandle<Result<Connection>>),
}

impl ConnectionRequestResult {
    fn unwrap_pooled_connection(self) -> Connection {
        match self {
            ConnectionRequestResult::Pooled(c) => c,
            _ => panic!("attempted to unwrap pooled connection when was establishing"),
        }
    }
}
