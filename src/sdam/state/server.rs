use std::sync::atomic::{AtomicU32, Ordering};

use crate::{
    cmap::{options::ConnectionPoolOptions, Connection, ConnectionPool},
    error::Result,
    options::{ClientOptions, StreamAddress},
    runtime::HttpClient,
};

/// Contains the state for a given server in the topology.
#[derive(Debug)]
pub(crate) struct Server {
    pub(crate) address: StreamAddress,

    /// The connection pool for the server.
    pool: ConnectionPool,

    /// Number of operations currently using this server.
    opcount: AtomicU32,
}

impl Server {
    #[cfg(test)]
    pub(crate) fn new_mocked(address: StreamAddress, opcount: u32) -> Self {
        Self {
            address: address.clone(),
            pool: ConnectionPool::new_mocked(address),
            opcount: AtomicU32::new(opcount),
        }
    }

    pub(crate) fn new(
        address: StreamAddress,
        options: &ClientOptions,
        http_client: HttpClient,
    ) -> Self {
        Self {
            pool: ConnectionPool::new(
                address.clone(),
                http_client,
                Some(ConnectionPoolOptions::from_client_options(options)),
            ),
            address,
            opcount: AtomicU32::new(0),
        }
    }

    /// Creates a new Server given the `address` and `options`.
    /// Checks out a connection from the server's pool.
    pub(crate) async fn checkout_connection(&self) -> Result<Connection> {
        self.pool.check_out().await
    }

    /// Clears the connection pool associated with the server.
    pub(crate) fn clear_connection_pool(&self) {
        self.pool.clear();
    }

    pub(crate) fn increment_opcount(&self) {
        self.opcount.fetch_add(1, Ordering::SeqCst);
    }

    pub(crate) fn decrement_opcount(&self) {
        self.opcount.fetch_sub(1, Ordering::SeqCst);
    }

    pub(crate) fn opcount(&self) -> u32 {
        self.opcount.load(Ordering::SeqCst)
    }
}
