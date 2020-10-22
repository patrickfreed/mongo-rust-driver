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
}

impl Server {
    #[cfg(test)]
    pub(crate) fn new_mocked(
        address: StreamAddress,
        max_pool_size: u32,
        active_connection_count: u32,
        available_connection_count: u32,
    ) -> Self {
        Self {
            address,
            pool: ConnectionPool::new_mocked(
                max_pool_size,
                active_connection_count,
                available_connection_count,
            ),
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

    /// Number of pending and in use connections. Approximates load against this server.
    pub(crate) fn active_connection_count(&self) -> u32 {
        self.pool.active_connection_count()
    }

    /// Number of unused connections in this server's pool.
    pub(crate) fn available_connection_count(&self) -> u32 {
        self.pool.available_connection_count()
    }

    /// Maximum number of connections that can be opened against this server.
    pub(crate) fn max_pool_size(&self) -> u32 {
        self.pool.max_pool_size()
    }
}
