pub mod auth;
mod executor;
pub mod options;
mod session;

use std::{sync::Arc, time::Duration};

use time::PreciseTime;

use bson::{doc, Bson, Document};
use derivative::Derivative;

#[cfg(test)]
use crate::options::StreamAddress;
use crate::{
    cmap::Command,
    concern::{ReadConcern, WriteConcern},
    db::Database,
    error::{ErrorKind, Result},
    event::command::CommandEventHandler,
    operation::ListDatabases,
    options::{ClientOptions, DatabaseOptions, ReadPreference, SelectionCriteria},
    sdam::{Server, SessionSupportStatus, Topology},
};
use session::ServerSessionPool;
pub(crate) use session::{ClientSession, ServerSession, ClusterTime, SESSIONS_UNSUPPORTED_COMMANDS};
use tokio::sync::RwLock;

const DEFAULT_SERVER_SELECTION_TIMEOUT: Duration = Duration::from_secs(30);

/// This is the main entry point for the API. A `Client` is used to connect to a MongoDB cluster.
/// By default, it will monitor the topology of the cluster, keeping track of any changes, such
/// as servers being added or removed
///
/// `Client` uses [`std::sync::Arc`](https://doc.rust-lang.org/std/sync/struct.Arc.html) internally,
/// so it can safely be shared across threads. For example:
///
/// ```rust
/// # use mongodb::{Client, error::Result};
/// #
/// # async fn start_workers() -> Result<()> {
/// let client = Client::with_uri_str("mongodb://example.com").await?;
///
/// for i in 0..5 {
///     let client_ref = client.clone();
///
///     std::thread::spawn(move || {
///         let collection = client_ref.database("items").collection(&format!("coll{}", i));
///
///         // Do something with the collection
///     });
/// }
/// #
/// # // Technically we should join the threads here, but for the purpose of the example, we'll just
/// # // sleep for a bit.
/// # std::thread::sleep(std::time::Duration::from_secs(3));
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct Client {
    inner: Arc<ClientInner>,
}

#[derive(Derivative)]
#[derivative(Debug)]
struct ClientInner {
    topology: Topology,
    options: ClientOptions,
    session_pool: ServerSessionPool,
    cluster_time: RwLock<Option<ClusterTime>>,
}

impl Client {
    /// Creates a new `Client` connected to the cluster specified by `uri`. `uri` must be a valid
    /// MongoDB connection string.
    ///
    /// See the documentation on
    /// [`ClientOptions::parse`](options/struct.ClientOptions.html#method.parse) for more details.
    pub async fn with_uri_str(uri: &str) -> Result<Self> {
        let options = ClientOptions::parse(uri).await?;

        Client::with_options(options)
    }

    /// Creates a new `Client` connected to the cluster specified by `options`.
    pub fn with_options(options: ClientOptions) -> Result<Self> {
        options.validate()?;

        let inner = Arc::new(ClientInner {
            topology: Topology::new(options.clone())?,
            session_pool: ServerSessionPool::new(),
            cluster_time: RwLock::new(None),
            options,
        });

        Ok(Self { inner })
    }

    pub(crate) fn emit_command_event(&self, emit: impl FnOnce(&Arc<dyn CommandEventHandler>)) {
        if let Some(ref handler) = self.inner.options.command_event_handler {
            emit(handler);
        }
    }

    /// Gets the default selection criteria the `Client` uses for operations..
    pub fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        self.inner.options.selection_criteria.as_ref()
    }

    /// Gets the default read concern the `Client` uses for operations.
    pub fn read_concern(&self) -> Option<&ReadConcern> {
        self.inner.options.read_concern.as_ref()
    }

    /// Gets the default write concern the `Client` uses for operations.
    pub fn write_concern(&self) -> Option<&WriteConcern> {
        self.inner.options.write_concern.as_ref()
    }

    /// Gets a handle to a database specified by `name` in the cluster the `Client` is connected to.
    /// The `Database` options (e.g. read preference and write concern) will default to those of the
    /// `Client`.
    ///
    /// This method does not send or receive anything across the wire to the database, so it can be
    /// used repeatedly without incurring any costs from I/O.
    pub fn database(&self, name: &str) -> Database {
        Database::new(self.clone(), name, None)
    }

    /// Gets a handle to a database specified by `name` in the cluster the `Client` is connected to.
    /// Operations done with this `Database` will use the options specified by `options` by default
    /// and will otherwise default to those of the `Client`.
    ///
    /// This method does not send or receive anything across the wire to the database, so it can be
    /// used repeatedly without incurring any costs from I/O.
    pub fn database_with_options(&self, name: &str, options: DatabaseOptions) -> Database {
        Database::new(self.clone(), name, Some(options))
    }

    /// Gets information about each database present in the cluster the Client is connected to.
    pub async fn list_databases(
        &self,
        filter: impl Into<Option<Document>>,
    ) -> Result<Vec<Document>> {
        let op = ListDatabases::new(filter.into(), false);
        self.execute_operation(op).await
    }

    /// Gets the names of the databases present in the cluster the Client is connected to.
    pub async fn list_database_names(
        &self,
        filter: impl Into<Option<Document>>,
    ) -> Result<Vec<String>> {
        let op = ListDatabases::new(filter.into(), true);
        match self.execute_operation(op).await {
            Ok(databases) => databases
                .into_iter()
                .map(|doc| {
                    let name = doc.get("name").and_then(Bson::as_str).ok_or_else(|| {
                        ErrorKind::ResponseError {
                            message: "Expected \"name\" field in server response, but it was not \
                                      found"
                                .to_string(),
                        }
                    })?;
                    Ok(name.to_string())
                })
                .collect(),
            Err(e) => Err(e),
        }
    }

    /// Check in a server session to the server session pool.
    /// If the session is expired or dirty, or the topology no longer supports sessions, the session
    /// will be discarded.
    pub(crate) async fn check_in_server_session(&self, session: ServerSession) {
        let session_support_status = self.inner.topology.session_support_status().await;
        if let SessionSupportStatus::Supported {
            logical_session_timeout,
        } = session_support_status
        {
            self.inner
                .session_pool
                .check_in(session, logical_session_timeout)
                .await;
        }
    }

    /// Ends a server session via the `endSessions` command if the session is not dirty.
    /// This method ignores any errors returned as part of the session ending process.
    #[allow(dead_code)]
    async fn end_server_session(&self, session: ServerSession) {
        async fn try_end(client: &Client, session: ServerSession) -> Result<()> {
            let server = client
                .select_server(Some(&ReadPreference::Primary.into()))
                .await?;
            let mut connection = server.checkout_connection().await?;
            let command = Command::new(
                "endSessions".to_string(),
                "admin".to_string(),
                doc! {
                    "endSessions": [ { "id": session.id } ]
                },
            );
            let _: Result<_> = connection.send_command(command, None).await;
            Ok(())
        }
        if !session.dirty {
            let _: Result<_> = try_end(self, session).await;
        }
    }

    pub(crate) async fn start_session_with_timeout(
        &self,
        logical_session_timeout: Duration,
    ) -> ClientSession {
        ClientSession::new(
            self.inner
                .session_pool
                .check_out(logical_session_timeout)
                .await,
            self.clone(),
        )
    }

    /// Sets the client's cluster time to the provided one if it is higher than the client's current highest
    /// seen value.
    async fn update_cluster_time(&self, cluster_time: &ClusterTime) {
        let mut client_time_lock = self.inner.cluster_time.write().await;
        if let Some(ref client_time) = *client_time_lock {
            if client_time > cluster_time {
                return;
            }
        }
        *client_time_lock = Some(cluster_time.clone());
    }
    
    /// Get the address of the server selected according to the given criteria.
    /// This method is only used in tests.
    #[cfg(test)]
    pub(crate) async fn test_select_server(
        &self,
        criteria: Option<&SelectionCriteria>,
    ) -> Result<StreamAddress> {
        let server = self.select_server(criteria).await?;
        Ok(server.address.clone())
    }

    /// Select a server using the provided criteria. If none is provided, a primary read preference
    /// will be used instead.
    async fn select_server(&self, criteria: Option<&SelectionCriteria>) -> Result<Arc<Server>> {
        let criteria =
            criteria.unwrap_or_else(|| &SelectionCriteria::ReadPreference(ReadPreference::Primary));

        let start_time = PreciseTime::now();
        let timeout = time::Duration::from_std(
            self.inner
                .options
                .server_selection_timeout
                .unwrap_or(DEFAULT_SERVER_SELECTION_TIMEOUT),
        )?;

        loop {
            let selected_server = self
                .inner
                .topology
                .attempt_to_select_server(criteria)
                .await?;

            if let Some(server) = selected_server {
                return Ok(server);
            }

            self.inner.topology.request_topology_check();

            let time_passed = start_time.to(PreciseTime::now());
            let time_remaining = std::cmp::max(time::Duration::zero(), timeout - time_passed);

            let message_received = self
                .inner
                .topology
                .wait_for_topology_change(time_remaining.to_std()?)
                .await;

            if !message_received {
                return Err(ErrorKind::ServerSelectionError {
                    message: self
                        .inner
                        .topology
                        .server_selection_timeout_error_message(&criteria)
                        .await,
                }
                .into());
            }
        }
    }
}
