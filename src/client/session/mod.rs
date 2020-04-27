#[cfg(test)]
mod test;

use std::{
    collections::{HashSet, VecDeque},
    time::{Duration, Instant},
};

use bson::{doc, spec::BinarySubtype, Bson, Document, TimeStamp};
use derivative::Derivative;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{Client, RUNTIME};

lazy_static! {
    pub(crate) static ref SESSIONS_UNSUPPORTED_COMMANDS: HashSet<&'static str> = {
        let mut hash_set = HashSet::new();
        hash_set.insert("killcursors");
        hash_set.insert("parallelcollectionscan");
        hash_set
    };
}

/// Session to be used with client operations. This acts as a handle to a server session.
/// This keeps the details of how server sessions are pooled opaque to users.
#[derive(Debug)]
pub struct ClientSession {
    cluster_time: Option<ClusterTime>,
    server_session: ServerSession,
    client: Client,
    is_implicit: bool,
}

impl ClientSession {
    /// Creates a new `ClientSession` wrapping the provided server session.
    pub(crate) fn new(server_session: ServerSession, client: Client) -> Self {
        Self {
            client,
            server_session,
            cluster_time: None,
            is_implicit: true,
        }
    }

    /// The id of this session.
    pub(crate) fn id(&self) -> &Document {
        self.server_session.assert_is_non_null();
        &self.server_session.id
    }

    /// Whether this session was created implicitly by the driver or explcitly by the user.
    pub(crate) fn is_implicit(&self) -> bool {
        self.is_implicit
    }

    /// The highest seen cluster time this session has seen so far.
    /// This will be `None` if this session has not been used in an operation yet.
    pub(crate) fn cluster_time(&self) -> Option<&ClusterTime> {
        self.cluster_time.as_ref()
    }

    /// Set the cluster time to the provided one if it is greater than this session's highest seen
    /// cluster time or if this session's cluster time is `None`.
    pub(crate) fn advance_cluster_time(&mut self, to: &ClusterTime) {
        if self.cluster_time().map(|ct| ct < to).unwrap_or(true) {
            self.cluster_time = Some(to.clone());
        }
    }

    /// Mark this session (and the underlying server session) as dirty.
    pub(crate) fn mark_dirty(&mut self) {
        self.server_session.dirty = true;
    }

    /// Updates the date that the underlying server session was last used as part of an operation
    /// sent to the server.
    pub(crate) fn update_last_use(&mut self) {
        self.server_session.last_use = Instant::now();
    }
}

impl Drop for ClientSession {
    fn drop(&mut self) {
        fn take_server_session(session: &mut ClientSession) -> ServerSession {
            std::mem::replace(&mut session.server_session, ServerSession::null())
        }
        let client = self.client.clone();
        let server_session = take_server_session(self);
        RUNTIME.execute(async move {
            client.check_in_server_session(server_session).await;
        })
    }
}

/// Client side abstraction of a server session. These are pooled and may be associated with
/// multiple `ClientSession`s over the course of their lifetime.
#[derive(Debug)]
pub(crate) struct ServerSession {
    /// The id of the server session to which this corresponds.
    id: Document,

    /// The last time an operation was executed with this session.
    last_use: std::time::Instant,

    /// Whether a network error was encounterd while using this session.
    dirty: bool,
}

impl ServerSession {
    /// Creates a new session, generating the id client side.
    fn new() -> Self {
        let binary = Bson::Binary(BinarySubtype::Uuid, Uuid::new_v4().as_bytes().to_vec());

        Self {
            id: doc! { "id": binary },
            last_use: Instant::now(),
            dirty: false,
        }
    }

    /// Creates a "null" session that is used as a placeholder while a `ClientSession` is being
    /// dropped.
    fn null() -> Self {
        Self {
            id: doc! { "id": Bson::Null },
            last_use: Instant::now(),
            dirty: false,
        }
    }

    /// Asserts this session is non-null.
    fn assert_is_non_null(&self) {
        assert!(self.id != doc! { "id": Bson::Null })
    }

    /// Determines if this server session is about to expire in a short amount of time (1 minute).
    fn is_about_to_expire(&self, logical_session_timeout: Duration) -> bool {
        let expiration_date = self.last_use + logical_session_timeout;
        expiration_date < Instant::now() + Duration::from_secs(60)
    }
}

#[derive(Debug)]
pub(crate) struct ServerSessionPool {
    pool: Mutex<VecDeque<ServerSession>>,
}

impl ServerSessionPool {
    pub(super) fn new() -> Self {
        Self {
            pool: Default::default(),
        }
    }

    /// Checks out a server session from the pool. Before doing so, it first clears out all the
    /// expired ssessions. If there are no sessions left in the pool after clearing expired ones
    /// out, a new session will be created.
    pub(crate) async fn check_out(&self, logical_session_timeout: Duration) -> ServerSession {
        let mut pool = self.pool.lock().await;
        while let Some(session) = pool.pop_front() {
            // If a session is about to expire within the next minute, remove it from pool.
            if session.is_about_to_expire(logical_session_timeout) {
                continue;
            }
            return session;
        }
        ServerSession::new()
    }

    /// Checks in a server session to the pool. If it is about to expire or is dirty, it will be
    /// discarded.
    pub(crate) async fn check_in(&self, session: ServerSession, logical_session_timeout: Duration) {
        let mut pool = self.pool.lock().await;
        while let Some(pooled_session) = pool.pop_back() {
            if session.is_about_to_expire(logical_session_timeout) {
                continue;
            }
            pool.push_back(pooled_session);
            break;
        }

        if !session.dirty && !session.is_about_to_expire(logical_session_timeout) {
            pool.push_front(session);
        }
    }

    #[cfg(test)]
    pub(crate) async fn clear(&self) {
        self.pool.lock().await.clear();
    }

    #[cfg(test)]
    pub(crate) async fn contains(&self, id: &Document) -> bool {
        self.pool.lock().await.iter().any(|s| &s.id == id)
    }
}

#[derive(Derivative, Deserialize, Clone, Serialize)]
#[derivative(Debug)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ClusterTime {
    cluster_time: TimeStamp,

    #[derivative(Debug = "ignore")]
    signature: Document,
}

impl std::cmp::PartialEq for ClusterTime {
    fn eq(&self, other: &ClusterTime) -> bool {
        self.cluster_time == other.cluster_time
    }
}

impl std::cmp::Eq for ClusterTime {}

impl std::cmp::Ord for ClusterTime {
    fn cmp(&self, other: &ClusterTime) -> std::cmp::Ordering {
        let lhs = (self.cluster_time.t, self.cluster_time.i);
        let rhs = (other.cluster_time.t, other.cluster_time.i);
        lhs.cmp(&rhs)
    }
}

impl std::cmp::PartialOrd for ClusterTime {
    fn partial_cmp(&self, other: &ClusterTime) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
