use std::{
    collections::VecDeque,
    ops::Deref,
    sync::Arc,
    time::{Duration, Instant}, cell::{Ref, RefCell},
};

use bson::{doc, spec::BinarySubtype, Bson, Document, TimeStamp};
use serde::{Serialize, Deserialize};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{Client, RUNTIME};

/// Session to be used with client operations. This acts as a handle to a server session.
/// This keeps the details of how server sessions are pooled opaque to users.
#[derive(Debug, Clone)]
pub struct ClientSession {
    state: RefCell<Arc<ClientSessionState>>,
    client: Client,
}

impl ClientSession {
    pub(crate) fn new(server_session: ServerSession, client: Client) -> Self {
        Self {
            client,
            state: RefCell::new(Arc::new(ClientSessionState::new(server_session))),
        }
    }

    /// The id of this session.
    pub(crate) fn id(&self) -> impl Deref<Target = Document> + '_ {
        // state.server_session.assert_is_non_null();
        Ref::map(self.state.borrow(), |state| &state.server_session.id)
    }

    pub(crate) fn cluster_time(&self) -> Option<&ClusterTime> {
        todo!()
        // self.state.cluster_time.as_ref()
    }

    pub(crate) fn advance_cluster_time(&mut self, to: ClusterTime) {
        todo!()
        // self.state.cluster_time = Some(to);
    }

    fn take(&mut self) -> ClientSessionState {
        todo!()
        // self.state.replace(ClientSessionState::new(ServerSession::null()))
    }
}

// impl Drop for ClientSession {
//     fn drop(&mut self) {
//         let client = self.client.clone();
//         let state = self.take();
//         RUNTIME.execute(async move {
//             client.check_in_server_session(state.server_session).await;
//         })
//     }
// }

/// Struct encapsulating shared state among `ClientSession` instances.
#[derive(Debug)]
struct ClientSessionState {
    cluster_time: Option<ClusterTime>,
    server_session: ServerSession,
}

impl ClientSessionState {
    fn new(server_session: ServerSession) -> Self {
        Self { cluster_time: None, server_session }
    }
}

/// Client side abstraction of a server session. These are pooled and may be associated with
/// multiple `ClientSession`s over the course of their lifetime.
#[derive(Debug)]
pub(crate) struct ServerSession {
    /// The id of the server session to which this corresponds.
    pub(super) id: Document,

    /// The last time an operation was executed in this session.
    last_use: std::time::Instant,

    /// Whether a network error was encounterd while using this session.
    pub(super) dirty: bool,
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
        self.last_use + logical_session_timeout > Instant::now() - Duration::from_secs(60)
    }
}

#[derive(Debug, Default)]
pub(crate) struct ServerSessionPool {
    pool: Mutex<VecDeque<ServerSession>>,
}

impl ServerSessionPool {
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
            break;
        }

        if !session.dirty && !session.is_about_to_expire(logical_session_timeout) {
            pool.push_front(session);
        }
    }
}

#[derive(Debug, Deserialize, Clone, Serialize)]
pub(crate) struct ClusterTime {
    cluster_time: TimeStamp,
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
        let lhs = self.cluster_time.t as u64 + self.cluster_time.i as u64;
        let rhs = other.cluster_time.t as u64 + other.cluster_time.i as u64;
        lhs.cmp(&rhs)
    }
}

impl std::cmp::PartialOrd for ClusterTime {
    fn partial_cmp(&self, other: &ClusterTime) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
