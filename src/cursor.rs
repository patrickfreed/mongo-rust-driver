use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use bson::{doc, Document};
use derivative::Derivative;
use futures::{future::BoxFuture, stream::Stream};

use crate::{
    error::{Error, Result},
    operation::GetMore,
    options::StreamAddress,
    results::GetMoreResult,
    Client,
    Namespace,
    RUNTIME, client::ClientSession, Collection,
};

#[derive(Debug, Clone)]
pub(crate) struct CursorSpecification {
    pub(crate) ns: Namespace,
    pub(crate) address: StreamAddress,
    pub(crate) id: i64,
    pub(crate) batch_size: Option<u32>,
    pub(crate) max_time: Option<Duration>,
    pub(crate) buffer: VecDeque<Document>,
}

struct OC<'a> {
    h: SessionCursorHandle<'a, 'a>
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct Cursor {
    session_cursor: SessionCursor,
    #[derivative(Debug = "ignore")]
    h: OC<'static>,
    session: ClientSession,
}

/// A future representing the result of a getMore.
type GetMoreFuture<'a> = BoxFuture<'a, FetchResult<'a>>;

/// Struct returned from awaiting on a `GetMoreFuture` containing the result of the getMore as
/// well as the reference to the `ClientSession` used for the getMore.
struct FetchResult<'a> {
    get_more_result: Result<GetMoreResult>,
    session: &'a mut ClientSession,
}

/// Wrapper around a mutable reference to a `ClientSession` that provides move semantics.
/// This is required 
struct MutableSessionReference<'a> {
    reference: &'a mut ClientSession
}

/// A cursor that was started with a session and must be iterated using one.
#[derive(Debug)]
pub(crate) struct SessionCursor {
    buffer: VecDeque<Document>,
    exhausted: bool,
    client: Client,
    spec: CursorSpecification,
}

impl SessionCursor {
    fn new(client: Client, spec: CursorSpecification) -> Self {
        Self {
            exhausted: spec.id == 0,
            client,
            spec,
            buffer: VecDeque::new(),
        }
    }

    fn with_session<'session>(&mut self, session: &'session mut ClientSession) -> SessionCursorHandle<'_, 'session> {
        SessionCursorHandle::new(self, session)
    }
}

/// Enum determining whether a `SessionCursorHandle` is excuting a getMore or not.
/// In charge of maintaining ownership of the session reference.
enum ExecutionState<'session> {
    /// The handle is currently executing a getMore via the future.
    ///
    /// This future owns the reference to the session and will return it on completion.
    Executing(GetMoreFuture<'session>),

    /// No future is being executed.
    ///
    /// This variant needs a `MutableSessionReference` struct that can be moved in order to transition
    /// to `Executing` via `take_mut`.
    Idle(MutableSessionReference<'session>)
}

impl<'session> ExecutionState<'session> {
    /// Create an idle instance of this state from the provided mutable reference.
    fn idle(reference: &'session mut ClientSession) -> Self {
        Self::Idle(MutableSessionReference { reference })
    }
}

struct SessionCursorHandle<'cursor, 'session> {
    cursor: &'cursor mut SessionCursor,
    execution_state: ExecutionState<'session>,
}

impl<'cursor, 'session> SessionCursorHandle<'cursor, 'session> {
    fn new(cursor: &'cursor mut SessionCursor, session: &'session mut ClientSession) -> Self {
        Self { cursor, execution_state: ExecutionState::idle(session) }
    }

    /// Start executing a getMore if there isn't one already in flight.
    fn start_get_more(&mut self) {
        let client = self.cursor.client.clone();
        let spec = self.cursor.spec.clone();

        take_mut::take(&mut self.execution_state, |execution_state| {
            if let ExecutionState::Idle(session) = execution_state {
                let future = Box::pin(async move {
                    let get_more = GetMore::new(spec);
                    let get_more_result = client.execute_operation_with_session(get_more, session.reference).await;
                    FetchResult {
                        get_more_result,
                        session: session.reference
                    }
                });
                return ExecutionState::Executing(future);
            }
            execution_state
        });
    }
}

impl<'cursor, 'session> Stream for SessionCursorHandle<'session, 'cursor> {
    type Item = Result<Document>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        loop {
            // If we're currently executing a getMore, check the status of it.
            if let ExecutionState::Executing(ref mut future) = self.execution_state {
                match Pin::new(future).poll(cx) {
                    Poll::Ready(fetch_result) => {
                        self.execution_state = ExecutionState::idle(fetch_result.session);

                        match fetch_result.get_more_result {
                            // If the getMore succeeded, update the parent cursor's buffer and mark whether it's
                            // exhausted or not.
                            Ok(get_more_result) => {
                                let buffer: VecDeque<_> =
                                    get_more_result.batch.into_iter().collect();
                                self.cursor.exhausted = get_more_result.exhausted;
                                self.cursor.buffer = buffer;
                            },
                            // If the getMore failed, just return the error.
                            Err(e) => return Poll::Ready(Some(Err(e)))
                        }
                    }
                    
                    // If the getMore has not finished, keep the state as Executing and return.
                    Poll::Pending => return Poll::Pending,
                }
            }

            match self.cursor.buffer.pop_front() {
                Some(doc) => return Poll::Ready(Some(Ok(doc))),
                None if !self.cursor.exhausted => {
                    self.start_get_more();
                },
                None => return Poll::Ready(None),
            }
        }
    }
}

impl<'session> GetMoreProviderResult for FetchResult<'session> {
    fn get_more_result(&self) -> Result<GetMoreResult> { unimplemented!() }
}

impl<'session> GetMoreProvider for ExecutionState<'session> {
    type GMResult = FetchResult<'session>;
    type GMFuture = GetMoreFuture<'session>;
    fn executing_future(&mut self) -> Option<&mut Self::GMFuture> { unimplemented!() }
    fn clear_execution(&mut self, result: Self::GMResult) { unimplemented!() }
    fn start_execution(&mut self, spec: CursorSpecification, client: Client) { unimplemented!() }
}

struct OwnedCursor {
    wrapped: GenericCursor<OwnedGetMoreProvider>,
}

impl OwnedCursor {
    pub(crate) fn new(spec: CursorSpecification, client: Client, session: ClientSession) -> Self {
        let provider = OwnedGetMoreProvider::Idle(session);
        Self { wrapped: GenericCursor::new(spec, client, provider) }
    }
}

// impl Collection {
//     async fn owned_find(&self) -> OwnedCursor {
//         let spec = CursorSpecification {
//             ns: self.namespace(),
//             address: StreamAddress::parse("localhost:27017").unwrap(),
//             id: 12,
//             batch_size: None,
//             max_time: None,
//             buffer: VecDeque::new(),
//         };

//         OwnedCursor::new(spec, client, session)
//     }
// }

impl Cursor {
    pub(crate) fn new(client: Client, spec: CursorSpecification, session: ClientSession) -> Self {
        todo!();
        
        // let mut session_cursor = SessionCursor::new(client, spec); 
        // Self {
        //     session_cursor,
        //     h: OC { h: session_cursor.with_session(&mut session) },
        //     session,
        // }
    }
}

impl Drop for SessionCursor {
    fn drop(&mut self) {
        if self.exhausted {
            return;
        }

        let namespace = self.spec.ns.clone();
        let client = self.client.clone();
        let cursor_id = self.spec.id.clone();

        RUNTIME.execute(async move {
            let _: Result<_> = client
                .database(&namespace.db)
                .run_command(
                    doc! {
                        "killCursors": &namespace.coll,
                        "cursors": [cursor_id]
                    },
                    None,
                )
                .await;
        })
    }
}

struct GenericCursor<T: GetMoreProvider + Unpin> {
    provider: T,
    buffer: VecDeque<Document>,
    client: Client,
    spec: CursorSpecification,
    exhausted: bool,
}

impl<T: GetMoreProvider + Unpin> GenericCursor<T> {
    fn new(spec: CursorSpecification, client: Client, get_more_provider: T) -> Self {
        Self {
            exhausted: spec.id == 0,
            spec,
            client,
            buffer: VecDeque::new(),
            provider: get_more_provider,
        }
    }
}
    
impl<T: GetMoreProvider + Unpin> Stream for GenericCursor<T> {
    type Item = Result<Document>;
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(future) = self.provider.executing_future() {
                match Pin::new(future).poll(cx) {
                    Poll::Ready(fetch_result) => {
                        let get_more_result = fetch_result.get_more_result();
                        self.provider.clear_execution(fetch_result);

                        match get_more_result {
                            Ok(get_more_result) => {
                                let buffer: VecDeque<_> =
                                    get_more_result.batch.into_iter().collect();
                                self.exhausted = get_more_result.exhausted;
                                self.buffer = buffer;
                            },
                            Err(e) => return Poll::Ready(Some(Err(e)))
                        }
                    },
                    Poll::Pending => return Poll::Pending,
                }
            }

            match self.buffer.pop_front() {
                Some(doc) => return Poll::Ready(Some(Ok(doc))),
                None if !self.exhausted => {
                    let spec = self.spec.clone();
                    let client = self.client.clone();
                    self.provider.start_execution(spec, client);
                },
                None => return Poll::Ready(None),
            }
        }
    }
}

trait GetMoreProvider {
    type GMResult: GetMoreProviderResult;
    type GMFuture: Future<Output = Self::GMResult> + Unpin;
    
    fn executing_future(&mut self) -> Option<&mut Self::GMFuture>;

    fn clear_execution(&mut self, result: Self::GMResult);

    fn start_execution(&mut self, spec: CursorSpecification, client: Client);
}

trait GetMoreProviderResult {
    fn get_more_result(&self) -> Result<GetMoreResult>;
}

struct OwnedGetMore {
    result: Result<GetMoreResult>,
    session: ClientSession,
}

impl GetMoreProviderResult for OwnedGetMore {
    fn get_more_result(&self) -> Result<GetMoreResult> {
        self.result.clone()
    }
}

enum OwnedGetMoreProvider {
    Executing(BoxFuture<'static, OwnedGetMore>),
    Idle(ClientSession)
}

impl GetMoreProvider for OwnedGetMoreProvider {
    type GMResult = OwnedGetMore;
    type GMFuture = BoxFuture<'static, OwnedGetMore>;
    fn executing_future(&mut self) -> Option<&mut Self::GMFuture> {
        match self {
            Self::Executing(ref mut future) => Some(future),
            Self::Idle(_) => None
        }
    }

    fn clear_execution(&mut self, result: Self::GMResult) {
        *self = Self::Idle(result.session)
    }

    fn start_execution(&mut self, spec: CursorSpecification, client: Client) {
        take_mut::take(self, |self_| {
            if let Self::Idle(mut session) = self_ {
                let future = Box::pin(async move {
                    let get_more = GetMore::new(spec);
                    let get_more_result = client.execute_operation_with_session(get_more, &mut session).await;
                    OwnedGetMore {
                        result: get_more_result,
                        session
                    }
                });
                Self::Executing(future)
            } else {
                self_
            }
        })
    }
}

impl Stream for Cursor {
    type Item = Result<Document>;
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let tmp = &mut *self.as_mut();
        let mut session_cursor_handle = &mut tmp.session_cursor.with_session(&mut tmp.session);
        Pin::new(&mut session_cursor_handle).poll_next(cx)
    }
}
