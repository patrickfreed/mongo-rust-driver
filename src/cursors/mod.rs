mod common;

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use bson::Document;
use futures::{future::BoxFuture, Stream};

use crate::{
    client::ClientSession,
    cursor::CursorSpecification,
    error::Result,
    operation::GetMore,
    results::GetMoreResult,
    Client,
};
use common::{GenericCursor, GetMoreProvider, GetMoreProviderResult};

pub struct Cursor {
    wrapped_cursor: GenericCursor<ImplicitSessionGetMoreProvider>,
}

impl Cursor {
    #[allow(dead_code)]
    pub(crate) fn new(client: Client, spec: CursorSpecification, session: ClientSession) -> Self {
        Self {
            wrapped_cursor: GenericCursor::new(
                client,
                spec,
                ImplicitSessionGetMoreProvider::new(session),
            ),
        }
    }
}

impl Stream for Cursor {
    type Item = Result<Document>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.wrapped_cursor).poll_next(cx)
    }
}

struct ExecutionResult {
    get_more_result: Result<GetMoreResult>,
    session: ClientSession,
}

impl GetMoreProviderResult for ExecutionResult {
    fn get_more_result(&self) -> Result<GetMoreResult> {
        self.get_more_result.clone()
    }
}

enum ImplicitSessionGetMoreProvider {
    Executing(BoxFuture<'static, ExecutionResult>),
    Idle(ClientSession),
}

impl ImplicitSessionGetMoreProvider {
    fn new(session: ClientSession) -> Self {
        Self::Idle(session)
    }
}

impl GetMoreProvider for ImplicitSessionGetMoreProvider {
    type GetMoreResult = ExecutionResult;
    type GetMoreFuture = BoxFuture<'static, ExecutionResult>;
    fn executing_future(&mut self) -> Option<&mut Self::GetMoreFuture> {
        match self {
            Self::Executing(ref mut future) => Some(future),
            Self::Idle(_) => None,
        }
    }

    fn clear_execution(&mut self, result: Self::GetMoreResult) {
        *self = Self::Idle(result.session)
    }

    fn start_execution(&mut self, spec: CursorSpecification, client: Client) {
        take_mut::take(self, |self_| match self_ {
            Self::Idle(mut session) => {
                let future = Box::pin(async move {
                    let get_more = GetMore::new(spec);
                    let get_more_result = client
                        .execute_operation_with_session(get_more, &mut session)
                        .await;
                    ExecutionResult {
                        get_more_result,
                        session,
                    }
                });
                Self::Executing(future)
            }
            Self::Executing(_) => self_,
        })
    }
}
