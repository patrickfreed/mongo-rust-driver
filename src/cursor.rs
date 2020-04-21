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
    error::Result,
    operation::GetMore,
    options::StreamAddress,
    results::GetMoreResult,
    Client,
    Namespace,
    RUNTIME, client::ClientSession,
};

#[derive(Debug)]
pub(crate) struct CursorSpecification {
    pub(crate) ns: Namespace,
    pub(crate) address: StreamAddress,
    pub(crate) id: i64,
    pub(crate) batch_size: Option<u32>,
    pub(crate) max_time: Option<Duration>,
    pub(crate) buffer: VecDeque<Document>,
}

#[derive(Debug)]
pub struct Cursor {
    client: Client,
    spec: CursorSpecification,
    state: ExecutionState,
    status: CursorStatus,
}

type GetMoreFuture = BoxFuture<'static, Result<GetMoreResult>>;

/// Describes the current state of the Cursor. If the state is Executing, then a getMore operation
/// is in progress. If the state is Buffer, then there are documents available from the current
/// batch.
#[derive(Derivative)]
#[derivative(Debug)]
enum ExecutionState {
    Executing(#[derivative(Debug = "ignore")] GetMoreFuture),
    Buffer(VecDeque<Document>),
    Done,
}

#[derive(Debug)]
enum CursorStatus {
    Alive { session: ClientSession },
    Exhausted,
}

pub(crate) struct SessionCursor {}

impl SessionCursor {
    fn new(client: Client, spec: CursorSpecification) -> Self {
        todo!()
    }
}

impl Cursor {
    pub(crate) fn new(client: Client, spec: CursorSpecification, session: ClientSession) -> Self {
        Self {
            client,
            spec,
            status: if spec.id == 0 { CursorStatus::Exhausted } else { CursorStatus::Alive { session } },
            state: ExecutionState::Buffer(spec.buffer),
        }
    }

    fn get_more(&self) -> GetMore<'static> {
        match self.status {
            CursorStatus::Alive { session } => {
                GetMore::new(
                    self.spec.ns,
                    self.spec.id,
                    self.spec.address,
                    self.spec.batch_size,
                    self.spec.max_time,
                    Some(&mut session),
                );
            },
            _ => panic!("wooo")
        }
        
    }

    fn is_exhausted(&self) -> bool {
        matches!(self.status, CursorStatus::Exhausted)
    }
}

impl Drop for Cursor {
    fn drop(&mut self) {
        if self.is_exhausted() {
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

impl Stream for Cursor {
    type Item = Result<Document>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        loop {
            match self.state {
                // If the current state is Executing, then we check the progress of the getMore
                // operation.
                ExecutionState::Executing(ref mut future) => {
                    match Pin::new(future).poll(cx) {
                        // If the getMore is finished and successful, then we pop off the first
                        // document from the batch, set the poll state to
                        // Buffer, record whether the cursor is exhausted,
                        // and return the popped document.
                        Poll::Ready(Ok(get_more_result)) => {
                            let mut buffer: VecDeque<_> =
                                get_more_result.batch.into_iter().collect();
                            let next_doc = buffer.pop_front();

                            self.state = ExecutionState::Buffer(buffer);
                            if get_more_result.exhausted {
                                self.status = CursorStatus::Exhausted;
                            }

                            match next_doc {
                                Some(doc) => return Poll::Ready(Some(Ok(doc))),
                                None => continue,
                            }
                        }

                        // If the getMore finished with an error, return that error.
                        Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e))),

                        // If the getMore has not finished, keep the state as Executing and return.
                        Poll::Pending => return Poll::Pending,
                    }
                }

                ExecutionState::Buffer(ref mut buffer) => {
                    // If there is a document ready, return it.
                    if let Some(doc) = buffer.pop_front() {
                        return Poll::Ready(Some(Ok(doc)));
                    }

                    // If no documents are left and the batch and the cursor is exhausted, set the
                    // state to None.
                    if self.is_exhausted() {
                        self.state = ExecutionState::Done;
                        return Poll::Ready(None);
                    // If the batch is empty and the cursor is not exhausted, start a new operation
                    // and set the state to Executing.
                    } else {
                        let client = self.client.clone();
                        let get_more = self.get_more();
                        let future = Box::pin(async move {
                            client.execute_operation(get_more).await
                        });

                        self.state = ExecutionState::Executing(future as GetMoreFuture);
                        continue;
                    }
                }

                // If the state is None, then the cursor has already exhausted all its results, so
                // do nothing.
                ExecutionState::Done => return Poll::Ready(None),
            }
        }
    }
}
