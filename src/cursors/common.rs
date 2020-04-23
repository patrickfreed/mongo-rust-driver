use crate::{cursor::CursorSpecification, error::Result, results::GetMoreResult, Client};
use bson::Document;
use futures::{Future, Stream};
use std::{
    collections::VecDeque,
    pin::Pin,
    task::{Context, Poll},
};

pub(super) struct GenericCursor<T: GetMoreProvider + Unpin> {
    provider: T,
    buffer: VecDeque<Document>,
    client: Client,
    spec: CursorSpecification,
    exhausted: bool,
}

#[allow(dead_code)]
impl<T: GetMoreProvider + Unpin> GenericCursor<T> {
    pub(super) fn new(client: Client, spec: CursorSpecification, get_more_provider: T) -> Self {
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
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
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
                            }
                            Err(e) => return Poll::Ready(Some(Err(e))),
                        }
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }

            match self.buffer.pop_front() {
                Some(doc) => return Poll::Ready(Some(Ok(doc))),
                None if !self.exhausted => {
                    let spec = self.spec.clone();
                    let client = self.client.clone();
                    self.provider.start_execution(spec, client);
                }
                None => return Poll::Ready(None),
            }
        }
    }
}

pub(super) trait GetMoreProvider {
    type GetMoreResult: GetMoreProviderResult;
    type GetMoreFuture: Future<Output = Self::GetMoreResult> + Unpin;

    fn executing_future(&mut self) -> Option<&mut Self::GetMoreFuture>;

    fn clear_execution(&mut self, result: Self::GetMoreResult);

    fn start_execution(&mut self, spec: CursorSpecification, client: Client);
}

pub(super) trait GetMoreProviderResult {
    fn get_more_result(&self) -> Result<GetMoreResult>;
}
