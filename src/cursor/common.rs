use crate::{error::Result, results::GetMoreResult, Client, Namespace, options::StreamAddress};
use bson::Document;
use futures::{Future, Stream};
use std::{
    collections::VecDeque,
    pin::Pin,
    task::{Context, Poll}, time::Duration,
};

/// An internal cursor that can be used in a variety of contexts depending on its `GetMoreProvider`.
pub(super) struct GenericCursor<T: GetMoreProvider> {
    provider: T,
    client: Client,
    info: CursorInformation,
    buffer: VecDeque<Document>,
    exhausted: bool,
}

#[allow(dead_code)]
impl<T: GetMoreProvider> GenericCursor<T> {
    pub(super) fn new(client: Client, spec: CursorSpecification, get_more_provider: T) -> Self {
        let exhausted = spec.id() == 0;
        Self {
            exhausted,
            client,
            provider: get_more_provider,
            buffer: spec.initial_buffer,
            info: spec.info
        }
    }

    pub(super) fn take_buffer(&mut self) -> VecDeque<Document> {
        std::mem::take(&mut self.buffer)
    }

    pub(super) fn is_exhausted(&self) -> bool {
        self.exhausted
    }
}

impl<T: GetMoreProvider> Stream for GenericCursor<T> {
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
                    let info = self.info.clone();
                    let client = self.client.clone();
                    self.provider.start_execution(info, client);
                }
                None => return Poll::Ready(None),
            }
        }
    }
}

/// A trait implemented by objects that can provide batches of documents to a cursor via the getMore command.
pub(super) trait GetMoreProvider: Unpin {
    /// The result type that the future running the getMore evaluates to.
    type GetMoreResult: GetMoreProviderResult;

    /// The type of future created by this provider when running a getMore.
    type GetMoreFuture: Future<Output = Self::GetMoreResult> + Unpin;

    /// Get the future being evaluated, if there is one.
    fn executing_future(&mut self) -> Option<&mut Self::GetMoreFuture>;

    /// Clear out any state remaining from previous getMore executions.
    fn clear_execution(&mut self, result: Self::GetMoreResult);

    /// Start executing a new getMore if one isn't already in flight.
    fn start_execution(&mut self, spec: CursorInformation, client: Client);
}

/// Trait describing results returned from a `GetMoreProvider`.
pub(super) trait GetMoreProviderResult {
    /// Get the result from the getMore command itself.
    fn get_more_result(&self) -> Result<GetMoreResult>;
}


#[derive(Debug, Clone)]
pub(crate) struct CursorSpecification {
    pub(crate) info: CursorInformation,
    pub(crate) initial_buffer: VecDeque<Document>,
}

impl CursorSpecification {
    pub(crate) fn new(
        ns: Namespace,
        address: StreamAddress,
        id: i64,
        batch_size: impl Into<Option<u32>>,
        max_time: impl Into<Option<Duration>>,
        initial_buffer: VecDeque<Document>
    ) -> Self {
        Self {
            info: CursorInformation { ns, id, address, batch_size: batch_size.into(), max_time: max_time.into() },
            initial_buffer
        }
    }

    pub(crate) fn namespace(&self) -> &Namespace {
        &self.info.ns
    }
    
    pub(crate) fn id(&self) -> i64 {
        self.info.id
    }

    #[cfg(test)]
    pub(crate) fn address(&self) -> &StreamAddress {
        &self.info.address
    }

    #[cfg(test)]
    pub(crate) fn batch_size(&self) -> Option<u32> {
        self.info.batch_size
    }

    #[cfg(test)]
    pub(crate) fn max_time(&self) -> Option<Duration> {
        self.info.max_time
    }

    #[cfg(test)]
    pub(crate) fn initial_buffer(&self) -> &VecDeque<Document> {
        &self.info.initial_buffer
    }
}

/// Static information about a cursor.
#[derive(Clone, Debug)]
pub(crate) struct CursorInformation {
    pub(crate) ns: Namespace,
    pub(crate) address: StreamAddress,
    pub(crate) id: i64,
    pub(crate) batch_size: Option<u32>,
    pub(crate) max_time: Option<Duration>,
}
