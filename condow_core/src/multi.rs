use std::{
    sync::Arc,
    task::{Context, Poll},
};

use futures::{channel::mpsc, ready, Stream, StreamExt};
use pin_project_lite::pin_project;

use crate::{
    condow_client::CondowClient,
    errors::CondowError,
    machinery,
    reporter::{NoReporting, ReporterFactory},
    streams::{BytesHint, Chunk, ChunkStream},
    Condow, DownloadRange, GetSizeMode,
};

/// A downloading API for downloading multiple ranges of a single BLOB.
///
/// The [MultiRangeDownloader] can have reporting enabled.
/// In this case it behaves like a [DownloadSession](crate::DownloadSession)
/// regarding reporting.
pub struct MultiRangeDownloader<C: CondowClient, RF: ReporterFactory = NoReporting> {
    /// Mode for handling upper bounds of a range and open ranges
    ///
    /// Default: As configured with [Condow] itself
    /// or the struct this was cloned from
    get_size_mode: GetSizeMode,
    condow: Condow<C>,
    reporter_factory: Arc<RF>,
}

impl<C: CondowClient, RF: ReporterFactory> MultiRangeDownloader<C, RF> {
    pub(crate) fn new_with_reporting_arc(condow: Condow<C>, rep_fac: Arc<RF>) -> Self {
        Self {
            condow,
            get_size_mode: GetSizeMode::default(),
            reporter_factory: rep_fac,
        }
    }

    /// Get the size of a file at the BLOB at location
    pub async fn get_size(&self, location: C::Location) -> Result<u64, CondowError> {
        self.condow.get_size(location).await
    }

    /// Download multiple ranges of the BLOB at `location`.
    ///
    /// The streams in the result are symmetrical to the ranges
    /// provided.
    pub async fn download_chunk_streams<R: Into<DownloadRange> + Copy>(
        &self,
        location: C::Location,
        ranges: &[R],
    ) -> Result<Vec<ChunkStream>, CondowError> {
        let mut chunk_streams = Vec::with_capacity(ranges.len());

        for range in ranges {
            let range: DownloadRange = (*range).into();
            let chunk_stream = machinery::download(
                &self.condow,
                location.clone(),
                range,
                self.get_size_mode,
                self.reporter_factory.make(),
            )
            .await
            .map(|o| o.stream)?;
            chunk_streams.push(chunk_stream);
        }

        Ok(chunk_streams)
    }

    pub async fn download_chunks<R: Into<DownloadRange> + Clone>(
        &self,
        location: C::Location,
        ranges: &[R],
    ) -> Result<RangesChunkStream, CondowError> {
        let (tx, receiver) = mpsc::unbounded::<RangeChunkStreamItem>();

        let mut bytes_hint = BytesHint::new_exact(0);

        for (range_idx, range) in ranges.iter().enumerate() {
            let range: DownloadRange = range.clone().into();
            let chunk_stream = machinery::download(
                &self.condow,
                location.clone(),
                range,
                self.get_size_mode,
                self.reporter_factory.make(),
            )
            .await
            .map(|o| o.stream)?;

            bytes_hint = bytes_hint.combine(chunk_stream.bytes_hint());

            let tx = tx.clone();
            let task = async move {
                chunk_stream
                    .for_each(move |chunk_res| {
                        let msg = chunk_res.map(|chunk| RangeChunk { range_idx, chunk });

                        let _ = tx.unbounded_send(msg);

                        futures::future::ready(())
                    })
                    .await
            };

            tokio::spawn(task);
        }

        Ok(RangesChunkStream {
            bytes_hint,
            receiver,
            is_closed: false,
            is_fresh: true,
        })
    }
}

impl<C: CondowClient, RF: ReporterFactory> Clone for MultiRangeDownloader<C, RF> {
    fn clone(&self) -> Self {
        Self {
            condow: self.condow.clone(),
            reporter_factory: Arc::clone(&self.reporter_factory),
            get_size_mode: self.get_size_mode,
        }
    }
}

/// The type of the elements returned by a [RangesChunkStream]
pub type RangeChunkStreamItem = Result<RangeChunk, CondowError>;

#[derive(Debug, Clone)]
pub struct RangeChunk {
    pub range_idx: usize,
    pub chunk: Chunk,
}

pin_project! {
    /// A stream of [RangeChunk]s received from the network for multiple ranges
    pub struct RangesChunkStream {
        bytes_hint: BytesHint,
        #[pin]
        receiver: mpsc::UnboundedReceiver<RangeChunkStreamItem>,
        is_closed: bool,
        is_fresh: bool,
    }
}

impl RangesChunkStream {
    /// Hint on the remaining bytes on this stream.
    pub fn bytes_hint(&self) -> BytesHint {
        self.bytes_hint
    }

    /// Returns `true`, if this stream was not iterated before
    pub fn is_fresh(&self) -> bool {
        self.is_fresh
    }
}

impl Stream for RangesChunkStream {
    type Item = RangeChunkStreamItem;

    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.is_closed {
            return Poll::Ready(None);
        }

        let mut this = self.project();
        *this.is_fresh = false;
        let receiver = this.receiver.as_mut();

        let next = ready!(mpsc::UnboundedReceiver::poll_next(receiver, cx));
        match next {
            Some(Ok(range_chunk_item)) => {
                this.bytes_hint.reduce_by(range_chunk_item.chunk.len());
                Poll::Ready(Some(Ok(range_chunk_item)))
            }
            Some(Err(err)) => {
                *this.is_closed = true;
                this.receiver.close();
                *this.bytes_hint = BytesHint::new_exact(0);
                Poll::Ready(Some(Err(err)))
            }
            None => {
                *this.is_closed = true;
                this.receiver.close();
                Poll::Ready(None)
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}
