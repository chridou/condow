use std::{
    io, iter,
    task::{Context, Poll},
};

use bytes::Bytes;
use futures::{
    channel::mpsc as futures_mpsc,
    future,
    stream::{self, BoxStream},
    Stream, StreamExt, TryStreamExt,
};
use pin_project_lite::pin_project;
use tokio::sync::mpsc as tokio_mpsc;

use crate::{
    errors::CondowError, machinery::download::PartsBytesStream, streams::BytesHint,
    streams::OrderedChunkStream,
};

/// Item of a [BytesStream]
pub type BytesStreamItem = Result<Bytes, CondowError>;

pin_project! {
    /// A stream of [Bytes] (chunks) where there can be an error for each chunk of bytes
    ///
    /// This stream is NOT fused.
    pub struct BytesStream {
        #[pin]
        source: SourceFlavour,
        bytes_hint: BytesHint,
    }
}

impl BytesStream {
    pub fn new<St>(stream: St, bytes_hint: BytesHint) -> Self
    where
        St: Stream<Item = BytesStreamItem> + Send + 'static,
    {
        Self {
            source: SourceFlavour::DynStream {
                stream: stream.boxed(),
            },
            bytes_hint,
        }
    }

    pub fn new_futures_receiver(
        receiver: futures_mpsc::UnboundedReceiver<BytesStreamItem>,
        bytes_hint: BytesHint,
    ) -> Self {
        Self {
            source: SourceFlavour::FuturesChannel { receiver },
            bytes_hint,
        }
    }
    pub fn new_tokio_receiver(
        receiver: tokio_mpsc::UnboundedReceiver<BytesStreamItem>,
        bytes_hint: BytesHint,
    ) -> Self {
        Self {
            source: SourceFlavour::TokioChannel { receiver },
            bytes_hint,
        }
    }

    pub fn new_chunk_stream(stream: OrderedChunkStream) -> Self {
        let bytes_hint = stream.bytes_hint();
        Self {
            source: SourceFlavour::ChunksOrdered { stream },
            bytes_hint,
        }
    }

    pub fn empty() -> Self {
        Self {
            source: SourceFlavour::Empty,
            bytes_hint: BytesHint::new_exact(0),
        }
    }

    pub fn once(item: BytesStreamItem) -> Self {
        match item {
            Ok(bytes) => {
                let bytes_hint = BytesHint::new_exact(bytes.len() as u64);
                Self::new(stream::iter(iter::once(Ok(bytes))), bytes_hint)
            }
            Err(err) => Self::new(stream::iter(iter::once(Err(err))), BytesHint::new_exact(0)),
        }
    }

    pub fn once_ok(bytes: Bytes) -> Self {
        Self::once(Ok(bytes))
    }

    pub fn into_io_stream(self) -> impl Stream<Item = Result<Bytes, io::Error>> {
        self.map_err(From::from)
    }

    pub(crate) fn new_parts_bytes_stream(stream: PartsBytesStream) -> Self {
        let bytes_hint = stream.bytes_hint();
        Self {
            source: SourceFlavour::PartsBytesStream { stream },
            bytes_hint,
        }
    }

    /// Writes all bytes left on the stream into the provided buffer
    ///
    /// Fails if the buffer is too small or there was an error on the stream.
    pub async fn write_buffer(mut self, buffer: &mut [u8]) -> Result<usize, CondowError> {
        if (buffer.len() as u64) < self.bytes_hint.lower_bound() {
            return Err(CondowError::new_other(format!(
                "buffer to small ({}). at least {} bytes required",
                buffer.len(),
                self.bytes_hint.lower_bound()
            )));
        }

        let mut offset = 0;
        while let Some(next) = self.next().await {
            let bytes = next?;

            let end_excl = offset + bytes.len();
            if end_excl > buffer.len() {
                return Err(CondowError::new_other(format!(
                    "write attempt beyond buffer end (buffer len = {}). \
                        attempted to write at index {}",
                    buffer.len(),
                    end_excl
                )));
            }

            buffer[offset..end_excl].copy_from_slice(&bytes[..]);

            offset = end_excl;
        }

        Ok(offset)
    }

    /// Creates a `Vec<u8>` filled with the bytes from the stream.
    ///
    /// Fails if the stream was already iterated.
    ///
    /// Since the parts and therefore the chunks are not ordered we can
    /// not know, whether we can fill the `Vec` in a contiguous way.
    /// Creates a `Vec<u8>` filled with the rest of the bytes from the stream.
    ///
    /// Fails if there is an error on the stream
    pub async fn into_vec(mut self) -> Result<Vec<u8>, CondowError> {
        if let Some(total_bytes) = self.bytes_hint.exact() {
            if total_bytes > usize::MAX as u64 {
                return Err(CondowError::new_other(
                    "usize overflow while casting from u64",
                ));
            }

            let mut buffer = vec![0; total_bytes as usize];
            let _ = self.write_buffer(buffer.as_mut()).await?;
            Ok(buffer)
        } else {
            let mut buffer = Vec::with_capacity(self.bytes_hint.lower_bound() as usize);

            while let Some(next) = self.next().await {
                let bytes = next?;

                buffer.extend(bytes);
            }

            Ok(buffer)
        }
    }
    /// Counts the number of bytes downloaded
    ///
    /// Provided mainly for testing.
    pub async fn count_bytes(self) -> Result<u64, CondowError> {
        self.try_fold(0u64, |acc, chunk| future::ok(acc + chunk.len() as u64))
            .await
    }

    pub fn bytes_hint(&self) -> BytesHint {
        self.bytes_hint
    }
}

impl Stream for BytesStream {
    type Item = BytesStreamItem;
    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        match this.source.as_mut().poll_next(cx) {
            Poll::Ready(Some(next)) => match next {
                Ok(bytes) => {
                    this.bytes_hint.reduce_by(bytes.len() as u64);
                    Poll::Ready(Some(Ok(bytes)))
                }
                Err(err) => {
                    *this.bytes_hint = BytesHint::new_exact(0);
                    Poll::Ready(Some(Err(err)))
                }
            },
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

pin_project! {
    #[project = SourceFlavourProj]
    enum SourceFlavour {
        DynStream{#[pin] stream: BoxStream<'static, BytesStreamItem>},
        PartsBytesStream{#[pin] stream: PartsBytesStream },
        ChunksOrdered{#[pin] stream: OrderedChunkStream},
        TokioChannel{#[pin] receiver: tokio_mpsc::UnboundedReceiver<BytesStreamItem>},
        FuturesChannel{#[pin] receiver: futures_mpsc::UnboundedReceiver<BytesStreamItem>},
        Empty,
    }
}

impl Stream for SourceFlavour {
    type Item = BytesStreamItem;

    #[inline]
    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        match this {
            SourceFlavourProj::DynStream { mut stream } => stream.as_mut().poll_next(cx),
            SourceFlavourProj::PartsBytesStream { stream } => match stream.poll_next(cx) {
                Poll::Ready(Some(res)) => Poll::Ready(Some(res)),
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            },
            SourceFlavourProj::ChunksOrdered { stream } => match stream.poll_next(cx) {
                Poll::Ready(Some(res)) => Poll::Ready(Some(res.map(|chunk| chunk.bytes))),
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            },
            SourceFlavourProj::TokioChannel { mut receiver } => receiver.poll_recv(cx),
            SourceFlavourProj::FuturesChannel { receiver } => receiver.poll_next(cx),
            SourceFlavourProj::Empty => Poll::Ready(None),
        }
    }
}
