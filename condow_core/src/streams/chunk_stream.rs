use std::{
    convert::TryFrom,
    task::{Context, Poll},
};

use futures::{future, ready, stream::BoxStream, Stream, StreamExt, TryStreamExt};
use pin_project_lite::pin_project;
use tokio::sync::mpsc;

use crate::{errors::CondowError, streams::ChunkStreamItem};

use super::{BytesHint, Chunk, OrderedChunkStream};

pin_project! {
    /// A stream of [Chunk]s received from the network
    ///
    /// This stream is fused. If it ever yields `None` it will always
    /// yield `None` afterwards. The stream will only ever return
    /// one error which is always followed by a `None`.
    pub struct ChunkStream {
        bytes_hint: BytesHint,
        #[pin]
        source: SourceFlavour,
        is_closed: bool,
        is_fresh: bool,
    }
}

impl ChunkStream {
    pub fn new_channel_sink_pair(
        bytes_hint: BytesHint,
    ) -> (Self, mpsc::UnboundedSender<ChunkStreamItem>) {
        let (tx, receiver) = mpsc::unbounded_channel();

        let source = SourceFlavour::Channel { receiver };

        let me = Self {
            bytes_hint,
            source,
            is_closed: false,
            is_fresh: true,
        };

        (me, tx)
    }

    pub fn from_stream(stream: BoxStream<'static, ChunkStreamItem>, bytes_hint: BytesHint) -> Self {
        let source = SourceFlavour::Stream { stream };

        Self {
            bytes_hint,
            source,
            is_closed: false,
            is_fresh: true,
        }
    }

    /// Returns a [ChunkStream] which does not yield any items
    pub fn empty() -> Self {
        let (mut me, _) = Self::new_channel_sink_pair(BytesHint(0, Some(0)));
        me.is_closed = true;
        me.source.close();
        me
    }

    /// Hint on the remaining bytes on this stream.
    pub fn bytes_hint(&self) -> BytesHint {
        self.bytes_hint
    }

    /// Returns `true`, if this stream was not iterated before
    pub fn is_fresh(&self) -> bool {
        self.is_fresh
    }

    /// Writes all received bytes into the provided buffer
    ///
    /// Fails if the buffer is too small or if the stream was already iterated.
    ///
    /// Since the parts and therefore the chunks are not ordered we can
    /// not know, whether we can fill the buffer in a contiguous way.
    #[deprecated]
    pub async fn fill_buffer(self, buffer: &mut [u8]) -> Result<usize, CondowError> {
        self.write_buffer(buffer).await
    }

    /// Writes all received bytes into the provided buffer
    ///
    /// Fails if the buffer is too small or if the stream was already iterated.
    ///
    /// Since the parts and therefore the chunks are not ordered we can
    /// not know, whether we can fill the buffer in a contiguous way.
    pub async fn write_buffer(mut self, buffer: &mut [u8]) -> Result<usize, CondowError> {
        if !self.is_fresh {
            self.source.close();
            return Err(CondowError::new_other(
                "stream already iterated".to_string(),
            ));
        }

        if (buffer.len() as u64) < self.bytes_hint.lower_bound() {
            self.source.close();
            return Err(CondowError::new_other(format!(
                "buffer to small ({}). at least {} bytes required",
                buffer.len(),
                self.bytes_hint.lower_bound()
            )));
        }

        let mut bytes_written = 0;

        while let Some(next) = self.next().await {
            let Chunk {
                range_offset,
                bytes,
                ..
            } = match next {
                Err(err) => return Err(err),
                Ok(next) => next,
            };

            if range_offset > usize::MAX as u64 {
                self.source.close();
                return Err(CondowError::new_other(
                    "usize overflow while casting from u64",
                ));
            }

            let range_offset = range_offset as usize;

            let end_excl = range_offset + bytes.len();
            if end_excl > buffer.len() {
                self.source.close();
                return Err(CondowError::new_other(format!(
                    "write attempt beyond buffer end (buffer len = {}). \
                    attempted to write at index {}",
                    buffer.len(),
                    end_excl
                )));
            }

            buffer[range_offset..end_excl].copy_from_slice(&bytes[..]);

            bytes_written += bytes.len();
        }

        Ok(bytes_written)
    }

    /// Creates a `Vec<u8>` filled with the bytes from the stream.
    ///
    /// Fails if the stream was already iterated.
    ///
    /// Since the parts and therefore the chunks are not ordered we can
    /// not know, whether we can fill the `Vec` in a contiguous way.
    pub async fn into_vec(mut self) -> Result<Vec<u8>, CondowError> {
        if let Some(total_bytes) = self.bytes_hint.exact() {
            if total_bytes > usize::MAX as u64 {
                self.source.close();
                return Err(CondowError::new_other(
                    "usize overflow while casting from u64",
                ));
            }

            let mut buffer = vec![0; total_bytes as usize];
            let _ = self.write_buffer(buffer.as_mut()).await?;
            Ok(buffer)
        } else {
            stream_into_vec_with_unknown_size(self).await
        }
    }

    /// Turns this stream into a [PartStream]
    ///
    /// Fails if this [ChunkStream] was already iterated.
    pub fn try_into_part_stream(self) -> Result<OrderedChunkStream, CondowError> {
        OrderedChunkStream::try_from(self)
    }

    /// Counts the number of bytes downloaded
    ///
    /// Provided mainly for testing.
    pub async fn count_bytes(self) -> Result<u64, CondowError> {
        self.try_fold(0u64, |acc, chunk| future::ok(acc + chunk.len() as u64))
            .await
    }
}

pin_project! {
    #[project = SourceFlavourProj]
    enum SourceFlavour {
         Channel{#[pin] receiver: mpsc::UnboundedReceiver<ChunkStreamItem>},
         Stream{#[pin] stream: BoxStream<'static, ChunkStreamItem>},
    }
}

impl SourceFlavour {
    fn close(&mut self) {
        match self {
            SourceFlavour::Channel { receiver } => receiver.close(),
            SourceFlavour::Stream { .. } => {}
        }
    }
}

impl Stream for SourceFlavour {
    type Item = ChunkStreamItem;
    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        match this {
            SourceFlavourProj::Stream { stream } => stream.poll_next(cx),
            SourceFlavourProj::Channel { mut receiver } => receiver.poll_recv(cx),
        }
    }
}

async fn stream_into_vec_with_unknown_size(
    mut stream: ChunkStream,
) -> Result<Vec<u8>, CondowError> {
    if !stream.is_fresh {
        stream.source.close();
        return Err(CondowError::new_other(
            "stream already iterated".to_string(),
        ));
    }

    let lower_bound = stream.bytes_hint.lower_bound();
    if lower_bound > usize::MAX as u64 {
        stream.source.close();
        return Err(CondowError::new_other(
            "usize overflow while casting from u64",
        ));
    }

    let mut buffer = Vec::with_capacity(lower_bound as usize);

    while let Some(next) = stream.next().await {
        let Chunk {
            range_offset,
            bytes,
            ..
        } = match next {
            Err(err) => return Err(err),
            Ok(next) => next,
        };

        if range_offset > usize::MAX as u64 {
            stream.source.close();
            return Err(CondowError::new_other(
                "usize overflow while casting from u64",
            ));
        }

        let range_offset = range_offset as usize;

        let end_excl = range_offset + bytes.len();
        if end_excl >= buffer.len() {
            let missing = end_excl - buffer.len();
            buffer.extend((0..missing).map(|_| 0));
        }

        buffer[range_offset..end_excl].copy_from_slice(&bytes[..]);
    }

    Ok(buffer)
}

impl Stream for ChunkStream {
    type Item = ChunkStreamItem;

    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.is_closed {
            return Poll::Ready(None);
        }

        let mut this = self.project();
        *this.is_fresh = false;
        let source = this.source.as_mut();

        let next = ready!(source.poll_next(cx));
        match next {
            Some(Ok(chunk_item)) => {
                this.bytes_hint.reduce_by(chunk_item.len() as u64);
                Poll::Ready(Some(Ok(chunk_item)))
            }
            Some(Err(err)) => {
                *this.is_closed = true;
                this.source.close();
                *this.bytes_hint = BytesHint::new_exact(0);
                Poll::Ready(Some(Err(err)))
            }
            None => {
                *this.is_closed = true;
                Poll::Ready(None)
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use crate::{
        errors::CondowError,
        streams::{BytesHint, Chunk, ChunkStream},
        test_utils::{create_chunk_stream, create_chunk_stream_with_err},
    };

    #[tokio::test]
    async fn check_ok() {
        for n_parts in 1..20 {
            for n_chunks in 1..20 {
                let (stream, expected) = create_chunk_stream(n_parts, n_chunks, true, Some(10));
                check_stream(stream, &expected).await.unwrap()
            }
        }
    }

    #[tokio::test]
    async fn check_err_begin() {
        for n_parts in 1..20 {
            for n_chunks in 1..20 {
                let (stream, expected) =
                    create_chunk_stream_with_err(n_parts, n_chunks, true, Some(10), 0);
                assert!(check_stream(stream, &expected).await.is_err())
            }
        }
    }

    #[tokio::test]
    async fn check_err_end() {
        for n_parts in 1..20u64 {
            for n_chunks in 1..20usize {
                let err_at_chunk = n_parts as usize * n_chunks - 1;
                let (stream, expected) =
                    create_chunk_stream_with_err(n_parts, n_chunks, true, Some(10), err_at_chunk);
                assert!(check_stream(stream, &expected).await.is_err())
            }
        }
    }

    #[tokio::test]
    async fn check_err_after_end() {
        for n_parts in 1..20u64 {
            for n_chunks in 1..20usize {
                let err_at_chunk = n_parts as usize * n_chunks;
                let (stream, expected) =
                    create_chunk_stream_with_err(n_parts, n_chunks, true, Some(10), err_at_chunk);
                assert!(check_stream(stream, &expected).await.is_err())
            }
        }
    }

    async fn check_stream(mut result_stream: ChunkStream, data: &[u8]) -> Result<(), CondowError> {
        let mut bytes_left = data.len();
        let mut first_blob_offset = 0;
        let mut got_first = false;

        assert_eq!(
            result_stream.bytes_hint(),
            BytesHint::new_exact(bytes_left as u64)
        );
        while let Some(next) = result_stream.next().await {
            let Chunk {
                range_offset,
                blob_offset,
                bytes,
                ..
            } = match next {
                Err(err) => return Err(err),
                Ok(next) => next,
            };

            let range_offset = range_offset as usize;
            let blob_offset = blob_offset as usize;

            if !got_first {
                first_blob_offset = blob_offset - range_offset;
                got_first = true;
            }

            bytes_left -= bytes.len();
            assert_eq!(
                result_stream.bytes_hint(),
                BytesHint::new_exact(bytes_left as u64)
            );

            assert_eq!(
                bytes[..],
                data[range_offset..range_offset + bytes.len()],
                "range_offset"
            );
            let adjusted_blob_offset = blob_offset - first_blob_offset;
            assert_eq!(adjusted_blob_offset, range_offset, "blob vs range");
            assert_eq!(
                bytes[..],
                data[adjusted_blob_offset..adjusted_blob_offset + bytes.len()],
                "blob_offset"
            );
        }

        Ok(())
    }
}
