use std::task::{Context, Poll};

use bytes::Bytes;
use futures::{channel::mpsc, ready, Stream, StreamExt};
use pin_project_lite::pin_project;

use crate::errors::CondowError;

use super::BytesHint;

/// The type of the elements returned by a [ChunkStream]
pub type ChunkStreamItem = Result<Chunk, CondowError>;

/// A chunk belonging to a downloaded part
///
/// All chunks of a part will have the correct order
/// for a part with the same `part_index` but the chunks
/// of different parts can be intermingled due
/// to the nature of a concurrent download.
#[derive(Debug, Clone)]
pub struct Chunk {
    /// Index of the part this chunk belongs to
    pub part_index: usize,
    /// Index of the chunk within the part
    pub chunk_index: usize,
    /// Offset of the chunk within the BLOB
    pub blob_offset: usize,
    /// Offset of the chunk within the downloaded range
    pub range_offset: usize,
    /// The bytes
    pub bytes: Bytes,
    /// Bytes left in following chunks. If 0 this is the last chunk of the part.
    pub bytes_left: usize,
}

impl Chunk {
    /// Returns `true` if this is the last chunk of the part
    ///
    /// Same as `len()==0`
    pub fn is_last(&self) -> bool {
        self.bytes_left == 0
    }

    /// Returns the number of bytes in this chunk
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Returns `true` if there are no bytes in this chunk.
    ///
    /// This should not happen since we would not expect
    /// "no bytes" being sent over the network.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pin_project! {
    /// A stream of [Chunk]s received from the network
    pub struct ChunkStream {
        bytes_hint: BytesHint,
        #[pin]
        receiver: mpsc::UnboundedReceiver<ChunkStreamItem>,
        is_closed: bool,
        is_fresh: bool,
    }
}

impl ChunkStream {
    pub fn new(bytes_hint: BytesHint) -> (Self, mpsc::UnboundedSender<ChunkStreamItem>) {
        let (tx, receiver) = mpsc::unbounded();

        let me = Self {
            bytes_hint,
            receiver,
            is_closed: false,
            is_fresh: true,
        };

        (me, tx)
    }

    /// Returns true if no more items can be pulled from this stream.
    ///
    /// Also `true` if an error occured
    pub fn empty() -> Self {
        let (mut me, _) = Self::new(BytesHint(0, Some(0)));
        me.is_closed = true;
        me.receiver.close();
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
            return Err(CondowError::Other("stream already iterated".to_string()));
        }

        if buffer.len() < self.bytes_hint.lower_bound() {
            return Err(CondowError::Other(format!(
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

            let end_excl = range_offset + bytes.len();
            if end_excl > buffer.len() {
                return Err(CondowError::Other(format!(
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
    pub async fn into_vec(self) -> Result<Vec<u8>, CondowError> {
        if let Some(total_bytes) = self.bytes_hint.exact() {
            let mut buffer = vec![0; total_bytes];
            let _ = self.write_buffer(buffer.as_mut()).await?;
            Ok(buffer)
        } else {
            stream_into_vec_with_unknown_size(self).await
        }
    }
}

async fn stream_into_vec_with_unknown_size(
    mut stream: ChunkStream,
) -> Result<Vec<u8>, CondowError> {
    if !stream.is_fresh {
        return Err(CondowError::Other("stream already iterated".to_string()));
    }

    let mut buffer = Vec::with_capacity(stream.bytes_hint.lower_bound());

    while let Some(next) = stream.next().await {
        let Chunk {
            range_offset,
            bytes,
            ..
        } = match next {
            Err(err) => return Err(err),
            Ok(next) => next,
        };

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
        let receiver = this.receiver.as_mut();

        let next = ready!(mpsc::UnboundedReceiver::poll_next(receiver, cx));
        match next {
            Some(Ok(chunk_item)) => {
                this.bytes_hint.reduce_by(chunk_item.len());
                Poll::Ready(Some(Ok(chunk_item)))
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

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use crate::{
        streams::{BytesHint, Chunk, ChunkStream},
        test_utils::create_chunk_stream,
    };

    #[tokio::test]
    async fn check() {
        for n_parts in 1..20 {
            for n_chunks in 1..20 {
                let (stream, expected) = create_chunk_stream(n_parts, n_chunks, true, Some(10));
                check_stream(stream, &expected).await
            }
        }
    }

    async fn check_stream(mut result_stream: ChunkStream, data: &[u8]) {
        let mut bytes_left = data.len();
        let mut first_blob_offset = 0;
        let mut got_first = false;

        assert_eq!(result_stream.bytes_hint(), BytesHint::new_exact(bytes_left));
        while let Some(next) = result_stream.next().await {
            let Chunk {
                range_offset,
                blob_offset,
                bytes,
                ..
            } = match next {
                Err(err) => panic!("{}", err),
                Ok(next) => next,
            };

            if !got_first {
                first_blob_offset = blob_offset - range_offset;
                got_first = true;
            }

            bytes_left -= bytes.len();
            assert_eq!(result_stream.bytes_hint(), BytesHint::new_exact(bytes_left));

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
    }
}
