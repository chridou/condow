use std::{
    collections::HashMap,
    convert::TryFrom,
    task::{Context, Poll},
};

use crate::reader::BytesAsyncReader;
use bytes::Bytes;
use futures::{ready, stream, Stream, StreamExt, TryStreamExt};
use pin_project_lite::pin_project;

use crate::errors::CondowError;

use super::{BytesHint, ChunkStream, ChunkStreamItem};

/// The type of the elements returned by a [PartStream]
pub type PartStreamItem = Result<Part, CondowError>;

/// A downloaded part consisting of 1 or more chunks
#[derive(Debug, Clone)]
pub struct Part {
    /// Index of the part this chunk belongs to
    pub part_index: usize,
    /// Offset of the first chunk within the BLOB
    pub blob_offset: usize,
    /// Offset of the first chunk within the downloaded range
    pub range_offset: usize,
    /// The chunks of bytes in the order received for this part
    pub chunks: Vec<Bytes>,
}

impl Part {
    /// Length of this [Part] in bytes
    pub fn len(&self) -> usize {
        self.chunks.iter().map(|b| b.len()).sum()
    }

    /// Returns `true` if there are no bytes in this [Part]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A struct to collect and aggregate received chunks for a part
struct PartEntry {
    part_index: usize,
    blob_offset: usize,
    range_offset: usize,
    chunks: Vec<Bytes>,
    is_complete: bool,
}

pin_project! {
    /// A stream of downloaded parts
    ///
    /// All parts and their chunks are ordered as they would
    /// have appeared in a sequential download of a range/BLOB
    pub struct PartStream<St> {
        bytes_hint: BytesHint,
        #[pin]
        stream: St,
        is_closed: bool,
        next_part_idx: usize,
        collected_parts: HashMap<usize, PartEntry>
    }
}

impl<St> PartStream<St>
where
    St: Stream<Item = ChunkStreamItem> + Send + Sync + 'static + Unpin,
{
    /// Create a new [PartStream].
    ///
    /// **Call with care.** This function will only work
    /// if the input stream was not iterated before
    /// since all chunks are needed. The stream might live lock
    /// if the input was already iterated.
    pub fn new(stream: St, bytes_hint: BytesHint) -> Self {
        Self {
            bytes_hint,
            stream,
            is_closed: false,
            next_part_idx: 0,
            collected_parts: HashMap::default(),
        }
    }

    /// Hint on the remaining bytes on this stream.
    pub fn bytes_hint(&self) -> BytesHint {
        self.bytes_hint
    }

    /// Writes all bytes left on the stream into the provided buffer
    ///
    /// Fails if the buffer is too small or there was an error on the stream.
    pub async fn write_buffer(mut self, buffer: &mut [u8]) -> Result<usize, CondowError> {
        if buffer.len() < self.bytes_hint.lower_bound() {
            return Err(CondowError::new_other(format!(
                "buffer to small ({}). at least {} bytes required",
                buffer.len(),
                self.bytes_hint.lower_bound()
            )));
        }

        let mut offset = 0;
        while let Some(next) = self.next().await {
            let part = next?;

            for chunk in part.chunks {
                let end_excl = offset + chunk.len();
                if end_excl > buffer.len() {
                    return Err(CondowError::new_other(format!(
                        "write attempt beyond buffer end (buffer len = {}). \
                        attempted to write at index {}",
                        buffer.len(),
                        end_excl
                    )));
                }

                buffer[offset..end_excl].copy_from_slice(&chunk[..]);

                offset = end_excl;
            }
        }

        Ok(offset)
    }

    /// Creates a `Vec<u8>` filled with the rest of the bytes from the stream.
    ///
    /// Fails if there is an error on the stream
    pub async fn into_vec(mut self) -> Result<Vec<u8>, CondowError> {
        if let Some(total_bytes) = self.bytes_hint.exact() {
            let mut buffer = vec![0; total_bytes];
            let _ = self.write_buffer(buffer.as_mut()).await?;
            Ok(buffer)
        } else {
            let mut buffer = Vec::with_capacity(self.bytes_hint.lower_bound());

            while let Some(next) = self.next().await {
                let part = next?;

                for chunk in part.chunks {
                    buffer.extend(chunk);
                }
            }

            Ok(buffer)
        }
    }

    pub fn bytes_stream(
        self,
    ) -> impl Stream<Item = Result<Bytes, CondowError>> + Send + Sync + 'static {
        self.map_ok(|part| stream::iter(part.chunks.into_iter().map(Ok)))
            .try_flatten()
    }
}

impl PartStream<ChunkStream> {
    /// Create a new [PartStream] from the given [ChunkStream]
    ///
    /// Will fail if the [ChunkStream] was already iterated.
    pub fn from_chunk_stream(chunk_stream: ChunkStream) -> Result<Self, CondowError> {
        if !chunk_stream.is_fresh() {
            return Err(CondowError::new_other(
                "chunk stream already iterated".to_string(),
            ));
        }
        let bytes_hint = chunk_stream.bytes_hint();
        Ok(Self::new(chunk_stream, bytes_hint))
    }
}

impl<St: Stream<Item = ChunkStreamItem>> Stream for PartStream<St> {
    type Item = PartStreamItem;

    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.is_closed {
            return Poll::Ready(None);
        }

        let this = self.project();

        let next = ready!(this.stream.poll_next(cx));
        match next {
            Some(Ok(chunk)) => {
                if chunk.chunk_index == 0
                    && chunk.is_last()
                    && chunk.part_index == *this.next_part_idx
                {
                    this.bytes_hint.reduce_by(chunk.len());
                    *this.next_part_idx += 1;
                    Poll::Ready(Some(Ok(Part {
                        part_index: chunk.part_index,
                        blob_offset: chunk.blob_offset,
                        range_offset: chunk.range_offset,
                        chunks: vec![chunk.bytes],
                    })))
                } else {
                    let entry = this
                        .collected_parts
                        .entry(chunk.part_index)
                        .or_insert_with(|| PartEntry {
                            part_index: chunk.part_index,
                            blob_offset: chunk.blob_offset,
                            range_offset: chunk.range_offset,
                            chunks: vec![],
                            is_complete: false,
                        });
                    entry.is_complete = chunk.is_last();
                    entry.chunks.push(chunk.bytes);

                    if let Some(entry) = this.collected_parts.get(this.next_part_idx) {
                        if entry.is_complete {
                            let PartEntry {
                                part_index,
                                blob_offset: file_offset,
                                range_offset,
                                chunks,
                                ..
                            } = this.collected_parts.remove(this.next_part_idx).unwrap();
                            this.bytes_hint
                                .reduce_by(chunks.iter().map(|c| c.len()).sum());
                            *this.next_part_idx += 1;
                            Poll::Ready(Some(Ok(Part {
                                part_index,
                                blob_offset: file_offset,
                                range_offset,
                                chunks,
                            })))
                        } else {
                            cx.waker().clone().wake();
                            Poll::Pending
                        }
                    } else {
                        cx.waker().clone().wake();
                        Poll::Pending
                    }
                }
            }
            Some(Err(err)) => {
                *this.is_closed = true;
                *this.bytes_hint = BytesHint::new_exact(0);
                Poll::Ready(Some(Err(err)))
            }
            None => {
                if let Some(next) = this.collected_parts.remove(this.next_part_idx) {
                    *this.next_part_idx += 1;
                    this.bytes_hint
                        .reduce_by(next.chunks.iter().map(|c| c.len()).sum());
                    Poll::Ready(Some(Ok(Part {
                        part_index: next.part_index,
                        blob_offset: next.blob_offset,
                        range_offset: next.range_offset,
                        chunks: next.chunks,
                    })))
                } else {
                    *this.is_closed = true;
                    *this.bytes_hint = BytesHint::new_exact(0);
                    Poll::Ready(None)
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

impl TryFrom<ChunkStream> for PartStream<ChunkStream> {
    type Error = CondowError;

    fn try_from(chunk_stream: ChunkStream) -> Result<Self, Self::Error> {
        PartStream::from_chunk_stream(chunk_stream)
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use crate::test_utils::create_part_stream;

    #[tokio::test]
    async fn check_iter_one_part_one_chunk() {
        let (mut stream, expected) = create_part_stream(1, 1, true, Some(10));

        let mut collected = Vec::new();

        while let Some(next) = stream.next().await {
            let next = next.unwrap();

            next.chunks.iter().flatten().for_each(|&v| {
                collected.push(v);
            });
        }

        assert_eq!(collected, expected);
    }

    #[tokio::test]
    async fn check_iter_one_part_two_chunks() {
        let (mut stream, expected) = create_part_stream(1, 2, true, Some(10));

        let mut collected = Vec::new();

        while let Some(next) = stream.next().await {
            let next = next.unwrap();

            next.chunks.iter().flatten().for_each(|&v| {
                collected.push(v);
            });
        }

        assert_eq!(collected, expected);
    }

    #[tokio::test]
    async fn check_iter_two_parts_one_chunk() {
        let (mut stream, expected) = create_part_stream(2, 1, true, Some(10));

        let mut collected = Vec::new();

        while let Some(next) = stream.next().await {
            let next = next.unwrap();

            next.chunks.iter().flatten().for_each(|&v| {
                collected.push(v);
            });
        }

        assert_eq!(collected, expected);
    }

    #[tokio::test]
    async fn check_iter_multiple() {
        for parts in 1..10 {
            for chunks in 1..10 {
                let (mut stream, expected) = create_part_stream(parts, chunks, true, Some(10));

                let mut collected = Vec::new();

                while let Some(next) = stream.next().await {
                    let next = next.unwrap();

                    next.chunks.iter().flatten().for_each(|&v| {
                        collected.push(v);
                    });
                }

                assert_eq!(collected, expected);
            }
        }
    }

    mod into_vec {
        use crate::test_utils::create_part_stream;

        #[tokio::test]
        async fn with_exact_hint() {
            for parts in 1..10 {
                for chunks in 1..10 {
                    let (stream, expected) = create_part_stream(parts, chunks, true, Some(10));

                    let result = stream.into_vec().await.unwrap();

                    assert_eq!(result, expected);
                }
            }
        }

        #[tokio::test]
        async fn with_at_max_hint() {
            for parts in 1..10 {
                for chunks in 1..10 {
                    let (stream, expected) = create_part_stream(parts, chunks, false, Some(10));

                    let result = stream.into_vec().await.unwrap();

                    assert_eq!(result, expected);
                }
            }
        }
    }
}
