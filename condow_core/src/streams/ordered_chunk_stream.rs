use std::{
    collections::{HashMap, VecDeque},
    convert::TryFrom,
    task::{Context, Poll},
};

use bytes::Bytes;
use futures::{future, stream::BoxStream, Stream, StreamExt, TryStreamExt};
use pin_project_lite::pin_project;

use crate::errors::CondowError;

use super::{BytesHint, Chunk, ChunkStream, ChunkStreamItem};

pin_project! {
    /// A stream of downloaded chunks ordered
    ///
    /// All chunks are ordered as they would
    /// have appeared in a sequential download of a range/BLOB
    pub struct OrderedChunkStream {
        #[pin]
        chunk_stream: BoxStream<'static, Result<Chunk, CondowError>>,
        current_part_index: u64,
        current_chunk_index: usize,
        collected: HashMap<u64, VecDeque<Chunk>>,
        is_closed: bool,
        bytes_hint: BytesHint,
    }
}

impl OrderedChunkStream {
    /// Create a new [PartStream].
    ///
    /// **Call with care.** This function will only work
    /// if the input stream was not iterated before
    /// since all chunks are needed. The stream might live lock
    /// if the input was already iterated.
    pub fn new<St>(chunk_stream: St, bytes_hint: BytesHint) -> Self
    where
        St: Stream<Item = ChunkStreamItem> + Send + 'static + Unpin,
    {
        Self {
            chunk_stream: Box::pin(chunk_stream),
            current_part_index: 0,
            current_chunk_index: 0,
            collected: HashMap::new(),
            is_closed: false,
            bytes_hint,
        }
    }

    /// Create a new [OrderedChunkStream] from the given [ChunkStream]
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

    /// Hint on the remaining bytes on this stream.
    pub fn bytes_hint(&self) -> BytesHint {
        self.bytes_hint
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
            let chunk = next?;

            let end_excl = offset + chunk.len();
            if end_excl > buffer.len() {
                return Err(CondowError::new_other(format!(
                    "write attempt beyond buffer end (buffer len = {}). \
                        attempted to write at index {}",
                    buffer.len(),
                    end_excl
                )));
            }

            buffer[offset..end_excl].copy_from_slice(&chunk.bytes[..]);

            offset = end_excl;
        }

        Ok(offset)
    }

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
                let chunk = next?;

                buffer.extend(chunk.bytes);
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

    pub fn bytes_stream(self) -> impl Stream<Item = Result<Bytes, CondowError>> + Send + 'static {
        self.map_ok(|chunk| chunk.bytes)
    }
}

impl Stream for OrderedChunkStream {
    type Item = ChunkStreamItem;

    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.is_closed {
            return Poll::Ready(None);
        }

        let this = self.project();

        match this.chunk_stream.poll_next(cx) {
            Poll::Ready(next) => match next {
                Some(Ok(chunk)) => {
                    if chunk.part_index == *this.current_part_index {
                        let chunk_to_send = if chunk.chunk_index == *this.current_chunk_index {
                            chunk
                        } else {
                            let chunks = this.collected.get_mut(this.current_part_index).unwrap();
                            chunks.push_back(chunk);
                            chunks.pop_front().unwrap()
                        };

                        if chunk_to_send.is_last() {
                            this.collected.remove(this.current_part_index);
                            *this.current_part_index += 1;
                            *this.current_chunk_index = 0;
                        } else {
                            *this.current_chunk_index += 1;
                        }

                        this.bytes_hint.reduce_by(chunk_to_send.len() as u64);
                        Poll::Ready(Some(Ok(chunk_to_send)))
                    } else {
                        let entry = this
                            .collected
                            .entry(chunk.part_index)
                            .or_insert_with(|| VecDeque::new());
                        entry.push_back(chunk);
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                }
                Some(Err(err)) => {
                    *this.is_closed = true;
                    *this.bytes_hint = BytesHint::new_exact(0);
                    Poll::Ready(Some(Err(err)))
                }
                None => {
                    if let Some(chunks) = this.collected.get_mut(this.current_part_index) {
                        let chunk = chunks.pop_front().expect("empty chunks");
                        if chunk.is_last() {
                            this.collected.remove(this.current_part_index);
                            *this.current_part_index += 1;
                            *this.current_chunk_index = 0;
                        } else {
                            *this.current_chunk_index += 1;
                        }
                        *this.bytes_hint = BytesHint::new_exact(0);
                        cx.waker().wake_by_ref();
                        Poll::Ready(Some(Ok(chunk)))
                    } else {
                        *this.is_closed = true;
                        Poll::Ready(None)
                    }
                }
            },
            Poll::Pending => {
                if let Some(chunks) = this.collected.get_mut(this.current_part_index) {
                    let chunk = chunks.pop_front().expect("empty chunks");
                    if chunk.is_last() {
                        this.collected.remove(this.current_part_index);
                        *this.current_part_index += 1;
                        *this.current_chunk_index = 0;
                    } else {
                        *this.current_chunk_index += 1;
                    }
                    Poll::Ready(Some(Ok(chunk)))
                } else {
                    //cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

impl TryFrom<ChunkStream> for OrderedChunkStream {
    type Error = CondowError;

    fn try_from(chunk_stream: ChunkStream) -> Result<Self, Self::Error> {
        OrderedChunkStream::from_chunk_stream(chunk_stream)
    }
}
#[cfg(test)]
mod tests {

    mod basic_streams {
        use bytes::Bytes;
        use futures::StreamExt;

        use crate::streams::{BytesHint, Chunk, ChunkStream};

        #[tokio::test]
        async fn parts_0_chunks_1() {
            let (chunk_stream, tx) = ChunkStream::new(BytesHint::new_exact(2));
            let mut ordered_chunk_stream = chunk_stream.try_into_part_stream().unwrap();

            let chunk = Chunk {
                part_index: 0,
                chunk_index: 0,
                blob_offset: 0,
                range_offset: 0,
                bytes: Bytes::from_static(&[1]),
                bytes_left: 0,
            };
            tx.unbounded_send(Ok(chunk)).unwrap();

            drop(tx); // Close the channel! Otherwise the stream does not end!

            let mut collected = Vec::new();

            while let Some(next) = ordered_chunk_stream.next().await {
                let chunk = next.unwrap();

                chunk.bytes.iter().for_each(|&v| {
                    collected.push(v);
                });
            }

            let expected = &[1];
            assert_eq!(collected, expected);
        }

        #[tokio::test]
        async fn parts_0_chunks_2() {
            let (chunk_stream, tx) = ChunkStream::new(BytesHint::new_exact(2));
            let mut ordered_chunk_stream = chunk_stream.try_into_part_stream().unwrap();

            let chunk = Chunk {
                part_index: 0,
                chunk_index: 0,
                blob_offset: 0,
                range_offset: 0,
                bytes: Bytes::from_static(&[1]),
                bytes_left: 1,
            };
            tx.unbounded_send(Ok(chunk)).unwrap();

            let chunk = Chunk {
                part_index: 0,
                chunk_index: 1,
                blob_offset: 1,
                range_offset: 1,
                bytes: Bytes::from_static(&[2]),
                bytes_left: 0,
            };
            tx.unbounded_send(Ok(chunk)).unwrap();

            drop(tx); // Close the channel! Otherwise the stream does not end!

            let mut collected = Vec::new();

            while let Some(next) = ordered_chunk_stream.next().await {
                let chunk = next.unwrap();

                chunk.bytes.iter().for_each(|&v| {
                    collected.push(v);
                });
            }

            let expected = &[1, 2];
            assert_eq!(collected, expected);
        }

        #[tokio::test]
        async fn parts_0_1_chunks_1() {
            let (chunk_stream, tx) = ChunkStream::new(BytesHint::new_exact(2));
            let mut ordered_chunk_stream = chunk_stream.try_into_part_stream().unwrap();

            let chunk = Chunk {
                part_index: 0,
                chunk_index: 0,
                blob_offset: 0,
                range_offset: 0,
                bytes: Bytes::from_static(&[1]),
                bytes_left: 0,
            };
            tx.unbounded_send(Ok(chunk)).unwrap();
            let chunk = Chunk {
                part_index: 1,
                chunk_index: 0,
                blob_offset: 1,
                range_offset: 1,
                bytes: Bytes::from_static(&[2]),
                bytes_left: 0,
            };
            tx.unbounded_send(Ok(chunk)).unwrap();

            drop(tx); // Close the channel! Otherwise the stream does not end!

            let mut collected = Vec::new();

            while let Some(next) = ordered_chunk_stream.next().await {
                let chunk = next.unwrap();

                chunk.bytes.iter().for_each(|&v| {
                    collected.push(v);
                });
            }

            let expected = &[1, 2];
            assert_eq!(collected, expected);
        }

        #[tokio::test]
        async fn parts_0_1_chunks_2_non_interleaved() {
            let (chunk_stream, tx) = ChunkStream::new(BytesHint::new_exact(2));
            let mut ordered_chunk_stream = chunk_stream.try_into_part_stream().unwrap();

            let chunk = Chunk {
                part_index: 0,
                chunk_index: 0,
                blob_offset: 0,
                range_offset: 0,
                bytes: Bytes::from_static(&[1]),
                bytes_left: 1,
            };
            tx.unbounded_send(Ok(chunk)).unwrap();
            let chunk = Chunk {
                part_index: 0,
                chunk_index: 1,
                blob_offset: 1,
                range_offset: 1,
                bytes: Bytes::from_static(&[2]),
                bytes_left: 0,
            };
            tx.unbounded_send(Ok(chunk)).unwrap();
            let chunk = Chunk {
                part_index: 1,
                chunk_index: 0,
                blob_offset: 2,
                range_offset: 2,
                bytes: Bytes::from_static(&[3]),
                bytes_left: 1,
            };
            tx.unbounded_send(Ok(chunk)).unwrap();
            let chunk = Chunk {
                part_index: 1,
                chunk_index: 1,
                blob_offset: 3,
                range_offset: 3,
                bytes: Bytes::from_static(&[4]),
                bytes_left: 0,
            };
            tx.unbounded_send(Ok(chunk)).unwrap();

            drop(tx); // Close the channel! Otherwise the stream does not end!

            let mut collected = Vec::new();

            while let Some(next) = ordered_chunk_stream.next().await {
                let chunk = next.unwrap();

                chunk.bytes.iter().for_each(|&v| {
                    collected.push(v);
                });
            }

            let expected = &[1, 2, 3, 4];
            assert_eq!(collected, expected);
        }

        #[tokio::test]
        async fn parts_0_1_chunks_2_interleaved() {
            let (chunk_stream, tx) = ChunkStream::new(BytesHint::new_exact(2));
            let mut ordered_chunk_stream = chunk_stream.try_into_part_stream().unwrap();

            let chunk = Chunk {
                part_index: 0,
                chunk_index: 0,
                blob_offset: 0,
                range_offset: 0,
                bytes: Bytes::from_static(&[1]),
                bytes_left: 1,
            };
            tx.unbounded_send(Ok(chunk)).unwrap();
            let chunk = Chunk {
                part_index: 1,
                chunk_index: 0,
                blob_offset: 2,
                range_offset: 2,
                bytes: Bytes::from_static(&[3]),
                bytes_left: 1,
            };
            tx.unbounded_send(Ok(chunk)).unwrap();
            let chunk = Chunk {
                part_index: 0,
                chunk_index: 1,
                blob_offset: 1,
                range_offset: 1,
                bytes: Bytes::from_static(&[2]),
                bytes_left: 0,
            };
            tx.unbounded_send(Ok(chunk)).unwrap();
            let chunk = Chunk {
                part_index: 1,
                chunk_index: 1,
                blob_offset: 3,
                range_offset: 3,
                bytes: Bytes::from_static(&[4]),
                bytes_left: 0,
            };
            tx.unbounded_send(Ok(chunk)).unwrap();

            drop(tx); // Close the channel! Otherwise the stream does not end!

            let mut collected = Vec::new();

            while let Some(next) = ordered_chunk_stream.next().await {
                let chunk = next.unwrap();

                chunk.bytes.iter().for_each(|&v| {
                    collected.push(v);
                });
            }

            let expected = &[1, 2, 3, 4];
            assert_eq!(collected, expected);
        }

        #[tokio::test]
        async fn parts_1_0_chunks_1() {
            let (chunk_stream, tx) = ChunkStream::new(BytesHint::new_exact(2));
            let mut ordered_chunk_stream = chunk_stream.try_into_part_stream().unwrap();

            let chunk = Chunk {
                part_index: 1,
                chunk_index: 0,
                blob_offset: 1,
                range_offset: 1,
                bytes: Bytes::from_static(&[2]),
                bytes_left: 0,
            };
            tx.unbounded_send(Ok(chunk)).unwrap();
            let chunk = Chunk {
                part_index: 0,
                chunk_index: 0,
                blob_offset: 0,
                range_offset: 0,
                bytes: Bytes::from_static(&[1]),
                bytes_left: 0,
            };
            tx.unbounded_send(Ok(chunk)).unwrap();

            drop(tx); // Close the channel! Otherwise the stream does not end!

            let mut collected = Vec::new();

            while let Some(next) = ordered_chunk_stream.next().await {
                let chunk = next.unwrap();

                chunk.bytes.iter().for_each(|&v| {
                    collected.push(v);
                });
            }

            let expected = &[1, 2];
            assert_eq!(collected, expected);
        }

        #[tokio::test]
        async fn parts_1_0_chunks_2_non_interleaved() {
            let (chunk_stream, tx) = ChunkStream::new(BytesHint::new_exact(2));
            let mut ordered_chunk_stream = chunk_stream.try_into_part_stream().unwrap();

            let chunk = Chunk {
                part_index: 1,
                chunk_index: 0,
                blob_offset: 2,
                range_offset: 2,
                bytes: Bytes::from_static(&[3]),
                bytes_left: 1,
            };
            tx.unbounded_send(Ok(chunk)).unwrap();
            let chunk = Chunk {
                part_index: 1,
                chunk_index: 1,
                blob_offset: 3,
                range_offset: 3,
                bytes: Bytes::from_static(&[4]),
                bytes_left: 0,
            };
            tx.unbounded_send(Ok(chunk)).unwrap();
            let chunk = Chunk {
                part_index: 0,
                chunk_index: 0,
                blob_offset: 0,
                range_offset: 0,
                bytes: Bytes::from_static(&[1]),
                bytes_left: 1,
            };
            tx.unbounded_send(Ok(chunk)).unwrap();
            let chunk = Chunk {
                part_index: 0,
                chunk_index: 1,
                blob_offset: 1,
                range_offset: 1,
                bytes: Bytes::from_static(&[2]),
                bytes_left: 0,
            };
            tx.unbounded_send(Ok(chunk)).unwrap();

            drop(tx); // Close the channel! Otherwise the stream does not end!

            let mut collected = Vec::new();

            while let Some(next) = ordered_chunk_stream.next().await {
                let chunk = next.unwrap();

                chunk.bytes.iter().for_each(|&v| {
                    collected.push(v);
                });
            }

            let expected = &[1, 2, 3, 4];
            assert_eq!(collected, expected);
        }

        #[tokio::test]
        async fn parts_1_0_chunks_2_interleaved() {
            let (chunk_stream, tx) = ChunkStream::new(BytesHint::new_exact(2));
            let mut ordered_chunk_stream = chunk_stream.try_into_part_stream().unwrap();

            let chunk = Chunk {
                part_index: 1,
                chunk_index: 0,
                blob_offset: 2,
                range_offset: 2,
                bytes: Bytes::from_static(&[3]),
                bytes_left: 1,
            };
            tx.unbounded_send(Ok(chunk)).unwrap();
            let chunk = Chunk {
                part_index: 0,
                chunk_index: 0,
                blob_offset: 0,
                range_offset: 0,
                bytes: Bytes::from_static(&[1]),
                bytes_left: 1,
            };
            tx.unbounded_send(Ok(chunk)).unwrap();
            let chunk = Chunk {
                part_index: 1,
                chunk_index: 1,
                blob_offset: 3,
                range_offset: 3,
                bytes: Bytes::from_static(&[4]),
                bytes_left: 0,
            };
            tx.unbounded_send(Ok(chunk)).unwrap();
            let chunk = Chunk {
                part_index: 0,
                chunk_index: 1,
                blob_offset: 1,
                range_offset: 1,
                bytes: Bytes::from_static(&[2]),
                bytes_left: 0,
            };
            tx.unbounded_send(Ok(chunk)).unwrap();

            drop(tx); // Close the channel! Otherwise the stream does not end!

            let mut collected = Vec::new();

            while let Some(next) = ordered_chunk_stream.next().await {
                let chunk = next.unwrap();

                chunk.bytes.iter().for_each(|&v| {
                    collected.push(v);
                });
            }

            let expected = &[1, 2, 3, 4];
            assert_eq!(collected, expected);
        }
    }

    mod generated_streams {
        use futures::StreamExt;

        use crate::test_utils::create_part_stream;
        #[tokio::test]
        async fn check_iter_one_part_one_chunk() {
            let (mut stream, expected) = create_part_stream(1, 1, true, Some(10));

            let mut collected = Vec::new();

            while let Some(next) = stream.next().await {
                let chunk = next.unwrap();

                chunk.bytes.iter().for_each(|&v| {
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
                let chunk = next.unwrap();

                chunk.bytes.iter().for_each(|&v| {
                    collected.push(v);
                });
            }

            assert_eq!(collected, expected);
        }

        #[tokio::test]
        async fn check_iter_two_parts_one_chunk() {
            for run in 1..100 {
                let (mut stream, expected) = create_part_stream(2, 1, true, Some(10));

                let mut collected = Vec::new();

                while let Some(next) = stream.next().await {
                    let chunk = next.unwrap();

                    chunk.bytes.iter().for_each(|&v| {
                        collected.push(v);
                    });
                }

                assert_eq!(collected, expected, "run: {run}");
            }
        }

        #[tokio::test]
        async fn check_iter_two_parts_two_chunk() {
            for run in 1..100 {
                let (mut stream, expected) = create_part_stream(2, 2, true, Some(10));

                let mut collected = Vec::new();

                while let Some(next) = stream.next().await {
                    let chunk = next.unwrap();

                    chunk.bytes.iter().for_each(|&v| {
                        collected.push(v);
                    });
                }

                assert_eq!(collected, expected, "run: {run}");
            }
        }

        #[tokio::test]
        async fn check_iter_three_parts_two_chunk() {
            for run in 1..100 {
                let (mut stream, expected) = create_part_stream(3, 2, true, Some(10));

                let mut collected = Vec::new();

                while let Some(next) = stream.next().await {
                    let chunk = next.unwrap();

                    chunk.bytes.iter().for_each(|&v| {
                        collected.push(v);
                    });
                }

                assert_eq!(collected, expected, "run: {run}");
            }
        }

        #[tokio::test]
        async fn check_iter_many_parts_many_chunk() {
            for run in 1..100 {
                let (mut stream, expected) = create_part_stream(10, 10, true, Some(10));

                let mut collected = Vec::new();

                while let Some(next) = stream.next().await {
                    let chunk = next.unwrap();

                    chunk.bytes.iter().for_each(|&v| {
                        collected.push(v);
                    });
                }

                assert_eq!(collected, expected, "run: {run}");
            }
        }

        #[tokio::test]
        async fn check_iter_multiple() {
            for parts in 1..10 {
                for chunks in 1..10 {
                    let (mut stream, expected) = create_part_stream(parts, chunks, true, Some(10));

                    let mut collected = Vec::new();

                    while let Some(next) = stream.next().await {
                        let next = next.unwrap();

                        next.bytes.iter().for_each(|&v| {
                            collected.push(v);
                        });
                    }

                    assert_eq!(collected, expected, "parts: {parts} - chunks: {chunks}");
                }
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
