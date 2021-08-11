use std::task::{Context, Poll};

use bytes::Bytes;
use futures::{channel::mpsc, ready, Stream, StreamExt};
use pin_project_lite::pin_project;

use crate::errors::{IoError, StreamError};

use super::{BytesHint, BytesStream};

pub type ChunkStreamItem = Result<RangeChunk, StreamError>;

#[derive(Debug, Clone)]
pub struct RangeChunk {
    /// Index of the part this chunk belongs to
    pub part: usize,
    /// Offset of the part this chunk belongs to within the range
    pub range_offset: usize,
    /// Offset of the part this chunk belongs within the file
    pub file_offset: usize,
    pub payload: RangeChunkPayload,
}

impl RangeChunk {
    pub fn chunk(&self) -> Option<&Chunk> {
        match self.payload {
            RangeChunkPayload::Chunk(ref c) => Some(c),
            RangeChunkPayload::Terminator => None,
        }
    }
}

#[derive(Debug, Clone)]
pub enum RangeChunkPayload {
    Chunk(Chunk),
    /// Last chunk of the part has already been sent.
    Terminator,
}

#[derive(Debug, Clone)]
pub struct Chunk {
    pub bytes: Bytes,
    /// Index of the chunk within the part
    pub index: usize,
    /// Offset of the chunk relative to the parts offset
    pub offset: usize,
}

pin_project! {
    pub struct ChunkStream {
        n_parts: usize,
        bytes_hint: BytesHint,
        #[pin]
        receiver: mpsc::UnboundedReceiver<ChunkStreamItem>,
        is_closed: bool,
        is_fresh: bool,
    }
}

impl ChunkStream {
    pub fn new(
        n_parts: usize,
        bytes_hint: BytesHint,
    ) -> (Self, mpsc::UnboundedSender<ChunkStreamItem>) {
        let (tx, receiver) = mpsc::unbounded();

        let me = Self {
            n_parts,
            bytes_hint,
            receiver,
            is_closed: false,
            is_fresh: true,
        };

        (me, tx)
    }

    pub fn from_full_file(mut bytes_stream: BytesStream, bytes_hint: BytesHint) -> Self {
        let (me, sender) = Self::new(0, bytes_hint);

        tokio::spawn(async move {
            let mut chunk_index = 0;
            let mut offset = 0;

            let mut bytes_stream = bytes_stream.as_mut();
            while let Some(next) = bytes_stream.next().await {
                match next {
                    Ok(bytes) => {
                        let n_bytes = bytes.len();
                        if sender
                            .unbounded_send(Ok(RangeChunk {
                                part: 0,
                                range_offset: 0,
                                file_offset: 0,
                                payload: RangeChunkPayload::Chunk(Chunk {
                                    bytes,
                                    index: chunk_index,
                                    offset,
                                }),
                            }))
                            .is_err()
                        {
                            break;
                        }
                        chunk_index += 1;
                        offset += n_bytes;
                    }
                    Err(IoError(msg)) => {
                        let _ = sender.unbounded_send(Err(StreamError::Io(msg)));
                        break;
                    }
                }
            }
        });

        me
    }

    pub fn empty() -> Self {
        let (mut me, _) = Self::new(0, BytesHint(0, Some(0)));
        me.is_closed = true;
        me.receiver.close();
        me
    }

    pub fn n_parts(&self) -> usize {
        self.n_parts
    }

    pub fn bytes_hint(&self) -> BytesHint {
        self.bytes_hint
    }

    pub async fn fill_buffer(mut self, buffer: &mut [u8]) -> Result<usize, StreamError> {
        if !self.is_fresh {
            return Err(StreamError::Other("stream already iterated".to_string()));
        }

        if let Some(total_bytes) = self.bytes_hint.upper_bound() {
            if buffer.len() < total_bytes {
                return Err(StreamError::Other(format!(
                    "buffer to small ({}). at least {} bytes required",
                    buffer.len(),
                    total_bytes
                )));
            }
        }

        let mut bytes_written = 0;

        while let Some(next) = self.next().await {
            let RangeChunk {
                range_offset,
                payload,
                ..
            } = match next {
                Err(err) => return Err(err),
                Ok(next) => next,
            };

            let (bytes, chunk_offset) = match payload {
                RangeChunkPayload::Terminator => continue,
                RangeChunkPayload::Chunk(Chunk { bytes, offset, .. }) => (bytes, offset),
            };

            let bytes_offset = range_offset + chunk_offset;
            let end_excl = bytes_offset + bytes.len();
            if end_excl > buffer.len() {
                return Err(StreamError::Other(format!(
                    "write attempt beyond buffer end (buffer len = {}). \
                    attempted to write at index {}",
                    buffer.len(),
                    end_excl
                )));
            }

            buffer[bytes_offset..end_excl].copy_from_slice(&bytes[..]);

            bytes_written += bytes.len();
        }

        Ok(bytes_written)
    }

    pub async fn into_vec(self) -> Result<Vec<u8>, StreamError> {
        if let Some(total_bytes) = self.bytes_hint.upper_bound() {
            let mut buffer = vec![0; total_bytes];
            let _ = self.fill_buffer(buffer.as_mut()).await?;
            Ok(buffer)
        } else {
            stream_into_vec_with_unknown_size(self).await
        }
    }
}

async fn stream_into_vec_with_unknown_size(
    mut stream: ChunkStream,
) -> Result<Vec<u8>, StreamError> {
    let mut buffer = Vec::with_capacity(stream.bytes_hint.lower_bound());

    while let Some(next) = stream.next().await {
        let RangeChunk {
            range_offset,
            payload,
            ..
        } = match next {
            Err(err) => return Err(err),
            Ok(next) => next,
        };

        let (bytes, chunk_offset) = match payload {
            RangeChunkPayload::Terminator => continue,
            RangeChunkPayload::Chunk(Chunk { bytes, offset, .. }) => (bytes, offset),
        };

        let bytes_offset = range_offset + chunk_offset;
        let end_excl = bytes_offset + bytes.len();
        if end_excl >= buffer.len() {
            let missing = end_excl - buffer.len();
            buffer.extend((0..missing).map(|_| 0));
        }

        buffer[bytes_offset..end_excl].copy_from_slice(&bytes[..]);
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
 
                Poll::Ready(Some(Ok(chunk_item)))},
            Some(Err(err)) => {
                *this.is_closed = true;
                this.receiver.close();
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

    use crate::streams::{Chunk, ChunkStream};

    use super::{RangeChunk, RangeChunkPayload};

    mod open {
        use crate::{config::Config, test_utils::create_test_data, test_utils::*, Condow};
        use std::sync::Arc;

        use super::check_stream;

        #[tokio::test]
        async fn from_always_get_size() {
            let buffer_size = 10;

            let data = Arc::new(create_test_data());

            for chunk_size in [1, 3, 5] {
                let client = TestCondowClient {
                    data: Arc::clone(&data),
                    max_jitter_ms: 0,
                    include_size_hint: true,
                    max_chunk_size: chunk_size,
                };

                for part_size in [1usize, 3, 50, 1_000] {
                    for n_concurrency in [1usize, 10] {
                        let config = Config::default()
                            .buffer_size(buffer_size)
                            .buffers_full_delay_ms(0)
                            .part_size_bytes(part_size)
                            .max_concurrency(n_concurrency);
                        let condow = Condow::new(client.clone(), config).unwrap();

                        for from_idx in [0usize, 101, 255, 256] {
                            let range = from_idx..;

                            let result_stream = condow
                                .download_range((), range.clone(), crate::GetSizeMode::Always)
                                .await
                                .unwrap();

                                check_stream(result_stream, &data, range.start).await
                            }
                    }
                }
            }
        }

        #[tokio::test]
        async fn from_when_required_get_size() {
            let buffer_size = 10;

            let data = Arc::new(create_test_data());

            for chunk_size in [1, 3, 5] {
                let client = TestCondowClient {
                    data: Arc::clone(&data),
                    max_jitter_ms: 0,
                    include_size_hint: true,
                    max_chunk_size: chunk_size,
                };

                for part_size in [1usize, 3, 50, 1_000] {
                    for n_concurrency in [1usize, 10] {
                        let config = Config::default()
                            .buffer_size(buffer_size)
                            .buffers_full_delay_ms(0)
                            .part_size_bytes(part_size)
                            .max_concurrency(n_concurrency);
                        let condow = Condow::new(client.clone(), config).unwrap();

                        for from_idx in [0usize, 101, 255, 256] {
                            let range = from_idx..;

                            let result_stream = condow
                                .download_range((), range.clone(), crate::GetSizeMode::Required)
                                .await
                                .unwrap();

                            check_stream(result_stream, &data, range.start).await
                        }
                    }
                }
            }
        }
    }

    async fn check_stream(mut result_stream: ChunkStream, data: &[u8], file_start: usize) {
        while let Some(Ok(next)) = result_stream.next().await {
            let RangeChunk {
                range_offset,
                payload,
                file_offset,
                ..
            } = next;

            if let RangeChunkPayload::Chunk(chunk) = payload {
                let Chunk { bytes, offset, .. } = chunk;

                assert_eq!(
                    bytes[..],
                    data[file_offset + offset..file_offset + offset + bytes.len()], "file_offset"
                );
                assert_eq!(
                    bytes[..],
                    data[file_start + range_offset + offset..file_start + range_offset + offset + bytes.len()], "range_offset"
                );
            }
        }
    }
}
