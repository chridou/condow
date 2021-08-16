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
    /// Returns true if this is the last chunk of the part
    ///
    /// Same as `len()==0`
    pub fn is_last(&self) -> bool {
        self.bytes_left == 0
    }

    /// returns the number of bytes in this chunk
    pub fn len(&self) -> usize {
        self.bytes.len()
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
    pub async fn fill_buffer(mut self, buffer: &mut [u8]) -> Result<usize, CondowError> {
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
            let _ = self.fill_buffer(buffer.as_mut()).await?;
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

    use crate::streams::{Chunk, ChunkStream};

    async fn check_stream(mut result_stream: ChunkStream, data: &[u8], file_start: usize) {
        while let Some(next) = result_stream.next().await {
            let Chunk {
                range_offset,
                blob_offset: file_offset,
                bytes,
                ..
            } = match next {
                Err(err) => panic!("{}", err),
                Ok(next) => next,
            };

            assert_eq!(
                bytes[..],
                data[file_offset..file_offset + bytes.len()],
                "file_offset"
            );
            assert_eq!(
                bytes[..],
                data[file_start + range_offset..file_start + range_offset + bytes.len()],
                "range_offset"
            );
        }
    }

    mod open {
        use std::sync::Arc;

        use crate::{config::Config, test_utils::create_test_data, test_utils::*, Condow};

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
                                .download_chunks((), range.clone(), crate::GetSizeMode::Always)
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
                                .download_chunks((), range.clone(), crate::GetSizeMode::Required)
                                .await
                                .unwrap();

                            check_stream(result_stream, &data, range.start).await
                        }
                    }
                }
            }
        }
    }

    mod closed {
        use std::sync::Arc;

        use crate::{config::Config, test_utils::create_test_data, test_utils::*, Condow};

        use super::check_stream;

        #[tokio::test]
        async fn to_inclusive() {
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

                        for end_incl in [0usize, 2, 101, 255] {
                            let range = 0..=end_incl;

                            let result_stream = condow
                                .download_chunks((), range.clone(), crate::GetSizeMode::Default)
                                .await
                                .unwrap();

                            check_stream(result_stream, &data, *range.start()).await
                        }
                    }
                }
            }
        }

        #[tokio::test]
        async fn to_exclusive() {
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

                        for end_excl in [0usize, 2, 101, 255, 256] {
                            let range = 0..end_excl;

                            let result_stream = condow
                                .download_chunks((), range.clone(), crate::GetSizeMode::Default)
                                .await
                                .unwrap();

                            check_stream(result_stream, &data, range.start).await
                        }
                    }
                }
            }
        }

        mod from_to {
            mod start_at_0 {
                use std::sync::Arc;

                use crate::{
                    config::Config, streams::chunk_stream::tests::check_stream,
                    test_utils::create_test_data, test_utils::*, Condow,
                };

                #[tokio::test]
                async fn from_0_to_inclusive() {
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

                                for end_incl in [0usize, 2, 101, 255, 255] {
                                    let range = 0..=end_incl;

                                    let result_stream = condow
                                        .download_chunks(
                                            (),
                                            range.clone(),
                                            crate::GetSizeMode::Default,
                                        )
                                        .await
                                        .unwrap();

                                    check_stream(result_stream, &data, *range.start()).await
                                }
                            }
                        }
                    }
                }

                #[tokio::test]
                async fn from_0_to_exclusive() {
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

                                for end_excl in [0usize, 2, 101, 255, 256] {
                                    let range = 0..end_excl;

                                    let result_stream = condow
                                        .download_chunks(
                                            (),
                                            range.clone(),
                                            crate::GetSizeMode::Default,
                                        )
                                        .await
                                        .unwrap();

                                    check_stream(result_stream, &data, range.start).await
                                }
                            }
                        }
                    }
                }
            }

            mod start_after_0 {
                use std::sync::Arc;

                use crate::{
                    config::Config, streams::chunk_stream::tests::check_stream,
                    test_utils::create_test_data, test_utils::*, Condow,
                };

                #[tokio::test]
                async fn from_to_inclusive() {
                    let buffer_size = 10;

                    let data = Arc::new(create_test_data());

                    for chunk_size in [3] {
                        let client = TestCondowClient {
                            data: Arc::clone(&data),
                            max_jitter_ms: 0,
                            include_size_hint: true,
                            max_chunk_size: chunk_size,
                        };

                        for part_size in [1usize, 33] {
                            for n_concurrency in [1usize, 10] {
                                let config = Config::default()
                                    .buffer_size(buffer_size)
                                    .buffers_full_delay_ms(0)
                                    .part_size_bytes(part_size)
                                    .max_concurrency(n_concurrency);
                                let condow = Condow::new(client.clone(), config).unwrap();

                                for start in [1usize, 87, 101, 201] {
                                    for len in [1, 10, 100] {
                                        let end_incl = start + len;
                                        let range = start..=(end_incl);

                                        let result_stream = condow
                                            .download_chunks(
                                                (),
                                                range.clone(),
                                                crate::GetSizeMode::Default,
                                            )
                                            .await
                                            .unwrap();

                                        check_stream(result_stream, &data, *range.start()).await
                                    }
                                }
                            }
                        }
                    }
                }

                #[tokio::test]
                async fn from_to_exclusive() {
                    let buffer_size = 10;

                    let data = Arc::new(create_test_data());

                    for chunk_size in [3] {
                        let client = TestCondowClient {
                            data: Arc::clone(&data),
                            max_jitter_ms: 0,
                            include_size_hint: true,
                            max_chunk_size: chunk_size,
                        };

                        for part_size in [1usize, 33] {
                            for n_concurrency in [1usize, 10] {
                                let config = Config::default()
                                    .buffer_size(buffer_size)
                                    .buffers_full_delay_ms(0)
                                    .part_size_bytes(part_size)
                                    .max_concurrency(n_concurrency);
                                let condow = Condow::new(client.clone(), config).unwrap();

                                for start in [1usize, 87, 101, 201] {
                                    for len in [1, 10, 100] {
                                        let end_excl = start + len;
                                        let range = start..(end_excl);

                                        let result_stream = condow
                                            .download_chunks(
                                                (),
                                                range.clone(),
                                                crate::GetSizeMode::Default,
                                            )
                                            .await
                                            .unwrap();
                                        check_stream(result_stream, &data, range.start).await
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
