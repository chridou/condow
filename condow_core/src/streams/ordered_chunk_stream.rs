use std::{
    collections::HashMap,
    convert::TryFrom,
    task::{Context, Poll},
};

use bytes::Bytes;
use futures::{channel::mpsc, ready, Stream, StreamExt, TryStreamExt};
use pin_project_lite::pin_project;

use crate::errors::CondowError;

use super::{BytesHint, Chunk, ChunkStream, ChunkStreamItem};

pin_project! {
    /// A stream of downloaded chunks ordered
    ///
    /// All chunks are ordered as they would
    /// have appeared in a sequential download of a range/BLOB
    pub struct OrderedChunkStream {
        bytes_hint: BytesHint,
        #[pin]
        inner_stream: Box<dyn Stream<Item = ChunkStreamItem> + Send + Sync + 'static + Unpin>,
        is_closed: bool,
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
        St: Stream<Item = ChunkStreamItem> + Send + Sync + 'static + Unpin,
    {
        let inner_stream = Box::new(collect_n_dispatch(chunk_stream));

        Self {
            bytes_hint,
            inner_stream,
            is_closed: false,
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

    pub fn bytes_stream(
        self,
    ) -> impl Stream<Item = Result<Bytes, CondowError>> + Send + Sync + 'static {
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

        let next = ready!(this.inner_stream.poll_next(cx));
        match next {
            Some(Ok(chunk)) => {
                this.bytes_hint.reduce_by(chunk.len() as u64);
                Poll::Ready(Some(Ok(chunk)))
            }
            Some(Err(err)) => {
                *this.is_closed = true;
                *this.bytes_hint = BytesHint::new_exact(0);
                Poll::Ready(Some(Err(err)))
            }
            None => {
                *this.is_closed = true;
                *this.bytes_hint = BytesHint::new_exact(0);
                Poll::Ready(None)
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

fn collect_n_dispatch<St>(
    chunk_stream: St,
) -> impl Stream<Item = ChunkStreamItem> + Send + Sync + 'static
where
    St: Stream<Item = ChunkStreamItem> + Send + Sync + 'static + Unpin,
{
    let (tx, rx) = mpsc::unbounded();

    tokio::spawn(collect_n_dispatch_loop(chunk_stream, tx));

    rx
}

async fn collect_n_dispatch_loop<StIn>(
    mut chunk_stream: StIn,
    send_item: mpsc::UnboundedSender<ChunkStreamItem>,
) where
    StIn: Stream<Item = ChunkStreamItem> + Send + Sync + 'static + Unpin,
{
    let mut current_part_idx = 0;
    let mut collected_chunks: HashMap<u64, Vec<Chunk>> = HashMap::new();
    let mut buffer_reservoir = Vec::new();

    while let Some(next) = chunk_stream.next().await {
        let chunk = match next {
            Ok(chunk) => chunk,
            Err(err) => {
                let _ = send_item.unbounded_send(Err(err));
                return;
            }
        };

        if chunk.part_index == current_part_idx {
            if chunk.is_last() {
                current_part_idx += 1;
            }

            if send_item.unbounded_send(Ok(chunk)).is_err() {
                return;
            }
        } else {
            let entry = collected_chunks.entry(chunk.part_index).or_insert_with(|| {
                buffer_reservoir
                    .pop()
                    .unwrap_or_else(|| Vec::with_capacity(16))
            });
            entry.push(chunk);

            continue;
        }

        while let Some(mut chunks_to_flush) = collected_chunks.remove(&current_part_idx) {
            for chunk in chunks_to_flush.drain(..) {
                if chunk.is_last() {
                    current_part_idx += 1;
                }

                if send_item.unbounded_send(Ok(chunk)).is_err() {
                    return;
                }
            }

            buffer_reservoir.push(chunks_to_flush);
        }
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
