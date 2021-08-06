use std::task::{Context, Poll};

use bytes::Bytes;
use futures::{channel::mpsc, ready, Stream, StreamExt};
use pin_project_lite::pin_project;

use super::{BytesStream, StreamError, TotalBytesHint};

pub type ChunkStreamItem = Result<ChunkItem, StreamError>;

#[derive(Debug, Clone)]
pub struct ChunkItem {
    /// Index of the part this chunk belongs to
    pub part: usize,
    /// Offset of the part this chunk belongs to
    pub offset: usize,
    pub payload: ChunkItemPayload,
}

#[derive(Debug, Clone)]
pub enum ChunkItemPayload {
    Chunk {
        bytes: Bytes,
        /// Index of the chunk within the part
        index: usize,
        /// Offset of the chunk relative to the parts offset
        offset: usize,
    },
    /// Last chunk of the part has already been sent.
    Terminator,
}

pin_project! {
    pub struct ChunkStream {
        n_parts: usize,
        total_bytes: TotalBytesHint,
        #[pin]
        receiver: mpsc::UnboundedReceiver<ChunkStreamItem>,
        is_closed: bool,
        is_fresh: bool,
    }
}

impl ChunkStream {
    pub fn new(
        n_parts: usize,
        total_bytes: TotalBytesHint,
    ) -> (Self, mpsc::UnboundedSender<ChunkStreamItem>) {
        let (tx, receiver) = mpsc::unbounded();

        let me = Self {
            n_parts,
            total_bytes,
            receiver,
            is_closed: false,
            is_fresh: true,
        };

        (me, tx)
    }

    pub fn from_full_file(mut bytes_stream: BytesStream, total_bytes: TotalBytesHint) -> Self {
        let (me, sender) = Self::new(0, total_bytes);

        tokio::spawn(async move {
            let mut chunk_index = 0;
            let mut offset = 0;

            let mut bytes_stream = bytes_stream.as_mut();
            while let Some(next) = bytes_stream.next().await {
                match next {
                    Ok(bytes) => {
                        let n_bytes = bytes.len();
                        if sender
                            .unbounded_send(Ok(ChunkItem {
                                part: 0,
                                offset: 0,
                                payload: ChunkItemPayload::Chunk {
                                    bytes,
                                    index: chunk_index,
                                    offset,
                                },
                            }))
                            .is_err()
                        {
                            break;
                        }
                        chunk_index += 1;
                        offset += n_bytes;
                    }
                    Err(err) => {
                        let _ = sender.unbounded_send(Err(StreamError::Io(err)));
                        break;
                    }
                }
            }
        });

        me
    }

    pub fn empty() -> Self {
        let (mut me, _) = Self::new(0, Some(0));
        me.is_closed = true;
        me.receiver.close();
        me
    }

    pub fn n_parts(&self) -> usize {
        self.n_parts
    }

    pub fn total_bytes(&self) -> TotalBytesHint {
        self.total_bytes
    }

    pub async fn fill_buffer(mut self, buffer: &mut [u8]) -> Result<usize, StreamError> {
        if !self.is_fresh {
            return Err(StreamError::Other("stream already iterated".to_string()));
        }

        if let Some(total_bytes) = self.total_bytes {
            if buffer.len() < total_bytes {
                return Err(StreamError::Other(format!(
                    "buffer to small. at least {} bytes required",
                    total_bytes
                )));
            }
        }

        let mut bytes_written = 0;

        while let Some(next) = self.next().await {
            let ChunkItem {
                offset, payload, ..
            } = match next {
                Err(err) => return Err(err),
                Ok(next) => next,
            };

            let (bytes, chunk_offset) = match payload {
                ChunkItemPayload::Terminator => continue,
                ChunkItemPayload::Chunk { bytes, offset, .. } => (bytes, offset),
            };

            let bytes_offset = offset + chunk_offset;
            let end_excl = bytes_offset + bytes.len();
            if end_excl >= buffer.len() {
                return Err(StreamError::Other(format!(
                    "buffer to small. at least {} bytes required",
                    end_excl
                )));
            }

            buffer[bytes_offset..end_excl].copy_from_slice(&bytes[..]);

            bytes_written += buffer.len();
        }

        Ok(bytes_written)
    }

    pub async fn into_vec(self) -> Result<Vec<u8>, StreamError> {
        if let Some(total_bytes) = self.total_bytes {
            let mut buffer = vec![0; total_bytes];
            let _ = self.fill_buffer(buffer.as_mut()).await?;
            Ok(buffer)
        } else {
            todo!()
        }
    }
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
            Some(Ok(chunk_item)) => Poll::Ready(Some(Ok(chunk_item))),
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
