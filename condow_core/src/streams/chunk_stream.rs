use std::task::{Context, Poll};

use bytes::Bytes;
use futures::{channel::mpsc, ready, Stream, StreamExt};
use pin_project_lite::pin_project;

use super::{BytesStream, StreamError, TotalBytesHint};

pub type ChunkStreamItem = Result<ChunkItem, StreamError>;

pub struct ChunkItem {
    /// Index of the part this chunk belongs to
    pub part: usize,
    /// Offset of the part this chunk belongs to
    pub offset: usize,
    pub payload: ChunkItemPayload,
}

pub enum ChunkItemPayload {
    Chunk {
        bytes: Bytes,
        /// Index of the chunk within the part
        index: usize,
        /// Offset of the chunk relative to the parts offset
        offset: usize,
    },
    /// Last chunk of the part.
    Terminator,
}

pin_project! {
    pub struct ChunkStream {
        n_parts: usize,
        total_bytes: TotalBytesHint,
        #[pin]
        receiver: mpsc::UnboundedReceiver<ChunkStreamItem>,
        is_closed: bool,
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
}

impl Stream for ChunkStream {
    type Item = ChunkStreamItem;

    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.is_closed {
            return Poll::Ready(None);
        }

        let mut this = self.project();
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
