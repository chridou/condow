use std::{
    collections::HashMap,
    task::{Context, Poll},
};

use bytes::Bytes;
use futures::{ready, Stream};
use pin_project_lite::pin_project;

use crate::{errors::StreamError, streams::Chunk};

use super::{BytesHint, ChunkStreamItem};

pub type PartStreamItem = Result<Part, StreamError>;

#[derive(Debug, Clone)]
pub struct Part {
    /// Index of the part this chunk belongs to
    pub part_index: usize,
    /// Offset of the chunk within the file
    pub file_offset: usize,
    /// Offset of the chunk within the downloaded range
    pub range_offset: usize,
    /// The chunks of bytes
    pub chunks: Vec<Bytes>,
}

struct PartEntry {
    part_idx: usize,
    file_offset: usize,
    range_offset: usize,
    bytes: Vec<Bytes>,
    is_complete: bool,
}

pin_project! {
    pub struct PartStream<St> {
        bytes_hint: BytesHint,
        #[pin]
        stream: St,
        is_closed: bool,
        next_part_idx: usize,
        collected_parts: HashMap<usize, PartEntry>
    }
}

impl<St: Stream<Item = ChunkStreamItem>> PartStream<St> {
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
}

impl<St: Stream<Item = ChunkStreamItem>> Stream for PartStream<St> {
    type Item = PartStreamItem;

    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.is_closed {
            return Poll::Ready(None);
        }

        let mut this = self.project();

        let next = ready!(this.stream.poll_next(cx));
        match next {
            Some(Ok(Chunk {
                part_index,
                chunk_index,
                file_offset,
                range_offset,
                bytes,
                bytes_left,
            })) => {
                this.bytes_hint.reduce_by(chunk_item.len());
                Poll::Ready(Some(Ok(chunk_item)))
            }
            Some(Err(err)) => {
                *this.is_closed = true;
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
