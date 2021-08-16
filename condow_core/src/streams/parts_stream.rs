use std::{
    collections::HashMap,
    task::{Context, Poll},
};

use bytes::Bytes;
use futures::{ready, Stream};
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

impl<St: Stream<Item = ChunkStreamItem>> PartStream<St> {
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
}

impl PartStream<ChunkStream> {
    /// Create a new [PartStream] from the given [ChunkStream]
    ///
    /// Will fail if the [ChunkStream] was already iterated.
    pub fn from_chunk_stream(chunk_stream: ChunkStream) -> Result<Self, CondowError> {
        if !chunk_stream.is_fresh() {
            return Err(CondowError::Other(
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
                            Poll::Pending
                        }
                    } else {
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
                *this.is_closed = true;
                Poll::Ready(None)
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}
