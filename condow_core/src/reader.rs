use std::{
    io::{Error as IoError, ErrorKind as IoErrorKind, Result as IoResult, SeekFrom},
    pin::Pin,
    task,
};

use bytes::Bytes;
use futures::{
    future::{BoxFuture, Future},
    ready,
    stream::{BoxStream, StreamExt},
    AsyncRead, AsyncSeek,
};

use crate::{errors::CondowError, Downloads};

type BytesStream = BoxStream<'static, Result<Vec<Bytes>, CondowError>>;
type GetNewStreamFuture = BoxFuture<'static, Result<BytesStream, CondowError>>;

const FETCH_AHEAD_BYTES: u64 = 8 * 1024 * 1024;

pub enum FetchAheadMode {
    /// Don't fetch any data in excess of those requested.
    None,
    /// Fetch n bytes ahead of the current position when bytes are requested.
    Bytes(u64),
    /// Fetch all data from the current position to the end of the BLOB.
    ToEnd,
}

impl Default for FetchAheadMode {
    fn default() -> Self {
        Self::Bytes(FETCH_AHEAD_BYTES)
    }
}

struct Buffer(u64, Vec<Bytes>);

impl Buffer {
    pub fn is_empty(&self) -> bool {
        self.1.is_empty()
    }
}

enum State {
    Initial,
    /// State that holds undelivered bytes
    Buffered {
        /// Position in the first element of `bytes`
        buffer: Buffer,
        /// Bytes following those already buffered
        stream: BytesStream,
    },
    /// Wait for a new stream to be created
    GetNewStreamFuture(GetNewStreamFuture),
    PollingStream(BytesStream),
    Finished,
}

/// Implements [AsyncRead] and [AsyncSeek]
pub struct Reader<D, L> {
    /// Reading position of the next byte
    pos: u64,
    /// Download logic
    downloader: D,
    /// location of the BLOB
    location: L,
    /// total length of the BLOB
    length: u64,
    state: State,
    pub fetch_ahead_mode: FetchAheadMode,
}

impl<D, L> Reader<D, L>
where
    D: Downloads<L>,
    L: std::fmt::Debug + std::fmt::Display + Clone + Send + Sync + 'static,
{
    pub async fn new(downloader: D, location: L) -> Result<Self, CondowError> {
        let length = downloader.get_size(location.clone()).await?;
        Ok(Self {
            downloader,
            location,
            pos: 0,
            length,
            state: State::Initial,
            fetch_ahead_mode: FetchAheadMode::default(),
        })
    }

    fn get_next_stream(&self, min_bytes: usize) -> Result<GetNewStreamFuture, CondowError> {
        todo!()
    }
}

impl<D, L> AsyncRead for Reader<D, L>
where
    D: Downloads<L> + Unpin,
    L: std::fmt::Debug + std::fmt::Display + Clone + Send + Sync + 'static + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
        dest_buf: &mut [u8],
    ) -> task::Poll<IoResult<usize>> {
        // 1. Initial: build future(stream)
        // 2. GetNewStreamFuture: check if future is ready to work
        // 3. PollingStream: request next item from stream (in this state until buffer filled)
        // 4. Buffered: buffer field deliver data
        // 5 . PollingStream
        // 6.
        // 7. If stream is empty -> GetNewStreamFuture: Buffered state created new future if
        // 8. PollingStream
        // 9.
        match self.state {
            State::Initial => {
                // Get next stream with a future
                let fut = match self.get_next_stream(dest_buf.len()) {
                    Ok(fut) => fut,
                    Err(err) => {
                        return task::Poll::Ready(Err(IoError::new(IoErrorKind::Other, err)))
                    }
                };
                self.state = State::GetNewStreamFuture(fut);
                task::Poll::Pending
            }
            State::Buffered {
                ref buffer,
                ref stream,
            } => {
                todo!()
            }
            State::GetNewStreamFuture(ref mut fut) => match ready!(fut.as_mut().poll(cx)) {
                Ok(mut stream) => {
                    self.state = State::PollingStream(stream);
                    task::Poll::Pending
                }
                Err(err) => task::Poll::Ready(Err(IoError::new(IoErrorKind::Other, err))),
            },
            State::PollingStream(mut stream) => match ready!(stream.as_mut().poll_next(cx)) {
                Some(Ok(mut bytes)) => {
                    let mut buffer = Buffer(0, bytes);
                    let bytes_written = fill_buffer(&mut buffer, dest_buf);
                    self.pos += bytes_written as u64;

                    if self.pos == self.length {
                        self.state = State::Finished;
                        return task::Poll::Ready(Ok(bytes_written as usize));
                    }

                    if buffer.is_empty() {
                        self.state = State::PollingStream(stream);
                        return task::Poll::Ready(Ok(bytes_written as usize));
                    }

                    self.state = State::Buffered { buffer, stream };
                    task::Poll::Ready(Ok(bytes_written as usize))
                }
                Some(Err(err)) => task::Poll::Ready(Err(IoError::new(IoErrorKind::Other, err))),
                None => todo!(),
            },
            State::Finished => task::Poll::Ready(Ok(0)),
        }
    }
}

///
fn fill_buffer(buffer: &mut Buffer, dest_buf: &mut [u8]) -> usize {
    0
}

impl<D, L> AsyncSeek for Reader<D, L>
where
    D: Unpin,
    L: Unpin,
{
    fn poll_seek(
        self: Pin<&mut Self>,
        _: &mut task::Context<'_>,
        pos: SeekFrom,
    ) -> task::Poll<IoResult<u64>> {
        let this = self.get_mut();
        match pos {
            SeekFrom::Start(pos) => this.pos = pos,
            SeekFrom::End(pos) => this.pos = (this.length as i64 + pos) as u64,
            SeekFrom::Current(pos) => this.pos = (this.pos as i64 + pos) as u64,
        };
        task::Poll::Ready(Ok(this.pos))
    }
}
