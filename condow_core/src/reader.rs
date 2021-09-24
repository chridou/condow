pub use bytes_async_reader::*;
pub use random_access_reader::*;

mod random_access_reader {
    use std::{
        io::{Error as IoError, ErrorKind as IoErrorKind, Result as IoResult, SeekFrom},
        pin::Pin,
        task,
    };

    use bytes::Bytes;
    use futures::{
        future::{BoxFuture, Future, FutureExt, TryFutureExt},
        ready,
        stream::{BoxStream, StreamExt},
        AsyncRead, AsyncSeek, Stream,
    };

    use crate::{errors::CondowError, Downloads};

    use super::BytesAsyncReader;

    type BytesStream = BoxStream<'static, Result<Bytes, CondowError>>;
    type AsyncReader = BytesAsyncReader<BytesStream>;
    type GetNewReaderFuture = BoxFuture<'static, Result<AsyncReader, CondowError>>;

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
        /// Wait for a new stream to be created
        GetNewReaderFuture(GetNewReaderFuture),
        PollingReader(AsyncReader),
        Finished,
    }

    /// Implements [AsyncRead] and [AsyncSeek]
    pub struct RandomAccessReader<D, L> {
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

    impl<D, L> RandomAccessReader<D, L>
    where
        D: Downloads<L> + Clone + Send + Sync + 'static,
        L: std::fmt::Debug + std::fmt::Display + Clone + Send + Sync + 'static,
    {
        pub async fn new(downloader: D, location: L) -> Result<Self, CondowError> {
            let length = downloader.get_size(location.clone()).await?;
            Ok(Self::new_with_length(downloader, location, length))
        }

        pub fn new_with_length(downloader: D, location: L, length: u64) -> Self {
            Self {
                downloader,
                location,
                pos: 0,
                length,
                state: State::Initial,
                fetch_ahead_mode: FetchAheadMode::default(),
            }
        }

        fn get_next_reader(&self, len: u64) -> GetNewReaderFuture {
            let len = match self.fetch_ahead_mode {
                FetchAheadMode::None => len,
                FetchAheadMode::Bytes(bytes) => len.max(bytes),
                FetchAheadMode::ToEnd => self.length,
            };

            let end_incl = (self.pos + len).min(len);

            let pos = self.pos as usize;
            let end_incl = end_incl as usize;

            let dl = self.downloader.clone();
            let location = self.location.clone();
            async move {
                dl.download(location, pos..=end_incl)
                    .map_ok(|stream| {
                        let stream = stream.bytes_stream().boxed();
                        let reader = super::BytesAsyncReader::new(stream);
                        reader
                    })
                    .await
            }
            .boxed()
        }
    }

    impl<D, L> AsyncRead for RandomAccessReader<D, L>
    where
        D: Downloads<L> + Clone + Send + Sync + 'static + Unpin,
        L: std::fmt::Debug + std::fmt::Display + Clone + Send + Sync + 'static + Unpin,
    {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut task::Context<'_>,
            dest_buf: &mut [u8],
        ) -> task::Poll<IoResult<usize>> {
            // Get ownership of the state to not deal with mutable references
            let current_state = std::mem::replace(&mut self.state, State::Initial);

            match current_state {
                State::Initial => {
                    // Get next stream with a future
                    let fut = self.get_next_reader(dest_buf.len() as u64);
                    self.state = State::GetNewReaderFuture(fut);
                    cx.waker().wake_by_ref();
                    task::Poll::Pending
                }
                State::GetNewReaderFuture(mut fut) => match ready!(fut.as_mut().poll(cx)) {
                    Ok(reader) => {
                        self.state = State::PollingReader(reader);
                        cx.waker().wake_by_ref();
                        task::Poll::Pending
                    }
                    Err(err) => task::Poll::Ready(Err(IoError::new(IoErrorKind::Other, err))),
                },
                State::PollingReader(mut reader) => {
                    match ready!(Pin::new(&mut reader).poll_read(cx, dest_buf)) {
                        Ok(bytes_written) => {
                            if bytes_written == 0 {
                                self.state = State::Finished;
                            }
                            task::Poll::Ready(Ok(bytes_written))
                        }
                        Err(err) => {
                            self.state = State::Finished;
                            task::Poll::Ready(Err(IoError::new(IoErrorKind::Other, err)))
                        }
                    }
                }
                State::Finished => {
                    self.state = State::Finished;
                    task::Poll::Ready(Ok(0))
                }
            }
        }
    }

    impl<D, L> AsyncSeek for RandomAccessReader<D, L>
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
}
mod bytes_async_reader {
    use std::io::{Error as IoError, ErrorKind as IoErrorKind, Result as IoResult};
    use std::pin::Pin;

    use bytes::Bytes;
    use futures::{
        future::{BoxFuture, Future},
        ready, task, AsyncRead, Stream,
    };

    use crate::errors::CondowError;

    /// A reader for streams of `Result<Bytes, CondowError>`.
    ///
    /// Consumes a stream of bytes and wraps it into an `AsyncRead`.
    pub struct BytesAsyncReader<St> {
        state: State<St>,
    }

    impl<St> BytesAsyncReader<St>
    where
        St: Stream<Item = Result<Bytes, CondowError>> + Send + 'static + Unpin,
    {
        pub fn new(stream: St) -> Self {
            Self {
                state: State::PollingStream(stream),
            }
        }
    }

    impl<St> AsyncRead for BytesAsyncReader<St>
    where
        St: Stream<Item = Result<Bytes, CondowError>> + Send + 'static + Unpin,
    {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut task::Context<'_>,
            dest_buf: &mut [u8],
        ) -> task::Poll<IoResult<usize>> {
            if dest_buf.is_empty() {
                self.state = State::Finished;
                return task::Poll::Ready(Err(IoError::new(
                    IoErrorKind::Other,
                    CondowError::new_other("'dest_buf' must have a length greater than 0"),
                )));
            }

            let current_state = std::mem::replace(&mut self.state, State::Finished);

            match current_state {
                State::PollingStream(mut stream) => {
                    match ready!(Pin::new(&mut stream).poll_next(cx)) {
                        Some(Ok(bytes)) => {
                            let mut buffer = Buffer(0, bytes);
                            let bytes_written = fill_destination_buffer(&mut buffer, dest_buf);

                            if buffer.is_empty() {
                                self.state = State::PollingStream(stream);
                            } else {
                                self.state = State::Buffered { buffer, stream };
                            }

                            task::Poll::Ready(Ok(bytes_written))
                        }
                        Some(Err(err)) => {
                            self.state = State::Finished;
                            task::Poll::Ready(Err(IoError::new(IoErrorKind::Other, err)))
                        }
                        None => {
                            self.state = State::Finished;
                            task::Poll::Ready(Ok(0))
                        }
                    }
                }
                State::Buffered { mut buffer, stream } => {
                    let n_bytes_written = fill_destination_buffer(&mut buffer, dest_buf);

                    if buffer.is_empty() {
                        self.state = State::PollingStream(stream);
                    } else {
                        self.state = State::Buffered { buffer, stream };
                    }

                    task::Poll::Ready(Ok(n_bytes_written))
                }
                State::Finished => {
                    self.state = State::Finished;
                    task::Poll::Ready(Ok(0))
                }
            }
        }
    }

    fn fill_destination_buffer(buf: &mut Buffer, dest: &mut [u8]) -> usize {
        let buf_slice = buf.as_slice();
        let mut bytes_written = 0;

        buf_slice.iter().zip(dest.iter_mut()).for_each(|(s, d)| {
            *d = *s;
            bytes_written += 1;
        });

        buf.0 += bytes_written;

        bytes_written
    }

    enum State<St> {
        PollingStream(St),
        /// State that holds undelivered bytes
        Buffered {
            /// Position in the first element of `bytes`
            buffer: Buffer,
            /// Bytes following those already buffered
            stream: St,
        },
        Finished,
    }

    /// (Next byte to read, Bytes[])
    struct Buffer(usize, Bytes);

    impl Buffer {
        pub fn is_empty(&self) -> bool {
            self.0 == self.1.len()
        }

        pub fn as_slice(&self) -> &[u8] {
            &self.1[self.0..]
        }
    }

    #[tokio::test]
    async fn test_unknown_at_this_point_of_in_at_sure_current_time() {
        use futures::io::AsyncReadExt as _;
        // create stream
        let bytes_stream: Vec<Result<Bytes, CondowError>> = vec![
            Ok(vec![0_u8, 1, 2].into()),
            Ok(vec![3_u8, 4, 5].into()),
            Ok(vec![6_u8, 7, 8].into()),
        ];
        let bytes_stream = futures::stream::iter(bytes_stream.into_iter());
        let mut reader = BytesAsyncReader::new(bytes_stream);

        let dest_buf: &mut [u8; 9] = &mut [42; 9];
        let bytes_written = reader.read(dest_buf).await.unwrap();

        assert_eq!(bytes_written, 9, "bytes_written");

        assert_eq!(dest_buf, &[0, 1, 2, 3, 4, 5, 6, 7, 8]);
    }

    #[test]
    fn test_buffer_is_empty() {
        let buffer = Buffer(0, Bytes::new());
        assert!(buffer.is_empty());

        let mut buffer = Buffer(0, vec![0_u8].into());
        assert!(!buffer.is_empty());

        buffer.0 = 1;
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_buffer_slice() {
        let buffer = Buffer(0, Bytes::new());
        assert_eq!(buffer.as_slice(), &[]);

        let mut buffer = Buffer(0, vec![0_u8].into());
        assert_eq!(buffer.as_slice(), &[0]);

        buffer.0 = 1;
        assert_eq!(buffer.as_slice(), &[]);

        let mut buffer = Buffer(0, vec![0_u8, 1_u8].into());
        assert_eq!(buffer.as_slice(), &[0, 1]);

        buffer.0 = 1;
        assert_eq!(buffer.as_slice(), &[1]);

        buffer.0 = 2;
        assert_eq!(buffer.as_slice(), &[]);
    }

    #[test]
    fn test_fill_destination_buffer_both_empty() {
        let mut buffer = Buffer(0, Bytes::new());
        let dest_buf: &mut [u8] = &mut [];

        let bytes_written = fill_destination_buffer(&mut buffer, dest_buf);
        assert_eq!(bytes_written, 0, "bytes_written");
        assert_eq!(buffer.0, 0, "buffer.0");
        assert_eq!(dest_buf, &[], "buffer.as_slice");
        assert_eq!(buffer.as_slice(), &[]);
    }

    #[test]
    fn test_fill_destination_buffer_1() {
        let mut buffer = Buffer(0, vec![0_u8].into());
        let dest_buf: &mut [u8] = &mut [10];

        let bytes_written = fill_destination_buffer(&mut buffer, dest_buf);
        assert_eq!(bytes_written, 1, "bytes_written");
        assert_eq!(buffer.0, 1, "buffer.0");
        assert_eq!(buffer.as_slice(), &[], "buffer.as_slice");
        assert_eq!(dest_buf, &[0]);

        let bytes_written = fill_destination_buffer(&mut buffer, dest_buf);
        assert_eq!(bytes_written, 0, "bytes_written");
        assert_eq!(buffer.0, 1, "buffer.0");
        assert_eq!(buffer.as_slice(), &[], "buffer.as_slice");
        assert_eq!(dest_buf, &[0]);
    }

    #[test]
    fn test_fill_destination_buffer_2() {
        let mut buffer = Buffer(0, vec![0_u8].into());
        let dest_buf: &mut [u8] = &mut [10, 11];

        let bytes_written = fill_destination_buffer(&mut buffer, dest_buf);
        assert_eq!(bytes_written, 1, "bytes_written");
        assert_eq!(buffer.0, 1, "buffer.0");
        assert_eq!(buffer.as_slice(), &[], "buffer.as_slice");
        assert_eq!(dest_buf, &[0, 11]);

        let bytes_written = fill_destination_buffer(&mut buffer, dest_buf);
        assert_eq!(bytes_written, 0, "bytes_written");
        assert_eq!(buffer.0, 1, "buffer.0");
        assert_eq!(buffer.as_slice(), &[], "buffer.as_slice");
        assert_eq!(dest_buf, &[0, 11]);
    }

    #[test]
    fn test_fill_destination_buffer_3() {
        let mut buffer = Buffer(0, vec![0_u8, 1].into());
        let dest_buf: &mut [u8] = &mut [10];

        let bytes_written = fill_destination_buffer(&mut buffer, dest_buf);
        assert_eq!(bytes_written, 1, "bytes_written");
        assert_eq!(buffer.0, 1, "buffer.0");
        assert_eq!(buffer.as_slice(), &[1], "buffer.as_slice");
        assert_eq!(dest_buf, &[0]);

        let bytes_written = fill_destination_buffer(&mut buffer, dest_buf);
        assert_eq!(bytes_written, 1, "buffer.0");
        assert_eq!(buffer.0, 2);
        assert_eq!(buffer.as_slice(), &[]);
        assert_eq!(dest_buf, &[1]);

        let bytes_written = fill_destination_buffer(&mut buffer, dest_buf);
        assert_eq!(bytes_written, 0, "buffer.0");
        assert_eq!(buffer.0, 2);
        assert_eq!(buffer.as_slice(), &[]);
        assert_eq!(dest_buf, &[1]);
    }

    #[test]
    fn test_fill_destination_buffer_4() {
        let mut buffer = Buffer(0, vec![0_u8, 1].into());
        let dest_buf: &mut [u8] = &mut [10, 11];

        let bytes_written = fill_destination_buffer(&mut buffer, dest_buf);
        assert_eq!(bytes_written, 2, "bytes_written");
        assert_eq!(buffer.0, 2, "buffer.0");
        assert_eq!(buffer.as_slice(), &[], "buffer.as_slice");
        assert_eq!(dest_buf, &[0, 1]);

        let bytes_written = fill_destination_buffer(&mut buffer, dest_buf);
        assert_eq!(bytes_written, 0, "bytes_written");
        assert_eq!(buffer.0, 2, "buffer.0");
        assert_eq!(buffer.as_slice(), &[]);
        assert_eq!(dest_buf, &[0, 1]);
    }

    #[test]
    fn test_fill_destination_buffer_5() {
        let mut buffer = Buffer(0, vec![0_u8, 1, 2].into());
        let dest_buf: &mut [u8] = &mut [10, 11];

        let bytes_written = fill_destination_buffer(&mut buffer, dest_buf);
        assert_eq!(bytes_written, 2, "bytes_written");
        assert_eq!(buffer.0, 2, "buffer.0");
        assert_eq!(buffer.as_slice(), &[2], "buffer.as_slice");
        assert_eq!(dest_buf, &[0, 1]);

        let bytes_written = fill_destination_buffer(&mut buffer, dest_buf);
        assert_eq!(bytes_written, 1, "bytes_written");
        assert_eq!(buffer.0, 3, "buffer.0");
        assert_eq!(buffer.as_slice(), &[]);
        assert_eq!(dest_buf, &[2, 1]);

        let bytes_written = fill_destination_buffer(&mut buffer, dest_buf);
        assert_eq!(bytes_written, 0, "bytes_written");
        assert_eq!(buffer.0, 3, "buffer.0");
        assert_eq!(buffer.as_slice(), &[]);
        assert_eq!(dest_buf, &[2, 1]);
    }

    #[test]
    fn test_fill_destination_buffer_dest_empty() {
        let mut buffer = Buffer(0, vec![0_u8].into());
        let dest_buf: &mut [u8] = &mut [];

        let bytes_written = fill_destination_buffer(&mut buffer, dest_buf);
        assert_eq!(bytes_written, 0, "bytes_written");
        assert_eq!(buffer.0, 0, "buffer.0");
        assert_eq!(buffer.as_slice(), &[0], "buffer.as_slice");
        assert_eq!(dest_buf, &[]);

        buffer.0 = 1;
        let bytes_written = fill_destination_buffer(&mut buffer, dest_buf);
        assert_eq!(bytes_written, 0, "bytes_written");
        assert_eq!(buffer.0, 1, "buffer.0");
        assert_eq!(buffer.as_slice(), &[], "buffer.as_slice");
        assert_eq!(dest_buf, &[]);
    }
}
