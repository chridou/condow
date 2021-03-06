use std::{
    io::{Error as IoError, ErrorKind as IoErrorKind, Result as IoResult, SeekFrom},
    pin::Pin,
    sync::Arc,
    task,
};

use futures::{future::BoxFuture, AsyncRead, AsyncSeek};

use crate::{config::Mebi, errors::CondowError, InclusiveRange};

use super::BytesAsyncReader;

type GetNewReaderFuture = BoxFuture<'static, Result<BytesAsyncReader, CondowError>>;

/// 8 MiBytes
const FETCH_AHEAD_BYTES: u64 = Mebi(8).value();

/// Specifies whether to fetch data ahead and if so how.
///
/// The default is to fetch 8 Mebibytes ahead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchAheadMode {
    /// Don't fetch any data in excess of those requested.
    None,
    /// Fetch n bytes ahead of the current position when bytes are requested.
    ///
    /// If the number of bytes queried is larger than the size of the
    /// parts to be downloaded the download will be executed with the
    /// parts downloaded concurrently.
    Bytes(u64),
    /// Fetch all data from the current position to the end of the BLOB.
    ///
    /// If the number of bytes queried is larger than the size of the
    /// parts to be downloaded the download will be executed with the
    /// parts downloaded concurrently.
    ToEnd,
}

impl Default for FetchAheadMode {
    fn default() -> Self {
        Self::Bytes(FETCH_AHEAD_BYTES)
    }
}

impl From<usize> for FetchAheadMode {
    fn from(v: usize) -> Self {
        Self::Bytes(v as u64)
    }
}

impl From<u64> for FetchAheadMode {
    fn from(v: u64) -> Self {
        Self::Bytes(v)
    }
}

enum State {
    Initial,
    /// Wait for a new stream to be created
    GetNewReaderFuture(GetNewReaderFuture),
    PollingReader(BytesAsyncReader),
    Finished,
    Error,
}

type GetReaderFn = Arc<
    dyn Fn(InclusiveRange) -> BoxFuture<'static, Result<BytesAsyncReader, CondowError>>
        + Send
        + Sync
        + 'static,
>;

/// Implements [AsyncRead] and [AsyncSeek]
///
/// This reader allows for random access on the BLOB.
///
/// Random access on a remote BLOB via a reader is a rather unusual case and
/// is provided to interface with libraries which are based on reading data
/// via [AsyncRead] and [AsyncSeek].
///
/// # Behaviour
///
/// The download is initiated once the first bytes have been
/// queried from the reader. Seek does not intiate a download
/// but currently forces a new download to be started once the reader
/// is polled for bytes again.
///
/// The BLOB is only downloaded concurrently
/// if prefetching is enabled via [FetchAheadMode::Bytes] or
/// [FetchAheadMode::ToEnd]. The In these cases the number of bytes
/// to be downloaded must be greater than the configured part size
/// for concurrent downloading.
pub struct RandomAccessReader {
    /// Reading position of the next byte
    pos: u64,
    /// Get a new stream
    get_reader: GetReaderFn,
    /// Range in which the reader can operate
    bounds: InclusiveRange,
    state: State,
    fetch_ahead_mode: FetchAheadMode,
}

impl RandomAccessReader {
    /// Will create a reader with the given known size of the BLOB.
    pub fn new<F, FM>(get_reader: F, bounds: InclusiveRange, fetch_ahead_mode: FM) -> Self
    where
        F: Fn(InclusiveRange) -> BoxFuture<'static, Result<BytesAsyncReader, CondowError>>
            + Send
            + Sync
            + 'static,
        FM: Into<FetchAheadMode>,
    {
        Self {
            get_reader: Arc::new(get_reader),
            pos: 0,
            bounds,
            state: State::Initial,
            fetch_ahead_mode: fetch_ahead_mode.into(),
        }
    }

    /// Returns the current offset of the next byte to read.
    ///
    /// The offset is from the start of the BLOB.
    pub fn pos(&self) -> u64 {
        self.pos
    }

    fn get_next_reader(&self, dest_buf_len: u64) -> GetNewReaderFuture {
        let bytes_to_fetch = match self.fetch_ahead_mode {
            FetchAheadMode::None => dest_buf_len,
            FetchAheadMode::Bytes(n_bytes) => dest_buf_len.max(n_bytes),
            FetchAheadMode::ToEnd => self.bounds.len(),
        };

        let end_incl =
            (self.bounds.start() + self.pos + bytes_to_fetch - 1).min(self.bounds.end_incl());

        let range = (self.bounds.start() + self.pos)..=end_incl;

        (self.get_reader)(range.into())
    }
}

impl RandomAccessReader {
    pub fn set_fetch_ahead_mode<T: Into<FetchAheadMode>>(&mut self, mode: T) {
        self.fetch_ahead_mode = mode.into();
    }

    pub fn fetch_ahead_mode(&self) -> FetchAheadMode {
        self.fetch_ahead_mode
    }
}

impl AsyncRead for RandomAccessReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
        dest_buf: &mut [u8],
    ) -> task::Poll<IoResult<usize>> {
        if dest_buf.is_empty() {
            return task::Poll::Ready(Ok(0));
        }

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
            State::GetNewReaderFuture(mut fut) => match fut.as_mut().poll(cx) {
                task::Poll::Ready(Ok(reader)) => {
                    self.state = State::PollingReader(reader);
                    cx.waker().wake_by_ref();
                    task::Poll::Pending
                }
                task::Poll::Ready(Err(err)) => {
                    self.state = State::Error;
                    task::Poll::Ready(Err(IoError::new(IoErrorKind::Other, err)))
                }
                task::Poll::Pending => {
                    self.state = State::GetNewReaderFuture(fut);
                    task::Poll::Pending
                }
            },
            State::PollingReader(mut reader) => {
                match Pin::new(&mut reader).poll_read(cx, dest_buf) {
                    task::Poll::Ready(Ok(bytes_written)) => {
                        assert!(
                            self.pos <= self.bounds.len(),
                            "Position can not be larger than length"
                        );
                        self.pos += bytes_written as u64;
                        if self.pos == self.bounds.len() {
                            assert!(bytes_written != 0, "Still bytes left");
                            self.state = State::Finished;
                            task::Poll::Ready(Ok(bytes_written))
                        } else if bytes_written == 0 {
                            self.state = State::Initial;
                            cx.waker().wake_by_ref();
                            task::Poll::Pending
                        } else {
                            self.state = State::PollingReader(reader);
                            task::Poll::Ready(Ok(bytes_written))
                        }
                    }
                    task::Poll::Ready(Err(err)) => {
                        self.state = State::Error;
                        task::Poll::Ready(Err(IoError::new(IoErrorKind::Other, err)))
                    }
                    task::Poll::Pending => {
                        self.state = State::PollingReader(reader);
                        task::Poll::Pending
                    }
                }
            }
            State::Finished => {
                self.state = State::Finished;
                task::Poll::Ready(Ok(0))
            }
            State::Error => {
                self.state = State::Error;
                task::Poll::Ready(Err(IoError::new(
                    IoErrorKind::Other,
                    "the reader is broken and will not yield any more values",
                )))
            }
        }
    }
}

impl AsyncSeek for RandomAccessReader {
    fn poll_seek(
        self: Pin<&mut Self>,
        _: &mut task::Context<'_>,
        pos: SeekFrom,
    ) -> task::Poll<IoResult<u64>> {
        let this = self.get_mut();
        let new_pos = match pos {
            SeekFrom::Start(offset) => offset,
            SeekFrom::End(offset) => {
                if offset < 0 && -offset as u64 > this.bounds.len() {
                    // This would go before the start
                    // and is an error by the specification of SeekFrom::End
                    let err = CondowError::new_invalid_range("Seek before start");
                    return task::Poll::Ready(Err(IoError::new(IoErrorKind::Other, err)));
                }
                (this.bounds.len() as i64 + offset) as u64
            }
            SeekFrom::Current(offset) => {
                if offset < 0 && -offset as u64 > this.pos {
                    // This would go before the start
                    // and is an error by the specification of SeekFrom::Current
                    let err = CondowError::new_invalid_range("Seek before start");
                    return task::Poll::Ready(Err(IoError::new(IoErrorKind::Other, err)));
                }
                (this.pos as i64 + offset) as u64
            }
        };
        if new_pos != this.pos {
            this.pos = new_pos;
            // Initiate a new download
            this.state = State::Initial;
        }
        task::Poll::Ready(Ok(this.pos))
    }
}

#[cfg(test)]
mod tests {
    use futures::io::{AsyncReadExt as _, AsyncSeekExt as _};

    use crate::{test_utils::TestDownloader, Downloads};

    use super::*;

    #[tokio::test]
    async fn check_reader() {
        for n in 1..255 {
            let expected: Vec<u8> = (0..n).collect();

            let downloader = TestDownloader::new(n as usize);

            let mut reader = downloader
                .blob()
                .random_access_reader()
                .finish()
                .await
                .unwrap();

            let mut buf = Vec::new();
            let bytes_read = reader.read_to_end(&mut buf).await.unwrap();

            assert_eq!(bytes_read, expected.len(), "n bytes read ({} items)", n);
            assert_eq!(buf, expected, "bytes read ({} items)", n);
        }
    }

    #[tokio::test]
    async fn check_read_1() {
        let expected = [0u8];

        let downloader = TestDownloader::new(1);

        let mut reader = downloader
            .blob()
            .random_access_reader()
            .finish()
            .await
            .unwrap();

        let mut buf = Vec::new();
        let bytes_read = reader.read_to_end(&mut buf).await.unwrap();

        assert_eq!(bytes_read, 1);
        assert_eq!(buf, expected);
    }

    #[tokio::test]
    async fn offsets_and_seek_from_start() {
        let mut reader = TestDownloader::new_with_blob(vec![0, 1, 2, 3])
            .blob()
            .random_access_reader()
            .finish()
            .await
            .unwrap();

        assert_eq!(reader.pos(), 0);

        reader.seek(SeekFrom::Start(0)).await.unwrap();
        assert_eq!(reader.pos(), 0, "SeekFrom::Start(0)");

        reader.seek(SeekFrom::Start(1)).await.unwrap();
        assert_eq!(reader.pos(), 1, "SeekFrom::Start(1)");

        reader.seek(SeekFrom::Start(1_000)).await.unwrap();
        assert_eq!(reader.pos(), 1_000, "SeekFrom::Start(1_000)");
    }

    #[tokio::test]
    async fn offsets_and_seek_from_end() {
        let mut reader = TestDownloader::new_with_blob(vec![0, 1, 2, 3])
            .blob()
            .random_access_reader()
            .finish()
            .await
            .unwrap();

        reader.seek(SeekFrom::End(0)).await.unwrap();
        assert_eq!(reader.pos(), 4, "SeekFrom::End(0)");

        reader.seek(SeekFrom::End(-1)).await.unwrap();
        assert_eq!(reader.pos(), 3, "SeekFrom::End(-1)");

        reader.seek(SeekFrom::End(-4)).await.unwrap();
        assert_eq!(reader.pos(), 0, "SeekFrom::End(-4)");
    }

    #[tokio::test]
    async fn offsets_and_seek_from_current() {
        let mut reader = TestDownloader::new_with_blob(vec![0, 1, 2, 3])
            .blob()
            .random_access_reader()
            .finish()
            .await
            .unwrap();

        assert_eq!(reader.pos(), 0, "Fresh");

        reader.seek(SeekFrom::Current(3)).await.unwrap();
        assert_eq!(reader.pos(), 3, "SeekFrom::Current(3)");

        reader.seek(SeekFrom::Current(-1)).await.unwrap();
        assert_eq!(reader.pos(), 2, "SeekFrom::Current(-1)");

        reader.seek(SeekFrom::Current(1_000)).await.unwrap();
        assert_eq!(reader.pos(), 1_002, "SeekFrom::Current(1_000)");

        reader.seek(SeekFrom::Current(-1_002)).await.unwrap();
        assert_eq!(reader.pos(), 0, "SeekFrom::Current(-1_002)");
    }

    #[tokio::test]
    async fn seek_from_start() {
        let expected = vec![0, 1, 2, 3, 0, 0, 4, 5, 0, 6, 7];
        let downloader = TestDownloader::new_with_blob(expected.clone());
        let mut reader = downloader
            .blob()
            .random_access_reader()
            .finish()
            .await
            .unwrap();

        let mut buf = vec![0, 0, 0];
        reader.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, vec![0, 1, 2]);

        reader.seek(SeekFrom::Start(1)).await.unwrap();
        let mut buf = vec![0, 0, 0];
        reader.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, vec![1, 2, 3]);

        reader.seek(SeekFrom::Start(6)).await.unwrap();
        let mut buf = vec![0, 0];
        reader.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, vec![4, 5]);

        reader.seek(SeekFrom::Start(9)).await.unwrap();
        let mut buf = vec![0, 0];
        reader.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, vec![6, 7]);
    }

    #[tokio::test]
    async fn seek_from_end() {
        let expected = vec![0, 1, 2, 3, 0, 0, 4, 5, 0, 6, 7];
        let downloader = TestDownloader::new_with_blob(expected.clone());
        let mut reader = downloader
            .blob()
            .random_access_reader()
            .finish()
            .await
            .unwrap();

        reader.seek(SeekFrom::End(-10)).await.unwrap();
        let mut buf = vec![0, 0, 0];
        reader.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, vec![1, 2, 3]);

        reader.seek(SeekFrom::End(-5)).await.unwrap();
        let mut buf = vec![0, 0];
        reader.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, vec![4, 5]);

        reader.seek(SeekFrom::End(-2)).await.unwrap();
        let mut buf = vec![0, 0];
        reader.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, vec![6, 7]);
    }

    #[tokio::test]
    async fn seek_from_end_before_byte_zero_must_err() {
        let mut reader = TestDownloader::new_with_blob(vec![0, 1, 2, 3])
            .blob()
            .random_access_reader()
            .finish()
            .await
            .unwrap();
        // Hit 0 is ok
        let result = reader.seek(SeekFrom::End(-4)).await;
        assert!(result.is_ok());
        // Hit -1 is not ok
        let result = reader.seek(SeekFrom::End(-5)).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn seek_from_current() {
        let expected = vec![0, 1, 2, 3, 0, 0, 4, 5, 0, 6, 7];
        let downloader = TestDownloader::new_with_blob(expected.clone());
        let mut reader = downloader
            .blob()
            .random_access_reader()
            .finish()
            .await
            .unwrap();

        reader.seek(SeekFrom::Current(1)).await.unwrap();
        let mut buf = vec![0, 0, 0];
        reader.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, vec![1, 2, 3], "SeekFrom::Current 1");

        reader.seek(SeekFrom::Current(2)).await.unwrap();
        let mut buf = vec![0, 0];
        reader.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, vec![4, 5], "SeekFrom::Current 2");

        reader.seek(SeekFrom::Current(1)).await.unwrap();
        let mut buf = vec![0, 0];
        reader.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, vec![6, 7], "SeekFrom::Current 3");
    }

    #[tokio::test]
    async fn seek_from_current_before_byte_zero_must_err() {
        let mut reader = TestDownloader::new_with_blob(vec![0, 1, 2, 3])
            .blob()
            .random_access_reader()
            .finish()
            .await
            .unwrap();

        assert_eq!(reader.pos(), 0);

        let result = reader.seek(SeekFrom::Current(0)).await;
        assert!(result.is_ok());

        let result = reader.seek(SeekFrom::Current(-1)).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn read_from_range_no_seek() {
        let mut reader = TestDownloader::new_with_blob(vec![1, 2, 3, 4, 5])
            .blob()
            .range(1..=3)
            .random_access_reader()
            .finish()
            .await
            .unwrap();

        let mut buf = Vec::new();
        let bytes_read = reader.read_to_end(&mut buf).await.unwrap();

        assert_eq!(bytes_read, 3);
        assert_eq!(buf, [2, 3, 4]);
    }

    #[tokio::test]
    async fn read_from_range_seek_from_start() {
        let mut reader = TestDownloader::new_with_blob(vec![1, 2, 3, 4, 5])
            .blob()
            .range(1..=3)
            .random_access_reader()
            .finish()
            .await
            .unwrap();

        let mut buf = vec![0, 0, 0];
        reader.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, vec![2, 3, 4]);

        reader.seek(SeekFrom::Start(1)).await.unwrap();
        let mut buf = vec![0, 0];
        reader.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, vec![3, 4]);
    }

    #[tokio::test]
    async fn read_from_range_seek_from_end() {
        let mut reader = TestDownloader::new_with_blob(vec![1, 2, 3, 4, 5])
            .blob()
            .range(1..=3)
            .random_access_reader()
            .finish()
            .await
            .unwrap();

        reader.seek(SeekFrom::End(-2)).await.unwrap();
        let mut buf = vec![0, 0];
        reader.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, vec![3, 4]);
    }

    #[tokio::test]
    async fn fetch_ahead() {
        for n in 1..255 {
            let modes = [
                FetchAheadMode::ToEnd,
                FetchAheadMode::Bytes(5_000),
                FetchAheadMode::Bytes(n as u64 + 1),
                FetchAheadMode::Bytes(n as u64),
                FetchAheadMode::Bytes(1.max(n as u64 - 1)),
                FetchAheadMode::Bytes(252),
                FetchAheadMode::None,
                FetchAheadMode::Bytes(1),
            ];
            for mode in modes {
                let expected: Vec<u8> = (0..n).collect();

                let downloader = TestDownloader::new_with_blob(expected.clone());

                let mut reader = downloader
                    .blob()
                    .random_access_reader()
                    .finish()
                    .await
                    .unwrap();
                reader.set_fetch_ahead_mode(mode);

                let mut buf = Vec::new();
                let bytes_read = reader.read_to_end(&mut buf).await.unwrap();

                assert_eq!(
                    bytes_read,
                    expected.len(),
                    "n bytes read ({} items, mode: {:?})",
                    n,
                    mode
                );
                assert_eq!(buf, expected, "bytes read ({} items, mode: {:?})", n, mode);
            }
        }
    }
}
