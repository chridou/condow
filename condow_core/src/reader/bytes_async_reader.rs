use std::io::{Error as IoError, ErrorKind as IoErrorKind, Result as IoResult};
use std::pin::Pin;

use bytes::Bytes;
use futures::{task, AsyncRead, Stream};

use crate::streams::BytesStream;

/// A reader for streams of `Result<Bytes, CondowError>`.
///
/// Consumes a stream of bytes and wraps it into an `AsyncRead`.
pub struct BytesAsyncReader {
    state: State,
}

impl BytesAsyncReader {
    pub fn new(stream: BytesStream) -> Self {
        Self {
            state: State::PollingStream(stream),
        }
    }
}

impl AsyncRead for BytesAsyncReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
        dest_buf: &mut [u8],
    ) -> task::Poll<IoResult<usize>> {
        if dest_buf.is_empty() {
            return task::Poll::Ready(Ok(0));
        }

        let current_state = std::mem::replace(&mut self.state, State::Finished);

        match current_state {
            State::PollingStream(mut stream) => match Pin::new(&mut stream).poll_next(cx) {
                task::Poll::Ready(Some(Ok(bytes))) => {
                    let mut buffer = Buffer(0, bytes);
                    let bytes_written = fill_destination_buffer(&mut buffer, dest_buf);

                    if buffer.is_empty() {
                        self.state = State::PollingStream(stream);
                    } else {
                        self.state = State::Buffered { buffer, stream };
                    }

                    task::Poll::Ready(Ok(bytes_written))
                }
                task::Poll::Ready(Some(Err(err))) => {
                    self.state = State::Error;
                    task::Poll::Ready(Err(IoError::new(IoErrorKind::Other, err)))
                }
                task::Poll::Ready(None) => {
                    self.state = State::Finished;
                    task::Poll::Ready(Ok(0))
                }
                task::Poll::Pending => {
                    self.state = State::PollingStream(stream);
                    task::Poll::Pending
                }
            },
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
            State::Error => {
                self.state = State::Error;
                task::Poll::Ready(Err(IoError::new(
                    IoErrorKind::Other,
                    "the reader is broken and will not yield any mor values",
                )))
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

enum State {
    PollingStream(BytesStream),
    /// State that holds undelivered bytes
    Buffered {
        /// Position in the first element of `bytes`
        buffer: Buffer,
        /// Bytes following those already buffered
        stream: BytesStream,
    },
    Finished,
    Error,
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

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures::AsyncReadExt;

    use crate::errors::CondowError;
    use crate::reader::bytes_async_reader::{fill_destination_buffer, Buffer};
    use crate::reader::BytesAsyncReader;
    use crate::streams::{BytesHint, BytesStream};
    use crate::test_utils::TestDownloader;
    use crate::Downloads;

    #[tokio::test]
    async fn test_read_from_stream_with_stream_chunk_and_destination_buffer_same_size() {
        use futures::io::AsyncReadExt as _;
        // create stream
        let bytes_stream: Vec<Result<Bytes, CondowError>> = vec![
            Ok(vec![0_u8, 1, 2].into()),
            Ok(vec![3_u8, 4, 5].into()),
            Ok(vec![6_u8, 7, 8].into()),
        ];
        let bytes_stream = futures::stream::iter(bytes_stream.into_iter());
        let bytes_stream = BytesStream::new(bytes_stream, BytesHint::new_no_hint());
        let mut reader = BytesAsyncReader::new(bytes_stream);
        let dest_buf: &mut [u8; 3] = &mut [42; 3];

        let bytes_written = reader.read(dest_buf).await.unwrap();
        assert_eq!(bytes_written, 3, "bytes_written");
        assert_eq!(dest_buf, &[0, 1, 2]);

        let bytes_written = reader.read(dest_buf).await.unwrap();
        assert_eq!(bytes_written, 3, "bytes_written");
        assert_eq!(dest_buf, &[3, 4, 5,]);

        let bytes_written = reader.read(dest_buf).await.unwrap();
        assert_eq!(bytes_written, 3, "bytes_written");
        assert_eq!(dest_buf, &[6, 7, 8,]);

        let bytes_written = reader.read(dest_buf).await.unwrap();
        assert_eq!(bytes_written, 0, "bytes_written");
        assert_eq!(dest_buf, &[6, 7, 8,]);
    }

    #[tokio::test]
    async fn read_from_downloader() {
        let expected = vec![0, 1, 2, 3, 0, 0, 4, 5, 0, 6, 7];
        let downloader = TestDownloader::new_with_blob(expected.clone());

        let mut reader = downloader.blob().range(..).reader().await.unwrap();
        let mut buf = vec![0, 0, 0];
        reader.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, vec![0, 1, 2]);

        let mut reader = downloader.blob().range(1..=3).reader().await.unwrap();
        let mut buf = vec![0, 0, 0];
        reader.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, vec![1, 2, 3]);

        let mut reader = downloader.blob().range(1..4).reader().await.unwrap();
        let mut buf = vec![0, 0, 0];
        reader.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, vec![1, 2, 3]);

        let mut reader = downloader.blob().range(..).reader().await.unwrap();
        let mut buf = vec![];
        reader.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, vec![]);

        let mut reader = downloader.blob().range(6..=6).reader().await.unwrap();
        let mut buf = vec![0, 0, 0];
        let bytes_read = reader.read(&mut buf).await.unwrap();
        assert_eq!(buf, vec![4, 0, 0]);
        assert_eq!(bytes_read, 1);
    }

    #[tokio::test]
    async fn test_read_from_stream_chunk_larger_than_destination_buffer() {
        use futures::io::AsyncReadExt as _;
        // create stream
        let bytes_stream: Vec<Result<Bytes, CondowError>> = vec![
            Ok(vec![0_u8, 1, 2].into()),
            Ok(vec![3_u8, 4, 5].into()),
            Ok(vec![6_u8, 7, 8].into()),
        ];
        let bytes_stream = futures::stream::iter(bytes_stream.into_iter());
        let bytes_stream = BytesStream::new(bytes_stream, BytesHint::new_no_hint());
        let mut reader = BytesAsyncReader::new(bytes_stream);
        let dest_buf: &mut [u8; 2] = &mut [42; 2];

        let bytes_written = reader.read(dest_buf).await.unwrap();
        assert_eq!(bytes_written, 2, "bytes_written");
        assert_eq!(dest_buf, &[0, 1]);

        let bytes_written = reader.read(dest_buf).await.unwrap();
        assert_eq!(bytes_written, 1, "bytes_written");
        assert_eq!(dest_buf, &[2, 1]);

        let bytes_written = reader.read(dest_buf).await.unwrap();
        assert_eq!(bytes_written, 2, "bytes_written");
        assert_eq!(dest_buf, &[3, 4]);

        let bytes_written = reader.read(dest_buf).await.unwrap();
        assert_eq!(bytes_written, 1, "bytes_written");
        assert_eq!(dest_buf, &[5, 4]);

        let bytes_written = reader.read(dest_buf).await.unwrap();
        assert_eq!(bytes_written, 2, "bytes_written");
        assert_eq!(dest_buf, &[6, 7]);

        let bytes_written = reader.read(dest_buf).await.unwrap();
        assert_eq!(bytes_written, 1, "bytes_written");
        assert_eq!(dest_buf, &[8, 7]);

        let bytes_written = reader.read(dest_buf).await.unwrap();
        assert_eq!(bytes_written, 0, "bytes_written");
        assert_eq!(dest_buf, &[8, 7]);
    }

    #[tokio::test]
    async fn test_read_from_stream_destination_buffer_larger_than_stream_chunk() {
        use futures::io::AsyncReadExt as _;
        // create stream
        let bytes_stream: Vec<Result<Bytes, CondowError>> =
            vec![Ok(vec![0_u8, 1, 2].into()), Ok(vec![3_u8, 4, 5].into())];
        let bytes_stream = futures::stream::iter(bytes_stream.into_iter());
        let bytes_stream = BytesStream::new(bytes_stream, BytesHint::new_no_hint());
        let mut reader = BytesAsyncReader::new(bytes_stream);
        let dest_buf: &mut [u8; 4] = &mut [42; 4];

        let bytes_written = reader.read(dest_buf).await.unwrap();
        assert_eq!(bytes_written, 3, "bytes_written");
        assert_eq!(dest_buf, &[0, 1, 2, 42,]);

        let bytes_written = reader.read(dest_buf).await.unwrap();
        assert_eq!(bytes_written, 3, "bytes_written");
        assert_eq!(dest_buf, &[3, 4, 5, 42]);
    }

    #[tokio::test]
    async fn test_read_to_end() {
        use futures::io::AsyncReadExt as _;
        // create stream
        let bytes_stream: Vec<Result<Bytes, CondowError>> =
            vec![Ok(vec![0_u8, 1, 2].into()), Ok(vec![3_u8, 4, 5].into())];
        let bytes_stream = futures::stream::iter(bytes_stream.into_iter());
        let bytes_stream = BytesStream::new(bytes_stream, BytesHint::new_no_hint());
        let mut reader = BytesAsyncReader::new(bytes_stream);

        let mut buf = Vec::new();

        reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf, vec![0, 1, 2, 3, 4, 5]);
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
