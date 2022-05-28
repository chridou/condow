use std::iter;

use anyhow::Error as AnyError;

use bytes::Bytes;
use futures::{future::BoxFuture, stream, FutureExt, StreamExt};

use condow_core::{
    condow_client::{CondowClient, IgnoreLocation},
    config::Config,
    errors::CondowError,
    streams::{BytesHint, BytesStream},
    Condow, InclusiveRange,
};

/// A client for benchmarking which returns zero bytes only.
#[derive(Clone)]
pub struct BenchmarkClient {
    chunk_size: u64,
    size: u64,
}

impl BenchmarkClient {
    pub fn new(size: u64, chunk_size: u64) -> Self {
        BenchmarkClient { chunk_size, size }
    }

    #[cfg(test)]
    pub fn new_always_one_chunk(size: u64) -> Self {
        BenchmarkClient::new(size, size)
    }
}

impl BenchmarkClient {
    pub fn condow(&self, config: Config) -> Result<Condow<Self, ()>, AnyError> {
        Condow::new(self.clone(), config)
    }
}

impl CondowClient for BenchmarkClient {
    type Location = IgnoreLocation;

    fn get_size(&self, _location: Self::Location) -> BoxFuture<'static, Result<u64, CondowError>> {
        futures::future::ready(Ok(self.size)).boxed()
    }

    fn download(
        &self,
        _location: Self::Location,
        range: InclusiveRange,
    ) -> BoxFuture<'static, Result<BytesStream, CondowError>> {
        let bytes_to_send = {
            if range.end_incl() >= self.size {
                return futures::future::err(CondowError::new_other("out of range")).boxed();
            } else {
                range.len()
            }
        };

        let chunk_size = self.chunk_size;
        let mut bytes_left_to_send = bytes_to_send;
        let iter = iter::from_fn(move || {
            if bytes_left_to_send == 0 {
                None
            } else if chunk_size > bytes_left_to_send {
                let bytes = Bytes::from_iter((0..bytes_left_to_send).map(|_| 0u8));

                bytes_left_to_send = 0;

                Some(Ok(bytes))
            } else {
                let bytes = Bytes::from_iter((0..chunk_size).map(|_| 0u8));

                bytes_left_to_send -= chunk_size;

                Some(Ok(bytes))
            }
        });

        let mut start = 0;
        let stream = stream::iter(iter).then(move |chunk| async move {
            start += 1;
            if start % 3 == 0 {
                // Cause some Poll::Pending
                tokio::task::yield_now().await;
            }

            chunk
        });
        //       let stream = stream::iter(iter);
        let stream = BytesStream::new(stream, BytesHint::new_exact(bytes_to_send));

        futures::future::ok(stream).boxed()
    }
}

#[cfg(test)]
mod test {
    use condow_core::{config::Mebi, ClosedRange};

    use super::BenchmarkClient;

    #[tokio::test]
    async fn full_range_one_chunk_one_mebi() {
        let client = BenchmarkClient::new_always_one_chunk(Mebi(1).into());
        let condow = client.condow(Default::default()).unwrap();

        let bytes_downloaded = condow
            .blob()
            .download_chunks_unordered()
            .await
            .unwrap()
            .count_bytes()
            .await
            .unwrap();

        assert_eq!(bytes_downloaded, Mebi(1).value())
    }

    #[tokio::test]
    async fn open_range_one_chunk_one_mebi() {
        let client = BenchmarkClient::new_always_one_chunk(Mebi(1).into());
        let condow = client.condow(Default::default()).unwrap();

        let bytes_downloaded = condow
            .blob()
            .range(0..)
            .download_chunks_unordered()
            .await
            .unwrap()
            .count_bytes()
            .await
            .unwrap();

        assert_eq!(bytes_downloaded, Mebi(1).value())
    }

    #[tokio::test]
    async fn closed_range_one_chunk_one_mebi() {
        let client = BenchmarkClient::new_always_one_chunk(Mebi(1).into());
        let condow = client.condow(Default::default()).unwrap();

        let range: ClosedRange = (45u64..934_123).into();
        let bytes_downloaded = condow
            .blob()
            .range(range)
            .download_chunks_unordered()
            .await
            .unwrap()
            .count_bytes()
            .await
            .unwrap();

        assert_eq!(bytes_downloaded, range.len())
    }

    #[tokio::test]
    async fn full_range_many_chunks_one_mebi() {
        let client = BenchmarkClient::new(Mebi(1).into(), 1667); // prime number
        let condow = client.condow(Default::default()).unwrap();

        let bytes_downloaded = condow
            .blob()
            .download_chunks_unordered()
            .await
            .unwrap()
            .count_bytes()
            .await
            .unwrap();

        assert_eq!(bytes_downloaded, Mebi(1).value())
    }

    #[tokio::test]
    async fn open_range_many_chunks_one_mebi() {
        let client = BenchmarkClient::new(Mebi(1).into(), 1667); // prime number
        let condow = client.condow(Default::default()).unwrap();

        let bytes_downloaded = condow
            .blob()
            .range(0..)
            .download_chunks_unordered()
            .await
            .unwrap()
            .count_bytes()
            .await
            .unwrap();

        assert_eq!(bytes_downloaded, Mebi(1).value())
    }

    #[tokio::test]
    async fn closed_range_many_chunks_one_mebi() {
        let client = BenchmarkClient::new(Mebi(1).into(), 1667); // prime number
        let condow = client.condow(Default::default()).unwrap();

        let range: ClosedRange = (45u64..934_123).into();
        let bytes_downloaded = condow
            .blob()
            .range(range)
            .download_chunks_unordered()
            .await
            .unwrap()
            .count_bytes()
            .await
            .unwrap();

        assert_eq!(bytes_downloaded, range.len())
    }
}
