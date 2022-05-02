use std::iter;

use anyhow::Error as AnyError;

use bytes::Bytes;
use futures::{future::BoxFuture, stream, FutureExt, StreamExt};

use condow_core::{
    condow_client::{CondowClient, DownloadSpec, IgnoreLocation},
    config::Config,
    errors::CondowError,
    streams::{BytesHint, BytesStream},
    Condow,
};

/// A client for benchmarking which returns zero bytes only.
#[derive(Clone)]
pub struct BenchmarkClient {
    chunk_size: u64,
    size: u64,
}

impl BenchmarkClient {
    pub fn new(chunk_size: u64, size: u64) -> Self {
        BenchmarkClient { chunk_size, size }
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
        spec: DownloadSpec,
    ) -> BoxFuture<'static, Result<(BytesStream, BytesHint), CondowError>> {
        let bytes_to_send = match spec {
            DownloadSpec::Complete => self.size,
            DownloadSpec::Range(r) => {
                if r.end_incl() >= self.size {
                    return futures::future::err(CondowError::new_other("out of range")).boxed();
                } else {
                    r.len()
                }
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

        let stream = Box::pin(stream::iter(iter)).boxed();

        futures::future::ok((stream, BytesHint::new_exact(bytes_to_send))).boxed()
    }
}
