use std::{fmt, sync::Arc, time::Duration};

use bytes::Bytes;
use futures::{
    future::{self, BoxFuture},
    stream, StreamExt as _,
};
use rand::{rngs::OsRng, thread_rng, Rng};
use tokio::time;

use crate::{condow_client::CondowClient, streams::BytesStream, DownloadRange};

#[derive(Clone)]
pub struct TestCondowClient {
    data: Arc<Vec<u8>>,
    max_jitter_us: usize,
    include_size_hit: bool,
    max_chunk_size: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct DummyLocation;

impl fmt::Display for DummyLocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "location")
    }
}

impl CondowClient for TestCondowClient {
    type Location = DummyLocation;

    fn get_size(
        &self,
        _location: Self::Location,
    ) -> BoxFuture<'static, Result<usize, crate::condow_client::GetSizeError>> {
        let f = future::ready(Ok(self.data.len()));
        Box::pin(f)
    }

    fn download(
        &self,
        _location: Self::Location,
        range: DownloadRange,
    ) -> BoxFuture<
        'static,
        Result<
            (crate::streams::BytesStream, crate::streams::TotalBytesHint),
            crate::condow_client::ClientDownloadError,
        >,
    > {
        let range = match range.boundaries_excl() {
            None => 0..0,
            Some((a, None)) => a..self.data.len(),
            Some((a, Some(b))) => a..b,
        };

        let slice = &self.data[range];

        let bytes_returned = slice.len();

        let iter = slice
            .chunks(self.max_chunk_size)
            .map(Bytes::copy_from_slice)
            .map(Ok);

        let owned_bytes: Vec<_> = iter.collect();

        let jitter = self.max_jitter_us;
        let stream = stream::iter(owned_bytes).then(move |bytes| async move {
            if jitter > 0 {
                let jitter = OsRng.gen_range(0..=(jitter as u64));
                time::sleep(Duration::from_micros(jitter)).await;
            }
            bytes
        });

        let stream: BytesStream = Box::pin(stream);

        let f = future::ready(Ok((stream, Some(bytes_returned))));

        Box::pin(f)
    }
}
