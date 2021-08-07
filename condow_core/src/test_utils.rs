use std::{fmt, sync::Arc, time::Duration};

use bytes::Bytes;
use futures::{
    future::{self, BoxFuture},
    stream, StreamExt as _,
};
use rand::{rngs::OsRng, thread_rng, Rng};
use tokio::time;

use crate::{
    condow_client::CondowClient,
    streams::{BytesHint, BytesStream},
    DownloadRange,
};

#[derive(Clone)]
pub struct TestCondowClient {
    pub data: Arc<Vec<u8>>,
    pub max_jitter_us: usize,
    pub include_size_hit: bool,
    pub max_chunk_size: usize,
}

impl CondowClient for TestCondowClient {
    type Location = ();

    fn get_size(
        &self,
        _location: Self::Location,
    ) -> BoxFuture<'static, Result<usize, crate::errors::GetSizeError>> {
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
            (crate::streams::BytesStream, crate::streams::BytesHint),
            crate::errors::DownloadRangeError,
        >,
    > {
        let range = match range.boundaries_excl() {
            None => 0..0,
            Some((a, None)) => a..self.data.len(),
            Some((a, Some(b))) => a..b,
        };

        let slice = &self.data[range];

        let bytes_hint = BytesHint::new(slice.len(), Some(slice.len()));

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

        let f = future::ready(Ok((stream, bytes_hint)));

        Box::pin(f)
    }
}
