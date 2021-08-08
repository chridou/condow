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
    DownloadRange, ExclusiveOpenRange,
};

#[derive(Clone)]
pub struct TestCondowClient {
    pub data: Arc<Vec<u8>>,
    pub max_jitter_ms: usize,
    pub include_size_hint: bool,
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
        let range = match range.excl_open_range() {
            None => 0..0,
            Some(ExclusiveOpenRange(a, None)) => a..self.data.len(),
            Some(ExclusiveOpenRange(a, Some(b))) => a..b,
        };

        let slice = &self.data[range];

        let bytes_hint = if self.include_size_hint {
            BytesHint::new(slice.len(), Some(slice.len()))
        } else {
            BytesHint::no_hint()
        };

        let iter = slice
            .chunks(self.max_chunk_size)
            .map(Bytes::copy_from_slice)
            .map(Ok);

        let owned_bytes: Vec<_> = iter.collect();

        let jitter = self.max_jitter_ms;
        let stream = stream::iter(owned_bytes).then(move |bytes| async move {
            if jitter > 0 {
                let jitter = OsRng.gen_range(0..=(jitter as u64));
                time::sleep(Duration::from_millis(jitter)).await;
            }
            bytes
        });

        let stream: BytesStream = Box::pin(stream);

        let f = future::ready(Ok((stream, bytes_hint)));

        Box::pin(f)
    }
}
