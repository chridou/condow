use std::{fmt, sync::Arc};

use bytes::Bytes;
use futures::{FutureExt, future::{self,BoxFuture}};

use crate::{DownloadRange, condow_client::CondowClient};

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

    fn get_size(&self, _location: Self::Location)
        -> BoxFuture<'static, Result<usize, crate::condow_client::GetSizeError>> {
            let f = future::ready(Ok(self.data.len()));
       Box::pin(f)
    }

    fn download(
        &self,
        _location: Self::Location,
        range: DownloadRange,
    ) -> BoxFuture<'static, Result<(crate::streams::BytesStream, crate::streams::TotalBytesHint), crate::condow_client::ClientDownloadError>> {
        let range: DownloadRange = range.into();


        (start_inclusive, end_inclusive) = range.into()
        
        let range = &self.data[start_inclusive..=end_inclusive];

        let iter = range.chunks(self.max_chunk_size).map(Bytes::copy_from_slice);

        
        todo!()
    }
}