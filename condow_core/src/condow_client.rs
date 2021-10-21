//! Adapter for [crate::Condow] to access BLOBs to be downloaded
use futures::future::BoxFuture;

use crate::{
    errors::CondowError,
    streams::{BytesHint, BytesStream},
    InclusiveRange,
};

pub use in_memory::{InMemoryClient, StaticBlobClient};

/// Specifies whether a whole BLOB or part of it should be downloaded
#[derive(Debug, Copy, Clone)]
pub enum DownloadSpec {
    /// Download the complete BLOB
    Complete,
    /// Download part of the BLOB given by an [InclusiveRange]
    Range(InclusiveRange),
}

impl DownloadSpec {
    /// Returns a value for an  `HTTP-Range` header with bytes as the unit
    /// if the variant is [DownloadSpec::Range]
    pub fn http_range_value(&self) -> Option<String> {
        match self {
            DownloadSpec::Complete => None,
            DownloadSpec::Range(r) => Some(r.http_range_value()),
        }
    }
}

/// A client to some service or other resource which supports
/// partial downloads
///
/// This is an adapter trait
pub trait CondowClient: Clone + Send + Sync + 'static {
    type Location: std::fmt::Debug + std::fmt::Display + Clone + Send + Sync + 'static;

    /// Returns the size of the BLOB at the given location
    fn get_size(&self, location: Self::Location) -> BoxFuture<'static, Result<u64, CondowError>>;

    /// Download a BLOB or part of a BLOB from the given location as specified by the [DownloadSpec]
    ///
    /// A valid [BytesHint] must be returned alongside the stream.
    /// A concurrent download will fail if the [BytesHint] does not match
    /// the number of bytes requested by a [DownloadSpec::Range].
    fn download(
        &self,
        location: Self::Location,
        spec: DownloadSpec,
    ) -> BoxFuture<'static, Result<(BytesStream, BytesHint), CondowError>>;
}

mod in_memory {
    use std::{marker::PhantomData, sync::Arc};

    use crate::{
        config::Config,
        errors::CondowError,
        streams::{BytesHint, BytesStream},
        Condow,
    };
    use anyhow::Error as AnyError;
    use bytes::Bytes;
    use futures::{
        future::{self, BoxFuture, FutureExt},
        stream,
    };

    use super::{CondowClient, DownloadSpec};

    /// Holds the BLOB in memory as owned data.
    ///
    /// Use for testing.
    #[derive(Clone)]
    pub struct InMemoryClient<L = ()> {
        blob: Arc<Vec<u8>>,
        chunk_size: usize,
        _location: PhantomData<L>,
    }

    impl<L> InMemoryClient<L> {
        pub fn new(blob: Vec<u8>) -> Self {
            Self::new_with_chunk_size(blob, 4 * 1024)
        }

        pub fn new_with_chunk_size(blob: Vec<u8>, chunk_size: usize) -> Self {
            if chunk_size == 0 {
                panic!("'chunk_size' ay not be 0");
            }

            Self {
                blob: Arc::new(blob),
                chunk_size,
                _location: PhantomData,
            }
        }
    }

    impl<L> InMemoryClient<L>
    where
        L: std::fmt::Debug + std::fmt::Display + Clone + Send + Sync + 'static,
    {
        pub fn condow(&self, config: Config) -> Result<Condow<Self>, AnyError> {
            Condow::new(self.clone(), config)
        }
    }

    impl<L> CondowClient for InMemoryClient<L>
    where
        L: std::fmt::Debug + std::fmt::Display + Clone + Send + Sync + 'static,
    {
        type Location = L;

        fn get_size(
            &self,
            _location: Self::Location,
        ) -> BoxFuture<'static, Result<u64, CondowError>> {
            futures::future::ready(Ok(self.blob.len() as u64)).boxed()
        }

        fn download(
            &self,
            _location: Self::Location,
            spec: DownloadSpec,
        ) -> BoxFuture<'static, Result<(BytesStream, BytesHint), CondowError>> {
            download(&self.blob, self.chunk_size, spec)
        }
    }

    /// References a BLOB of data with a static lifetime.
    ///
    /// Use for testing especially with `include_bytes!`
    #[derive(Clone)]
    pub struct StaticBlobClient<L = ()> {
        blob: &'static [u8],
        chunk_size: usize,
        _location: PhantomData<L>,
    }

    impl<L> StaticBlobClient<L> {
        pub fn new(blob: &'static [u8]) -> Self {
            Self::new_with_chunk_size(blob, 4 * 1024)
        }

        pub fn new_with_chunk_size(blob: &'static [u8], chunk_size: usize) -> Self {
            if chunk_size == 0 {
                panic!("'chunk_size' may not be 0");
            }

            Self {
                blob,
                chunk_size,
                _location: PhantomData,
            }
        }
    }

    impl<L> StaticBlobClient<L>
    where
        L: std::fmt::Debug + std::fmt::Display + Clone + Send + Sync + 'static,
    {
        pub fn condow(&self, config: Config) -> Result<Condow<Self>, AnyError> {
            Condow::new(self.clone(), config)
        }
    }

    impl<L> CondowClient for StaticBlobClient<L>
    where
        L: std::fmt::Debug + std::fmt::Display + Clone + Send + Sync + 'static,
    {
        type Location = L;

        fn get_size(
            &self,
            _location: Self::Location,
        ) -> BoxFuture<'static, Result<u64, CondowError>> {
            futures::future::ready(Ok(self.blob.len() as u64)).boxed()
        }

        fn download(
            &self,
            _location: Self::Location,
            spec: DownloadSpec,
        ) -> BoxFuture<'static, Result<(BytesStream, BytesHint), CondowError>> {
            download(self.blob, self.chunk_size, spec)
        }
    }

    fn download(
        blob: &[u8],
        chunk_size: usize,
        spec: DownloadSpec,
    ) -> BoxFuture<'static, Result<(BytesStream, BytesHint), CondowError>> {
        let range = match spec {
            DownloadSpec::Complete => 0..blob.len(),
            DownloadSpec::Range(r) => {
                let r = r.to_std_range_excl();
                r.start as usize..r.end as usize
            }
        };

        if range.end > blob.len() {
            return Box::pin(future::ready(Err(CondowError::new_invalid_range(format!(
                "max upper bound is {} but {} was requested",
                blob.len(),
                range.end - 1
            )))));
        }

        let slice = &blob[range];

        let bytes_hint = BytesHint::new_exact(slice.len() as u64);

        let iter = slice.chunks(chunk_size).map(Bytes::copy_from_slice).map(Ok);

        let owned_bytes: Vec<_> = iter.collect();

        let stream = stream::iter(owned_bytes);

        let stream: BytesStream = Box::pin(stream);

        let f = future::ready(Ok((stream, bytes_hint)));

        Box::pin(f)
    }
}
