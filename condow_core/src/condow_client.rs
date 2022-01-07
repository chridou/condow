//! Adapter for [crate::Condow] to access BLOBs to be downloaded
use std::ops::RangeInclusive;

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

    /// Returns the position of the first byte to be fetched
    pub fn start(&self) -> u64 {
        match self {
            DownloadSpec::Complete => 0,
            DownloadSpec::Range(r) => r.start(),
        }
    }
}

impl From<InclusiveRange> for DownloadSpec {
    fn from(r: InclusiveRange) -> Self {
        Self::Range(r)
    }
}

impl From<RangeInclusive<u64>> for DownloadSpec {
    fn from(r: RangeInclusive<u64>) -> Self {
        Self::Range(r.into())
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

/// A location usable for testing.
///
/// The clients in this module do not support downloading BLOBs from
/// different locations.
#[derive(Debug, Clone, Copy)]
pub struct NoLocation;

impl std::fmt::Display for NoLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<no location>")
    }
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

    use super::{CondowClient, DownloadSpec, NoLocation};

    /// Holds the BLOB in memory as owned data.
    ///
    /// Use for testing.
    #[derive(Clone)]
    pub struct InMemoryClient<L = NoLocation> {
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
                panic!("'chunk_size' may not be 0");
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
    pub struct StaticBlobClient<L = NoLocation> {
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
                blob.len() - 1,
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

    #[cfg(test)]
    mod test {
        use futures::{pin_mut, StreamExt};

        use crate::{
            condow_client::DownloadSpec, errors::CondowError, streams::BytesHint, InclusiveRange,
        };

        const BLOB: &[u8] = b"abcdefghijklmnopqrstuvwxyz";

        async fn download_to_vec(
            blob: &[u8],
            chunk_size: usize,
            spec: DownloadSpec,
        ) -> Result<(Vec<u8>, BytesHint), CondowError> {
            let (stream, bytes_hint) = super::download(blob, chunk_size, spec).await?;

            let mut buf = Vec::with_capacity(bytes_hint.lower_bound() as usize);
            pin_mut!(stream);
            while let Some(next) = stream.next().await {
                let bytes = next?;
                buf.extend_from_slice(bytes.as_ref())
            }
            Ok((buf, bytes_hint))
        }

        #[tokio::test]
        async fn download_all() {
            for chunk_size in 1..30 {
                let (bytes, bytes_hint) = download_to_vec(BLOB, chunk_size, DownloadSpec::Complete)
                    .await
                    .unwrap();

                assert_eq!(&bytes, BLOB);
                assert_eq!(bytes_hint, BytesHint::new_exact(bytes.len() as u64));
            }
        }

        #[tokio::test]
        async fn download_range_begin() {
            for chunk_size in 1..30 {
                let range = InclusiveRange(0, 9);
                let (bytes, bytes_hint) =
                    download_to_vec(BLOB, chunk_size, DownloadSpec::Range(range))
                        .await
                        .unwrap();

                let expected = b"abcdefghij";

                assert_eq!(&bytes, expected);
                assert_eq!(bytes_hint, BytesHint::new_exact(expected.len() as u64));
            }
        }

        #[tokio::test]
        async fn download_range_middle() {
            for chunk_size in 1..30 {
                let range = InclusiveRange(10, 19);
                let (bytes, bytes_hint) =
                    download_to_vec(BLOB, chunk_size, DownloadSpec::Range(range))
                        .await
                        .unwrap();

                let expected = b"klmnopqrst";

                assert_eq!(&bytes, expected);
                assert_eq!(bytes_hint, BytesHint::new_exact(expected.len() as u64));
            }
        }

        #[tokio::test]
        async fn download_range_end() {
            for chunk_size in 1..30 {
                let range = InclusiveRange(16, 25);
                let (bytes, bytes_hint) =
                    download_to_vec(BLOB, chunk_size, DownloadSpec::Range(range))
                        .await
                        .unwrap();

                let expected = b"qrstuvwxyz";

                assert_eq!(&bytes, expected);
                assert_eq!(bytes_hint, BytesHint::new_exact(expected.len() as u64));
            }
        }
    }
}

pub mod failing_client_simulator {
    //! Simulate failing requests and streams
    use std::{marker::PhantomData, sync::Arc};

    use bytes::Bytes;
    use futures::{future, lock::Mutex, task, FutureExt, Stream, StreamExt};

    use crate::{
        condow_client::{CondowClient, DownloadSpec},
        errors::{CondowError, IoError},
        streams::{BytesHint, BytesStream},
        InclusiveRange,
    };

    use super::NoLocation;

    /// A builder for a [FailingClientSimulator]
    pub struct FailingClientSimulatorBuilder {
        /// The BLOB that would be streamed on a successful request without any errors
        pub blob: Arc<Vec<u8>>,
        /// Offsets at which an IO-Error occurs while streaming bytes
        pub stream_errors_at: Vec<usize>,
        /// Chains of errors on complete requests
        pub request_error_chains: Vec<Vec<CondowError>>,
        /// Size of the streamed chunks
        pub chunk_size: usize,
    }

    impl FailingClientSimulatorBuilder {
        /// Start with shared bytes
        pub fn blob_arc(mut self, blob: Arc<Vec<u8>>) -> Self {
            self.blob = blob;
            self
        }

        /// Start with owned bytes
        pub fn blob(mut self, blob: Vec<u8>) -> Self {
            self.blob = Arc::new(blob);
            self
        }

        /// Start with an owned copy of the given bytes
        pub fn blob_from_slice(mut self, blob: &[u8]) -> Self {
            self.blob = Arc::new(blob.to_vec());
            self
        }

        /// Set the chunk size
        ///
        /// # Panics
        ///
        /// If `chunk_size` is 0.
        pub fn chunk_size(mut self, chunk_size: usize) -> Self {
            if chunk_size == 0 {
                panic!("chun size must be greater than 0")
            }

            self.chunk_size = chunk_size;
            self
        }

        /// Add a single successful request
        ///
        /// After an error or error chain this will add another successful request
        /// since there is always a successful request after errors.
        ///
        /// Use also to add successful requests before the first errors occur.
        pub fn successful_request(mut self) -> Self {
            self.request_error_chains.push(vec![]);
            self
        }

        /// Add a single failed request which will be followed by a successful request
        pub fn failed_request<E: Into<CondowError>>(mut self, error: E) -> Self {
            self.request_error_chains.push(vec![error.into()]);
            self
        }

        /// Add a chain of failing requests which will be followed by a successful request
        pub fn failed_request_chain<I, E>(mut self, errors: I) -> Self
        where
            I: IntoIterator<Item = E>,
            E: Into<CondowError>,
        {
            let chain = errors.into_iter().map(|e| e.into()).collect();
            self.request_error_chains.push(chain);
            self
        }

        /// Add an offset at which streaming will fail
        pub fn next_stream_error_at(mut self, at_byte_idx: usize) -> Self {
            self.stream_errors_at.push(at_byte_idx);

            self
        }

        /// Add multiple offsets at which streaming will fail
        pub fn next_stream_errors_at<T>(self, at_byte_idxs: T) -> Self
        where
            T: IntoIterator<Item = usize>,
        {
            let iter = at_byte_idxs.into_iter();

            iter.fold(self, |agg, next| agg.next_stream_error_at(next))
        }

        pub fn finish(self) -> FailingClientSimulator {
            FailingClientSimulator::new(
                self.blob,
                self.stream_errors_at,
                self.request_error_chains,
                self.chunk_size,
            )
        }
    }

    impl Default for FailingClientSimulatorBuilder {
        fn default() -> Self {
            Self {
                blob: Default::default(),
                stream_errors_at: Default::default(),
                request_error_chains: Default::default(),
                chunk_size: 3,
            }
        }
    }

    /// Simulates a failing client.
    ///
    /// Has limited capabilities to simulate failure scenarios for testing.
    ///
    /// On a request level single errors or chains of errors can be defined.
    /// After failing with a single error or a chain of errors there will always
    /// be a successful request which means that streaming will be initiated.
    ///
    /// Once a stream is obtained the stream will fail at givvn offsets.
    /// Streaming will fail as many times as there are offsets defined.
    ///
    /// `get_size` will always succeed.
    #[derive(Clone)]
    pub struct FailingClientSimulator<L = NoLocation> {
        blob: Arc<Vec<u8>>,
        stream_error_at_idx_rev: Arc<Mutex<Vec<usize>>>,
        request_error_chains_rev: Arc<Mutex<Vec<Vec<CondowError>>>>,
        chunk_size: usize,
        _phantom: PhantomData<L>,
    }

    impl<L> FailingClientSimulator<L>
    where
        L: std::fmt::Debug + std::fmt::Display + Clone + Send + Sync + 'static,
    {
        /// Create a new instance
        ///
        /// There is a [FailingClientSimulatorBuilder] for easier configuration
        pub fn new(
            blob: Arc<Vec<u8>>,
            mut stream_errors_at: Vec<usize>,
            mut request_error_chains: Vec<Vec<CondowError>>,
            chunk_size: usize,
        ) -> Self {
            stream_errors_at.reverse();
            request_error_chains.reverse();
            request_error_chains
                .iter_mut()
                .for_each(|chain| chain.reverse());

            Self {
                blob,
                stream_error_at_idx_rev: Arc::new(Mutex::new(stream_errors_at)),
                request_error_chains_rev: Arc::new(Mutex::new(request_error_chains)),
                chunk_size,
                _phantom: PhantomData,
            }
        }
    }

    impl<L> CondowClient for FailingClientSimulator<L>
    where
        L: std::fmt::Debug + std::fmt::Display + Clone + Send + Sync + 'static,
    {
        type Location = L;

        fn get_size(
            &self,
            _location: Self::Location,
        ) -> futures::future::BoxFuture<'static, Result<u64, CondowError>> {
            future::ready(Ok(self.blob.len() as u64)).boxed()
        }

        fn download(
            &self,
            _location: Self::Location,
            spec: DownloadSpec,
        ) -> futures::future::BoxFuture<'static, Result<(BytesStream, BytesHint), CondowError>>
        {
            let me = self.clone();
            async move {
                let mut request_failures = me.request_error_chains_rev.lock().await;
                if let Some(mut next_err_chain) = request_failures.pop() {
                    if let Some(err) = next_err_chain.pop() {
                        request_failures.push(next_err_chain);
                        return Err(err);
                    }
                }
                drop(request_failures);

                let range_incl = match spec {
                    DownloadSpec::Range(r) => r,
                    DownloadSpec::Complete => InclusiveRange(0, (me.blob.len() - 1) as u64),
                };

                let bytes_hint = BytesHint::new_exact(range_incl.len());

                let stream = if let Some(stream_err_at_idx) =
                    me.stream_error_at_idx_rev.lock().await.pop()
                {
                    let end_excl = stream_err_at_idx.min(range_incl.end_incl() as usize + 1);
                    BytesStreamWithError {
                        blob: Arc::clone(&me.blob),
                        next: range_incl.start() as usize,
                        end_excl,
                        error: Some(IoError(stream_err_at_idx.to_string())),
                        chunk_size: me.chunk_size,
                    }
                } else {
                    BytesStreamWithError {
                        blob: Arc::clone(&me.blob),
                        next: range_incl.start() as usize,
                        end_excl: range_incl.end_incl() as usize + 1,
                        error: None,
                        chunk_size: me.chunk_size,
                    }
                };

                Ok((stream.boxed(), bytes_hint))
            }
            .boxed()
        }
    }

    struct BytesStreamWithError {
        blob: Arc<Vec<u8>>,
        next: usize,
        end_excl: usize,
        error: Option<IoError>,
        chunk_size: usize,
    }

    impl Stream for BytesStreamWithError {
        type Item = Result<Bytes, IoError>;

        fn poll_next(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> task::Poll<Option<Self::Item>> {
            if self.next == self.end_excl {
                if let Some(err) = self.error.take() {
                    return task::Poll::Ready(Some(Err(err)));
                } else {
                    return task::Poll::Ready(None);
                }
            }

            let chunk_size = self.chunk_size.min(self.end_excl - self.next);
            let start = self.next;
            self.next += chunk_size;
            let slice: &[u8] = &self.blob.as_slice()[start..self.next];
            let bytes = Bytes::copy_from_slice(slice);

            task::Poll::Ready(Some(Ok(bytes)))
        }
    }

    #[cfg(test)]
    mod test_client {
        use crate::errors::CondowErrorKind;

        use super::*;

        const BLOB: &[u8] = &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];

        #[tokio::test]
        async fn all_ok() {
            let client = get_builder().finish();
            let range = DownloadSpec::Complete;

            let result = download(&client, range).await.unwrap().unwrap();

            assert_eq!(result, BLOB);
        }

        #[tokio::test]
        async fn failed_request_1() {
            let client = get_builder()
                .failed_request(CondowErrorKind::NotFound)
                .finish();

            let range = DownloadSpec::Complete;

            let result = download(&client, range).await.unwrap_err();

            assert_eq!(result.kind(), CondowErrorKind::NotFound);
        }

        #[tokio::test]
        async fn failed_request_2() {
            let client = get_builder()
                .failed_request(CondowErrorKind::InvalidRange)
                .failed_request(CondowErrorKind::NotFound)
                .finish();

            let range = DownloadSpec::Complete;

            let result = download(&client, range).await.unwrap_err();
            assert_eq!(result.kind(), CondowErrorKind::InvalidRange);
            let result = download(&client, range).await.unwrap().unwrap();
            assert_eq!(result, BLOB);
            let result = download(&client, range).await.unwrap_err();
            assert_eq!(result.kind(), CondowErrorKind::NotFound);
        }

        #[tokio::test]
        async fn failed_request_3() {
            let client = get_builder()
                .failed_request_chain([CondowErrorKind::InvalidRange, CondowErrorKind::Io])
                .failed_request(CondowErrorKind::NotFound)
                .finish();

            let range = DownloadSpec::Complete;

            let result = download(&client, range).await.unwrap_err();
            assert_eq!(result.kind(), CondowErrorKind::InvalidRange);
            let result = download(&client, range).await.unwrap_err();
            assert_eq!(result.kind(), CondowErrorKind::Io);
            let result = download(&client, range).await.unwrap().unwrap();
            assert_eq!(result, BLOB);
            let result = download(&client, range).await.unwrap_err();
            assert_eq!(result.kind(), CondowErrorKind::NotFound);
        }

        #[tokio::test]
        async fn fail_and_success() {
            let client = get_builder()
                .successful_request()
                .failed_request(CondowErrorKind::NotFound)
                .failed_request_chain([CondowErrorKind::InvalidRange, CondowErrorKind::Io])
                .successful_request()
                .failed_request(CondowErrorKind::Remote)
                .finish();

            let range = DownloadSpec::Complete;

            let result = download(&client, range).await.unwrap().unwrap();
            assert_eq!(result, BLOB, "1");
            let result = download(&client, range).await.unwrap_err();
            assert_eq!(result.kind(), CondowErrorKind::NotFound, "2");
            let result = download(&client, range).await.unwrap().unwrap();
            assert_eq!(result, BLOB, "3");
            let result = download(&client, range).await.unwrap_err();
            assert_eq!(result.kind(), CondowErrorKind::InvalidRange, "4");
            let result = download(&client, range).await.unwrap_err();
            assert_eq!(result.kind(), CondowErrorKind::Io, "5");
            let result = download(&client, range).await.unwrap().unwrap();
            assert_eq!(result, BLOB, "6");
            let result = download(&client, range).await.unwrap().unwrap();
            assert_eq!(result, BLOB, "7");
            let result = download(&client, range).await.unwrap_err();
            assert_eq!(result.kind(), CondowErrorKind::Remote, "8");
            let result = download(&client, range).await.unwrap().unwrap();
            assert_eq!(result, BLOB, "9");
        }

        #[tokio::test]
        async fn failed_stream_start_1() {
            let client = get_builder().next_stream_error_at(0).finish();

            let range = DownloadSpec::Complete;

            let result = download(&client, range).await.unwrap().unwrap_err();
            assert_eq!(result, &[], "err");
            let result = download(&client, range).await.unwrap().unwrap();
            assert_eq!(result, BLOB, "ok");
        }

        #[tokio::test]
        async fn failed_stream_start_2() {
            let client = get_builder().next_stream_errors_at([0, 0]).finish();

            let range = DownloadSpec::Complete;

            let result = download(&client, range).await.unwrap().unwrap_err();
            assert_eq!(result, &[], "err");
            let result = download(&client, range).await.unwrap().unwrap_err();
            assert_eq!(result, &[], "err");
            let result = download(&client, range).await.unwrap().unwrap();
            assert_eq!(result, BLOB, "ok");
        }

        #[tokio::test]
        async fn failed_stream_1() {
            let client = get_builder().next_stream_errors_at([5]).finish();

            let range = DownloadSpec::Complete;

            let result = download(&client, range).await.unwrap().unwrap_err();
            assert_eq!(result, &BLOB[0..5], "err");
            let result = download(&client, range).await.unwrap().unwrap();
            assert_eq!(result, BLOB, "ok");
        }

        #[tokio::test]
        async fn failed_stream_2() {
            let client = get_builder().next_stream_errors_at([5, 10]).finish();

            let range = DownloadSpec::Complete;

            let result = download(&client, range).await.unwrap().unwrap_err();
            assert_eq!(result, &BLOB[0..5], "err");
            let result = download(&client, range).await.unwrap().unwrap_err();
            assert_eq!(result, &BLOB[0..10], "err");
            let result = download(&client, range).await.unwrap().unwrap();
            assert_eq!(result, BLOB, "ok");
        }

        #[tokio::test]
        async fn failed_stream_3() {
            let client = get_builder().next_stream_errors_at([5, 5, 5]).finish();

            let range = DownloadSpec::Complete;

            let result = download(&client, range).await.unwrap().unwrap_err();
            assert_eq!(result, &BLOB[0..5], "err");
            let result = download(&client, range).await.unwrap().unwrap_err();
            assert_eq!(result, &BLOB[0..5], "err");
            let result = download(&client, range).await.unwrap().unwrap_err();
            assert_eq!(result, &BLOB[0..5], "err");
            let result = download(&client, range).await.unwrap().unwrap();
            assert_eq!(result, BLOB, "ok");
        }

        #[tokio::test]
        async fn failed_stream_end_1() {
            let client = get_builder().next_stream_error_at(BLOB.len() - 1).finish();

            let range = DownloadSpec::Complete;

            let result = download(&client, range).await.unwrap().unwrap_err();
            assert_eq!(result, &BLOB[0..BLOB.len() - 1], "err");
            let result = download(&client, range).await.unwrap().unwrap();
            assert_eq!(result, BLOB, "ok");
        }

        #[tokio::test]
        async fn failed_stream_end_2() {
            let client = get_builder().next_stream_error_at(BLOB.len()).finish();

            let range = DownloadSpec::Complete;

            let result = download(&client, range).await.unwrap().unwrap_err();
            assert_eq!(result, BLOB, "err");
            let result = download(&client, range).await.unwrap().unwrap();
            assert_eq!(result, BLOB, "ok");
        }

        #[tokio::test]
        async fn combined_errors() {
            let client = get_builder()
                .next_stream_errors_at([0, 5, 9])
                .failed_request(CondowErrorKind::Io)
                .failed_request_chain([CondowErrorKind::Remote, CondowErrorKind::InvalidRange])
                .finish();
            let range = DownloadSpec::Complete;

            let result = download(&client, range).await.unwrap_err();
            assert_eq!(result.kind(), CondowErrorKind::Io, "1");
            let result = download(&client, range).await.unwrap().unwrap_err();
            assert_eq!(result, &[], "2");
            let result = download(&client, range).await.unwrap_err();
            assert_eq!(result.kind(), CondowErrorKind::Remote, "3");
            let result = download(&client, range).await.unwrap_err();
            assert_eq!(result.kind(), CondowErrorKind::InvalidRange, "4");
            let result = download(&client, range).await.unwrap().unwrap_err();
            assert_eq!(result, &BLOB[0..5], "5");
            let result = download(&client, range).await.unwrap().unwrap_err();
            assert_eq!(result, &BLOB[0..9], "6");

            let result = download(&client, range).await.unwrap().unwrap();
            assert_eq!(result, BLOB, "ok");
        }

        #[tokio::test]
        async fn combined_errors_with_range() {
            let client = get_builder()
                .next_stream_errors_at([0, 5, 9])
                .failed_request(CondowErrorKind::Io)
                .failed_request_chain([CondowErrorKind::Remote, CondowErrorKind::InvalidRange])
                .finish();

            let result = download(&client, DownloadSpec::Complete).await.unwrap_err();
            assert_eq!(result.kind(), CondowErrorKind::Io, "1");
            let result = download(&client, DownloadSpec::Complete)
                .await
                .unwrap()
                .unwrap_err();
            assert_eq!(result, &[], "2");
            let result = download(&client, DownloadSpec::Complete).await.unwrap_err();
            assert_eq!(result.kind(), CondowErrorKind::Remote, "3");
            let result = download(&client, DownloadSpec::Complete).await.unwrap_err();
            assert_eq!(result.kind(), CondowErrorKind::InvalidRange, "4");
            let result = download(&client, 2..=9).await.unwrap().unwrap_err();
            assert_eq!(result, &BLOB[2..5], "5");
            let result = download(&client, 5..=50).await.unwrap().unwrap_err();
            assert_eq!(result, &BLOB[5..9], "6");

            let result = download(&client, 3..=8).await.unwrap().unwrap();
            assert_eq!(result, &BLOB[3..=8], "ok");
        }

        fn get_builder() -> FailingClientSimulatorBuilder {
            FailingClientSimulatorBuilder::default()
                .blob_from_slice(BLOB)
                .chunk_size(3)
        }

        async fn download<R: Into<DownloadSpec>>(
            client: &FailingClientSimulator,
            range: R,
        ) -> Result<Result<Vec<u8>, Vec<u8>>, CondowError> {
            let (mut stream, _bytes_hint) = client.download(NoLocation, range.into()).await?;

            let mut received = Vec::new();

            while let Some(next) = stream.next().await {
                if let Ok(bytes) = next {
                    received.extend_from_slice(&bytes);
                } else {
                    return Ok(Err(received));
                }
            }

            Ok(Ok(received))
        }
    }

    #[cfg(test)]
    mod test_stream {
        use std::ops::Range;

        use super::*;

        const BLOB: &[u8] = &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9];

        #[tokio::test]
        async fn empty_ok() {
            for start in 0..BLOB.len() {
                for chunk_size in 1..BLOB.len() + 1 {
                    let result = consume(start..start, chunk_size, false).await.unwrap();
                    assert!(
                        result.is_empty(),
                        "chunk_size: {}, start: {}",
                        chunk_size,
                        start
                    )
                }
            }
        }

        #[tokio::test]
        async fn empty_err() {
            for start in 0..BLOB.len() {
                for chunk_size in 1..BLOB.len() + 1 {
                    let result = consume(start..start, chunk_size, true).await.unwrap_err();
                    assert!(
                        result.is_empty(),
                        "chunk_size: {}, start: {}",
                        chunk_size,
                        start
                    )
                }
            }
        }

        #[tokio::test]
        async fn to_end_ok() {
            for start in 0..BLOB.len() {
                for chunk_size in 1..BLOB.len() + 1 {
                    let result = consume(start..BLOB.len(), chunk_size, false).await.unwrap();
                    assert_eq!(
                        result,
                        BLOB[start..BLOB.len()],
                        "chunk_size: {}, start: {}",
                        chunk_size,
                        start
                    )
                }
            }
        }

        #[tokio::test]
        async fn to_end_err() {
            for start in 0..BLOB.len() {
                for chunk_size in 1..BLOB.len() + 1 {
                    let result = consume(start..BLOB.len(), chunk_size, true)
                        .await
                        .unwrap_err();
                    assert_eq!(
                        result,
                        BLOB[start..BLOB.len()],
                        "chunk_size: {}, start: {}",
                        chunk_size,
                        start
                    )
                }
            }
        }

        #[tokio::test]
        async fn from_start_ok() {
            for end in 0..BLOB.len() {
                for chunk_size in 1..BLOB.len() + 1 {
                    let result = consume(0..end, chunk_size, false).await.unwrap();
                    assert_eq!(
                        result,
                        BLOB[0..end],
                        "chunk_size: {}, end: {}",
                        chunk_size,
                        end
                    )
                }
            }
        }

        #[tokio::test]
        async fn from_start_err() {
            for end in 0..BLOB.len() {
                for chunk_size in 1..BLOB.len() + 1 {
                    let result = consume(0..end, chunk_size, true).await.unwrap_err();
                    assert_eq!(
                        result,
                        BLOB[0..end],
                        "chunk_size: {}, end: {}",
                        chunk_size,
                        end
                    )
                }
            }
        }

        #[tokio::test]
        async fn get_a_slice_ok() {
            let start = 3;
            let end_excl = 7;
            for chunk_size in 1..BLOB.len() + 1 {
                let result = consume(start..end_excl, chunk_size, false).await.unwrap();
                assert_eq!(
                    result,
                    BLOB[start..end_excl],
                    "chunk_size: {}, start: {}, end_excl: {}",
                    chunk_size,
                    start,
                    end_excl,
                )
            }
        }

        #[tokio::test]
        async fn get_a_slice_err() {
            let start = 3;
            let end_excl = 7;
            for chunk_size in 1..BLOB.len() + 1 {
                let result = consume(start..end_excl, chunk_size, true)
                    .await
                    .unwrap_err();
                assert_eq!(
                    result,
                    BLOB[start..end_excl],
                    "chunk_size: {}, start: {}, end_excl: {}",
                    chunk_size,
                    start,
                    end_excl,
                )
            }
        }

        async fn consume(
            range: Range<usize>,
            chunk_size: usize,
            err: bool,
        ) -> Result<Vec<u8>, Vec<u8>> {
            let mut stream = BytesStreamWithError {
                blob: Arc::new(BLOB.to_vec()),
                next: range.start,
                end_excl: range.end,
                error: if err {
                    Some(IoError("bang!".to_string()))
                } else {
                    None
                },
                chunk_size,
            };

            let mut collected = Vec::new();
            while let Some(next) = stream.next().await {
                match next {
                    Ok(bytes) => collected.extend_from_slice(&bytes),
                    Err(_err) => return Err(collected),
                }
            }

            Ok(collected)
        }
    }
}
