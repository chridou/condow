//! Adapter for [crate::Condow] to access BLOBs to be downloaded
use std::ops::RangeInclusive;

use futures::future::BoxFuture;

use crate::{
    errors::CondowError,
    streams::{BytesHint, BytesStream},
    InclusiveRange,
};

pub use in_memory::InMemoryClient;

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
        config::{Config, Mebi},
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
        blob: Blob,
        chunk_size: usize,
        _location: PhantomData<L>,
    }

    impl<L> InMemoryClient<L> {
        /// Blob from owned bytes
        pub fn new(blob: Vec<u8>) -> Self {
            Self::new_shared(Arc::new(blob))
        }

        /// Blob from shared bytes
        pub fn new_shared(blob: Arc<Vec<u8>>) -> Self {
            Self {
                blob: Blob::Owned(blob),
                chunk_size: Mebi(4).value() as usize,
                _location: PhantomData,
            }
        }

        /// Blob copied from slice
        pub fn new_from_slice(blob: &[u8]) -> Self {
            Self::new(blob.to_vec())
        }

        /// Blob with static byte slice
        pub fn new_static(blob: &'static [u8]) -> Self {
            Self {
                blob: Blob::Static(blob),
                chunk_size: Mebi(4).value() as usize,
                _location: PhantomData,
            }
        }

        pub fn chunk_size(mut self, chunk_size: usize) -> Self {
            self.chunk_size = chunk_size;
            self
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
            download(&self.blob.as_slice(), self.chunk_size, spec)
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

    #[derive(Clone)]
    enum Blob {
        Static(&'static [u8]),
        Owned(Arc<Vec<u8>>),
    }

    impl Blob {
        pub fn len(&self) -> usize {
            match self {
                Blob::Static(b) => b.len(),
                Blob::Owned(b) => b.len(),
            }
        }

        pub fn as_slice(&self) -> &[u8] {
            match self {
                Blob::Static(b) => b,
                Blob::Owned(b) => &b,
            }
        }
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
    use std::{fmt::Display, marker::PhantomData, sync::Arc, vec};

    use bytes::Bytes;
    use futures::{future, lock::Mutex, task, FutureExt, Stream, StreamExt};

    use crate::{
        condow_client::{CondowClient, DownloadSpec},
        errors::{CondowError, IoError},
        streams::{BytesHint, BytesStream},
        InclusiveRange,
    };

    pub use super::NoLocation;

    /// A builder for a [FailingClientSimulator]
    pub struct FailingClientSimulatorBuilder {
        /// The BLOB that would be streamed on a successful request without any errors
        blob: Blob,
        /// Offsets at which an IO-Error occurs while streaming bytes
        response_player: ResponsePlayer,
        /// Size of the streamed chunks
        chunk_size: usize,
    }

    impl FailingClientSimulatorBuilder {
        /// Blob from owned bytes
        pub fn blob(mut self, blob: Vec<u8>) -> Self {
            self.blob = Blob::Owned(Arc::new(blob));
            self
        }

        /// Blob from shared bytes
        pub fn blob_arc(mut self, blob: Arc<Vec<u8>>) -> Self {
            self.blob = Blob::Owned(blob);
            self
        }

        /// Blob copied from slice
        pub fn blob_from_slice(self, blob: &[u8]) -> Self {
            self.blob(blob.to_vec())
        }

        /// Blob with static byte slice
        pub fn blob_static(mut self, blob: &'static [u8]) -> Self {
            self.blob = Blob::Static(blob);
            self
        }

        /// Set the given [ResponsePlayer]
        pub fn response_player(mut self, player: ResponsePlayer) -> Self {
            self.response_player = player;
            self
        }

        /// Add to the current response player with a [ResponsesBuilder]
        pub fn responses(self) -> ResponsesBuilder {
            ResponsesBuilder(self)
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

        /// Create the [FailingClientSimulator]
        pub fn finish(self) -> FailingClientSimulator {
            FailingClientSimulator::new(self.blob, self.response_player, self.chunk_size)
        }
    }

    impl Default for FailingClientSimulatorBuilder {
        fn default() -> Self {
            Self {
                blob: Blob::Static(&[]),
                response_player: Default::default(),
                chunk_size: 3,
            }
        }
    }

    /// Simulates a failing client.
    ///
    /// Has limited capabilities to simulate failure scenarios (mostly) for testing.
    ///
    /// `get_size` will always succeed.
    ///
    /// Clones will share the responses to be played back
    #[derive(Clone)]
    pub struct FailingClientSimulator<L = NoLocation> {
        blob: Blob,
        responses: Arc<Mutex<vec::IntoIter<ResponseBehaviour>>>,
        chunk_size: usize,
        _phantom: PhantomData<L>,
    }

    impl<L> FailingClientSimulator<L>
    where
        L: std::fmt::Debug + std::fmt::Display + Clone + Send + Sync + 'static,
    {
        /// Create a new instance
        fn new(blob: Blob, response_player: ResponsePlayer, chunk_size: usize) -> Self {
            Self {
                blob,
                responses: Arc::new(Mutex::new(response_player.into_iter())),
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
            let range_incl = match spec {
                DownloadSpec::Range(r) => r,
                DownloadSpec::Complete => InclusiveRange(0, (me.blob.len() - 1) as u64),
            };

            let bytes_hint = BytesHint::new_exact(range_incl.len());

            async move {
                let next_response = me
                    .responses
                    .lock()
                    .await
                    .next()
                    .unwrap_or(ResponseBehaviour::Success);

                match next_response {
                    ResponseBehaviour::Success => {
                        let stream = BytesStreamWithError {
                            blob: me.blob,
                            next: range_incl.start() as usize,
                            end_excl: range_incl.end_incl() as usize + 1,
                            error: None,
                            chunk_size: me.chunk_size,
                        };
                        Ok((stream.boxed(), bytes_hint))
                    }
                    ResponseBehaviour::SuccessWithFailungStream(error_offset) => {
                        let end_excl = error_offset.min(range_incl.end_incl() as usize + 1);
                        let stream = BytesStreamWithError {
                            blob: me.blob,
                            next: range_incl.start() as usize,
                            end_excl,
                            error: Some(IoError(error_offset.to_string())),
                            chunk_size: me.chunk_size,
                        };
                        Ok((stream.boxed(), bytes_hint))
                    }
                    ResponseBehaviour::Error(error) => Err(error),
                    ResponseBehaviour::Panic(msg) => {
                        panic!("{}", msg)
                    }
                }
            }
            .boxed()
        }
    }

    #[derive(Clone)]
    enum Blob {
        Static(&'static [u8]),
        Owned(Arc<Vec<u8>>),
    }

    impl Blob {
        pub fn len(&self) -> usize {
            match self {
                Blob::Static(b) => b.len(),
                Blob::Owned(b) => b.len(),
            }
        }

        pub fn as_slice(&self) -> &[u8] {
            match self {
                Blob::Static(b) => b,
                Blob::Owned(b) => &b,
            }
        }
    }

    /// A builder to add responses to a [FailingClientSimulator]
    ///
    /// This is simply a seperated an API for the [FailingClientSimulatorBuilder]
    pub struct ResponsesBuilder(FailingClientSimulatorBuilder);

    impl ResponsesBuilder {
        /// Add a successful response with a successful stream
        pub fn success(mut self) -> Self {
            self.0.response_player = self.0.response_player.success();
            self
        }

        /// Add multiple successful responses with successful streams
        pub fn successes(mut self, count: usize) -> Self {
            self.0.response_player = self.0.response_player.successes(count);
            self
        }

        /// Add a successful response with the stream failing at the given offset
        pub fn success_with_stream_failure(mut self, failure_offset: usize) -> Self {
            self.0.response_player = self
                .0
                .response_player
                .success_with_stream_failure(failure_offset);
            self
        }

        /// Add multiple successful responses with the streams each failing at a given offset
        pub fn successes_with_stream_failure<I>(mut self, failure_offsets: I) -> Self
        where
            I: IntoIterator<Item = usize>,
        {
            self.0.response_player = self
                .0
                .response_player
                .successes_with_stream_failure(failure_offsets);
            self
        }

        /// Add a single failing response
        pub fn failure<E: Into<CondowError>>(mut self, error: E) -> Self {
            self.0.response_player = self.0.response_player.failure(error);
            self
        }

        /// Add a chain of failing responses
        pub fn failures<I, E>(mut self, errors: I) -> Self
        where
            I: IntoIterator<Item = E>,
            E: Into<CondowError>,
        {
            self.0.response_player = self.0.response_player.failures(errors);
            self
        }

        /// Causes a panic once the request is made
        pub fn panic<M: Display + Send + 'static>(mut self, message: M) -> Self {
            self.0.response_player = self.0.response_player.panic(message);
            self
        }

        /// Causes a panic once the request is made
        ///
        /// The panic will contain a message containing the request number
        pub fn never(mut self) -> Self {
            self.0.response_player = self.0.response_player.never();
            self
        }

        /// Get back to the [FailingClientSimulatorBuilder]s API.
        pub fn done(self) -> FailingClientSimulatorBuilder {
            self.0
        }

        /// Create the [FailingClientSimulator]
        pub fn finish(self) -> FailingClientSimulator {
            self.0.finish()
        }
    }

    impl From<ResponsesBuilder> for FailingClientSimulatorBuilder {
        fn from(rb: ResponsesBuilder) -> Self {
            rb.0
        }
    }

    /// Plays back responses for each request made
    ///
    /// Responses are delivered in the ordering the requests are made.
    #[derive(Default)]
    pub struct ResponsePlayer {
        responses: Vec<ResponseBehaviour>,
        counter: usize,
    }

    impl ResponsePlayer {
        /// Add a successful response with a successful stream
        pub fn success(self) -> Self {
            self.successes(1)
        }

        /// Add multiple successful responses with successful streams
        pub fn successes(mut self, count: usize) -> Self {
            (0..count).for_each(|_| {
                self.counter += 1;
                self.responses.push(ResponseBehaviour::Success)
            });
            self
        }

        /// Add a successful response with the stream failing at the given offset
        pub fn success_with_stream_failure(self, failure_offset: usize) -> Self {
            self.successes_with_stream_failure([failure_offset])
        }

        /// Add multiple successful responses with the streams each failing at a given offset
        pub fn successes_with_stream_failure<I>(mut self, failure_offsets: I) -> Self
        where
            I: IntoIterator<Item = usize>,
        {
            failure_offsets.into_iter().for_each(|offset| {
                self.counter += 1;
                self.responses
                    .push(ResponseBehaviour::SuccessWithFailungStream(offset))
            });
            self
        }

        /// Add a single failing response
        pub fn failure<E: Into<CondowError>>(self, error: E) -> Self {
            self.failures([error])
        }

        /// Add a chain of failing responses
        pub fn failures<I, E>(mut self, errors: I) -> Self
        where
            I: IntoIterator<Item = E>,
            E: Into<CondowError>,
        {
            errors.into_iter().for_each(|e| {
                self.counter += 1;
                self.responses.push(ResponseBehaviour::Error(e.into()))
            });
            self
        }

        /// Causes a panic once the request is made
        pub fn panic<M: Display + Send + 'static>(mut self, message: M) -> Self {
            self.counter += 1;
            self.responses
                .push(ResponseBehaviour::Panic(Box::new(message)));
            self
        }

        /// Causes a panic once the request is made
        ///
        /// The panic will contain a message containing the request number
        pub fn never(mut self) -> Self {
            self.counter += 1;
            let message = format!("request {} should have never happened", self.counter);
            self.responses
                .push(ResponseBehaviour::Panic(Box::new(message)));
            self
        }
    }

    impl IntoIterator for ResponsePlayer {
        type Item = ResponseBehaviour;

        type IntoIter = vec::IntoIter<ResponseBehaviour>;

        fn into_iter(self) -> Self::IntoIter {
            self.responses.into_iter()
        }
    }

    /// The behaviour to respond to a request
    pub enum ResponseBehaviour {
        /// Respond with a success and also a non failing stream
        Success,
        /// Respond with a success but the stream will fail at the given offset
        SuccessWithFailungStream(usize),
        /// The response will be an error
        Error(CondowError),
        /// The request will cause a panic
        Panic(Box<dyn Display + Send + 'static>),
    }

    struct BytesStreamWithError {
        blob: Blob,
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
        #[should_panic(expected = "request 1 should have never happened")]
        async fn never_1() {
            let client = get_builder().responses().never().finish();
            let range = DownloadSpec::Complete;

            let _result = download(&client, range).await;
        }

        #[tokio::test]
        #[should_panic(expected = "request 2 should have never happened")]
        async fn never_2() {
            let client = get_builder().responses().success().never().finish();
            let range = DownloadSpec::Complete;

            let _result = download(&client, range).await.unwrap().unwrap();
            let _result = download(&client, range).await;
        }

        #[tokio::test]
        async fn failed_request_1() {
            let client = get_builder()
                .responses()
                .failure(CondowErrorKind::NotFound)
                .finish();

            let range = DownloadSpec::Complete;

            let result = download(&client, range).await.unwrap_err();

            assert_eq!(result.kind(), CondowErrorKind::NotFound);
        }

        #[tokio::test]
        async fn failed_request_2() {
            let client = get_builder()
                .responses()
                .failure(CondowErrorKind::InvalidRange)
                .failure(CondowErrorKind::NotFound)
                .finish();

            let range = DownloadSpec::Complete;

            let result = download(&client, range).await.unwrap_err();
            assert_eq!(result.kind(), CondowErrorKind::InvalidRange);
            let result = download(&client, range).await.unwrap_err();
            assert_eq!(result.kind(), CondowErrorKind::NotFound);
        }

        #[tokio::test]
        async fn failed_request_3() {
            let client = get_builder()
                .responses()
                .failures([CondowErrorKind::InvalidRange, CondowErrorKind::Io])
                .success()
                .failure(CondowErrorKind::NotFound)
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
                .responses()
                .success()
                .failure(CondowErrorKind::NotFound)
                .success()
                .failures([CondowErrorKind::InvalidRange, CondowErrorKind::Io])
                .success()
                .success()
                .failure(CondowErrorKind::Remote)
                .success()
                .never()
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
            let client = get_builder()
                .responses()
                .success_with_stream_failure(0)
                .finish();

            let range = DownloadSpec::Complete;

            let result = download(&client, range).await.unwrap().unwrap_err();
            assert_eq!(result, &[], "err");
            let result = download(&client, range).await.unwrap().unwrap();
            assert_eq!(result, BLOB, "ok");
        }

        #[tokio::test]
        async fn failed_stream_start_2() {
            let client = get_builder()
                .responses()
                .successes_with_stream_failure([0, 0])
                .finish();

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
            let client = get_builder()
                .responses()
                .successes_with_stream_failure([5])
                .finish();

            let range = DownloadSpec::Complete;

            let result = download(&client, range).await.unwrap().unwrap_err();
            assert_eq!(result, &BLOB[0..5], "err");
            let result = download(&client, range).await.unwrap().unwrap();
            assert_eq!(result, BLOB, "ok");
        }

        #[tokio::test]
        async fn failed_stream_2() {
            let client = get_builder()
                .responses()
                .successes_with_stream_failure([5, 10])
                .finish();

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
            let client = get_builder()
                .responses()
                .successes_with_stream_failure([5, 5, 5])
                .finish();

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
            let client = get_builder()
                .responses()
                .success_with_stream_failure(BLOB.len() - 1)
                .finish();

            let range = DownloadSpec::Complete;

            let result = download(&client, range).await.unwrap().unwrap_err();
            assert_eq!(result, &BLOB[0..BLOB.len() - 1], "err");
            let result = download(&client, range).await.unwrap().unwrap();
            assert_eq!(result, BLOB, "ok");
        }

        #[tokio::test]
        async fn failed_stream_end_2() {
            let client = get_builder()
                .responses()
                .success_with_stream_failure(BLOB.len())
                .finish();

            let range = DownloadSpec::Complete;

            let result = download(&client, range).await.unwrap().unwrap_err();
            assert_eq!(result, BLOB, "err");
            let result = download(&client, range).await.unwrap().unwrap();
            assert_eq!(result, BLOB, "ok");
        }

        #[tokio::test]
        async fn combined_errors() {
            let client = get_builder()
                .responses()
                .failure(CondowErrorKind::Io)
                .success_with_stream_failure(0)
                .failures([CondowErrorKind::Remote, CondowErrorKind::InvalidRange])
                .successes_with_stream_failure([5, 9])
                .success()
                .never()
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
                .responses()
                .failure(CondowErrorKind::Io)
                .success_with_stream_failure(0)
                .failures([CondowErrorKind::Remote, CondowErrorKind::InvalidRange])
                .successes_with_stream_failure([5, 9])
                .success()
                .never()
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
                .blob_static(BLOB)
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
                blob: Blob::Static(BLOB),
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
