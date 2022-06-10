//! Adapter for [crate::Condow] to access BLOBs to be downloaded
//!
//! There are also implementation of a client mostly for testing
//!
//! * [InMemoryClient]: A client which keeps data in memory and never fails
//! * [failing_client_simulator]: A module containing a client with data kept in memory
//! which can fail and cause panics.
use std::{convert::Infallible, str::FromStr};

use futures::{future::BoxFuture, FutureExt};

use crate::{errors::CondowError, InclusiveRange};

pub use client_bytes_stream::ClientBytesStream;
pub use in_memory::InMemoryClient;

/// A client to some service or other resource which supports
/// partial downloads
///
/// This is an adapter trait
///
/// Implementors of this trait may not panic on calling any of the methods nor
/// within any of the futures returned.
///
/// It is suggested that `FromStr` is implemented for `Location` as well as a [From] implentation
/// for [IgnoreLocation].
pub trait CondowClient: Clone + Send + Sync + 'static {
    type Location: std::fmt::Debug + std::fmt::Display + Clone + Send + Sync + 'static;

    /// Returns the size of the BLOB at the given location
    fn get_size(&self, location: Self::Location) -> BoxFuture<'static, Result<u64, CondowError>>;

    /// Download a BLOB or part of a BLOB from the given location as specified by the [InclusiveRange]
    fn download(
        &self,
        location: Self::Location,
        range: InclusiveRange,
    ) -> BoxFuture<'static, Result<ClientBytesStream, CondowError>>;

    /// Download a complete BLOB
    fn download_full(
        &self,
        location: Self::Location,
    ) -> BoxFuture<'static, Result<ClientBytesStream, CondowError>> {
        let me = self.clone();
        async move {
            let len = me.get_size(location.clone()).await?;
            me.download(location, InclusiveRange(0, len - 1)).await
        }
        .boxed()
    }
}

/// A location usable for testing.
#[derive(Debug, Clone, Copy)]
pub struct IgnoreLocation;

impl std::fmt::Display for IgnoreLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<no location>")
    }
}

impl FromStr for IgnoreLocation {
    type Err = Infallible;

    fn from_str(_: &str) -> Result<Self, Self::Err> {
        Ok(IgnoreLocation)
    }
}

impl<T> From<T> for IgnoreLocation
where
    T: AsRef<str>,
{
    fn from(_: T) -> Self {
        IgnoreLocation
    }
}

mod client_bytes_stream {
    use std::{
        io, iter,
        task::{Context, Poll},
    };

    use bytes::Bytes;
    use futures::{
        channel::mpsc as futures_mpsc,
        stream::{self, BoxStream},
        Stream, StreamExt, TryStreamExt,
    };
    use pin_project_lite::pin_project;
    use tokio::sync::mpsc as tokio_mpsc;

    use crate::{
        errors::CondowError,
        streams::{BytesHint, BytesStreamItem},
    };

    pin_project! {
    /// A stream of [Bytes] (chunks) where there can be an error for each chunk of bytes.
    ///
    /// Implementors of [CondowClient] return this stream.
    ///
    /// This stream is NOT fused and depends on the input stream.
    ///
    /// [CondowClient]:crate::condow_client::CondowClient
    pub struct ClientBytesStream {
        #[pin]
        source: SourceFlavour,
        exact_bytes_left: u64,
    }
    }

    impl ClientBytesStream {
        pub fn new<St>(stream: St, exact_bytes_left: u64) -> Self
        where
            St: Stream<Item = BytesStreamItem> + Send + 'static,
        {
            Self {
                source: SourceFlavour::DynStream {
                    stream: stream.boxed(),
                },
                exact_bytes_left,
            }
        }

        pub fn new_io<St>(stream: St, exact_bytes_left: u64) -> Self
        where
            St: Stream<Item = Result<Bytes, io::Error>> + Send + 'static,
        {
            Self {
                source: SourceFlavour::IoDynStream {
                    stream: stream.boxed(),
                },
                exact_bytes_left,
            }
        }

        pub fn new_io_dyn(
            stream: BoxStream<'static, Result<Bytes, io::Error>>,
            exact_bytes_left: u64,
        ) -> Self {
            Self {
                source: SourceFlavour::IoDynStream { stream },
                exact_bytes_left,
            }
        }

        pub fn new_futures_receiver(
            receiver: futures_mpsc::UnboundedReceiver<BytesStreamItem>,
            exact_bytes_left: u64,
        ) -> Self {
            Self {
                source: SourceFlavour::FuturesChannel { receiver },
                exact_bytes_left,
            }
        }

        pub fn new_tokio_receiver(
            receiver: tokio_mpsc::UnboundedReceiver<BytesStreamItem>,
            exact_bytes_left: u64,
        ) -> Self {
            Self {
                source: SourceFlavour::TokioChannel { receiver },
                exact_bytes_left,
            }
        }

        pub fn empty() -> Self {
            Self {
                source: SourceFlavour::Empty,
                exact_bytes_left: 0,
            }
        }

        pub fn once(item: BytesStreamItem) -> Self {
            match item {
                Ok(bytes) => {
                    let exact_bytes_left = bytes.len() as u64;
                    Self::new(stream::iter(iter::once(Ok(bytes))), exact_bytes_left)
                }
                Err(err) => Self::new(stream::iter(iter::once(Err(err))), 0),
            }
        }

        pub fn once_ok(bytes: Bytes) -> Self {
            Self::once(Ok(bytes))
        }

        pub fn once_err(error: CondowError) -> Self {
            Self::once(Err(error))
        }

        pub fn into_io_stream(self) -> impl Stream<Item = Result<Bytes, io::Error>> {
            self.map_err(From::from)
        }

        pub fn bytes_hint(&self) -> BytesHint {
            BytesHint::new_exact(self.exact_bytes_left)
        }

        pub fn exact_bytes_left(&self) -> u64 {
            self.exact_bytes_left
        }
    }

    impl Stream for ClientBytesStream {
        type Item = BytesStreamItem;
        fn poll_next(
            self: std::pin::Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Option<Self::Item>> {
            let mut this = self.project();

            match this.source.as_mut().poll_next(cx) {
                Poll::Ready(Some(next)) => match next {
                    Ok(bytes) => {
                        *this.exact_bytes_left -= bytes.len() as u64;
                        Poll::Ready(Some(Ok(bytes)))
                    }
                    Err(err) => {
                        *this.exact_bytes_left = 0;
                        Poll::Ready(Some(Err(err)))
                    }
                },
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            }
        }
    }

    pin_project! {
        #[project = SourceFlavourProj]
        enum SourceFlavour {
            DynStream{#[pin] stream: BoxStream<'static, BytesStreamItem>},
            IoDynStream{#[pin] stream: BoxStream<'static, Result<Bytes, io::Error>>},
            TokioChannel{#[pin] receiver: tokio_mpsc::UnboundedReceiver<BytesStreamItem>},
            FuturesChannel{#[pin] receiver: futures_mpsc::UnboundedReceiver<BytesStreamItem>},
            Empty,
        }
    }

    impl Stream for SourceFlavour {
        type Item = BytesStreamItem;

        #[inline]
        fn poll_next(
            self: std::pin::Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Option<Self::Item>> {
            let this = self.project();

            match this {
                SourceFlavourProj::DynStream { mut stream } => stream.as_mut().poll_next(cx),
                SourceFlavourProj::IoDynStream { mut stream } => {
                    match stream.as_mut().poll_next(cx) {
                        Poll::Ready(Some(Ok(bytes))) => Poll::Ready(Some(Ok(bytes))),
                        Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err.into()))),
                        Poll::Ready(None) => Poll::Ready(None),
                        Poll::Pending => Poll::Pending,
                    }
                }
                SourceFlavourProj::TokioChannel { mut receiver } => receiver.poll_recv(cx),
                SourceFlavourProj::FuturesChannel { receiver } => receiver.poll_next(cx),
                SourceFlavourProj::Empty => Poll::Ready(None),
            }
        }
    }
}

mod in_memory {
    use std::{marker::PhantomData, sync::Arc};

    use crate::{
        config::{Config, Mebi},
        errors::CondowError,
        Condow, InclusiveRange,
    };
    use anyhow::Error as AnyError;
    use bytes::Bytes;
    use futures::{
        future::{self, BoxFuture, FutureExt},
        stream,
    };
    use tracing::trace;

    use super::{ClientBytesStream, CondowClient, IgnoreLocation};

    /// Holds the BLOB in memory as owned or static data.
    ///
    /// Use for testing.
    #[derive(Clone)]
    pub struct InMemoryClient<L = IgnoreLocation> {
        blob: Blob,
        chunk_size: usize,
        _location: PhantomData<L>,
    }

    impl<L> InMemoryClient<L> {
        /// BLOB from owned bytes
        pub fn new(blob: Vec<u8>) -> Self {
            Self::new_shared(Arc::new(blob))
        }

        /// BLOB from shared bytes
        pub fn new_shared(blob: Arc<Vec<u8>>) -> Self {
            Self {
                blob: Blob::Owned(blob),
                chunk_size: Mebi(4).value() as usize,
                _location: PhantomData,
            }
        }

        /// BLOB copied from slice
        pub fn new_from_slice(blob: &[u8]) -> Self {
            Self::new(blob.to_vec())
        }

        /// BLOB with static byte slice
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
        pub fn condow(&self, config: Config) -> Result<Condow<Self, ()>, AnyError> {
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
            trace!("in-memory-client: get_size");

            futures::future::ready(Ok(self.blob.len() as u64)).boxed()
        }

        fn download(
            &self,
            _location: Self::Location,
            range: InclusiveRange,
        ) -> BoxFuture<'static, Result<ClientBytesStream, CondowError>> {
            trace!("in-memory-client: download");

            download(self.blob.as_slice(), self.chunk_size, range)
        }
    }

    fn download(
        blob: &[u8],
        chunk_size: usize,
        range: InclusiveRange,
    ) -> BoxFuture<'static, Result<ClientBytesStream, CondowError>> {
        let range = {
            let r = range.to_std_range_excl();
            r.start as usize..r.end as usize
        };

        if range.end > blob.len() {
            return Box::pin(future::ready(Err(CondowError::new_invalid_range(format!(
                "max upper bound is {} but {} was requested",
                blob.len() - 1,
                range.end - 1
            )))));
        }

        let slice = &blob[range];

        let exact_bytes = slice.len() as u64;

        let iter = slice.chunks(chunk_size).map(Bytes::copy_from_slice).map(Ok);

        let owned_bytes: Vec<_> = iter.collect();

        let stream = stream::iter(owned_bytes);

        let stream = ClientBytesStream::new(stream, exact_bytes);

        let f = future::ready(Ok(stream));

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
                Blob::Owned(b) => b,
            }
        }
    }

    #[cfg(test)]
    mod test {
        use futures::{pin_mut, StreamExt};

        use crate::{errors::CondowError, streams::BytesHint, InclusiveRange};

        const BLOB: &[u8] = b"abcdefghijklmnopqrstuvwxyz";

        async fn download_to_vec(
            blob: &[u8],
            chunk_size: usize,
            range: InclusiveRange,
        ) -> Result<(Vec<u8>, BytesHint), CondowError> {
            let stream = super::download(blob, chunk_size, range).await?;

            let bytes_hint = stream.bytes_hint();
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
                let (bytes, bytes_hint) =
                    download_to_vec(BLOB, chunk_size, (0..=BLOB.len() as u64 - 1).into())
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
                let (bytes, bytes_hint) = download_to_vec(BLOB, chunk_size, range).await.unwrap();

                let expected = b"abcdefghij";

                assert_eq!(&bytes, expected);
                assert_eq!(bytes_hint, BytesHint::new_exact(expected.len() as u64));
            }
        }

        #[tokio::test]
        async fn download_range_middle() {
            for chunk_size in 1..30 {
                let range = InclusiveRange(10, 19);
                let (bytes, bytes_hint) = download_to_vec(BLOB, chunk_size, range).await.unwrap();

                let expected = b"klmnopqrst";

                assert_eq!(&bytes, expected);
                assert_eq!(bytes_hint, BytesHint::new_exact(expected.len() as u64));
            }
        }

        #[tokio::test]
        async fn download_range_end() {
            for chunk_size in 1..30 {
                let range = InclusiveRange(16, 25);
                let (bytes, bytes_hint) = download_to_vec(BLOB, chunk_size, range).await.unwrap();

                let expected = b"qrstuvwxyz";

                assert_eq!(&bytes, expected);
                assert_eq!(bytes_hint, BytesHint::new_exact(expected.len() as u64));
            }
        }
    }
}

pub mod failing_client_simulator {
    //! Simulate failing requests and streams
    use bytes::Bytes;
    use futures::{future, lock::Mutex, task, FutureExt, Stream};
    use std::{fmt::Display, marker::PhantomData, sync::Arc, vec};
    use tracing::trace;

    use crate::{
        condow_client::CondowClient, config::Config, errors::CondowError, streams::BytesStreamItem,
        Condow, InclusiveRange,
    };

    pub use super::{ClientBytesStream, IgnoreLocation};

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
                panic!("chunk size must be greater than 0")
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
    pub struct FailingClientSimulator<L = IgnoreLocation> {
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

        pub fn condow(&self, config: Config) -> Result<Condow<Self>, anyhow::Error> {
            Condow::new(self.clone(), config)
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
            trace!("failing-client-simulator: get_size");
            future::ready(Ok(self.blob.len() as u64)).boxed()
        }

        fn download(
            &self,
            _location: Self::Location,
            range: InclusiveRange,
        ) -> futures::future::BoxFuture<'static, Result<ClientBytesStream, CondowError>> {
            trace!("failing-client-simulator: download");
            let me = self.clone();

            if range.end_incl() >= me.blob.len() as u64 {
                let msg = format!(
                    "end of range incl. {} is behind slice end (len = {})",
                    range,
                    me.blob.len()
                );
                return futures::future::ready(Err(CondowError::new(
                    &msg,
                    crate::errors::CondowErrorKind::InvalidRange,
                )))
                .boxed();
            }

            let exact_bytes_left = range.len();

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
                            next: range.start() as usize,
                            end_excl: range.end_incl() as usize + 1,
                            error: None,
                            chunk_size: me.chunk_size,
                        };
                        Ok(ClientBytesStream::new(stream, exact_bytes_left))
                    }
                    ResponseBehaviour::SuccessWithFailungStream(error_offset) => {
                        let start = range.start() as usize;
                        let end_excl = (start + error_offset).min(range.end_incl() as usize + 1);
                        if start > end_excl {
                            panic!(
                                "start ({}) > end_excl ({}) with range {:?} and error offset {}",
                                start, end_excl, range, error_offset
                            );
                        }

                        let stream = BytesStreamWithError {
                            blob: me.blob,
                            next: start,
                            end_excl,
                            error: Some(ErrorAction::Err(format!(
                                "stream error at {}",
                                error_offset
                            ))),
                            chunk_size: me.chunk_size,
                        };
                        Ok(ClientBytesStream::new(stream, exact_bytes_left))
                    }
                    ResponseBehaviour::Error(error) => Err(error),
                    ResponseBehaviour::Panic(msg) => {
                        panic!("{}", msg)
                    }
                    ResponseBehaviour::SuccessWithStreamPanic(panic_offset) => {
                        let start = range.start() as usize;
                        let end_excl = (start + panic_offset).min(range.end_incl() as usize + 1);
                        if start > end_excl {
                            panic!(
                                "start ({}) > end_excl ({}) with range {:?} and error offset {}",
                                start, end_excl, range, panic_offset
                            );
                        }

                        let stream = BytesStreamWithError {
                            blob: me.blob,
                            next: start,
                            end_excl,
                            error: Some(ErrorAction::Panic(format!(
                                "panic at byte {} of range {}",
                                panic_offset, range
                            ))),
                            chunk_size: me.chunk_size,
                        };
                        Ok(ClientBytesStream::new(stream, exact_bytes_left))
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
                Blob::Owned(b) => b,
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
        ///
        /// The failure will occur at the given offset of the queried range
        pub fn success_with_stream_failure(mut self, failure_offset: usize) -> Self {
            self.0.response_player = self
                .0
                .response_player
                .success_with_stream_failure(failure_offset);
            self
        }

        /// Add multiple successful responses with the streams each failing at a given offset
        ///
        /// The failures will occur at the given offsets of the queried ranges
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

        /// Add a successful response with the stream panicking at the given offset
        ///
        /// The panic will occur at the given offset of the queried range
        pub fn success_with_stream_panic(mut self, panic_offset: usize) -> Self {
            self.0.response_player = self
                .0
                .response_player
                .success_with_stream_panic(panic_offset);
            self
        }

        /// Add multiple successful responses with the streams each panicking at a given offset
        ///
        /// The panics will occur at the given offsets of the queried ranges
        pub fn successes_with_stream_panic<I>(mut self, panic_offsets: I) -> Self
        where
            I: IntoIterator<Item = usize>,
        {
            self.0.response_player = self
                .0
                .response_player
                .successes_with_stream_panic(panic_offsets);
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
        ///
        /// The failure will occur at the given offset of the queried range
        pub fn success_with_stream_failure(self, failure_offset: usize) -> Self {
            self.successes_with_stream_failure([failure_offset])
        }

        /// Add multiple successful responses with the streams each failing at a given offset
        ///
        /// The failures will occur at the given offsets of the queried ranges
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

        /// Add a successful response with the stream panicking at the given offset
        ///
        /// The panics will occur at the given offset of the queried range
        pub fn success_with_stream_panic(self, panic_offset: usize) -> Self {
            self.successes_with_stream_panic([panic_offset])
        }

        /// Add multiple successful responses with the streams each panicking at a given offset
        ///
        /// The panics will occur at the given offsets of the queried ranges
        pub fn successes_with_stream_panic<I>(mut self, panic_offset: I) -> Self
        where
            I: IntoIterator<Item = usize>,
        {
            panic_offset.into_iter().for_each(|offset| {
                self.counter += 1;
                self.responses
                    .push(ResponseBehaviour::SuccessWithStreamPanic(offset))
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
        ///
        /// A stream is defined by the queried range.
        SuccessWithFailungStream(usize),
        /// The response will be an error
        Error(CondowError),
        /// The request will cause a panic
        Panic(Box<dyn Display + Send + 'static>),
        /// Respond with a success but the stream will panic at the given offset
        ///
        /// A stream is defined by the queried range.
        SuccessWithStreamPanic(usize),
    }

    pub enum ErrorAction {
        Err(String),
        Panic(String),
    }

    struct BytesStreamWithError {
        blob: Blob,
        next: usize,
        end_excl: usize,
        error: Option<ErrorAction>,
        chunk_size: usize,
    }

    impl Stream for BytesStreamWithError {
        type Item = BytesStreamItem;

        fn poll_next(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> task::Poll<Option<Self::Item>> {
            if self.next == self.end_excl || self.chunk_size == 0 {
                if let Some(error_action) = self.error.take() {
                    match error_action {
                        ErrorAction::Err(msg) => {
                            let err = CondowError::new_io(msg);
                            return task::Poll::Ready(Some(Err(err)));
                        }
                        ErrorAction::Panic(msg) => panic!("{}", msg),
                    }
                } else {
                    return task::Poll::Ready(None);
                }
            }

            if self.end_excl < self.next {
                panic!(
                    "invalid state in BytesStreamWithError! end_excl ({}) < next ({})",
                    self.end_excl, self.next
                );
            }

            let effective_chunk_size = self.chunk_size.min(self.end_excl - self.next);
            let start = self.next;
            self.next += effective_chunk_size;
            let slice: &[u8] = &self.blob.as_slice()[start..self.next];
            let bytes = Bytes::copy_from_slice(slice);

            task::Poll::Ready(Some(Ok(bytes)))
        }
    }

    #[cfg(test)]
    mod test_client {
        use futures::StreamExt;

        use crate::errors::CondowErrorKind;

        use super::*;

        const BLOB: &[u8] = &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];

        #[tokio::test]
        async fn all_ok() {
            let client = get_builder().finish();
            let range = InclusiveRange(0u64, BLOB.len() as u64 - 1);

            let result = download(&client, range).await.unwrap().unwrap();

            assert_eq!(result, BLOB);
        }

        #[tokio::test]
        #[should_panic(expected = "request 1 should have never happened")]
        async fn never_1() {
            let client = get_builder().responses().never().finish();
            let range = InclusiveRange(0u64, BLOB.len() as u64 - 1);

            let _result = download(&client, range).await;
        }

        #[tokio::test]
        #[should_panic(expected = "request 2 should have never happened")]
        async fn never_2() {
            let client = get_builder().responses().success().never().finish();
            let range = InclusiveRange(0u64, BLOB.len() as u64 - 1);

            let _result = download(&client, range).await.unwrap().unwrap();
            let _result = download(&client, range).await;
        }

        #[tokio::test]
        async fn failed_request_1() {
            let client = get_builder()
                .responses()
                .failure(CondowErrorKind::NotFound)
                .finish();

            let range = InclusiveRange(0u64, BLOB.len() as u64 - 1);

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

            let range = InclusiveRange(0u64, BLOB.len() as u64 - 1);

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

            let range = InclusiveRange(0u64, BLOB.len() as u64 - 1);

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

            let range = InclusiveRange(0u64, BLOB.len() as u64 - 1);

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

            let range = InclusiveRange(0u64, BLOB.len() as u64 - 1);

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

            let range = InclusiveRange(0u64, BLOB.len() as u64 - 1);

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

            let range = InclusiveRange(0u64, BLOB.len() as u64 - 1);

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

            let range = InclusiveRange(0u64, BLOB.len() as u64 - 1);

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

            let range = InclusiveRange(0u64, BLOB.len() as u64 - 1);

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

            let range = InclusiveRange(0u64, BLOB.len() as u64 - 1);

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

            let range = InclusiveRange(0u64, BLOB.len() as u64 - 1);

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
            let range = InclusiveRange(0u64, BLOB.len() as u64 - 1);

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
                .successes_with_stream_failure([3, 4])
                .success()
                .never()
                .finish();

            let result = download(&client, InclusiveRange(0u64, BLOB.len() as u64 - 1))
                .await
                .unwrap_err();
            assert_eq!(result.kind(), CondowErrorKind::Io, "1");
            let result = download(&client, InclusiveRange(0u64, BLOB.len() as u64 - 1))
                .await
                .unwrap()
                .unwrap_err();
            assert_eq!(result, &[], "2");
            let result = download(&client, InclusiveRange(0u64, BLOB.len() as u64 - 1))
                .await
                .unwrap_err();
            assert_eq!(result.kind(), CondowErrorKind::Remote, "3");
            let result = download(&client, InclusiveRange(0u64, BLOB.len() as u64 - 1))
                .await
                .unwrap_err();
            assert_eq!(result.kind(), CondowErrorKind::InvalidRange, "4");
            let result = download(&client, 2..=9).await.unwrap().unwrap_err();
            assert_eq!(result, &BLOB[2..5], "5");
            let result = download(&client, 5..=BLOB.len() as u64 - 1)
                .await
                .unwrap()
                .unwrap_err();
            assert_eq!(result, &BLOB[5..9], "6");

            let result = download(&client, 3..=8).await.unwrap().unwrap();
            assert_eq!(result, &BLOB[3..=8], "ok");
        }

        fn get_builder() -> FailingClientSimulatorBuilder {
            FailingClientSimulatorBuilder::default()
                .blob_static(BLOB)
                .chunk_size(3)
        }

        async fn download<R: Into<InclusiveRange>>(
            client: &FailingClientSimulator,
            range: R,
        ) -> Result<Result<Vec<u8>, Vec<u8>>, CondowError> {
            let mut stream = client.download(IgnoreLocation, range.into()).await?;

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

        use futures::StreamExt;

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
        async fn range_ok() {
            let result = consume(5..BLOB.len(), 3, false).await.unwrap();
            assert_eq!(result, &BLOB[5..BLOB.len()])
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
                    Some(ErrorAction::Err("bang!".to_string()))
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
