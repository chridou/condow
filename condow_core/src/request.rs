//! Download requests
//!
//! Builder style APIs to configure individual downloads.

use std::{str::FromStr, sync::Arc};

use futures::{
    future::{self, BoxFuture},
    FutureExt, TryStreamExt,
};

use crate::{
    condow_client::IgnoreLocation,
    config::Config,
    errors::CondowError,
    probe::Probe,
    reader::{BytesAsyncReader, FetchAheadMode, RandomAccessReader},
    streams::BytesStream,
    ChunkStream, DownloadRange, InclusiveRange, OrderedChunkStream,
};

pub(crate) trait RequestAdapter<L>: Send + Sync + 'static {
    fn bytes(
        &self,
        location: L,
        params: Params,
    ) -> BoxFuture<'_, Result<BytesStream, CondowError>>;
    fn chunks(
        &self,
        location: L,
        params: Params,
    ) -> BoxFuture<'_, Result<ChunkStream, CondowError>>;
    fn size(&self, location: L, params: Params) -> BoxFuture<'_, Result<u64, CondowError>>;
}

/// A request for a download where the location is not yet known
///
/// The default is to download the complete BLOB.
///
/// A location to download from must be provided to actually download
/// from a remote location.
///
/// This can only directly download if the type of the location is [IgnoreLocation]
/// which probably only makes sense while testing.
pub struct RequestNoLocation<L> {
    adapter: Box<dyn RequestAdapter<L>>,
    params: Params,
}

impl<L> RequestNoLocation<L> {
    pub(crate) fn new<A>(adapter: A, config: Config) -> Self
    where
        A: RequestAdapter<L>,
    {
        Self {
            adapter: Box::new(adapter),
            params: Params {
                probe: None,
                range: (..).into(),
                config,
                trusted_blob_size: None,
            },
        }
    }

    /// Specify the location to download the Blob from
    pub fn at<LL: Into<L>>(self, location: LL) -> Request<L> {
        Request {
            adapter: self.adapter,
            location: location.into(),
            params: self.params,
        }
    }

    /// Specify the location to download the BLOB from
    ///
    /// Fails if `location` is not convertable to `Self::L`.
    pub fn try_at<LL>(self, location: LL) -> Result<Request<L>, CondowError>
    where
        LL: TryInto<L>,
        LL::Error: std::error::Error + Send + Sync + 'static,
    {
        Ok(Request {
            adapter: self.adapter,
            location: location.try_into().map_err(|err| {
                CondowError::new_other(format!("invalid location - {err}")).with_source(err)
            })?,
            params: self.params,
        })
    }

    /// Specify the location as a string slice to download the BLOB from
    ///
    /// Fails if `location` is not parsable.
    pub fn try_at_str(self, location: &str) -> Result<Request<L>, CondowError>
    where
        L: FromStr,
        <L as FromStr>::Err: std::error::Error + Send + Sync + 'static,
    {
        Ok(Request {
            adapter: self.adapter,
            location: location.parse().map_err(|err| {
                CondowError::new_other(format!("invalid location: {location}")).with_source(err)
            })?,
            params: self.params,
        })
    }

    /// Specify the range to download
    pub fn range<DR: Into<DownloadRange>>(mut self, range: DR) -> Self {
        self.params.range = range.into();
        self
    }

    /// Specify the total size of the BLOB.
    ///
    /// This will prevent condow from querying the size of the BLOB.
    /// The supplied value must be correct.
    pub fn trusted_blob_size(mut self, size: u64) -> Self {
        self.params.trusted_blob_size = Some(size);
        self
    }

    /// Attach a [Probe] to the download
    pub fn probe(mut self, probe: Arc<dyn Probe>) -> Self {
        self.params.probe = Some(probe);
        self
    }

    /// Override the configuration for this request
    pub fn reconfigure<F>(mut self, mut reconfigure: F) -> Self
    where
        F: FnMut(Config) -> Config,
    {
        self.params.config = reconfigure(self.params.config);
        self
    }
}

impl RequestNoLocation<IgnoreLocation> {
    /// Download chunks of bytes
    ///
    /// Provided mainly for testing.
    ///
    pub async fn download(self) -> Result<BytesStream, CondowError> {
        self.at(IgnoreLocation).download().await
    }

    /// Download as an [OrderedChunkStream]
    ///
    /// Provided mainly for testing.
    pub async fn download_chunks_ordered(self) -> Result<OrderedChunkStream, CondowError> {
        self.at(IgnoreLocation).download_chunks_ordered().await
    }

    /// Download as a [ChunkStream]
    ///
    /// Provided mainly for testing.
    pub async fn download_chunks_unordered(self) -> Result<ChunkStream, CondowError> {
        self.at(IgnoreLocation).download_chunks_unordered().await
    }

    /// Downloads into a freshly allocated [Vec]
    ///
    /// Provided mainly for testing.
    pub async fn download_into_vec(self) -> Result<Vec<u8>, CondowError> {
        let stream = self.download_chunks_unordered().await?;
        stream.into_vec().await
    }

    /// Writes all received bytes into the provided buffer
    ///
    /// Fails if the buffer is too small.
    ///
    /// Provided mainly for testing.
    pub async fn download_into_buffer(self, buffer: &mut [u8]) -> Result<usize, CondowError> {
        let stream = self.download_chunks_unordered().await?;
        stream.write_buffer(buffer).await
    }

    /// Returns an [AsyncRead] which reads over the bytes of the stream
    ///
    /// Provided mainly for testing.
    pub async fn reader(self) -> Result<BytesAsyncReader, CondowError> {
        let stream = self.download_chunks_ordered().await?.into_bytes_stream();
        Ok(BytesAsyncReader::new(stream))
    }

    /// Returns a builder for a [RandomAccessReader] which implements [AsyncRead] and [AsyncSeek].
    ///
    /// To create a random access reader the size of the BLOB must be known.
    /// If `trusted_blob_size` is set that value will be used. Otherwise a request
    /// to get the size of the BLOB is made.
    pub fn random_access_reader(self) -> RandomAccessReaderBuilder<IgnoreLocation> {
        self.at(IgnoreLocation).random_access_reader()
    }

    /// Pulls the bytes into the void
    ///
    /// Provided mainly for testing.
    pub async fn wc(self) -> Result<(), CondowError> {
        self.download_chunks_unordered()
            .await?
            .try_for_each(|_| future::ok(()))
            .await?;
        Ok(())
    }
}

/// A request for a download from a specific location
///
/// The default is to download the complete BLOB.
pub struct Request<L> {
    adapter: Box<dyn RequestAdapter<L>>,
    location: L,
    params: Params,
}

impl<L> Request<L>
where
    L: Send + Sync + 'static,
{
    /// Specify the location to download the Blob from
    pub fn at<LL: Into<L>>(mut self, location: LL) -> Self {
        self.location = location.into();
        self
    }

    /// Specify the range to download
    pub fn range<DR: Into<DownloadRange>>(mut self, range: DR) -> Self {
        self.params.range = range.into();
        self
    }

    /// Specify the total size of the BLOB.
    ///
    /// This will prevent condow from querying the size of the BLOB.
    /// The supplied value must be correct.
    pub fn trusted_blob_size(mut self, size: u64) -> Self {
        self.params.trusted_blob_size = Some(size);
        self
    }

    /// Attach a [Probe] to the download
    pub fn probe(mut self, probe: Arc<dyn Probe>) -> Self {
        self.params.probe = Some(probe);
        self
    }

    /// Override the configuration for this request
    pub fn reconfigure<F>(mut self, mut reconfigure: F) -> Self
    where
        F: FnMut(Config) -> Config,
    {
        self.params.config = reconfigure(self.params.config);
        self
    }

    /// Download chunks of bytes
    pub async fn download(self) -> Result<BytesStream, CondowError> {
        self.params
            .config
            .validate()
            .map_err(|err| CondowError::new_other("invalid configuration").with_source(err))?;
        self.adapter.bytes(self.location, self.params).await
    }

    /// Download as an [OrderedChunkStream]
    pub async fn download_chunks_ordered(self) -> Result<OrderedChunkStream, CondowError> {
        OrderedChunkStream::from_chunk_stream(self.download_chunks_unordered().await?)
    }

    /// Download as a [ChunkStream]
    pub async fn download_chunks_unordered(self) -> Result<ChunkStream, CondowError> {
        self.params
            .config
            .validate()
            .map_err(|err| CondowError::new_other("invalid configuration").with_source(err))?;
        self.adapter.chunks(self.location, self.params).await
    }

    /// Downloads into a freshly allocated [Vec]
    pub async fn download_into_vec(self) -> Result<Vec<u8>, CondowError> {
        let stream = self.download_chunks_unordered().await?;
        stream.into_vec().await
    }

    /// Writes all received bytes into the provided buffer
    ///
    /// Fails if the buffer is too small.
    pub async fn download_into_buffer(self, buffer: &mut [u8]) -> Result<usize, CondowError> {
        let stream = self.download_chunks_unordered().await?;
        stream.write_buffer(buffer).await
    }

    /// Returns an [AsyncRead] which reads over the bytes of the stream
    pub async fn reader(self) -> Result<BytesAsyncReader, CondowError> {
        let stream = self.download().await?;
        Ok(BytesAsyncReader::new(stream))
    }

    /// Returns a builder for a [RandomAccessReader] which implements [AsyncRead] and [AsyncSeek].
    ///
    /// To create a random access reader the size of the BLOB must be known.
    /// If `trusted_blob_size` is set that value will be used. Otherwise a request
    /// to get the size of the BLOB is made.
    pub fn random_access_reader(self) -> RandomAccessReaderBuilder<L> {
        RandomAccessReaderBuilder {
            adapter: self.adapter,
            location: self.location,
            params: self.params,
            fetch_ahead_mode: FetchAheadMode::default(),
        }
    }

    /// Pulls the bytes into the void
    pub async fn wc(self) -> Result<(), CondowError> {
        self.download_chunks_unordered()
            .await?
            .try_for_each(|_| future::ok(()))
            .await?;
        Ok(())
    }
}

pub struct RandomAccessReaderBuilder<L> {
    adapter: Box<dyn RequestAdapter<L>>,
    location: L,
    params: Params,
    fetch_ahead_mode: FetchAheadMode,
}

impl<L> RandomAccessReaderBuilder<L>
where
    L: Clone + Send + Sync + 'static,
{
    /// Specify the location to download the Blob from
    pub fn at<LL: Into<L>>(mut self, location: LL) -> Self {
        self.location = location.into();
        self
    }

    /// Specify the range to the reader operates on.
    ///
    /// The reader to be created can only operate within th ebounds of this range.
    /// By default the whole BLOB is expected to be read from.
    pub fn range<DR: Into<DownloadRange>>(mut self, range: DR) -> Self {
        self.params.range = range.into();
        self
    }

    /// Specify the total size of the BLOB.
    ///
    /// This will prevent condow from querying the size of the BLOB.
    /// The supplied value must be correct.
    pub fn trusted_blob_size(mut self, size: u64) -> Self {
        self.params.trusted_blob_size = Some(size);
        self
    }

    /// Attach a [Probe] to the reader
    pub fn probe(mut self, probe: Arc<dyn Probe>) -> Self {
        self.params.probe = Some(probe);
        self
    }

    /// Override the configuration downloads
    pub fn reconfigure<F>(mut self, reconfigure: F) -> Self
    where
        F: FnOnce(Config) -> Config,
    {
        self.params.config = reconfigure(self.params.config);
        self
    }

    /// Returns an [AsyncRead] + [AsyncSeek] which reads over the bytes of the BLOB(-range)
    pub async fn finish(self) -> Result<RandomAccessReader, CondowError> {
        let bounds = match self.params.range {
            DownloadRange::Open(or) => {
                let size = if let Some(trusted_size) = self.params.trusted_blob_size {
                    trusted_size
                } else {
                    self.adapter
                        .size(self.location.clone(), self.params.clone())
                        .await?
                };
                if let Some(range) = or.incl_range_from_size(size)? {
                    range
                } else {
                    return Err(CondowError::new_invalid_range(format!(
                        "{or} with blob size {size}"
                    )));
                }
            }
            DownloadRange::Closed(cl) => {
                let range = if let Some(range) = cl.incl_range() {
                    range
                } else {
                    return Err(CondowError::new_invalid_range(format!("{cl}")));
                };

                range.validate()?;

                if let Some(trusted_size) = self.params.trusted_blob_size {
                    if range.end_incl() >= trusted_size {
                        return Err(CondowError::new_invalid_range(format!("{cl}")));
                    }
                }

                range
            }
        };

        let fetch_ahead_mode = self.fetch_ahead_mode;

        let adapter = Arc::new(self.adapter);
        let params = self.params;
        let location = self.location;
        let get_stream_fn = move |range: InclusiveRange| {
            let mut params = params.clone();
            let location = location.clone();
            let adapter = Arc::clone(&adapter);

            params.range = range.into();
            async move {
                Ok(BytesAsyncReader::new(
                    adapter.bytes(location, params).await?,
                ))
            }
            .boxed()
        };

        Ok(RandomAccessReader::new(
            get_stream_fn,
            bounds,
            fetch_ahead_mode,
        ))
    }
}

/// Internal struct to keep common parameters independen of
/// the dispatch mechanism together
#[derive(Clone)]
pub(crate) struct Params {
    pub probe: Option<Arc<dyn Probe>>,
    pub range: DownloadRange,
    pub config: Config,
    pub trusted_blob_size: Option<u64>,
}

#[cfg(test)]
mod tests {
    use futures::{future::BoxFuture, FutureExt};

    use crate::{
        condow_client::InMemoryClient,
        config::{ClientRetryWrapper, Config},
        errors::CondowError,
        machinery,
        request::{Params, RequestAdapter},
        streams::{BytesStream, ChunkStream},
        RequestNoLocation,
    };

    #[tokio::test]
    async fn request_adapter_typed_compiles() {
        let client = InMemoryClient::<i32>::new_static(b"a remote BLOB");
        let config = Config::default();
        let client = ClientRetryWrapper::new(client, config.retries.clone());

        struct FooAdapter {
            client: ClientRetryWrapper<InMemoryClient<i32>>,
        }

        impl RequestAdapter<i32> for FooAdapter {
            fn bytes(
                &self,
                location: i32,
                params: Params,
            ) -> BoxFuture<'_, Result<BytesStream, CondowError>> {
                machinery::download_bytes(
                    self.client.clone(),
                    params.config,
                    location,
                    params.range,
                    (),
                    None,
                )
                .boxed()
            }

            fn chunks(
                & self,
                _location: i32,
                _params: Params,
            ) -> BoxFuture<'_, Result<ChunkStream, CondowError>> {
                unimplemented!()
            }

            fn size(
                &self,
                location: i32,
                _params: Params,
            ) -> BoxFuture<'_, Result<u64, CondowError>> {
                self.client.get_size(location, &()).boxed()
            }
        }

        let adapter = FooAdapter { client };

        let params = Params {
            probe: None,
            range: (1..=10).into(),
            config,
            trusted_blob_size: None,
        };

        let request = RequestNoLocation {
            adapter: Box::new(adapter),
            params,
        };

        let _bytes = request.at(42).download().await.unwrap();
    }

    #[tokio::test]
    async fn request_adapter_str_compiles() {
        let client = InMemoryClient::<i32>::new_static(b"a remote BLOB");
        let config = Config::default();
        let client = ClientRetryWrapper::new(client, config.retries.clone());

        struct FooAdapter {
            client: ClientRetryWrapper<InMemoryClient<i32>>,
        }

        impl RequestAdapter<&str> for FooAdapter {
            fn bytes<'a>(
                &'a self,
                location: &str,
                params: Params,
            ) -> BoxFuture<'a, Result<BytesStream, CondowError>> {
                machinery::download_bytes(
                    self.client.clone(),
                    params.config,
                    location.parse().unwrap(),
                    params.range,
                    (),
                    None,
                )
                .boxed()
            }

            fn chunks<'a>(
                &'a self,
                _location: &str,
                _params: Params,
            ) -> BoxFuture<'a, Result<ChunkStream, CondowError>> {
                unimplemented!()
            }

            fn size<'a>(
                &'a self,
                location: &str,
                _params: Params,
            ) -> BoxFuture<'a, Result<u64, CondowError>> {
                self.client.get_size(location.parse().unwrap(), &()).boxed()
            }
        }

        let adapter = FooAdapter { client };

        let params = Params {
            probe: None,
            range: (1..=10).into(),
            config,
            trusted_blob_size: None,
        };

        let request = RequestNoLocation {
            adapter: Box::new(adapter),
            params,
        };

        let _bytes = request.at("42").download().await.unwrap();
    }
}
