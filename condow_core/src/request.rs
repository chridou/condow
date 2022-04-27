//! Download requests
//!
//! Builder style APIs to configure individual downloads.

use std::{str::FromStr, sync::Arc};

use futures::{future::BoxFuture, AsyncRead};

use crate::{
    condow_client::IgnoreLocation, errors::CondowError, probe::Probe, reader::BytesAsyncReader,
    ChunkStream, DownloadRange, GetSizeMode, PartStream,
};

/// A function which downloads from the givel location and the given [Params].
///
/// This is to make the request objects independent from the actual mechanism
/// used to download.
type DownloadFn<L> = Box<
    dyn FnOnce(L, Params) -> BoxFuture<'static, Result<ChunkStream, CondowError>> + Send + 'static,
>;

/// A request for a download where the location is not yet known
///
/// The default is to download the complete BLOB.
///
/// This can only directly download if the type of the location is [IgnoreLocation].
pub struct RequestNoLocation<L> {
    download_fn: DownloadFn<L>,
    params: Params,
}

impl<L> RequestNoLocation<L> {
    pub(crate) fn new<F>(download_fn: F) -> Self
    where
        F: FnOnce(L, Params) -> BoxFuture<'static, Result<ChunkStream, CondowError>>
            + Send
            + 'static,
    {
        Self {
            download_fn: Box::new(download_fn),
            params: Params {
                probe: None,
                range: (..).into(),
                get_size_mode: GetSizeMode::Default,
            },
        }
    }

    /// Specify the location to download the Blob from
    pub fn at<LL: Into<L>>(self, location: LL) -> Request<L> {
        Request {
            download_fn: self.download_fn,
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
            download_fn: self.download_fn,
            location: location
                .try_into()
                .map_err(|err| CondowError::new_other("invalid location").with_source(err))?,
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
            download_fn: self.download_fn,
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

    /// Attach a [Probe] to the download
    pub fn probe(mut self, probe: Arc<dyn Probe>) -> Self {
        self.params.probe = Some(probe);
        self
    }

    /// Explicitly set the condition on when to query for the Blob size
    pub fn get_size_mode(mut self, get_size_mode: GetSizeMode) -> Self {
        self.params.get_size_mode = get_size_mode;
        self
    }
}

impl RequestNoLocation<IgnoreLocation> {
    /// Download as a [PartStream]
    pub async fn download(self) -> Result<PartStream<ChunkStream>, CondowError> {
        self.at(IgnoreLocation).download().await
    }

    /// Download as a [ChunkStream]
    pub async fn download_chunks(self) -> Result<ChunkStream, CondowError> {
        self.at(IgnoreLocation).download_chunks().await
    }

    /// Downloads into a freshly allocated [Vec]
    pub async fn download_into_vec(self) -> Result<Vec<u8>, CondowError> {
        let stream = self.download_chunks().await?;
        stream.into_vec().await
    }

    /// Writes all received bytes into the provided buffer
    ///
    /// Fails if the buffer is too small.
    pub async fn download_into_buffer(self, buffer: &mut [u8]) -> Result<usize, CondowError> {
        let stream = self.download_chunks().await?;
        stream.write_buffer(buffer).await
    }

    /// Returns an [AsyncRead] which reads over the bytes of the stream
    pub async fn reader(self) -> Result<impl AsyncRead, CondowError> {
        let stream = self.download().await?.bytes_stream();
        Ok(BytesAsyncReader::new(stream))
    }
}

/// A request for a download from a specific location
///
/// The default is to download the complete BLOB.
pub struct Request<L> {
    download_fn: DownloadFn<L>,
    location: L,
    params: Params,
}

impl<L> Request<L> {
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

    /// Attach a [Probe] to the download
    pub fn probe(mut self, probe: Arc<dyn Probe>) -> Self {
        self.params.probe = Some(probe);
        self
    }

    /// Explicitly set the condition on when to query for the Blob size
    pub fn get_size_mode(mut self, get_size_mode: GetSizeMode) -> Self {
        self.params.get_size_mode = get_size_mode;
        self
    }

    /// Download as a [PartStream]
    pub async fn download(self) -> Result<PartStream<ChunkStream>, CondowError> {
        PartStream::from_chunk_stream(self.download_chunks().await?)
    }

    /// Download as a [ChunkStream]
    pub async fn download_chunks(self) -> Result<ChunkStream, CondowError> {
        (self.download_fn)(self.location, self.params).await
    }

    /// Downloads into a freshly allocated [Vec]
    pub async fn download_into_vec(self) -> Result<Vec<u8>, CondowError> {
        let stream = self.download_chunks().await?;
        stream.into_vec().await
    }

    /// Writes all received bytes into the provided buffer
    ///
    /// Fails if the buffer is too small.
    pub async fn download_into_buffer(self, buffer: &mut [u8]) -> Result<usize, CondowError> {
        let stream = self.download_chunks().await?;
        stream.write_buffer(buffer).await
    }

    /// Returns an [AsyncRead] which reads over the bytes of the stream
    pub async fn reader(self) -> Result<impl AsyncRead, CondowError> {
        let stream = self.download().await?.bytes_stream();
        Ok(BytesAsyncReader::new(stream))
    }
}

/// Internal struct to keep common parameters independen of
/// the dispatch mechanism together
pub(crate) struct Params {
    pub probe: Option<Arc<dyn Probe>>,
    pub range: DownloadRange,
    pub get_size_mode: GetSizeMode,
}
