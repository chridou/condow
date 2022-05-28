use std::{str::FromStr, sync::Arc};

use anyhow::Error as AnyError;
use futures::{future::BoxFuture, FutureExt};

use crate::{
    condow_client::CondowClient,
    config::{ClientRetryWrapper, Config},
    errors::CondowError,
    machinery::{self, ProbeInternal},
    probe::ProbeFactory,
    reader::{CondowAdapter, RandomAccessReader},
    request::{Params, RequestAdapter},
    streams::{BytesStream, ChunkStream},
    Downloads, DownloadsUntyped, RequestNoLocation,
};

/// The CONcurrent DOWnloader
///
/// Downloads BLOBs by splitting the download into parts
/// which are downloaded concurrently.
///
/// It is recommended to use one of the traits [Downloads] or
/// [DownloadsUntyped] instead of [Condow] itself since this
/// struct might get more type parameters in the future.
///
/// ## Wording
///
/// * `Range`: A range to be downloaded of a BLOB (Can also be the complete BLOB)
/// * `Part`: The downloaded range is split into parts of certain ranges which are downloaded concurrently
/// * `Chunk`: A chunk of bytes received from the network (or else). Multiple chunks make up a part.
pub struct Condow<C, PF = ()> {
    pub(crate) client: ClientRetryWrapper<C>,
    pub(crate) config: Config,
    pub(crate) probe_factory: Option<Arc<PF>>,
}

impl<C: CondowClient, PF: ProbeFactory> Clone for Condow<C, PF> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            config: self.config.clone(),
            probe_factory: self.probe_factory.clone(),
        }
    }
}

impl<C> Condow<C>
where
    C: CondowClient,
{
    /// Create a new CONcurrent DOWnloader.
    ///
    /// Fails if the [Config] is not valid.
    pub fn new(client: C, config: Config) -> Result<Condow<C, ()>, AnyError> {
        let config = config.validated()?;
        Ok(Condow {
            client: ClientRetryWrapper::new(client, config.retries.clone()),
            config,
            probe_factory: None,
        })
    }
}

impl<C, PF> Condow<C, PF>
where
    C: CondowClient,
    PF: ProbeFactory,
{
    /// Set a factory for [Probe]s which will add a [Probe] to each request
    ///
    /// The [ProbeFactory] is intended to share state with the [Probe] to
    /// add instrumentation
    pub fn probe_factory<PPF: ProbeFactory>(self, factory: PPF) -> Condow<C, PPF> {
        self.probe_factory_shared(Arc::new(factory))
    }

    /// Set a factory for [Probe]s which will add a [Probe] to each request
    ///
    /// The [ProbeFactory] is intended to share state with the [Probe] to
    /// add instrumentation
    pub fn probe_factory_shared<PPF: ProbeFactory>(self, factory: Arc<PPF>) -> Condow<C, PPF> {
        Condow {
            client: self.client,
            config: self.config,
            probe_factory: Some(factory),
        }
    }

    /// Download a BLOB via the returned request object
    pub fn blob(&self) -> RequestNoLocation<C::Location> {
        let condow = self.clone();
        let adapter = CondowDownloadAdapter::new(condow);

        RequestNoLocation::new(adapter, self.config.clone())
    }

    /// Get the size of a BLOB at the given location
    pub async fn get_size<L: Into<C::Location>>(&self, location: L) -> Result<u64, CondowError> {
        self.client.get_size(location.into(), &()).await
    }

    /// Creates a [RandomAccessReader] for the given location
    pub async fn reader<L: Into<C::Location>>(
        &self,
        location: L,
    ) -> Result<RandomAccessReader, CondowError> {
        RandomAccessReader::new(CondowAdapter::new(self.clone(), location.into())).await
    }

    /// Creates a [RandomAccessReader] for the given location
    pub fn reader_with_length<L: Into<C::Location>>(
        &self,
        location: L,
        length: u64,
    ) -> RandomAccessReader {
        RandomAccessReader::new_with_length(
            CondowAdapter::new(self.clone(), location.into()),
            length,
        )
    }
}

impl<C, PF> Downloads for Condow<C, PF>
where
    C: CondowClient,
    PF: ProbeFactory,
{
    type Location = C::Location;

    fn blob(&self) -> RequestNoLocation<Self::Location> {
        self.blob()
    }

    fn get_size<'a>(&'a self, location: Self::Location) -> BoxFuture<'a, Result<u64, CondowError>> {
        Box::pin(self.get_size(location))
    }

    fn reader_with_length(&self, location: Self::Location, length: u64) -> RandomAccessReader
    where
        Self: Sized,
    {
        self.reader_with_length(location, length)
    }
}

impl<C, PF> DownloadsUntyped for Condow<C, PF>
where
    C: CondowClient,
    C::Location: FromStr,
    <C::Location as FromStr>::Err: std::error::Error + Sync + Send + 'static,
    PF: ProbeFactory,
{
    fn blob(&self) -> RequestNoLocation<&str> {
        let condow = self.clone();
        let adapter = CondowDownloadAdapterUntyped::new(condow);

        RequestNoLocation::new(adapter, self.config.clone())
    }

    fn get_size<'a>(&'a self, location: &str) -> BoxFuture<'a, Result<u64, CondowError>> {
        let location = match location.parse::<C::Location>() {
            Ok(loc) => loc,
            Err(parse_err) => {
                return futures::future::err(
                    CondowError::new_other(format!("invalid location: {location}"))
                        .with_source(parse_err),
                )
                .boxed();
            }
        };

        Box::pin(self.get_size(location))
    }

    fn reader_with_length(
        &self,
        location: &str,
        length: u64,
    ) -> Result<RandomAccessReader, CondowError>
    where
        Self: Sized,
    {
        let location = match location.parse::<C::Location>() {
            Ok(loc) => loc,
            Err(parse_err) => {
                return Err(
                    CondowError::new_other(format!("invalid location: {location}"))
                        .with_source(parse_err),
                )
            }
        };

        Ok(self.reader_with_length(location, length))
    }
}

struct CondowDownloadAdapter<C: CondowClient, PF> {
    condow: Condow<C, PF>,
}

impl<C, PF> CondowDownloadAdapter<C, PF>
where
    C: CondowClient,
    PF: ProbeFactory,
{
    fn new(condow: Condow<C, PF>) -> Self {
        Self { condow }
    }
}

impl<C, PF, L> RequestAdapter<L> for CondowDownloadAdapter<C, PF>
where
    C: CondowClient<Location = L>,
    PF: ProbeFactory,
    L: std::fmt::Debug + std::fmt::Display + Clone + Send + Sync + 'static,
{
    fn bytes<'a>(
        &'a self,
        location: L,
        params: Params,
    ) -> BoxFuture<'a, Result<BytesStream, CondowError>> {
        get_bytes_stream(self.condow.clone(), location, params).boxed()
    }

    fn chunks<'a>(
        &'a self,
        location: L,
        params: Params,
    ) -> BoxFuture<'a, Result<ChunkStream, CondowError>> {
        get_chunk_stream(self.condow.clone(), location, params).boxed()
    }
}

struct CondowDownloadAdapterUntyped<C: CondowClient, PF> {
    typed_adapter: CondowDownloadAdapter<C, PF>,
}

impl<C, PF> CondowDownloadAdapterUntyped<C, PF>
where
    C: CondowClient,
    PF: ProbeFactory,
{
    fn new(condow: Condow<C, PF>) -> Self {
        Self {
            typed_adapter: CondowDownloadAdapter::new(condow),
        }
    }
}

impl<C, PF> RequestAdapter<&str> for CondowDownloadAdapterUntyped<C, PF>
where
    C: CondowClient,
    PF: ProbeFactory,
    C::Location: FromStr,
    <C::Location as FromStr>::Err: std::error::Error + Send + Sync + 'static,
{
    fn bytes<'a>(
        &'a self,
        location: &str,
        params: Params,
    ) -> BoxFuture<'a, Result<BytesStream, CondowError>> {
        let location = match location.parse::<C::Location>() {
            Ok(loc) => loc,
            Err(parse_err) => {
                return futures::future::err(
                    CondowError::new_other(format!("invalid location: {location}"))
                        .with_source(parse_err),
                )
                .boxed();
            }
        };

        self.typed_adapter.bytes(location, params).boxed()
    }

    fn chunks<'a>(
        &'a self,
        location: &str,
        params: Params,
    ) -> BoxFuture<'a, Result<ChunkStream, CondowError>> {
        let location = match location.parse::<C::Location>() {
            Ok(loc) => loc,
            Err(parse_err) => {
                return futures::future::err(
                    CondowError::new_other(format!("invalid location: {location}"))
                        .with_source(parse_err),
                )
                .boxed();
            }
        };

        self.typed_adapter.chunks(location, params).boxed()
    }
}

async fn get_chunk_stream<C, PF>(
    condow: Condow<C, PF>,
    location: C::Location,
    params: Params,
) -> Result<ChunkStream, CondowError>
where
    C: CondowClient,
    PF: ProbeFactory,
{
    match (
        params.probe,
        condow.probe_factory.as_ref().map(|f| f.make(&location)),
    ) {
        (None, None) => {
            machinery::download_chunks(condow.client, params.config, location, params.range, ())
                .await
        }
        (Some(request_probe), None) => {
            machinery::download_chunks(
                condow.client,
                params.config,
                location,
                params.range,
                ProbeInternal::RequestProbe::<()>(request_probe),
            )
            .await
        }
        (None, Some(factory_probe)) => {
            machinery::download_chunks(
                condow.client,
                params.config,
                location,
                params.range,
                factory_probe,
            )
            .await
        }
        (Some(request_probe), Some(factory_probe)) => {
            machinery::download_chunks(
                condow.client,
                params.config,
                location,
                params.range,
                ProbeInternal::FactoryAndRequestProbe(factory_probe, request_probe),
            )
            .await
        }
    }
}

async fn get_bytes_stream<C, PF>(
    condow: Condow<C, PF>,
    location: C::Location,
    params: Params,
) -> Result<BytesStream, CondowError>
where
    C: CondowClient,
    PF: ProbeFactory,
{
    match (
        params.probe,
        condow.probe_factory.as_ref().map(|f| f.make(&location)),
    ) {
        (None, None) => {
            machinery::download_bytes(condow.client, params.config, location, params.range, ())
                .await
        }
        (Some(request_probe), None) => {
            machinery::download_bytes(
                condow.client,
                params.config,
                location,
                params.range,
                ProbeInternal::RequestProbe::<()>(request_probe),
            )
            .await
        }
        (None, Some(factory_probe)) => {
            machinery::download_bytes(
                condow.client,
                params.config,
                location,
                params.range,
                factory_probe,
            )
            .await
        }
        (Some(request_probe), Some(factory_probe)) => {
            machinery::download_bytes(
                condow.client,
                params.config,
                location,
                params.range,
                ProbeInternal::FactoryAndRequestProbe(factory_probe, request_probe),
            )
            .await
        }
    }
}

#[cfg(test)]
mod tests;
