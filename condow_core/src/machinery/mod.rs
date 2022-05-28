//! Streams for handling downloads
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use tracing::{info_span, Span};

use crate::condow_client::CondowClient;
use crate::config::{ClientRetryWrapper, Config};
use crate::errors::CondowError;
use crate::streams::{BytesStream, ChunkStream};
use crate::Probe;
use crate::{DownloadRange, InclusiveRange};

mod configure_download;
mod download;
mod part_request;

pub(crate) async fn download_chunks<C: CondowClient, DR: Into<DownloadRange>, P: Probe + Clone>(
    client: ClientRetryWrapper<C>,
    config: Config,
    location: C::Location,
    range: DR,
    probe: P,
) -> Result<ChunkStream, CondowError> {
    let range = range.into();

    let parent = Span::current();
    // This span will track the lifetime of the whole download for all parts...
    let download_span = info_span!(parent: &parent, "download", %location, %range);
    let download_span_enter_guard = download_span.enter();
    let download_guard = DownloadSpanGuard::new(download_span.clone());

    let configuration = if let Some(configuration) = configure_download::configure(
        location,
        range,
        config,
        &client,
        &probe,
        download_guard.span(),
    )
    .await?
    {
        configuration
    } else {
        return Ok(ChunkStream::empty());
    };

    let stream = if configuration.max_concurrency() <= 1 {
        download::download_chunks_sequentially(client, configuration, probe, download_guard)
    } else {
        download::download_chunks_concurrently(client, configuration, probe, download_guard)
    };

    // Explicit drops for scope visualization
    drop(download_span_enter_guard);

    Ok(stream)
}

pub(crate) async fn download_bytes<C: CondowClient, DR: Into<DownloadRange>, P: Probe + Clone>(
    client: ClientRetryWrapper<C>,
    config: Config,
    location: C::Location,
    range: DR,
    probe: P,
) -> Result<BytesStream, CondowError> {
    let range = range.into();

    let parent = Span::current();
    // This span will track the lifetime of the whole download for all parts...
    let download_span = info_span!(parent: &parent, "download", %location, %range);
    let download_span_enter_guard = download_span.enter();
    let download_guard = DownloadSpanGuard::new(download_span.clone());

    let configuration = if let Some(configuration) = configure_download::configure(
        location,
        range,
        config,
        &client,
        &probe,
        download_guard.span(),
    )
    .await?
    {
        configuration
    } else {
        return Ok(BytesStream::empty());
    };

    let stream = if configuration.max_concurrency() <= 1 {
        download::download_bytes_sequentially(client, configuration, probe, download_guard)
    } else {
        download::download_bytes_concurrently(client, configuration, probe, download_guard)
    };

    // Explicit drops for scope visualization
    drop(download_span_enter_guard);

    Ok(stream)
}

/// This struct contains a span which must be kept alive until whole download is completed
/// which means that all parts have been downloaded (and pushed on the result stream).
#[derive(Clone)]
pub(crate) struct DownloadSpanGuard(Arc<Span>);

impl DownloadSpanGuard {
    pub fn new(span: Span) -> Self {
        Self(Arc::new(span))
    }

    pub fn span(&self) -> &Span {
        &self.0
    }

    pub fn shared_span(&self) -> Arc<Span> {
        Arc::clone(&self.0)
    }
}

#[cfg(test)]
impl Default for DownloadSpanGuard {
    fn default() -> Self {
        Self(Arc::new(Span::none()))
    }
}

/// This is just a wrapper for not having to do
/// all the "matches" all over the place...
#[derive(Clone)]
pub(crate) enum ProbeInternal<P: Probe + Clone> {
    /// We have no internal [Probe] (from the ProbeFactory) but got a [Probe] via
    /// the request API
    RequestProbe(Arc<dyn Probe>),
    /// We have a [Probe] from the internal probe factory
    /// and got a [Probe] via
    /// the request API
    FactoryAndRequestProbe(P, Arc<dyn Probe>),
}

impl<P: Probe + Clone> Probe for ProbeInternal<P> {
    #[inline]
    fn effective_range(&self, range: InclusiveRange) {
        match self {
            ProbeInternal::RequestProbe(p) => p.effective_range(range),
            ProbeInternal::FactoryAndRequestProbe(factory_probe, request_probe) => {
                factory_probe.effective_range(range);
                request_probe.effective_range(range);
            }
        }
    }

    #[inline]
    fn download_started(&self) {
        match self {
            ProbeInternal::RequestProbe(p) => p.download_started(),
            ProbeInternal::FactoryAndRequestProbe(factory_probe, request_probe) => {
                factory_probe.download_started();
                request_probe.download_started();
            }
        }
    }

    #[inline]
    fn download_completed(&self, time: Duration) {
        match self {
            ProbeInternal::RequestProbe(p) => p.download_completed(time),
            ProbeInternal::FactoryAndRequestProbe(factory_probe, request_probe) => {
                factory_probe.download_completed(time);
                request_probe.download_completed(time);
            }
        }
    }

    #[inline]
    fn download_failed(&self, time: Option<Duration>) {
        match self {
            ProbeInternal::RequestProbe(p) => p.download_failed(time),
            ProbeInternal::FactoryAndRequestProbe(factory_probe, request_probe) => {
                factory_probe.download_failed(time);
                request_probe.download_failed(time);
            }
        }
    }

    #[inline]
    fn retry_attempt(&self, error: &CondowError, next_in: Duration) {
        match self {
            ProbeInternal::RequestProbe(p) => p.retry_attempt(error, next_in),
            ProbeInternal::FactoryAndRequestProbe(factory_probe, request_probe) => {
                factory_probe.retry_attempt(error, next_in);
                request_probe.retry_attempt(error, next_in);
            }
        }
    }

    #[inline]
    fn stream_resume_attempt(
        &self,
        error: &CondowError,
        orig_range: InclusiveRange,
        remaining_range: InclusiveRange,
    ) {
        match self {
            ProbeInternal::RequestProbe(p) => {
                p.stream_resume_attempt(error, orig_range, remaining_range)
            }
            ProbeInternal::FactoryAndRequestProbe(factory_probe, request_probe) => {
                factory_probe.stream_resume_attempt(error, orig_range, remaining_range);
                request_probe.stream_resume_attempt(error, orig_range, remaining_range);
            }
        }
    }

    #[inline]
    fn panic_detected(&self, msg: &str) {
        match self {
            ProbeInternal::RequestProbe(p) => p.panic_detected(msg),
            ProbeInternal::FactoryAndRequestProbe(factory_probe, request_probe) => {
                factory_probe.panic_detected(msg);
                request_probe.panic_detected(msg);
            }
        }
    }

    #[inline]
    fn queue_full(&self) {
        match self {
            ProbeInternal::RequestProbe(p) => p.queue_full(),
            ProbeInternal::FactoryAndRequestProbe(factory_probe, request_probe) => {
                factory_probe.queue_full();
                request_probe.queue_full();
            }
        }
    }

    #[inline]
    fn chunk_received(&self, part_index: u64, chunk_index: usize, n_bytes: usize) {
        match self {
            ProbeInternal::RequestProbe(p) => p.chunk_received(part_index, chunk_index, n_bytes),
            ProbeInternal::FactoryAndRequestProbe(factory_probe, request_probe) => {
                factory_probe.chunk_received(part_index, chunk_index, n_bytes);
                request_probe.chunk_received(part_index, chunk_index, n_bytes);
            }
        }
    }

    #[inline]
    fn part_started(&self, part_index: u64, range: crate::InclusiveRange) {
        match self {
            ProbeInternal::RequestProbe(p) => p.part_started(part_index, range),
            ProbeInternal::FactoryAndRequestProbe(factory_probe, request_probe) => {
                factory_probe.part_started(part_index, range);
                request_probe.part_started(part_index, range);
            }
        }
    }

    #[inline]
    fn part_completed(
        &self,
        part_index: u64,
        n_chunks: usize,
        n_bytes: u64,
        time: std::time::Duration,
    ) {
        match self {
            ProbeInternal::RequestProbe(p) => p.part_completed(part_index, n_chunks, n_bytes, time),
            ProbeInternal::FactoryAndRequestProbe(factory_probe, request_probe) => {
                factory_probe.part_completed(part_index, n_chunks, n_bytes, time);
                request_probe.part_completed(part_index, n_chunks, n_bytes, time);
            }
        }
    }

    #[inline]
    fn part_failed(&self, error: &CondowError, part_index: u64, range: &InclusiveRange) {
        match self {
            ProbeInternal::RequestProbe(p) => p.part_failed(error, part_index, range),
            ProbeInternal::FactoryAndRequestProbe(factory_probe, request_probe) => {
                factory_probe.part_failed(error, part_index, range);
                request_probe.part_failed(error, part_index, range);
            }
        }
    }
}

#[cfg(test)]
mod tests;
