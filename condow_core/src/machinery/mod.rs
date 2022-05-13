//! Streams for handling downloads
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, info_span, Instrument, Span};

use crate::condow_client::CondowClient;
use crate::config::{ClientRetryWrapper, Config};
use crate::errors::{CondowError, IoError};
use crate::streams::{BytesHint, ChunkStream};
use crate::Probe;
use crate::{DownloadRange, InclusiveRange};

use self::part_request::PartRequestIterator;

mod download;

mod part_request;

pub(crate) async fn download_range<C: CondowClient, DR: Into<DownloadRange>, P: Probe + Clone>(
    client: ClientRetryWrapper<C>,
    mut config: Config,
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

    range.validate()?;

    let range = if let Some(range) = range.sanitized() {
        range
    } else {
        return Ok(ChunkStream::empty());
    };

    let configure_stream_span = info_span!(parent: &download_span, "configure_stream");

    let (effective_range, bytes_hint) = match range {
        DownloadRange::Open(or) => {
            debug!(parent: &configure_stream_span, "open range");
            let size = client
                .get_size(location.clone(), &probe)
                .instrument(configure_stream_span.clone())
                .await?;
            if let Somedownload_part(range) = or.incl_range_from_size(size) {
                (range, BytesHint::new_exact(range.len()))
            } else {
                return Ok(ChunkStream::empty());
            }
        }
        DownloadRange::Closed(cl) => {
            debug!(parent: &configure_stream_span, "closed range");
            if config.always_get_size.into_inner() {
                debug!(parent: &configure_stream_span, "get size enforced");
                let size = client
                    .get_size(location.clone(), &probe)
                    .instrument(configure_stream_span.clone())
                    .await?;
                if let Some(range) = cl.incl_range_from_size(size) {
                    (range, BytesHint::new_exact(range.len()))
                } else {
                    return Ok(ChunkStream::empty());
                }
            } else if let Some(range) = cl.incl_range() {
                (range, BytesHint::new_at_max(range.len()))
            } else {
                return Ok(ChunkStream::empty());
            }
        }
    };

    let part_requests = PartRequestIterator::new(effective_range, config.part_size_bytes.into());

    debug!(
        parent: &configure_stream_span,
        "download {} parts",
        part_requests.exact_size_hint()
    );

    if part_requests.exact_size_hint() == 0 {
        panic!("n_parts must not be 0. This is a bug");
    }

    determine_effective_concurrency(
        &mut config,
        effective_range,
        part_requests.exact_size_hint(),
        &configure_stream_span,
    );

    config.log_download_messages_as_debug.log(format!(
        "download started ({} bytes) with effective concurrency {} and {} parts",
        effective_range.len(),
        config.max_concurrency,
        part_requests.exact_size_hint()
    ));

    probe.effective_range(effective_range);

    drop(configure_stream_span);

    let stream = download_chunks(
        client.clone(),
        location,
        part_requests,
        bytes_hint,
        config.clone(),
        probe.clone(),
        download_guard,
    )
    .await?;

    // Explicit drops for scope visualization
    drop(download_span_enter_guard);

    Ok(stream)
}

async fn download_chunks<C: CondowClient, P: Probe + Clone>(
    client: ClientRetryWrapper<C>,
    location: C::Location,
    part_requests: PartRequestIterator,
    bytes_hint: BytesHint,
    config: Config,
    probe: P,
    download_span_guard: DownloadSpanGuard,
) -> Result<ChunkStream, CondowError> {
    if *config.max_concurrency <= 1 {
        download::download_chunks_sequentially(part_requests, client, location, probe, config).await
    } else {
        let (chunk_stream, sender) = ChunkStream::new_channel_sink_pair(bytes_hint);
        tokio::spawn(async move {
            let result = download::download_concurrently(
                part_requests,
                *config.max_concurrency,
                sender,
                client,
                config,
                location,
                probe,
                download_span_guard,
            )
            .await;
            result
        });
        Ok(chunk_stream)
    }
}

fn determine_effective_concurrency(
    config: &mut Config,
    effective_range: InclusiveRange,
    n_parts: u64,
    log_span: &Span,
) {
    config.max_concurrency = if *config.max_concurrency <= 1 {
        config.max_concurrency
    } else {
        let n_conc_1 = if effective_range.len() < *config.min_bytes_for_concurrent_download {
            debug!(
                parent: log_span,
                "sequential download forced by 'min_bytes_for_concurrent_download'"
            );
            1
        } else {
            *config.max_concurrency
        };
        let n_conc_2 = if n_parts < *config.min_parts_for_concurrent_download {
            debug!(
                parent: log_span,
                "sequential download forced by 'min_parts_for_concurrent_download'"
            );
            1
        } else {
            *config.max_concurrency
        };

        (*config.max_concurrency)
            .min(n_conc_1)
            .min(n_conc_2)
            .min(*config.min_parts_for_concurrent_download as usize)
            .into()
    };
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
    fn retry_attempt(&self, location: &dyn fmt::Display, error: &CondowError, next_in: Duration) {
        match self {
            ProbeInternal::RequestProbe(p) => p.retry_attempt(location, error, next_in),
            ProbeInternal::FactoryAndRequestProbe(factory_probe, request_probe) => {
                factory_probe.retry_attempt(location, error, next_in);
                request_probe.retry_attempt(location, error, next_in);
            }
        }
    }

    #[inline]
    fn stream_resume_attempt(
        &self,
        location: &dyn fmt::Display,
        error: &IoError,
        orig_range: InclusiveRange,
        remaining_range: InclusiveRange,
    ) {
        match self {
            ProbeInternal::RequestProbe(p) => {
                p.stream_resume_attempt(location, error, orig_range, remaining_range)
            }
            ProbeInternal::FactoryAndRequestProbe(factory_probe, request_probe) => {
                factory_probe.stream_resume_attempt(location, error, orig_range, remaining_range);
                request_probe.stream_resume_attempt(location, error, orig_range, remaining_range);
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
