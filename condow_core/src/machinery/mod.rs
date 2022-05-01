//! Streams for handling downloads
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, info, info_span, Span};

use crate::condow_client::CondowClient;
use crate::config::{ClientRetryWrapper, Config};
use crate::errors::{CondowError, IoError};
use crate::streams::{BytesHint, ChunkStream};
use crate::Probe;
use crate::{DownloadRange, InclusiveRange};

use self::range_stream::RangeStream;

mod download;
mod range_stream;

pub(crate) async fn download_range<C: CondowClient, DR: Into<DownloadRange>, P: Probe + Clone>(
    client: ClientRetryWrapper<C>,
    config: Config,
    location: C::Location,
    range: DR,
    probe: ProbeInternal<P>,
) -> Result<ChunkStream, CondowError> {
    let range = range.into();

    let parent = Span::current();
    // This span will track the lifetime of the whole download for all parts...
    let download_span = info_span!(parent: &parent, "download", %location, %range);
    let download_span_enter_guard = download_span.enter();
    let download_guard = DownloadSpanGuard::new(download_span.clone());

    info!("starting");

    // This span will track how long it takes to create the stream of chunks
    let get_stream_span = info_span!(parent: &download_span, "create_stream");
    let get_stream_guard = get_stream_span.enter();

    range.validate()?;

    let range = if let Some(range) = range.sanitized() {
        range
    } else {
        return Ok(ChunkStream::empty());
    };

    let (inclusive_range, bytes_hint) = match range {
        DownloadRange::Open(or) => {
            debug!(parent: &get_stream_span, "open range");
            let size = client.get_size(location.clone(), &probe).await?;
            if let Some(range) = or.incl_range_from_size(size) {
                (range, BytesHint::new_exact(range.len()))
            } else {
                return Ok(ChunkStream::empty());
            }
        }
        DownloadRange::Closed(cl) => {
            debug!(parent: &get_stream_span, "closed range");
            if config.always_get_size.into_inner() {
                debug!(parent: &get_stream_span, "get size enforced");
                let size = client.get_size(location.clone(), &probe).await?;
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

    let stream = download_chunks(
        client.clone(),
        location,
        inclusive_range,
        bytes_hint,
        config.clone(),
        probe.clone(),
        download_guard,
    )
    .await?;

    // Explicit drops for scope visualization
    drop(get_stream_guard);
    drop(download_span_enter_guard);

    Ok(stream)
}

async fn download_chunks<C: CondowClient, P: Probe + Clone>(
    client: ClientRetryWrapper<C>,
    location: C::Location,
    range: InclusiveRange,
    bytes_hint: BytesHint,
    config: Config,
    probe: ProbeInternal<P>,
    download_span_guard: DownloadSpanGuard,
) -> Result<ChunkStream, CondowError> {
    probe.effective_range(range);

    let (n_parts, ranges_stream) = RangeStream::create(range, config.part_size_bytes.into());

    debug!(parent: download_span_guard.span(), "downloading {n_parts} parts");

    if n_parts == 0 {
        panic!("n_parts must not be 0. This is a bug");
    }

    let (chunk_stream, sender) = ChunkStream::new(bytes_hint);

    if n_parts > usize::MAX as u64 {
        return Err(CondowError::new_other(
            "usize overflow while casting from u64",
        ));
    }
    let n_parts = n_parts as usize;

    tokio::spawn(async move {
        let result = download::download_concurrently(
            ranges_stream,
            config.max_concurrency.into_inner().min(n_parts),
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
    /// Nothing measured
    Off,
    OneStatic(P),
    OneDyn(Arc<dyn Probe>),
    Two(P, Arc<dyn Probe>),
}

#[cfg(test)]
impl<P: Probe + Clone> ProbeInternal<P> {
    pub fn new(probe: P) -> Self {
        Self::OneStatic(probe)
    }
}

impl<P: Probe + Clone> Probe for ProbeInternal<P> {
    #[inline]
    fn effective_range(&self, range: InclusiveRange) {
        match self {
            ProbeInternal::Off => {}
            ProbeInternal::OneDyn(p) => p.effective_range(range),
            ProbeInternal::OneStatic(p) => p.effective_range(range),
            ProbeInternal::Two(p1, p2) => {
                p1.effective_range(range);
                p2.effective_range(range);
            }
        }
    }

    #[inline]
    fn download_started(&self) {
        match self {
            ProbeInternal::Off => {}
            ProbeInternal::OneDyn(p) => p.download_started(),
            ProbeInternal::OneStatic(p) => p.download_started(),
            ProbeInternal::Two(p1, p2) => {
                p1.download_started();
                p2.download_started();
            }
        }
    }

    #[inline]
    fn download_completed(&self, time: Duration) {
        match self {
            ProbeInternal::Off => {}
            ProbeInternal::OneDyn(p) => p.download_completed(time),
            ProbeInternal::OneStatic(p) => p.download_completed(time),
            ProbeInternal::Two(p1, p2) => {
                p1.download_completed(time);
                p2.download_completed(time);
            }
        }
    }

    #[inline]
    fn download_failed(&self, time: Option<Duration>) {
        match self {
            ProbeInternal::Off => {}
            ProbeInternal::OneDyn(p) => p.download_failed(time),
            ProbeInternal::OneStatic(p) => p.download_failed(time),
            ProbeInternal::Two(p1, p2) => {
                p1.download_failed(time);
                p2.download_failed(time);
            }
        }
    }

    #[inline]
    fn retry_attempt(&self, location: &dyn fmt::Display, error: &CondowError, next_in: Duration) {
        match self {
            ProbeInternal::Off => {}
            ProbeInternal::OneDyn(p) => p.retry_attempt(location, error, next_in),
            ProbeInternal::OneStatic(p) => p.retry_attempt(location, error, next_in),
            ProbeInternal::Two(p1, p2) => {
                p1.retry_attempt(location, error, next_in);
                p2.retry_attempt(location, error, next_in);
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
            ProbeInternal::Off => {}
            ProbeInternal::OneDyn(p) => {
                p.stream_resume_attempt(location, error, orig_range, remaining_range)
            }
            ProbeInternal::OneStatic(p) => {
                p.stream_resume_attempt(location, error, orig_range, remaining_range)
            }
            ProbeInternal::Two(p1, p2) => {
                p1.stream_resume_attempt(location, error, orig_range, remaining_range);
                p2.stream_resume_attempt(location, error, orig_range, remaining_range);
            }
        }
    }

    #[inline]
    fn panic_detected(&self, msg: &str) {
        match self {
            ProbeInternal::Off => {}
            ProbeInternal::OneDyn(p) => p.panic_detected(msg),
            ProbeInternal::OneStatic(p) => p.panic_detected(msg),
            ProbeInternal::Two(p1, p2) => {
                p1.panic_detected(msg);
                p2.panic_detected(msg);
            }
        }
    }

    #[inline]
    fn queue_full(&self) {
        match self {
            ProbeInternal::Off => {}
            ProbeInternal::OneDyn(p) => p.queue_full(),
            ProbeInternal::OneStatic(p) => p.queue_full(),
            ProbeInternal::Two(p1, p2) => {
                p1.queue_full();
                p2.queue_full();
            }
        }
    }

    #[inline]
    fn chunk_completed(
        &self,
        part_index: u64,
        chunk_index: usize,
        n_bytes: usize,
        time: std::time::Duration,
    ) {
        match self {
            ProbeInternal::Off => {}
            ProbeInternal::OneDyn(p) => p.chunk_completed(part_index, chunk_index, n_bytes, time),
            ProbeInternal::OneStatic(p) => {
                p.chunk_completed(part_index, chunk_index, n_bytes, time)
            }
            ProbeInternal::Two(p1, p2) => {
                p1.chunk_completed(part_index, chunk_index, n_bytes, time);
                p2.chunk_completed(part_index, chunk_index, n_bytes, time);
            }
        }
    }

    #[inline]
    fn part_started(&self, part_index: u64, range: crate::InclusiveRange) {
        match self {
            ProbeInternal::Off => {}
            ProbeInternal::OneDyn(p) => p.part_started(part_index, range),
            ProbeInternal::OneStatic(p) => p.part_started(part_index, range),
            ProbeInternal::Two(p1, p2) => {
                p1.part_started(part_index, range);
                p2.part_started(part_index, range);
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
            ProbeInternal::Off => {}
            ProbeInternal::OneDyn(p) => p.part_completed(part_index, n_chunks, n_bytes, time),
            ProbeInternal::OneStatic(p) => p.part_completed(part_index, n_chunks, n_bytes, time),
            ProbeInternal::Two(p1, p2) => {
                p1.part_completed(part_index, n_chunks, n_bytes, time);
                p2.part_completed(part_index, n_chunks, n_bytes, time);
            }
        }
    }

    #[inline]
    fn part_failed(&self, error: &CondowError, part_index: u64, range: &InclusiveRange) {
        match self {
            ProbeInternal::Off => {}
            ProbeInternal::OneDyn(p) => p.part_failed(error, part_index, range),
            ProbeInternal::OneStatic(p) => p.part_failed(error, part_index, range),
            ProbeInternal::Two(p1, p2) => {
                p1.part_failed(error, part_index, range);
                p2.part_failed(error, part_index, range);
            }
        }
    }
}

impl From<Option<Arc<dyn Probe>>> for ProbeInternal<()> {
    fn from(probe: Option<Arc<dyn Probe>>) -> Self {
        match probe {
            Some(probe) => Self::OneDyn(probe),
            None => Self::Off,
        }
    }
}

impl<P: Probe + Clone> From<Option<P>> for ProbeInternal<P>
where
    P: Probe + Clone,
{
    fn from(probe: Option<P>) -> Self {
        match probe {
            Some(probe) => Self::OneStatic(probe),
            None => Self::Off,
        }
    }
}

impl Default for ProbeInternal<()> {
    fn default() -> Self {
        Self::Off
    }
}

#[cfg(test)]
mod tests;
