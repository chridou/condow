//! Streams for handling downloads
use std::sync::Arc;

use tracing::{debug, info, info_span, Span};

use crate::condow_client::CondowClient;
use crate::config::{ClientRetryWrapper, Config};
use crate::errors::CondowError;
use crate::streams::{BytesHint, ChunkStream};
use crate::Reporter;
use crate::{Condow, DownloadRange, GetSizeMode, InclusiveRange, StreamWithReport};

use self::range_stream::RangeStream;

mod download;
mod range_stream;

pub async fn download_range<C: CondowClient, DR: Into<DownloadRange>, R: Reporter>(
    condow: &Condow<C>,
    location: C::Location,
    range: DR,
    get_size_mode: GetSizeMode,
    reporter: R,
) -> Result<StreamWithReport<ChunkStream, R>, CondowError> {
    let range = range.into();

    let parent = Span::current();
    let download_span = info_span!(parent: &parent, "download", %location, %range);
    let download_span_enter_guard = download_span.enter();
    let download_guard = DownloadSpanGuard::new(download_span.clone());

    info!("starting");

    let get_stream_span = info_span!(parent: &download_span, "get_stream");
    let get_stream_guard = get_stream_span.enter();

    range.validate()?;

    let range = if let Some(range) = range.sanitized() {
        range
    } else {
        return Ok(StreamWithReport::new(ChunkStream::empty(), reporter));
    };

    let (inclusive_range, bytes_hint) = match range {
        DownloadRange::Open(or) => {
            debug!(parent: &get_stream_span, "open range");
            let size = condow.client.get_size(location.clone(), &reporter).await?;
            if let Some(range) = or.incl_range_from_size(size) {
                (range, BytesHint::new_exact(range.len()))
            } else {
                return Ok(StreamWithReport::new(ChunkStream::empty(), reporter));
            }
        }
        DownloadRange::Closed(cl) => {
            debug!(parent: &get_stream_span, "closed range");
            if get_size_mode.is_load_size_enforced(condow.config.always_get_size) {
                debug!(parent: &get_stream_span, "get size enforced");
                let size = condow.client.get_size(location.clone(), &reporter).await?;
                if let Some(range) = cl.incl_range_from_size(size) {
                    (range, BytesHint::new_exact(range.len()))
                } else {
                    return Ok(StreamWithReport::new(ChunkStream::empty(), reporter));
                }
            } else if let Some(range) = cl.incl_range() {
                (range, BytesHint::new_at_max(range.len()))
            } else {
                return Ok(StreamWithReport::new(ChunkStream::empty(), reporter));
            }
        }
    };

    let stream = download_chunks(
        condow.client.clone(),
        location,
        inclusive_range,
        bytes_hint,
        condow.config.clone(),
        reporter.clone(),
        download_guard,
    )
    .await?;

    drop(get_stream_guard);
    drop(download_span_enter_guard);

    Ok(StreamWithReport { reporter, stream })
}

async fn download_chunks<C: CondowClient, R: Reporter>(
    client: ClientRetryWrapper<C>,
    location: C::Location,
    range: InclusiveRange,
    bytes_hint: BytesHint,
    config: Config,
    reporter: R,
    download_span_guard: DownloadSpanGuard,
) -> Result<ChunkStream, CondowError> {
    reporter.effective_range(range);

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
            reporter,
            download_span_guard,
        )
        .await;
        result
    });

    Ok(chunk_stream)
}

/// Thus struct contains a span which must be kept alive until whole download is completed
/// which means that all parts have been downloaded.
#[derive(Clone)]
pub(crate) struct DownloadSpanGuard {
    span: Arc<Span>,
}

impl DownloadSpanGuard {
    pub fn new(span: Span) -> Self {
        Self {
            span: Arc::new(span),
        }
    }

    pub fn span(&self) -> &Span {
        &self.span
    }
}

#[cfg(test)]
mod tests;
