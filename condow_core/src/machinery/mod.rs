//! Streams for handling downloads
use std::fmt::Display;

use tracing::span::{Entered, EnteredSpan};
use tracing::{debug, error, info, info_span, span, Level, Span, Instrument};

use crate::condow_client::CondowClient;
use crate::config::{ClientRetryWrapper, Config};
use crate::errors::CondowError;
use crate::streams::{BytesHint, ChunkStream};
use crate::Reporter;
use crate::{Condow, DownloadRange, GetSizeMode, InclusiveRange, StreamWithReport};

use self::range_stream::RangeStream;

mod download;
mod range_stream;

// pub async fn download<C: CondowClient, DR: Into<DownloadRange>, R: Reporter>(
//     condow: &Condow<C>,
//     location: C::Location,
//     range: DR,
//     get_size_mode: GetSizeMode,
//     reporter: R,
// ) -> Result<StreamWithReport<ChunkStream, R>, CondowError> {

//     let range = range.into();
//     let parent = Span::current();
//     let download_span = info_span!(parent: &parent, "download", %location, %range).entered();

//     info!(parent: &download_span, "starting");

//     download_range(condow, location, range, get_size_mode, reporter.clone())
//         .await
//         .map_err(|err| {
//             reporter.download_failed(None);
//             err
//         })
// }

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
    let guard = download_span.enter();

    info!(parent: &download_span, "starting");

    range.validate()?;

    let range = if let Some(range) = range.sanitized() {
        range
    } else {
        return Ok(StreamWithReport::new(ChunkStream::empty(), reporter));
    };

    let (inclusive_range, bytes_hint) = match range {
        DownloadRange::Open(or) => {
            debug!(parent: &download_span, "open range");
            let size = condow.client.get_size(location.clone(), &reporter).await?;
            if let Some(range) = or.incl_range_from_size(size) {
                (range, BytesHint::new_exact(range.len()))
            } else {
                return Ok(StreamWithReport::new(ChunkStream::empty(), reporter));
            }
        }
        DownloadRange::Closed(cl) => {
            debug!(parent: &download_span, "closed range");
            if get_size_mode.is_load_size_enforced(condow.config.always_get_size) {
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
    )
    .await?;

    drop(guard);

    Ok(StreamWithReport { reporter, stream })
}

async fn download_chunks<C: CondowClient, R: Reporter>(
    client: ClientRetryWrapper<C>,
    location: C::Location,
    range: InclusiveRange,
    bytes_hint: BytesHint,
    config: Config,
    reporter: R,
) -> Result<ChunkStream, CondowError> {
    let span = Span::current();
    let _guard = span.enter();

    reporter.effective_range(range);

    let (n_parts, ranges_stream) = RangeStream::create(range, config.part_size_bytes.into());
    debug!(parent: &span, "downloading {n_parts} parts");

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

    let download_parts_span = info_span!(parent: &span, "download_parts", n_parts = %n_parts);

    tokio::spawn(async move {
         let guard = download_parts_span.enter();
         let result = download::download_concurrently(
            ranges_stream,
            config.max_concurrency.into_inner().min(n_parts),
            sender,
            client,
            config,
            location,
            reporter,
        )
        .await;
        drop(guard);
        result
    });

    Ok(chunk_stream)
}

#[cfg(test)]
mod tests;
