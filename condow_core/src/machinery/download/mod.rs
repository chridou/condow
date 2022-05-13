//! Perform the actual download
//!
//! Downloads can be done concurrently or sequentially.

use tokio::sync::mpsc::UnboundedSender;

use crate::{
    condow_client::CondowClient,
    config::{ClientRetryWrapper, Config},
    errors::CondowError,
    probe::Probe,
    streams::{ChunkStream, ChunkStreamItem},
};

use super::{part_request::PartRequestIterator, DownloadSpanGuard};

mod concurrent;
mod sequential;

/// Download the chunks concurrently
///
/// This has more overhead than downloading sequentially.
pub(crate) async fn download_concurrently<C: CondowClient, P: Probe + Clone>(
    ranges: PartRequestIterator,
    n_concurrent: usize,
    results_sender: UnboundedSender<ChunkStreamItem>,
    client: ClientRetryWrapper<C>,
    config: Config,
    location: C::Location,
    probe: P,
    download_span_guard: DownloadSpanGuard,
) -> Result<(), ()> {
    self::concurrent::download_concurrently(
        ranges,
        n_concurrent,
        results_sender,
        client,
        config,
        location,
        probe,
        download_span_guard,
    )
    .await
}

/// Download the chunks sequentially.
///
/// This has less overhead than dowloading concurrently.
pub(crate) async fn download_chunks_sequentially<C: CondowClient, P: Probe + Clone>(
    part_requests: PartRequestIterator,
    client: ClientRetryWrapper<C>,
    location: C::Location,
    probe: P,
    config: Config,
) -> Result<ChunkStream, CondowError> {
    Ok(self::sequential::download_chunks_sequentially(
        part_requests,
        client,
        location,
        probe,
        config,
    )
    .await)
}
