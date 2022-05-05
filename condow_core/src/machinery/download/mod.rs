//! Perform the actual download

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use futures::channel::mpsc::UnboundedSender;

use crate::{
    condow_client::CondowClient,
    config::{ClientRetryWrapper, Config},
    errors::CondowError,
    probe::Probe,
    streams::{Chunk, ChunkStreamItem},
};

use super::{part_request::PartRequestIterator, DownloadSpanGuard, ProbeInternal};

mod concurrent;
mod sequential;

pub(crate) async fn download_concurrently<C: CondowClient, P: Probe + Clone>(
    ranges: PartRequestIterator,
    n_concurrent: usize,
    results_sender: UnboundedSender<ChunkStreamItem>,
    client: ClientRetryWrapper<C>,
    config: Config,
    location: C::Location,
    probe: ProbeInternal<P>,
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

pub(crate) async fn download_chunks_sequentially<C: CondowClient, P: Probe + Clone>(
    part_requests: PartRequestIterator,
    client: ClientRetryWrapper<C>,
    location: C::Location,
    probe: ProbeInternal<P>,
    sender: UnboundedSender<Result<Chunk, CondowError>>,
) {
    self::sequential::download_chunks_sequentially(part_requests, client, location, probe, sender)
        .await
}

/// Shared state to control cancellation of a download
#[derive(Clone)]
pub(crate) struct KillSwitch {
    is_pushed: Arc<AtomicBool>,
}

impl KillSwitch {
    pub fn new() -> Self {
        Self {
            is_pushed: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Check whether cancellation of the download was requested
    pub fn is_pushed(&self) -> bool {
        self.is_pushed.load(Ordering::SeqCst)
    }

    /// Request cancellation of the download
    pub fn push_the_button(&self) {
        self.is_pushed.store(true, Ordering::SeqCst)
    }
}
