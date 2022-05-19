//! Spawns multiple [SequentialDownloader]s to download parts

use futures::StreamExt;

use crate::{
    condow_client::CondowClient,
    config::{ClientRetryWrapper, Config},
    machinery::{part_request::PartRequestIterator, DownloadSpanGuard},
    probe::Probe,
    streams::ChunkStream,
};

use self::parallel::ParallelDownloader;

use super::active_pull;

mod parallel;
mod three_concurrently;
mod two_concurrently;

/// Download the chunks concurrently
///
/// This has more overhead than downloading sequentially.
///
/// Debending on the level of concurrency the returned stream
/// will either poll chunks eagerly or has to be driven
/// by the consumer.
pub(crate) fn download_concurrently<C: CondowClient, P: Probe + Clone>(
    part_requests: PartRequestIterator,
    client: ClientRetryWrapper<C>,
    location: C::Location,
    probe: P,
    config: Config,
    download_span_guard: DownloadSpanGuard,
) -> ChunkStream {
    if *config.max_concurrency <= 2 {
        download_two_concurrently(
            part_requests,
            client,
            location,
            probe,
            config,
            download_span_guard,
        )
    } else if *config.max_concurrency == 3 {
        download_three_concurrently(
            part_requests,
            client,
            location,
            probe,
            config,
            download_span_guard,
        )
    } else {
        download_concurrently_parallel(
            part_requests,
            client,
            location,
            probe,
            config,
            download_span_guard,
        )
    }
}

/// Download the parst of a BLOB concurrently spawning tasks to create parallelism
fn download_concurrently_parallel<C: CondowClient, P: Probe + Clone>(
    part_requests: PartRequestIterator,
    client: ClientRetryWrapper<C>,
    location: C::Location,
    probe: P,
    config: Config,
    download_span_guard: DownloadSpanGuard,
) -> ChunkStream {
    let bytes_hint = part_requests.bytes_hint();

    let (chunk_stream, results_sender) = ChunkStream::new_channel_sink_pair(bytes_hint);
    tokio::spawn(async move {
        let mut downloader = ParallelDownloader::new(
            results_sender,
            client,
            config,
            location,
            probe,
            download_span_guard,
        );

        downloader.download(part_requests).await
    });
    chunk_stream
}

/// Download with a maximum concurrency of 2
fn download_two_concurrently<C: CondowClient, P: Probe + Clone>(
    part_requests: PartRequestIterator,
    client: ClientRetryWrapper<C>,
    location: C::Location,
    probe: P,
    config: Config,
    download_span_guard: DownloadSpanGuard,
) -> ChunkStream {
    let bytes_hint = part_requests.bytes_hint();
    let downloader = two_concurrently::TwoPartsConcurrently::from_client(
        client,
        location,
        part_requests,
        probe.clone(),
        config.log_download_messages_as_debug,
        download_span_guard,
    );

    if *config.ensure_active_pull {
        let active_stream = active_pull(downloader, probe, config);
        ChunkStream::from_receiver(active_stream, bytes_hint)
    } else {
        ChunkStream::from_stream(downloader.boxed(), bytes_hint)
    }
}

/// Download with a maximum concurrency of 3
fn download_three_concurrently<C: CondowClient, P: Probe + Clone>(
    part_requests: PartRequestIterator,
    client: ClientRetryWrapper<C>,
    location: C::Location,
    probe: P,
    config: Config,
    download_span_guard: DownloadSpanGuard,
) -> ChunkStream {
    let bytes_hint = part_requests.bytes_hint();
    let downloader = three_concurrently::ThreePartsConcurrently::from_client(
        client,
        location,
        part_requests,
        probe.clone(),
        config.log_download_messages_as_debug,
        download_span_guard,
    );

    if *config.ensure_active_pull {
        let active_stream = active_pull(downloader, probe, config);
        ChunkStream::from_receiver(active_stream, bytes_hint)
    } else {
        ChunkStream::from_stream(downloader.boxed(), bytes_hint)
    }
}
