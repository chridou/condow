//! Components for concurrent downloads

use futures::StreamExt;

use crate::{
    condow_client::CondowClient,
    config::ClientRetryWrapper,
    machinery::{configure_download::DownloadConfiguration, DownloadSpanGuard},
    probe::Probe,
    streams::{BytesHint, BytesStream, ChunkStream},
};

use self::parallel::ParallelDownloader;

use super::active_pull;

mod four_concurrently;
mod parallel;
mod three_concurrently;
mod two_concurrently;

pub use four_concurrently::FourPartsConcurrently;
pub use three_concurrently::ThreePartsConcurrently;
pub use two_concurrently::TwoPartsConcurrently;

/// Download the chunks concurrently
///
/// This has more overhead than downloading sequentially.
///
/// Debending on the level of concurrency the returned stream
/// will either poll chunks eagerly or has to be driven
/// by the consumer.
pub(crate) fn download_chunks_concurrently<C: CondowClient, P: Probe + Clone>(
    client: ClientRetryWrapper<C>,
    configuration: DownloadConfiguration<C::Location>,
    probe: P,
    download_span_guard: DownloadSpanGuard,
) -> ChunkStream {
    if configuration.max_concurrency() <= 2 {
        download_chunks_two_concurrently(client, configuration, probe, download_span_guard)
    } else if configuration.max_concurrency() == 3 {
        download_chunks_three_concurrently(client, configuration, probe, download_span_guard)
    } else if configuration.max_concurrency() == 4 {
        download_chunks_four_concurrently(client, configuration, probe, download_span_guard)
    } else {
        download_chunks_parallel(client, configuration, probe, download_span_guard)
    }
}

pub(crate) fn download_bytes_concurrently<C: CondowClient, P: Probe + Clone>(
    client: ClientRetryWrapper<C>,
    configuration: DownloadConfiguration<C::Location>,
    probe: P,
    download_span_guard: DownloadSpanGuard,
) -> BytesStream {
    let unordered_chunks = if configuration.max_concurrency() <= 2 {
        download_chunks_two_concurrently(client, configuration, probe, download_span_guard)
    } else if configuration.max_concurrency() == 3 {
        download_chunks_three_concurrently(client, configuration, probe, download_span_guard)
    } else if configuration.max_concurrency() == 4 {
        download_chunks_four_concurrently(client, configuration, probe, download_span_guard)
    } else {
        download_chunks_parallel(client, configuration, probe, download_span_guard)
    };

    unordered_chunks
        .try_into_ordered_chunk_stream()
        .expect("chunk stream to be fresh")
        .into_bytes_stream()
}

/// Download the parst of a BLOB concurrently spawning tasks to create parallelism
fn download_chunks_parallel<C: CondowClient, P: Probe + Clone>(
    client: ClientRetryWrapper<C>,
    configuration: DownloadConfiguration<C::Location>,
    probe: P,
    download_span_guard: DownloadSpanGuard,
) -> ChunkStream {
    let bytes_hint = BytesHint::new_exact(configuration.exact_bytes());

    let DownloadConfiguration {
        location,
        config,
        part_requests,
        ..
    } = configuration;

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
fn download_chunks_two_concurrently<C: CondowClient, P: Probe + Clone>(
    client: ClientRetryWrapper<C>,
    configuration: DownloadConfiguration<C::Location>,
    probe: P,
    download_span_guard: DownloadSpanGuard,
) -> ChunkStream {
    let bytes_hint = BytesHint::new_exact(configuration.exact_bytes());

    let DownloadConfiguration {
        location,
        config,
        part_requests,
        ..
    } = configuration;

    let log_dl_msg_as_dbg = config.log_download_messages_as_debug;

    let stream = two_concurrently::TwoPartsConcurrently::from_client(
        client,
        location,
        part_requests,
        probe.clone(),
        log_dl_msg_as_dbg,
        download_span_guard,
    );

    if *config.ensure_active_pull {
        let active_stream = active_pull(stream, probe, log_dl_msg_as_dbg);
        ChunkStream::from_receiver(active_stream, bytes_hint)
    } else {
        ChunkStream::from_two_concurrently(stream, bytes_hint)
    }
}

/// Download with a maximum concurrency of 3
fn download_chunks_three_concurrently<C: CondowClient, P: Probe + Clone>(
    client: ClientRetryWrapper<C>,
    configuration: DownloadConfiguration<C::Location>,
    probe: P,
    download_span_guard: DownloadSpanGuard,
) -> ChunkStream {
    let bytes_hint = BytesHint::new_exact(configuration.exact_bytes());

    let DownloadConfiguration {
        location,
        config,
        part_requests,
        ..
    } = configuration;

    let log_dl_msg_as_dbg = config.log_download_messages_as_debug;

    let stream = three_concurrently::ThreePartsConcurrently::from_client(
        client,
        location,
        part_requests,
        probe.clone(),
        log_dl_msg_as_dbg,
        download_span_guard,
    );

    if *config.ensure_active_pull {
        let active_stream = active_pull(stream, probe, log_dl_msg_as_dbg);
        ChunkStream::from_receiver(active_stream, bytes_hint)
    } else {
        ChunkStream::from_three_concurrently(stream, bytes_hint)
    }
}

/// Download with a maximum concurrency of 3
fn download_chunks_four_concurrently<C: CondowClient, P: Probe + Clone>(
    client: ClientRetryWrapper<C>,
    configuration: DownloadConfiguration<C::Location>,
    probe: P,
    download_span_guard: DownloadSpanGuard,
) -> ChunkStream {
    let bytes_hint = BytesHint::new_exact(configuration.exact_bytes());

    let DownloadConfiguration {
        location,
        config,
        part_requests,
        ..
    } = configuration;

    let log_dl_msg_as_dbg = config.log_download_messages_as_debug;

    let stream = four_concurrently::FourPartsConcurrently::from_client(
        client,
        location,
        part_requests,
        probe.clone(),
        log_dl_msg_as_dbg,
        download_span_guard,
    );

    if *config.ensure_active_pull {
        let active_stream = active_pull(stream, probe, log_dl_msg_as_dbg);
        ChunkStream::from_receiver(active_stream, bytes_hint)
    } else {
        ChunkStream::from_four_concurrently(stream, bytes_hint)
    }
}
