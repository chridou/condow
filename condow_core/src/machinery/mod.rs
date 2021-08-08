use crate::condow_client::CondowClient;
use crate::config::Config;
use crate::errors::DownloadRangeError;
use crate::streams::{BytesHint, ChunkStream};
use crate::InclusiveRange;

mod downloader;
mod range_stream;

pub async fn download<C: CondowClient>(
    client: C,
    location: C::Location,
    range: InclusiveRange,
    bytes_hint: BytesHint,
    config: Config,
) -> Result<ChunkStream, DownloadRangeError> {
    let (n_parts, ranges_stream) = range_stream::create(range, config.part_size_bytes.into());

    if n_parts == 0 {
        panic!("n_parts must not be 0. This is a bug");
    }

    let (chunk_stream, sender) = ChunkStream::new(n_parts, bytes_hint);

    tokio::spawn(async move {
        downloader::download_concurrently(
            ranges_stream,
            config.max_concurrency.into_inner().min(n_parts),
            sender,
            client,
            config,
            location,
        )
        .await
    });

    Ok(chunk_stream)
}
