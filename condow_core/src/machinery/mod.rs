use crate::condow_client::CondowClient;
use crate::config::Config;
use crate::errors::DownloadRangeError;
use crate::streams::ChunkStream;

mod downloader;
mod range_stream;

pub async fn download<C: CondowClient>(
    client: C,
    location: C::Location,
    start: usize,
    end_incl: usize,
    config: Config,
) -> Result<ChunkStream, DownloadRangeError> {
    let (n_parts, ranges_stream) =
        range_stream::create(config.part_size_bytes.into(), start, end_incl);

    if n_parts == 0 {
        panic!("n_parts must not be 0. This is a bug");
    }

    let total_bytes = start - end_incl + 1;

    let (chunk_stream, sender) = ChunkStream::new(n_parts, Some(total_bytes));

    tokio::spawn(async move {
        downloader::download_concurrently(
            ranges_stream,
            config.max_concurrency.0.min(n_parts),
            sender,
            client,
            config,
            location,
        )
        .await
    });

    Ok(chunk_stream)
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::test_utils::*;

//     #[tokio_test]
//     async fn download() {

//     }
// }
