use crate::condow_client::CondowClient;
use crate::config::Config;
use crate::errors::DownloadRangeError;
use crate::streams::{BytesHint, ChunkStream};

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

    let total_bytes = end_incl - start + 1;
    let bytes_hint = BytesHint::new(0, None);

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

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use super::*;
    use crate::{test_utils::*, Condow};

    #[tokio::test]
    async fn download() {
        let mut data: Vec<u8> = Vec::new();

        for n in 1u8..=255 {
            let bytes = n.to_be_bytes();
            data.extend_from_slice(bytes.as_ref());
        }

        let data = Arc::new(data);

        for chunk_size in [1, 3, 5, 7] {
            let client = TestCondowClient {
                data: Arc::clone(&data),
                max_jitter_us: 3,
                include_size_hit: true,
                max_chunk_size: chunk_size,
            };

            for buffer_size in [0usize, 1, 2] {
                for part_size in [1usize, 2, 3, 5, 7, 10, 11] {
                    for len in [1usize, 2, 3, 5, 16, 17, 255, 256] {
                        for n_concurrency in [1usize, 2, 3, 5, 11] {
                            let config = Config::default()
                                .buffer_size(buffer_size)
                                .buffers_full_delay(Duration::from_micros(50))
                                .part_size_bytes(part_size)
                                .max_concurrency(n_concurrency);
                            let condow = Condow::new(client.clone(), config).unwrap();

                            let result_stream = condow.download_file(()).await.unwrap();

                            let result = result_stream.into_vec().await.unwrap();

                            assert_eq!(&result, data.as_ref());
                        }
                    }
                }
            }
        }
    }
}
