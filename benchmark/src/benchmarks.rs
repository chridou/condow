use crate::{Benchmarks};


pub async fn run() -> Result<Benchmarks, anyhow::Error> {
    let mut benchmarks_collected = Benchmarks::new();

    condow_client::run(&mut benchmarks_collected).await?;
    chunk_stream_unordered::run(&mut benchmarks_collected).await?;


    Ok(benchmarks_collected)
}

mod condow_client {
    use std::time::Instant;

    use condow_core::condow_client::{CondowClient, DownloadSpec, IgnoreLocation};
    use futures::{future, TryStreamExt};

    use crate::{Benchmarks, create_client, Measurements};

    pub async fn run(benchmarks_collected: &mut Benchmarks) -> Result<(), anyhow::Error> {
        benchmarks_collected.add_measurements(many_chunks(100).await?);
        Ok(())
    }

    async fn many_chunks(num_iterations: usize) -> Result<Measurements, anyhow::Error> {
        let mut results = Measurements::new("client_query_chunks");


        let client = create_client();

        for _ in 0..num_iterations {
            let start = Instant::now();

            let (bytes_stream, _size_hint) = client.download(IgnoreLocation, DownloadSpec::Complete).await?;

            let bytes_read = bytes_stream.try_fold(0u64, |acc, chunk| future::ok(acc + chunk.len() as u64)).await?;            

            results.measurement(start, Instant::now());

            assert_eq!(bytes_read, client.get_size(IgnoreLocation).await?);
        }

        Ok(results)
    }
}

mod chunk_stream_unordered {
    use std::time::Instant;

    use condow_core::{condow_client::{CondowClient, DownloadSpec, IgnoreLocation}, config::{self, Config}};
    use futures::{future, TryStreamExt};

    use crate::{Benchmarks, create_client, Measurements};

    pub async fn run(benchmarks_collected: &mut Benchmarks) -> Result<(), anyhow::Error> {
        benchmarks_collected.add_measurements(consume_dump_no_concurrency(100).await?);
        benchmarks_collected.add_measurements(consume_dump_16_concurrency(100).await?);
       Ok(())
    }

    async fn consume_dump_no_concurrency(num_iterations: usize) -> Result<Measurements, anyhow::Error> {
        let mut results = Measurements::new("consume chunk stream - no concurrency");


        let client = create_client();
        let expected_byte_count = client.get_size(IgnoreLocation).await?;

        let config = Config::default().max_concurrency(1);
        let condow =client.condow(config)?;

        for _ in 0..num_iterations {
            let start = Instant::now();

            let bytes_read = condow.blob().download_chunks().await?.count_bytes().await?;

            results.measurement(start, Instant::now());

            assert_eq!(bytes_read, expected_byte_count);
        }

        Ok(results)
    }

    async fn consume_dump_16_concurrency(num_iterations: usize) -> Result<Measurements, anyhow::Error> {
        let mut results = Measurements::new("consume chunk stream - max 16 parallel");


        let client = create_client();
        let expected_byte_count = client.get_size(IgnoreLocation).await?;

        let config = Config::default().max_concurrency(16);
        let condow =client.condow(config)?;

        for _ in 0..num_iterations {
            let start = Instant::now();

            let bytes_read = condow.blob().download_chunks().await?.count_bytes().await?;

            results.measurement(start, Instant::now());

            assert_eq!(bytes_read, expected_byte_count);
        }

        Ok(results)
    }

}