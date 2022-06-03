use std::time::{Duration, Instant};

use condow_core::config::Mebi;
use serde::{Deserialize, Serialize};

use self::scenarios::{gen_client_scenarios, gen_scenarios};

pub async fn run(num_iterations: usize) -> Result<Benchmarks, anyhow::Error> {
    let mut benchmarks_collected = Benchmarks::new();

    let scenarios = gen_client_scenarios();
    for (blob_size, chunk_size) in scenarios {
        condow_client::run(
            blob_size,
            chunk_size,
            num_iterations,
            &mut benchmarks_collected,
        )
        .await?;
    }

    let scenarios = gen_scenarios();
    for scenario in scenarios {
        raw::run(&scenario, num_iterations, &mut benchmarks_collected).await?;
        bytes_stream::run(&scenario, num_iterations, &mut benchmarks_collected).await?;
        chunk_stream_unordered::run(&scenario, num_iterations, &mut benchmarks_collected).await?;
        chunk_stream_ordered::run(&scenario, num_iterations, &mut benchmarks_collected).await?;
    }

    Ok(benchmarks_collected)
}

/// Measured timings for a benchmark
#[derive(Debug)]
pub struct Benchmark {
    name: String,
    scenario: String,
    timings: Vec<(Duration, u64)>,
}

impl Benchmark {
    pub fn new<T: Into<String>, S: ToString>(name: T, scenario: &S) -> Self {
        Self {
            name: name.into(),
            scenario: scenario.to_string(),
            timings: Vec::with_capacity(128),
        }
    }

    /// Add a new measured time
    pub fn measured(&mut self, start: Instant, end: Instant, bytes: u64) {
        self.timings.push((end - start, bytes));
    }

    /// Convert to [Stats]
    pub fn stats(&self) -> Stats {
        Stats::new(self)
    }
}

/// Simple statistical values to compare benchmarks
#[derive(Debug, Serialize, Deserialize)]
pub struct Stats {
    #[serde(rename = "#")]
    pub index: usize,
    pub name: String,
    pub scenario: String,
    pub avg_mebi_bytes_per_sec: f64,
    pub avg_ms: f64,
    pub first_ms: f64,
    pub min_ms: f64,
    pub max_ms: f64,
}

impl Stats {
    pub fn new(benchmark: &Benchmark) -> Self {
        let first_ms = benchmark.timings[0].0.as_secs_f64() * 1_000.0;
        let mut min_ms = f64::MAX;
        let mut max_ms = 0.0f64;
        let mut sum_ms = 0.0f64;
        let mut total_bytes = 0;

        benchmark
            .timings
            .iter()
            .map(|(time, bytes)| (time.as_secs_f64() * 1_000.0, bytes))
            .for_each(|(time_ms, bytes)| {
                min_ms = min_ms.min(time_ms);
                max_ms = max_ms.max(time_ms);
                sum_ms += time_ms;
                total_bytes += bytes;
            });

        let avg_mebi_bytes_per_sec =
            (total_bytes as f64 / Mebi(1).value() as f64) / (sum_ms / 1_000.0);

        Self {
            index: 0,
            name: benchmark.name.clone(),
            scenario: benchmark.scenario.to_string(),
            first_ms,
            min_ms,
            max_ms,
            avg_ms: sum_ms / benchmark.timings.len() as f64,
            avg_mebi_bytes_per_sec,
        }
    }
}

/// Simply multiple [Benchmark]s
#[derive(Debug)]
pub struct Benchmarks {
    benchmarks: Vec<Benchmark>,
}

impl Benchmarks {
    pub fn new() -> Self {
        Benchmarks {
            benchmarks: Vec::new(),
        }
    }

    pub fn add_measurements(&mut self, measurements: Benchmark) {
        self.benchmarks.push(measurements);
    }

    pub fn stats(&self) -> Vec<Stats> {
        self.benchmarks
            .iter()
            .enumerate()
            .map(|(idx, m)| {
                let mut stats = m.stats();
                stats.index = idx + 1;
                stats
            })
            .collect()
    }
}

mod scenarios {
    use std::fmt;

    use condow_core::{
        condow_client::{CondowClient, IgnoreLocation},
        config::{Config, MaxConcurrency, Mebi, PartSizeBytes},
        Downloads,
    };

    use crate::client::BenchmarkClient;

    pub const BLOB_SIZE: u64 = Mebi(32).value();

    const PART_SIZE_BYTES: &[PartSizeBytes] = &[
        PartSizeBytes::from_u64(BLOB_SIZE),
        PartSizeBytes::from_u64(BLOB_SIZE / 2),
        PartSizeBytes::from_u64(BLOB_SIZE / 8),
        PartSizeBytes::from_u64(BLOB_SIZE / 32),
        PartSizeBytes::from_u64(BLOB_SIZE / 128),
        PartSizeBytes::from_u64(BLOB_SIZE / 512),
    ];
    const MAX_CONCURRENCIES: &[MaxConcurrency] = &[
        MaxConcurrency::from_usize(1),
        MaxConcurrency::from_usize(2),
        MaxConcurrency::from_usize(3),
        MaxConcurrency::from_usize(4),
        MaxConcurrency::from_usize(5), // forced tasks from here on
        MaxConcurrency::from_usize(8),
        MaxConcurrency::from_usize(16),
        MaxConcurrency::from_usize(32),
    ];
    const CHUNK_SIZES: &[u64] = &[/*512,*/ 1_024 /*65535*/];

    pub fn gen_client_scenarios() -> Vec<(u64, u64)> {
        let mut collected = Vec::new();
        for &chunk_size in CHUNK_SIZES {
            collected.push((BLOB_SIZE, chunk_size));
        }
        collected
    }

    pub fn gen_scenarios() -> Vec<Scenario> {
        let mut collected = Vec::new();
        for &chunk_size in CHUNK_SIZES {
            for &part_size in PART_SIZE_BYTES {
                for &max_concurrency in MAX_CONCURRENCIES {
                    let num_parts: u64 = BLOB_SIZE / part_size.into_inner();
                    if num_parts < max_concurrency.into_inner() as u64 {
                        // Makes no sense since there can
                        // not be "more concurrency" than parts.
                        continue;
                    }

                    let scenario = Scenario {
                        blob_size: BLOB_SIZE,
                        chunk_size,
                        max_concurrency,
                        part_size,
                    };

                    collected.push(scenario);
                }
            }
        }

        collected
    }

    /// A scenario is a configuration which is applied to (almost) all benchmarks
    #[derive(Debug, Clone)]
    pub struct Scenario {
        pub blob_size: u64,
        pub chunk_size: u64,
        pub max_concurrency: MaxConcurrency,
        pub part_size: PartSizeBytes,
    }

    impl Scenario {
        pub fn gen_downloader(&self) -> impl Downloads<Location = IgnoreLocation> {
            let config = Config::default()
                .max_concurrency(self.max_concurrency)
                .part_size_bytes(self.part_size);
            let client = BenchmarkClient::new(self.blob_size, self.chunk_size);
            client.condow(config).unwrap()
        }

        pub fn gen_client(&self) -> impl CondowClient<Location = IgnoreLocation> {
            BenchmarkClient::new(self.blob_size, self.chunk_size)
        }
    }

    impl fmt::Display for Scenario {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                f,
                "bs_{:05}k_cs_{:05}_mc_{:03}_ps_{:05}k",
                self.blob_size / 1_000,
                self.chunk_size,
                self.max_concurrency,
                u64::from(self.part_size) / 1_000
            )
        }
    }
}

mod bytes_stream {
    //! Benchmarks for [ChunkStream](condow_core::streams::ChumkStream)
    use std::time::Instant;

    use condow_core::Downloads;

    use super::{scenarios::Scenario, Benchmark, Benchmarks};

    pub async fn run(
        scenario: &Scenario,
        num_iterations: usize,
        benchmarks_collected: &mut Benchmarks,
    ) -> Result<(), anyhow::Error> {
        count_bytes(scenario, num_iterations, benchmarks_collected).await?;
        count_bytes_no_retries(scenario, num_iterations, benchmarks_collected).await?;
        active_pull_count_bytes(scenario, num_iterations, benchmarks_collected).await?;
        Ok(())
    }

    async fn count_bytes(
        scenario: &Scenario,
        num_iterations: usize,
        benchmarks_collected: &mut Benchmarks,
    ) -> Result<(), anyhow::Error> {
        let mut measurements = Benchmark::new("bytes_stream_count_bytes", scenario);

        let downloader = scenario.gen_downloader();

        let expected_byte_count = scenario.blob_size;

        for _ in 0..num_iterations {
            let start = Instant::now();

            let bytes_read = downloader.blob().download().await?.count_bytes().await?;

            measurements.measured(start, Instant::now(), scenario.blob_size);

            assert_eq!(bytes_read, expected_byte_count);
        }

        benchmarks_collected.add_measurements(measurements);

        Ok(())
    }
    async fn count_bytes_no_retries(
        scenario: &Scenario,
        num_iterations: usize,
        benchmarks_collected: &mut Benchmarks,
    ) -> Result<(), anyhow::Error> {
        let mut measurements = Benchmark::new("bytes_stream_no_retries_count_bytes", scenario);

        let downloader = scenario.gen_downloader();

        let expected_byte_count = scenario.blob_size;

        for _ in 0..num_iterations {
            let start = Instant::now();

            let bytes_read = downloader
                .blob()
                .reconfigure(|config| config.disable_retries())
                .download()
                .await?
                .count_bytes()
                .await?;

            measurements.measured(start, Instant::now(), scenario.blob_size);

            assert_eq!(bytes_read, expected_byte_count);
        }

        benchmarks_collected.add_measurements(measurements);

        Ok(())
    }

    async fn active_pull_count_bytes(
        scenario: &Scenario,
        num_iterations: usize,
        benchmarks_collected: &mut Benchmarks,
    ) -> Result<(), anyhow::Error> {
        let mut measurements = Benchmark::new("bytes_stream_active_pull_count_bytes", scenario);

        let downloader = scenario.gen_downloader();

        let expected_byte_count = scenario.blob_size;

        for _ in 0..num_iterations {
            let start = Instant::now();

            let bytes_read = downloader
                .blob()
                .reconfigure(|c| c.ensure_active_pull(true))
                .download()
                .await?
                .count_bytes()
                .await?;

            measurements.measured(start, Instant::now(), scenario.blob_size);

            assert_eq!(bytes_read, expected_byte_count);
        }

        benchmarks_collected.add_measurements(measurements);

        Ok(())
    }
}

mod chunk_stream_unordered {
    //! Benchmarks for [ChunkStream](condow_core::streams::ChumkStream)
    use std::time::Instant;

    use condow_core::Downloads;

    use super::{scenarios::Scenario, Benchmark, Benchmarks};

    pub async fn run(
        scenario: &Scenario,
        num_iterations: usize,
        benchmarks_collected: &mut Benchmarks,
    ) -> Result<(), anyhow::Error> {
        count_bytes(scenario, num_iterations, benchmarks_collected).await?;
        count_bytes_no_retries(scenario, num_iterations, benchmarks_collected).await?;
        active_pull_count_bytes(scenario, num_iterations, benchmarks_collected).await?;
        Ok(())
    }

    async fn count_bytes(
        scenario: &Scenario,
        num_iterations: usize,
        benchmarks_collected: &mut Benchmarks,
    ) -> Result<(), anyhow::Error> {
        let mut measurements = Benchmark::new("chunk_stream_count_bytes", scenario);

        let downloader = scenario.gen_downloader();

        let expected_byte_count = scenario.blob_size;

        for _ in 0..num_iterations {
            let start = Instant::now();

            let bytes_read = downloader
                .blob()
                .download_chunks_unordered()
                .await?
                .count_bytes()
                .await?;

            measurements.measured(start, Instant::now(), scenario.blob_size);

            assert_eq!(bytes_read, expected_byte_count);
        }

        benchmarks_collected.add_measurements(measurements);

        Ok(())
    }
    async fn count_bytes_no_retries(
        scenario: &Scenario,
        num_iterations: usize,
        benchmarks_collected: &mut Benchmarks,
    ) -> Result<(), anyhow::Error> {
        let mut measurements = Benchmark::new("chunk_stream_no_retries_count_bytes", scenario);

        let downloader = scenario.gen_downloader();

        let expected_byte_count = scenario.blob_size;

        for _ in 0..num_iterations {
            let start = Instant::now();

            let bytes_read = downloader
                .blob()
                .reconfigure(|config| config.disable_retries())
                .download_chunks_unordered()
                .await?
                .count_bytes()
                .await?;

            measurements.measured(start, Instant::now(), scenario.blob_size);

            assert_eq!(bytes_read, expected_byte_count);
        }

        benchmarks_collected.add_measurements(measurements);

        Ok(())
    }

    async fn active_pull_count_bytes(
        scenario: &Scenario,
        num_iterations: usize,
        benchmarks_collected: &mut Benchmarks,
    ) -> Result<(), anyhow::Error> {
        let mut measurements = Benchmark::new("chunk_stream_active_pull_count_bytes", scenario);

        let downloader = scenario.gen_downloader();

        let expected_byte_count = scenario.blob_size;

        for _ in 0..num_iterations {
            let start = Instant::now();

            let bytes_read = downloader
                .blob()
                .reconfigure(|c| c.ensure_active_pull(true))
                .download_chunks_unordered()
                .await?
                .count_bytes()
                .await?;

            measurements.measured(start, Instant::now(), scenario.blob_size);

            assert_eq!(bytes_read, expected_byte_count);
        }

        benchmarks_collected.add_measurements(measurements);

        Ok(())
    }
}

mod chunk_stream_ordered {
    //! Benchmarks for [OrderedChunkStream](condow_core::streams::OrderedChumkStream)
    use std::time::Instant;

    use condow_core::Downloads;

    use super::scenarios::Scenario;
    use super::{Benchmark, Benchmarks};

    pub async fn run(
        scenario: &Scenario,
        num_iterations: usize,
        benchmarks_collected: &mut Benchmarks,
    ) -> Result<(), anyhow::Error> {
        count_bytes(scenario, num_iterations, benchmarks_collected).await?;
        Ok(())
    }

    async fn count_bytes(
        scenario: &Scenario,
        num_iterations: usize,
        benchmarks_collected: &mut Benchmarks,
    ) -> Result<(), anyhow::Error> {
        let mut measurements = Benchmark::new("chunk_stream_ordered_count_bytes", scenario);

        let downloader = scenario.gen_downloader();

        let expected_byte_count = scenario.blob_size;

        for _ in 0..num_iterations {
            let start = Instant::now();

            let bytes_read = downloader
                .blob()
                .download_chunks_ordered()
                .await?
                .count_bytes()
                .await?;

            measurements.measured(start, Instant::now(), scenario.blob_size);

            assert_eq!(bytes_read, expected_byte_count);
        }

        benchmarks_collected.add_measurements(measurements);

        Ok(())
    }
}

mod raw {
    //! Manual implementation with almost no overhead to compare against

    use std::time::Instant;

    use condow_core::{
        condow_client::{CondowClient, IgnoreLocation},
        probe::Probe,
        streams::{BytesHint, BytesStreamItem, Chunk, ChunkStreamItem, OrderedChunkStream},
        InclusiveRange,
    };
    use futures::{future, Stream, TryStreamExt};

    use super::{scenarios::Scenario, Benchmark, Benchmarks};

    pub async fn run(
        scenario: &Scenario,
        num_iterations: usize,
        benchmarks_collected: &mut Benchmarks,
    ) -> Result<(), anyhow::Error> {
        map_to_chunks_count_bytes(scenario, num_iterations, benchmarks_collected).await?;
        map_to_chunks_ordered_count_bytes(scenario, num_iterations, benchmarks_collected).await?;
        Ok(())
    }

    async fn map_to_chunks_count_bytes(
        scenario: &Scenario,
        num_iterations: usize,
        benchmarks_collected: &mut Benchmarks,
    ) -> Result<(), anyhow::Error> {
        if scenario.max_concurrency.into_inner() > 1
            || scenario.part_size.into_inner() != scenario.blob_size
        {
            // 1 part purely sequentially
            return Ok(());
        }

        let mut measurements = Benchmark::new("raw_map_to_chunks_count_bytes", scenario);

        let client = scenario.gen_client();

        let expected_byte_count = scenario.blob_size;

        for _ in 0..num_iterations {
            let start = Instant::now();

            let bytes_stream = client
                .download(IgnoreLocation, InclusiveRange(0, scenario.blob_size - 1))
                .await?;

            let chunks_stream = make_chunks_stream(bytes_stream, scenario, ());

            let bytes_read = chunks_stream
                .try_fold(0u64, |acc, chunk| future::ok(acc + chunk.len() as u64))
                .await?;

            measurements.measured(start, Instant::now(), scenario.blob_size);

            assert_eq!(bytes_read, expected_byte_count);
        }

        benchmarks_collected.add_measurements(measurements);

        Ok(())
    }

    async fn map_to_chunks_ordered_count_bytes(
        scenario: &Scenario,
        num_iterations: usize,
        benchmarks_collected: &mut Benchmarks,
    ) -> Result<(), anyhow::Error> {
        if scenario.max_concurrency.into_inner() > 1
            || scenario.part_size.into_inner() != scenario.blob_size
        {
            // 1 part purely sequentially
            return Ok(());
        }

        let mut measurements = Benchmark::new("raw_map_to_chunks_ordered_count_bytes", scenario);

        let client = scenario.gen_client();

        let expected_byte_count = scenario.blob_size;

        for _ in 0..num_iterations {
            let start = Instant::now();

            let bytes_stream = client
                .download(IgnoreLocation, InclusiveRange(0, scenario.blob_size - 1))
                .await?;

            let chunks_stream = make_chunks_stream(bytes_stream, scenario, ());
            let ordered_stream =
                OrderedChunkStream::new(chunks_stream, BytesHint::new_exact(scenario.blob_size));

            let bytes_read = ordered_stream.count_bytes().await?;

            measurements.measured(start, Instant::now(), scenario.blob_size);

            assert_eq!(bytes_read, expected_byte_count);
        }

        benchmarks_collected.add_measurements(measurements);

        Ok(())
    }

    fn make_chunks_stream<St, P>(
        bytes_stream: St,
        scenario: &Scenario,
        probe: P,
    ) -> impl Stream<Item = ChunkStreamItem> + Send + 'static
    where
        St: Stream<Item = BytesStreamItem> + Send + 'static,
        P: Probe,
    {
        let mut chunk_index = 0;
        let mut range_offset = 0;
        let mut blob_offset = 0;
        let mut bytes_left = scenario.blob_size;

        bytes_stream
            .map_ok(move |bytes| {
                let bytes_len = bytes.len() as u64;
                bytes_left -= bytes_len;
                let chunk = Chunk {
                    part_index: 0,
                    chunk_index,
                    blob_offset,
                    range_offset,
                    bytes,
                    bytes_left,
                };

                probe.chunk_received(0, chunk_index, bytes_len as usize);

                chunk_index += 1;
                blob_offset += bytes_len;
                range_offset += bytes_len;
                chunk
            })
            .map_err(Into::into)
    }
}
mod condow_client {
    //! Benchmarks for the client which delivers byte streams.
    //!
    //! Ths is the [BenchmarkClient] used to deliver data for the other benchmarks
    use std::time::Instant;

    use condow_core::{
        condow_client::{CondowClient, IgnoreLocation},
        InclusiveRange,
    };
    use futures::{future, TryStreamExt};

    use crate::client::BenchmarkClient;

    use super::{Benchmark, Benchmarks};

    pub async fn run(
        blob_size: u64,
        chunk_size: u64,
        num_iterations: usize,
        benchmarks_collected: &mut Benchmarks,
    ) -> Result<(), anyhow::Error> {
        benchmarks_collected
            .add_measurements(many_chunks(blob_size, chunk_size, num_iterations).await?);
        Ok(())
    }

    async fn many_chunks(
        blob_size: u64,
        chunk_size: u64,
        num_iterations: usize,
    ) -> Result<Benchmark, anyhow::Error> {
        let mut results = Benchmark::new(
            "client_get_chunks",
            &format!("bs_{:05}k_cs_{:05}", blob_size / 1_000, chunk_size),
        );

        let client = BenchmarkClient::new(blob_size, chunk_size);

        for _ in 0..num_iterations {
            let start = Instant::now();

            let bytes_stream = client
                .download(IgnoreLocation, InclusiveRange(0, blob_size - 1))
                .await?;

            let bytes_read = bytes_stream
                .try_fold(0u64, |acc, chunk| future::ok(acc + chunk.len() as u64))
                .await?;

            results.measured(start, Instant::now(), blob_size);

            assert_eq!(bytes_read, client.get_size(IgnoreLocation).await?);
        }

        Ok(results)
    }
}
