use std::time::{Duration, Instant};

use condow_core::config::Mebi;
use serde::{Deserialize, Serialize};

use self::scenarios::gen_scenarios;

pub async fn run(num_iterations: usize) -> Result<Benchmarks, anyhow::Error> {
    let mut benchmarks_collected = Benchmarks::new();

    let scenarios = gen_scenarios();

    condow_client::run(&mut benchmarks_collected).await?;
    for scenario in scenarios {
        chunk_stream_unordered::run(&scenario, num_iterations, &mut benchmarks_collected).await?;
        chunk_stream_ordered::run(&scenario, num_iterations, &mut benchmarks_collected).await?;
    }

    Ok(benchmarks_collected)
}

/// Measured timings for a benchmark
#[derive(Debug)]
pub struct Benchmark {
    name: String,
    timings: Vec<(Duration, u64)>,
}

impl Benchmark {
    pub fn new<T: Into<String>>(name: T) -> Self {
        Self {
            name: name.into(),
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
    pub avg_mebi_bytes_per_sec: f64,
    pub avg_ms: f64,
    pub first_ms: f64,
    pub min_ms: f64,
    pub max_ms: f64,
}

impl Stats {
    pub fn new(measurements: &Benchmark) -> Self {
        let first_ms = measurements.timings[0].0.as_secs_f64() * 1_000.0;
        let mut min_ms = f64::MAX;
        let mut max_ms = 0.0f64;
        let mut sum_ms = 0.0f64;
        let mut total_bytes = 0;

        measurements
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
            name: measurements.name.clone(),
            first_ms,
            min_ms,
            max_ms,
            avg_ms: sum_ms / measurements.timings.len() as f64,
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
        condow_client::IgnoreLocation,
        config::{Config, MaxConcurrency, Mebi, PartSizeBytes},
        Downloads,
    };

    use crate::client::BenchmarkClient;

    pub const BLOB_SIZE: u64 = Mebi(16).value();
    pub const CHUNK_SIZE: u64 = 7489; // prime

    const PART_SIZE_BYTES: &[PartSizeBytes] = &[
        PartSizeBytes::from_u64(BLOB_SIZE),
        PartSizeBytes::from_u64(BLOB_SIZE / 2),
        PartSizeBytes::from_u64(BLOB_SIZE / 16),
        PartSizeBytes::from_u64(BLOB_SIZE / 64),
    ];
    const MAX_CONCURRENCIES: &[MaxConcurrency] = &[
        MaxConcurrency::from_usize(1),
        MaxConcurrency::from_usize(2),
        MaxConcurrency::from_usize(16),
        MaxConcurrency::from_usize(64),
    ];

    pub fn gen_scenarios() -> Vec<Scenario> {
        let mut collected = Vec::new();
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
                    max_concurrency,
                    part_size,
                };

                collected.push(scenario);
            }
        }

        collected
    }

    /// A scenario is a configuration which is applied to (almost) all benchmarks
    #[derive(Debug, Clone)]
    pub struct Scenario {
        pub blob_size: u64,
        pub max_concurrency: MaxConcurrency,
        pub part_size: PartSizeBytes,
    }

    impl Scenario {
        pub fn gen_downloader(&self) -> impl Downloads<Location = IgnoreLocation> {
            let config = Config::default()
                .max_concurrency(self.max_concurrency)
                .part_size_bytes(self.part_size);
            let client = BenchmarkClient::new(self.blob_size, CHUNK_SIZE);
            client.condow(config).unwrap()
        }
    }

    impl fmt::Display for Scenario {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                f,
                "bs_{:05}k_mc_{:03}_ps_{:05}k",
                self.blob_size / 1_000,
                self.max_concurrency,
                u64::from(self.part_size) / 1_000
            )
        }
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
        fill_buffer(scenario, num_iterations, benchmarks_collected).await?;
        Ok(())
    }

    async fn count_bytes(
        scenario: &Scenario,
        num_iterations: usize,
        benchmarks_collected: &mut Benchmarks,
    ) -> Result<(), anyhow::Error> {
        let mut measurements = Benchmark::new(format!("chunk_stream_count_bytes_{scenario}"));

        let downloader = scenario.gen_downloader();

        let expected_byte_count = scenario.blob_size;

        for _ in 0..num_iterations {
            let start = Instant::now();

            let bytes_read = downloader
                .blob()
                .download_chunks()
                .await?
                .count_bytes()
                .await?;

            measurements.measured(start, Instant::now(), scenario.blob_size);

            assert_eq!(bytes_read, expected_byte_count);
        }

        benchmarks_collected.add_measurements(measurements);

        Ok(())
    }

    async fn fill_buffer(
        scenario: &Scenario,
        num_iterations: usize,
        benchmarks_collected: &mut Benchmarks,
    ) -> Result<(), anyhow::Error> {
        let mut measurements = Benchmark::new(format!("chunk_stream_fill_buffer_{scenario}"));

        let downloader = scenario.gen_downloader();

        let expected_byte_count = scenario.blob_size;
        let mut bytes_buffer: Vec<u8> = vec![0u8; scenario.blob_size as usize];

        for _ in 0..num_iterations {
            let start = Instant::now();

            let bytes_read = downloader
                .blob()
                .download_chunks()
                .await?
                .write_buffer(&mut bytes_buffer)
                .await?;

            measurements.measured(start, Instant::now(), scenario.blob_size);

            assert_eq!(bytes_read as u64, expected_byte_count);
        }

        benchmarks_collected.add_measurements(measurements);

        Ok(())
    }
}

mod chunk_stream_ordered {
    //! Benchmarks for [OrderedChunkStream](condow_core::streams::OrderedChumkStream)
    use std::time::Instant;

    use condow_core::condow_client::IgnoreLocation;
    use condow_core::reader::FetchAheadMode;
    use condow_core::Downloads;
    use futures::AsyncReadExt;

    use super::scenarios::{Scenario, CHUNK_SIZE};
    use super::{Benchmark, Benchmarks};

    pub async fn run(
        scenario: &Scenario,
        num_iterations: usize,
        benchmarks_collected: &mut Benchmarks,
    ) -> Result<(), anyhow::Error> {
        count_bytes(scenario, num_iterations, benchmarks_collected).await?;
        fill_buffer(scenario, num_iterations, benchmarks_collected).await?;
        random_access_reader(scenario, num_iterations, benchmarks_collected).await?;
        Ok(())
    }

    async fn count_bytes(
        scenario: &Scenario,
        num_iterations: usize,
        benchmarks_collected: &mut Benchmarks,
    ) -> Result<(), anyhow::Error> {
        let mut measurements =
            Benchmark::new(format!("chunk_stream_ordered_count_bytes_{scenario}"));

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

    async fn fill_buffer(
        scenario: &Scenario,
        num_iterations: usize,
        benchmarks_collected: &mut Benchmarks,
    ) -> Result<(), anyhow::Error> {
        let mut measurements =
            Benchmark::new(format!("chunk_stream_ordered_fill_buffer_{scenario}"));

        let downloader = scenario.gen_downloader();

        let expected_byte_count = scenario.blob_size;
        let mut bytes_buffer: Vec<u8> = vec![0u8; scenario.blob_size as usize];

        for _ in 0..num_iterations {
            let start = Instant::now();

            let bytes_read = downloader
                .blob()
                .download()
                .await?
                .write_buffer(&mut bytes_buffer)
                .await?;

            measurements.measured(start, Instant::now(), scenario.blob_size);

            assert_eq!(bytes_read as u64, expected_byte_count);
        }

        benchmarks_collected.add_measurements(measurements);

        Ok(())
    }

    async fn random_access_reader(
        scenario: &Scenario,
        num_iterations: usize,
        benchmarks_collected: &mut Benchmarks,
    ) -> Result<(), anyhow::Error> {
        let buf_sizes = &[1_024usize, 4_096, CHUNK_SIZE as usize];

        for &buf_size in buf_sizes {
            let mut measurements = Benchmark::new(format!(
                "chunk_stream_ordered_random_access_reader_buf_{buf_size}_{scenario}"
            ));

            let downloader = scenario.gen_downloader();

            let expected_byte_count = scenario.blob_size;
            let mut bytes_buffer: Vec<u8> = vec![0u8; buf_size];

            for _ in 0..num_iterations {
                let start = Instant::now();

                let mut reader = downloader.reader_with_length(IgnoreLocation, scenario.blob_size);
                reader.set_fetch_ahead_mode(FetchAheadMode::ToEnd);

                let mut bytes_read = 0;

                loop {
                    let read_into_buf = reader.read(&mut bytes_buffer).await?;
                    bytes_read += read_into_buf;

                    if read_into_buf == 0 {
                        break;
                    }
                }

                measurements.measured(start, Instant::now(), scenario.blob_size);

                assert_eq!(bytes_read as u64, expected_byte_count);
            }

            benchmarks_collected.add_measurements(measurements);
        }

        Ok(())
    }
}
mod condow_client {
    //! Benchmarks for the client which delivers byte streams.
    //!
    //! Ths is the [BenchmarkClient] used to deliver data for the other benchmarks
    use std::time::Instant;

    use condow_core::condow_client::{CondowClient, DownloadSpec, IgnoreLocation};
    use futures::{future, TryStreamExt};

    use crate::client::BenchmarkClient;

    use super::{
        scenarios::{BLOB_SIZE, CHUNK_SIZE},
        Benchmark, Benchmarks,
    };

    pub async fn run(benchmarks_collected: &mut Benchmarks) -> Result<(), anyhow::Error> {
        benchmarks_collected.add_measurements(many_chunks(100).await?);
        Ok(())
    }

    async fn many_chunks(num_iterations: usize) -> Result<Benchmark, anyhow::Error> {
        let mut results = Benchmark::new(format!("client_get_chunks_{:05}k", BLOB_SIZE / 1000));

        let client = BenchmarkClient::new(BLOB_SIZE, CHUNK_SIZE);

        for _ in 0..num_iterations {
            let start = Instant::now();

            let (bytes_stream, _size_hint) = client
                .download(IgnoreLocation, DownloadSpec::Complete)
                .await?;

            let bytes_read = bytes_stream
                .try_fold(0u64, |acc, chunk| future::ok(acc + chunk.len() as u64))
                .await?;

            results.measured(start, Instant::now(), BLOB_SIZE);

            assert_eq!(bytes_read, client.get_size(IgnoreLocation).await?);
        }

        Ok(results)
    }
}
