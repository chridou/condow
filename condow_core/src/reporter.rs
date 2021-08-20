use std::time::Duration;

use crate::InclusiveRange;

pub use simple_reporter::*;

pub trait ReporterFactory: Send + Sync + 'static {
    type ReporterType: Reporter;

    fn make(&self) -> Self::ReporterType;
}

#[allow(unused_variables)]
pub trait Reporter: Clone + Send + Sync + 'static {
    /// The effective range which is downloaded (and split into parts)
    fn effective_range(&self, range: InclusiveRange) {}
    /// The actual IO started
    fn download_started(&self) {}
    /// IO tasks finished
    fn download_finished(&self) {}
    /// All queues are full so no new request could be scheduled
    fn queue_full(&self) {}
    /// A part was completed
    fn chunk_completed(&self, part_index: usize, n_bytes: usize, time: Duration) {}
    /// Download of a part has started
    fn part_started(&self, part_index: usize, range: InclusiveRange) {}
    /// Download of a part was completed
    fn part_completed(&self, part_index: usize, n_chunks: usize, n_bytes: usize, time: Duration) {}
}

#[derive(Copy, Clone)]
pub struct NoReporter;

impl Reporter for NoReporter {}
impl ReporterFactory for NoReporter {
    type ReporterType = Self;

    fn make(&self) -> Self {
        NoReporter
    }
}

mod simple_reporter {
    use std::{
        sync::{
            atomic::{AtomicU64, AtomicUsize, Ordering},
            Arc, Mutex,
        },
        time::{Duration, Instant},
    };

    use super::{Reporter, ReporterFactory};

    pub struct SimpleReporterFactory;
    impl ReporterFactory for SimpleReporterFactory {
        type ReporterType = SimpleReporter;

        fn make(&self) -> Self::ReporterType {
            SimpleReporter::new()
        }
    }

    #[derive(Clone)]
    pub struct SimpleReporter {
        inner: Arc<Inner>,
    }

    impl SimpleReporter {
        pub fn new() -> Self {
            SimpleReporter {
                inner: Arc::new(Inner::new()),
            }
        }

        pub fn report(&self) -> SimpleReport {
            let inner = self.inner.as_ref();
            let download_time = *inner.download_finished_at.lock().unwrap()
                - *inner.download_started_at.lock().unwrap();
            let n_bytes_received = inner.n_bytes_received.load(Ordering::SeqCst);
            let bytes_per_second_f64 = if n_bytes_received > 0 {
                n_bytes_received as f64 / download_time.as_secs_f64()
            } else {
                0.0
            };

            SimpleReport {
                download_time,
                bytes_per_second: bytes_per_second_f64 as u64,
                megabytes_per_second: (bytes_per_second_f64 / 1_000_000.0) as u64,
                mebibytes_per_second: (bytes_per_second_f64 / 1_048_576.0) as u64,
                gigabits_per_second: (bytes_per_second_f64 / 1_000_000_000.0) as u64 * 8,
                gibibits_per_second: ((bytes_per_second_f64 * 8.0) / 1_073_741_824.0) as u64,
                n_queue_full: inner.n_queue_full.load(Ordering::SeqCst),
                n_bytes_received,
                n_chunks_received: inner.n_chunks_received.load(Ordering::SeqCst),
                n_parts_received: inner.n_parts_received.load(Ordering::SeqCst),
                min_chunk_bytes: inner.min_chunk_bytes.load(Ordering::SeqCst),
                max_chunk_bytes: inner.max_chunk_bytes.load(Ordering::SeqCst),
                min_chunk_time: Duration::from_millis(inner.min_chunk_ms.load(Ordering::SeqCst)),
                max_chunk_time: Duration::from_millis(inner.max_chunk_ms.load(Ordering::SeqCst)),
                min_part_bytes: inner.min_part_bytes.load(Ordering::SeqCst),
                max_part_bytes: inner.max_part_bytes.load(Ordering::SeqCst),
                min_chunks_per_part: inner.min_chunks_per_part.load(Ordering::SeqCst),
                max_chunks_per_part: inner.max_chunks_per_part.load(Ordering::SeqCst),
                min_part_time: Duration::from_millis(inner.min_part_ms.load(Ordering::SeqCst)),
                max_part_time: Duration::from_millis(inner.max_part_ms.load(Ordering::SeqCst)),
            }
        }
    }

    impl Default for SimpleReporter {
        fn default() -> Self {
            Self::new()
        }
    }

    #[derive(Debug, Clone)]
    pub struct SimpleReport {
        pub download_time: Duration,
        pub bytes_per_second: u64,
        pub megabytes_per_second: u64,
        pub mebibytes_per_second: u64,
        pub gigabits_per_second: u64,
        pub gibibits_per_second: u64,
        pub n_queue_full: usize,
        pub n_bytes_received: usize,
        pub n_chunks_received: usize,
        pub n_parts_received: usize,
        pub min_chunk_bytes: usize,
        pub max_chunk_bytes: usize,
        pub min_chunk_time: Duration,
        pub max_chunk_time: Duration,
        pub min_part_bytes: usize,
        pub max_part_bytes: usize,
        pub min_chunks_per_part: usize,
        pub max_chunks_per_part: usize,
        pub min_part_time: Duration,
        pub max_part_time: Duration,
    }

    impl Reporter for SimpleReporter {
        fn download_started(&self) {
            *self.inner.download_started_at.lock().unwrap() = Instant::now();
        }

        fn download_finished(&self) {
            *self.inner.download_finished_at.lock().unwrap() = Instant::now();
        }

        fn queue_full(&self) {
            self.inner.n_queue_full.fetch_add(1, Ordering::SeqCst);
        }

        fn chunk_completed(&self, _part_index: usize, n_bytes: usize, time: Duration) {
            let inner = self.inner.as_ref();
            inner.n_chunks_received.fetch_add(1, Ordering::SeqCst);
            inner.min_chunk_bytes.fetch_min(n_bytes, Ordering::SeqCst);
            inner.max_chunk_bytes.fetch_max(n_bytes, Ordering::SeqCst);
            let ms = time.as_millis() as u64;
            inner.min_chunk_ms.fetch_min(ms, Ordering::SeqCst);
            inner.max_chunk_ms.fetch_max(ms, Ordering::SeqCst);
        }

        fn part_completed(
            &self,
            _part_index: usize,
            n_chunks: usize,
            n_bytes: usize,
            time: Duration,
        ) {
            let inner = self.inner.as_ref();
            inner.n_parts_received.fetch_add(1, Ordering::SeqCst);
            inner.n_bytes_received.fetch_add(n_bytes, Ordering::SeqCst);
            inner.min_part_bytes.fetch_min(n_bytes, Ordering::SeqCst);
            inner.max_part_bytes.fetch_max(n_bytes, Ordering::SeqCst);
            inner
                .min_chunks_per_part
                .fetch_min(n_chunks, Ordering::SeqCst);
            inner
                .max_chunks_per_part
                .fetch_max(n_chunks, Ordering::SeqCst);
            let ms = time.as_millis() as u64;
            inner.min_part_ms.fetch_min(ms, Ordering::SeqCst);
            inner.max_part_ms.fetch_max(ms, Ordering::SeqCst);
        }
    }

    struct Inner {
        download_started_at: Mutex<Instant>,
        download_finished_at: Mutex<Instant>,
        n_queue_full: AtomicUsize,
        n_bytes_received: AtomicUsize,
        n_chunks_received: AtomicUsize,
        n_parts_received: AtomicUsize,
        min_chunk_bytes: AtomicUsize,
        max_chunk_bytes: AtomicUsize,
        min_chunk_ms: AtomicU64,
        max_chunk_ms: AtomicU64,
        min_part_bytes: AtomicUsize,
        max_part_bytes: AtomicUsize,
        min_chunks_per_part: AtomicUsize,
        max_chunks_per_part: AtomicUsize,
        min_part_ms: AtomicU64,
        max_part_ms: AtomicU64,
    }

    impl Inner {
        fn new() -> Self {
            Inner {
                download_started_at: Mutex::new(Instant::now()),
                download_finished_at: Mutex::new(Instant::now()),
                n_bytes_received: AtomicUsize::new(0),
                n_chunks_received: AtomicUsize::new(0),
                n_parts_received: AtomicUsize::new(0),
                n_queue_full: AtomicUsize::new(0),
                min_chunk_bytes: AtomicUsize::new(usize::MAX),
                max_chunk_bytes: AtomicUsize::new(0),
                min_chunk_ms: AtomicU64::new(u64::MAX),
                max_chunk_ms: AtomicU64::new(0),
                min_part_bytes: AtomicUsize::new(usize::MAX),
                max_part_bytes: AtomicUsize::new(0),
                min_chunks_per_part: AtomicUsize::new(usize::MAX),
                max_chunks_per_part: AtomicUsize::new(0),
                min_part_ms: AtomicU64::new(u64::MAX),
                max_part_ms: AtomicU64::new(0),
            }
        }
    }
}
