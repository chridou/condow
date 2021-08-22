//! Reporting
//!
//! This goes more into the direction of instrumentation. Unfortunately
//! `tokio` uses the word `Instrumentation` already for their tracing
//! implementation.
use std::{fmt, time::Duration};

use crate::InclusiveRange;

pub use simple_reporter::*;

pub trait ReporterFactory: Send + Sync + 'static {
    type ReporterType: Reporter;

    /// Create a new [Reporter].
    ///
    /// This might share state with the factory or not
    fn make(&self) -> Self::ReporterType;
}

/// A Reporter is an interface to track occurences of different kinds
///
/// Implementors can use this to put instrumentation on downloads
/// or simply to log.
///
/// All methods should return quickly to not to influence the
/// downloading too much with measuring.
#[allow(unused_variables)]
pub trait Reporter: Clone + Send + Sync + 'static {
    /// The location and range of this download
    ///
    /// **This is always the first method called.**
    fn download(&self, location: &dyn fmt::Display, range: InclusiveRange) {}
    /// The actual IO started
    fn download_started(&self) {}
    /// IO tasks finished
    ///
    /// **This always is the last method called on a [Reporter] if the download was successful.**
    fn download_completed(&self) {}
    /// IO tasks finished
    ///
    /// **This always is the last method called on a [Reporter] if the download failed.**
    fn download_failed(&self) {}
    /// All queues are full so no new request could be scheduled
    fn queue_full(&self) {}
    /// A part was completed
    fn chunk_completed(&self, part_index: usize, n_bytes: usize, time: Duration) {}
    /// Download of a part has started
    fn part_started(&self, part_index: usize, range: InclusiveRange) {}
    /// Download of a part was completed
    fn part_completed(&self, part_index: usize, n_chunks: usize, n_bytes: usize, time: Duration) {}
}

/// Disables reporting
#[derive(Copy, Clone)]
pub struct NoReporting;

impl Reporter for NoReporting {}
impl ReporterFactory for NoReporting {
    type ReporterType = Self;

    fn make(&self) -> Self {
        NoReporting
    }
}

/// Plug 2 [Reporter]s into one and have them both notified.
///
/// `RA` is notified first.
#[derive(Clone)]
pub struct CompositeReporter<RA: Reporter, RB: Reporter>(pub RA, pub RB);

impl<RA: Reporter, RB: Reporter> Reporter for CompositeReporter<RA, RB> {
    fn download(&self, location: &dyn fmt::Display, range: crate::InclusiveRange) {
        self.0.download(location, range);
        self.1.download(location, range);
    }

    fn download_started(&self) {
        self.0.download_started();
        self.1.download_started();
    }

    fn download_completed(&self) {
        self.0.download_completed();
        self.1.download_completed();
    }

    fn download_failed(&self) {
        self.0.download_failed();
        self.1.download_failed();
    }

    fn queue_full(&self) {
        self.0.queue_full();
        self.1.queue_full();
    }

    fn chunk_completed(&self, part_index: usize, n_bytes: usize, time: std::time::Duration) {
        self.0.chunk_completed(part_index, n_bytes, time);
        self.1.chunk_completed(part_index, n_bytes, time);
    }

    fn part_started(&self, part_index: usize, range: crate::InclusiveRange) {
        self.0.part_started(part_index, range);
        self.1.part_started(part_index, range);
    }

    fn part_completed(
        &self,
        part_index: usize,
        n_chunks: usize,
        n_bytes: usize,
        time: std::time::Duration,
    ) {
        self.0.part_completed(part_index, n_chunks, n_bytes, time);
        self.1.part_completed(part_index, n_chunks, n_bytes, time);
    }
}

mod simple_reporter {
    //! Simple reporting with counters
    use std::{
        fmt,
        sync::{
            atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
            Arc, Mutex,
        },
        time::{Duration, Instant},
    };

    use crate::InclusiveRange;

    use super::{Reporter, ReporterFactory};

    /// Creates [SimpleReporter]s
    pub struct SimpleReporterFactory;
    impl ReporterFactory for SimpleReporterFactory {
        type ReporterType = SimpleReporter;

        fn make(&self) -> Self::ReporterType {
            SimpleReporter::new()
        }
    }

    /// A `SimpleReporter` collects metrics and creates a [SimpleReport]
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

        /// Returns true once the download is finished
        ///
        /// As long as this method does not return `true` values in the [SimpleReport]
        /// will change.
        pub fn is_download_finished(&self) -> bool {
            self.inner.download_finished_at.lock().unwrap().is_some()
        }

        /// Get a [SimpleReport].
        ///
        /// Takes a snapshot. A download might still be running when a snapshot
        /// is taken.
        pub fn report(&self) -> SimpleReport {
            let inner = self.inner.as_ref();
            let download_time =
                if let Some(finished_at) = *inner.download_finished_at.lock().unwrap() {
                    finished_at - *inner.download_started_at.lock().unwrap()
                } else {
                    Instant::now() - *inner.download_started_at.lock().unwrap()
                };
            let n_bytes_received = inner.n_bytes_received.load(Ordering::SeqCst);
            let bytes_per_second_f64 = if n_bytes_received > 0 {
                n_bytes_received as f64 / download_time.as_secs_f64()
            } else {
                0.0
            };

            SimpleReport {
                location: inner.location.lock().unwrap().clone(),
                effective_range: *inner.range.lock().unwrap(),
                is_finished: self.is_download_finished(),
                is_failed: inner.is_failed.load(Ordering::SeqCst),
                download_time,
                bytes_per_second: bytes_per_second_f64 as u64,
                megabytes_per_second: bytes_per_second_f64 / 1_000_000.0,
                mebibytes_per_second: bytes_per_second_f64 / 1_048_576.0,
                gigabits_per_second: (bytes_per_second_f64 * 8.0) / 1_000_000_000.0,
                gibibits_per_second: (bytes_per_second_f64 * 8.0) / 1_073_741_824.0,
                n_queue_full: inner.n_queue_full.load(Ordering::SeqCst),
                n_bytes_received,
                n_chunks_received: inner.n_chunks_received.load(Ordering::SeqCst),
                n_parts_received: inner.n_parts_received.load(Ordering::SeqCst),
                min_chunk_bytes: inner.min_chunk_bytes.load(Ordering::SeqCst),
                max_chunk_bytes: inner.max_chunk_bytes.load(Ordering::SeqCst),
                min_chunk_time: Duration::from_micros(inner.min_chunk_us.load(Ordering::SeqCst)),
                max_chunk_time: Duration::from_micros(inner.max_chunk_us.load(Ordering::SeqCst)),
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
        pub location: String,
        pub effective_range: InclusiveRange,
        /// `true` if the download was finished
        pub is_finished: bool,
        pub is_failed: bool,
        /// If the download is not yet finished, this is the time
        /// elapsed since the start of the download.
        pub download_time: Duration,
        pub bytes_per_second: u64,
        pub megabytes_per_second: f64,
        pub mebibytes_per_second: f64,
        pub gigabits_per_second: f64,
        pub gibibits_per_second: f64,
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
        fn download(&self, location: &dyn fmt::Display, range: InclusiveRange) {
            self.inner
                .location
                .lock()
                .unwrap()
                .push_str(&location.to_string());
            *self.inner.range.lock().unwrap() = range;
        }

        fn download_started(&self) {
            *self.inner.download_started_at.lock().unwrap() = Instant::now();
        }

        fn download_completed(&self) {
            *self.inner.download_finished_at.lock().unwrap() = Some(Instant::now());
        }

        fn download_failed(&self) {
            *self.inner.download_finished_at.lock().unwrap() = Some(Instant::now());
            self.inner.is_failed.store(true, Ordering::SeqCst);
        }

        fn queue_full(&self) {
            self.inner.n_queue_full.fetch_add(1, Ordering::SeqCst);
        }

        fn chunk_completed(&self, _part_index: usize, n_bytes: usize, time: Duration) {
            let inner = self.inner.as_ref();
            inner.n_chunks_received.fetch_add(1, Ordering::SeqCst);
            inner.min_chunk_bytes.fetch_min(n_bytes, Ordering::SeqCst);
            inner.max_chunk_bytes.fetch_max(n_bytes, Ordering::SeqCst);
            let us = time.as_micros() as u64;
            inner.min_chunk_us.fetch_min(us, Ordering::SeqCst);
            inner.max_chunk_us.fetch_max(us, Ordering::SeqCst);
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
        location: Mutex<String>,
        range: Mutex<InclusiveRange>,
        download_started_at: Mutex<Instant>,
        download_finished_at: Mutex<Option<Instant>>,
        is_failed: AtomicBool,
        n_queue_full: AtomicUsize,
        n_bytes_received: AtomicUsize,
        n_chunks_received: AtomicUsize,
        n_parts_received: AtomicUsize,
        min_chunk_bytes: AtomicUsize,
        max_chunk_bytes: AtomicUsize,
        min_chunk_us: AtomicU64,
        max_chunk_us: AtomicU64,
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
                location: Mutex::new(String::new()),
                range: Mutex::new(InclusiveRange(0, 0)),
                download_started_at: Mutex::new(Instant::now()),
                download_finished_at: Mutex::new(None),
                is_failed: AtomicBool::new(false),
                n_bytes_received: AtomicUsize::new(0),
                n_chunks_received: AtomicUsize::new(0),
                n_parts_received: AtomicUsize::new(0),
                n_queue_full: AtomicUsize::new(0),
                min_chunk_bytes: AtomicUsize::new(usize::MAX),
                max_chunk_bytes: AtomicUsize::new(0),
                min_chunk_us: AtomicU64::new(u64::MAX),
                max_chunk_us: AtomicU64::new(0),
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
