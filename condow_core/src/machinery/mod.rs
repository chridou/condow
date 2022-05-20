//! Streams for handling downloads
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, info_span, Instrument, Span};

use crate::condow_client::CondowClient;
use crate::config::{ClientRetryWrapper, Config, SequentialDownloadMode};
use crate::errors::{CondowError, IoError};
use crate::streams::{BytesHint, ChunkStream};
use crate::Probe;
use crate::{DownloadRange, InclusiveRange};

use self::part_request::PartRequestIterator;

mod download;

mod part_request;

pub(crate) async fn download_range<C: CondowClient, DR: Into<DownloadRange>, P: Probe + Clone>(
    client: ClientRetryWrapper<C>,
    mut config: Config,
    location: C::Location,
    range: DR,
    probe: P,
) -> Result<ChunkStream, CondowError> {
    let range = range.into();

    let parent = Span::current();
    // This span will track the lifetime of the whole download for all parts...
    let download_span = info_span!(parent: &parent, "download", %location, %range);
    let download_span_enter_guard = download_span.enter();
    let download_guard = DownloadSpanGuard::new(download_span.clone());

    range.validate()?;

    let range = if let Some(range) = range.sanitized() {
        range
    } else {
        return Ok(ChunkStream::empty());
    };

    let configure_stream_span = info_span!(parent: &download_span, "configure_stream");

    let (effective_range, _bytes_hint) = match range {
        DownloadRange::Open(or) => {
            debug!(parent: &configure_stream_span, "open range");
            let size = client
                .get_size(location.clone(), &probe)
                .instrument(configure_stream_span.clone())
                .await?;
            if let Some(range) = or.incl_range_from_size(size) {
                (range, BytesHint::new_exact(range.len()))
            } else {
                return Ok(ChunkStream::empty());
            }
        }
        DownloadRange::Closed(cl) => {
            debug!(parent: &configure_stream_span, "closed range");
            if config.always_get_size.into_inner() {
                debug!(parent: &configure_stream_span, "get size enforced");
                let size = client
                    .get_size(location.clone(), &probe)
                    .instrument(configure_stream_span.clone())
                    .await?;
                if let Some(range) = cl.incl_range_from_size(size) {
                    (range, BytesHint::new_exact(range.len()))
                } else {
                    return Ok(ChunkStream::empty());
                }
            } else if let Some(range) = cl.incl_range() {
                (range, BytesHint::new_at_max(range.len()))
            } else {
                return Ok(ChunkStream::empty());
            }
        }
    };

    probe.effective_range(effective_range);

    let mut part_requests =
        PartRequestIterator::new(effective_range, config.part_size_bytes.into());

    debug!(
        parent: &configure_stream_span,
        "download {} parts",
        part_requests.exact_size_hint()
    );

    if part_requests.exact_size_hint() == 0 {
        panic!("n_parts must not be 0. This is a bug");
    }

    determine_effective_concurrency(
        &mut config,
        effective_range,
        part_requests.exact_size_hint(),
        &configure_stream_span,
    );

    // If we have a sequential download, we might need to reconfigure the parts
    if *config.max_concurrency == 1 {
        part_requests = reconfigure_parts_for_sequential(
            effective_range,
            part_requests,
            config.sequential_download_mode,
            &configure_stream_span,
        );
    }

    config.log_download_messages_as_debug.log(format!(
        "download started ({} bytes) with effective concurrency {} and {} part(s)",
        effective_range.len(),
        config.max_concurrency,
        part_requests.exact_size_hint()
    ));

    drop(configure_stream_span);

    let stream = download_chunks(
        client.clone(),
        location,
        part_requests,
        config.clone(),
        probe.clone(),
        download_guard,
    );

    // Explicit drops for scope visualization
    drop(download_span_enter_guard);

    Ok(stream)
}

fn download_chunks<C: CondowClient, P: Probe + Clone>(
    client: ClientRetryWrapper<C>,
    location: C::Location,
    part_requests: PartRequestIterator,
    config: Config,
    probe: P,
    download_span_guard: DownloadSpanGuard,
) -> ChunkStream {
    if *config.max_concurrency <= 1 {
        download::download_sequentially(
            part_requests,
            client,
            location,
            probe,
            config,
            download_span_guard,
        )
    } else {
        download::download_concurrently(
            part_requests,
            client,
            location,
            probe,
            config,
            download_span_guard,
        )
    }
}

/// This procedure determines the "effective concurrency" and
/// wirties it back as "max_concurrency" into the given mutable
/// [Config].
///
/// The following values are taken into account:
/// * The original "max_concurrency" in the config (concurrrency will never be greater)
/// * The number of parts (concurrrency will never be greater)
/// * The minimum number of parts from the config to allow concurrrent downloads
/// * The minimum number of bytes for a download to be eligble to be downloaded concurrently
fn determine_effective_concurrency<T: Into<InclusiveRange>>(
    config: &mut Config,
    effective_range: T,
    n_parts: u64,
    log_span: &Span,
) {
    let effective_range = effective_range.into();
    config.max_concurrency = if *config.max_concurrency <= 1 {
        config.max_concurrency
    } else {
        let n_conc_1 = if effective_range.len() < *config.min_bytes_for_concurrent_download {
            debug!(
                parent: log_span,
                "sequential download forced by 'min_bytes_for_concurrent_download'"
            );
            1
        } else {
            *config.max_concurrency
        };
        let n_conc_2 = if n_parts < *config.min_parts_for_concurrent_download {
            debug!(
                parent: log_span,
                "sequential download forced by 'min_parts_for_concurrent_download'"
            );
            1
        } else {
            *config.max_concurrency
        };

        (*config.max_concurrency).min(n_conc_1).min(n_conc_2).into()
    };
}

/// Takes a [PartRequestIterator] for a sequential downloads and
/// returns a new [PartRequestIterator].
///
/// The returned iterator is based on the
/// configuration value "sequential_download_mode"
fn reconfigure_parts_for_sequential(
    effective_range: InclusiveRange,
    part_requests: PartRequestIterator,
    download_mode: SequentialDownloadMode,
    span: &Span,
) -> PartRequestIterator {
    match download_mode {
        SequentialDownloadMode::KeepParts => part_requests,
        SequentialDownloadMode::MergeParts => {
            if part_requests.exact_size_hint() > 1 {
                debug!(parent: span, "switching to single part download");
                PartRequestIterator::new(effective_range, effective_range.len())
            } else {
                part_requests
            }
        }
        SequentialDownloadMode::Repartition { part_size } => {
            let part_requests = PartRequestIterator::new(effective_range, part_size.into_inner());
            debug!(
                parent: span,
                "repartition to {} part(s) with approx. {} bytes each",
                part_requests.exact_size_hint(),
                part_size
            );

            part_requests
        }
    }
}

/// This struct contains a span which must be kept alive until whole download is completed
/// which means that all parts have been downloaded (and pushed on the result stream).
#[derive(Clone)]
pub(crate) struct DownloadSpanGuard(Arc<Span>);

impl DownloadSpanGuard {
    pub fn new(span: Span) -> Self {
        Self(Arc::new(span))
    }

    pub fn span(&self) -> &Span {
        &self.0
    }
}

#[cfg(test)]
impl Default for DownloadSpanGuard {
    fn default() -> Self {
        Self(Arc::new(Span::none()))
    }
}

/// This is just a wrapper for not having to do
/// all the "matches" all over the place...
#[derive(Clone)]
pub(crate) enum ProbeInternal<P: Probe + Clone> {
    /// We have no internal [Probe] (from the ProbeFactory) but got a [Probe] via
    /// the request API
    RequestProbe(Arc<dyn Probe>),
    /// We have a [Probe] from the internal probe factory
    /// and got a [Probe] via
    /// the request API
    FactoryAndRequestProbe(P, Arc<dyn Probe>),
}

impl<P: Probe + Clone> Probe for ProbeInternal<P> {
    #[inline]
    fn effective_range(&self, range: InclusiveRange) {
        match self {
            ProbeInternal::RequestProbe(p) => p.effective_range(range),
            ProbeInternal::FactoryAndRequestProbe(factory_probe, request_probe) => {
                factory_probe.effective_range(range);
                request_probe.effective_range(range);
            }
        }
    }

    #[inline]
    fn download_started(&self) {
        match self {
            ProbeInternal::RequestProbe(p) => p.download_started(),
            ProbeInternal::FactoryAndRequestProbe(factory_probe, request_probe) => {
                factory_probe.download_started();
                request_probe.download_started();
            }
        }
    }

    #[inline]
    fn download_completed(&self, time: Duration) {
        match self {
            ProbeInternal::RequestProbe(p) => p.download_completed(time),
            ProbeInternal::FactoryAndRequestProbe(factory_probe, request_probe) => {
                factory_probe.download_completed(time);
                request_probe.download_completed(time);
            }
        }
    }

    #[inline]
    fn download_failed(&self, time: Option<Duration>) {
        match self {
            ProbeInternal::RequestProbe(p) => p.download_failed(time),
            ProbeInternal::FactoryAndRequestProbe(factory_probe, request_probe) => {
                factory_probe.download_failed(time);
                request_probe.download_failed(time);
            }
        }
    }

    #[inline]
    fn retry_attempt(&self, location: &dyn fmt::Display, error: &CondowError, next_in: Duration) {
        match self {
            ProbeInternal::RequestProbe(p) => p.retry_attempt(location, error, next_in),
            ProbeInternal::FactoryAndRequestProbe(factory_probe, request_probe) => {
                factory_probe.retry_attempt(location, error, next_in);
                request_probe.retry_attempt(location, error, next_in);
            }
        }
    }

    #[inline]
    fn stream_resume_attempt(
        &self,
        location: &dyn fmt::Display,
        error: &IoError,
        orig_range: InclusiveRange,
        remaining_range: InclusiveRange,
    ) {
        match self {
            ProbeInternal::RequestProbe(p) => {
                p.stream_resume_attempt(location, error, orig_range, remaining_range)
            }
            ProbeInternal::FactoryAndRequestProbe(factory_probe, request_probe) => {
                factory_probe.stream_resume_attempt(location, error, orig_range, remaining_range);
                request_probe.stream_resume_attempt(location, error, orig_range, remaining_range);
            }
        }
    }

    #[inline]
    fn panic_detected(&self, msg: &str) {
        match self {
            ProbeInternal::RequestProbe(p) => p.panic_detected(msg),
            ProbeInternal::FactoryAndRequestProbe(factory_probe, request_probe) => {
                factory_probe.panic_detected(msg);
                request_probe.panic_detected(msg);
            }
        }
    }

    #[inline]
    fn queue_full(&self) {
        match self {
            ProbeInternal::RequestProbe(p) => p.queue_full(),
            ProbeInternal::FactoryAndRequestProbe(factory_probe, request_probe) => {
                factory_probe.queue_full();
                request_probe.queue_full();
            }
        }
    }

    #[inline]
    fn chunk_received(&self, part_index: u64, chunk_index: usize, n_bytes: usize) {
        match self {
            ProbeInternal::RequestProbe(p) => p.chunk_received(part_index, chunk_index, n_bytes),
            ProbeInternal::FactoryAndRequestProbe(factory_probe, request_probe) => {
                factory_probe.chunk_received(part_index, chunk_index, n_bytes);
                request_probe.chunk_received(part_index, chunk_index, n_bytes);
            }
        }
    }

    #[inline]
    fn part_started(&self, part_index: u64, range: crate::InclusiveRange) {
        match self {
            ProbeInternal::RequestProbe(p) => p.part_started(part_index, range),
            ProbeInternal::FactoryAndRequestProbe(factory_probe, request_probe) => {
                factory_probe.part_started(part_index, range);
                request_probe.part_started(part_index, range);
            }
        }
    }

    #[inline]
    fn part_completed(
        &self,
        part_index: u64,
        n_chunks: usize,
        n_bytes: u64,
        time: std::time::Duration,
    ) {
        match self {
            ProbeInternal::RequestProbe(p) => p.part_completed(part_index, n_chunks, n_bytes, time),
            ProbeInternal::FactoryAndRequestProbe(factory_probe, request_probe) => {
                factory_probe.part_completed(part_index, n_chunks, n_bytes, time);
                request_probe.part_completed(part_index, n_chunks, n_bytes, time);
            }
        }
    }

    #[inline]
    fn part_failed(&self, error: &CondowError, part_index: u64, range: &InclusiveRange) {
        match self {
            ProbeInternal::RequestProbe(p) => p.part_failed(error, part_index, range),
            ProbeInternal::FactoryAndRequestProbe(factory_probe, request_probe) => {
                factory_probe.part_failed(error, part_index, range);
                request_probe.part_failed(error, part_index, range);
            }
        }
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod function_tests {
    mod determine_effective_concurrency {
        use tracing::Span;

        use crate::{config::Config, machinery::determine_effective_concurrency};

        #[test]
        fn max_concurrency() {
            for max_conc in 1..10 {
                let mut config = Config::default()
                    .max_concurrency(max_conc)
                    .min_bytes_for_concurrent_download(0)
                    .min_parts_for_concurrent_download(0);
                determine_effective_concurrency(&mut config, 0..=999, 10, &Span::none());

                assert_eq!(*config.max_concurrency, max_conc);
            }
        }

        #[test]
        fn max_config_concurrency_limits_concurrency() {
            for n_parts in 1..=10 {
                for range in [0..=1, 0..=4, 0..=9] {
                    let mut config = Config::default()
                        .max_concurrency(1)
                        .min_bytes_for_concurrent_download(0)
                        .min_parts_for_concurrent_download(0);
                    determine_effective_concurrency(&mut config, range, n_parts, &Span::none());

                    assert_eq!(*config.max_concurrency, 1);
                }
            }
        }

        #[test]
        fn min_bytes_limits_concurrency() {
            for range in [0..=1, 0..=4, 0..=9] {
                let mut config = Config::default()
                    .max_concurrency(1)
                    .min_bytes_for_concurrent_download(10)
                    .min_parts_for_concurrent_download(0);
                determine_effective_concurrency(&mut config, range, 999, &Span::none());

                assert_eq!(*config.max_concurrency, 1);
            }
        }

        #[test]
        fn min_parts_limits_concurrency() {
            for n_parts in 1..=10 {
                let mut config = Config::default()
                    .max_concurrency(1)
                    .min_bytes_for_concurrent_download(0)
                    .min_parts_for_concurrent_download(10);
                determine_effective_concurrency(&mut config, 0..=9, n_parts, &Span::none());

                assert_eq!(*config.max_concurrency, 1);
            }
        }
    }

    mod reconfigure_parts_for_sequential_tests {
        use tracing::Span;

        use crate::{
            config::SequentialDownloadMode,
            machinery::{part_request::PartRequestIterator, reconfigure_parts_for_sequential},
            InclusiveRange,
        };

        #[test]
        fn keep_parts_1_part() {
            let download_mode = SequentialDownloadMode::KeepParts;
            let effective_range = (0..=999).into();
            let iter_orig = PartRequestIterator::new(effective_range, 1000);
            assert_eq!(iter_orig.exact_size_hint(), 1, "wrong size");
            let expected: Vec<_> = iter_orig.clone_continue().collect();

            let result = reconfigure_parts_for_sequential(
                effective_range,
                iter_orig,
                download_mode,
                &Span::none(),
            );
            let result: Vec<_> = result.collect();

            assert_eq!(result, expected);
        }

        #[test]
        fn keep_parts_2_parts() {
            let download_mode = SequentialDownloadMode::KeepParts;
            let effective_range = (0..=999).into();
            let iter_orig = PartRequestIterator::new(effective_range, 500);
            assert_eq!(iter_orig.exact_size_hint(), 2, "wrong size");
            let expected: Vec<_> = iter_orig.clone_continue().collect();

            let result = reconfigure_parts_for_sequential(
                effective_range,
                iter_orig,
                download_mode,
                &Span::none(),
            );
            let result: Vec<_> = result.collect();

            assert_eq!(result, expected);
        }

        #[test]
        fn keep_parts_3_parts() {
            let download_mode = SequentialDownloadMode::KeepParts;
            let effective_range = (0..=999).into();
            let iter_orig = PartRequestIterator::new(effective_range, 400);
            assert_eq!(iter_orig.exact_size_hint(), 3, "wrong size");
            let expected: Vec<_> = iter_orig.clone_continue().collect();

            let result = reconfigure_parts_for_sequential(
                effective_range,
                iter_orig,
                download_mode,
                &Span::none(),
            );
            let result: Vec<_> = result.collect();

            assert_eq!(result, expected);
        }

        #[test]
        fn merge_1_part() {
            let download_mode = SequentialDownloadMode::MergeParts;
            let effective_range = (0..=999).into();
            let iter_orig = PartRequestIterator::new(effective_range, 1000);
            assert_eq!(iter_orig.exact_size_hint(), 1, "wrong size");
            let expected: Vec<_> = iter_orig.clone_continue().collect();

            let result = reconfigure_parts_for_sequential(
                effective_range,
                iter_orig,
                download_mode,
                &Span::none(),
            );
            let result: Vec<_> = result.collect();

            assert_eq!(result, expected);
        }

        #[test]
        fn merge_2_parts() {
            let download_mode = SequentialDownloadMode::MergeParts;
            let effective_range = (0..=999).into();
            let iter_orig = PartRequestIterator::new(effective_range, 500);
            assert_eq!(iter_orig.exact_size_hint(), 2, "wrong size");

            let result = reconfigure_parts_for_sequential(
                effective_range,
                iter_orig,
                download_mode,
                &Span::none(),
            );
            let mut result: Vec<_> = result.collect();

            assert_eq!(result.len(), 1);
            let request = result.pop().unwrap();

            assert_eq!(request.part_index, 0);
            assert_eq!(request.blob_range, effective_range);
            assert_eq!(request.range_offset, 0);
        }

        #[test]
        fn merge_3_parts() {
            let download_mode = SequentialDownloadMode::MergeParts;
            let effective_range = (0..=999).into();
            let iter_orig = PartRequestIterator::new(effective_range, 400);
            assert_eq!(iter_orig.exact_size_hint(), 3, "wrong size");

            let result = reconfigure_parts_for_sequential(
                effective_range,
                iter_orig,
                download_mode,
                &Span::none(),
            );
            let mut result: Vec<_> = result.collect();

            assert_eq!(result.len(), 1);
            let request = result.pop().unwrap();

            assert_eq!(request.part_index, 0);
            assert_eq!(request.blob_range, effective_range);
            assert_eq!(request.range_offset, 0);
        }

        #[test]
        fn repartition_1_part_smaller_target_parts() {
            let download_mode = SequentialDownloadMode::Repartition {
                part_size: 800.into(),
            };
            let effective_range = (0..=999).into();
            let iter_orig = PartRequestIterator::new(effective_range, 1000);
            assert_eq!(iter_orig.exact_size_hint(), 1, "wrong size");

            let mut result = reconfigure_parts_for_sequential(
                effective_range,
                iter_orig,
                download_mode,
                &Span::none(),
            );

            assert_eq!(result.exact_size_hint(), 2);
            let request_1 = result.next().unwrap();
            let request_2 = result.next().unwrap();

            assert_eq!(request_1.part_index, 0);
            assert_eq!(request_1.blob_range, InclusiveRange(0, 799));
            assert_eq!(request_1.range_offset, 0);

            assert_eq!(request_2.part_index, 1);
            assert_eq!(request_2.blob_range, InclusiveRange(800, 999));
            assert_eq!(request_2.range_offset, 800);
        }

        #[test]
        fn repartition_1_part_same_target_parts() {
            let download_mode = SequentialDownloadMode::Repartition {
                part_size: 1000.into(),
            };
            let effective_range = (0..=999).into();
            let iter_orig = PartRequestIterator::new(effective_range, 1000);
            assert_eq!(iter_orig.exact_size_hint(), 1, "wrong size");

            let mut result = reconfigure_parts_for_sequential(
                effective_range,
                iter_orig,
                download_mode,
                &Span::none(),
            );

            assert_eq!(result.exact_size_hint(), 1);
            let request_1 = result.next().unwrap();

            assert_eq!(request_1.part_index, 0);
            assert_eq!(request_1.blob_range, InclusiveRange(0, 999));
            assert_eq!(request_1.range_offset, 0);
        }

        #[test]
        fn repartition_1_part_larger_target_parts() {
            let download_mode = SequentialDownloadMode::Repartition {
                part_size: 5000.into(),
            };
            let effective_range = (0..=999).into();
            let iter_orig = PartRequestIterator::new(effective_range, 1000);
            assert_eq!(iter_orig.exact_size_hint(), 1, "wrong size");

            let mut result = reconfigure_parts_for_sequential(
                effective_range,
                iter_orig,
                download_mode,
                &Span::none(),
            );

            assert_eq!(result.exact_size_hint(), 1);
            let request_1 = result.next().unwrap();

            assert_eq!(request_1.part_index, 0);
            assert_eq!(request_1.blob_range, InclusiveRange(0, 999));
            assert_eq!(request_1.range_offset, 0);
        }
    }
}
