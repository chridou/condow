use tracing::{debug, info_span, Instrument, Span};

use crate::{
    condow_client::CondowClient,
    config::{Config, SequentialDownloadMode},
    errors::CondowError,
    probe::Probe,
    retry::ClientRetryWrapper,
    DownloadRange, InclusiveRange,
};

use super::part_request::PartRequestIterator;

pub struct DownloadConfiguration<L> {
    pub(crate) location: L,
    pub(crate) config: Config,
    pub(crate) part_requests: PartRequestIterator,
}

impl<L> DownloadConfiguration<L> {
    /// Returns the maximum concurrency for this download
    pub fn max_concurrency(&self) -> usize {
        self.config.max_concurrency.into_inner()
    }

    /// Retrns a hint on the amount of bytes returned from this stream
    pub fn exact_bytes(&self) -> u64 {
        self.part_requests.exact_bytes_left()
    }
}

pub(crate) async fn configure<R, C, P>(
    location: C::Location,
    range: R,
    trusted_size: Option<u64>,
    mut config: Config,
    client: &ClientRetryWrapper<C>,
    probe: &P,
    parent_span: &Span,
) -> Result<Option<DownloadConfiguration<C::Location>>, CondowError>
where
    R: Into<DownloadRange>,
    C: CondowClient,
    P: Probe + Clone,
{
    let configure_stream_span = info_span!(parent: parent_span, "configure_stream");

    let range = range.into();
    range.validate()?;

    let range = if let Some(range) = range.sanitized() {
        range
    } else {
        return Ok(None);
    };

    let effective_range = match range {
        DownloadRange::Open(or) => {
            debug!(parent: &configure_stream_span, "open range");
            let size = if let Some(trusted_size) = trusted_size {
                trusted_size
            } else {
                debug!(parent: &configure_stream_span, "get size for open range");
                client
                    .get_size(location.clone(), probe)
                    .instrument(configure_stream_span.clone())
                    .await?
            };
            if let Some(range) = or.incl_range_from_size(size)? {
                range
            } else {
                return Ok(None);
            }
        }
        DownloadRange::Closed(cl) => {
            debug!(parent: &configure_stream_span, "closed range");
            let range = if let Some(range) = cl.incl_range() {
                range
            } else {
                return Ok(None);
            };

            range.validate()?;

            if let Some(trusted_size) = trusted_size {
                if range.end_incl() >= trusted_size {
                    return Err(CondowError::new_invalid_range(format!("{cl}")));
                }
            }

            range
        }
    };

    probe.effective_range(effective_range);

    let mut part_requests =
        PartRequestIterator::new(effective_range, config.part_size_bytes.into());

    debug!(
        parent: &configure_stream_span,
        "download {} parts",
        part_requests.parts_hint()
    );

    if part_requests.parts_hint() == 0 {
        panic!("n_parts must not be 0. This is a bug");
    }

    determine_effective_concurrency(
        &mut config,
        effective_range,
        part_requests.parts_hint(),
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
        "download {} bytes with effective concurrency {} and {} part(s)",
        effective_range.len(),
        config.max_concurrency,
        part_requests.parts_hint()
    ));

    drop(configure_stream_span);

    Ok(Some(DownloadConfiguration {
        location,
        config,
        part_requests,
    }))
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
            if part_requests.parts_hint() > 1 {
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
                part_requests.parts_hint(),
                part_size
            );

            part_requests
        }
    }
}

#[cfg(test)]
mod function_tests {
    mod determine_effective_concurrency {
        use tracing::Span;

        use crate::{
            config::Config, machinery::configure_download::determine_effective_concurrency,
        };

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
            machinery::{
                configure_download::reconfigure_parts_for_sequential,
                part_request::PartRequestIterator,
            },
            InclusiveRange,
        };

        #[test]
        fn keep_parts_1_part() {
            let download_mode = SequentialDownloadMode::KeepParts;
            let effective_range = (0..=999).into();
            let iter_orig = PartRequestIterator::new(effective_range, 1000);
            assert_eq!(iter_orig.parts_hint(), 1, "wrong size");
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
            assert_eq!(iter_orig.parts_hint(), 2, "wrong size");
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
            assert_eq!(iter_orig.parts_hint(), 3, "wrong size");
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
            assert_eq!(iter_orig.parts_hint(), 1, "wrong size");
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
            assert_eq!(iter_orig.parts_hint(), 2, "wrong size");

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
            assert_eq!(iter_orig.parts_hint(), 3, "wrong size");

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
            assert_eq!(iter_orig.parts_hint(), 1, "wrong size");

            let mut result = reconfigure_parts_for_sequential(
                effective_range,
                iter_orig,
                download_mode,
                &Span::none(),
            );

            assert_eq!(result.parts_hint(), 2);
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
            assert_eq!(iter_orig.parts_hint(), 1, "wrong size");

            let mut result = reconfigure_parts_for_sequential(
                effective_range,
                iter_orig,
                download_mode,
                &Span::none(),
            );

            assert_eq!(result.parts_hint(), 1);
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
            assert_eq!(iter_orig.parts_hint(), 1, "wrong size");

            let mut result = reconfigure_parts_for_sequential(
                effective_range,
                iter_orig,
                download_mode,
                &Span::none(),
            );

            assert_eq!(result.parts_hint(), 1);
            let request_1 = result.next().unwrap();

            assert_eq!(request_1.part_index, 0);
            assert_eq!(request_1.blob_range, InclusiveRange(0, 999));
            assert_eq!(request_1.range_offset, 0);
        }
    }
}
