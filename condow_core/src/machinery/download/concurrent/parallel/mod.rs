use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use futures::StreamExt;
use tokio::sync::mpsc::UnboundedSender;

use crate::{
    components::part_request::PartRequestIterator, condow_client::CondowClient, config::Config,
    errors::CondowError, machinery::DownloadSpanGuard, probe::Probe, retry::ClientRetryWrapper,
    streams::ChunkStreamItem,
};

use worker::{DownloaderContext, SequentialDownloader};

mod worker;

pub(crate) struct ParallelDownloader<P: Probe + Clone> {
    downloaders: Vec<SequentialDownloader>,
    counter: usize,
    kill_switch: KillSwitch,
    config: Arc<Config>,
    probe: P,
    results_sender: UnboundedSender<ChunkStreamItem>,
}

impl<P: Probe + Clone> ParallelDownloader<P> {
    pub fn new<C: CondowClient>(
        results_sender: UnboundedSender<ChunkStreamItem>,
        client: ClientRetryWrapper<C>,
        config: Config,
        location: C::Location,
        probe: P,
        download_span_guard: DownloadSpanGuard,
    ) -> Self {
        let started_at = Instant::now();
        let kill_switch = KillSwitch::new();
        let counter = Arc::new(AtomicUsize::new(0));
        let config = Arc::new(config.clone());
        let downloaders: Vec<_> = (0..*config.max_concurrency)
            .map(|_| {
                SequentialDownloader::new(
                    client.clone(),
                    location.clone(),
                    config.buffer_size.into(),
                    DownloaderContext::new(
                        results_sender.clone(),
                        Arc::clone(&counter),
                        kill_switch.clone(),
                        probe.clone(),
                        started_at,
                        download_span_guard.clone(),
                        Arc::clone(&config),
                    ),
                )
            })
            .collect();

        Self {
            downloaders,
            counter: 0,
            kill_switch,
            config,
            probe,
            results_sender,
        }
    }

    pub async fn download(&mut self, ranges: PartRequestIterator) {
        self.probe.download_started();
        let mut ranges_stream = ranges.into_stream();

        let max_buffers_full_delay: Duration = self.config.max_buffers_full_delay_ms.into();
        let n_downloaders = self.downloaders.len();

        while let Some(mut range_request) = ranges_stream.next().await {
            let mut attempt = 1;
            let mut current_buffers_full_delay = Duration::from_secs(0);

            loop {
                if attempt % self.downloaders.len() == 0 {
                    self.probe.queue_full();
                    tokio::time::sleep(current_buffers_full_delay).await;
                    current_buffers_full_delay = max_buffers_full_delay
                        .min(current_buffers_full_delay + Duration::from_millis(1));
                }
                let idx = self.counter + attempt;
                let downloader = &mut self.downloaders[idx % n_downloaders];

                match downloader.enqueue(range_request) {
                    Ok(None) => break,
                    Ok(Some(msg)) => {
                        range_request = msg; // Try this one again
                    }
                    Err(()) => {
                        // This is the only error case and we send it over the channel
                        self.kill_switch.push_the_button();
                        let _ = self.results_sender.send(Err(CondowError::new_other(
                            "failed to send part request - receiver gone",
                        )));
                        return;
                    }
                }

                attempt += 1;
            }

            self.counter += 1;
        }
    }
}

/// Shared state to control cancellation of a download
#[derive(Clone)]
pub(crate) struct KillSwitch {
    is_pushed: Arc<AtomicBool>,
}

impl KillSwitch {
    pub fn new() -> Self {
        Self {
            is_pushed: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Check whether cancellation of the download was requested
    pub fn is_pushed(&self) -> bool {
        self.is_pushed.load(Ordering::SeqCst)
    }

    /// Request cancellation of the download
    pub fn push_the_button(&self) {
        self.is_pushed.store(true, Ordering::SeqCst)
    }
}
