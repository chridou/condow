//! Spawns multiple [SequentialDownloader]s to download parts

use std::{
    sync::{atomic::AtomicUsize, Arc},
    time::Instant,
};

use futures::{channel::mpsc::UnboundedSender, Stream, StreamExt};

use crate::{
    condow_client::CondowClient,
    config::{ClientRetryWrapper, Config},
    machinery::{range_stream::RangeRequest, DownloadSpanGuard, ProbeInternal},
    probe::Probe,
    streams::ChunkStreamItem,
};

use super::{
    sequential::{DownloaderContext, SequentialDownloader},
    KillSwitch,
};

pub(crate) struct ConcurrentDownloader<P: Probe + Clone> {
    downloaders: Vec<SequentialDownloader>,
    counter: usize,
    kill_switch: KillSwitch,
    config: Config,
    probe: ProbeInternal<P>,
}

impl<P: Probe + Clone> ConcurrentDownloader<P> {
    pub fn new<C: CondowClient>(
        n_concurrent: usize,
        results_sender: UnboundedSender<ChunkStreamItem>,
        client: ClientRetryWrapper<C>,
        config: Config,
        location: C::Location,
        probe: ProbeInternal<P>,
        download_span_guard: DownloadSpanGuard,
    ) -> Self {
        let started_at = Instant::now();
        let kill_switch = KillSwitch::new();
        let counter = Arc::new(AtomicUsize::new(0));
        let downloaders: Vec<_> = (0..n_concurrent)
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
        }
    }

    pub async fn download(
        &mut self,
        ranges_stream: impl Stream<Item = RangeRequest>,
    ) -> Result<(), ()> {
        self.probe.download_started();
        let mut ranges_stream = Box::pin(ranges_stream);
        while let Some(mut range_request) = ranges_stream.next().await {
            let mut attempt = 1;

            let buffers_full_delay = self.config.buffers_full_delay_ms.into();
            let n_downloaders = self.downloaders.len();

            loop {
                if attempt % self.downloaders.len() == 0 {
                    self.probe.queue_full();
                    tokio::time::sleep(buffers_full_delay).await;
                }
                let idx = self.counter + attempt;
                let downloader = &mut self.downloaders[idx % n_downloaders];

                match downloader.enqueue(range_request) {
                    Ok(None) => break,
                    Ok(Some(msg)) => {
                        range_request = msg;
                    }
                    Err(()) => {
                        self.kill_switch.push_the_button();
                        return Err(());
                    }
                }

                attempt += 1;
            }

            self.counter += 1;
        }
        Ok(())
    }
}
