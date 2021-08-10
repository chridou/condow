use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use futures::{
    channel::mpsc::{self, Sender, UnboundedSender},
    Stream, StreamExt,
};

use crate::{
    condow_client::{CondowClient, DownloadSpec},
    config::Config,
    errors::{IoError, StreamError},
    streams::{BytesStream, ChunkItem, ChunkItemPayload, ChunkStreamItem},
};

use super::range_stream::RangeRequest;

pub async fn download_concurrently<C: CondowClient>(
    ranges_stream: impl Stream<Item = RangeRequest>,
    n_concurrent: usize,
    results_sender: UnboundedSender<ChunkStreamItem>,
    client: C,
    config: Config,
    location: C::Location,
) -> Result<(), ()> {
    let mut downloader = ConcurrentDownloader::new(
        n_concurrent,
        results_sender,
        client,
        config.clone(),
        location,
    );
    downloader.download(ranges_stream).await
}

struct ConcurrentDownloader {
    downloaders: Vec<Downloader>,
    counter: usize,
    kill_switch: Arc<AtomicBool>,
    config: Config,
}

impl ConcurrentDownloader {
    pub fn new<C: CondowClient>(
        n_concurrent: usize,
        results_sender: UnboundedSender<ChunkStreamItem>,
        client: C,
        config: Config,
        location: C::Location,
    ) -> Self {
        let kill_switch = Arc::new(AtomicBool::new(false));
        let downloaders: Vec<_> = (0..n_concurrent)
            .map(|_| {
                Downloader::new(
                    client.clone(),
                    results_sender.clone(),
                    Arc::clone(&kill_switch),
                    location.clone(),
                    config.buffer_size.into(),
                )
            })
            .collect();

        Self {
            downloaders,
            counter: 0,
            kill_switch,
            config,
        }
    }

    pub async fn download(
        &mut self,
        ranges_stream: impl Stream<Item = RangeRequest>,
    ) -> Result<(), ()> {
        let mut ranges_stream = Box::pin(ranges_stream);
        while let Some(mut range_request) = ranges_stream.next().await {
            let mut attempt = 1;

            let buffers_full_delay = self.config.buffers_full_delay_ms.into();
            let n_downloaders = self.downloaders.len();

            loop {
                if attempt % self.downloaders.len() == 0 {
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
                        self.kill_switch.store(true, Ordering::Relaxed);
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

struct Downloader {
    sender: Sender<RangeRequest>,
    kill_switch: Arc<AtomicBool>,
}

impl Downloader {
    pub fn new<C: CondowClient>(
        client: C,
        results_sender: UnboundedSender<ChunkStreamItem>,
        kill_switch: Arc<AtomicBool>,
        location: C::Location,
        buffer_size: usize,
    ) -> Self {
        let (sender, request_receiver) = mpsc::channel::<RangeRequest>(buffer_size);

        tokio::spawn({
            let kill_switch = Arc::clone(&kill_switch);
            async move {
                let mut request_receiver = Box::pin(request_receiver);
                while let Some(range_request) = request_receiver.next().await {
                    if kill_switch.load(Ordering::Relaxed) {
                        break;
                    }

                    match client
                        .download(
                            location.clone(),
                            DownloadSpec::Range(range_request.file_range),
                        )
                        .await
                    {
                        Ok((bytes_stream, _total_bytes)) => {
                            if consume_and_dispatch_bytes(
                                bytes_stream,
                                &results_sender,
                                range_request,
                            )
                            .await
                            .is_err()
                            {
                                kill_switch.store(true, Ordering::Relaxed);
                                request_receiver.close();
                                break;
                            }
                        }
                        Err(err) => {
                            kill_switch.store(true, Ordering::Relaxed);
                            request_receiver.close();
                            let _ = results_sender.unbounded_send(Err(err.into()));
                            break;
                        }
                    };
                }
            }
        });

        Downloader {
            sender,
            kill_switch,
        }
    }

    pub fn enqueue(&mut self, req: RangeRequest) -> Result<Option<RangeRequest>, ()> {
        if self.kill_switch.load(Ordering::Relaxed) {
            return Err(());
        }

        match self.sender.try_send(req) {
            Ok(()) => Ok(None),
            Err(err) => {
                if err.is_disconnected() {
                    self.kill_switch.store(true, Ordering::Relaxed);
                    Err(())
                } else {
                    Ok(Some(err.into_inner()))
                }
            }
        }
    }
}

async fn consume_and_dispatch_bytes(
    mut bytes_stream: BytesStream,
    results_sender: &UnboundedSender<ChunkStreamItem>,
    range_request: RangeRequest,
) -> Result<(), ()> {
    let mut chunk_index = 0;
    let mut chunk_offset = 0;
    while let Some(bytes_res) = bytes_stream.next().await {
        match bytes_res {
            Ok(bytes) => {
                let n_bytes = bytes.len();
                results_sender
                    .unbounded_send(Ok(ChunkItem {
                        part: range_request.part,
                        file_offset: range_request.file_range.start(),
                        range_offset: range_request.range_offset,
                        payload: ChunkItemPayload::Chunk {
                            bytes,
                            index: chunk_index,
                            offset: chunk_offset,
                        },
                    }))
                    .map_err(|_| ())?;
                chunk_index += 1;
                chunk_offset += n_bytes;
            }
            Err(IoError(msg)) => {
                let _ = results_sender.unbounded_send(Err(StreamError::Io(msg)));
                return Err(());
            }
        }
    }

    results_sender
        .unbounded_send(Ok(ChunkItem {
            part: range_request.part,
            file_offset: range_request.file_range.start(),
            range_offset: range_request.range_offset,
            payload: ChunkItemPayload::Terminator,
        }))
        .map_err(|_| ())
}
