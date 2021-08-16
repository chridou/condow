use futures::{
    channel::mpsc::{self, Sender, UnboundedSender},
    Stream, StreamExt,
};

use crate::{
    condow_client::{CondowClient, DownloadSpec},
    config::Config,
    errors::{CondowError, IoError},
    streams::{BytesStream, Chunk, ChunkStreamItem},
};

use super::{range_stream::RangeRequest, KillSwitch};

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
    kill_switch: KillSwitch,
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
        let kill_switch = KillSwitch::new();
        let downloaders: Vec<_> = (0..n_concurrent)
            .map(|_| {
                Downloader::new(
                    client.clone(),
                    results_sender.clone(),
                    kill_switch.clone(),
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

struct Downloader {
    sender: Sender<RangeRequest>,
    kill_switch: KillSwitch,
}

impl Downloader {
    pub fn new<C: CondowClient>(
        client: C,
        results_sender: UnboundedSender<ChunkStreamItem>,
        kill_switch: KillSwitch,
        location: C::Location,
        buffer_size: usize,
    ) -> Self {
        let (sender, request_receiver) = mpsc::channel::<RangeRequest>(buffer_size);

        tokio::spawn({
            let kill_switch = kill_switch.clone();
            async move {
                let mut request_receiver = Box::pin(request_receiver);
                while let Some(range_request) = request_receiver.next().await {
                    if kill_switch.is_pushed() {
                        break;
                    }

                    match client
                        .download(
                            location.clone(),
                            DownloadSpec::Range(range_request.blob_range),
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
                                kill_switch.push_the_button();
                                request_receiver.close();
                                break;
                            }
                        }
                        Err(err) => {
                            kill_switch.push_the_button();
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
        if self.kill_switch.is_pushed() {
            return Err(());
        }

        match self.sender.try_send(req) {
            Ok(()) => Ok(None),
            Err(err) => {
                if err.is_disconnected() {
                    self.kill_switch.push_the_button();
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
    let mut offset_in_range = 0;
    let mut bytes_received = 0;
    let bytes_expected = range_request.blob_range.len();
    while let Some(bytes_res) = bytes_stream.next().await {
        match bytes_res {
            Ok(bytes) => {
                let n_bytes = bytes.len();
                bytes_received += bytes.len();

                if bytes_received > bytes_expected {
                    let msg = Err(CondowError::Other(format!(
                        "received mor ebytes than expected for part {} ({}..={}). expected {}, received {}",
                        range_request.part,
                        range_request.blob_range.start(),
                        range_request.blob_range.end_incl(),
                        range_request.blob_range.len(),
                        bytes_received
                    )));
                    let _ = results_sender.unbounded_send(msg);
                    return Err(());
                }

                results_sender
                    .unbounded_send(Ok(Chunk {
                        part_index: range_request.part,
                        chunk_index,
                        blob_offset: range_request.blob_range.start() + offset_in_range,
                        range_offset: range_request.range_offset + offset_in_range,
                        bytes,
                        bytes_left: bytes_expected - bytes_received,
                    }))
                    .map_err(|_| ())?;
                chunk_index += 1;
                offset_in_range += n_bytes;
            }
            Err(IoError(msg)) => {
                let _ = results_sender.unbounded_send(Err(CondowError::Io(msg)));
                return Err(());
            }
        }
    }

    if bytes_received != bytes_expected {
        let msg = Err(CondowError::Other(format!(
            "received wrong number of bytes for part {} ({}..={}). expected {}, received {}",
            range_request.part,
            range_request.blob_range.start(),
            range_request.blob_range.end_incl(),
            range_request.blob_range.len(),
            bytes_received
        )));
        let _ = results_sender.unbounded_send(msg);
        Err(())
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use crate::{
        config::Config,
        machinery::{downloader::Downloader, range_stream::RangeStream, KillSwitch},
        streams::{BytesHint, Chunk, ChunkStream},
        test_utils::*,
        InclusiveRange,
    };

    #[tokio::test]
    async fn from_0_to_inclusive_range_larger_than_part_size() {
        let client = TestCondowClient::new().max_chunk_size(3);

        for range in [
            InclusiveRange(0, 8),
            InclusiveRange(0, 9),
            InclusiveRange(0, 10),
        ] {
            check(range, client.clone(), 10).await
        }
    }

    async fn check(range: InclusiveRange, client: TestCondowClient, part_size_bytes: usize) {
        let config = Config::default()
            .buffer_size(10)
            .buffers_full_delay_ms(0)
            .part_size_bytes(part_size_bytes)
            .max_concurrency(1); // Won't work otherwise

        let bytes_hint = BytesHint::new(range.len(), Some(range.len()));

        let (_n_parts, mut ranges_stream) =
            RangeStream::create(range, config.part_size_bytes.into());

        let (result_stream, results_sender) = ChunkStream::new(bytes_hint);

        let mut downloader = Downloader::new(
            client,
            results_sender,
            KillSwitch::new(),
            (),
            config.buffer_size.into(),
        );

        while let Some(next) = ranges_stream.next().await {
            let _ = downloader.enqueue(next).unwrap();
        }

        drop(downloader); // Ends the stream

        let result = result_stream.collect::<Vec<_>>().await;
        let result = result.into_iter().collect::<Result<Vec<_>, _>>().unwrap();

        let total_bytes: usize = result.iter().map(|c| c.bytes.len()).sum();
        assert_eq!(total_bytes, range.len(), "total_bytes");

        let mut next_range_offset = 0;
        let mut next_blob_offset = range.start();

        result.iter().for_each(|c| {
            let Chunk {
                part_index,
                blob_offset,
                range_offset,
                bytes,
                ..
            } = c;
            assert_eq!(
                *range_offset, next_range_offset,
                "part {}, range_offset: {:?}",
                part_index, range
            );
            assert_eq!(
                *blob_offset, next_blob_offset,
                "part {}, blob_offset: {:?}",
                part_index, range
            );
            next_range_offset += bytes.len();
            next_blob_offset += bytes.len();
        });
    }
}
