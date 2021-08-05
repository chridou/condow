use crate::condow_client::CondowClient;
use crate::config::Config;
use crate::errors::DownloadPartError;
use crate::streams::ChunkStream;

pub async fn download<C: CondowClient>(
    client: C,
    location: C::Location,
    start: usize,
    end_incl: usize,
    config: Config,
) -> Result<ChunkStream, DownloadPartError> {
    let (n_parts, ranges_stream) = range_stream::create(config.part_size.0, start, end_incl);

    if n_parts == 0 {
        panic!("n_parts must not be 0. This is a bug");
    }

    let total_bytes = start - end_incl + 1;

    let (chunk_stream, sender) = ChunkStream::new(n_parts, Some(total_bytes));

    tokio::spawn(async move {
        downloader::download_concurrently(
            ranges_stream,
            config.max_concurrent.0.min(n_parts),
            sender,
            client,
            config,
            location,
        )
        .await
    });

    Ok(chunk_stream)
}

mod downloader {
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };

    use futures::{
        channel::mpsc::{self, Sender, UnboundedSender},
        Stream, StreamExt,
    };

    use crate::{
        condow_client::CondowClient,
        config::Config,
        streams::{BytesStream, ChunkItem, ChunkItemPayload, ChunkStreamItem, StreamError},
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
                        config.buffer_size.0,
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

                let n_downloaders = self.downloaders.len();

                loop {
                    if attempt % self.downloaders.len() == 0 {
                        tokio::time::sleep(self.config.buffers_full_delay.0).await;
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
                            .download_range(
                                location.clone(),
                                range_request.start,
                                range_request.end_incl,
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
                            offset: range_request.start,
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
                Err(err) => {
                    let _ = results_sender.unbounded_send(Err(StreamError::Io(err)));
                    return Err(());
                }
            }
        }

        results_sender
            .unbounded_send(Ok(ChunkItem {
                part: range_request.part,
                offset: range_request.start,
                payload: ChunkItemPayload::Terminator,
            }))
            .map_err(|_| ())
    }
}

mod range_stream {
    use futures::Stream;

    pub struct RangeRequest {
        /// Index of the part
        pub part: usize,
        pub start: usize,
        pub end_incl: usize,
    }

    impl RangeRequest {
        #[cfg(test)]
        pub fn len(&self) -> usize {
            self.end_incl - self.start + 1
        }
    }

    pub fn create(
        part_size: usize,
        mut start: usize,
        end_incl: usize,
    ) -> (usize, impl Stream<Item = RangeRequest>) {
        if part_size == 0 {
            panic!("part_size must not be 0. This is a bug.");
        }

        let num_parts = calc_num_parts(part_size, start, end_incl);

        let mut counter = 0;
        let iter = std::iter::from_fn(move || {
            if start > end_incl {
                return None;
            }

            let current_start = start;
            let current_end_incl = (current_start + part_size - 1).min(end_incl);
            start = current_end_incl + 1;

            let res = Some(RangeRequest {
                part: counter,
                start: current_start,
                end_incl: current_end_incl,
            });

            counter += 1;

            res
        });

        (num_parts, futures::stream::iter(iter))
    }

    fn calc_num_parts(part_size: usize, start: usize, end_incl: usize) -> usize {
        let len = end_incl - start + 1;

        let mut n_parts = len / part_size;
        if len % part_size != 0 {
            n_parts += 1;
        }

        n_parts
    }

    #[test]
    fn test_calc_num_parts() {
        let part_size = 1;
        let start = 0;
        let end_incl = 0;
        assert_eq!(
            calc_num_parts(part_size, start, end_incl),
            1,
            "size={} start={}, end_incl={}",
            part_size,
            start,
            end_incl
        );

        let part_size = 1;
        let start = 0;
        let end_incl = 1;
        assert_eq!(
            calc_num_parts(part_size, start, end_incl),
            2,
            "size={} start={}, end_incl={}",
            part_size,
            start,
            end_incl
        );

        let part_size = 1;
        let start = 1;
        let end_incl = 3;
        assert_eq!(
            calc_num_parts(part_size, start, end_incl),
            3,
            "size={} start={}, end_incl={}",
            part_size,
            start,
            end_incl
        );

        let part_size = 2;
        let start = 1;
        let end_incl = 3;
        assert_eq!(
            calc_num_parts(part_size, start, end_incl),
            2,
            "size={} start={}, end_incl={}",
            part_size,
            start,
            end_incl
        );

        let part_size = 3;
        let start = 1;
        let end_incl = 3;
        assert_eq!(
            calc_num_parts(part_size, start, end_incl),
            1,
            "size={} start={}, end_incl={}",
            part_size,
            start,
            end_incl
        );

        let part_size = 4;
        let start = 1;
        let end_incl = 3;
        assert_eq!(
            calc_num_parts(part_size, start, end_incl),
            1,
            "size={} start={}, end_incl={}",
            part_size,
            start,
            end_incl
        );

        // let part_size = 0;
        // let start = 0;
        // let end_incl = 0;
        // assert_eq!(calc_num_parts(part_size, start, end_incl), 0, "size={} start={}, end_incl={}", part_size, start, end_incl);

        // let part_size = 0;
        // let start = 0;
        // let end_incl = 0;
        // assert_eq!(calc_num_parts(part_size, start, end_incl), 0, "size={} start={}, end_incl={}", part_size, start, end_incl);

        // let part_size = 0;
        // let start = 0;
        // let end_incl = 0;
        // assert_eq!(calc_num_parts(part_size, start, end_incl), 0, "size={} start={}, end_incl={}", part_size, start, end_incl);

        // let part_size = 0;
        // let start = 0;
        // let end_incl = 0;
        // assert_eq!(calc_num_parts(part_size, start, end_incl), 0, "size={} start={}, end_incl={}", part_size, start, end_incl);

        // let part_size = 0;
        // let start = 0;
        // let end_incl = 0;
        // assert_eq!(calc_num_parts(part_size, start, end_incl), 0, "size={} start={}, end_incl={}", part_size, start, end_incl);

        // let part_size = 0;
        // let start = 0;
        // let end_incl = 0;
        // assert_eq!(calc_num_parts(part_size, start, end_incl), 0, "size={} start={}, end_incl={}", part_size, start, end_incl);

        // let part_size = 0;
        // let start = 0;
        // let end_incl = 0;
        // assert_eq!(calc_num_parts(part_size, start, end_incl), 0, "size={} start={}, end_incl={}", part_size, start, end_incl);

        // let part_size = 0;
        // let start = 0;
        // let end_incl = 0;
        // assert_eq!(calc_num_parts(part_size, start, end_incl), 0, "size={} start={}, end_incl={}", part_size, start, end_incl);
    }

    #[tokio::test]
    async fn test_n_parts_vs_stream_count() {
        use futures::StreamExt as _;

        for part_size in 1..30 {
            for start in 0..20 {
                for len in 0..20 {
                    let end_incl = start + len;
                    let (n_parts, stream) = create(part_size, start, end_incl);
                    let items = stream.collect::<Vec<_>>().await;

                    assert_eq!(
                        items.len(),
                        n_parts,
                        "count: size={} start={}, end_incl={}",
                        part_size,
                        start,
                        end_incl
                    );
                    let total_len: usize = items.iter().map(|r| r.len()).sum();
                    assert_eq!(
                        total_len,
                        len + 1,
                        "len: size={} start={}, end_incl={}",
                        part_size,
                        start,
                        end_incl
                    );
                    assert_eq!(
                        items[0].part, 0,
                        "len: size={} start={}, end_incl={}",
                        part_size, start, end_incl
                    );
                    assert_eq!(
                        items[items.len() - 1].part,
                        n_parts - 1,
                        "len: size={} start={}, end_incl={}",
                        part_size,
                        start,
                        end_incl
                    );
                }
            }
        }
    }
}
