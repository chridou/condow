use crate::condow_client::CondowClient;
use crate::config::Config;
use crate::errors::DownloadPartError;
use crate::streams::ChunkStream;

pub async fn download<C: CondowClient>(
    client: &C,
    location: C::Location,
    start: usize,
    end_incl: usize,
    config: &Config,
) -> Result<ChunkStream, DownloadPartError> {
    unimplemented!()
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
        pub fn len(&self) -> usize {
            self.end_incl - self.start + 1
        }
    }

    pub fn create(part_size: usize, mut start: usize, end_incl: usize) -> (usize, impl Stream<Item=RangeRequest>) {
        if part_size == 0 {
            panic!("part_size must not be 0. This is a bug.");
        }

        let num_parts = calc_num_parts(part_size, start, end_incl);

        let mut counter = 0;
        let iter = std::iter::from_fn(move || {
            if start > end_incl {
                return None
            }

            let current_start = start;
            let current_end_incl = (current_start + part_size -1).min(end_incl);
            start = current_end_incl +1;

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
        assert_eq!(calc_num_parts(part_size, start, end_incl), 1, "size={} start={}, end_incl={}", part_size, start, end_incl);

        let part_size = 1;
        let start = 0;
        let end_incl = 1;
        assert_eq!(calc_num_parts(part_size, start, end_incl), 2, "size={} start={}, end_incl={}", part_size, start, end_incl);

        let part_size = 1;
        let start = 1;
        let end_incl = 3;
        assert_eq!(calc_num_parts(part_size, start, end_incl), 3, "size={} start={}, end_incl={}", part_size, start, end_incl);

        let part_size = 2;
        let start = 1;
        let end_incl = 3;
        assert_eq!(calc_num_parts(part_size, start, end_incl), 2, "size={} start={}, end_incl={}", part_size, start, end_incl);

        let part_size = 3;
        let start = 1;
        let end_incl = 3;
        assert_eq!(calc_num_parts(part_size, start, end_incl), 1, "size={} start={}, end_incl={}", part_size, start, end_incl);

        let part_size = 4;
        let start = 1;
        let end_incl = 3;
        assert_eq!(calc_num_parts(part_size, start, end_incl), 1, "size={} start={}, end_incl={}", part_size, start, end_incl);

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
                    let end_incl = start+len;
                    let (n_parts, stream) = create(part_size, start, end_incl);
                    let items = stream.collect::<Vec<_>>().await;

                    assert_eq!(items.len(), n_parts, "count: size={} start={}, end_incl={}", part_size, start, end_incl);
                    let total_len: usize = items.iter().map(|r| r.len()).sum();
                    assert_eq!(total_len, len + 1, "len: size={} start={}, end_incl={}", part_size, start, end_incl);
                    assert_eq!(items[0].part, 0, "len: size={} start={}, end_incl={}", part_size, start, end_incl);
                    assert_eq!(items[items.len()-1].part, n_parts-1, "len: size={} start={}, end_incl={}", part_size, start, end_incl);
                }
            }
        }
    }
}