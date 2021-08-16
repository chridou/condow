use futures::Stream;

use crate::InclusiveRange;

#[derive(Debug)]
pub struct RangeRequest {
    /// Index of the part
    pub part: usize,
    pub blob_range: InclusiveRange,
    /// Offset of the part within the downloaded range
    pub range_offset: usize,
}

#[cfg(test)]
impl RangeRequest {
    pub fn len(&self) -> usize {
        self.blob_range.len()
    }
}

pub struct RangeStream;

impl RangeStream {
    pub fn create(
        range: InclusiveRange,
        part_size: usize,
    ) -> (usize, impl Stream<Item = RangeRequest>) {
        if part_size == 0 {
            panic!("part_size must not be 0. This is a bug.");
        }

        let mut start: usize = range.start();
        let mut next_range_offset: usize = 0;

        let num_parts = calc_num_parts(range, part_size);

        let mut counter = 0;
        let iter = std::iter::from_fn(move || {
            if start > range.end_incl() {
                return None;
            }

            let current_end_incl = (start + part_size - 1).min(range.end_incl());
            let blob_range = InclusiveRange(start, current_end_incl);
            start = current_end_incl + 1;

            let res = Some(RangeRequest {
                part: counter,
                blob_range,
                range_offset: next_range_offset,
            });

            next_range_offset += blob_range.len();
            counter += 1;

            res
        });

        (num_parts, futures::stream::iter(iter))
    }
}

fn calc_num_parts(range: InclusiveRange, part_size: usize) -> usize {
    let mut n_parts = range.len() / part_size;
    if range.len() % part_size != 0 {
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
        calc_num_parts(InclusiveRange(start, end_incl), part_size),
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
        calc_num_parts(InclusiveRange(start, end_incl), part_size),
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
        calc_num_parts(InclusiveRange(start, end_incl), part_size),
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
        calc_num_parts(InclusiveRange(start, end_incl), part_size),
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
        calc_num_parts(InclusiveRange(start, end_incl), part_size),
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
        calc_num_parts(InclusiveRange(start, end_incl), part_size),
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

    for part_size in 1..50 {
        for start in 0..50 {
            for end_offset in 0..50 {
                let end_incl = start + end_offset;
                let range = InclusiveRange(start, end_incl);
                let (n_parts, stream) = RangeStream::create(range, part_size);
                let items = stream.collect::<Vec<_>>().await;

                assert_eq!(
                    items.len(),
                    n_parts,
                    "count: part_size={} start={}, end_incl={}",
                    part_size,
                    start,
                    end_incl
                );
                let total_len: usize = items.iter().map(|r| r.len()).sum();
                assert_eq!(
                    total_len,
                    end_offset + 1,
                    "total len: part_size={} start={}, end_incl={}",
                    part_size,
                    start,
                    end_incl
                );
                assert_eq!(
                    items[0].part, 0,
                    "first item part index: part_size={} start={}, end_incl={}",
                    part_size, start, end_incl
                );
                assert_eq!(
                    items[items.len() - 1].part,
                    n_parts - 1,
                    "last item part index: part_size={} start={}, end_incl={}",
                    part_size,
                    start,
                    end_incl
                );
                assert_eq!(
                    items[0].blob_range,
                    InclusiveRange(start, (start + end_offset).min(start + part_size - 1)),
                    "first item file range: part_size={} start={}, end_incl={}",
                    part_size,
                    start,
                    end_incl
                );
                items.windows(2).for_each(|window| {
                    assert_eq!(
                        window[1].part - window[0].part,
                        1,
                        "first item file range: part_size={} start={}, end_incl={}",
                        part_size,
                        start,
                        end_incl
                    );
                });
                items.windows(2).for_each(|window| {
                    assert_eq!(
                        window[1].blob_range.start() - window[0].blob_range.end_incl(),
                        1,
                        "first item file range: part_size={} start={}, end_incl={}",
                        part_size,
                        start,
                        end_incl
                    );
                });

                let mut next_range_offset = 0;
                let mut next_file_start = range.start();
                items.iter().for_each(|rr| {
                    let current_range_offset = rr.range_offset;
                    assert_eq!(
                        current_range_offset, next_range_offset,
                        "range_offset (part {}): part_size={} start={}, end_incl={}",
                        rr.part, part_size, start, end_incl
                    );
                    next_range_offset += rr.blob_range.len();

                    let current_file_start = rr.blob_range.start();
                    assert_eq!(
                        current_file_start, next_file_start,
                        "file_offset (part {}): part_size={} start={}, end_incl={}",
                        rr.part, part_size, start, end_incl
                    );
                    next_file_start += rr.blob_range.len();
                })
            }
        }
    }
}
