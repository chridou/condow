use futures::Stream;

use crate::InclusiveRange;

pub struct RangeRequest {
    /// Index of the part
    pub part: usize,
    pub file_range: InclusiveRange,
    /// Offset of the part within the downloaded range
    pub range_offset: usize,
}

#[cfg(test)]
impl RangeRequest {
    pub fn len(&self) -> usize {
        self.file_range.len()
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
        let mut range_offset: usize = 0;

        let num_parts = calc_num_parts(range, part_size);

        let mut counter = 0;
        let iter = std::iter::from_fn(move || {
            if start > range.end_incl() {
                return None;
            }

            let current_end_incl = (start + part_size - 1).min(range.end_incl());
            let file_range = InclusiveRange(start, current_end_incl);
            let current_range_offset = range_offset;
            start = current_end_incl + 1;
            range_offset += file_range.len() + 1;

            let res = Some(RangeRequest {
                part: counter,
                file_range,
                range_offset: current_range_offset,
            });

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

    for part_size in 1..30 {
        for start in 0..20 {
            for end_offset in 0..40 {
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
                    items[0].file_range,
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
                        window[1].file_range.start() - window[0].file_range.end_incl(),
                        1,
                        "first item file range: part_size={} start={}, end_incl={}",
                        part_size,
                        start,
                        end_incl
                    );
                });
            }
        }
    }
}
