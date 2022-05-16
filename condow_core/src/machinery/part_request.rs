use futures::Stream;

use crate::{streams::BytesHint, InclusiveRange};

/// A request to download a part.
///
/// This is a part a download is split into.
#[derive(Debug, PartialEq, Eq)]
pub struct PartRequest {
    /// Index of the part
    pub part_index: u64,
    /// The range to be downloaded from the BLOB.
    pub blob_range: InclusiveRange,
    /// Offset of the part within the downloaded range
    pub range_offset: u64,
}

#[cfg(test)]
impl PartRequest {
    /// Returns the length of the part in bytes
    pub fn len(&self) -> u64 {
        self.blob_range.len()
    }
}

/// An [Iterator] over all the parts required to complete a download.
pub struct PartRequestIterator {
    start: u64,
    part_size: u64,
    next_range_offset: u64,
    next_part_index: u64,
    parts_left: u64,
    end_incl: u64,
    bytes_left: u64,
}

impl PartRequestIterator {
    pub fn new<R: Into<InclusiveRange>>(range: R, part_size: u64) -> Self {
        if part_size == 0 {
            panic!("part_size must not be 0. This is a bug somewhere else.");
        }

        let range = range.into();

        Self {
            start: range.start(),
            part_size,
            next_range_offset: 0,
            next_part_index: 0,
            parts_left: calc_num_parts(range, part_size),
            end_incl: range.end_incl(),
            bytes_left: range.len(),
        }
    }

    pub fn exact_size_hint(&self) -> u64 {
        self.parts_left
    }

    pub fn bytes_hint(&self) -> BytesHint {
        BytesHint::new_exact(self.bytes_left)
    }

    /// Turns this iterator into a [Stream]
    pub fn into_stream(self) -> impl Stream<Item = PartRequest> {
        futures::stream::iter(self)
    }

    #[cfg(test)]
    pub fn clone_continue(&self) -> Self {
        Self {
            start: self.start,
            part_size: self.part_size,
            next_range_offset: self.next_range_offset,
            next_part_index: self.next_part_index,
            parts_left: self.parts_left,
            end_incl: self.end_incl,
            bytes_left: self.bytes_left,
        }
    }
}

impl Iterator for PartRequestIterator {
    type Item = PartRequest;

    fn next(&mut self) -> Option<Self::Item> {
        if self.start > self.end_incl {
            return None;
        }

        let current_end_incl = (self.start + self.part_size - 1).min(self.end_incl);
        let blob_range = InclusiveRange(self.start, current_end_incl);
        self.start = current_end_incl + 1;

        let request = PartRequest {
            part_index: self.next_part_index,
            blob_range,
            range_offset: self.next_range_offset,
        };

        self.next_range_offset += blob_range.len();
        self.bytes_left -= blob_range.len();
        self.next_part_index += 1;

        self.parts_left -= 1;

        Some(request)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.parts_left as usize, Some(self.parts_left as usize))
    }
}

fn calc_num_parts(range: InclusiveRange, part_size: u64) -> u64 {
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

#[test]
fn test_n_parts_vs_iter_count() {
    for part_size in 1..50 {
        for start in 0..50 {
            for end_offset in 0..50 {
                let end_incl = start + end_offset;
                let range = InclusiveRange(start, end_incl);
                let iter = PartRequestIterator::new(range, part_size);
                let n_parts = iter.exact_size_hint();
                let items = iter.collect::<Vec<_>>();

                assert_eq!(
                    items.len() as u64,
                    n_parts,
                    "count: part_size={} start={}, end_incl={}",
                    part_size,
                    start,
                    end_incl
                );
                let total_len: u64 = items.iter().map(|r| r.len()).sum();
                assert_eq!(
                    total_len,
                    end_offset + 1,
                    "total len: part_size={} start={}, end_incl={}",
                    part_size,
                    start,
                    end_incl
                );
                assert_eq!(
                    items[0].part_index, 0,
                    "first item part index: part_size={} start={}, end_incl={}",
                    part_size, start, end_incl
                );
                assert_eq!(
                    items[items.len() - 1].part_index,
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
                        window[1].part_index - window[0].part_index,
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
                        rr.part_index, part_size, start, end_incl
                    );
                    next_range_offset += rr.blob_range.len();

                    let current_file_start = rr.blob_range.start();
                    assert_eq!(
                        current_file_start, next_file_start,
                        "file_offset (part {}): part_size={} start={}, end_incl={}",
                        rr.part_index, part_size, start, end_incl
                    );
                    next_file_start += rr.blob_range.len();
                })
            }
        }
    }
}
