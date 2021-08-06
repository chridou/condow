use std::ops::{Range, RangeFrom, RangeFull, RangeInclusive, RangeTo, RangeToInclusive};

use crate::errors::DownloadRangeError;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DownloadRange {
    FromTo(usize, usize),
    FromToInclusive(usize, usize),
    From(usize),
    To(usize),
    ToInclusive(usize),
    Full,
    Empty,
}

impl DownloadRange {
    pub fn validate(&self) -> Result<(), DownloadRangeError> {
        match self {
            Self::FromTo(a, b) => {
                if b < a {
                    Err(DownloadRangeError::InvalidRange(format!(
                        "FromTo: 'to'({}) must be lesser or equal than 'from'({})",
                        a, b
                    )))
                } else {
                    Ok(())
                }
            }
            Self::FromToInclusive(a, b) => {
                if b < a {
                    Err(DownloadRangeError::InvalidRange(format!(
                        "FromToInclusive: 'to'({}) must be lesser or equal than 'from'({})",
                        a, b
                    )))
                } else {
                    Ok(())
                }
            }
            _ => Ok(()),
        }
    }

    pub fn sanitize(&mut self) {
        match self {
            Self::FromTo(a, b) => {
                if b <= a {
                    *self = Self::Empty
                }
            }
            Self::FromToInclusive(a, b) => {
                if b < a {
                    *self = Self::Empty
                }
            }
            Self::To(0) => *self = Self::Empty,
            _ => {}
        }
    }
    pub fn inclusive_boundaries(self, size: usize) -> Option<(usize, usize)> {
        if size == 0 {
            return None;
        }

        let max_inclusive = size - 1;
        let inclusive = match self {
            Self::FromTo(a, b) => Some((a, (max_inclusive).min(b - 1))),
            Self::FromToInclusive(a, b) => Some((a, (max_inclusive).min(b))),
            Self::From(a) => Some((a, max_inclusive)),
            Self::To(b) => Some((0, (max_inclusive).min(b - 1))),
            Self::ToInclusive(b) => Some((0, (max_inclusive).min(b))),
            Self::Full => Some((0, max_inclusive)),
            Self::Empty => None,
        };

        if let Some((a, b)) = inclusive {
            if b < a {
                return None;
            }
        }

        inclusive
    }
}

impl From<RangeFull> for DownloadRange {
    fn from(_: RangeFull) -> Self {
        Self::Full
    }
}

impl From<Range<usize>> for DownloadRange {
    fn from(r: Range<usize>) -> Self {
        Self::FromTo(r.start, r.end)
    }
}

impl From<RangeInclusive<usize>> for DownloadRange {
    fn from(r: RangeInclusive<usize>) -> Self {
        Self::FromToInclusive(*r.start(), *r.end())
    }
}

impl From<RangeFrom<usize>> for DownloadRange {
    fn from(r: RangeFrom<usize>) -> Self {
        Self::From(r.start)
    }
}

impl From<RangeTo<usize>> for DownloadRange {
    fn from(r: RangeTo<usize>) -> Self {
        Self::To(r.end)
    }
}

impl From<RangeToInclusive<usize>> for DownloadRange {
    fn from(r: RangeToInclusive<usize>) -> Self {
        Self::ToInclusive(r.end)
    }
}

#[test]
fn range_full() {
    let result: DownloadRange = (..).into();
    assert_eq!(result, DownloadRange::Full);
}

#[test]
fn range() {
    let result: DownloadRange = (3..10).into();
    assert_eq!(result, DownloadRange::FromTo(3, 10));
}

#[test]
fn range_inclusive() {
    let result: DownloadRange = (3..=10).into();
    assert_eq!(result, DownloadRange::FromToInclusive(3, 10));
}

#[test]
fn range_from() {
    let result: DownloadRange = (3..).into();
    assert_eq!(result, DownloadRange::From(3));
}

#[test]
fn range_to() {
    let result: DownloadRange = (..10).into();
    assert_eq!(result, DownloadRange::To(10));
}

#[test]
fn range_to_inclusive() {
    let result: DownloadRange = (..=10).into();
    assert_eq!(result, DownloadRange::ToInclusive(10));
}
