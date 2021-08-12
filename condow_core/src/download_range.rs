use std::ops::{Range, RangeFrom, RangeFull, RangeInclusive, RangeTo, RangeToInclusive};

use crate::errors::DownloadError;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct InclusiveRange(pub usize, pub usize);

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct ExclusiveOpenRange(pub usize, pub Option<usize>);

impl InclusiveRange {
    pub fn start(&self) -> usize {
        self.0
    }

    pub fn end_incl(&self) -> usize {
        self.1
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        if self.1 < self.0 {
            return 0;
        }

        self.1 - self.0 + 1
    }

    pub fn to_std_range(self) -> RangeInclusive<usize> {
        self.0..=self.1
    }

    pub fn to_std_range_excl(self) -> Range<usize> {
        self.0..self.1 + 1
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ClosedRange {
    FromTo(usize, usize),
    FromToInclusive(usize, usize),
    To(usize),
    ToInclusive(usize),
}

impl ClosedRange {
    pub fn validate(&self) -> Result<(), DownloadError> {
        match self {
            Self::FromTo(a, b) => {
                if b < a {
                    Err(DownloadError::InvalidRange(format!(
                        "FromTo: 'to'({}) must be lesser or equal than 'from'({})",
                        a, b
                    )))
                } else {
                    Ok(())
                }
            }
            Self::FromToInclusive(a, b) => {
                if b < a {
                    Err(DownloadError::InvalidRange(format!(
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

    pub fn sanitized(self) -> Option<Self> {
        match self {
            Self::FromTo(a, b) => {
                if b <= a {
                    return None;
                }
            }
            Self::FromToInclusive(a, b) => {
                if b < a {
                    return None;
                }
            }
            Self::To(0) => return None,
            Self::To(_) => {}
            Self::ToInclusive(_) => {}
        }

        Some(self)
    }

    pub fn incl_range_from_size(self, size: usize) -> Option<InclusiveRange> {
        if size == 0 {
            return None;
        }

        let max_inclusive = size - 1;
        let inclusive = match self {
            Self::FromTo(a, b) => {
                if b == 0 {
                    return None;
                }
                Some(InclusiveRange(a, (max_inclusive).min(b - 1)))
            }
            Self::FromToInclusive(a, b) => Some(InclusiveRange(a, (max_inclusive).min(b))),
            Self::To(b) => {
                if b == 0 {
                    return None;
                }
                Some(InclusiveRange(0, (max_inclusive).min(b - 1)))
            }
            Self::ToInclusive(b) => Some(InclusiveRange(0, (max_inclusive).min(b))),
        };

        if let Some(InclusiveRange(a, b)) = inclusive {
            if b < a {
                return None;
            }
        }

        inclusive
    }

    pub fn incl_range(self) -> Option<InclusiveRange> {
        let inclusive = match self {
            Self::FromTo(a, b) => {
                if b == 0 {
                    return None;
                }
                Some(InclusiveRange(a, b - 1))
            }
            Self::FromToInclusive(a, b) => Some(InclusiveRange(a, b)),
            Self::To(b) => {
                if b == 0 {
                    return None;
                }
                Some(InclusiveRange(0, b - 1))
            }
            Self::ToInclusive(b) => Some(InclusiveRange(0, b)),
        };

        if let Some(InclusiveRange(a, b)) = inclusive {
            if b < a {
                return None;
            }
        }

        inclusive
    }

    pub fn excl_open_range(self) -> Option<ExclusiveOpenRange> {
        let exclusive = match self {
            Self::FromTo(a, b) => {
                if b == 0 {
                    return None;
                }
                Some(ExclusiveOpenRange(a, Some(b)))
            }
            Self::FromToInclusive(a, b) => Some(ExclusiveOpenRange(a, Some(b + 1))),
            Self::To(b) => {
                if b == 0 {
                    return None;
                }
                Some(ExclusiveOpenRange(0, Some(b)))
            }
            Self::ToInclusive(b) => Some(ExclusiveOpenRange(0, Some(b + 1))),
        };

        if let Some(ExclusiveOpenRange(a, Some(b))) = exclusive {
            if b <= a {
                return None;
            }
        }

        exclusive
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OpenRange {
    From(usize),
    Full,
}

impl OpenRange {
    pub fn incl_range_from_size(self, size: usize) -> Option<InclusiveRange> {
        if size == 0 {
            return None;
        }

        let max_inclusive = size - 1;
        let inclusive = match self {
            Self::From(a) => Some(InclusiveRange(a, max_inclusive)),
            Self::Full => Some(InclusiveRange(0, max_inclusive)),
        };

        if let Some(InclusiveRange(a, b)) = inclusive {
            if b < a {
                return None;
            }
        }

        inclusive
    }

    pub fn excl_open_range(self) -> Option<ExclusiveOpenRange> {
        match self {
            Self::From(a) => Some(ExclusiveOpenRange(a, None)),
            Self::Full => Some(ExclusiveOpenRange(0, None)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DownloadRange {
    Open(OpenRange),
    Closed(ClosedRange),
}

impl DownloadRange {
    pub fn validate(&self) -> Result<(), DownloadError> {
        match self {
            DownloadRange::Open(_) => Ok(()),
            DownloadRange::Closed(r) => r.validate(),
        }
    }

    pub fn sanitized(self) -> Option<Self> {
        match self {
            DownloadRange::Open(_) => Some(self),
            DownloadRange::Closed(r) => r.sanitized().map(DownloadRange::Closed),
        }
    }

    pub fn incl_range_from_size(self, size: usize) -> Option<InclusiveRange> {
        match self {
            DownloadRange::Open(r) => r.incl_range_from_size(size),
            DownloadRange::Closed(r) => r.incl_range_from_size(size),
        }
    }

    pub fn excl_open_range(self) -> Option<ExclusiveOpenRange> {
        match self {
            DownloadRange::Open(r) => r.excl_open_range(),
            DownloadRange::Closed(r) => r.excl_open_range(),
        }
    }
}

impl From<RangeFull> for DownloadRange {
    fn from(_: RangeFull) -> Self {
        Self::Open(OpenRange::Full)
    }
}

impl From<Range<usize>> for DownloadRange {
    fn from(r: Range<usize>) -> Self {
        Self::Closed(ClosedRange::FromTo(r.start, r.end))
    }
}

impl From<RangeInclusive<usize>> for DownloadRange {
    fn from(r: RangeInclusive<usize>) -> Self {
        Self::Closed(ClosedRange::FromToInclusive(*r.start(), *r.end()))
    }
}

impl From<RangeFrom<usize>> for DownloadRange {
    fn from(r: RangeFrom<usize>) -> Self {
        Self::Open(OpenRange::From(r.start))
    }
}

impl From<RangeTo<usize>> for DownloadRange {
    fn from(r: RangeTo<usize>) -> Self {
        Self::Closed(ClosedRange::To(r.end))
    }
}

impl From<RangeToInclusive<usize>> for DownloadRange {
    fn from(r: RangeToInclusive<usize>) -> Self {
        Self::Closed(ClosedRange::ToInclusive(r.end))
    }
}

#[test]
fn range_full() {
    let result: DownloadRange = (..).into();
    assert_eq!(result, DownloadRange::Open(OpenRange::Full));
}

#[test]
fn range() {
    let result: DownloadRange = (3..10).into();
    assert_eq!(result, DownloadRange::Closed(ClosedRange::FromTo(3, 10)));
}

#[test]
fn range_inclusive() {
    let result: DownloadRange = (3..=10).into();
    assert_eq!(
        result,
        DownloadRange::Closed(ClosedRange::FromToInclusive(3, 10))
    );
}

#[test]
fn range_from() {
    let result: DownloadRange = (3..).into();
    assert_eq!(result, DownloadRange::Open(OpenRange::From(3)));
}

#[test]
fn range_to() {
    let result: DownloadRange = (..10).into();
    assert_eq!(result, DownloadRange::Closed(ClosedRange::To(10)));
}

#[test]
fn range_to_inclusive() {
    let result: DownloadRange = (..=10).into();
    assert_eq!(result, DownloadRange::Closed(ClosedRange::ToInclusive(10)));
}
