//! Ranges for specifying downloads
use std::{
    fmt,
    ops::{Range, RangeFrom, RangeFull, RangeInclusive, RangeTo, RangeToInclusive},
};

use crate::errors::CondowError;

/// An inclusive range which can not have a length of 0.
///
/// A replacement for [RangeInclusive] with some sugar.
///
///
/// ## Examples
///
/// ```
/// # use condow_core::InclusiveRange;
///
/// let range1: InclusiveRange = (10..=20).into();
/// let range2 = InclusiveRange(10, 20);
///
/// assert_eq!(range1, range2);
/// ```
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct InclusiveRange(pub u64, pub u64);

impl InclusiveRange {
    /// Returns the index of the first item within the range
    ///
    /// ```
    /// use condow_core::InclusiveRange;
    ///
    /// let range: InclusiveRange = (4..=9).into();
    ///
    /// assert_eq!(range.start(), 4);
    /// ```
    pub fn start(&self) -> u64 {
        self.0
    }

    /// Returns the index of the last item within the range
    ///
    /// ```
    /// use condow_core::InclusiveRange;
    ///
    /// let range: InclusiveRange = (4..=9).into();
    ///
    /// assert_eq!(range.end_incl(), 9);
    /// ```
    pub fn end_incl(&self) -> u64 {
        self.1
    }

    pub fn validate(&self) -> Result<(), CondowError> {
        if self.end_incl() < self.start() {
            Err(CondowError::new_invalid_range(format!(
                "End must not be smaller than start: {}",
                self
            )))
        } else {
            Ok(())
        }
    }

    /// Returns the length of the range
    ///
    /// ```
    /// use condow_core::InclusiveRange;
    ///
    /// let range: InclusiveRange = (4..=9).into();
    ///
    /// assert_eq!(range.len(), 6);
    /// ```
    #[allow(clippy::len_without_is_empty)]
    #[inline]
    pub fn len(&self) -> u64 {
        if self.1 < self.0 {
            return 0;
        }

        self.1 - self.0 + 1
    }

    pub fn to_std_range(self) -> RangeInclusive<u64> {
        self.0..=self.1
    }

    #[cfg(test)]
    pub fn to_std_range_usize(self) -> RangeInclusive<usize> {
        self.0 as usize..=self.1 as usize
    }

    pub fn to_std_range_excl(self) -> Range<u64> {
        self.0..self.1 + 1
    }

    #[inline]
    pub fn advance(&mut self, by: u64) {
        self.0 += by
    }

    /// Returns a value for an  `HTTP-Range` header with bytes as the unit
    ///
    /// ```
    /// use condow_core::InclusiveRange;
    ///
    /// let range: InclusiveRange = (4..=9).into();
    ///
    /// assert_eq!(range.http_bytes_range_value(), "bytes=4-9");
    /// ```
    pub fn http_bytes_range_value(&self) -> String {
        format!("bytes={}-{}", self.0, self.1)
    }
}

impl fmt::Display for InclusiveRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}..{}]", self.0, self.1)
    }
}

impl From<RangeInclusive<u64>> for InclusiveRange {
    fn from(ri: RangeInclusive<u64>) -> Self {
        Self(*ri.start(), *ri.end())
    }
}

impl From<RangeToInclusive<u64>> for InclusiveRange {
    fn from(ri: RangeToInclusive<u64>) -> Self {
        Self(0, ri.end)
    }
}
impl From<InclusiveRange> for RangeInclusive<u64> {
    fn from(ir: InclusiveRange) -> Self {
        ir.to_std_range()
    }
}

impl From<InclusiveRange> for Range<u64> {
    fn from(ir: InclusiveRange) -> Self {
        ir.to_std_range_excl()
    }
}

/// A range defined by an offset and a length.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct OffsetRange(pub u64, pub u64);

impl OffsetRange {
    pub fn new(offset: u64, len: u64) -> Self {
        Self(offset, len)
    }

    /// Returns the index of the first item within the range
    ///
    /// ```
    /// use condow_core::OffsetRange;
    ///
    /// let range = OffsetRange::new(4, 6);
    ///
    /// assert_eq!(range.start(), 4);
    /// ```
    pub fn start(&self) -> u64 {
        self.0
    }

    /// Returns the index of the first item after the range
    ///
    /// ```
    /// use condow_core::OffsetRange;
    ///
    /// let range = OffsetRange::new(4, 6);
    ///
    /// assert_eq!(range.end_excl(), 10);
    /// ```
    pub fn end_excl(&self) -> u64 {
        self.0 + self.1
    }

    #[allow(clippy::len_without_is_empty)]
    /// Returns the length of the range
    ///
    /// ```
    /// use condow_core::OffsetRange;
    ///
    /// let range = OffsetRange::new(4, 6);
    ///
    /// assert_eq!(range.len(), 6);
    /// ```
    pub fn len(&self) -> u64 {
        self.1
    }
}

impl fmt::Display for OffsetRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}({})", self.0, self.1)
    }
}

/// A closed range
///
/// A closed range has a "defined end".
/// This does not require [Condow](crate::Condow) to do a size request.
/// [Condow](crate::Condow) can be configured to do a size request anyways
/// which allows to adjust the end of the range so that the whole range
/// is part of the file. This is the default behaviour. If this
/// behaviour is disabled, it is up to the caller to ensure a valid
/// range which does not exceed the end of the file is supplied.
///
/// Naming
///
/// variants where the last index is included in the range are suffixed with
/// "Inclusive". Those missing the suffix do not include the last index which
/// makes them exlusive. This is the same convention as with the stdlib.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ClosedRange {
    /// From including the first value to excluding the second value
    FromTo(u64, u64),
    /// From including the first value to including the second value
    FromToInclusive(u64, u64),
    /// From the beginning to excluding the value
    To(u64),
    /// From the beginning to including the value
    ToInclusive(u64),
}

impl ClosedRange {
    /// Validates the range.
    ///
    /// Fails with an error containig the reason if the range is invalid.
    ///
    /// Ranges with only 1 parameter are always valid.
    ///
    /// ## Examples
    ///
    /// [ClosedRange::FromTo]:
    ///
    /// ```
    /// use condow_core::ClosedRange;
    ///
    /// let range = ClosedRange::FromTo(0,1);
    /// assert!(range.validate().is_ok());
    ///
    /// let range = ClosedRange::FromTo(1,1);
    /// assert!(range.validate().is_ok());
    ///
    /// let range = ClosedRange::FromTo(1,0);
    /// assert!(range.validate().is_err());
    /// ```
    ///
    /// [ClosedRange::FromToInclusive]:
    ///
    /// ```
    /// use condow_core::ClosedRange;
    ///
    /// let range = ClosedRange::FromToInclusive(0,1);
    /// assert!(range.validate().is_ok());
    ///
    /// let range = ClosedRange::FromToInclusive(1,1);
    /// assert!(range.validate().is_ok());
    ///
    /// let range = ClosedRange::FromToInclusive(1,0);
    /// assert!(range.validate().is_err());
    /// ```
    pub fn validate(&self) -> Result<(), CondowError> {
        match self {
            Self::FromTo(a, b) => {
                if b < a {
                    Err(CondowError::new_invalid_range(format!(
                        "FromTo: 'to'({b}) must not be lesser than 'from'({a})"
                    )))
                } else {
                    Ok(())
                }
            }
            Self::FromToInclusive(a, b) => {
                if b < a {
                    Err(CondowError::new_invalid_range(format!(
                        "FromToInclusive: 'to'({b}) must not be lesser than 'from'({a})"
                    )))
                } else {
                    Ok(())
                }
            }
            _ => Ok(()),
        }
    }

    pub fn is_empty(self) -> bool {
        self.len() == 0 // is this efficient enough?
    }

    pub fn len(self) -> u64 {
        match self {
            Self::FromTo(a, b) => b - a,
            Self::FromToInclusive(a, b) => b - a + 1,
            Self::To(last_excl) => last_excl,
            Self::ToInclusive(last_incl) => last_incl + 1,
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

    pub fn incl_range(self) -> Option<InclusiveRange> {
        let inclusive = match self {
            Self::FromTo(a, b) => {
                if a == b {
                    return None;
                }
                InclusiveRange(a, b - 1)
            }
            Self::FromToInclusive(a, b) => InclusiveRange(a, b),
            Self::To(b) => {
                if b == 0 {
                    return None;
                }
                InclusiveRange(0, b - 1)
            }
            Self::ToInclusive(b) => InclusiveRange(0, b),
        };

        Some(inclusive)
    }
}

impl fmt::Display for ClosedRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClosedRange::To(to) => write!(f, "[0..{to}["),
            ClosedRange::ToInclusive(to) => write!(f, "[0..{to}]"),
            ClosedRange::FromTo(from, to) => write!(f, "[{from}..{to}["),
            ClosedRange::FromToInclusive(from, to) => write!(f, "[{from}..{to}]"),
        }
    }
}

impl From<Range<u64>> for ClosedRange {
    fn from(r: Range<u64>) -> Self {
        ClosedRange::FromTo(r.start, r.end)
    }
}

impl From<RangeInclusive<u64>> for ClosedRange {
    fn from(r: RangeInclusive<u64>) -> Self {
        ClosedRange::FromToInclusive(*r.start(), *r.end())
    }
}

impl From<RangeTo<u64>> for ClosedRange {
    fn from(r: RangeTo<u64>) -> Self {
        ClosedRange::To(r.end)
    }
}

impl From<RangeToInclusive<u64>> for ClosedRange {
    fn from(r: RangeToInclusive<u64>) -> Self {
        ClosedRange::ToInclusive(r.end)
    }
}

impl From<InclusiveRange> for ClosedRange {
    fn from(r: InclusiveRange) -> Self {
        ClosedRange::FromToInclusive(r.0, r.1)
    }
}

/// An open range
///
/// An open range has no "defined end".
/// This always requires [Condow](crate::Condow) to do a size request
/// so that the download can be split into parts of known size.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OpenRange {
    /// Download from the specified byte to the end
    From(u64),
    /// Download the whole file
    Full,
}

impl OpenRange {
    pub fn incl_range_from_size(self, size: u64) -> Result<Option<InclusiveRange>, CondowError> {
        if size == 0 {
            return Ok(None);
        }

        let max_inclusive = size - 1;
        let inclusive = match self {
            Self::From(a) => InclusiveRange(a, max_inclusive),
            Self::Full => InclusiveRange(0, max_inclusive),
        };

        inclusive.validate()?;

        Ok(Some(inclusive))
    }
}

impl fmt::Display for OpenRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OpenRange::From(from) => write!(f, "[{}..]", from),
            OpenRange::Full => write!(f, "[0..]"),
        }
    }
}

/// A range which specifies a download
///
/// Conversions for the standard Rust range syntax exist.
///
/// # Examples
///
/// ```rust
/// # use condow_core::*;
/// let dl = DownloadRange::from(..);
/// assert_eq!(dl, DownloadRange::Open(OpenRange::Full));
/// ```
///
/// ```rust
/// # use condow_core::*;
/// let dl = DownloadRange::from(3..);
/// assert_eq!(dl, DownloadRange::Open(OpenRange::From(3)));
/// ```
///
/// ```rust
/// # use condow_core::*;
/// let dl = DownloadRange::from(5..10);
/// assert_eq!(dl, DownloadRange::Closed(ClosedRange::FromTo(5,10)));
/// ```
///
/// ```rust
/// # use condow_core::*;
/// let dl = DownloadRange::from(5..=10);
/// assert_eq!(dl, DownloadRange::Closed(ClosedRange::FromToInclusive(5, 10)));
/// ```
///
/// ```rust
/// # use condow_core::*;
/// let dl = DownloadRange::from(..7);
/// assert_eq!(dl, DownloadRange::Closed(ClosedRange::To(7)));
/// ```
///
/// ```rust
/// # use condow_core::*;
/// let dl = DownloadRange::from(..=7);
/// assert_eq!(dl, DownloadRange::Closed(ClosedRange::ToInclusive(7)));
/// ```
///
/// ```rust
/// # use condow_core::*;
/// let dl = DownloadRange::from(InclusiveRange(1, 7));
/// assert_eq!(dl, DownloadRange::Closed(ClosedRange::FromToInclusive(1,7)));
/// ```
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DownloadRange {
    Open(OpenRange),
    Closed(ClosedRange),
}

impl DownloadRange {
    pub fn validate(&self) -> Result<(), CondowError> {
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

    pub fn incl_range_from_size(self, size: u64) -> Result<Option<InclusiveRange>, CondowError> {
        match self {
            DownloadRange::Open(r) => r.incl_range_from_size(size),
            DownloadRange::Closed(r) => {
                let inclusive = r.incl_range();
                if let Some(inclusive) = inclusive {
                    inclusive.validate()?
                }
                Ok(inclusive)
            }
        }
    }

    /// Returns the length in bytes for ranges where an end is defined
    pub fn len(&self) -> Option<u64> {
        match self {
            DownloadRange::Open(_) => None,
            DownloadRange::Closed(r) => Some(r.len()),
        }
    }

    pub fn is_empty(&self) -> Option<bool> {
        match self {
            DownloadRange::Open(_) => None,
        DownloadRange::Closed(r) => Some(r.is_empty()),
       }   }
}

impl fmt::Display for DownloadRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DownloadRange::Open(open) => open.fmt(f),
            DownloadRange::Closed(closed) => closed.fmt(f),
        }
    }
}

impl From<RangeFull> for DownloadRange {
    fn from(_: RangeFull) -> Self {
        Self::Open(OpenRange::Full)
    }
}

impl From<Range<u64>> for DownloadRange {
    fn from(r: Range<u64>) -> Self {
        Self::Closed(r.into())
    }
}

impl From<RangeInclusive<u64>> for DownloadRange {
    fn from(r: RangeInclusive<u64>) -> Self {
        Self::Closed(r.into())
    }
}

impl From<RangeFrom<u64>> for DownloadRange {
    fn from(r: RangeFrom<u64>) -> Self {
        Self::Open(OpenRange::From(r.start))
    }
}

impl From<RangeTo<u64>> for DownloadRange {
    fn from(r: RangeTo<u64>) -> Self {
        Self::Closed(r.into())
    }
}

impl From<RangeToInclusive<u64>> for DownloadRange {
    fn from(r: RangeToInclusive<u64>) -> Self {
        Self::Closed(r.into())
    }
}

impl From<InclusiveRange> for DownloadRange {
    fn from(r: InclusiveRange) -> Self {
        Self::Closed(r.into())
    }
}

impl From<OffsetRange> for DownloadRange {
    fn from(r: OffsetRange) -> Self {
        Self::Closed(ClosedRange::FromTo(r.start(), r.end_excl()))
    }
}

impl From<ClosedRange> for DownloadRange {
    fn from(r: ClosedRange) -> Self {
        Self::Closed(r)
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
