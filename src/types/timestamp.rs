//! Time-related types used by `MetricQ`.

use std::time::Duration;

use derive_more::From;

/// A point in time represented as nanoseconds.
///
/// The internal value stores the timestamp as an integer number of
/// nanoseconds accourding to the `MetricQ` time representation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, From)]
pub struct Timestamp(i64);

impl Timestamp {
    /// Returns the timestamp value in nanoseconds.
    #[must_use]
    pub const fn as_nanos(&self) -> i64 {
        self.0
    }

    ///
    pub fn from_nanos(nanos: i64) -> Self {
        Self(nanos)
    }
}

/// A duration between two timestamps.
pub type TimeDelta = Duration;
