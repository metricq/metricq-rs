//! Metric value types.

use crate::types::{metric::MetricId, timestamp::Timestamp};

/// A single metric measurement at a specific point in time.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TimeValue {
    /// Time at which the measurement was recorded.
    pub timestamp: Timestamp,

    /// Meadured value.
    pub value: f64,
}

impl TimeValue {
    /// Create a new [`TimeValue`] instance.
    #[must_use]
    pub const fn new(timestamp: Timestamp, value: f64) -> Self {
        Self { timestamp, value }
    }
}

/// A metric measurement associated with its metric identifier.
#[derive(Debug, Clone, PartialEq)]
pub struct MetricTimeValue {
    /// Identifier of the metric this sample belong to.
    pub metric: MetricId,

    /// Timestamped metric value.
    pub sample: TimeValue,
}

impl MetricTimeValue {
    /// Create a new [`MetricTimeValue`] instance.
    #[must_use]
    pub const fn new(metric: MetricId, sample: TimeValue) -> Self {
        Self { metric, sample }
    }
}
