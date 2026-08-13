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

/// A metric measurement associated with its metric identifier.
#[derive(Debug, Clone, PartialEq)]
pub struct MetricTimeValue {
    /// Identifier of the metric this sample belong to.
    pub metric: MetricId,

    /// Timestamped metric value.
    pub sample: TimeValue,
}
