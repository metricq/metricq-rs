//! Metric aggregation types.

use std::time::Duration;

/// Represents an aggregated metric value over a time interval.
///
/// Aggreagates contain statistical information collected for a metric
/// within a time range. They are used when querying historical metric
/// data or when processing aggregated measurements.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Aggregate {
    /// Lowest value observed during the aggregation period.
    pub min: f64,

    /// Highest value observed during the aggregation period.
    pub max: f64,

    /// Sum of all values obserced during the aggregation period.
    pub sum: f64,

    /// Number of values inclueded in this aggregate.
    pub count: u64,

    /// Integral of the metric values over time in nanoseconds.
    pub integral_ns: f64,

    /// Amount of time during which the metric was considered active.
    pub active_time: Duration,
}
