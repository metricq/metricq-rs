//! Core `MetricQ` data types.
//!
//! This module caontains the domain types used to represent metrics,
//! timestamps, values, and aggregated measurements.
//!

// These types are re-exported here to provide a single entry point
// for consumers of the MetricQ data model.
pub mod aggregate;
pub mod metric;
pub mod timestamp;
pub mod value;

// Aggregated metric statistics.
pub use aggregate::Aggregate;

// Idenfier used to referende a MetricQ metric.
pub use metric::MetricId;

pub use timestamp::{TimeDelta, Timestamp};
