//! Types representing metric timelines.

use crate::types::{aggregate::Aggregate, value::TimeValue};

/// A sequence of metric data points over time.
///
/// A timeline can either contain raw metric values or pre-computed
/// aggregates. An empty timeline represents the absence of data.
#[derive(Debug, Clone)]
pub enum Timeline {
    /// Raw metric values ordered by timestamp.
    Values(Vec<TimeValue>),

    /// Aggregated metric values ordered by timestamp.
    Aggregates(Vec<Aggregate>),

    /// A timeline containing no data.
    Empty,
}

impl Timeline {
    /// Returns whether the timeline contains no data points.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        match &self {
            Self::Empty => true,
            Self::Aggregates(aggrs) if aggrs.is_empty() => true,
            Self::Values(vals) if vals.is_empty() => true,
            _ => false,
        }
    }

    /// Returns the number of data points contained in the timeline.
    #[must_use]
    pub const fn len(&self) -> usize {
        match &self {
            Self::Empty => 0,
            Self::Aggregates(aggrs) => aggrs.len(),
            Self::Values(vals) => vals.len(),
        }
    }
}
