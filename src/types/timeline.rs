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
    pub fn is_empty(&self) -> bool {
        todo!()
    }

    /// Returns the number of data points contained in the timeline.
    #[must_use]
    pub fn len(&self) -> usize {
        todo!()
    }
}
