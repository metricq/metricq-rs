//! Response structures and value representations for historical metric data queries.

use bytes::Bytes;

use crate::{
    codec::decode_response,
    error::MetricQResult,
    types::{Aggregate, MetricId, Timestamp},
};

/// Represents the response received from a historical metric query,
/// containing samples or an error message.
#[derive(Debug, Clone, PartialEq)]
pub struct HistoryResponse {
    /// The identifier of the metric for which history was requested.
    pub metric: MetricId,

    /// A vector of historical data samples returned by the query.
    pub samples: Vec<HistorySample>,

    /// An optional error message if the history query encountered
    /// a server-side failure.
    pub error: Option<String>,
}

impl HistoryResponse {
    /// Decodes a raw protocol buffer byte payload into a [`HistoryResponse`].
    ///
    /// # Errors
    ///
    /// Returns an error if the payload cannot be parsed as a valid protocol buffer message.
    pub fn decode(payload: &Bytes) -> MetricQResult<Self> {
        decode_response(payload)
    }
}

/// Represents an individual historical sample at a specific timestamp.
#[derive(Debug, Clone, PartialEq)]
pub struct HistorySample {
    /// The timestamp of the sample.
    pub timestamp: Timestamp,

    /// The value or aggregated statistics of the sample.
    pub value: HistoryValue,
}

/// Represents the value of a historical sample, which can be a single numeric values or an aggregated summary.
#[derive(Debug, Clone, PartialEq)]
pub enum HistoryValue {
    /// A single floating-point metric value.
    Value(f64),

    /// An aggregated summary of metric values over a time interval.
    Aggregate {
        /// The minimum metric value recorded in the interval.
        minimum: f64,
        /// The maximum metric value recorded in the interval.
        maximum: f64,
        /// The average metric value recorded in the interval.
        average: f64,
        /// Detailed aggregate statistics including sum, count, integral, and active time.
        statistics: Aggregate,
    },
}
