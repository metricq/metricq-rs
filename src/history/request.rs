//! History request configuration and types for querying metric data.

use bytes::Bytes;

use crate::{
    codec::encode_request,
    error::MetricQResult,
    types::{TimeDelta, Timestamp},
};

/// Represents a request for historical metric data within a given
/// timeframe and interval.
pub struct HistoryRequest {
    /// The start timestamp of the requested historical data range.
    pub start: Timestamp,

    /// The end timestamp of the requested historical data range.
    pub end: Timestamp,

    /// The maximum time interval/resolution between samples.
    pub interval: TimeDelta,

    /// The type of historical query to perform.
    pub type_: HistoryRequestType,
}

impl HistoryRequest {
    /// Encodes the request into protocol buffer bytes.
    ///
    /// # Errors
    ///
    /// Returns and error if serialization into protocol buffer bytes fails.
    pub fn encode(&self) -> MetricQResult<Bytes> {
        encode_request(self)
    }
}

/// Specifies the type of history request to execute.
pub enum HistoryRequestType {
    /// Standard timeline query returning individual or flexible interval samples.
    Timeline,

    /// Aggregated timeline query returning statistical summaries over intervals.
    AggregateTimeline,
}
