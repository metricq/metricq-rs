//! History request configuration and types for querying metric data.

use bytes::Bytes;
use derive_builder::Builder;

use crate::{
    codec::encode_request,
    error::MetricQResult,
    types::{TimeDelta, Timestamp},
};

/// Represents a request for historical metric data within a given
/// timeframe and interval.
#[derive(Debug, Clone, Builder)]
#[builder(setter(into))]
pub struct HistoryRequest {
    /// The start timestamp of the requested historical data range.
    pub start_time: Timestamp,

    /// The end timestamp of the requested historical data range.
    pub end_time: Timestamp,

    /// The maximum time interval/resolution between samples.
    pub interval_max: TimeDelta,

    /// The type of historical query to perform.
    pub request_type: HistoryRequestType,
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
#[derive(Debug, Clone)]
pub enum HistoryRequestType {
    /// Standard timeline query returning individual or flexible interval samples.
    Timeline,

    /// Aggregated timeline query returning statistical summaries over intervals.
    AggregateTimeline,
}
