//!

use bytes::Bytes;

use crate::{
    codec::encode_request,
    error::MetricQResult,
    types::{TimeDelta, Timestamp},
};

///
pub struct HistoryRequest {
    ///
    pub start: Timestamp,

    ///
    pub end: Timestamp,

    ///
    pub interval: TimeDelta,

    ///
    pub type_: HistoryRequestType,
}

impl HistoryRequest {
    ///
    pub fn encode(&self) -> MetricQResult<Bytes> {
        encode_request(self)
    }
}

pub enum HistoryRequestType {
    ///
    Timeline,

    ///
    AggregateTimeline,
}
