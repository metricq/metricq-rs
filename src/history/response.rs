//!

use bytes::Bytes;

use crate::{
    codec::decode_response,
    error::MetricQResult,
    types::{Aggregate, MetricId, Timestamp},
};

///
#[derive(Debug, Clone, PartialEq)]
pub struct HistoryResponse {
    ///
    pub metric: MetricId,

    ///
    pub samples: Vec<HistorySample>,

    ///
    pub error: Option<String>,
}

impl HistoryResponse {
    ///
    pub fn decode(payload: Bytes) -> MetricQResult<Self> {
        decode_response(payload)
    }
}

///
#[derive(Debug, Clone, PartialEq)]
pub struct HistorySample {
    ///
    pub timestamp: Timestamp,

    ///
    pub value: HistoryValue,
}

///
#[derive(Debug, Clone, PartialEq)]
pub enum HistoryValue {
    ///
    Value(f64),

    ///
    Aggregate {
        minimum: f64,
        maximum: f64,
        average: f64,
        statistics: Aggregate,
    },
}
