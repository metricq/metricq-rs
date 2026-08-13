//!

use std::time::Duration;

use bytes::Bytes;
use protobuf::Message;

use crate::{
    error::MetricQResult,
    history::{
        HistoryRequest, HistoryResponse,
        response::{HistorySample, HistoryValue},
    },
    protobuf::metricq,
    types::{Aggregate, MetricId, Timestamp},
};

///
pub fn decode_response(payload: Bytes) -> MetricQResult<HistoryResponse> {
    let message = metricq::history::HistoryResponse::parse_from_bytes(&payload)?;

    if !message.error.is_empty() {
        return Ok(HistoryResponse {
            metric: message.metric.into(),
            samples: vec![],
            error: Some(message.error),
        });
    }

    let mut time_stamp = 0i64;

    let mut samples = Vec::new();

    for (index, delta) in message.time_delta.iter().enumerate() {
        time_stamp += *delta;
        let value = if let Some(value) = message.value.get(index) {
            HistoryValue::Value(*value)
        } else if let Some(aggregate) = message.aggregate.get(index) {
            HistoryValue::Aggregate {
                minimum: aggregate.minimum,
                maximum: aggregate.maximum,
                average: if aggregate.count > 0 {
                    aggregate.sum / aggregate.count as f64
                } else {
                    0.0
                },
                statistics: Aggregate {
                    min: aggregate.minimum,
                    max: aggregate.maximum,
                    sum: aggregate.sum,
                    count: aggregate.count,
                    integral_ns: aggregate.integral,
                    active_time: Duration::from_nanos(aggregate.active_time as u64),
                },
            }
        } else {
            continue;
        };

        samples.push(HistorySample {
            timestamp: Timestamp::from_nanos(time_stamp),
            value,
        });
    }

    Ok(HistoryResponse {
        metric: MetricId::from(message.metric),
        samples,
        error: if message.error.is_empty() {
            None
        } else {
            Some(message.error)
        },
    })
}

///
pub fn encode_request(request: &HistoryRequest) -> MetricQResult<Bytes> {
    let mut message = metricq::history::HistoryRequest::new();

    message.start_time = request.start.as_nanos();
    message.end_time = request.end.as_nanos();
    message.interval_max = request.interval.as_micros() as i64;

    message.type_ = metricq::history::history_request::RequestType::FLEX_TIMELINE.into();

    Ok(Bytes::from(message.write_to_bytes()?))
}
