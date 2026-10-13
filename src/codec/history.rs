//! Encoding and decoding utilities for history requests and responses using protocol buffers.

use std::time::Duration;

use bytes::Bytes;
use protobuf::Message;
use tracing::{debug, instrument, warn};

use crate::{
    error::MetricQResult,
    history::{
        HistoryRequest, HistoryResponse,
        response::{HistorySample, HistoryValue},
    },
    protobuf::metricq,
    types::{Aggregate, MetricId, Timestamp},
};

/// Decodes a raw protocol buffer payload into a [`HistoryResponse`].
///
/// # Errors
///
/// Returns an error if the payload fails to parse as a valid protocol buffer message.
#[instrument(skip(payload))]
pub fn decode_response(payload: &Bytes) -> MetricQResult<HistoryResponse> {
    debug!(
        payload_len = payload.len(),
        "decoding history response payload"
    );
    let message = metricq::history::HistoryResponse::parse_from_bytes(payload)?;

    if !message.error.is_empty() {
        warn!(error = %message.error, "history response contains server error");
        return Ok(HistoryResponse {
            metric: message.metric.into(),
            samples: vec![],
            error: Some(message.error),
        });
    }

    debug!(metric = %message.metric, "parsing history samples");
    let mut time_stamp = 0i64;
    let mut samples = Vec::new();

    for (index, delta) in message.time_delta.iter().enumerate() {
        time_stamp += *delta;
        let value = if let Some(value) = message.value.get(index) {
            HistoryValue::Value(*value)
        } else if let Some(aggregate) = message.aggregate.get(index) {
            #[allow(clippy::cast_precision_loss)]
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
                    #[allow(clippy::cast_sign_loss)]
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

    debug!(
        samples_count = samples.len(),
        "successfully decoded history response"
    );

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

/// Encodes a [`HistoryRequests`] into a protocol buffer byte array.
///
/// # Errors
///
/// Returns an error if the request message cannot be serialized into bytes.
#[allow(clippy::cast_possible_truncation)]
#[instrument(skip(request), fields(start = ?request.start, end = ?request.end, interval = ?request.interval))]
pub fn encode_request(request: &HistoryRequest) -> MetricQResult<Bytes> {
    debug!("encoding history request to protobuf bytes");
    let mut message = metricq::history::HistoryRequest::new();

    message.start_time = request.start.as_nanos();
    message.end_time = request.end.as_nanos();
    message.interval_max = request.interval.as_micros() as i64;

    message.type_ = metricq::history::history_request::RequestType::FLEX_TIMELINE.into();

    let bytes = message.write_to_bytes()?;
    debug!(
        bytes_len = bytes.len(),
        "history request encoded successfully"
    );

    Ok(Bytes::from(message.write_to_bytes()?))
}
