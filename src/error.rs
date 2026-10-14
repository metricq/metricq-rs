//! Error types returned by the `MetricQ` client.

use std::time::SystemTimeError;

use thiserror::Error;
use tokio::sync::oneshot;

use crate::{transport::types::CorrelationId, types::metric::MetricId};

/// Errors that can occur while interacting with `MetricQ`.
///
/// This includes connection failures, RPC lifecycle errors, invalid
/// input, protocol encoding/decoding failures, and client lifecycle errors.
#[derive(Debug, Error)]
pub enum MetricQError {
    /// Failed to establish or use an AMQP connection.
    #[error("connection failed")]
    Connection(#[from] lapin::Error),

    /// Failed to reestablish connection with the broker.
    #[error("failed to reestablish connection")]
    Reconnect,

    /// RPC request failed.
    #[error("rpc request failed")]
    Rpc,

    /// RPC request exceeded  its configured timeout.
    #[error("request timed out")]
    Timeout,

    /// Metric identifier did not satisfy the required format.
    #[error("invalid metric id: {metric}")]
    InvalidMetric {
        /// The metric's id.
        metric: MetricId,
    },

    /// Input provided by the caller was invalid.
    #[error("invalid input: {message}")]
    InvalidInput {
        /// The input that was invalid.
        message: String,
    },

    /// Received a message without an AMQP correlation identifier.
    #[error("found no correlation id for delivery")]
    MissingCorrelationId,

    /// No pending RPC request exists for the given correlation identifier.
    #[error("expected sender with correlation id: {_0}, but found none")]
    MissingCorrelationEntry(CorrelationId),

    /// Weiting for an RPC response failed because the response channel closed.
    #[error("failed to read from channel")]
    RecvError(#[from] oneshot::error::RecvError),

    /// Failed to decode a protobuf message.
    #[error("failed to decode protobuf")]
    Decode,

    /// Failed to encode a protobuf message.
    #[error("failed to encode protobuf")]
    Encode,

    /// Operation was attempted after the client was shut down.
    #[error("client has been shut down")]
    Shutdown,

    /// RPC response could not be delivered because the response channel closed.
    #[error("failed to receive RPC response because the channel was closed")]
    RpcChannelClosed,

    /// TODO
    #[error("failed to convert to/from bytes using protobuf: {0}")]
    ProtobufError(#[from] protobuf::Error),

    /// TODO
    #[error("failed to serialize to json")]
    JsonSerialize(#[from] serde_json::Error),

    /// TODO
    #[error("server side error: {_0}")]
    Server(String),

    /// TODO
    #[error("failed to build {_0}")]
    BuildError(String),

    /// TODO
    #[error("{_0}")]
    SystemTimeError(#[from] SystemTimeError),
}

/// Result type used throughout the `MetricQ` client.
pub type MetricQResult<T> = Result<T, MetricQError>;
