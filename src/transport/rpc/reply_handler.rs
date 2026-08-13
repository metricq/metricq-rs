//! Internal RPC response handling.
//!
//! This module contains the message handler responsible for receiving
//! RPC replies from the `MetricQ` broker and forwarding them to the
//! corresponding pending request.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

use crate::{
    error::{MetricQError, MetricQResult},
    transport::{
        consumer::{ConsumeResult, IncomingMessage, MessageHandler},
        correlation::CorrelationRegistry,
    },
};

/// Handles incoming RPC replies and resolves pending requests.
///
/// RPC requests are matched with their responses using the `AMQP`
/// correlation id. When a response arrives, the handler forwards the
/// payload to the [`CorrelationRegistry`] entry waiting for that id.
///
/// Messages without a correlation id cannot be associated with a
/// pending request and are rejected.
///
/// Successfully processed replies are acknoledged.
pub(crate) struct RpcReplyHandler {
    registry: Arc<CorrelationRegistry<Bytes>>,
}

impl RpcReplyHandler {
    /// Creates a new RPC reply handler backend by the given registry.
    #[must_use]
    pub const fn new(registry: Arc<CorrelationRegistry<Bytes>>) -> Self {
        Self { registry }
    }
}

#[async_trait]
impl MessageHandler for RpcReplyHandler {
    async fn handle(&mut self, msg: &IncomingMessage) -> MetricQResult<ConsumeResult> {
        let id = msg
            .correlation_id
            .as_deref()
            .ok_or(MetricQError::MissingCorrelationId)?;

        self.registry.dispatch(id, msg.payload.clone()).await;

        Ok(ConsumeResult::Ack)
    }
}
