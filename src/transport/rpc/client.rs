//! Client-side implementation of the AMQP request/response pattern.

use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use lapin::BasicProperties;
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::{
    error::{MetricQError, MetricQResult},
    transport::{
        channel::ChannelHandle,
        correlation::CorrelationRegistry,
        types::{ExchangeName, QueueName, RoutingKey},
    },
};

/// Sends RPC requests and awaits their responses.
///
/// Each request is assigned a unique correlation identifier and
/// published with a `reply_to` queue. Responses are received by the
/// transport's reply consumer and forwarded through the
/// [`CorrelationRegistry`] to the waiting caller.
///
/// Requests automatically time out if no response is received within
/// the configured timeout.
#[derive(Debug, Clone)]
pub(crate) struct RpcClient {
    channel: ChannelHandle,
    registry: Arc<CorrelationRegistry<Bytes>>,
    reply_queue: QueueName,
    timeout: Duration,
}

impl RpcClient {
    pub const fn new(
        channel: ChannelHandle,
        registry: Arc<CorrelationRegistry<Bytes>>,
        reply_queue: QueueName,
        timeout: Duration,
    ) -> Self {
        Self {
            channel,
            registry,
            reply_queue,
            timeout,
        }
    }

    /// Sends an RPC request and waits for the corresponding response.
    ///
    /// The request is registered in teh [`CorrelationRegistry`] before it
    /// is published to endsure that replies arriving immediately after
    /// publication can always be matched.
    ///
    /// The registry entry is removed automatically when:
    ///
    /// - the response is received,
    /// - publishing fails,
    /// - or the request times out.
    pub async fn call(
        &self,
        exchange: ExchangeName,
        routing_key: &RoutingKey,
        payload: Bytes,
    ) -> MetricQResult<Bytes> {
        let correlation_id = Uuid::new_v4().to_string();

        let (tx, rx) = oneshot::channel();

        self.registry.insert(correlation_id.clone(), tx).await;

        if let Err(err) = self
            .channel
            .publish(
                exchange,
                routing_key.into(),
                BasicProperties::default()
                    .with_reply_to(self.reply_queue.clone().into())
                    .with_correlation_id(correlation_id.clone().into()),
                payload,
            )
            .await
        {
            self.registry.remove(&correlation_id).await?;
            return Err(err);
        }

        let response = if let Ok(value) = tokio::time::timeout(self.timeout, rx).await {
            value.map_err(|_| MetricQError::RpcChannelClosed)?
        } else {
            self.registry.remove(&correlation_id).await?;
            return Err(MetricQError::Timeout);
        };

        Ok(response)
    }
}
