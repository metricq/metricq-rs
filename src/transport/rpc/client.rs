//! Client-side implementation of the AMQP request/response pattern.

use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use lapin::BasicProperties;
use tokio::sync::{RwLock, oneshot};
use tracing::{debug, error, instrument, warn};
use uuid::Uuid;

use crate::{
    error::{MetricQError, MetricQResult},
    transport::{
        channel::ChannelHandle,
        connection::ConnectionManager,
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
    channel: Arc<RwLock<ChannelHandle>>,
    connection_manager: ConnectionManager,
    registry: Arc<CorrelationRegistry<Bytes>>,
    reply_queue: QueueName,
    timeout: Duration,
    token: String,
}

impl RpcClient {
    pub fn new(
        channel: ChannelHandle,
        connection_manager: ConnectionManager,
        registry: Arc<CorrelationRegistry<Bytes>>,
        reply_queue: QueueName,
        timeout: Duration,
        token: String,
    ) -> Self {
        let channel = Arc::new(RwLock::new(channel));
        Self {
            channel,
            connection_manager,
            registry,
            reply_queue,
            timeout,
            token,
        }
    }

    /// Sends an RPC request and waits for the corresponding response.
    ///
    /// The request is registered in the [`CorrelationRegistry`] before it
    /// is published to ensure that replies arriving immediately after
    /// publication can always be matched.
    ///
    /// The registry entry is removed automatically when:
    ///
    /// - the response is received,
    /// - publishing fails,
    /// - or the request times out.
    #[instrument(skip(self, payload), fields(%exchange, %routing_key))]
    pub async fn call(
        &self,
        exchange: ExchangeName,
        routing_key: RoutingKey,
        payload: Bytes,
    ) -> MetricQResult<Bytes> {
        let correlation_id = Uuid::new_v4().to_string();
        tracing::Span::current().record("correlation_id", &correlation_id);

        debug!("registering request in correlation registry");
        let (tx, rx) = oneshot::channel();
        self.registry.insert(correlation_id.clone(), tx).await;

        let properties = BasicProperties::default()
            .with_reply_to(self.reply_queue.clone().into())
            .with_correlation_id(correlation_id.clone().into())
            .with_type(routing_key.clone().into())
            .with_app_id(self.token.clone().into());

        let publish_result = {
            let channel_guard = self.channel.read().await;
            channel_guard
                .publish(
                    exchange.clone(),
                    routing_key.clone(),
                    properties.clone(),
                    payload.clone(),
                )
                .await
        };

        let final_publish_result = if let Err(err) = publish_result {
            warn!(
                ?err,
                "failed to publish rpc request, attempting reconnection"
            );
            if let Err(reconnect_err) = self.connection_manager.reconnect().await {
                error!(?reconnect_err, "reconnection failed");
                self.registry.remove(&correlation_id).await?;
                return Err(reconnect_err);
            }

            let new_channel = match self.connection_manager.open_channel().await {
                Ok(ch) => {
                    debug!("successfully opened new channel after reconnection");
                    ch
                }
                Err(err) => {
                    error!(?err, "failed to open new channel after reconnection");
                    self.registry.remove(&correlation_id).await?;
                    return Err(err);
                }
            };

            let mut channel_write = self.channel.write().await;
            *channel_write = new_channel;

            debug!("re-publishing rpc request on new channel");
            channel_write
                .publish(
                    exchange.clone(),
                    routing_key.clone(),
                    properties,
                    payload.clone(),
                )
                .await
        } else {
            Ok(())
        };

        if let Err(err) = final_publish_result {
            error!(?err, "failed to publish rpc request");
            self.registry.remove(&correlation_id).await?;
            return Err(err);
        }

        debug!(timeout = ?self.timeout, "waiting for rpc response");
        let response = if let Ok(value) = tokio::time::timeout(self.timeout, rx).await {
            value.map_err(|_| {
                error!("rpc channel closed while waiting for response");
                MetricQError::RpcChannelClosed
            })?
        } else {
            warn!("rpc request timed out");
            self.registry.remove(&correlation_id).await?;
            return Err(MetricQError::Timeout);
        };

        debug!("successfully received rpc response");
        Ok(response)
    }
}
