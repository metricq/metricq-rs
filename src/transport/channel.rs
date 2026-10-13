//! Abstractions over AMQP channel operations.
//!
//! This module provides a thin wrapper arount `lapin::Channel` used by
//! the transport layer. It exposes only the operations required by
//! `MetricQ` while keeping the underlying AMQP implementation localized.

use bytes::Bytes;
use lapin::{
    BasicProperties, Channel, Consumer, ExchangeKind,
    options::{
        BasicConsumeOptions, BasicPublishOptions, ExchangeDeclareOptions, QueueBindOptions,
        QueueDeclareOptions,
    },
    types::FieldTable,
};
use tracing::{debug, instrument};

use crate::{
    error::MetricQResult,
    transport::types::{ExchangeName, QueueName, RoutingKey},
};

/// A handle to an AMQP channel.
///
/// Channels are the primary communication primitive used by the
/// transport layer. They support publishing messages, declaring AMQP
/// resources, and creating consumers.
#[derive(Debug, Clone)]
pub(crate) struct ChannelHandle {
    inner: Channel,
}

impl ChannelHandle {
    pub const fn new(inner: Channel) -> Self {
        Self { inner }
    }

    /// Returns the underlying `laping::Channel`.
    ///
    /// This is intended for transport code that requires functionality not
    /// yet exposed by `ChannelHandle`.
    #[allow(unused)]
    pub const fn inner(&self) -> &Channel {
        &self.inner
    }

    /// Publishes a message to an exchange.
    #[instrument(skip(self, properties, payload), fields(%exchange, %routing_key))]
    pub async fn publish(
        &self,
        exchange: ExchangeName,
        routing_key: RoutingKey,
        properties: BasicProperties,
        payload: Bytes,
    ) -> MetricQResult<()> {
        debug!("publishing message to exchange");
        self.inner
            .basic_publish(
                exchange.into(),
                routing_key.into(),
                BasicPublishOptions::default(),
                &payload,
                properties,
            )
            .await?
            .await?;

        debug!("message published successfully");
        Ok(())
    }

    /// Declares a durable queue using the default queue options.
    #[allow(unused)]
    #[instrument(skip(self), fields(%name))]
    pub async fn declare_queue(&self, name: QueueName) -> MetricQResult<()> {
        debug!("declaring durable queue");
        self.inner
            .queue_declare(
                name.into(),
                QueueDeclareOptions::default(),
                FieldTable::default(),
            )
            .await?;
        debug!("queue declared successfully");
        Ok(())
    }

    /// Starts consuming messages from a queue.
    #[instrument(skip(self), fields(%queue, %consumer_tag))]
    pub async fn consume(&self, queue: QueueName, consumer_tag: &str) -> MetricQResult<Consumer> {
        debug!("starting basic consumer on queue");
        let consumer = self
            .inner
            .basic_consume(
                queue.into(),
                consumer_tag.into(),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await?;
        debug!("consumer started successfully");
        Ok(consumer)
    }

    /// Binds a queue to an exchange using the given routing key.
    #[allow(unused)]
    #[instrument(skip(self), fields(%queue, %exchange, %routing_key))]
    pub async fn bind_queue(
        &self,
        queue: QueueName,
        exchange: ExchangeName,
        routing_key: RoutingKey,
    ) -> MetricQResult<()> {
        debug!("binding queue to exchange");
        self.inner
            .queue_bind(
                queue.into(),
                exchange.into(),
                routing_key.into(),
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await?;
        debug!("queue bound successfully");
        Ok(())
    }

    /// Declares a topic exchange.
    #[allow(unused)]
    #[instrument(skip(self), fields(%exchange))]
    pub async fn declare_exchange(&self, exchange: ExchangeName) -> MetricQResult<()> {
        debug!("declaring topic exchange");
        self.inner
            .exchange_declare(
                exchange.into(),
                ExchangeKind::Topic,
                ExchangeDeclareOptions::default(),
                FieldTable::default(),
            )
            .await?;
        debug!("exchange declared successfully");
        Ok(())
    }

    /// Declares an exclusive, auto-deleted queue with a server-generated name.
    ///
    /// The queue exists only for the lifetime of the owning connection and
    /// is automatically removed onve the connection closes.
    ///
    /// This is primarily used for temporary RPC reply queues.
    pub async fn declare_temporary_queue(&self) -> MetricQResult<QueueName> {
        debug!("declared temporary exclusive queue");
        let queue = self
            .inner
            .queue_declare(
                "".into(),
                QueueDeclareOptions {
                    passive: false,
                    durable: false,
                    exclusive: true,
                    auto_delete: true,
                    nowait: false,
                },
                FieldTable::default(),
            )
            .await?;
        let name = queue.name().to_string();
        debug!(queue_name = %name, "temporary queue declared successfully");
        Ok(queue.name().to_string())
    }

    /// Gracefully closes the channel.
    #[allow(unused)]
    #[instrument(skip(self))]
    pub async fn close(&self) -> MetricQResult<()> {
        debug!("closing amqp channel");
        self.inner().close(200, "shutdown".into()).await?;
        debug!("amqp channel closed successfully");
        Ok(())
    }
}
