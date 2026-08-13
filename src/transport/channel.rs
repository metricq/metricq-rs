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
    pub fn new(inner: Channel) -> Self {
        Self { inner }
    }

    /// Returns the underlying `laping::Channel`.
    ///
    /// This is intended for transport code that requires functionality not
    /// yet exposed by `ChannelHandle`.
    pub fn inner(&self) -> &Channel {
        &self.inner
    }

    /// Publishes a message to an exchange.
    pub async fn publish(
        &self,
        exchange: ExchangeName,
        routing_key: RoutingKey,
        properties: BasicProperties,
        payload: Bytes,
    ) -> MetricQResult<()> {
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

        Ok(())
    }

    /// Declares a durable queue using the default queue options.
    pub async fn declare_queue(&self, name: QueueName) -> MetricQResult<()> {
        self.inner
            .queue_declare(
                name.into(),
                QueueDeclareOptions::default(),
                FieldTable::default(),
            )
            .await?;
        Ok(())
    }

    /// Starts consuming messages from a queue.
    pub async fn consume(&self, queue: QueueName, consumer_tag: &str) -> MetricQResult<Consumer> {
        Ok(self
            .inner
            .basic_consume(
                queue.into(),
                consumer_tag.into(),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await?)
    }

    /// Binds a queue to an exchange using the given routing key.
    pub async fn bind_queue(
        &self,
        queue: QueueName,
        exchange: ExchangeName,
        routing_key: RoutingKey,
    ) -> MetricQResult<()> {
        self.inner
            .queue_bind(
                queue.into(),
                exchange.into(),
                routing_key.into(),
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await?;
        Ok(())
    }

    /// Declares a topic exchange.
    pub async fn declare_exchange(&self, exchange: ExchangeName) -> MetricQResult<()> {
        self.inner
            .exchange_declare(
                exchange.into(),
                ExchangeKind::Topic,
                ExchangeDeclareOptions::default(),
                FieldTable::default(),
            )
            .await?;
        Ok(())
    }

    /// Declares an exclusive, auto-deleted queue with a server-generated name.
    ///
    /// The queue exists only for the lifetime of the owning connection and
    /// is automatically removed onve the connection closes.
    ///
    /// This is primarily used for temporary RPC reply queues.
    pub async fn declare_temporary_queue(&self) -> MetricQResult<QueueName> {
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
        Ok(queue.name().to_string())
    }

    /// Gracefully closes the channel.
    pub async fn close(&self) -> MetricQResult<()> {
        self.inner().close(200, "shutdown".into()).await?;
        Ok(())
    }
}
