//! Consumer infrastructure for processing incoming AMQP requests.
//!
//! This module provides the generic consumer loop used throughout the
//! transport layer. It continuously receives deliveries from `RabbitMQ`,
//! converts them into [`IncomingMessage`] values, delegates processing to
//! a [`MessageHandler`], and performs the corresponding acknowledgement.

use async_trait::async_trait;
use bytes::Bytes;
use lapin::{
    Consumer,
    message::Delivery,
    options::{BasicAckOptions, BasicRejectOptions},
};
use tokio::select;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

use crate::{
    error::MetricQResult,
    transport::types::{CorrelationId, RoutingKey},
};

/// Handles incoming AMQP messages.
///
/// A handler receives decoded message metadata together with the payload
/// and decides how the message should be acknowledged.
///
/// Returning [`ConsumeResult::Ack`] acknowledges successful processing.
/// Returning [`ConsumeResult::Reject`] permanently rejects the message.
/// Returning [`ConsumeResult::Requeue`] requests that the broker delivers
/// the message again.
///
/// Returning an error aborts the consumer loop.
#[async_trait]
pub(crate) trait MessageHandler: Send {
    async fn handle(&mut self, msg: &IncomingMessage) -> MetricQResult<ConsumeResult>;
}

/// Drives an AMQP consumer until shutdown or stram termination.
///
/// The consumer loop receives deliveries from `RabbitMQ`, forwards each
/// message to the configured [`MessageHandler`], and performs the
/// acknowledgement requested by the handler.
///
/// The loop exits when:
///
/// - the shutdown token is cancelled,
/// - the consumer stream ends,
/// - or message processing returns an error.
pub(crate) struct ConsumerLoop<H> {
    consumer: Consumer,
    handler: H,
    shutdown: CancellationToken,
}

impl<H> ConsumerLoop<H>
where
    H: MessageHandler,
{
    /// Starts processing incoming messages.
    ///
    /// This method blocks until shutdown is requested, the consumer stream
    /// terminates, or an unrecoverable error occurs.
    ///
    /// Every successfully received delivery is acknowledged according to the
    /// [`ConsumeResult`] returned by the handler.
    pub async fn run(mut self) -> MetricQResult<()> {
        loop {
            select! {
                _ = self.shutdown.cancelled() => {
                    break;
                }

                next = self.consumer.next() => {
                    let Some(delivery) = next else {
                        break;
                    };

                    let msg = IncomingMessage::from(delivery?);

                    match self.handler.handle(&msg).await? {

                        ConsumeResult::Ack => {
                            msg._delivery
                                .ack(BasicAckOptions::default())
                                .await?;
                        }

                        ConsumeResult::Reject => {
                            msg._delivery
                                .reject(BasicRejectOptions {
                                    requeue: false,
                                })
                                .await?;
                        }

                        ConsumeResult::Requeue => {
                            msg._delivery
                                .reject(BasicRejectOptions {
                                    requeue: true,
                                })
                                .await?;
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

/// A decoded AMQP delivery.
///
/// This type exposes the message metadata commonly used by `MetricQ`
/// handlers while retaining ownership of the underlying delivery for
/// acknowledgment by the consumer loop.
pub(crate) struct IncomingMessage {
    /// AMQP routing key used to deliver the message.
    pub routing_key: RoutingKey,

    /// Correlation identifier used for RPC request/response matching.
    pub correlation_id: Option<CorrelationId>,

    /// Queue to which a reply should be sent.
    pub reply_to: Option<String>,

    /// Raw message payload.
    pub payload: Bytes,

    _delivery: Delivery,
}

impl From<Delivery> for IncomingMessage {
    fn from(value: Delivery) -> Self {
        Self::new(value)
    }
}

impl IncomingMessage {
    pub(crate) fn new(delivery: Delivery) -> Self {
        let routing_key = delivery.routing_key.to_string();

        let correlation_id = delivery
            .properties
            .correlation_id()
            .as_ref()
            .map(ToString::to_string);

        let reply_to = delivery
            .properties
            .reply_to()
            .as_ref()
            .map(ToString::to_string);

        let payload = delivery.data.clone().into();

        Self {
            routing_key,
            correlation_id,
            reply_to,
            payload,
            _delivery: delivery,
        }
    }
}

/// Indicates how a processed message shouuld be acknowledged.
pub(crate) enum ConsumeResult {
    /// Acknowledge the message and remove it from the queue.
    Ack,

    /// Reject the message without requeueing it.
    Reject,

    /// Reject the message and request that it will be requeued.
    Requeue,
}
