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
use tracing::{debug, info, instrument};

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
pub trait MessageHandler: Send {
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
pub struct ConsumerLoop<H> {
    pub consumer: Consumer,
    pub handler: H,
    pub shutdown: CancellationToken,
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
    #[instrument(skip(self))]
    pub async fn run(mut self) -> MetricQResult<()> {
        info!("starting consumer loop");

        loop {
            select! {
                () = self.shutdown.cancelled() => {
                    info!("shutdown signal received, terminating consumer loop");
                    break;
                }

                next = self.consumer.next() => {
                    let Some(delivery) = next else {
                        info!("consumer stream ended");
                        break;
                    };

                    let msg = IncomingMessage::from(delivery?);
                    debug!(
                        routing_key = %msg.routing_key,
                        correlation_id = ?msg.correlation_id,
                        payload_len = msg.payload.len(),
                        "received incoming message"
                    );

                    match self.handler.handle(&msg).await? {

                        ConsumeResult::Ack => {
                            debug!("acknowledging message");
                            msg.delivery
                                .ack(BasicAckOptions::default())
                                .await?;
                        }

                        ConsumeResult::Reject => {
                            debug!("rejecting message without requeue");
                            msg.delivery
                                .reject(BasicRejectOptions {
                                    requeue: false,
                                })
                                .await?;
                        }

                        ConsumeResult::Requeue => {
                            debug!("rejecting message with requeue");
                            msg.delivery
                                .reject(BasicRejectOptions {
                                    requeue: true,
                                })
                                .await?;
                        }
                    }
                }
            }
        }

        info!("consumer loop stopped successfully");
        Ok(())
    }
}

/// A decoded AMQP delivery.
///
/// This type exposes the message metadata commonly used by `MetricQ`
/// handlers while retaining ownership of the underlying delivery for
/// acknowledgment by the consumer loop.
pub struct IncomingMessage {
    /// AMQP routing key used to deliver the message.
    pub routing_key: RoutingKey,

    /// Correlation identifier used for RPC request/response matching.
    pub correlation_id: Option<CorrelationId>,

    /// Raw message payload.
    pub payload: Bytes,

    delivery: Delivery,
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

        let payload = delivery.data.clone().into();

        Self {
            routing_key,
            correlation_id,
            payload,
            delivery,
        }
    }
}

/// Indicates how a processed message shouuld be acknowledged.
#[allow(unused)]
pub enum ConsumeResult {
    /// Acknowledge the message and remove it from the queue.
    Ack,

    /// Reject the message without requeueing it.
    Reject,

    /// Reject the message and request that it will be requeued.
    Requeue,
}
