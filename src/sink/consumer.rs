//! Consumer implementation for processing real-time metric data streams.

use async_trait::async_trait;
use protobuf::Message;
use tracing::{debug, instrument};

use crate::{
    error::MetricQResult,
    protobuf::metricq::datachunk::DataChunk,
    transport::consumer::{ConsumeResult, IncomingMessage, MessageHandler},
    types::Timestamp,
};

/// A trait for receiving real-time metric data points from a sink subscription.
#[async_trait]
pub trait DataConsumer: Send {
    /// Called when a new metric data point is received.
    async fn on_data(
        &mut self,
        metric: &str,
        timestamp: Timestamp,
        value: f64,
    ) -> MetricQResult<()>;
}

/// Adapts a [`DataConsumer`] into transport-level [`MessageHandler`] to parse
/// and dispatch incoming data chunks.
pub(crate) struct SinkMessageHandler<C> {
    consumer: C,
}

impl<C> SinkMessageHandler<C> {
    /// Creates a new [`SinkMessageHandler`] wrapping the given consumer.
    pub const fn new(consumer: C) -> Self {
        Self { consumer }
    }
}

#[async_trait]
impl<C> MessageHandler for SinkMessageHandler<C>
where
    C: DataConsumer + Send,
{
    /// Parses an incoming binary message payload into a [`DataChunk`], reconstructs
    /// absolute timestamps from delta values, and dispatches each data point to the consumer.
    #[instrument(skip(self, msg), fields(routing_key = %msg.routing_key))]
    async fn handle(&mut self, msg: &IncomingMessage) -> MetricQResult<ConsumeResult> {
        debug!("pasing data chunk from message payload");
        let data = DataChunk::parse_from_bytes(&msg.payload)?;

        let metric = &msg.routing_key;
        let mut timestamp = 0i64;

        assert_eq!(data.time_delta.len(), data.value.len()); // TODO

        debug!(
            points_count = data.value.len(),
            "dispatching data points to consumer"
        );

        for (delta, &value) in data.time_delta.iter().zip(data.value.iter()) {
            timestamp += delta;
            self.consumer
                .on_data(metric, Timestamp::from_nanos(timestamp), value)
                .await?;
        }

        debug!("successfully processed data chunk");
        Ok(ConsumeResult::Ack)
    }
}
