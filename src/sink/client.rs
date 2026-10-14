//! Client implementation for subscribing to and consuming real-time metric streams from a sink.

use std::{fmt::Debug, time::Duration};

use tokio_util::sync::CancellationToken;
use tracing::{debug, info, instrument};

use crate::{
    config::ReconnectPolicy,
    error::MetricQResult,
    sink::{
        consumer::{DataConsumer, SinkMessageHandler},
        response::SinkSubscribeResponse,
    },
    transport::{connection::ConnectionManager, consumer::ConsumerLoop, rpc::client::RpcClient},
};

/// Client for interacting with the `MetricQ` sink service to subscribe to real-time metric streams.
#[derive(Debug, Clone)]
pub struct SinkClient {
    rpc: RpcClient,
    base_uri: String,
    management_exchange: String,
    reconnect_policy: ReconnectPolicy,
    connection_timeout: Duration,
}

impl SinkClient {
    pub(crate) const fn new(
        rpc: RpcClient,
        base_uri: String,
        management_exchange: String,
        reconnect_policy: ReconnectPolicy,
        connection_timeout: Duration,
    ) -> Self {
        Self {
            rpc,
            base_uri,
            management_exchange,
            reconnect_policy,
            connection_timeout,
        }
    }

    /// Subscribes to a stream of metrics, connected to the designated data server
    /// and running the consumer loop to process incoming data points.
    ///
    /// # Errors
    ///
    /// Returns an error if the RPC request fails, the response cannot be parsed,
    /// connection to the data server fails, or merge consumption encounters an error.
    #[instrument(skip(self, consumer, shutdown), fields(?metrics))]
    pub async fn subscribe<C, S>(
        &self,
        metrics: &[S],
        consumer: C,
        shutdown: CancellationToken,
    ) -> MetricQResult<()>
    where
        C: DataConsumer + Send,
        S: AsRef<str> + serde::Serialize + Debug + Send + Sync,
    {
        info!("requesting sink subscription from manager");
        let sub_response: SinkSubscribeResponse = self
            .rpc
            .call_json(
                self.management_exchange.clone(),
                "sink.subscribe",
                serde_json::json!({ "metrics": metrics }),
            )
            .await?;

        // Derive the full AMQP URI from the vhost shorthand
        let data_server_uri =
            derive_data_host_uri(&self.base_uri, &sub_response.data_server_address);
        info!(
            server = %data_server_uri,
            queue = %sub_response.data_queue,
            "received sink subscription response, connecting to data server"
        );

        let connection_manager = ConnectionManager::connect(
            &data_server_uri,
            self.reconnect_policy.clone(),
            self.connection_timeout,
        )
        .await?;

        debug!("opening channel for data stream");
        let channel = connection_manager.open_channel().await?;

        debug!("starting consumer for data queue");
        let lapin_consumer = channel
            .consume(sub_response.data_queue, "metricq-sink")
            .await?;

        let handler = SinkMessageHandler::new(consumer);
        let consumer_loop = ConsumerLoop {
            consumer: lapin_consumer,
            handler,
            shutdown,
        };

        info!("starting sink stream consumer loop");
        consumer_loop.run().await
    }
}

/// Translates `MetricQ` data server shorthands (like "vhost:/") into a full AMQP URI using the base client URI.
fn derive_data_host_uri(base_uri: &str, server_address: &str) -> String {
    if server_address.starts_with("amqp://") || server_address.starts_with("amqps://") {
        return server_address.to_string();
    }

    let Some(vhost) = server_address.strip_prefix("vhost:") else {
        return server_address.to_string();
    };

    let Ok(parsed) = url::Url::parse(base_uri) else {
        return server_address.to_string();
    };

    let scheme = parsed.scheme();
    let auth = parsed.authority();
    let encoded_vhost = if vhost == "/" { "%2f" } else { vhost };

    format!("{scheme}://{auth}/{encoded_vhost}")
}
