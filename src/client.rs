//! High-level `MetricQ` client implementation.
//!
//! Provides the primary [`Client`] entry point for establishing connections,
//! managing background tasks, and accessing domain-specific clients such as
//! history querying ([`HistoryClient`]) and real-time metric subscriptions ([`SinkClient`]).

use std::{sync::Arc, time::Duration};

use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument};

use crate::{
    DataConsumer,
    config::{ClientConfig, ReconnectPolicy},
    error::MetricQResult,
    history::HistoryClient,
    sink::SinkClient,
    transport::{
        connection::ConnectionManager,
        consumer::ConsumerLoop,
        correlation::CorrelationRegistry,
        rpc::{client::RpcClient, reply_handler::RpcReplyHandler},
        task_manager::TaskManager,
        types::ExchangeName,
    },
};

/// The primary entry point for interacting with a `MetricQ` broker.
///
/// Manages the underlying AMQP connection, RPC correlation registry, background
/// consumer loops, and graceful lifecycle termination via a [`TaskManager`].
#[derive(Debug)]
pub struct Client {
    connection_manager: ConnectionManager,
    rpc: RpcClient,
    history_exchange: ExchangeName,
    connection_timeout: Duration,
    reconnect_policy: ReconnectPolicy,
    task_manager: TaskManager,
}

impl Client {
    /// Connects the `MetricQ` broker using the provided configuration.
    ///
    /// This establishes the main AMQP connection, initializes the RPC communication
    /// channel, spawns the background reply consumer loop, and sets up lifecycle handlers.
    ///
    /// # Errors
    ///
    /// Returns an error if connection establishment, channel creation, or queue declaration fails.
    #[instrument(skip(config), fields(uri = %config.uri, history_exchange = %config.history_exchange))]
    pub async fn connect(config: &ClientConfig) -> MetricQResult<Self> {
        let mut task_manager = TaskManager::new();
        let shutdown_token = task_manager.token();

        let connection_timeout = Duration::from_millis(config.timeouts.connection_timeout_ms);

        info!("connecting to metricq broker");
        let connection_manager = ConnectionManager::connect(
            &config.uri,
            config.reconnect_policy.clone(),
            connection_timeout,
        )
        .await?;

        debug!("opening rpc channel");
        let rpc_channel = connection_manager.open_channel().await?;

        debug!("declaring temporary reply queue");
        let reply_queue = rpc_channel.declare_temporary_queue().await?;

        let registry = Arc::new(CorrelationRegistry::new());

        debug!("subscribing to rpc reply queue");
        let lapin_consumer = rpc_channel
            .consume(reply_queue.clone(), "metricq-rpc-replies")
            .await?;

        let reply_handler = RpcReplyHandler::new(registry.clone());
        let consumer_loop = ConsumerLoop {
            consumer: lapin_consumer,
            handler: reply_handler,
            shutdown: shutdown_token,
        };

        task_manager.spawn(async move {
            if let Err(err) = consumer_loop.run().await {
                error!(?err, "rpc reply consumer loop exited with error");
            }
        });

        let client_token = "rust-sink-client".to_string();
        let timeout = Duration::from_millis(config.timeouts.rpc_timeout_ms);
        let rpc = RpcClient::new(
            rpc_channel,
            connection_manager.clone(),
            registry,
            reply_queue,
            timeout,
            client_token,
        );

        info!("successfully established connection and initialized client");

        Ok(Self {
            connection_manager,
            rpc,
            history_exchange: config.history_exchange.clone(),
            connection_timeout,
            reconnect_policy: config.reconnect_policy.clone(),
            task_manager,
        })
    }

    /// Creates a new [`HistoryClient`] for querying historical metric data.
    #[must_use]
    pub fn history(&self) -> HistoryClient {
        HistoryClient::new(self.rpc.clone(), self.history_exchange.clone())
    }

    /// Returns the cancellation token of the client.
    #[must_use]
    pub fn shutdown_token(&self) -> CancellationToken {
        self.task_manager.token()
    }

    /// Creates a new [`SinkClient`] for real-time metric ingestion and subsciptions.
    #[must_use]
    pub fn sink(&self) -> SinkClient {
        SinkClient::new(
            self.rpc.clone(),
            self.connection_manager.uri.clone(),
            String::from("metricq.management"),
            self.reconnect_policy.clone(),
            self.connection_timeout,
        )
    }

    /// Triggers a graceful shutdown of all background tasks and consumer loops.
    #[instrument(skip(self))]
    pub async fn shutdown(self) {
        info!("intiating shutdown of background tasks");
        self.task_manager.shutdown().await;
        info!("background tasks shut down successfully");
    }

    /// Gracefully shuts down background tasks and closes the underlying AMQP connection.
    ///
    /// # Errors
    ///
    /// Returns and error if the underlying AMQP connection fails.
    #[instrument(skip(self))]
    pub async fn close(self) -> MetricQResult<()> {
        info!("intiating client close sequence");

        info!("shutting down background tasks");
        self.task_manager.shutdown().await;

        info!("closing underlying AMQP connection");
        self.connection_manager.close().await?;

        info!("client closed successfully");

        Ok(())
    }

    /// Convenience wrapper around `self.sink().subscribe(...)`
    ///
    /// # Errors
    ///
    /// Returns an error if the RPC request fails, the response cannot be parsed,
    /// connection to the data server fails, or merge consumption encounters an error.
    pub async fn subscribe<C, S>(&self, metrics: &[S], consumer: C) -> MetricQResult<()>
    where
        C: DataConsumer + Send,
        S: AsRef<str> + serde::Serialize + std::fmt::Debug + Send + Sync,
    {
        self.sink()
            .subscribe(metrics, consumer, self.shutdown_token())
            .await
    }
}
