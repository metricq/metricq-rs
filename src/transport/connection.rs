//! Connection management for the AMQP transport layer.
//!
//! This module owns the underlying AMQP connection and provides a
//! centralized way to create channels used throughout the transport
//! layer.

use std::{sync::Arc, time::Duration};

use lapin::{Connection, ConnectionProperties};
use tokio::{
    sync::RwLock,
    time::{sleep, timeout},
};
use tracing::{error, info, instrument, warn};

use crate::{
    config::ReconnectPolicy,
    error::{MetricQError, MetricQResult},
    transport::channel::ChannelHandle,
};

/// Owns the transport's AMQP connection.
///
/// A single connection is shared by multiple channels. New channels
/// should be obtained through this type instead of creating additional
/// connections.
#[derive(Debug, Clone)]
pub struct ConnectionManager {
    pub uri: String,
    connection: Arc<RwLock<Connection>>,
    reconnect_policy: ReconnectPolicy,
    connection_timeout: Duration,
}

impl ConnectionManager {
    /// Establishes a new AMQP connection to the broker.
    #[instrument(skip(reconnect_policy), fields(%uri))]
    pub async fn connect(
        uri: &str,
        reconnect_policy: ReconnectPolicy,
        timeout_duration: Duration,
    ) -> MetricQResult<Self> {
        info!("establishing new amqp connection");
        let connection = timeout(
            timeout_duration,
            Connection::connect(uri, ConnectionProperties::default()),
        )
        .await
        .map_err(|_| {
            error!("connection attemp timed out");
            MetricQError::Timeout
        })?
        .map_err(|err| {
            error!(?err, "failed to connect to amqp broker");
            MetricQError::from(err)
        })?;

        info!("amqp connection established successfully");
        Ok(Self {
            uri: uri.to_string(),
            connection: Arc::new(RwLock::new(connection)),
            reconnect_policy,
            connection_timeout: timeout_duration,
        })
    }

    /// Opens a new channel on the managed connection.
    ///
    /// Channels are lightweight and intended to be created as needed for
    /// publishers, consumers, and RPC communication.
    #[instrument(skip(self))]
    pub async fn open_channel(&self) -> MetricQResult<ChannelHandle> {
        let conn = self.connection.read().await;
        let channel = conn.create_channel().await?;
        drop(conn);
        Ok(ChannelHandle::new(channel))
    }

    /// Gracefully closes the AMQP connection.
    ///
    /// Closing the connection also closes all channels created from it.
    #[instrument(skip(self))]
    pub async fn close(self) -> MetricQResult<()> {
        let conn = self.connection.write().await;
        conn.close(200, "shutdown".into()).await.map_err(|err| {
            error!(?err, "failed to close connection gracefully");
            MetricQError::from(err)
        })?;
        drop(conn);
        info!("amqp connection closed successfully");
        Ok(())
    }

    /// Attempts to re-establish the AMQP connection using the configured retry policy
    /// and exponential backoff.
    ///
    /// # Errors
    ///
    /// Returns [`MetricQError::Reconnect`] if the maximum number of reconnection attempts
    /// is exceeded.
    #[instrument(skip(self))]
    pub async fn reconnect(&self) -> MetricQResult<()> {
        let mut attempt = 0u32;
        let mut delay_ms = self.reconnect_policy.initial_delay_ms;

        info!("starting reconnection process");

        loop {
            if let Some(max) = self.reconnect_policy.max_retries
                && attempt >= max
            {
                error!(attempt, max, "exceeded maximum reconnection attempts");
                return Err(MetricQError::Reconnect);
            }

            attempt += 1;
            warn!(attempt, delay_ms, "attempting to reconnect to amqp broker");

            let connect_future = Connection::connect(&self.uri, ConnectionProperties::default());
            match timeout(self.connection_timeout, connect_future).await {
                Ok(Ok(new_conn)) => {
                    let mut conn_lock = self.connection.write().await;
                    *conn_lock = new_conn;
                    info!(attempt, "successfully reconnected to amqp broker");
                    drop(conn_lock);
                    return Ok(());
                }
                Ok(Err(err)) => {
                    warn!(?err, attempt, delay_ms, "reconnection failed, retrying");
                }
                Err(_) => {
                    warn!(
                        attempt,
                        delay_ms, "reconnection attempt timed out, retrying"
                    );
                }
            }
            sleep(Duration::from_millis(delay_ms)).await;
            delay_ms = std::cmp::min(delay_ms * 2, self.reconnect_policy.max_delay_ms);
        }
    }
}
