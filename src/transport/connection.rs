//! Connection management for the AMQP transport layer.
//!
//! This module owns the underlying AMQP connection and provides a
//! centralized way to create channels used throughout the transport
//! layer.

use lapin::{Connection, ConnectionProperties};

use crate::{error::MetricQResult, transport::channel::ChannelHandle};

/// Owns the transport's AMQP connection.
///
/// A single connection is shared by multiple channels. New channels
/// should be obtained through this type instead of creating additional
/// connections.
pub(crate) struct ConnectionManager {
    connection: Connection,
}

impl ConnectionManager {
    /// Establishes a new AMQP connection to the broker.
    pub async fn connect(uri: &str) -> MetricQResult<Self> {
        let connection = Connection::connect(uri, ConnectionProperties::default()).await?;

        Ok(Self { connection })
    }

    /// Opens a new channel on the managed connection.
    ///
    /// Channels are lightweight and intended to be created as needed for
    /// publishers, consumers, and RPC communication.
    pub async fn open_channel(&self) -> MetricQResult<ChannelHandle> {
        let channel = self.connection.create_channel().await?;
        Ok(ChannelHandle::new(channel))
    }

    /// Gracefully closes the AMQP connection.
    ///
    /// Closing the connection also closes all channels created from it.
    pub async fn close(self) -> MetricQResult<()> {
        self.connection.close(200, "shutdown".into()).await?;
        Ok(())
    }

    pub async fn reconnect(&self) {
        todo!()
    }
}
