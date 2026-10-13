//! Configuration structures for the `MetricQ` client, including connection settings,
//! timeouts, and policies.

use serde::{Deserialize, Serialize};

/// Configuration settings for initializing and managing a `MetricQ` client connection.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, rename_all = "kebab-case")]
pub struct ClientConfig {
    /// The AMQP connection URI (e.g. `"amqp://localhost/%2f"`).
    pub uri: String,

    /// The name of the manager queue used for administrative and RPC commands.
    pub manager_queue: String,

    /// The name of the exchange used for history queries.
    pub history_exchange: String,

    /// Optional client name identification.
    pub name: Option<String>,

    /// Policy govering reconnection behavior upon connection loss.
    pub reconnect_policy: ReconnectPolicy,

    /// Timeout configurations for RPC calls and connection attempts.
    pub timeouts: Timeouts,

    /// Policy defining buffering limits for metric streams.
    pub buffering_policy: BufferingPolicy,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            uri: "amqp://localhost/%2f".to_string(),
            manager_queue: "metricq.manager".to_string(),
            history_exchange: "metricq.history".to_string(),
            name: None,
            reconnect_policy: ReconnectPolicy::default(),
            timeouts: Timeouts::default(),
            buffering_policy: BufferingPolicy::default(),
        }
    }
}

/// Defines the retry policy for reconnecting to the AMQP broker after
/// a connection failure.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, rename_all = "kebab-case")]
pub struct ReconnectPolicy {
    /// The maximum number of reconnection attempts (`None` for infinite retries).
    pub max_retries: Option<u32>,

    /// The initial delay in milliseconds before the first reconnection attempt.
    pub initial_delay_ms: u64,

    /// The maximum delay in milliseconds between reconnection attempts (used with backoff).
    pub max_delay_ms: u64,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            max_retries: None,
            initial_delay_ms: 1000,
            max_delay_ms: 30000,
        }
    }
}

/// Timeout configuration settings for network and RPC operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, rename_all = "kebab-case")]
pub struct Timeouts {
    /// Timeout in milliseconds for individual RPC requests.
    pub rpc_timeout_ms: u64,

    /// Timeout in milliseconds for establishing a new connection.
    pub connection_timeout_ms: u64,
}

impl Default for Timeouts {
    fn default() -> Self {
        Self {
            rpc_timeout_ms: 5000,
            connection_timeout_ms: 10000,
        }
    }
}

/// Policy defining buffer capacities and limits.
///
/// # Note
///
/// This needs to be used by implementing a designated writer.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, rename_all = "kebab-case")]
pub struct BufferingPolicy {
    /// The maximum number of elements or bytes allowed in the buffer.
    pub max_buffer_size: usize,
}

impl Default for BufferingPolicy {
    fn default() -> Self {
        Self {
            max_buffer_size: 10000,
        }
    }
}
