//! Response payload for subscribing to a metric sink.

use serde::{Deserialize, Serialize};

/// The response payload received after successfully subscribing to a metric sink.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SinkSubscribeResponse {
    /// The name of the AMQP queue where metric data points will be delivered.
    pub data_queue: String,

    /// The address of the data server or broker connection point for streaming data.
    pub data_server_address: String,
}
