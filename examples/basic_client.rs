use std::time::{SystemTime, UNIX_EPOCH};

use metricq_rs::{
    client::Client,
    config::ClientConfig,
    error::MetricQResult,
    types::{MetricId, TimeDelta, Timestamp},
};
use tracing::{Level, debug, error};

#[tokio::main]
async fn main() -> MetricQResult<()> {
    tracing_subscriber::fmt()
        .with_max_level(Level::DEBUG)
        .init();
    let config = ClientConfig {
        uri: "amqp://admin:admin@localhost/%2f".to_string(),
        ..Default::default()
    };

    let client = match Client::connect(&config).await {
        Ok(c) => {
            debug!("successfully connected to MetricQ broker");
            c
        }
        Err(e) => {
            error!(?e, "failed to connect to AMQP broker. is RabbitMQ running?");
            return Err(e);
        }
    };

    let metric_id = MetricId::from("dummy.source");
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;

    let end = Timestamp::from_nanos(now);
    let start = Timestamp::from_nanos(now - 300_000_000_000);
    let interval = TimeDelta::from_secs(60);

    debug!("querying history for metric '{}'...", metric_id);
    match client.history().get(metric_id, start, end, interval).await {
        Ok(response) => {
            debug!(
                samples_count = response.samples.len(),
                "received history response",
            );
            for sample in response.samples {
                debug!(timestamp = ?sample.timestamp, value = ?sample.value);
            }
        }
        Err(err) => {
            error!(?err, "history query failed")
        }
    }

    client.shutdown().await;

    Ok(())
}
