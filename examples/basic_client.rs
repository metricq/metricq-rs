use std::time::{SystemTime, UNIX_EPOCH};

use metricq_rs::{
    Client, ClientConfigBuilder, HistoryRequestBuilder, HistoryRequestType, MetricId, MetricQError,
    MetricQResult, TimeDelta, Timestamp,
};
use tracing::{Level, debug, error};

#[tokio::main]
async fn main() -> MetricQResult<()> {
    tracing_subscriber::fmt()
        .with_max_level(Level::DEBUG)
        .init();
    let config = ClientConfigBuilder::default()
        .uri("amqp://admin:admin@localhost/%2f".into())
        .build()
        .map_err(|_| MetricQError::BuildError("client config".into()))?;

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
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos() as i64;

    let end = Timestamp::from_nanos(now);
    let start = Timestamp::from_nanos(now - 300_000_000_000);
    let interval = TimeDelta::from_secs(60);

    debug!("querying history for metric '{}'...", metric_id);
    let request = HistoryRequestBuilder::default()
        .start_time(start)
        .end_time(end)
        .interval_max(interval)
        .request_type(HistoryRequestType::Timeline)
        .build()
        .map_err(|_| MetricQError::BuildError("history request".into()))?;
    match client.history().get(metric_id, request).await {
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
