use async_trait::async_trait;
use metricq_rs::{Client, ClientConfig, DataConsumer, MetricQResult, MetricTimeValue};
use tracing::{Level, debug, error};

/// A custom consumer struct that handles incoming metric data points.
struct PrintConsumer;

#[async_trait]
impl DataConsumer for PrintConsumer {
    async fn on_data(&mut self, mtv: &MetricTimeValue) -> MetricQResult<()> {
        debug!(%mtv.metric, ?mtv.sample.timestamp, %mtv.sample.value, "received data point");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> MetricQResult<()> {
    tracing_subscriber::fmt()
        .with_max_level(Level::DEBUG)
        .init();

    let config = ClientConfig {
        uri: "amqp://admin:admin@localhost/%2f".to_string(),
        ..Default::default()
    };

    let client = Client::connect(&config).await?;

    let token_clone = client.shutdown_token();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        token_clone.cancel();
    });

    let consumer = PrintConsumer;

    match client.subscribe(&["dummy.source"], consumer).await {
        Ok(()) => debug!("Sink subscription stream closed."),
        Err(err) => error!("Sink subscription failed: {err:?}"),
    }
    client.shutdown().await;

    Ok(())
}
