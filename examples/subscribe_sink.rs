use async_trait::async_trait;
use metricq_rs::{
    client::Client, config::ClientConfig, error::MetricQResult, sink::consumer::DataConsumer,
    types::Timestamp,
};
use tokio_util::sync::CancellationToken;
use tracing::{Level, debug, error};

/// A custom consumer struct that handles incoming metric data points.
struct PrintConsumer;

#[async_trait]
impl DataConsumer for PrintConsumer {
    async fn on_data(
        &mut self,
        metric: &str,
        timestamp: Timestamp,
        value: f64,
    ) -> MetricQResult<()> {
        debug!(%metric, ?timestamp, %value, "received data point");
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

    let shutdown_token = CancellationToken::new();

    let token_clone = shutdown_token.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        token_clone.cancel();
    });

    let consumer = PrintConsumer;

    match client
        .sink()
        .subscribe("dummy.source", consumer, shutdown_token)
        .await
    {
        Ok(()) => debug!("Sink subscription stream closed."),
        Err(err) => error!("Sink subscription failed: {err:?}"),
    }
    client.shutdown().await;

    Ok(())
}
