//! Client implementation for querying historical metric data via RPC.

use tracing::{debug, instrument};

use crate::{
    error::MetricQResult,
    history::{HistoryRequest, HistoryResponse, request::HistoryRequestType},
    transport::{rpc::client::RpcClient, types::ExchangeName},
    types::{MetricId, TimeDelta, Timestamp},
};

/// Client for interacting with the `MetricQ` history service to retrieve past metric values and aggregates.
#[derive(Debug, Clone)]
pub struct HistoryClient {
    rpc: RpcClient,
    exchange: ExchangeName,
}

impl HistoryClient {
    /// Creates a new [`HistoryClient`].
    #[must_use]
    pub(crate) const fn new(rpc: RpcClient, exchange: ExchangeName) -> Self {
        Self { rpc, exchange }
    }

    /// Retrieves historical samples for the specified metric within a given time
    /// range and interval.
    ///
    /// # Errors
    ///
    /// Returns an error if encoding the request fails, the RPC call fails,
    /// or the response cannot be decoded.
    #[instrument(skip(self), fields(%metric, ?start, ?end, ?interval))]
    pub async fn get(
        &self,
        metric: MetricId,
        start: Timestamp,
        end: Timestamp,
        interval: TimeDelta,
    ) -> MetricQResult<HistoryResponse> {
        debug!("building and encoding history request");
        let request = HistoryRequest {
            start,
            end,
            interval,
            type_: HistoryRequestType::Timeline,
        };

        let payload = request.encode()?;

        debug!(exchange = %self.exchange, "sending history rpc request");
        let response = self
            .rpc
            .call(self.exchange.clone(), metric.into(), payload)
            .await?;

        debug!("decoding history response payload");
        HistoryResponse::decode(&response)
    }
}
