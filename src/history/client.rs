//! Client implementation for querying historical metric data via RPC.

use tracing::{debug, instrument};

use crate::{
    error::MetricQResult,
    history::{HistoryRequest, HistoryResponse},
    transport::{rpc::client::RpcClient, types::ExchangeName},
    types::MetricId,
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
    #[
        instrument(
            skip(self),
            fields(
                %metric,
                start_time = ?request.start_time,
                end_time = ?request.end_time,
                interval_max = ?request.interval_max,
                request_time = ?request.request_type
            )
        )
    ]
    pub async fn get(
        &self,
        metric: MetricId,
        // start_time: Timestamp,
        // end_time: Timestamp,
        // interval_max: TimeDelta,
        // request_type: HistoryRequestType,
        request: HistoryRequest,
    ) -> MetricQResult<HistoryResponse> {
        debug!("encoding history request");
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
