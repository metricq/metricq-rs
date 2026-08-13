//!

use crate::{
    error::MetricQResult,
    history::{HistoryRequest, HistoryResponse, request::HistoryRequestType},
    transport::{rpc::client::RpcClient, types::ExchangeName},
    types::{MetricId, TimeDelta, Timestamp},
};

///
#[derive(Debug, Clone)]
pub struct HistoryClient {
    rpc: RpcClient,
    exchange: ExchangeName,
}

impl HistoryClient {
    #[must_use]
    pub(crate) const fn new(rpc: RpcClient, exchange: ExchangeName) -> Self {
        Self { rpc, exchange }
    }

    // TODO
    /// .
    ///
    /// # Errors
    ///
    /// This function will return an error if .
    pub async fn get(
        &self,
        metric: MetricId,
        start: Timestamp,
        end: Timestamp,
        interval: TimeDelta,
    ) -> MetricQResult<HistoryResponse> {
        let request = HistoryRequest {
            start,
            end,
            interval,
            type_: HistoryRequestType::Timeline,
        };

        let payload = request.encode()?;

        let response = self
            .rpc
            .call(self.exchange.clone(), &metric.into(), payload)
            .await?;

        HistoryResponse::decode(response)
    }
}
