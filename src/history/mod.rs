//! History client module for querying and managing historical metric data.

mod client;
pub mod request;
pub mod response;

pub use client::HistoryClient;
pub use request::{HistoryRequest, HistoryRequestBuilder, HistoryRequestType};
pub use response::HistoryResponse;
