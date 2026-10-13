//! History client module for querying and managing historical metric data.

mod client;
pub(crate) mod request;
pub(crate) mod response;

pub use client::HistoryClient;
pub use request::HistoryRequest;
pub use response::HistoryResponse;
