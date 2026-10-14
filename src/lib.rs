//! `MetricQ` client library.
//!
//! This crate provides a Rust interface for communicating with `MetricQ`
//! servers. It contains the data model, transport layer, configuration,
//! and utilitiy components required to publish metrics and interact with
//! `MetricQ` services.
//!
//! The crate is organized into the following modules:
//!
//! - [`config`] - Client configuration and connection settings.
//! - [`error`] - Error types returned by the library.
//! - [`transport`] - Internal communication layer responsible for AMQP
//!   connections, messaging, and RPC handling.
//! - [`types`] - Core `MetricQ` data types such as metrics, timestamps,
//!   values, and aggregates.
//! - [`util`] - Shared utility functionality.

#![deny(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod client;
mod codec;
mod config;
mod error;
mod history;
mod protobuf;
mod sink;
mod transport;
mod types;

pub use client::Client;
pub use config::{
    ClientConfig, ClientConfigBuilder, ReconnectPolicy, ReconnectPolicyBuilder, Timeouts,
    TimeoutsBuilder,
};
pub use error::{MetricQError, MetricQResult};
pub use history::{
    HistoryClient, HistoryRequest, HistoryRequestBuilder, HistoryRequestType, HistoryResponse,
};
pub use sink::{DataConsumer, SinkClient};
pub use types::value::{MetricTimeValue, TimeValue};
pub use types::{MetricId, TimeDelta, Timestamp};
