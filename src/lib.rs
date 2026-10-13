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

pub mod client;
pub mod codec;
pub mod config;
pub mod error;
pub mod history;
pub mod protobuf;
pub mod sink;
pub mod transport;
pub mod types;
