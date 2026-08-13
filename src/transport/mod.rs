//! Internal AMQP transport layer.
//!
//! This module provides the communication infrastructure used by the
//! `MetricQ` client. It manages AMQP connections and channels, consumes
//! and publishes messages, coordinates RPC request/response handling,
//! and provides common transport utilities.
//!
//! The transport layer is intentionally hidden from the public API.
//! Users interact with higher-level client abstractions instead.

pub mod channel;
pub mod connection;
pub mod consumer;
pub mod correlation;
pub mod rpc;
pub mod task_manager;
pub mod types;
