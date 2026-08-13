//! Internal support for RPC communication.
//!
//! This module implements the request/response pattern used by MetricQ
//! over AMQP. It provides the client-side request API as well as the
//! infrastructure for receiving and dispatching RPC replies.

pub mod client;
pub mod reply_handler;
