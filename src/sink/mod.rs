//! Sink client infrastructure for subscribing to and consuming real-time metric streams.

pub mod client;
pub mod consumer;
pub mod response;

pub use client::SinkClient;
pub use consumer::DataConsumer;
