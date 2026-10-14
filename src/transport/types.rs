//! Shared transport types used throughout the AMQP implementation.

/// AMQP correlation identifier used to match RPC requests and reponses.
pub type CorrelationId = String;

/// AMQP routing key used for message routing.
pub type RoutingKey = String;

/// Name of an AMQP exchange.
pub type ExchangeName = String;

/// Name of an AMQP queue.
pub type QueueName = String;
