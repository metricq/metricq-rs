//! Shared transport types used throughout the AMQP implementation.

/// AMQP correlation identifier used to match RPC requests and reponses.
pub(crate) type CorrelationId = String;

/// AMQP routing key used for message routing.
pub(crate) type RoutingKey = String;

/// Name of an AMQP exchange.
pub(crate) type ExchangeName = String;

/// Name of an AMQP queue.
pub(crate) type QueueName = String;
