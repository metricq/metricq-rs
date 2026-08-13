//! Types used to identify metrics.

use derive_more::{Display, Into};

/// Identifier of a `MetricQ` metric.
///
/// `MetricId` is a lightweight wrapper around a string value that
/// provides type safety when working with metric identifiers.
#[derive(Debug, Clone, PartialEq, Eq, Display, Hash, PartialOrd, Ord, Into)]
#[display("{_0}")]
pub struct MetricId(String);

impl MetricId {
    /// Creates a new metric identifier from a string value.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Returns the metric identifier as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for MetricId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<String> for MetricId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for MetricId {
    fn from(value: &str) -> Self {
        Self(value.into())
    }
}
