//! Correlation identifier management for the RPC transport.
//!
//! Pending RPC requests are registered by correlation id until a
//! matching response is received.

use std::collections::HashMap;

use tokio::sync::{Mutex, oneshot::Sender};
use tracing::debug;

use crate::{
    error::{MetricQError, MetricQResult},
    transport::types::CorrelationId,
};

/// Stores pending RPC requests indexed by their correlation identifier.
///
/// Each outgoing RPC request registers a one-shot sender under a unique
/// correlation id. When the corresponding response arrives, the entry is
/// removed and the response is forwarded to the waiting receiver.
///
/// ## Invariants
///
/// - Each correlation id is registered at most once.
/// - Each entry is removed exactly once.
/// - Dispatch consumes the registered entry.
#[derive(Debug)]
pub(crate) struct CorrelationRegistry<T> {
    inner: Mutex<HashMap<CorrelationId, Sender<T>>>,
}

impl<T> CorrelationRegistry<T> {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }

    /// Registered a pending request.
    ///
    /// The provided sender will receive the response associated with the
    /// given correlation id.
    pub async fn insert(&self, id: CorrelationId, sender: Sender<T>) {
        debug!(%id, "registering correlation id in registry");
        self.inner.lock().await.insert(id, sender);
    }

    /// Removes a pending request without dispatching a response.
    ///
    /// # Error
    ///
    /// Returns an error if no request is registered under the given
    /// correlation id.
    pub async fn remove(&self, id: &CorrelationId) -> MetricQResult<()> {
        debug!(%id, "removing correlation id from registry");
        self.inner.lock().await.remove(id).ok_or_else(|| {
            debug!(%id, "failed to remove correlation id: entry not found");
            MetricQError::MissingCorrelationEntry(id.clone())
        })?;
        Ok(())
    }

    /// Delivers a response to the waiting request.
    ///
    /// If a matching correlation id exists, the associated sender is
    /// removed from the registry and the response is forwarded.
    ///
    /// Unknown correlation ids are silently ignored.
    pub async fn dispatch(&self, id: &str, value: T) {
        let val = self.inner.lock().await.remove(id);
        if let Some(sender) = val {
            debug!(%id, "dispatching response to waiting request");
            let _ = sender.send(value);
        } else {
            debug!(%id, "received response for unknown or expired correlation id (ignoring)");
        }
    }
}

impl<T> Default for CorrelationRegistry<T> {
    fn default() -> Self {
        Self::new()
    }
}
