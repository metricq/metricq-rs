//! Utilities for spawning and shutting down transport background tasks.

use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

/// Manages the lifetime of background taksks.
///
/// All spawned tasks share a common [`CancellationToken`], allowing them
/// to cooperatively terminate during shutdown.
///
/// Shutdown is initiated by cancelling the shared token and waiting for
/// all spawned tasks to complete.
pub(crate) struct TaskManager {
    token: CancellationToken,
    tasks: JoinSet<()>,
}

impl TaskManager {
    #[must_use]
    pub fn new() -> Self {
        Self {
            token: CancellationToken::new(),
            tasks: JoinSet::new(),
        }
    }

    /// Returns the shared cancellation token.
    ///
    /// Background taksks should monitor this token and
    /// terminate promptly when cancellation is requested.
    pub fn token(&self) -> CancellationToken {
        self.token.clone()
    }

    /// Spawns a background taks managed by this instance.
    ///
    /// The taks is awaited during [`TaskManager::shutdown`].
    pub fn spawn<F>(&mut self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.tasks.spawn(future);
    }

    /// Requests shutdown and waits for all manages tasks to finish.
    ///
    /// Shutdown is cooperative: tasks are notified by cancelling the shared
    /// [`CancellationToken`]. Each task is expected to observe the token and
    /// exit in a timely manner.
    pub async fn shutdown(mut self) {
        self.token.cancel();

        while self.tasks.join_next().await.is_some() {}
    }
}

impl Default for TaskManager {
    fn default() -> Self {
        Self::new()
    }
}
