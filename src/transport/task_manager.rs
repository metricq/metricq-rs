//! Utilities for spawning and shutting down transport background tasks.

use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument};

/// Manages the lifetime of background taksks.
///
/// All spawned tasks share a common [`CancellationToken`], allowing them
/// to cooperatively terminate during shutdown.
///
/// Shutdown is initiated by cancelling the shared token and waiting for
/// all spawned tasks to complete.
#[derive(Debug)]
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

    /// Returns the shutdown token of the task manager.
    pub fn token(&self) -> CancellationToken {
        self.token.clone()
    }

    /// Spawns a background taks managed by this instance.
    ///
    /// The taks is awaited during [`TaskManager::shutdown`].
    #[instrument(skip(self, future))]
    pub fn spawn<F>(&mut self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        debug!("spawning new background task");
        self.tasks.spawn(future);
    }

    /// Requests shutdown and waits for all manages tasks to finish.
    ///
    /// Shutdown is cooperative: tasks are notified by cancelling the shared
    /// [`CancellationToken`]. Each task is expected to observe the token and
    /// exit in a timely manner.
    #[instrument(skip(self))]
    pub async fn shutdown(mut self) {
        info!("initiating task manager shutdown");
        self.token.cancel();

        let mut count = 0;
        while let Some(res) = self.tasks.join_next().await {
            count += 1;
            match res {
                Ok(()) => {
                    debug!(task_index = count, "backgorund task completed successfully");
                }
                Err(err) => {
                    error!(
                        task_index = count,
                        ?err,
                        "background task panicked or failed to join"
                    );
                }
            }
        }

        info!(
            total_tasks = count,
            "all background tasks completed and shut down"
        );
    }
}

impl Default for TaskManager {
    fn default() -> Self {
        Self::new()
    }
}
