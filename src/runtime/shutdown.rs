use std::fmt;

use tokio::sync::watch;

use crate::domain::{JobState, ReasonText};
use crate::errors::DatabaseError;
use crate::persistence::repository::{JobStateUpdate, Repository};
use crate::runtime::queue::WorkReceiver;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ShutdownPhase {
    Running,
    Draining,
    Cancelled,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ShutdownState {
    pub phase: ShutdownPhase,
    pub reason: Option<ReasonText>,
}

#[derive(Debug, Clone)]
pub struct ShutdownController {
    sender: watch::Sender<ShutdownState>,
}

#[derive(Debug, Clone)]
pub struct ShutdownSignal {
    receiver: watch::Receiver<ShutdownState>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum QueueShutdownPolicy {
    Drain,
    Cancel,
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub struct QueueShutdownSummary {
    pub drained: usize,
    pub cancelled: bool,
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub struct ShutdownPersistenceSummary {
    pub waiting_jobs: usize,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ShutdownError {
    InvalidReason { message: String },
    Database { source: DatabaseError },
}

pub fn shutdown_channel() -> (ShutdownController, ShutdownSignal) {
    let state = ShutdownState {
        phase: ShutdownPhase::Running,
        reason: None,
    };
    let (sender, receiver) = watch::channel(state);
    (ShutdownController { sender }, ShutdownSignal { receiver })
}

impl ShutdownController {
    pub fn begin_draining(&self, reason: &str) -> Result<(), ShutdownError> {
        self.send(ShutdownPhase::Draining, reason)
    }

    pub fn cancel_now(&self, reason: &str) -> Result<(), ShutdownError> {
        self.send(ShutdownPhase::Cancelled, reason)
    }

    pub fn state(&self) -> ShutdownState {
        self.sender.borrow().clone()
    }

    fn send(&self, phase: ShutdownPhase, reason: &str) -> Result<(), ShutdownError> {
        let reason = ReasonText::new(reason).map_err(|error| ShutdownError::InvalidReason {
            message: error.to_string(),
        })?;
        self.sender
            .send(ShutdownState {
                phase,
                reason: Some(reason),
            })
            .map_err(|error| ShutdownError::InvalidReason {
                message: error.to_string(),
            })
    }
}

impl ShutdownSignal {
    pub fn state(&self) -> ShutdownState {
        self.receiver.borrow().clone()
    }

    pub async fn cancelled(&mut self) -> ShutdownState {
        loop {
            let state = self.receiver.borrow().clone();
            if state.phase != ShutdownPhase::Running {
                return state;
            }
            if self.receiver.changed().await.is_err() {
                return self.receiver.borrow().clone();
            }
        }
    }
}

pub async fn shutdown_queue<T>(
    receiver: &mut WorkReceiver<T>,
    policy: QueueShutdownPolicy,
) -> QueueShutdownSummary {
    receiver.close();

    match policy {
        QueueShutdownPolicy::Cancel => {
            let mut cancelled_items = 0;
            while receiver.recv().await.is_some() {
                receiver.mark_cancelled();
                cancelled_items += 1;
            }
            QueueShutdownSummary {
                drained: cancelled_items,
                cancelled: true,
            }
        }
        QueueShutdownPolicy::Drain => {
            let mut drained = 0;
            while receiver.recv().await.is_some() {
                receiver.mark_completed();
                drained += 1;
            }
            QueueShutdownSummary {
                drained,
                cancelled: false,
            }
        }
    }
}

pub async fn record_safe_job_shutdown(
    repository: &Repository,
    now_ms: i64,
) -> Result<ShutdownPersistenceSummary, ShutdownError> {
    let jobs = repository.job_status_snapshot(1_000).await?;
    let mut waiting_jobs = 0;

    for job in jobs.iter().filter(|job| job.state == "running") {
        repository
            .record_job_status(
                &job.name,
                JobStateUpdate {
                    state: JobState::Waiting,
                    last_started_at_ms: None,
                    last_finished_at_ms: Some(now_ms),
                    next_run_at_ms: Some(now_ms),
                    last_error: Some("shutdown before job completed"),
                },
            )
            .await?;
        waiting_jobs += 1;
    }

    Ok(ShutdownPersistenceSummary { waiting_jobs })
}

impl From<DatabaseError> for ShutdownError {
    fn from(source: DatabaseError) -> Self {
        Self::Database { source }
    }
}

impl fmt::Display for ShutdownError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidReason { message } => {
                write!(formatter, "invalid shutdown reason: {message}")
            }
            Self::Database { source } => write!(formatter, "{source}"),
        }
    }
}

impl std::error::Error for ShutdownError {}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use super::*;
    use crate::domain::{JobName, JobState};
    use crate::persistence::repository::JobStateUpdate;
    use crate::runtime::queue::{QueueKind, bounded_work_queue};

    #[tokio::test]
    async fn shutdown_signal_notifies_waiters() {
        let (controller, mut signal) = shutdown_channel();

        controller.begin_draining("sigterm").unwrap();
        let state = signal.cancelled().await;

        assert_eq!(ShutdownPhase::Draining, state.phase);
        assert_eq!("sigterm", state.reason.unwrap().as_str());
    }

    #[tokio::test]
    async fn shutdown_queue_drains_or_cancels_by_policy() {
        let (drain_queue, mut drain_receiver) = bounded_work_queue(QueueKind::Indexing, nonzero(4));
        drain_queue.try_enqueue(1).unwrap();
        drain_queue.try_enqueue(2).unwrap();

        let drained = shutdown_queue(&mut drain_receiver, QueueShutdownPolicy::Drain).await;
        let rejected = drain_queue.try_enqueue(3).unwrap_err();

        assert_eq!(
            QueueShutdownSummary {
                drained: 2,
                cancelled: false
            },
            drained
        );
        assert!(matches!(
            rejected,
            crate::runtime::queue::EnqueueError::Closed { .. }
        ));

        let (cancel_queue, mut cancel_receiver) =
            bounded_work_queue(QueueKind::Indexing, nonzero(4));
        cancel_queue.try_enqueue(1).unwrap();

        let cancelled = shutdown_queue(&mut cancel_receiver, QueueShutdownPolicy::Cancel).await;

        assert_eq!(
            QueueShutdownSummary {
                drained: 1,
                cancelled: true
            },
            cancelled
        );
        assert_eq!(0, cancel_queue.stats().depth);
        assert_eq!(1, cancel_queue.stats().cancelled);
    }

    #[tokio::test]
    async fn shutdown_records_running_jobs_as_waiting() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let running = JobName::new("rss").unwrap();
        let succeeded = JobName::new("cleanup").unwrap();
        repository
            .record_job_status(
                &running,
                JobStateUpdate {
                    state: JobState::Running,
                    last_started_at_ms: Some(100),
                    last_finished_at_ms: None,
                    next_run_at_ms: None,
                    last_error: None,
                },
            )
            .await
            .unwrap();
        repository
            .record_job_status(
                &succeeded,
                JobStateUpdate {
                    state: JobState::Succeeded,
                    last_started_at_ms: Some(100),
                    last_finished_at_ms: Some(200),
                    next_run_at_ms: Some(1_000),
                    last_error: None,
                },
            )
            .await
            .unwrap();

        let summary = record_safe_job_shutdown(&repository, 300).await.unwrap();
        let jobs = repository.job_status_snapshot(10).await.unwrap();
        let running_job = jobs.iter().find(|job| job.name == running).unwrap();
        let succeeded_job = jobs.iter().find(|job| job.name == succeeded).unwrap();

        assert_eq!(1, summary.waiting_jobs);
        assert_eq!("waiting", running_job.state);
        assert_eq!(Some(300), running_job.next_run_at_ms);
        assert_eq!(
            Some("shutdown before job completed".to_owned()),
            running_job.last_error
        );
        assert_eq!("succeeded", succeeded_job.state);
    }

    fn nonzero(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).unwrap_or(NonZeroUsize::MIN)
    }
}
