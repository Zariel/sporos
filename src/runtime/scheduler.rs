use std::collections::BTreeMap;
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;

use crate::config::SchedulingConfig;
use crate::domain::{JobName, JobState};
use crate::errors::DatabaseError;
use crate::persistence::repository::{JobStateUpdate, Repository};
use crate::runtime::backoff::fixed_retry_deadline_ms;
use crate::runtime::queue::{
    BoundedWorkQueue, EnqueueError, QueueKind, WorkReceiver, bounded_work_queue,
};
use crate::time::unix_ms_to_rfc3339_seconds;
use tokio::sync::Mutex;
use tracing::{debug_span, info_span};

pub const INDEXER_CAPS_JOB_NAME: &str = "indexer_caps";
pub const CLEANUP_JOB_NAME: &str = "cleanup";
pub const MEDIA_INVENTORY_JOB_NAME: &str = "media_inventory";

pub fn scheduled_job_has_executor(job_name: &JobName) -> bool {
    matches!(
        job_name.as_str(),
        INDEXER_CAPS_JOB_NAME | CLEANUP_JOB_NAME | MEDIA_INVENTORY_JOB_NAME
    )
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SchedulerConfig {
    pub jobs: Vec<ScheduledJob>,
    pub claim_limit: u16,
    pub failure_backoff_ms: i64,
}

impl SchedulerConfig {
    pub fn from_scheduling_config(config: &SchedulingConfig) -> Result<Self, SchedulerError> {
        Ok(Self {
            jobs: vec![
                ScheduledJob::new(CLEANUP_JOB_NAME, &config.cleanup_interval)?,
                ScheduledJob::new(MEDIA_INVENTORY_JOB_NAME, &config.media_inventory_interval)?,
                ScheduledJob::new(INDEXER_CAPS_JOB_NAME, &config.indexer_caps_interval)?,
            ],
            claim_limit: 16,
            failure_backoff_ms: 60_000,
        })
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ScheduledJob {
    pub name: JobName,
    pub interval_ms: i64,
}

impl ScheduledJob {
    pub fn new(name: &str, interval: &str) -> Result<Self, SchedulerError> {
        Ok(Self {
            name: JobName::new(name).map_err(|error| SchedulerError::InvalidConfig {
                field: "job name",
                message: error.to_string(),
            })?,
            interval_ms: parse_interval_ms(interval)?,
        })
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ScheduledJobRun {
    pub job_name: JobName,
    pub scheduled_at_ms: i64,
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub struct SchedulerTickSummary {
    pub seeded: usize,
    pub enqueued: usize,
    pub deferred: usize,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ImmediateRunOutcome {
    Queued,
    Coalesced,
    Deferred,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SchedulerError {
    InvalidConfig {
        field: &'static str,
        message: String,
    },
    UnknownJob {
        name: JobName,
    },
    Database {
        source: DatabaseError,
    },
}

#[derive(Debug, Clone)]
pub struct PersistedScheduler {
    repository: Repository,
    queue: BoundedWorkQueue<ScheduledJobRun>,
    jobs: BTreeMap<JobName, ScheduledJob>,
    claim_limit: u16,
    failure_backoff_ms: i64,
    claim_lock: Arc<Mutex<()>>,
}

impl PersistedScheduler {
    pub fn new(
        repository: Repository,
        queue: BoundedWorkQueue<ScheduledJobRun>,
        config: SchedulerConfig,
    ) -> Self {
        let jobs = config
            .jobs
            .into_iter()
            .map(|job| (job.name.clone(), job))
            .collect();
        Self {
            repository,
            queue,
            jobs,
            claim_limit: config.claim_limit,
            failure_backoff_ms: config.failure_backoff_ms,
            claim_lock: Arc::new(Mutex::new(())),
        }
    }

    pub async fn seed_jobs(&self, now_ms: i64) -> Result<usize, SchedulerError> {
        let snapshots = self.repository.job_status_snapshot(1_000).await?;
        self.disable_unsupported_jobs(&snapshots).await?;
        self.seed_supported_jobs(&snapshots, now_ms).await
    }

    async fn disable_unsupported_jobs(
        &self,
        snapshots: &[crate::persistence::repository::JobStatusSnapshot],
    ) -> Result<usize, SchedulerError> {
        let mut disabled = 0;
        for snapshot in snapshots {
            if self.jobs.contains_key(&snapshot.name) || snapshot.state == "disabled" {
                continue;
            }
            self.repository
                .record_job_status(
                    &snapshot.name,
                    JobStateUpdate {
                        state: JobState::Disabled,
                        last_started_at_ms: None,
                        last_finished_at_ms: None,
                        next_run_at_ms: None,
                        last_error: None,
                    },
                )
                .await?;
            disabled += 1;
        }
        Ok(disabled)
    }

    async fn seed_supported_jobs(
        &self,
        snapshots: &[crate::persistence::repository::JobStatusSnapshot],
        now_ms: i64,
    ) -> Result<usize, SchedulerError> {
        let mut seeded = 0;

        for job in self.jobs.values() {
            match snapshots.iter().find(|snapshot| snapshot.name == job.name) {
                Some(snapshot) if snapshot.state == "disabled" => {
                    self.repository
                        .upsert_job_state(&job.name, JobState::Pending, Some(now_ms), None)
                        .await?;
                }
                Some(_) => {
                    continue;
                }
                None => {
                    self.repository
                        .record_job_status(
                            &job.name,
                            JobStateUpdate {
                                state: JobState::Pending,
                                last_started_at_ms: None,
                                last_finished_at_ms: None,
                                next_run_at_ms: Some(now_ms),
                                last_error: None,
                            },
                        )
                        .await?;
                    seeded += 1;
                }
            }
        }

        Ok(seeded)
    }

    pub async fn tick(&self, now_ms: i64) -> Result<SchedulerTickSummary, SchedulerError> {
        let _span = info_span!(
            "scheduler.tick",
            now = %unix_ms_to_rfc3339_seconds(now_ms),
            claim_limit = self.claim_limit
        );
        let _claim_guard = self.claim_lock.lock().await;
        let seeded = self.seed_jobs(now_ms).await?;
        let ready_jobs = self.repository.ready_jobs(now_ms, self.claim_limit).await?;
        let mut enqueued = 0;
        let mut deferred = 0;

        for job_name in ready_jobs {
            let _job_span = debug_span!("scheduler.enqueue_job", job_name = %job_name);
            if !self.jobs.contains_key(&job_name) {
                continue;
            }
            if !self
                .repository
                .claim_scheduled_job_run(&job_name, now_ms)
                .await?
            {
                continue;
            }
            match self.queue.try_enqueue(ScheduledJobRun {
                job_name: job_name.clone(),
                scheduled_at_ms: now_ms,
            }) {
                Ok(()) => enqueued += 1,
                Err(EnqueueError::Full { .. } | EnqueueError::Closed { .. }) => {
                    self.repository
                        .record_job_status(
                            &job_name,
                            JobStateUpdate {
                                state: JobState::Waiting,
                                last_started_at_ms: None,
                                last_finished_at_ms: None,
                                next_run_at_ms: Some(fixed_retry_deadline_ms(
                                    now_ms,
                                    self.failure_backoff_ms,
                                    None,
                                )),
                                last_error: Some("scheduler queue unavailable"),
                            },
                        )
                        .await?;
                    deferred += 1;
                }
            }
        }

        Ok(SchedulerTickSummary {
            seeded,
            enqueued,
            deferred,
        })
    }

    pub async fn trigger_now(
        &self,
        job_name: &JobName,
        now_ms: i64,
    ) -> Result<bool, SchedulerError> {
        let _claim_guard = self.claim_lock.lock().await;
        if !self.jobs.contains_key(job_name) {
            return Err(SchedulerError::UnknownJob {
                name: job_name.clone(),
            });
        }
        let snapshots = self.repository.job_status_snapshot(1_000).await?;
        if snapshots.iter().any(|snapshot| {
            snapshot.name == *job_name
                && (snapshot.state == "running"
                    || snapshot.next_run_at_ms.is_some_and(|next| next <= now_ms))
        }) {
            return Ok(false);
        }

        self.repository
            .record_job_status(
                job_name,
                JobStateUpdate {
                    state: JobState::Pending,
                    last_started_at_ms: None,
                    last_finished_at_ms: None,
                    next_run_at_ms: Some(now_ms),
                    last_error: None,
                },
            )
            .await?;
        Ok(true)
    }

    pub async fn enqueue_immediate_run(
        &self,
        job_name: &JobName,
        now_ms: i64,
    ) -> Result<ImmediateRunOutcome, SchedulerError> {
        let _claim_guard = self.claim_lock.lock().await;
        self.job(job_name)?;
        if !self
            .repository
            .claim_immediate_job_run(job_name, now_ms)
            .await?
        {
            return Ok(ImmediateRunOutcome::Coalesced);
        }

        match self.queue.try_enqueue(ScheduledJobRun {
            job_name: job_name.clone(),
            scheduled_at_ms: now_ms,
        }) {
            Ok(()) => Ok(ImmediateRunOutcome::Queued),
            Err(EnqueueError::Full { .. } | EnqueueError::Closed { .. }) => {
                self.repository
                    .record_job_status(
                        job_name,
                        JobStateUpdate {
                            state: JobState::Waiting,
                            last_started_at_ms: None,
                            last_finished_at_ms: Some(now_ms),
                            next_run_at_ms: Some(fixed_retry_deadline_ms(
                                now_ms,
                                self.failure_backoff_ms,
                                None,
                            )),
                            last_error: Some("scheduler queue unavailable"),
                        },
                    )
                    .await?;
                Ok(ImmediateRunOutcome::Deferred)
            }
        }
    }

    pub async fn complete_success(
        &self,
        job_name: &JobName,
        finished_at_ms: i64,
    ) -> Result<(), SchedulerError> {
        let job = self.job(job_name)?;
        self.repository
            .record_job_status(
                job_name,
                JobStateUpdate {
                    state: JobState::Succeeded,
                    last_started_at_ms: None,
                    last_finished_at_ms: Some(finished_at_ms),
                    next_run_at_ms: Some(finished_at_ms + job.interval_ms),
                    last_error: None,
                },
            )
            .await?;
        Ok(())
    }

    pub async fn complete_failure(
        &self,
        job_name: &JobName,
        finished_at_ms: i64,
        error: &str,
    ) -> Result<(), SchedulerError> {
        self.job(job_name)?;
        self.repository
            .record_job_status(
                job_name,
                JobStateUpdate {
                    state: JobState::Failed,
                    last_started_at_ms: None,
                    last_finished_at_ms: Some(finished_at_ms),
                    next_run_at_ms: Some(fixed_retry_deadline_ms(
                        finished_at_ms,
                        self.failure_backoff_ms,
                        None,
                    )),
                    last_error: Some(error),
                },
            )
            .await?;
        Ok(())
    }

    pub async fn complete_shutdown(
        &self,
        job_name: &JobName,
        finished_at_ms: i64,
    ) -> Result<(), SchedulerError> {
        self.job(job_name)?;
        self.repository
            .record_job_status(
                job_name,
                JobStateUpdate {
                    state: JobState::Waiting,
                    last_started_at_ms: None,
                    last_finished_at_ms: Some(finished_at_ms),
                    next_run_at_ms: Some(finished_at_ms),
                    last_error: Some("scheduler shutting down"),
                },
            )
            .await?;
        Ok(())
    }

    pub fn failure_backoff(&self) -> std::time::Duration {
        std::time::Duration::from_millis(u64::try_from(self.failure_backoff_ms).unwrap_or(0))
    }

    fn job(&self, job_name: &JobName) -> Result<&ScheduledJob, SchedulerError> {
        self.jobs
            .get(job_name)
            .ok_or_else(|| SchedulerError::UnknownJob {
                name: job_name.clone(),
            })
    }
}

pub fn scheduler_queue(
    capacity: NonZeroUsize,
) -> (
    BoundedWorkQueue<ScheduledJobRun>,
    WorkReceiver<ScheduledJobRun>,
) {
    bounded_work_queue(QueueKind::Indexing, capacity)
}

pub(crate) fn parse_interval_ms(value: &str) -> Result<i64, SchedulerError> {
    let trimmed = value.trim();
    let split_at = trimmed
        .find(|character: char| !character.is_ascii_digit())
        .ok_or_else(|| SchedulerError::InvalidConfig {
            field: "interval",
            message: format!("{value} is missing a duration unit"),
        })?;
    let (amount, unit) = trimmed.split_at(split_at);
    let amount = amount
        .parse::<i64>()
        .map_err(|error| SchedulerError::InvalidConfig {
            field: "interval",
            message: error.to_string(),
        })?;
    if amount <= 0 {
        return Err(SchedulerError::InvalidConfig {
            field: "interval",
            message: "interval must be positive".to_owned(),
        });
    }
    let multiplier = match unit {
        "s" => 1_000,
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        _ => {
            return Err(SchedulerError::InvalidConfig {
                field: "interval",
                message: format!("unsupported duration unit {unit}"),
            });
        }
    };

    amount
        .checked_mul(multiplier)
        .ok_or_else(|| SchedulerError::InvalidConfig {
            field: "interval",
            message: "interval is too large".to_owned(),
        })
}

impl From<DatabaseError> for SchedulerError {
    fn from(source: DatabaseError) -> Self {
        Self::Database { source }
    }
}

impl fmt::Display for SchedulerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig { field, message } => {
                write!(formatter, "invalid scheduler config {field}: {message}")
            }
            Self::UnknownJob { name } => write!(formatter, "unknown scheduled job {name}"),
            Self::Database { source } => write!(formatter, "{source}"),
        }
    }
}

impl std::error::Error for SchedulerError {}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use super::*;

    #[tokio::test]
    async fn scheduler_seeds_and_enqueues_due_jobs() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let (queue, mut receiver) = scheduler_queue(nonzero(4));
        let scheduler = PersistedScheduler::new(repository.clone(), queue, test_config());

        let summary = scheduler.tick(100).await.unwrap();
        let first = receiver.recv().await.unwrap();
        let second = receiver.recv().await.unwrap();
        let jobs = repository.job_status_snapshot(10).await.unwrap();

        assert_eq!(2, summary.seeded);
        assert_eq!(2, summary.enqueued);
        assert_eq!("cleanup", first.job_name.as_str());
        assert_eq!(INDEXER_CAPS_JOB_NAME, second.job_name.as_str());
        assert!(jobs.iter().all(|job| job.state == "running"));
    }

    #[tokio::test]
    async fn scheduler_avoids_overlapping_same_name_jobs() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let (queue, mut receiver) = scheduler_queue(nonzero(4));
        let scheduler = PersistedScheduler::new(repository, queue, test_config());

        scheduler.tick(100).await.unwrap();
        scheduler.tick(100).await.unwrap();
        let mut count = 0;
        while receiver.recv().await.is_some() {
            count += 1;
            if count == 2 {
                break;
            }
        }

        assert_eq!(2, count);
    }

    #[tokio::test]
    async fn scheduler_persists_success_and_failure_outcomes() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let (queue, _receiver) = scheduler_queue(nonzero(4));
        let scheduler = PersistedScheduler::new(repository.clone(), queue, test_config());
        let indexer_caps = JobName::new(INDEXER_CAPS_JOB_NAME).unwrap();
        let cleanup = JobName::new("cleanup").unwrap();

        scheduler.tick(100).await.unwrap();
        scheduler
            .complete_success(&indexer_caps, 200)
            .await
            .unwrap();
        scheduler
            .complete_failure(&cleanup, 250, "temporary failure")
            .await
            .unwrap();
        let jobs = repository.job_status_snapshot(10).await.unwrap();
        let indexer_caps_job = jobs.iter().find(|job| job.name == indexer_caps).unwrap();
        let cleanup_job = jobs.iter().find(|job| job.name == cleanup).unwrap();

        assert_eq!("succeeded", indexer_caps_job.state);
        assert_eq!(Some(1_200), indexer_caps_job.next_run_at_ms);
        assert_eq!("failed", cleanup_job.state);
        assert_eq!(Some(60_250), cleanup_job.next_run_at_ms);
        assert_eq!(Some("temporary failure".to_owned()), cleanup_job.last_error);
    }

    #[tokio::test]
    async fn scheduler_coalesces_immediate_triggers() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let (queue, _receiver) = scheduler_queue(nonzero(4));
        let scheduler = PersistedScheduler::new(repository, queue, test_config());
        let cleanup = JobName::new(CLEANUP_JOB_NAME).unwrap();

        assert!(scheduler.trigger_now(&cleanup, 100).await.unwrap());
        assert!(!scheduler.trigger_now(&cleanup, 100).await.unwrap());
        scheduler.tick(100).await.unwrap();
        assert!(!scheduler.trigger_now(&cleanup, 100).await.unwrap());
    }

    #[tokio::test]
    async fn scheduler_immediate_run_enqueues_only_requested_job() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let (queue, mut receiver) = scheduler_queue(nonzero(4));
        let scheduler = PersistedScheduler::new(repository.clone(), queue, test_config());
        let cleanup = JobName::new(CLEANUP_JOB_NAME).unwrap();

        let outcome = scheduler
            .enqueue_immediate_run(&cleanup, 100)
            .await
            .unwrap();
        let run = receiver.recv().await.unwrap();
        let jobs = repository.job_status_snapshot(10).await.unwrap();

        assert_eq!(ImmediateRunOutcome::Queued, outcome);
        assert_eq!(CLEANUP_JOB_NAME, run.job_name.as_str());
        assert_eq!(1, jobs.len());
        assert_eq!("running", jobs[0].state);
    }

    #[tokio::test]
    async fn scheduler_coalesces_concurrent_tick_and_immediate_run() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let (queue, _receiver) = scheduler_queue(nonzero(4));
        let scheduler = PersistedScheduler::new(
            repository.clone(),
            queue.clone(),
            SchedulerConfig {
                jobs: vec![ScheduledJob::new(CLEANUP_JOB_NAME, "1s").unwrap()],
                claim_limit: 10,
                failure_backoff_ms: 60_000,
            },
        );
        let cleanup = JobName::new(CLEANUP_JOB_NAME).unwrap();

        let (tick, immediate) = tokio::join!(
            scheduler.tick(100),
            scheduler.enqueue_immediate_run(&cleanup, 100)
        );
        let tick = tick.unwrap();
        let immediate = immediate.unwrap();
        let jobs = repository.job_status_snapshot(10).await.unwrap();

        assert_eq!(1, queue.stats().accepted);
        assert_eq!(1, jobs.len());
        assert_eq!("running", jobs[0].state);
        assert!(
            (tick.enqueued == 1 && immediate == ImmediateRunOutcome::Coalesced)
                || (tick.enqueued == 0 && immediate == ImmediateRunOutcome::Queued)
        );
    }

    #[tokio::test]
    async fn scheduler_immediate_run_defers_when_queue_is_full() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let (queue, _receiver) = scheduler_queue(nonzero(1));
        let scheduler = PersistedScheduler::new(repository.clone(), queue, test_config());
        let indexer_caps = JobName::new(INDEXER_CAPS_JOB_NAME).unwrap();
        let cleanup = JobName::new("cleanup").unwrap();

        scheduler
            .enqueue_immediate_run(&indexer_caps, 100)
            .await
            .unwrap();
        let outcome = scheduler
            .enqueue_immediate_run(&cleanup, 150)
            .await
            .unwrap();
        let jobs = repository.job_status_snapshot(10).await.unwrap();
        let cleanup_job = jobs.iter().find(|job| job.name == cleanup).unwrap();

        assert_eq!(ImmediateRunOutcome::Deferred, outcome);
        assert_eq!("waiting", cleanup_job.state);
        assert_eq!(Some(60_150), cleanup_job.next_run_at_ms);
        assert_eq!(
            Some("scheduler queue unavailable".to_owned()),
            cleanup_job.last_error
        );
    }

    #[tokio::test]
    async fn scheduler_defers_when_queue_is_full_without_exiting() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let (queue, _receiver) = scheduler_queue(nonzero(1));
        let scheduler = PersistedScheduler::new(repository.clone(), queue, test_config());

        let summary = scheduler.tick(100).await.unwrap();
        let jobs = repository.job_status_snapshot(10).await.unwrap();

        assert_eq!(1, summary.enqueued);
        assert_eq!(1, summary.deferred);
        assert!(jobs.iter().any(|job| job.state == "waiting"));
    }

    #[tokio::test]
    async fn scheduler_disables_persisted_rss_and_reactivates_cleanup() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let rss = JobName::new("rss").unwrap();
        let cleanup = JobName::new(CLEANUP_JOB_NAME).unwrap();
        repository
            .record_job_status(
                &rss,
                JobStateUpdate {
                    state: JobState::Failed,
                    last_started_at_ms: Some(10),
                    last_finished_at_ms: Some(20),
                    next_run_at_ms: Some(100),
                    last_error: Some("scheduled job rss does not have an executor yet"),
                },
            )
            .await
            .unwrap();
        repository
            .record_job_status(
                &cleanup,
                JobStateUpdate {
                    state: JobState::Waiting,
                    last_started_at_ms: Some(30),
                    last_finished_at_ms: Some(40),
                    next_run_at_ms: Some(100),
                    last_error: Some("scheduled job cleanup does not have an executor yet"),
                },
            )
            .await
            .unwrap();
        let (queue, mut receiver) = scheduler_queue(nonzero(4));
        let scheduler = PersistedScheduler::new(
            repository.clone(),
            queue,
            SchedulerConfig::from_scheduling_config(&SchedulingConfig::default()).unwrap(),
        );

        let summary = scheduler.tick(100).await.unwrap();
        let runs = [
            receiver.recv().await.unwrap(),
            receiver.recv().await.unwrap(),
            receiver.recv().await.unwrap(),
        ];
        let jobs = repository.job_status_snapshot(10).await.unwrap();
        let rss_job = jobs.iter().find(|job| job.name == rss).unwrap();
        let cleanup_job = jobs.iter().find(|job| job.name == cleanup).unwrap();

        assert_eq!(2, summary.seeded);
        assert_eq!(3, summary.enqueued);
        assert!(
            runs.iter()
                .any(|run| run.job_name.as_str() == CLEANUP_JOB_NAME)
        );
        assert!(
            runs.iter()
                .any(|run| run.job_name.as_str() == MEDIA_INVENTORY_JOB_NAME)
        );
        assert!(
            runs.iter()
                .any(|run| run.job_name.as_str() == INDEXER_CAPS_JOB_NAME)
        );
        assert_eq!("disabled", rss_job.state);
        assert_eq!(None, rss_job.next_run_at_ms);
        assert_eq!(None, rss_job.last_error);
        assert_eq!("running", cleanup_job.state);
        assert_eq!(None, cleanup_job.next_run_at_ms);
        assert_eq!(None, cleanup_job.last_error);
    }

    #[tokio::test]
    async fn scheduler_disables_unknown_persisted_jobs() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let stale = JobName::new("stale_job").unwrap();
        repository
            .record_job_status(
                &stale,
                JobStateUpdate {
                    state: JobState::Failed,
                    last_started_at_ms: Some(10),
                    last_finished_at_ms: Some(20),
                    next_run_at_ms: Some(100),
                    last_error: Some("obsolete job"),
                },
            )
            .await
            .unwrap();
        let (queue, _receiver) = scheduler_queue(nonzero(4));
        let scheduler = PersistedScheduler::new(
            repository.clone(),
            queue,
            SchedulerConfig::from_scheduling_config(&SchedulingConfig::default()).unwrap(),
        );

        let summary = scheduler.tick(100).await.unwrap();
        let jobs = repository.job_status_snapshot(10).await.unwrap();
        let stale_job = jobs.iter().find(|job| job.name == stale).unwrap();

        assert_eq!(3, summary.seeded);
        assert_eq!("disabled", stale_job.state);
        assert_eq!(None, stale_job.next_run_at_ms);
        assert_eq!(None, stale_job.last_error);
    }

    #[tokio::test]
    async fn scheduler_reactivates_supported_disabled_jobs() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let indexer_caps = JobName::new(INDEXER_CAPS_JOB_NAME).unwrap();
        repository
            .record_job_status(
                &indexer_caps,
                JobStateUpdate {
                    state: JobState::Disabled,
                    last_started_at_ms: Some(10),
                    last_finished_at_ms: Some(20),
                    next_run_at_ms: None,
                    last_error: Some("job disabled while unsupported"),
                },
            )
            .await
            .unwrap();
        let (queue, mut receiver) = scheduler_queue(nonzero(4));
        let scheduler = PersistedScheduler::new(
            repository.clone(),
            queue,
            SchedulerConfig::from_scheduling_config(&SchedulingConfig::default()).unwrap(),
        );

        let summary = scheduler.tick(100).await.unwrap();
        let runs = [
            receiver.recv().await.unwrap(),
            receiver.recv().await.unwrap(),
            receiver.recv().await.unwrap(),
        ];
        let jobs = repository.job_status_snapshot(10).await.unwrap();
        let job = jobs.iter().find(|job| job.name == indexer_caps).unwrap();

        assert_eq!(2, summary.seeded);
        assert_eq!(3, summary.enqueued);
        assert!(
            runs.iter()
                .any(|run| run.job_name.as_str() == INDEXER_CAPS_JOB_NAME)
        );
        assert_eq!("running", job.state);
        assert_eq!(None, job.next_run_at_ms);
        assert_eq!(None, job.last_error);
    }

    #[test]
    fn default_scheduler_jobs_are_executable() {
        let config = SchedulerConfig::from_scheduling_config(&SchedulingConfig::default()).unwrap();

        assert_eq!(3, config.jobs.len());
        assert_eq!(CLEANUP_JOB_NAME, config.jobs[0].name.as_str());
        assert_eq!(86_400_000, config.jobs[0].interval_ms);
        assert_eq!(MEDIA_INVENTORY_JOB_NAME, config.jobs[1].name.as_str());
        assert_eq!(86_400_000, config.jobs[1].interval_ms);
        assert_eq!(INDEXER_CAPS_JOB_NAME, config.jobs[2].name.as_str());
        assert_eq!(86_400_000, config.jobs[2].interval_ms);
        assert!(
            config
                .jobs
                .iter()
                .all(|job| scheduled_job_has_executor(&job.name))
        );
    }

    #[test]
    fn scheduler_parses_config_intervals() {
        ScheduledJob::new("bad", "0s").unwrap_err();
        ScheduledJob::new("bad", "1w").unwrap_err();
    }

    #[test]
    fn scheduler_validates_inactive_config_intervals() {
        let cleanup = SchedulingConfig {
            cleanup_interval: "1w".to_owned(),
            ..SchedulingConfig::default()
        };

        SchedulerConfig::from_scheduling_config(&cleanup).unwrap_err();
    }

    fn test_config() -> SchedulerConfig {
        SchedulerConfig {
            jobs: vec![
                ScheduledJob::new("cleanup", "1s").unwrap(),
                ScheduledJob::new(INDEXER_CAPS_JOB_NAME, "1s").unwrap(),
            ],
            claim_limit: 10,
            failure_backoff_ms: 60_000,
        }
    }

    fn nonzero(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).unwrap_or(NonZeroUsize::MIN)
    }
}
