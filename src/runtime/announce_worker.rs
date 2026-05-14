use std::fmt;
use std::future::Future;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug_span, info_span};

use crate::announce::{AnnounceQueueConfig, AnnounceReason, AnnounceWorkId};
use crate::domain::ReasonText;
use crate::errors::DatabaseError;
use crate::persistence::repository::{AnnounceRetryUpdate, Repository};
use crate::runtime::shutdown::{ShutdownPhase, ShutdownSignal};

#[derive(Debug, Clone)]
pub struct AnnounceWorker {
    repository: Repository,
    config: AnnounceWorkerConfig,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AnnounceWorkerConfig {
    pub owner: ReasonText,
    pub claim_batch_size: u16,
    pub lease_duration: Duration,
    pub lease_renewal: Duration,
    pub dependency_recovery_probe_interval: Duration,
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub struct AnnounceStartupSummary {
    pub expired: u64,
    pub recovered_leases: u64,
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub struct AnnounceWorkerSummary {
    pub claimed: usize,
    pub completed: usize,
    pub released: usize,
    pub cancelled: usize,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum AnnounceWorkOutcome {
    Succeeded {
        reason: AnnounceReason,
        outcome: String,
    },
    Waiting {
        reason: AnnounceReason,
        next_attempt_at_ms: i64,
        dependency: Option<(String, String)>,
    },
    Retryable {
        reason: AnnounceReason,
        next_attempt_at_ms: i64,
        error_class: String,
        redacted_message: String,
    },
    TerminalFailed {
        reason: AnnounceReason,
        redacted_message: String,
    },
    Release {
        reason: AnnounceReason,
        next_attempt_at_ms: i64,
    },
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum AnnounceWorkerError {
    InvalidConfig { message: String },
    Database { source: DatabaseError },
}

impl AnnounceWorker {
    pub fn new(
        repository: Repository,
        owner: &str,
        queue_config: &AnnounceQueueConfig,
    ) -> Result<Self, AnnounceWorkerError> {
        queue_config
            .validate()
            .map_err(|error| AnnounceWorkerError::InvalidConfig {
                message: error.to_string(),
            })?;
        let owner = ReasonText::new(owner).map_err(|error| AnnounceWorkerError::InvalidConfig {
            message: error.to_string(),
        })?;
        let config = AnnounceWorkerConfig {
            owner,
            claim_batch_size: queue_config.claim_batch_size,
            lease_duration: Duration::from_secs(queue_config.lease_duration_secs),
            lease_renewal: Duration::from_secs(queue_config.lease_renewal_secs),
            dependency_recovery_probe_interval: Duration::from_secs(
                queue_config.retry_initial_delay_secs,
            ),
        };

        Ok(Self { repository, config })
    }

    pub const fn config(&self) -> &AnnounceWorkerConfig {
        &self.config
    }

    pub async fn recover_startup(
        &self,
        now_ms: i64,
    ) -> Result<AnnounceStartupSummary, AnnounceWorkerError> {
        let _span = info_span!("announce.recover_startup", now_ms);
        let expired = self.repository.expire_announce_work(now_ms).await?;
        let recovered_leases = self
            .repository
            .recover_stale_announce_leases(now_ms)
            .await?;

        Ok(AnnounceStartupSummary {
            expired,
            recovered_leases,
        })
    }

    pub async fn claim_ready(
        &self,
        now_ms: i64,
    ) -> Result<Vec<AnnounceWorkId>, AnnounceWorkerError> {
        let lease_until_ms = now_ms.saturating_add(duration_ms(self.config.lease_duration));
        let reconcile_limit = self.config.claim_batch_size.saturating_mul(4).max(1);
        self.repository
            .schedule_announce_dependency_backoff(
                now_ms,
                duration_ms(self.config.dependency_recovery_probe_interval),
                reconcile_limit,
            )
            .await?;
        self.repository
            .claim_announce_work(
                self.config.owner.as_str(),
                now_ms,
                lease_until_ms,
                self.config.claim_batch_size,
            )
            .await
            .map_err(AnnounceWorkerError::from)
    }

    pub async fn renew_lease(
        &self,
        id: &AnnounceWorkId,
        now_ms: i64,
    ) -> Result<bool, AnnounceWorkerError> {
        let lease_until_ms = now_ms.saturating_add(duration_ms(self.config.lease_duration));
        self.repository
            .renew_announce_lease(id, self.config.owner.as_str(), lease_until_ms, now_ms)
            .await
            .map_err(AnnounceWorkerError::from)
    }

    pub async fn complete(
        &self,
        id: &AnnounceWorkId,
        outcome: AnnounceWorkOutcome,
        now_ms: i64,
    ) -> Result<bool, AnnounceWorkerError> {
        let owner = self.config.owner.as_str();
        match outcome {
            AnnounceWorkOutcome::Succeeded { reason, outcome } => self
                .repository
                .mark_announce_succeeded(id, owner, reason, &outcome, now_ms)
                .await
                .map_err(AnnounceWorkerError::from),
            AnnounceWorkOutcome::Waiting {
                reason,
                next_attempt_at_ms,
                dependency,
            } => {
                let dependency = dependency
                    .as_ref()
                    .map(|(kind, name)| (kind.as_str(), name.as_str()));
                self.repository
                    .mark_announce_waiting(
                        id,
                        owner,
                        reason,
                        next_attempt_at_ms,
                        now_ms,
                        dependency,
                    )
                    .await
                    .map_err(AnnounceWorkerError::from)
            }
            AnnounceWorkOutcome::Retryable {
                reason,
                next_attempt_at_ms,
                error_class,
                redacted_message,
            } => self
                .repository
                .mark_announce_retryable(
                    id,
                    owner,
                    AnnounceRetryUpdate {
                        reason,
                        next_attempt_at_ms,
                        now_ms,
                        error_class: &error_class,
                        redacted_message: &redacted_message,
                    },
                )
                .await
                .map_err(AnnounceWorkerError::from),
            AnnounceWorkOutcome::TerminalFailed {
                reason,
                redacted_message,
            } => self
                .repository
                .mark_announce_terminal_failed(id, owner, reason, &redacted_message, now_ms)
                .await
                .map_err(AnnounceWorkerError::from),
            AnnounceWorkOutcome::Release {
                reason,
                next_attempt_at_ms,
            } => self
                .repository
                .release_announce_lease(id, owner, reason, next_attempt_at_ms, now_ms)
                .await
                .map_err(AnnounceWorkerError::from),
        }
    }

    pub async fn run_batch<F, Fut>(
        &self,
        now_ms: i64,
        mut shutdown: ShutdownSignal,
        mut process: F,
    ) -> Result<AnnounceWorkerSummary, AnnounceWorkerError>
    where
        F: FnMut(AnnounceWorkId) -> Fut,
        Fut: Future<Output = AnnounceWorkOutcome>,
    {
        let _span = info_span!(
            "announce.worker_batch",
            lease_owner = %self.config.owner,
            claim_batch_size = self.config.claim_batch_size
        );
        let claimed = self.claim_ready(now_ms).await?;
        let mut summary = AnnounceWorkerSummary {
            claimed: claimed.len(),
            ..AnnounceWorkerSummary::default()
        };

        for id in claimed {
            let _work_span = debug_span!("announce.process", announce_id = %id);
            if shutdown.state().phase != ShutdownPhase::Running {
                self.release_for_shutdown(&id, now_ms).await?;
                summary.cancelled += 1;
                continue;
            }

            tokio::select! {
                outcome = process(id.clone()) => {
                    if self.complete(&id, outcome, now_ms).await? {
                        summary.completed += 1;
                    } else {
                        summary.released += 1;
                    }
                }
                _state = shutdown.cancelled() => {
                    self.release_for_shutdown(&id, now_ms).await?;
                    summary.cancelled += 1;
                }
            }
        }

        Ok(summary)
    }

    async fn release_for_shutdown(
        &self,
        id: &AnnounceWorkId,
        now_ms: i64,
    ) -> Result<bool, AnnounceWorkerError> {
        self.repository
            .release_announce_lease(
                id,
                self.config.owner.as_str(),
                AnnounceReason::DependencyBackoff,
                now_ms,
                now_ms,
            )
            .await
            .map_err(AnnounceWorkerError::from)
    }
}

pub fn unix_time_ms() -> i64 {
    let duration = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration,
        Err(_error) => return 0,
    };
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

fn duration_ms(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

impl From<DatabaseError> for AnnounceWorkerError {
    fn from(source: DatabaseError) -> Self {
        Self::Database { source }
    }
}

impl fmt::Display for AnnounceWorkerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig { message } => {
                write!(formatter, "invalid announce worker config: {message}")
            }
            Self::Database { source } => write!(formatter, "{source}"),
        }
    }
}

impl std::error::Error for AnnounceWorkerError {}

#[cfg(test)]
mod tests {
    use sqlx::Row;

    use super::*;
    use crate::announce::{AnnounceDedupeIdentity, AnnounceStatus, AnnounceWorkItem};
    use crate::domain::{
        ByteSize, CandidateGuid, DependencyName, DependencyState, ItemTitle, TrackerName,
    };
    use crate::persistence::repository::AnnounceInsertResult;
    use crate::runtime::shutdown::shutdown_channel;

    #[tokio::test]
    async fn worker_claims_and_completes_bounded_batches() {
        let repository = Repository::connect_in_memory().await.unwrap();
        insert_work(&repository, "ann_10", "guid-10", 1).await;
        insert_work(&repository, "ann_11", "guid-11", 2).await;
        insert_work(&repository, "ann_12", "guid-12", 3).await;
        let worker = AnnounceWorker::new(repository.clone(), "worker-1", &test_config()).unwrap();
        let (_controller, signal) = shutdown_channel();

        let summary = worker
            .run_batch(10, signal, |id| async move {
                AnnounceWorkOutcome::Succeeded {
                    reason: AnnounceReason::Saved,
                    outcome: id.as_str().to_owned(),
                }
            })
            .await
            .unwrap();

        assert_eq!(
            AnnounceWorkerSummary {
                claimed: 2,
                completed: 2,
                released: 0,
                cancelled: 0
            },
            summary
        );
        assert_eq!(
            vec![
                ("succeeded".to_owned(), "saved".to_owned()),
                ("succeeded".to_owned(), "saved".to_owned()),
                ("queued".to_owned(), "accepted".to_owned())
            ],
            status_rows(&repository).await
        );
    }

    #[tokio::test]
    async fn worker_recovers_expired_and_stale_running_work_on_startup() {
        let repository = Repository::connect_in_memory().await.unwrap();
        insert_work(&repository, "ann_20", "guid-20", 1).await;
        insert_work(&repository, "ann_21", "guid-21", 1).await;
        repository
            .claim_announce_work("old-worker", 2, 3, 1)
            .await
            .unwrap();
        let worker = AnnounceWorker::new(repository.clone(), "worker-1", &test_config()).unwrap();

        let summary = worker.recover_startup(4).await.unwrap();

        assert_eq!(
            AnnounceStartupSummary {
                expired: 0,
                recovered_leases: 1
            },
            summary
        );
        assert_eq!(
            vec![
                ("queued".to_owned(), "dependency_backoff".to_owned()),
                ("queued".to_owned(), "accepted".to_owned())
            ],
            status_rows(&repository).await
        );
    }

    #[tokio::test]
    async fn worker_renews_active_leases() {
        let repository = Repository::connect_in_memory().await.unwrap();
        insert_work(&repository, "ann_30", "guid-30", 1).await;
        let worker = AnnounceWorker::new(repository.clone(), "worker-1", &test_config()).unwrap();
        let claimed = worker.claim_ready(10).await.unwrap();

        assert!(worker.renew_lease(&claimed[0], 15).await.unwrap());

        let lease_until: i64 =
            sqlx::query_scalar("SELECT lease_until FROM announce_work WHERE id = 'ann_30'")
                .fetch_one(repository.pool())
                .await
                .unwrap();
        assert_eq!(10_015, lease_until);
    }

    #[tokio::test]
    async fn worker_honors_dependency_retry_after_before_claiming() {
        let repository = Repository::connect_in_memory().await.unwrap();
        insert_work(&repository, "ann_35", "guid-35", 1).await;
        let worker = AnnounceWorker::new(repository.clone(), "worker-1", &test_config()).unwrap();
        let claimed = worker.claim_ready(10).await.unwrap();

        assert!(
            worker
                .complete(
                    &claimed[0],
                    AnnounceWorkOutcome::Waiting {
                        reason: AnnounceReason::DependencyBackoff,
                        next_attempt_at_ms: 20,
                        dependency: Some(("indexer".to_owned(), "main".to_owned())),
                    },
                    10,
                )
                .await
                .unwrap()
        );
        repository
            .record_dependency_health(
                "indexer",
                &DependencyName::new("main").unwrap(),
                &DependencyState::Unavailable {
                    reason: ReasonText::new("rate limited").unwrap(),
                    retry_after_ms: Some(100),
                },
                50,
            )
            .await
            .unwrap();

        let early = worker.claim_ready(50).await.unwrap();
        let ready = worker.claim_ready(100).await.unwrap();

        assert!(early.is_empty());
        assert_eq!(vec![AnnounceWorkId::new("ann_35").unwrap()], ready);
        assert_eq!(
            vec![(
                "running".to_owned(),
                "accepted".to_owned(),
                Some("indexer".to_owned()),
                Some("main".to_owned())
            )],
            dependency_rows(&repository).await
        );
    }

    #[tokio::test]
    async fn worker_shutdown_releases_claimed_work() {
        let repository = Repository::connect_in_memory().await.unwrap();
        insert_work(&repository, "ann_40", "guid-40", 1).await;
        insert_work(&repository, "ann_41", "guid-41", 2).await;
        let worker = AnnounceWorker::new(repository.clone(), "worker-1", &test_config()).unwrap();
        let (controller, signal) = shutdown_channel();

        let summary = worker
            .run_batch(10, signal, move |_id| {
                let controller = controller.clone();
                async move {
                    controller.cancel_now("test shutdown").unwrap();
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    AnnounceWorkOutcome::TerminalFailed {
                        reason: AnnounceReason::InvalidRequest,
                        redacted_message: "should not complete".to_owned(),
                    }
                }
            })
            .await
            .unwrap();

        assert_eq!(
            AnnounceWorkerSummary {
                claimed: 2,
                completed: 0,
                released: 0,
                cancelled: 2
            },
            summary
        );
        assert_eq!(
            vec![
                ("queued".to_owned(), "dependency_backoff".to_owned()),
                ("queued".to_owned(), "dependency_backoff".to_owned())
            ],
            status_rows(&repository).await
        );
        assert_eq!(0, leased_count(&repository).await);
    }

    fn test_config() -> AnnounceQueueConfig {
        AnnounceQueueConfig {
            claim_batch_size: 2,
            lease_duration_secs: 10,
            lease_renewal_secs: 5,
            default_ttl_secs: 100,
            retry_initial_delay_secs: 5,
            retry_max_delay_secs: 20,
            ..AnnounceQueueConfig::default()
        }
    }

    async fn insert_work(repository: &Repository, id: &str, guid: &str, received_at_ms: i64) {
        let work = test_work(id, guid, received_at_ms);
        let result = repository
            .insert_or_dedupe_announce_work(&work, 10)
            .await
            .unwrap();
        assert_eq!(
            AnnounceInsertResult::Inserted {
                id: work.id.clone()
            },
            result
        );
    }

    fn test_work(id: &str, guid: &str, received_at_ms: i64) -> AnnounceWorkItem {
        let tracker = TrackerName::new("tracker.example").unwrap();
        let guid = CandidateGuid::new(guid).unwrap();
        AnnounceWorkItem {
            id: AnnounceWorkId::new(id).unwrap(),
            status: AnnounceStatus::Queued,
            reason: AnnounceReason::Accepted,
            dedupe_hash: AnnounceDedupeIdentity::Guid {
                tracker: tracker.clone(),
                guid: guid.clone(),
            }
            .hash(),
            title: ItemTitle::new("Example").unwrap(),
            tracker,
            guid: Some(guid),
            info_hash: None,
            size: Some(ByteSize::new(42)),
            received_at_ms,
            updated_at_ms: received_at_ms,
            first_attempt_at_ms: None,
            finished_at_ms: None,
            attempt_count: 0,
            next_attempt_at_ms: received_at_ms,
            expires_at_ms: received_at_ms.saturating_add(100),
            lease: None,
            last_dependency_kind: None,
            last_dependency_name: None,
            last_error_class: None,
            last_redacted_message: None,
        }
    }

    async fn status_rows(repository: &Repository) -> Vec<(String, String)> {
        sqlx::query("SELECT status, reason FROM announce_work ORDER BY id")
            .fetch_all(repository.pool())
            .await
            .unwrap()
            .into_iter()
            .map(|row| (row.get("status"), row.get("reason")))
            .collect()
    }

    async fn dependency_rows(
        repository: &Repository,
    ) -> Vec<(String, String, Option<String>, Option<String>)> {
        sqlx::query(
            "SELECT status, reason, last_dependency_kind, last_dependency_name FROM announce_work ORDER BY id",
        )
        .fetch_all(repository.pool())
        .await
        .unwrap()
        .into_iter()
        .map(|row| {
            (
                row.get("status"),
                row.get("reason"),
                row.get("last_dependency_kind"),
                row.get("last_dependency_name"),
            )
        })
        .collect()
    }

    async fn leased_count(repository: &Repository) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM announce_work WHERE lease_owner IS NOT NULL")
            .fetch_one(repository.pool())
            .await
            .unwrap()
    }
}
