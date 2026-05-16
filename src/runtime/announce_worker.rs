use std::fmt;
use std::future::Future;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug_span, info_span};

use crate::announce::{AnnounceQueueConfig, AnnounceReason, AnnounceWorkId};
use crate::domain::{DecisionReason, InjectionOutcome, MatchDecision, ReasonText};
use crate::errors::{ClassifyFailure, DatabaseError, FailureClass, WorkerError};
use crate::matching::{PersistedCandidateAssessment, ReverseLookupOutcome};
use crate::persistence::repository::{AnnounceRetryUpdate, Repository};
use crate::runtime::injection_worker::InjectionWorkResult;
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

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct AnnounceOutcomeConfig {
    pub source_wait_ms: i64,
    pub candidate_download_wait_ms: i64,
    pub retry_delay_ms: i64,
}

impl Default for AnnounceOutcomeConfig {
    fn default() -> Self {
        Self {
            source_wait_ms: 5 * 60 * 1_000,
            candidate_download_wait_ms: 30 * 1_000,
            retry_delay_ms: 60 * 1_000,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum AnnounceWorkflowResult {
    Saved,
    Injected,
    AlreadyExists,
    SourceIncomplete {
        dependency: Option<(String, String)>,
    },
    CandidateDownloading,
    DependencyBackoff {
        dependency_kind: String,
        dependency_name: String,
        retry_after_ms: Option<i64>,
    },
    NoMatch,
    RetryableDependency {
        retry_after_ms: Option<i64>,
        error_class: String,
        redacted_message: String,
    },
    TerminalFailure {
        reason: AnnounceReason,
        redacted_message: String,
    },
    Expired,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum AnnounceWorkerError {
    InvalidConfig { message: String },
    Database { source: DatabaseError },
}

pub fn classify_announce_result(
    result: AnnounceWorkflowResult,
    now_ms: i64,
    config: AnnounceOutcomeConfig,
) -> AnnounceWorkOutcome {
    match result {
        AnnounceWorkflowResult::Saved => AnnounceWorkOutcome::Succeeded {
            reason: AnnounceReason::Saved,
            outcome: "saved".to_owned(),
        },
        AnnounceWorkflowResult::Injected => AnnounceWorkOutcome::Succeeded {
            reason: AnnounceReason::Injected,
            outcome: "injected".to_owned(),
        },
        AnnounceWorkflowResult::AlreadyExists => AnnounceWorkOutcome::Succeeded {
            reason: AnnounceReason::AlreadyExists,
            outcome: "already_exists".to_owned(),
        },
        AnnounceWorkflowResult::SourceIncomplete { dependency } => AnnounceWorkOutcome::Waiting {
            reason: AnnounceReason::SourceIncomplete,
            next_attempt_at_ms: now_ms.saturating_add(config.source_wait_ms.max(1)),
            dependency,
        },
        AnnounceWorkflowResult::CandidateDownloading => AnnounceWorkOutcome::Waiting {
            reason: AnnounceReason::CandidateDownloading,
            next_attempt_at_ms: now_ms.saturating_add(config.candidate_download_wait_ms.max(1)),
            dependency: None,
        },
        AnnounceWorkflowResult::DependencyBackoff {
            dependency_kind,
            dependency_name,
            retry_after_ms,
        } => AnnounceWorkOutcome::Waiting {
            reason: retry_after_ms.map_or(AnnounceReason::DependencyBackoff, |_| {
                AnnounceReason::RetryAfter
            }),
            next_attempt_at_ms: retry_after_ms
                .filter(|retry_after| *retry_after > now_ms)
                .unwrap_or_else(|| now_ms.saturating_add(config.retry_delay_ms.max(1))),
            dependency: Some((dependency_kind, dependency_name)),
        },
        AnnounceWorkflowResult::NoMatch => AnnounceWorkOutcome::Waiting {
            reason: AnnounceReason::InventoryRefreshing,
            next_attempt_at_ms: now_ms.saturating_add(config.source_wait_ms.max(1)),
            dependency: None,
        },
        AnnounceWorkflowResult::RetryableDependency {
            retry_after_ms,
            error_class,
            redacted_message,
        } => AnnounceWorkOutcome::Retryable {
            reason: retry_after_ms.map_or(AnnounceReason::TransientDependencyFailure, |_| {
                AnnounceReason::RetryAfter
            }),
            next_attempt_at_ms: retry_after_ms
                .filter(|retry_after| *retry_after > now_ms)
                .unwrap_or_else(|| now_ms.saturating_add(config.retry_delay_ms.max(1))),
            error_class,
            redacted_message,
        },
        AnnounceWorkflowResult::TerminalFailure {
            reason,
            redacted_message,
        } => AnnounceWorkOutcome::TerminalFailed {
            reason,
            redacted_message,
        },
        AnnounceWorkflowResult::Expired => AnnounceWorkOutcome::TerminalFailed {
            reason: AnnounceReason::Expired,
            redacted_message: "announce work expired".to_owned(),
        },
    }
}

pub fn classify_injection_result(
    result: &InjectionWorkResult,
    now_ms: i64,
    config: AnnounceOutcomeConfig,
) -> AnnounceWorkOutcome {
    let workflow = match result.outcome {
        InjectionOutcome::Injected => AnnounceWorkflowResult::Injected,
        InjectionOutcome::Saved => AnnounceWorkflowResult::Saved,
        InjectionOutcome::AlreadyExists => AnnounceWorkflowResult::AlreadyExists,
        InjectionOutcome::SourceIncomplete => AnnounceWorkflowResult::SourceIncomplete {
            dependency: result
                .target_client
                .as_ref()
                .map(|name| ("client".to_owned(), name.as_str().to_owned())),
        },
        InjectionOutcome::Failed if result.saved_for_retry => {
            AnnounceWorkflowResult::DependencyBackoff {
                dependency_kind: "client".to_owned(),
                dependency_name: result
                    .target_client
                    .as_ref()
                    .map_or_else(|| "unknown".to_owned(), ToString::to_string),
                retry_after_ms: None,
            }
        }
        InjectionOutcome::Failed => AnnounceWorkflowResult::RetryableDependency {
            retry_after_ms: None,
            error_class: "torrent_client".to_owned(),
            redacted_message: "torrent client injection failed before side effects".to_owned(),
        },
    };
    classify_announce_result(workflow, now_ms, config)
}

pub fn classify_reverse_lookup_outcome(
    outcome: &ReverseLookupOutcome,
    now_ms: i64,
    config: AnnounceOutcomeConfig,
) -> AnnounceWorkOutcome {
    let workflow = match outcome {
        ReverseLookupOutcome::AlreadyPresent { .. } => AnnounceWorkflowResult::AlreadyExists,
        ReverseLookupOutcome::NeedsTorrentDownload { .. } => {
            AnnounceWorkflowResult::CandidateDownloading
        }
        ReverseLookupOutcome::NoCandidates => AnnounceWorkflowResult::NoMatch,
        ReverseLookupOutcome::BestFailure { assessment, .. } => {
            classify_assessment_failure(assessment)
        }
        ReverseLookupOutcome::Matched { assessment, .. } => classify_matched_assessment(assessment),
    };
    classify_announce_result(workflow, now_ms, config)
}

pub fn classify_worker_error(
    error: &WorkerError,
    now_ms: i64,
    config: AnnounceOutcomeConfig,
) -> AnnounceWorkOutcome {
    let workflow = match error.failure_class() {
        FailureClass::RetryableDependency => AnnounceWorkflowResult::RetryableDependency {
            retry_after_ms: error.retry_after_ms(),
            error_class: "retryable_dependency".to_owned(),
            redacted_message: error.to_string(),
        },
        FailureClass::BadRemoteData => AnnounceWorkflowResult::TerminalFailure {
            reason: AnnounceReason::InvalidTorrentMetadata,
            redacted_message: error.to_string(),
        },
        FailureClass::UserActionRequired => AnnounceWorkflowResult::TerminalFailure {
            reason: AnnounceReason::UnsupportedShape,
            redacted_message: error.to_string(),
        },
        FailureClass::FatalLocal => AnnounceWorkflowResult::TerminalFailure {
            reason: AnnounceReason::InvalidRequest,
            redacted_message: error.to_string(),
        },
    };
    classify_announce_result(workflow, now_ms, config)
}

fn classify_matched_assessment(
    assessment: &PersistedCandidateAssessment,
) -> AnnounceWorkflowResult {
    let Some(assessment) = persisted_assessment(assessment) else {
        return AnnounceWorkflowResult::CandidateDownloading;
    };
    if matches!(
        assessment.decision,
        MatchDecision::Exact | MatchDecision::SizeOnly | MatchDecision::Partial
    ) {
        AnnounceWorkflowResult::Saved
    } else {
        classify_decision_failure(assessment)
    }
}

fn classify_assessment_failure(
    assessment: &PersistedCandidateAssessment,
) -> AnnounceWorkflowResult {
    let Some(assessment) = persisted_assessment(assessment) else {
        return AnnounceWorkflowResult::CandidateDownloading;
    };
    classify_decision_failure(assessment)
}

fn classify_decision_failure(
    assessment: &crate::domain::CandidateAssessment,
) -> AnnounceWorkflowResult {
    match assessment.reason {
        DecisionReason::AlreadyExists
        | DecisionReason::SameInfoHash
        | DecisionReason::InfoHashAlreadyExists => AnnounceWorkflowResult::AlreadyExists,
        DecisionReason::SourceIncomplete => {
            AnnounceWorkflowResult::SourceIncomplete { dependency: None }
        }
        DecisionReason::BlockedRelease
        | DecisionReason::ReleaseGroupMismatch
        | DecisionReason::ResolutionMismatch
        | DecisionReason::SourceMismatch
        | DecisionReason::ProperRepackMismatch
        | DecisionReason::FuzzySizeMismatch
        | DecisionReason::MissingDownloadLink
        | DecisionReason::SingleEpisodeForSeasonPack
        | DecisionReason::CandidateInvalid
        | DecisionReason::PolicyRejected
        | DecisionReason::UnsupportedLayout => AnnounceWorkflowResult::TerminalFailure {
            reason: terminal_reason_for_decision(assessment.reason),
            redacted_message: format!("candidate rejected: {:?}", assessment.reason),
        },
        DecisionReason::FileTreeMatched
        | DecisionReason::SizeMatched
        | DecisionReason::PartialOverlap => AnnounceWorkflowResult::Saved,
        DecisionReason::NameMismatch => AnnounceWorkflowResult::NoMatch,
    }
}

fn persisted_assessment(
    assessment: &PersistedCandidateAssessment,
) -> Option<&crate::domain::CandidateAssessment> {
    match assessment {
        PersistedCandidateAssessment::Assessed { assessment, .. }
        | PersistedCandidateAssessment::Rejected { assessment, .. } => Some(assessment),
        PersistedCandidateAssessment::NeedsTorrentDownload { .. } => None,
    }
}

fn terminal_reason_for_decision(reason: DecisionReason) -> AnnounceReason {
    match reason {
        DecisionReason::BlockedRelease => AnnounceReason::InvalidRequest,
        DecisionReason::MissingDownloadLink => AnnounceReason::InvalidRequest,
        DecisionReason::AlreadyExists
        | DecisionReason::SameInfoHash
        | DecisionReason::InfoHashAlreadyExists => AnnounceReason::AlreadyExists,
        DecisionReason::FileTreeMatched
        | DecisionReason::SizeMatched
        | DecisionReason::PartialOverlap
        | DecisionReason::SourceIncomplete
        | DecisionReason::NameMismatch => AnnounceReason::NoMatchTerminal,
        DecisionReason::ReleaseGroupMismatch
        | DecisionReason::ResolutionMismatch
        | DecisionReason::SourceMismatch
        | DecisionReason::ProperRepackMismatch
        | DecisionReason::FuzzySizeMismatch
        | DecisionReason::SingleEpisodeForSeasonPack
        | DecisionReason::CandidateInvalid
        | DecisionReason::PolicyRejected
        | DecisionReason::UnsupportedLayout => AnnounceReason::NoMatchTerminal,
    }
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
        self.repository.expire_announce_work(now_ms).await?;
        self.repository
            .recover_stale_announce_leases(now_ms)
            .await?;
        self.repository
            .schedule_announce_dependency_backoff(
                now_ms,
                duration_ms(self.config.dependency_recovery_probe_interval),
                reconcile_limit,
            )
            .await?;
        self.repository
            .wake_due_waiting_announce_work(now_ms, reconcile_limit)
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
        shutdown: ShutdownSignal,
        mut process: F,
    ) -> Result<AnnounceWorkerSummary, AnnounceWorkerError>
    where
        F: FnMut(AnnounceWorkId, ShutdownSignal) -> Fut,
        Fut: Future<Output = AnnounceWorkOutcome>,
    {
        // The processing future owns side effects for this work item. Once it
        // starts, keep its lease alive until it records a durable outcome;
        // dropping it on shutdown can duplicate remote side effects.
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
                self.release_for_shutdown(&id, unix_time_ms()).await?;
                summary.cancelled += 1;
                continue;
            }
            if !self.renew_lease(&id, unix_time_ms()).await? {
                summary.released += 1;
                continue;
            }

            let processing = process(id.clone(), shutdown.clone());
            tokio::pin!(processing);
            let renewal = tokio::time::sleep(self.config.lease_renewal);
            tokio::pin!(renewal);

            loop {
                tokio::select! {
                    outcome = &mut processing => {
                        if self.complete(&id, outcome, unix_time_ms()).await? {
                            summary.completed += 1;
                        } else {
                            summary.released += 1;
                        }
                        break;
                    }
                    () = &mut renewal => {
                        if self.renew_lease(&id, unix_time_ms()).await? {
                            renewal.as_mut().reset(tokio::time::Instant::now() + self.config.lease_renewal);
                        } else {
                            summary.released += 1;
                            break;
                        }
                    }
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
        ByteSize, CandidateAssessment, CandidateGuid, DependencyName, DependencyState,
        InjectionOutcome, ItemTitle, LocalItem, LocalItemSource, MatchDecision, MatchRatio,
        MediaType, SourceKey, TrackerName,
    };
    use crate::matching::{CandidateCacheStatus, PersistedCandidateAssessment};
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
            .run_batch(10, signal, |id, _shutdown| async move {
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
    async fn worker_resumes_durable_work_after_repository_restart() {
        let root = unique_temp_dir("announce-restart");
        let database = root.join("sporos.db");
        let repository = Repository::connect(&database).await.unwrap();
        insert_work(&repository, "ann_22", "guid-22", 1).await;
        repository
            .claim_announce_work("old-worker", 2, 3, 1)
            .await
            .unwrap();
        drop(repository);

        let restarted = Repository::connect(&database).await.unwrap();
        let worker = AnnounceWorker::new(restarted.clone(), "worker-1", &test_config()).unwrap();

        let startup = worker.recover_startup(4).await.unwrap();
        let claimed = worker.claim_ready(4).await.unwrap();
        assert!(
            worker
                .complete(
                    &claimed[0],
                    AnnounceWorkOutcome::Succeeded {
                        reason: AnnounceReason::Saved,
                        outcome: "saved".to_owned(),
                    },
                    5,
                )
                .await
                .unwrap()
        );

        assert_eq!(
            AnnounceStartupSummary {
                expired: 0,
                recovered_leases: 1,
            },
            startup
        );
        assert_eq!(vec![AnnounceWorkId::new("ann_22").unwrap()], claimed);
        assert_eq!(
            vec![("succeeded".to_owned(), "saved".to_owned())],
            status_rows(&restarted).await
        );

        drop(worker);
        drop(restarted);
        std::fs::remove_dir_all(root).unwrap();
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
    async fn worker_recovers_expired_running_work_during_claims() {
        let repository = Repository::connect_in_memory().await.unwrap();
        insert_work(&repository, "ann_31", "guid-31", 1).await;
        let first = repository
            .claim_announce_work("old-worker", 2, 20, 1)
            .await
            .unwrap();
        let worker = AnnounceWorker::new(repository.clone(), "worker-1", &test_config()).unwrap();

        let early = worker.claim_ready(19).await.unwrap();
        let recovered = worker.claim_ready(20).await.unwrap();

        assert_eq!(vec![AnnounceWorkId::new("ann_31").unwrap()], first);
        assert!(early.is_empty());
        assert_eq!(vec![AnnounceWorkId::new("ann_31").unwrap()], recovered);

        let attempt_count: i64 =
            sqlx::query_scalar("SELECT attempt_count FROM announce_work WHERE id = 'ann_31'")
                .fetch_one(repository.pool())
                .await
                .unwrap();
        assert_eq!(2, attempt_count);
    }

    #[tokio::test]
    async fn worker_expires_ttl_dead_running_work_before_recovery() {
        let repository = Repository::connect_in_memory().await.unwrap();
        insert_work(&repository, "ann_32", "guid-32", 1).await;
        repository
            .claim_announce_work("old-worker", 2, 20, 1)
            .await
            .unwrap();
        let worker = AnnounceWorker::new(repository.clone(), "worker-1", &test_config()).unwrap();

        let claimed = worker.claim_ready(200).await.unwrap();

        assert!(claimed.is_empty());
        assert_eq!(
            vec![("expired".to_owned(), "expired".to_owned())],
            status_rows(&repository).await
        );
    }

    #[tokio::test]
    async fn worker_renews_long_running_batch_work() {
        let repository = Repository::connect_in_memory().await.unwrap();
        insert_work(&repository, "ann_33", "guid-33", 1).await;
        let ttl_expires_at = unix_time_ms().saturating_add(60_000);
        sqlx::query("UPDATE announce_work SET expires_at = ? WHERE id = 'ann_33'")
            .bind(ttl_expires_at)
            .execute(repository.pool())
            .await
            .unwrap();
        let config = AnnounceQueueConfig {
            claim_batch_size: 1,
            lease_duration_secs: 2,
            lease_renewal_secs: 1,
            ..test_config()
        };
        let first_worker = AnnounceWorker::new(repository.clone(), "worker-1", &config).unwrap();
        let second_worker = AnnounceWorker::new(repository.clone(), "worker-2", &config).unwrap();
        let (_controller, signal) = shutdown_channel();

        let handle = tokio::spawn(async move {
            first_worker
                .run_batch(10, signal, |_id, _shutdown| async move {
                    tokio::time::sleep(Duration::from_millis(2_500)).await;
                    AnnounceWorkOutcome::Succeeded {
                        reason: AnnounceReason::Saved,
                        outcome: "saved".to_owned(),
                    }
                })
                .await
                .unwrap()
        });

        tokio::time::sleep(Duration::from_millis(2_200)).await;
        let duplicate = second_worker.claim_ready(unix_time_ms()).await.unwrap();
        let summary = handle.await.unwrap();

        assert!(duplicate.is_empty());
        assert_eq!(
            AnnounceWorkerSummary {
                claimed: 1,
                completed: 1,
                released: 0,
                cancelled: 0
            },
            summary
        );
        assert_eq!(
            vec![("succeeded".to_owned(), "saved".to_owned())],
            status_rows(&repository).await
        );
        let row =
            sqlx::query("SELECT updated_at, finished_at FROM announce_work WHERE id = 'ann_33'")
                .fetch_one(repository.pool())
                .await
                .unwrap();
        let updated_at: i64 = row.get("updated_at");
        let finished_at: i64 = row.get("finished_at");
        assert!(finished_at >= updated_at);
    }

    #[tokio::test]
    async fn worker_skips_claimed_work_lost_before_processing() {
        let repository = Repository::connect_in_memory().await.unwrap();
        insert_work(&repository, "ann_34", "guid-34", 1).await;
        insert_work(&repository, "ann_35", "guid-35", 2).await;
        sqlx::query(
            "UPDATE announce_work SET expires_at = 10_000 WHERE id IN ('ann_34', 'ann_35')",
        )
        .execute(repository.pool())
        .await
        .unwrap();
        let config = AnnounceQueueConfig {
            claim_batch_size: 2,
            lease_duration_secs: 2,
            lease_renewal_secs: 1,
            ..test_config()
        };
        let first_worker = AnnounceWorker::new(repository.clone(), "worker-1", &config).unwrap();
        let second_worker = AnnounceWorker::new(repository.clone(), "worker-2", &config).unwrap();
        let (_controller, signal) = shutdown_channel();
        let processed = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let processed_by_first = processed.clone();

        let handle = tokio::spawn(async move {
            first_worker
                .run_batch(10, signal, move |id, _shutdown| {
                    let processed = processed_by_first.clone();
                    async move {
                        processed.lock().unwrap().push(id.as_str().to_owned());
                        if id.as_str() == "ann_34" {
                            tokio::time::sleep(Duration::from_millis(2_500)).await;
                        }
                        AnnounceWorkOutcome::Succeeded {
                            reason: AnnounceReason::Saved,
                            outcome: "saved".to_owned(),
                        }
                    }
                })
                .await
                .unwrap()
        });

        tokio::time::sleep(Duration::from_millis(2_200)).await;
        let reclaimed = second_worker.claim_ready(2_020).await.unwrap();
        let summary = handle.await.unwrap();

        assert_eq!(vec![AnnounceWorkId::new("ann_35").unwrap()], reclaimed);
        assert_eq!(
            AnnounceWorkerSummary {
                claimed: 2,
                completed: 1,
                released: 1,
                cancelled: 0
            },
            summary
        );
        assert_eq!(vec!["ann_34".to_owned()], *processed.lock().unwrap());
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
    async fn worker_wakes_due_waiting_work_before_claiming() {
        let repository = Repository::connect_in_memory().await.unwrap();
        insert_work(&repository, "ann_37", "guid-37", 1).await;
        let worker = AnnounceWorker::new(repository.clone(), "worker-1", &test_config()).unwrap();
        let claimed = worker.claim_ready(10).await.unwrap();

        assert!(
            worker
                .complete(
                    &claimed[0],
                    AnnounceWorkOutcome::Waiting {
                        reason: AnnounceReason::SourceIncomplete,
                        next_attempt_at_ms: 20,
                        dependency: None,
                    },
                    10,
                )
                .await
                .unwrap()
        );

        let early = worker.claim_ready(19).await.unwrap();
        let ready = worker.claim_ready(20).await.unwrap();

        assert!(early.is_empty());
        assert_eq!(vec![AnnounceWorkId::new("ann_37").unwrap()], ready);
    }

    #[test]
    fn classifier_maps_workflow_results_to_queue_transitions() {
        let config = AnnounceOutcomeConfig {
            source_wait_ms: 10,
            candidate_download_wait_ms: 20,
            retry_delay_ms: 30,
        };

        assert_eq!(
            AnnounceWorkOutcome::Succeeded {
                reason: AnnounceReason::Injected,
                outcome: "injected".to_owned(),
            },
            classify_announce_result(AnnounceWorkflowResult::Injected, 100, config)
        );
        assert_eq!(
            AnnounceWorkOutcome::Waiting {
                reason: AnnounceReason::SourceIncomplete,
                next_attempt_at_ms: 110,
                dependency: Some(("client".to_owned(), "qbit.local".to_owned())),
            },
            classify_announce_result(
                AnnounceWorkflowResult::SourceIncomplete {
                    dependency: Some(("client".to_owned(), "qbit.local".to_owned())),
                },
                100,
                config,
            )
        );
        assert_eq!(
            AnnounceWorkOutcome::Waiting {
                reason: AnnounceReason::RetryAfter,
                next_attempt_at_ms: 500,
                dependency: Some(("indexer".to_owned(), "main".to_owned())),
            },
            classify_announce_result(
                AnnounceWorkflowResult::DependencyBackoff {
                    dependency_kind: "indexer".to_owned(),
                    dependency_name: "main".to_owned(),
                    retry_after_ms: Some(500),
                },
                100,
                config,
            )
        );
        assert_eq!(
            AnnounceWorkOutcome::Retryable {
                reason: AnnounceReason::TransientDependencyFailure,
                next_attempt_at_ms: 130,
                error_class: "indexer".to_owned(),
                redacted_message: "timeout".to_owned(),
            },
            classify_announce_result(
                AnnounceWorkflowResult::RetryableDependency {
                    retry_after_ms: None,
                    error_class: "indexer".to_owned(),
                    redacted_message: "timeout".to_owned(),
                },
                100,
                config,
            )
        );
        assert_eq!(
            AnnounceWorkOutcome::Waiting {
                reason: AnnounceReason::InventoryRefreshing,
                next_attempt_at_ms: 110,
                dependency: None,
            },
            classify_announce_result(AnnounceWorkflowResult::NoMatch, 100, config)
        );
    }

    #[test]
    fn classifier_maps_reverse_lookup_and_injection_results() {
        let config = AnnounceOutcomeConfig {
            source_wait_ms: 10,
            candidate_download_wait_ms: 20,
            retry_delay_ms: 30,
        };
        let local_item = classified_local_item();
        let source_incomplete = ReverseLookupOutcome::BestFailure {
            local_item: local_item.clone(),
            assessment: persisted_assessment(
                MatchDecision::NoMatch,
                DecisionReason::SourceIncomplete,
            ),
        };
        let rejected = ReverseLookupOutcome::BestFailure {
            local_item: local_item.clone(),
            assessment: persisted_assessment(
                MatchDecision::Rejected,
                DecisionReason::BlockedRelease,
            ),
        };
        let injection = crate::runtime::injection_worker::InjectionWorkResult {
            outcome: InjectionOutcome::SourceIncomplete,
            target_client: Some(DependencyName::new("qbit.local").unwrap()),
            saved_for_retry: true,
            linked_files: 0,
        };

        assert_eq!(
            AnnounceWorkOutcome::Waiting {
                reason: AnnounceReason::SourceIncomplete,
                next_attempt_at_ms: 110,
                dependency: None,
            },
            classify_reverse_lookup_outcome(&source_incomplete, 100, config)
        );
        assert_eq!(
            AnnounceWorkOutcome::TerminalFailed {
                reason: AnnounceReason::InvalidRequest,
                redacted_message: "candidate rejected: BlockedRelease".to_owned(),
            },
            classify_reverse_lookup_outcome(&rejected, 100, config)
        );
        assert_eq!(
            AnnounceWorkOutcome::Waiting {
                reason: AnnounceReason::SourceIncomplete,
                next_attempt_at_ms: 110,
                dependency: Some(("client".to_owned(), "qbit.local".to_owned())),
            },
            classify_injection_result(&injection, 100, config)
        );

        let ambiguous_failure = crate::runtime::injection_worker::InjectionWorkResult {
            outcome: InjectionOutcome::Failed,
            target_client: Some(DependencyName::new("qbit.local").unwrap()),
            saved_for_retry: true,
            linked_files: 1,
        };

        assert_eq!(
            AnnounceWorkOutcome::Waiting {
                reason: AnnounceReason::DependencyBackoff,
                next_attempt_at_ms: 130,
                dependency: Some(("client".to_owned(), "qbit.local".to_owned())),
            },
            classify_injection_result(&ambiguous_failure, 100, config)
        );
    }

    #[tokio::test]
    async fn worker_shutdown_lets_in_flight_work_complete() {
        let repository = Repository::connect_in_memory().await.unwrap();
        insert_work(&repository, "ann_40", "guid-40", 1).await;
        insert_work(&repository, "ann_41", "guid-41", 2).await;
        let worker = AnnounceWorker::new(repository.clone(), "worker-1", &test_config()).unwrap();
        let (controller, signal) = shutdown_channel();

        let summary = worker
            .run_batch(10, signal, move |_id, mut shutdown| {
                let controller = controller.clone();
                async move {
                    controller.cancel_now("test shutdown").unwrap();
                    let state = shutdown.cancelled().await;
                    assert_eq!(ShutdownPhase::Cancelled, state.phase);
                    AnnounceWorkOutcome::Succeeded {
                        reason: AnnounceReason::Saved,
                        outcome: "saved".to_owned(),
                    }
                }
            })
            .await
            .unwrap();

        assert_eq!(
            AnnounceWorkerSummary {
                claimed: 2,
                completed: 1,
                released: 0,
                cancelled: 1
            },
            summary
        );
        assert_eq!(
            vec![
                ("succeeded".to_owned(), "saved".to_owned()),
                ("queued".to_owned(), "dependency_backoff".to_owned())
            ],
            status_rows(&repository).await
        );
        assert_eq!(0, leased_count(&repository).await);
    }

    #[tokio::test]
    async fn worker_shutdown_keeps_stalled_in_flight_work_leased() {
        let repository = Repository::connect_in_memory().await.unwrap();
        insert_work(&repository, "ann_42", "guid-42", 1).await;
        let config = AnnounceQueueConfig {
            claim_batch_size: 1,
            lease_duration_secs: 2,
            lease_renewal_secs: 1,
            ..test_config()
        };
        let worker = AnnounceWorker::new(repository.clone(), "worker-1", &config).unwrap();
        let second_worker = AnnounceWorker::new(repository.clone(), "worker-2", &config).unwrap();
        let (controller, signal) = shutdown_channel();
        let (started_sender, started_receiver) = tokio::sync::oneshot::channel();

        let handle = tokio::spawn(async move {
            let mut started_sender = Some(started_sender);
            worker
                .run_batch(10, signal, move |_id, mut shutdown| {
                    let controller = controller.clone();
                    let started_sender = started_sender.take();
                    async move {
                        controller.cancel_now("test shutdown").unwrap();
                        let _state = shutdown.cancelled().await;
                        if let Some(started_sender) = started_sender {
                            let _ = started_sender.send(());
                        }
                        std::future::pending().await
                    }
                })
                .await
        });

        started_receiver.await.unwrap();
        let lease_after_shutdown: i64 =
            sqlx::query_scalar("SELECT lease_until FROM announce_work WHERE id = 'ann_42'")
                .fetch_one(repository.pool())
                .await
                .unwrap();
        tokio::time::sleep(Duration::from_millis(2_200)).await;
        let lease_after_renewal_intervals: i64 =
            sqlx::query_scalar("SELECT lease_until FROM announce_work WHERE id = 'ann_42'")
                .fetch_one(repository.pool())
                .await
                .unwrap();
        let duplicate = second_worker.claim_ready(unix_time_ms()).await.unwrap();

        assert!(
            lease_after_renewal_intervals > lease_after_shutdown,
            "active in-flight work must keep its lease until it records an outcome"
        );
        assert!(duplicate.is_empty());
        assert!(!handle.is_finished());
        handle.abort();
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
            fetch: None,
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

    fn classified_local_item() -> LocalItem {
        LocalItem {
            id: None,
            source: LocalItemSource::Virtual {
                source_key: SourceKey::new("example").unwrap(),
            },
            title: ItemTitle::new("Example").unwrap(),
            display_name: crate::domain::DisplayName::new("Example").unwrap(),
            media_type: MediaType::Movie,
            info_hash: None,
            path: None,
            save_path: None,
            total_size: ByteSize::new(10),
            mtime_ms: None,
        }
    }

    fn persisted_assessment(
        decision: MatchDecision,
        reason: DecisionReason,
    ) -> PersistedCandidateAssessment {
        PersistedCandidateAssessment::Rejected {
            candidate_id: crate::domain::RemoteCandidateId::new(1).unwrap(),
            assessment: CandidateAssessment {
                decision,
                reason,
                matched_size: Some(ByteSize::new(5)),
                matched_ratio: MatchRatio::new(0.5).ok(),
            },
            cache_status: CandidateCacheStatus::Reused,
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

    fn unique_temp_dir(label: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        let path = std::env::temp_dir().join(format!("sporos-announce-test-{label}-{nanos}"));
        std::fs::create_dir_all(&path).unwrap();
        path
    }
}
