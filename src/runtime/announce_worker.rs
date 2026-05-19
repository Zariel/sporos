#![cfg_attr(
    test,
    expect(
        clippy::let_underscore_must_use,
        reason = "test synchronization sends are best-effort and tracked for cleanup"
    )
)]

use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug_span, info_span};

use crate::announce::{AnnounceQueueConfig, AnnounceReason, AnnounceWorkId};
use crate::domain::{DecisionReason, InjectionOutcome, MatchDecision, ReasonText};
use crate::errors::{ClassifyFailure, DatabaseError, FailureClass, WorkerError};
use crate::matching::{PersistedCandidateAssessment, ReverseLookupOutcome};
use crate::persistence::repository::{AnnounceRetryUpdate, Repository};
use crate::runtime::backoff::stable_jitter_ms;
use crate::runtime::injection_worker::InjectionWorkResult;
use crate::runtime::shutdown::{ShutdownPhase, ShutdownSignal};

#[derive(Debug, Clone)]
pub struct AnnounceWorker {
    repository: Repository,
    config: AnnounceWorkerConfig,
    retention_cleanup: AnnounceRetentionCleanup,
}

#[derive(Debug, Clone)]
pub struct AnnounceRetentionCleanup {
    last_cleanup_ms: Arc<AtomicI64>,
}

const RETENTION_CLEANUP_IN_PROGRESS_MS: i64 = i64::MAX;

impl Default for AnnounceRetentionCleanup {
    fn default() -> Self {
        Self {
            last_cleanup_ms: Arc::new(AtomicI64::new(i64::MIN)),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AnnounceWorkerConfig {
    pub owner: ReasonText,
    pub claim_batch_size: u16,
    pub lease_duration: Duration,
    pub lease_renewal: Duration,
    pub dependency_recovery_probe_interval: Duration,
    pub success_retention: Duration,
    pub failure_retention: Duration,
    pub retention_cleanup_interval: Duration,
    pub retention_cleanup_batch_size: u16,
    pub retention_cleanup_max_batches: u16,
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub struct AnnounceStartupSummary {
    pub expired: u64,
    pub recovered_leases: u64,
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub struct AnnounceMaintenanceSummary {
    pub expired: u64,
    pub retained_deleted: u64,
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

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AnnounceOutcomeConfig {
    pub source_wait_ms: i64,
    pub candidate_download_wait_ms: i64,
    pub retry_initial_delay_ms: i64,
    pub retry_max_delay_ms: i64,
    pub retry_jitter_ratio: f64,
}

impl Default for AnnounceOutcomeConfig {
    fn default() -> Self {
        Self {
            source_wait_ms: 5 * 60 * 1_000,
            candidate_download_wait_ms: 30 * 1_000,
            retry_initial_delay_ms: 60 * 1_000,
            retry_max_delay_ms: 60 * 1_000,
            retry_jitter_ratio: 0.0,
        }
    }
}

impl AnnounceOutcomeConfig {
    pub fn from_queue_config(config: &AnnounceQueueConfig) -> Self {
        Self {
            retry_initial_delay_ms: seconds_to_millis(config.retry_initial_delay_secs),
            retry_max_delay_ms: seconds_to_millis(config.retry_max_delay_secs),
            retry_jitter_ratio: config.retry_jitter_ratio,
            ..Self::default()
        }
    }

    pub fn retry_deadline_ms(
        self,
        now_ms: i64,
        attempt_count: u16,
        explicit_retry_after_ms: Option<i64>,
        jitter_key: &str,
    ) -> i64 {
        if let Some(retry_after_ms) =
            explicit_retry_after_ms.filter(|retry_after| *retry_after > now_ms)
        {
            return retry_after_ms;
        }
        now_ms.saturating_add(self.retry_delay_ms(attempt_count, jitter_key))
    }

    fn retry_delay_ms(self, attempt_count: u16, jitter_key: &str) -> i64 {
        let retry_index = attempt_count.saturating_sub(1);
        let shift = u32::from(retry_index).min(62);
        let multiplier = 1_i64.checked_shl(shift).unwrap_or(i64::MAX);
        let delay = self
            .retry_initial_delay_ms
            .max(1)
            .saturating_mul(multiplier)
            .min(self.retry_max_delay_ms.max(1));
        delay
            .saturating_add(stable_jitter_ms(
                jitter_key,
                retry_index,
                jitter_ms(delay, self.retry_jitter_ratio),
            ))
            .min(self.retry_max_delay_ms.max(1))
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
    Shutdown,
    Database { source: DatabaseError },
}

pub fn classify_announce_result(
    result: AnnounceWorkflowResult,
    now_ms: i64,
    attempt_count: u16,
    jitter_key: &str,
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
            next_attempt_at_ms: config.retry_deadline_ms(
                now_ms,
                attempt_count,
                retry_after_ms,
                jitter_key,
            ),
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
            next_attempt_at_ms: config.retry_deadline_ms(
                now_ms,
                attempt_count,
                retry_after_ms,
                jitter_key,
            ),
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
    attempt_count: u16,
    jitter_key: &str,
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
    classify_announce_result(workflow, now_ms, attempt_count, jitter_key, config)
}

pub fn classify_reverse_lookup_outcome(
    outcome: &ReverseLookupOutcome,
    now_ms: i64,
    attempt_count: u16,
    jitter_key: &str,
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
    classify_announce_result(workflow, now_ms, attempt_count, jitter_key, config)
}

pub fn classify_worker_error(
    error: &WorkerError,
    now_ms: i64,
    attempt_count: u16,
    jitter_key: &str,
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
    classify_announce_result(workflow, now_ms, attempt_count, jitter_key, config)
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
            success_retention: Duration::from_secs(queue_config.success_retention_secs),
            failure_retention: Duration::from_secs(queue_config.failure_retention_secs),
            retention_cleanup_interval: Duration::from_secs(60),
            retention_cleanup_batch_size: retention_cleanup_batch_size(queue_config),
            retention_cleanup_max_batches: 4,
        };

        Ok(Self {
            repository,
            config,
            retention_cleanup: AnnounceRetentionCleanup::default(),
        })
    }

    pub fn with_retention_cleanup(mut self, retention_cleanup: AnnounceRetentionCleanup) -> Self {
        self.retention_cleanup = retention_cleanup;
        self
    }

    pub fn retention_cleanup(&self) -> AnnounceRetentionCleanup {
        self.retention_cleanup.clone()
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
        self.cleanup_retained(now_ms, true).await?;
        let recovered_leases = self
            .repository
            .recover_stale_announce_leases(now_ms)
            .await?;

        Ok(AnnounceStartupSummary {
            expired,
            recovered_leases,
        })
    }

    pub async fn run_scheduled_cleanup(
        &self,
        now_ms: i64,
        shutdown: &ShutdownSignal,
    ) -> Result<AnnounceMaintenanceSummary, AnnounceWorkerError> {
        let _span = info_span!("announce.scheduled_cleanup", now_ms);
        let batch_size = self.config.retention_cleanup_batch_size;
        let max_batches = self.config.retention_cleanup_max_batches.max(1);
        ensure_cleanup_running(shutdown)?;
        let expired = self
            .run_batched_cleanup(shutdown, batch_size, max_batches, || {
                self.repository
                    .expire_announce_work_batch(now_ms, batch_size)
            })
            .await?;
        ensure_cleanup_running(shutdown)?;
        let retained_deleted = self
            .cleanup_retained_until_shutdown(now_ms, true, shutdown)
            .await?;
        ensure_cleanup_running(shutdown)?;
        let recovered_leases = self
            .run_batched_cleanup(shutdown, batch_size, max_batches, || {
                self.repository
                    .recover_stale_announce_leases_batch(now_ms, batch_size)
            })
            .await?;

        Ok(AnnounceMaintenanceSummary {
            expired,
            retained_deleted,
            recovered_leases,
        })
    }

    async fn run_batched_cleanup<F, Fut>(
        &self,
        shutdown: &ShutdownSignal,
        batch_size: u16,
        max_batches: u16,
        mut cleanup: F,
    ) -> Result<u64, AnnounceWorkerError>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<u64, DatabaseError>>,
    {
        let mut affected = 0_u64;
        for _ in 0..max_batches {
            ensure_cleanup_running(shutdown)?;
            let batch_affected = cleanup().await?;
            affected = affected.saturating_add(batch_affected);
            if batch_affected < u64::from(batch_size) {
                break;
            }
        }

        Ok(affected)
    }

    pub async fn claim_ready(
        &self,
        now_ms: i64,
    ) -> Result<Vec<AnnounceWorkId>, AnnounceWorkerError> {
        let lease_until_ms = now_ms.saturating_add(duration_ms(self.config.lease_duration));
        let reconcile_limit = self.config.claim_batch_size.saturating_mul(4).max(1);
        self.repository.expire_announce_work(now_ms).await?;
        self.cleanup_retained(now_ms, false).await?;
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

    async fn cleanup_retained(&self, now_ms: i64, force: bool) -> Result<u64, AnnounceWorkerError> {
        self.cleanup_retained_inner(now_ms, force, None).await
    }

    async fn cleanup_retained_until_shutdown(
        &self,
        now_ms: i64,
        force: bool,
        shutdown: &ShutdownSignal,
    ) -> Result<u64, AnnounceWorkerError> {
        self.cleanup_retained_inner(now_ms, force, Some(shutdown))
            .await
    }

    async fn cleanup_retained_inner(
        &self,
        now_ms: i64,
        force: bool,
        shutdown: Option<&ShutdownSignal>,
    ) -> Result<u64, AnnounceWorkerError> {
        let Some(previous_cleanup_ms) = self.start_retention_cleanup(now_ms, force) else {
            return Ok(0);
        };
        if let Some(shutdown) = shutdown
            && let Err(error) = ensure_cleanup_running(shutdown)
        {
            self.retention_cleanup
                .last_cleanup_ms
                .store(previous_cleanup_ms, Ordering::Release);
            return Err(error);
        }
        let success_cutoff_ms = now_ms.saturating_sub(duration_ms(self.config.success_retention));
        let failure_cutoff_ms = now_ms.saturating_sub(duration_ms(self.config.failure_retention));
        let mut deleted = 0_u64;
        let batch_size = self.config.retention_cleanup_batch_size;
        let max_batches = self.config.retention_cleanup_max_batches.max(1);
        for _ in 0..max_batches {
            if let Some(shutdown) = shutdown
                && let Err(error) = ensure_cleanup_running(shutdown)
            {
                self.retention_cleanup
                    .last_cleanup_ms
                    .store(previous_cleanup_ms, Ordering::Release);
                return Err(error);
            }
            let batch_deleted = match self
                .repository
                .cleanup_terminal_announce_work(success_cutoff_ms, failure_cutoff_ms, batch_size)
                .await
            {
                Ok(batch_deleted) => batch_deleted,
                Err(error) => {
                    self.retention_cleanup
                        .last_cleanup_ms
                        .store(previous_cleanup_ms, Ordering::Release);
                    return Err(AnnounceWorkerError::from(error));
                }
            };
            deleted = deleted.saturating_add(batch_deleted);
            if batch_deleted < u64::from(batch_size) {
                break;
            }
        }
        self.retention_cleanup
            .last_cleanup_ms
            .store(now_ms, Ordering::Release);

        Ok(deleted)
    }

    fn start_retention_cleanup(&self, now_ms: i64, force: bool) -> Option<i64> {
        if force {
            let previous_cleanup_ms = self
                .retention_cleanup
                .last_cleanup_ms
                .swap(RETENTION_CLEANUP_IN_PROGRESS_MS, Ordering::AcqRel);
            return Some(previous_cleanup_ms);
        }
        let interval_ms = duration_ms(self.config.retention_cleanup_interval);
        loop {
            let last_ms = self
                .retention_cleanup
                .last_cleanup_ms
                .load(Ordering::Acquire);
            if last_ms == RETENTION_CLEANUP_IN_PROGRESS_MS {
                return None;
            }
            if last_ms != i64::MIN && now_ms.saturating_sub(last_ms) < interval_ms {
                return None;
            }
            if self
                .retention_cleanup
                .last_cleanup_ms
                .compare_exchange(
                    last_ms,
                    RETENTION_CLEANUP_IN_PROGRESS_MS,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                return Some(last_ms);
            }
        }
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

fn seconds_to_millis(seconds: u64) -> i64 {
    i64::try_from(u128::from(seconds).saturating_mul(1_000)).unwrap_or(i64::MAX)
}

fn jitter_ms(delay_ms: i64, jitter_ratio: f64) -> i64 {
    if jitter_ratio <= 0.0 || !jitter_ratio.is_finite() {
        return 0;
    }
    (delay_ms.max(0) as f64 * jitter_ratio)
        .round()
        .clamp(0.0, i64::MAX as f64) as i64
}

fn retention_cleanup_batch_size(queue_config: &AnnounceQueueConfig) -> u16 {
    let per_minute_claim_capacity = u32::from(queue_config.claim_batch_size)
        .saturating_mul(u32::from(queue_config.worker_concurrency))
        .saturating_mul(120);
    u16::try_from(per_minute_claim_capacity.max(1)).unwrap_or(u16::MAX)
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
            Self::Shutdown => formatter.write_str("scheduler is shutting down"),
            Self::Database { source } => write!(formatter, "{source}"),
        }
    }
}

impl std::error::Error for AnnounceWorkerError {}

fn ensure_cleanup_running(shutdown: &ShutdownSignal) -> Result<(), AnnounceWorkerError> {
    if shutdown.state().phase == ShutdownPhase::Running {
        Ok(())
    } else {
        Err(AnnounceWorkerError::Shutdown)
    }
}

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
    async fn worker_cleans_terminal_work_using_retention_config() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let mut config = test_config();
        config.success_retention_secs = 10;
        config.failure_retention_secs = 20;
        for (id, guid, status, reason, finished_at_ms) in [
            (
                "ann_failed_old",
                "guid-failed-old",
                AnnounceStatus::TerminalFailed,
                AnnounceReason::NoMatchTerminal,
                79_000,
            ),
            (
                "ann_failed_retained",
                "guid-failed-retained",
                AnnounceStatus::TerminalFailed,
                AnnounceReason::NoMatchTerminal,
                81_000,
            ),
            (
                "ann_success_old",
                "guid-success-old",
                AnnounceStatus::Succeeded,
                AnnounceReason::Saved,
                89_000,
            ),
            (
                "ann_success_retained",
                "guid-success-retained",
                AnnounceStatus::Succeeded,
                AnnounceReason::Saved,
                91_000,
            ),
        ] {
            insert_terminal_work(&repository, id, guid, status, reason, finished_at_ms).await;
        }
        insert_work(&repository, "ann_queued_old", "guid-queued", 1).await;
        sqlx::query("UPDATE announce_work SET expires_at = ? WHERE id = 'ann_queued_old'")
            .bind(10_000_000_i64)
            .execute(repository.pool())
            .await
            .unwrap();
        let startup_worker =
            AnnounceWorker::new(repository.clone(), "startup-worker", &config).unwrap();

        let summary = startup_worker.recover_startup(100_000).await.unwrap();
        insert_terminal_work(
            &repository,
            "ann_success_after_cleanup",
            "guid-success-after-cleanup",
            AnnounceStatus::Succeeded,
            AnnounceReason::Saved,
            80_000,
        )
        .await;
        let worker = AnnounceWorker::new(repository.clone(), "worker-1", &config)
            .unwrap()
            .with_retention_cleanup(startup_worker.retention_cleanup());
        let early_claim = worker.claim_ready(100_001).await.unwrap();
        let after_early_claim = announce_ids(&repository).await;
        let later_claim = worker.claim_ready(100_000 + 60 * 1_000 + 1).await.unwrap();

        assert_eq!(
            AnnounceStartupSummary {
                expired: 0,
                recovered_leases: 0
            },
            summary
        );
        assert_eq!(
            vec!["ann_queued_old"],
            early_claim
                .iter()
                .map(AnnounceWorkId::as_str)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            vec![
                "ann_failed_retained".to_owned(),
                "ann_queued_old".to_owned(),
                "ann_success_after_cleanup".to_owned(),
                "ann_success_retained".to_owned(),
            ],
            after_early_claim
        );
        assert_eq!(
            vec!["ann_queued_old"],
            later_claim
                .iter()
                .map(AnnounceWorkId::as_str)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            vec!["ann_queued_old".to_owned()],
            announce_ids(&repository).await
        );
    }

    #[tokio::test]
    async fn worker_drains_multiple_retention_batches_per_cleanup() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let mut config = test_config();
        config.success_retention_secs = 10;
        for index in 0..5 {
            insert_terminal_work(
                &repository,
                &format!("ann_success_old_{index}"),
                &format!("guid-success-old-{index}"),
                AnnounceStatus::Succeeded,
                AnnounceReason::Saved,
                89_000,
            )
            .await;
        }
        let mut worker = AnnounceWorker::new(repository.clone(), "worker-1", &config).unwrap();
        worker.config.retention_cleanup_batch_size = 2;
        worker.config.retention_cleanup_max_batches = 3;

        let summary = worker.recover_startup(100_000).await.unwrap();

        assert_eq!(
            AnnounceStartupSummary {
                expired: 0,
                recovered_leases: 0
            },
            summary
        );
        assert!(announce_ids(&repository).await.is_empty());
    }

    #[tokio::test]
    async fn scheduled_cleanup_expires_recovers_and_deletes_retained_work() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let mut config = test_config();
        config.success_retention_secs = 10;
        config.failure_retention_secs = 20;
        insert_work(&repository, "ann_expired", "guid-expired", 1).await;
        insert_work(&repository, "ann_running", "guid-running", 1).await;
        repository
            .claim_announce_work("old-worker", 2, 3, 2)
            .await
            .unwrap();
        sqlx::query("UPDATE announce_work SET expires_at = ? WHERE id = 'ann_running'")
            .bind(200_000_i64)
            .execute(repository.pool())
            .await
            .unwrap();
        insert_terminal_work(
            &repository,
            "ann_success_old",
            "guid-success-old",
            AnnounceStatus::Succeeded,
            AnnounceReason::Saved,
            89_000,
        )
        .await;
        let worker = AnnounceWorker::new(repository.clone(), "worker-1", &config).unwrap();
        let (_controller, signal) = shutdown_channel();

        let summary = worker
            .run_scheduled_cleanup(100_000, &signal)
            .await
            .unwrap();

        assert_eq!(
            AnnounceMaintenanceSummary {
                expired: 1,
                retained_deleted: 1,
                recovered_leases: 1,
            },
            summary
        );
        assert_eq!(
            vec![
                ("expired".to_owned(), "expired".to_owned()),
                ("queued".to_owned(), "dependency_backoff".to_owned()),
            ],
            status_rows(&repository).await
        );
    }

    #[tokio::test]
    async fn scheduled_cleanup_reports_shutdown_before_work() {
        let repository = Repository::connect_in_memory().await.unwrap();
        insert_work(&repository, "ann_expired", "guid-expired", 1).await;
        let worker = AnnounceWorker::new(repository.clone(), "worker-1", &test_config()).unwrap();
        let (controller, signal) = shutdown_channel();
        controller.cancel_now("test shutdown").unwrap();

        let result = worker.run_scheduled_cleanup(100_000, &signal).await;

        assert!(matches!(result, Err(AnnounceWorkerError::Shutdown)));
        assert_eq!(
            vec![("queued".to_owned(), "accepted".to_owned())],
            status_rows(&repository).await
        );
    }

    #[tokio::test]
    async fn retention_cleanup_failure_does_not_advance_cadence() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let worker = AnnounceWorker::new(repository.clone(), "worker-1", &test_config()).unwrap();
        repository.pool().close().await;

        let result = worker.cleanup_retained(100_000, false).await;

        result.unwrap_err();
        assert_eq!(
            i64::MIN,
            worker
                .retention_cleanup
                .last_cleanup_ms
                .load(Ordering::Acquire)
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
            retry_initial_delay_ms: 30,
            retry_max_delay_ms: 30,
            retry_jitter_ratio: 0.0,
        };

        assert_eq!(
            AnnounceWorkOutcome::Succeeded {
                reason: AnnounceReason::Injected,
                outcome: "injected".to_owned(),
            },
            classify_announce_result(AnnounceWorkflowResult::Injected, 100, 1, "ann-1", config)
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
                1,
                "ann-1",
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
                1,
                "ann-1",
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
                1,
                "ann-1",
                config,
            )
        );
        assert_eq!(
            AnnounceWorkOutcome::Waiting {
                reason: AnnounceReason::InventoryRefreshing,
                next_attempt_at_ms: 110,
                dependency: None,
            },
            classify_announce_result(AnnounceWorkflowResult::NoMatch, 100, 1, "ann-1", config)
        );
    }

    #[test]
    fn classifier_uses_configured_retry_backoff_and_preserves_retry_after() {
        let config = AnnounceOutcomeConfig {
            source_wait_ms: 10,
            candidate_download_wait_ms: 20,
            retry_initial_delay_ms: 5_000,
            retry_max_delay_ms: 20_000,
            retry_jitter_ratio: 0.0,
        };

        assert_eq!(
            AnnounceWorkOutcome::Retryable {
                reason: AnnounceReason::TransientDependencyFailure,
                next_attempt_at_ms: 6_000,
                error_class: "indexer".to_owned(),
                redacted_message: "timeout".to_owned(),
            },
            classify_announce_result(
                AnnounceWorkflowResult::RetryableDependency {
                    retry_after_ms: None,
                    error_class: "indexer".to_owned(),
                    redacted_message: "timeout".to_owned(),
                },
                1_000,
                1,
                "ann-backoff",
                config,
            )
        );
        assert_eq!(
            AnnounceWorkOutcome::Retryable {
                reason: AnnounceReason::TransientDependencyFailure,
                next_attempt_at_ms: 21_000,
                error_class: "indexer".to_owned(),
                redacted_message: "timeout".to_owned(),
            },
            classify_announce_result(
                AnnounceWorkflowResult::RetryableDependency {
                    retry_after_ms: None,
                    error_class: "indexer".to_owned(),
                    redacted_message: "timeout".to_owned(),
                },
                1_000,
                4,
                "ann-backoff",
                config,
            )
        );
        assert_eq!(
            AnnounceWorkOutcome::Retryable {
                reason: AnnounceReason::RetryAfter,
                next_attempt_at_ms: 1_500,
                error_class: "indexer".to_owned(),
                redacted_message: "timeout".to_owned(),
            },
            classify_announce_result(
                AnnounceWorkflowResult::RetryableDependency {
                    retry_after_ms: Some(1_500),
                    error_class: "indexer".to_owned(),
                    redacted_message: "timeout".to_owned(),
                },
                1_000,
                4,
                "ann-backoff",
                config,
            )
        );
        let default_config = AnnounceOutcomeConfig::from_queue_config(&AnnounceQueueConfig {
            retry_jitter_ratio: 0.0,
            ..AnnounceQueueConfig::default()
        });
        assert_eq!(
            AnnounceWorkOutcome::Retryable {
                reason: AnnounceReason::TransientDependencyFailure,
                next_attempt_at_ms: 3_601_000,
                error_class: "indexer".to_owned(),
                redacted_message: "timeout".to_owned(),
            },
            classify_announce_result(
                AnnounceWorkflowResult::RetryableDependency {
                    retry_after_ms: None,
                    error_class: "indexer".to_owned(),
                    redacted_message: "timeout".to_owned(),
                },
                1_000,
                8,
                "ann-backoff",
                default_config,
            )
        );
    }

    #[test]
    fn classifier_maps_reverse_lookup_and_injection_results() {
        let config = AnnounceOutcomeConfig {
            source_wait_ms: 10,
            candidate_download_wait_ms: 20,
            retry_initial_delay_ms: 30,
            retry_max_delay_ms: 30,
            retry_jitter_ratio: 0.0,
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
            prepared_link_cleanup_incomplete: false,
        };

        assert_eq!(
            AnnounceWorkOutcome::Waiting {
                reason: AnnounceReason::SourceIncomplete,
                next_attempt_at_ms: 110,
                dependency: None,
            },
            classify_reverse_lookup_outcome(&source_incomplete, 100, 1, "ann-1", config)
        );
        assert_eq!(
            AnnounceWorkOutcome::TerminalFailed {
                reason: AnnounceReason::InvalidRequest,
                redacted_message: "candidate rejected: BlockedRelease".to_owned(),
            },
            classify_reverse_lookup_outcome(&rejected, 100, 1, "ann-1", config)
        );
        assert_eq!(
            AnnounceWorkOutcome::Waiting {
                reason: AnnounceReason::SourceIncomplete,
                next_attempt_at_ms: 110,
                dependency: Some(("client".to_owned(), "qbit.local".to_owned())),
            },
            classify_injection_result(&injection, 100, 1, "ann-1", config)
        );

        let ambiguous_failure = crate::runtime::injection_worker::InjectionWorkResult {
            outcome: InjectionOutcome::Failed,
            target_client: Some(DependencyName::new("qbit.local").unwrap()),
            saved_for_retry: true,
            linked_files: 1,
            prepared_link_cleanup_incomplete: false,
        };

        assert_eq!(
            AnnounceWorkOutcome::Waiting {
                reason: AnnounceReason::DependencyBackoff,
                next_attempt_at_ms: 130,
                dependency: Some(("client".to_owned(), "qbit.local".to_owned())),
            },
            classify_injection_result(&ambiguous_failure, 100, 1, "ann-1", config)
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

    async fn insert_terminal_work(
        repository: &Repository,
        id: &str,
        guid: &str,
        status: AnnounceStatus,
        reason: AnnounceReason,
        finished_at_ms: i64,
    ) {
        let mut work = test_work(id, guid, 1);
        work.status = status;
        work.reason = reason;
        work.updated_at_ms = finished_at_ms;
        work.finished_at_ms = Some(finished_at_ms);
        let result = repository
            .insert_or_dedupe_announce_work(&work, 100)
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

    async fn announce_ids(repository: &Repository) -> Vec<String> {
        sqlx::query_scalar("SELECT id FROM announce_work ORDER BY id")
            .fetch_all(repository.pool())
            .await
            .unwrap()
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
