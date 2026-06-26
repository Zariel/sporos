use std::fmt;

use serde::{Deserialize, Serialize};

pub const CONTRACT_SCHEMA_VERSION: u16 = 1;
pub const DEFAULT_WORKFLOW_VERSION: &str = "1.0.0";

const ID_SEPARATOR: char = ':';
const DUROXIDE_RESERVED_ACTIVITY_PREFIX: &str = "__duroxide_syscall:";

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum WorkflowContractError {
    EmptyIdSegment { field: &'static str },
    InvalidIdSegment { field: &'static str, value: String },
}

impl fmt::Display for WorkflowContractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyIdSegment { field } => {
                write!(formatter, "workflow id segment `{field}` must not be empty")
            }
            Self::InvalidIdSegment { field, value } => {
                write!(
                    formatter,
                    "workflow id segment `{field}` contains unsupported characters: {value}"
                )
            }
        }
    }
}

impl std::error::Error for WorkflowContractError {}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowKind {
    Announce,
    Search,
    ScheduledJob,
    InventoryRefresh,
    SavedTorrentRetry,
}

impl WorkflowKind {
    pub const ALL: [Self; 5] = [
        Self::Announce,
        Self::Search,
        Self::ScheduledJob,
        Self::InventoryRefresh,
        Self::SavedTorrentRetry,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Announce => "announce",
            Self::Search => "search",
            Self::ScheduledJob => "scheduled_job",
            Self::InventoryRefresh => "inventory_refresh",
            Self::SavedTorrentRetry => "saved_torrent_retry",
        }
    }

    pub const fn orchestration_name(self) -> &'static str {
        match self {
            Self::Announce => "sporos.announce.v1",
            Self::Search => "sporos.search.v1",
            Self::ScheduledJob => "sporos.scheduled_job.v1",
            Self::InventoryRefresh => "sporos.inventory_refresh.v1",
            Self::SavedTorrentRetry => "sporos.saved_torrent_retry.v1",
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivityKind {
    RepositoryRead,
    RepositoryWrite,
    InventoryScanMedia,
    InventoryRefreshClient,
    MatchingReverseLookup,
    CandidateDownload,
    TorrentClientMutate,
    ActionsPrepareLinks,
    ActionsSaveTorrent,
    NotificationsDeliver,
    CleanupRun,
    ScheduledJobClaim,
    ScheduledJobComplete,
    ScheduledJobRun,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivityEffect {
    ReadOnly,
    LocalStateMutation,
    ExternalMutation,
    FilesystemMutation,
    NotificationDelivery,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DuplicateSafety {
    NaturallyIdempotent,
    DeterministicAtomicWrite,
    VerifyBeforeRetry,
    RepeatAcceptedByContract,
    DeliveryPolicyBounded,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivityRetryBoundary {
    SafeToRetryInsideActivity,
    RetryOnlyAfterVerification,
    RetryOnlyUnderDeliveryPolicy,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Serialize)]
pub struct ActivityRetryContract {
    pub activity: ActivityKind,
    pub effect: ActivityEffect,
    pub duplicate_safety: DuplicateSafety,
    pub retry_boundary: ActivityRetryBoundary,
    pub contract: &'static str,
}

impl ActivityRetryContract {
    pub const fn allows_bounded_inner_retry(self) -> bool {
        matches!(
            self.retry_boundary,
            ActivityRetryBoundary::SafeToRetryInsideActivity
        )
    }
}

impl ActivityKind {
    pub const ALL: [Self; 14] = [
        Self::RepositoryRead,
        Self::RepositoryWrite,
        Self::InventoryScanMedia,
        Self::InventoryRefreshClient,
        Self::MatchingReverseLookup,
        Self::CandidateDownload,
        Self::TorrentClientMutate,
        Self::ActionsPrepareLinks,
        Self::ActionsSaveTorrent,
        Self::NotificationsDeliver,
        Self::CleanupRun,
        Self::ScheduledJobClaim,
        Self::ScheduledJobComplete,
        Self::ScheduledJobRun,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RepositoryRead => "sporos.repository.read.v1",
            Self::RepositoryWrite => "sporos.repository.write.v1",
            Self::InventoryScanMedia => "sporos.inventory.scan_media.v1",
            Self::InventoryRefreshClient => "sporos.inventory.refresh_client.v1",
            Self::MatchingReverseLookup => "sporos.matching.reverse_lookup.v1",
            Self::CandidateDownload => "sporos.candidate.download.v1",
            Self::TorrentClientMutate => "sporos.torrent_client.mutate.v1",
            Self::ActionsPrepareLinks => "sporos.actions.prepare_links.v1",
            Self::ActionsSaveTorrent => "sporos.actions.save_torrent.v1",
            Self::NotificationsDeliver => "sporos.notifications.deliver.v1",
            Self::CleanupRun => "sporos.cleanup.run.v1",
            Self::ScheduledJobClaim => "sporos.scheduled_job.claim.v1",
            Self::ScheduledJobComplete => "sporos.scheduled_job.complete.v1",
            Self::ScheduledJobRun => "sporos.scheduled_job.run.v1",
        }
    }

    pub fn uses_duroxide_reserved_prefix(self) -> bool {
        self.as_str().starts_with(DUROXIDE_RESERVED_ACTIVITY_PREFIX)
    }

    pub const fn retry_contract(self) -> ActivityRetryContract {
        match self {
            Self::RepositoryRead => ActivityRetryContract {
                activity: self,
                effect: ActivityEffect::ReadOnly,
                duplicate_safety: DuplicateSafety::NaturallyIdempotent,
                retry_boundary: ActivityRetryBoundary::SafeToRetryInsideActivity,
                contract: "repository reads do not mutate external or local state and may use bounded transient database retry",
            },
            Self::RepositoryWrite => ActivityRetryContract {
                activity: self,
                effect: ActivityEffect::LocalStateMutation,
                duplicate_safety: DuplicateSafety::VerifyBeforeRetry,
                retry_boundary: ActivityRetryBoundary::RetryOnlyAfterVerification,
                contract: "repository writes must use transactions, stable keys, or a follow-up read before retrying an ambiguous write",
            },
            Self::InventoryScanMedia => ActivityRetryContract {
                activity: self,
                effect: ActivityEffect::ReadOnly,
                duplicate_safety: DuplicateSafety::NaturallyIdempotent,
                retry_boundary: ActivityRetryBoundary::SafeToRetryInsideActivity,
                contract: "media scans only read filesystem metadata and may retry transient local IO without publishing partial state",
            },
            Self::InventoryRefreshClient => ActivityRetryContract {
                activity: self,
                effect: ActivityEffect::LocalStateMutation,
                duplicate_safety: DuplicateSafety::VerifyBeforeRetry,
                retry_boundary: ActivityRetryBoundary::RetryOnlyAfterVerification,
                contract: "client inventory refresh may retry transient client reads before commit, but ambiguous persisted refresh state must be verified before repeating the activity",
            },
            Self::MatchingReverseLookup => ActivityRetryContract {
                activity: self,
                effect: ActivityEffect::LocalStateMutation,
                duplicate_safety: DuplicateSafety::VerifyBeforeRetry,
                retry_boundary: ActivityRetryBoundary::RetryOnlyAfterVerification,
                contract: "reverse lookup and assessment persist deterministic candidate and decision rows, so ambiguous writes must be verified by stable candidate identity before retry",
            },
            Self::CandidateDownload => ActivityRetryContract {
                activity: self,
                effect: ActivityEffect::FilesystemMutation,
                duplicate_safety: DuplicateSafety::DeterministicAtomicWrite,
                retry_boundary: ActivityRetryBoundary::SafeToRetryInsideActivity,
                contract: "candidate downloads use deterministic cache keys and atomic writes; retry must accept a verified existing cache file",
            },
            Self::TorrentClientMutate => ActivityRetryContract {
                activity: self,
                effect: ActivityEffect::ExternalMutation,
                duplicate_safety: DuplicateSafety::VerifyBeforeRetry,
                retry_boundary: ActivityRetryBoundary::RetryOnlyAfterVerification,
                contract: "torrent client mutation must check existing info hash or verify post-failure state before retrying injection, recheck, pause, resume, or start operations",
            },
            Self::ActionsPrepareLinks => ActivityRetryContract {
                activity: self,
                effect: ActivityEffect::FilesystemMutation,
                duplicate_safety: DuplicateSafety::VerifyBeforeRetry,
                retry_boundary: ActivityRetryBoundary::RetryOnlyAfterVerification,
                contract: "link preparation uses deterministic destinations and must revalidate existing links and cleanup checkpoints before repeating a partial attempt",
            },
            Self::ActionsSaveTorrent => ActivityRetryContract {
                activity: self,
                effect: ActivityEffect::FilesystemMutation,
                duplicate_safety: DuplicateSafety::DeterministicAtomicWrite,
                retry_boundary: ActivityRetryBoundary::SafeToRetryInsideActivity,
                contract: "saved torrent writes use deterministic metadata, atomic output files, and verified existing-file handling before retrying transient local IO",
            },
            Self::NotificationsDeliver => ActivityRetryContract {
                activity: self,
                effect: ActivityEffect::NotificationDelivery,
                duplicate_safety: DuplicateSafety::DeliveryPolicyBounded,
                retry_boundary: ActivityRetryBoundary::RetryOnlyUnderDeliveryPolicy,
                contract: "notification delivery retries are bounded by endpoint policy; ambiguous timeout-after-send cases must not be replayed unless the policy accepts duplicate delivery",
            },
            Self::CleanupRun => ActivityRetryContract {
                activity: self,
                effect: ActivityEffect::LocalStateMutation,
                duplicate_safety: DuplicateSafety::RepeatAcceptedByContract,
                retry_boundary: ActivityRetryBoundary::SafeToRetryInsideActivity,
                contract: "cleanup activities delete or mark deterministic stale records and files, and repeating cleanup must leave retained state unchanged",
            },
            Self::ScheduledJobClaim => ActivityRetryContract {
                activity: self,
                effect: ActivityEffect::LocalStateMutation,
                duplicate_safety: DuplicateSafety::VerifyBeforeRetry,
                retry_boundary: ActivityRetryBoundary::RetryOnlyAfterVerification,
                contract: "scheduled job claims use durable job rows and stable job names; ambiguous retries must observe the row before claiming again",
            },
            Self::ScheduledJobComplete => ActivityRetryContract {
                activity: self,
                effect: ActivityEffect::LocalStateMutation,
                duplicate_safety: DuplicateSafety::VerifyBeforeRetry,
                retry_boundary: ActivityRetryBoundary::RetryOnlyAfterVerification,
                contract: "scheduled job completion updates durable job rows and must preserve terminal status across ambiguous retries",
            },
            Self::ScheduledJobRun => ActivityRetryContract {
                activity: self,
                effect: ActivityEffect::LocalStateMutation,
                duplicate_safety: DuplicateSafety::VerifyBeforeRetry,
                retry_boundary: ActivityRetryBoundary::RetryOnlyAfterVerification,
                contract: "scheduled job run activities perform job-specific side effects and rely on the workflow checkpoint before repeating work",
            },
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowEventName {
    MediaInventoryCompleted,
    ClientInventoryCompleted,
    DependencyRecovered,
    CandidateCacheCompleted,
    ManualJobRequested,
    WorkflowCancelRequested,
    ShutdownRequested,
}

impl WorkflowEventName {
    pub const ALL: [Self; 7] = [
        Self::MediaInventoryCompleted,
        Self::ClientInventoryCompleted,
        Self::DependencyRecovered,
        Self::CandidateCacheCompleted,
        Self::ManualJobRequested,
        Self::WorkflowCancelRequested,
        Self::ShutdownRequested,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MediaInventoryCompleted => "media_inventory_completed",
            Self::ClientInventoryCompleted => "client_inventory_completed",
            Self::DependencyRecovered => "dependency_recovered",
            Self::CandidateCacheCompleted => "candidate_cache_completed",
            Self::ManualJobRequested => "manual_job_requested",
            Self::WorkflowCancelRequested => "workflow_cancel_requested",
            Self::ShutdownRequested => "shutdown_requested",
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InventoryRefreshKind {
    MediaFull,
    MediaChanged,
    Client,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowState {
    Running,
    Waiting,
    Retrying,
    Succeeded,
    Failed,
    Cancelled,
}

impl WorkflowState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Waiting => "waiting",
            Self::Retrying => "retrying",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowReason {
    Accepted,
    WaitingForInventory,
    WaitingForDependency,
    RunningActivity,
    BackingOff,
    Completed,
    Failed,
    Cancelled,
}

impl WorkflowReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::WaitingForInventory => "waiting_for_inventory",
            Self::WaitingForDependency => "waiting_for_dependency",
            Self::RunningActivity => "running_activity",
            Self::BackingOff => "backing_off",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct WorkflowInstanceId(String);

impl WorkflowInstanceId {
    pub fn announce(work_id: impl AsRef<str>) -> Result<Self, WorkflowContractError> {
        Self::from_segments([("work_id", work_id.as_ref())], "announce")
    }

    pub fn search(request_id: impl AsRef<str>) -> Result<Self, WorkflowContractError> {
        Self::from_segments([("request_id", request_id.as_ref())], "search")
    }

    pub fn scheduled_job_supervisor(
        job_name: impl AsRef<str>,
    ) -> Result<Self, WorkflowContractError> {
        Self::from_segments([("job_name", job_name.as_ref())], "job")
    }

    pub fn scheduled_job_run(
        job_name: impl AsRef<str>,
        scheduled_at_ms: u64,
    ) -> Result<Self, WorkflowContractError> {
        let scheduled_at = scheduled_at_ms.to_string();
        Self::from_segments(
            [
                ("job_name", job_name.as_ref()),
                ("scheduled_at_ms", &scheduled_at),
            ],
            "job:run",
        )
    }

    pub fn inventory_refresh(
        kind: InventoryRefreshKind,
        scope_hash: Option<&str>,
    ) -> Result<Self, WorkflowContractError> {
        match kind {
            InventoryRefreshKind::MediaFull => Ok(Self("inventory:media:full".to_owned())),
            InventoryRefreshKind::Client => Ok(Self("inventory:client".to_owned())),
            InventoryRefreshKind::MediaChanged => {
                let scope_hash = scope_hash.unwrap_or_default();
                Self::from_segments([("scope_hash", scope_hash)], "inventory:media:changed")
            }
        }
    }

    pub fn saved_retry_supervisor() -> Self {
        Self("saved-retry".to_owned())
    }

    pub fn saved_retry_item(item_key: impl AsRef<str>) -> Result<Self, WorkflowContractError> {
        Self::from_segments([("item_key", item_key.as_ref())], "saved-retry:item")
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn from_segments<const N: usize>(
        segments: [(&'static str, &str); N],
        prefix: &str,
    ) -> Result<Self, WorkflowContractError> {
        let mut value = String::from(prefix);
        for (field, segment) in segments {
            validate_id_segment(field, segment)?;
            value.push(ID_SEPARATOR);
            value.push_str(segment);
        }
        Ok(Self(value))
    }
}

impl fmt::Display for WorkflowInstanceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkflowInputEnvelope<T> {
    pub schema_version: u16,
    pub public_id: String,
    pub submitted_at_ms: i64,
    pub payload: T,
}

impl<T> WorkflowInputEnvelope<T> {
    pub fn new(public_id: impl Into<String>, submitted_at_ms: i64, payload: T) -> Self {
        Self {
            schema_version: CONTRACT_SCHEMA_VERSION,
            public_id: public_id.into(),
            submitted_at_ms,
            payload,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct ActivityInputEnvelope<T> {
    pub schema_version: u16,
    pub workflow_id: String,
    pub activity_id: String,
    pub payload: T,
}

impl<T> ActivityInputEnvelope<T> {
    pub fn new(workflow_id: impl Into<String>, activity_id: impl Into<String>, payload: T) -> Self {
        Self {
            schema_version: CONTRACT_SCHEMA_VERSION,
            workflow_id: workflow_id.into(),
            activity_id: activity_id.into(),
            payload,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct ActivityOutputEnvelope<T> {
    pub schema_version: u16,
    pub payload: T,
}

impl<T> ActivityOutputEnvelope<T> {
    pub const fn new(payload: T) -> Self {
        Self {
            schema_version: CONTRACT_SCHEMA_VERSION,
            payload,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct AnnounceWorkflowInput {
    pub work_id: String,
    pub dedupe_hash: String,
    pub tracker: String,
    pub candidate_guid: String,
    pub candidate_title: String,
    pub received_at_ms: i64,
    pub expires_at_ms: i64,
    pub fetch_material_present: bool,
    pub raw_secret_material_count: u16,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SearchWorkflowInput {
    pub request_id: String,
    pub media_type: String,
    pub query: String,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct ScheduledJobWorkflowInput {
    pub job_name: String,
    pub forced: bool,
    pub requested_at_ms: i64,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct InventoryRefreshWorkflowInput {
    pub kind: InventoryRefreshKind,
    pub scope_hash: Option<String>,
    pub requested_at_ms: i64,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SavedRetryWorkflowInput {
    pub reason: String,
    pub requested_at_ms: i64,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkflowDependencyRef {
    pub kind: String,
    pub name: String,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkflowCustomStatus {
    pub schema_version: u16,
    pub public_id: String,
    pub kind: WorkflowKind,
    pub state: WorkflowState,
    pub reason: WorkflowReason,
    pub next_action: Option<String>,
    pub blocked_dependency: Option<WorkflowDependencyRef>,
    pub raw_secret_material_count: u16,
}

impl WorkflowCustomStatus {
    pub fn new(
        public_id: impl Into<String>,
        kind: WorkflowKind,
        state: WorkflowState,
        reason: WorkflowReason,
    ) -> Self {
        Self {
            schema_version: CONTRACT_SCHEMA_VERSION,
            public_id: public_id.into(),
            kind,
            state,
            reason,
            next_action: None,
            blocked_dependency: None,
            raw_secret_material_count: 0,
        }
    }
}

fn validate_id_segment(field: &'static str, segment: &str) -> Result<(), WorkflowContractError> {
    if segment.is_empty() {
        return Err(WorkflowContractError::EmptyIdSegment { field });
    }
    if !segment
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(WorkflowContractError::InvalidIdSegment {
            field,
            value: segment.to_owned(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::error::Error;

    #[test]
    fn workflow_instance_ids_are_deterministic_and_reject_unsafe_segments()
    -> Result<(), Box<dyn Error>> {
        ensure_eq(
            WorkflowInstanceId::announce("abc123")?.as_str(),
            "announce:abc123",
            "announce id",
        )?;
        ensure_eq(
            WorkflowInstanceId::scheduled_job_run("media_inventory", 1_782_491_200_000)?.as_str(),
            "job:run:media_inventory:1782491200000",
            "scheduled job run id",
        )?;
        ensure_eq(
            WorkflowInstanceId::inventory_refresh(
                InventoryRefreshKind::MediaChanged,
                Some("scope-1"),
            )?
            .as_str(),
            "inventory:media:changed:scope-1",
            "changed inventory id",
        )?;
        ensure_eq(
            WorkflowInstanceId::saved_retry_item("sha1.deadbeef")?.as_str(),
            "saved-retry:item:sha1.deadbeef",
            "saved retry item id",
        )?;

        ensure_eq(
            matches!(
                WorkflowInstanceId::announce("bad:value"),
                Err(WorkflowContractError::InvalidIdSegment {
                    field: "work_id",
                    ..
                })
            ),
            true,
            "unsafe separator rejection",
        )?;
        ensure_eq(
            matches!(
                WorkflowInstanceId::inventory_refresh(InventoryRefreshKind::MediaChanged, None),
                Err(WorkflowContractError::EmptyIdSegment {
                    field: "scope_hash"
                })
            ),
            true,
            "missing scope hash rejection",
        )?;

        Ok(())
    }

    #[test]
    fn workflow_activity_and_event_names_are_stable_unique_and_duroxide_safe()
    -> Result<(), Box<dyn Error>> {
        let workflow_names = WorkflowKind::ALL.map(WorkflowKind::orchestration_name);
        ensure_unique(&workflow_names, "workflow names")?;
        for name in workflow_names {
            ensure_eq(name.starts_with("sporos."), true, "workflow namespace")?;
        }

        let activity_names = ActivityKind::ALL.map(ActivityKind::as_str);
        ensure_unique(&activity_names, "activity names")?;
        for activity in ActivityKind::ALL {
            ensure_eq(
                activity.uses_duroxide_reserved_prefix(),
                false,
                "activity reserved prefix",
            )?;
        }

        let event_names = WorkflowEventName::ALL.map(WorkflowEventName::as_str);
        ensure_unique(&event_names, "event names")?;
        ensure_eq(
            WorkflowEventName::MediaInventoryCompleted.as_str(),
            "media_inventory_completed",
            "media inventory event name",
        )?;

        Ok(())
    }

    #[test]
    fn activity_retry_contracts_cover_every_registered_activity() -> Result<(), Box<dyn Error>> {
        for activity in ActivityKind::ALL {
            let contract = activity.retry_contract();
            ensure_eq(contract.activity, activity, "contract activity identity")?;
            ensure_eq(
                contract.contract.len() > 40,
                true,
                "contract description is operator-meaningful",
            )?;
            ensure_eq(
                contract.contract.contains("secret"),
                false,
                "contract description must not mention secret material",
            )?;

            let json = serde_json::to_value(contract)?;
            ensure_eq(
                json["activity"].clone(),
                json!(activity),
                "contract activity json",
            )?;
            ensure_eq(
                json["retry_boundary"].is_string(),
                true,
                "contract retry boundary json",
            )?;
            ensure_eq(
                json["duplicate_safety"].is_string(),
                true,
                "contract duplicate safety json",
            )?;
        }

        Ok(())
    }

    #[test]
    fn side_effect_contracts_are_effect_safe() -> Result<(), Box<dyn Error>> {
        for activity in ActivityKind::ALL {
            let contract = activity.retry_contract();
            match contract.effect {
                ActivityEffect::ReadOnly => ensure_eq(
                    contract.retry_boundary,
                    ActivityRetryBoundary::SafeToRetryInsideActivity,
                    "read only retry boundary",
                )?,
                ActivityEffect::LocalStateMutation => ensure_eq(
                    matches!(
                        contract.retry_boundary,
                        ActivityRetryBoundary::RetryOnlyAfterVerification
                            | ActivityRetryBoundary::SafeToRetryInsideActivity
                    ),
                    true,
                    "local mutation retry boundary",
                )?,
                ActivityEffect::ExternalMutation => {
                    ensure_eq(
                        contract.retry_boundary,
                        ActivityRetryBoundary::RetryOnlyAfterVerification,
                        "external mutation retry boundary",
                    )?;
                    ensure_eq(
                        contract.duplicate_safety,
                        DuplicateSafety::VerifyBeforeRetry,
                        "external mutation duplicate safety",
                    )?;
                }
                ActivityEffect::FilesystemMutation => ensure_eq(
                    matches!(
                        contract.duplicate_safety,
                        DuplicateSafety::DeterministicAtomicWrite
                            | DuplicateSafety::VerifyBeforeRetry
                    ),
                    true,
                    "filesystem mutation duplicate safety",
                )?,
                ActivityEffect::NotificationDelivery => {
                    ensure_eq(
                        contract.retry_boundary,
                        ActivityRetryBoundary::RetryOnlyUnderDeliveryPolicy,
                        "notification delivery retry boundary",
                    )?;
                    ensure_eq(
                        contract.duplicate_safety,
                        DuplicateSafety::DeliveryPolicyBounded,
                        "notification delivery duplicate safety",
                    )?;
                }
            }
        }

        Ok(())
    }

    #[test]
    fn ambiguous_mutation_contracts_require_verification() -> Result<(), Box<dyn Error>> {
        for activity in [
            ActivityKind::RepositoryWrite,
            ActivityKind::InventoryRefreshClient,
            ActivityKind::MatchingReverseLookup,
            ActivityKind::TorrentClientMutate,
            ActivityKind::ActionsPrepareLinks,
        ] {
            let contract = activity.retry_contract();
            ensure_eq(
                contract.allows_bounded_inner_retry(),
                false,
                "ambiguous mutation inner retry boundary",
            )?;
            ensure_eq(
                contract.duplicate_safety,
                DuplicateSafety::VerifyBeforeRetry,
                "ambiguous mutation duplicate safety",
            )?;
            ensure_eq(
                contract.retry_boundary,
                ActivityRetryBoundary::RetryOnlyAfterVerification,
                "ambiguous mutation retry boundary",
            )?;
        }
        Ok(())
    }

    #[test]
    fn deterministic_side_effect_contracts_allow_inner_retries() -> Result<(), Box<dyn Error>> {
        for (activity, safety) in [
            (
                ActivityKind::CandidateDownload,
                DuplicateSafety::DeterministicAtomicWrite,
            ),
            (
                ActivityKind::ActionsSaveTorrent,
                DuplicateSafety::DeterministicAtomicWrite,
            ),
            (
                ActivityKind::CleanupRun,
                DuplicateSafety::RepeatAcceptedByContract,
            ),
        ] {
            let contract = activity.retry_contract();
            ensure_eq(
                contract.allows_bounded_inner_retry(),
                true,
                "deterministic side effect retry boundary",
            )?;
            ensure_eq(
                contract.duplicate_safety,
                safety,
                "deterministic side effect duplicate safety",
            )?;
        }

        Ok(())
    }

    #[test]
    fn workflow_inputs_are_versioned_and_json_compatible() -> Result<(), Box<dyn Error>> {
        let input = WorkflowInputEnvelope::new(
            "ann_public_1",
            1_782_491_200_000,
            AnnounceWorkflowInput {
                work_id: "ann_1782491200000_dedupe123".to_owned(),
                dedupe_hash: "dedupe123".to_owned(),
                tracker: "tracker-a".to_owned(),
                candidate_guid: "guid-1".to_owned(),
                candidate_title: "Ubuntu 24.04".to_owned(),
                received_at_ms: 1_782_491_200_000,
                expires_at_ms: 1_782_494_800_000,
                fetch_material_present: true,
                raw_secret_material_count: 2,
            },
        );

        let value = serde_json::to_value(&input)?;
        ensure_eq(
            value.clone(),
            json!({
                "schema_version": 1,
                "public_id": "ann_public_1",
                "submitted_at_ms": 1782491200000_i64,
                "payload": {
                    "work_id": "ann_1782491200000_dedupe123",
                    "dedupe_hash": "dedupe123",
                    "tracker": "tracker-a",
                    "candidate_guid": "guid-1",
                    "candidate_title": "Ubuntu 24.04",
                    "received_at_ms": 1782491200000_i64,
                    "expires_at_ms": 1782494800000_i64,
                    "fetch_material_present": true,
                    "raw_secret_material_count": 2
                }
            }),
            "announce input json",
        )?;

        let round_trip: WorkflowInputEnvelope<AnnounceWorkflowInput> =
            serde_json::from_value(value.clone())?;
        ensure_eq(round_trip, input, "announce input round trip")?;

        Ok(())
    }

    #[test]
    fn activity_envelopes_are_versioned_and_json_compatible() -> Result<(), Box<dyn Error>> {
        let input = ActivityInputEnvelope::new(
            "announce:dedupe123",
            "activity-1",
            InventoryRefreshWorkflowInput {
                kind: InventoryRefreshKind::MediaFull,
                scope_hash: None,
                requested_at_ms: 1_782_491_200_000,
            },
        );
        let output = ActivityOutputEnvelope::new(SearchWorkflowInput {
            request_id: "search-1".to_owned(),
            media_type: "movie".to_owned(),
            query: "arrival".to_owned(),
        });

        ensure_eq(
            serde_json::to_value(&input)?,
            json!({
                "schema_version": 1,
                "workflow_id": "announce:dedupe123",
                "activity_id": "activity-1",
                "payload": {
                    "kind": "media_full",
                    "scope_hash": null,
                    "requested_at_ms": 1782491200000_i64
                }
            }),
            "activity input json",
        )?;
        ensure_eq(
            serde_json::to_value(&output)?,
            json!({
                "schema_version": 1,
                "payload": {
                    "request_id": "search-1",
                    "media_type": "movie",
                    "query": "arrival"
                }
            }),
            "activity output json",
        )?;

        Ok(())
    }

    #[test]
    fn custom_status_is_secret_safe_and_json_compatible() -> Result<(), Box<dyn Error>> {
        let mut status = WorkflowCustomStatus::new(
            "ann_public_1",
            WorkflowKind::Announce,
            WorkflowState::Waiting,
            WorkflowReason::WaitingForInventory,
        );
        status.next_action = Some("wait_for_inventory".to_owned());
        status.blocked_dependency = Some(WorkflowDependencyRef {
            kind: "local_state".to_owned(),
            name: "media_inventory".to_owned(),
        });
        status.raw_secret_material_count = 2;

        let json_value = serde_json::to_value(&status)?;
        ensure_eq(
            json_value,
            json!({
                "schema_version": 1,
                "public_id": "ann_public_1",
                "kind": "announce",
                "state": "waiting",
                "reason": "waiting_for_inventory",
                "next_action": "wait_for_inventory",
                "blocked_dependency": {
                    "kind": "local_state",
                    "name": "media_inventory"
                },
                "raw_secret_material_count": 2
            }),
            "custom status json",
        )?;

        let debug = format!("{status:?}");
        ensure_eq(debug.contains("passkey"), false, "debug passkey redaction")?;
        ensure_eq(debug.contains("token"), false, "debug token redaction")?;

        Ok(())
    }

    fn ensure_unique(values: &[&str], context: &str) -> Result<(), Box<dyn Error>> {
        for (index, value) in values.iter().enumerate() {
            for other in values.iter().skip(index.saturating_add(1)) {
                if value == other {
                    return Err(format!("{context}: duplicate value `{value}`").into());
                }
            }
        }
        Ok(())
    }

    fn ensure_eq<T>(actual: T, expected: T, context: &str) -> Result<(), Box<dyn Error>>
    where
        T: fmt::Debug + Eq,
    {
        if actual == expected {
            Ok(())
        } else {
            Err(format!("{context}: expected {expected:?}, got {actual:?}").into())
        }
    }
}
