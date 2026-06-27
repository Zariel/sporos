use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use duroxide::providers::Provider;
use duroxide::providers::sqlite::SqliteProvider;
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{OrchestrationStatus, Runtime, RuntimeOptions};
use duroxide::{ActivityContext, Client, Either2, OrchestrationContext, OrchestrationRegistry};
use serde::{Deserialize, Serialize};

use crate::announce::{
    AnnounceQueueConfig, AnnounceReason, AnnounceStatus, AnnounceWorkId, AnnounceWorkItem,
};
use tokio::sync::mpsc;

use crate::domain::{DependencyKind, DependencyName, ItemTitle, JobName};
use crate::errors::DatabaseError;
use crate::inventory_refresh::{
    InventoryRefreshRequest, InventoryRefreshSummary, InventoryRefreshWorker,
    record_inventory_refresh_health, scan_failure_reason,
};
use crate::persistence::repository::{
    AnnounceDependency, Repository, SearchCandidateMaterialRef, WorkflowInventoryCompletionRecord,
    WorkflowInventoryWaiterRecord, WorkflowProjectionDependency, WorkflowProjectionUpdate,
};
use crate::runtime::announce_worker::{
    AnnounceWorkOutcome, AnnounceWorker, retry_database_call, unix_time_ms,
};
use crate::runtime::backoff::{
    RetryDecision, RetryErrorKind, RetryOutcome, TRANSIENT_IO_RETRY_MAX_ATTEMPTS,
    classify_database_error, retry_with_classification, transient_io_retry_policy,
};
use crate::runtime::daemon::{
    AnnounceInventoryRefreshMode, AnnounceProcessor, SEARCH_CANDIDATE_PREFLIGHT_CONCURRENCY,
    SearchWorkflowExecutionSummary, execute_scheduled_job, finalize_duroxide_search_workflow,
    process_announce_work_with_processor_mode, process_duroxide_search_candidate,
    saved_torrent_retry_config,
};
use crate::runtime::injection_worker::{
    InjectionWorker, SavedTorrentRetryItem, SavedTorrentRetrySummary,
};
use crate::runtime::scheduler::{
    CLIENT_INVENTORY_JOB_NAME, MEDIA_INVENTORY_JOB_NAME, PersistedScheduler,
    ScheduledJobClaimOutcome, SchedulerError,
};
use crate::runtime::shutdown::{ShutdownPhase, ShutdownSignal};
use crate::runtime::workflow_contracts::{
    ActivityInputEnvelope, ActivityKind, AnnounceWorkflowInput, InventoryRefreshKind,
    InventoryRefreshWorkflowInput, SavedRetryWorkflowInput, ScheduledJobWorkflowInput,
    SearchWorkflowInput, WorkflowCustomStatus, WorkflowEventName, WorkflowInstanceId, WorkflowKind,
    WorkflowReason, WorkflowState,
};

pub const WORKFLOW_RUNTIME_DEPENDENCY: &str = "workflow-runtime";
const WORKFLOW_DATABASE_FILE: &str = "sporos-workflows.db";
const DEFAULT_DATABASE_DIR: &str = "db";
const STARTUP_POLL_INTERVAL: Duration = Duration::from_millis(10);
const STARTUP_LONG_POLL_TIMEOUT: Duration = Duration::from_millis(50);
const INVENTORY_REFRESH_QUEUE: &str = "inventory_refresh_requests";
const SCHEDULED_JOB_RUN_ORCHESTRATION: &str = "sporos.scheduled_job.run.v1";
const SCHEDULED_JOB_MANUAL_QUEUE: &str = "manual_job_requests";
const INVENTORY_REFRESH_ACTIVITY_ID: &str = "inventory-refresh";
const SCHEDULED_JOB_CLAIM_ACTIVITY_ID: &str = "scheduled-job-claim";
const SCHEDULED_JOB_COMPLETE_ACTIVITY_ID: &str = "scheduled-job-complete";
const SCHEDULED_JOB_RUN_ACTIVITY_ID: &str = "scheduled-job-run";
const ANNOUNCE_PROCESS_ACTIVITY_ID: &str = "announce-process";
const ANNOUNCE_WAIT_ACTIVITY_ID: &str = "announce-wait";
const SEARCH_PLAN_ACTIVITY_ID: &str = "search-plan";
const SEARCH_CANDIDATE_PAGE_ACTIVITY_ID_PREFIX: &str = "search-candidate-page";
const SEARCH_CANDIDATE_ACTIVITY_ID_PREFIX: &str = "search-candidate";
const SEARCH_FINALIZE_ACTIVITY_ID: &str = "search-finalize";
const SEARCH_CANDIDATE_PAGE_LIMIT: u16 = 64;
const SAVED_RETRY_ITEM_ORCHESTRATION: &str = "sporos.saved_torrent_retry.item.v1";
const SAVED_RETRY_SCAN_ACTIVITY_ID: &str = "saved-retry-scan";
const SAVED_RETRY_PROCESS_ACTIVITY_ID: &str = "saved-retry-process";
const SAVED_RETRY_FINALIZE_ACTIVITY_ID: &str = "saved-retry-finalize";
const SAVED_RETRY_ITEM_CHILD_CONCURRENCY: usize = 1;
const ANNOUNCE_INVENTORY_WAIT_RECHECK_INTERVAL: Duration = Duration::from_secs(1);
const ANNOUNCE_WORKFLOW_OWNER: &str = "sporos-announce-workflow";
const INVENTORY_COMPLETION_FANOUT_LIMIT: u16 = 1_000;
const INVENTORY_COMPLETION_LEASE_MS: i64 = 60_000;
#[cfg(test)]
const TEST_INVENTORY_WAIT_ORCHESTRATION: &str = "sporos.test.inventory_wait.v1";

mod activities;
mod contracts;
mod database;
mod inventory_completion;
mod orchestrations;
mod registry;
mod runtime;

use activities::*;
use contracts::*;
use database::*;
use inventory_completion::*;
use orchestrations::*;
use registry::*;

pub use contracts::{
    AnnounceWorkflowActivities, AnnounceWorkflowSubmission, AnnounceWorkflowSubmissionOutcome,
    InventoryWorkflowActivities, InventoryWorkflowRequest, InventoryWorkflowSubmission,
    InventoryWorkflowSubmissionOutcome, SavedRetryWorkflowActivities,
    SavedRetryWorkflowStateHandle, ScheduledJobStateHandle, ScheduledJobWorkflowActivities,
    SearchWorkflowActivities, SearchWorkflowStateHandle, SearchWorkflowSubmission,
    SearchWorkflowSubmissionOutcome,
};
pub use database::{workflow_database_path, workflow_runtime_dependency_name};
pub use inventory_completion::{
    InventoryCompletionEvent, InventoryCompletionEventBridge, InventoryCompletionFanout,
    InventoryCompletionWaitRegistration, InventoryCompletionWaiter,
};
pub use runtime::{
    DuroxideWorkflowRuntime, DuroxideWorkflowRuntimeError, WorkflowSupervisorSeedSummary,
};

#[cfg(test)]
include!("tests.rs");
