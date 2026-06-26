use std::collections::BTreeSet;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use duroxide::providers::Provider;
use duroxide::providers::sqlite::SqliteProvider;
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{OrchestrationStatus, Runtime, RuntimeOptions};
use duroxide::{ActivityContext, Client, OrchestrationContext, OrchestrationRegistry};
use serde::{Deserialize, Serialize};

use crate::domain::{DependencyKind, DependencyName};
use crate::inventory_refresh::{
    InventoryRefreshRequest, InventoryRefreshSummary, InventoryRefreshWorker,
    record_inventory_refresh_health, scan_failure_reason,
};
use crate::persistence::repository::{
    Repository, WorkflowProjectionDependency, WorkflowProjectionUpdate,
};
use crate::runtime::announce_worker::unix_time_ms;
use crate::runtime::injection_worker::InjectionWorker;
use crate::runtime::shutdown::{ShutdownPhase, ShutdownSignal};
use crate::runtime::workflow_contracts::{
    ActivityInputEnvelope, ActivityKind, InventoryRefreshKind, InventoryRefreshWorkflowInput,
    WorkflowCustomStatus, WorkflowInstanceId, WorkflowKind, WorkflowReason, WorkflowState,
};

pub const WORKFLOW_RUNTIME_DEPENDENCY: &str = "workflow-runtime";
const WORKFLOW_DATABASE_FILE: &str = "sporos-workflows.db";
const DEFAULT_DATABASE_DIR: &str = "db";
const STARTUP_POLL_INTERVAL: Duration = Duration::from_millis(10);
const STARTUP_LONG_POLL_TIMEOUT: Duration = Duration::from_millis(50);
const INVENTORY_REFRESH_QUEUE: &str = "inventory_refresh_requests";
const INVENTORY_REFRESH_ACTIVITY_ID: &str = "inventory-refresh";

#[derive(Clone)]
pub struct DuroxideWorkflowRuntime {
    database_path: PathBuf,
    repository: Option<Repository>,
    store: Arc<dyn Provider>,
    runtime: Arc<Runtime>,
    seeded_supervisors: Arc<Mutex<BTreeSet<String>>>,
    active_inventory_refreshes: Arc<Mutex<BTreeSet<String>>>,
}

impl DuroxideWorkflowRuntime {
    pub async fn start(database_path: PathBuf) -> Result<Self, DuroxideWorkflowRuntimeError> {
        Self::start_inner(database_path, None).await
    }

    pub async fn start_with_inventory_activities(
        database_path: PathBuf,
        activities: InventoryWorkflowActivities,
    ) -> Result<Self, DuroxideWorkflowRuntimeError> {
        let repository = activities.repository.clone();
        Self::start_inner(database_path, Some((repository, activities))).await
    }

    async fn start_inner(
        database_path: PathBuf,
        inventory: Option<(Repository, InventoryWorkflowActivities)>,
    ) -> Result<Self, DuroxideWorkflowRuntimeError> {
        prepare_workflow_database(&database_path).await?;
        let database_url = format!("sqlite:{}", database_path.display());
        let store = Arc::new(
            SqliteProvider::new(&database_url, None)
                .await
                .map_err(|error| DuroxideWorkflowRuntimeError::OpenDatabase {
                    path: database_path.clone(),
                    message: error.to_string(),
                })?,
        ) as Arc<dyn Provider>;
        let repository = inventory
            .as_ref()
            .map(|(repository, _activities)| repository.clone());
        let active_inventory_refreshes = Arc::new(Mutex::new(BTreeSet::new()));
        let activity_registry = match inventory {
            Some((_repository, activities)) => activity_registry_with_inventory_activities(
                activities,
                Arc::clone(&active_inventory_refreshes),
            ),
            None => activity_registry(),
        };
        let runtime = Runtime::start_with_options(
            Arc::clone(&store),
            activity_registry,
            orchestration_registry(),
            RuntimeOptions {
                dispatcher_min_poll_interval: STARTUP_POLL_INTERVAL,
                dispatcher_long_poll_timeout: STARTUP_LONG_POLL_TIMEOUT,
                orchestration_concurrency: 1,
                worker_concurrency: 1,
                ..RuntimeOptions::default()
            },
        )
        .await;

        Ok(Self {
            database_path,
            repository,
            store,
            runtime,
            seeded_supervisors: Arc::new(Mutex::new(BTreeSet::new())),
            active_inventory_refreshes,
        })
    }

    pub fn database_path(&self) -> &Path {
        &self.database_path
    }

    pub fn client(&self) -> Client {
        Client::new(Arc::clone(&self.store))
    }

    pub async fn submit_inventory_refresh(
        &self,
        request: InventoryWorkflowRequest,
    ) -> Result<InventoryWorkflowSubmission, DuroxideWorkflowRuntimeError> {
        let instance_id = request
            .instance_id()
            .map_err(DuroxideWorkflowRuntimeError::InvalidInventoryWorkflowId)?;
        let instance_id = instance_id.as_str().to_owned();
        let started_at_ms = unix_time_ms();
        let active_check = self.mark_inventory_refresh_active(&request, &instance_id)?;
        if active_check.already_active {
            return Ok(InventoryWorkflowSubmission {
                workflow_id: active_check.workflow_id,
                outcome: InventoryWorkflowSubmissionOutcome::Coalesced,
            });
        }
        let client = self.client();
        match client
            .get_orchestration_status(&instance_id)
            .await
            .map_err(
                |error| DuroxideWorkflowRuntimeError::ReadInventoryWorkflow {
                    instance_id: instance_id.clone(),
                    message: error.to_string(),
                },
            )? {
            OrchestrationStatus::NotFound => {
                client
                    .start_orchestration_typed(
                        &instance_id,
                        WorkflowKind::InventoryRefresh.orchestration_name(),
                        request.workflow_input(),
                    )
                    .await
                    .map_err(
                        |error| DuroxideWorkflowRuntimeError::StartInventoryWorkflow {
                            instance_id: instance_id.clone(),
                            message: error.to_string(),
                        },
                    )
                    .inspect_err(|_error| self.clear_inventory_refresh_active(&instance_id))?;
            }
            OrchestrationStatus::Running { .. } => {}
            OrchestrationStatus::Completed { .. } => {
                self.clear_inventory_refresh_active(&instance_id);
                return Err(DuroxideWorkflowRuntimeError::CompletedInventoryWorkflow {
                    instance_id,
                });
            }
            OrchestrationStatus::Failed { details, .. } => {
                self.clear_inventory_refresh_active(&instance_id);
                return Err(DuroxideWorkflowRuntimeError::FailedInventoryWorkflow {
                    instance_id,
                    message: details.display_message().to_string(),
                });
            }
        }

        client
            .enqueue_event_typed(&instance_id, INVENTORY_REFRESH_QUEUE, &request)
            .await
            .map_err(
                |error| DuroxideWorkflowRuntimeError::EnqueueInventoryRefresh {
                    instance_id: instance_id.clone(),
                    message: error.to_string(),
                },
            )
            .inspect_err(|_error| self.clear_inventory_refresh_active(&instance_id))?;

        let public_id = request.public_id();
        self.record_inventory_projection(InventoryProjectionRecord {
            workflow_id: &instance_id,
            public_id: &public_id,
            state: WorkflowState::Waiting,
            reason: WorkflowReason::WaitingForInventory,
            next_action: Some("queued"),
            started_at_ms,
            updated_at_ms: unix_time_ms(),
            finished_at_ms: None,
            blocked_dependency_name: None,
        })
        .await
        .inspect_err(|_error| self.clear_inventory_refresh_active(&instance_id))?;

        Ok(InventoryWorkflowSubmission {
            workflow_id: instance_id,
            outcome: InventoryWorkflowSubmissionOutcome::Queued,
        })
    }

    pub async fn wait_for_inventory_refresh_outcome(
        &self,
        workflow_id: &str,
        submitted_at_ms: i64,
        mut shutdown: ShutdownSignal,
    ) -> Result<(), DuroxideWorkflowRuntimeError> {
        let Some(repository) = self.repository.as_ref() else {
            return Ok(());
        };
        loop {
            let snapshot = repository
                .workflow_projection_snapshot(100, unix_time_ms())
                .await
                .map_err(
                    |error| DuroxideWorkflowRuntimeError::ReadInventoryProjection {
                        workflow_id: workflow_id.to_owned(),
                        message: error.to_string(),
                    },
                )?;
            if let Some(item) = snapshot.recent.iter().find(|item| {
                item.workflow_id == workflow_id && item.updated_at_ms >= submitted_at_ms
            }) {
                match item.state.as_str() {
                    "succeeded" => return Ok(()),
                    "retrying" | "failed" | "cancelled" => {
                        return Err(
                            DuroxideWorkflowRuntimeError::UnsuccessfulInventoryWorkflow {
                                workflow_id: workflow_id.to_owned(),
                                state: item.state.clone(),
                                reason: item.reason.clone(),
                                detail: item
                                    .blocked_dependency_name
                                    .clone()
                                    .or_else(|| item.next_action.clone()),
                            },
                        );
                    }
                    _ => {}
                }
            }
            tokio::select! {
                _state = shutdown.cancelled() => {
                    return Err(DuroxideWorkflowRuntimeError::InventoryWorkflowWaitCancelled {
                        workflow_id: workflow_id.to_owned(),
                    });
                }
                () = tokio::time::sleep(Duration::from_millis(50)) => {}
            }
        }
    }

    pub async fn seed_supervisors(
        &self,
        scheduled_jobs: &[&str],
    ) -> Result<WorkflowSupervisorSeedSummary, DuroxideWorkflowRuntimeError> {
        let client = self.client();
        let mut summary = WorkflowSupervisorSeedSummary::default();
        for job_name in scheduled_jobs {
            let instance_id = WorkflowInstanceId::scheduled_job_supervisor(job_name)
                .map_err(DuroxideWorkflowRuntimeError::InvalidSupervisorId)?;
            let seeded = self
                .seed_supervisor(
                    &client,
                    instance_id.as_str(),
                    WorkflowKind::ScheduledJob,
                    job_name,
                )
                .await?;
            summary.record(seeded);
        }

        let saved_retry_id = WorkflowInstanceId::saved_retry_supervisor();
        let seeded = self
            .seed_supervisor(
                &client,
                saved_retry_id.as_str(),
                WorkflowKind::SavedTorrentRetry,
                "saved-retry",
            )
            .await?;
        summary.record(seeded);

        Ok(summary)
    }

    pub async fn shutdown(&self, timeout_ms: Option<u64>) {
        self.runtime.clone().shutdown(timeout_ms).await;
    }

    async fn seed_supervisor(
        &self,
        client: &Client,
        instance_id: &str,
        kind: WorkflowKind,
        public_id: &str,
    ) -> Result<SupervisorSeedOutcome, DuroxideWorkflowRuntimeError> {
        if self.already_seeded(instance_id)? {
            return Ok(SupervisorSeedOutcome::AlreadyPresent);
        }
        match client
            .get_orchestration_status(instance_id)
            .await
            .map_err(|error| DuroxideWorkflowRuntimeError::ReadSupervisor {
                instance_id: instance_id.to_owned(),
                message: error.to_string(),
            })? {
            OrchestrationStatus::NotFound => {
                let input = WorkflowSupervisorInput {
                    kind,
                    public_id: public_id.to_owned(),
                };
                client
                    .start_orchestration_typed(instance_id, kind.orchestration_name(), input)
                    .await
                    .map_err(|error| DuroxideWorkflowRuntimeError::StartSupervisor {
                        instance_id: instance_id.to_owned(),
                        message: error.to_string(),
                    })?;
                self.mark_seeded(instance_id)?;
                Ok(SupervisorSeedOutcome::Started)
            }
            OrchestrationStatus::Running { .. } | OrchestrationStatus::Completed { .. } => {
                self.mark_seeded(instance_id)?;
                Ok(SupervisorSeedOutcome::AlreadyPresent)
            }
            OrchestrationStatus::Failed { details, .. } => {
                Err(DuroxideWorkflowRuntimeError::FailedSupervisor {
                    instance_id: instance_id.to_owned(),
                    message: details.display_message().to_string(),
                })
            }
        }
    }

    fn already_seeded(&self, instance_id: &str) -> Result<bool, DuroxideWorkflowRuntimeError> {
        let seeded = self
            .seeded_supervisors
            .lock()
            .map_err(|_error| DuroxideWorkflowRuntimeError::SeedTrackerPoisoned)?;
        Ok(seeded.contains(instance_id))
    }

    fn mark_seeded(&self, instance_id: &str) -> Result<(), DuroxideWorkflowRuntimeError> {
        let mut seeded = self
            .seeded_supervisors
            .lock()
            .map_err(|_error| DuroxideWorkflowRuntimeError::SeedTrackerPoisoned)?;
        seeded.insert(instance_id.to_owned());
        Ok(())
    }

    async fn record_inventory_projection(
        &self,
        record: InventoryProjectionRecord<'_>,
    ) -> Result<(), DuroxideWorkflowRuntimeError> {
        let Some(repository) = self.repository.as_ref() else {
            return Ok(());
        };
        repository
            .record_workflow_projection(&WorkflowProjectionUpdate {
                workflow_id: record.workflow_id,
                workflow_kind: WorkflowKind::InventoryRefresh,
                public_id: record.public_id,
                state: record.state,
                reason: record.reason,
                next_action: record.next_action,
                blocked_dependency: None,
                raw_secret_material_count: 0,
                started_at_ms: record.started_at_ms,
                updated_at_ms: record.updated_at_ms,
                finished_at_ms: record.finished_at_ms,
            })
            .await
            .map_err(
                |error| DuroxideWorkflowRuntimeError::RecordInventoryProjection {
                    workflow_id: record.workflow_id.to_owned(),
                    message: error.to_string(),
                },
            )
    }

    fn mark_inventory_refresh_active(
        &self,
        request: &InventoryWorkflowRequest,
        workflow_id: &str,
    ) -> Result<InventoryActiveCheck, DuroxideWorkflowRuntimeError> {
        let mut active = self
            .active_inventory_refreshes
            .lock()
            .map_err(|_error| DuroxideWorkflowRuntimeError::InventoryTrackerPoisoned)?;
        if request.kind == InventoryRefreshKind::MediaChanged {
            let full_id =
                WorkflowInstanceId::inventory_refresh(InventoryRefreshKind::MediaFull, None)
                    .map_err(DuroxideWorkflowRuntimeError::InvalidInventoryWorkflowId)?
                    .to_string();
            if active.contains(&full_id) {
                return Ok(InventoryActiveCheck {
                    workflow_id: full_id,
                    already_active: true,
                });
            }
        }
        Ok(InventoryActiveCheck {
            workflow_id: workflow_id.to_owned(),
            already_active: !active.insert(workflow_id.to_owned()),
        })
    }

    fn clear_inventory_refresh_active(&self, workflow_id: &str) {
        let Ok(mut active) = self.active_inventory_refreshes.lock() else {
            return;
        };
        active.remove(workflow_id);
    }
}

impl fmt::Debug for DuroxideWorkflowRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DuroxideWorkflowRuntime")
            .field("database_path", &self.database_path)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub struct WorkflowSupervisorSeedSummary {
    pub started: usize,
    pub already_present: usize,
}

impl WorkflowSupervisorSeedSummary {
    fn record(&mut self, outcome: SupervisorSeedOutcome) {
        match outcome {
            SupervisorSeedOutcome::Started => self.started += 1,
            SupervisorSeedOutcome::AlreadyPresent => self.already_present += 1,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum SupervisorSeedOutcome {
    Started,
    AlreadyPresent,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct InventoryActiveCheck {
    workflow_id: String,
    already_active: bool,
}

#[derive(Debug)]
pub enum DuroxideWorkflowRuntimeError {
    InvalidDependencyName {
        message: String,
    },
    PrepareDatabase {
        path: PathBuf,
        message: String,
    },
    OpenDatabase {
        path: PathBuf,
        message: String,
    },
    InvalidSupervisorId(crate::runtime::workflow_contracts::WorkflowContractError),
    InvalidInventoryWorkflowId(crate::runtime::workflow_contracts::WorkflowContractError),
    ReadSupervisor {
        instance_id: String,
        message: String,
    },
    FailedSupervisor {
        instance_id: String,
        message: String,
    },
    StartSupervisor {
        instance_id: String,
        message: String,
    },
    ReadInventoryWorkflow {
        instance_id: String,
        message: String,
    },
    StartInventoryWorkflow {
        instance_id: String,
        message: String,
    },
    CompletedInventoryWorkflow {
        instance_id: String,
    },
    FailedInventoryWorkflow {
        instance_id: String,
        message: String,
    },
    EnqueueInventoryRefresh {
        instance_id: String,
        message: String,
    },
    RecordInventoryProjection {
        workflow_id: String,
        message: String,
    },
    ReadInventoryProjection {
        workflow_id: String,
        message: String,
    },
    UnsuccessfulInventoryWorkflow {
        workflow_id: String,
        state: String,
        reason: String,
        detail: Option<String>,
    },
    InventoryWorkflowWaitCancelled {
        workflow_id: String,
    },
    SeedTrackerPoisoned,
    InventoryTrackerPoisoned,
}

impl fmt::Display for DuroxideWorkflowRuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidDependencyName { message } => {
                write!(
                    formatter,
                    "workflow runtime dependency name is invalid: {message}"
                )
            }
            Self::PrepareDatabase { path, message } => {
                write!(
                    formatter,
                    "prepare workflow database `{}` failed: {message}",
                    path.display()
                )
            }
            Self::OpenDatabase { path, message } => {
                write!(
                    formatter,
                    "open workflow database `{}` failed: {message}",
                    path.display()
                )
            }
            Self::InvalidSupervisorId(error) => write!(formatter, "{error}"),
            Self::InvalidInventoryWorkflowId(error) => write!(formatter, "{error}"),
            Self::ReadSupervisor {
                instance_id,
                message,
            } => write!(
                formatter,
                "read workflow supervisor `{instance_id}` failed: {message}"
            ),
            Self::FailedSupervisor {
                instance_id,
                message,
            } => write!(
                formatter,
                "workflow supervisor `{instance_id}` is failed: {message}"
            ),
            Self::StartSupervisor {
                instance_id,
                message,
            } => write!(
                formatter,
                "start workflow supervisor `{instance_id}` failed: {message}"
            ),
            Self::ReadInventoryWorkflow {
                instance_id,
                message,
            } => write!(
                formatter,
                "read inventory workflow `{instance_id}` failed: {message}"
            ),
            Self::StartInventoryWorkflow {
                instance_id,
                message,
            } => write!(
                formatter,
                "start inventory workflow `{instance_id}` failed: {message}"
            ),
            Self::CompletedInventoryWorkflow { instance_id } => write!(
                formatter,
                "inventory workflow `{instance_id}` completed and cannot accept refresh requests"
            ),
            Self::FailedInventoryWorkflow {
                instance_id,
                message,
            } => write!(
                formatter,
                "inventory workflow `{instance_id}` is failed: {message}"
            ),
            Self::EnqueueInventoryRefresh {
                instance_id,
                message,
            } => write!(
                formatter,
                "enqueue inventory refresh for workflow `{instance_id}` failed: {message}"
            ),
            Self::RecordInventoryProjection {
                workflow_id,
                message,
            } => write!(
                formatter,
                "record inventory workflow projection `{workflow_id}` failed: {message}"
            ),
            Self::ReadInventoryProjection {
                workflow_id,
                message,
            } => write!(
                formatter,
                "read inventory workflow projection `{workflow_id}` failed: {message}"
            ),
            Self::UnsuccessfulInventoryWorkflow {
                workflow_id,
                state,
                reason,
                detail,
            } => write!(
                formatter,
                "inventory workflow `{workflow_id}` finished unsuccessfully: state={state} reason={reason}{}",
                detail
                    .as_ref()
                    .map(|detail| format!(" detail={detail}"))
                    .unwrap_or_default()
            ),
            Self::InventoryWorkflowWaitCancelled { workflow_id } => write!(
                formatter,
                "wait for inventory workflow `{workflow_id}` cancelled"
            ),
            Self::SeedTrackerPoisoned => {
                formatter.write_str("workflow supervisor seed tracker is poisoned")
            }
            Self::InventoryTrackerPoisoned => {
                formatter.write_str("workflow inventory refresh tracker is poisoned")
            }
        }
    }
}

impl std::error::Error for DuroxideWorkflowRuntimeError {}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
struct WorkflowSupervisorInput {
    kind: WorkflowKind,
    public_id: String,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
struct WorkflowSupervisorOutput {
    kind: WorkflowKind,
    public_id: String,
    state: WorkflowState,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
struct WorkflowShellActivityInput {
    activity: ActivityKind,
    workflow_id: String,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
struct WorkflowShellActivityOutput {
    activity: ActivityKind,
    accepted: bool,
}

#[derive(Debug, Clone)]
pub struct InventoryWorkflowActivities {
    repository: Repository,
    inventory_refresh: InventoryRefreshWorker,
    injection_worker: InjectionWorker,
    shutdown: ShutdownSignal,
    failure_backoff: Duration,
}

impl InventoryWorkflowActivities {
    pub fn new(
        repository: Repository,
        inventory_refresh: InventoryRefreshWorker,
        injection_worker: InjectionWorker,
        shutdown: ShutdownSignal,
        failure_backoff: Duration,
    ) -> Self {
        Self {
            repository,
            inventory_refresh,
            injection_worker,
            shutdown,
            failure_backoff,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct InventoryWorkflowRequest {
    pub kind: InventoryRefreshKind,
    pub scope_hash: Option<String>,
    pub media_dirs: Vec<PathBuf>,
    pub changed_paths: Vec<PathBuf>,
    pub requested_at_ms: i64,
}

impl InventoryWorkflowRequest {
    pub fn media_full(media_dirs: Vec<PathBuf>, requested_at_ms: i64) -> Self {
        Self {
            kind: InventoryRefreshKind::MediaFull,
            scope_hash: None,
            media_dirs,
            changed_paths: Vec::new(),
            requested_at_ms,
        }
    }

    pub fn media_changed(
        media_dirs: Vec<PathBuf>,
        changed_paths: Vec<PathBuf>,
        requested_at_ms: i64,
    ) -> Self {
        let scope_hash = changed_paths_scope_hash(&changed_paths);
        Self {
            kind: InventoryRefreshKind::MediaChanged,
            scope_hash: Some(scope_hash),
            media_dirs,
            changed_paths,
            requested_at_ms,
        }
    }

    pub fn client(requested_at_ms: i64) -> Self {
        Self {
            kind: InventoryRefreshKind::Client,
            scope_hash: None,
            media_dirs: Vec::new(),
            changed_paths: Vec::new(),
            requested_at_ms,
        }
    }

    pub fn from_inventory_request(request: InventoryRefreshRequest, requested_at_ms: i64) -> Self {
        if request.changed_paths.is_empty() {
            Self::media_full(request.media_dirs, requested_at_ms)
        } else {
            Self::media_changed(request.media_dirs, request.changed_paths, requested_at_ms)
        }
    }

    fn instance_id(
        &self,
    ) -> Result<WorkflowInstanceId, crate::runtime::workflow_contracts::WorkflowContractError> {
        WorkflowInstanceId::inventory_refresh(self.kind, self.scope_hash.as_deref())
    }

    fn workflow_input(&self) -> InventoryRefreshWorkflowInput {
        InventoryRefreshWorkflowInput {
            kind: self.kind,
            scope_hash: self.scope_hash.clone(),
            requested_at_ms: self.requested_at_ms,
        }
    }

    fn activity_kind(&self) -> ActivityKind {
        match self.kind {
            InventoryRefreshKind::MediaFull | InventoryRefreshKind::MediaChanged => {
                ActivityKind::InventoryScanMedia
            }
            InventoryRefreshKind::Client => ActivityKind::InventoryRefreshClient,
        }
    }

    fn public_id(&self) -> String {
        match self.kind {
            InventoryRefreshKind::MediaFull => "media:full".to_owned(),
            InventoryRefreshKind::MediaChanged => {
                let scope_hash = self.scope_hash.as_deref().unwrap_or("unknown");
                format!("media:changed:{scope_hash}")
            }
            InventoryRefreshKind::Client => "client".to_owned(),
        }
    }

    fn media_request(&self) -> InventoryRefreshRequest {
        if self.changed_paths.is_empty() {
            InventoryRefreshRequest::full(self.media_dirs.clone())
        } else {
            InventoryRefreshRequest::changed_paths(
                self.media_dirs.clone(),
                self.changed_paths.clone(),
            )
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct InventoryWorkflowSubmission {
    pub workflow_id: String,
    pub outcome: InventoryWorkflowSubmissionOutcome,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum InventoryWorkflowSubmissionOutcome {
    Queued,
    Coalesced,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
struct InventoryActivityInput {
    request: InventoryWorkflowRequest,
    started_at_ms: i64,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
struct InventoryActivityOutput {
    scanned_items: usize,
    persisted_items: usize,
    pruned_items: u64,
    scan_failure_count: usize,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct InventoryProjectionRecord<'a> {
    workflow_id: &'a str,
    public_id: &'a str,
    state: WorkflowState,
    reason: WorkflowReason,
    next_action: Option<&'a str>,
    started_at_ms: i64,
    updated_at_ms: i64,
    finished_at_ms: Option<i64>,
    blocked_dependency_name: Option<&'a str>,
}

pub fn workflow_database_path(sporos_database_path: &Path) -> PathBuf {
    let parent = sporos_database_path
        .parent()
        .unwrap_or_else(|| Path::new("."));
    let state_root = if parent
        .file_name()
        .is_some_and(|name| name == DEFAULT_DATABASE_DIR)
    {
        parent.parent().unwrap_or(parent)
    } else {
        parent
    };
    state_root.join(WORKFLOW_DATABASE_FILE)
}

pub fn workflow_runtime_dependency_name() -> Result<DependencyName, DuroxideWorkflowRuntimeError> {
    DependencyName::new(WORKFLOW_RUNTIME_DEPENDENCY).map_err(|error| {
        DuroxideWorkflowRuntimeError::InvalidDependencyName {
            message: error.to_string(),
        }
    })
}

async fn prepare_workflow_database(path: &Path) -> Result<(), DuroxideWorkflowRuntimeError> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await.map_err(|error| {
            DuroxideWorkflowRuntimeError::PrepareDatabase {
                path: path.to_path_buf(),
                message: error.to_string(),
            }
        })?;
    }
    tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
        .map_err(|error| DuroxideWorkflowRuntimeError::PrepareDatabase {
            path: path.to_path_buf(),
            message: error.to_string(),
        })?;
    Ok(())
}

fn activity_registry() -> ActivityRegistry {
    let mut builder = ActivityRegistry::builder();
    for activity in ActivityKind::ALL {
        builder = builder.register_typed(
            activity.as_str(),
            move |_ctx: ActivityContext, input: WorkflowShellActivityInput| async move {
                Ok(WorkflowShellActivityOutput {
                    activity: input.activity,
                    accepted: input.activity == activity,
                })
            },
        );
    }
    builder.build()
}

fn activity_registry_with_inventory_activities(
    activities: InventoryWorkflowActivities,
    active_inventory_refreshes: Arc<Mutex<BTreeSet<String>>>,
) -> ActivityRegistry {
    let mut builder = ActivityRegistry::builder();
    for activity in ActivityKind::ALL {
        match activity {
            ActivityKind::InventoryScanMedia | ActivityKind::InventoryRefreshClient => {
                let activities = activities.clone();
                let active_inventory_refreshes = Arc::clone(&active_inventory_refreshes);
                builder = builder.register_typed(
                    activity.as_str(),
                    move |_ctx: ActivityContext,
                          input: ActivityInputEnvelope<InventoryActivityInput>| {
                        let activities = activities.clone();
                        let active_inventory_refreshes = Arc::clone(&active_inventory_refreshes);
                        async move {
                            run_inventory_activity(
                                activities,
                                active_inventory_refreshes,
                                input.workflow_id,
                                input.payload,
                            )
                            .await
                        }
                    },
                );
            }
            _ => {
                builder = builder.register_typed(
                    activity.as_str(),
                    move |_ctx: ActivityContext, input: WorkflowShellActivityInput| async move {
                        Ok(WorkflowShellActivityOutput {
                            activity: input.activity,
                            accepted: input.activity == activity,
                        })
                    },
                );
            }
        }
    }
    builder.build()
}

async fn run_inventory_activity(
    activities: InventoryWorkflowActivities,
    active_inventory_refreshes: Arc<Mutex<BTreeSet<String>>>,
    workflow_id: String,
    input: InventoryActivityInput,
) -> Result<InventoryActivityOutput, String> {
    let public_id = input.request.public_id();
    record_inventory_activity_projection(
        &activities.repository,
        InventoryProjectionRecord {
            workflow_id: &workflow_id,
            public_id: &public_id,
            state: WorkflowState::Running,
            reason: WorkflowReason::RunningActivity,
            next_action: Some("refreshing"),
            started_at_ms: input.started_at_ms,
            updated_at_ms: unix_time_ms(),
            finished_at_ms: None,
            blocked_dependency_name: None,
        },
    )
    .await?;

    let result = match input.request.kind {
        InventoryRefreshKind::MediaFull | InventoryRefreshKind::MediaChanged => activities
            .inventory_refresh
            .refresh_data_dirs_until_shutdown(
                input.request.media_request(),
                activities.shutdown.clone(),
            )
            .await
            .map(|summary| vec![summary]),
        InventoryRefreshKind::Client => {
            activities
                .injection_worker
                .refresh_client_inventories_until_shutdown(
                    &activities.inventory_refresh,
                    activities.shutdown.clone(),
                )
                .await
        }
    };
    let finished_at_ms = unix_time_ms();
    let output = match result {
        Ok(summaries) => {
            let output = inventory_activity_output(&summaries);
            if output.scan_failure_count == 0 {
                record_inventory_refresh_health(&activities.inventory_refresh, None, None).await;
                record_inventory_activity_projection(
                    &activities.repository,
                    InventoryProjectionRecord {
                        workflow_id: &workflow_id,
                        public_id: &public_id,
                        state: WorkflowState::Succeeded,
                        reason: WorkflowReason::Completed,
                        next_action: Some("completed"),
                        started_at_ms: input.started_at_ms,
                        updated_at_ms: finished_at_ms,
                        finished_at_ms: Some(finished_at_ms),
                        blocked_dependency_name: None,
                    },
                )
                .await?;
                Ok(output)
            } else {
                let failures = summaries
                    .iter()
                    .flat_map(|summary| summary.scan_failures.iter().cloned())
                    .collect::<Vec<_>>();
                let reason = scan_failure_reason(&failures);
                record_inventory_refresh_health(
                    &activities.inventory_refresh,
                    Some(reason.clone()),
                    Some(activities.failure_backoff),
                )
                .await;
                record_inventory_activity_projection(
                    &activities.repository,
                    InventoryProjectionRecord {
                        workflow_id: &workflow_id,
                        public_id: &public_id,
                        state: WorkflowState::Retrying,
                        reason: WorkflowReason::BackingOff,
                        next_action: Some("scan_failures"),
                        started_at_ms: input.started_at_ms,
                        updated_at_ms: finished_at_ms,
                        finished_at_ms: None,
                        blocked_dependency_name: Some(reason.as_str()),
                    },
                )
                .await?;
                Ok(output)
            }
        }
        Err(_error) if activities.shutdown.state().phase != ShutdownPhase::Running => {
            record_inventory_activity_projection(
                &activities.repository,
                InventoryProjectionRecord {
                    workflow_id: &workflow_id,
                    public_id: &public_id,
                    state: WorkflowState::Cancelled,
                    reason: WorkflowReason::Cancelled,
                    next_action: Some("shutdown"),
                    started_at_ms: input.started_at_ms,
                    updated_at_ms: finished_at_ms,
                    finished_at_ms: Some(finished_at_ms),
                    blocked_dependency_name: None,
                },
            )
            .await?;
            Ok(InventoryActivityOutput {
                scanned_items: 0,
                persisted_items: 0,
                pruned_items: 0,
                scan_failure_count: 1,
            })
        }
        Err(error) => {
            let reason = error.to_string();
            record_inventory_refresh_health(
                &activities.inventory_refresh,
                Some(reason.clone()),
                Some(activities.failure_backoff),
            )
            .await;
            record_inventory_activity_projection(
                &activities.repository,
                InventoryProjectionRecord {
                    workflow_id: &workflow_id,
                    public_id: &public_id,
                    state: WorkflowState::Retrying,
                    reason: WorkflowReason::BackingOff,
                    next_action: Some("retry_after_failure"),
                    started_at_ms: input.started_at_ms,
                    updated_at_ms: finished_at_ms,
                    finished_at_ms: None,
                    blocked_dependency_name: Some(reason.as_str()),
                },
            )
            .await?;
            Ok(InventoryActivityOutput {
                scanned_items: 0,
                persisted_items: 0,
                pruned_items: 0,
                scan_failure_count: 1,
            })
        }
    };
    if let Ok(mut active) = active_inventory_refreshes.lock() {
        active.remove(&workflow_id);
    }
    output
}

async fn record_inventory_activity_projection(
    repository: &Repository,
    record: InventoryProjectionRecord<'_>,
) -> Result<(), String> {
    repository
        .record_workflow_projection(&WorkflowProjectionUpdate {
            workflow_id: record.workflow_id,
            workflow_kind: WorkflowKind::InventoryRefresh,
            public_id: record.public_id,
            state: record.state,
            reason: record.reason,
            next_action: record.next_action,
            blocked_dependency: record.blocked_dependency_name.map(|name| {
                WorkflowProjectionDependency {
                    kind: DependencyKind::LocalState,
                    name,
                }
            }),
            raw_secret_material_count: 0,
            started_at_ms: record.started_at_ms,
            updated_at_ms: record.updated_at_ms,
            finished_at_ms: record.finished_at_ms,
        })
        .await
        .map_err(|error| error.to_string())
}

fn inventory_activity_output(summaries: &[InventoryRefreshSummary]) -> InventoryActivityOutput {
    InventoryActivityOutput {
        scanned_items: summaries.iter().map(|summary| summary.scanned_items).sum(),
        persisted_items: summaries
            .iter()
            .map(|summary| summary.persisted_items)
            .sum(),
        pruned_items: summaries.iter().map(|summary| summary.pruned_items).sum(),
        scan_failure_count: summaries
            .iter()
            .map(|summary| summary.scan_failures.len())
            .sum(),
    }
}

fn orchestration_registry() -> OrchestrationRegistry {
    let mut builder = OrchestrationRegistry::builder();
    for workflow in WorkflowKind::ALL {
        if workflow == WorkflowKind::InventoryRefresh {
            builder = builder.register_typed(
                workflow.orchestration_name(),
                inventory_refresh_orchestration,
            );
        } else {
            builder = builder.register_typed(
                workflow.orchestration_name(),
                move |ctx: OrchestrationContext, input: WorkflowSupervisorInput| async move {
                    let status = WorkflowCustomStatus::new(
                        input.public_id.clone(),
                        input.kind,
                        WorkflowState::Succeeded,
                        WorkflowReason::Completed,
                    );
                    let status =
                        serde_json::to_string(&status).map_err(|error| error.to_string())?;
                    ctx.set_custom_status(status);
                    Ok(WorkflowSupervisorOutput {
                        kind: input.kind,
                        public_id: input.public_id,
                        state: WorkflowState::Succeeded,
                    })
                },
            );
        }
    }
    builder.build()
}

async fn inventory_refresh_orchestration(
    ctx: OrchestrationContext,
    input: InventoryRefreshWorkflowInput,
) -> Result<String, String> {
    set_inventory_custom_status(
        &ctx,
        &input,
        WorkflowState::Waiting,
        WorkflowReason::WaitingForInventory,
        Some("await_request"),
    )?;
    let request: InventoryWorkflowRequest = ctx.dequeue_event_typed(INVENTORY_REFRESH_QUEUE).await;
    set_inventory_custom_status(
        &ctx,
        &input,
        WorkflowState::Running,
        WorkflowReason::RunningActivity,
        Some("refreshing"),
    )?;
    let workflow_id =
        WorkflowInstanceId::inventory_refresh(request.kind, request.scope_hash.as_deref())
            .map_err(|error| error.to_string())?
            .to_string();
    let activity_input = ActivityInputEnvelope::new(
        workflow_id,
        INVENTORY_REFRESH_ACTIVITY_ID,
        InventoryActivityInput {
            request: request.clone(),
            started_at_ms: request.requested_at_ms,
        },
    );
    let output: InventoryActivityOutput = ctx
        .schedule_activity_typed(request.activity_kind().as_str(), &activity_input)
        .await?;
    if output.scan_failure_count == 0 {
        set_inventory_custom_status(
            &ctx,
            &input,
            WorkflowState::Succeeded,
            WorkflowReason::Completed,
            Some("completed"),
        )?;
    } else {
        set_inventory_custom_status(
            &ctx,
            &input,
            WorkflowState::Retrying,
            WorkflowReason::BackingOff,
            Some("retry_after_failure"),
        )?;
    }
    ctx.continue_as_new_typed(&input).await
}

fn set_inventory_custom_status(
    ctx: &OrchestrationContext,
    input: &InventoryRefreshWorkflowInput,
    state: WorkflowState,
    reason: WorkflowReason,
    next_action: Option<&str>,
) -> Result<(), String> {
    let mut status = WorkflowCustomStatus::new(
        inventory_public_id(input.kind, input.scope_hash.as_deref()),
        WorkflowKind::InventoryRefresh,
        state,
        reason,
    );
    status.next_action = next_action.map(str::to_owned);
    let status = serde_json::to_string(&status).map_err(|error| error.to_string())?;
    ctx.set_custom_status(status);
    Ok(())
}

fn inventory_public_id(kind: InventoryRefreshKind, scope_hash: Option<&str>) -> String {
    match kind {
        InventoryRefreshKind::MediaFull => "media:full".to_owned(),
        InventoryRefreshKind::MediaChanged => {
            let scope_hash = scope_hash.unwrap_or("unknown");
            format!("media:changed:{scope_hash}")
        }
        InventoryRefreshKind::Client => "client".to_owned(),
    }
}

fn changed_paths_scope_hash(paths: &[PathBuf]) -> String {
    let mut normalized = paths
        .iter()
        .map(|path| path.display().to_string())
        .collect::<Vec<_>>();
    normalized.sort();
    stable_hash_hex(&normalized.join("\n"))
}

fn stable_hash_hex(value: &str) -> String {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, Instant};

    use duroxide::runtime::OrchestrationStatus;

    use super::*;
    use crate::inventory::InventoryScanOptions;
    use crate::persistence::repository::Repository;
    use crate::runtime::health::HealthRegistry;
    use crate::runtime::injection_worker::InjectionWorker;
    use crate::runtime::scheduler::{
        CLEANUP_JOB_NAME, CLIENT_INVENTORY_JOB_NAME, INDEXER_CAPS_JOB_NAME,
        MEDIA_INVENTORY_JOB_NAME,
    };
    use crate::runtime::shutdown::shutdown_channel;

    static TEMP_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn workflow_database_path_uses_state_root_for_default_database_layout() {
        assert_eq!(
            PathBuf::from("/app/state/sporos-workflows.db"),
            workflow_database_path(Path::new("/app/state/db/sporos.db"))
        );
    }

    #[test]
    fn workflow_database_path_uses_database_parent_for_custom_layout() {
        assert_eq!(
            PathBuf::from("/var/lib/sporos/sporos-workflows.db"),
            workflow_database_path(Path::new("/var/lib/sporos/sporos.db"))
        );
    }

    #[test]
    fn inventory_workflow_request_uses_deterministic_instances_and_scope_hashes() {
        let first = InventoryWorkflowRequest::media_changed(
            vec![PathBuf::from("/media")],
            vec![
                PathBuf::from("/media/show/b"),
                PathBuf::from("/media/show/a"),
            ],
            100,
        );
        let second = InventoryWorkflowRequest::media_changed(
            vec![PathBuf::from("/media")],
            vec![
                PathBuf::from("/media/show/a"),
                PathBuf::from("/media/show/b"),
            ],
            200,
        );

        assert_eq!(first.scope_hash, second.scope_hash);
        assert_eq!(
            first.instance_id().unwrap().as_str(),
            second.instance_id().unwrap().as_str()
        );
        assert_eq!(
            "inventory:media:full",
            InventoryWorkflowRequest::media_full(vec![PathBuf::from("/media")], 100)
                .instance_id()
                .unwrap()
                .as_str()
        );
        assert_eq!(
            "inventory:client",
            InventoryWorkflowRequest::client(100)
                .instance_id()
                .unwrap()
                .as_str()
        );
    }

    #[tokio::test]
    async fn runtime_starts_seeds_supervisors_idempotently_and_shuts_down() {
        let temp_dir = TestTempDir::new("duroxide-workflow-runtime");
        let database_path = temp_dir.path().join("state").join(WORKFLOW_DATABASE_FILE);
        let runtime = DuroxideWorkflowRuntime::start(database_path.clone())
            .await
            .expect("workflow runtime should start");

        assert_eq!(database_path, runtime.database_path());
        assert!(database_path.exists());

        let scheduled_jobs = [
            CLEANUP_JOB_NAME,
            MEDIA_INVENTORY_JOB_NAME,
            CLIENT_INVENTORY_JOB_NAME,
            INDEXER_CAPS_JOB_NAME,
        ];
        let first_summary = runtime
            .seed_supervisors(&scheduled_jobs)
            .await
            .expect("first supervisor seed should succeed");
        assert_eq!(
            WorkflowSupervisorSeedSummary {
                started: 5,
                already_present: 0
            },
            first_summary
        );

        let second_summary = runtime
            .seed_supervisors(&scheduled_jobs)
            .await
            .expect("second supervisor seed should succeed");
        assert_eq!(
            WorkflowSupervisorSeedSummary {
                started: 0,
                already_present: 5
            },
            second_summary
        );

        let cleanup_id = WorkflowInstanceId::scheduled_job_supervisor(CLEANUP_JOB_NAME).unwrap();
        wait_for_supervisor_completion(&runtime.client(), cleanup_id.as_str()).await;

        runtime.shutdown(Some(1_000)).await;
    }

    #[tokio::test]
    async fn inventory_refresh_workflow_runs_media_activity_and_updates_projection() {
        let temp_dir = TestTempDir::new("duroxide-inventory-workflow");
        let media_dir = temp_dir.path().join("media");
        std::fs::create_dir_all(&media_dir).unwrap();
        std::fs::write(media_dir.join("Example.Movie.2025.mkv"), b"movie").unwrap();
        let repository = Repository::connect(temp_dir.path().join("sporos.sqlite"))
            .await
            .unwrap();
        let inventory_refresh =
            InventoryRefreshWorker::new(repository.clone(), InventoryScanOptions::default())
                .with_health_registry(HealthRegistry::new());
        let injection_worker = InjectionWorker::new(repository.clone(), Vec::new());
        let (_shutdown, shutdown_signal) = shutdown_channel();
        let runtime = DuroxideWorkflowRuntime::start_with_inventory_activities(
            temp_dir.path().join("state").join(WORKFLOW_DATABASE_FILE),
            InventoryWorkflowActivities::new(
                repository.clone(),
                inventory_refresh,
                injection_worker,
                shutdown_signal,
                Duration::from_secs(60),
            ),
        )
        .await
        .unwrap();

        let submission = runtime
            .submit_inventory_refresh(InventoryWorkflowRequest::media_full(vec![media_dir], 1_000))
            .await
            .unwrap();
        assert_eq!(
            InventoryWorkflowSubmissionOutcome::Queued,
            submission.outcome
        );

        wait_for_inventory_projection_state(
            &repository,
            &submission.workflow_id,
            WorkflowState::Succeeded,
        )
        .await;
        let snapshot = repository
            .workflow_projection_snapshot(10, unix_time_ms())
            .await
            .unwrap();
        let item = snapshot
            .recent
            .iter()
            .find(|item| item.workflow_id == submission.workflow_id)
            .unwrap();
        assert_eq!("inventory_refresh", item.workflow_kind);
        assert_eq!("succeeded", item.state);
        assert_eq!("completed", item.reason);
        assert_eq!(Some("completed".to_owned()), item.next_action);
        assert!(item.finished_at_ms.is_some());

        runtime.shutdown(Some(1_000)).await;
    }

    #[tokio::test]
    async fn inventory_refresh_submission_coalesces_duplicate_active_request() {
        let temp_dir = TestTempDir::new("duroxide-inventory-coalesce");
        let runtime = DuroxideWorkflowRuntime::start(
            temp_dir.path().join("state").join(WORKFLOW_DATABASE_FILE),
        )
        .await
        .unwrap();

        let first = runtime
            .submit_inventory_refresh(InventoryWorkflowRequest::client(1_000))
            .await
            .unwrap();
        let second = runtime
            .submit_inventory_refresh(InventoryWorkflowRequest::client(2_000))
            .await
            .unwrap();

        assert_eq!(first.workflow_id, second.workflow_id);
        assert_eq!(InventoryWorkflowSubmissionOutcome::Queued, first.outcome);
        assert_eq!(
            InventoryWorkflowSubmissionOutcome::Coalesced,
            second.outcome
        );

        runtime.shutdown(Some(1_000)).await;
    }

    #[tokio::test]
    async fn active_full_media_refresh_coalesces_changed_path_refresh() {
        let temp_dir = TestTempDir::new("duroxide-inventory-full-coalesces-changed");
        let runtime = DuroxideWorkflowRuntime::start(
            temp_dir.path().join("state").join(WORKFLOW_DATABASE_FILE),
        )
        .await
        .unwrap();

        let full = runtime
            .submit_inventory_refresh(InventoryWorkflowRequest::media_full(
                vec![PathBuf::from("/media")],
                1_000,
            ))
            .await
            .unwrap();
        let changed = runtime
            .submit_inventory_refresh(InventoryWorkflowRequest::media_changed(
                vec![PathBuf::from("/media")],
                vec![PathBuf::from("/media/show/episode.mkv")],
                2_000,
            ))
            .await
            .unwrap();

        assert_eq!("inventory:media:full", full.workflow_id);
        assert_eq!(full.workflow_id, changed.workflow_id);
        assert_eq!(InventoryWorkflowSubmissionOutcome::Queued, full.outcome);
        assert_eq!(
            InventoryWorkflowSubmissionOutcome::Coalesced,
            changed.outcome
        );

        runtime.shutdown(Some(1_000)).await;
    }

    #[tokio::test]
    async fn inventory_refresh_failure_does_not_prevent_later_refresh() {
        let temp_dir = TestTempDir::new("duroxide-inventory-retry-after-failure");
        let missing = temp_dir.path().join("missing");
        let repository = Repository::connect(temp_dir.path().join("sporos.sqlite"))
            .await
            .unwrap();
        let inventory_refresh =
            InventoryRefreshWorker::new(repository.clone(), InventoryScanOptions::default());
        let injection_worker = InjectionWorker::new(repository.clone(), Vec::new());
        let (_shutdown, shutdown_signal) = shutdown_channel();
        let runtime = DuroxideWorkflowRuntime::start_with_inventory_activities(
            temp_dir.path().join("state").join(WORKFLOW_DATABASE_FILE),
            InventoryWorkflowActivities::new(
                repository.clone(),
                inventory_refresh,
                injection_worker,
                shutdown_signal,
                Duration::from_secs(60),
            ),
        )
        .await
        .unwrap();

        let first = runtime
            .submit_inventory_refresh(InventoryWorkflowRequest::media_full(
                vec![missing.clone()],
                1_000,
            ))
            .await
            .unwrap();
        wait_for_inventory_projection_state(
            &repository,
            &first.workflow_id,
            WorkflowState::Retrying,
        )
        .await;

        std::fs::create_dir_all(&missing).unwrap();
        std::fs::write(missing.join("Recovered.Movie.2026.mkv"), b"movie").unwrap();
        let second = runtime
            .submit_inventory_refresh(InventoryWorkflowRequest::media_full(vec![missing], 2_000))
            .await
            .unwrap();

        assert_eq!(first.workflow_id, second.workflow_id);
        assert_eq!(InventoryWorkflowSubmissionOutcome::Queued, second.outcome);
        wait_for_inventory_projection_state(
            &repository,
            &second.workflow_id,
            WorkflowState::Succeeded,
        )
        .await;

        runtime.shutdown(Some(1_000)).await;
    }

    #[tokio::test]
    async fn runtime_rejects_failed_supervisor_as_not_seeded() {
        let temp_dir = TestTempDir::new("duroxide-workflow-runtime-failed-supervisor");
        let database_path = temp_dir.path().join("state").join(WORKFLOW_DATABASE_FILE);
        let runtime = DuroxideWorkflowRuntime::start(database_path)
            .await
            .expect("workflow runtime should start");
        let failed_job = "failed_job";
        let failed_id = WorkflowInstanceId::scheduled_job_supervisor(failed_job).unwrap();

        runtime
            .client()
            .start_orchestration_typed(
                failed_id.as_str(),
                WorkflowKind::ScheduledJob.orchestration_name(),
                "not a supervisor input".to_owned(),
            )
            .await
            .expect("failed supervisor should be queued");
        wait_for_supervisor_failure(&runtime.client(), failed_id.as_str()).await;

        let error = runtime
            .seed_supervisors(&[failed_job])
            .await
            .expect_err("failed supervisor must not be treated as seeded");
        assert!(matches!(
            error,
            DuroxideWorkflowRuntimeError::FailedSupervisor { .. }
        ));

        runtime.shutdown(Some(1_000)).await;
    }

    #[test]
    fn workflow_runtime_dependency_name_is_stable() {
        assert_eq!(
            WORKFLOW_RUNTIME_DEPENDENCY,
            workflow_runtime_dependency_name().unwrap().as_str()
        );
    }

    async fn wait_for_supervisor_completion(client: &Client, instance_id: &str) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match client
                .get_orchestration_status(instance_id)
                .await
                .expect("supervisor status should be readable")
            {
                OrchestrationStatus::Completed { .. } => return,
                OrchestrationStatus::Failed { details, .. } => {
                    panic!(
                        "supervisor failed unexpectedly: {}",
                        details.display_message()
                    );
                }
                OrchestrationStatus::NotFound | OrchestrationStatus::Running { .. } => {}
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for supervisor completion"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn wait_for_supervisor_failure(client: &Client, instance_id: &str) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match client
                .get_orchestration_status(instance_id)
                .await
                .expect("supervisor status should be readable")
            {
                OrchestrationStatus::Failed { .. } => return,
                OrchestrationStatus::Completed { .. } => {
                    panic!("supervisor completed unexpectedly");
                }
                OrchestrationStatus::NotFound | OrchestrationStatus::Running { .. } => {}
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for supervisor failure"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn wait_for_inventory_projection_state(
        repository: &Repository,
        workflow_id: &str,
        state: WorkflowState,
    ) {
        let expected = state.as_str();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let snapshot = repository
                .workflow_projection_snapshot(10, unix_time_ms())
                .await
                .expect("workflow projection should be readable");
            if snapshot
                .recent
                .iter()
                .any(|item| item.workflow_id == workflow_id && item.state == expected)
            {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for inventory workflow projection state {expected}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    struct TestTempDir {
        path: PathBuf,
    }

    impl TestTempDir {
        fn new(label: &str) -> Self {
            let unique = TEMP_DIR_COUNTER.fetch_add(1, Ordering::SeqCst);
            let path = std::env::temp_dir()
                .join(format!("sporos-{label}-{}-{unique}", std::process::id()));
            std::fs::create_dir_all(&path).expect("test temp directory should be created");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestTempDir {
        fn drop(&mut self) {
            drop(std::fs::remove_dir_all(&self.path));
        }
    }
}
