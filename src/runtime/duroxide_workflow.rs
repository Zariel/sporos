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

#[derive(Clone)]
pub struct DuroxideWorkflowRuntime {
    database_path: PathBuf,
    repository: Option<Repository>,
    store: Arc<dyn Provider>,
    runtime: Arc<Runtime>,
    seeded_supervisors: Arc<Mutex<BTreeSet<String>>>,
    active_inventory_refreshes: Arc<Mutex<BTreeSet<String>>>,
    inventory_completion_events: InventoryCompletionEventBridge,
    scheduled_job_state: Option<ScheduledJobStateHandle>,
    scheduled_job_scheduler: Option<PersistedScheduler>,
    scheduled_job_shutdown: Option<ShutdownSignal>,
    search_state: Option<SearchWorkflowStateHandle>,
    saved_retry_state: Option<SavedRetryWorkflowStateHandle>,
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
        Self::start_inner(
            database_path,
            Some(WorkflowRuntimeActivities {
                repository,
                inventory: Some(activities),
                announce: None,
                scheduled_jobs: None,
                search: None,
                saved_retry: None,
            }),
        )
        .await
    }

    pub async fn start_with_activities(
        database_path: PathBuf,
        inventory: InventoryWorkflowActivities,
        announce: AnnounceWorkflowActivities,
        scheduled_jobs: ScheduledJobWorkflowActivities,
        search: SearchWorkflowActivities,
        saved_retry: SavedRetryWorkflowActivities,
    ) -> Result<Self, DuroxideWorkflowRuntimeError> {
        let repository = inventory.repository.clone();
        Self::start_inner(
            database_path,
            Some(WorkflowRuntimeActivities {
                repository,
                inventory: Some(inventory),
                announce: Some(announce),
                scheduled_jobs: Some(scheduled_jobs),
                search: Some(search),
                saved_retry: Some(saved_retry),
            }),
        )
        .await
    }

    async fn start_inner(
        database_path: PathBuf,
        activities: Option<WorkflowRuntimeActivities>,
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
        let repository = activities
            .as_ref()
            .map(|activities| activities.repository.clone());
        let scheduled_job_state = activities
            .as_ref()
            .and_then(|activities| activities.scheduled_jobs.as_ref())
            .map(|activities| activities.state.clone());
        let scheduled_job_scheduler = activities
            .as_ref()
            .and_then(|activities| activities.scheduled_jobs.as_ref())
            .map(|activities| activities.scheduler.clone());
        let scheduled_job_shutdown = activities
            .as_ref()
            .and_then(|activities| activities.scheduled_jobs.as_ref())
            .map(|activities| activities.shutdown.clone());
        let search_state = activities
            .as_ref()
            .and_then(|activities| activities.search.as_ref())
            .map(|activities| activities.state.clone());
        let saved_retry_state = activities
            .as_ref()
            .and_then(|activities| activities.saved_retry.as_ref())
            .map(|activities| activities.state.clone());
        let active_inventory_refreshes = Arc::new(Mutex::new(BTreeSet::new()));
        let inventory_completion_events =
            InventoryCompletionEventBridge::new(Arc::clone(&store), repository.clone());
        let activity_registry = match activities {
            Some(activities) => activity_registry_with_runtime_activities(
                activities.with_completion_event_bridge(inventory_completion_events.clone()),
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
                orchestration_concurrency: 2,
                worker_concurrency: 2,
                ..RuntimeOptions::default()
            },
        )
        .await;

        let runtime = Self {
            database_path,
            repository,
            store,
            runtime,
            seeded_supervisors: Arc::new(Mutex::new(BTreeSet::new())),
            active_inventory_refreshes,
            inventory_completion_events,
            scheduled_job_state,
            scheduled_job_scheduler,
            scheduled_job_shutdown,
            search_state,
            saved_retry_state,
        };
        if let Err(error) = runtime
            .inventory_completion_events
            .drain_persisted_completions()
            .await
        {
            tracing::warn!(
                error = %error,
                "persisted inventory completion drain failed during workflow runtime startup"
            );
        }
        Ok(runtime)
    }

    pub fn database_path(&self) -> &Path {
        &self.database_path
    }

    pub fn client(&self) -> Client {
        Client::new(Arc::clone(&self.store))
    }

    pub fn without_scheduled_job_state(&self) -> Self {
        Self {
            database_path: self.database_path.clone(),
            repository: self.repository.clone(),
            store: Arc::clone(&self.store),
            runtime: Arc::clone(&self.runtime),
            seeded_supervisors: Arc::clone(&self.seeded_supervisors),
            active_inventory_refreshes: Arc::clone(&self.active_inventory_refreshes),
            inventory_completion_events: self.inventory_completion_events.clone(),
            scheduled_job_state: None,
            scheduled_job_scheduler: self.scheduled_job_scheduler.clone(),
            scheduled_job_shutdown: self.scheduled_job_shutdown.clone(),
            search_state: None,
            saved_retry_state: None,
        }
    }

    pub async fn submit_scheduled_job_run(
        &self,
        job_name: &JobName,
        forced: bool,
        requested_at_ms: i64,
    ) -> Result<(), DuroxideWorkflowRuntimeError> {
        let claimed_scheduled_at_ms = if let Some(scheduler) = &self.scheduled_job_scheduler {
            match retry_scheduler_call(
                "claim manual scheduled job run",
                self.scheduled_job_shutdown.as_ref(),
                || {
                    let scheduler = scheduler.clone();
                    let job_name = job_name.clone();
                    async move {
                        scheduler
                            .claim_manual_run(&job_name, requested_at_ms, forced)
                            .await
                    }
                },
            )
            .await
            .map_err(|message| DuroxideWorkflowRuntimeError::StartSupervisor {
                instance_id: job_name.as_str().to_owned(),
                message,
            })? {
                ScheduledJobClaimOutcome::Claimed => Some(requested_at_ms),
                ScheduledJobClaimOutcome::Coalesced
                | ScheduledJobClaimOutcome::BackingOff { .. } => return Ok(()),
            }
        } else {
            None
        };
        let instance_id = WorkflowInstanceId::scheduled_job_supervisor(job_name.as_str())
            .map_err(DuroxideWorkflowRuntimeError::InvalidSupervisorId)?;
        let enqueue_result = self
            .client()
            .enqueue_event_typed(
                instance_id.as_str(),
                SCHEDULED_JOB_MANUAL_QUEUE,
                &ScheduledJobManualRequest {
                    requested_at_ms,
                    forced,
                    claimed_scheduled_at_ms,
                },
            )
            .await
            .map_err(|error| DuroxideWorkflowRuntimeError::StartSupervisor {
                instance_id: instance_id.to_string(),
                message: error.to_string(),
            });
        if enqueue_result.is_err()
            && claimed_scheduled_at_ms.is_some()
            && let Some(scheduler) = &self.scheduled_job_scheduler
            && let Err(error) = retry_scheduler_call("complete scheduled job failure", None, || {
                let scheduler = scheduler.clone();
                let job_name = job_name.clone();
                async move {
                    scheduler
                        .complete_failure(
                            &job_name,
                            unix_time_ms(),
                            "scheduled job trigger enqueue failed",
                        )
                        .await
                }
            })
            .await
        {
            tracing::warn!(
                job_name = %job_name,
                error = %error,
                "failed to release scheduled job claim after trigger enqueue failure"
            );
        }
        enqueue_result
    }

    pub async fn register_inventory_completion_waiter(
        &self,
        waiter: InventoryCompletionWaiter,
    ) -> Result<InventoryCompletionWaitRegistration, DuroxideWorkflowRuntimeError> {
        self.inventory_completion_events
            .register_waiter(waiter)
            .await
            .map_err(|message| DuroxideWorkflowRuntimeError::InventoryCompletionBridge { message })
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

    pub async fn submit_announcement(
        &self,
        work: &AnnounceWorkItem,
    ) -> Result<AnnounceWorkflowSubmission, DuroxideWorkflowRuntimeError> {
        let instance_id = WorkflowInstanceId::announce(work.id.as_str())
            .map_err(DuroxideWorkflowRuntimeError::InvalidAnnounceWorkflowId)?;
        let instance_id = instance_id.as_str().to_owned();
        let input = announce_workflow_input(work);
        let client = self.client();
        match client
            .get_orchestration_status(&instance_id)
            .await
            .map_err(|error| DuroxideWorkflowRuntimeError::ReadAnnounceWorkflow {
                instance_id: instance_id.clone(),
                message: error.to_string(),
            })? {
            OrchestrationStatus::NotFound => {
                client
                    .start_orchestration_typed(
                        &instance_id,
                        WorkflowKind::Announce.orchestration_name(),
                        input,
                    )
                    .await
                    .map_err(
                        |error| DuroxideWorkflowRuntimeError::StartAnnounceWorkflow {
                            instance_id: instance_id.clone(),
                            message: error.to_string(),
                        },
                    )?;
                record_announce_start_projection(
                    &self.repository,
                    &instance_id,
                    work,
                    unix_time_ms(),
                )
                .await
                .unwrap_or_else(|error| {
                    tracing::warn!(
                        workflow_id = %instance_id,
                        error = %error,
                        "failed to record initial announce workflow projection after workflow start"
                    );
                });
                Ok(AnnounceWorkflowSubmission {
                    workflow_id: instance_id,
                    outcome: AnnounceWorkflowSubmissionOutcome::Started,
                })
            }
            OrchestrationStatus::Running { .. } => Ok(AnnounceWorkflowSubmission {
                workflow_id: instance_id,
                outcome: AnnounceWorkflowSubmissionOutcome::AlreadyRunning,
            }),
            OrchestrationStatus::Completed { .. } => {
                Err(DuroxideWorkflowRuntimeError::CompletedAnnounceWorkflow { instance_id })
            }
            OrchestrationStatus::Failed { details, .. } => {
                Err(DuroxideWorkflowRuntimeError::FailedAnnounceWorkflow {
                    instance_id,
                    message: details.display_message().to_string(),
                })
            }
        }
    }

    pub async fn submit_search(
        &self,
        input: SearchWorkflowInput,
    ) -> Result<SearchWorkflowSubmission, DuroxideWorkflowRuntimeError> {
        let instance_id = WorkflowInstanceId::search(&input.request_id)
            .map_err(DuroxideWorkflowRuntimeError::InvalidSearchWorkflowId)?;
        let instance_id = instance_id.as_str().to_owned();
        let client = self.client();
        match client
            .get_orchestration_status(&instance_id)
            .await
            .map_err(|error| DuroxideWorkflowRuntimeError::ReadSearchWorkflow {
                instance_id: instance_id.clone(),
                message: error.to_string(),
            })? {
            OrchestrationStatus::NotFound => {
                client
                    .start_orchestration_typed(
                        &instance_id,
                        WorkflowKind::Search.orchestration_name(),
                        input,
                    )
                    .await
                    .map_err(|error| DuroxideWorkflowRuntimeError::StartSearchWorkflow {
                        instance_id: instance_id.clone(),
                        message: error.to_string(),
                    })?;
                Ok(SearchWorkflowSubmission {
                    workflow_id: instance_id,
                    outcome: SearchWorkflowSubmissionOutcome::Started,
                })
            }
            OrchestrationStatus::Running { .. } => Ok(SearchWorkflowSubmission {
                workflow_id: instance_id,
                outcome: SearchWorkflowSubmissionOutcome::AlreadyRunning,
            }),
            OrchestrationStatus::Completed { .. } => {
                Err(DuroxideWorkflowRuntimeError::CompletedSearchWorkflow { instance_id })
            }
            OrchestrationStatus::Failed { details, .. } => {
                Err(DuroxideWorkflowRuntimeError::FailedSearchWorkflow {
                    instance_id,
                    message: details.display_message().to_string(),
                })
            }
        }
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
        if let Some(handle) = &self.scheduled_job_state {
            handle.clear();
        }
        if let Some(handle) = &self.search_state {
            handle.clear();
        }
        if let Some(handle) = &self.saved_retry_state {
            handle.clear();
        }
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

impl Drop for DuroxideWorkflowRuntime {
    fn drop(&mut self) {
        let is_last_runtime_owner = Arc::strong_count(&self.runtime) == 1;
        if is_last_runtime_owner && let Some(handle) = &self.scheduled_job_state {
            handle.clear();
        }
        if is_last_runtime_owner && let Some(handle) = &self.search_state {
            handle.clear();
        }
        if is_last_runtime_owner && let Some(handle) = &self.saved_retry_state {
            handle.clear();
        }
        if is_last_runtime_owner && let Ok(handle) = tokio::runtime::Handle::try_current() {
            let runtime = Arc::clone(&self.runtime);
            handle.spawn(async move {
                runtime.shutdown(Some(0)).await;
            });
        }
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
    InvalidAnnounceWorkflowId(crate::runtime::workflow_contracts::WorkflowContractError),
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
    ReadAnnounceWorkflow {
        instance_id: String,
        message: String,
    },
    StartAnnounceWorkflow {
        instance_id: String,
        message: String,
    },
    CompletedAnnounceWorkflow {
        instance_id: String,
    },
    FailedAnnounceWorkflow {
        instance_id: String,
        message: String,
    },
    InvalidSearchWorkflowId(crate::runtime::workflow_contracts::WorkflowContractError),
    ReadSearchWorkflow {
        instance_id: String,
        message: String,
    },
    StartSearchWorkflow {
        instance_id: String,
        message: String,
    },
    CompletedSearchWorkflow {
        instance_id: String,
    },
    FailedSearchWorkflow {
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
    InventoryCompletionBridge {
        message: String,
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
            Self::InvalidAnnounceWorkflowId(error) => write!(formatter, "{error}"),
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
            Self::ReadAnnounceWorkflow {
                instance_id,
                message,
            } => write!(
                formatter,
                "read announce workflow `{instance_id}` failed: {message}"
            ),
            Self::StartAnnounceWorkflow {
                instance_id,
                message,
            } => write!(
                formatter,
                "start announce workflow `{instance_id}` failed: {message}"
            ),
            Self::CompletedAnnounceWorkflow { instance_id } => write!(
                formatter,
                "announce workflow `{instance_id}` completed and cannot accept duplicate work"
            ),
            Self::FailedAnnounceWorkflow {
                instance_id,
                message,
            } => write!(
                formatter,
                "announce workflow `{instance_id}` is failed: {message}"
            ),
            Self::InvalidSearchWorkflowId(error) => write!(formatter, "{error}"),
            Self::ReadSearchWorkflow {
                instance_id,
                message,
            } => write!(
                formatter,
                "read search workflow `{instance_id}` failed: {message}"
            ),
            Self::StartSearchWorkflow {
                instance_id,
                message,
            } => write!(
                formatter,
                "start search workflow `{instance_id}` failed: {message}"
            ),
            Self::CompletedSearchWorkflow { instance_id } => write!(
                formatter,
                "search workflow `{instance_id}` completed and cannot accept duplicate work"
            ),
            Self::FailedSearchWorkflow {
                instance_id,
                message,
            } => write!(
                formatter,
                "search workflow `{instance_id}` is failed: {message}"
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
            Self::InventoryCompletionBridge { message } => {
                write!(
                    formatter,
                    "inventory completion event bridge failed: {message}"
                )
            }
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

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
struct ScheduledJobManualRequest {
    requested_at_ms: i64,
    forced: bool,
    claimed_scheduled_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
struct ScheduledJobClaimActivityInput {
    job_name: String,
    now_ms: i64,
    manual: Option<ScheduledJobManualRequest>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
struct ScheduledJobClaimActivityOutput {
    job_name: String,
    scheduled_at_ms: Option<i64>,
    next_run_at_ms: Option<i64>,
    coalesced: bool,
    backing_off: bool,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
struct ScheduledJobCompleteActivityInput {
    job_name: String,
    scheduled_at_ms: i64,
    succeeded: bool,
    error: Option<String>,
    finished_at_ms: i64,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
struct ScheduledJobRunActivityInput {
    job_name: String,
    scheduled_at_ms: i64,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
struct ScheduledJobRunActivityOutput {
    succeeded: bool,
    error: Option<String>,
}

#[derive(Debug, Clone)]
struct WorkflowRuntimeActivities {
    repository: Repository,
    inventory: Option<InventoryWorkflowActivities>,
    announce: Option<AnnounceWorkflowActivities>,
    scheduled_jobs: Option<ScheduledJobWorkflowActivities>,
    search: Option<SearchWorkflowActivities>,
    saved_retry: Option<SavedRetryWorkflowActivities>,
}

impl WorkflowRuntimeActivities {
    fn with_completion_event_bridge(
        mut self,
        completion_events: InventoryCompletionEventBridge,
    ) -> Self {
        if let Some(inventory) = self.inventory.take() {
            self.inventory =
                Some(inventory.with_completion_event_bridge(completion_events.clone()));
        }
        if let Some(announce) = self.announce.take() {
            self.announce = Some(announce.with_completion_event_bridge(completion_events));
        }
        self
    }
}

#[derive(Debug, Clone)]
pub struct ScheduledJobWorkflowActivities {
    scheduler: PersistedScheduler,
    shutdown: ShutdownSignal,
    state: ScheduledJobStateHandle,
    inventory: Option<InventoryWorkflowActivities>,
    active_inventory_refreshes: Option<Arc<Mutex<BTreeSet<String>>>>,
}

impl ScheduledJobWorkflowActivities {
    pub fn new(
        scheduler: PersistedScheduler,
        shutdown: ShutdownSignal,
        state: ScheduledJobStateHandle,
    ) -> Self {
        Self {
            scheduler,
            shutdown,
            state,
            inventory: None,
            active_inventory_refreshes: None,
        }
    }

    fn with_inventory_runtime(
        mut self,
        inventory: InventoryWorkflowActivities,
        active_inventory_refreshes: Arc<Mutex<BTreeSet<String>>>,
    ) -> Self {
        self.inventory = Some(inventory);
        self.active_inventory_refreshes = Some(active_inventory_refreshes);
        self
    }
}

#[derive(Debug, Clone, Default)]
pub struct ScheduledJobStateHandle {
    state: Arc<Mutex<Option<crate::runtime::app::AppState>>>,
}

impl ScheduledJobStateHandle {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn bind(&self, state: crate::runtime::app::AppState) -> bool {
        let Ok(mut guard) = self.state.lock() else {
            return false;
        };
        if guard.is_some() {
            return false;
        }
        *guard = Some(state);
        true
    }

    fn get(&self) -> Option<crate::runtime::app::AppState> {
        self.state.lock().ok().and_then(|guard| guard.clone())
    }

    fn clear(&self) {
        if let Ok(mut guard) = self.state.lock() {
            *guard = None;
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SearchWorkflowStateHandle {
    state: Arc<Mutex<Option<crate::runtime::app::AppState>>>,
}

impl SearchWorkflowStateHandle {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn bind(&self, state: crate::runtime::app::AppState) -> bool {
        let Ok(mut guard) = self.state.lock() else {
            return false;
        };
        if guard.is_some() {
            return false;
        }
        *guard = Some(state);
        true
    }

    fn get(&self) -> Option<crate::runtime::app::AppState> {
        self.state.lock().ok().and_then(|guard| guard.clone())
    }

    fn clear(&self) {
        if let Ok(mut guard) = self.state.lock() {
            *guard = None;
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SavedRetryWorkflowStateHandle {
    state: Arc<Mutex<Option<crate::runtime::app::AppState>>>,
}

impl SavedRetryWorkflowStateHandle {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn bind(&self, state: crate::runtime::app::AppState) -> bool {
        let Ok(mut guard) = self.state.lock() else {
            return false;
        };
        if guard.is_some() {
            return false;
        }
        *guard = Some(state);
        true
    }

    fn get(&self) -> Option<crate::runtime::app::AppState> {
        self.state.lock().ok().and_then(|guard| guard.clone())
    }

    fn clear(&self) {
        if let Ok(mut guard) = self.state.lock() {
            *guard = None;
        }
    }
}

#[derive(Debug, Clone)]
pub struct SearchWorkflowActivities {
    state: SearchWorkflowStateHandle,
    shutdown: ShutdownSignal,
}

impl SearchWorkflowActivities {
    pub fn new(state: SearchWorkflowStateHandle, shutdown: ShutdownSignal) -> Self {
        Self { state, shutdown }
    }
}

#[derive(Debug, Clone)]
pub struct SavedRetryWorkflowActivities {
    state: SavedRetryWorkflowStateHandle,
    shutdown: ShutdownSignal,
}

impl SavedRetryWorkflowActivities {
    pub fn new(state: SavedRetryWorkflowStateHandle, shutdown: ShutdownSignal) -> Self {
        Self { state, shutdown }
    }
}

#[derive(Debug, Clone)]
pub struct AnnounceWorkflowActivities {
    repository: Repository,
    processor: AnnounceProcessor,
    queue_config: AnnounceQueueConfig,
    shutdown: ShutdownSignal,
    completion_events: Option<InventoryCompletionEventBridge>,
}

impl AnnounceWorkflowActivities {
    pub fn new(
        repository: Repository,
        processor: AnnounceProcessor,
        queue_config: AnnounceQueueConfig,
        shutdown: ShutdownSignal,
    ) -> Self {
        Self {
            repository,
            processor,
            queue_config,
            shutdown,
            completion_events: None,
        }
    }

    fn with_completion_event_bridge(
        mut self,
        completion_events: InventoryCompletionEventBridge,
    ) -> Self {
        self.completion_events = Some(completion_events);
        self
    }
}

#[derive(Debug, Clone)]
pub struct InventoryWorkflowActivities {
    repository: Repository,
    inventory_refresh: InventoryRefreshWorker,
    injection_worker: InjectionWorker,
    shutdown: ShutdownSignal,
    failure_backoff: Duration,
    completion_events: Option<InventoryCompletionEventBridge>,
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
            completion_events: None,
        }
    }

    fn with_completion_event_bridge(
        mut self,
        completion_events: InventoryCompletionEventBridge,
    ) -> Self {
        self.completion_events = Some(completion_events);
        self
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

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AnnounceWorkflowSubmission {
    pub workflow_id: String,
    pub outcome: AnnounceWorkflowSubmissionOutcome,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum AnnounceWorkflowSubmissionOutcome {
    Started,
    AlreadyRunning,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SearchWorkflowSubmission {
    pub workflow_id: String,
    pub outcome: SearchWorkflowSubmissionOutcome,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum SearchWorkflowSubmissionOutcome {
    Started,
    AlreadyRunning,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
struct SavedRetryScanActivityInput {
    requested_at_ms: i64,
    reason: String,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
struct SavedRetryScanActivityOutput {
    items: Vec<SavedTorrentRetryItem>,
    interval_ms: u64,
    failed: usize,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
struct SavedRetryProcessActivityInput {
    item: SavedTorrentRetryItem,
    requested_at_ms: i64,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
struct SavedRetryFinalizeActivityInput {
    requested_at_ms: i64,
    summary: SavedTorrentRetrySummary,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
struct SavedRetryFinalizeActivityOutput {
    summary: SavedTorrentRetrySummary,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
struct SearchPlanActivityInput {
    input: SearchWorkflowInput,
    planned_at_ms: i64,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
struct SearchPlanActivityOutput {
    planned_indexers: usize,
    failed_indexers: usize,
    candidate_count: usize,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
struct SearchCandidatePageActivityInput {
    start_ordinal: u32,
    limit: u16,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
struct SearchCandidatePageActivityOutput {
    refs: Vec<SearchCandidateRef>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
struct SearchCandidateActivityInput {
    candidate: SearchCandidateRef,
    planned_at_ms: i64,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
struct SearchFinalizeActivityInput {
    summary: SearchWorkflowExecutionSummary,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
struct SearchFinalizeActivityOutput {
    summary: SearchWorkflowExecutionSummary,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
struct SearchCandidateRef {
    ordinal: u32,
}

impl From<SearchCandidateMaterialRef> for SearchCandidateRef {
    fn from(value: SearchCandidateMaterialRef) -> Self {
        Self {
            ordinal: value.ordinal,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
struct AnnounceActivityInput {
    work_id: String,
    received_at_ms: i64,
    raw_secret_material_count: u16,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
struct AnnounceProcessActivityOutput {
    state: AnnounceActivityState,
    reason: String,
    next_attempt_at_ms: Option<i64>,
    retry_delay_ms: Option<u64>,
    dependency: Option<AnnounceProjectionDependency>,
    events: Vec<WorkflowEventName>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
struct AnnounceWaitActivityOutput {
    events: Vec<WorkflowEventName>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
struct AnnounceProjectionDependency {
    kind: String,
    name: String,
}

impl From<&AnnounceDependency> for AnnounceProjectionDependency {
    fn from(value: &AnnounceDependency) -> Self {
        Self {
            kind: value.kind.as_str().to_owned(),
            name: value.name.as_str().to_owned(),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum AnnounceActivityState {
    Succeeded,
    Failed,
    WaitingInventory,
    WaitingDependency,
    Retrying,
    Released,
}

fn announce_workflow_input(work: &AnnounceWorkItem) -> AnnounceWorkflowInput {
    AnnounceWorkflowInput {
        work_id: work.id.as_str().to_owned(),
        dedupe_hash: work.dedupe_hash.as_str().to_owned(),
        tracker: work.tracker.as_str().to_owned(),
        candidate_guid: work
            .guid
            .as_ref()
            .map(|guid| guid.as_str().to_owned())
            .unwrap_or_default(),
        candidate_title: work.title.as_str().to_owned(),
        received_at_ms: work.received_at_ms,
        expires_at_ms: work.expires_at_ms,
        fetch_material_present: work.fetch.is_some(),
        raw_secret_material_count: u16::from(work.fetch.is_some()),
    }
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

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct InventoryCompletionWaiter {
    pub workflow_id: String,
    pub event_name: WorkflowEventName,
    pub required_after_ms: i64,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct InventoryCompletionWaitRegistration {
    pub inserted: bool,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct InventoryCompletionEvent {
    pub inventory_kind: InventoryRefreshKind,
    pub source_workflow_id: String,
    pub completed_at_ms: i64,
    pub scanned_items: usize,
    pub persisted_items: usize,
    pub pruned_items: u64,
}

impl InventoryCompletionEvent {
    fn event_name(&self) -> WorkflowEventName {
        match self.inventory_kind {
            InventoryRefreshKind::MediaFull | InventoryRefreshKind::MediaChanged => {
                WorkflowEventName::MediaInventoryCompleted
            }
            InventoryRefreshKind::Client => WorkflowEventName::ClientInventoryCompleted,
        }
    }

    fn to_record(&self) -> WorkflowInventoryCompletionRecord {
        WorkflowInventoryCompletionRecord {
            event_name: self.event_name().as_str().to_owned(),
            source_workflow_id: self.source_workflow_id.clone(),
            completed_at_ms: self.completed_at_ms,
            inventory_kind: inventory_refresh_kind_key(self.inventory_kind).to_owned(),
            scanned_items: self.scanned_items,
            persisted_items: self.persisted_items,
            pruned_items: self.pruned_items,
        }
    }

    fn from_record(record: &WorkflowInventoryCompletionRecord) -> Result<Self, String> {
        let inventory_kind = inventory_refresh_kind_from_key(&record.inventory_kind)?;
        Ok(Self {
            inventory_kind,
            source_workflow_id: record.source_workflow_id.clone(),
            completed_at_ms: record.completed_at_ms,
            scanned_items: record.scanned_items,
            persisted_items: record.persisted_items,
            pruned_items: record.pruned_items,
        })
    }
}

fn inventory_refresh_kind_key(kind: InventoryRefreshKind) -> &'static str {
    match kind {
        InventoryRefreshKind::MediaFull => "media_full",
        InventoryRefreshKind::MediaChanged => "media_changed",
        InventoryRefreshKind::Client => "client",
    }
}

fn inventory_refresh_kind_from_key(value: &str) -> Result<InventoryRefreshKind, String> {
    match value {
        "media_full" => Ok(InventoryRefreshKind::MediaFull),
        "media_changed" => Ok(InventoryRefreshKind::MediaChanged),
        "client" => Ok(InventoryRefreshKind::Client),
        _ => Err(format!("unknown inventory refresh kind `{value}`")),
    }
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub struct InventoryCompletionFanout {
    pub waiters: usize,
    pub delivered: usize,
    pub failed: usize,
}

#[derive(Clone)]
pub struct InventoryCompletionEventBridge {
    store: Arc<dyn Provider>,
    repository: Option<Repository>,
    waiters: Arc<Mutex<InventoryCompletionWaiters>>,
}

#[derive(Debug, Default)]
struct InventoryCompletionWaiters {
    by_event: BTreeMap<String, BTreeMap<String, i64>>,
}

impl InventoryCompletionEventBridge {
    fn new(store: Arc<dyn Provider>, repository: Option<Repository>) -> Self {
        Self {
            store,
            repository,
            waiters: Arc::new(Mutex::new(InventoryCompletionWaiters::default())),
        }
    }

    async fn register_waiter(
        &self,
        waiter: InventoryCompletionWaiter,
    ) -> Result<InventoryCompletionWaitRegistration, String> {
        let event_name = inventory_completion_event_name(waiter.event_name)?;
        if waiter.workflow_id.is_empty() {
            return Err("inventory completion waiter workflow id must not be empty".to_owned());
        }
        if let Some(repository) = &self.repository {
            let inserted = repository
                .record_workflow_inventory_waiter(
                    event_name,
                    &waiter.workflow_id,
                    waiter.required_after_ms,
                    unix_time_ms(),
                )
                .await
                .map_err(|error| error.to_string())?;
            return Ok(InventoryCompletionWaitRegistration { inserted });
        }
        self.register_memory_waiter(event_name, waiter)
    }

    fn register_memory_waiter(
        &self,
        event_name: &str,
        waiter: InventoryCompletionWaiter,
    ) -> Result<InventoryCompletionWaitRegistration, String> {
        let mut waiters = self
            .waiters
            .lock()
            .map_err(|_error| "inventory completion waiter registry is poisoned".to_owned())?;
        let event_waiters = waiters.by_event.entry(event_name.to_owned()).or_default();
        let inserted = event_waiters
            .insert(waiter.workflow_id, waiter.required_after_ms)
            .is_none();
        Ok(InventoryCompletionWaitRegistration { inserted })
    }

    async fn publish_completion(
        &self,
        event: &InventoryCompletionEvent,
    ) -> Result<InventoryCompletionFanout, String> {
        self.record_completion(event).await?;
        let summary = self.drain_completion_event(event).await;
        if let Ok(fanout) = summary.as_ref()
            && self.completion_can_be_deleted(event, *fanout).await?
        {
            self.delete_persisted_completion(event).await?;
        }
        summary
    }

    async fn drain_persisted_completions(&self) -> Result<InventoryCompletionFanout, String> {
        let Some(repository) = &self.repository else {
            return Ok(InventoryCompletionFanout::default());
        };
        let completions = repository
            .workflow_inventory_completions(INVENTORY_COMPLETION_FANOUT_LIMIT)
            .await
            .map_err(|error| format!("read persisted inventory completions failed: {error}"))?;
        let mut total = InventoryCompletionFanout::default();
        for completion in completions {
            let event = InventoryCompletionEvent::from_record(&completion)?;
            let summary = self.drain_completion_event(&event).await?;
            total.waiters += summary.waiters;
            total.delivered += summary.delivered;
            total.failed += summary.failed;
            if self.completion_can_be_deleted(&event, summary).await? {
                self.delete_persisted_completion(&event).await?;
            }
        }
        Ok(total)
    }

    async fn record_completion(&self, event: &InventoryCompletionEvent) -> Result<(), String> {
        let Some(repository) = &self.repository else {
            return Ok(());
        };
        repository
            .record_workflow_inventory_completion(&event.to_record(), unix_time_ms())
            .await
            .map_err(|error| {
                format!(
                    "record inventory completion `{}` failed: {error}",
                    event.source_workflow_id
                )
            })?;
        Ok(())
    }

    async fn delete_persisted_completion(
        &self,
        event: &InventoryCompletionEvent,
    ) -> Result<(), String> {
        let Some(repository) = &self.repository else {
            return Ok(());
        };
        repository
            .delete_workflow_inventory_completion(
                event.event_name().as_str(),
                &event.source_workflow_id,
                event.completed_at_ms,
            )
            .await
            .map_err(|error| {
                format!(
                    "delete inventory completion `{}` failed: {error}",
                    event.source_workflow_id
                )
            })?;
        Ok(())
    }

    async fn completion_can_be_deleted(
        &self,
        event: &InventoryCompletionEvent,
        summary: InventoryCompletionFanout,
    ) -> Result<bool, String> {
        if summary.waiters > 0 {
            return Ok(true);
        }
        let Some(repository) = &self.repository else {
            return Ok(true);
        };
        let due_count = repository
            .workflow_inventory_waiters_due_count(
                event.event_name().as_str(),
                event.completed_at_ms,
            )
            .await
            .map_err(|error| {
                format!(
                    "count waiters for inventory completion `{}` failed: {error}",
                    event.source_workflow_id
                )
            })?;
        Ok(due_count == 0)
    }

    async fn drain_completion_event(
        &self,
        event: &InventoryCompletionEvent,
    ) -> Result<InventoryCompletionFanout, String> {
        let event_name = event.event_name().as_str().to_owned();
        let lease_owner = format!(
            "inventory-completion:{}:{}",
            event.source_workflow_id, event.completed_at_ms
        );
        let mut summary = InventoryCompletionFanout::default();
        let mut cleanup_conflict = false;
        loop {
            let ready = self
                .ready_waiters(&event_name, event.completed_at_ms, &lease_owner)
                .await?;
            let batch_len = ready.len();
            if ready.is_empty() {
                break;
            }
            summary.waiters += batch_len;
            let mut batch_failed = false;
            let mut cleanup_failed = false;
            for waiter in ready {
                let client = Client::new(Arc::clone(&self.store));
                match self
                    .enqueue_completion_if_target_is_running(&client, &waiter, &event_name, event)
                    .await
                {
                    Ok(()) => {
                        summary.delivered += 1;
                        cleanup_failed |=
                            !self.remove_delivered_waiter(&event_name, &waiter).await?;
                    }
                    Err(error) => {
                        summary.failed += 1;
                        batch_failed = true;
                        self.release_waiter_after_delivery_failure(
                            &event_name,
                            &waiter,
                            &error.to_string(),
                        )
                        .await?;
                        tracing::warn!(
                            workflow_id = waiter.workflow_id,
                            event_name,
                            error = %error,
                            "inventory completion event delivery failed"
                        );
                    }
                }
            }
            let maybe_more_repository_waiters = self.repository.is_some()
                && batch_len == usize::from(INVENTORY_COMPLETION_FANOUT_LIMIT);
            cleanup_conflict |= cleanup_failed;
            if !maybe_more_repository_waiters || batch_failed || cleanup_failed {
                break;
            }
        }
        if cleanup_conflict {
            Err("inventory completion fanout could not confirm delivered waiter cleanup".to_owned())
        } else if summary.failed > 0 {
            Err(format!(
                "inventory completion fanout failed for {} of {} waiters",
                summary.failed, summary.waiters
            ))
        } else {
            Ok(summary)
        }
    }

    async fn enqueue_completion_if_target_is_running(
        &self,
        client: &Client,
        waiter: &InventoryCompletionReadyWaiter,
        event_name: &str,
        event: &InventoryCompletionEvent,
    ) -> Result<(), String> {
        match client
            .get_orchestration_status(&waiter.workflow_id)
            .await
            .map_err(|error| error.to_string())?
        {
            OrchestrationStatus::Running { .. } => client
                .enqueue_event_typed(&waiter.workflow_id, event_name, event)
                .await
                .map_err(|error| error.to_string()),
            OrchestrationStatus::NotFound => Err(format!(
                "target workflow `{}` is not found",
                waiter.workflow_id
            )),
            OrchestrationStatus::Completed { .. } => Err(format!(
                "target workflow `{}` is already completed",
                waiter.workflow_id
            )),
            OrchestrationStatus::Failed { details, .. } => Err(format!(
                "target workflow `{}` is failed: {}",
                waiter.workflow_id,
                details.display_message()
            )),
        }
    }

    async fn ready_waiters(
        &self,
        event_name: &str,
        completed_at_ms: i64,
        lease_owner: &str,
    ) -> Result<Vec<InventoryCompletionReadyWaiter>, String> {
        let mut ready = BTreeMap::<String, InventoryCompletionReadyWaiter>::new();
        if let Some(repository) = &self.repository {
            let now_ms = unix_time_ms();
            let rows = repository
                .claim_workflow_inventory_waiters_ready(
                    event_name,
                    completed_at_ms,
                    now_ms,
                    lease_owner,
                    now_ms.saturating_add(INVENTORY_COMPLETION_LEASE_MS),
                    INVENTORY_COMPLETION_FANOUT_LIMIT,
                )
                .await
                .map_err(|error| {
                    format!("claim inventory completion waiters for `{event_name}` failed: {error}")
                })?;
            for row in rows {
                ready.insert(
                    row.workflow_id.clone(),
                    InventoryCompletionReadyWaiter::from_repository(row, lease_owner.to_owned()),
                );
            }
        }
        for workflow_id in self.ready_memory_waiters(event_name, completed_at_ms) {
            ready
                .entry(workflow_id.clone())
                .and_modify(|waiter| waiter.memory = true)
                .or_insert_with(|| InventoryCompletionReadyWaiter::from_memory(workflow_id));
        }
        Ok(ready.into_values().collect())
    }

    async fn release_waiter_after_delivery_failure(
        &self,
        event_name: &str,
        waiter: &InventoryCompletionReadyWaiter,
        error: &str,
    ) -> Result<(), String> {
        if waiter.repository
            && let Some(repository) = &self.repository
        {
            let lease_owner = waiter.lease_owner.as_deref().unwrap_or_default();
            repository
                .release_workflow_inventory_waiter(
                    event_name,
                    &waiter.workflow_id,
                    lease_owner,
                    error,
                )
                .await
                .map_err(|error| {
                    format!(
                        "release inventory completion waiter `{}` failed: {error}",
                        waiter.workflow_id
                    )
                })?;
        }
        Ok(())
    }

    fn ready_memory_waiters(&self, event_name: &str, completed_at_ms: i64) -> Vec<String> {
        let Ok(waiters) = self.waiters.lock() else {
            return Vec::new();
        };
        waiters
            .by_event
            .get(event_name)
            .into_iter()
            .flat_map(|event_waiters| event_waiters.iter())
            .filter(|(_workflow_id, required_after_ms)| completed_at_ms >= **required_after_ms)
            .map(|(workflow_id, _required_after_ms)| workflow_id.clone())
            .collect()
    }

    async fn remove_delivered_waiter(
        &self,
        event_name: &str,
        waiter: &InventoryCompletionReadyWaiter,
    ) -> Result<bool, String> {
        let mut removed = true;
        if waiter.repository
            && let Some(repository) = &self.repository
        {
            let lease_owner = waiter.lease_owner.as_deref().unwrap_or_default();
            let deleted = repository
                .delete_claimed_workflow_inventory_waiter(
                    event_name,
                    &waiter.workflow_id,
                    lease_owner,
                )
                .await
                .map_err(|error| {
                    format!(
                        "delete delivered inventory completion waiter `{}` failed: {error}",
                        waiter.workflow_id
                    )
                })?;
            removed &= deleted;
        }
        if waiter.memory {
            self.remove_memory_waiter(event_name, &waiter.workflow_id);
        }
        Ok(removed)
    }

    fn remove_memory_waiter(&self, event_name: &str, workflow_id: &str) {
        let Ok(mut waiters) = self.waiters.lock() else {
            return;
        };
        if let Some(event_waiters) = waiters.by_event.get_mut(event_name) {
            event_waiters.remove(workflow_id);
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct InventoryCompletionReadyWaiter {
    workflow_id: String,
    repository: bool,
    memory: bool,
    lease_owner: Option<String>,
}

impl InventoryCompletionReadyWaiter {
    fn from_repository(row: WorkflowInventoryWaiterRecord, lease_owner: String) -> Self {
        Self {
            workflow_id: row.workflow_id,
            repository: true,
            memory: false,
            lease_owner: Some(lease_owner),
        }
    }

    fn from_memory(workflow_id: String) -> Self {
        Self {
            workflow_id,
            repository: false,
            memory: true,
            lease_owner: None,
        }
    }
}

fn inventory_completion_event_name(event_name: WorkflowEventName) -> Result<&'static str, String> {
    match event_name {
        WorkflowEventName::MediaInventoryCompleted
        | WorkflowEventName::ClientInventoryCompleted => Ok(event_name.as_str()),
        _ => Err(format!(
            "workflow event `{}` is not an inventory completion event",
            event_name.as_str()
        )),
    }
}

impl fmt::Debug for InventoryCompletionEventBridge {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InventoryCompletionEventBridge")
            .finish_non_exhaustive()
    }
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
        match activity {
            ActivityKind::ScheduledJobClaim => {
                builder = builder.register_typed(
                    activity.as_str(),
                    move |_ctx: ActivityContext,
                          input: ActivityInputEnvelope<ScheduledJobClaimActivityInput>| async move {
                        Ok(ScheduledJobClaimActivityOutput {
                            job_name: input.payload.job_name,
                            scheduled_at_ms: None,
                            next_run_at_ms: Some(input.payload.now_ms.saturating_add(60_000)),
                            coalesced: false,
                            backing_off: false,
                        })
                    },
                );
            }
            ActivityKind::ScheduledJobComplete => {
                builder = builder.register_typed(
                    activity.as_str(),
                    move |_ctx: ActivityContext,
                          input: ActivityInputEnvelope<ScheduledJobCompleteActivityInput>| async move {
                        Ok(ScheduledJobRunActivityOutput {
                            succeeded: input.payload.succeeded,
                            error: input.payload.error,
                        })
                    },
                );
            }
            ActivityKind::ScheduledJobRun => {
                builder = builder.register_typed(
                    activity.as_str(),
                    move |_ctx: ActivityContext,
                          _input: ActivityInputEnvelope<ScheduledJobRunActivityInput>| async move {
                        Ok(ScheduledJobRunActivityOutput {
                            succeeded: true,
                            error: None,
                        })
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

fn activity_registry_with_runtime_activities(
    mut activities: WorkflowRuntimeActivities,
    active_inventory_refreshes: Arc<Mutex<BTreeSet<String>>>,
) -> ActivityRegistry {
    if let (Some(scheduled_jobs), Some(inventory)) = (
        activities.scheduled_jobs.take(),
        activities.inventory.clone(),
    ) {
        activities.scheduled_jobs = Some(
            scheduled_jobs
                .with_inventory_runtime(inventory, Arc::clone(&active_inventory_refreshes)),
        );
    }
    let mut builder = ActivityRegistry::builder();
    for activity in ActivityKind::ALL {
        match activity {
            ActivityKind::InventoryScanMedia | ActivityKind::InventoryRefreshClient => {
                if let Some(inventory) = activities.inventory.clone() {
                    let active_inventory_refreshes = Arc::clone(&active_inventory_refreshes);
                    builder = builder.register_typed(
                        activity.as_str(),
                        move |_ctx: ActivityContext,
                              input: ActivityInputEnvelope<InventoryActivityInput>| {
                            let inventory = inventory.clone();
                            let active_inventory_refreshes =
                                Arc::clone(&active_inventory_refreshes);
                            async move {
                                run_inventory_activity(
                                    inventory,
                                    active_inventory_refreshes,
                                    input.workflow_id,
                                    input.payload,
                                )
                                .await
                            }
                        },
                    );
                } else {
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
            ActivityKind::MatchingReverseLookup => {
                if let Some(announce) = activities.announce.clone() {
                    builder = builder.register_typed(
                        activity.as_str(),
                        move |_ctx: ActivityContext,
                              input: ActivityInputEnvelope<AnnounceActivityInput>| {
                            let announce = announce.clone();
                            async move {
                                Box::pin(run_announce_process_activity(
                                    announce,
                                    input.workflow_id,
                                    input.payload,
                                ))
                                .await
                            }
                        },
                    );
                } else {
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
            ActivityKind::RepositoryWrite => {
                if let Some(announce) = activities.announce.clone() {
                    builder = builder.register_typed(
                        activity.as_str(),
                        move |_ctx: ActivityContext,
                              input: ActivityInputEnvelope<AnnounceActivityInput>| {
                            let announce = announce.clone();
                            async move {
                                run_announce_queue_inventory_activity(
                                    announce,
                                    input.workflow_id,
                                    input.payload,
                                )
                                .await
                            }
                        },
                    );
                } else {
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
            ActivityKind::ScheduledJobClaim => {
                if let Some(scheduled_jobs) = activities.scheduled_jobs.clone() {
                    builder = builder.register_typed(
                        activity.as_str(),
                        move |_ctx: ActivityContext,
                              input: ActivityInputEnvelope<ScheduledJobClaimActivityInput>| {
                            let scheduled_jobs = scheduled_jobs.clone();
                            async move {
                                run_scheduled_job_claim_activity(
                                    scheduled_jobs,
                                    input.workflow_id,
                                    input.payload,
                                )
                                .await
                            }
                        },
                    );
                } else {
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
            ActivityKind::ScheduledJobComplete => {
                if let Some(scheduled_jobs) = activities.scheduled_jobs.clone() {
                    builder =
                        builder.register_typed(
                            activity.as_str(),
                            move |_ctx: ActivityContext,
                                  input: ActivityInputEnvelope<
                                ScheduledJobCompleteActivityInput,
                            >| {
                                let scheduled_jobs = scheduled_jobs.clone();
                                async move {
                                    run_scheduled_job_complete_activity(
                                        scheduled_jobs,
                                        input.workflow_id,
                                        input.payload,
                                    )
                                    .await
                                }
                            },
                        );
                } else {
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
            ActivityKind::ScheduledJobRun => {
                if let Some(scheduled_jobs) = activities.scheduled_jobs.clone() {
                    builder = builder.register_typed(
                        activity.as_str(),
                        move |_ctx: ActivityContext,
                              input: ActivityInputEnvelope<ScheduledJobRunActivityInput>| {
                            let scheduled_jobs = scheduled_jobs.clone();
                            async move {
                                run_scheduled_job_activity(
                                    scheduled_jobs,
                                    input.workflow_id,
                                    input.payload,
                                )
                                .await
                            }
                        },
                    );
                } else {
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
            ActivityKind::SearchPlan => {
                if let Some(search) = activities.search.clone() {
                    builder = builder.register_typed(
                        activity.as_str(),
                        move |_ctx: ActivityContext,
                              input: ActivityInputEnvelope<SearchPlanActivityInput>| {
                            let search = search.clone();
                            async move {
                                run_search_plan_activity(search, input.workflow_id, input.payload)
                                    .await
                            }
                        },
                    );
                } else {
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
            ActivityKind::SearchCandidatePage => {
                if let Some(search) = activities.search.clone() {
                    builder =
                        builder.register_typed(
                            activity.as_str(),
                            move |_ctx: ActivityContext,
                                  input: ActivityInputEnvelope<
                                SearchCandidatePageActivityInput,
                            >| {
                                let search = search.clone();
                                async move {
                                    run_search_candidate_page_activity(
                                        search,
                                        input.workflow_id,
                                        input.payload,
                                    )
                                    .await
                                }
                            },
                        );
                } else {
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
            ActivityKind::SearchCandidateProcess => {
                if let Some(search) = activities.search.clone() {
                    builder = builder.register_typed(
                        activity.as_str(),
                        move |_ctx: ActivityContext,
                              input: ActivityInputEnvelope<SearchCandidateActivityInput>| {
                            let search = search.clone();
                            async move {
                                Box::pin(run_search_candidate_activity(
                                    search,
                                    input.workflow_id,
                                    input.payload,
                                ))
                                .await
                            }
                        },
                    );
                } else {
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
            ActivityKind::SearchFinalize => {
                if let Some(search) = activities.search.clone() {
                    builder = builder.register_typed(
                        activity.as_str(),
                        move |_ctx: ActivityContext,
                              input: ActivityInputEnvelope<SearchFinalizeActivityInput>| {
                            let search = search.clone();
                            async move {
                                run_search_finalize_activity(search, input.workflow_id, input.payload)
                                    .await
                            }
                        },
                    );
                } else {
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
            ActivityKind::SavedRetryScan => {
                if let Some(saved_retry) = activities.saved_retry.clone() {
                    builder = builder.register_typed(
                        activity.as_str(),
                        move |_ctx: ActivityContext,
                              input: ActivityInputEnvelope<SavedRetryScanActivityInput>| {
                            let saved_retry = saved_retry.clone();
                            async move {
                                run_saved_retry_scan_activity(
                                    saved_retry,
                                    input.workflow_id,
                                    input.payload,
                                )
                                .await
                            }
                        },
                    );
                } else {
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
            ActivityKind::SavedRetryProcess => {
                if let Some(saved_retry) = activities.saved_retry.clone() {
                    builder = builder.register_typed(
                        activity.as_str(),
                        move |_ctx: ActivityContext,
                              input: ActivityInputEnvelope<SavedRetryProcessActivityInput>| {
                            let saved_retry = saved_retry.clone();
                            async move {
                                run_saved_retry_process_activity(
                                    saved_retry,
                                    input.workflow_id,
                                    input.payload,
                                )
                                .await
                            }
                        },
                    );
                } else {
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
            ActivityKind::SavedRetryFinalize => {
                if let Some(saved_retry) = activities.saved_retry.clone() {
                    builder = builder.register_typed(
                        activity.as_str(),
                        move |_ctx: ActivityContext,
                              input: ActivityInputEnvelope<SavedRetryFinalizeActivityInput>| {
                            let saved_retry = saved_retry.clone();
                            async move {
                                run_saved_retry_finalize_activity(
                                    saved_retry,
                                    input.workflow_id,
                                    input.payload,
                                )
                                .await
                            }
                        },
                    );
                } else {
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

async fn run_saved_retry_scan_activity(
    activities: SavedRetryWorkflowActivities,
    workflow_id: String,
    input: SavedRetryScanActivityInput,
) -> Result<SavedRetryScanActivityOutput, String> {
    let state = activities
        .state
        .get()
        .ok_or_else(|| "saved retry workflow app state is not bound".to_owned())?;
    let mut config = saved_torrent_retry_config(&state.config);
    config.assessed_at_ms = input.requested_at_ms;
    record_saved_retry_projection(
        &state.repository,
        SavedRetryProjectionRecord {
            workflow_id: &workflow_id,
            state: WorkflowState::Running,
            reason: WorkflowReason::RunningActivity,
            next_action: Some("scanning"),
            started_at_ms: input.requested_at_ms,
            updated_at_ms: unix_time_ms(),
            finished_at_ms: None,
        },
    )
    .await?;
    let mut shutdown = activities.shutdown.clone();
    let interval_ms = duration_millis_u64(state.saved_retry_interval);
    let items = match state
        .injection_worker
        .scan_saved_torrent_retry_items_until_shutdown(config, &mut shutdown)
        .await
    {
        Ok(items) => items,
        Err(error) => {
            tracing::warn!(error = ?error, "saved torrent retry scan failed");
            return Ok(SavedRetryScanActivityOutput {
                items: Vec::new(),
                interval_ms,
                failed: 1,
            });
        }
    };
    Ok(SavedRetryScanActivityOutput {
        items,
        interval_ms,
        failed: 0,
    })
}

async fn run_saved_retry_process_activity(
    activities: SavedRetryWorkflowActivities,
    _workflow_id: String,
    input: SavedRetryProcessActivityInput,
) -> Result<SavedTorrentRetrySummary, String> {
    let state = activities
        .state
        .get()
        .ok_or_else(|| "saved retry workflow app state is not bound".to_owned())?;
    let mut config = saved_torrent_retry_config(&state.config);
    config.assessed_at_ms = input.requested_at_ms;
    let mut shutdown = activities.shutdown.clone();
    match state
        .injection_worker
        .retry_saved_torrent_item_until_shutdown(input.item, config, &mut shutdown)
        .await
    {
        Ok(summary) => Ok(summary),
        Err(error) => {
            tracing::warn!(error = ?error, "saved torrent retry item failed");
            Ok(SavedTorrentRetrySummary {
                scanned: 1,
                failed: 1,
                kept: 1,
                ..SavedTorrentRetrySummary::default()
            })
        }
    }
}

async fn run_saved_retry_finalize_activity(
    activities: SavedRetryWorkflowActivities,
    workflow_id: String,
    input: SavedRetryFinalizeActivityInput,
) -> Result<SavedRetryFinalizeActivityOutput, String> {
    let state = activities
        .state
        .get()
        .ok_or_else(|| "saved retry workflow app state is not bound".to_owned())?;
    let now_ms = unix_time_ms();
    record_saved_retry_projection(
        &state.repository,
        SavedRetryProjectionRecord {
            workflow_id: &workflow_id,
            state: WorkflowState::Succeeded,
            reason: WorkflowReason::Completed,
            next_action: Some("completed"),
            started_at_ms: input.requested_at_ms,
            updated_at_ms: now_ms,
            finished_at_ms: Some(now_ms),
        },
    )
    .await?;
    tracing::info!(
        scanned = input.summary.scanned,
        attempted = input.summary.attempted,
        injected = input.summary.injected,
        failed = input.summary.failed,
        kept = input.summary.kept,
        deleted = input.summary.deleted,
        "saved torrent retry completed"
    );
    Ok(SavedRetryFinalizeActivityOutput {
        summary: input.summary,
    })
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct SavedRetryProjectionRecord<'a> {
    workflow_id: &'a str,
    state: WorkflowState,
    reason: WorkflowReason,
    next_action: Option<&'a str>,
    started_at_ms: i64,
    updated_at_ms: i64,
    finished_at_ms: Option<i64>,
}

async fn record_saved_retry_projection(
    repository: &Repository,
    record: SavedRetryProjectionRecord<'_>,
) -> Result<(), String> {
    repository
        .record_workflow_projection(&WorkflowProjectionUpdate {
            workflow_id: record.workflow_id,
            workflow_kind: WorkflowKind::SavedTorrentRetry,
            public_id: "saved-retry",
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
        .map_err(|error| error.to_string())
}

fn duration_millis_u64(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

async fn run_search_plan_activity(
    activities: SearchWorkflowActivities,
    workflow_id: String,
    input: SearchPlanActivityInput,
) -> Result<SearchPlanActivityOutput, String> {
    if activities.shutdown.state().phase != ShutdownPhase::Running {
        return Err("search workflow is shutting down".to_owned());
    }
    let state = activities
        .state
        .get()
        .ok_or_else(|| "search workflow app state is not bound".to_owned())?;
    let request = crate::http::SearchWorkflowRequest {
        query: ItemTitle::new(input.input.query).map_err(|error| error.to_string())?,
    };
    let (sender, mut receiver) = mpsc::channel(SEARCH_CANDIDATE_PREFLIGHT_CONCURRENCY);
    let planning_state = state.clone();
    let planning_shutdown = activities.shutdown.clone();
    let planning = Box::pin(async move {
        planning_state
            .stream_search_workflow_candidates(
                request,
                input.planned_at_ms,
                sender,
                planning_shutdown,
            )
            .await
            .map_err(|error| error.to_string())
    });
    let repository = state.repository.clone();
    let storing = Box::pin(async move {
        let mut stored = 0_usize;
        while let Some(candidate) = receiver.recv().await {
            let ordinal = u32::try_from(stored)
                .map_err(|error| format!("search candidate ordinal overflow: {error}"))?;
            repository
                .upsert_search_candidate_material(
                    &workflow_id,
                    ordinal,
                    &candidate,
                    input.planned_at_ms,
                )
                .await
                .map_err(|error| error.to_string())?;
            stored = stored.saturating_add(1);
        }
        Ok::<usize, String>(stored)
    });
    let (summary, stored) = tokio::try_join!(planning, storing)?;
    Ok(SearchPlanActivityOutput {
        planned_indexers: summary.plans.len(),
        failed_indexers: summary.failed_indexers,
        candidate_count: stored,
    })
}

async fn run_search_candidate_page_activity(
    activities: SearchWorkflowActivities,
    workflow_id: String,
    input: SearchCandidatePageActivityInput,
) -> Result<SearchCandidatePageActivityOutput, String> {
    let state = activities
        .state
        .get()
        .ok_or_else(|| "search workflow app state is not bound".to_owned())?;
    let page = state
        .repository
        .search_candidate_material_page(&workflow_id, input.start_ordinal, input.limit)
        .await
        .map_err(|error| error.to_string())?;
    Ok(SearchCandidatePageActivityOutput {
        refs: page
            .refs
            .into_iter()
            .map(SearchCandidateRef::from)
            .collect(),
    })
}

async fn run_search_candidate_activity(
    activities: SearchWorkflowActivities,
    workflow_id: String,
    input: SearchCandidateActivityInput,
) -> Result<SearchWorkflowExecutionSummary, String> {
    if activities.shutdown.state().phase != ShutdownPhase::Running {
        return Err("search workflow is shutting down".to_owned());
    }
    let state = activities
        .state
        .get()
        .ok_or_else(|| "search workflow app state is not bound".to_owned())?;
    let candidate = state
        .repository
        .search_candidate_material(&workflow_id, input.candidate.ordinal)
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| {
            format!(
                "search candidate material missing: workflow_id={workflow_id} ordinal={}",
                input.candidate.ordinal
            )
        })?;
    Box::pin(process_duroxide_search_candidate(
        state,
        candidate,
        input.planned_at_ms,
        activities.shutdown.clone(),
    ))
    .await
}

async fn run_search_finalize_activity(
    activities: SearchWorkflowActivities,
    workflow_id: String,
    input: SearchFinalizeActivityInput,
) -> Result<SearchFinalizeActivityOutput, String> {
    let state = activities
        .state
        .get()
        .ok_or_else(|| "search workflow app state is not bound".to_owned())?;
    let claimed = state
        .repository
        .claim_search_workflow_finalization(&workflow_id, unix_time_ms())
        .await
        .map_err(|error| error.to_string())?;
    if claimed {
        finalize_duroxide_search_workflow(&state, &input.summary);
        state
            .repository
            .delete_search_candidate_material(&workflow_id)
            .await
            .map_err(|error| error.to_string())?;
    }
    Ok(SearchFinalizeActivityOutput {
        summary: input.summary,
    })
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
            let mut output = inventory_activity_output(&summaries);
            if output.scan_failure_count == 0 {
                if let Some(completion_events) = &activities.completion_events {
                    match completion_events
                        .publish_completion(&InventoryCompletionEvent {
                            inventory_kind: input.request.kind,
                            source_workflow_id: workflow_id.clone(),
                            completed_at_ms: finished_at_ms,
                            scanned_items: output.scanned_items,
                            persisted_items: output.persisted_items,
                            pruned_items: output.pruned_items,
                        })
                        .await
                    {
                        Ok(fanout) => {
                            if fanout.waiters > 0 {
                                tracing::info!(
                                    workflow_id,
                                    waiters = fanout.waiters,
                                    delivered = fanout.delivered,
                                    failed = fanout.failed,
                                    "inventory completion events delivered"
                                );
                            }
                        }
                        Err(error) => {
                            output.scan_failure_count = 1;
                            record_inventory_refresh_health(
                                &activities.inventory_refresh,
                                Some(error.clone()),
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
                                    next_action: Some("completion_fanout_failed"),
                                    started_at_ms: input.started_at_ms,
                                    updated_at_ms: finished_at_ms,
                                    finished_at_ms: None,
                                    blocked_dependency_name: Some(error.as_str()),
                                },
                            )
                            .await?;
                            return Ok(output);
                        }
                    }
                }
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

async fn run_scheduled_job_claim_activity(
    activities: ScheduledJobWorkflowActivities,
    _workflow_id: String,
    input: ScheduledJobClaimActivityInput,
) -> Result<ScheduledJobClaimActivityOutput, String> {
    let job_name = JobName::new(&input.job_name).map_err(|error| error.to_string())?;
    if let Some(manual) = input.manual {
        let requested_at_ms = manual.requested_at_ms;
        let forced = manual.forced;
        let outcome = retry_scheduler_call(
            "claim manual scheduled job run",
            Some(&activities.shutdown),
            || {
                let scheduler = activities.scheduler.clone();
                let job_name = job_name.clone();
                async move {
                    scheduler
                        .claim_manual_run(&job_name, requested_at_ms, forced)
                        .await
                }
            },
        )
        .await?;
        return Ok(match outcome {
            ScheduledJobClaimOutcome::Claimed => ScheduledJobClaimActivityOutput {
                job_name: input.job_name,
                scheduled_at_ms: Some(requested_at_ms),
                next_run_at_ms: None,
                coalesced: false,
                backing_off: false,
            },
            ScheduledJobClaimOutcome::Coalesced => ScheduledJobClaimActivityOutput {
                job_name: input.job_name,
                scheduled_at_ms: None,
                next_run_at_ms: None,
                coalesced: true,
                backing_off: false,
            },
            ScheduledJobClaimOutcome::BackingOff { next_run_at_ms } => {
                ScheduledJobClaimActivityOutput {
                    job_name: input.job_name,
                    scheduled_at_ms: None,
                    next_run_at_ms: Some(next_run_at_ms),
                    coalesced: false,
                    backing_off: true,
                }
            }
        });
    }

    let claimed = retry_scheduler_call(
        "claim due scheduled job run",
        Some(&activities.shutdown),
        || {
            let scheduler = activities.scheduler.clone();
            let job_name = job_name.clone();
            async move { scheduler.claim_due_run(&job_name, input.now_ms).await }
        },
    )
    .await?;
    let scheduled_at_ms = claimed.map(|run| run.scheduled_at_ms);
    let next_run_at_ms = if scheduled_at_ms.is_some() {
        None
    } else {
        retry_scheduler_call(
            "read next scheduled job run",
            Some(&activities.shutdown),
            || {
                let scheduler = activities.scheduler.clone();
                let job_name = job_name.clone();
                async move { scheduler.next_run_at(&job_name).await }
            },
        )
        .await?
    };
    Ok(ScheduledJobClaimActivityOutput {
        job_name: input.job_name,
        scheduled_at_ms,
        next_run_at_ms,
        coalesced: false,
        backing_off: false,
    })
}

async fn run_scheduled_job_complete_activity(
    activities: ScheduledJobWorkflowActivities,
    _workflow_id: String,
    input: ScheduledJobCompleteActivityInput,
) -> Result<ScheduledJobRunActivityOutput, String> {
    let job_name = JobName::new(&input.job_name).map_err(|error| error.to_string())?;
    if activities.shutdown.state().phase != ShutdownPhase::Running {
        retry_scheduler_call("complete scheduled job shutdown", None, || {
            let scheduler = activities.scheduler.clone();
            let job_name = job_name.clone();
            async move {
                scheduler
                    .complete_shutdown(&job_name, input.finished_at_ms)
                    .await
            }
        })
        .await?;
        return Ok(ScheduledJobRunActivityOutput {
            succeeded: false,
            error: Some("scheduler is shutting down".to_owned()),
        });
    }
    if input.succeeded {
        retry_scheduler_call("complete scheduled job success", None, || {
            let scheduler = activities.scheduler.clone();
            let job_name = job_name.clone();
            async move {
                scheduler
                    .complete_success(&job_name, input.finished_at_ms)
                    .await
            }
        })
        .await?;
    } else {
        retry_scheduler_call("complete scheduled job failure", None, || {
            let scheduler = activities.scheduler.clone();
            let job_name = job_name.clone();
            let error = input.error.clone();
            async move {
                scheduler
                    .complete_failure(
                        &job_name,
                        input.finished_at_ms,
                        error.as_deref().unwrap_or("scheduled job failed"),
                    )
                    .await
            }
        })
        .await?;
    }
    Ok(ScheduledJobRunActivityOutput {
        succeeded: input.succeeded,
        error: input.error,
    })
}

async fn retry_scheduler_call<T, F, Fut>(
    operation: &'static str,
    shutdown: Option<&ShutdownSignal>,
    mut call: F,
) -> Result<T, String>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, SchedulerError>>,
{
    match retry_with_classification(
        TRANSIENT_IO_RETRY_MAX_ATTEMPTS,
        transient_io_retry_policy(),
        operation,
        shutdown,
        |_attempt| call(),
        classify_scheduler_error,
    )
    .await
    {
        RetryOutcome::Completed(result) => result.map_err(|error| error.to_string()),
        RetryOutcome::Shutdown => Err("scheduler is shutting down".to_owned()),
        RetryOutcome::Exhausted => Err(DatabaseError::Busy {
            operation: operation.to_owned(),
            retry_after_ms: None,
        }
        .to_string()),
    }
}

fn classify_scheduler_error(error: &SchedulerError) -> RetryDecision {
    match error {
        SchedulerError::Database { source } => classify_database_error(source),
        SchedulerError::InvalidConfig { .. } | SchedulerError::UnknownJob { .. } => {
            RetryDecision::do_not_retry(RetryErrorKind::FatalLocal)
        }
    }
}

async fn run_scheduled_job_activity(
    activities: ScheduledJobWorkflowActivities,
    _workflow_id: String,
    input: ScheduledJobRunActivityInput,
) -> Result<ScheduledJobRunActivityOutput, String> {
    if activities.shutdown.state().phase != ShutdownPhase::Running {
        return Ok(ScheduledJobRunActivityOutput {
            succeeded: false,
            error: Some("scheduler is shutting down".to_owned()),
        });
    }
    let Some(state) = activities.state.get() else {
        return Ok(ScheduledJobRunActivityOutput {
            succeeded: false,
            error: Some("scheduled job state handle is not bound".to_owned()),
        });
    };
    let job_name = JobName::new(&input.job_name).map_err(|error| error.to_string())?;
    if input.job_name == MEDIA_INVENTORY_JOB_NAME || input.job_name == CLIENT_INVENTORY_JOB_NAME {
        let Some(inventory) = activities.inventory.clone() else {
            return Ok(ScheduledJobRunActivityOutput {
                succeeded: false,
                error: Some("scheduled job inventory activities are not registered".to_owned()),
            });
        };
        let Some(active_inventory_refreshes) = activities.active_inventory_refreshes.clone() else {
            return Ok(ScheduledJobRunActivityOutput {
                succeeded: false,
                error: Some("scheduled job inventory tracker is not registered".to_owned()),
            });
        };
        let request = if input.job_name == MEDIA_INVENTORY_JOB_NAME {
            InventoryWorkflowRequest::media_full(
                state.config.paths.media_dirs.clone(),
                input.scheduled_at_ms,
            )
        } else {
            InventoryWorkflowRequest::client(input.scheduled_at_ms)
        };
        let workflow_id = request
            .instance_id()
            .map_err(|error| error.to_string())?
            .to_string();
        {
            let Ok(mut active) = active_inventory_refreshes.lock() else {
                return Ok(ScheduledJobRunActivityOutput {
                    succeeded: false,
                    error: Some("inventory refresh tracker is poisoned".to_owned()),
                });
            };
            if !active.insert(workflow_id.clone()) {
                return Ok(ScheduledJobRunActivityOutput {
                    succeeded: true,
                    error: None,
                });
            }
        }
        let output = run_inventory_activity(
            inventory,
            active_inventory_refreshes,
            workflow_id,
            InventoryActivityInput {
                request,
                started_at_ms: input.scheduled_at_ms,
            },
        )
        .await?;
        return Ok(ScheduledJobRunActivityOutput {
            succeeded: output.scan_failure_count == 0,
            error: (output.scan_failure_count > 0).then(|| "inventory refresh failed".to_owned()),
        });
    }
    match execute_scheduled_job(&state, &job_name, activities.shutdown.clone()).await {
        Ok(()) => Ok(ScheduledJobRunActivityOutput {
            succeeded: true,
            error: None,
        }),
        Err(error) => Ok(ScheduledJobRunActivityOutput {
            succeeded: false,
            error: Some(error),
        }),
    }
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

async fn run_announce_process_activity(
    activities: AnnounceWorkflowActivities,
    workflow_id: String,
    input: AnnounceActivityInput,
) -> Result<AnnounceProcessActivityOutput, String> {
    let id = AnnounceWorkId::new(input.work_id.clone()).map_err(|error| error.to_string())?;
    let now_ms = unix_time_ms();
    let lease_until_ms = now_ms.saturating_add(
        i64::try_from(
            activities
                .queue_config
                .lease_duration_secs
                .saturating_mul(1_000),
        )
        .unwrap_or(i64::MAX),
    );
    let worker = AnnounceWorker::new(
        activities.repository.clone(),
        ANNOUNCE_WORKFLOW_OWNER,
        &activities.queue_config,
    )
    .map_err(|error| error.to_string())?;
    if let Some(output) =
        complete_checkpointed_announce_activity(&activities, &worker, &workflow_id, &input, &id)
            .await?
    {
        return Ok(output);
    }
    let claimed = activities
        .repository
        .claim_announce_work_by_id(&id, ANNOUNCE_WORKFLOW_OWNER, now_ms, lease_until_ms)
        .await
        .map_err(|error| error.to_string())?;
    if !claimed {
        if let Some(output) =
            completed_announce_activity_output(&activities.repository, &id, now_ms).await?
        {
            record_announce_activity_projection(
                &activities.repository,
                &workflow_id,
                &input,
                &output,
                unix_time_ms(),
            )
            .await?;
            return Ok(output);
        }
        return Err(format!(
            "announce work `{}` could not be claimed",
            id.as_str()
        ));
    }

    let outcome = process_announce_work_with_processor_mode(
        activities.processor.clone(),
        id.clone(),
        activities.shutdown.clone(),
        AnnounceInventoryRefreshMode::DeferToWorkflow,
    )
    .await;
    let mut output = announce_process_activity_output(&outcome, now_ms);
    if output.state == AnnounceActivityState::WaitingInventory {
        output.events =
            register_announce_inventory_waiters(&activities, &workflow_id, input.received_at_ms)
                .await?;
    }
    record_announce_action_checkpoint(&activities.repository, &id, &outcome, unix_time_ms())
        .await?;
    let completed = worker
        .complete(&id, outcome, unix_time_ms())
        .await
        .map_err(|error| error.to_string())?;
    if !completed {
        return Err(format!(
            "announce work `{}` outcome could not be recorded for workflow `{workflow_id}`",
            id.as_str()
        ));
    }
    record_announce_activity_projection(
        &activities.repository,
        &workflow_id,
        &input,
        &output,
        unix_time_ms(),
    )
    .await?;
    Ok(output)
}

async fn complete_checkpointed_announce_activity(
    activities: &AnnounceWorkflowActivities,
    worker: &AnnounceWorker,
    workflow_id: &str,
    input: &AnnounceActivityInput,
    id: &AnnounceWorkId,
) -> Result<Option<AnnounceProcessActivityOutput>, String> {
    let Some(checkpoint) = activities
        .repository
        .announce_action_checkpoint(id)
        .await
        .map_err(|error| error.to_string())?
    else {
        return Ok(None);
    };
    if checkpoint.status != AnnounceStatus::Running
        || checkpoint.lease_owner.as_deref() != Some(ANNOUNCE_WORKFLOW_OWNER)
    {
        return Ok(None);
    }
    let Some(outcome) = announce_checkpoint_outcome(checkpoint.action_outcome.as_deref()) else {
        return Ok(None);
    };
    let now_ms = unix_time_ms();
    let output = announce_process_activity_output(&outcome, now_ms);
    let completed = worker
        .complete(id, outcome, now_ms)
        .await
        .map_err(|error| error.to_string())?;
    if !completed {
        return Err(format!(
            "checkpointed announce work `{}` outcome could not be recorded for workflow `{workflow_id}`",
            id.as_str()
        ));
    }
    record_announce_activity_projection(
        &activities.repository,
        workflow_id,
        input,
        &output,
        unix_time_ms(),
    )
    .await?;
    Ok(Some(output))
}

fn announce_checkpoint_outcome(outcome: Option<&str>) -> Option<AnnounceWorkOutcome> {
    let (reason, outcome) = match outcome? {
        "saved" => (AnnounceReason::Saved, "saved"),
        "injected" => (AnnounceReason::Injected, "injected"),
        "dry_run" => (AnnounceReason::DryRun, "dry_run"),
        "already_exists" => (AnnounceReason::AlreadyExists, "already_exists"),
        _ => return None,
    };
    Some(AnnounceWorkOutcome::Succeeded {
        reason,
        outcome: outcome.to_owned(),
    })
}

async fn record_announce_action_checkpoint(
    repository: &Repository,
    id: &AnnounceWorkId,
    outcome: &AnnounceWorkOutcome,
    now_ms: i64,
) -> Result<(), String> {
    let AnnounceWorkOutcome::Succeeded { reason, outcome } = outcome else {
        return Ok(());
    };
    let recorded = retry_database_call("record announce action checkpoint", None, || {
        repository.record_announce_action_checkpoint(
            id,
            ANNOUNCE_WORKFLOW_OWNER,
            *reason,
            outcome,
            now_ms,
        )
    })
    .await
    .map_err(|error| error.to_string())?;
    if recorded {
        Ok(())
    } else {
        Err(format!(
            "announce work `{}` action checkpoint could not be recorded",
            id.as_str()
        ))
    }
}

async fn record_announce_start_projection(
    repository: &Option<Repository>,
    workflow_id: &str,
    work: &AnnounceWorkItem,
    now_ms: i64,
) -> Result<(), String> {
    let Some(repository) = repository.as_ref() else {
        return Ok(());
    };
    let update = WorkflowProjectionUpdate {
        workflow_id,
        workflow_kind: WorkflowKind::Announce,
        public_id: work.id.as_str(),
        state: WorkflowState::Running,
        reason: WorkflowReason::RunningActivity,
        next_action: Some("starting"),
        raw_secret_material_count: u16::from(work.fetch.is_some()),
        blocked_dependency: None,
        started_at_ms: work.received_at_ms,
        updated_at_ms: now_ms,
        finished_at_ms: None,
    };
    retry_database_call("record initial announce workflow projection", None, || {
        repository.record_workflow_projection(&update)
    })
    .await
    .map(|_| ())
    .map_err(|error| error.to_string())
}

async fn completed_announce_activity_output(
    repository: &Repository,
    id: &AnnounceWorkId,
    now_ms: i64,
) -> Result<Option<AnnounceProcessActivityOutput>, String> {
    let Some(work) = repository
        .announce_work_item(id)
        .await
        .map_err(|error| error.to_string())?
    else {
        return Ok(None);
    };
    match work.status {
        AnnounceStatus::Succeeded => Ok(Some(announce_process_activity_output(
            &AnnounceWorkOutcome::Succeeded {
                reason: work.reason,
                outcome: announce_reason_label(work.reason),
            },
            now_ms,
        ))),
        AnnounceStatus::TerminalFailed | AnnounceStatus::Expired => {
            Ok(Some(announce_process_activity_output(
                &AnnounceWorkOutcome::TerminalFailed {
                    reason: work.reason,
                    redacted_message: work
                        .last_redacted_message
                        .map(|message| message.as_str().to_owned())
                        .unwrap_or_else(|| announce_reason_label(work.reason)),
                },
                now_ms,
            )))
        }
        AnnounceStatus::Queued
        | AnnounceStatus::Running
        | AnnounceStatus::Waiting
        | AnnounceStatus::Retryable => Ok(None),
    }
}

async fn run_announce_queue_inventory_activity(
    activities: AnnounceWorkflowActivities,
    _workflow_id: String,
    input: AnnounceActivityInput,
) -> Result<AnnounceWaitActivityOutput, String> {
    let events = activities
        .processor
        .stale_inventory_completion_events(input.received_at_ms)
        .await
        .map_err(|error| error.to_string())?;
    activities
        .processor
        .queue_stale_inventory_refreshes(input.received_at_ms, unix_time_ms())
        .await
        .map_err(|error| error.to_string())?;
    Ok(AnnounceWaitActivityOutput { events })
}

async fn register_announce_inventory_waiters(
    activities: &AnnounceWorkflowActivities,
    workflow_id: &str,
    received_at_ms: i64,
) -> Result<Vec<WorkflowEventName>, String> {
    let Some(completion_events) = activities.completion_events.as_ref() else {
        return Err("inventory completion event bridge is unavailable".to_owned());
    };
    let events = activities
        .processor
        .stale_inventory_completion_events(received_at_ms)
        .await
        .map_err(|error| error.to_string())?;
    for event_name in &events {
        completion_events
            .register_waiter(InventoryCompletionWaiter {
                workflow_id: workflow_id.to_owned(),
                event_name: *event_name,
                required_after_ms: received_at_ms,
            })
            .await?;
    }
    activities
        .processor
        .stale_inventory_completion_events(received_at_ms)
        .await
        .map_err(|error| error.to_string())
}

fn announce_process_activity_output(
    outcome: &AnnounceWorkOutcome,
    now_ms: i64,
) -> AnnounceProcessActivityOutput {
    match outcome {
        AnnounceWorkOutcome::Succeeded { reason, .. } => AnnounceProcessActivityOutput {
            state: AnnounceActivityState::Succeeded,
            reason: announce_reason_label(*reason),
            next_attempt_at_ms: None,
            retry_delay_ms: None,
            dependency: None,
            events: Vec::new(),
        },
        AnnounceWorkOutcome::TerminalFailed { reason, .. } => AnnounceProcessActivityOutput {
            state: AnnounceActivityState::Failed,
            reason: announce_reason_label(*reason),
            next_attempt_at_ms: None,
            retry_delay_ms: None,
            dependency: None,
            events: Vec::new(),
        },
        AnnounceWorkOutcome::Waiting {
            reason,
            next_attempt_at_ms,
            dependency,
        } => AnnounceProcessActivityOutput {
            state: if *reason == AnnounceReason::InventoryRefreshing {
                AnnounceActivityState::WaitingInventory
            } else {
                AnnounceActivityState::WaitingDependency
            },
            reason: announce_reason_label(*reason),
            next_attempt_at_ms: Some(*next_attempt_at_ms),
            retry_delay_ms: Some(retry_delay_ms(now_ms, *next_attempt_at_ms)),
            dependency: dependency.as_ref().map(AnnounceProjectionDependency::from),
            events: Vec::new(),
        },
        AnnounceWorkOutcome::Retryable {
            reason,
            next_attempt_at_ms,
            ..
        } => AnnounceProcessActivityOutput {
            state: AnnounceActivityState::Retrying,
            reason: announce_reason_label(*reason),
            next_attempt_at_ms: Some(*next_attempt_at_ms),
            retry_delay_ms: Some(retry_delay_ms(now_ms, *next_attempt_at_ms)),
            dependency: None,
            events: Vec::new(),
        },
        AnnounceWorkOutcome::Release {
            reason,
            next_attempt_at_ms,
        } => AnnounceProcessActivityOutput {
            state: AnnounceActivityState::Released,
            reason: announce_reason_label(*reason),
            next_attempt_at_ms: Some(*next_attempt_at_ms),
            retry_delay_ms: Some(retry_delay_ms(now_ms, *next_attempt_at_ms)),
            dependency: None,
            events: Vec::new(),
        },
    }
}

fn retry_delay_ms(now_ms: i64, next_attempt_at_ms: i64) -> u64 {
    u64::try_from(next_attempt_at_ms.saturating_sub(now_ms).max(1)).unwrap_or(u64::MAX)
}

fn announce_reason_label(reason: AnnounceReason) -> String {
    format!("{reason:?}")
}

async fn record_announce_activity_projection(
    repository: &Repository,
    workflow_id: &str,
    input: &AnnounceActivityInput,
    output: &AnnounceProcessActivityOutput,
    now_ms: i64,
) -> Result<(), String> {
    let mut blocked_dependency = None;
    let (state, reason, next_action, finished_at_ms, raw_secret_material_count) = match output.state
    {
        AnnounceActivityState::Succeeded => (
            WorkflowState::Succeeded,
            WorkflowReason::Completed,
            Some(output.reason.as_str()),
            Some(now_ms),
            0,
        ),
        AnnounceActivityState::Failed => (
            WorkflowState::Failed,
            WorkflowReason::Failed,
            Some(output.reason.as_str()),
            Some(now_ms),
            0,
        ),
        AnnounceActivityState::WaitingInventory => (
            WorkflowState::Waiting,
            WorkflowReason::WaitingForInventory,
            Some("await_inventory"),
            None,
            input.raw_secret_material_count,
        ),
        AnnounceActivityState::WaitingDependency => (
            WorkflowState::Waiting,
            WorkflowReason::WaitingForDependency,
            Some(output.reason.as_str()),
            None,
            input.raw_secret_material_count,
        ),
        AnnounceActivityState::Retrying | AnnounceActivityState::Released => (
            WorkflowState::Retrying,
            WorkflowReason::BackingOff,
            Some(output.reason.as_str()),
            None,
            input.raw_secret_material_count,
        ),
    };
    if output.state == AnnounceActivityState::WaitingDependency {
        blocked_dependency = output
            .dependency
            .as_ref()
            .and_then(|dependency| {
                DependencyKind::from_persisted(&dependency.kind).map(|kind| (kind, dependency))
            })
            .map(|dependency| WorkflowProjectionDependency {
                kind: dependency.0,
                name: dependency.1.name.as_str(),
            });
    }
    let update = WorkflowProjectionUpdate {
        workflow_id,
        workflow_kind: WorkflowKind::Announce,
        public_id: &input.work_id,
        state,
        reason,
        next_action,
        raw_secret_material_count,
        blocked_dependency,
        started_at_ms: input.received_at_ms,
        updated_at_ms: now_ms,
        finished_at_ms,
    };
    retry_database_call("record announce workflow projection", None, || {
        repository.record_workflow_projection(&update)
    })
    .await
    .map(|_| ())
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
        } else if workflow == WorkflowKind::Announce {
            builder = builder.register_typed(workflow.orchestration_name(), announce_orchestration);
        } else if workflow == WorkflowKind::Search {
            builder = builder.register_typed(workflow.orchestration_name(), search_orchestration);
        } else if workflow == WorkflowKind::ScheduledJob {
            builder = builder.register_typed(
                workflow.orchestration_name(),
                scheduled_job_supervisor_orchestration,
            );
        } else if workflow == WorkflowKind::SavedTorrentRetry {
            builder = builder.register_typed(
                workflow.orchestration_name(),
                saved_retry_supervisor_orchestration,
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
    #[cfg(test)]
    {
        builder = builder.register_typed(
            TEST_INVENTORY_WAIT_ORCHESTRATION,
            |ctx: OrchestrationContext, event_name: String| async move {
                let event: InventoryCompletionEvent = ctx.dequeue_event_typed(event_name).await;
                Ok(format!(
                    "{}:{}",
                    event.source_workflow_id, event.persisted_items
                ))
            },
        );
    }
    builder = builder.register_typed(
        SCHEDULED_JOB_RUN_ORCHESTRATION,
        scheduled_job_run_orchestration,
    );
    builder = builder.register_typed(
        SAVED_RETRY_ITEM_ORCHESTRATION,
        saved_retry_item_orchestration,
    );
    builder.build()
}

async fn scheduled_job_supervisor_orchestration(
    ctx: OrchestrationContext,
    input: WorkflowSupervisorInput,
) -> Result<WorkflowSupervisorOutput, String> {
    if input.kind != WorkflowKind::ScheduledJob {
        return Err(format!(
            "scheduled job supervisor received {} input",
            input.kind.as_str()
        ));
    }
    loop {
        let now_ms = orchestration_now_ms(&ctx).await?;
        let claim = claim_scheduled_job(&ctx, &input.public_id, now_ms, None).await?;
        if let Some(scheduled_at_ms) = claim.scheduled_at_ms {
            let run_input = ScheduledJobWorkflowInput {
                job_name: input.public_id.clone(),
                forced: false,
                requested_at_ms: scheduled_at_ms,
            };
            run_scheduled_job_child(&ctx, &input.public_id, scheduled_at_ms, &run_input).await?;
            continue;
        }

        let wait_ms = claim
            .next_run_at_ms
            .map(|next_run_at_ms| next_run_at_ms.saturating_sub(now_ms).max(1))
            .unwrap_or(60_000);
        let timer = ctx.schedule_timer(Duration::from_millis(
            u64::try_from(wait_ms).map_err(|error| error.to_string())?,
        ));
        let manual =
            ctx.dequeue_event_typed::<ScheduledJobManualRequest>(SCHEDULED_JOB_MANUAL_QUEUE);
        match ctx.select2(timer, manual).await {
            Either2::First(()) => {}
            Either2::Second(manual) => {
                let scheduled_at_ms = if let Some(scheduled_at_ms) = manual.claimed_scheduled_at_ms
                {
                    Some(scheduled_at_ms)
                } else {
                    claim_scheduled_job(&ctx, &input.public_id, now_ms, Some(manual.clone()))
                        .await?
                        .scheduled_at_ms
                };
                if let Some(scheduled_at_ms) = scheduled_at_ms {
                    let run_input = ScheduledJobWorkflowInput {
                        job_name: input.public_id.clone(),
                        forced: manual.forced,
                        requested_at_ms: scheduled_at_ms,
                    };
                    run_scheduled_job_child(&ctx, &input.public_id, scheduled_at_ms, &run_input)
                        .await?;
                }
            }
        }
    }
}

async fn claim_scheduled_job(
    ctx: &OrchestrationContext,
    job_name: &str,
    now_ms: i64,
    manual: Option<ScheduledJobManualRequest>,
) -> Result<ScheduledJobClaimActivityOutput, String> {
    let input = ActivityInputEnvelope::new(
        ctx.instance_id(),
        SCHEDULED_JOB_CLAIM_ACTIVITY_ID,
        ScheduledJobClaimActivityInput {
            job_name: job_name.to_owned(),
            now_ms,
            manual,
        },
    );
    ctx.schedule_activity_typed(ActivityKind::ScheduledJobClaim.as_str(), &input)
        .await
}

async fn run_scheduled_job_child(
    ctx: &OrchestrationContext,
    job_name: &str,
    scheduled_at_ms: i64,
    input: &ScheduledJobWorkflowInput,
) -> Result<(), String> {
    let scheduled_at_ms_u64 = u64::try_from(scheduled_at_ms)
        .map_err(|_error| format!("scheduled_at_ms must be non-negative: {scheduled_at_ms}"))?;
    let child_id = WorkflowInstanceId::scheduled_job_run(job_name, scheduled_at_ms_u64)
        .map_err(|error| error.to_string())?;
    let result: ScheduledJobRunActivityOutput = ctx
        .schedule_sub_orchestration_with_id_typed(
            SCHEDULED_JOB_RUN_ORCHESTRATION,
            child_id.as_str(),
            input,
        )
        .await?;
    if !result.succeeded {
        ctx.trace_warn(format!(
            "Scheduled job `{job_name}` run finished unsuccessfully: {}",
            result
                .error
                .unwrap_or_else(|| "scheduled job failed".to_owned())
        ));
    }
    Ok(())
}

async fn scheduled_job_run_orchestration(
    ctx: OrchestrationContext,
    input: ScheduledJobWorkflowInput,
) -> Result<ScheduledJobRunActivityOutput, String> {
    let scheduled_at_ms = input.requested_at_ms;
    let run_input = ActivityInputEnvelope::new(
        ctx.instance_id(),
        SCHEDULED_JOB_RUN_ACTIVITY_ID,
        ScheduledJobRunActivityInput {
            job_name: input.job_name.clone(),
            scheduled_at_ms,
        },
    );
    let result: ScheduledJobRunActivityOutput = ctx
        .schedule_activity_typed(ActivityKind::ScheduledJobRun.as_str(), &run_input)
        .await?;
    let finished_at_ms = orchestration_now_ms(&ctx).await?;
    let complete_input = ActivityInputEnvelope::new(
        ctx.instance_id(),
        SCHEDULED_JOB_COMPLETE_ACTIVITY_ID,
        ScheduledJobCompleteActivityInput {
            job_name: input.job_name,
            scheduled_at_ms,
            succeeded: result.succeeded,
            error: result.error.clone(),
            finished_at_ms,
        },
    );
    ctx.schedule_activity_typed(ActivityKind::ScheduledJobComplete.as_str(), &complete_input)
        .await
}

async fn saved_retry_supervisor_orchestration(
    ctx: OrchestrationContext,
    input: WorkflowSupervisorInput,
) -> Result<WorkflowSupervisorOutput, String> {
    if input.kind != WorkflowKind::SavedTorrentRetry {
        return Err(format!(
            "saved retry supervisor received {} input",
            input.kind.as_str()
        ));
    }
    let mut run_reason = "startup".to_owned();
    loop {
        let requested_at_ms = orchestration_now_ms(&ctx).await?;
        let run_input = SavedRetryWorkflowInput {
            reason: run_reason.clone(),
            requested_at_ms,
        };
        let run = run_saved_retry_batch(&ctx, &input.public_id, &run_input).await?;
        set_saved_retry_custom_status(
            &ctx,
            &input.public_id,
            WorkflowState::Waiting,
            WorkflowReason::WaitingForDependency,
            Some("await_interval"),
        )?;
        ctx.schedule_timer(Duration::from_millis(run.interval_ms.max(1)))
            .await;
        run_reason = "interval".to_owned();
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct SavedRetryRunOutput {
    summary: SavedTorrentRetrySummary,
    interval_ms: u64,
}

async fn run_saved_retry_batch(
    ctx: &OrchestrationContext,
    public_id: &str,
    input: &SavedRetryWorkflowInput,
) -> Result<SavedRetryRunOutput, String> {
    set_saved_retry_custom_status(
        ctx,
        public_id,
        WorkflowState::Running,
        WorkflowReason::RunningActivity,
        Some("scanning"),
    )?;
    let scan_input = ActivityInputEnvelope::new(
        ctx.instance_id(),
        SAVED_RETRY_SCAN_ACTIVITY_ID,
        SavedRetryScanActivityInput {
            requested_at_ms: input.requested_at_ms,
            reason: input.reason.clone(),
        },
    );
    let scan: SavedRetryScanActivityOutput = ctx
        .schedule_activity_typed(ActivityKind::SavedRetryScan.as_str(), &scan_input)
        .await?;
    let mut summary = SavedTorrentRetrySummary {
        failed: scan.failed,
        ..SavedTorrentRetrySummary::default()
    };
    set_saved_retry_custom_status(
        ctx,
        public_id,
        WorkflowState::Running,
        WorkflowReason::RunningActivity,
        Some("processing_items"),
    )?;
    let mut chunks = scan.items.chunks(SAVED_RETRY_ITEM_CHILD_CONCURRENCY);
    for chunk in &mut chunks {
        match chunk {
            [first, second] => {
                let first = saved_retry_item_child(ctx, first.clone(), input.requested_at_ms)?;
                let second = saved_retry_item_child(ctx, second.clone(), input.requested_at_ms)?;
                let (first, second) = ctx.join2(first, second).await;
                summary.merge(first?);
                summary.merge(second?);
            }
            [single] => {
                let item = saved_retry_item_child(ctx, single.clone(), input.requested_at_ms)?;
                summary.merge(item.await?);
            }
            _ => {}
        }
    }
    set_saved_retry_custom_status(
        ctx,
        public_id,
        WorkflowState::Running,
        WorkflowReason::RunningActivity,
        Some("finalizing"),
    )?;
    let finalize_input = ActivityInputEnvelope::new(
        ctx.instance_id(),
        SAVED_RETRY_FINALIZE_ACTIVITY_ID,
        SavedRetryFinalizeActivityInput {
            requested_at_ms: input.requested_at_ms,
            summary,
        },
    );
    let output: SavedRetryFinalizeActivityOutput = ctx
        .schedule_activity_typed(ActivityKind::SavedRetryFinalize.as_str(), &finalize_input)
        .await?;
    set_saved_retry_custom_status(
        ctx,
        public_id,
        WorkflowState::Succeeded,
        WorkflowReason::Completed,
        Some("completed"),
    )?;
    Ok(SavedRetryRunOutput {
        summary: output.summary,
        interval_ms: scan.interval_ms,
    })
}

async fn saved_retry_item_orchestration(
    ctx: OrchestrationContext,
    input: SavedRetryProcessActivityInput,
) -> Result<SavedTorrentRetrySummary, String> {
    let process_input =
        ActivityInputEnvelope::new(ctx.instance_id(), SAVED_RETRY_PROCESS_ACTIVITY_ID, input);
    ctx.schedule_activity_typed(ActivityKind::SavedRetryProcess.as_str(), &process_input)
        .await
}

fn saved_retry_item_child(
    ctx: &OrchestrationContext,
    item: SavedTorrentRetryItem,
    requested_at_ms: i64,
) -> Result<impl std::future::Future<Output = Result<SavedTorrentRetrySummary, String>>, String> {
    let requested_at_ms = u64::try_from(requested_at_ms)
        .map_err(|_error| format!("saved retry requested_at_ms is negative: {requested_at_ms}"))?;
    let child_key = format!("{}.{}", item.item_key, requested_at_ms);
    let child_id =
        WorkflowInstanceId::saved_retry_item(&child_key).map_err(|error| error.to_string())?;
    let input = SavedRetryProcessActivityInput {
        item,
        requested_at_ms: i64::try_from(requested_at_ms).unwrap_or(i64::MAX),
    };
    Ok(ctx.schedule_sub_orchestration_with_id_typed(
        SAVED_RETRY_ITEM_ORCHESTRATION,
        child_id.as_str(),
        &input,
    ))
}

async fn orchestration_now_ms(ctx: &OrchestrationContext) -> Result<i64, String> {
    system_time_to_unix_ms(ctx.utc_now().await?)
}

fn system_time_to_unix_ms(time: SystemTime) -> Result<i64, String> {
    let duration = time
        .duration_since(UNIX_EPOCH)
        .map_err(|error| error.to_string())?;
    i64::try_from(duration.as_millis()).map_err(|error| error.to_string())
}

async fn search_orchestration(
    ctx: OrchestrationContext,
    input: SearchWorkflowInput,
) -> Result<SearchWorkflowExecutionSummary, String> {
    set_search_custom_status(
        &ctx,
        &input,
        WorkflowState::Running,
        WorkflowReason::RunningActivity,
        Some("planning"),
    )?;
    let planned_at_ms = orchestration_now_ms(&ctx).await?;
    let plan_input = ActivityInputEnvelope::new(
        ctx.instance_id(),
        SEARCH_PLAN_ACTIVITY_ID,
        SearchPlanActivityInput {
            input: input.clone(),
            planned_at_ms,
        },
    );
    let plan: SearchPlanActivityOutput = ctx
        .schedule_activity_typed(ActivityKind::SearchPlan.as_str(), &plan_input)
        .await?;
    let mut summary = SearchWorkflowExecutionSummary {
        planned_indexers: plan.planned_indexers,
        failed_indexers: plan.failed_indexers,
        candidates: plan.candidate_count,
        ..SearchWorkflowExecutionSummary::default()
    };

    set_search_custom_status(
        &ctx,
        &input,
        WorkflowState::Running,
        WorkflowReason::RunningActivity,
        Some("processing_candidates"),
    )?;
    let mut next_ordinal = 0_u32;
    while usize::try_from(next_ordinal).map_err(|error| error.to_string())? < plan.candidate_count {
        let page_input = ActivityInputEnvelope::new(
            ctx.instance_id(),
            format!("{SEARCH_CANDIDATE_PAGE_ACTIVITY_ID_PREFIX}-{next_ordinal}"),
            SearchCandidatePageActivityInput {
                start_ordinal: next_ordinal,
                limit: SEARCH_CANDIDATE_PAGE_LIMIT,
            },
        );
        let page: SearchCandidatePageActivityOutput = ctx
            .schedule_activity_typed(ActivityKind::SearchCandidatePage.as_str(), &page_input)
            .await?;
        if page.refs.is_empty() {
            break;
        }
        for candidate in page.refs {
            next_ordinal = candidate.ordinal.saturating_add(1);
            let activity_id = format!(
                "{SEARCH_CANDIDATE_ACTIVITY_ID_PREFIX}-{}",
                candidate.ordinal
            );
            let input = ActivityInputEnvelope::new(
                ctx.instance_id(),
                activity_id,
                SearchCandidateActivityInput {
                    candidate,
                    planned_at_ms,
                },
            );
            let output: SearchWorkflowExecutionSummary = ctx
                .schedule_activity_typed(ActivityKind::SearchCandidateProcess.as_str(), &input)
                .await?;
            merge_search_summary(&mut summary, output);
        }
    }

    set_search_custom_status(
        &ctx,
        &input,
        WorkflowState::Running,
        WorkflowReason::RunningActivity,
        Some("finalizing"),
    )?;
    let finalize_input = ActivityInputEnvelope::new(
        ctx.instance_id(),
        SEARCH_FINALIZE_ACTIVITY_ID,
        SearchFinalizeActivityInput {
            summary: summary.clone(),
        },
    );
    let output: SearchFinalizeActivityOutput = ctx
        .schedule_activity_typed(ActivityKind::SearchFinalize.as_str(), &finalize_input)
        .await?;
    set_search_custom_status(
        &ctx,
        &input,
        WorkflowState::Succeeded,
        WorkflowReason::Completed,
        Some("completed"),
    )?;
    Ok(output.summary)
}

fn merge_search_summary(
    total: &mut SearchWorkflowExecutionSummary,
    candidate: SearchWorkflowExecutionSummary,
) {
    total.persisted = total.persisted.saturating_add(candidate.persisted);
    total.downloaded = total.downloaded.saturating_add(candidate.downloaded);
    total.saved = total.saved.saturating_add(candidate.saved);
    total.injected = total.injected.saturating_add(candidate.injected);
    total.dry_run = total.dry_run.saturating_add(candidate.dry_run);
    total.already_present = total
        .already_present
        .saturating_add(candidate.already_present);
    total.rejected = total.rejected.saturating_add(candidate.rejected);
    total.failed = total.failed.saturating_add(candidate.failed);
}

async fn announce_orchestration(
    ctx: OrchestrationContext,
    input: AnnounceWorkflowInput,
) -> Result<String, String> {
    set_announce_custom_status(
        &ctx,
        &input,
        WorkflowState::Running,
        WorkflowReason::RunningActivity,
        Some("processing"),
        input.raw_secret_material_count,
    )?;
    let activity_input = AnnounceActivityInput {
        work_id: input.work_id.clone(),
        received_at_ms: input.received_at_ms,
        raw_secret_material_count: input.raw_secret_material_count,
    };
    loop {
        let process_input = ActivityInputEnvelope::new(
            ctx.instance_id(),
            ANNOUNCE_PROCESS_ACTIVITY_ID,
            activity_input.clone(),
        );
        let output: AnnounceProcessActivityOutput = ctx
            .schedule_activity_typed(ActivityKind::MatchingReverseLookup.as_str(), &process_input)
            .await?;
        match output.state {
            AnnounceActivityState::Succeeded => {
                set_announce_custom_status(
                    &ctx,
                    &input,
                    WorkflowState::Succeeded,
                    WorkflowReason::Completed,
                    Some(output.reason.as_str()),
                    0,
                )?;
                return Ok(output.reason);
            }
            AnnounceActivityState::Failed => {
                set_announce_custom_status(
                    &ctx,
                    &input,
                    WorkflowState::Failed,
                    WorkflowReason::Failed,
                    Some(output.reason.as_str()),
                    0,
                )?;
                return Ok(output.reason);
            }
            AnnounceActivityState::WaitingInventory => {
                set_announce_custom_status(
                    &ctx,
                    &input,
                    WorkflowState::Waiting,
                    WorkflowReason::WaitingForInventory,
                    Some("await_inventory"),
                    input.raw_secret_material_count,
                )?;
                if output.events.is_empty() {
                    continue;
                }
                let setup = queue_announce_inventory_refresh(&ctx, &activity_input).await?;
                if setup.events.is_empty() {
                    continue;
                }
                let wait_for_media = setup
                    .events
                    .contains(&WorkflowEventName::MediaInventoryCompleted);
                let wait_for_client = setup
                    .events
                    .contains(&WorkflowEventName::ClientInventoryCompleted);
                wait_for_announce_inventory_or_recheck(&ctx, wait_for_media, wait_for_client).await;
                set_announce_custom_status(
                    &ctx,
                    &input,
                    WorkflowState::Running,
                    WorkflowReason::RunningActivity,
                    Some("processing"),
                    input.raw_secret_material_count,
                )?;
            }
            AnnounceActivityState::WaitingDependency
            | AnnounceActivityState::Retrying
            | AnnounceActivityState::Released => {
                let delay_ms = output.retry_delay_ms.unwrap_or(1);
                set_announce_custom_status(
                    &ctx,
                    &input,
                    WorkflowState::Retrying,
                    WorkflowReason::BackingOff,
                    Some(output.reason.as_str()),
                    input.raw_secret_material_count,
                )?;
                ctx.schedule_timer(Duration::from_millis(delay_ms)).await;
                set_announce_custom_status(
                    &ctx,
                    &input,
                    WorkflowState::Running,
                    WorkflowReason::RunningActivity,
                    Some("processing"),
                    input.raw_secret_material_count,
                )?;
            }
        }
    }
}

async fn wait_for_announce_inventory_or_recheck(
    ctx: &OrchestrationContext,
    wait_for_media: bool,
    wait_for_client: bool,
) {
    if wait_for_media
        && wait_for_announce_inventory_event_or_recheck(
            ctx,
            WorkflowEventName::MediaInventoryCompleted,
        )
        .await
    {
        return;
    }
    if wait_for_client {
        let _timed_out = wait_for_announce_inventory_event_or_recheck(
            ctx,
            WorkflowEventName::ClientInventoryCompleted,
        )
        .await;
    }
}

async fn wait_for_announce_inventory_event_or_recheck(
    ctx: &OrchestrationContext,
    event_name: WorkflowEventName,
) -> bool {
    let timer = ctx.schedule_timer(ANNOUNCE_INVENTORY_WAIT_RECHECK_INTERVAL);
    let inventory = ctx.dequeue_event_typed::<InventoryCompletionEvent>(event_name.as_str());
    ctx.select2(timer, inventory).await.is_first()
}

async fn queue_announce_inventory_refresh(
    ctx: &OrchestrationContext,
    activity_input: &AnnounceActivityInput,
) -> Result<AnnounceWaitActivityOutput, String> {
    let wait_input = ActivityInputEnvelope::new(
        ctx.instance_id(),
        ANNOUNCE_WAIT_ACTIVITY_ID,
        activity_input.clone(),
    );
    let output: AnnounceWaitActivityOutput = ctx
        .schedule_activity_typed(ActivityKind::RepositoryWrite.as_str(), &wait_input)
        .await?;
    Ok(output)
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

fn set_announce_custom_status(
    ctx: &OrchestrationContext,
    input: &AnnounceWorkflowInput,
    state: WorkflowState,
    reason: WorkflowReason,
    next_action: Option<&str>,
    raw_secret_material_count: u16,
) -> Result<(), String> {
    let mut status =
        WorkflowCustomStatus::new(input.work_id.clone(), WorkflowKind::Announce, state, reason);
    status.next_action = next_action.map(str::to_owned);
    status.raw_secret_material_count = raw_secret_material_count;
    let status = serde_json::to_string(&status).map_err(|error| error.to_string())?;
    ctx.set_custom_status(status);
    Ok(())
}

fn set_search_custom_status(
    ctx: &OrchestrationContext,
    input: &SearchWorkflowInput,
    state: WorkflowState,
    reason: WorkflowReason,
    next_action: Option<&str>,
) -> Result<(), String> {
    let mut status = WorkflowCustomStatus::new(
        input.request_id.clone(),
        WorkflowKind::Search,
        state,
        reason,
    );
    status.next_action = next_action.map(str::to_owned);
    let status = serde_json::to_string(&status).map_err(|error| error.to_string())?;
    ctx.set_custom_status(status);
    Ok(())
}

fn set_saved_retry_custom_status(
    ctx: &OrchestrationContext,
    public_id: &str,
    state: WorkflowState,
    reason: WorkflowReason,
    next_action: Option<&str>,
) -> Result<(), String> {
    let mut status = WorkflowCustomStatus::new(
        public_id.to_owned(),
        WorkflowKind::SavedTorrentRetry,
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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use duroxide::runtime::OrchestrationStatus;

    use super::*;
    use crate::announce::{AnnounceDedupeIdentity, AnnounceFetchMaterial};
    use crate::config::SporosConfig;
    use crate::domain::{ByteSize, CandidateGuid, DownloadUrl, ItemTitle, TrackerName};
    use crate::inventory::InventoryScanOptions;
    use crate::persistence::repository::Repository;
    use crate::runtime::app::AppRuntime;
    use crate::runtime::health::HealthRegistry;
    use crate::runtime::injection_worker::InjectionWorker;
    use crate::runtime::scheduler::{
        CLEANUP_JOB_NAME, CLIENT_INVENTORY_JOB_NAME, INDEXER_CAPS_JOB_NAME,
        MEDIA_INVENTORY_JOB_NAME,
    };
    use crate::runtime::shutdown::shutdown_channel;
    use crate::secrets::CookieSecret;

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
        wait_for_orchestration_running(&runtime.client(), cleanup_id.as_str()).await;

        runtime.shutdown(Some(1_000)).await;
    }

    #[tokio::test]
    async fn scheduled_job_retry_retries_transient_database_failures_before_success() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_for_call = Arc::clone(&attempts);

        let result = retry_scheduler_call("test scheduled retry", None, move || {
            let attempts = Arc::clone(&attempts_for_call);
            async move {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                if attempt == 0 {
                    Err(SchedulerError::Database {
                        source: DatabaseError::Busy {
                            operation: "test scheduled retry".to_owned(),
                            retry_after_ms: Some(1),
                        },
                    })
                } else {
                    Ok("claimed")
                }
            }
        })
        .await
        .unwrap();

        assert_eq!("claimed", result);
        assert_eq!(2, attempts.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn scheduled_job_retry_retries_completion_write_before_success() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_for_call = Arc::clone(&attempts);

        retry_scheduler_call("complete scheduled job failure", None, move || {
            let attempts = Arc::clone(&attempts_for_call);
            async move {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                if attempt == 0 {
                    Err(SchedulerError::Database {
                        source: DatabaseError::Unavailable {
                            operation: "complete scheduled job failure".to_owned(),
                            message: "pool closed".to_owned(),
                        },
                    })
                } else {
                    Ok(())
                }
            }
        })
        .await
        .unwrap();

        assert_eq!(2, attempts.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn scheduled_job_retry_does_not_retry_terminal_scheduler_errors() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_for_call = Arc::clone(&attempts);
        let missing = JobName::new("missing").unwrap();

        let error = retry_scheduler_call("test scheduled terminal", None, move || {
            let attempts = Arc::clone(&attempts_for_call);
            let missing = missing.clone();
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                Err::<(), _>(SchedulerError::UnknownJob { name: missing })
            }
        })
        .await
        .unwrap_err();

        assert!(error.contains("unknown scheduled job missing"));
        assert_eq!(1, attempts.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn scheduled_job_retry_respects_shutdown_before_claim_attempt() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_for_call = Arc::clone(&attempts);
        let (shutdown, shutdown_signal) = shutdown_channel();
        shutdown.begin_draining("test shutdown").unwrap();

        let error = retry_scheduler_call(
            "claim manual scheduled job run",
            Some(&shutdown_signal),
            move || {
                let attempts = Arc::clone(&attempts_for_call);
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, SchedulerError>("claimed")
                }
            },
        )
        .await
        .unwrap_err();

        assert_eq!("scheduler is shutting down", error);
        assert_eq!(0, attempts.load(Ordering::SeqCst));
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
    async fn inventory_refresh_activity_publishes_completion_event_to_registered_waiter() {
        let temp_dir = TestTempDir::new("duroxide-inventory-workflow-event");
        let media_dir = temp_dir.path().join("media");
        let workflow_database = temp_dir.path().join("state").join(WORKFLOW_DATABASE_FILE);
        std::fs::create_dir_all(&media_dir).unwrap();
        std::fs::write(media_dir.join("Event.Movie.2025.mkv"), b"movie").unwrap();
        let repository = Repository::connect(temp_dir.path().join("sporos.sqlite"))
            .await
            .unwrap();
        let inventory_refresh =
            InventoryRefreshWorker::new(repository.clone(), InventoryScanOptions::default());
        let injection_worker = InjectionWorker::new(repository.clone(), Vec::new());
        let (_shutdown, shutdown_signal) = shutdown_channel();
        let runtime = DuroxideWorkflowRuntime::start_with_inventory_activities(
            workflow_database,
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
        runtime
            .client()
            .start_orchestration_typed(
                "waiting-search",
                TEST_INVENTORY_WAIT_ORCHESTRATION,
                WorkflowEventName::MediaInventoryCompleted
                    .as_str()
                    .to_owned(),
            )
            .await
            .unwrap();
        wait_for_orchestration_running(&runtime.client(), "waiting-search").await;
        runtime
            .register_inventory_completion_waiter(InventoryCompletionWaiter {
                workflow_id: "waiting-search".to_owned(),
                event_name: WorkflowEventName::MediaInventoryCompleted,
                required_after_ms: 1_000,
            })
            .await
            .unwrap();

        let submission = runtime
            .submit_inventory_refresh(InventoryWorkflowRequest::media_full(vec![media_dir], 1_000))
            .await
            .unwrap();
        wait_for_inventory_projection_state(
            &repository,
            &submission.workflow_id,
            WorkflowState::Succeeded,
        )
        .await;
        let status = runtime
            .client()
            .wait_for_orchestration("waiting-search", Duration::from_secs(5))
            .await
            .unwrap();

        match status {
            OrchestrationStatus::Completed { output, .. } => {
                assert_eq!("inventory:media:full:1", output);
            }
            other => panic!("expected completed waiting workflow, got {other:?}"),
        }

        runtime.shutdown(Some(1_000)).await;
    }

    #[tokio::test]
    async fn inventory_completion_waiter_survives_bridge_recreation() {
        let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap()) as Arc<dyn Provider>;
        let repository = Repository::connect_in_memory().await.unwrap();
        let orchestration_registry = OrchestrationRegistry::builder()
            .register(
                "WaitInventoryCompletionDurable",
                |ctx: OrchestrationContext, _: String| async move {
                    let event: InventoryCompletionEvent = ctx
                        .dequeue_event_typed(WorkflowEventName::MediaInventoryCompleted.as_str())
                        .await;
                    Ok(format!(
                        "{}:{}:{}",
                        event.source_workflow_id, event.completed_at_ms, event.persisted_items
                    ))
                },
            )
            .build();
        let runtime = Runtime::start_with_store(
            Arc::clone(&store),
            ActivityRegistry::builder().build(),
            orchestration_registry,
        )
        .await;
        let client = Client::new(Arc::clone(&store));
        client
            .start_orchestration(
                "waiting-search-durable",
                "WaitInventoryCompletionDurable",
                "",
            )
            .await
            .unwrap();
        wait_for_orchestration_running(&client, "waiting-search-durable").await;

        let first_bridge =
            InventoryCompletionEventBridge::new(Arc::clone(&store), Some(repository.clone()));
        first_bridge
            .register_waiter(InventoryCompletionWaiter {
                workflow_id: "waiting-search-durable".to_owned(),
                event_name: WorkflowEventName::MediaInventoryCompleted,
                required_after_ms: 2_000,
            })
            .await
            .unwrap();
        drop(first_bridge);

        let second_bridge =
            InventoryCompletionEventBridge::new(Arc::clone(&store), Some(repository.clone()));
        let fanout = second_bridge
            .publish_completion(&InventoryCompletionEvent {
                inventory_kind: InventoryRefreshKind::MediaFull,
                source_workflow_id: "inventory:media:full".to_owned(),
                completed_at_ms: 2_000,
                scanned_items: 1,
                persisted_items: 1,
                pruned_items: 0,
            })
            .await
            .unwrap();
        let status = client
            .wait_for_orchestration("waiting-search-durable", Duration::from_secs(5))
            .await
            .unwrap();

        assert_eq!(
            InventoryCompletionFanout {
                waiters: 1,
                delivered: 1,
                failed: 0
            },
            fanout
        );
        match status {
            OrchestrationStatus::Completed { output, .. } => {
                assert_eq!("inventory:media:full:2000:1", output);
            }
            other => panic!("expected completed waiting workflow, got {other:?}"),
        }

        let remaining = repository
            .workflow_inventory_waiters_ready("media_inventory_completed", 2_000, 10)
            .await
            .unwrap();
        assert!(remaining.is_empty());

        runtime.shutdown(Some(1_000)).await;
    }

    #[tokio::test]
    async fn inventory_completion_waiter_survives_file_backed_restart() {
        let temp_dir = TestTempDir::new("duroxide-inventory-completion-file-restart");
        let workflow_database = temp_dir.path().join("state").join(WORKFLOW_DATABASE_FILE);
        let sporos_database = temp_dir.path().join("sporos.sqlite");
        prepare_workflow_database(&workflow_database).await.unwrap();
        let workflow_database_url = format!("sqlite:{}", workflow_database.display());
        let first_store = Arc::new(
            SqliteProvider::new(&workflow_database_url, None)
                .await
                .unwrap(),
        ) as Arc<dyn Provider>;
        let first_repository = Repository::connect(&sporos_database).await.unwrap();
        let orchestration_registry = OrchestrationRegistry::builder()
            .register(
                "FileBackedInventoryCompletionConsumer",
                |ctx: OrchestrationContext, _: String| async move {
                    let event: InventoryCompletionEvent = ctx
                        .dequeue_event_typed(WorkflowEventName::MediaInventoryCompleted.as_str())
                        .await;
                    Ok(format!(
                        "{}:{}",
                        event.source_workflow_id, event.persisted_items
                    ))
                },
            )
            .build();
        let first_runtime = Runtime::start_with_store(
            Arc::clone(&first_store),
            ActivityRegistry::builder().build(),
            orchestration_registry,
        )
        .await;
        let first_client = Client::new(Arc::clone(&first_store));
        first_client
            .start_orchestration(
                "waiting-search-file-restart",
                "FileBackedInventoryCompletionConsumer",
                "",
            )
            .await
            .unwrap();
        wait_for_orchestration_running(&first_client, "waiting-search-file-restart").await;
        InventoryCompletionEventBridge::new(
            Arc::clone(&first_store),
            Some(first_repository.clone()),
        )
        .register_waiter(InventoryCompletionWaiter {
            workflow_id: "waiting-search-file-restart".to_owned(),
            event_name: WorkflowEventName::MediaInventoryCompleted,
            required_after_ms: 2_000,
        })
        .await
        .unwrap();
        first_runtime.shutdown(Some(1_000)).await;
        first_repository.pool().close().await;

        let second_repository = Repository::connect(&sporos_database).await.unwrap();
        let second_store = Arc::new(
            SqliteProvider::new(&workflow_database_url, None)
                .await
                .unwrap(),
        ) as Arc<dyn Provider>;
        let second_runtime = Runtime::start_with_store(
            Arc::clone(&second_store),
            ActivityRegistry::builder().build(),
            OrchestrationRegistry::builder()
                .register(
                    "FileBackedInventoryCompletionConsumer",
                    |ctx: OrchestrationContext, _: String| async move {
                        let event: InventoryCompletionEvent = ctx
                            .dequeue_event_typed(
                                WorkflowEventName::MediaInventoryCompleted.as_str(),
                            )
                            .await;
                        Ok(format!(
                            "{}:{}",
                            event.source_workflow_id, event.persisted_items
                        ))
                    },
                )
                .build(),
        )
        .await;
        let second_client = Client::new(Arc::clone(&second_store));
        let fanout =
            InventoryCompletionEventBridge::new(Arc::clone(&second_store), Some(second_repository))
                .publish_completion(&InventoryCompletionEvent {
                    inventory_kind: InventoryRefreshKind::MediaFull,
                    source_workflow_id: "inventory:media:full".to_owned(),
                    completed_at_ms: 2_000,
                    scanned_items: 1,
                    persisted_items: 1,
                    pruned_items: 0,
                })
                .await
                .unwrap();
        let status = second_client
            .wait_for_orchestration("waiting-search-file-restart", Duration::from_secs(5))
            .await
            .unwrap();

        assert_eq!(
            InventoryCompletionFanout {
                waiters: 1,
                delivered: 1,
                failed: 0
            },
            fanout
        );
        match status {
            OrchestrationStatus::Completed { output, .. } => {
                assert_eq!("inventory:media:full:1", output);
            }
            other => panic!("expected completed waiting workflow after restart, got {other:?}"),
        }

        second_runtime.shutdown(Some(1_000)).await;
    }

    #[tokio::test]
    async fn inventory_completion_event_waits_until_workflow_dequeues_it() {
        let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap()) as Arc<dyn Provider>;
        let repository = Repository::connect_in_memory().await.unwrap();
        let orchestration_registry = OrchestrationRegistry::builder()
            .register(
                "DelayedInventoryCompletionConsumer",
                |ctx: OrchestrationContext, _: String| async move {
                    let _gate: String = ctx.dequeue_event_typed("gate").await;
                    let event: InventoryCompletionEvent = ctx
                        .dequeue_event_typed(WorkflowEventName::MediaInventoryCompleted.as_str())
                        .await;
                    Ok(format!(
                        "{}:{}",
                        event.source_workflow_id, event.persisted_items
                    ))
                },
            )
            .build();
        let runtime = Runtime::start_with_store(
            Arc::clone(&store),
            ActivityRegistry::builder().build(),
            orchestration_registry,
        )
        .await;
        let client = Client::new(Arc::clone(&store));
        client
            .start_orchestration(
                "waiting-search-delayed",
                "DelayedInventoryCompletionConsumer",
                "",
            )
            .await
            .unwrap();
        wait_for_orchestration_running(&client, "waiting-search-delayed").await;

        let bridge =
            InventoryCompletionEventBridge::new(Arc::clone(&store), Some(repository.clone()));
        bridge
            .register_waiter(InventoryCompletionWaiter {
                workflow_id: "waiting-search-delayed".to_owned(),
                event_name: WorkflowEventName::MediaInventoryCompleted,
                required_after_ms: 2_000,
            })
            .await
            .unwrap();
        let fanout = bridge
            .publish_completion(&InventoryCompletionEvent {
                inventory_kind: InventoryRefreshKind::MediaFull,
                source_workflow_id: "inventory:media:full".to_owned(),
                completed_at_ms: 2_000,
                scanned_items: 1,
                persisted_items: 1,
                pruned_items: 0,
            })
            .await
            .unwrap();
        client
            .enqueue_event_typed("waiting-search-delayed", "gate", &"continue".to_owned())
            .await
            .unwrap();
        let status = client
            .wait_for_orchestration("waiting-search-delayed", Duration::from_secs(5))
            .await
            .unwrap();

        assert_eq!(
            InventoryCompletionFanout {
                waiters: 1,
                delivered: 1,
                failed: 0
            },
            fanout
        );
        match status {
            OrchestrationStatus::Completed { output, .. } => {
                assert_eq!("inventory:media:full:1", output);
            }
            other => panic!("expected completed waiting workflow, got {other:?}"),
        }

        runtime.shutdown(Some(1_000)).await;
    }

    #[tokio::test]
    async fn announce_orchestration_waits_for_media_and_client_inventory_events() {
        let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap()) as Arc<dyn Provider>;
        let process_calls = Arc::new(AtomicUsize::new(0));
        let wait_calls = Arc::new(AtomicUsize::new(0));
        let runtime = Runtime::start_with_options(
            Arc::clone(&store),
            announce_inventory_wait_test_activities(
                Arc::clone(&process_calls),
                Arc::clone(&wait_calls),
                vec![
                    WorkflowEventName::MediaInventoryCompleted,
                    WorkflowEventName::ClientInventoryCompleted,
                ],
            ),
            orchestration_registry(),
            RuntimeOptions {
                dispatcher_min_poll_interval: Duration::from_millis(5),
                dispatcher_long_poll_timeout: Duration::from_millis(10),
                orchestration_concurrency: 2,
                worker_concurrency: 2,
                ..RuntimeOptions::default()
            },
        )
        .await;
        let client = Client::new(Arc::clone(&store));
        client
            .start_orchestration_typed(
                "announce:wait-both",
                WorkflowKind::Announce.orchestration_name(),
                test_announce_workflow_input("ann_wait_both"),
            )
            .await
            .unwrap();

        client
            .enqueue_event_typed(
                "announce:wait-both",
                WorkflowEventName::MediaInventoryCompleted.as_str(),
                &test_inventory_completion_event(InventoryRefreshKind::MediaFull),
            )
            .await
            .unwrap();
        client
            .enqueue_event_typed(
                "announce:wait-both",
                WorkflowEventName::ClientInventoryCompleted.as_str(),
                &test_inventory_completion_event(InventoryRefreshKind::Client),
            )
            .await
            .unwrap();
        wait_for_atomic_at_least(&process_calls, 1).await;
        wait_for_atomic_at_least(&wait_calls, 1).await;
        let status = client
            .wait_for_orchestration("announce:wait-both", Duration::from_secs(5))
            .await
            .unwrap();

        match status {
            OrchestrationStatus::Completed { output, .. } => {
                assert_eq!("Saved", output);
            }
            other => panic!("expected completed announce workflow, got {other:?}"),
        }
        assert!(process_calls.load(Ordering::SeqCst) >= 2);
        assert!(wait_calls.load(Ordering::SeqCst) >= 1);

        runtime.shutdown(Some(1_000)).await;
    }

    #[tokio::test]
    async fn announce_orchestration_waits_for_client_inventory_event_only() {
        let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap()) as Arc<dyn Provider>;
        let process_calls = Arc::new(AtomicUsize::new(0));
        let wait_calls = Arc::new(AtomicUsize::new(0));
        let runtime = Runtime::start_with_options(
            Arc::clone(&store),
            announce_inventory_wait_test_activities(
                Arc::clone(&process_calls),
                Arc::clone(&wait_calls),
                vec![WorkflowEventName::ClientInventoryCompleted],
            ),
            orchestration_registry(),
            RuntimeOptions {
                dispatcher_min_poll_interval: Duration::from_millis(5),
                dispatcher_long_poll_timeout: Duration::from_millis(10),
                orchestration_concurrency: 2,
                worker_concurrency: 2,
                ..RuntimeOptions::default()
            },
        )
        .await;
        let client = Client::new(Arc::clone(&store));
        client
            .start_orchestration_typed(
                "announce:wait-client",
                WorkflowKind::Announce.orchestration_name(),
                test_announce_workflow_input("ann_wait_client"),
            )
            .await
            .unwrap();

        client
            .enqueue_event_typed(
                "announce:wait-client",
                WorkflowEventName::ClientInventoryCompleted.as_str(),
                &test_inventory_completion_event(InventoryRefreshKind::Client),
            )
            .await
            .unwrap();
        wait_for_atomic_at_least(&process_calls, 1).await;
        wait_for_atomic_at_least(&wait_calls, 1).await;
        let status = client
            .wait_for_orchestration("announce:wait-client", Duration::from_secs(5))
            .await
            .unwrap();

        match status {
            OrchestrationStatus::Completed { output, .. } => {
                assert_eq!("Saved", output);
            }
            other => panic!("expected completed announce workflow, got {other:?}"),
        }
        assert!(process_calls.load(Ordering::SeqCst) >= 2);
        assert!(wait_calls.load(Ordering::SeqCst) >= 1);

        runtime.shutdown(Some(1_000)).await;
    }

    #[tokio::test]
    async fn announce_orchestration_rechecks_when_inventory_completion_event_is_missed() {
        let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap()) as Arc<dyn Provider>;
        let process_calls = Arc::new(AtomicUsize::new(0));
        let wait_calls = Arc::new(AtomicUsize::new(0));
        let runtime = Runtime::start_with_options(
            Arc::clone(&store),
            announce_inventory_wait_test_activities(
                Arc::clone(&process_calls),
                Arc::clone(&wait_calls),
                vec![WorkflowEventName::MediaInventoryCompleted],
            ),
            orchestration_registry(),
            RuntimeOptions {
                dispatcher_min_poll_interval: Duration::from_millis(5),
                dispatcher_long_poll_timeout: Duration::from_millis(10),
                orchestration_concurrency: 2,
                worker_concurrency: 2,
                ..RuntimeOptions::default()
            },
        )
        .await;
        let client = Client::new(Arc::clone(&store));
        client
            .start_orchestration_typed(
                "announce:missed-inventory-event",
                WorkflowKind::Announce.orchestration_name(),
                test_announce_workflow_input("ann_missed_inventory_event"),
            )
            .await
            .unwrap();
        let status = client
            .wait_for_orchestration("announce:missed-inventory-event", Duration::from_secs(5))
            .await
            .unwrap();

        match status {
            OrchestrationStatus::Completed { output, .. } => {
                assert_eq!("Saved", output);
            }
            other => panic!("expected completed announce workflow, got {other:?}"),
        }
        assert_eq!(2, process_calls.load(Ordering::SeqCst));
        assert_eq!(1, wait_calls.load(Ordering::SeqCst));

        runtime.shutdown(Some(1_000)).await;
    }

    #[tokio::test]
    async fn announce_orchestration_preserves_partial_inventory_wait_after_recheck() {
        let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap()) as Arc<dyn Provider>;
        let process_calls = Arc::new(AtomicUsize::new(0));
        let wait_calls = Arc::new(AtomicUsize::new(0));
        let runtime = Runtime::start_with_options(
            Arc::clone(&store),
            announce_partial_inventory_wait_test_activities(
                Arc::clone(&process_calls),
                Arc::clone(&wait_calls),
            ),
            orchestration_registry(),
            RuntimeOptions {
                dispatcher_min_poll_interval: Duration::from_millis(5),
                dispatcher_long_poll_timeout: Duration::from_millis(10),
                orchestration_concurrency: 2,
                worker_concurrency: 2,
                ..RuntimeOptions::default()
            },
        )
        .await;
        let client = Client::new(Arc::clone(&store));
        client
            .start_orchestration_typed(
                "announce:partial-inventory-wait",
                WorkflowKind::Announce.orchestration_name(),
                test_announce_workflow_input("ann_partial_inventory_wait"),
            )
            .await
            .unwrap();
        wait_for_atomic_at_least(&wait_calls, 1).await;
        client
            .enqueue_event_typed(
                "announce:partial-inventory-wait",
                WorkflowEventName::MediaInventoryCompleted.as_str(),
                &test_inventory_completion_event(InventoryRefreshKind::MediaFull),
            )
            .await
            .unwrap();
        wait_for_atomic_at_least(&process_calls, 2).await;
        client
            .enqueue_event_typed(
                "announce:partial-inventory-wait",
                WorkflowEventName::ClientInventoryCompleted.as_str(),
                &test_inventory_completion_event(InventoryRefreshKind::Client),
            )
            .await
            .unwrap();
        let status = client
            .wait_for_orchestration("announce:partial-inventory-wait", Duration::from_secs(5))
            .await
            .unwrap();

        match status {
            OrchestrationStatus::Completed { output, .. } => {
                assert_eq!("Saved", output);
            }
            other => panic!("expected completed announce workflow, got {other:?}"),
        }
        assert!(process_calls.load(Ordering::SeqCst) >= 3);
        assert!(wait_calls.load(Ordering::SeqCst) >= 2);

        runtime.shutdown(Some(1_000)).await;
    }

    #[tokio::test]
    async fn announce_orchestration_resumes_after_candidate_cache_dependency_wait() {
        let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap()) as Arc<dyn Provider>;
        let process_calls = Arc::new(AtomicUsize::new(0));
        let runtime = Runtime::start_with_options(
            Arc::clone(&store),
            announce_dependency_retry_test_activities(Arc::clone(&process_calls), 5),
            orchestration_registry(),
            RuntimeOptions {
                dispatcher_min_poll_interval: Duration::from_millis(5),
                dispatcher_long_poll_timeout: Duration::from_millis(10),
                orchestration_concurrency: 2,
                worker_concurrency: 2,
                ..RuntimeOptions::default()
            },
        )
        .await;
        let client = Client::new(Arc::clone(&store));
        client
            .start_orchestration_typed(
                "announce:wait-cache",
                WorkflowKind::Announce.orchestration_name(),
                test_announce_workflow_input("ann_wait_cache"),
            )
            .await
            .unwrap();
        let status = client
            .wait_for_orchestration("announce:wait-cache", Duration::from_secs(5))
            .await
            .unwrap();

        match status {
            OrchestrationStatus::Completed { output, .. } => {
                assert_eq!("Saved", output);
            }
            other => panic!("expected completed announce workflow, got {other:?}"),
        }
        assert_eq!(2, process_calls.load(Ordering::SeqCst));

        runtime.shutdown(Some(1_000)).await;
    }

    #[tokio::test]
    async fn announce_orchestration_resumes_dependency_wait_after_file_backed_restart() {
        let temp_dir = TestTempDir::new("duroxide-announce-dependency-restart");
        let database_path = temp_dir.path().join(WORKFLOW_DATABASE_FILE);
        prepare_workflow_database(&database_path).await.unwrap();
        let database_url = format!("sqlite:{}", database_path.display());
        let process_calls = Arc::new(AtomicUsize::new(0));
        let first_store =
            Arc::new(SqliteProvider::new(&database_url, None).await.unwrap()) as Arc<dyn Provider>;
        let first_runtime = Runtime::start_with_options(
            Arc::clone(&first_store),
            announce_dependency_retry_test_activities(Arc::clone(&process_calls), 1_000),
            orchestration_registry(),
            RuntimeOptions {
                dispatcher_min_poll_interval: Duration::from_millis(5),
                dispatcher_long_poll_timeout: Duration::from_millis(10),
                orchestration_concurrency: 2,
                worker_concurrency: 2,
                ..RuntimeOptions::default()
            },
        )
        .await;
        let first_client = Client::new(Arc::clone(&first_store));
        first_client
            .start_orchestration_typed(
                "announce:wait-cache-restart",
                WorkflowKind::Announce.orchestration_name(),
                test_announce_workflow_input("ann_wait_cache_restart"),
            )
            .await
            .unwrap();
        wait_for_atomic_at_least(&process_calls, 1).await;
        first_runtime.shutdown(Some(1)).await;
        assert_eq!(1, process_calls.load(Ordering::SeqCst));

        let second_store =
            Arc::new(SqliteProvider::new(&database_url, None).await.unwrap()) as Arc<dyn Provider>;
        let second_runtime = Runtime::start_with_options(
            Arc::clone(&second_store),
            announce_dependency_retry_test_activities(Arc::clone(&process_calls), 1_000),
            orchestration_registry(),
            RuntimeOptions {
                dispatcher_min_poll_interval: Duration::from_millis(5),
                dispatcher_long_poll_timeout: Duration::from_millis(10),
                orchestration_concurrency: 2,
                worker_concurrency: 2,
                ..RuntimeOptions::default()
            },
        )
        .await;
        let second_client = Client::new(Arc::clone(&second_store));
        let status = second_client
            .wait_for_orchestration("announce:wait-cache-restart", Duration::from_secs(5))
            .await
            .unwrap();

        match status {
            OrchestrationStatus::Completed { output, .. } => {
                assert_eq!("Saved", output);
            }
            other => panic!("expected completed announce workflow after restart, got {other:?}"),
        }
        assert_eq!(2, process_calls.load(Ordering::SeqCst));

        second_runtime.shutdown(Some(1_000)).await;
    }

    #[tokio::test]
    async fn workflow_runtime_rejects_failed_announce_instance_on_resubmission() {
        let temp_dir = TestTempDir::new("duroxide-announce-failed-resubmit");
        let database_path = temp_dir.path().join("state").join(WORKFLOW_DATABASE_FILE);
        let runtime = DuroxideWorkflowRuntime::start(database_path)
            .await
            .expect("workflow runtime should start");
        let work = test_announce_work("ann_failed_instance", "guid-failed-instance", 1_000);

        runtime
            .client()
            .start_orchestration_typed(
                "announce:ann_failed_instance",
                WorkflowKind::Announce.orchestration_name(),
                "not an announce workflow input".to_owned(),
            )
            .await
            .expect("failed announce workflow should be queued");
        wait_for_orchestration_failure(&runtime.client(), "announce:ann_failed_instance").await;

        let error = runtime
            .submit_announcement(&work)
            .await
            .expect_err("failed announce instance must not be treated as recoverable");
        assert!(matches!(
            error,
            DuroxideWorkflowRuntimeError::FailedAnnounceWorkflow { .. }
        ));

        runtime.shutdown(Some(1_000)).await;
    }

    #[tokio::test]
    async fn submit_announcement_records_initial_running_projection() {
        let temp_dir = TestTempDir::new("duroxide-announce-start-projection");
        let repository = Repository::connect_in_memory().await.unwrap();
        let mut work = test_announce_work("ann_start_projection", "guid-start-projection", 1_000);
        work.fetch = Some(test_announce_fetch_material());
        repository
            .insert_or_dedupe_announce_work(&work, 10)
            .await
            .unwrap();
        let runtime = DuroxideWorkflowRuntime::start_inner(
            temp_dir.path().join("workflows.db"),
            Some(WorkflowRuntimeActivities {
                repository: repository.clone(),
                inventory: None,
                announce: None,
                scheduled_jobs: None,
                search: None,
                saved_retry: None,
            }),
        )
        .await
        .unwrap();

        let submission = runtime.submit_announcement(&work).await.unwrap();

        assert_eq!(
            AnnounceWorkflowSubmissionOutcome::Started,
            submission.outcome
        );
        let snapshot = repository
            .workflow_projection_snapshot(10, 1_500)
            .await
            .unwrap();
        let projection = snapshot
            .recent
            .iter()
            .find(|item| item.workflow_id == "announce:ann_start_projection")
            .unwrap();
        assert_eq!("announce", projection.workflow_kind);
        assert_eq!("ann_start_projection", projection.public_id);
        assert_eq!("running", projection.state);
        assert_eq!("running_activity", projection.reason);
        assert_eq!(Some("starting".to_owned()), projection.next_action);
        assert_eq!(1, projection.raw_secret_material_count);
        assert!(!projection.terminal);
        assert_eq!(1_000, projection.started_at_ms);
        assert_eq!(1, snapshot.active_count);
        assert_eq!(1, snapshot.raw_secret_material_count);

        runtime.shutdown(Some(1_000)).await;
    }

    #[tokio::test]
    async fn announce_process_activity_recovers_terminal_row_after_projection_retry() {
        let temp_dir = TestTempDir::new("duroxide-announce-terminal-retry");
        let repository = Repository::connect_in_memory().await.unwrap();
        let work = test_announce_work("ann_terminal_retry", "guid-terminal-retry", 1_000);
        repository
            .insert_or_dedupe_announce_work(&work, 10)
            .await
            .unwrap();
        assert!(
            repository
                .claim_announce_work_by_id(&work.id, ANNOUNCE_WORKFLOW_OWNER, 1_100, 2_000)
                .await
                .unwrap()
        );
        assert!(
            repository
                .mark_announce_succeeded(
                    &work.id,
                    ANNOUNCE_WORKFLOW_OWNER,
                    AnnounceReason::Saved,
                    "saved",
                    1_200,
                )
                .await
                .unwrap()
        );
        let mut config = SporosConfig::default();
        config.paths.database = temp_dir.path().join("sporos.db");
        let runtime = AppRuntime::from_repository(config.clone(), repository.clone())
            .await
            .unwrap();
        let activities = AnnounceWorkflowActivities::new(
            repository.clone(),
            AnnounceProcessor::new(
                runtime.state.config.clone(),
                repository.clone(),
                runtime.state.health.clone(),
                runtime.state.metrics.clone(),
                runtime.state.scheduler.clone(),
                runtime.state.injection_worker.clone(),
            ),
            config.announce.clone(),
            runtime.state.shutdown_signal.clone(),
        );

        let output = Box::pin(run_announce_process_activity(
            activities,
            "announce:ann_terminal_retry".to_owned(),
            AnnounceActivityInput {
                work_id: work.id.as_str().to_owned(),
                received_at_ms: work.received_at_ms,
                raw_secret_material_count: 0,
            },
        ))
        .await
        .unwrap();

        assert_eq!(AnnounceActivityState::Succeeded, output.state);
        assert_eq!("Saved", output.reason);
        let loaded = repository
            .announce_work_item(&work.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(1, loaded.attempt_count);
        let snapshot = repository
            .workflow_projection_snapshot(10, unix_time_ms())
            .await
            .unwrap();
        let projection = snapshot
            .recent
            .iter()
            .find(|item| item.workflow_id == "announce:ann_terminal_retry")
            .unwrap();
        assert_eq!("succeeded", projection.state);
        assert_eq!(Some("Saved".to_owned()), projection.next_action);

        runtime.state.workflow_runtime.shutdown(Some(1_000)).await;
    }

    #[tokio::test]
    async fn announce_process_activity_recovers_terminal_failure_projection_without_fetch_material()
    {
        let temp_dir = TestTempDir::new("duroxide-announce-terminal-failure-retry");
        let repository = Repository::connect_in_memory().await.unwrap();
        let mut work = test_announce_work(
            "ann_terminal_failure_retry",
            "guid-terminal-failure-retry",
            1_000,
        );
        work.fetch = Some(test_announce_fetch_material());
        repository
            .insert_or_dedupe_announce_work(&work, 10)
            .await
            .unwrap();
        assert!(
            repository
                .mark_announce_rejected(
                    &work.id,
                    AnnounceReason::InvalidTorrentMetadata,
                    "invalid torrent metadata",
                    1_200,
                )
                .await
                .unwrap()
        );
        let mut config = SporosConfig::default();
        config.paths.database = temp_dir.path().join("sporos.db");
        let runtime = AppRuntime::from_repository(config.clone(), repository.clone())
            .await
            .unwrap();
        let activities = AnnounceWorkflowActivities::new(
            repository.clone(),
            AnnounceProcessor::new(
                runtime.state.config.clone(),
                repository.clone(),
                runtime.state.health.clone(),
                runtime.state.metrics.clone(),
                runtime.state.scheduler.clone(),
                runtime.state.injection_worker.clone(),
            ),
            config.announce.clone(),
            runtime.state.shutdown_signal.clone(),
        );

        let output = Box::pin(run_announce_process_activity(
            activities,
            "announce:ann_terminal_failure_retry".to_owned(),
            AnnounceActivityInput {
                work_id: work.id.as_str().to_owned(),
                received_at_ms: work.received_at_ms,
                raw_secret_material_count: 1,
            },
        ))
        .await
        .unwrap();

        assert_eq!(AnnounceActivityState::Failed, output.state);
        assert_eq!("InvalidTorrentMetadata", output.reason);
        let snapshot = repository
            .workflow_projection_snapshot(10, unix_time_ms())
            .await
            .unwrap();
        let projection = snapshot
            .recent
            .iter()
            .find(|item| item.workflow_id == "announce:ann_terminal_failure_retry")
            .unwrap();
        assert_eq!("failed", projection.state);
        assert_eq!("failed", projection.reason);
        assert_eq!(
            Some("InvalidTorrentMetadata".to_owned()),
            projection.next_action
        );
        assert_eq!(0, projection.raw_secret_material_count);
        assert!(projection.terminal);
        assert!(projection.finished_at_ms.is_some());
        assert_eq!(0, snapshot.raw_secret_material_count);

        runtime.state.workflow_runtime.shutdown(Some(1_000)).await;
    }

    #[tokio::test]
    async fn announce_waiting_dependency_projection_records_blocker() {
        let repository = Repository::connect_in_memory().await.unwrap();
        record_announce_activity_projection(
            &repository,
            "announce:ann_waiting_dependency",
            &AnnounceActivityInput {
                work_id: "ann_waiting_dependency".to_owned(),
                received_at_ms: 1_000,
                raw_secret_material_count: 1,
            },
            &AnnounceProcessActivityOutput {
                state: AnnounceActivityState::WaitingDependency,
                reason: "RetryAfter".to_owned(),
                next_attempt_at_ms: Some(2_000),
                retry_delay_ms: Some(1_000),
                dependency: Some(AnnounceProjectionDependency {
                    kind: DependencyKind::Indexer.as_str().to_owned(),
                    name: "https://indexer.example/api?apikey=dependency-secret".to_owned(),
                }),
                events: Vec::new(),
            },
            1_100,
        )
        .await
        .unwrap();

        let snapshot = repository
            .workflow_projection_snapshot(10, 1_500)
            .await
            .unwrap();
        let projection = snapshot
            .recent
            .iter()
            .find(|item| item.workflow_id == "announce:ann_waiting_dependency")
            .unwrap();
        assert_eq!("waiting", projection.state);
        assert_eq!("waiting_for_dependency", projection.reason);
        assert_eq!(
            Some("indexer".to_owned()),
            projection.blocked_dependency_kind
        );
        assert_eq!(
            Some("https://indexer.example/api?apikey=[REDACTED]".to_owned()),
            projection.blocked_dependency_name
        );
        assert_eq!(1, snapshot.raw_secret_material_count);
    }

    #[tokio::test]
    async fn announce_process_activity_completes_from_action_checkpoint_without_reprocessing() {
        let temp_dir = TestTempDir::new("duroxide-announce-action-checkpoint");
        let repository = Repository::connect_in_memory().await.unwrap();
        let work = test_announce_work("ann_action_checkpoint", "guid-action-checkpoint", 1_000);
        repository
            .insert_or_dedupe_announce_work(&work, 10)
            .await
            .unwrap();
        assert!(
            repository
                .claim_announce_work_by_id(&work.id, ANNOUNCE_WORKFLOW_OWNER, 1_100, 2_000)
                .await
                .unwrap()
        );
        assert!(
            repository
                .record_announce_action_checkpoint(
                    &work.id,
                    ANNOUNCE_WORKFLOW_OWNER,
                    AnnounceReason::Saved,
                    "saved",
                    1_200,
                )
                .await
                .unwrap()
        );
        let mut config = SporosConfig::default();
        config.paths.database = temp_dir.path().join("sporos.db");
        let runtime = AppRuntime::from_repository(config.clone(), repository.clone())
            .await
            .unwrap();
        let activities = AnnounceWorkflowActivities::new(
            repository.clone(),
            AnnounceProcessor::new(
                runtime.state.config.clone(),
                repository.clone(),
                runtime.state.health.clone(),
                runtime.state.metrics.clone(),
                runtime.state.scheduler.clone(),
                runtime.state.injection_worker.clone(),
            ),
            config.announce.clone(),
            runtime.state.shutdown_signal.clone(),
        );

        let output = Box::pin(run_announce_process_activity(
            activities,
            "announce:ann_action_checkpoint".to_owned(),
            AnnounceActivityInput {
                work_id: work.id.as_str().to_owned(),
                received_at_ms: work.received_at_ms,
                raw_secret_material_count: 0,
            },
        ))
        .await
        .unwrap();

        assert_eq!(AnnounceActivityState::Succeeded, output.state);
        assert_eq!("Saved", output.reason);
        let loaded = repository
            .announce_work_item(&work.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(AnnounceStatus::Succeeded, loaded.status);
        assert_eq!(AnnounceReason::Saved, loaded.reason);
        assert_eq!(1, loaded.attempt_count);
        let snapshot = repository
            .workflow_projection_snapshot(10, unix_time_ms())
            .await
            .unwrap();
        let projection = snapshot
            .recent
            .iter()
            .find(|item| item.workflow_id == "announce:ann_action_checkpoint")
            .unwrap();
        assert_eq!("succeeded", projection.state);
        assert_eq!(Some("Saved".to_owned()), projection.next_action);

        runtime.state.workflow_runtime.shutdown(Some(1_000)).await;
    }

    #[tokio::test]
    async fn inventory_completion_for_missing_workflow_is_retried_after_workflow_starts() {
        let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap()) as Arc<dyn Provider>;
        let repository = Repository::connect_in_memory().await.unwrap();
        let orchestration_registry = OrchestrationRegistry::builder()
            .register(
                "LateInventoryCompletionConsumer",
                |ctx: OrchestrationContext, _: String| async move {
                    let event: InventoryCompletionEvent = ctx
                        .dequeue_event_typed(WorkflowEventName::MediaInventoryCompleted.as_str())
                        .await;
                    Ok(format!(
                        "{}:{}",
                        event.source_workflow_id, event.persisted_items
                    ))
                },
            )
            .build();
        let runtime = Runtime::start_with_store(
            Arc::clone(&store),
            ActivityRegistry::builder().build(),
            orchestration_registry,
        )
        .await;
        let client = Client::new(Arc::clone(&store));
        let bridge =
            InventoryCompletionEventBridge::new(Arc::clone(&store), Some(repository.clone()));
        bridge
            .register_waiter(InventoryCompletionWaiter {
                workflow_id: "waiting-search-late".to_owned(),
                event_name: WorkflowEventName::MediaInventoryCompleted,
                required_after_ms: 2_000,
            })
            .await
            .unwrap();

        let error = bridge
            .publish_completion(&InventoryCompletionEvent {
                inventory_kind: InventoryRefreshKind::MediaFull,
                source_workflow_id: "inventory:media:full".to_owned(),
                completed_at_ms: 2_000,
                scanned_items: 1,
                persisted_items: 1,
                pruned_items: 0,
            })
            .await
            .expect_err("missing target workflow should not be treated as delivered");
        assert!(error.contains("inventory completion fanout failed"));
        assert_eq!(
            1,
            repository
                .workflow_inventory_completions(10)
                .await
                .unwrap()
                .len()
        );

        client
            .start_orchestration("waiting-search-late", "LateInventoryCompletionConsumer", "")
            .await
            .unwrap();
        wait_for_orchestration_running(&client, "waiting-search-late").await;
        let fanout = bridge.drain_persisted_completions().await.unwrap();
        let status = client
            .wait_for_orchestration("waiting-search-late", Duration::from_secs(5))
            .await
            .unwrap();

        assert_eq!(
            InventoryCompletionFanout {
                waiters: 1,
                delivered: 1,
                failed: 0
            },
            fanout
        );
        match status {
            OrchestrationStatus::Completed { output, .. } => {
                assert_eq!("inventory:media:full:1", output);
            }
            other => panic!("expected completed waiting workflow, got {other:?}"),
        }
        assert!(
            repository
                .workflow_inventory_completions(10)
                .await
                .unwrap()
                .is_empty()
        );

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
    async fn inventory_completion_bridge_delivers_typed_events_to_waiters() {
        let store = Arc::new(SqliteProvider::new_in_memory().await.unwrap()) as Arc<dyn Provider>;
        let bridge = InventoryCompletionEventBridge::new(Arc::clone(&store), None);
        let orchestration_registry = OrchestrationRegistry::builder()
            .register(
                "WaitInventoryCompletion",
                |ctx: OrchestrationContext, _: String| async move {
                    let event: InventoryCompletionEvent = ctx
                        .dequeue_event_typed(WorkflowEventName::MediaInventoryCompleted.as_str())
                        .await;
                    Ok(format!(
                        "{}:{}:{}",
                        event.source_workflow_id, event.completed_at_ms, event.persisted_items
                    ))
                },
            )
            .build();
        let runtime = Runtime::start_with_store(
            Arc::clone(&store),
            ActivityRegistry::builder().build(),
            orchestration_registry,
        )
        .await;
        let client = Client::new(Arc::clone(&store));
        client
            .start_orchestration("waiting-search", "WaitInventoryCompletion", "")
            .await
            .unwrap();
        wait_for_orchestration_running(&client, "waiting-search").await;
        let registration = bridge
            .register_waiter(InventoryCompletionWaiter {
                workflow_id: "waiting-search".to_owned(),
                event_name: WorkflowEventName::MediaInventoryCompleted,
                required_after_ms: 2_000,
            })
            .await
            .unwrap();

        let stale = bridge
            .publish_completion(&InventoryCompletionEvent {
                inventory_kind: InventoryRefreshKind::MediaFull,
                source_workflow_id: "inventory:media:full".to_owned(),
                completed_at_ms: 1_999,
                scanned_items: 1,
                persisted_items: 1,
                pruned_items: 0,
            })
            .await
            .unwrap();
        let delivered = bridge
            .publish_completion(&InventoryCompletionEvent {
                inventory_kind: InventoryRefreshKind::MediaFull,
                source_workflow_id: "inventory:media:full".to_owned(),
                completed_at_ms: 2_000,
                scanned_items: 2,
                persisted_items: 2,
                pruned_items: 0,
            })
            .await
            .unwrap();
        let status = client
            .wait_for_orchestration("waiting-search", Duration::from_secs(5))
            .await
            .unwrap();

        assert!(registration.inserted);
        assert_eq!(InventoryCompletionFanout::default(), stale);
        assert_eq!(
            InventoryCompletionFanout {
                waiters: 1,
                delivered: 1,
                failed: 0
            },
            delivered
        );
        match status {
            OrchestrationStatus::Completed { output, .. } => {
                assert_eq!("inventory:media:full:2000:2", output);
            }
            other => panic!("expected completed waiting workflow, got {other:?}"),
        }

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

    async fn wait_for_supervisor_failure(client: &Client, instance_id: &str) {
        wait_for_orchestration_failure(client, instance_id).await;
    }

    async fn wait_for_orchestration_failure(client: &Client, instance_id: &str) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match client
                .get_orchestration_status(instance_id)
                .await
                .expect("orchestration status should be readable")
            {
                OrchestrationStatus::Failed { .. } => return,
                OrchestrationStatus::Completed { .. } => {
                    panic!("orchestration completed unexpectedly");
                }
                OrchestrationStatus::NotFound | OrchestrationStatus::Running { .. } => {}
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for orchestration {instance_id} failure"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn wait_for_orchestration_running(client: &Client, instance_id: &str) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match client
                .get_orchestration_status(instance_id)
                .await
                .expect("orchestration status should be readable")
            {
                OrchestrationStatus::Running { .. } => return,
                OrchestrationStatus::Completed { .. } => {
                    panic!("orchestration completed before test could raise event");
                }
                OrchestrationStatus::Failed { details, .. } => {
                    panic!(
                        "orchestration failed before test could raise event: {}",
                        details.display_message()
                    );
                }
                OrchestrationStatus::NotFound => {}
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for orchestration {instance_id} to run"
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

    fn test_announce_work(id: &str, guid: &str, received_at_ms: i64) -> AnnounceWorkItem {
        let tracker = TrackerName::new("tracker.example").unwrap();
        let guid = CandidateGuid::new(guid).unwrap();
        let dedupe_hash = AnnounceDedupeIdentity::Guid {
            tracker: tracker.clone(),
            guid: guid.clone(),
        }
        .hash();

        AnnounceWorkItem {
            id: AnnounceWorkId::new(id).unwrap(),
            status: AnnounceStatus::Queued,
            reason: AnnounceReason::Accepted,
            dedupe_hash,
            title: ItemTitle::new("Example").unwrap(),
            tracker,
            guid: Some(guid),
            info_hash: None,
            size: Some(ByteSize::new(42)),
            fetch: Option::<AnnounceFetchMaterial>::None,
            received_at_ms,
            updated_at_ms: received_at_ms,
            first_attempt_at_ms: None,
            finished_at_ms: None,
            attempt_count: 0,
            next_attempt_at_ms: received_at_ms,
            expires_at_ms: received_at_ms.saturating_add(60_000),
            lease: None,
            last_dependency_kind: None,
            last_dependency_name: None,
            last_error_class: None,
            last_redacted_message: None,
        }
    }

    fn test_announce_workflow_input(work_id: &str) -> AnnounceWorkflowInput {
        AnnounceWorkflowInput {
            work_id: work_id.to_owned(),
            dedupe_hash: format!("dedupe-{work_id}"),
            tracker: "tracker.example".to_owned(),
            candidate_guid: format!("guid-{work_id}"),
            candidate_title: "Example".to_owned(),
            received_at_ms: 1_000,
            expires_at_ms: 61_000,
            fetch_material_present: true,
            raw_secret_material_count: 1,
        }
    }

    fn test_inventory_completion_event(kind: InventoryRefreshKind) -> InventoryCompletionEvent {
        let source_workflow_id = match kind {
            InventoryRefreshKind::MediaFull => "inventory:media:full",
            InventoryRefreshKind::MediaChanged => "inventory:media:changed:test",
            InventoryRefreshKind::Client => "inventory:client",
        };

        InventoryCompletionEvent {
            inventory_kind: kind,
            source_workflow_id: source_workflow_id.to_owned(),
            completed_at_ms: 2_000,
            scanned_items: 1,
            persisted_items: 1,
            pruned_items: 0,
        }
    }

    fn test_announce_fetch_material() -> AnnounceFetchMaterial {
        let download_url =
            DownloadUrl::new("https://tracker.example/download?id=1&passkey=download-secret")
                .unwrap();
        AnnounceFetchMaterial::new(
            &download_url,
            Some(CookieSecret::new("sid=cookie-secret").unwrap()),
        )
        .unwrap()
    }

    async fn wait_for_atomic_at_least(counter: &AtomicUsize, expected: usize) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if counter.load(Ordering::SeqCst) >= expected {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for counter to reach {expected}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn announce_inventory_wait_test_activities(
        process_calls: Arc<AtomicUsize>,
        wait_calls: Arc<AtomicUsize>,
        events: Vec<WorkflowEventName>,
    ) -> ActivityRegistry {
        let process_events = events.clone();
        let wait_events = events;
        ActivityRegistry::builder()
            .register_typed(
                ActivityKind::MatchingReverseLookup.as_str(),
                move |_ctx: ActivityContext,
                      _input: ActivityInputEnvelope<AnnounceActivityInput>| {
                    let process_calls = Arc::clone(&process_calls);
                    let events = process_events.clone();
                    async move {
                        let call_index = process_calls.fetch_add(1, Ordering::SeqCst);
                        if call_index == 0 {
                            Ok(AnnounceProcessActivityOutput {
                                state: AnnounceActivityState::WaitingInventory,
                                reason: "InventoryRefreshing".to_owned(),
                                next_attempt_at_ms: Some(2_000),
                                retry_delay_ms: Some(1),
                                dependency: None,
                                events,
                            })
                        } else {
                            Ok(AnnounceProcessActivityOutput {
                                state: AnnounceActivityState::Succeeded,
                                reason: "Saved".to_owned(),
                                next_attempt_at_ms: None,
                                retry_delay_ms: None,
                                dependency: None,
                                events: Vec::new(),
                            })
                        }
                    }
                },
            )
            .register_typed(
                ActivityKind::RepositoryWrite.as_str(),
                move |_ctx: ActivityContext,
                      _input: ActivityInputEnvelope<AnnounceActivityInput>| {
                    let wait_calls = Arc::clone(&wait_calls);
                    let events = wait_events.clone();
                    async move {
                        wait_calls.fetch_add(1, Ordering::SeqCst);
                        Ok(AnnounceWaitActivityOutput { events })
                    }
                },
            )
            .build()
    }

    fn announce_partial_inventory_wait_test_activities(
        process_calls: Arc<AtomicUsize>,
        wait_calls: Arc<AtomicUsize>,
    ) -> ActivityRegistry {
        ActivityRegistry::builder()
            .register_typed(
                ActivityKind::MatchingReverseLookup.as_str(),
                move |_ctx: ActivityContext,
                      _input: ActivityInputEnvelope<AnnounceActivityInput>| {
                    let process_calls = Arc::clone(&process_calls);
                    async move {
                        match process_calls.fetch_add(1, Ordering::SeqCst) {
                            0 => Ok(AnnounceProcessActivityOutput {
                                state: AnnounceActivityState::WaitingInventory,
                                reason: "InventoryRefreshing".to_owned(),
                                next_attempt_at_ms: Some(2_000),
                                retry_delay_ms: Some(1),
                                dependency: None,
                                events: vec![
                                    WorkflowEventName::MediaInventoryCompleted,
                                    WorkflowEventName::ClientInventoryCompleted,
                                ],
                            }),
                            1 => Ok(AnnounceProcessActivityOutput {
                                state: AnnounceActivityState::WaitingInventory,
                                reason: "InventoryRefreshing".to_owned(),
                                next_attempt_at_ms: Some(3_000),
                                retry_delay_ms: Some(1),
                                dependency: None,
                                events: vec![WorkflowEventName::ClientInventoryCompleted],
                            }),
                            _ => Ok(AnnounceProcessActivityOutput {
                                state: AnnounceActivityState::Succeeded,
                                reason: "Saved".to_owned(),
                                next_attempt_at_ms: None,
                                retry_delay_ms: None,
                                dependency: None,
                                events: Vec::new(),
                            }),
                        }
                    }
                },
            )
            .register_typed(
                ActivityKind::RepositoryWrite.as_str(),
                move |_ctx: ActivityContext,
                      _input: ActivityInputEnvelope<AnnounceActivityInput>| {
                    let wait_calls = Arc::clone(&wait_calls);
                    async move {
                        let events = if wait_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                            vec![
                                WorkflowEventName::MediaInventoryCompleted,
                                WorkflowEventName::ClientInventoryCompleted,
                            ]
                        } else {
                            vec![WorkflowEventName::ClientInventoryCompleted]
                        };
                        Ok(AnnounceWaitActivityOutput { events })
                    }
                },
            )
            .build()
    }

    fn announce_dependency_retry_test_activities(
        process_calls: Arc<AtomicUsize>,
        retry_delay_ms: u64,
    ) -> ActivityRegistry {
        ActivityRegistry::builder()
            .register_typed(
                ActivityKind::MatchingReverseLookup.as_str(),
                move |_ctx: ActivityContext,
                      _input: ActivityInputEnvelope<AnnounceActivityInput>| {
                    let process_calls = Arc::clone(&process_calls);
                    async move {
                        let call_index = process_calls.fetch_add(1, Ordering::SeqCst);
                        if call_index == 0 {
                            Ok(AnnounceProcessActivityOutput {
                                state: AnnounceActivityState::WaitingDependency,
                                reason: "CandidateCacheUnavailable".to_owned(),
                                next_attempt_at_ms: Some(2_000),
                                retry_delay_ms: Some(retry_delay_ms),
                                dependency: Some(AnnounceProjectionDependency {
                                    kind: DependencyKind::LocalState.as_str().to_owned(),
                                    name: "candidate_cache".to_owned(),
                                }),
                                events: Vec::new(),
                            })
                        } else {
                            Ok(AnnounceProcessActivityOutput {
                                state: AnnounceActivityState::Succeeded,
                                reason: "Saved".to_owned(),
                                next_attempt_at_ms: None,
                                retry_delay_ms: None,
                                dependency: None,
                                events: Vec::new(),
                            })
                        }
                    }
                },
            )
            .build()
    }

    #[tokio::test]
    async fn saved_retry_supervisor_runs_startup_and_interval_with_bounded_children() {
        let temp_dir = TestTempDir::new("duroxide-saved-retry-supervisor");
        let database_path = temp_dir.path().join("state").join(WORKFLOW_DATABASE_FILE);
        prepare_workflow_database(&database_path).await.unwrap();
        let database_url = format!("sqlite:{}", database_path.display());
        let store =
            Arc::new(SqliteProvider::new(&database_url, None).await.unwrap()) as Arc<dyn Provider>;
        let test_state = Arc::new(SavedRetrySupervisorTestState::new(temp_dir.path()));
        let runtime = Runtime::start_with_options(
            Arc::clone(&store),
            saved_retry_test_activity_registry(Arc::clone(&test_state)),
            orchestration_registry(),
            RuntimeOptions {
                dispatcher_min_poll_interval: Duration::from_millis(5),
                dispatcher_long_poll_timeout: Duration::from_millis(10),
                orchestration_concurrency: 4,
                worker_concurrency: 4,
                ..RuntimeOptions::default()
            },
        )
        .await;
        let client = Client::new(Arc::clone(&store));
        client
            .start_orchestration_typed(
                WorkflowInstanceId::saved_retry_supervisor().as_str(),
                WorkflowKind::SavedTorrentRetry.orchestration_name(),
                WorkflowSupervisorInput {
                    kind: WorkflowKind::SavedTorrentRetry,
                    public_id: "saved-retry".to_owned(),
                },
            )
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if test_state.finalize_count.load(Ordering::SeqCst) >= 2 {
                    return;
                }
                test_state.finalized.notified().await;
            }
        })
        .await
        .unwrap();

        runtime.shutdown(Some(1_000)).await;
        assert!(test_state.scan_count.load(Ordering::SeqCst) >= 2);
        assert!(test_state.process_count.load(Ordering::SeqCst) >= 8);
        assert!(
            test_state.max_active_processes.load(Ordering::SeqCst)
                <= SAVED_RETRY_ITEM_CHILD_CONCURRENCY
        );
    }

    #[derive(Debug)]
    struct SavedRetrySupervisorTestState {
        items: Vec<SavedTorrentRetryItem>,
        scan_count: AtomicUsize,
        process_count: AtomicUsize,
        active_processes: AtomicUsize,
        max_active_processes: AtomicUsize,
        finalize_count: AtomicUsize,
        finalized: tokio::sync::Notify,
    }

    impl SavedRetrySupervisorTestState {
        fn new(root: &Path) -> Self {
            let items = (0..4)
                .map(|index| SavedTorrentRetryItem {
                    directory: root.to_path_buf(),
                    path: root.join(format!("saved-{index}.torrent")),
                    item_key: format!("item.{index}"),
                })
                .collect();
            Self {
                items,
                scan_count: AtomicUsize::new(0),
                process_count: AtomicUsize::new(0),
                active_processes: AtomicUsize::new(0),
                max_active_processes: AtomicUsize::new(0),
                finalize_count: AtomicUsize::new(0),
                finalized: tokio::sync::Notify::new(),
            }
        }

        fn track_process_start(&self) {
            let active = self.active_processes.fetch_add(1, Ordering::SeqCst) + 1;
            let mut current = self.max_active_processes.load(Ordering::SeqCst);
            while active > current {
                match self.max_active_processes.compare_exchange(
                    current,
                    active,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => break,
                    Err(next) => current = next,
                }
            }
        }
    }

    fn saved_retry_test_activity_registry(
        state: Arc<SavedRetrySupervisorTestState>,
    ) -> ActivityRegistry {
        let scan_state = Arc::clone(&state);
        let process_state = Arc::clone(&state);
        let finalize_state = state;
        ActivityRegistry::builder()
            .register_typed(
                ActivityKind::SavedRetryScan.as_str(),
                move |_ctx: ActivityContext,
                      _input: ActivityInputEnvelope<SavedRetryScanActivityInput>| {
                    let state = Arc::clone(&scan_state);
                    async move {
                        state.scan_count.fetch_add(1, Ordering::SeqCst);
                        Ok(SavedRetryScanActivityOutput {
                            items: state.items.clone(),
                            interval_ms: 25,
                            failed: 0,
                        })
                    }
                },
            )
            .register_typed(
                ActivityKind::SavedRetryProcess.as_str(),
                move |_ctx: ActivityContext,
                      _input: ActivityInputEnvelope<SavedRetryProcessActivityInput>| {
                    let state = Arc::clone(&process_state);
                    async move {
                        state.track_process_start();
                        state.process_count.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        state.active_processes.fetch_sub(1, Ordering::SeqCst);
                        Ok(SavedTorrentRetrySummary {
                            scanned: 1,
                            attempted: 1,
                            kept: 1,
                            ..SavedTorrentRetrySummary::default()
                        })
                    }
                },
            )
            .register_typed(
                ActivityKind::SavedRetryFinalize.as_str(),
                move |_ctx: ActivityContext,
                      input: ActivityInputEnvelope<SavedRetryFinalizeActivityInput>| {
                    let state = Arc::clone(&finalize_state);
                    async move {
                        state.finalize_count.fetch_add(1, Ordering::SeqCst);
                        state.finalized.notify_waiters();
                        Ok(SavedRetryFinalizeActivityOutput {
                            summary: input.payload.summary,
                        })
                    }
                },
            )
            .build()
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
