use super::*;

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

    pub(super) async fn start_inner(
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
