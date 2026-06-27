use super::*;

pub(super) async fn run_saved_retry_scan_activity(
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

pub(super) async fn run_saved_retry_process_activity(
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

pub(super) async fn run_saved_retry_finalize_activity(
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
pub(super) struct SavedRetryProjectionRecord<'a> {
    pub(super) workflow_id: &'a str,
    pub(super) state: WorkflowState,
    pub(super) reason: WorkflowReason,
    pub(super) next_action: Option<&'a str>,
    pub(super) started_at_ms: i64,
    pub(super) updated_at_ms: i64,
    pub(super) finished_at_ms: Option<i64>,
}

pub(super) async fn record_saved_retry_projection(
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

pub(super) fn duration_millis_u64(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

pub(super) async fn run_search_plan_activity(
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

pub(super) async fn run_search_candidate_page_activity(
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

pub(super) async fn run_search_candidate_activity(
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

pub(super) async fn run_search_finalize_activity(
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

pub(super) async fn run_inventory_activity(
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

pub(super) async fn run_scheduled_job_claim_activity(
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

pub(super) async fn run_scheduled_job_complete_activity(
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

pub(super) async fn retry_scheduler_call<T, F, Fut>(
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

pub(super) fn classify_scheduler_error(error: &SchedulerError) -> RetryDecision {
    match error {
        SchedulerError::Database { source } => classify_database_error(source),
        SchedulerError::InvalidConfig { .. } | SchedulerError::UnknownJob { .. } => {
            RetryDecision::do_not_retry(RetryErrorKind::FatalLocal)
        }
    }
}

pub(super) async fn run_scheduled_job_activity(
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

pub(super) async fn record_inventory_activity_projection(
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

pub(super) async fn run_announce_process_activity(
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

pub(super) async fn complete_checkpointed_announce_activity(
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

pub(super) fn announce_checkpoint_outcome(outcome: Option<&str>) -> Option<AnnounceWorkOutcome> {
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

pub(super) async fn record_announce_action_checkpoint(
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

pub(super) async fn record_announce_start_projection(
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

pub(super) async fn completed_announce_activity_output(
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

pub(super) async fn run_announce_queue_inventory_activity(
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

pub(super) async fn register_announce_inventory_waiters(
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

pub(super) fn announce_process_activity_output(
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

pub(super) fn retry_delay_ms(now_ms: i64, next_attempt_at_ms: i64) -> u64 {
    u64::try_from(next_attempt_at_ms.saturating_sub(now_ms).max(1)).unwrap_or(u64::MAX)
}

pub(super) fn announce_reason_label(reason: AnnounceReason) -> String {
    format!("{reason:?}")
}

pub(super) async fn record_announce_activity_projection(
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

pub(super) fn inventory_activity_output(
    summaries: &[InventoryRefreshSummary],
) -> InventoryActivityOutput {
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
