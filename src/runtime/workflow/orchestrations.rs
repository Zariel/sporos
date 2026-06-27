use super::*;

pub(super) fn orchestration_registry() -> OrchestrationRegistry {
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

pub(super) async fn scheduled_job_supervisor_orchestration(
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

pub(super) async fn claim_scheduled_job(
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

pub(super) async fn run_scheduled_job_child(
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

pub(super) async fn scheduled_job_run_orchestration(
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

pub(super) async fn saved_retry_supervisor_orchestration(
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
pub(super) struct SavedRetryRunOutput {
    pub(super) summary: SavedTorrentRetrySummary,
    pub(super) interval_ms: u64,
}

pub(super) async fn run_saved_retry_batch(
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

pub(super) async fn saved_retry_item_orchestration(
    ctx: OrchestrationContext,
    input: SavedRetryProcessActivityInput,
) -> Result<SavedTorrentRetrySummary, String> {
    let process_input =
        ActivityInputEnvelope::new(ctx.instance_id(), SAVED_RETRY_PROCESS_ACTIVITY_ID, input);
    ctx.schedule_activity_typed(ActivityKind::SavedRetryProcess.as_str(), &process_input)
        .await
}

pub(super) fn saved_retry_item_child(
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

pub(super) async fn orchestration_now_ms(ctx: &OrchestrationContext) -> Result<i64, String> {
    system_time_to_unix_ms(ctx.utc_now().await?)
}

pub(super) fn system_time_to_unix_ms(time: SystemTime) -> Result<i64, String> {
    let duration = time
        .duration_since(UNIX_EPOCH)
        .map_err(|error| error.to_string())?;
    i64::try_from(duration.as_millis()).map_err(|error| error.to_string())
}

pub(super) async fn search_orchestration(
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

pub(super) fn merge_search_summary(
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

pub(super) async fn announce_orchestration(
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

pub(super) async fn wait_for_announce_inventory_or_recheck(
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

pub(super) async fn wait_for_announce_inventory_event_or_recheck(
    ctx: &OrchestrationContext,
    event_name: WorkflowEventName,
) -> bool {
    let timer = ctx.schedule_timer(ANNOUNCE_INVENTORY_WAIT_RECHECK_INTERVAL);
    let inventory = ctx.dequeue_event_typed::<InventoryCompletionEvent>(event_name.as_str());
    ctx.select2(timer, inventory).await.is_first()
}

pub(super) async fn queue_announce_inventory_refresh(
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

pub(super) async fn inventory_refresh_orchestration(
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

pub(super) fn set_inventory_custom_status(
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

pub(super) fn set_announce_custom_status(
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

pub(super) fn set_search_custom_status(
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

pub(super) fn set_saved_retry_custom_status(
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

pub(super) fn inventory_public_id(kind: InventoryRefreshKind, scope_hash: Option<&str>) -> String {
    match kind {
        InventoryRefreshKind::MediaFull => "media:full".to_owned(),
        InventoryRefreshKind::MediaChanged => {
            let scope_hash = scope_hash.unwrap_or("unknown");
            format!("media:changed:{scope_hash}")
        }
        InventoryRefreshKind::Client => "client".to_owned(),
    }
}

pub(super) fn changed_paths_scope_hash(paths: &[PathBuf]) -> String {
    let mut normalized = paths
        .iter()
        .map(|path| path.display().to_string())
        .collect::<Vec<_>>();
    normalized.sort();
    stable_hash_hex(&normalized.join("\n"))
}

pub(super) fn stable_hash_hex(value: &str) -> String {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}
