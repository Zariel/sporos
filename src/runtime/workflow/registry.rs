use super::*;

pub(super) fn activity_registry() -> ActivityRegistry {
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

pub(super) fn activity_registry_with_runtime_activities(
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
