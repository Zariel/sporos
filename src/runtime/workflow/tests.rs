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
