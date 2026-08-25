use std::sync::Arc;
use std::time::Duration;

use duroxide::runtime::registry::ActivityRegistry;
use duroxide::{
    ActivityContext, BackoffStrategy, OrchestrationContext, OrchestrationRegistry, RetryPolicy,
};
use serde::{Deserialize, Serialize};
use sporos_model::TaskId;

use crate::storage::Storage;
use crate::task_projection::ProjectionUpdate;
use crate::{candidate_workflow, completion, completion::CompletionInput};

pub const FAKE_TASK_NAME: &str = "Phase1FakeTask";
pub const FAKE_TASK_VERSION: &str = "1.0.0";
const PROJECT_TASK_ACTIVITY: &str = "ProjectTask";

pub(crate) fn activity_retry_policy() -> RetryPolicy {
    RetryPolicy::new(5)
        .with_backoff(BackoffStrategy::Exponential {
            base: Duration::from_secs(1),
            multiplier: 2.0,
            max: Duration::from_secs(5 * 60),
        })
        .with_jitter(20)
        .with_error_filter(crate::activity_failure::retryable)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FakeTaskInput {
    pub task_id: [u8; 16],
    pub accepted_at_ms: i64,
    pub delay_ms: u64,
}

#[cfg(test)]
pub(crate) fn registries(
    storage: Arc<Storage>,
    qbit: Option<crate::qbittorrent::QbittorrentClient>,
    search: Option<crate::search::SearchExecutor>,
    data_scan: Option<crate::data_scan::DataScanExecutor>,
) -> (ActivityRegistry, OrchestrationRegistry) {
    registries_with_limits(storage, qbit, search, data_scan, None)
}

pub(crate) fn registries_with_limits(
    storage: Arc<Storage>,
    qbit: Option<crate::qbittorrent::QbittorrentClient>,
    search: Option<crate::search::SearchExecutor>,
    data_scan: Option<crate::data_scan::DataScanExecutor>,
    execution: Option<crate::execution::ExecutionLimits>,
) -> (ActivityRegistry, OrchestrationRegistry) {
    let completion_storage = Arc::clone(&storage);
    let candidate_storage = Arc::clone(&storage);
    let injection_storage = Arc::clone(&storage);
    let completion_search = search.clone();
    let candidate_limiter = execution
        .as_ref()
        .map(crate::execution::ExecutionLimits::candidate);
    let activities = ActivityRegistry::builder()
        .register(
            PROJECT_TASK_ACTIVITY,
            move |_context: ActivityContext, input: String| {
                let storage = Arc::clone(&storage);
                async move {
                    // Projection failure is deliberately best-effort: Duroxide history is
                    // authoritative and repair can recreate this operator-facing state.
                    match serde_json::from_str::<ProjectionActivity>(&input) {
                        Ok(update) => {
                            if let Err(error) = storage.project_task(&update.into()).await {
                                tracing::warn!(
                                    service = "sporos",
                                    error = %crate::error_report::ErrorReport::new(&error),
                                    "best-effort task projection failed"
                                );
                            }
                        }
                        Err(error) => tracing::warn!(
                            service = "sporos",
                            error = %crate::error_report::ErrorReport::new(&error),
                            "best-effort task projection input is invalid"
                        ),
                    }
                    Ok(input)
                }
            },
        )
        .register(
            completion::PROJECT_ACTIVITY,
            move |_context: ActivityContext, input: String| {
                let storage = Arc::clone(&completion_storage);
                let completion_search = completion_search.clone();
                async move {
                    let input: CompletionInput = serde_json::from_str(&input).map_err(|error| {
                        crate::activity_failure::permanent("invalid_completion_input", &error)
                    })?;
                    if let Some(search) = completion_search.as_ref() {
                        search
                            .project_completion(&input)
                            .await
                            .map_err(|error| error.activity_failure())?;
                    } else {
                        storage.project_completion(&input).await.map_err(|error| {
                            crate::activity_failure::transient(
                                "completion_projection_failed",
                                &error,
                            )
                        })?;
                    }
                    Ok("{}".to_owned())
                }
            },
        )
        .register(
            candidate_workflow::EVALUATE_ACTIVITY,
            move |_context: ActivityContext, input: String| {
                let storage = Arc::clone(&candidate_storage);
                let candidate_limiter = candidate_limiter.clone();
                async move {
                    let _permit = match &candidate_limiter {
                        Some(limiter) => Some(crate::execution::permit(limiter).await),
                        None => None,
                    };
                    let input: crate::candidate::CandidateWorkflowInput =
                        serde_json::from_str(&input).map_err(|error| {
                            crate::activity_failure::permanent("invalid_candidate_input", &error)
                        })?;
                    let result = storage
                        .evaluate_candidate(&input, now_ms())
                        .await
                        .map_err(|error| error.activity_failure())?;
                    serde_json::to_string(&result).map_err(|error| {
                        crate::activity_failure::permanent("encode_candidate_result", &error)
                    })
                }
            },
        );
    let activities = if let Some(search) = search {
        search.register(activities)
    } else {
        activities
    };
    let activities = if let Some(data_scan) = data_scan {
        data_scan.register(activities)
    } else {
        activities
    };
    let activities =
        crate::task_control::TaskControl::new(Arc::clone(&injection_storage)).register(activities);
    let injection = crate::injection::InjectionExecutor::new(injection_storage, qbit);
    let injection = if let Some(execution) = execution {
        injection.with_limiters(execution.candidate(), execution.filesystem())
    } else {
        injection
    };
    let activities = injection.register(activities).build();
    let orchestrations = OrchestrationRegistry::builder()
        .register_versioned(FAKE_TASK_NAME, FAKE_TASK_VERSION, fake_task)
        .register_versioned(
            completion::ORCHESTRATION_NAME,
            completion::ORCHESTRATION_VERSION,
            completion_workflow,
        )
        .register_versioned(
            crate::candidate::ORCHESTRATION_NAME,
            crate::candidate::ORCHESTRATION_VERSION,
            candidate_workflow::workflow,
        )
        .register_versioned(
            crate::search::ORCHESTRATION_NAME,
            crate::search::ORCHESTRATION_VERSION,
            crate::search::workflow,
        )
        .register_versioned(
            crate::search::BACKFILL_ORCHESTRATION_NAME,
            crate::search::BACKFILL_ORCHESTRATION_VERSION,
            crate::search::backfill_workflow,
        )
        .register_versioned(
            crate::data_scan::ORCHESTRATION_NAME,
            crate::data_scan::ORCHESTRATION_VERSION,
            crate::data_scan::workflow,
        )
        .register_versioned(
            crate::task_control::ORCHESTRATION_NAME,
            crate::task_control::ORCHESTRATION_VERSION,
            crate::task_control::workflow,
        )
        .build();
    (activities, orchestrations)
}

fn now_ms() -> i64 {
    let elapsed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or(Duration::ZERO);
    i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX)
}

async fn completion_workflow(
    context: OrchestrationContext,
    input: String,
) -> Result<String, String> {
    let _: CompletionInput = serde_json::from_str(&input)
        .map_err(|error| format!("invalid completion input: {error}"))?;
    context
        .schedule_activity_with_retry(
            completion::PROJECT_ACTIVITY,
            input.clone(),
            activity_retry_policy(),
        )
        .await?;
    Ok(input)
}

async fn fake_task(context: OrchestrationContext, input: String) -> Result<String, String> {
    let input: FakeTaskInput = serde_json::from_str(&input)
        .map_err(|error| format!("invalid phase 1 fake-task input: {error}"))?;
    project(&context, ProjectionActivity::running(&input)).await?;
    context
        .schedule_timer(Duration::from_millis(input.delay_ms))
        .await;
    project(&context, ProjectionActivity::completed(&input)).await?;
    serde_json::to_string(&input).map_err(|error| format!("encode fake-task output: {error}"))
}

async fn project(context: &OrchestrationContext, update: ProjectionActivity) -> Result<(), String> {
    let payload =
        serde_json::to_string(&update).map_err(|error| format!("encode projection: {error}"))?;
    let _ = context
        .schedule_activity_with_retry(PROJECT_TASK_ACTIVITY, payload, activity_retry_policy())
        .await?;
    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectionActivity {
    task_id: [u8; 16],
    expected_generation: u64,
    state: String,
    occurred_at: i64,
    terminal: bool,
}

impl ProjectionActivity {
    fn running(input: &FakeTaskInput) -> Self {
        Self {
            task_id: input.task_id,
            expected_generation: 0,
            state: "running".to_owned(),
            occurred_at: input.accepted_at_ms,
            terminal: false,
        }
    }

    fn completed(input: &FakeTaskInput) -> Self {
        Self {
            task_id: input.task_id,
            expected_generation: 1,
            state: "dry_run_complete".to_owned(),
            occurred_at: input
                .accepted_at_ms
                .saturating_add(i64::try_from(input.delay_ms).unwrap_or(i64::MAX)),
            terminal: true,
        }
    }
}

impl From<ProjectionActivity> for ProjectionUpdate {
    fn from(value: ProjectionActivity) -> Self {
        Self {
            task_id: TaskId::from_bytes(value.task_id),
            expected_generation: value.expected_generation,
            state: value.state,
            reason_code: None,
            execution_id: Some("1".to_owned()),
            observed_retry_count: 0,
            detail_json: Some("{}".to_owned()),
            occurred_at: value.occurred_at,
            terminal: value.terminal,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use duroxide::runtime::Runtime;
    use duroxide::runtime::registry::ActivityRegistry;
    use duroxide::{ActivityContext, Client, OrchestrationRegistry, OrchestrationStatus};
    use std::process::Command;
    use tempfile::TempDir;

    use super::*;
    use crate::durable_ingress::{NewTask, PolicySnapshot};
    use crate::outbox::OutboxDispatcher;
    use sporos_model::{PolicySnapshotId, TaskKey};

    const CRASH_PROBE_DIRECTORY: &str = "SPOROS_PHASE1_CRASH_PROBE_DIRECTORY";

    #[tokio::test]
    async fn retries_only_classified_transient_activity_failures() {
        let directory = TempDir::new().expect("create temporary directory");
        let storage = open(&directory).await;
        let provider = storage.duroxide_provider();
        let client = Client::new(provider.clone());
        let transient_attempts = Arc::new(AtomicUsize::new(0));
        let permanent_attempts = Arc::new(AtomicUsize::new(0));
        let transient_counter = Arc::clone(&transient_attempts);
        let permanent_counter = Arc::clone(&permanent_attempts);
        let activities = ActivityRegistry::builder()
            .register(
                "TransientFixture",
                move |_context: ActivityContext, _input: String| {
                    let attempts = Arc::clone(&transient_counter);
                    async move {
                        if attempts.fetch_add(1, Ordering::SeqCst) < 2 {
                            let error = std::io::Error::other("retry");
                            Err(crate::activity_failure::transient(
                                "fixture_unavailable",
                                &error,
                            ))
                        } else {
                            Ok("complete".to_owned())
                        }
                    }
                },
            )
            .register(
                "PermanentFixture",
                move |_context: ActivityContext, _input: String| {
                    let attempts = Arc::clone(&permanent_counter);
                    async move {
                        attempts.fetch_add(1, Ordering::SeqCst);
                        let error = std::io::Error::other("stop");
                        Err(crate::activity_failure::permanent(
                            "invalid_fixture",
                            &error,
                        ))
                    }
                },
            )
            .build();
        let orchestrations = OrchestrationRegistry::builder()
            .register_versioned("RetryFixture", "1.0.0", retry_fixture)
            .build();
        let runtime = Runtime::start_with_store(provider, activities, orchestrations).await;
        client
            .start_orchestration_versioned(
                "retry-transient",
                "RetryFixture",
                "1.0.0",
                "TransientFixture",
            )
            .await
            .unwrap();
        client
            .start_orchestration_versioned(
                "retry-permanent",
                "RetryFixture",
                "1.0.0",
                "PermanentFixture",
            )
            .await
            .unwrap();

        assert!(matches!(
            client
                .wait_for_orchestration("retry-transient", Duration::from_secs(5))
                .await
                .unwrap(),
            OrchestrationStatus::Completed { .. }
        ));
        assert!(matches!(
            client
                .wait_for_orchestration("retry-permanent", Duration::from_secs(5))
                .await
                .unwrap(),
            OrchestrationStatus::Failed { .. }
        ));
        assert_eq!(transient_attempts.load(Ordering::SeqCst), 3);
        assert_eq!(permanent_attempts.load(Ordering::SeqCst), 1);
        runtime.shutdown(None).await;
    }

    async fn retry_fixture(
        context: OrchestrationContext,
        activity: String,
    ) -> Result<String, String> {
        context
            .schedule_activity_with_retry(activity, "{}", activity_retry_policy())
            .await
    }

    #[tokio::test]
    async fn persisted_fake_task_survives_process_kill() {
        let directory = TempDir::new().expect("create temporary directory");
        let marker = directory.path().join("timer-created");
        let mut child = Command::new(std::env::current_exe().expect("locate test executable"))
            .args([
                "--exact",
                "engine::tests::fake_task_crash_probe",
                "--nocapture",
            ])
            .env(CRASH_PROBE_DIRECTORY, directory.path())
            .spawn()
            .expect("start fake-task crash probe");
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if marker.exists() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("crash probe did not persist its timer");
        child.kill().expect("kill crash probe");
        assert!(!child.wait().expect("wait for crash probe").success());

        let storage = Arc::new(open(&directory).await);
        let provider = storage.duroxide_provider();
        let client = Client::new(provider.clone());
        let (activities, orchestrations) = registries(Arc::clone(&storage), None, None, None);
        let runtime = Runtime::start_with_store(provider, activities, orchestrations).await;
        let status = client
            .wait_for_orchestration("fake-v1:1", Duration::from_secs(7))
            .await
            .expect("wait for recovered fake task");

        assert!(matches!(status, OrchestrationStatus::Completed { .. }));
        assert_eq!(
            projected_state(&storage).await.as_deref(),
            Some("dry_run_complete")
        );
        runtime.shutdown(Some(100)).await;
    }

    #[tokio::test]
    async fn fake_task_crash_probe() {
        let Some(directory) = std::env::var_os(CRASH_PROBE_DIRECTORY) else {
            return;
        };
        let directory = TempDir::new_in(directory).expect("create crash-probe directory");
        let parent = directory
            .path()
            .parent()
            .expect("crash-probe parent")
            .to_owned();
        let storage = Arc::new(
            Storage::open(parent.join("sporos.lock"), parent.join("sporos.db"))
                .await
                .expect("open crash-probe storage"),
        );
        storage
            .accept_task(&task(1, 2_000))
            .await
            .expect("accept crash-probe task");
        let provider = storage.duroxide_provider();
        let client = Client::new(provider.clone());
        let (activities, orchestrations) = registries(Arc::clone(&storage), None, None, None);
        let _runtime = Runtime::start_with_store(provider, activities, orchestrations).await;
        OutboxDispatcher::new(&storage, client, 1)
            .run_once(10)
            .await
            .expect("dispatch crash-probe task");
        wait_for_timer(&storage).await;
        std::fs::write(parent.join("timer-created"), b"ready").expect("write crash marker");
        tokio::time::sleep(Duration::from_secs(60)).await;
    }

    #[tokio::test]
    async fn completes_a_persisted_fake_task() {
        let directory = TempDir::new().expect("create temporary directory");
        let storage = Arc::new(open(&directory).await);
        storage
            .accept_task(&task(1, 1))
            .await
            .expect("accept fake task");
        let provider = storage.duroxide_provider();
        let client = Client::new(provider.clone());
        let (activities, orchestrations) = registries(Arc::clone(&storage), None, None, None);
        let runtime = Runtime::start_with_store(provider, activities, orchestrations).await;
        OutboxDispatcher::new(&storage, client.clone(), 1)
            .run_once(10)
            .await
            .expect("dispatch fake task");

        let status = client
            .wait_for_orchestration("fake-v1:1", Duration::from_secs(5))
            .await
            .expect("wait for fake task");
        assert!(matches!(status, OrchestrationStatus::Completed { .. }));
        assert_eq!(
            projected_state(&storage).await.as_deref(),
            Some("dry_run_complete")
        );
        runtime.shutdown(Some(100)).await;
    }

    #[tokio::test]
    async fn missing_projection_does_not_control_the_workflow() {
        let directory = TempDir::new().expect("create temporary directory");
        let storage = Arc::new(open(&directory).await);
        let provider = storage.duroxide_provider();
        let client = Client::new(provider.clone());
        let (activities, orchestrations) = registries(Arc::clone(&storage), None, None, None);
        let runtime = Runtime::start_with_store(provider, activities, orchestrations).await;
        let input = serde_json::to_string(&FakeTaskInput {
            task_id: [9; 16],
            accepted_at_ms: 1,
            delay_ms: 1,
        })
        .expect("encode fake task");
        client
            .start_orchestration_versioned(
                "fake-v1:missing",
                FAKE_TASK_NAME,
                FAKE_TASK_VERSION,
                input,
            )
            .await
            .expect("start fake task without projection");

        let status = client
            .wait_for_orchestration("fake-v1:missing", Duration::from_secs(5))
            .await
            .expect("wait for fake task");
        assert!(matches!(status, OrchestrationStatus::Completed { .. }));
        runtime.shutdown(Some(100)).await;
    }

    #[tokio::test]
    async fn stale_projection_does_not_control_the_workflow() {
        let directory = TempDir::new().expect("create temporary directory");
        let storage = Arc::new(open(&directory).await);
        storage
            .accept_task(&task(1, 1))
            .await
            .expect("accept fake task");
        storage
            .project_task(&ProjectionUpdate {
                task_id: TaskId::from_bytes([1; 16]),
                expected_generation: 0,
                state: "searching".to_owned(),
                reason_code: None,
                execution_id: None,
                observed_retry_count: 0,
                detail_json: None,
                occurred_at: 1,
                terminal: false,
            })
            .await
            .expect("make projection stale");
        let provider = storage.duroxide_provider();
        let client = Client::new(provider.clone());
        let (activities, orchestrations) = registries(Arc::clone(&storage), None, None, None);
        let runtime = Runtime::start_with_store(provider, activities, orchestrations).await;
        OutboxDispatcher::new(&storage, client.clone(), 1)
            .run_once(10)
            .await
            .expect("dispatch fake task");

        let status = client
            .wait_for_orchestration("fake-v1:1", Duration::from_secs(5))
            .await
            .expect("wait for fake task");
        assert!(matches!(status, OrchestrationStatus::Completed { .. }));
        runtime.shutdown(Some(100)).await;
    }

    #[tokio::test]
    async fn resumes_a_durable_fake_task_after_runtime_restart() {
        let directory = TempDir::new().expect("create temporary directory");
        let storage = Arc::new(open(&directory).await);
        storage
            .accept_task(&task(1, 5_000))
            .await
            .expect("accept fake task");
        let provider = storage.duroxide_provider();
        let client = Client::new(provider.clone());
        let (activities, orchestrations) = registries(Arc::clone(&storage), None, None, None);
        let runtime = Runtime::start_with_store(provider, activities, orchestrations).await;
        OutboxDispatcher::new(&storage, client, 1)
            .run_once(10)
            .await
            .expect("dispatch fake task");
        wait_for_timer(&storage).await;
        runtime.shutdown(Some(0)).await;

        let provider = storage.duroxide_provider();
        let client = Client::new(provider.clone());
        let (activities, orchestrations) = registries(Arc::clone(&storage), None, None, None);
        let runtime = Runtime::start_with_store(provider, activities, orchestrations).await;
        let status = client
            .wait_for_orchestration("fake-v1:1", Duration::from_secs(7))
            .await
            .expect("wait for restarted task");

        assert!(matches!(status, OrchestrationStatus::Completed { .. }));
        assert_eq!(
            projected_state(&storage).await.as_deref(),
            Some("dry_run_complete")
        );
        runtime.shutdown(Some(100)).await;
    }

    fn task(marker: u8, delay_ms: u64) -> NewTask {
        NewTask {
            id: TaskId::from_bytes([marker; 16]),
            key: TaskKey::from_bytes([marker; 32]),
            kind: "fake".to_owned(),
            policy: PolicySnapshot {
                id: PolicySnapshotId::from_bytes([marker; 16]),
                config_hash: [marker; 32],
                matcher_version: "phase1".to_owned(),
                payload_json: "{}".to_owned(),
                created_at: 1,
            },
            orchestration_name: FAKE_TASK_NAME.to_owned(),
            orchestration_version: FAKE_TASK_VERSION.to_owned(),
            instance_id: format!("fake-v1:{marker}"),
            input_json: serde_json::to_string(&FakeTaskInput {
                task_id: [marker; 16],
                accepted_at_ms: 1,
                delay_ms,
            })
            .expect("encode fake-task input"),
            created_at: 1,
        }
    }

    async fn open(directory: &TempDir) -> Storage {
        Storage::open(
            directory.path().join("sporos.lock"),
            directory.path().join("sporos.db"),
        )
        .await
        .expect("open storage")
    }

    async fn projected_state(storage: &Storage) -> Option<String> {
        sqlx::query_scalar("SELECT state FROM sporos_task WHERE id = ?")
            .bind([1_u8; 16].as_slice())
            .fetch_optional(storage.pool())
            .await
            .expect("read projected state")
    }

    async fn wait_for_timer(storage: &Storage) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let count = sqlx::query_scalar::<_, i64>(
                    "SELECT count(*) FROM history
                     WHERE instance_id = 'fake-v1:1' AND event_type = 'TimerCreated'",
                )
                .fetch_one(storage.pool())
                .await
                .expect("inspect timer history");
                if count == 1 {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("fake task did not schedule its timer");
    }
}
