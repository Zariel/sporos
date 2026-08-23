use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use duroxide::providers::sqlite::SqliteProvider;
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{Runtime, RuntimeOptions};
use duroxide::{ActivityContext, Client, OrchestrationContext, OrchestrationRegistry};
use tempfile::TempDir;

use crate::storage::Storage;

const PROBE_NAME: &str = "ReplayProbe";
const V1: &str = "1.0.0";
const V2: &str = "2.0.0";
const CRASH_PROBE_DIR: &str = "SPOROS_DUROXIDE_CRASH_PROBE_DIR";

#[tokio::test]
async fn replays_a_pinned_version_after_restart() {
    let directory = TempDir::new().expect("create temporary directory");
    let activity_runs = Arc::new(AtomicUsize::new(0));
    let storage = open_in(&directory).await;
    let provider = storage.duroxide_provider();
    let runtime = Runtime::start_with_store(
        provider.clone(),
        activities(activity_runs.clone()),
        OrchestrationRegistry::builder()
            .register_versioned(PROBE_NAME, V1, replay_v1)
            .build(),
    )
    .await;
    let client = Client::new(provider.clone());

    client
        .start_orchestration_versioned("replay-v1", PROBE_NAME, V1, "input")
        .await
        .expect("start version 1");
    wait_for_event(&provider, "replay-v1", "ExternalSubscribed").await;
    assert_eq!(activity_runs.load(Ordering::SeqCst), 1);

    runtime.shutdown(Some(50)).await;
    client
        .raise_event("replay-v1", "Continue", "signal")
        .await
        .expect("persist event while runtime is stopped");
    drop(client);
    drop(provider);
    drop(storage);

    let storage = open_in(&directory).await;
    let provider = storage.duroxide_provider();
    let runtime = Runtime::start_with_store(
        provider.clone(),
        activities(activity_runs.clone()),
        OrchestrationRegistry::builder()
            .register_versioned(PROBE_NAME, V1, replay_v1)
            .register_versioned(PROBE_NAME, V2, replay_v2)
            .build(),
    )
    .await;
    let client = Client::new(provider);

    let status = client
        .wait_for_orchestration("replay-v1", Duration::from_secs(5))
        .await
        .expect("wait for replayed version 1");
    let duroxide::OrchestrationStatus::Completed { output, .. } = status else {
        panic!("version 1 did not complete");
    };
    assert_eq!(output, "v1:input:signal");
    assert_eq!(activity_runs.load(Ordering::SeqCst), 1);
    assert_eq!(
        client
            .get_instance_info("replay-v1")
            .await
            .expect("inspect version 1")
            .orchestration_version,
        V1
    );

    client
        .start_orchestration_versioned("replay-v2", PROBE_NAME, V2, "input")
        .await
        .expect("start version 2");
    let status = client
        .wait_for_orchestration("replay-v2", Duration::from_secs(5))
        .await
        .expect("wait for version 2");
    let duroxide::OrchestrationStatus::Completed { output, .. } = status else {
        panic!("version 2 did not complete");
    };
    assert_eq!(output, "v2:input");
    assert_eq!(
        client
            .get_instance_info("replay-v2")
            .await
            .expect("inspect version 2")
            .orchestration_version,
        V2
    );

    runtime.shutdown(Some(50)).await;
}

#[tokio::test]
async fn retries_an_activity_interrupted_by_process_death() {
    let directory = TempDir::new().expect("create temporary directory");
    let started_path = directory.path().join("activity-started");
    let mut child = Command::new(std::env::current_exe().expect("locate test executable"))
        .args(["--exact", "duroxide_phase0::crash_probe", "--nocapture"])
        .env(CRASH_PROBE_DIR, directory.path())
        .spawn()
        .expect("start crash probe");

    let started = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if started_path.exists() {
                return true;
            }
            if child.try_wait().expect("inspect crash probe").is_some() {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or(false);
    if !started {
        let _ = child.kill();
        let _ = child.wait();
        panic!("crash probe activity did not start");
    }

    child.kill().expect("kill crash probe");
    assert!(!child.wait().expect("reap crash probe").success());
    tokio::time::sleep(Duration::from_millis(500)).await;

    let storage = open_in(&directory).await;
    let provider = storage.duroxide_provider();
    let recovered_path = directory.path().join("activity-recovered");
    let activities = ActivityRegistry::builder()
        .register(
            "CrashProbe",
            move |_context: ActivityContext, _input: String| {
                let recovered_path = recovered_path.clone();
                async move {
                    std::fs::write(recovered_path, b"recovered")
                        .map_err(|error| error.to_string())?;
                    Ok("recovered".to_owned())
                }
            },
        )
        .build();
    let runtime = Runtime::start_with_options(
        provider.clone(),
        activities,
        OrchestrationRegistry::builder()
            .register_versioned("CrashWorkflow", V1, crash_workflow)
            .build(),
        crash_options(),
    )
    .await;
    let client = Client::new(provider);

    let status = client
        .wait_for_orchestration("crash-probe", Duration::from_secs(5))
        .await
        .expect("wait for recovered activity");
    let duroxide::OrchestrationStatus::Completed { output, .. } = status else {
        panic!("recovered orchestration did not complete");
    };
    assert_eq!(output, "recovered");
    assert!(directory.path().join("activity-recovered").exists());

    runtime.shutdown(Some(50)).await;
}

#[tokio::test]
async fn recovers_a_durable_timer_after_restart() {
    let directory = TempDir::new().expect("create temporary directory");
    let storage = open_in(&directory).await;
    let provider = storage.duroxide_provider();
    let runtime = Runtime::start_with_store(
        provider.clone(),
        ActivityRegistry::builder().build(),
        OrchestrationRegistry::builder()
            .register_versioned("TimerProbe", V1, timer_workflow)
            .build(),
    )
    .await;
    let client = Client::new(provider.clone());
    client
        .start_orchestration_versioned("timer-probe", "TimerProbe", V1, "input")
        .await
        .expect("start timer workflow");
    wait_for_event(&provider, "timer-probe", "TimerCreated").await;

    runtime.shutdown(Some(0)).await;
    drop(client);
    drop(provider);
    drop(storage);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let storage = open_in(&directory).await;
    let provider = storage.duroxide_provider();
    let runtime = Runtime::start_with_store(
        provider.clone(),
        ActivityRegistry::builder().build(),
        OrchestrationRegistry::builder()
            .register_versioned("TimerProbe", V1, timer_workflow)
            .build(),
    )
    .await;
    let client = Client::new(provider);
    let status = client
        .wait_for_orchestration("timer-probe", Duration::from_secs(5))
        .await
        .expect("wait for recovered timer");
    let duroxide::OrchestrationStatus::Completed { output, .. } = status else {
        panic!("timer workflow did not complete");
    };
    assert_eq!(output, "timer-fired");

    runtime.shutdown(Some(50)).await;
}

#[tokio::test]
async fn terminates_an_orchestration_with_corrupted_history() {
    let directory = TempDir::new().expect("create temporary directory");
    let storage = open_in(&directory).await;
    let provider = storage.duroxide_provider();
    let runtime = Runtime::start_with_store(
        provider.clone(),
        activities(Arc::new(AtomicUsize::new(0))),
        OrchestrationRegistry::builder()
            .register_versioned(PROBE_NAME, V1, replay_v1)
            .build(),
    )
    .await;
    let client = Client::new(provider.clone());
    client
        .start_orchestration_versioned("poison-probe", PROBE_NAME, V1, "input")
        .await
        .expect("start poison probe");
    wait_for_event(&provider, "poison-probe", "ExternalSubscribed").await;

    runtime.shutdown(Some(50)).await;
    let updated = duroxide_sqlx::query(
        "UPDATE history SET event_data = 'not-json' WHERE instance_id = ? AND event_id = (SELECT min(event_id) FROM history WHERE instance_id = ?)",
    )
    .bind("poison-probe")
    .bind("poison-probe")
    .execute(provider.get_pool())
    .await
    .expect("corrupt persisted history");
    assert_eq!(updated.rows_affected(), 1);
    client
        .raise_event("poison-probe", "Continue", "signal")
        .await
        .expect("enqueue poisoned orchestration");

    let runtime = Runtime::start_with_options(
        provider.clone(),
        activities(Arc::new(AtomicUsize::new(0))),
        OrchestrationRegistry::builder()
            .register_versioned(PROBE_NAME, V1, replay_v1)
            .build(),
        RuntimeOptions {
            dispatcher_min_poll_interval: Duration::from_millis(10),
            max_attempts: 1,
            ..RuntimeOptions::default()
        },
    )
    .await;

    let output = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let row = duroxide_sqlx::query_as::<_, (String, Option<String>)>(
                "SELECT status, output FROM executions WHERE instance_id = ?",
            )
            .bind("poison-probe")
            .fetch_one(provider.get_pool())
            .await
            .expect("inspect poisoned execution");
            if row.0 == "Failed" {
                return row.1.expect("failed execution output");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("poisoned orchestration did not terminate");
    assert!(output.contains("history deserialization failed"));

    let queued = duroxide_sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM orchestrator_queue WHERE instance_id = ?",
    )
    .bind("poison-probe")
    .fetch_one(provider.get_pool())
    .await
    .expect("inspect poisoned queue");
    assert_eq!(queued, 0);

    runtime.shutdown(Some(50)).await;
}

#[tokio::test]
async fn makes_progress_with_concurrent_domain_writes() {
    const WORK_ITEMS: usize = 64;

    let directory = TempDir::new().expect("create temporary directory");
    let storage = open_in(&directory).await;
    let provider = storage.duroxide_provider();
    let runtime = Runtime::start_with_store(
        provider.clone(),
        ActivityRegistry::builder().build(),
        OrchestrationRegistry::builder()
            .register_versioned(PROBE_NAME, V2, replay_v2)
            .build(),
    )
    .await;
    let client = Client::new(provider);

    let domain_writes = async {
        for marker in 0..WORK_ITEMS {
            sqlx::query(
                "UPDATE sporos_schema_metadata SET value = ? WHERE key = 'application_schema'",
            )
            .bind(marker.to_string())
            .execute(storage.pool())
            .await
            .expect("write domain state");
        }
    };
    let orchestration_writes = async {
        for marker in 0..WORK_ITEMS {
            let instance = format!("contention-probe-{marker}");
            client
                .start_orchestration_versioned(&instance, PROBE_NAME, V2, "input")
                .await
                .expect("start orchestration under contention");
        }
        for marker in 0..WORK_ITEMS {
            let instance = format!("contention-probe-{marker}");
            let status = client
                .wait_for_orchestration(&instance, Duration::from_secs(5))
                .await
                .expect("wait for orchestration under contention");
            assert!(matches!(
                status,
                duroxide::OrchestrationStatus::Completed { .. }
            ));
        }
    };

    tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(domain_writes, orchestration_writes)
    })
    .await
    .expect("concurrent pools stopped making progress");

    runtime.shutdown(Some(50)).await;
}

#[tokio::test]
async fn crash_probe() {
    let Some(directory) = std::env::var_os(CRASH_PROBE_DIR) else {
        return;
    };
    let directory = std::path::PathBuf::from(directory);
    let storage = Storage::open(directory.join("sporos.lock"), directory.join("sporos.db"))
        .await
        .expect("open crash probe storage");
    let provider = storage.duroxide_provider();
    let started_path = directory.join("activity-started");
    let activities = ActivityRegistry::builder()
        .register(
            "CrashProbe",
            move |_context: ActivityContext, _input: String| {
                let started_path = started_path.clone();
                async move {
                    std::fs::write(started_path, b"started").map_err(|error| error.to_string())?;
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    Ok("unexpected-completion".to_owned())
                }
            },
        )
        .build();
    let runtime = Runtime::start_with_options(
        provider.clone(),
        activities,
        OrchestrationRegistry::builder()
            .register_versioned("CrashWorkflow", V1, crash_workflow)
            .build(),
        crash_options(),
    )
    .await;
    let client = Client::new(provider);
    client
        .start_orchestration_versioned("crash-probe", "CrashWorkflow", V1, "input")
        .await
        .expect("start crash workflow");

    tokio::time::sleep(Duration::from_secs(60)).await;
    runtime.shutdown(Some(0)).await;
}

fn activities(runs: Arc<AtomicUsize>) -> ActivityRegistry {
    ActivityRegistry::builder()
        .register("Record", move |_context: ActivityContext, input: String| {
            let runs = runs.clone();
            async move {
                runs.fetch_add(1, Ordering::SeqCst);
                Ok(input)
            }
        })
        .build()
}

async fn replay_v1(context: OrchestrationContext, input: String) -> Result<String, String> {
    let recorded = context.schedule_activity("Record", input).await?;
    let signal = context.schedule_wait("Continue").await;
    Ok(format!("v1:{recorded}:{signal}"))
}

async fn replay_v2(_context: OrchestrationContext, input: String) -> Result<String, String> {
    Ok(format!("v2:{input}"))
}

async fn crash_workflow(context: OrchestrationContext, input: String) -> Result<String, String> {
    context.schedule_activity("CrashProbe", input).await
}

async fn timer_workflow(context: OrchestrationContext, _input: String) -> Result<String, String> {
    context.schedule_timer(Duration::from_millis(250)).await;
    Ok("timer-fired".to_owned())
}

fn crash_options() -> RuntimeOptions {
    RuntimeOptions {
        dispatcher_min_poll_interval: Duration::from_millis(10),
        worker_lock_timeout: Duration::from_millis(300),
        worker_lock_renewal_buffer: Duration::from_millis(50),
        ..RuntimeOptions::default()
    }
}

async fn wait_for_event(provider: &SqliteProvider, instance: &str, event_type: &str) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let count = duroxide_sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM history WHERE instance_id = ? AND event_type = ?",
            )
            .bind(instance)
            .bind(event_type)
            .fetch_one(provider.get_pool())
            .await
            .expect("inspect Duroxide history");
            if count > 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("Duroxide history event timeout");
}

async fn open_in(directory: &TempDir) -> Storage {
    Storage::open(
        directory.path().join("sporos.lock"),
        directory.path().join("sporos.db"),
    )
    .await
    .expect("open storage")
}
