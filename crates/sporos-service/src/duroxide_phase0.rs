use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use duroxide::providers::sqlite::SqliteProvider;
use duroxide::runtime::Runtime;
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::{ActivityContext, Client, OrchestrationContext, OrchestrationRegistry};
use tempfile::TempDir;

use crate::storage::Storage;

const PROBE_NAME: &str = "ReplayProbe";
const V1: &str = "1.0.0";
const V2: &str = "2.0.0";

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
