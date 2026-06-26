use duroxide::providers::Provider;
use duroxide::providers::sqlite::SqliteProvider;
use duroxide::runtime::registry::ActivityRegistry;
use duroxide::runtime::{self, OrchestrationStatus, RuntimeOptions};
use duroxide::{ActivityContext, Client, OrchestrationContext, OrchestrationRegistry};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt::Debug;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

const ORCHESTRATION_NAME: &str = "SporosDuroxideEvaluation";
const ACTIVITY_NAME: &str = "RecordEvaluationInput";
const CONTINUE_EVENT_NAME: &str = "EvaluationContinue";
const WAITING_STATUS: &str = "waiting-for-event";

static TEMP_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EvaluationInput {
    subject: String,
    timer_ms: u64,
}

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
struct EvaluationActivityOutput {
    recorded_subject: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct EvaluationContinueEvent {
    decision: String,
}

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
struct EvaluationOutput {
    recorded_subject: String,
    decision: String,
}

#[tokio::test]
async fn sqlite_runtime_resumes_waiting_orchestration_after_restart_without_repeating_activity()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let temp_dir = TestTempDir::new("duroxide-evaluation")?;
    let db_path = temp_dir.path().join("sporos-workflows.db");
    fs::File::create(&db_path)?;
    let db_url = format!("sqlite:{}", db_path.display());
    let activity_invocations = Arc::new(AtomicUsize::new(0));

    let store = open_store(&db_url).await?;
    let runtime = start_runtime(Arc::clone(&store), Arc::clone(&activity_invocations)).await;
    let client = Client::new(Arc::clone(&store));
    let instance_id = "sporos-duroxide-evaluation-restart";

    client
        .start_orchestration_typed(
            instance_id,
            ORCHESTRATION_NAME,
            EvaluationInput {
                subject: "announce".to_string(),
                timer_ms: 1,
            },
        )
        .await?;

    wait_until_waiting_for_event(&client, instance_id).await?;
    ensure_eq(
        activity_invocations.load(Ordering::SeqCst),
        1,
        "activity invocation count before restart",
    )?;

    runtime.shutdown(Some(1_000)).await;
    drop(client);
    drop(store);

    let restarted_store = open_store(&db_url).await?;
    let restarted_runtime = start_runtime(
        Arc::clone(&restarted_store),
        Arc::clone(&activity_invocations),
    )
    .await;
    let restarted_client = Client::new(restarted_store);

    restarted_client
        .raise_event_typed(
            instance_id,
            CONTINUE_EVENT_NAME,
            &EvaluationContinueEvent {
                decision: "continue".to_string(),
            },
        )
        .await?;

    let output = match restarted_client
        .wait_for_orchestration_typed::<EvaluationOutput>(instance_id, Duration::from_secs(5))
        .await?
    {
        Ok(output) => output,
        Err(error) => return Err(format!("orchestration failed after restart: {error}").into()),
    };

    ensure_eq(
        output,
        EvaluationOutput {
            recorded_subject: "announce".to_string(),
            decision: "continue".to_string(),
        },
        "workflow output after restart",
    )?;
    ensure_eq(
        activity_invocations.load(Ordering::SeqCst),
        1,
        "activity invocation count after restart",
    )?;

    restarted_runtime.shutdown(Some(1_000)).await;

    Ok(())
}

fn ensure_eq<T>(actual: T, expected: T, context: &str) -> Result<(), Box<dyn Error + Send + Sync>>
where
    T: Debug + Eq,
{
    if actual == expected {
        Ok(())
    } else {
        Err(format!("{context}: expected {expected:?}, got {actual:?}").into())
    }
}

async fn open_store(db_url: &str) -> Result<Arc<dyn Provider>, Box<dyn Error + Send + Sync>> {
    Ok(Arc::new(SqliteProvider::new(db_url, None).await?) as Arc<dyn Provider>)
}

async fn start_runtime(
    store: Arc<dyn Provider>,
    activity_invocations: Arc<AtomicUsize>,
) -> Arc<runtime::Runtime> {
    let activities = ActivityRegistry::builder()
        .register_typed(
            ACTIVITY_NAME,
            move |_ctx: ActivityContext, input: EvaluationInput| {
                let activity_invocations = Arc::clone(&activity_invocations);
                async move {
                    activity_invocations.fetch_add(1, Ordering::SeqCst);
                    Ok(EvaluationActivityOutput {
                        recorded_subject: input.subject,
                    })
                }
            },
        )
        .build();

    let orchestrations = OrchestrationRegistry::builder()
        .register_typed(
            ORCHESTRATION_NAME,
            |ctx: OrchestrationContext, input: EvaluationInput| async move {
                let activity_output: EvaluationActivityOutput =
                    ctx.schedule_activity_typed(ACTIVITY_NAME, &input).await?;
                ctx.schedule_timer(Duration::from_millis(input.timer_ms))
                    .await;
                ctx.set_custom_status(WAITING_STATUS);
                let event: EvaluationContinueEvent =
                    ctx.schedule_wait_typed(CONTINUE_EVENT_NAME).await;
                Ok(EvaluationOutput {
                    recorded_subject: activity_output.recorded_subject,
                    decision: event.decision,
                })
            },
        )
        .build();

    runtime::Runtime::start_with_options(
        store,
        activities,
        orchestrations,
        RuntimeOptions {
            dispatcher_min_poll_interval: Duration::from_millis(10),
            dispatcher_long_poll_timeout: Duration::from_millis(50),
            orchestration_concurrency: 1,
            worker_concurrency: 1,
            ..RuntimeOptions::default()
        },
    )
    .await
}

async fn wait_until_waiting_for_event(
    client: &Client,
    instance_id: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let deadline = Instant::now() + Duration::from_secs(5);

    loop {
        match client.get_orchestration_status(instance_id).await? {
            OrchestrationStatus::Running {
                custom_status: Some(status),
                ..
            } if status == WAITING_STATUS => return Ok(()),
            OrchestrationStatus::Completed { output, .. } => {
                return Err(
                    format!("orchestration completed before external wait: {output}").into(),
                );
            }
            OrchestrationStatus::Failed { details, .. } => {
                return Err(format!(
                    "orchestration failed before external wait: {}",
                    details.display_message()
                )
                .into());
            }
            OrchestrationStatus::Running { .. } | OrchestrationStatus::NotFound => {}
        }

        if Instant::now() >= deadline {
            return Err(
                "timed out waiting for Duroxide orchestration to reach external wait".into(),
            );
        }

        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

struct TestTempDir {
    path: PathBuf,
}

impl TestTempDir {
    fn new(label: &str) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let unique = TEMP_DIR_COUNTER.fetch_add(1, Ordering::SeqCst);
        let path =
            std::env::temp_dir().join(format!("sporos-{label}-{}-{unique}", std::process::id()));
        fs::create_dir_all(&path)?;
        Ok(Self { path })
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestTempDir {
    fn drop(&mut self) {
        drop(fs::remove_dir_all(&self.path));
    }
}
