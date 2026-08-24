use std::sync::Arc;
use std::time::Duration;

use duroxide::runtime::registry::ActivityRegistryBuilder;
use duroxide::{
    ActivityContext, AppErrorKind, Client, ErrorDetails, OrchestrationContext, OrchestrationStatus,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::Row;
use thiserror::Error;

use crate::storage::Storage;

pub(crate) const ORCHESTRATION_NAME: &str = "ReconcileTaskCancellation";
pub(crate) const ORCHESTRATION_VERSION: &str = "1.0.0";
const ACTIVITY: &str = "InspectTaskCancellation";
const POLL_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CancellationInput {
    task_id: [u8; 16],
    target_instance: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CancellationStep {
    done: bool,
}

#[derive(Clone)]
pub(crate) struct TaskControl {
    storage: Arc<Storage>,
    client: Client,
}

impl TaskControl {
    pub(crate) fn new(storage: Arc<Storage>) -> Self {
        let client = Client::new(storage.duroxide_provider());
        Self { storage, client }
    }

    pub(crate) fn register(self, activities: ActivityRegistryBuilder) -> ActivityRegistryBuilder {
        activities.register(ACTIVITY, move |_context: ActivityContext, input: String| {
            let control = self.clone();
            async move {
                let input: CancellationInput = serde_json::from_str(&input).map_err(|error| {
                    crate::activity_failure::permanent("invalid_cancellation_input", error)
                })?;
                let done = control
                    .reconcile(&input)
                    .await
                    .map_err(|error| error.activity_failure())?;
                serde_json::to_string(&CancellationStep { done }).map_err(|error| {
                    crate::activity_failure::permanent("encode_cancellation_result", error)
                })
            }
        })
    }

    async fn reconcile(&self, input: &CancellationInput) -> Result<bool, TaskControlError> {
        let (state, reason) = match self
            .client
            .get_orchestration_status(&input.target_instance)
            .await?
        {
            OrchestrationStatus::Running { .. } => {
                // Inspect before every submission so a retry after an ambiguous cancel
                // response reconciles the durable side effect before repeating it.
                self.client
                    .cancel_instance(&input.target_instance, "operator request")
                    .await?;
                return Ok(false);
            }
            OrchestrationStatus::Completed { .. } => ("completed", "cancellation_raced_completion"),
            OrchestrationStatus::Failed { details, .. } if cancelled(&details) => {
                ("cancelled", "cancelled_by_operator")
            }
            OrchestrationStatus::Failed { .. } => ("failed", "orchestration_failed"),
            OrchestrationStatus::NotFound => ("failed", "orchestration_missing"),
        };
        record_terminal(&self.storage, input.task_id, state, reason, now_ms()).await?;
        Ok(true)
    }
}

pub(crate) async fn retry(
    storage: &Storage,
    task_id: [u8; 16],
    now: i64,
) -> Result<RetryAccepted, TaskControlError> {
    let mut transaction = storage.pool().begin().await?;
    let row = sqlx::query(
        "SELECT t.kind, t.state, t.projection_generation, t.terminal_at,
                o.task_key, o.orchestration_name, o.orchestration_version,
                o.instance_id, o.input_json
         FROM sporos_task t
         JOIN sporos_outbox o ON o.instance_id = t.duroxide_instance_id
         WHERE t.id = ?",
    )
    .bind(task_id.as_slice())
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or(TaskControlError::TaskNotFound)?;
    let kind: String = row.try_get("kind")?;
    if !matches!(
        kind.as_str(),
        "process_candidate" | "search_source_indexer" | "inventory_search" | "data_scan"
    ) {
        return Err(TaskControlError::UnsupportedRetry);
    }
    let state: String = row.try_get("state")?;
    if row.try_get::<Option<i64>, _>("terminal_at")?.is_none()
        || !matches!(state.as_str(), "failed" | "cancelled")
    {
        return Err(TaskControlError::TaskNotRetryable);
    }
    let generation = row
        .try_get::<i64, _>("projection_generation")?
        .checked_add(1)
        .ok_or(TaskControlError::GenerationRange)?;
    let old_key: Vec<u8> = row.try_get("task_key")?;
    let mut hash = Sha256::new();
    hash.update(b"task-retry-v1");
    hash.update(&old_key);
    hash.update(generation.to_be_bytes());
    let task_key: [u8; 32] = hash.finalize().into();
    let previous_instance: String = row.try_get("instance_id")?;
    let instance_id = format!("{previous_instance}:retry:{generation}");
    sqlx::query(
        "INSERT INTO sporos_outbox
         (task_id, task_key, orchestration_name, orchestration_version, instance_id,
          input_json, visible_at, start_delivery_attempt_count)
         VALUES (?, ?, ?, ?, ?, ?, ?, 0)",
    )
    .bind(task_id.as_slice())
    .bind(task_key.as_slice())
    .bind(row.try_get::<String, _>("orchestration_name")?)
    .bind(row.try_get::<String, _>("orchestration_version")?)
    .bind(&instance_id)
    .bind(row.try_get::<String, _>("input_json")?)
    .bind(now)
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        "UPDATE sporos_task SET state = 'queued', projection_generation = ?,
         duroxide_instance_id = ?, duroxide_execution_id = NULL, reason_code = NULL,
         last_error_class = NULL, last_error_message = NULL,
         observed_retry_count = observed_retry_count + 1, updated_at = ?, terminal_at = NULL
         WHERE id = ?",
    )
    .bind(generation)
    .bind(&instance_id)
    .bind(now)
    .bind(task_id.as_slice())
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        "INSERT INTO sporos_task_event
         (task_id, sequence, state, reason_code, detail_json, created_at)
         VALUES (?, ?, 'queued', 'operator_retry', ?, ?)",
    )
    .bind(task_id.as_slice())
    .bind(generation)
    .bind(
        serde_json::json!({
            "previousInstanceId": previous_instance,
            "instanceId": instance_id,
        })
        .to_string(),
    )
    .bind(now)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(RetryAccepted { instance_id })
}

pub(crate) async fn request_cancel(
    storage: &Storage,
    task_id: [u8; 16],
    now: i64,
) -> Result<CancelAccepted, TaskControlError> {
    let mut transaction = storage.pool().begin_with("BEGIN IMMEDIATE").await?;
    let row = sqlx::query(
        "SELECT state, projection_generation, terminal_at, duroxide_instance_id
         FROM sporos_task WHERE id = ?",
    )
    .bind(task_id.as_slice())
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or(TaskControlError::TaskNotFound)?;
    let state: String = row.try_get("state")?;
    if row.try_get::<Option<i64>, _>("terminal_at")?.is_some() {
        return Err(TaskControlError::TaskTerminal);
    }
    let target_instance: String = row.try_get("duroxide_instance_id")?;
    if state == "cancellation_requested" {
        return Ok(CancelAccepted {
            target_instance,
            duplicate: true,
        });
    }
    let generation = row
        .try_get::<i64, _>("projection_generation")?
        .checked_add(1)
        .ok_or(TaskControlError::GenerationRange)?;
    let input = CancellationInput {
        task_id,
        target_instance: target_instance.clone(),
    };
    let reconciliation_instance = format!("cancel-task:{}:{generation}", hex(&task_id));
    let changed = sqlx::query(
        "UPDATE sporos_task SET state = 'cancellation_requested',
         projection_generation = ?, reason_code = 'operator_cancel', updated_at = ?
         WHERE id = ? AND terminal_at IS NULL AND state != 'cancellation_requested'",
    )
    .bind(generation)
    .bind(now)
    .bind(task_id.as_slice())
    .execute(&mut *transaction)
    .await?
    .rows_affected();
    if changed == 1 {
        let mut hash = Sha256::new();
        hash.update(b"task-cancellation-v1");
        hash.update(task_id);
        hash.update(generation.to_be_bytes());
        let task_key: [u8; 32] = hash.finalize().into();
        sqlx::query(
            "INSERT INTO sporos_outbox
             (task_id, task_key, orchestration_name, orchestration_version, instance_id,
              input_json, visible_at, start_delivery_attempt_count)
             VALUES (?, ?, ?, ?, ?, ?, ?, 0)",
        )
        .bind(task_id.as_slice())
        .bind(task_key.as_slice())
        .bind(ORCHESTRATION_NAME)
        .bind(ORCHESTRATION_VERSION)
        .bind(&reconciliation_instance)
        .bind(serde_json::to_string(&input).map_err(TaskControlError::EncodeCancellation)?)
        .bind(now)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "INSERT INTO sporos_task_event
             (task_id, sequence, state, reason_code, detail_json, created_at)
             VALUES (?, ?, 'cancellation_requested', 'operator_cancel', ?, ?)",
        )
        .bind(task_id.as_slice())
        .bind(generation)
        .bind(
            serde_json::json!({
                "instanceId": target_instance,
                "reconciliationInstanceId": reconciliation_instance,
            })
            .to_string(),
        )
        .bind(now)
        .execute(&mut *transaction)
        .await?;
    }
    transaction.commit().await?;
    Ok(CancelAccepted {
        target_instance,
        duplicate: changed == 0,
    })
}

async fn record_terminal(
    storage: &Storage,
    task_id: [u8; 16],
    state: &str,
    reason: &str,
    now: i64,
) -> Result<(), TaskControlError> {
    let mut transaction = storage.pool().begin().await?;
    let row = sqlx::query(
        "SELECT projection_generation FROM sporos_task
         WHERE id = ? AND state = 'cancellation_requested' AND terminal_at IS NULL",
    )
    .bind(task_id.as_slice())
    .fetch_optional(&mut *transaction)
    .await?;
    let Some(row) = row else {
        return Ok(());
    };
    let generation = row
        .try_get::<i64, _>("projection_generation")?
        .checked_add(1)
        .ok_or(TaskControlError::GenerationRange)?;
    sqlx::query(
        "UPDATE sporos_task SET state = ?, projection_generation = ?, reason_code = ?,
         updated_at = ?, terminal_at = ? WHERE id = ?",
    )
    .bind(state)
    .bind(generation)
    .bind(reason)
    .bind(now)
    .bind(now)
    .bind(task_id.as_slice())
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        "INSERT INTO sporos_task_event
         (task_id, sequence, state, reason_code, created_at) VALUES (?, ?, ?, ?, ?)",
    )
    .bind(task_id.as_slice())
    .bind(generation)
    .bind(state)
    .bind(reason)
    .bind(now)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(())
}

pub(crate) async fn workflow(
    context: OrchestrationContext,
    input: String,
) -> Result<String, String> {
    let _: CancellationInput = serde_json::from_str(&input)
        .map_err(|error| format!("invalid cancellation input: {error}"))?;
    let output = context
        .schedule_activity_with_retry(
            ACTIVITY,
            input.clone(),
            crate::engine::activity_retry_policy(),
        )
        .await?;
    let step: CancellationStep = serde_json::from_str(&output)
        .map_err(|error| format!("invalid cancellation step: {error}"))?;
    if step.done {
        Ok(output)
    } else {
        context.schedule_timer(POLL_INTERVAL).await;
        context.continue_as_new(input).await
    }
}

fn cancelled(details: &ErrorDetails) -> bool {
    matches!(
        details,
        ErrorDetails::Application {
            kind: AppErrorKind::Cancelled { .. },
            ..
        }
    )
}

fn hex(value: &[u8]) -> String {
    value.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn now_ms() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(i64::MAX)
}

#[derive(Debug)]
pub(crate) struct RetryAccepted {
    pub instance_id: String,
}

#[derive(Debug)]
pub(crate) struct CancelAccepted {
    pub target_instance: String,
    pub duplicate: bool,
}

#[derive(Debug, Error)]
pub(crate) enum TaskControlError {
    #[error("task control database operation failed")]
    Database(#[from] sqlx::Error),
    #[error("Duroxide control request failed")]
    Duroxide(#[from] duroxide::ClientError),
    #[error("task does not exist")]
    TaskNotFound,
    #[error("task is already terminal")]
    TaskTerminal,
    #[error("task is not failed or cancelled")]
    TaskNotRetryable,
    #[error("task kind does not support an operator retry")]
    UnsupportedRetry,
    #[error("task projection generation is outside the supported range")]
    GenerationRange,
    #[error("cancellation command could not be encoded")]
    EncodeCancellation(#[source] serde_json::Error),
}

impl TaskControlError {
    fn activity_failure(&self) -> String {
        match self {
            Self::Database(_) | Self::Duroxide(_) => {
                crate::activity_failure::transient("task_control_dependency_unavailable", self)
            }
            Self::TaskNotFound
            | Self::TaskTerminal
            | Self::TaskNotRetryable
            | Self::UnsupportedRetry
            | Self::GenerationRange
            | Self::EncodeCancellation(_) => {
                crate::activity_failure::permanent("invalid_task_control_state", self)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::durable_ingress::{NewTask, PolicySnapshot};
    use crate::engine::{FAKE_TASK_NAME, FAKE_TASK_VERSION, FakeTaskInput, registries};
    use crate::outbox::OutboxDispatcher;
    use sporos_model::{PolicySnapshotId, TaskId, TaskKey};

    #[tokio::test]
    async fn retry_preserves_evidence_and_enqueues_a_new_pinned_start() {
        let directory = TempDir::new().unwrap();
        let storage = open(&directory).await;
        storage
            .accept_task(&task("process_candidate", 3, 0))
            .await
            .unwrap();
        sqlx::query(
            "UPDATE sporos_task SET state = 'failed', projection_generation = 1,
             reason_code = 'fixture', terminal_at = 2 WHERE id = ?",
        )
        .bind([3_u8; 16].as_slice())
        .execute(storage.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO sporos_outbox
             (task_id, task_key, orchestration_name, orchestration_version, instance_id,
              input_json, visible_at, start_delivery_attempt_count)
             VALUES (?, ?, ?, ?, ?, '{}', 2, 0)",
        )
        .bind([3_u8; 16].as_slice())
        .bind([9_u8; 32].as_slice())
        .bind(ORCHESTRATION_NAME)
        .bind(ORCHESTRATION_VERSION)
        .bind("cancel-task:fixture")
        .execute(storage.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO sporos_task_event
             (task_id, sequence, state, reason_code, created_at)
             VALUES (?, 1, 'failed', 'fixture', 2)",
        )
        .bind([3_u8; 16].as_slice())
        .execute(storage.pool())
        .await
        .unwrap();

        let accepted = retry(&storage, [3; 16], 3).await.unwrap();

        assert!(accepted.instance_id.ends_with(":retry:2"));
        let row = sqlx::query(
            "SELECT state, projection_generation, terminal_at, observed_retry_count
             FROM sporos_task WHERE id = ?",
        )
        .bind([3_u8; 16].as_slice())
        .fetch_one(storage.pool())
        .await
        .unwrap();
        assert_eq!(row.get::<String, _>("state"), "queued");
        assert_eq!(row.get::<i64, _>("projection_generation"), 2);
        assert_eq!(row.get::<Option<i64>, _>("terminal_at"), None);
        assert_eq!(row.get::<i64, _>("observed_retry_count"), 1);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM sporos_task_event WHERE task_id = ?",
            )
            .bind([3_u8; 16].as_slice())
            .fetch_one(storage.pool())
            .await
            .unwrap(),
            3
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM sporos_outbox WHERE task_id = ?")
                .bind([3_u8; 16].as_slice())
                .fetch_one(storage.pool())
                .await
                .unwrap(),
            3
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT orchestration_name FROM sporos_outbox WHERE instance_id = ?",
            )
            .bind(&accepted.instance_id)
            .fetch_one(storage.pool())
            .await
            .unwrap(),
            FAKE_TASK_NAME
        );
    }

    #[tokio::test]
    async fn cancellation_is_reconciled_from_the_authoritative_workflow() {
        let directory = TempDir::new().unwrap();
        let storage = Arc::new(open(&directory).await);
        storage.accept_task(&task("fake", 7, 60_000)).await.unwrap();
        let provider = storage.duroxide_provider();
        let client = Client::new(provider.clone());
        let (activities, orchestrations) = registries(Arc::clone(&storage), None, None, None);
        let runtime =
            duroxide::runtime::Runtime::start_with_store(provider, activities, orchestrations)
                .await;
        OutboxDispatcher::new(&storage, client.clone(), 10)
            .run_once(2)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let accepted = request_cancel(&storage, [7; 16], 3).await.unwrap();
        assert!(!accepted.duplicate);
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT state FROM sporos_task WHERE id = ?")
                .bind([7_u8; 16].as_slice())
                .fetch_one(storage.pool())
                .await
                .unwrap(),
            "cancellation_requested"
        );
        OutboxDispatcher::new(&storage, client.clone(), 10)
            .run_once(4)
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let state =
                sqlx::query_scalar::<_, String>("SELECT state FROM sporos_task WHERE id = ?")
                    .bind([7_u8; 16].as_slice())
                    .fetch_one(storage.pool())
                    .await
                    .unwrap();
            if state == "cancelled" {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "cancellation did not reconcile"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(matches!(
            client
                .get_orchestration_status(&accepted.target_instance)
                .await
                .unwrap(),
            OrchestrationStatus::Failed { .. }
        ));
        runtime.shutdown(None).await;
    }

    fn task(kind: &str, marker: u8, delay_ms: u64) -> NewTask {
        NewTask {
            id: TaskId::from_bytes([marker; 16]),
            key: TaskKey::from_bytes([marker; 32]),
            kind: kind.to_owned(),
            policy: PolicySnapshot {
                id: PolicySnapshotId::from_bytes([marker; 16]),
                config_hash: [marker; 32],
                matcher_version: "test".to_owned(),
                payload_json: "{}".to_owned(),
                created_at: 1,
            },
            orchestration_name: FAKE_TASK_NAME.to_owned(),
            orchestration_version: FAKE_TASK_VERSION.to_owned(),
            instance_id: format!("control-test-{marker}"),
            input_json: serde_json::to_string(&FakeTaskInput {
                task_id: [marker; 16],
                accepted_at_ms: 1,
                delay_ms,
            })
            .unwrap(),
            created_at: 1,
        }
    }

    async fn open(directory: &TempDir) -> Storage {
        Storage::open(
            directory.path().join("sporos.lock"),
            directory.path().join("sporos.db"),
        )
        .await
        .unwrap()
    }
}
