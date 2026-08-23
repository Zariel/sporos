use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{Sqlite, Transaction};
use thiserror::Error;

use crate::storage::Storage;

pub const ORCHESTRATION_NAME: &str = "QbittorrentCompletion";
pub const ORCHESTRATION_VERSION: &str = "1.0.0";
pub const PROJECT_ACTIVITY: &str = "ProjectQbittorrentCompletion";
const POLICY_PAYLOAD: &str = r#"{"kind":"qbittorrent_completion","version":1}"#;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CompletionInput {
    pub operation_id: [u8; 16],
    pub task_id: [u8; 16],
    pub source_id: [u8; 16],
    pub completed_at: i64,
    pub observed_at: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AcceptedCompletion {
    pub operation_id: [u8; 16],
    pub task_id: [u8; 16],
    pub duplicate: bool,
}

pub async fn accept(
    transaction: &mut Transaction<'_, Sqlite>,
    source_id: [u8; 16],
    completed_at: i64,
    observed_at: i64,
) -> Result<AcceptedCompletion, CompletionError> {
    let operation_id = id16(b"qbit-completion-operation", &source_id, completed_at);
    let task_id = id16(b"qbit-completion-task", &source_id, completed_at);
    let task_key = id32(b"qbit-completion-key", &source_id, completed_at);
    let policy_id = id16(b"qbit-completion-policy", &[0; 16], 1);
    let policy_hash: [u8; 32] = Sha256::digest(POLICY_PAYLOAD.as_bytes()).into();
    let instance_id = format!("qbit-completion:{}", hex(&operation_id));
    let input = CompletionInput {
        operation_id,
        task_id,
        source_id,
        completed_at,
        observed_at,
    };
    let input_json = serde_json::to_string(&input)?;
    let request_json = serde_json::json!({
        "sourceId": hex(&source_id),
        "completedAt": completed_at,
    })
    .to_string();

    sqlx::query(
        "INSERT INTO sporos_policy_snapshot (
            id, config_hash, matcher_version, payload_json, created_at
         ) VALUES (?, ?, 'not_applicable', ?, ?)
         ON CONFLICT(id) DO NOTHING",
    )
    .bind(policy_id.as_slice())
    .bind(policy_hash.as_slice())
    .bind(POLICY_PAYLOAD)
    .bind(0_i64)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        "INSERT INTO sporos_operation (
            id, kind, state, duroxide_instance_id, request_json,
            produced_tasks, completed_tasks, failed_tasks, created_at, updated_at
         ) VALUES (?, 'qbittorrent_completion', 'queued', ?, ?, 0, 0, 0, ?, ?)
         ON CONFLICT(id) DO NOTHING",
    )
    .bind(operation_id.as_slice())
    .bind(&instance_id)
    .bind(request_json)
    .bind(observed_at)
    .bind(observed_at)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        "INSERT INTO sporos_task (
            id, kind, state, projection_generation, operation_id,
            duroxide_instance_id, policy_snapshot_id, observed_retry_count,
            created_at, updated_at
         ) VALUES (?, 'qbittorrent_completion', 'queued', 0, ?, ?, ?, 0, ?, ?)
         ON CONFLICT(id) DO NOTHING",
    )
    .bind(task_id.as_slice())
    .bind(operation_id.as_slice())
    .bind(&instance_id)
    .bind(policy_id.as_slice())
    .bind(observed_at)
    .bind(observed_at)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        "INSERT INTO sporos_task_event (task_id, sequence, state, created_at)
         VALUES (?, 0, 'queued', ?) ON CONFLICT(task_id, sequence) DO NOTHING",
    )
    .bind(task_id.as_slice())
    .bind(observed_at)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        "INSERT INTO sporos_outbox (
            task_id, task_key, orchestration_name, orchestration_version,
            instance_id, input_json, visible_at, start_delivery_attempt_count
         ) VALUES (?, ?, ?, ?, ?, ?, ?, 0)
         ON CONFLICT(task_key) DO NOTHING",
    )
    .bind(task_id.as_slice())
    .bind(task_key.as_slice())
    .bind(ORCHESTRATION_NAME)
    .bind(ORCHESTRATION_VERSION)
    .bind(&instance_id)
    .bind(input_json)
    .bind(observed_at)
    .execute(&mut **transaction)
    .await?;
    let inserted = sqlx::query(
        "INSERT INTO sporos_qbit_completion (
            source_id, completed_at, operation_id, task_id, created_at
         ) VALUES (?, ?, ?, ?, ?)
         ON CONFLICT(source_id, completed_at) DO NOTHING",
    )
    .bind(source_id.as_slice())
    .bind(completed_at)
    .bind(operation_id.as_slice())
    .bind(task_id.as_slice())
    .bind(observed_at)
    .execute(&mut **transaction)
    .await?
    .rows_affected()
        == 1;

    Ok(AcceptedCompletion {
        operation_id,
        task_id,
        duplicate: !inserted,
    })
}

impl Storage {
    pub async fn project_completion(&self, input: &CompletionInput) -> Result<(), CompletionError> {
        let mut transaction = self.pool().begin().await?;
        sqlx::query(
            "UPDATE sporos_task
             SET state = 'completed', projection_generation = 1,
                 updated_at = ?, terminal_at = ?
             WHERE id = ? AND projection_generation = 0",
        )
        .bind(input.observed_at)
        .bind(input.observed_at)
        .bind(input.task_id.as_slice())
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "INSERT INTO sporos_task_event (
                task_id, sequence, state, detail_json, created_at
             ) VALUES (?, 1, 'completed', ?, ?)
             ON CONFLICT(task_id, sequence) DO NOTHING",
        )
        .bind(input.task_id.as_slice())
        .bind(
            serde_json::json!({
                "sourceId": hex(&input.source_id),
                "completedAt": input.completed_at,
            })
            .to_string(),
        )
        .bind(input.observed_at)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE sporos_operation
             SET state = 'completed', updated_at = ? WHERE id = ?",
        )
        .bind(input.observed_at)
        .bind(input.operation_id.as_slice())
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }
}

fn id16(namespace: &[u8], source_id: &[u8; 16], completed_at: i64) -> [u8; 16] {
    let digest = identity(namespace, source_id, completed_at);
    let mut id = [0_u8; 16];
    id.copy_from_slice(&digest[..16]);
    id
}

fn id32(namespace: &[u8], source_id: &[u8; 16], completed_at: i64) -> [u8; 32] {
    identity(namespace, source_id, completed_at)
}

fn identity(namespace: &[u8], source_id: &[u8; 16], completed_at: i64) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update((namespace.len() as u64).to_be_bytes());
    hash.update(namespace);
    hash.update(source_id);
    hash.update(completed_at.to_be_bytes());
    hash.finalize().into()
}

fn hex(value: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(value.len() * 2);
    for byte in value {
        output.push(DIGITS[(byte >> 4) as usize] as char);
        output.push(DIGITS[(byte & 0xf) as usize] as char);
    }
    output
}

#[derive(Debug, Error)]
pub enum CompletionError {
    #[error("completion database operation failed")]
    Database(#[from] sqlx::Error),
    #[error("completion payload serialization failed")]
    Serialize(#[from] serde_json::Error),
}
