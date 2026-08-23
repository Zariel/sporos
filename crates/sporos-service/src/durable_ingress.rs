use sporos_model::{PolicySnapshotId, TaskId, TaskKey};
use sqlx::Row;
use thiserror::Error;

use crate::storage::Storage;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicySnapshot {
    pub id: PolicySnapshotId,
    pub config_hash: [u8; 32],
    pub matcher_version: String,
    pub payload_json: String,
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewTask {
    pub id: TaskId,
    pub key: TaskKey,
    pub kind: String,
    pub policy: PolicySnapshot,
    pub orchestration_name: String,
    pub orchestration_version: String,
    pub instance_id: String,
    pub input_json: String,
    pub created_at: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AcceptedTask {
    pub id: TaskId,
    pub duplicate: bool,
}

impl Storage {
    pub async fn accept_task(&self, task: &NewTask) -> Result<AcceptedTask, DurableIngressError> {
        let mut transaction = self.pool().begin().await?;

        sqlx::query(
            "INSERT INTO sporos_policy_snapshot (
                id, config_hash, matcher_version, payload_json, created_at
             ) VALUES (?, ?, ?, ?, ?)
             ON CONFLICT(id) DO NOTHING",
        )
        .bind(task.policy.id.as_bytes().as_slice())
        .bind(task.policy.config_hash.as_slice())
        .bind(&task.policy.matcher_version)
        .bind(&task.policy.payload_json)
        .bind(task.policy.created_at)
        .execute(&mut *transaction)
        .await?;
        verify_policy(&mut transaction, &task.policy).await?;

        sqlx::query(
            "INSERT INTO sporos_task (
                id, kind, state, generation, duroxide_instance_id,
                policy_snapshot_id, attempt_count, created_at, updated_at
             ) VALUES (?, ?, 'queued', 0, ?, ?, 0, ?, ?)
             ON CONFLICT(id) DO NOTHING",
        )
        .bind(task.id.as_bytes().as_slice())
        .bind(&task.kind)
        .bind(&task.instance_id)
        .bind(task.policy.id.as_bytes().as_slice())
        .bind(task.created_at)
        .bind(task.created_at)
        .execute(&mut *transaction)
        .await?;
        verify_task(&mut transaction, task).await?;

        sqlx::query(
            "INSERT INTO sporos_task_event (
                task_id, sequence, state, created_at
             ) VALUES (?, 0, 'queued', ?)
             ON CONFLICT(task_id, sequence) DO NOTHING",
        )
        .bind(task.id.as_bytes().as_slice())
        .bind(task.created_at)
        .execute(&mut *transaction)
        .await?;

        let inserted = sqlx::query(
            "INSERT INTO sporos_outbox (
                task_id, task_key, orchestration_name, orchestration_version,
                instance_id, input_json, visible_at, attempt_count
             ) VALUES (?, ?, ?, ?, ?, ?, ?, 0)
             ON CONFLICT(task_key) DO NOTHING",
        )
        .bind(task.id.as_bytes().as_slice())
        .bind(task.key.as_bytes().as_slice())
        .bind(&task.orchestration_name)
        .bind(&task.orchestration_version)
        .bind(&task.instance_id)
        .bind(&task.input_json)
        .bind(task.created_at)
        .execute(&mut *transaction)
        .await?
        .rows_affected()
            == 1;
        verify_outbox(&mut transaction, task).await?;

        transaction.commit().await?;
        Ok(AcceptedTask {
            id: task.id,
            duplicate: !inserted,
        })
    }
}

async fn verify_policy(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    expected: &PolicySnapshot,
) -> Result<(), DurableIngressError> {
    let row = sqlx::query(
        "SELECT config_hash, matcher_version, payload_json, created_at
         FROM sporos_policy_snapshot WHERE id = ?",
    )
    .bind(expected.id.as_bytes().as_slice())
    .fetch_one(&mut **transaction)
    .await?;

    let matches = row.try_get::<Vec<u8>, _>("config_hash")?.as_slice() == expected.config_hash
        && row.try_get::<String, _>("matcher_version")? == expected.matcher_version
        && row.try_get::<String, _>("payload_json")? == expected.payload_json
        && row.try_get::<i64, _>("created_at")? == expected.created_at;
    if matches {
        Ok(())
    } else {
        Err(DurableIngressError::PolicyIdentityCollision)
    }
}

async fn verify_task(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    expected: &NewTask,
) -> Result<(), DurableIngressError> {
    let row = sqlx::query(
        "SELECT kind, duroxide_instance_id, policy_snapshot_id, created_at
         FROM sporos_task WHERE id = ?",
    )
    .bind(expected.id.as_bytes().as_slice())
    .fetch_one(&mut **transaction)
    .await?;

    let matches = row.try_get::<String, _>("kind")? == expected.kind
        && row.try_get::<String, _>("duroxide_instance_id")? == expected.instance_id
        && row.try_get::<Vec<u8>, _>("policy_snapshot_id")?.as_slice()
            == expected.policy.id.as_bytes()
        && row.try_get::<i64, _>("created_at")? == expected.created_at;
    if matches {
        Ok(())
    } else {
        Err(DurableIngressError::TaskIdentityCollision)
    }
}

async fn verify_outbox(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    expected: &NewTask,
) -> Result<(), DurableIngressError> {
    let row = sqlx::query(
        "SELECT task_id, orchestration_name, orchestration_version,
                instance_id, input_json, visible_at
         FROM sporos_outbox WHERE task_key = ?",
    )
    .bind(expected.key.as_bytes().as_slice())
    .fetch_one(&mut **transaction)
    .await?;

    let stored_task_id = row.try_get::<Vec<u8>, _>("task_id")?;
    if stored_task_id.as_slice() != expected.id.as_bytes() {
        return Err(DurableIngressError::TaskKeyCollision);
    }

    let matches = row.try_get::<String, _>("orchestration_name")? == expected.orchestration_name
        && row.try_get::<String, _>("orchestration_version")? == expected.orchestration_version
        && row.try_get::<String, _>("instance_id")? == expected.instance_id
        && row.try_get::<String, _>("input_json")? == expected.input_json
        && row.try_get::<i64, _>("visible_at")? == expected.created_at;
    if matches {
        Ok(())
    } else {
        Err(DurableIngressError::OutboxIdentityCollision)
    }
}

#[derive(Debug, Error)]
pub enum DurableIngressError {
    #[error("durable-ingress database operation failed")]
    Database(#[from] sqlx::Error),
    #[error("policy snapshot ID refers to different content")]
    PolicyIdentityCollision,
    #[error("task ID refers to different content")]
    TaskIdentityCollision,
    #[error("task idempotency key refers to another task")]
    TaskKeyCollision,
    #[error("outbox identity refers to different content")]
    OutboxIdentityCollision,
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[tokio::test]
    async fn commits_the_ingress_unit() {
        let directory = TempDir::new().expect("create temporary directory");
        let storage = open_in(&directory).await;
        let task = task(1);

        assert_eq!(
            storage.accept_task(&task).await.expect("accept task"),
            AcceptedTask {
                id: task.id,
                duplicate: false,
            }
        );
        assert_counts(&storage, [1, 1, 1, 1]).await;
    }

    #[tokio::test]
    async fn returns_the_existing_task_for_a_duplicate() {
        let directory = TempDir::new().expect("create temporary directory");
        let storage = open_in(&directory).await;
        let task = task(1);
        storage.accept_task(&task).await.expect("accept task");

        assert_eq!(
            storage.accept_task(&task).await.expect("accept duplicate"),
            AcceptedTask {
                id: task.id,
                duplicate: true,
            }
        );
        assert_counts(&storage, [1, 1, 1, 1]).await;
    }

    #[tokio::test]
    async fn converges_concurrent_duplicates() {
        let directory = TempDir::new().expect("create temporary directory");
        let storage = open_in(&directory).await;
        let task = task(1);

        let (first, second) = tokio::join!(storage.accept_task(&task), storage.accept_task(&task));
        let duplicates = [
            first.expect("accept first task").duplicate,
            second.expect("accept second task").duplicate,
        ];

        assert!(duplicates.contains(&false));
        assert!(duplicates.contains(&true));
        assert_counts(&storage, [1, 1, 1, 1]).await;
    }

    #[tokio::test]
    async fn rejects_an_idempotency_collision() {
        let directory = TempDir::new().expect("create temporary directory");
        let storage = open_in(&directory).await;
        let first = task(1);
        storage.accept_task(&first).await.expect("accept task");
        let mut colliding = task(2);
        colliding.key = first.key;

        let result = storage.accept_task(&colliding).await;
        assert!(matches!(result, Err(DurableIngressError::TaskKeyCollision)));
        assert_counts(&storage, [1, 1, 1, 1]).await;
    }

    #[tokio::test]
    async fn rejects_a_policy_identity_collision() {
        let directory = TempDir::new().expect("create temporary directory");
        let storage = open_in(&directory).await;
        let first = task(1);
        storage.accept_task(&first).await.expect("accept task");
        let mut colliding = task(2);
        colliding.policy.id = first.policy.id;

        let result = storage.accept_task(&colliding).await;
        assert!(matches!(
            result,
            Err(DurableIngressError::PolicyIdentityCollision)
        ));
        assert_counts(&storage, [1, 1, 1, 1]).await;
    }

    #[tokio::test]
    async fn rejects_an_outbox_identity_collision() {
        let directory = TempDir::new().expect("create temporary directory");
        let storage = open_in(&directory).await;
        let first = task(1);
        storage.accept_task(&first).await.expect("accept task");
        let mut colliding = first.clone();
        colliding.input_json = r#"{"changed":true}"#.to_owned();

        let result = storage.accept_task(&colliding).await;
        assert!(matches!(
            result,
            Err(DurableIngressError::OutboxIdentityCollision)
        ));
        assert_counts(&storage, [1, 1, 1, 1]).await;
    }

    #[tokio::test]
    async fn rolls_back_a_late_failure() {
        let directory = TempDir::new().expect("create temporary directory");
        let storage = open_in(&directory).await;
        let mut task = task(1);
        task.input_json = "not json".to_owned();

        let result = storage.accept_task(&task).await;
        assert!(matches!(result, Err(DurableIngressError::Database(_))));
        assert_counts(&storage, [0, 0, 0, 0]).await;
    }

    #[tokio::test]
    async fn survives_reopen() {
        let directory = TempDir::new().expect("create temporary directory");
        let storage = open_in(&directory).await;
        storage.accept_task(&task(1)).await.expect("accept task");
        drop(storage);

        let reopened = open_in(&directory).await;
        assert_counts(&reopened, [1, 1, 1, 1]).await;
    }

    fn task(marker: u8) -> NewTask {
        NewTask {
            id: TaskId::from_bytes([marker; 16]),
            key: TaskKey::from_bytes([marker; 32]),
            kind: "process_candidate".to_owned(),
            policy: PolicySnapshot {
                id: PolicySnapshotId::from_bytes([marker; 16]),
                config_hash: [marker; 32],
                matcher_version: "phase0".to_owned(),
                payload_json: "{}".to_owned(),
                created_at: i64::from(marker),
            },
            orchestration_name: "ProcessCandidate".to_owned(),
            orchestration_version: "1".to_owned(),
            instance_id: format!("candidate-v1:{marker}"),
            input_json: "{}".to_owned(),
            created_at: i64::from(marker),
        }
    }

    async fn open_in(directory: &TempDir) -> Storage {
        Storage::open(
            directory.path().join("sporos.lock"),
            directory.path().join("sporos.db"),
        )
        .await
        .expect("open storage")
    }

    async fn assert_counts(storage: &Storage, expected: [i64; 4]) {
        let actual = sqlx::query_as::<_, (i64, i64, i64, i64)>(
            "SELECT
                (SELECT count(*) FROM sporos_policy_snapshot),
                (SELECT count(*) FROM sporos_task),
                (SELECT count(*) FROM sporos_task_event),
                (SELECT count(*) FROM sporos_outbox)",
        )
        .fetch_one(storage.pool())
        .await
        .expect("count durable ingress rows");
        assert_eq!([actual.0, actual.1, actual.2, actual.3], expected);
    }
}
