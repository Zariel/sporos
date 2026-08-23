use sporos_model::TaskId;
use sqlx::Row;
use thiserror::Error;

use crate::storage::Storage;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionUpdate {
    pub task_id: TaskId,
    pub expected_generation: u64,
    pub state: String,
    pub reason_code: Option<String>,
    pub execution_id: Option<String>,
    pub observed_retry_count: u64,
    pub detail_json: Option<String>,
    pub occurred_at: i64,
    pub terminal: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionOutcome {
    Applied { generation: u64 },
    AlreadyApplied { generation: u64 },
    Stale { actual_generation: u64 },
    Terminal { generation: u64 },
    Missing,
}

impl Storage {
    pub async fn project_task(
        &self,
        update: &ProjectionUpdate,
    ) -> Result<ProjectionOutcome, ProjectionError> {
        validate(update)?;
        let mut transaction = self.pool().begin().await?;
        let row = sqlx::query(
            "SELECT state, projection_generation, duroxide_execution_id, terminal_at
             FROM sporos_task WHERE id = ?",
        )
        .bind(update.task_id.as_bytes().as_slice())
        .fetch_optional(&mut *transaction)
        .await?;
        let Some(row) = row else {
            return Ok(ProjectionOutcome::Missing);
        };

        let generation = to_u64(row.try_get::<i64, _>("projection_generation")?)?;
        let terminal_at = row.try_get::<Option<i64>, _>("terminal_at")?;
        if generation != update.expected_generation {
            if generation == update.expected_generation.saturating_add(1)
                && event_matches(&mut transaction, update, generation).await?
            {
                return Ok(ProjectionOutcome::AlreadyApplied { generation });
            }
            return Ok(ProjectionOutcome::Stale {
                actual_generation: generation,
            });
        }
        if terminal_at.is_some() {
            return Ok(ProjectionOutcome::Terminal { generation });
        }

        let next_generation = generation
            .checked_add(1)
            .ok_or(ProjectionError::GenerationOverflow)?;
        let next_generation_i64 = to_i64(next_generation)?;
        let retry_count = to_i64(update.observed_retry_count)?;
        let changed = sqlx::query(
            "UPDATE sporos_task SET
                state = ?, projection_generation = ?, duroxide_execution_id = ?,
                reason_code = ?, observed_retry_count = ?, updated_at = ?,
                terminal_at = ?
             WHERE id = ? AND projection_generation = ? AND terminal_at IS NULL",
        )
        .bind(&update.state)
        .bind(next_generation_i64)
        .bind(&update.execution_id)
        .bind(&update.reason_code)
        .bind(retry_count)
        .bind(update.occurred_at)
        .bind(update.terminal.then_some(update.occurred_at))
        .bind(update.task_id.as_bytes().as_slice())
        .bind(to_i64(update.expected_generation)?)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        if changed != 1 {
            transaction.rollback().await?;
            return current_outcome(self, update).await;
        }

        sqlx::query(
            "INSERT INTO sporos_task_event (
                task_id, sequence, state, reason_code, detail_json, created_at
             ) VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(update.task_id.as_bytes().as_slice())
        .bind(next_generation_i64)
        .bind(&update.state)
        .bind(&update.reason_code)
        .bind(&update.detail_json)
        .bind(update.occurred_at)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;

        Ok(ProjectionOutcome::Applied {
            generation: next_generation,
        })
    }
}

async fn current_outcome(
    storage: &Storage,
    update: &ProjectionUpdate,
) -> Result<ProjectionOutcome, ProjectionError> {
    let row =
        sqlx::query("SELECT projection_generation, terminal_at FROM sporos_task WHERE id = ?")
            .bind(update.task_id.as_bytes().as_slice())
            .fetch_optional(storage.pool())
            .await?;
    let Some(row) = row else {
        return Ok(ProjectionOutcome::Missing);
    };
    let generation = to_u64(row.try_get::<i64, _>("projection_generation")?)?;
    if row.try_get::<Option<i64>, _>("terminal_at")?.is_some() {
        Ok(ProjectionOutcome::Terminal { generation })
    } else {
        Ok(ProjectionOutcome::Stale {
            actual_generation: generation,
        })
    }
}

async fn event_matches(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    update: &ProjectionUpdate,
    sequence: u64,
) -> Result<bool, ProjectionError> {
    let event = sqlx::query(
        "SELECT state, reason_code, detail_json FROM sporos_task_event
         WHERE task_id = ? AND sequence = ?",
    )
    .bind(update.task_id.as_bytes().as_slice())
    .bind(to_i64(sequence)?)
    .fetch_optional(&mut **transaction)
    .await?;
    Ok(event.is_some_and(|event| {
        event.try_get::<String, _>("state").ok().as_ref() == Some(&update.state)
            && event
                .try_get::<Option<String>, _>("reason_code")
                .ok()
                .as_ref()
                == Some(&update.reason_code)
            && event
                .try_get::<Option<String>, _>("detail_json")
                .ok()
                .as_ref()
                == Some(&update.detail_json)
    }))
}

fn validate(update: &ProjectionUpdate) -> Result<(), ProjectionError> {
    if update.state.is_empty() {
        return Err(ProjectionError::EmptyState);
    }
    if let Some(detail) = &update.detail_json {
        serde_json::from_str::<serde_json::Value>(detail)
            .map_err(ProjectionError::InvalidDetail)?;
    }
    Ok(())
}

fn to_i64(value: u64) -> Result<i64, ProjectionError> {
    value.try_into().map_err(|_| ProjectionError::ValueOverflow)
}

fn to_u64(value: i64) -> Result<u64, ProjectionError> {
    value
        .try_into()
        .map_err(|_| ProjectionError::CorruptGeneration)
}

#[derive(Debug, Error)]
pub enum ProjectionError {
    #[error("task projection database operation failed")]
    Database(#[from] sqlx::Error),
    #[error("task projection state cannot be empty")]
    EmptyState,
    #[error("task projection detail is not valid JSON")]
    InvalidDetail(#[source] serde_json::Error),
    #[error("task projection generation overflowed")]
    GenerationOverflow,
    #[error("task projection value does not fit SQLite")]
    ValueOverflow,
    #[error("task projection contains a negative generation")]
    CorruptGeneration,
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::durable_ingress::{NewTask, PolicySnapshot};
    use sporos_model::{PolicySnapshotId, TaskKey};

    #[tokio::test]
    async fn applies_once_and_recognises_the_duplicate() {
        let directory = TempDir::new().expect("create temporary directory");
        let storage = open(&directory).await;
        let update = update(0, "running", false);

        assert_eq!(
            storage.project_task(&update).await.expect("project task"),
            ProjectionOutcome::Applied { generation: 1 }
        );
        assert_eq!(
            storage
                .project_task(&update)
                .await
                .expect("repeat projection"),
            ProjectionOutcome::AlreadyApplied { generation: 1 }
        );
        assert_eq!(event_count(&storage).await, 2);
    }

    #[tokio::test]
    async fn rejects_stale_and_terminal_regressions() {
        let directory = TempDir::new().expect("create temporary directory");
        let storage = open(&directory).await;
        storage
            .project_task(&update(0, "running", false))
            .await
            .expect("project running task");

        assert_eq!(
            storage
                .project_task(&update(0, "searching", false))
                .await
                .expect("reject stale projection"),
            ProjectionOutcome::Stale {
                actual_generation: 1
            }
        );
        assert_eq!(
            storage
                .project_task(&update(1, "dry_run_complete", true))
                .await
                .expect("project terminal task"),
            ProjectionOutcome::Applied { generation: 2 }
        );
        assert_eq!(
            storage
                .project_task(&update(2, "running", false))
                .await
                .expect("reject terminal regression"),
            ProjectionOutcome::Terminal { generation: 2 }
        );
        assert_eq!(event_count(&storage).await, 3);
    }

    #[tokio::test]
    async fn converges_concurrent_updates() {
        let directory = TempDir::new().expect("create temporary directory");
        let storage = open(&directory).await;
        let first = update(0, "running", false);
        let second = update(0, "searching", false);

        let (first, second) =
            tokio::join!(storage.project_task(&first), storage.project_task(&second));
        let outcomes = [first.expect("first update"), second.expect("second update")];
        assert!(outcomes.contains(&ProjectionOutcome::Applied { generation: 1 }));
        assert!(outcomes.contains(&ProjectionOutcome::Stale {
            actual_generation: 1
        }));
        assert_eq!(event_count(&storage).await, 2);
    }

    #[tokio::test]
    async fn reports_a_missing_projection_without_creating_work() {
        let directory = TempDir::new().expect("create temporary directory");
        let storage = crate::storage::Storage::open(
            directory.path().join("sporos.lock"),
            directory.path().join("sporos.db"),
        )
        .await
        .expect("open storage");

        assert_eq!(
            storage
                .project_task(&update(0, "running", false))
                .await
                .expect("report missing projection"),
            ProjectionOutcome::Missing
        );
        assert_eq!(event_count(&storage).await, 0);
    }

    fn update(generation: u64, state: &str, terminal: bool) -> ProjectionUpdate {
        ProjectionUpdate {
            task_id: TaskId::from_bytes([1; 16]),
            expected_generation: generation,
            state: state.to_owned(),
            reason_code: None,
            execution_id: Some("execution-1".to_owned()),
            observed_retry_count: 0,
            detail_json: Some("{}".to_owned()),
            occurred_at: generation as i64 + 2,
            terminal,
        }
    }

    async fn open(directory: &TempDir) -> Storage {
        let storage = Storage::open(
            directory.path().join("sporos.lock"),
            directory.path().join("sporos.db"),
        )
        .await
        .expect("open storage");
        storage
            .accept_task(&NewTask {
                id: TaskId::from_bytes([1; 16]),
                key: TaskKey::from_bytes([1; 32]),
                kind: "fake".to_owned(),
                policy: PolicySnapshot {
                    id: PolicySnapshotId::from_bytes([1; 16]),
                    config_hash: [1; 32],
                    matcher_version: "phase1".to_owned(),
                    payload_json: "{}".to_owned(),
                    created_at: 1,
                },
                orchestration_name: "FakeTask".to_owned(),
                orchestration_version: "1".to_owned(),
                instance_id: "fake-v1:1".to_owned(),
                input_json: "{}".to_owned(),
                created_at: 1,
            })
            .await
            .expect("accept task");
        storage
    }

    async fn event_count(storage: &Storage) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM sporos_task_event")
            .fetch_one(storage.pool())
            .await
            .expect("count task events")
    }
}
