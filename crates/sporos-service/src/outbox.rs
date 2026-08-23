use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use duroxide::providers::WorkItem;
use duroxide::{Client, Event, EventKind};
use sqlx::Row;
use thiserror::Error;

use crate::storage::Storage;

static LEASE_SEQUENCE: AtomicU64 = AtomicU64::new(1);
const INITIAL_BACKOFF_MS: i64 = 1_000;
const MAX_BACKOFF_MS: i64 = 5 * 60 * 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DispatchReport {
    pub claimed: usize,
    pub dispatched: usize,
    pub retrying: usize,
    pub permanently_failed: usize,
}

pub struct OutboxDispatcher<'a> {
    storage: &'a Storage,
    client: Client,
    batch_size: usize,
    lease_duration: Duration,
}

impl<'a> OutboxDispatcher<'a> {
    pub fn new(storage: &'a Storage, client: Client, batch_size: usize) -> Self {
        Self {
            storage,
            client,
            batch_size,
            lease_duration: Duration::from_secs(60),
        }
    }

    pub async fn run_once(&self, now_ms: i64) -> Result<DispatchReport, DispatchError> {
        if self.batch_size == 0 {
            return Err(DispatchError::ZeroBatchSize);
        }
        let lease_ms = i64::try_from(self.lease_duration.as_millis())
            .map_err(|_| DispatchError::TimeOverflow)?;
        let lease_until = now_ms
            .checked_add(lease_ms)
            .ok_or(DispatchError::TimeOverflow)?;
        let token = lease_token(now_ms);
        let starts = claim(self.storage, now_ms, lease_until, token, self.batch_size).await?;
        let mut report = DispatchReport {
            claimed: starts.len(),
            ..DispatchReport::default()
        };

        for start in starts {
            match self
                .client
                .start_orchestration_versioned(
                    &start.instance_id,
                    &start.orchestration_name,
                    &start.orchestration_version,
                    &start.input_json,
                )
                .await
            {
                Ok(()) => {
                    mark_dispatched(self.storage, start.id, token, now_ms).await?;
                    report.dispatched += 1;
                }
                Err(error) => match reconcile(self.storage, &start).await? {
                    ExistingStart::Same => {
                        mark_dispatched(self.storage, start.id, token, now_ms).await?;
                        report.dispatched += 1;
                    }
                    ExistingStart::Collision => {
                        mark_permanent(
                            self.storage,
                            start.id,
                            token,
                            now_ms,
                            "duroxide_identity_collision",
                        )
                        .await?;
                        report.permanently_failed += 1;
                    }
                    ExistingStart::Missing if error.is_retryable() => {
                        mark_retry(self.storage, &start, token, now_ms).await?;
                        report.retrying += 1;
                    }
                    ExistingStart::Missing => {
                        mark_permanent(
                            self.storage,
                            start.id,
                            token,
                            now_ms,
                            "duroxide_start_rejected",
                        )
                        .await?;
                        report.permanently_failed += 1;
                    }
                },
            }
        }
        Ok(report)
    }
}

#[derive(Debug)]
struct OutboxStart {
    id: i64,
    orchestration_name: String,
    orchestration_version: String,
    instance_id: String,
    input_json: String,
    attempt_count: u64,
}

async fn claim(
    storage: &Storage,
    now_ms: i64,
    lease_until: i64,
    token: [u8; 16],
    batch_size: usize,
) -> Result<Vec<OutboxStart>, DispatchError> {
    let limit = i64::try_from(batch_size).map_err(|_| DispatchError::BatchTooLarge)?;
    let mut transaction = storage.pool().begin().await?;
    let rows = sqlx::query(
        "SELECT id, orchestration_name, orchestration_version, instance_id,
                input_json, start_delivery_attempt_count
         FROM sporos_outbox
         WHERE dispatched_at IS NULL AND permanent_failure_at IS NULL
           AND visible_at <= ? AND (lease_until IS NULL OR lease_until <= ?)
         ORDER BY visible_at, id LIMIT ?",
    )
    .bind(now_ms)
    .bind(now_ms)
    .bind(limit)
    .fetch_all(&mut *transaction)
    .await?;

    let mut starts = Vec::with_capacity(rows.len());
    for row in rows {
        let id = row.try_get::<i64, _>("id")?;
        let changed = sqlx::query(
            "UPDATE sporos_outbox SET lease_token = ?, lease_until = ?
             WHERE id = ? AND dispatched_at IS NULL AND permanent_failure_at IS NULL
               AND (lease_until IS NULL OR lease_until <= ?)",
        )
        .bind(token.as_slice())
        .bind(lease_until)
        .bind(id)
        .bind(now_ms)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        if changed == 1 {
            starts.push(OutboxStart {
                id,
                orchestration_name: row.try_get("orchestration_name")?,
                orchestration_version: row.try_get("orchestration_version")?,
                instance_id: row.try_get("instance_id")?,
                input_json: row.try_get("input_json")?,
                attempt_count: row
                    .try_get::<i64, _>("start_delivery_attempt_count")?
                    .try_into()
                    .map_err(|_| DispatchError::CorruptAttemptCount)?,
            });
        }
    }
    transaction.commit().await?;
    Ok(starts)
}

async fn mark_dispatched(
    storage: &Storage,
    id: i64,
    token: [u8; 16],
    now_ms: i64,
) -> Result<(), DispatchError> {
    let changed = sqlx::query(
        "UPDATE sporos_outbox SET
            dispatched_at = ?, last_error = NULL,
            lease_token = NULL, lease_until = NULL
         WHERE id = ? AND lease_token = ?",
    )
    .bind(now_ms)
    .bind(id)
    .bind(token.as_slice())
    .execute(storage.pool())
    .await?
    .rows_affected();
    require_lease(changed, id)
}

async fn mark_permanent(
    storage: &Storage,
    id: i64,
    token: [u8; 16],
    now_ms: i64,
    error: &'static str,
) -> Result<(), DispatchError> {
    let changed = sqlx::query(
        "UPDATE sporos_outbox SET
            permanent_failure_at = ?, last_error = ?,
            lease_token = NULL, lease_until = NULL
         WHERE id = ? AND lease_token = ?",
    )
    .bind(now_ms)
    .bind(error)
    .bind(id)
    .bind(token.as_slice())
    .execute(storage.pool())
    .await?
    .rows_affected();
    require_lease(changed, id)
}

async fn mark_retry(
    storage: &Storage,
    start: &OutboxStart,
    token: [u8; 16],
    now_ms: i64,
) -> Result<(), DispatchError> {
    let attempt = start
        .attempt_count
        .checked_add(1)
        .ok_or(DispatchError::AttemptOverflow)?;
    let delay = retry_delay(start.id, attempt);
    let visible_at = now_ms
        .checked_add(delay)
        .ok_or(DispatchError::TimeOverflow)?;
    let changed = sqlx::query(
        "UPDATE sporos_outbox SET
            start_delivery_attempt_count = ?, visible_at = ?,
            lease_token = NULL, lease_until = NULL,
            last_error = 'duroxide_start_retryable'
         WHERE id = ? AND lease_token = ?",
    )
    .bind(i64::try_from(attempt).map_err(|_| DispatchError::AttemptOverflow)?)
    .bind(visible_at)
    .bind(start.id)
    .bind(token.as_slice())
    .execute(storage.pool())
    .await?
    .rows_affected();
    require_lease(changed, start.id)
}

fn require_lease(changed: u64, id: i64) -> Result<(), DispatchError> {
    if changed == 1 {
        Ok(())
    } else {
        Err(DispatchError::LostLease(id))
    }
}

fn retry_delay(id: i64, attempt: u64) -> i64 {
    let exponent = attempt.saturating_sub(1).min(18) as u32;
    let base = INITIAL_BACKOFF_MS
        .saturating_mul(1_i64.checked_shl(exponent).unwrap_or(i64::MAX))
        .min(MAX_BACKOFF_MS);
    let jitter_window = (base / 5).max(1);
    let mixed = id.unsigned_abs() ^ attempt.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    base.saturating_add((mixed % jitter_window as u64) as i64)
        .min(MAX_BACKOFF_MS)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExistingStart {
    Same,
    Collision,
    Missing,
}

async fn reconcile(
    storage: &Storage,
    expected: &OutboxStart,
) -> Result<ExistingStart, DispatchError> {
    let queued = sqlx::query_scalar::<_, String>(
        "SELECT work_item FROM orchestrator_queue WHERE instance_id = ? ORDER BY id",
    )
    .bind(&expected.instance_id)
    .fetch_all(storage.pool())
    .await?;
    for item in queued {
        let item: WorkItem =
            serde_json::from_str(&item).map_err(DispatchError::CorruptDuroxideWork)?;
        if let WorkItem::StartOrchestration {
            orchestration,
            input,
            version,
            parent_instance,
            ..
        } = item
        {
            return Ok(
                if orchestration == expected.orchestration_name
                    && version.as_deref() == Some(expected.orchestration_version.as_str())
                    && input == expected.input_json
                    && parent_instance.is_none()
                {
                    ExistingStart::Same
                } else {
                    ExistingStart::Collision
                },
            );
        }
    }

    let history = sqlx::query_scalar::<_, String>(
        "SELECT event_data FROM history
         WHERE instance_id = ? AND execution_id = 1 AND event_id = 1",
    )
    .bind(&expected.instance_id)
    .fetch_optional(storage.pool())
    .await?;
    let Some(history) = history else {
        return Ok(ExistingStart::Missing);
    };
    let event: Event =
        serde_json::from_str(&history).map_err(DispatchError::CorruptDuroxideHistory)?;
    let EventKind::OrchestrationStarted {
        name,
        version,
        input,
        parent_instance,
        ..
    } = event.kind
    else {
        return Ok(ExistingStart::Collision);
    };
    Ok(
        if name == expected.orchestration_name
            && version == expected.orchestration_version
            && input == expected.input_json
            && parent_instance.is_none()
        {
            ExistingStart::Same
        } else {
            ExistingStart::Collision
        },
    )
}

fn lease_token(now_ms: i64) -> [u8; 16] {
    let sequence = LEASE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let mut token = [0; 16];
    token[..8].copy_from_slice(&now_ms.to_be_bytes());
    token[8..].copy_from_slice(&sequence.to_be_bytes());
    token
}

#[derive(Debug, Error)]
pub enum DispatchError {
    #[error("outbox database operation failed")]
    Database(#[from] sqlx::Error),
    #[error("outbox batch size must be greater than zero")]
    ZeroBatchSize,
    #[error("outbox batch size does not fit SQLite")]
    BatchTooLarge,
    #[error("outbox time value overflowed")]
    TimeOverflow,
    #[error("outbox delivery attempt overflowed")]
    AttemptOverflow,
    #[error("outbox contains a negative delivery attempt count")]
    CorruptAttemptCount,
    #[error("outbox row {0} lost its delivery lease")]
    LostLease(i64),
    #[error("Duroxide queued work is unreadable")]
    CorruptDuroxideWork(#[source] serde_json::Error),
    #[error("Duroxide history is unreadable")]
    CorruptDuroxideHistory(#[source] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::durable_ingress::{NewTask, PolicySnapshot};
    use sporos_model::{PolicySnapshotId, TaskId, TaskKey};

    #[tokio::test]
    async fn dispatches_a_bounded_start_batch() {
        let directory = TempDir::new().expect("create temporary directory");
        let storage = open(&directory).await;
        for marker in 1..=3 {
            storage
                .accept_task(&task(marker))
                .await
                .expect("accept task");
        }
        let dispatcher = dispatcher(&storage, 2);

        assert_eq!(
            dispatcher.run_once(10).await.expect("dispatch batch"),
            DispatchReport {
                claimed: 2,
                dispatched: 2,
                retrying: 0,
                permanently_failed: 0,
            }
        );
        assert_eq!(outbox_count(&storage, "dispatched_at IS NOT NULL").await, 2);
        assert_eq!(outbox_count(&storage, "dispatched_at IS NULL").await, 1);
    }

    #[tokio::test]
    async fn reconciles_acceptance_before_dispatch_marking() {
        let directory = TempDir::new().expect("create temporary directory");
        let storage = open(&directory).await;
        let task = task(1);
        storage.accept_task(&task).await.expect("accept task");
        let client = Client::new(storage.duroxide_provider());
        client
            .start_orchestration_versioned(
                &task.instance_id,
                &task.orchestration_name,
                &task.orchestration_version,
                &task.input_json,
            )
            .await
            .expect("simulate accepted start");

        assert_eq!(
            dispatcher(&storage, 1)
                .run_once(10)
                .await
                .expect("reconcile dispatch"),
            DispatchReport {
                claimed: 1,
                dispatched: 1,
                retrying: 0,
                permanently_failed: 0,
            }
        );
        assert_eq!(outbox_count(&storage, "dispatched_at IS NOT NULL").await, 1);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM orchestrator_queue WHERE instance_id = ?"
            )
            .bind(&task.instance_id)
            .fetch_one(storage.pool())
            .await
            .expect("count Duroxide starts"),
            1
        );
    }

    #[tokio::test]
    async fn marks_an_identity_collision_permanent() {
        let directory = TempDir::new().expect("create temporary directory");
        let storage = open(&directory).await;
        let task = task(1);
        storage.accept_task(&task).await.expect("accept task");
        Client::new(storage.duroxide_provider())
            .start_orchestration_versioned(
                &task.instance_id,
                "DifferentWorkflow",
                &task.orchestration_version,
                &task.input_json,
            )
            .await
            .expect("reserve conflicting identity");

        let report = dispatcher(&storage, 1)
            .run_once(10)
            .await
            .expect("dispatch collision");
        assert_eq!(report.permanently_failed, 1);
        assert_eq!(
            outbox_count(&storage, "permanent_failure_at IS NOT NULL").await,
            1
        );
        assert_eq!(
            dispatcher(&storage, 1).run_once(11).await.unwrap().claimed,
            0
        );
    }

    #[test]
    fn bounds_retry_backoff_with_deterministic_jitter() {
        assert!((1_000..=1_200).contains(&retry_delay(1, 1)));
        assert!((2_000..=2_400).contains(&retry_delay(1, 2)));
        assert_eq!(retry_delay(1, 100), MAX_BACKOFF_MS);
        assert_eq!(retry_delay(1, 5), retry_delay(1, 5));
    }

    fn dispatcher(storage: &Storage, batch_size: usize) -> OutboxDispatcher<'_> {
        OutboxDispatcher::new(
            storage,
            Client::new(storage.duroxide_provider()),
            batch_size,
        )
    }

    async fn open(directory: &TempDir) -> Storage {
        Storage::open(
            directory.path().join("sporos.lock"),
            directory.path().join("sporos.db"),
        )
        .await
        .expect("open storage")
    }

    fn task(marker: u8) -> NewTask {
        NewTask {
            id: TaskId::from_bytes([marker; 16]),
            key: TaskKey::from_bytes([marker; 32]),
            kind: "fake".to_owned(),
            policy: PolicySnapshot {
                id: PolicySnapshotId::from_bytes([marker; 16]),
                config_hash: [marker; 32],
                matcher_version: "phase1".to_owned(),
                payload_json: "{}".to_owned(),
                created_at: i64::from(marker),
            },
            orchestration_name: "FakeTask".to_owned(),
            orchestration_version: "1.0.0".to_owned(),
            instance_id: format!("fake-v1:{marker}"),
            input_json: format!(r#"{{"marker":{marker}}}"#),
            created_at: i64::from(marker),
        }
    }

    async fn outbox_count(storage: &Storage, predicate: &str) -> i64 {
        let query = format!("SELECT count(*) FROM sporos_outbox WHERE {predicate}");
        sqlx::query_scalar(sqlx::AssertSqlSafe(query))
            .fetch_one(storage.pool())
            .await
            .expect("count outbox rows")
    }
}
