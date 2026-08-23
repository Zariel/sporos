use sha2::{Digest, Sha256};
use sqlx::{Row, Sqlite, Transaction};
use thiserror::Error;

use crate::inventory::{InventoryChange, InventoryDelta, InventoryTorrent};
use crate::storage::Storage;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InventoryState {
    pub response_id: Option<u64>,
    pub generation: u64,
    pub has_baseline: bool,
    pub last_success_at: Option<i64>,
    pub last_full_reconcile_at: Option<i64>,
    pub reconcile_requested_at: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompletionTransition {
    pub source_id: [u8; 16],
    pub completed_at: i64,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct ProjectionBatch {
    pub changed: usize,
    pub completions: Vec<CompletionTransition>,
    pub manifests_needed: Vec<[u8; 16]>,
}

impl Storage {
    pub async fn qbit_inventory_state(&self) -> Result<InventoryState, ProjectionError> {
        let row = sqlx::query(
            "SELECT response_id, generation, baseline_at, last_success_at,
                    last_full_reconcile_at, reconcile_requested_at
             FROM sporos_qbit_inventory_state WHERE singleton = 1",
        )
        .fetch_one(self.pool())
        .await?;
        Ok(InventoryState {
            response_id: optional_u64(&row, "response_id")?,
            generation: required_u64(&row, "generation")?,
            has_baseline: row.try_get::<Option<i64>, _>("baseline_at")?.is_some(),
            last_success_at: row.try_get("last_success_at")?,
            last_full_reconcile_at: row.try_get("last_full_reconcile_at")?,
            reconcile_requested_at: row.try_get("reconcile_requested_at")?,
        })
    }

    pub async fn record_qbit_versions(
        &self,
        application: &str,
        web_api: &str,
    ) -> Result<(), ProjectionError> {
        sqlx::query(
            "UPDATE sporos_qbit_inventory_state
             SET application_version = ?, web_api_version = ? WHERE singleton = 1",
        )
        .bind(application)
        .bind(web_api)
        .execute(self.pool())
        .await?;
        Ok(())
    }

    pub async fn project_qbit_batch(
        &self,
        changes: &[InventoryChange],
        generation: u64,
        detect_completions: bool,
        now: i64,
    ) -> Result<ProjectionBatch, ProjectionError> {
        let generation = to_i64(generation, "generation")?;
        let mut transaction = self.pool().begin().await?;
        let mut result = ProjectionBatch::default();
        for change in changes {
            match change {
                InventoryChange::Upsert { qbit_id, delta } => {
                    let outcome = project_torrent(
                        &mut transaction,
                        qbit_id,
                        delta,
                        generation,
                        detect_completions,
                        now,
                    )
                    .await?;
                    if let Some(completion) = outcome.completion {
                        result.completions.push(completion);
                    }
                    if outcome.manifest_needed {
                        result.manifests_needed.push(outcome.source_id);
                    }
                }
                InventoryChange::Removed { qbit_id } => {
                    sqlx::query(
                        "UPDATE sporos_qbit_torrent
                         SET available = 0, updated_at = ? WHERE qbit_id = ?",
                    )
                    .bind(now)
                    .bind(qbit_id)
                    .execute(&mut *transaction)
                    .await?;
                }
            }
            result.changed += 1;
        }
        transaction.commit().await?;
        Ok(result)
    }

    pub async fn finish_qbit_sync(
        &self,
        response_id: u64,
        full_generation: Option<u64>,
        now: i64,
    ) -> Result<(), ProjectionError> {
        let response_id = to_i64(response_id, "response ID")?;
        let mut transaction = self.pool().begin().await?;
        if let Some(generation) = full_generation {
            let generation = to_i64(generation, "generation")?;
            sqlx::query(
                "UPDATE sporos_qbit_torrent SET available = 0, updated_at = ?
                 WHERE available = 1 AND last_seen_generation <> ?",
            )
            .bind(now)
            .bind(generation)
            .execute(&mut *transaction)
            .await?;
            sqlx::query(
                "UPDATE sporos_qbit_inventory_state
                 SET response_id = ?, generation = ?, baseline_at = COALESCE(baseline_at, ?),
                     last_success_at = ?, last_full_reconcile_at = ?
                 WHERE singleton = 1",
            )
            .bind(response_id)
            .bind(generation)
            .bind(now)
            .bind(now)
            .bind(now)
            .execute(&mut *transaction)
            .await?;
        } else {
            sqlx::query(
                "UPDATE sporos_qbit_inventory_state
                 SET response_id = ?, last_success_at = ? WHERE singleton = 1",
            )
            .bind(response_id)
            .bind(now)
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    pub async fn finish_qbit_reconcile(
        &self,
        generation: u64,
        now: i64,
    ) -> Result<(), ProjectionError> {
        let generation = to_i64(generation, "generation")?;
        let mut transaction = self.pool().begin().await?;
        sqlx::query(
            "UPDATE sporos_qbit_torrent SET available = 0, updated_at = ?
             WHERE available = 1 AND last_seen_generation <> ?",
        )
        .bind(now)
        .bind(generation)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE sporos_qbit_inventory_state
             SET generation = ?, baseline_at = COALESCE(baseline_at, ?),
                 last_success_at = ?, last_full_reconcile_at = ?, reconcile_requested_at = NULL
             WHERE singleton = 1",
        )
        .bind(generation)
        .bind(now)
        .bind(now)
        .bind(now)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }
}

impl From<InventoryTorrent> for InventoryChange {
    fn from(torrent: InventoryTorrent) -> Self {
        Self::Upsert {
            qbit_id: torrent.hash,
            delta: Box::new(InventoryDelta {
                infohash_v1: Some(torrent.infohash_v1),
                infohash_v2: Some(torrent.infohash_v2),
                name: Some(torrent.name),
                total_size: Some(torrent.total_size),
                amount_left: Some(torrent.amount_left),
                progress: Some(torrent.progress),
                state: Some(torrent.state),
                save_path: Some(torrent.save_path),
                content_path: Some(torrent.content_path),
                category: Some(torrent.category),
                tags: Some(torrent.tags),
                added_on: Some(torrent.added_on),
                completion_on: Some(torrent.completion_on),
            }),
        }
    }
}

struct TorrentOutcome {
    source_id: [u8; 16],
    completion: Option<CompletionTransition>,
    manifest_needed: bool,
}

async fn project_torrent(
    transaction: &mut Transaction<'_, Sqlite>,
    qbit_id: &str,
    delta: &InventoryDelta,
    generation: i64,
    detect_completions: bool,
    now: i64,
) -> Result<TorrentOutcome, ProjectionError> {
    let existing = sqlx::query(
        "SELECT id, v1_hash AS infohash_v1, v2_hash AS infohash_v2, name, total_size, amount_left,
                progress_ppm, state, save_path, content_path, category, tags_json,
                is_complete, file_manifest_version, file_manifest_state,
                content_fingerprint, added_at, completed_at
         FROM sporos_qbit_torrent WHERE qbit_id = ?",
    )
    .bind(qbit_id)
    .fetch_optional(&mut **transaction)
    .await?;

    let source_id = source_id(qbit_id);
    let v1 = optional_hash(delta.infohash_v1.as_deref(), 20, "infohash_v1")?
        .or_else(|| existing.as_ref().and_then(|row| row.get("infohash_v1")));
    let v2 = optional_hash(delta.infohash_v2.as_deref(), 32, "infohash_v2")?
        .or_else(|| existing.as_ref().and_then(|row| row.get("infohash_v2")));
    if v1.is_none() && v2.is_none() {
        return Err(ProjectionError::MissingField("infohash"));
    }
    let name = text(delta.name.as_ref(), existing.as_ref(), "name")?;
    let total_size = integer(delta.total_size, existing.as_ref(), "total_size")?;
    let amount_left = integer(delta.amount_left, existing.as_ref(), "amount_left")?;
    let progress_ppm = delta
        .progress
        .map(progress_ppm)
        .transpose()?
        .or_else(|| existing.as_ref().and_then(|row| row.get("progress_ppm")))
        .ok_or(ProjectionError::MissingField("progress"))?;
    let state = text(delta.state.as_ref(), existing.as_ref(), "state")?;
    let save_path = text(delta.save_path.as_ref(), existing.as_ref(), "save_path")?;
    let content_path = text(
        delta.content_path.as_ref(),
        existing.as_ref(),
        "content_path",
    )?;
    let category = text(delta.category.as_ref(), existing.as_ref(), "category")?;
    let tags_json = delta
        .tags
        .as_deref()
        .map(tags_json)
        .transpose()?
        .or_else(|| existing.as_ref().and_then(|row| row.get("tags_json")))
        .ok_or(ProjectionError::MissingField("tags"))?;
    let added_at = timestamp(delta.added_on, existing.as_ref(), "added_at");
    let observed_completion = delta.completion_on.filter(|value| *value > 0);
    let was_complete = existing
        .as_ref()
        .is_some_and(|row| row.get::<i64, _>("is_complete") == 1);
    let is_complete = complete(amount_left, progress_ppm, &state);
    let completed_at = if is_complete {
        observed_completion
            .or_else(|| existing.as_ref().and_then(|row| row.get("completed_at")))
            .or(Some(now))
    } else {
        existing.as_ref().and_then(|row| row.get("completed_at"))
    };
    let fingerprint = fingerprint(&v1, &v2, &name, total_size, &save_path, &content_path);
    let fingerprint_changed = existing.as_ref().is_some_and(|row| {
        row.get::<Option<Vec<u8>>, _>("content_fingerprint")
            .as_deref()
            != Some(fingerprint.as_slice())
    });
    let manifest_version = existing
        .as_ref()
        .map_or(0, |row| row.get("file_manifest_version"));
    let prior_manifest_state = existing.as_ref().map_or_else(
        || "unloaded".to_owned(),
        |row| row.get("file_manifest_state"),
    );
    let became_complete = detect_completions && existing.is_some() && !was_complete && is_complete;
    let manifest_needed = became_complete || (fingerprint_changed && manifest_version > 0);
    let manifest_state = if manifest_needed {
        "stale"
    } else {
        &prior_manifest_state
    };

    sqlx::query(
        "INSERT INTO sporos_qbit_torrent (
            id, qbit_id, v1_hash, v2_hash, name, total_size, amount_left,
            progress_ppm, state, save_path, content_path, category, tags_json,
            is_complete, available, file_manifest_version, file_manifest_state,
            content_fingerprint, added_at, completed_at, last_seen_generation, updated_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(qbit_id) WHERE qbit_id IS NOT NULL DO UPDATE SET
            v1_hash = excluded.v1_hash, v2_hash = excluded.v2_hash,
            name = excluded.name, total_size = excluded.total_size,
            amount_left = excluded.amount_left, progress_ppm = excluded.progress_ppm,
            state = excluded.state, save_path = excluded.save_path,
            content_path = excluded.content_path, category = excluded.category,
            tags_json = excluded.tags_json, is_complete = excluded.is_complete,
            available = 1, file_manifest_state = excluded.file_manifest_state,
            content_fingerprint = excluded.content_fingerprint,
            added_at = excluded.added_at, completed_at = excluded.completed_at,
            last_seen_generation = excluded.last_seen_generation, updated_at = excluded.updated_at",
    )
    .bind(source_id.as_slice())
    .bind(qbit_id)
    .bind(v1)
    .bind(v2)
    .bind(name)
    .bind(total_size)
    .bind(amount_left)
    .bind(progress_ppm)
    .bind(state)
    .bind(save_path)
    .bind(content_path)
    .bind(category)
    .bind(tags_json)
    .bind(i64::from(is_complete))
    .bind(manifest_version)
    .bind(manifest_state)
    .bind(fingerprint.as_slice())
    .bind(added_at)
    .bind(completed_at)
    .bind(generation)
    .bind(now)
    .execute(&mut **transaction)
    .await?;

    Ok(TorrentOutcome {
        source_id,
        completion: if became_complete {
            Some(CompletionTransition {
                source_id,
                completed_at: completed_at.expect("complete torrents have completion time"),
            })
        } else {
            None
        },
        manifest_needed,
    })
}

fn source_id(qbit_id: &str) -> [u8; 16] {
    let digest = Sha256::digest([b"qbit:".as_slice(), qbit_id.as_bytes()].concat());
    let mut id = [0_u8; 16];
    id.copy_from_slice(&digest[..16]);
    id
}

fn fingerprint(
    v1: &Option<Vec<u8>>,
    v2: &Option<Vec<u8>>,
    name: &str,
    total_size: i64,
    save_path: &str,
    content_path: &str,
) -> [u8; 32] {
    let mut hash = Sha256::new();
    let total_size = total_size.to_be_bytes();
    for value in [
        v1.as_deref().unwrap_or_default(),
        v2.as_deref().unwrap_or_default(),
        name.as_bytes(),
        &total_size,
        save_path.as_bytes(),
        content_path.as_bytes(),
    ] {
        hash.update((value.len() as u64).to_be_bytes());
        hash.update(value);
    }
    hash.finalize().into()
}

fn complete(amount_left: i64, progress_ppm: i64, state: &str) -> bool {
    amount_left == 0
        && progress_ppm == 1_000_000
        && matches!(
            state,
            "uploading" | "stoppedUP" | "queuedUP" | "stalledUP" | "checkingUP" | "forcedUP"
        )
}

fn text(
    value: Option<&String>,
    existing: Option<&sqlx::sqlite::SqliteRow>,
    field: &'static str,
) -> Result<String, ProjectionError> {
    value
        .cloned()
        .or_else(|| existing.map(|row| row.get(field)))
        .ok_or(ProjectionError::MissingField(field))
}

fn integer(
    value: Option<u64>,
    existing: Option<&sqlx::sqlite::SqliteRow>,
    field: &'static str,
) -> Result<i64, ProjectionError> {
    value
        .map(|value| to_i64(value, field))
        .transpose()?
        .or_else(|| existing.map(|row| row.get(field)))
        .ok_or(ProjectionError::MissingField(field))
}

fn timestamp(
    value: Option<i64>,
    existing: Option<&sqlx::sqlite::SqliteRow>,
    field: &'static str,
) -> Option<i64> {
    value
        .filter(|value| *value > 0)
        .or_else(|| existing.and_then(|row| row.get(field)))
}

fn progress_ppm(value: f64) -> Result<i64, ProjectionError> {
    if !value.is_finite() || !(0.0..=1.0).contains(&value) {
        return Err(ProjectionError::InvalidProgress);
    }
    Ok((value * 1_000_000.0).round() as i64)
}

fn tags_json(value: &str) -> Result<String, ProjectionError> {
    serde_json::to_string(
        &value
            .split(',')
            .map(str::trim)
            .filter(|tag| !tag.is_empty())
            .collect::<Vec<_>>(),
    )
    .map_err(ProjectionError::Tags)
}

fn optional_hash(
    value: Option<&str>,
    bytes: usize,
    field: &'static str,
) -> Result<Option<Vec<u8>>, ProjectionError> {
    value
        .filter(|value| !value.is_empty())
        .map(|value| decode_hex(value, bytes, field))
        .transpose()
}

fn decode_hex(value: &str, bytes: usize, field: &'static str) -> Result<Vec<u8>, ProjectionError> {
    if value.len() != bytes * 2 {
        return Err(ProjectionError::InvalidHash(field));
    }
    value
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            let high = hex_digit(pair[0]).ok_or(ProjectionError::InvalidHash(field))?;
            let low = hex_digit(pair[1]).ok_or(ProjectionError::InvalidHash(field))?;
            Ok((high << 4) | low)
        })
        .collect()
}

const fn hex_digit(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn required_u64(
    row: &sqlx::sqlite::SqliteRow,
    field: &'static str,
) -> Result<u64, ProjectionError> {
    let value = row.try_get::<i64, _>(field)?;
    u64::try_from(value).map_err(|_| ProjectionError::StoredRange(field))
}

fn optional_u64(
    row: &sqlx::sqlite::SqliteRow,
    field: &'static str,
) -> Result<Option<u64>, ProjectionError> {
    row.try_get::<Option<i64>, _>(field)?
        .map(|value| u64::try_from(value).map_err(|_| ProjectionError::StoredRange(field)))
        .transpose()
}

fn to_i64(value: u64, field: &'static str) -> Result<i64, ProjectionError> {
    i64::try_from(value).map_err(|_| ProjectionError::Range(field))
}

#[derive(Debug, Error)]
pub enum ProjectionError {
    #[error("qBittorrent projection database operation failed")]
    Database(#[from] sqlx::Error),
    #[error("new qBittorrent torrent is missing {0}")]
    MissingField(&'static str),
    #[error("qBittorrent {0} is out of range")]
    Range(&'static str),
    #[error("stored qBittorrent {0} is out of range")]
    StoredRange(&'static str),
    #[error("qBittorrent {0} is not a valid infohash")]
    InvalidHash(&'static str),
    #[error("qBittorrent progress is invalid")]
    InvalidProgress,
    #[error("could not encode qBittorrent tags")]
    Tags(#[source] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[tokio::test]
    async fn projects_partial_updates_and_advances_cursor_last() {
        let directory = TempDir::new().expect("temporary directory");
        let storage = open(&directory).await;
        storage
            .project_qbit_batch(&[snapshot(false)], 1, false, 10)
            .await
            .expect("project baseline batch");
        assert_eq!(
            storage.qbit_inventory_state().await.unwrap().response_id,
            None
        );
        storage.finish_qbit_sync(7, Some(1), 11).await.unwrap();

        let result = storage
            .project_qbit_batch(&[completion()], 1, true, 12)
            .await
            .expect("project completion delta");
        assert_eq!(result.completions.len(), 1);
        assert_eq!(result.manifests_needed.len(), 1);
        storage.finish_qbit_sync(8, None, 12).await.unwrap();

        let row = sqlx::query_as::<_, (i64, i64, String)>(
            "SELECT amount_left, is_complete, file_manifest_state
             FROM sporos_qbit_torrent",
        )
        .fetch_one(storage.pool())
        .await
        .unwrap();
        assert_eq!(row, (0, 1, "stale".to_owned()));
        assert_eq!(
            storage.qbit_inventory_state().await.unwrap().response_id,
            Some(8)
        );
    }

    #[tokio::test]
    async fn baseline_suppresses_existing_completions_and_marks_absent_rows() {
        let directory = TempDir::new().expect("temporary directory");
        let storage = open(&directory).await;
        let first = storage
            .project_qbit_batch(&[snapshot(true)], 1, false, 10)
            .await
            .unwrap();
        assert!(first.completions.is_empty());
        storage.finish_qbit_sync(1, Some(1), 10).await.unwrap();
        storage.finish_qbit_reconcile(2, 20).await.unwrap();

        let available = sqlx::query_scalar::<_, i64>(
            "SELECT available FROM sporos_qbit_torrent WHERE qbit_id = ?",
        )
        .bind(id())
        .fetch_one(storage.pool())
        .await
        .unwrap();
        assert_eq!(available, 0);
        let state = storage.qbit_inventory_state().await.unwrap();
        assert!(state.has_baseline);
        assert_eq!(state.generation, 2);
    }

    #[tokio::test]
    async fn rejects_a_partial_record_without_a_baseline_row() {
        let directory = TempDir::new().expect("temporary directory");
        let storage = open(&directory).await;
        let error = storage
            .project_qbit_batch(&[completion()], 1, false, 10)
            .await
            .expect_err("reject partial first sighting");
        assert!(matches!(error, ProjectionError::MissingField(_)));
    }

    fn snapshot(complete: bool) -> InventoryChange {
        InventoryTorrent {
            hash: id().to_owned(),
            infohash_v1: id().to_owned(),
            infohash_v2: String::new(),
            name: "release".to_owned(),
            total_size: 4,
            amount_left: if complete { 0 } else { 4 },
            progress: if complete { 1.0 } else { 0.0 },
            state: if complete { "stoppedUP" } else { "stoppedDL" }.to_owned(),
            save_path: "/data".to_owned(),
            content_path: "/data/release".to_owned(),
            category: "video".to_owned(),
            tags: "one, two".to_owned(),
            added_on: 1,
            completion_on: if complete { 2 } else { 0 },
        }
        .into()
    }

    fn completion() -> InventoryChange {
        InventoryChange::Upsert {
            qbit_id: id().to_owned(),
            delta: Box::new(InventoryDelta {
                amount_left: Some(0),
                progress: Some(1.0),
                state: Some("uploading".to_owned()),
                completion_on: Some(12),
                ..InventoryDelta::default()
            }),
        }
    }

    fn id() -> &'static str {
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    }

    async fn open(directory: &TempDir) -> Storage {
        Storage::open(
            directory.path().join("sporos.lock"),
            directory.path().join("sporos.db"),
        )
        .await
        .expect("open storage")
    }
}
