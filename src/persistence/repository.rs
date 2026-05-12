use std::path::{Path, PathBuf};

use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Executor, Row, SqlitePool};

use crate::announce::{
    AnnounceDedupeHash, AnnounceReason, AnnounceStatus, AnnounceWorkId, AnnounceWorkItem,
};
use crate::domain::{
    ByteSize, CandidateAssessment, CandidateGuid, DependencyName, DependencyState, InfoHash,
    JobName, JobState, LocalFile, LocalItem, LocalItemId, LocalItemSource, MatchDecision,
    MediaType, ReasonText, RemoteCandidate, RemoteCandidateId,
};
use crate::errors::DatabaseError;

use super::schema::{CONNECTION_PRAGMAS, initial_schema_statements};

#[derive(Debug, Clone)]
pub struct Repository {
    pool: SqlitePool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum AnnounceInsertResult {
    Inserted { id: AnnounceWorkId },
    Deduplicated { id: AnnounceWorkId },
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AnnounceStatusCount {
    pub status: String,
    pub reason: String,
    pub count: i64,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct JobStateUpdate<'a> {
    pub state: JobState,
    pub last_started_at_ms: Option<i64>,
    pub last_finished_at_ms: Option<i64>,
    pub next_run_at_ms: Option<i64>,
    pub last_error: Option<&'a str>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct JobStatusSnapshot {
    pub name: JobName,
    pub state: String,
    pub last_started_at_ms: Option<i64>,
    pub last_finished_at_ms: Option<i64>,
    pub next_run_at_ms: Option<i64>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DependencyHealthSnapshot {
    pub dependency_type: String,
    pub dependency_name: DependencyName,
    pub state: String,
    pub reason: Option<String>,
    pub retry_after_ms: Option<i64>,
    pub checked_at_ms: i64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RemoteCandidateSnapshot {
    pub id: RemoteCandidateId,
    pub indexer_id: u64,
    pub guid: CandidateGuid,
    pub title: String,
    pub info_hash: Option<InfoHash>,
    pub torrent_cache_path: Option<std::path::PathBuf>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MatchDecisionSnapshot {
    pub candidate_id: RemoteCandidateId,
    pub decision: String,
    pub matched_size: Option<i64>,
    pub matched_ratio: Option<f64>,
    pub reason_code: String,
    pub assessed_at_ms: i64,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct AnnounceRetryUpdate<'a> {
    pub reason: AnnounceReason,
    pub next_attempt_at_ms: i64,
    pub now_ms: i64,
    pub error_class: &'a str,
    pub redacted_message: &'a str,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct LeasedTransition<'a> {
    status: AnnounceStatus,
    reason: AnnounceReason,
    next_attempt_at_ms: Option<i64>,
    now_ms: i64,
    dependency: Option<(&'a str, &'a str)>,
}

impl Repository {
    pub async fn connect(path: impl AsRef<Path>) -> Result<Self, DatabaseError> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(options)
            .await
            .map_err(|error| db_error("connect sqlite database", error))?;

        let repository = Self { pool };
        repository.initialize().await?;
        Ok(repository)
    }

    pub async fn connect_in_memory() -> Result<Self, DatabaseError> {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .map_err(|error| db_error("connect in-memory sqlite database", error))?;

        let repository = Self { pool };
        repository.initialize().await?;
        Ok(repository)
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    pub async fn initialize(&self) -> Result<(), DatabaseError> {
        for pragma in CONNECTION_PRAGMAS {
            self.pool
                .execute(*pragma)
                .await
                .map_err(|error| db_error("apply sqlite pragma", error))?;
        }

        for statement in initial_schema_statements() {
            self.pool
                .execute(statement)
                .await
                .map_err(|error| db_error("initialize sqlite schema", error))?;
        }

        Ok(())
    }

    pub async fn upsert_local_item_with_files(
        &self,
        item: &LocalItem,
        files: &[LocalFile],
    ) -> Result<LocalItemId, DatabaseError> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|error| db_error("begin local item transaction", error))?;

        let (source_type, source_key) = local_source(&item.source);
        sqlx::query(
            r#"
            INSERT INTO local_items (
                source_type,
                source_key,
                title,
                display_name,
                media_type,
                info_hash,
                path,
                save_path,
                total_size,
                mtime_ms,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, unixepoch() * 1000, unixepoch() * 1000)
            ON CONFLICT (source_type, source_key) DO UPDATE SET
                title = excluded.title,
                display_name = excluded.display_name,
                media_type = excluded.media_type,
                info_hash = excluded.info_hash,
                path = excluded.path,
                save_path = excluded.save_path,
                total_size = excluded.total_size,
                mtime_ms = excluded.mtime_ms,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(&source_type)
        .bind(&source_key)
        .bind(item.title.as_str())
        .bind(item.display_name.as_str())
        .bind(media_type_key(item.media_type))
        .bind(item.info_hash.as_ref().map(InfoHash::as_str))
        .bind(item.path.as_ref().map(path_to_string))
        .bind(item.save_path.as_ref().map(path_to_string))
        .bind(i64_from_u64(
            item.total_size.get(),
            "local item total size",
        )?)
        .bind(item.mtime_ms)
        .execute(&mut *transaction)
        .await
        .map_err(|error| db_error("upsert local item", error))?;

        let row =
            sqlx::query("SELECT id FROM local_items WHERE source_type = ? AND source_key = ?")
                .bind(&source_type)
                .bind(&source_key)
                .fetch_one(&mut *transaction)
                .await
                .map_err(|error| db_error("select local item id", error))?;
        let item_id = id_from_i64(row.get::<i64, _>("id"), "local item id")?;

        sqlx::query("DELETE FROM local_files WHERE item_id = ?")
            .bind(i64_from_u64(item_id.get(), "local item id")?)
            .execute(&mut *transaction)
            .await
            .map_err(|error| db_error("replace local files", error))?;

        for file in files {
            sqlx::query(
                r#"
                INSERT INTO local_files (
                    item_id,
                    relative_path,
                    file_name,
                    size,
                    mtime_ms,
                    file_index
                )
                VALUES (?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(i64_from_u64(item_id.get(), "local item id")?)
            .bind(path_to_string(&file.relative_path))
            .bind(file.file_name.as_str())
            .bind(i64_from_u64(file.size.get(), "local file size")?)
            .bind(None::<i64>)
            .bind(i64::from(file.file_index.get()))
            .execute(&mut *transaction)
            .await
            .map_err(|error| db_error("insert local file", error))?;
        }

        transaction
            .commit()
            .await
            .map_err(|error| db_error("commit local item transaction", error))?;

        Ok(item_id)
    }

    pub async fn upsert_remote_candidate(
        &self,
        candidate: &RemoteCandidate,
    ) -> Result<RemoteCandidateId, DatabaseError> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|error| db_error("begin remote candidate transaction", error))?;

        sqlx::query(
            r#"
            INSERT INTO remote_candidates (
                indexer_id,
                guid,
                download_url,
                title,
                tracker,
                size,
                published_at,
                info_hash,
                torrent_cache_path,
                first_seen_at,
                last_seen_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, unixepoch() * 1000, unixepoch() * 1000)
            ON CONFLICT (indexer_id, guid) DO UPDATE SET
                download_url = excluded.download_url,
                title = excluded.title,
                tracker = excluded.tracker,
                size = excluded.size,
                published_at = excluded.published_at,
                info_hash = excluded.info_hash,
                torrent_cache_path = excluded.torrent_cache_path,
                last_seen_at = excluded.last_seen_at
            "#,
        )
        .bind(i64_from_u64(candidate.indexer_id.get(), "indexer id")?)
        .bind(candidate.guid.as_str())
        .bind(candidate.download_url.as_str())
        .bind(candidate.title.as_str())
        .bind(candidate.tracker.as_str())
        .bind(
            candidate
                .size
                .map(ByteSize::get)
                .map(|size| i64_from_u64(size, "candidate size"))
                .transpose()?,
        )
        .bind(candidate.published_at_ms)
        .bind(candidate.info_hash.as_ref().map(InfoHash::as_str))
        .bind(candidate.torrent_cache_path.as_ref().map(path_to_string))
        .execute(&mut *transaction)
        .await
        .map_err(|error| db_error("upsert remote candidate", error))?;

        let row = sqlx::query("SELECT id FROM remote_candidates WHERE indexer_id = ? AND guid = ?")
            .bind(i64_from_u64(candidate.indexer_id.get(), "indexer id")?)
            .bind(candidate.guid.as_str())
            .fetch_one(&mut *transaction)
            .await
            .map_err(|error| db_error("select remote candidate id", error))?;
        let candidate_id = remote_id_from_i64(row.get::<i64, _>("id"), "remote candidate id")?;

        transaction
            .commit()
            .await
            .map_err(|error| db_error("commit remote candidate transaction", error))?;

        Ok(candidate_id)
    }

    pub async fn record_match_decision(
        &self,
        local_item_id: LocalItemId,
        candidate_id: RemoteCandidateId,
        assessment: CandidateAssessment,
        assessed_at_ms: i64,
    ) -> Result<(), DatabaseError> {
        sqlx::query(
            r#"
            INSERT INTO match_decisions (
                local_item_id,
                candidate_id,
                decision,
                matched_size,
                matched_ratio,
                reason_code,
                assessed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (local_item_id, candidate_id) DO UPDATE SET
                decision = excluded.decision,
                matched_size = excluded.matched_size,
                matched_ratio = excluded.matched_ratio,
                reason_code = excluded.reason_code,
                assessed_at = excluded.assessed_at
            "#,
        )
        .bind(i64_from_u64(local_item_id.get(), "local item id")?)
        .bind(i64_from_u64(candidate_id.get(), "remote candidate id")?)
        .bind(match_decision_key(assessment.decision))
        .bind(
            assessment
                .matched_size
                .map(ByteSize::get)
                .map(|size| i64_from_u64(size, "matched size"))
                .transpose()?,
        )
        .bind(assessment.matched_ratio.map(|ratio| ratio.get()))
        .bind(format!("{:?}", assessment.reason).to_ascii_snake_case())
        .bind(assessed_at_ms)
        .execute(&self.pool)
        .await
        .map_err(|error| db_error("record match decision", error))?;

        Ok(())
    }

    pub async fn remote_candidates_by_info_hash(
        &self,
        info_hash: &InfoHash,
        limit: u16,
    ) -> Result<Vec<RemoteCandidateSnapshot>, DatabaseError> {
        let rows = sqlx::query(
            r#"
            SELECT id, indexer_id, guid, title, info_hash, torrent_cache_path
            FROM remote_candidates
            WHERE info_hash = ?
            ORDER BY last_seen_at DESC, id
            LIMIT ?
            "#,
        )
        .bind(info_hash.as_str())
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(|error| db_error("lookup remote candidates by info hash", error))?;

        rows.into_iter()
            .map(remote_candidate_snapshot_from_row)
            .collect()
    }

    pub async fn match_decisions_for_local_item(
        &self,
        local_item_id: LocalItemId,
        limit: u16,
    ) -> Result<Vec<MatchDecisionSnapshot>, DatabaseError> {
        let rows = sqlx::query(
            r#"
            SELECT candidate_id, decision, matched_size, matched_ratio, reason_code, assessed_at
            FROM match_decisions
            WHERE local_item_id = ?
            ORDER BY assessed_at DESC, candidate_id
            LIMIT ?
            "#,
        )
        .bind(i64_from_u64(local_item_id.get(), "local item id")?)
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(|error| db_error("read local item match decisions", error))?;

        rows.into_iter()
            .map(|row| {
                Ok(MatchDecisionSnapshot {
                    candidate_id: remote_id_from_i64(row.get("candidate_id"), "candidate id")?,
                    decision: row.get("decision"),
                    matched_size: row.get("matched_size"),
                    matched_ratio: row.get("matched_ratio"),
                    reason_code: row.get("reason_code"),
                    assessed_at_ms: row.get("assessed_at"),
                })
            })
            .collect()
    }

    pub async fn upsert_job_state(
        &self,
        name: &JobName,
        state: JobState,
        next_run_at: Option<i64>,
        last_error: Option<&str>,
    ) -> Result<(), DatabaseError> {
        self.record_job_status(
            name,
            JobStateUpdate {
                state,
                last_started_at_ms: None,
                last_finished_at_ms: None,
                next_run_at_ms: next_run_at,
                last_error,
            },
        )
        .await
    }

    pub async fn record_job_status(
        &self,
        name: &JobName,
        update: JobStateUpdate<'_>,
    ) -> Result<(), DatabaseError> {
        sqlx::query(
            r#"
            INSERT INTO jobs (
                name,
                state,
                last_started_at,
                last_finished_at,
                next_run_at,
                last_error
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (name) DO UPDATE SET
                state = excluded.state,
                last_started_at = COALESCE(excluded.last_started_at, jobs.last_started_at),
                last_finished_at = COALESCE(excluded.last_finished_at, jobs.last_finished_at),
                next_run_at = excluded.next_run_at,
                last_error = excluded.last_error
            "#,
        )
        .bind(name.as_str())
        .bind(job_state_key(update.state))
        .bind(update.last_started_at_ms)
        .bind(update.last_finished_at_ms)
        .bind(update.next_run_at_ms)
        .bind(update.last_error)
        .execute(&self.pool)
        .await
        .map_err(|error| db_error("record job status", error))?;

        Ok(())
    }

    pub async fn ready_jobs(&self, now_ms: i64, limit: u16) -> Result<Vec<JobName>, DatabaseError> {
        let rows = sqlx::query(
            r#"
            SELECT name FROM jobs
            WHERE state NOT IN ('running', 'disabled')
              AND (next_run_at IS NULL OR next_run_at <= ?)
            ORDER BY COALESCE(next_run_at, -9223372036854775808), name
            LIMIT ?
            "#,
        )
        .bind(now_ms)
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(|error| db_error("read ready jobs", error))?;

        rows.into_iter()
            .map(|row| {
                JobName::new(row.get::<String, _>("name")).map_err(|error| {
                    DatabaseError::QueryFailed {
                        operation: "read ready job name".to_owned(),
                        message: error.to_string(),
                    }
                })
            })
            .collect()
    }

    pub async fn job_status_snapshot(
        &self,
        limit: u16,
    ) -> Result<Vec<JobStatusSnapshot>, DatabaseError> {
        let rows = sqlx::query(
            r#"
            SELECT name, state, last_started_at, last_finished_at, next_run_at, last_error
            FROM jobs
            ORDER BY name
            LIMIT ?
            "#,
        )
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(|error| db_error("read job status snapshot", error))?;

        rows.into_iter()
            .map(|row| {
                Ok(JobStatusSnapshot {
                    name: JobName::new(row.get::<String, _>("name")).map_err(|error| {
                        DatabaseError::QueryFailed {
                            operation: "read job status name".to_owned(),
                            message: error.to_string(),
                        }
                    })?,
                    state: row.get("state"),
                    last_started_at_ms: row.get("last_started_at"),
                    last_finished_at_ms: row.get("last_finished_at"),
                    next_run_at_ms: row.get("next_run_at"),
                    last_error: row.get("last_error"),
                })
            })
            .collect()
    }

    pub async fn record_dependency_health(
        &self,
        dependency_type: &str,
        dependency_name: &DependencyName,
        state: &DependencyState,
        checked_at_ms: i64,
    ) -> Result<(), DatabaseError> {
        let (state_key, reason, retry_after) = dependency_state_row(state);
        sqlx::query(
            r#"
            INSERT INTO dependency_health (
                dependency_type,
                dependency_name,
                state,
                reason,
                retry_after,
                checked_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (dependency_type, dependency_name) DO UPDATE SET
                state = excluded.state,
                reason = excluded.reason,
                retry_after = excluded.retry_after,
                checked_at = excluded.checked_at
            "#,
        )
        .bind(dependency_type)
        .bind(dependency_name.as_str())
        .bind(state_key)
        .bind(reason)
        .bind(retry_after)
        .bind(checked_at_ms)
        .execute(&self.pool)
        .await
        .map_err(|error| db_error("record dependency health", error))?;

        Ok(())
    }

    pub async fn dependency_health_snapshot(
        &self,
        limit: u16,
    ) -> Result<Vec<DependencyHealthSnapshot>, DatabaseError> {
        let rows = sqlx::query(
            r#"
            SELECT dependency_type, dependency_name, state, reason, retry_after, checked_at
            FROM dependency_health
            ORDER BY dependency_type, dependency_name
            LIMIT ?
            "#,
        )
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(|error| db_error("read dependency health snapshot", error))?;

        rows.into_iter()
            .map(|row| {
                Ok(DependencyHealthSnapshot {
                    dependency_type: row.get("dependency_type"),
                    dependency_name: DependencyName::new(row.get::<String, _>("dependency_name"))
                        .map_err(|error| DatabaseError::QueryFailed {
                        operation: "read dependency name".to_owned(),
                        message: error.to_string(),
                    })?,
                    state: row.get("state"),
                    reason: row.get("reason"),
                    retry_after_ms: row.get("retry_after"),
                    checked_at_ms: row.get("checked_at"),
                })
            })
            .collect()
    }

    pub async fn insert_or_dedupe_announce_work(
        &self,
        work: &AnnounceWorkItem,
        max_pending: u32,
    ) -> Result<AnnounceInsertResult, DatabaseError> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|error| db_error("begin announce insert transaction", error))?;

        if let Some(id) = select_active_announce_id(&mut transaction, &work.dedupe_hash).await? {
            transaction
                .commit()
                .await
                .map_err(|error| db_error("commit announce dedupe transaction", error))?;
            return Ok(AnnounceInsertResult::Deduplicated { id });
        }

        let active_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM announce_work WHERE status IN ('queued', 'running', 'waiting', 'retryable')",
        )
        .fetch_one(&mut *transaction)
        .await
        .map_err(|error| db_error("count active announce work", error))?;

        if active_count >= i64::from(max_pending) {
            return Err(DatabaseError::Busy {
                operation: "accept announce work".to_owned(),
                retry_after_ms: None,
            });
        }

        insert_announce_work(&mut transaction, work).await?;
        transaction
            .commit()
            .await
            .map_err(|error| db_error("commit announce insert transaction", error))?;

        Ok(AnnounceInsertResult::Inserted {
            id: work.id.clone(),
        })
    }

    pub async fn claim_announce_work(
        &self,
        owner: &str,
        now_ms: i64,
        lease_until_ms: i64,
        limit: u16,
    ) -> Result<Vec<AnnounceWorkId>, DatabaseError> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|error| db_error("begin announce claim transaction", error))?;

        let rows = sqlx::query(
            r#"
            SELECT id FROM announce_work
            WHERE status IN ('queued', 'retryable')
              AND next_attempt_at <= ?
              AND expires_at > ?
            ORDER BY next_attempt_at, received_at
            LIMIT ?
            "#,
        )
        .bind(now_ms)
        .bind(now_ms)
        .bind(i64::from(limit))
        .fetch_all(&mut *transaction)
        .await
        .map_err(|error| db_error("select claimable announce work", error))?;

        let mut claimed = Vec::with_capacity(rows.len());
        for row in rows {
            let id = AnnounceWorkId::new(row.get::<String, _>("id")).map_err(|error| {
                DatabaseError::QueryFailed {
                    operation: "read announce work id".to_owned(),
                    message: error.to_string(),
                }
            })?;
            sqlx::query(
                r#"
                UPDATE announce_work
                SET status = 'running',
                    reason = 'accepted',
                    attempt_count = attempt_count + 1,
                    first_attempt_at = COALESCE(first_attempt_at, ?),
                    updated_at = ?,
                    lease_owner = ?,
                    lease_until = ?
                WHERE id = ?
                "#,
            )
            .bind(now_ms)
            .bind(now_ms)
            .bind(owner)
            .bind(lease_until_ms)
            .bind(id.as_str())
            .execute(&mut *transaction)
            .await
            .map_err(|error| db_error("claim announce work", error))?;
            claimed.push(id);
        }

        transaction
            .commit()
            .await
            .map_err(|error| db_error("commit announce claim transaction", error))?;

        Ok(claimed)
    }

    pub async fn renew_announce_lease(
        &self,
        id: &AnnounceWorkId,
        owner: &str,
        lease_until_ms: i64,
        now_ms: i64,
    ) -> Result<bool, DatabaseError> {
        let result = sqlx::query(
            r#"
            UPDATE announce_work
            SET lease_until = ?, updated_at = ?
            WHERE id = ? AND lease_owner = ? AND status = 'running'
            "#,
        )
        .bind(lease_until_ms)
        .bind(now_ms)
        .bind(id.as_str())
        .bind(owner)
        .execute(&self.pool)
        .await
        .map_err(|error| db_error("renew announce lease", error))?;

        Ok(result.rows_affected() == 1)
    }

    pub async fn release_announce_lease(
        &self,
        id: &AnnounceWorkId,
        owner: &str,
        reason: AnnounceReason,
        next_attempt_at_ms: i64,
        now_ms: i64,
    ) -> Result<bool, DatabaseError> {
        self.transition_leased(
            id,
            owner,
            LeasedTransition {
                status: AnnounceStatus::Queued,
                reason,
                next_attempt_at_ms: Some(next_attempt_at_ms),
                now_ms,
                dependency: None,
            },
        )
        .await
    }

    pub async fn mark_announce_waiting(
        &self,
        id: &AnnounceWorkId,
        owner: &str,
        reason: AnnounceReason,
        next_attempt_at_ms: i64,
        now_ms: i64,
        dependency: Option<(&str, &str)>,
    ) -> Result<bool, DatabaseError> {
        self.transition_leased(
            id,
            owner,
            LeasedTransition {
                status: AnnounceStatus::Waiting,
                reason,
                next_attempt_at_ms: Some(next_attempt_at_ms),
                now_ms,
                dependency,
            },
        )
        .await
    }

    pub async fn mark_announce_retryable(
        &self,
        id: &AnnounceWorkId,
        owner: &str,
        update: AnnounceRetryUpdate<'_>,
    ) -> Result<bool, DatabaseError> {
        let result = sqlx::query(
            r#"
            UPDATE announce_work
            SET status = 'retryable',
                reason = ?,
                next_attempt_at = ?,
                updated_at = ?,
                lease_owner = NULL,
                lease_until = NULL,
                last_error_class = ?,
                last_error_message = ?
            WHERE id = ? AND lease_owner = ? AND status = 'running'
            "#,
        )
        .bind(announce_reason_key(update.reason))
        .bind(update.next_attempt_at_ms)
        .bind(update.now_ms)
        .bind(update.error_class)
        .bind(update.redacted_message)
        .bind(id.as_str())
        .bind(owner)
        .execute(&self.pool)
        .await
        .map_err(|error| db_error("mark announce retryable", error))?;

        Ok(result.rows_affected() == 1)
    }

    pub async fn mark_announce_succeeded(
        &self,
        id: &AnnounceWorkId,
        owner: &str,
        reason: AnnounceReason,
        outcome: &str,
        now_ms: i64,
    ) -> Result<bool, DatabaseError> {
        self.finish_announce(
            id,
            owner,
            AnnounceStatus::Succeeded,
            reason,
            Some(outcome),
            now_ms,
        )
        .await
    }

    pub async fn mark_announce_terminal_failed(
        &self,
        id: &AnnounceWorkId,
        owner: &str,
        reason: AnnounceReason,
        redacted_message: &str,
        now_ms: i64,
    ) -> Result<bool, DatabaseError> {
        let result = sqlx::query(
            r#"
            UPDATE announce_work
            SET status = 'terminal_failed',
                reason = ?,
                updated_at = ?,
                finished_at = ?,
                lease_owner = NULL,
                lease_until = NULL,
                last_error_message = ?
            WHERE id = ? AND lease_owner = ? AND status = 'running'
            "#,
        )
        .bind(announce_reason_key(reason))
        .bind(now_ms)
        .bind(now_ms)
        .bind(redacted_message)
        .bind(id.as_str())
        .bind(owner)
        .execute(&self.pool)
        .await
        .map_err(|error| db_error("mark announce terminal failed", error))?;

        Ok(result.rows_affected() == 1)
    }

    pub async fn expire_announce_work(&self, now_ms: i64) -> Result<u64, DatabaseError> {
        let result = sqlx::query(
            r#"
            UPDATE announce_work
            SET status = 'expired',
                reason = 'expired',
                updated_at = ?,
                finished_at = ?,
                lease_owner = NULL,
                lease_until = NULL
            WHERE status IN ('queued', 'running', 'waiting', 'retryable')
              AND expires_at <= ?
            "#,
        )
        .bind(now_ms)
        .bind(now_ms)
        .bind(now_ms)
        .execute(&self.pool)
        .await
        .map_err(|error| db_error("expire announce work", error))?;

        Ok(result.rows_affected())
    }

    pub async fn recover_stale_announce_leases(&self, now_ms: i64) -> Result<u64, DatabaseError> {
        let result = sqlx::query(
            r#"
            UPDATE announce_work
            SET status = 'queued',
                reason = 'dependency_backoff',
                updated_at = ?,
                lease_owner = NULL,
                lease_until = NULL
            WHERE status = 'running' AND lease_until <= ?
            "#,
        )
        .bind(now_ms)
        .bind(now_ms)
        .execute(&self.pool)
        .await
        .map_err(|error| db_error("recover stale announce leases", error))?;

        Ok(result.rows_affected())
    }

    pub async fn announce_status_counts(
        &self,
        limit: u16,
    ) -> Result<Vec<AnnounceStatusCount>, DatabaseError> {
        let rows = sqlx::query(
            r#"
            SELECT status, reason, COUNT(*) AS count
            FROM announce_work
            GROUP BY status, reason
            ORDER BY count DESC, status, reason
            LIMIT ?
            "#,
        )
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(|error| db_error("read announce status counts", error))?;

        Ok(rows
            .into_iter()
            .map(|row| AnnounceStatusCount {
                status: row.get("status"),
                reason: row.get("reason"),
                count: row.get("count"),
            })
            .collect())
    }

    async fn transition_leased(
        &self,
        id: &AnnounceWorkId,
        owner: &str,
        transition: LeasedTransition<'_>,
    ) -> Result<bool, DatabaseError> {
        let (dependency_kind, dependency_name) = transition.dependency.unwrap_or(("", ""));
        let result = sqlx::query(
            r#"
            UPDATE announce_work
            SET status = ?,
                reason = ?,
                next_attempt_at = COALESCE(?, next_attempt_at),
                updated_at = ?,
                lease_owner = NULL,
                lease_until = NULL,
                last_dependency_kind = NULLIF(?, ''),
                last_dependency_name = NULLIF(?, '')
            WHERE id = ? AND lease_owner = ? AND status = 'running'
            "#,
        )
        .bind(announce_status_key(transition.status))
        .bind(announce_reason_key(transition.reason))
        .bind(transition.next_attempt_at_ms)
        .bind(transition.now_ms)
        .bind(dependency_kind)
        .bind(dependency_name)
        .bind(id.as_str())
        .bind(owner)
        .execute(&self.pool)
        .await
        .map_err(|error| db_error("transition leased announce work", error))?;

        Ok(result.rows_affected() == 1)
    }

    async fn finish_announce(
        &self,
        id: &AnnounceWorkId,
        owner: &str,
        status: AnnounceStatus,
        reason: AnnounceReason,
        outcome: Option<&str>,
        now_ms: i64,
    ) -> Result<bool, DatabaseError> {
        let result = sqlx::query(
            r#"
            UPDATE announce_work
            SET status = ?,
                reason = ?,
                updated_at = ?,
                finished_at = ?,
                lease_owner = NULL,
                lease_until = NULL,
                last_action_outcome = ?
            WHERE id = ? AND lease_owner = ? AND status = 'running'
            "#,
        )
        .bind(announce_status_key(status))
        .bind(announce_reason_key(reason))
        .bind(now_ms)
        .bind(now_ms)
        .bind(outcome)
        .bind(id.as_str())
        .bind(owner)
        .execute(&self.pool)
        .await
        .map_err(|error| db_error("finish announce work", error))?;

        Ok(result.rows_affected() == 1)
    }
}

async fn select_active_announce_id(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    dedupe_hash: &AnnounceDedupeHash,
) -> Result<Option<AnnounceWorkId>, DatabaseError> {
    let row = sqlx::query(
        r#"
        SELECT id FROM announce_work
        WHERE dedupe_hash = ?
          AND status IN ('queued', 'running', 'waiting', 'retryable')
        ORDER BY received_at
        LIMIT 1
        "#,
    )
    .bind(dedupe_hash.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|error| db_error("select active announce dedupe", error))?;

    row.map(|row| AnnounceWorkId::new(row.get::<String, _>("id")))
        .transpose()
        .map_err(|error| DatabaseError::QueryFailed {
            operation: "read announce work id".to_owned(),
            message: error.to_string(),
        })
}

async fn insert_announce_work(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    work: &AnnounceWorkItem,
) -> Result<(), DatabaseError> {
    sqlx::query(
        r#"
        INSERT INTO announce_work (
            id,
            dedupe_hash,
            received_at,
            updated_at,
            first_attempt_at,
            finished_at,
            tracker,
            guid,
            info_hash,
            title,
            size,
            status,
            reason,
            attempt_count,
            next_attempt_at,
            expires_at,
            lease_owner,
            lease_until,
            last_dependency_kind,
            last_dependency_name,
            last_error_class,
            last_error_message
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
    )
    .bind(work.id.as_str())
    .bind(work.dedupe_hash.as_str())
    .bind(work.received_at_ms)
    .bind(work.updated_at_ms)
    .bind(work.first_attempt_at_ms)
    .bind(work.finished_at_ms)
    .bind(work.tracker.as_str())
    .bind(work.guid.as_ref().map(CandidateGuid::as_str))
    .bind(work.info_hash.as_ref().map(InfoHash::as_str))
    .bind(work.title.as_str())
    .bind(
        work.size
            .map(ByteSize::get)
            .map(|size| i64_from_u64(size, "announce size"))
            .transpose()?,
    )
    .bind(announce_status_key(work.status))
    .bind(announce_reason_key(work.reason))
    .bind(i64::from(work.attempt_count))
    .bind(work.next_attempt_at_ms)
    .bind(work.expires_at_ms)
    .bind(work.lease.as_ref().map(|lease| lease.owner.as_str()))
    .bind(work.lease.as_ref().map(|lease| lease.lease_until_ms))
    .bind(work.last_dependency_kind.as_ref().map(ReasonText::as_str))
    .bind(work.last_dependency_name.as_ref().map(ReasonText::as_str))
    .bind(work.last_error_class.as_ref().map(ReasonText::as_str))
    .bind(work.last_redacted_message.as_ref().map(ReasonText::as_str))
    .execute(&mut **transaction)
    .await
    .map_err(|error| db_error("insert announce work", error))?;

    Ok(())
}

fn local_source(source: &LocalItemSource) -> (String, String) {
    match source {
        LocalItemSource::Client {
            client_host,
            source_key,
        } => ("client".to_owned(), format!("{client_host}:{source_key}")),
        LocalItemSource::TorrentCache { path } => {
            ("torrent_cache".to_owned(), path_to_string(path))
        }
        LocalItemSource::DataRoot { path } => ("data_root".to_owned(), path_to_string(path)),
        LocalItemSource::Virtual { source_key } => ("virtual".to_owned(), source_key.to_string()),
    }
}

fn path_to_string(path: impl AsRef<Path>) -> String {
    path.as_ref().to_string_lossy().into_owned()
}

fn media_type_key(media_type: MediaType) -> &'static str {
    match media_type {
        MediaType::Episode => "episode",
        MediaType::SeasonPack => "season_pack",
        MediaType::Movie => "movie",
        MediaType::Anime => "anime",
        MediaType::Video => "video",
        MediaType::Audio => "audio",
        MediaType::Book => "book",
        MediaType::Archive => "archive",
        MediaType::Unknown => "unknown",
    }
}

fn match_decision_key(decision: MatchDecision) -> &'static str {
    match decision {
        MatchDecision::Exact => "exact",
        MatchDecision::SizeOnly => "size_only",
        MatchDecision::Partial => "partial",
        MatchDecision::NoMatch => "no_match",
        MatchDecision::Rejected => "rejected",
    }
}

fn job_state_key(state: JobState) -> &'static str {
    match state {
        JobState::Pending => "pending",
        JobState::Running => "running",
        JobState::Waiting => "waiting",
        JobState::Succeeded => "succeeded",
        JobState::Failed => "failed",
        JobState::Disabled => "disabled",
    }
}

fn dependency_state_row(state: &DependencyState) -> (&'static str, Option<&str>, Option<i64>) {
    match state {
        DependencyState::Unknown => ("unknown", None, None),
        DependencyState::Healthy { .. } => ("healthy", None, None),
        DependencyState::Degraded {
            reason,
            retry_after_ms,
        } => ("degraded", Some(reason.as_str()), *retry_after_ms),
        DependencyState::Unavailable {
            reason,
            retry_after_ms,
        } => ("unavailable", Some(reason.as_str()), *retry_after_ms),
    }
}

fn announce_status_key(status: AnnounceStatus) -> &'static str {
    match status {
        AnnounceStatus::Queued => "queued",
        AnnounceStatus::Running => "running",
        AnnounceStatus::Waiting => "waiting",
        AnnounceStatus::Retryable => "retryable",
        AnnounceStatus::Succeeded => "succeeded",
        AnnounceStatus::TerminalFailed => "terminal_failed",
        AnnounceStatus::Expired => "expired",
    }
}

fn announce_reason_key(reason: AnnounceReason) -> &'static str {
    match reason {
        AnnounceReason::Accepted => "accepted",
        AnnounceReason::Deduplicated => "deduplicated",
        AnnounceReason::SourceIncomplete => "source_incomplete",
        AnnounceReason::InventoryRefreshing => "inventory_refreshing",
        AnnounceReason::DependencyBackoff => "dependency_backoff",
        AnnounceReason::CandidateDownloading => "candidate_downloading",
        AnnounceReason::ClientChecking => "client_checking",
        AnnounceReason::RetryAfter => "retry_after",
        AnnounceReason::TransientDependencyFailure => "transient_dependency_failure",
        AnnounceReason::Saved => "saved",
        AnnounceReason::Injected => "injected",
        AnnounceReason::AlreadyExists => "already_exists",
        AnnounceReason::NoMatchTerminal => "no_match_terminal",
        AnnounceReason::InvalidRequest => "invalid_request",
        AnnounceReason::UnsupportedShape => "unsupported_shape",
        AnnounceReason::UnsafePath => "unsafe_path",
        AnnounceReason::InvalidTorrentMetadata => "invalid_torrent_metadata",
        AnnounceReason::Expired => "expired",
    }
}

fn i64_from_u64(value: u64, field: &'static str) -> Result<i64, DatabaseError> {
    i64::try_from(value).map_err(|error| DatabaseError::QueryFailed {
        operation: format!("convert {field} to sqlite integer"),
        message: error.to_string(),
    })
}

fn id_from_i64(value: i64, field: &'static str) -> Result<LocalItemId, DatabaseError> {
    u64::try_from(value)
        .ok()
        .and_then(|value| LocalItemId::new(value).ok())
        .ok_or_else(|| DatabaseError::QueryFailed {
            operation: format!("read {field}"),
            message: format!("invalid positive id {value}"),
        })
}

fn remote_id_from_i64(value: i64, field: &'static str) -> Result<RemoteCandidateId, DatabaseError> {
    u64::try_from(value)
        .ok()
        .and_then(|value| RemoteCandidateId::new(value).ok())
        .ok_or_else(|| DatabaseError::QueryFailed {
            operation: format!("read {field}"),
            message: format!("invalid positive id {value}"),
        })
}

fn remote_candidate_snapshot_from_row(
    row: sqlx::sqlite::SqliteRow,
) -> Result<RemoteCandidateSnapshot, DatabaseError> {
    let indexer_id = u64::try_from(row.get::<i64, _>("indexer_id")).map_err(|error| {
        DatabaseError::QueryFailed {
            operation: "read candidate indexer id".to_owned(),
            message: error.to_string(),
        }
    })?;
    let info_hash = row
        .get::<Option<String>, _>("info_hash")
        .map(InfoHash::new)
        .transpose()
        .map_err(|error| DatabaseError::QueryFailed {
            operation: "read candidate info hash".to_owned(),
            message: error.to_string(),
        })?;

    Ok(RemoteCandidateSnapshot {
        id: remote_id_from_i64(row.get("id"), "remote candidate id")?,
        indexer_id,
        guid: CandidateGuid::new(row.get::<String, _>("guid")).map_err(|error| {
            DatabaseError::QueryFailed {
                operation: "read candidate guid".to_owned(),
                message: error.to_string(),
            }
        })?,
        title: row.get("title"),
        info_hash,
        torrent_cache_path: row
            .get::<Option<String>, _>("torrent_cache_path")
            .map(PathBuf::from),
    })
}

fn db_error(operation: &'static str, error: sqlx::Error) -> DatabaseError {
    let message = error.to_string();
    if message.contains("database is locked") || message.contains("database is busy") {
        DatabaseError::Busy {
            operation: operation.to_owned(),
            retry_after_ms: None,
        }
    } else {
        DatabaseError::QueryFailed {
            operation: operation.to_owned(),
            message,
        }
    }
}

trait SnakeCase {
    fn to_ascii_snake_case(&self) -> String;
}

impl SnakeCase for str {
    fn to_ascii_snake_case(&self) -> String {
        let mut result = String::with_capacity(self.len());
        for (index, character) in self.chars().enumerate() {
            if character.is_ascii_uppercase() {
                if index > 0 {
                    result.push('_');
                }
                result.push(character.to_ascii_lowercase());
            } else {
                result.push(character);
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::announce::{
        AnnounceDedupeIdentity, AnnounceLease, AnnounceReason, AnnounceStatus, AnnounceWorkId,
        AnnounceWorkItem,
    };
    use crate::domain::{
        CandidateGuid, ClientHost, DecisionReason, DisplayName, DownloadUrl, FileIndex, IndexerId,
        ItemTitle, MatchRatio, ReasonText, SourceKey, TrackerName,
    };
    use crate::persistence::schema::{BUSY_TIMEOUT_MS, REQUIRED_TABLES};
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[tokio::test]
    async fn file_backed_repository_initializes_schema_and_connection_pragmas() {
        let root = unique_temp_dir("sqlite-pragmas");
        let database = root.join("sporos.db");
        let repository = Repository::connect(&database).await.unwrap();

        let foreign_keys: i64 = sqlx::query_scalar("PRAGMA foreign_keys;")
            .fetch_one(repository.pool())
            .await
            .unwrap();
        let busy_timeout: i64 = sqlx::query_scalar("PRAGMA busy_timeout;")
            .fetch_one(repository.pool())
            .await
            .unwrap();
        let journal_mode: String = sqlx::query_scalar("PRAGMA journal_mode;")
            .fetch_one(repository.pool())
            .await
            .unwrap();

        assert_eq!(1, foreign_keys);
        assert_eq!(i64::from(BUSY_TIMEOUT_MS), busy_timeout);
        assert_eq!("wal", journal_mode);

        for table in REQUIRED_TABLES {
            let exists: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
            )
            .bind(table)
            .fetch_one(repository.pool())
            .await
            .unwrap();
            assert_eq!(1, exists, "{table} should be initialized");
        }

        repository.pool().close().await;
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn local_item_upsert_replaces_file_batch_in_transaction() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let item = test_local_item("Example");
        let first_file = LocalFile::new(
            None,
            PathBuf::from("Example/file-a.mkv"),
            ByteSize::new(10),
            FileIndex::new(0),
        )
        .unwrap();
        let second_file = LocalFile::new(
            None,
            PathBuf::from("Example/file-b.mkv"),
            ByteSize::new(20),
            FileIndex::new(1),
        )
        .unwrap();

        let item_id = repository
            .upsert_local_item_with_files(&item, &[first_file.clone(), second_file])
            .await
            .unwrap();
        repository
            .upsert_local_item_with_files(&item, &[first_file])
            .await
            .unwrap();

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM local_files WHERE item_id = ?")
            .bind(i64_from_u64(item_id.get(), "local item id").unwrap())
            .fetch_one(repository.pool())
            .await
            .unwrap();

        assert_eq!(1, count);
    }

    #[tokio::test]
    async fn failed_local_file_batch_rolls_back_item_and_file_replacement() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let mut item = test_local_item("Original");
        let first_file = LocalFile::new(
            None,
            PathBuf::from("Example/file-a.mkv"),
            ByteSize::new(10),
            FileIndex::new(0),
        )
        .unwrap();
        let duplicate_a = LocalFile::new(
            None,
            PathBuf::from("Example/duplicate-a.mkv"),
            ByteSize::new(20),
            FileIndex::new(0),
        )
        .unwrap();
        let duplicate_b = LocalFile::new(
            None,
            PathBuf::from("Example/duplicate-b.mkv"),
            ByteSize::new(30),
            FileIndex::new(0),
        )
        .unwrap();

        let item_id = repository
            .upsert_local_item_with_files(&item, &[first_file])
            .await
            .unwrap();
        item.title = ItemTitle::new("Should Roll Back").unwrap();
        let result = repository
            .upsert_local_item_with_files(&item, &[duplicate_a, duplicate_b])
            .await;

        let title: String = sqlx::query_scalar("SELECT title FROM local_items WHERE id = ?")
            .bind(i64_from_u64(item_id.get(), "local item id").unwrap())
            .fetch_one(repository.pool())
            .await
            .unwrap();
        let file_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM local_files WHERE item_id = ?")
                .bind(i64_from_u64(item_id.get(), "local item id").unwrap())
                .fetch_one(repository.pool())
                .await
                .unwrap();

        assert!(matches!(result, Err(DatabaseError::QueryFailed { .. })));
        assert_eq!("Original", title);
        assert_eq!(1, file_count);
    }

    #[tokio::test]
    async fn remote_candidate_upsert_uses_indexer_guid_natural_key() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let mut candidate = test_remote_candidate("guid-1", "Original");

        let first_id = repository
            .upsert_remote_candidate(&candidate)
            .await
            .unwrap();
        candidate.title = ItemTitle::new("Updated").unwrap();
        candidate.torrent_cache_path = Some(PathBuf::from("/cache/fedcba.cached.torrent"));
        let second_id = repository
            .upsert_remote_candidate(&candidate)
            .await
            .unwrap();

        let title: String = sqlx::query_scalar("SELECT title FROM remote_candidates WHERE id = ?")
            .bind(i64_from_u64(first_id.get(), "remote candidate id").unwrap())
            .fetch_one(repository.pool())
            .await
            .unwrap();
        let info_hash_matches = repository
            .remote_candidates_by_info_hash(candidate.info_hash.as_ref().unwrap(), 10)
            .await
            .unwrap();

        assert_eq!(first_id, second_id);
        assert_eq!("Updated", title);
        assert_eq!(1, info_hash_matches.len());
        assert_eq!(first_id, info_hash_matches[0].id);
        assert_eq!(
            Some(PathBuf::from("/cache/fedcba.cached.torrent")),
            info_hash_matches[0].torrent_cache_path
        );
    }

    #[tokio::test]
    async fn match_decision_uses_foreign_keys_for_failure_paths() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let assessment = CandidateAssessment {
            decision: MatchDecision::Exact,
            reason: DecisionReason::FileTreeMatched,
            matched_size: Some(ByteSize::new(42)),
            matched_ratio: Some(MatchRatio::new(1.0).unwrap()),
        };

        let result = repository
            .record_match_decision(
                LocalItemId::new(9_999).unwrap(),
                RemoteCandidateId::new(8_888).unwrap(),
                assessment,
                1_700_000_000_000,
            )
            .await;

        assert!(matches!(result, Err(DatabaseError::QueryFailed { .. })));
    }

    #[tokio::test]
    async fn match_decision_records_and_reassesses_candidate_for_local_item() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let item_id = repository
            .upsert_local_item_with_files(&test_local_item("Example"), &[])
            .await
            .unwrap();
        let candidate_id = repository
            .upsert_remote_candidate(&test_remote_candidate("guid-1", "Example"))
            .await
            .unwrap();

        repository
            .record_match_decision(
                item_id,
                candidate_id,
                CandidateAssessment {
                    decision: MatchDecision::Exact,
                    reason: DecisionReason::FileTreeMatched,
                    matched_size: Some(ByteSize::new(42)),
                    matched_ratio: Some(MatchRatio::new(1.0).unwrap()),
                },
                100,
            )
            .await
            .unwrap();
        repository
            .record_match_decision(
                item_id,
                candidate_id,
                CandidateAssessment {
                    decision: MatchDecision::NoMatch,
                    reason: DecisionReason::NameMismatch,
                    matched_size: Some(ByteSize::new(1)),
                    matched_ratio: Some(MatchRatio::new(0.1).unwrap()),
                },
                200,
            )
            .await
            .unwrap();

        let row = sqlx::query(
            "SELECT decision, matched_size, reason_code, assessed_at FROM match_decisions",
        )
        .fetch_one(repository.pool())
        .await
        .unwrap();
        let decision: String = row.get("decision");
        let matched_size: i64 = row.get("matched_size");
        let reason_code: String = row.get("reason_code");
        let assessed_at: i64 = row.get("assessed_at");
        let decision_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM match_decisions")
            .fetch_one(repository.pool())
            .await
            .unwrap();
        let decisions = repository
            .match_decisions_for_local_item(item_id, 10)
            .await
            .unwrap();

        assert_eq!(1, decision_count);
        assert_eq!("no_match", decision);
        assert_eq!(1, matched_size);
        assert_eq!("name_mismatch", reason_code);
        assert_eq!(200, assessed_at);
        assert_eq!(1, decisions.len());
        assert_eq!(candidate_id, decisions[0].candidate_id);
        assert_eq!("no_match", decisions[0].decision);
        assert_eq!(Some(1), decisions[0].matched_size);
        assert_eq!(Some(0.1), decisions[0].matched_ratio);
    }

    #[tokio::test]
    async fn delete_cascades_remove_owned_files_and_match_decisions() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let item_id = repository
            .upsert_local_item_with_files(
                &test_local_item("Example"),
                &[LocalFile::new(
                    None,
                    PathBuf::from("Example/file-a.mkv"),
                    ByteSize::new(10),
                    FileIndex::new(0),
                )
                .unwrap()],
            )
            .await
            .unwrap();
        let candidate_id = repository
            .upsert_remote_candidate(&test_remote_candidate("guid-1", "Example"))
            .await
            .unwrap();
        let assessment = CandidateAssessment {
            decision: MatchDecision::Exact,
            reason: DecisionReason::FileTreeMatched,
            matched_size: Some(ByteSize::new(10)),
            matched_ratio: Some(MatchRatio::new(1.0).unwrap()),
        };

        repository
            .record_match_decision(item_id, candidate_id, assessment, 100)
            .await
            .unwrap();
        sqlx::query("DELETE FROM local_items WHERE id = ?")
            .bind(i64_from_u64(item_id.get(), "local item id").unwrap())
            .execute(repository.pool())
            .await
            .unwrap();

        let file_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM local_files")
            .fetch_one(repository.pool())
            .await
            .unwrap();
        let decision_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM match_decisions")
            .fetch_one(repository.pool())
            .await
            .unwrap();
        assert_eq!(0, file_count);
        assert_eq!(0, decision_count);

        let next_item_id = repository
            .upsert_local_item_with_files(&test_local_item("Example 2"), &[])
            .await
            .unwrap();
        repository
            .record_match_decision(next_item_id, candidate_id, assessment, 200)
            .await
            .unwrap();
        sqlx::query("DELETE FROM remote_candidates WHERE id = ?")
            .bind(i64_from_u64(candidate_id.get(), "remote candidate id").unwrap())
            .execute(repository.pool())
            .await
            .unwrap();

        let decision_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM match_decisions")
            .fetch_one(repository.pool())
            .await
            .unwrap();
        assert_eq!(0, decision_count);
    }

    #[tokio::test]
    async fn records_jobs_and_dependency_health() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let dependency_name = DependencyName::new("indexer-main").unwrap();
        let dependency_state = DependencyState::Degraded {
            reason: ReasonText::new("rate limited").unwrap(),
            retry_after_ms: Some(123),
        };

        repository
            .record_job_status(
                &JobName::new("rss").unwrap(),
                JobStateUpdate {
                    state: JobState::Running,
                    last_started_at_ms: Some(100),
                    last_finished_at_ms: None,
                    next_run_at_ms: Some(456),
                    last_error: None,
                },
            )
            .await
            .unwrap();
        repository
            .record_job_status(
                &JobName::new("rss").unwrap(),
                JobStateUpdate {
                    state: JobState::Waiting,
                    last_started_at_ms: None,
                    last_finished_at_ms: Some(200),
                    next_run_at_ms: Some(456),
                    last_error: Some("rate limited"),
                },
            )
            .await
            .unwrap();
        repository
            .upsert_job_state(
                &JobName::new("cleanup").unwrap(),
                JobState::Pending,
                Some(10),
                None,
            )
            .await
            .unwrap();
        repository
            .upsert_job_state(
                &JobName::new("disabled").unwrap(),
                JobState::Disabled,
                Some(1),
                None,
            )
            .await
            .unwrap();
        repository
            .record_dependency_health("indexer", &dependency_name, &dependency_state, 789)
            .await
            .unwrap();
        repository
            .record_dependency_health(
                "client",
                &DependencyName::new("qbit").unwrap(),
                &DependencyState::Healthy { checked_at_ms: 900 },
                900,
            )
            .await
            .unwrap();

        let ready = repository.ready_jobs(10, 10).await.unwrap();
        let jobs = repository.job_status_snapshot(10).await.unwrap();
        let health = repository.dependency_health_snapshot(1).await.unwrap();

        assert_eq!(vec![JobName::new("cleanup").unwrap()], ready);
        assert_eq!(3, jobs.len());
        let rss = jobs.iter().find(|job| job.name.as_str() == "rss").unwrap();
        assert_eq!("waiting", rss.state);
        assert_eq!(Some(100), rss.last_started_at_ms);
        assert_eq!(Some(200), rss.last_finished_at_ms);
        assert_eq!(Some(456), rss.next_run_at_ms);
        assert_eq!(Some("rate limited".to_owned()), rss.last_error);
        assert_eq!(1, health.len());
        assert_eq!("client", health[0].dependency_type);
        assert_eq!("healthy", health[0].state);
    }

    #[tokio::test]
    async fn announce_insert_deduplicates_and_enforces_capacity() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let work = test_announce_work("ann_01", "guid-1", 1);
        let duplicate = test_announce_work("ann_02", "guid-1", 2);
        let other = test_announce_work("ann_03", "guid-2", 3);

        let inserted = repository
            .insert_or_dedupe_announce_work(&work, 1)
            .await
            .unwrap();
        let deduped = repository
            .insert_or_dedupe_announce_work(&duplicate, 1)
            .await
            .unwrap();
        let full = repository.insert_or_dedupe_announce_work(&other, 1).await;

        assert_eq!(
            AnnounceInsertResult::Inserted {
                id: AnnounceWorkId::new("ann_01").unwrap()
            },
            inserted
        );
        assert_eq!(
            AnnounceInsertResult::Deduplicated {
                id: AnnounceWorkId::new("ann_01").unwrap()
            },
            deduped
        );
        assert!(matches!(full, Err(DatabaseError::Busy { .. })));
    }

    #[tokio::test]
    async fn announce_claim_lease_retry_and_success_flow() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let work = test_announce_work("ann_10", "guid-10", 1);
        repository
            .insert_or_dedupe_announce_work(&work, 10)
            .await
            .unwrap();

        let claimed = repository
            .claim_announce_work("worker-1", 10, 100, 5)
            .await
            .unwrap();
        assert_eq!(vec![AnnounceWorkId::new("ann_10").unwrap()], claimed);
        assert!(
            repository
                .renew_announce_lease(&claimed[0], "worker-1", 120, 20)
                .await
                .unwrap()
        );
        assert!(
            repository
                .mark_announce_retryable(
                    &claimed[0],
                    "worker-1",
                    AnnounceRetryUpdate {
                        reason: AnnounceReason::RetryAfter,
                        next_attempt_at_ms: 50,
                        now_ms: 25,
                        error_class: "retryable_dependency",
                        redacted_message: "rate limited",
                    },
                )
                .await
                .unwrap()
        );

        let early = repository
            .claim_announce_work("worker-1", 49, 150, 5)
            .await
            .unwrap();
        let ready = repository
            .claim_announce_work("worker-1", 50, 150, 5)
            .await
            .unwrap();
        assert!(early.is_empty());
        assert_eq!(vec![AnnounceWorkId::new("ann_10").unwrap()], ready);
        assert!(
            repository
                .mark_announce_succeeded(
                    &ready[0],
                    "worker-1",
                    AnnounceReason::Injected,
                    "injected",
                    60
                )
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn announce_expiry_recovery_and_status_snapshots_work() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let mut expired = test_announce_work("ann_20", "guid-20", 1);
        expired.expires_at_ms = 5;
        let mut running = test_announce_work("ann_21", "guid-21", 1);
        running.status = AnnounceStatus::Running;
        running.lease =
            Some(AnnounceLease::new(ReasonText::new("worker-1").unwrap(), 5, 1).unwrap());

        repository
            .insert_or_dedupe_announce_work(&expired, 10)
            .await
            .unwrap();
        repository
            .insert_or_dedupe_announce_work(&running, 10)
            .await
            .unwrap();

        assert_eq!(1, repository.expire_announce_work(10).await.unwrap());
        assert_eq!(
            1,
            repository.recover_stale_announce_leases(10).await.unwrap()
        );

        let stats = repository.announce_status_counts(10).await.unwrap();

        assert!(
            stats
                .iter()
                .any(|count| count.status == "expired" && count.count == 1)
        );
        assert!(
            stats
                .iter()
                .any(|count| count.status == "queued" && count.count == 1)
        );
    }

    fn test_local_item(title: &str) -> LocalItem {
        LocalItem {
            id: None,
            source: LocalItemSource::Client {
                client_host: ClientHost::new("qbit.local").unwrap(),
                source_key: SourceKey::new("0123456789abcdef0123456789abcdef01234567").unwrap(),
            },
            title: ItemTitle::new(title).unwrap(),
            display_name: DisplayName::new(title).unwrap(),
            media_type: MediaType::Movie,
            info_hash: Some(InfoHash::new("0123456789abcdef0123456789abcdef01234567").unwrap()),
            path: None,
            save_path: Some(PathBuf::from("/downloads")),
            total_size: ByteSize::new(30),
            mtime_ms: Some(1_700_000_000_000),
        }
    }

    fn test_remote_candidate(guid: &str, title: &str) -> RemoteCandidate {
        RemoteCandidate {
            id: None,
            indexer_id: IndexerId::new(1).unwrap(),
            guid: CandidateGuid::new(guid).unwrap(),
            download_url: DownloadUrl::new("https://indexer.example/download").unwrap(),
            title: ItemTitle::new(title).unwrap(),
            tracker: TrackerName::new("tracker.example").unwrap(),
            size: Some(ByteSize::new(42)),
            published_at_ms: Some(1_700_000_000_000),
            info_hash: Some(InfoHash::new("fedcba9876543210fedcba9876543210fedcba98").unwrap()),
            torrent_cache_path: None,
        }
    }

    fn test_announce_work(id: &str, guid: &str, received_at_ms: i64) -> AnnounceWorkItem {
        let tracker = TrackerName::new("tracker.example").unwrap();
        let guid = CandidateGuid::new(guid).unwrap();
        let dedupe_hash = AnnounceDedupeIdentity::Guid {
            tracker: tracker.clone(),
            guid: guid.clone(),
        }
        .hash();

        AnnounceWorkItem {
            id: AnnounceWorkId::new(id).unwrap(),
            status: AnnounceStatus::Queued,
            reason: AnnounceReason::Accepted,
            dedupe_hash,
            title: ItemTitle::new("Example").unwrap(),
            tracker,
            guid: Some(guid),
            info_hash: None,
            size: Some(ByteSize::new(42)),
            received_at_ms,
            updated_at_ms: received_at_ms,
            first_attempt_at_ms: None,
            finished_at_ms: None,
            attempt_count: 0,
            next_attempt_at_ms: received_at_ms,
            expires_at_ms: 1_000,
            lease: None,
            last_dependency_kind: None,
            last_dependency_name: None,
            last_error_class: None,
            last_redacted_message: None,
        }
    }

    fn unique_temp_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("sporos-repository-test-{label}-{nanos}"));
        fs::create_dir_all(&path).unwrap();
        path
    }
}
