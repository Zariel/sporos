use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Executor, Row, Sqlite, SqlitePool, Transaction};
use tracing::{debug_span, info_span};

use crate::announce::{
    AnnounceDedupeHash, AnnounceFetchMaterial, AnnounceReason, AnnounceStatus, AnnounceWorkId,
    AnnounceWorkItem,
};
use crate::domain::{
    ByteSize, CandidateAssessment, CandidateGuid, ClientHost, DependencyName, DependencyState,
    DisplayName, DownloadUrl, FileIndex, IndexerId, InfoHash, ItemTitle, JobName, JobState,
    LocalFile, LocalItem, LocalItemId, LocalItemSource, MatchDecision, MediaType, ReasonText,
    RemoteCandidate, RemoteCandidateId, SourceKey,
};
use crate::errors::DatabaseError;
use crate::indexers::{ConfiguredTorznabIndexer, TorznabCaps};
use crate::secrets::{CookieSecret, sanitize_url_for_logging};

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

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AnnounceAttemptCount {
    pub outcome_class: String,
    pub attempts: i64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AnnounceDependencyWaitCount {
    pub dependency_kind: String,
    pub dependency_name: String,
    pub count: i64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AnnounceQueueSnapshot {
    pub active_count: i64,
    pub oldest_active_age_ms: Option<i64>,
    pub next_retry_delay_ms: Option<i64>,
    pub running_leases: i64,
    pub status_counts: Vec<AnnounceStatusCount>,
    pub attempt_counts: Vec<AnnounceAttemptCount>,
    pub dependency_wait_counts: Vec<AnnounceDependencyWaitCount>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct LocalItemFileBatch<'a> {
    pub item: &'a LocalItem,
    pub files: &'a [LocalFile],
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum LocalInventoryScope {
    Client { client_host: ClientHost },
    DataRoot,
    TorrentCache,
    Virtual,
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub struct LocalInventoryReplaceSummary {
    pub upserted: usize,
    pub pruned: u64,
}

impl LocalInventoryScope {
    fn accepts(&self, source_type: &str, source_key: &str) -> bool {
        match self {
            Self::Client { client_host } => {
                source_type == "client"
                    && source_key.starts_with(&client_source_key_prefix(client_host))
            }
            Self::DataRoot => source_type == "data_root",
            Self::TorrentCache => source_type == "torrent_cache",
            Self::Virtual => source_type == "virtual",
        }
    }

    fn source_type(&self) -> &'static str {
        match self {
            Self::Client { .. } => "client",
            Self::DataRoot => "data_root",
            Self::TorrentCache => "torrent_cache",
            Self::Virtual => "virtual",
        }
    }

    fn source_key_prefix(&self) -> Option<String> {
        match self {
            Self::Client { client_host } => Some(client_source_key_prefix(client_host)),
            Self::DataRoot | Self::TorrentCache | Self::Virtual => None,
        }
    }
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
pub struct IndexerRegistryRow {
    pub id: u64,
    pub name: DependencyName,
    pub url: String,
    pub api_key_source: String,
    pub enabled: bool,
    pub state: String,
    pub retry_after_ms: Option<i64>,
    pub last_caps_refresh_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct SearchHistoryRow {
    pub local_item_id: LocalItemId,
    pub indexer_id: IndexerId,
    pub first_searched_at_ms: i64,
    pub last_searched_at_ms: i64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct LocalFileSnapshot {
    pub item_id: LocalItemId,
    pub relative_path: PathBuf,
    pub file_name: String,
    pub size: ByteSize,
    pub mtime_ms: Option<i64>,
    pub file_index: FileIndex,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct LocalFilePage {
    pub files: Vec<LocalFileSnapshot>,
    pub truncated: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RemoteCandidateSnapshot {
    pub id: RemoteCandidateId,
    pub indexer_id: u64,
    pub guid: CandidateGuid,
    pub redacted_download_url: String,
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

#[derive(Debug, Clone, Eq, PartialEq)]
struct AnnounceDependencyScheduleRow {
    id: AnnounceWorkId,
    status: String,
    next_attempt_at_ms: i64,
    dependency_state: Option<String>,
    retry_after_ms: Option<i64>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum AnnounceDependencyScheduleAction {
    None,
    Wait {
        reason: AnnounceReason,
        next_attempt_at_ms: i64,
    },
    Probe,
    ClearDependency,
}

impl Repository {
    pub async fn connect(path: impl AsRef<Path>) -> Result<Self, DatabaseError> {
        let path = path.as_ref();
        let _span = info_span!("sqlite.connect", database_path = %path.display());
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
        let _span = info_span!("sqlite.connect", database_path = ":memory:");
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

        let item_id =
            upsert_local_item_with_files_in_transaction(&mut transaction, item, files).await?;

        transaction
            .commit()
            .await
            .map_err(|error| db_error("commit local item transaction", error))?;

        Ok(item_id)
    }

    pub async fn replace_local_inventory(
        &self,
        scope: LocalInventoryScope,
        items: &[LocalItemFileBatch<'_>],
    ) -> Result<LocalInventoryReplaceSummary, DatabaseError> {
        self.replace_local_inventory_stream(scope, items.iter().copied())
            .await
    }

    pub async fn replace_local_inventory_stream<'a, I>(
        &self,
        scope: LocalInventoryScope,
        items: I,
    ) -> Result<LocalInventoryReplaceSummary, DatabaseError>
    where
        I: IntoIterator<Item = LocalItemFileBatch<'a>> + Send,
        I::IntoIter: Send,
    {
        let _span = info_span!("inventory.replace", source_type = scope.source_type());
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|error| db_error("begin local inventory transaction", error))?;

        if let LocalInventoryScope::Client { client_host } = &scope {
            normalize_client_source_keys(&mut transaction, client_host).await?;
        }
        initialize_retained_keys(&mut transaction).await?;

        let mut upserted = 0usize;
        for batch in items {
            let (source_type, source_key) = local_source(&batch.item.source);
            if !scope.accepts(&source_type, &source_key) {
                return Err(DatabaseError::QueryFailed {
                    operation: "validate local inventory refresh scope".to_owned(),
                    message: format!("item source {source_type}:{source_key} is outside {scope:?}"),
                });
            }

            upsert_local_item_with_files_in_transaction(&mut transaction, batch.item, batch.files)
                .await?;
            insert_retained_key(&mut transaction, &source_key).await?;
            upserted = upserted.saturating_add(1);
        }

        let pruned = prune_local_items_not_retained(&mut transaction, &scope).await?;

        clear_retained_keys(&mut transaction).await?;
        transaction
            .commit()
            .await
            .map_err(|error| db_error("commit local inventory transaction", error))?;

        Ok(LocalInventoryReplaceSummary { upserted, pruned })
    }

    pub async fn local_items_by_info_hash(
        &self,
        info_hash: &InfoHash,
        limit: u16,
    ) -> Result<Vec<LocalItem>, DatabaseError> {
        let rows = sqlx::query(
            r#"
            SELECT id, source_type, source_key, title, display_name, media_type,
                   info_hash, path, save_path, total_size, mtime_ms
            FROM local_items
            WHERE info_hash = ?
            ORDER BY source_type, source_key
            LIMIT ?
            "#,
        )
        .bind(info_hash.as_str())
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(|error| db_error("lookup local items by info hash", error))?;

        rows.into_iter().map(local_item_from_row).collect()
    }

    pub async fn local_items_by_media_type(
        &self,
        media_type: MediaType,
        limit: u16,
    ) -> Result<Vec<LocalItem>, DatabaseError> {
        let rows = sqlx::query(
            r#"
            SELECT id, source_type, source_key, title, display_name, media_type,
                   info_hash, path, save_path, total_size, mtime_ms
            FROM local_items
            WHERE media_type = ?
            ORDER BY title, source_type, source_key
            LIMIT ?
            "#,
        )
        .bind(media_type_key(media_type))
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(|error| db_error("lookup local items by media type", error))?;

        rows.into_iter().map(local_item_from_row).collect()
    }

    pub async fn local_items_by_media_type_and_title_token(
        &self,
        media_type: MediaType,
        title_token: &str,
        limit: u16,
    ) -> Result<Vec<LocalItem>, DatabaseError> {
        let rows = sqlx::query(
            r#"
            SELECT id, source_type, source_key, title, display_name, media_type,
                   info_hash, path, save_path, total_size, mtime_ms
            FROM local_items
            WHERE media_type = ?
              AND title LIKE '%' || ? || '%'
            ORDER BY title, source_type, source_key
            LIMIT ?
            "#,
        )
        .bind(media_type_key(media_type))
        .bind(title_token)
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(|error| db_error("lookup local items by media type and title", error))?;

        rows.into_iter().map(local_item_from_row).collect()
    }

    pub async fn local_files_for_item(
        &self,
        item_id: LocalItemId,
        limit: u16,
    ) -> Result<Vec<LocalFileSnapshot>, DatabaseError> {
        Ok(self.local_files_for_item_page(item_id, limit).await?.files)
    }

    pub async fn local_files_for_item_page(
        &self,
        item_id: LocalItemId,
        limit: u16,
    ) -> Result<LocalFilePage, DatabaseError> {
        let rows = sqlx::query(
            r#"
            SELECT item_id, relative_path, file_name, size, mtime_ms, file_index
            FROM local_files
            WHERE item_id = ?
            ORDER BY file_index
            LIMIT ?
            "#,
        )
        .bind(i64_from_u64(item_id.get(), "local item id")?)
        .bind(i64::from(limit) + 1)
        .fetch_all(&self.pool)
        .await
        .map_err(|error| db_error("lookup local files by item", error))?;

        let mut files = rows
            .into_iter()
            .map(local_file_snapshot_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        let truncated = files.len() > usize::from(limit);
        if truncated {
            files.truncate(usize::from(limit));
        }

        Ok(LocalFilePage { files, truncated })
    }

    pub async fn local_files_by_size(
        &self,
        size: ByteSize,
        limit: u16,
    ) -> Result<Vec<LocalFileSnapshot>, DatabaseError> {
        let rows = sqlx::query(
            r#"
            SELECT item_id, relative_path, file_name, size, mtime_ms, file_index
            FROM local_files
            WHERE size = ?
            ORDER BY item_id, file_index
            LIMIT ?
            "#,
        )
        .bind(i64_from_u64(size.get(), "local file size")?)
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(|error| db_error("lookup local files by size", error))?;

        rows.into_iter().map(local_file_snapshot_from_row).collect()
    }

    pub async fn local_files_by_size_and_name(
        &self,
        size: ByteSize,
        file_name: &str,
        limit: u16,
    ) -> Result<Vec<LocalFileSnapshot>, DatabaseError> {
        let rows = sqlx::query(
            r#"
            SELECT item_id, relative_path, file_name, size, mtime_ms, file_index
            FROM local_files
            WHERE size = ? AND file_name = ?
            ORDER BY item_id, file_index
            LIMIT ?
            "#,
        )
        .bind(i64_from_u64(size.get(), "local file size")?)
        .bind(file_name)
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(|error| db_error("lookup local files by size and name", error))?;

        rows.into_iter().map(local_file_snapshot_from_row).collect()
    }

    pub async fn local_files_by_relative_path(
        &self,
        relative_path: &Path,
        limit: u16,
    ) -> Result<Vec<LocalFileSnapshot>, DatabaseError> {
        let rows = sqlx::query(
            r#"
            SELECT item_id, relative_path, file_name, size, mtime_ms, file_index
            FROM local_files
            WHERE relative_path = ?
            ORDER BY item_id, file_index
            LIMIT ?
            "#,
        )
        .bind(path_to_string(relative_path))
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(|error| db_error("lookup local files by relative path", error))?;

        rows.into_iter().map(local_file_snapshot_from_row).collect()
    }

    pub async fn upsert_remote_candidate(
        &self,
        candidate: &RemoteCandidate,
    ) -> Result<RemoteCandidateId, DatabaseError> {
        let _span = debug_span!(
            "candidate.upsert",
            indexer_id = candidate.indexer_id.get(),
            candidate_guid = %candidate.guid,
            tracker = %candidate.tracker,
            info_hash_prefix = candidate
                .info_hash
                .as_ref()
                .map(info_hash_prefix)
                .unwrap_or_default()
        );
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
                redacted_download_url,
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
                redacted_download_url = excluded.redacted_download_url,
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
        .bind(sanitize_url_for_logging(candidate.download_url.as_str()).to_string())
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
        let _span = debug_span!(
            "match_decision.record",
            local_item_id = local_item_id.get(),
            candidate_id = candidate_id.get(),
            decision = match_decision_key(assessment.decision)
        );
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
            SELECT id, indexer_id, guid, redacted_download_url, title, info_hash, torrent_cache_path
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
        let _span = debug_span!(
            "job_status.record",
            job_name = %name,
            job_state = job_state_key(update.state)
        );
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
        let _span = debug_span!(
            "dependency_health.record",
            dependency_type,
            dependency_name = %dependency_name,
            dependency_state = state_key
        );
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

        if matches!(
            state,
            DependencyState::Healthy { .. } | DependencyState::Unknown
        ) {
            self.wake_announce_dependency_recovery(
                dependency_type,
                dependency_name,
                checked_at_ms,
                1_000,
            )
            .await?;
        }

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

    pub async fn sync_torznab_indexers(
        &self,
        configured: &[ConfiguredTorznabIndexer],
        now_ms: i64,
    ) -> Result<Vec<IndexerRegistryRow>, DatabaseError> {
        let _span = info_span!("indexers.sync", configured_count = configured.len());
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|error| db_error("begin indexer sync transaction", error))?;
        let configured_names = configured
            .iter()
            .map(|indexer| indexer.name.as_str().to_owned())
            .collect::<BTreeSet<_>>();

        for indexer in configured {
            sqlx::query(
                r#"
                INSERT INTO indexers (
                    name,
                    url,
                    api_key_source,
                    enabled,
                    capabilities_json,
                    state,
                    retry_after,
                    last_caps_refresh_at,
                    created_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, '{}', 'unknown', NULL, NULL, ?, ?)
                ON CONFLICT (name) DO UPDATE SET
                    url = excluded.url,
                    api_key_source = excluded.api_key_source,
                    enabled = excluded.enabled,
                    state = CASE
                        WHEN indexers.state = 'unknown_error' THEN 'unknown'
                        ELSE indexers.state
                    END,
                    updated_at = excluded.updated_at
                "#,
            )
            .bind(indexer.name.as_str())
            .bind(indexer.url.as_str())
            .bind(indexer.api_key_source.storage_value())
            .bind(if indexer.enabled { 1_i64 } else { 0_i64 })
            .bind(now_ms)
            .bind(now_ms)
            .execute(&mut *transaction)
            .await
            .map_err(|error| db_error("upsert configured indexer", error))?;
        }

        let existing_rows = sqlx::query("SELECT name FROM indexers")
            .fetch_all(&mut *transaction)
            .await
            .map_err(|error| db_error("read existing indexer names", error))?;
        for row in existing_rows {
            let name: String = row.get("name");
            if !configured_names.contains(&name) {
                sqlx::query("UPDATE indexers SET enabled = 0, updated_at = ? WHERE name = ?")
                    .bind(now_ms)
                    .bind(name)
                    .execute(&mut *transaction)
                    .await
                    .map_err(|error| db_error("disable unconfigured indexer", error))?;
            }
        }

        transaction
            .commit()
            .await
            .map_err(|error| db_error("commit indexer sync transaction", error))?;

        self.indexer_registry_snapshot(1_000).await
    }

    pub async fn indexer_registry_snapshot(
        &self,
        limit: u16,
    ) -> Result<Vec<IndexerRegistryRow>, DatabaseError> {
        let rows = sqlx::query(
            r#"
            SELECT id, name, url, api_key_source, enabled, state, retry_after, last_caps_refresh_at
            FROM indexers
            ORDER BY name
            LIMIT ?
            "#,
        )
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(|error| db_error("read indexer registry snapshot", error))?;

        rows.into_iter()
            .map(|row| {
                let id = u64::try_from(row.get::<i64, _>("id")).map_err(|error| {
                    DatabaseError::QueryFailed {
                        operation: "read indexer id".to_owned(),
                        message: error.to_string(),
                    }
                })?;
                Ok(IndexerRegistryRow {
                    id,
                    name: DependencyName::new(row.get::<String, _>("name")).map_err(|error| {
                        DatabaseError::QueryFailed {
                            operation: "read indexer name".to_owned(),
                            message: error.to_string(),
                        }
                    })?,
                    url: row.get("url"),
                    api_key_source: row.get("api_key_source"),
                    enabled: row.get::<i64, _>("enabled") != 0,
                    state: row.get("state"),
                    retry_after_ms: row.get("retry_after"),
                    last_caps_refresh_at_ms: row.get("last_caps_refresh_at"),
                })
            })
            .collect()
    }

    pub async fn record_indexer_caps_success(
        &self,
        name: &DependencyName,
        caps: &TorznabCaps,
        refreshed_at_ms: i64,
    ) -> Result<(), DatabaseError> {
        let caps_json =
            serde_json::to_string(caps).map_err(|error| DatabaseError::QueryFailed {
                operation: "serialize indexer caps".to_owned(),
                message: error.to_string(),
            })?;
        sqlx::query(
            r#"
            UPDATE indexers
            SET capabilities_json = ?,
                state = 'healthy',
                retry_after = NULL,
                last_caps_refresh_at = ?,
                updated_at = ?
            WHERE name = ?
            "#,
        )
        .bind(caps_json)
        .bind(refreshed_at_ms)
        .bind(refreshed_at_ms)
        .bind(name.as_str())
        .execute(&self.pool)
        .await
        .map_err(|error| db_error("record indexer caps success", error))?;
        self.record_dependency_health(
            "indexer",
            name,
            &DependencyState::Healthy {
                checked_at_ms: refreshed_at_ms,
            },
            refreshed_at_ms,
        )
        .await
    }

    pub async fn search_history_for_item(
        &self,
        local_item_id: LocalItemId,
        limit: u16,
    ) -> Result<Vec<SearchHistoryRow>, DatabaseError> {
        let rows = sqlx::query(
            r#"
            SELECT local_item_id, indexer_id, first_searched_at, last_searched_at
            FROM search_history
            WHERE local_item_id = ?
            ORDER BY indexer_id
            LIMIT ?
            "#,
        )
        .bind(i64_from_u64(local_item_id.get(), "local item id")?)
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(|error| db_error("read search history for item", error))?;

        rows.into_iter()
            .map(|row| {
                Ok(SearchHistoryRow {
                    local_item_id: id_from_i64(row.get("local_item_id"), "search history item id")?,
                    indexer_id: indexer_id_from_i64(
                        row.get("indexer_id"),
                        "search history indexer id",
                    )?,
                    first_searched_at_ms: row.get("first_searched_at"),
                    last_searched_at_ms: row.get("last_searched_at"),
                })
            })
            .collect()
    }

    pub async fn record_search_history(
        &self,
        local_item_id: LocalItemId,
        indexer_id: IndexerId,
        searched_at_ms: i64,
        rate_limited: bool,
    ) -> Result<(), DatabaseError> {
        if rate_limited {
            return Ok(());
        }

        sqlx::query(
            r#"
            INSERT INTO search_history (
                local_item_id,
                indexer_id,
                first_searched_at,
                last_searched_at
            )
            VALUES (?, ?, ?, ?)
            ON CONFLICT (local_item_id, indexer_id) DO UPDATE SET
                first_searched_at = MIN(search_history.first_searched_at, excluded.first_searched_at),
                last_searched_at = MAX(search_history.last_searched_at, excluded.last_searched_at)
            "#,
        )
        .bind(i64_from_u64(local_item_id.get(), "local item id")?)
        .bind(i64_from_u64(indexer_id.get(), "indexer id")?)
        .bind(searched_at_ms)
        .bind(searched_at_ms)
        .execute(&self.pool)
        .await
        .map_err(|error| db_error("record search history", error))?;

        Ok(())
    }

    pub async fn record_indexer_caps_failure(
        &self,
        name: &DependencyName,
        reason: &ReasonText,
        retry_after_ms: Option<i64>,
        checked_at_ms: i64,
    ) -> Result<(), DatabaseError> {
        sqlx::query(
            r#"
            UPDATE indexers
            SET state = 'degraded',
                retry_after = ?,
                updated_at = ?
            WHERE name = ?
            "#,
        )
        .bind(retry_after_ms)
        .bind(checked_at_ms)
        .bind(name.as_str())
        .execute(&self.pool)
        .await
        .map_err(|error| db_error("record indexer caps failure", error))?;
        self.record_dependency_health(
            "indexer",
            name,
            &DependencyState::Degraded {
                reason: reason.clone(),
                retry_after_ms,
            },
            checked_at_ms,
        )
        .await
    }

    pub async fn record_indexer_request_backoff(
        &self,
        name: &DependencyName,
        reason: &ReasonText,
        retry_after_ms: i64,
        checked_at_ms: i64,
        unavailable: bool,
    ) -> Result<(), DatabaseError> {
        let state = if unavailable {
            "unavailable"
        } else {
            "degraded"
        };
        sqlx::query(
            r#"
            UPDATE indexers
            SET state = ?,
                retry_after = ?,
                updated_at = ?
            WHERE name = ?
            "#,
        )
        .bind(state)
        .bind(retry_after_ms)
        .bind(checked_at_ms)
        .bind(name.as_str())
        .execute(&self.pool)
        .await
        .map_err(|error| db_error("record indexer request backoff", error))?;

        let dependency_state = if unavailable {
            DependencyState::Unavailable {
                reason: reason.clone(),
                retry_after_ms: Some(retry_after_ms),
            }
        } else {
            DependencyState::Degraded {
                reason: reason.clone(),
                retry_after_ms: Some(retry_after_ms),
            }
        };
        self.record_dependency_health("indexer", name, &dependency_state, checked_at_ms)
            .await
    }

    pub async fn insert_or_dedupe_announce_work(
        &self,
        work: &AnnounceWorkItem,
        max_pending: u32,
    ) -> Result<AnnounceInsertResult, DatabaseError> {
        let _span = info_span!(
            "announce.accept",
            announce_id = %work.id,
            tracker = %work.tracker,
            candidate_guid = work.guid.as_ref().map(CandidateGuid::as_str).unwrap_or(""),
            info_hash_prefix = work
                .info_hash
                .as_ref()
                .map(info_hash_prefix)
                .unwrap_or_default()
        );
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
        let _span = debug_span!(
            "announce.claim",
            lease_owner = owner,
            claim_limit = limit,
            lease_until_ms
        );
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
            let was_claimed = sqlx::query(
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
                  AND status IN ('queued', 'retryable')
                  AND next_attempt_at <= ?
                  AND expires_at > ?
                "#,
            )
            .bind(now_ms)
            .bind(now_ms)
            .bind(owner)
            .bind(lease_until_ms)
            .bind(id.as_str())
            .bind(now_ms)
            .bind(now_ms)
            .execute(&mut *transaction)
            .await
            .map_err(|error| db_error("claim announce work", error))
            .and_then(|result| {
                if result.rows_affected() == 1 {
                    Ok(true)
                } else if result.rows_affected() == 0 {
                    Ok(false)
                } else {
                    Err(DatabaseError::QueryFailed {
                        operation: "claim announce work".to_owned(),
                        message: format!(
                            "expected to claim at most one row for {}, claimed {}",
                            id.as_str(),
                            result.rows_affected()
                        ),
                    })
                }
            })?;
            if was_claimed {
                claimed.push(id);
            }
        }

        transaction
            .commit()
            .await
            .map_err(|error| db_error("commit announce claim transaction", error))?;

        Ok(claimed)
    }

    pub async fn announce_fetch_material(
        &self,
        id: &AnnounceWorkId,
    ) -> Result<Option<AnnounceFetchMaterial>, DatabaseError> {
        let row = sqlx::query(
            r#"
            SELECT download_url, cookie FROM announce_work
            WHERE id = ?
            "#,
        )
        .bind(id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| db_error("read announce fetch material", error))?;

        let Some(row) = row else {
            return Ok(None);
        };
        let Some(download_url) = row.get::<Option<String>, _>("download_url") else {
            return Ok(None);
        };
        let download_url =
            DownloadUrl::new(download_url).map_err(|error| DatabaseError::QueryFailed {
                operation: "read announce download url".to_owned(),
                message: error.to_string(),
            })?;
        let cookie = row
            .get::<Option<String>, _>("cookie")
            .map(CookieSecret::new)
            .transpose()
            .map_err(|error| DatabaseError::QueryFailed {
                operation: "read announce cookie".to_owned(),
                message: error.to_string(),
            })?;
        let material = AnnounceFetchMaterial::new(&download_url, cookie).map_err(|error| {
            DatabaseError::QueryFailed {
                operation: "read announce fetch material".to_owned(),
                message: error.to_string(),
            }
        })?;

        Ok(Some(material))
    }

    pub async fn schedule_announce_dependency_backoff(
        &self,
        now_ms: i64,
        recovery_probe_interval_ms: i64,
        limit: u16,
    ) -> Result<u64, DatabaseError> {
        let rows = sqlx::query(
            r#"
            SELECT
                work.id,
                work.status,
                work.next_attempt_at,
                health.state AS dependency_state,
                health.retry_after
            FROM announce_work work
            LEFT JOIN dependency_health health
              ON health.dependency_type = work.last_dependency_kind
             AND health.dependency_name = work.last_dependency_name
            WHERE work.status IN ('queued', 'retryable', 'waiting')
              AND work.expires_at > ?
              AND work.last_dependency_kind IS NOT NULL
              AND work.last_dependency_name IS NOT NULL
            ORDER BY work.next_attempt_at, work.received_at
            LIMIT ?
            "#,
        )
        .bind(now_ms)
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(|error| db_error("read announce dependency waits", error))?;

        let mut updated = 0_u64;
        for row in rows {
            let row = AnnounceDependencyScheduleRow {
                id: AnnounceWorkId::new(row.get::<String, _>("id")).map_err(|error| {
                    DatabaseError::QueryFailed {
                        operation: "read announce work id".to_owned(),
                        message: error.to_string(),
                    }
                })?,
                status: row.get("status"),
                next_attempt_at_ms: row.get("next_attempt_at"),
                dependency_state: row.get("dependency_state"),
                retry_after_ms: row.get("retry_after"),
            };
            let action = announce_dependency_schedule_action(
                &row,
                now_ms,
                recovery_probe_interval_ms.max(1),
            );
            updated += self
                .apply_announce_dependency_schedule(&row.id, action, now_ms)
                .await?;
        }

        Ok(updated)
    }

    pub async fn wake_announce_inventory_refresh(
        &self,
        now_ms: i64,
        limit: u16,
    ) -> Result<u64, DatabaseError> {
        let _span = debug_span!("announce.wake_inventory_refresh", limit);
        let result = sqlx::query(
            r#"
            UPDATE announce_work
            SET status = 'queued',
                next_attempt_at = ?,
                updated_at = ?
            WHERE id IN (
                SELECT id FROM announce_work
                WHERE status = 'waiting'
                  AND expires_at > ?
                  AND reason IN ('source_incomplete', 'inventory_refreshing')
                  AND last_dependency_kind IS NULL
                  AND last_dependency_name IS NULL
                ORDER BY next_attempt_at, received_at
                LIMIT ?
            )
            "#,
        )
        .bind(now_ms)
        .bind(now_ms)
        .bind(now_ms)
        .bind(i64::from(limit))
        .execute(&self.pool)
        .await
        .map_err(|error| db_error("wake inventory announce work", error))?;

        Ok(result.rows_affected())
    }

    pub async fn wake_announce_client_source_completion(
        &self,
        client_host: &ClientHost,
        now_ms: i64,
        limit: u16,
    ) -> Result<u64, DatabaseError> {
        let _span = debug_span!(
            "announce.wake_client_source_completion",
            client_host = %client_host,
            limit
        );
        let result = sqlx::query(
            r#"
            UPDATE announce_work
            SET status = 'queued',
                next_attempt_at = ?,
                updated_at = ?,
                last_dependency_kind = NULL,
                last_dependency_name = NULL
            WHERE id IN (
                SELECT id FROM announce_work
                WHERE status = 'waiting'
                  AND expires_at > ?
                  AND reason IN ('source_incomplete', 'client_checking')
                  AND (
                      last_dependency_kind IS NULL
                      OR (
                          last_dependency_kind = 'client'
                          AND last_dependency_name = ?
                      )
                  )
                ORDER BY next_attempt_at, received_at
                LIMIT ?
            )
            "#,
        )
        .bind(now_ms)
        .bind(now_ms)
        .bind(now_ms)
        .bind(client_host.as_str())
        .bind(i64::from(limit))
        .execute(&self.pool)
        .await
        .map_err(|error| db_error("wake client source announce work", error))?;

        Ok(result.rows_affected())
    }

    pub async fn wake_announce_dependency_recovery(
        &self,
        dependency_type: &str,
        dependency_name: &DependencyName,
        now_ms: i64,
        limit: u16,
    ) -> Result<u64, DatabaseError> {
        let _span = debug_span!(
            "announce.wake_dependency_recovery",
            dependency_type,
            dependency_name = %dependency_name,
            limit
        );
        let result = sqlx::query(
            r#"
            UPDATE announce_work
            SET status = 'queued',
                next_attempt_at = ?,
                updated_at = ?,
                last_dependency_kind = NULL,
                last_dependency_name = NULL
            WHERE id IN (
                SELECT id FROM announce_work
                WHERE status = 'waiting'
                  AND expires_at > ?
                  AND last_dependency_kind = ?
                  AND last_dependency_name = ?
                ORDER BY next_attempt_at, received_at
                LIMIT ?
            )
            "#,
        )
        .bind(now_ms)
        .bind(now_ms)
        .bind(now_ms)
        .bind(dependency_type)
        .bind(dependency_name.as_str())
        .bind(i64::from(limit))
        .execute(&self.pool)
        .await
        .map_err(|error| db_error("wake dependency announce work", error))?;

        Ok(result.rows_affected())
    }

    pub async fn wake_announce_candidate_cache_completion(
        &self,
        info_hash: Option<&InfoHash>,
        guid: Option<&CandidateGuid>,
        now_ms: i64,
        limit: u16,
    ) -> Result<u64, DatabaseError> {
        let _span = debug_span!(
            "announce.wake_candidate_cache_completion",
            info_hash_prefix = info_hash.map(info_hash_prefix).unwrap_or_default(),
            candidate_guid = guid.map(CandidateGuid::as_str).unwrap_or(""),
            limit
        );
        let result = sqlx::query(
            r#"
            UPDATE announce_work
            SET status = 'queued',
                next_attempt_at = ?,
                updated_at = ?,
                last_dependency_kind = NULL,
                last_dependency_name = NULL
            WHERE id IN (
                SELECT id FROM announce_work
                WHERE status = 'waiting'
                  AND expires_at > ?
                  AND reason = 'candidate_downloading'
                  AND (? IS NULL OR info_hash = ?)
                  AND (? IS NULL OR guid = ?)
                ORDER BY next_attempt_at, received_at
                LIMIT ?
            )
            "#,
        )
        .bind(now_ms)
        .bind(now_ms)
        .bind(now_ms)
        .bind(info_hash.map(InfoHash::as_str))
        .bind(info_hash.map(InfoHash::as_str))
        .bind(guid.map(CandidateGuid::as_str))
        .bind(guid.map(CandidateGuid::as_str))
        .bind(i64::from(limit))
        .execute(&self.pool)
        .await
        .map_err(|error| db_error("wake candidate cache announce work", error))?;

        Ok(result.rows_affected())
    }

    pub async fn wake_due_waiting_announce_work(
        &self,
        now_ms: i64,
        limit: u16,
    ) -> Result<u64, DatabaseError> {
        let _span = debug_span!("announce.wake_due_waiting", limit);
        let result = sqlx::query(
            r#"
            UPDATE announce_work
            SET status = 'queued',
                next_attempt_at = ?,
                updated_at = ?
            WHERE id IN (
                SELECT id FROM announce_work
                WHERE status = 'waiting'
                  AND next_attempt_at <= ?
                  AND expires_at > ?
                  AND last_dependency_kind IS NULL
                  AND last_dependency_name IS NULL
                ORDER BY next_attempt_at, received_at
                LIMIT ?
            )
            "#,
        )
        .bind(now_ms)
        .bind(now_ms)
        .bind(now_ms)
        .bind(now_ms)
        .bind(i64::from(limit))
        .execute(&self.pool)
        .await
        .map_err(|error| db_error("wake due waiting announce work", error))?;

        Ok(result.rows_affected())
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

    pub async fn announce_queue_snapshot(
        &self,
        limit: u16,
        now_ms: i64,
    ) -> Result<AnnounceQueueSnapshot, DatabaseError> {
        let summary = sqlx::query(
            r#"
            SELECT
                COUNT(*) AS active_count,
                MIN(received_at) AS oldest_received_at,
                MIN(CASE
                    WHEN status IN ('queued', 'retryable', 'waiting')
                    THEN next_attempt_at
                END) AS next_attempt_at,
                COALESCE(SUM(CASE
                    WHEN status = 'running' AND lease_owner IS NOT NULL
                    THEN 1 ELSE 0
                END), 0) AS running_leases
            FROM announce_work
            WHERE status IN ('queued', 'running', 'waiting', 'retryable')
            "#,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|error| db_error("read announce queue summary", error))?;
        let active_count = summary.get("active_count");
        let oldest_received_at: Option<i64> = summary.get("oldest_received_at");
        let next_attempt_at: Option<i64> = summary.get("next_attempt_at");
        let running_leases = summary.get("running_leases");
        let oldest_active_age_ms =
            oldest_received_at.map(|received_at| now_ms.saturating_sub(received_at).max(0));
        let next_retry_delay_ms =
            next_attempt_at.map(|next_attempt| next_attempt.saturating_sub(now_ms).max(0));

        let attempt_rows = sqlx::query(
            r#"
            SELECT
                COALESCE(last_error_class, last_action_outcome, reason, status) AS outcome_class,
                SUM(attempt_count) AS attempts
            FROM announce_work
            WHERE attempt_count > 0
            GROUP BY outcome_class
            ORDER BY attempts DESC, outcome_class
            LIMIT ?
            "#,
        )
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(|error| db_error("read announce attempt counts", error))?;
        let dependency_rows = sqlx::query(
            r#"
            SELECT last_dependency_kind, last_dependency_name, COUNT(*) AS count
            FROM announce_work
            WHERE last_dependency_kind IS NOT NULL
              AND last_dependency_name IS NOT NULL
              AND status IN ('queued', 'running', 'waiting', 'retryable')
            GROUP BY last_dependency_kind, last_dependency_name
            ORDER BY count DESC, last_dependency_kind, last_dependency_name
            LIMIT ?
            "#,
        )
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(|error| db_error("read announce dependency wait counts", error))?;

        Ok(AnnounceQueueSnapshot {
            active_count,
            oldest_active_age_ms,
            next_retry_delay_ms,
            running_leases,
            status_counts: self.announce_status_counts(limit).await?,
            attempt_counts: attempt_rows
                .into_iter()
                .map(|row| AnnounceAttemptCount {
                    outcome_class: row.get("outcome_class"),
                    attempts: row.get("attempts"),
                })
                .collect(),
            dependency_wait_counts: dependency_rows
                .into_iter()
                .map(|row| AnnounceDependencyWaitCount {
                    dependency_kind: row.get("last_dependency_kind"),
                    dependency_name: row.get("last_dependency_name"),
                    count: row.get("count"),
                })
                .collect(),
        })
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

    async fn apply_announce_dependency_schedule(
        &self,
        id: &AnnounceWorkId,
        action: AnnounceDependencyScheduleAction,
        now_ms: i64,
    ) -> Result<u64, DatabaseError> {
        let result = match action {
            AnnounceDependencyScheduleAction::None => return Ok(0),
            AnnounceDependencyScheduleAction::Wait {
                reason,
                next_attempt_at_ms,
            } => sqlx::query(
                r#"
                    UPDATE announce_work
                    SET status = 'waiting',
                        reason = ?,
                        next_attempt_at = ?,
                        updated_at = ?
                    WHERE id = ?
                      AND status IN ('queued', 'retryable', 'waiting')
                    "#,
            )
            .bind(announce_reason_key(reason))
            .bind(next_attempt_at_ms)
            .bind(now_ms)
            .bind(id.as_str())
            .execute(&self.pool)
            .await
            .map_err(|error| db_error("wait announce dependency", error))?,
            AnnounceDependencyScheduleAction::Probe => sqlx::query(
                r#"
                    UPDATE announce_work
                    SET status = 'queued',
                        reason = 'dependency_backoff',
                        next_attempt_at = ?,
                        updated_at = ?
                    WHERE id = ?
                      AND status = 'waiting'
                    "#,
            )
            .bind(now_ms)
            .bind(now_ms)
            .bind(id.as_str())
            .execute(&self.pool)
            .await
            .map_err(|error| db_error("probe announce dependency", error))?,
            AnnounceDependencyScheduleAction::ClearDependency => sqlx::query(
                r#"
                    UPDATE announce_work
                    SET status = 'queued',
                        reason = 'dependency_backoff',
                        next_attempt_at = ?,
                        updated_at = ?,
                        last_dependency_kind = NULL,
                        last_dependency_name = NULL
                    WHERE id = ?
                      AND status = 'waiting'
                    "#,
            )
            .bind(now_ms)
            .bind(now_ms)
            .bind(id.as_str())
            .execute(&self.pool)
            .await
            .map_err(|error| db_error("clear announce dependency", error))?,
        };

        Ok(result.rows_affected())
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
            download_url,
            redacted_download_url,
            cookie,
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
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
    .bind(
        work.fetch
            .as_ref()
            .map(AnnounceFetchMaterial::expose_download_url),
    )
    .bind(
        work.fetch
            .as_ref()
            .map(|fetch| fetch.redacted_download_url().as_str()),
    )
    .bind(
        work.fetch
            .as_ref()
            .and_then(AnnounceFetchMaterial::cookie)
            .map(CookieSecret::expose_secret),
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
        } => (
            "client".to_owned(),
            client_source_key(client_host, source_key),
        ),
        LocalItemSource::TorrentCache { path } => {
            ("torrent_cache".to_owned(), path_to_string(path))
        }
        LocalItemSource::DataRoot { path } => ("data_root".to_owned(), path_to_string(path)),
        LocalItemSource::Virtual { source_key } => ("virtual".to_owned(), source_key.to_string()),
    }
}

async fn upsert_local_item_with_files_in_transaction(
    transaction: &mut Transaction<'_, Sqlite>,
    item: &LocalItem,
    files: &[LocalFile],
) -> Result<LocalItemId, DatabaseError> {
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
    .execute(&mut **transaction)
    .await
    .map_err(|error| db_error("upsert local item", error))?;

    let row = sqlx::query("SELECT id FROM local_items WHERE source_type = ? AND source_key = ?")
        .bind(&source_type)
        .bind(&source_key)
        .fetch_one(&mut **transaction)
        .await
        .map_err(|error| db_error("select local item id", error))?;
    let item_id = id_from_i64(row.get::<i64, _>("id"), "local item id")?;

    sqlx::query("DELETE FROM local_files WHERE item_id = ?")
        .bind(i64_from_u64(item_id.get(), "local item id")?)
        .execute(&mut **transaction)
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
        .bind(file.mtime_ms)
        .bind(i64::from(file.file_index.get()))
        .execute(&mut **transaction)
        .await
        .map_err(|error| db_error("insert local file", error))?;
    }

    Ok(item_id)
}

async fn initialize_retained_keys(
    transaction: &mut Transaction<'_, Sqlite>,
) -> Result<(), DatabaseError> {
    sqlx::query(
        r#"
        CREATE TEMP TABLE IF NOT EXISTS retained_local_item_keys (
            source_key TEXT PRIMARY KEY
        ) WITHOUT ROWID
        "#,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|error| db_error("initialize retained local item keys", error))?;

    clear_retained_keys(transaction).await
}

async fn insert_retained_key(
    transaction: &mut Transaction<'_, Sqlite>,
    source_key: &str,
) -> Result<(), DatabaseError> {
    sqlx::query("INSERT OR IGNORE INTO retained_local_item_keys (source_key) VALUES (?)")
        .bind(source_key)
        .execute(&mut **transaction)
        .await
        .map_err(|error| db_error("insert retained local item key", error))?;

    Ok(())
}

fn announce_dependency_schedule_action(
    row: &AnnounceDependencyScheduleRow,
    now_ms: i64,
    recovery_probe_interval_ms: i64,
) -> AnnounceDependencyScheduleAction {
    match row.dependency_state.as_deref() {
        Some("degraded" | "unavailable") => {
            if let Some(retry_after_ms) = row.retry_after_ms {
                if retry_after_ms > now_ms {
                    return AnnounceDependencyScheduleAction::Wait {
                        reason: AnnounceReason::RetryAfter,
                        next_attempt_at_ms: retry_after_ms.max(row.next_attempt_at_ms),
                    };
                }
            }

            if row.status == "waiting" {
                if row.next_attempt_at_ms <= now_ms {
                    AnnounceDependencyScheduleAction::Probe
                } else {
                    AnnounceDependencyScheduleAction::None
                }
            } else {
                AnnounceDependencyScheduleAction::Wait {
                    reason: AnnounceReason::DependencyBackoff,
                    next_attempt_at_ms: row
                        .next_attempt_at_ms
                        .max(now_ms.saturating_add(recovery_probe_interval_ms)),
                }
            }
        }
        Some("healthy" | "unknown") | None => {
            if row.status == "waiting" {
                AnnounceDependencyScheduleAction::ClearDependency
            } else {
                AnnounceDependencyScheduleAction::None
            }
        }
        Some(_) => AnnounceDependencyScheduleAction::None,
    }
}

async fn prune_local_items_not_retained(
    transaction: &mut Transaction<'_, Sqlite>,
    scope: &LocalInventoryScope,
) -> Result<u64, DatabaseError> {
    let result = if let Some(prefix) = scope.source_key_prefix() {
        sqlx::query(
            r#"
            DELETE FROM local_items
            WHERE source_type = ?
              AND instr(source_key, ?) = 1
              AND NOT EXISTS (
                  SELECT 1
                  FROM retained_local_item_keys retained
                  WHERE retained.source_key = local_items.source_key
              )
            "#,
        )
        .bind(scope.source_type())
        .bind(prefix)
        .execute(&mut **transaction)
        .await
    } else {
        sqlx::query(
            r#"
            DELETE FROM local_items
            WHERE source_type = ?
              AND NOT EXISTS (
                  SELECT 1
                  FROM retained_local_item_keys retained
                  WHERE retained.source_key = local_items.source_key
              )
            "#,
        )
        .bind(scope.source_type())
        .execute(&mut **transaction)
        .await
    }
    .map_err(|error| db_error("prune missing local inventory", error))?;

    Ok(result.rows_affected())
}

async fn normalize_client_source_keys(
    transaction: &mut Transaction<'_, Sqlite>,
    client_host: &ClientHost,
) -> Result<(), DatabaseError> {
    let encoded_prefix = client_source_key_prefix(client_host);
    let rows = sqlx::query(
        r#"
        SELECT id, source_key
        FROM local_items
        WHERE source_type = 'client'
        "#,
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(|error| db_error("read client inventory source keys", error))?;

    for row in rows {
        let id: i64 = row.get("id");
        let old_source_key: String = row.get("source_key");
        if old_source_key.starts_with(&encoded_prefix) {
            continue;
        }
        let Some((row_client_host, row_source_key)) = parse_client_source_key(&old_source_key)
        else {
            continue;
        };
        if row_client_host != client_host.as_str() {
            continue;
        }

        let row_source_key =
            SourceKey::new(row_source_key).map_err(|error| DatabaseError::QueryFailed {
                operation: "normalize client inventory source key".to_owned(),
                message: error.to_string(),
            })?;
        let new_source_key = client_source_key(client_host, &row_source_key);
        if new_source_key == old_source_key {
            continue;
        }

        let existing_id: Option<i64> = sqlx::query_scalar(
            "SELECT id FROM local_items WHERE source_type = 'client' AND source_key = ?",
        )
        .bind(&new_source_key)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|error| db_error("read normalized client inventory source key", error))?;
        if matches!(existing_id, Some(existing_id) if existing_id != id) {
            sqlx::query("DELETE FROM local_items WHERE id = ?")
                .bind(id)
                .execute(&mut **transaction)
                .await
                .map_err(|error| db_error("delete duplicate legacy client inventory", error))?;
        } else {
            sqlx::query("UPDATE local_items SET source_key = ? WHERE id = ?")
                .bind(new_source_key)
                .bind(id)
                .execute(&mut **transaction)
                .await
                .map_err(|error| db_error("normalize legacy client inventory source key", error))?;
        }
    }

    Ok(())
}

async fn clear_retained_keys(
    transaction: &mut Transaction<'_, Sqlite>,
) -> Result<(), DatabaseError> {
    sqlx::query("DELETE FROM retained_local_item_keys")
        .execute(&mut **transaction)
        .await
        .map_err(|error| db_error("clear retained local item keys", error))?;

    Ok(())
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

fn indexer_id_from_i64(value: i64, field: &'static str) -> Result<IndexerId, DatabaseError> {
    u64::try_from(value)
        .ok()
        .and_then(|value| IndexerId::new(value).ok())
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

fn byte_size_from_i64(value: i64, field: &'static str) -> Result<ByteSize, DatabaseError> {
    u64::try_from(value)
        .map(ByteSize::new)
        .map_err(|error| DatabaseError::QueryFailed {
            operation: format!("read {field}"),
            message: error.to_string(),
        })
}

fn file_index_from_i64(value: i64, field: &'static str) -> Result<FileIndex, DatabaseError> {
    u32::try_from(value)
        .map(FileIndex::new)
        .map_err(|error| DatabaseError::QueryFailed {
            operation: format!("read {field}"),
            message: error.to_string(),
        })
}

fn local_file_snapshot_from_row(
    row: sqlx::sqlite::SqliteRow,
) -> Result<LocalFileSnapshot, DatabaseError> {
    Ok(LocalFileSnapshot {
        item_id: id_from_i64(row.get("item_id"), "local file item id")?,
        relative_path: PathBuf::from(row.get::<String, _>("relative_path")),
        file_name: row.get("file_name"),
        size: byte_size_from_i64(row.get("size"), "local file size")?,
        mtime_ms: row.get("mtime_ms"),
        file_index: file_index_from_i64(row.get("file_index"), "local file index")?,
    })
}

fn local_item_from_row(row: sqlx::sqlite::SqliteRow) -> Result<LocalItem, DatabaseError> {
    let id = id_from_i64(row.get("id"), "local item id")?;
    let source_type: String = row.get("source_type");
    let source_key: String = row.get("source_key");
    let info_hash = row
        .get::<Option<String>, _>("info_hash")
        .map(InfoHash::new)
        .transpose()
        .map_err(|error| DatabaseError::QueryFailed {
            operation: "read local item info hash".to_owned(),
            message: error.to_string(),
        })?;

    Ok(LocalItem {
        id: Some(id),
        source: local_item_source_from_row(&source_type, &source_key)?,
        title: ItemTitle::new(row.get::<String, _>("title")).map_err(|error| {
            DatabaseError::QueryFailed {
                operation: "read local item title".to_owned(),
                message: error.to_string(),
            }
        })?,
        display_name: DisplayName::new(row.get::<String, _>("display_name")).map_err(|error| {
            DatabaseError::QueryFailed {
                operation: "read local item display name".to_owned(),
                message: error.to_string(),
            }
        })?,
        media_type: media_type_from_key(row.get::<String, _>("media_type").as_str())?,
        info_hash,
        path: row.get::<Option<String>, _>("path").map(PathBuf::from),
        save_path: row.get::<Option<String>, _>("save_path").map(PathBuf::from),
        total_size: byte_size_from_i64(row.get("total_size"), "local item total size")?,
        mtime_ms: row.get("mtime_ms"),
    })
}

fn local_item_source_from_row(
    source_type: &str,
    source_key: &str,
) -> Result<LocalItemSource, DatabaseError> {
    match source_type {
        "client" => {
            let Some((client_host, source_key)) = parse_client_source_key(source_key) else {
                return Err(DatabaseError::QueryFailed {
                    operation: "read local item client source".to_owned(),
                    message: "client source key is missing host separator".to_owned(),
                });
            };
            Ok(LocalItemSource::Client {
                client_host: ClientHost::new(client_host).map_err(|error| {
                    DatabaseError::QueryFailed {
                        operation: "read local item client host".to_owned(),
                        message: error.to_string(),
                    }
                })?,
                source_key: SourceKey::new(source_key).map_err(|error| {
                    DatabaseError::QueryFailed {
                        operation: "read local item source key".to_owned(),
                        message: error.to_string(),
                    }
                })?,
            })
        }
        "torrent_cache" => Ok(LocalItemSource::TorrentCache {
            path: PathBuf::from(source_key),
        }),
        "data_root" => Ok(LocalItemSource::DataRoot {
            path: PathBuf::from(source_key),
        }),
        "virtual" => Ok(LocalItemSource::Virtual {
            source_key: SourceKey::new(source_key).map_err(|error| DatabaseError::QueryFailed {
                operation: "read virtual item source key".to_owned(),
                message: error.to_string(),
            })?,
        }),
        _ => Err(DatabaseError::QueryFailed {
            operation: "read local item source type".to_owned(),
            message: format!("unsupported local item source type {source_type}"),
        }),
    }
}

fn client_source_key(client_host: &ClientHost, source_key: &SourceKey) -> String {
    format!(
        "{}:{}:{}",
        client_host.as_str().len(),
        client_host.as_str(),
        source_key.as_str()
    )
}

fn client_source_key_prefix(client_host: &ClientHost) -> String {
    format!("{}:{}:", client_host.as_str().len(), client_host.as_str())
}

fn parse_client_source_key(source_key: &str) -> Option<(&str, &str)> {
    if let Some((host_len, rest)) = source_key.split_once(':')
        && let Ok(host_len) = host_len.parse::<usize>()
        && let Some(client_host) = rest.get(..host_len)
        && let Some(after_host) = rest.get(host_len..)
        && let Some(source_key) = after_host.strip_prefix(':')
        && !client_host.is_empty()
        && !source_key.is_empty()
    {
        return Some((client_host, source_key));
    }

    source_key.rsplit_once(':')
}

fn media_type_from_key(value: &str) -> Result<MediaType, DatabaseError> {
    match value {
        "episode" => Ok(MediaType::Episode),
        "season_pack" => Ok(MediaType::SeasonPack),
        "movie" => Ok(MediaType::Movie),
        "anime" => Ok(MediaType::Anime),
        "video" => Ok(MediaType::Video),
        "audio" => Ok(MediaType::Audio),
        "book" => Ok(MediaType::Book),
        "archive" => Ok(MediaType::Archive),
        "unknown" => Ok(MediaType::Unknown),
        _ => Err(DatabaseError::QueryFailed {
            operation: "read local item media type".to_owned(),
            message: format!("unsupported media type {value}"),
        }),
    }
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
        redacted_download_url: row.get("redacted_download_url"),
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

fn info_hash_prefix(info_hash: &InfoHash) -> String {
    info_hash.as_str().chars().take(8).collect()
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
    use crate::indexers::{
        ApiKeySource, ConfiguredTorznabIndexer, SanitizedTorznabUrl, parse_torznab_caps,
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
        .unwrap()
        .with_mtime_ms(Some(1_700_000_000_001));
        let second_file = LocalFile::new(
            None,
            PathBuf::from("Example/file-b.mkv"),
            ByteSize::new(20),
            FileIndex::new(1),
        )
        .unwrap()
        .with_mtime_ms(Some(1_700_000_000_002));

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

        let files = repository.local_files_for_item(item_id, 10).await.unwrap();

        assert_eq!(1, files.len());
        assert_eq!(PathBuf::from("Example/file-a.mkv"), files[0].relative_path);
        assert_eq!("file-a.mkv", files[0].file_name);
        assert_eq!(ByteSize::new(10), files[0].size);
        assert_eq!(Some(1_700_000_000_001), files[0].mtime_ms);
        assert_eq!(FileIndex::new(0), files[0].file_index);
    }

    #[tokio::test]
    async fn local_file_queries_find_duplicate_sizes_names_and_paths() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let first_item = test_local_item("Example");
        let mut second_item = test_local_item("Other");
        second_item.source = LocalItemSource::Virtual {
            source_key: SourceKey::new("other-source").unwrap(),
        };
        second_item.info_hash = None;

        let first_file = LocalFile::new(
            None,
            PathBuf::from("Example/file-a.mkv"),
            ByteSize::new(10),
            FileIndex::new(0),
        )
        .unwrap()
        .with_mtime_ms(Some(1_700_000_000_001));
        let second_file = LocalFile::new(
            None,
            PathBuf::from("Other/file-a.mkv"),
            ByteSize::new(10),
            FileIndex::new(0),
        )
        .unwrap()
        .with_mtime_ms(Some(1_700_000_000_002));
        let third_file = LocalFile::new(
            None,
            PathBuf::from("Other/file-c.mkv"),
            ByteSize::new(20),
            FileIndex::new(1),
        )
        .unwrap();

        let first_item_id = repository
            .upsert_local_item_with_files(&first_item, &[first_file])
            .await
            .unwrap();
        let second_item_id = repository
            .upsert_local_item_with_files(&second_item, &[second_file, third_file])
            .await
            .unwrap();

        let size_matches = repository
            .local_files_by_size(ByteSize::new(10), 10)
            .await
            .unwrap();
        let name_matches = repository
            .local_files_by_size_and_name(ByteSize::new(10), "file-a.mkv", 10)
            .await
            .unwrap();
        let path_matches = repository
            .local_files_by_relative_path(&PathBuf::from("Other/file-a.mkv"), 10)
            .await
            .unwrap();

        assert_eq!(2, size_matches.len());
        assert_eq!(2, name_matches.len());
        assert_eq!(1, path_matches.len());
        assert_eq!(second_item_id, path_matches[0].item_id);
        assert_eq!(
            PathBuf::from("Other/file-a.mkv"),
            path_matches[0].relative_path
        );
        assert_eq!(Some(1_700_000_000_002), path_matches[0].mtime_ms);
        assert!(
            size_matches
                .iter()
                .any(|file| file.item_id == first_item_id)
        );
        assert!(
            size_matches
                .iter()
                .any(|file| file.item_id == second_item_id)
        );
    }

    #[tokio::test]
    async fn large_local_file_batches_commit_in_one_transaction() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let item = test_local_item("Example");
        let files = (0..1_024)
            .map(|index| {
                LocalFile::new(
                    None,
                    PathBuf::from(format!("Example/file-{index:04}.mkv")),
                    ByteSize::new(u64::from(index) + 1),
                    FileIndex::new(index),
                )
                .unwrap()
                .with_mtime_ms(Some(1_700_000_000_000 + i64::from(index)))
            })
            .collect::<Vec<_>>();

        let item_id = repository
            .upsert_local_item_with_files(&item, &files)
            .await
            .unwrap();
        let stored = repository
            .local_files_for_item(item_id, 2_000)
            .await
            .unwrap();

        assert_eq!(files.len(), stored.len());
        assert_eq!(
            PathBuf::from("Example/file-0000.mkv"),
            stored[0].relative_path
        );
        assert_eq!(
            PathBuf::from("Example/file-1023.mkv"),
            stored[1023].relative_path
        );
        assert_eq!(Some(1_700_000_001_023), stored[1023].mtime_ms);
    }

    #[tokio::test]
    async fn local_files_for_item_page_reports_truncation() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let item = test_local_item("Example");
        let files = [
            LocalFile::new(
                None,
                PathBuf::from("Example/file-a.mkv"),
                ByteSize::new(10),
                FileIndex::new(0),
            )
            .unwrap(),
            LocalFile::new(
                None,
                PathBuf::from("Example/file-b.mkv"),
                ByteSize::new(20),
                FileIndex::new(1),
            )
            .unwrap(),
        ];
        let item_id = repository
            .upsert_local_item_with_files(&item, &files)
            .await
            .unwrap();

        let limited = repository
            .local_files_for_item_page(item_id, 1)
            .await
            .unwrap();
        let complete = repository
            .local_files_for_item_page(item_id, 2)
            .await
            .unwrap();

        assert!(limited.truncated);
        assert_eq!(1, limited.files.len());
        assert!(!complete.truncated);
        assert_eq!(2, complete.files.len());
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
        candidate.download_url =
            DownloadUrl::new("https://user:password@indexer.example/download?id=1&passkey=secret")
                .unwrap();

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

        let row =
            sqlx::query("SELECT title, redacted_download_url FROM remote_candidates WHERE id = ?")
                .bind(i64_from_u64(first_id.get(), "remote candidate id").unwrap())
                .fetch_one(repository.pool())
                .await
                .unwrap();
        let title: String = row.get("title");
        let redacted_download_url: String = row.get("redacted_download_url");
        let info_hash_matches = repository
            .remote_candidates_by_info_hash(candidate.info_hash.as_ref().unwrap(), 10)
            .await
            .unwrap();

        assert_eq!(first_id, second_id);
        assert_eq!("Updated", title);
        assert_eq!(
            "https://[REDACTED]@indexer.example/download?id=1&passkey=[REDACTED]",
            redacted_download_url
        );
        assert!(!redacted_download_url.contains("secret"));
        assert!(!redacted_download_url.contains("password"));
        assert_eq!(1, info_hash_matches.len());
        assert_eq!(first_id, info_hash_matches[0].id);
        assert_eq!(
            redacted_download_url,
            info_hash_matches[0].redacted_download_url
        );
        assert_eq!(
            Some(PathBuf::from("/cache/fedcba.cached.torrent")),
            info_hash_matches[0].torrent_cache_path
        );
    }

    #[tokio::test]
    async fn sync_torznab_indexers_upserts_and_disables_removed_rows() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let first = test_indexer(
            "main",
            "https://indexer.example/api",
            ApiKeySource::Env("INDEXER_KEY".to_owned()),
        );
        let second = test_indexer(
            "backup",
            "https://backup.example/api",
            ApiKeySource::File("/run/secrets/backup".to_owned()),
        );

        let synced = repository
            .sync_torznab_indexers(&[first.clone(), second.clone()], 100)
            .await
            .unwrap();
        let main = synced
            .iter()
            .find(|indexer| indexer.name.as_str() == "main")
            .unwrap();
        assert_eq!("https://indexer.example/api", main.url);
        assert_eq!("env:INDEXER_KEY", main.api_key_source);
        assert!(main.enabled);
        assert_eq!("unknown", main.state);

        let updated = test_indexer(
            "main",
            "https://indexer.example/prowlarr/api",
            ApiKeySource::Direct,
        );
        let resynced = repository
            .sync_torznab_indexers(&[updated], 200)
            .await
            .unwrap();
        let main = resynced
            .iter()
            .find(|indexer| indexer.name.as_str() == "main")
            .unwrap();
        let backup = resynced
            .iter()
            .find(|indexer| indexer.name.as_str() == "backup")
            .unwrap();

        assert_eq!("https://indexer.example/prowlarr/api", main.url);
        assert_eq!("direct", main.api_key_source);
        assert!(main.enabled);
        assert!(!backup.enabled);
        assert!(!format!("{resynced:?}").contains("secret-value"));
    }

    #[tokio::test]
    async fn indexer_caps_updates_registry_and_dependency_health() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let indexer = test_indexer(
            "main",
            "https://indexer.example/api",
            ApiKeySource::Env("INDEXER_KEY".to_owned()),
        );
        repository
            .sync_torznab_indexers(&[indexer], 100)
            .await
            .unwrap();
        let name = DependencyName::new("main").unwrap();
        let caps = parse_torznab_caps(
            r#"
            <caps>
              <searching><search available="yes"/></searching>
              <categories><category id="5000" name="TV"/></categories>
            </caps>
            "#,
        )
        .unwrap();

        repository
            .record_indexer_caps_success(&name, &caps, 200)
            .await
            .unwrap();
        let rows = repository.indexer_registry_snapshot(10).await.unwrap();
        let health = repository.dependency_health_snapshot(10).await.unwrap();

        assert_eq!("healthy", rows[0].state);
        assert_eq!(Some(200), rows[0].last_caps_refresh_at_ms);
        assert_eq!("healthy", health[0].state);

        repository
            .record_indexer_caps_failure(
                &name,
                &ReasonText::new("bad caps").unwrap(),
                Some(5_000),
                300,
            )
            .await
            .unwrap();
        let rows = repository.indexer_registry_snapshot(10).await.unwrap();
        let health = repository.dependency_health_snapshot(10).await.unwrap();

        assert_eq!("degraded", rows[0].state);
        assert_eq!(Some(5_000), rows[0].retry_after_ms);
        assert_eq!("degraded", health[0].state);
        assert_eq!(Some("bad caps".to_owned()), health[0].reason);
    }

    #[tokio::test]
    async fn indexer_request_backoff_updates_retry_and_health_state() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let indexer = test_indexer(
            "main",
            "https://indexer.example/api",
            ApiKeySource::Env("INDEXER_KEY".to_owned()),
        );
        repository
            .sync_torznab_indexers(&[indexer], 100)
            .await
            .unwrap();
        let name = DependencyName::new("main").unwrap();

        repository
            .record_indexer_request_backoff(
                &name,
                &ReasonText::new("rate limited").unwrap(),
                10_000,
                200,
                false,
            )
            .await
            .unwrap();

        let rows = repository.indexer_registry_snapshot(10).await.unwrap();
        let health = repository.dependency_health_snapshot(10).await.unwrap();

        assert_eq!("degraded", rows[0].state);
        assert_eq!(Some(10_000), rows[0].retry_after_ms);
        assert_eq!("degraded", health[0].state);

        repository
            .record_indexer_request_backoff(
                &name,
                &ReasonText::new("network unavailable").unwrap(),
                20_000,
                300,
                true,
            )
            .await
            .unwrap();

        let rows = repository.indexer_registry_snapshot(10).await.unwrap();
        let health = repository.dependency_health_snapshot(10).await.unwrap();

        assert_eq!("unavailable", rows[0].state);
        assert_eq!(Some(20_000), rows[0].retry_after_ms);
        assert_eq!("unavailable", health[0].state);
        assert_eq!(Some("network unavailable".to_owned()), health[0].reason);
    }

    #[tokio::test]
    async fn search_history_updates_only_for_non_rate_limited_searches() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let item_id = repository
            .upsert_local_item_with_files(&test_local_item("Example"), &[])
            .await
            .unwrap();
        repository
            .sync_torznab_indexers(
                &[test_indexer(
                    "main",
                    "https://indexer.example/api",
                    ApiKeySource::Direct,
                )],
                100,
            )
            .await
            .unwrap();
        let indexer_id =
            IndexerId::new(repository.indexer_registry_snapshot(10).await.unwrap()[0].id).unwrap();

        repository
            .record_search_history(item_id, indexer_id, 200, false)
            .await
            .unwrap();
        repository
            .record_search_history(item_id, indexer_id, 300, false)
            .await
            .unwrap();
        repository
            .record_search_history(item_id, indexer_id, 150, false)
            .await
            .unwrap();
        repository
            .record_search_history(item_id, indexer_id, 400, true)
            .await
            .unwrap();

        let history = repository
            .search_history_for_item(item_id, 10)
            .await
            .unwrap();

        assert_eq!(
            vec![SearchHistoryRow {
                local_item_id: item_id,
                indexer_id,
                first_searched_at_ms: 150,
                last_searched_at_ms: 300,
            }],
            history
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
    async fn announce_insert_persists_fetch_material() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let mut work = test_announce_work("ann_fetch", "guid-fetch", 1);
        let download_url =
            DownloadUrl::new("https://tracker.example/download?id=1&apikey=secret").unwrap();
        work.fetch = Some(
            AnnounceFetchMaterial::new(
                &download_url,
                Some(CookieSecret::new("sid=secret-cookie").unwrap()),
            )
            .unwrap(),
        );

        repository
            .insert_or_dedupe_announce_work(&work, 10)
            .await
            .unwrap();
        let stored = repository
            .announce_fetch_material(&work.id)
            .await
            .unwrap()
            .unwrap();
        let redacted: String =
            sqlx::query_scalar("SELECT redacted_download_url FROM announce_work WHERE id = ?")
                .bind(work.id.as_str())
                .fetch_one(repository.pool())
                .await
                .unwrap();

        assert_eq!(download_url.as_str(), stored.expose_download_url());
        assert_eq!(
            "sid=secret-cookie",
            stored.cookie().unwrap().expose_secret()
        );
        assert!(redacted.contains("[REDACTED]"));
        assert!(!redacted.contains("secret"));
    }

    #[tokio::test]
    async fn concurrent_announce_claims_do_not_steal_leases() {
        let root = unique_temp_dir("announce-atomic-claim");
        let database = root.join("sporos.db");
        let repository = Repository::connect(&database).await.unwrap();
        repository
            .insert_or_dedupe_announce_work(&test_announce_work("ann_atomic", "guid-atomic", 1), 10)
            .await
            .unwrap();

        let first_repository = repository.clone();
        let second_repository = repository.clone();
        let (first, second) = tokio::join!(
            first_repository.claim_announce_work("worker-1", 10, 100, 1),
            second_repository.claim_announce_work("worker-2", 10, 100, 1)
        );
        let first = claimed_or_busy_empty(first);
        let second = claimed_or_busy_empty(second);
        let total_claims = first.len() + second.len();
        let owner: String =
            sqlx::query_scalar("SELECT lease_owner FROM announce_work WHERE id = 'ann_atomic'")
                .fetch_one(repository.pool())
                .await
                .unwrap();

        assert_eq!(1, total_claims);
        assert!(
            (first.is_empty() && owner == "worker-2") || (second.is_empty() && owner == "worker-1")
        );

        fs::remove_dir_all(root).unwrap();
    }

    fn claimed_or_busy_empty(
        result: Result<Vec<AnnounceWorkId>, DatabaseError>,
    ) -> Vec<AnnounceWorkId> {
        match result {
            Ok(claimed) => claimed,
            Err(DatabaseError::Busy { .. }) => Vec::new(),
            Err(error) => panic!("unexpected claim error: {error}"),
        }
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

    #[tokio::test]
    async fn announce_wakeups_make_matching_waits_claimable() {
        let repository = Repository::connect_in_memory().await.unwrap();
        for (id, guid) in [
            ("ann_30", "guid-inventory"),
            ("ann_31", "guid-refreshing"),
            ("ann_32", "guid-client"),
            ("ann_33", "guid-dependency"),
            ("ann_34", "guid-cache"),
            ("ann_35", "guid-scheduled"),
            ("ann_36", "guid-other-client"),
        ] {
            repository
                .insert_or_dedupe_announce_work(&test_announce_work(id, guid, 1), 10)
                .await
                .unwrap();
        }
        set_announce_waiting(
            &repository,
            "ann_30",
            AnnounceReason::SourceIncomplete,
            500,
            None,
        )
        .await;
        set_announce_waiting(
            &repository,
            "ann_31",
            AnnounceReason::InventoryRefreshing,
            500,
            None,
        )
        .await;
        set_announce_waiting(
            &repository,
            "ann_32",
            AnnounceReason::ClientChecking,
            500,
            Some(("client", "qbit.local")),
        )
        .await;
        set_announce_waiting(
            &repository,
            "ann_33",
            AnnounceReason::DependencyBackoff,
            500,
            Some(("indexer", "main")),
        )
        .await;
        set_announce_waiting(
            &repository,
            "ann_34",
            AnnounceReason::CandidateDownloading,
            500,
            Some(("candidate", "guid-cache")),
        )
        .await;
        set_announce_waiting(&repository, "ann_35", AnnounceReason::RetryAfter, 50, None).await;
        set_announce_waiting(
            &repository,
            "ann_36",
            AnnounceReason::ClientChecking,
            500,
            Some(("client", "other.local")),
        )
        .await;

        assert_eq!(
            2,
            repository
                .wake_announce_inventory_refresh(100, 10)
                .await
                .unwrap()
        );
        assert_eq!(
            1,
            repository
                .wake_announce_client_source_completion(
                    &ClientHost::new("qbit.local").unwrap(),
                    101,
                    10
                )
                .await
                .unwrap()
        );
        assert_eq!(
            1,
            repository
                .wake_announce_dependency_recovery(
                    "indexer",
                    &DependencyName::new("main").unwrap(),
                    102,
                    10
                )
                .await
                .unwrap()
        );
        assert_eq!(
            1,
            repository
                .wake_announce_candidate_cache_completion(
                    None,
                    Some(&CandidateGuid::new("guid-cache").unwrap()),
                    103,
                    10
                )
                .await
                .unwrap()
        );
        assert_eq!(
            1,
            repository
                .wake_due_waiting_announce_work(104, 10)
                .await
                .unwrap()
        );

        let claimed = repository
            .claim_announce_work("worker-1", 104, 200, 10)
            .await
            .unwrap();

        assert_eq!(
            vec![
                AnnounceWorkId::new("ann_30").unwrap(),
                AnnounceWorkId::new("ann_31").unwrap(),
                AnnounceWorkId::new("ann_32").unwrap(),
                AnnounceWorkId::new("ann_33").unwrap(),
                AnnounceWorkId::new("ann_34").unwrap(),
                AnnounceWorkId::new("ann_35").unwrap(),
            ],
            claimed
        );
        assert_eq!(
            Some(("waiting".to_owned(), "client_checking".to_owned())),
            announce_status_reason(&repository, "ann_36").await
        );
    }

    #[tokio::test]
    async fn healthy_dependency_record_wakes_matching_waits() {
        let repository = Repository::connect_in_memory().await.unwrap();
        repository
            .insert_or_dedupe_announce_work(&test_announce_work("ann_40", "guid-40", 1), 10)
            .await
            .unwrap();
        set_announce_waiting(
            &repository,
            "ann_40",
            AnnounceReason::DependencyBackoff,
            500,
            Some(("indexer", "main")),
        )
        .await;

        repository
            .record_dependency_health(
                "indexer",
                &DependencyName::new("main").unwrap(),
                &DependencyState::Healthy { checked_at_ms: 100 },
                100,
            )
            .await
            .unwrap();

        assert_eq!(
            Some(("queued".to_owned(), "dependency_backoff".to_owned())),
            announce_status_reason(&repository, "ann_40").await
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

    fn test_indexer(
        name: &str,
        url: &str,
        api_key_source: ApiKeySource,
    ) -> ConfiguredTorznabIndexer {
        ConfiguredTorznabIndexer {
            name: DependencyName::new(name).unwrap(),
            url: SanitizedTorznabUrl::new(url).unwrap(),
            api_key_source,
            enabled: true,
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
            fetch: None,
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

    async fn set_announce_waiting(
        repository: &Repository,
        id: &str,
        reason: AnnounceReason,
        next_attempt_at_ms: i64,
        dependency: Option<(&str, &str)>,
    ) {
        let (dependency_kind, dependency_name) = dependency.unwrap_or(("", ""));
        sqlx::query(
            r#"
            UPDATE announce_work
            SET status = 'waiting',
                reason = ?,
                next_attempt_at = ?,
                expires_at = 1_000,
                last_dependency_kind = NULLIF(?, ''),
                last_dependency_name = NULLIF(?, '')
            WHERE id = ?
            "#,
        )
        .bind(announce_reason_key(reason))
        .bind(next_attempt_at_ms)
        .bind(dependency_kind)
        .bind(dependency_name)
        .bind(id)
        .execute(repository.pool())
        .await
        .unwrap();
    }

    async fn announce_status_reason(repository: &Repository, id: &str) -> Option<(String, String)> {
        sqlx::query("SELECT status, reason FROM announce_work WHERE id = ?")
            .bind(id)
            .fetch_optional(repository.pool())
            .await
            .unwrap()
            .map(|row| (row.get("status"), row.get("reason")))
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
