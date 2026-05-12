use std::path::Path;

use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Executor, Row, SqlitePool};

use crate::domain::{
    ByteSize, CandidateAssessment, DependencyName, DependencyState, InfoHash, JobName, JobState,
    LocalFile, LocalItem, LocalItemId, LocalItemSource, MatchDecision, MediaType, RemoteCandidate,
    RemoteCandidateId,
};
use crate::errors::DatabaseError;

use super::schema::{CONNECTION_PRAGMAS, initial_schema_statements};

#[derive(Debug, Clone)]
pub struct Repository {
    pool: SqlitePool,
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

    pub async fn upsert_job_state(
        &self,
        name: &JobName,
        state: JobState,
        next_run_at: Option<i64>,
        last_error: Option<&str>,
    ) -> Result<(), DatabaseError> {
        sqlx::query(
            r#"
            INSERT INTO jobs (name, state, next_run_at, last_error)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (name) DO UPDATE SET
                state = excluded.state,
                next_run_at = excluded.next_run_at,
                last_error = excluded.last_error
            "#,
        )
        .bind(name.as_str())
        .bind(job_state_key(state))
        .bind(next_run_at)
        .bind(last_error)
        .execute(&self.pool)
        .await
        .map_err(|error| db_error("upsert job state", error))?;

        Ok(())
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
    use crate::domain::{
        CandidateGuid, ClientHost, DecisionReason, DisplayName, DownloadUrl, FileIndex, IndexerId,
        ItemTitle, MatchRatio, ReasonText, SourceKey, TrackerName,
    };
    use std::path::PathBuf;

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
    async fn remote_candidate_upsert_uses_indexer_guid_natural_key() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let mut candidate = test_remote_candidate("guid-1", "Original");

        let first_id = repository
            .upsert_remote_candidate(&candidate)
            .await
            .unwrap();
        candidate.title = ItemTitle::new("Updated").unwrap();
        let second_id = repository
            .upsert_remote_candidate(&candidate)
            .await
            .unwrap();

        let title: String = sqlx::query_scalar("SELECT title FROM remote_candidates WHERE id = ?")
            .bind(i64_from_u64(first_id.get(), "remote candidate id").unwrap())
            .fetch_one(repository.pool())
            .await
            .unwrap();

        assert_eq!(first_id, second_id);
        assert_eq!("Updated", title);
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
    async fn records_jobs_and_dependency_health() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let dependency_name = DependencyName::new("indexer-main").unwrap();
        let dependency_state = DependencyState::Degraded {
            reason: ReasonText::new("rate limited").unwrap(),
            retry_after_ms: Some(123),
        };

        repository
            .upsert_job_state(
                &JobName::new("rss").unwrap(),
                JobState::Waiting,
                Some(456),
                None,
            )
            .await
            .unwrap();
        repository
            .record_dependency_health("indexer", &dependency_name, &dependency_state, 789)
            .await
            .unwrap();

        let job_state: String = sqlx::query_scalar("SELECT state FROM jobs WHERE name = 'rss'")
            .fetch_one(repository.pool())
            .await
            .unwrap();
        let health_state: String = sqlx::query_scalar(
            "SELECT state FROM dependency_health WHERE dependency_type = 'indexer'",
        )
        .fetch_one(repository.pool())
        .await
        .unwrap();

        assert_eq!("waiting", job_state);
        assert_eq!("degraded", health_state);
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
}
