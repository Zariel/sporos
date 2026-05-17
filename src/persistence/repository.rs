#![expect(
    clippy::large_enum_variant,
    clippy::string_slice,
    reason = "mechanical clippy gate enablement leaves repository lint classes to linked cleanup beads"
)]
#![cfg_attr(
    test,
    expect(
        clippy::cloned_ref_to_slice_refs,
        reason = "repository single-row test fixtures are tracked for cleanup"
    )
)]

use std::cmp::Ordering as CompareOrdering;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions, SqliteRow};
use sqlx::{Acquire, Executor, QueryBuilder, Row, Sqlite, SqlitePool, Transaction};
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tracing::{debug_span, info_span};

use crate::announce::{
    AnnounceDedupeHash, AnnounceFetchMaterial, AnnounceReason, AnnounceStatus, AnnounceWorkId,
    AnnounceWorkItem,
};
use crate::config::ProwlarrRemovePolicy;
use crate::domain::{
    ByteSize, CandidateAssessment, CandidateGuid, ClientHost, DependencyName, DependencyState,
    DisplayName, DownloadUrl, FileIndex, IndexerId, InfoHash, ItemTitle, JobName, JobState,
    LocalFile, LocalItem, LocalItemId, LocalItemSource, MatchDecision, MediaType, ReasonText,
    RemoteCandidate, RemoteCandidateId, SourceKey,
};
use crate::errors::DatabaseError;
use crate::indexers::{ConfiguredTorznabIndexer, ProwlarrIndexer, TorznabCaps};
use crate::secrets::{CookieSecret, sanitize_url_for_logging};

use super::schema::{CONNECTION_PRAGMAS, initial_schema_statements};

#[derive(Debug, Clone)]
pub struct Repository {
    pool: SqlitePool,
    inventory_staging_pool: SqlitePool,
    prowlarr_sync_lock: Arc<Mutex<()>>,
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
pub struct OwnedLocalItemFileBatch {
    pub item: LocalItem,
    pub files: Vec<LocalFile>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum OwnedLocalInventoryMessage {
    Item(OwnedLocalItemFileBatch),
    Finished,
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

pub struct LocalInventoryReplaceTransaction<'a> {
    scope: LocalInventoryScope,
    transaction: Transaction<'a, Sqlite>,
    upserted: usize,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StagedVirtualEpisodeCandidate {
    pub title: String,
    pub season: u16,
    pub episode: u16,
    pub source_file: PathBuf,
    pub size: ByteSize,
    pub mtime_ms: Option<i64>,
    pub newest_mtime_ms: Option<i64>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StagedVirtualSeasonCursor {
    pub title: String,
    pub season: u16,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StagedVirtualSeasonEpisode {
    pub episode: u16,
    pub source_file: PathBuf,
    pub size: ByteSize,
    pub mtime_ms: Option<i64>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StagedVirtualSeason {
    pub title: String,
    pub season: u16,
    pub newest_mtime_ms: Option<i64>,
    pub episodes: Vec<StagedVirtualSeasonEpisode>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct SourceKeyPrefixRange {
    start: String,
    end: Option<String>,
}

impl SourceKeyPrefixRange {
    fn new(prefix: String) -> Self {
        let end = next_text_prefix(&prefix);
        Self { start: prefix, end }
    }
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

    fn source_key_range(&self) -> Option<SourceKeyPrefixRange> {
        match self {
            Self::Client { client_host } => Some(SourceKeyPrefixRange::new(
                client_source_key_prefix(client_host),
            )),
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
    pub failure_count: u16,
    pub checked_at_ms: i64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct IndexerRegistryRow {
    pub id: u64,
    pub name: DependencyName,
    pub url: String,
    pub source_kind: String,
    pub source_name: String,
    pub source_indexer_id: String,
    pub api_key_source: String,
    pub enabled: bool,
    pub state: String,
    pub retry_after_ms: Option<i64>,
    pub last_caps_refresh_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ProwlarrSyncSummary {
    pub registry: Vec<IndexerRegistryRow>,
    pub imported: usize,
    pub deactivated: u64,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct SearchHistoryRow {
    pub local_item_id: LocalItemId,
    pub indexer_id: IndexerId,
    pub first_searched_at_ms: i64,
    pub last_searched_at_ms: i64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct IndexerSearchCapsRow {
    pub indexer_id: IndexerId,
    pub name: DependencyName,
    pub url: String,
    pub source_kind: String,
    pub source_name: String,
    pub source_indexer_id: String,
    pub api_key_source: String,
    pub enabled: bool,
    pub retry_after_ms: Option<i64>,
    pub caps: TorznabCaps,
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
pub struct LocalItemWithFile {
    pub item: LocalItem,
    pub file: LocalFileSnapshot,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct LocalItemPageCursor {
    pub title: ItemTitle,
    pub source_type: String,
    pub source_key: String,
}

impl LocalItemPageCursor {
    pub fn from_item(item: &LocalItem) -> Self {
        let (source_type, source_key) = local_source(&item.source);
        Self {
            title: item.title.clone(),
            source_type,
            source_key,
        }
    }
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
        let pool = sqlite_pool_options(5)
            .connect_with(options.clone())
            .await
            .map_err(|error| db_error("connect sqlite database", error))?;
        let inventory_staging_pool = sqlite_pool_options(1)
            .connect_with(options)
            .await
            .map_err(|error| db_error("connect sqlite database", error))?;

        let repository = Self {
            pool,
            inventory_staging_pool,
            prowlarr_sync_lock: Arc::new(Mutex::new(())),
        };
        repository.initialize().await?;
        Ok(repository)
    }

    pub async fn connect_in_memory() -> Result<Self, DatabaseError> {
        let _span = info_span!("sqlite.connect", database_path = ":memory:");
        let pool = sqlite_pool_options(1)
            .connect("sqlite::memory:")
            .await
            .map_err(|error| db_error("connect in-memory sqlite database", error))?;

        let repository = Self {
            inventory_staging_pool: pool.clone(),
            pool,
            prowlarr_sync_lock: Arc::new(Mutex::new(())),
        };
        repository.initialize().await?;
        Ok(repository)
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    pub async fn begin_local_inventory_replace_transaction(
        &self,
        scope: LocalInventoryScope,
    ) -> Result<LocalInventoryReplaceTransaction<'_>, DatabaseError> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|error| db_error("begin local inventory transaction", error))?;

        if let LocalInventoryScope::Client { client_host } = &scope {
            normalize_client_source_keys(&mut transaction, client_host).await?;
        }
        initialize_retained_keys(&mut transaction).await?;

        Ok(LocalInventoryReplaceTransaction {
            scope,
            transaction,
            upserted: 0,
        })
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
        reconcile_inline_schema(&self.pool).await?;
        for statement in initial_schema_statements() {
            self.pool
                .execute(statement)
                .await
                .map_err(|error| db_error("initialize reconciled sqlite schema", error))?;
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

    pub async fn replace_local_inventory_owned_receiver(
        &self,
        scope: LocalInventoryScope,
        items: mpsc::Receiver<OwnedLocalInventoryMessage>,
    ) -> Result<LocalInventoryReplaceSummary, DatabaseError> {
        self.replace_local_inventory_owned_receiver_with_staging_signal(scope, items, None)
            .await
    }

    pub(crate) async fn replace_local_inventory_owned_receiver_with_staging_signal(
        &self,
        scope: LocalInventoryScope,
        mut items: mpsc::Receiver<OwnedLocalInventoryMessage>,
        staging_started: Option<Arc<AtomicBool>>,
    ) -> Result<LocalInventoryReplaceSummary, DatabaseError> {
        let _span = info_span!("inventory.replace", source_type = scope.source_type());
        let mut connection = self
            .inventory_staging_pool
            .acquire()
            .await
            .map_err(|error| db_error("acquire local inventory connection", error))?;
        initialize_staged_local_inventory(&mut connection).await?;
        if let Some(staging_started) = staging_started {
            staging_started.store(true, Ordering::Release);
        }

        let mut upserted = 0usize;
        let mut finished = false;
        while let Some(message) = items.recv().await {
            let OwnedLocalInventoryMessage::Item(batch) = message else {
                finished = true;
                break;
            };
            let (source_type, source_key) = local_source(&batch.item.source);
            if !scope.accepts(&source_type, &source_key) {
                let _cleanup_result = clear_staged_local_inventory(&mut connection).await;
                return Err(DatabaseError::QueryFailed {
                    operation: "validate local inventory refresh scope".to_owned(),
                    message: format!("item source {source_type}:{source_key} is outside {scope:?}"),
                });
            }

            if let Err(error) =
                stage_local_item_with_files(&mut connection, &batch.item, &batch.files).await
            {
                let _cleanup_result = clear_staged_local_inventory(&mut connection).await;
                return Err(error);
            }
            upserted = upserted.saturating_add(1);
        }
        if !finished {
            clear_staged_local_inventory(&mut connection).await?;
            return Err(DatabaseError::IncompleteStream {
                operation: "replace local inventory stream".to_owned(),
                message: "inventory stream ended before completion marker".to_owned(),
            });
        }

        let replace_result = async {
            let mut transaction = connection
                .begin()
                .await
                .map_err(|error| db_error("begin local inventory transaction", error))?;

            if let LocalInventoryScope::Client { client_host } = &scope {
                normalize_client_source_keys(&mut transaction, client_host).await?;
            }
            initialize_retained_keys(&mut transaction).await?;
            upsert_staged_local_inventory(&mut transaction).await?;
            insert_staged_retained_keys(&mut transaction).await?;
            let pruned = prune_local_items_not_retained(&mut transaction, &scope).await?;

            clear_staged_local_inventory_in_transaction(&mut transaction).await?;
            clear_retained_keys(&mut transaction).await?;
            transaction
                .commit()
                .await
                .map_err(|error| db_error("commit local inventory transaction", error))?;

            Ok(pruned)
        }
        .await;
        match replace_result {
            Ok(pruned) => Ok(LocalInventoryReplaceSummary { upserted, pruned }),
            Err(error) => {
                let _cleanup_result = clear_staged_local_inventory(&mut connection).await;
                Err(error)
            }
        }
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

    pub async fn local_items_by_info_hash_and_media_types(
        &self,
        info_hash: &InfoHash,
        media_types: &[MediaType],
        limit: u16,
    ) -> Result<Vec<LocalItem>, DatabaseError> {
        if media_types.is_empty() {
            return self.local_items_by_info_hash(info_hash, limit).await;
        }

        let mut query = QueryBuilder::<Sqlite>::new(
            r#"
            SELECT id, source_type, source_key, title, display_name, media_type,
                   info_hash, path, save_path, total_size, mtime_ms
            FROM local_items
            WHERE info_hash =
            "#,
        );
        query.push_bind(info_hash.as_str());
        query.push(" AND media_type IN (");
        let mut separated = query.separated(", ");
        for media_type in media_types {
            separated.push_bind(media_type_key(*media_type));
        }
        separated.push_unseparated(") ORDER BY source_type, source_key LIMIT ");
        query.push_bind(i64::from(limit));

        let rows =
            query.build().fetch_all(&self.pool).await.map_err(|error| {
                db_error("lookup local items by info hash and media type", error)
            })?;

        rows.into_iter().map(local_item_from_row).collect()
    }

    pub async fn local_items_by_media_type(
        &self,
        media_type: MediaType,
        limit: u16,
    ) -> Result<Vec<LocalItem>, DatabaseError> {
        self.local_items_by_media_type_page(media_type, limit, 0)
            .await
    }

    pub async fn local_items_by_media_type_page(
        &self,
        media_type: MediaType,
        limit: u16,
        offset: u32,
    ) -> Result<Vec<LocalItem>, DatabaseError> {
        let rows = sqlx::query(
            r#"
            SELECT id, source_type, source_key, title, display_name, media_type,
                   info_hash, path, save_path, total_size, mtime_ms
            FROM local_items
            WHERE media_type = ?
            ORDER BY title, source_type, source_key
            LIMIT ?
            OFFSET ?
            "#,
        )
        .bind(media_type_key(media_type))
        .bind(i64::from(limit))
        .bind(i64::from(offset))
        .fetch_all(&self.pool)
        .await
        .map_err(|error| db_error("lookup local items by media type", error))?;

        rows.into_iter().map(local_item_from_row).collect()
    }

    pub async fn local_items_by_media_type_keyset_page(
        &self,
        media_type: MediaType,
        limit: u16,
        after: Option<&LocalItemPageCursor>,
    ) -> Result<Vec<LocalItem>, DatabaseError> {
        let rows = if let Some(after) = after {
            sqlx::query(
                r#"
                SELECT id, source_type, source_key, title, display_name, media_type,
                       info_hash, path, save_path, total_size, mtime_ms
                FROM local_items
                WHERE media_type = ?
                  AND (title, source_type, source_key) > (?, ?, ?)
                ORDER BY title, source_type, source_key
                LIMIT ?
                "#,
            )
            .bind(media_type_key(media_type))
            .bind(after.title.as_str())
            .bind(after.source_type.as_str())
            .bind(after.source_key.as_str())
            .bind(i64::from(limit))
            .fetch_all(&self.pool)
            .await
        } else {
            sqlx::query(
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
        }
        .map_err(|error| db_error("lookup local items by media type keyset page", error))?;

        rows.into_iter().map(local_item_from_row).collect()
    }

    pub async fn local_items_by_media_type_and_title_token(
        &self,
        media_type: MediaType,
        title_token: &str,
        limit: u16,
    ) -> Result<Vec<LocalItem>, DatabaseError> {
        self.local_items_by_media_type_and_title_token_page(media_type, title_token, limit, 0)
            .await
    }

    pub async fn local_items_by_media_type_and_title_token_page(
        &self,
        media_type: MediaType,
        title_token: &str,
        limit: u16,
        offset: u32,
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
            OFFSET ?
            "#,
        )
        .bind(media_type_key(media_type))
        .bind(title_token)
        .bind(i64::from(limit))
        .bind(i64::from(offset))
        .fetch_all(&self.pool)
        .await
        .map_err(|error| db_error("lookup local items by media type and title", error))?;

        rows.into_iter().map(local_item_from_row).collect()
    }

    pub async fn local_items_by_media_type_and_title_tokens_page(
        &self,
        media_type: MediaType,
        title_tokens: &[&str],
        preferred_title: &str,
        limit: u16,
        offset: u32,
    ) -> Result<Vec<LocalItem>, DatabaseError> {
        let mut query = QueryBuilder::<Sqlite>::new(
            r#"
            SELECT id, source_type, source_key, title, display_name, media_type,
                   info_hash, path, save_path, total_size, mtime_ms
            FROM local_items
            WHERE media_type =
            "#,
        );
        query.push_bind(media_type_key(media_type));
        for title_token in title_tokens {
            query
                .push(" AND title LIKE '%' || ")
                .push_bind(*title_token)
                .push(" || '%'");
        }
        query
            .push(" ORDER BY CASE WHEN lower(title) = ")
            .push_bind(preferred_title)
            .push(" OR lower(display_name) = ")
            .push_bind(preferred_title)
            .push(" THEN 0 ELSE 1 END, length(title), title, source_type, source_key LIMIT ")
            .push_bind(i64::from(limit))
            .push(" OFFSET ")
            .push_bind(i64::from(offset));

        let rows = query.build().fetch_all(&self.pool).await.map_err(|error| {
            db_error("lookup local items by media type and title tokens", error)
        })?;

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

    pub async fn largest_local_file_for_item(
        &self,
        item_id: LocalItemId,
    ) -> Result<Option<LocalFileSnapshot>, DatabaseError> {
        let row = sqlx::query(
            r#"
            SELECT item_id, relative_path, file_name, size, mtime_ms, file_index
            FROM local_files
            WHERE item_id = ?
            ORDER BY size DESC, COALESCE(mtime_ms, -9223372036854775808), file_index
            LIMIT 1
            "#,
        )
        .bind(i64_from_u64(item_id.get(), "local item id")?)
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| db_error("lookup largest local file", error))?;

        row.map(local_file_snapshot_from_row).transpose()
    }

    pub async fn local_items_with_largest_file_by_media_type_page(
        &self,
        media_type: MediaType,
        limit: u16,
        offset: u32,
    ) -> Result<Vec<LocalItemWithFile>, DatabaseError> {
        let rows = sqlx::query(
            r#"
            WITH paged_items AS (
                SELECT id, source_type, source_key, title, display_name, media_type,
                       info_hash, path, save_path, total_size, mtime_ms
                FROM local_items
                WHERE media_type = ?
                  AND EXISTS (
                      SELECT 1
                      FROM local_files
                      WHERE local_files.item_id = local_items.id
                  )
                ORDER BY title, source_type, source_key
                LIMIT ?
                OFFSET ?
            ),
            ranked_files AS (
                SELECT paged_items.id, paged_items.source_type, paged_items.source_key,
                       paged_items.title, paged_items.display_name, paged_items.media_type,
                       paged_items.info_hash, paged_items.path, paged_items.save_path,
                       paged_items.total_size, paged_items.mtime_ms,
                       local_files.item_id, local_files.relative_path, local_files.file_name,
                       local_files.size, local_files.mtime_ms AS file_mtime_ms,
                       local_files.file_index,
                       ROW_NUMBER() OVER (
                           PARTITION BY paged_items.id
                           ORDER BY local_files.size DESC,
                                    COALESCE(local_files.mtime_ms, -9223372036854775808),
                                    local_files.file_index
                       ) AS file_rank
                FROM paged_items
                JOIN local_files ON local_files.item_id = paged_items.id
            )
            SELECT id, source_type, source_key, title, display_name, media_type,
                   info_hash, path, save_path, total_size, mtime_ms AS item_mtime_ms,
                   item_id, relative_path, file_name, size, file_mtime_ms,
                   file_index
            FROM ranked_files
            WHERE file_rank = 1
            ORDER BY title, source_type, source_key
            "#,
        )
        .bind(media_type_key(media_type))
        .bind(i64::from(limit))
        .bind(i64::from(offset))
        .fetch_all(&self.pool)
        .await
        .map_err(|error| db_error("lookup local items with largest file", error))?;

        rows.into_iter()
            .map(|row| {
                Ok(LocalItemWithFile {
                    item: local_item_with_file_item_from_row_ref(&row)?,
                    file: local_item_with_file_file_from_row_ref(&row)?,
                })
            })
            .collect()
    }

    pub async fn local_items_with_largest_file_by_media_type_keyset_page(
        &self,
        media_type: MediaType,
        limit: u16,
        after: Option<&LocalItemPageCursor>,
    ) -> Result<Vec<LocalItemWithFile>, DatabaseError> {
        let rows = if let Some(after) = after {
            sqlx::query(
                r#"
                WITH paged_items AS (
                    SELECT id, source_type, source_key, title, display_name, media_type,
                           info_hash, path, save_path, total_size, mtime_ms
                    FROM local_items
                    WHERE media_type = ?
                      AND (title, source_type, source_key) > (?, ?, ?)
                      AND EXISTS (
                          SELECT 1
                          FROM local_files
                          WHERE local_files.item_id = local_items.id
                      )
                    ORDER BY title, source_type, source_key
                    LIMIT ?
                ),
                ranked_files AS (
                    SELECT paged_items.id, paged_items.source_type, paged_items.source_key,
                           paged_items.title, paged_items.display_name, paged_items.media_type,
                           paged_items.info_hash, paged_items.path, paged_items.save_path,
                           paged_items.total_size, paged_items.mtime_ms,
                           local_files.item_id, local_files.relative_path, local_files.file_name,
                           local_files.size, local_files.mtime_ms AS file_mtime_ms,
                           local_files.file_index,
                           ROW_NUMBER() OVER (
                               PARTITION BY paged_items.id
                               ORDER BY local_files.size DESC,
                                        COALESCE(local_files.mtime_ms, -9223372036854775808),
                                        local_files.file_index
                           ) AS file_rank
                    FROM paged_items
                    JOIN local_files ON local_files.item_id = paged_items.id
                )
                SELECT id, source_type, source_key, title, display_name, media_type,
                       info_hash, path, save_path, total_size, mtime_ms AS item_mtime_ms,
                       item_id, relative_path, file_name, size, file_mtime_ms,
                       file_index
                FROM ranked_files
                WHERE file_rank = 1
                ORDER BY title, source_type, source_key
                "#,
            )
            .bind(media_type_key(media_type))
            .bind(after.title.as_str())
            .bind(after.source_type.as_str())
            .bind(after.source_key.as_str())
            .bind(i64::from(limit))
            .fetch_all(&self.pool)
            .await
        } else {
            sqlx::query(
                r#"
                WITH paged_items AS (
                    SELECT id, source_type, source_key, title, display_name, media_type,
                           info_hash, path, save_path, total_size, mtime_ms
                    FROM local_items
                    WHERE media_type = ?
                      AND EXISTS (
                          SELECT 1
                          FROM local_files
                          WHERE local_files.item_id = local_items.id
                      )
                    ORDER BY title, source_type, source_key
                    LIMIT ?
                ),
                ranked_files AS (
                    SELECT paged_items.id, paged_items.source_type, paged_items.source_key,
                           paged_items.title, paged_items.display_name, paged_items.media_type,
                           paged_items.info_hash, paged_items.path, paged_items.save_path,
                           paged_items.total_size, paged_items.mtime_ms,
                           local_files.item_id, local_files.relative_path, local_files.file_name,
                           local_files.size, local_files.mtime_ms AS file_mtime_ms,
                           local_files.file_index,
                           ROW_NUMBER() OVER (
                               PARTITION BY paged_items.id
                               ORDER BY local_files.size DESC,
                                        COALESCE(local_files.mtime_ms, -9223372036854775808),
                                        local_files.file_index
                           ) AS file_rank
                    FROM paged_items
                    JOIN local_files ON local_files.item_id = paged_items.id
                )
                SELECT id, source_type, source_key, title, display_name, media_type,
                       info_hash, path, save_path, total_size, mtime_ms AS item_mtime_ms,
                       item_id, relative_path, file_name, size, file_mtime_ms,
                       file_index
                FROM ranked_files
                WHERE file_rank = 1
                ORDER BY title, source_type, source_key
                "#,
            )
            .bind(media_type_key(media_type))
            .bind(i64::from(limit))
            .fetch_all(&self.pool)
            .await
        }
        .map_err(|error| db_error("lookup local items with largest file keyset page", error))?;

        rows.into_iter()
            .map(|row| {
                Ok(LocalItemWithFile {
                    item: local_item_with_file_item_from_row_ref(&row)?,
                    file: local_item_with_file_file_from_row_ref(&row)?,
                })
            })
            .collect()
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
                info_hash = COALESCE(excluded.info_hash, remote_candidates.info_hash),
                torrent_cache_path = COALESCE(excluded.torrent_cache_path, remote_candidates.torrent_cache_path),
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

    pub async fn record_running_jobs_waiting_on_shutdown(
        &self,
        now_ms: i64,
    ) -> Result<u64, DatabaseError> {
        let result = sqlx::query(
            r#"
            UPDATE jobs
            SET state = 'waiting',
                last_finished_at = ?,
                next_run_at = ?,
                last_error = 'shutdown before job completed'
            WHERE state = 'running'
            "#,
        )
        .bind(now_ms)
        .bind(now_ms)
        .execute(&self.pool)
        .await
        .map_err(|error| db_error("record running jobs waiting on shutdown", error))?;

        Ok(result.rows_affected())
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

    pub async fn claim_scheduled_job_run(
        &self,
        name: &JobName,
        now_ms: i64,
    ) -> Result<bool, DatabaseError> {
        let result = sqlx::query(
            r#"
            UPDATE jobs
            SET state = 'running',
                last_started_at = ?,
                next_run_at = NULL,
                last_error = NULL
            WHERE name = ?
              AND state NOT IN ('running', 'disabled')
              AND (next_run_at IS NULL OR next_run_at <= ?)
            "#,
        )
        .bind(now_ms)
        .bind(name.as_str())
        .bind(now_ms)
        .execute(&self.pool)
        .await
        .map_err(|error| db_error("claim scheduled job run", error))?;

        Ok(result.rows_affected() > 0)
    }

    pub async fn claim_immediate_job_run(
        &self,
        name: &JobName,
        now_ms: i64,
    ) -> Result<bool, DatabaseError> {
        let result = sqlx::query(
            r#"
            INSERT INTO jobs (
                name,
                state,
                last_started_at,
                last_finished_at,
                next_run_at,
                last_error
            )
            VALUES (?, 'running', ?, NULL, NULL, NULL)
            ON CONFLICT (name) DO UPDATE SET
                state = excluded.state,
                last_started_at = excluded.last_started_at,
                last_finished_at = COALESCE(excluded.last_finished_at, jobs.last_finished_at),
                next_run_at = excluded.next_run_at,
                last_error = excluded.last_error
            WHERE jobs.state NOT IN ('running', 'disabled')
            "#,
        )
        .bind(name.as_str())
        .bind(now_ms)
        .execute(&self.pool)
        .await
        .map_err(|error| db_error("claim immediate job run", error))?;

        Ok(result.rows_affected() > 0)
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
        let failure_count = if matches!(
            state,
            DependencyState::Healthy { .. } | DependencyState::Unknown
        ) {
            0_i64
        } else {
            1_i64
        };
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
                failure_count,
                checked_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (dependency_type, dependency_name) DO UPDATE SET
                state = excluded.state,
                reason = excluded.reason,
                retry_after = excluded.retry_after,
                failure_count = CASE
                    WHEN excluded.failure_count = 0 THEN 0
                    ELSE MIN(dependency_health.failure_count + 1, 65535)
                END,
                checked_at = excluded.checked_at
            "#,
        )
        .bind(dependency_type)
        .bind(dependency_name.as_str())
        .bind(state_key)
        .bind(reason)
        .bind(retry_after)
        .bind(failure_count)
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

    pub async fn dependency_failure_count(
        &self,
        dependency_type: &str,
        dependency_name: &DependencyName,
    ) -> Result<u16, DatabaseError> {
        let row = sqlx::query(
            r#"
            SELECT failure_count
            FROM dependency_health
            WHERE dependency_type = ?
              AND dependency_name = ?
            "#,
        )
        .bind(dependency_type)
        .bind(dependency_name.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| db_error("read dependency failure count", error))?;
        row.map(|row| failure_count_from_i64(row.get("failure_count")))
            .transpose()
            .map(|count| count.unwrap_or(0))
    }

    pub async fn dependency_health_snapshot(
        &self,
        limit: u16,
    ) -> Result<Vec<DependencyHealthSnapshot>, DatabaseError> {
        let rows = sqlx::query(
            r#"
            SELECT dependency_type, dependency_name, state, reason, retry_after, failure_count,
                   checked_at
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
                    failure_count: failure_count_from_i64(row.get("failure_count"))?,
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
            displace_non_static_indexer_conflicts(
                &mut transaction,
                indexer.name.as_str(),
                indexer.url.as_str(),
                now_ms,
            )
            .await?;
            displace_static_url_rename_conflicts(
                &mut transaction,
                indexer.name.as_str(),
                indexer.url.as_str(),
                now_ms,
            )
            .await?;
            sqlx::query(
                r#"
                INSERT INTO indexers (
                    name,
                    url,
                    source_kind,
                    source_name,
                    source_indexer_id,
                    api_key_source,
                    enabled,
                    capabilities_json,
                    state,
                    retry_after,
                    last_caps_refresh_at,
                    created_at,
                    updated_at
                )
                VALUES (?, ?, 'static', '', ?, ?, ?, '{}', 'unknown', NULL, NULL, ?, ?)
                ON CONFLICT (source_kind, source_name, source_indexer_id) DO UPDATE SET
                    name = excluded.name,
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
            .bind(indexer.name.as_str())
            .bind(indexer.api_key_source.storage_value())
            .bind(if indexer.enabled { 1_i64 } else { 0_i64 })
            .bind(now_ms)
            .bind(now_ms)
            .execute(&mut *transaction)
            .await
            .map_err(|error| db_error("upsert configured indexer", error))?;
        }

        let existing_rows = sqlx::query("SELECT name FROM indexers WHERE source_kind = 'static'")
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

    pub async fn sync_prowlarr_indexers(
        &self,
        source: &DependencyName,
        discovered: &[ProwlarrIndexer],
        remove_policy: ProwlarrRemovePolicy,
        now_ms: i64,
    ) -> Result<Vec<IndexerRegistryRow>, DatabaseError> {
        Ok(self
            .sync_prowlarr_indexers_with_summary(source, discovered, remove_policy, now_ms)
            .await?
            .registry)
    }

    pub async fn sync_prowlarr_indexers_with_summary(
        &self,
        source: &DependencyName,
        discovered: &[ProwlarrIndexer],
        remove_policy: ProwlarrRemovePolicy,
        now_ms: i64,
    ) -> Result<ProwlarrSyncSummary, DatabaseError> {
        let _sync_guard = self.prowlarr_sync_lock.lock().await;
        let _span = info_span!(
            "indexers.prowlarr.sync",
            source = source.as_str(),
            discovered_count = discovered.len()
        );
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|error| db_error("begin Prowlarr indexer sync transaction", error))?;
        let mut source_indexer_ids = BTreeSet::new();
        let mut imported = 0_usize;
        let mut deactivated = 0_u64;
        let mut discovered = discovered.iter().collect::<Vec<_>>();
        discovered.sort_by_key(|indexer| indexer.prowlarr_id);
        for indexer in &discovered {
            if indexer.source != *source {
                return Err(DatabaseError::QueryFailed {
                    operation: "sync Prowlarr indexers".to_owned(),
                    message: format!(
                        "discovered indexer `{}` belongs to source `{}` not `{}`",
                        indexer.name.as_str(),
                        indexer.source.as_str(),
                        source.as_str()
                    ),
                });
            }
        }

        for indexer in discovered {
            let source_indexer_id = indexer.prowlarr_id.to_string();
            source_indexer_ids.insert(source_indexer_id.clone());
            if resolve_active_url_conflicts(
                &mut transaction,
                indexer.url.as_str(),
                "prowlarr",
                source.as_str(),
                &source_indexer_id,
                now_ms,
            )
            .await?
            {
                let result = sqlx::query(
                    r#"
                    UPDATE indexers
                    SET enabled = 0, updated_at = ?
                    WHERE source_kind = 'prowlarr'
                      AND source_name = ?
                      AND source_indexer_id = ?
                      AND enabled != 0
                    "#,
                )
                .bind(now_ms)
                .bind(source.as_str())
                .bind(&source_indexer_id)
                .execute(&mut *transaction)
                .await
                .map_err(|error| db_error("disable duplicate Prowlarr indexer", error))?;
                deactivated = deactivated.saturating_add(result.rows_affected());
                continue;
            }

            let desired_name = prowlarr_registry_name(source, indexer);
            let name = available_indexer_name(
                &mut transaction,
                &desired_name,
                "prowlarr",
                source.as_str(),
                &source_indexer_id,
            )
            .await?;
            sqlx::query(
                r#"
                INSERT INTO indexers (
                    name,
                    url,
                    source_kind,
                    source_name,
                    source_indexer_id,
                    api_key_source,
                    enabled,
                    capabilities_json,
                    state,
                    retry_after,
                    last_caps_refresh_at,
                    created_at,
                    updated_at
                )
                VALUES (?, ?, 'prowlarr', ?, ?, ?, 1, '{}', 'unknown', NULL, NULL, ?, ?)
                ON CONFLICT (source_kind, source_name, source_indexer_id) DO UPDATE SET
                    name = excluded.name,
                    url = excluded.url,
                    api_key_source = excluded.api_key_source,
                    enabled = 1,
                    state = CASE
                        WHEN indexers.state = 'unknown_error' THEN 'unknown'
                        ELSE indexers.state
                    END,
                    updated_at = excluded.updated_at
                "#,
            )
            .bind(name.as_str())
            .bind(indexer.url.as_str())
            .bind(source.as_str())
            .bind(&source_indexer_id)
            .bind(indexer.api_key_source.storage_value())
            .bind(now_ms)
            .bind(now_ms)
            .execute(&mut *transaction)
            .await
            .map_err(|error| db_error("upsert Prowlarr indexer", error))?;
            imported = imported.saturating_add(1);
        }

        if remove_policy == ProwlarrRemovePolicy::Deactivate {
            if source_indexer_ids.is_empty() {
                let result = sqlx::query(
                    r#"
                    UPDATE indexers
                    SET enabled = 0, updated_at = ?
                    WHERE source_kind = 'prowlarr' AND source_name = ? AND enabled != 0
                    "#,
                )
                .bind(now_ms)
                .bind(source.as_str())
                .execute(&mut *transaction)
                .await
                .map_err(|error| db_error("disable removed Prowlarr indexers", error))?;
                deactivated = deactivated.saturating_add(result.rows_affected());
            } else {
                let mut query = QueryBuilder::new("UPDATE indexers SET enabled = 0, updated_at = ");
                query.push_bind(now_ms);
                query.push(" WHERE source_kind = 'prowlarr' AND source_name = ");
                query.push_bind(source.as_str());
                query.push(" AND enabled != 0");
                query.push(" AND source_indexer_id NOT IN (");
                let mut separated = query.separated(", ");
                for source_indexer_id in &source_indexer_ids {
                    separated.push_bind(source_indexer_id);
                }
                separated.push_unseparated(")");
                let result = query
                    .build()
                    .execute(&mut *transaction)
                    .await
                    .map_err(|error| db_error("disable removed Prowlarr indexers", error))?;
                deactivated = deactivated.saturating_add(result.rows_affected());
            }
        }

        transaction
            .commit()
            .await
            .map_err(|error| db_error("commit Prowlarr indexer sync transaction", error))?;

        Ok(ProwlarrSyncSummary {
            registry: self.indexer_registry_snapshot(1_000).await?,
            imported,
            deactivated,
        })
    }

    pub async fn indexer_registry_snapshot(
        &self,
        limit: u16,
    ) -> Result<Vec<IndexerRegistryRow>, DatabaseError> {
        let rows = sqlx::query(
            r#"
            SELECT
                id,
                name,
                url,
                source_kind,
                source_name,
                source_indexer_id,
                api_key_source,
                enabled,
                state,
                retry_after,
                last_caps_refresh_at
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
            .map(indexer_registry_row_from_row)
            .collect()
    }

    pub async fn due_indexer_registry_page(
        &self,
        now_ms: i64,
        after_name: Option<&DependencyName>,
        limit: u16,
    ) -> Result<Vec<IndexerRegistryRow>, DatabaseError> {
        let rows = sqlx::query(
            r#"
            SELECT
                id,
                name,
                url,
                source_kind,
                source_name,
                source_indexer_id,
                api_key_source,
                enabled,
                state,
                retry_after,
                last_caps_refresh_at
            FROM indexers
            WHERE enabled != 0
              AND (retry_after IS NULL OR retry_after <= ?)
              AND (? IS NULL OR name > ?)
            ORDER BY name
            LIMIT ?
            "#,
        )
        .bind(now_ms)
        .bind(after_name.map(DependencyName::as_str))
        .bind(after_name.map(DependencyName::as_str))
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(|error| db_error("read due indexer registry page", error))?;

        rows.into_iter()
            .map(indexer_registry_row_from_row)
            .collect()
    }

    pub async fn indexer_caps_backoff_summary(
        &self,
        now_ms: i64,
    ) -> Result<(usize, Option<i64>), DatabaseError> {
        let row = sqlx::query(
            r#"
            SELECT COUNT(*) AS backoff_count, MIN(retry_after) AS next_retry_after
            FROM indexers
            WHERE enabled != 0
              AND retry_after > ?
            "#,
        )
        .bind(now_ms)
        .fetch_one(&self.pool)
        .await
        .map_err(|error| db_error("read indexer caps backoff summary", error))?;
        let count = usize::try_from(row.get::<i64, _>("backoff_count")).map_err(|error| {
            DatabaseError::QueryFailed {
                operation: "read indexer caps backoff count".to_owned(),
                message: error.to_string(),
            }
        })?;
        Ok((count, row.get("next_retry_after")))
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

    pub async fn indexer_search_caps_snapshot(
        &self,
        limit: u16,
    ) -> Result<Vec<IndexerSearchCapsRow>, DatabaseError> {
        let rows = sqlx::query(
            r#"
            SELECT
                id,
                name,
                url,
                source_kind,
                source_name,
                source_indexer_id,
                api_key_source,
                enabled,
                retry_after,
                capabilities_json
            FROM indexers
            WHERE capabilities_json IS NOT NULL
            ORDER BY name
            LIMIT ?
            "#,
        )
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(|error| db_error("read indexer search caps snapshot", error))?;

        rows.into_iter()
            .map(indexer_search_caps_row_from_row)
            .collect()
    }

    pub async fn ready_indexer_search_caps_page(
        &self,
        now_ms: i64,
        after_name: Option<&DependencyName>,
        limit: u16,
    ) -> Result<Vec<IndexerSearchCapsRow>, DatabaseError> {
        let rows = sqlx::query(
            r#"
            SELECT
                id,
                name,
                url,
                source_kind,
                source_name,
                source_indexer_id,
                api_key_source,
                enabled,
                retry_after,
                capabilities_json
            FROM indexers
            WHERE enabled != 0
              AND last_caps_refresh_at IS NOT NULL
              AND (retry_after IS NULL OR retry_after <= ?)
              AND (? IS NULL OR name > ?)
            ORDER BY name
            LIMIT ?
            "#,
        )
        .bind(now_ms)
        .bind(after_name.map(DependencyName::as_str))
        .bind(after_name.map(DependencyName::as_str))
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(|error| db_error("read ready indexer search caps page", error))?;

        rows.into_iter()
            .map(indexer_search_caps_row_from_row)
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

    pub async fn record_indexer_request_success(
        &self,
        name: &DependencyName,
        checked_at_ms: i64,
    ) -> Result<(), DatabaseError> {
        sqlx::query(
            r#"
            UPDATE indexers
            SET state = 'healthy',
                retry_after = NULL,
                updated_at = ?
            WHERE name = ?
            "#,
        )
        .bind(checked_at_ms)
        .bind(name.as_str())
        .execute(&self.pool)
        .await
        .map_err(|error| db_error("record indexer request success", error))?;

        self.record_dependency_health(
            "indexer",
            name,
            &DependencyState::Healthy { checked_at_ms },
            checked_at_ms,
        )
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
                download_url = NULL,
                cookie = NULL,
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
                download_url = NULL,
                cookie = NULL,
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

    pub async fn cleanup_terminal_announce_work(
        &self,
        success_cutoff_ms: i64,
        failure_cutoff_ms: i64,
        limit: u16,
    ) -> Result<u64, DatabaseError> {
        let success = sqlx::query(
            r#"
            DELETE FROM announce_work
            WHERE id IN (
                SELECT id
                FROM announce_work
                WHERE status = 'succeeded'
                  AND finished_at IS NOT NULL
                  AND finished_at <= ?
                ORDER BY finished_at, id
                LIMIT ?
            )
            "#,
        )
        .bind(success_cutoff_ms)
        .bind(i64::from(limit))
        .execute(&self.pool)
        .await
        .map_err(|error| db_error("cleanup succeeded announce work", error))?;
        let terminal_failed = self
            .cleanup_terminal_announce_status("terminal_failed", failure_cutoff_ms, limit)
            .await?;
        let expired = self
            .cleanup_terminal_announce_status("expired", failure_cutoff_ms, limit)
            .await?;

        Ok(success
            .rows_affected()
            .saturating_add(terminal_failed)
            .saturating_add(expired))
    }

    async fn cleanup_terminal_announce_status(
        &self,
        status: &str,
        cutoff_ms: i64,
        limit: u16,
    ) -> Result<u64, DatabaseError> {
        let query = match status {
            "terminal_failed" => {
                r#"
            DELETE FROM announce_work
            WHERE id IN (
                SELECT id
                FROM announce_work
                WHERE status = 'terminal_failed'
                  AND finished_at IS NOT NULL
                  AND finished_at <= ?
                ORDER BY finished_at, id
                LIMIT ?
            )
            "#
            }
            "expired" => {
                r#"
            DELETE FROM announce_work
            WHERE id IN (
                SELECT id
                FROM announce_work
                WHERE status = 'expired'
                  AND finished_at IS NOT NULL
                  AND finished_at <= ?
                ORDER BY finished_at, id
                LIMIT ?
            )
            "#
            }
            _ => {
                return Err(DatabaseError::QueryFailed {
                    operation: "cleanup terminal announce status".to_owned(),
                    message: format!("unsupported terminal status {status}"),
                });
            }
        };
        let result = sqlx::query(query)
            .bind(cutoff_ms)
            .bind(i64::from(limit))
            .execute(&self.pool)
            .await
            .map_err(|error| db_error("cleanup terminal announce status", error))?;

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
                download_url = NULL,
                cookie = NULL,
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

impl<'a> LocalInventoryReplaceTransaction<'a> {
    pub async fn local_items_by_media_type_keyset_page(
        &mut self,
        media_type: MediaType,
        limit: u16,
        after: Option<&LocalItemPageCursor>,
    ) -> Result<Vec<LocalItem>, DatabaseError> {
        let rows = if let Some(after) = after {
            sqlx::query(
                r#"
                SELECT id, source_type, source_key, title, display_name, media_type,
                       info_hash, path, save_path, total_size, mtime_ms
                FROM local_items
                WHERE media_type = ?
                  AND (title, source_type, source_key) > (?, ?, ?)
                ORDER BY title, source_type, source_key
                LIMIT ?
                "#,
            )
            .bind(media_type_key(media_type))
            .bind(after.title.as_str())
            .bind(after.source_type.as_str())
            .bind(after.source_key.as_str())
            .bind(i64::from(limit))
            .fetch_all(&mut *self.transaction)
            .await
        } else {
            sqlx::query(
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
            .fetch_all(&mut *self.transaction)
            .await
        }
        .map_err(|error| db_error("lookup local items by media type keyset page", error))?;

        rows.into_iter().map(local_item_from_row).collect()
    }

    pub async fn local_items_with_largest_file_by_media_type_keyset_page(
        &mut self,
        media_type: MediaType,
        limit: u16,
        after: Option<&LocalItemPageCursor>,
    ) -> Result<Vec<LocalItemWithFile>, DatabaseError> {
        let rows = if let Some(after) = after {
            sqlx::query(
                r#"
                WITH paged_items AS (
                    SELECT id, source_type, source_key, title, display_name, media_type,
                           info_hash, path, save_path, total_size, mtime_ms
                    FROM local_items
                    WHERE media_type = ?
                      AND (title, source_type, source_key) > (?, ?, ?)
                      AND EXISTS (
                          SELECT 1
                          FROM local_files
                          WHERE local_files.item_id = local_items.id
                      )
                    ORDER BY title, source_type, source_key
                    LIMIT ?
                ),
                ranked_files AS (
                    SELECT paged_items.id, paged_items.source_type, paged_items.source_key,
                           paged_items.title, paged_items.display_name, paged_items.media_type,
                           paged_items.info_hash, paged_items.path, paged_items.save_path,
                           paged_items.total_size, paged_items.mtime_ms,
                           local_files.item_id, local_files.relative_path, local_files.file_name,
                           local_files.size, local_files.mtime_ms AS file_mtime_ms,
                           local_files.file_index,
                           ROW_NUMBER() OVER (
                               PARTITION BY paged_items.id
                               ORDER BY local_files.size DESC,
                                        COALESCE(local_files.mtime_ms, -9223372036854775808),
                                        local_files.file_index
                           ) AS file_rank
                    FROM paged_items
                    JOIN local_files ON local_files.item_id = paged_items.id
                )
                SELECT id, source_type, source_key, title, display_name, media_type,
                       info_hash, path, save_path, total_size, mtime_ms AS item_mtime_ms,
                       item_id, relative_path, file_name, size, file_mtime_ms,
                       file_index
                FROM ranked_files
                WHERE file_rank = 1
                ORDER BY title, source_type, source_key
                "#,
            )
            .bind(media_type_key(media_type))
            .bind(after.title.as_str())
            .bind(after.source_type.as_str())
            .bind(after.source_key.as_str())
            .bind(i64::from(limit))
            .fetch_all(&mut *self.transaction)
            .await
        } else {
            sqlx::query(
                r#"
                WITH paged_items AS (
                    SELECT id, source_type, source_key, title, display_name, media_type,
                           info_hash, path, save_path, total_size, mtime_ms
                    FROM local_items
                    WHERE media_type = ?
                      AND EXISTS (
                          SELECT 1
                          FROM local_files
                          WHERE local_files.item_id = local_items.id
                      )
                    ORDER BY title, source_type, source_key
                    LIMIT ?
                ),
                ranked_files AS (
                    SELECT paged_items.id, paged_items.source_type, paged_items.source_key,
                           paged_items.title, paged_items.display_name, paged_items.media_type,
                           paged_items.info_hash, paged_items.path, paged_items.save_path,
                           paged_items.total_size, paged_items.mtime_ms,
                           local_files.item_id, local_files.relative_path, local_files.file_name,
                           local_files.size, local_files.mtime_ms AS file_mtime_ms,
                           local_files.file_index,
                           ROW_NUMBER() OVER (
                               PARTITION BY paged_items.id
                               ORDER BY local_files.size DESC,
                                        COALESCE(local_files.mtime_ms, -9223372036854775808),
                                        local_files.file_index
                           ) AS file_rank
                    FROM paged_items
                    JOIN local_files ON local_files.item_id = paged_items.id
                )
                SELECT id, source_type, source_key, title, display_name, media_type,
                       info_hash, path, save_path, total_size, mtime_ms AS item_mtime_ms,
                       item_id, relative_path, file_name, size, file_mtime_ms,
                       file_index
                FROM ranked_files
                WHERE file_rank = 1
                ORDER BY title, source_type, source_key
                "#,
            )
            .bind(media_type_key(media_type))
            .bind(i64::from(limit))
            .fetch_all(&mut *self.transaction)
            .await
        }
        .map_err(|error| db_error("lookup local items with largest file keyset page", error))?;

        rows.into_iter()
            .map(|row| {
                Ok(LocalItemWithFile {
                    item: local_item_with_file_item_from_row_ref(&row)?,
                    file: local_item_with_file_file_from_row_ref(&row)?,
                })
            })
            .collect()
    }

    pub async fn initialize_virtual_season_candidate_stage(&mut self) -> Result<(), DatabaseError> {
        for statement in [
            "DROP TABLE IF EXISTS staged_virtual_season_real_keys",
            "DROP TABLE IF EXISTS staged_virtual_season_state",
            "DROP TABLE IF EXISTS staged_virtual_season_episodes",
            r#"
            CREATE TEMP TABLE staged_virtual_season_real_keys (
                title TEXT NOT NULL,
                season INTEGER NOT NULL,
                PRIMARY KEY (title, season)
            ) WITHOUT ROWID
            "#,
            r#"
            CREATE TEMP TABLE staged_virtual_season_state (
                title TEXT NOT NULL,
                season INTEGER NOT NULL,
                newest_mtime_ms INTEGER,
                PRIMARY KEY (title, season)
            ) WITHOUT ROWID
            "#,
            r#"
            CREATE TEMP TABLE staged_virtual_season_episodes (
                title TEXT NOT NULL,
                season INTEGER NOT NULL,
                episode INTEGER NOT NULL,
                source_file TEXT NOT NULL,
                size INTEGER NOT NULL,
                mtime_ms INTEGER,
                PRIMARY KEY (title, season, episode)
            ) WITHOUT ROWID
            "#,
        ] {
            sqlx::query(statement)
                .execute(&mut *self.transaction)
                .await
                .map_err(|error| db_error("initialize virtual season stage", error))?;
        }

        Ok(())
    }

    pub async fn stage_virtual_real_season_key(
        &mut self,
        title: &str,
        season: u16,
    ) -> Result<(), DatabaseError> {
        sqlx::query(
            r#"
            INSERT OR IGNORE INTO staged_virtual_season_real_keys (title, season)
            VALUES (?, ?)
            "#,
        )
        .bind(title)
        .bind(i64::from(season))
        .execute(&mut *self.transaction)
        .await
        .map_err(|error| db_error("stage real virtual season key", error))?;

        Ok(())
    }

    pub async fn stage_virtual_episode_candidate(
        &mut self,
        candidate: &StagedVirtualEpisodeCandidate,
    ) -> Result<(), DatabaseError> {
        sqlx::query(
            r#"
            INSERT INTO staged_virtual_season_state (
                title,
                season,
                newest_mtime_ms
            )
            VALUES (?, ?, ?)
            ON CONFLICT (title, season) DO UPDATE SET
                newest_mtime_ms = CASE
                    WHEN staged_virtual_season_state.newest_mtime_ms IS NULL
                        THEN excluded.newest_mtime_ms
                    WHEN excluded.newest_mtime_ms IS NULL
                        THEN staged_virtual_season_state.newest_mtime_ms
                    WHEN excluded.newest_mtime_ms > staged_virtual_season_state.newest_mtime_ms
                        THEN excluded.newest_mtime_ms
                    ELSE staged_virtual_season_state.newest_mtime_ms
                END
            "#,
        )
        .bind(&candidate.title)
        .bind(i64::from(candidate.season))
        .bind(candidate.newest_mtime_ms)
        .execute(&mut *self.transaction)
        .await
        .map_err(|error| db_error("stage virtual season state", error))?;

        sqlx::query(
            r#"
            INSERT INTO staged_virtual_season_episodes (
                title,
                season,
                episode,
                source_file,
                size,
                mtime_ms
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (title, season, episode) DO UPDATE SET
                source_file = excluded.source_file,
                size = excluded.size,
                mtime_ms = excluded.mtime_ms
            WHERE excluded.size > staged_virtual_season_episodes.size
               OR (
                   excluded.size = staged_virtual_season_episodes.size
                   AND COALESCE(excluded.mtime_ms, -9223372036854775808)
                       < COALESCE(staged_virtual_season_episodes.mtime_ms, -9223372036854775808)
               )
            "#,
        )
        .bind(&candidate.title)
        .bind(i64::from(candidate.season))
        .bind(i64::from(candidate.episode))
        .bind(path_to_string(&candidate.source_file))
        .bind(i64_from_u64(
            candidate.size.get(),
            "virtual season candidate size",
        )?)
        .bind(candidate.mtime_ms)
        .execute(&mut *self.transaction)
        .await
        .map_err(|error| db_error("stage virtual episode candidate", error))?;

        Ok(())
    }

    pub async fn staged_virtual_seasons_page(
        &mut self,
        limit: u16,
        after: Option<&StagedVirtualSeasonCursor>,
    ) -> Result<Vec<StagedVirtualSeason>, DatabaseError> {
        let rows = if let Some(after) = after {
            sqlx::query(
                r#"
                WITH paged_seasons AS (
                    SELECT state.title, state.season, state.newest_mtime_ms
                    FROM staged_virtual_season_state state
                    WHERE (state.title, state.season) > (?, ?)
                      AND EXISTS (
                            SELECT 1
                            FROM staged_virtual_season_episodes episodes
                            WHERE episodes.title = state.title
                              AND episodes.season = state.season
                          )
                      AND NOT EXISTS (
                            SELECT 1
                            FROM staged_virtual_season_real_keys real_keys
                            WHERE real_keys.title = state.title
                              AND real_keys.season = state.season
                          )
                    ORDER BY state.title, state.season
                    LIMIT ?
                )
                SELECT paged_seasons.title, paged_seasons.season,
                       paged_seasons.newest_mtime_ms, episodes.episode,
                       episodes.source_file, episodes.size, episodes.mtime_ms
                FROM paged_seasons
                INNER JOIN staged_virtual_season_episodes episodes
                    ON episodes.title = paged_seasons.title
                   AND episodes.season = paged_seasons.season
                ORDER BY paged_seasons.title, paged_seasons.season, episodes.episode
                "#,
            )
            .bind(after.title.as_str())
            .bind(i64::from(after.season))
            .bind(i64::from(limit))
            .fetch_all(&mut *self.transaction)
            .await
        } else {
            sqlx::query(
                r#"
                WITH paged_seasons AS (
                    SELECT state.title, state.season, state.newest_mtime_ms
                    FROM staged_virtual_season_state state
                    WHERE EXISTS (
                            SELECT 1
                            FROM staged_virtual_season_episodes episodes
                            WHERE episodes.title = state.title
                              AND episodes.season = state.season
                          )
                      AND NOT EXISTS (
                            SELECT 1
                            FROM staged_virtual_season_real_keys real_keys
                            WHERE real_keys.title = state.title
                              AND real_keys.season = state.season
                          )
                    ORDER BY state.title, state.season
                    LIMIT ?
                )
                SELECT paged_seasons.title, paged_seasons.season,
                       paged_seasons.newest_mtime_ms, episodes.episode,
                       episodes.source_file, episodes.size, episodes.mtime_ms
                FROM paged_seasons
                INNER JOIN staged_virtual_season_episodes episodes
                    ON episodes.title = paged_seasons.title
                   AND episodes.season = paged_seasons.season
                ORDER BY paged_seasons.title, paged_seasons.season, episodes.episode
                "#,
            )
            .bind(i64::from(limit))
            .fetch_all(&mut *self.transaction)
            .await
        }
        .map_err(|error| db_error("lookup staged virtual seasons", error))?;

        let mut seasons = Vec::<StagedVirtualSeason>::new();
        for row in rows {
            let title: String = row.get("title");
            let season = u16_from_i64(row.get("season"), "virtual season number")?;
            let is_new_season = seasons
                .last()
                .is_none_or(|current| current.title != title || current.season != season);
            if is_new_season {
                seasons.push(StagedVirtualSeason {
                    title,
                    season,
                    newest_mtime_ms: row.get("newest_mtime_ms"),
                    episodes: Vec::new(),
                });
            }
            let season = seasons
                .last_mut()
                .expect("staged season exists after insertion");
            season.episodes.push(StagedVirtualSeasonEpisode {
                episode: u16_from_i64(row.get("episode"), "virtual season episode number")?,
                source_file: PathBuf::from(row.get::<String, _>("source_file")),
                size: byte_size_from_i64(row.get("size"), "virtual season episode size")?,
                mtime_ms: row.get("mtime_ms"),
            });
        }

        Ok(seasons)
    }

    pub async fn retain_item(
        &mut self,
        batch: &OwnedLocalItemFileBatch,
    ) -> Result<(), DatabaseError> {
        let (source_type, source_key) = local_source(&batch.item.source);
        if !self.scope.accepts(&source_type, &source_key) {
            return Err(DatabaseError::QueryFailed {
                operation: "validate local inventory refresh scope".to_owned(),
                message: format!(
                    "item source {source_type}:{source_key} is outside {:?}",
                    self.scope
                ),
            });
        }

        upsert_local_item_with_files_in_transaction(
            &mut self.transaction,
            &batch.item,
            &batch.files,
        )
        .await?;
        insert_retained_key(&mut self.transaction, &source_key).await?;
        self.upserted = self.upserted.saturating_add(1);

        Ok(())
    }

    pub async fn commit(mut self) -> Result<LocalInventoryReplaceSummary, DatabaseError> {
        let pruned = prune_local_items_not_retained(&mut self.transaction, &self.scope).await?;
        clear_retained_keys(&mut self.transaction).await?;
        self.transaction
            .commit()
            .await
            .map_err(|error| db_error("commit local inventory transaction", error))?;

        Ok(LocalInventoryReplaceSummary {
            upserted: self.upserted,
            pruned,
        })
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

async fn reconcile_inline_schema(pool: &SqlitePool) -> Result<(), DatabaseError> {
    if table_exists(pool, "indexers").await? {
        let columns = table_columns(pool, "indexers").await?;
        if !columns.contains("source_kind")
            || !columns.contains("source_name")
            || !columns.contains("source_indexer_id")
            || indexers_has_legacy_url_unique(pool).await?
        {
            rebuild_indexers_table(pool).await?;
        }
    }
    add_column_if_missing(
        pool,
        "dependency_health",
        "failure_count",
        "ALTER TABLE dependency_health ADD COLUMN failure_count INTEGER NOT NULL DEFAULT 0",
    )
    .await?;
    Ok(())
}

async fn table_exists(pool: &SqlitePool, table: &str) -> Result<bool, DatabaseError> {
    let exists: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?")
            .bind(table)
            .fetch_one(pool)
            .await
            .map_err(|error| db_error("inspect sqlite schema", error))?;
    Ok(exists > 0)
}

async fn table_columns(pool: &SqlitePool, table: &str) -> Result<BTreeSet<String>, DatabaseError> {
    let statement = format!("PRAGMA table_info({table})");
    let rows = sqlx::query(&statement)
        .fetch_all(pool)
        .await
        .map_err(|error| db_error("inspect sqlite table columns", error))?;
    Ok(rows
        .into_iter()
        .map(|row| row.get::<String, _>("name"))
        .collect())
}

async fn add_column_if_missing(
    pool: &SqlitePool,
    table: &str,
    column: &str,
    statement: &str,
) -> Result<(), DatabaseError> {
    if !table_columns(pool, table).await?.contains(column) {
        pool.execute(statement)
            .await
            .map_err(|error| db_error("reconcile sqlite schema", error))?;
    }
    Ok(())
}

async fn indexers_has_legacy_url_unique(pool: &SqlitePool) -> Result<bool, DatabaseError> {
    let rows = sqlx::query("PRAGMA index_list(indexers)")
        .fetch_all(pool)
        .await
        .map_err(|error| db_error("inspect indexer indexes", error))?;
    for row in rows {
        let unique: i64 = row.get("unique");
        let partial: i64 = row.get("partial");
        if unique == 0 || partial != 0 {
            continue;
        }
        let name: String = row.get("name");
        let statement = format!("PRAGMA index_info({name})");
        let columns = sqlx::query(&statement)
            .fetch_all(pool)
            .await
            .map_err(|error| db_error("inspect indexer index columns", error))?
            .into_iter()
            .map(|row| row.get::<String, _>("name"))
            .collect::<Vec<_>>();
        if columns == ["url"] {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn rebuild_indexers_table(pool: &SqlitePool) -> Result<(), DatabaseError> {
    let legacy_columns = table_columns(pool, "indexers").await?;
    let source_kind = if legacy_columns.contains("source_kind") {
        "COALESCE(source_kind, 'static')"
    } else {
        "'static'"
    };
    let source_name = if legacy_columns.contains("source_name") {
        "COALESCE(source_name, '')"
    } else {
        "''"
    };
    let source_indexer_id = if legacy_columns.contains("source_indexer_id") {
        "COALESCE(NULLIF(source_indexer_id, ''), name)"
    } else {
        "name"
    };
    let insert_statement = format!(
        r#"
        INSERT INTO indexers (
            id,
            name,
            url,
            source_kind,
            source_name,
            source_indexer_id,
            api_key_source,
            enabled,
            capabilities_json,
            state,
            retry_after,
            last_caps_refresh_at,
            created_at,
            updated_at
        )
        SELECT
            id,
            name,
            url,
            {source_kind},
            {source_name},
            {source_indexer_id},
            api_key_source,
            enabled,
            capabilities_json,
            state,
            retry_after,
            last_caps_refresh_at,
            created_at,
            updated_at
        FROM indexers_legacy
        "#
    );
    let mut connection = pool
        .acquire()
        .await
        .map_err(|error| db_error("acquire indexer schema reconciliation connection", error))?;
    sqlx::query("PRAGMA legacy_alter_table = ON")
        .execute(&mut *connection)
        .await
        .map_err(|error| db_error("enable legacy sqlite table rename", error))?;
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&mut *connection)
        .await
        .map_err(|error| {
            db_error(
                "disable sqlite foreign keys for schema reconciliation",
                error,
            )
        })?;

    let rebuild_result = async {
        let mut transaction = connection
            .begin()
            .await
            .map_err(|error| db_error("begin indexer schema reconciliation", error))?;
        for statement in [
            "ALTER TABLE indexers RENAME TO indexers_legacy",
            r#"
            CREATE TABLE indexers (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                url TEXT NOT NULL,
                source_kind TEXT NOT NULL DEFAULT 'static',
                source_name TEXT NOT NULL DEFAULT '',
                source_indexer_id TEXT NOT NULL DEFAULT '',
                api_key_source TEXT NOT NULL,
                enabled INTEGER NOT NULL,
                capabilities_json TEXT NOT NULL DEFAULT '{}',
                state TEXT NOT NULL,
                retry_after INTEGER,
                last_caps_refresh_at INTEGER,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                UNIQUE (name),
                UNIQUE (source_kind, source_name, source_indexer_id)
            )
            "#,
            insert_statement.as_str(),
            "DROP TABLE indexers_legacy",
        ] {
            sqlx::query(statement)
                .execute(&mut *transaction)
                .await
                .map_err(|error| db_error("reconcile indexer schema", error))?;
        }
        transaction
            .commit()
            .await
            .map_err(|error| db_error("commit indexer schema reconciliation", error))
    }
    .await;
    let restore_foreign_keys = sqlx::query("PRAGMA foreign_keys = ON")
        .execute(&mut *connection)
        .await
        .map_err(|error| db_error("restore sqlite foreign keys", error));
    let restore_legacy_alter_table = sqlx::query("PRAGMA legacy_alter_table = OFF")
        .execute(&mut *connection)
        .await
        .map_err(|error| db_error("restore sqlite table rename behavior", error));

    rebuild_result?;
    restore_foreign_keys?;
    restore_legacy_alter_table?;
    Ok(())
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

async fn initialize_staged_local_inventory(
    connection: &mut sqlx::pool::PoolConnection<Sqlite>,
) -> Result<(), DatabaseError> {
    for statement in [
        "DROP TABLE IF EXISTS staged_local_inventory_items",
        "DROP TABLE IF EXISTS staged_local_inventory_files",
        r#"
        CREATE TEMP TABLE staged_local_inventory_items (
            source_type TEXT NOT NULL,
            source_key TEXT NOT NULL,
            title TEXT NOT NULL,
            display_name TEXT NOT NULL,
            media_type TEXT NOT NULL,
            info_hash TEXT,
            path TEXT,
            save_path TEXT,
            total_size INTEGER NOT NULL,
            mtime_ms INTEGER,
            PRIMARY KEY (source_type, source_key)
        ) WITHOUT ROWID
        "#,
        r#"
        CREATE TEMP TABLE staged_local_inventory_files (
            source_type TEXT NOT NULL,
            source_key TEXT NOT NULL,
            relative_path TEXT NOT NULL,
            file_name TEXT NOT NULL,
            size INTEGER NOT NULL,
            mtime_ms INTEGER,
            file_index INTEGER NOT NULL
        )
        "#,
        r#"
        CREATE INDEX staged_local_inventory_files_item_idx
            ON staged_local_inventory_files (source_type, source_key)
        "#,
    ] {
        sqlx::query(statement)
            .execute(&mut **connection)
            .await
            .map_err(|error| db_error("initialize staged local inventory", error))?;
    }

    Ok(())
}

async fn clear_staged_local_inventory(
    connection: &mut sqlx::pool::PoolConnection<Sqlite>,
) -> Result<(), DatabaseError> {
    for statement in STAGED_LOCAL_INVENTORY_CLEAR_STATEMENTS {
        sqlx::query(statement)
            .execute(&mut **connection)
            .await
            .map_err(|error| db_error("clear staged local inventory", error))?;
    }

    Ok(())
}

const STAGED_LOCAL_INVENTORY_CLEAR_STATEMENTS: [&str; 2] = [
    "DELETE FROM staged_local_inventory_files",
    "DELETE FROM staged_local_inventory_items",
];

async fn clear_staged_local_inventory_in_transaction(
    transaction: &mut Transaction<'_, Sqlite>,
) -> Result<(), DatabaseError> {
    for statement in STAGED_LOCAL_INVENTORY_CLEAR_STATEMENTS {
        sqlx::query(statement)
            .execute(&mut **transaction)
            .await
            .map_err(|error| db_error("clear staged local inventory", error))?;
    }

    Ok(())
}

async fn stage_local_item_with_files(
    connection: &mut sqlx::pool::PoolConnection<Sqlite>,
    item: &LocalItem,
    files: &[LocalFile],
) -> Result<(), DatabaseError> {
    let (source_type, source_key) = local_source(&item.source);
    sqlx::query(
        r#"
        INSERT INTO staged_local_inventory_items (
            source_type,
            source_key,
            title,
            display_name,
            media_type,
            info_hash,
            path,
            save_path,
            total_size,
            mtime_ms
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (source_type, source_key) DO UPDATE SET
            title = excluded.title,
            display_name = excluded.display_name,
            media_type = excluded.media_type,
            info_hash = excluded.info_hash,
            path = excluded.path,
            save_path = excluded.save_path,
            total_size = excluded.total_size,
            mtime_ms = excluded.mtime_ms
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
    .execute(&mut **connection)
    .await
    .map_err(|error| db_error("stage local item", error))?;

    sqlx::query(
        "DELETE FROM staged_local_inventory_files WHERE source_type = ? AND source_key = ?",
    )
    .bind(&source_type)
    .bind(&source_key)
    .execute(&mut **connection)
    .await
    .map_err(|error| db_error("replace staged local files", error))?;

    for file in files {
        sqlx::query(
            r#"
            INSERT INTO staged_local_inventory_files (
                source_type,
                source_key,
                relative_path,
                file_name,
                size,
                mtime_ms,
                file_index
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(&source_type)
        .bind(&source_key)
        .bind(path_to_string(&file.relative_path))
        .bind(file.file_name.as_str())
        .bind(i64_from_u64(file.size.get(), "local file size")?)
        .bind(file.mtime_ms)
        .bind(i64::from(file.file_index.get()))
        .execute(&mut **connection)
        .await
        .map_err(|error| db_error("stage local file", error))?;
    }

    Ok(())
}

async fn upsert_staged_local_inventory(
    transaction: &mut Transaction<'_, Sqlite>,
) -> Result<(), DatabaseError> {
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
        SELECT
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
            unixepoch() * 1000,
            unixepoch() * 1000
        FROM staged_local_inventory_items
        WHERE true
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
    .execute(&mut **transaction)
    .await
    .map_err(|error| db_error("upsert staged local inventory", error))?;

    sqlx::query(
        r#"
        DELETE FROM local_files
        WHERE item_id IN (
            SELECT local_items.id
            FROM local_items
            INNER JOIN staged_local_inventory_items staged
                ON staged.source_type = local_items.source_type
               AND staged.source_key = local_items.source_key
        )
        "#,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|error| db_error("replace staged local files", error))?;

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
        SELECT
            local_items.id,
            staged.relative_path,
            staged.file_name,
            staged.size,
            staged.mtime_ms,
            staged.file_index
        FROM staged_local_inventory_files staged
        INNER JOIN local_items
            ON local_items.source_type = staged.source_type
           AND local_items.source_key = staged.source_key
        "#,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|error| db_error("insert staged local files", error))?;

    Ok(())
}

async fn insert_staged_retained_keys(
    transaction: &mut Transaction<'_, Sqlite>,
) -> Result<(), DatabaseError> {
    sqlx::query(
        r#"
        INSERT OR IGNORE INTO retained_local_item_keys (source_key)
        SELECT source_key
        FROM staged_local_inventory_items
        "#,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|error| db_error("insert staged retained local item keys", error))?;

    Ok(())
}

fn announce_dependency_schedule_action(
    row: &AnnounceDependencyScheduleRow,
    now_ms: i64,
    recovery_probe_interval_ms: i64,
) -> AnnounceDependencyScheduleAction {
    match row.dependency_state.as_deref() {
        Some("degraded" | "unavailable") => {
            if let Some(retry_after_ms) = row.retry_after_ms
                && retry_after_ms > now_ms
            {
                return AnnounceDependencyScheduleAction::Wait {
                    reason: AnnounceReason::RetryAfter,
                    next_attempt_at_ms: retry_after_ms.max(row.next_attempt_at_ms),
                };
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
    let result = if let Some(range) = scope.source_key_range() {
        if let Some(end) = range.end {
            sqlx::query(
                r#"
                DELETE FROM local_items
                WHERE source_type = ?
                  AND source_key >= ?
                  AND source_key < ?
                  AND NOT EXISTS (
                      SELECT 1
                      FROM retained_local_item_keys retained
                      WHERE retained.source_key = local_items.source_key
                  )
                "#,
            )
            .bind(scope.source_type())
            .bind(range.start)
            .bind(end)
            .execute(&mut **transaction)
            .await
        } else {
            sqlx::query(
                r#"
                DELETE FROM local_items
                WHERE source_type = ?
                  AND source_key >= ?
                  AND NOT EXISTS (
                      SELECT 1
                      FROM retained_local_item_keys retained
                      WHERE retained.source_key = local_items.source_key
                  )
                "#,
            )
            .bind(scope.source_type())
            .bind(range.start)
            .execute(&mut **transaction)
            .await
        }
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
    let legacy_range = SourceKeyPrefixRange::new(format!("{}:", client_host.as_str()));
    let rows = if let Some(end) = legacy_range.end {
        sqlx::query(
            r#"
            SELECT id, source_key
            FROM local_items
            WHERE source_type = 'client'
              AND source_key >= ?
              AND source_key < ?
            "#,
        )
        .bind(legacy_range.start)
        .bind(end)
        .fetch_all(&mut **transaction)
        .await
    } else {
        sqlx::query(
            r#"
            SELECT id, source_key
            FROM local_items
            WHERE source_type = 'client'
              AND source_key >= ?
            "#,
        )
        .bind(legacy_range.start)
        .fetch_all(&mut **transaction)
        .await
    }
    .map_err(|error| db_error("read client inventory source keys", error))?;

    for row in rows {
        let id: i64 = row.get("id");
        let old_source_key: String = row.get("source_key");
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

fn u16_from_i64(value: i64, field: &'static str) -> Result<u16, DatabaseError> {
    u16::try_from(value).map_err(|error| DatabaseError::QueryFailed {
        operation: format!("read {field}"),
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

fn failure_count_from_i64(value: i64) -> Result<u16, DatabaseError> {
    u16::try_from(value).map_err(|error| DatabaseError::QueryFailed {
        operation: "read dependency failure count".to_owned(),
        message: error.to_string(),
    })
}

fn indexer_registry_row_from_row(row: SqliteRow) -> Result<IndexerRegistryRow, DatabaseError> {
    let id =
        u64::try_from(row.get::<i64, _>("id")).map_err(|error| DatabaseError::QueryFailed {
            operation: "read indexer id".to_owned(),
            message: error.to_string(),
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
        source_kind: row.get("source_kind"),
        source_name: row.get("source_name"),
        source_indexer_id: row.get("source_indexer_id"),
        api_key_source: row.get("api_key_source"),
        enabled: row.get::<i64, _>("enabled") != 0,
        state: row.get("state"),
        retry_after_ms: row.get("retry_after"),
        last_caps_refresh_at_ms: row.get("last_caps_refresh_at"),
    })
}

fn indexer_search_caps_row_from_row(row: SqliteRow) -> Result<IndexerSearchCapsRow, DatabaseError> {
    let caps_json: String = row.get("capabilities_json");
    let caps = serde_json::from_str::<TorznabCaps>(&caps_json).map_err(|error| {
        DatabaseError::QueryFailed {
            operation: "deserialize indexer caps".to_owned(),
            message: error.to_string(),
        }
    })?;
    Ok(IndexerSearchCapsRow {
        indexer_id: indexer_id_from_i64(row.get("id"), "indexer search caps id")?,
        name: DependencyName::new(row.get::<String, _>("name")).map_err(|error| {
            DatabaseError::QueryFailed {
                operation: "read indexer search caps name".to_owned(),
                message: error.to_string(),
            }
        })?,
        url: row.get("url"),
        source_kind: row.get("source_kind"),
        source_name: row.get("source_name"),
        source_indexer_id: row.get("source_indexer_id"),
        api_key_source: row.get("api_key_source"),
        enabled: row.get::<i64, _>("enabled") != 0,
        retry_after_ms: row.get("retry_after"),
        caps,
    })
}

fn local_file_snapshot_from_row(
    row: sqlx::sqlite::SqliteRow,
) -> Result<LocalFileSnapshot, DatabaseError> {
    local_file_snapshot_from_row_ref(&row)
}

fn local_file_snapshot_from_row_ref(
    row: &sqlx::sqlite::SqliteRow,
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
    local_item_from_row_ref(&row)
}

fn local_item_from_row_ref(row: &sqlx::sqlite::SqliteRow) -> Result<LocalItem, DatabaseError> {
    local_item_from_row_ref_with_mtime(row, "mtime_ms")
}

fn local_item_with_file_item_from_row_ref(
    row: &sqlx::sqlite::SqliteRow,
) -> Result<LocalItem, DatabaseError> {
    local_item_from_row_ref_with_mtime(row, "item_mtime_ms")
}

fn local_item_from_row_ref_with_mtime(
    row: &sqlx::sqlite::SqliteRow,
    mtime_column: &'static str,
) -> Result<LocalItem, DatabaseError> {
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
        mtime_ms: row.get(mtime_column),
    })
}

fn local_item_with_file_file_from_row_ref(
    row: &sqlx::sqlite::SqliteRow,
) -> Result<LocalFileSnapshot, DatabaseError> {
    Ok(LocalFileSnapshot {
        item_id: id_from_i64(row.get("item_id"), "local file item id")?,
        relative_path: PathBuf::from(row.get::<String, _>("relative_path")),
        file_name: row.get("file_name"),
        size: byte_size_from_i64(row.get("size"), "local file size")?,
        mtime_ms: row.get("file_mtime_ms"),
        file_index: file_index_from_i64(row.get("file_index"), "local file index")?,
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

fn next_text_prefix(prefix: &str) -> Option<String> {
    let (last_index, last_char) = prefix.char_indices().next_back()?;
    let mut next_codepoint = u32::from(last_char).checked_add(1)?;
    while next_codepoint <= 0x10ffff {
        if let Some(next_char) = char::from_u32(next_codepoint) {
            let mut end = String::with_capacity(last_index + next_char.len_utf8());
            end.push_str(&prefix[..last_index]);
            end.push(next_char);
            return Some(end);
        }
        next_codepoint = next_codepoint.saturating_add(1);
    }

    None
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

async fn displace_non_static_indexer_conflicts(
    transaction: &mut Transaction<'_, Sqlite>,
    name: &str,
    url: &str,
    now_ms: i64,
) -> Result<(), DatabaseError> {
    sqlx::query(
        r#"
        UPDATE indexers
        SET
            enabled = 0,
            name = CASE
                WHEN name = ? THEN name || '#' || source_indexer_id
                ELSE name
            END,
            updated_at = ?
        WHERE source_kind != 'static'
          AND (name = ? OR url = ?)
        "#,
    )
    .bind(name)
    .bind(now_ms)
    .bind(name)
    .bind(url)
    .execute(&mut **transaction)
    .await
    .map_err(|error| db_error("displace duplicate imported indexer", error))?;
    Ok(())
}

async fn displace_static_url_rename_conflicts(
    transaction: &mut Transaction<'_, Sqlite>,
    name: &str,
    url: &str,
    now_ms: i64,
) -> Result<(), DatabaseError> {
    sqlx::query(
        r#"
        UPDATE indexers
        SET enabled = 0,
            updated_at = ?
        WHERE source_kind = 'static'
          AND source_indexer_id != ?
          AND url = ?
          AND enabled != 0
        "#,
    )
    .bind(now_ms)
    .bind(name)
    .bind(url)
    .execute(&mut **transaction)
    .await
    .map_err(|error| db_error("displace renamed static indexer url conflict", error))?;
    Ok(())
}

async fn resolve_active_url_conflicts(
    transaction: &mut Transaction<'_, Sqlite>,
    url: &str,
    source_kind: &str,
    source_name: &str,
    source_indexer_id: &str,
    now_ms: i64,
) -> Result<bool, DatabaseError> {
    let rows = sqlx::query(
        r#"
        SELECT id, source_kind, source_name, source_indexer_id
        FROM indexers
        WHERE url = ?
          AND enabled != 0
        "#,
    )
    .bind(url)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|error| db_error("read indexer URL conflict", error))?;
    let mut current_loses = false;
    for row in rows {
        let existing_kind: String = row.get("source_kind");
        let existing_name: String = row.get("source_name");
        let existing_indexer_id: String = row.get("source_indexer_id");
        if same_indexer_source_identity(
            existing_kind.as_str(),
            existing_name.as_str(),
            existing_indexer_id.as_str(),
            source_kind,
            source_name,
            source_indexer_id,
        ) {
            continue;
        }
        if indexer_source_identity_cmp(
            existing_kind.as_str(),
            existing_name.as_str(),
            existing_indexer_id.as_str(),
            source_kind,
            source_name,
            source_indexer_id,
        ) != CompareOrdering::Greater
        {
            current_loses = true;
            continue;
        }

        let id: i64 = row.get("id");
        sqlx::query("UPDATE indexers SET enabled = 0, updated_at = ? WHERE id = ?")
            .bind(now_ms)
            .bind(id)
            .execute(&mut **transaction)
            .await
            .map_err(|error| db_error("disable lower-priority duplicate URL indexer", error))?;
    }
    Ok(current_loses)
}

async fn available_indexer_name(
    transaction: &mut Transaction<'_, Sqlite>,
    desired_name: &str,
    source_kind: &str,
    source_name: &str,
    source_indexer_id: &str,
) -> Result<DependencyName, DatabaseError> {
    let mut candidate = desired_name.to_owned();
    for suffix in 0_u8..=10 {
        let row = sqlx::query(
            r#"
            SELECT source_kind, source_name, source_indexer_id
            FROM indexers
            WHERE name = ?
            LIMIT 1
            "#,
        )
        .bind(&candidate)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|error| db_error("read indexer name conflict", error))?;
        let available = row.is_none_or(|row| {
            same_indexer_source_identity(
                row.get::<String, _>("source_kind").as_str(),
                row.get::<String, _>("source_name").as_str(),
                row.get::<String, _>("source_indexer_id").as_str(),
                source_kind,
                source_name,
                source_indexer_id,
            )
        });
        if available {
            return DependencyName::new(candidate).map_err(|error| DatabaseError::QueryFailed {
                operation: "build indexer registry name".to_owned(),
                message: error.to_string(),
            });
        }
        candidate = if suffix == 0 {
            format!("{desired_name}#{source_indexer_id}")
        } else {
            format!("{desired_name}#{source_indexer_id}-{suffix}")
        };
    }

    Err(DatabaseError::QueryFailed {
        operation: "build indexer registry name".to_owned(),
        message: format!("could not resolve duplicate indexer name `{desired_name}`"),
    })
}

fn same_indexer_source_identity(
    left_kind: &str,
    left_name: &str,
    left_indexer_id: &str,
    right_kind: &str,
    right_name: &str,
    right_indexer_id: &str,
) -> bool {
    left_kind == right_kind && left_name == right_name && left_indexer_id == right_indexer_id
}

fn indexer_source_identity_cmp(
    left_kind: &str,
    left_name: &str,
    left_indexer_id: &str,
    right_kind: &str,
    right_name: &str,
    right_indexer_id: &str,
) -> CompareOrdering {
    (
        indexer_source_kind_rank(left_kind),
        left_name,
        left_indexer_id,
        left_kind,
    )
        .cmp(&(
            indexer_source_kind_rank(right_kind),
            right_name,
            right_indexer_id,
            right_kind,
        ))
}

fn indexer_source_kind_rank(source_kind: &str) -> u8 {
    match source_kind {
        "static" => 0,
        _ => 1,
    }
}

fn prowlarr_registry_name(source: &DependencyName, indexer: &ProwlarrIndexer) -> String {
    format!("{}:{}", source.as_str(), indexer.name.as_str())
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

fn sqlite_pool_options(max_connections: u32) -> SqlitePoolOptions {
    SqlitePoolOptions::new()
        .max_connections(max_connections)
        .after_connect(|connection, _metadata| {
            Box::pin(async move {
                for pragma in CONNECTION_PRAGMAS {
                    connection.execute(*pragma).await?;
                }
                Ok(())
            })
        })
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
        ApiKeySource, ConfiguredTorznabIndexer, ProwlarrIndexer, SanitizedTorznabUrl,
        parse_torznab_caps,
    };
    use crate::persistence::schema::{BUSY_TIMEOUT_MS, REQUIRED_TABLES};
    use crate::secrets::ApiKey;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
    async fn file_backed_repository_reconciles_pre_prowlarr_schema() {
        let root = unique_temp_dir("sqlite-reconcile");
        let database = root.join("sporos.db");
        let setup_pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(&database)
                    .create_if_missing(true),
            )
            .await
            .unwrap();
        for statement in [
            r#"
            CREATE TABLE indexers (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                url TEXT NOT NULL,
                api_key_source TEXT NOT NULL,
                enabled INTEGER NOT NULL,
                capabilities_json TEXT NOT NULL DEFAULT '{}',
                state TEXT NOT NULL,
                retry_after INTEGER,
                last_caps_refresh_at INTEGER,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                UNIQUE (name),
                UNIQUE (url)
            )
            "#,
            r#"
            CREATE TABLE dependency_health (
                dependency_type TEXT NOT NULL,
                dependency_name TEXT NOT NULL,
                state TEXT NOT NULL,
                reason TEXT,
                retry_after INTEGER,
                checked_at INTEGER NOT NULL,
                PRIMARY KEY (dependency_type, dependency_name)
            )
            "#,
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
            VALUES ('legacy', 'https://indexer.example/api', 'direct', 1, '{}',
                    'unknown', NULL, NULL, 10, 10)
            "#,
            r#"
            INSERT INTO dependency_health (
                dependency_type,
                dependency_name,
                state,
                reason,
                retry_after,
                checked_at
            )
            VALUES ('indexer', 'legacy', 'degraded', 'rate limited', 123, 20)
            "#,
        ] {
            sqlx::query(statement).execute(&setup_pool).await.unwrap();
        }
        setup_pool.close().await;

        let repository = Repository::connect(&database).await.unwrap();
        let rows = repository.indexer_registry_snapshot(10).await.unwrap();
        let legacy = rows
            .iter()
            .find(|indexer| indexer.name.as_str() == "legacy")
            .unwrap();
        let failure_count = repository
            .dependency_failure_count("indexer", &DependencyName::new("legacy").unwrap())
            .await
            .unwrap();

        assert_eq!("static", legacy.source_kind);
        assert_eq!("", legacy.source_name);
        assert_eq!("legacy", legacy.source_indexer_id);
        assert_eq!(0, failure_count);

        let renamed = repository
            .sync_torznab_indexers(
                &[test_indexer(
                    "renamed",
                    "https://indexer.example/api",
                    ApiKeySource::Direct,
                )],
                100,
            )
            .await
            .unwrap();
        assert!(renamed.iter().any(|indexer| {
            indexer.name.as_str() == "renamed"
                && indexer.url == "https://indexer.example/api"
                && indexer.enabled
        }));

        repository.pool().close().await;
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn file_backed_repository_reconciles_partial_indexer_source_schema() {
        let root = unique_temp_dir("sqlite-reconcile-partial-source");
        let database = root.join("sporos.db");
        let setup_pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(&database)
                    .create_if_missing(true),
            )
            .await
            .unwrap();
        for statement in [
            r#"
            CREATE TABLE indexers (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                url TEXT NOT NULL,
                source_kind TEXT NOT NULL DEFAULT 'static',
                api_key_source TEXT NOT NULL,
                enabled INTEGER NOT NULL,
                capabilities_json TEXT NOT NULL DEFAULT '{}',
                state TEXT NOT NULL,
                retry_after INTEGER,
                last_caps_refresh_at INTEGER,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                UNIQUE (name)
            )
            "#,
            r#"
            INSERT INTO indexers (
                name,
                url,
                source_kind,
                api_key_source,
                enabled,
                capabilities_json,
                state,
                retry_after,
                last_caps_refresh_at,
                created_at,
                updated_at
            )
            VALUES ('partial', 'https://partial.example/api', 'static', 'direct', 1,
                    '{}', 'unknown', NULL, NULL, 10, 10)
            "#,
        ] {
            sqlx::query(statement).execute(&setup_pool).await.unwrap();
        }
        setup_pool.close().await;

        let repository = Repository::connect(&database).await.unwrap();
        let rows = repository.indexer_registry_snapshot(10).await.unwrap();
        let partial = rows
            .iter()
            .find(|indexer| indexer.name.as_str() == "partial")
            .unwrap();

        assert_eq!("static", partial.source_kind);
        assert_eq!("", partial.source_name);
        assert_eq!("partial", partial.source_indexer_id);

        repository.pool().close().await;
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn indexer_schema_rebuild_restores_pragmas_after_failure() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::query("PRAGMA foreign_keys = ON")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            r#"
            CREATE TABLE indexers (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                url TEXT NOT NULL,
                source_kind TEXT NOT NULL DEFAULT 'static',
                source_name TEXT NOT NULL DEFAULT '',
                source_indexer_id TEXT NOT NULL DEFAULT '',
                enabled INTEGER NOT NULL,
                capabilities_json TEXT NOT NULL DEFAULT '{}',
                state TEXT NOT NULL,
                retry_after INTEGER,
                last_caps_refresh_at INTEGER,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                UNIQUE (name)
            )
            "#,
        )
        .execute(&pool)
        .await
        .unwrap();

        let error = rebuild_indexers_table(&pool).await.unwrap_err();
        let foreign_keys: i64 = sqlx::query_scalar("PRAGMA foreign_keys")
            .fetch_one(&pool)
            .await
            .unwrap();
        let legacy_alter_table: i64 = sqlx::query_scalar("PRAGMA legacy_alter_table")
            .fetch_one(&pool)
            .await
            .unwrap();

        assert!(error.to_string().contains("api_key_source"));
        assert_eq!(1, foreign_keys);
        assert_eq!(0, legacy_alter_table);
    }

    #[test]
    fn source_key_prefix_range_uses_exclusive_text_bound() {
        assert_eq!(
            SourceKeyPrefixRange::new("12:qbit-a.local:".to_owned()),
            SourceKeyPrefixRange {
                start: "12:qbit-a.local:".to_owned(),
                end: Some("12:qbit-a.local;".to_owned()),
            }
        );
        assert_eq!(
            SourceKeyPrefixRange::new("rtorrent:5000:".to_owned()),
            SourceKeyPrefixRange {
                start: "rtorrent:5000:".to_owned(),
                end: Some("rtorrent:5000;".to_owned()),
            }
        );
    }

    #[tokio::test]
    async fn client_source_key_range_queries_use_local_item_key_index() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let range = SourceKeyPrefixRange::new(client_source_key_prefix(
            &ClientHost::new("qbit.local").unwrap(),
        ));
        let rows = sqlx::query(
            r#"
            EXPLAIN QUERY PLAN
            SELECT id
            FROM local_items
            WHERE source_type = 'client'
              AND source_key >= ?
              AND source_key < ?
            "#,
        )
        .bind(range.start)
        .bind(range.end.unwrap())
        .fetch_all(repository.pool())
        .await
        .unwrap();
        let details = rows
            .into_iter()
            .map(|row| row.get::<String, _>("detail"))
            .collect::<Vec<_>>()
            .join("\n");

        assert!(details.contains("USING COVERING INDEX") || details.contains("USING INDEX"));
        assert!(details.contains("source_type"));
        assert!(details.contains("source_key"));
    }

    #[tokio::test]
    async fn local_item_keyset_queries_use_media_title_source_index() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let rows = sqlx::query(
            r#"
            EXPLAIN QUERY PLAN
            SELECT id
            FROM local_items
            WHERE media_type = 'episode'
              AND (title, source_type, source_key) > (?, ?, ?)
            ORDER BY title, source_type, source_key
            LIMIT 512
            "#,
        )
        .bind("Paged Show 0511 S01E03")
        .bind("data_root")
        .bind("/media/paged-0511-e03.mkv")
        .fetch_all(repository.pool())
        .await
        .unwrap();
        let details = rows
            .into_iter()
            .map(|row| row.get::<String, _>("detail"))
            .collect::<Vec<_>>()
            .join("\n");

        assert!(details.contains("idx_local_items_media_title_source"));
        assert!(details.contains("media_type"));
        assert!(details.contains("title"));
    }

    #[tokio::test]
    async fn staged_virtual_season_keyset_query_uses_primary_key_range() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let mut replacement = repository
            .begin_local_inventory_replace_transaction(LocalInventoryScope::Virtual)
            .await
            .unwrap();
        replacement
            .initialize_virtual_season_candidate_stage()
            .await
            .unwrap();
        let rows = sqlx::query(
            r#"
            EXPLAIN QUERY PLAN
            SELECT state.title, state.season
            FROM staged_virtual_season_state state
            WHERE (state.title, state.season) > (?, ?)
            ORDER BY state.title, state.season
            LIMIT 512
            "#,
        )
        .bind("Paged Show 0511")
        .bind(1_i64)
        .fetch_all(&mut *replacement.transaction)
        .await
        .unwrap();
        let details = rows
            .into_iter()
            .map(|row| row.get::<String, _>("detail"))
            .collect::<Vec<_>>()
            .join("\n");

        assert!(details.contains("USING PRIMARY KEY"));
        assert!(details.contains("title"));
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
    async fn local_item_with_largest_file_preserves_distinct_mtimes() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let mut item = test_local_item("Example");
        item.media_type = MediaType::SeasonPack;
        item.mtime_ms = Some(1_700_000_000_111);
        let small_newer_file = LocalFile::new(
            None,
            PathBuf::from("Example/file-a.mkv"),
            ByteSize::new(10),
            FileIndex::new(0),
        )
        .unwrap()
        .with_mtime_ms(Some(1_700_000_000_999));
        let largest_older_file = LocalFile::new(
            None,
            PathBuf::from("Example/file-b.mkv"),
            ByteSize::new(20),
            FileIndex::new(1),
        )
        .unwrap()
        .with_mtime_ms(Some(1_700_000_000_222));
        repository
            .upsert_local_item_with_files(&item, &[small_newer_file, largest_older_file])
            .await
            .unwrap();

        let rows = repository
            .local_items_with_largest_file_by_media_type_page(MediaType::SeasonPack, 10, 0)
            .await
            .unwrap();

        assert_eq!(1, rows.len());
        assert_eq!(Some(1_700_000_000_111), rows[0].item.mtime_ms);
        assert_eq!("file-b.mkv", rows[0].file.file_name);
        assert_eq!(Some(1_700_000_000_222), rows[0].file.mtime_ms);
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
    async fn successful_owned_inventory_stream_clears_staged_rows() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let mut item = test_local_item("Complete");
        item.source = LocalItemSource::DataRoot {
            path: PathBuf::from("/media/complete"),
        };
        item.info_hash = None;
        item.path = Some(PathBuf::from("/media/complete"));
        let file = LocalFile::new(
            None,
            PathBuf::from("complete.mkv"),
            ByteSize::new(20),
            FileIndex::new(0),
        )
        .unwrap();
        let (sender, receiver) = mpsc::channel(2);
        sender
            .send(OwnedLocalInventoryMessage::Item(OwnedLocalItemFileBatch {
                item,
                files: vec![file],
            }))
            .await
            .unwrap();
        sender
            .send(OwnedLocalInventoryMessage::Finished)
            .await
            .unwrap();
        drop(sender);

        let summary = repository
            .replace_local_inventory_owned_receiver(LocalInventoryScope::DataRoot, receiver)
            .await
            .unwrap();
        let local_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM local_items")
            .fetch_one(repository.pool())
            .await
            .unwrap();
        let staged_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM temp.staged_local_inventory_items")
                .fetch_one(repository.pool())
                .await
                .unwrap();
        let staged_file_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM temp.staged_local_inventory_files")
                .fetch_one(repository.pool())
                .await
                .unwrap();

        assert_eq!(1, summary.upserted);
        assert_eq!(0, summary.pruned);
        assert_eq!(1, local_count);
        assert_eq!(0, staged_count);
        assert_eq!(0, staged_file_count);
    }

    #[tokio::test]
    async fn stalled_owned_inventory_stream_does_not_starve_main_pool() {
        let root = unique_temp_dir("sqlite-inventory-staging-pool");
        let database = root.join("sporos.db");
        let repository = Repository::connect(&database).await.unwrap();
        repository
            .inventory_staging_pool
            .acquire()
            .await
            .unwrap()
            .close()
            .await
            .unwrap();
        let staging_foreign_keys: i64 = sqlx::query_scalar("PRAGMA foreign_keys;")
            .fetch_one(&repository.inventory_staging_pool)
            .await
            .unwrap();
        let staging_busy_timeout: i64 = sqlx::query_scalar("PRAGMA busy_timeout;")
            .fetch_one(&repository.inventory_staging_pool)
            .await
            .unwrap();
        let mut held_connections = Vec::new();
        for _ in 0..4 {
            held_connections.push(repository.pool().acquire().await.unwrap());
        }
        let (sender, receiver) = mpsc::channel(1);
        let staging_started = Arc::new(AtomicBool::new(false));
        let refresh_repository = repository.clone();
        let refresh_started = Arc::clone(&staging_started);
        let refresh_task = tokio::spawn(async move {
            refresh_repository
                .replace_local_inventory_owned_receiver_with_staging_signal(
                    LocalInventoryScope::DataRoot,
                    receiver,
                    Some(refresh_started),
                )
                .await
        });

        tokio::time::timeout(Duration::from_secs(1), async {
            while !staging_started.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let still_available: i64 = tokio::time::timeout(Duration::from_millis(100), async {
            sqlx::query_scalar("SELECT 1")
                .fetch_one(repository.pool())
                .await
        })
        .await
        .unwrap()
        .unwrap();

        sender
            .send(OwnedLocalInventoryMessage::Finished)
            .await
            .unwrap();
        let summary = refresh_task.await.unwrap().unwrap();
        drop(held_connections);
        repository.inventory_staging_pool.close().await;
        repository.pool().close().await;
        fs::remove_dir_all(root).unwrap();

        assert_eq!(1, staging_foreign_keys);
        assert_eq!(i64::from(BUSY_TIMEOUT_MS), staging_busy_timeout);
        assert_eq!(1, still_available);
        assert_eq!(0, summary.upserted);
        assert_eq!(0, summary.pruned);
    }

    #[tokio::test]
    async fn invalid_owned_inventory_scope_clears_staged_rows() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let mut valid = test_local_item("Valid");
        valid.source = LocalItemSource::DataRoot {
            path: PathBuf::from("/media/valid"),
        };
        valid.info_hash = None;
        valid.path = Some(PathBuf::from("/media/valid"));
        let valid_file = LocalFile::new(
            None,
            PathBuf::from("valid.mkv"),
            ByteSize::new(20),
            FileIndex::new(0),
        )
        .unwrap();
        let invalid = test_local_item("Invalid");
        let invalid_file = LocalFile::new(
            None,
            PathBuf::from("invalid.mkv"),
            ByteSize::new(30),
            FileIndex::new(0),
        )
        .unwrap();
        let (sender, receiver) = mpsc::channel(3);
        sender
            .send(OwnedLocalInventoryMessage::Item(OwnedLocalItemFileBatch {
                item: valid,
                files: vec![valid_file],
            }))
            .await
            .unwrap();
        sender
            .send(OwnedLocalInventoryMessage::Item(OwnedLocalItemFileBatch {
                item: invalid,
                files: vec![invalid_file],
            }))
            .await
            .unwrap();
        sender
            .send(OwnedLocalInventoryMessage::Finished)
            .await
            .unwrap();
        drop(sender);

        let result = repository
            .replace_local_inventory_owned_receiver(LocalInventoryScope::DataRoot, receiver)
            .await;
        let local_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM local_items")
            .fetch_one(repository.pool())
            .await
            .unwrap();
        let (staged_count, staged_file_count) = staged_inventory_counts(&repository).await;

        assert!(matches!(result, Err(DatabaseError::QueryFailed { .. })));
        assert_eq!(0, local_count);
        assert_eq!(0, staged_count);
        assert_eq!(0, staged_file_count);
    }

    #[tokio::test]
    async fn unfinished_owned_inventory_stream_rolls_back_partial_refresh() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let mut existing = test_local_item("Existing");
        existing.source = LocalItemSource::DataRoot {
            path: PathBuf::from("/media/existing"),
        };
        existing.info_hash = None;
        existing.path = Some(PathBuf::from("/media/existing"));
        let existing_file = LocalFile::new(
            None,
            PathBuf::from("existing.mkv"),
            ByteSize::new(10),
            FileIndex::new(0),
        )
        .unwrap();
        repository
            .upsert_local_item_with_files(&existing, &[existing_file])
            .await
            .unwrap();

        let mut partial = test_local_item("Partial");
        partial.source = LocalItemSource::DataRoot {
            path: PathBuf::from("/media/partial"),
        };
        partial.info_hash = None;
        partial.path = Some(PathBuf::from("/media/partial"));
        let partial_file = LocalFile::new(
            None,
            PathBuf::from("partial.mkv"),
            ByteSize::new(20),
            FileIndex::new(0),
        )
        .unwrap();
        let (sender, receiver) = mpsc::channel(1);
        sender
            .send(OwnedLocalInventoryMessage::Item(OwnedLocalItemFileBatch {
                item: partial,
                files: vec![partial_file],
            }))
            .await
            .unwrap();
        drop(sender);

        let result = repository
            .replace_local_inventory_owned_receiver(LocalInventoryScope::DataRoot, receiver)
            .await;
        let existing_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM local_items WHERE source_key = ?")
                .bind("/media/existing")
                .fetch_one(repository.pool())
                .await
                .unwrap();
        let partial_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM local_items WHERE source_key = ?")
                .bind("/media/partial")
                .fetch_one(repository.pool())
                .await
                .unwrap();
        let staged_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM temp.staged_local_inventory_items")
                .fetch_one(repository.pool())
                .await
                .unwrap();
        let staged_file_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM temp.staged_local_inventory_files")
                .fetch_one(repository.pool())
                .await
                .unwrap();

        assert!(matches!(
            result,
            Err(DatabaseError::IncompleteStream { .. })
        ));
        assert_eq!(1, existing_count);
        assert_eq!(0, partial_count);
        assert_eq!(0, staged_count);
        assert_eq!(0, staged_file_count);
    }

    #[tokio::test]
    async fn failed_staged_inventory_commit_rolls_back_item_files_and_prune() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let mut existing = test_local_item("Original");
        existing.source = LocalItemSource::DataRoot {
            path: PathBuf::from("/media/existing"),
        };
        existing.info_hash = None;
        existing.path = Some(PathBuf::from("/media/existing"));
        let existing_file = LocalFile::new(
            None,
            PathBuf::from("existing.mkv"),
            ByteSize::new(10),
            FileIndex::new(0),
        )
        .unwrap();
        let item_id = repository
            .upsert_local_item_with_files(&existing, &[existing_file])
            .await
            .unwrap();

        let mut prunable = test_local_item("Prunable");
        prunable.source = LocalItemSource::DataRoot {
            path: PathBuf::from("/media/prunable"),
        };
        prunable.info_hash = None;
        prunable.path = Some(PathBuf::from("/media/prunable"));
        let prunable_file = LocalFile::new(
            None,
            PathBuf::from("prunable.mkv"),
            ByteSize::new(15),
            FileIndex::new(0),
        )
        .unwrap();
        repository
            .upsert_local_item_with_files(&prunable, &[prunable_file])
            .await
            .unwrap();

        let mut staged = existing.clone();
        staged.title = ItemTitle::new("Should Roll Back").unwrap();
        let duplicate_a = LocalFile::new(
            None,
            PathBuf::from("duplicate-a.mkv"),
            ByteSize::new(20),
            FileIndex::new(0),
        )
        .unwrap();
        let duplicate_b = LocalFile::new(
            None,
            PathBuf::from("duplicate-b.mkv"),
            ByteSize::new(30),
            FileIndex::new(0),
        )
        .unwrap();
        let (sender, receiver) = mpsc::channel(2);
        sender
            .send(OwnedLocalInventoryMessage::Item(OwnedLocalItemFileBatch {
                item: staged,
                files: vec![duplicate_a, duplicate_b],
            }))
            .await
            .unwrap();
        sender
            .send(OwnedLocalInventoryMessage::Finished)
            .await
            .unwrap();
        drop(sender);

        let result = repository
            .replace_local_inventory_owned_receiver(LocalInventoryScope::DataRoot, receiver)
            .await;
        let title: String = sqlx::query_scalar("SELECT title FROM local_items WHERE id = ?")
            .bind(i64_from_u64(item_id.get(), "local item id").unwrap())
            .fetch_one(repository.pool())
            .await
            .unwrap();
        let existing_files = repository.local_files_for_item(item_id, 10).await.unwrap();
        let prunable_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM local_items WHERE source_key = ?")
                .bind("/media/prunable")
                .fetch_one(repository.pool())
                .await
                .unwrap();
        let (staged_count, staged_file_count) = staged_inventory_counts(&repository).await;

        assert!(matches!(result, Err(DatabaseError::QueryFailed { .. })));
        assert_eq!("Original", title);
        assert_eq!(1, existing_files.len());
        assert_eq!(
            PathBuf::from("existing.mkv"),
            existing_files[0].relative_path
        );
        assert_eq!(1, prunable_count);
        assert_eq!(0, staged_count);
        assert_eq!(0, staged_file_count);
    }

    #[tokio::test]
    async fn failed_staged_inventory_prune_rolls_back_item_files_and_prune() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let mut existing = test_local_item("Original");
        existing.source = LocalItemSource::DataRoot {
            path: PathBuf::from("/media/existing"),
        };
        existing.info_hash = None;
        existing.path = Some(PathBuf::from("/media/existing"));
        let existing_file = LocalFile::new(
            None,
            PathBuf::from("existing.mkv"),
            ByteSize::new(10),
            FileIndex::new(0),
        )
        .unwrap();
        let item_id = repository
            .upsert_local_item_with_files(&existing, &[existing_file])
            .await
            .unwrap();

        let mut prunable = test_local_item("Prunable");
        prunable.source = LocalItemSource::DataRoot {
            path: PathBuf::from("/media/prunable"),
        };
        prunable.info_hash = None;
        prunable.path = Some(PathBuf::from("/media/prunable"));
        let prunable_file = LocalFile::new(
            None,
            PathBuf::from("prunable.mkv"),
            ByteSize::new(15),
            FileIndex::new(0),
        )
        .unwrap();
        repository
            .upsert_local_item_with_files(&prunable, &[prunable_file])
            .await
            .unwrap();
        sqlx::query(
            r#"
            CREATE TRIGGER abort_prunable_local_item_delete
            BEFORE DELETE ON local_items
            WHEN old.source_key = '/media/prunable'
            BEGIN
                SELECT RAISE(ABORT, 'abort prunable delete');
            END
            "#,
        )
        .execute(repository.pool())
        .await
        .unwrap();

        let mut staged = existing.clone();
        staged.title = ItemTitle::new("Should Roll Back").unwrap();
        let staged_file = LocalFile::new(
            None,
            PathBuf::from("staged.mkv"),
            ByteSize::new(20),
            FileIndex::new(0),
        )
        .unwrap();
        let (sender, receiver) = mpsc::channel(2);
        sender
            .send(OwnedLocalInventoryMessage::Item(OwnedLocalItemFileBatch {
                item: staged,
                files: vec![staged_file],
            }))
            .await
            .unwrap();
        sender
            .send(OwnedLocalInventoryMessage::Finished)
            .await
            .unwrap();
        drop(sender);

        let result = repository
            .replace_local_inventory_owned_receiver(LocalInventoryScope::DataRoot, receiver)
            .await;
        let title: String = sqlx::query_scalar("SELECT title FROM local_items WHERE id = ?")
            .bind(i64_from_u64(item_id.get(), "local item id").unwrap())
            .fetch_one(repository.pool())
            .await
            .unwrap();
        let existing_files = repository.local_files_for_item(item_id, 10).await.unwrap();
        let prunable_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM local_items WHERE source_key = ?")
                .bind("/media/prunable")
                .fetch_one(repository.pool())
                .await
                .unwrap();
        let (staged_count, staged_file_count) = staged_inventory_counts(&repository).await;

        assert!(matches!(result, Err(DatabaseError::QueryFailed { .. })));
        assert_eq!("Original", title);
        assert_eq!(1, existing_files.len());
        assert_eq!(
            PathBuf::from("existing.mkv"),
            existing_files[0].relative_path
        );
        assert_eq!(1, prunable_count);
        assert_eq!(0, staged_count);
        assert_eq!(0, staged_file_count);
    }

    #[tokio::test]
    async fn remote_candidate_upsert_uses_indexer_guid_natural_key() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let mut candidate = test_remote_candidate("guid-1", "Original");
        candidate.download_url = DownloadUrl::new(
            "https://user:password@indexer.example/download?id=1&authkey=secret&torrent_pass=other-secret",
        )
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
        candidate.torrent_cache_path = None;
        let third_id = repository
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
        assert_eq!(first_id, third_id);
        assert_eq!("Updated", title);
        assert_eq!(
            "https://[REDACTED]@indexer.example/download?id=1&authkey=[REDACTED]&torrent_pass=[REDACTED]",
            redacted_download_url
        );
        assert!(!redacted_download_url.contains("secret"));
        assert!(!redacted_download_url.contains("other-secret"));
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
    async fn sync_prowlarr_indexers_updates_by_stable_source_identity() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let source = DependencyName::new("main").unwrap();
        let first =
            test_prowlarr_indexer("main", 101, "Movies", "https://prowlarr.example/101/api");

        let synced = repository
            .sync_prowlarr_indexers(&source, &[first], ProwlarrRemovePolicy::Deactivate, 100)
            .await
            .unwrap();
        let row = synced
            .iter()
            .find(|indexer| indexer.source_indexer_id == "101")
            .unwrap();
        let row_id = row.id;
        assert_eq!("prowlarr", row.source_kind);
        assert_eq!("main", row.source_name);
        assert_eq!("main:Movies", row.name.as_str());
        assert_eq!("https://prowlarr.example/101/api", row.url);
        assert!(row.enabled);

        let renamed = test_prowlarr_indexer(
            "main",
            101,
            "Movies Renamed",
            "https://new-prowlarr.example/101/api",
        );
        let resynced = repository
            .sync_prowlarr_indexers(&source, &[renamed], ProwlarrRemovePolicy::Deactivate, 200)
            .await
            .unwrap();
        let row = resynced
            .iter()
            .find(|indexer| indexer.source_indexer_id == "101")
            .unwrap();

        assert_eq!(row_id, row.id);
        assert_eq!("main:Movies Renamed", row.name.as_str());
        assert_eq!("https://new-prowlarr.example/101/api", row.url);
        assert!(row.enabled);
    }

    #[tokio::test]
    async fn sync_prowlarr_indexers_does_not_infer_source_name_changes() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let source = DependencyName::new("main").unwrap();
        let renamed_source = DependencyName::new("renamed").unwrap();
        let first =
            test_prowlarr_indexer("main", 101, "Movies", "https://prowlarr.example/101/api");

        let synced = repository
            .sync_prowlarr_indexers(&source, &[first], ProwlarrRemovePolicy::Deactivate, 100)
            .await
            .unwrap();
        let row_id = synced
            .iter()
            .find(|indexer| indexer.source_indexer_id == "101")
            .unwrap()
            .id;
        let renamed =
            test_prowlarr_indexer("renamed", 101, "Movies", "https://prowlarr.example/101/api");

        let resynced = repository
            .sync_prowlarr_indexers(
                &renamed_source,
                &[renamed],
                ProwlarrRemovePolicy::Deactivate,
                200,
            )
            .await
            .unwrap();
        let row = resynced
            .iter()
            .find(|indexer| indexer.source_name == "main" && indexer.source_indexer_id == "101")
            .unwrap();

        assert_eq!(row_id, row.id);
        assert_eq!("main", row.source_name);
        assert!(row.enabled);
        assert!(
            resynced
                .iter()
                .all(|indexer| indexer.source_name != "renamed")
        );
    }

    #[tokio::test]
    async fn sync_prowlarr_indexers_does_not_infer_source_name_and_url_changes() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let source = DependencyName::new("main").unwrap();
        let renamed_source = DependencyName::new("renamed").unwrap();
        let first =
            test_prowlarr_indexer("main", 101, "Movies", "https://prowlarr.example/101/api");
        let synced = repository
            .sync_prowlarr_indexers(&source, &[first], ProwlarrRemovePolicy::Deactivate, 100)
            .await
            .unwrap();
        let row_id = synced
            .iter()
            .find(|indexer| indexer.source_indexer_id == "101")
            .unwrap()
            .id;
        let renamed = test_prowlarr_indexer(
            "renamed",
            101,
            "Movies",
            "https://new-prowlarr.example/101/api",
        );

        let resynced = repository
            .sync_prowlarr_indexers(
                &renamed_source,
                &[renamed],
                ProwlarrRemovePolicy::Deactivate,
                200,
            )
            .await
            .unwrap();
        let rows = resynced
            .iter()
            .filter(|indexer| indexer.source_indexer_id == "101")
            .collect::<Vec<_>>();

        assert_eq!(2, rows.len());
        assert!(rows.iter().any(|row| row.id == row_id
            && row.source_name == "main"
            && row.url == "https://prowlarr.example/101/api"
            && row.enabled));
        assert!(rows.iter().any(|row| row.id != row_id
            && row.source_name == "renamed"
            && row.url == "https://new-prowlarr.example/101/api"
            && row.enabled));
    }

    #[tokio::test]
    async fn sync_prowlarr_indexers_keeps_ids_source_local() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let main = DependencyName::new("main").unwrap();
        let backup = DependencyName::new("backup").unwrap();
        let main_indexer =
            test_prowlarr_indexer("main", 101, "Movies", "https://main.example/101/api");
        let backup_indexer =
            test_prowlarr_indexer("backup", 101, "Movies", "https://backup.example/101/api");

        repository
            .sync_prowlarr_indexers(
                &main,
                &[main_indexer],
                ProwlarrRemovePolicy::Deactivate,
                100,
            )
            .await
            .unwrap();
        let rows = repository
            .sync_prowlarr_indexers(
                &backup,
                &[backup_indexer],
                ProwlarrRemovePolicy::Deactivate,
                200,
            )
            .await
            .unwrap();
        let main_row = rows
            .iter()
            .find(|indexer| indexer.source_name == "main" && indexer.source_indexer_id == "101")
            .unwrap();
        let backup_row = rows
            .iter()
            .find(|indexer| indexer.source_name == "backup" && indexer.source_indexer_id == "101")
            .unwrap();

        assert_ne!(main_row.id, backup_row.id);
        assert_eq!("https://main.example/101/api", main_row.url);
        assert_eq!("https://backup.example/101/api", backup_row.url);
        assert!(main_row.enabled);
        assert!(backup_row.enabled);
    }

    #[tokio::test]
    async fn sync_prowlarr_indexers_does_not_rewrite_same_url_source() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let main = DependencyName::new("main").unwrap();
        let backup = DependencyName::new("backup").unwrap();
        let url = "https://prowlarr.example/shared/api";
        let main_indexer = test_prowlarr_indexer("main", 101, "Movies", url);
        let backup_indexer = test_prowlarr_indexer("backup", 101, "Movies", url);

        let synced = repository
            .sync_prowlarr_indexers(
                &main,
                &[main_indexer],
                ProwlarrRemovePolicy::Deactivate,
                100,
            )
            .await
            .unwrap();
        let main_id = synced
            .iter()
            .find(|indexer| indexer.source_name == "main" && indexer.source_indexer_id == "101")
            .unwrap()
            .id;
        let rows = repository
            .sync_prowlarr_indexers(
                &backup,
                &[backup_indexer],
                ProwlarrRemovePolicy::Deactivate,
                200,
            )
            .await
            .unwrap();
        let main_row = rows
            .iter()
            .find(|indexer| indexer.source_name == "main" && indexer.source_indexer_id == "101")
            .unwrap();
        let backup_row = rows
            .iter()
            .find(|indexer| indexer.source_name == "backup" && indexer.source_indexer_id == "101")
            .unwrap();

        assert_eq!(main_id, main_row.id);
        assert!(!main_row.enabled);
        assert_ne!(main_id, backup_row.id);
        assert!(backup_row.enabled);
    }

    #[tokio::test]
    async fn sync_prowlarr_indexers_rejects_mismatched_source_rows() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let source = DependencyName::new("main").unwrap();
        let other =
            test_prowlarr_indexer("other", 101, "Movies", "https://prowlarr.example/101/api");

        let error = repository
            .sync_prowlarr_indexers(&source, &[other], ProwlarrRemovePolicy::Deactivate, 100)
            .await
            .unwrap_err();

        assert!(error.to_string().contains("belongs to source `other`"));
    }

    #[tokio::test]
    async fn sync_prowlarr_indexers_deactivates_removals_and_reactivates() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let source = DependencyName::new("main").unwrap();
        let first =
            test_prowlarr_indexer("main", 101, "Movies", "https://prowlarr.example/101/api");
        let second = test_prowlarr_indexer("main", 102, "TV", "https://prowlarr.example/102/api");

        let initial = repository
            .sync_prowlarr_indexers_with_summary(
                &source,
                &[first.clone(), second.clone()],
                ProwlarrRemovePolicy::Deactivate,
                100,
            )
            .await
            .unwrap();
        assert_eq!(2, initial.imported);
        assert_eq!(0, initial.deactivated);
        let removed = repository
            .sync_prowlarr_indexers_with_summary(
                &source,
                &[first.clone()],
                ProwlarrRemovePolicy::Deactivate,
                200,
            )
            .await
            .unwrap();
        assert_eq!(1, removed.imported);
        assert_eq!(1, removed.deactivated);
        let tv = removed
            .registry
            .iter()
            .find(|indexer| indexer.source_indexer_id == "102")
            .unwrap();
        assert!(!tv.enabled);

        let unchanged = repository
            .sync_prowlarr_indexers_with_summary(
                &source,
                &[first.clone()],
                ProwlarrRemovePolicy::Deactivate,
                250,
            )
            .await
            .unwrap();
        assert_eq!(1, unchanged.imported);
        assert_eq!(0, unchanged.deactivated);

        let reactivated = repository
            .sync_prowlarr_indexers_with_summary(
                &source,
                &[first, second],
                ProwlarrRemovePolicy::Deactivate,
                300,
            )
            .await
            .unwrap();
        assert_eq!(2, reactivated.imported);
        assert_eq!(0, reactivated.deactivated);
        let tv = reactivated
            .registry
            .iter()
            .find(|indexer| indexer.source_indexer_id == "102")
            .unwrap();
        assert!(tv.enabled);
    }

    #[tokio::test]
    async fn sync_prowlarr_indexers_can_ignore_removals() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let source = DependencyName::new("main").unwrap();
        let indexer =
            test_prowlarr_indexer("main", 101, "Movies", "https://prowlarr.example/101/api");

        repository
            .sync_prowlarr_indexers(&source, &[indexer], ProwlarrRemovePolicy::Deactivate, 100)
            .await
            .unwrap();
        let rows = repository
            .sync_prowlarr_indexers(&source, &[], ProwlarrRemovePolicy::Ignore, 200)
            .await
            .unwrap();
        let row = rows
            .iter()
            .find(|indexer| indexer.source_indexer_id == "101")
            .unwrap();

        assert!(row.enabled);
    }

    #[tokio::test]
    async fn sync_prowlarr_indexers_can_take_over_disabled_static_url() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let source = DependencyName::new("main").unwrap();
        let url = "https://prowlarr.example/101/api";
        let imported = test_prowlarr_indexer("main", 101, "Movies", url);

        repository
            .sync_torznab_indexers(&[test_indexer("legacy", url, ApiKeySource::Direct)], 100)
            .await
            .unwrap();
        let disabled_static = repository.sync_torznab_indexers(&[], 200).await.unwrap();
        let static_row = disabled_static
            .iter()
            .find(|indexer| indexer.source_kind == "static")
            .unwrap();
        assert!(!static_row.enabled);

        let rows = repository
            .sync_prowlarr_indexers(&source, &[imported], ProwlarrRemovePolicy::Deactivate, 300)
            .await
            .unwrap();
        let prowlarr_row = rows
            .iter()
            .find(|indexer| indexer.source_kind == "prowlarr")
            .unwrap();

        assert_eq!(url, prowlarr_row.url);
        assert!(prowlarr_row.enabled);
    }

    #[tokio::test]
    async fn concurrent_prowlarr_sync_keeps_one_active_url() {
        let root = unique_temp_dir("prowlarr-active-url");
        let database = root.join("sporos.db");
        let repository = Repository::connect(&database).await.unwrap();
        let main_repository = repository.clone();
        let backup_repository = repository.clone();
        let main = DependencyName::new("main").unwrap();
        let backup = DependencyName::new("backup").unwrap();
        let url = "https://prowlarr.example/shared/api";
        let main_indexer = test_prowlarr_indexer("main", 101, "Movies", url);
        let backup_indexer = test_prowlarr_indexer("backup", 202, "Movies", url);

        let (main_result, backup_result) = tokio::join!(
            async move {
                main_repository
                    .sync_prowlarr_indexers(
                        &main,
                        &[main_indexer],
                        ProwlarrRemovePolicy::Deactivate,
                        100,
                    )
                    .await
            },
            async move {
                backup_repository
                    .sync_prowlarr_indexers(
                        &backup,
                        &[backup_indexer],
                        ProwlarrRemovePolicy::Deactivate,
                        100,
                    )
                    .await
            }
        );

        main_result.unwrap();
        backup_result.unwrap();
        let active_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM indexers WHERE url = ? AND enabled != 0")
                .bind(url)
                .fetch_one(repository.pool())
                .await
                .unwrap();

        assert_eq!(1, active_count);
        let active_source: String =
            sqlx::query_scalar("SELECT source_name FROM indexers WHERE url = ? AND enabled != 0")
                .bind(url)
                .fetch_one(repository.pool())
                .await
                .unwrap();
        assert_eq!("backup", active_source);

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn sync_prowlarr_indexers_resolves_static_duplicates() {
        let repository = Repository::connect_in_memory().await.unwrap();
        repository
            .sync_torznab_indexers(
                &[test_indexer(
                    "main:Movies",
                    "https://prowlarr.example/101/api",
                    ApiKeySource::Direct,
                )],
                100,
            )
            .await
            .unwrap();
        let source = DependencyName::new("main").unwrap();
        let duplicate =
            test_prowlarr_indexer("main", 101, "Movies", "https://prowlarr.example/101/api");
        let distinct =
            test_prowlarr_indexer("main", 102, "Movies", "https://prowlarr.example/102/api");

        let rows = repository
            .sync_prowlarr_indexers(
                &source,
                &[duplicate, distinct],
                ProwlarrRemovePolicy::Deactivate,
                200,
            )
            .await
            .unwrap();
        let static_row = rows
            .iter()
            .find(|indexer| indexer.source_kind == "static")
            .unwrap();
        let imported = rows
            .iter()
            .find(|indexer| indexer.source_indexer_id == "102")
            .unwrap();

        assert!(static_row.enabled);
        assert_eq!("main:Movies#102", imported.name.as_str());
        assert!(imported.enabled);
        assert!(
            rows.iter()
                .all(|indexer| indexer.source_indexer_id != "101" || !indexer.enabled)
        );
    }

    #[tokio::test]
    async fn static_sync_displaces_existing_prowlarr_duplicates() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let source = DependencyName::new("main").unwrap();
        let imported =
            test_prowlarr_indexer("main", 101, "Movies", "https://prowlarr.example/101/api");
        let imported_rows = repository
            .sync_prowlarr_indexers(
                &source,
                &[imported.clone()],
                ProwlarrRemovePolicy::Deactivate,
                100,
            )
            .await
            .unwrap();
        let imported_id = imported_rows
            .iter()
            .find(|indexer| indexer.source_indexer_id == "101")
            .unwrap()
            .id;

        let rows = repository
            .sync_torznab_indexers(
                &[test_indexer(
                    "main:Movies",
                    "https://prowlarr.example/101/api",
                    ApiKeySource::Direct,
                )],
                200,
            )
            .await
            .unwrap();
        let static_row = rows
            .iter()
            .find(|indexer| indexer.source_kind == "static")
            .unwrap();
        let imported_row = rows
            .iter()
            .find(|indexer| indexer.source_indexer_id == "101")
            .unwrap();

        assert_ne!(imported_id, static_row.id);
        assert_eq!("main:Movies", static_row.name.as_str());
        assert!(static_row.enabled);
        assert_eq!(imported_id, imported_row.id);
        assert_eq!("prowlarr", imported_row.source_kind);
        assert_eq!("main:Movies#101", imported_row.name.as_str());
        assert!(!imported_row.enabled);

        let resynced = repository
            .sync_prowlarr_indexers(&source, &[imported], ProwlarrRemovePolicy::Deactivate, 300)
            .await
            .unwrap();
        let imported_row = resynced
            .iter()
            .find(|indexer| indexer.source_indexer_id == "101")
            .unwrap();
        assert_eq!(imported_id, imported_row.id);
        assert!(!imported_row.enabled);
    }

    #[tokio::test]
    async fn static_sync_renames_imported_name_conflicts_without_url_conflict() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let source = DependencyName::new("main").unwrap();
        let imported =
            test_prowlarr_indexer("main", 101, "Movies", "https://prowlarr.example/101/api");
        repository
            .sync_prowlarr_indexers(
                &source,
                &[imported.clone()],
                ProwlarrRemovePolicy::Deactivate,
                100,
            )
            .await
            .unwrap();

        let rows = repository
            .sync_torznab_indexers(
                &[test_indexer(
                    "main:Movies",
                    "https://static.example/api",
                    ApiKeySource::Direct,
                )],
                200,
            )
            .await
            .unwrap();
        let imported_row = rows
            .iter()
            .find(|indexer| indexer.source_indexer_id == "101")
            .unwrap();
        assert_eq!("main:Movies#101", imported_row.name.as_str());
        assert!(!imported_row.enabled);

        let resynced = repository
            .sync_prowlarr_indexers(&source, &[imported], ProwlarrRemovePolicy::Deactivate, 300)
            .await
            .unwrap();
        let imported_row = resynced
            .iter()
            .find(|indexer| indexer.source_indexer_id == "101")
            .unwrap();
        assert_eq!("main:Movies#101", imported_row.name.as_str());
        assert!(imported_row.enabled);
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
        assert_eq!(1, health[0].failure_count);

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
        assert_eq!(2, health[0].failure_count);

        repository
            .record_indexer_caps_success(&name, &TorznabCaps::default(), 400)
            .await
            .unwrap();
        let health = repository.dependency_health_snapshot(10).await.unwrap();
        assert_eq!("healthy", health[0].state);
        assert_eq!(0, health[0].failure_count);
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
    async fn terminal_announce_transitions_clear_fetch_material() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let mut work = test_announce_work("ann_fetch_clear", "guid-fetch-clear", 1);
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
        let claimed = repository
            .claim_announce_work("worker-1", 1, 100, 1)
            .await
            .unwrap();

        assert!(
            repository
                .mark_announce_succeeded(
                    &claimed[0],
                    "worker-1",
                    AnnounceReason::Injected,
                    "injected",
                    2,
                )
                .await
                .unwrap()
        );

        assert!(
            repository
                .announce_fetch_material(&work.id)
                .await
                .unwrap()
                .is_none()
        );
        assert_announce_fetch_columns_cleared(&repository, work.id.as_str()).await;
    }

    #[tokio::test]
    async fn expired_announce_work_clears_fetch_material() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let mut work = test_announce_work("ann_fetch_expired", "guid-fetch-expired", 1);
        let download_url =
            DownloadUrl::new("https://tracker.example/download?id=1&apikey=secret").unwrap();
        work.fetch = Some(
            AnnounceFetchMaterial::new(
                &download_url,
                Some(CookieSecret::new("sid=secret-cookie").unwrap()),
            )
            .unwrap(),
        );
        work.expires_at_ms = 10;
        repository
            .insert_or_dedupe_announce_work(&work, 10)
            .await
            .unwrap();

        assert_eq!(1, repository.expire_announce_work(10).await.unwrap());

        assert!(
            repository
                .announce_fetch_material(&work.id)
                .await
                .unwrap()
                .is_none()
        );
        assert_announce_fetch_columns_cleared(&repository, work.id.as_str()).await;
    }

    async fn assert_announce_fetch_columns_cleared(repository: &Repository, id: &str) {
        let (download_url, cookie): (Option<String>, Option<String>) =
            sqlx::query_as("SELECT download_url, cookie FROM announce_work WHERE id = ?")
                .bind(id)
                .fetch_one(repository.pool())
                .await
                .unwrap();

        assert!(download_url.is_none());
        assert!(cookie.is_none());
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
    async fn terminal_announce_cleanup_uses_success_and_failure_retention() {
        let repository = Repository::connect_in_memory().await.unwrap();
        for (id, guid, status, finished_at_ms) in [
            (
                "ann_success_old",
                "guid-success-old",
                AnnounceStatus::Succeeded,
                999,
            ),
            (
                "ann_success_retained",
                "guid-success-retained",
                AnnounceStatus::Succeeded,
                1001,
            ),
            (
                "ann_failed_old",
                "guid-failed-old",
                AnnounceStatus::TerminalFailed,
                1999,
            ),
            (
                "ann_failed_retained",
                "guid-failed-retained",
                AnnounceStatus::TerminalFailed,
                2001,
            ),
            (
                "ann_expired_old",
                "guid-expired-old",
                AnnounceStatus::Expired,
                1999,
            ),
            (
                "ann_expired_retained",
                "guid-expired-retained",
                AnnounceStatus::Expired,
                2001,
            ),
        ] {
            let mut work = test_announce_work(id, guid, 1);
            work.status = status;
            work.finished_at_ms = Some(finished_at_ms);
            work.updated_at_ms = finished_at_ms;
            repository
                .insert_or_dedupe_announce_work(&work, 10)
                .await
                .unwrap();
        }
        repository
            .insert_or_dedupe_announce_work(
                &test_announce_work("ann_queued_old", "guid-queued", 1),
                10,
            )
            .await
            .unwrap();

        let deleted = repository
            .cleanup_terminal_announce_work(1_000, 2_000, 10)
            .await
            .unwrap();
        let remaining: Vec<String> = sqlx::query_scalar("SELECT id FROM announce_work ORDER BY id")
            .fetch_all(repository.pool())
            .await
            .unwrap();

        assert_eq!(3, deleted);
        assert_eq!(
            vec![
                "ann_expired_retained".to_owned(),
                "ann_failed_retained".to_owned(),
                "ann_queued_old".to_owned(),
                "ann_success_retained".to_owned(),
            ],
            remaining
        );
    }

    #[tokio::test]
    async fn terminal_announce_cleanup_uses_retention_indexes() {
        let repository = Repository::connect_in_memory().await.unwrap();
        for (query, index_name) in [
            (
                r#"
            SELECT id
            FROM announce_work
            WHERE status = 'succeeded'
              AND finished_at IS NOT NULL
              AND finished_at <= ?
            ORDER BY finished_at, id
            LIMIT ?
            "#,
                "idx_announce_work_succeeded_retention",
            ),
            (
                r#"
            SELECT id
            FROM announce_work
            WHERE status = 'terminal_failed'
              AND finished_at IS NOT NULL
              AND finished_at <= ?
            ORDER BY finished_at, id
            LIMIT ?
            "#,
                "idx_announce_work_terminal_failed_retention",
            ),
            (
                r#"
            SELECT id
            FROM announce_work
            WHERE status = 'expired'
              AND finished_at IS NOT NULL
              AND finished_at <= ?
            ORDER BY finished_at, id
            LIMIT ?
            "#,
                "idx_announce_work_expired_retention",
            ),
        ] {
            let plan = explain_query_plan(&repository, query, 10_000, 100).await;

            assert!(
                !plan
                    .iter()
                    .any(|detail| detail.contains("SCAN announce_work")),
                "retention cleanup should not scan announce_work: {plan:?}"
            );
            assert!(
                !plan.iter().any(|detail| detail.contains("USE TEMP B-TREE")),
                "retention cleanup should not sort with a temp b-tree: {plan:?}"
            );
            assert!(
                plan.iter().any(|detail| detail.contains(index_name)),
                "retention cleanup should use a retention index: {plan:?}"
            );
        }
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

    async fn staged_inventory_counts(repository: &Repository) -> (i64, i64) {
        let staged_count =
            sqlx::query_scalar("SELECT COUNT(*) FROM temp.staged_local_inventory_items")
                .fetch_one(repository.pool())
                .await
                .unwrap();
        let staged_file_count =
            sqlx::query_scalar("SELECT COUNT(*) FROM temp.staged_local_inventory_files")
                .fetch_one(repository.pool())
                .await
                .unwrap();

        (staged_count, staged_file_count)
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
            api_key: None,
            api_key_source,
            enabled: true,
        }
    }

    fn test_prowlarr_indexer(
        source: &str,
        prowlarr_id: i64,
        name: &str,
        url: &str,
    ) -> ProwlarrIndexer {
        ProwlarrIndexer {
            source: DependencyName::new(source).unwrap(),
            prowlarr_id,
            name: DependencyName::new(name).unwrap(),
            url: SanitizedTorznabUrl::new(url).unwrap(),
            api_key: Some(ApiKey::new("prowlarr-secret").unwrap()),
            api_key_source: ApiKeySource::Direct,
            tags: Vec::new(),
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

    async fn explain_query_plan(
        repository: &Repository,
        query: &str,
        cutoff_ms: i64,
        limit: u16,
    ) -> Vec<String> {
        sqlx::query(&format!("EXPLAIN QUERY PLAN {query}"))
            .bind(cutoff_ms)
            .bind(i64::from(limit))
            .fetch_all(repository.pool())
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.get("detail"))
            .collect()
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
