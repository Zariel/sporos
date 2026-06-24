use std::cmp::Ordering as CompareOrdering;
use std::collections::BTreeSet;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use sqlx::sqlite::SqliteRow;
use sqlx::{Acquire, Executor, QueryBuilder, Row, Sqlite, SqlitePool, Transaction};
#[cfg(test)]
use tokio::sync::Barrier;
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tracing::{debug_span, info_span};

use crate::announce::{
    AnnounceDedupeHash, AnnounceFetchMaterial, AnnounceReason, AnnounceStatus, AnnounceWorkId,
    AnnounceWorkItem,
};
use crate::config::ProwlarrRemovePolicy;
use crate::domain::{
    ByteSize, CandidateAssessment, CandidateGuid, ClientHost, DependencyKind, DependencyName,
    DependencyState, DisplayName, DownloadUrl, FileIndex, IndexerId, InfoHash, ItemTitle, JobName,
    JobState, LocalFile, LocalItem, LocalItemId, LocalItemSource, MatchDecision, MediaType,
    ReasonText, RemoteCandidate, RemoteCandidateId, SourceKey, TrackerName,
};
use crate::errors::DatabaseError;
use crate::indexers::{ConfiguredTorznabIndexer, ProwlarrIndexer, TorznabCaps};
use crate::secrets::{CookieSecret, sanitize_url_for_logging};

use super::schema::{CONNECTION_PRAGMAS, REQUIRED_TABLES, initial_schema_statements};

mod connection;
mod schema_setup;

use schema_setup::reconcile_inline_schema;

const INVENTORY_STAGING_POOL_MAX_CONNECTIONS: u32 = 4;

#[derive(Debug, Clone)]
pub struct Repository {
    pool: SqlitePool,
    inventory_staging_pool: SqlitePool,
    inventory_commit_lock: Arc<Mutex<()>>,
    prowlarr_sync_lock: Arc<Mutex<()>>,
    #[cfg(test)]
    announce_insert_barrier: Option<Arc<Barrier>>,
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
    pub active_fetch_material_count: i64,
    pub oldest_fetch_material_age_ms: Option<i64>,
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
    Item(Box<OwnedLocalItemFileBatch>),
    Finished,
}

impl OwnedLocalInventoryMessage {
    pub fn item(batch: OwnedLocalItemFileBatch) -> Self {
        Self::Item(Box::new(batch))
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum LocalInventoryScope {
    Client { client_host: ClientHost },
    DataRoot,
    DataRoots { roots: Vec<PathBuf> },
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

const ACTIVE_ANNOUNCE_STATUSES: [AnnounceStatus; 4] = [
    AnnounceStatus::Queued,
    AnnounceStatus::Running,
    AnnounceStatus::Waiting,
    AnnounceStatus::Retryable,
];
const LEGACY_CLIENT_SOURCE_KEY_NORMALIZE_PAGE_SIZE: i64 = 128;

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

fn data_root_source_key_range(root: &Path) -> SourceKeyPrefixRange {
    let mut prefix = path_to_string(root);
    if !prefix.ends_with(std::path::MAIN_SEPARATOR) {
        prefix.push(std::path::MAIN_SEPARATOR);
    }
    SourceKeyPrefixRange::new(prefix)
}

fn data_root_source_key_is_in_roots(source_key: &str, roots: &[PathBuf]) -> bool {
    roots.iter().any(|root| {
        let root_key = path_to_string(root);
        if source_key == root_key {
            return true;
        }
        let range = data_root_source_key_range(root);
        match range.end {
            Some(end) => source_key >= range.start.as_str() && source_key < end.as_str(),
            None => source_key >= range.start.as_str(),
        }
    })
}

impl LocalInventoryScope {
    fn accepts(&self, source_type: &str, source_key: &str) -> bool {
        match self {
            Self::Client { client_host } => {
                source_type == "client"
                    && source_key.starts_with(&client_source_key_prefix(client_host))
            }
            Self::DataRoot => source_type == "data_root",
            Self::DataRoots { roots } => {
                source_type == "data_root" && data_root_source_key_is_in_roots(source_key, roots)
            }
            Self::TorrentCache => source_type == "torrent_cache",
            Self::Virtual => source_type == "virtual",
        }
    }

    fn source_type(&self) -> &'static str {
        match self {
            Self::Client { .. } => "client",
            Self::DataRoot | Self::DataRoots { .. } => "data_root",
            Self::TorrentCache => "torrent_cache",
            Self::Virtual => "virtual",
        }
    }

    fn source_key_ranges(&self) -> Vec<SourceKeyPrefixRange> {
        match self {
            Self::Client { client_host } => {
                vec![SourceKeyPrefixRange::new(client_source_key_prefix(
                    client_host,
                ))]
            }
            Self::DataRoots { roots } => roots
                .iter()
                .map(|root| data_root_source_key_range(root))
                .collect::<Vec<_>>(),
            Self::DataRoot | Self::TorrentCache | Self::Virtual => Vec::new(),
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

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RemoteCandidateCacheMaterial {
    pub info_hash: Option<String>,
    pub torrent_cache_path: Option<std::path::PathBuf>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DeletedRemoteCandidate {
    pub id: RemoteCandidateId,
    pub torrent_cache_path: Option<PathBuf>,
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

#[derive(Clone, Eq, PartialEq)]
pub struct AnnounceCandidateMaterial {
    pub title: ItemTitle,
    pub tracker: TrackerName,
    pub guid: Option<String>,
    pub info_hash: Option<InfoHash>,
    pub size: Option<ByteSize>,
    pub download_url: Option<DownloadUrl>,
    pub cookie: Option<String>,
    pub attempt_count: u16,
}

impl fmt::Debug for AnnounceCandidateMaterial {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AnnounceCandidateMaterial")
            .field("title", &self.title)
            .field("tracker", &self.tracker)
            .field("guid", &self.guid)
            .field("info_hash", &self.info_hash)
            .field("size", &self.size)
            .field(
                "download_url",
                &self
                    .download_url
                    .as_ref()
                    .map(|url| sanitize_url_for_logging(url.as_str())),
            )
            .field("cookie", &self.cookie.as_ref().map(|_| "[REDACTED]"))
            .field("attempt_count", &self.attempt_count)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct AnnounceRetryUpdate<'a> {
    pub reason: AnnounceReason,
    pub next_attempt_at_ms: i64,
    pub now_ms: i64,
    pub error_class: &'a str,
    pub redacted_message: &'a str,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AnnounceDependency {
    pub kind: DependencyKind,
    pub name: DependencyName,
}

impl AnnounceDependency {
    pub fn new(kind: DependencyKind, name: DependencyName) -> Self {
        Self { kind, name }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct LeasedTransition<'a> {
    status: AnnounceStatus,
    reason: AnnounceReason,
    next_attempt_at_ms: Option<i64>,
    now_ms: i64,
    dependency: Option<&'a AnnounceDependency>,
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

    pub async fn check_connection(&self) -> Result<(), DatabaseError> {
        let value = sqlx::query_scalar::<_, i64>("SELECT 1")
            .fetch_one(&self.pool)
            .await
            .map_err(|error| db_error("check sqlite connection", error))?;
        if value == 1 {
            Ok(())
        } else {
            Err(DatabaseError::QueryFailed {
                operation: "check sqlite connection".to_owned(),
                message: format!("unexpected probe result {value}"),
            })
        }
    }

    pub async fn schema_initialized(&self) -> Result<bool, DatabaseError> {
        for table in REQUIRED_TABLES {
            let found: Option<String> = sqlx::query_scalar(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
            )
            .bind(table)
            .fetch_optional(&self.pool)
            .await
            .map_err(|error| db_error("check sqlite schema", error))?;
            if found.is_none() {
                return Ok(false);
            }
        }
        Ok(true)
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
        let mut connection = self
            .inventory_staging_pool
            .acquire()
            .await
            .map_err(|error| db_error("acquire local inventory connection", error))?;
        initialize_staged_local_inventory(&mut connection).await?;

        let mut upserted = 0usize;
        for batch in items {
            let (source_type, source_key) = local_source(&batch.item.source);
            if !scope.accepts(&source_type, &source_key) {
                let _cleanup_result = clear_staged_local_inventory(&mut connection).await;
                return Err(DatabaseError::QueryFailed {
                    operation: "validate local inventory refresh scope".to_owned(),
                    message: format!("item source {source_type}:{source_key} is outside {scope:?}"),
                });
            }

            if let Err(error) =
                stage_local_item_with_files(&mut connection, batch.item, batch.files).await
            {
                let _cleanup_result = clear_staged_local_inventory(&mut connection).await;
                return Err(error);
            }
            upserted = upserted.saturating_add(1);
        }

        let _commit_guard = self.inventory_commit_lock.lock().await;
        let replace_result = commit_staged_local_inventory(&mut connection, &scope).await;
        let pruned = match replace_result {
            Ok(pruned) => pruned,
            Err(error) => {
                let _cleanup_result = clear_staged_local_inventory(&mut connection).await;
                return Err(error);
            }
        };

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

        let _commit_guard = self.inventory_commit_lock.lock().await;
        let replace_result = commit_staged_local_inventory(&mut connection, &scope).await;
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
        let title_grams = title_token_grams(title_token);
        let Some((seed_gram, remaining_grams)) = title_grams.split_first() else {
            return Ok(Vec::new());
        };
        let mut query = QueryBuilder::<Sqlite>::new(
            r#"
            SELECT local_items.id, local_items.source_type, local_items.source_key,
                   local_items.title, local_items.display_name, local_items.media_type,
                   local_items.info_hash, local_items.path, local_items.save_path,
                   local_items.total_size, local_items.mtime_ms
            FROM local_item_title_grams title_match
            INNER JOIN local_items
                ON local_items.id = title_match.item_id
            WHERE title_match.media_type =
            "#,
        );
        query
            .push_bind(media_type_key(media_type))
            .push(" AND title_match.gram = ")
            .push_bind(seed_gram);
        push_title_gram_exists(&mut query, remaining_grams);
        query
            .push(
                r#"
            ORDER BY title_match.title, title_match.source_type, title_match.source_key
            LIMIT
            "#,
            )
            .push_bind(i64::from(limit))
            .push(" OFFSET ")
            .push_bind(i64::from(offset));

        let rows = query
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(|error| db_error("lookup local items by media type and title", error))?;

        rows.into_iter().map(local_item_from_row).collect()
    }

    pub async fn local_items_by_media_type_and_title_tokens_page(
        &self,
        media_type: MediaType,
        title_tokens: &[&str],
        _preferred_title: &str,
        limit: u16,
        offset: u32,
    ) -> Result<Vec<LocalItem>, DatabaseError> {
        let title_grams = title_tokens
            .iter()
            .flat_map(|token| title_token_grams(token))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let Some((seed_gram, remaining_grams)) = title_grams.split_first() else {
            return self
                .local_items_by_media_type_page(media_type, limit, offset)
                .await;
        };

        let mut query = QueryBuilder::<Sqlite>::new(
            r#"
            SELECT local_items.id, local_items.source_type, local_items.source_key,
                   local_items.title, local_items.display_name, local_items.media_type,
                   local_items.info_hash, local_items.path, local_items.save_path,
                   local_items.total_size, local_items.mtime_ms
            FROM local_item_title_grams title_match
            INNER JOIN local_items
                ON local_items.id = title_match.item_id
            WHERE title_match.media_type =
            "#,
        );
        query
            .push_bind(media_type_key(media_type))
            .push(" AND title_match.gram = ")
            .push_bind(seed_gram);
        push_title_gram_exists(&mut query, remaining_grams);
        query
            .push(
                r#"
            ORDER BY title_match.title, title_match.source_type, title_match.source_key
            LIMIT
            "#,
            )
            .push_bind(i64::from(limit))
            .push(" OFFSET ")
            .push_bind(i64::from(offset));

        let rows = query.build().fetch_all(&self.pool).await.map_err(|error| {
            db_error("lookup local items by media type and title tokens", error)
        })?;

        rows.into_iter().map(local_item_from_row).collect()
    }

    pub async fn local_items_by_media_type_and_title_key_page(
        &self,
        media_type: MediaType,
        title_key: &str,
        limit: u16,
        offset: u32,
    ) -> Result<Vec<LocalItem>, DatabaseError> {
        let title_grams = title_token_grams(title_key);
        let Some(seed_gram) = title_grams.first() else {
            return Ok(Vec::new());
        };
        let rows = sqlx::query(
            r#"
            SELECT local_items.id, local_items.source_type, local_items.source_key,
                   local_items.title, local_items.display_name, local_items.media_type,
                   local_items.info_hash, local_items.path, local_items.save_path,
                   local_items.total_size, local_items.mtime_ms
            FROM local_item_title_grams title_match
            INNER JOIN local_items
                ON local_items.id = title_match.item_id
            WHERE title_match.media_type = ?
              AND title_match.gram = ?
              AND title_match.normalized_title = ?
            ORDER BY title_match.title, title_match.source_type, title_match.source_key
            LIMIT ?
            OFFSET ?
            "#,
        )
        .bind(media_type_key(media_type))
        .bind(seed_gram)
        .bind(title_key)
        .bind(i64::from(limit))
        .bind(i64::from(offset))
        .fetch_all(&self.pool)
        .await
        .map_err(|error| db_error("lookup local items by media type and title key", error))?;

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

    pub async fn remote_candidate_cache_material(
        &self,
        indexer_id: &IndexerId,
        guid: &CandidateGuid,
    ) -> Result<Option<RemoteCandidateCacheMaterial>, DatabaseError> {
        let row = sqlx::query(
            r#"
            SELECT info_hash, torrent_cache_path
            FROM remote_candidates
            WHERE indexer_id = ? AND guid = ?
            "#,
        )
        .bind(i64_from_u64(
            indexer_id.get(),
            "remote candidate indexer id",
        )?)
        .bind(guid.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| db_error("read remote candidate cache material", error))?;

        row.map(remote_candidate_cache_material_from_row)
            .transpose()
    }

    pub async fn cleanup_stale_remote_candidates_batch(
        &self,
        last_seen_cutoff_ms: i64,
        decision_cutoff_ms: i64,
        limit: u16,
    ) -> Result<Vec<DeletedRemoteCandidate>, DatabaseError> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|error| db_error("begin stale remote candidate cleanup", error))?;
        let rows = sqlx::query(
            r#"
            SELECT id, torrent_cache_path
            FROM remote_candidates
            WHERE last_seen_at <= ?
              AND NOT EXISTS (
                  SELECT 1
                  FROM match_decisions
                  WHERE candidate_id = remote_candidates.id
                    AND assessed_at > ?
              )
            ORDER BY last_seen_at, id
            LIMIT ?
            "#,
        )
        .bind(last_seen_cutoff_ms)
        .bind(decision_cutoff_ms)
        .bind(i64::from(limit))
        .fetch_all(&mut *transaction)
        .await
        .map_err(|error| db_error("read stale remote candidates", error))?;
        if rows.is_empty() {
            transaction
                .commit()
                .await
                .map_err(|error| db_error("commit stale remote candidate cleanup", error))?;
            return Ok(Vec::new());
        }

        let mut deleted = Vec::with_capacity(rows.len());
        let mut candidate_paths = Vec::with_capacity(rows.len());
        let mut delete = QueryBuilder::new("DELETE FROM remote_candidates WHERE id IN (");
        let mut separated = delete.separated(", ");
        for row in rows {
            let id: i64 = row.get("id");
            let torrent_cache_path = row.get::<Option<String>, _>("torrent_cache_path");
            separated.push_bind(id);
            deleted.push(DeletedRemoteCandidate {
                id: remote_id_from_i64(id, "remote candidate id")?,
                torrent_cache_path: torrent_cache_path.as_ref().map(PathBuf::from),
            });
            candidate_paths.push(torrent_cache_path);
        }
        separated.push_unseparated(")");
        delete
            .build()
            .execute(&mut *transaction)
            .await
            .map_err(|error| db_error("delete stale remote candidates", error))?;
        for (candidate, path) in deleted.iter_mut().zip(candidate_paths.iter()) {
            let Some(path) = path else {
                continue;
            };
            let remaining_reference: Option<i64> = sqlx::query_scalar(
                "SELECT 1 FROM remote_candidates WHERE torrent_cache_path = ? LIMIT 1",
            )
            .bind(path)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|error| db_error("read retained remote candidate cache reference", error))?;
            if remaining_reference.is_some() {
                candidate.torrent_cache_path = None;
            }
        }
        transaction
            .commit()
            .await
            .map_err(|error| db_error("commit stale remote candidate cleanup", error))?;

        Ok(deleted)
    }

    pub async fn remote_candidate_cache_path_is_referenced(
        &self,
        path: &Path,
    ) -> Result<bool, DatabaseError> {
        let referenced: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM remote_candidates WHERE torrent_cache_path = ? LIMIT 1",
        )
        .bind(path_to_string(path))
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| db_error("read remote candidate cache reference", error))?;

        Ok(referenced.is_some())
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
        dependency_type: DependencyKind,
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
            dependency_type = dependency_type.as_str(),
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
        .bind(dependency_type.as_str())
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
        dependency_type: DependencyKind,
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
        .bind(dependency_type.as_str())
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
            .map(dependency_health_snapshot_from_row)
            .collect()
    }

    pub async fn dependency_health_for_type_names(
        &self,
        dependency_type: DependencyKind,
        dependency_names: &[DependencyName],
    ) -> Result<Vec<DependencyHealthSnapshot>, DatabaseError> {
        if dependency_names.is_empty() {
            return Ok(Vec::new());
        }

        let mut query = QueryBuilder::new(
            r#"
            SELECT dependency_type, dependency_name, state, reason, retry_after, failure_count,
                   checked_at
            FROM dependency_health
            WHERE dependency_type = 
            "#,
        );
        query.push_bind(dependency_type.as_str());
        query.push(" AND dependency_name IN (");
        let mut separated = query.separated(", ");
        for dependency_name in dependency_names {
            separated.push_bind(dependency_name.as_str());
        }
        separated.push_unseparated(
            r#")
            ORDER BY dependency_type, dependency_name
            "#,
        );

        let rows = query
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(|error| db_error("read dependency health by names", error))?;

        rows.into_iter()
            .map(dependency_health_snapshot_from_row)
            .collect()
    }

    pub async fn dependency_health_for_indexer_registry(
        &self,
    ) -> Result<Vec<DependencyHealthSnapshot>, DatabaseError> {
        let rows = sqlx::query(
            r#"
            SELECT
                health.dependency_type,
                health.dependency_name,
                health.state,
                health.reason,
                health.retry_after,
                health.failure_count,
                health.checked_at
            FROM indexers INDEXED BY idx_indexers_enabled_name
            INNER JOIN dependency_health AS health
                ON health.dependency_type = ?
               AND health.dependency_name = indexers.name
            WHERE indexers.enabled = 1
            ORDER BY indexers.name
            "#,
        )
        .bind(DependencyKind::Indexer.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(|error| db_error("read indexer registry dependency health", error))?;

        rows.into_iter()
            .map(dependency_health_snapshot_from_row)
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
        self.clear_due_indexer_backoffs(now_ms).await?;
        let rows = if let Some(after_name) = after_name {
            sqlx::query(
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
                FROM indexers INDEXED BY idx_indexers_due_page
                WHERE enabled = 1
                  AND name > ?
                  AND retry_after IS NULL
                ORDER BY name
                LIMIT ?
                "#,
            )
            .bind(after_name.as_str())
            .bind(i64::from(limit))
            .fetch_all(&self.pool)
            .await
        } else {
            sqlx::query(
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
                FROM indexers INDEXED BY idx_indexers_due_page
                WHERE enabled = 1
                  AND retry_after IS NULL
                ORDER BY name
                LIMIT ?
                "#,
            )
            .bind(i64::from(limit))
            .fetch_all(&self.pool)
            .await
        }
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
            FROM indexers INDEXED BY idx_indexers_due_page
            WHERE enabled = 1
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

    async fn clear_due_indexer_backoffs(&self, now_ms: i64) -> Result<(), DatabaseError> {
        sqlx::query(
            r#"
            UPDATE indexers
            SET retry_after = NULL
            WHERE enabled = 1
              AND retry_after IS NOT NULL
              AND retry_after <= ?
            "#,
        )
        .bind(now_ms)
        .execute(&self.pool)
        .await
        .map_err(|error| db_error("clear due indexer backoffs", error))?;

        Ok(())
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
            DependencyKind::Indexer,
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
        self.clear_due_indexer_backoffs(now_ms).await?;
        let rows = if let Some(after_name) = after_name {
            sqlx::query(
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
                FROM indexers INDEXED BY idx_indexers_search_ready_page
                WHERE enabled = 1
                  AND name > ?
                  AND last_caps_refresh_at IS NOT NULL
                  AND retry_after IS NULL
                ORDER BY name
                LIMIT ?
                "#,
            )
            .bind(after_name.as_str())
            .bind(i64::from(limit))
            .fetch_all(&self.pool)
            .await
        } else {
            sqlx::query(
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
                FROM indexers INDEXED BY idx_indexers_search_ready_page
                WHERE enabled = 1
                  AND last_caps_refresh_at IS NOT NULL
                  AND retry_after IS NULL
                ORDER BY name
                LIMIT ?
                "#,
            )
            .bind(i64::from(limit))
            .fetch_all(&self.pool)
            .await
        }
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
            DependencyKind::Indexer,
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
        self.record_dependency_health(
            DependencyKind::Indexer,
            name,
            &dependency_state,
            checked_at_ms,
        )
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
            DependencyKind::Indexer,
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
        for attempt in 0..2 {
            if let Some(id) = select_active_announce_id(&self.pool, &work.dedupe_hash).await? {
                return Ok(AnnounceInsertResult::Deduplicated { id });
            }

            self.wait_before_announce_insert_attempt(attempt).await;
            let result =
                insert_announce_work_if_below_capacity(&self.pool, work, max_pending).await;
            let rows_affected = match result {
                Ok(result) => result.rows_affected(),
                Err(AnnounceInsertAttemptError::DuplicateActiveDedupe) => {
                    if let Some(id) =
                        select_active_announce_id(&self.pool, &work.dedupe_hash).await?
                    {
                        return Ok(AnnounceInsertResult::Deduplicated { id });
                    }
                    if attempt == 0 {
                        continue;
                    }
                    return Err(DatabaseError::Busy {
                        operation: "accept announce work".to_owned(),
                        retry_after_ms: None,
                    });
                }
                Err(AnnounceInsertAttemptError::Database(error)) => return Err(error),
            };

            if rows_affected > 0 {
                return Ok(AnnounceInsertResult::Inserted {
                    id: work.id.clone(),
                });
            }

            if let Some(id) = select_active_announce_id(&self.pool, &work.dedupe_hash).await? {
                return Ok(AnnounceInsertResult::Deduplicated { id });
            }

            return Err(DatabaseError::Busy {
                operation: "accept announce work".to_owned(),
                retry_after_ms: None,
            });
        }

        if let Some(id) = select_active_announce_id(&self.pool, &work.dedupe_hash).await? {
            Ok(AnnounceInsertResult::Deduplicated { id })
        } else {
            Err(DatabaseError::Busy {
                operation: "accept announce work".to_owned(),
                retry_after_ms: None,
            })
        }
    }

    pub async fn announce_candidate_material(
        &self,
        id: &AnnounceWorkId,
    ) -> Result<Option<AnnounceCandidateMaterial>, DatabaseError> {
        let row = sqlx::query(
            r#"
            SELECT title, tracker, guid, info_hash, size, download_url, cookie, attempt_count
            FROM announce_work
            WHERE id = ?
            "#,
        )
        .bind(id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| db_error("load announce work candidate", error))?;

        row.map(announce_candidate_material_from_row).transpose()
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

    pub async fn scrub_announce_fetch_material(
        &self,
        id: &AnnounceWorkId,
        now_ms: i64,
    ) -> Result<bool, DatabaseError> {
        let result = sqlx::query(
            r#"
            UPDATE announce_work
            SET download_url = NULL,
                cookie = NULL,
                updated_at = ?
            WHERE id = ?
              AND status IN ('queued', 'running', 'waiting', 'retryable')
              AND (download_url IS NOT NULL OR cookie IS NOT NULL)
            "#,
        )
        .bind(now_ms)
        .bind(id.as_str())
        .execute(&self.pool)
        .await
        .map_err(|error| db_error("scrub announce fetch material", error))?;

        Ok(result.rows_affected() == 1)
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
            FROM announce_work AS work INDEXED BY idx_announce_work_dependency_schedule
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
                SELECT id FROM announce_work INDEXED BY idx_announce_work_inventory_wakeup
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
                          last_dependency_kind = 'torrent_client'
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
        dependency_type: DependencyKind,
        dependency_name: &DependencyName,
        now_ms: i64,
        limit: u16,
    ) -> Result<u64, DatabaseError> {
        let _span = debug_span!(
            "announce.wake_dependency_recovery",
            dependency_type = dependency_type.as_str(),
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
                SELECT id FROM announce_work INDEXED BY idx_announce_work_waiting_dependency_due
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
        .bind(dependency_type.as_str())
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
                SELECT id FROM announce_work INDEXED BY idx_announce_work_waiting_due
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
        dependency: Option<&AnnounceDependency>,
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
        self.expire_announce_work_batch_limit(now_ms, i64::MAX)
            .await
    }

    pub async fn expire_announce_work_batch(
        &self,
        now_ms: i64,
        limit: u16,
    ) -> Result<u64, DatabaseError> {
        self.expire_announce_work_batch_limit(now_ms, i64::from(limit))
            .await
    }

    async fn expire_announce_work_batch_limit(
        &self,
        now_ms: i64,
        limit: i64,
    ) -> Result<u64, DatabaseError> {
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
            WHERE id IN (
                SELECT id
                FROM announce_work INDEXED BY idx_announce_work_expires_at
                WHERE status IN ('queued', 'running', 'waiting', 'retryable')
                  AND expires_at <= ?
                ORDER BY expires_at, id
                LIMIT ?
            )
            "#,
        )
        .bind(now_ms)
        .bind(now_ms)
        .bind(now_ms)
        .bind(limit)
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

    pub async fn scrub_stale_announce_fetch_material_batch(
        &self,
        now_ms: i64,
        limit: u16,
    ) -> Result<u64, DatabaseError> {
        let active_expired = sqlx::query(
            r#"
            UPDATE announce_work
            SET download_url = NULL,
                cookie = NULL,
                updated_at = ?
            WHERE id IN (
                SELECT id
                FROM announce_work INDEXED BY idx_announce_work_active_fetch_scrub
                WHERE status IN ('queued', 'running', 'waiting', 'retryable')
                  AND expires_at <= ?
                  AND (download_url IS NOT NULL OR cookie IS NOT NULL)
                ORDER BY expires_at, id
                LIMIT ?
            )
            "#,
        )
        .bind(now_ms)
        .bind(now_ms)
        .bind(i64::from(limit))
        .execute(&self.pool)
        .await
        .map_err(|error| db_error("scrub expired announce fetch material", error))?;
        let succeeded = self
            .scrub_terminal_announce_fetch_material_status("succeeded", now_ms, limit)
            .await?;
        let terminal_failed = self
            .scrub_terminal_announce_fetch_material_status("terminal_failed", now_ms, limit)
            .await?;
        let expired = self
            .scrub_terminal_announce_fetch_material_status("expired", now_ms, limit)
            .await?;

        Ok(active_expired
            .rows_affected()
            .saturating_add(succeeded)
            .saturating_add(terminal_failed)
            .saturating_add(expired))
    }

    async fn scrub_terminal_announce_fetch_material_status(
        &self,
        status: &str,
        now_ms: i64,
        limit: u16,
    ) -> Result<u64, DatabaseError> {
        let query = match status {
            "succeeded" => {
                r#"
            UPDATE announce_work
            SET download_url = NULL,
                cookie = NULL,
                updated_at = ?
            WHERE id IN (
                SELECT id
                FROM announce_work INDEXED BY idx_announce_work_succeeded_fetch_scrub
                WHERE status = 'succeeded'
                  AND finished_at IS NOT NULL
                  AND finished_at <= ?
                  AND (download_url IS NOT NULL OR cookie IS NOT NULL)
                ORDER BY finished_at, id
                LIMIT ?
            )
            "#
            }
            "terminal_failed" => {
                r#"
            UPDATE announce_work
            SET download_url = NULL,
                cookie = NULL,
                updated_at = ?
            WHERE id IN (
                SELECT id
                FROM announce_work INDEXED BY idx_announce_work_terminal_failed_fetch_scrub
                WHERE status = 'terminal_failed'
                  AND finished_at IS NOT NULL
                  AND finished_at <= ?
                  AND (download_url IS NOT NULL OR cookie IS NOT NULL)
                ORDER BY finished_at, id
                LIMIT ?
            )
            "#
            }
            "expired" => {
                r#"
            UPDATE announce_work
            SET download_url = NULL,
                cookie = NULL,
                updated_at = ?
            WHERE id IN (
                SELECT id
                FROM announce_work INDEXED BY idx_announce_work_expired_fetch_scrub
                WHERE status = 'expired'
                  AND finished_at IS NOT NULL
                  AND finished_at <= ?
                  AND (download_url IS NOT NULL OR cookie IS NOT NULL)
                ORDER BY finished_at, id
                LIMIT ?
            )
            "#
            }
            _ => {
                return Err(DatabaseError::QueryFailed {
                    operation: "scrub terminal announce fetch material".to_owned(),
                    message: format!("unsupported terminal status {status}"),
                });
            }
        };
        let result = sqlx::query(query)
            .bind(now_ms)
            .bind(now_ms)
            .bind(i64::from(limit))
            .execute(&self.pool)
            .await
            .map_err(|error| db_error("scrub terminal announce fetch material", error))?;

        Ok(result.rows_affected())
    }

    pub async fn recover_stale_announce_leases(&self, now_ms: i64) -> Result<u64, DatabaseError> {
        self.recover_stale_announce_leases_batch_limit(now_ms, i64::MAX)
            .await
    }

    pub async fn recover_stale_announce_leases_batch(
        &self,
        now_ms: i64,
        limit: u16,
    ) -> Result<u64, DatabaseError> {
        self.recover_stale_announce_leases_batch_limit(now_ms, i64::from(limit))
            .await
    }

    async fn recover_stale_announce_leases_batch_limit(
        &self,
        now_ms: i64,
        limit: i64,
    ) -> Result<u64, DatabaseError> {
        let result = sqlx::query(
            r#"
            UPDATE announce_work
            SET status = 'queued',
                reason = 'dependency_backoff',
                updated_at = ?,
                lease_owner = NULL,
                lease_until = NULL
            WHERE id IN (
                SELECT id
                FROM announce_work INDEXED BY idx_announce_work_lease_until
                WHERE status = 'running'
                  AND lease_until <= ?
                  AND expires_at > ?
                ORDER BY lease_until, id
                LIMIT ?
            )
            "#,
        )
        .bind(now_ms)
        .bind(now_ms)
        .bind(now_ms)
        .bind(limit)
        .execute(&self.pool)
        .await
        .map_err(|error| db_error("recover stale announce leases", error))?;

        Ok(result.rows_affected())
    }

    pub async fn announce_status_counts(
        &self,
        limit: u16,
    ) -> Result<Vec<AnnounceStatusCount>, DatabaseError> {
        self.announce_status_counts_limited(Some(limit)).await
    }

    async fn announce_status_counts_limited(
        &self,
        limit: Option<u16>,
    ) -> Result<Vec<AnnounceStatusCount>, DatabaseError> {
        let mut counts = Vec::new();
        for status in ACTIVE_ANNOUNCE_STATUSES {
            let status = announce_status_key(status);
            let rows = sqlx::query(
                r#"
                SELECT status, reason, COUNT(*) AS count
                FROM announce_work
                WHERE status = ?
                GROUP BY status, reason
                ORDER BY status, reason
                "#,
            )
            .bind(status)
            .fetch_all(&self.pool)
            .await
            .map_err(|error| db_error("read announce status counts", error))?;

            counts.extend(rows.into_iter().map(|row| AnnounceStatusCount {
                status: row.get("status"),
                reason: row.get("reason"),
                count: row.get("count"),
            }));
        }
        counts.sort_by(|left, right| {
            right
                .count
                .cmp(&left.count)
                .then_with(|| left.status.cmp(&right.status))
                .then_with(|| left.reason.cmp(&right.reason))
        });
        if let Some(limit) = limit {
            counts.truncate(usize::from(limit));
        }
        Ok(counts)
    }

    pub async fn announce_queue_snapshot(
        &self,
        limit: u16,
        now_ms: i64,
    ) -> Result<AnnounceQueueSnapshot, DatabaseError> {
        self.announce_queue_snapshot_limited(Some(limit), now_ms)
            .await
    }

    pub async fn announce_queue_metrics_snapshot(
        &self,
        now_ms: i64,
    ) -> Result<AnnounceQueueSnapshot, DatabaseError> {
        self.announce_queue_snapshot_limited(None, now_ms).await
    }

    async fn announce_queue_snapshot_limited(
        &self,
        limit: Option<u16>,
        now_ms: i64,
    ) -> Result<AnnounceQueueSnapshot, DatabaseError> {
        let summary = sqlx::query(
            r#"
            SELECT
                COUNT(*) AS active_count,
                MIN(received_at) AS oldest_received_at,
                COALESCE(SUM(CASE
                    WHEN download_url IS NOT NULL OR cookie IS NOT NULL
                    THEN 1 ELSE 0
                END), 0) AS active_fetch_material_count,
                MIN(CASE
                    WHEN download_url IS NOT NULL OR cookie IS NOT NULL
                    THEN received_at
                END) AS oldest_fetch_material_received_at,
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
        let active_fetch_material_count = summary.get("active_fetch_material_count");
        let oldest_fetch_material_received_at: Option<i64> =
            summary.get("oldest_fetch_material_received_at");
        let next_attempt_at: Option<i64> = summary.get("next_attempt_at");
        let running_leases = summary.get("running_leases");
        let oldest_active_age_ms =
            oldest_received_at.map(|received_at| now_ms.saturating_sub(received_at).max(0));
        let oldest_fetch_material_age_ms = oldest_fetch_material_received_at
            .map(|received_at| now_ms.saturating_sub(received_at).max(0));
        let next_retry_delay_ms =
            next_attempt_at.map(|next_attempt| next_attempt.saturating_sub(now_ms).max(0));

        let attempt_counts = self.active_announce_attempt_counts(limit).await?;
        let dependency_wait_counts = self.active_announce_dependency_wait_counts(limit).await?;

        Ok(AnnounceQueueSnapshot {
            active_count,
            oldest_active_age_ms,
            active_fetch_material_count,
            oldest_fetch_material_age_ms,
            next_retry_delay_ms,
            running_leases,
            status_counts: self.announce_status_counts_limited(limit).await?,
            attempt_counts,
            dependency_wait_counts,
        })
    }

    async fn active_announce_attempt_counts(
        &self,
        limit: Option<u16>,
    ) -> Result<Vec<AnnounceAttemptCount>, DatabaseError> {
        let query = match limit {
            Some(_) => {
                r#"
            SELECT
                COALESCE(last_error_class, last_action_outcome, NULLIF(reason, ''), status)
                    AS outcome_class,
                SUM(attempt_count) AS attempts
            FROM announce_work INDEXED BY idx_announce_work_active_attempt_summary
            WHERE status IN ('queued', 'running', 'waiting', 'retryable')
              AND attempt_count > 0
            GROUP BY outcome_class
            ORDER BY attempts DESC, outcome_class
            LIMIT ?
            "#
            }
            None => {
                r#"
            SELECT
                COALESCE(last_error_class, last_action_outcome, NULLIF(reason, ''), status)
                    AS outcome_class,
                SUM(attempt_count) AS attempts
            FROM announce_work INDEXED BY idx_announce_work_active_attempt_summary
            WHERE status IN ('queued', 'running', 'waiting', 'retryable')
              AND attempt_count > 0
            GROUP BY outcome_class
            ORDER BY attempts DESC, outcome_class
            "#
            }
        };
        let mut query = sqlx::query(query);
        if let Some(limit) = limit {
            query = query.bind(i64::from(limit));
        }
        let rows = query
            .fetch_all(&self.pool)
            .await
            .map_err(|error| db_error("read announce attempt counts", error))?;

        Ok(rows
            .into_iter()
            .map(|row| AnnounceAttemptCount {
                outcome_class: row.get("outcome_class"),
                attempts: row.get("attempts"),
            })
            .collect())
    }

    async fn active_announce_dependency_wait_counts(
        &self,
        limit: Option<u16>,
    ) -> Result<Vec<AnnounceDependencyWaitCount>, DatabaseError> {
        let query = match limit {
            Some(_) => {
                r#"
            SELECT
                last_dependency_kind AS dependency_kind,
                last_dependency_name AS dependency_name,
                COUNT(*) AS count
            FROM announce_work INDEXED BY idx_announce_work_active_dependency
            WHERE status IN ('queued', 'running', 'waiting', 'retryable')
              AND last_dependency_kind IS NOT NULL
              AND last_dependency_name IS NOT NULL
            GROUP BY dependency_kind, dependency_name
            ORDER BY count DESC, dependency_kind, dependency_name
            LIMIT ?
            "#
            }
            None => {
                r#"
            SELECT
                last_dependency_kind AS dependency_kind,
                last_dependency_name AS dependency_name,
                COUNT(*) AS count
            FROM announce_work INDEXED BY idx_announce_work_active_dependency
            WHERE status IN ('queued', 'running', 'waiting', 'retryable')
              AND last_dependency_kind IS NOT NULL
              AND last_dependency_name IS NOT NULL
            GROUP BY dependency_kind, dependency_name
            ORDER BY count DESC, dependency_kind, dependency_name
            "#
            }
        };
        let mut query = sqlx::query(query);
        if let Some(limit) = limit {
            query = query.bind(i64::from(limit));
        }
        let rows = query
            .fetch_all(&self.pool)
            .await
            .map_err(|error| db_error("read announce dependency wait counts", error))?;

        Ok(rows
            .into_iter()
            .map(|row| AnnounceDependencyWaitCount {
                dependency_kind: row.get("dependency_kind"),
                dependency_name: row.get("dependency_name"),
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
        let dependency_kind = transition
            .dependency
            .map(|dependency| dependency.kind.as_str())
            .unwrap_or_default();
        let dependency_name = transition
            .dependency
            .map(|dependency| dependency.name.as_str())
            .unwrap_or_default();
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
    pool: &SqlitePool,
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
    .fetch_optional(pool)
    .await
    .map_err(|error| db_error("select active announce dedupe", error))?;

    row.map(|row| AnnounceWorkId::new(row.get::<String, _>("id")))
        .transpose()
        .map_err(|error| DatabaseError::QueryFailed {
            operation: "read announce work id".to_owned(),
            message: error.to_string(),
        })
}

async fn insert_announce_work_if_below_capacity(
    pool: &SqlitePool,
    work: &AnnounceWorkItem,
    max_pending: u32,
) -> Result<sqlx::sqlite::SqliteQueryResult, AnnounceInsertAttemptError> {
    let size = work
        .size
        .map(ByteSize::get)
        .map(|size| i64_from_u64(size, "announce size"))
        .transpose()
        .map_err(AnnounceInsertAttemptError::Database)?;

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
        SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        WHERE (
            SELECT COUNT(*) FROM announce_work
            WHERE status IN ('queued', 'running', 'waiting', 'retryable')
        ) < ?
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
    .bind(size)
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
    .bind(i64::from(max_pending))
    .execute(pool)
    .await
    .map_err(|error| {
        if is_announce_dedupe_constraint(&error) {
            AnnounceInsertAttemptError::DuplicateActiveDedupe
        } else {
            AnnounceInsertAttemptError::Database(db_error("insert announce work", error))
        }
    })
}

#[derive(Debug)]
enum AnnounceInsertAttemptError {
    DuplicateActiveDedupe,
    Database(DatabaseError),
}

fn is_announce_dedupe_constraint(error: &sqlx::Error) -> bool {
    error.as_database_error().is_some_and(|database_error| {
        database_error.constraint() == Some("idx_announce_work_active_dedupe")
            || database_error
                .message()
                .contains("announce_work.dedupe_hash")
    })
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

fn title_token_grams(title_token: &str) -> Vec<String> {
    let normalized = title_token
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect::<String>();
    token_grams(&normalized)
}

fn normalized_lookup_title(value: &str) -> String {
    value
        .split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|token| !token.is_empty())
        .map(str::to_ascii_lowercase)
        .filter(|token| !is_lookup_noise_token(token))
        .collect::<Vec<_>>()
        .join(" ")
}

fn push_title_gram_exists<'a>(query: &mut QueryBuilder<'a, Sqlite>, grams: &'a [String]) {
    for gram in grams {
        query
            .push(
                r#"
                AND EXISTS (
                    SELECT 1
                    FROM local_item_title_grams required_match
                    WHERE required_match.item_id = title_match.item_id
                      AND required_match.gram =
                "#,
            )
            .push_bind(gram)
            .push(")");
    }
}

fn local_item_title_grams(title: &str) -> BTreeSet<String> {
    normalized_lookup_title(title)
        .split_whitespace()
        .flat_map(token_grams)
        .collect()
}

fn token_grams(token: &str) -> Vec<String> {
    if token.len() < 3 {
        return Vec::new();
    }
    token
        .as_bytes()
        .windows(3)
        .filter_map(|gram| std::str::from_utf8(gram).ok())
        .map(str::to_owned)
        .collect()
}

fn is_lookup_noise_token(token: &str) -> bool {
    matches!(
        token,
        "480p"
            | "576p"
            | "720p"
            | "1080p"
            | "2160p"
            | "web"
            | "webdl"
            | "webrip"
            | "bluray"
            | "bdrip"
            | "brrip"
            | "hdtv"
            | "dvdrip"
            | "remux"
            | "x264"
            | "x265"
            | "h264"
            | "h265"
            | "hevc"
            | "av1"
            | "aac"
            | "dts"
            | "proper"
            | "repack"
    )
}

async fn replace_local_item_title_grams(
    transaction: &mut Transaction<'_, Sqlite>,
    item_id: LocalItemId,
    item: &LocalItem,
    source_type: &str,
    source_key: &str,
) -> Result<(), DatabaseError> {
    let item_id = i64_from_u64(item_id.get(), "local item id")?;
    sqlx::query("DELETE FROM local_item_title_grams WHERE item_id = ?")
        .bind(item_id)
        .execute(&mut **transaction)
        .await
        .map_err(|error| db_error("replace local item title grams", error))?;

    insert_local_item_title_grams(
        transaction,
        item_id,
        media_type_key(item.media_type),
        item.title.as_str(),
        source_type,
        source_key,
    )
    .await
}

async fn insert_local_item_title_grams(
    transaction: &mut Transaction<'_, Sqlite>,
    item_id: i64,
    media_type: &str,
    title: &str,
    source_type: &str,
    source_key: &str,
) -> Result<(), DatabaseError> {
    let normalized_title = normalized_lookup_title(title);
    for gram in local_item_title_grams(title) {
        sqlx::query(
            r#"
            INSERT OR IGNORE INTO local_item_title_grams (
                item_id,
                media_type,
                gram,
                normalized_title,
                title,
                source_type,
                source_key
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(item_id)
        .bind(media_type)
        .bind(gram)
        .bind(normalized_title.as_str())
        .bind(title)
        .bind(source_type)
        .bind(source_key)
        .execute(&mut **transaction)
        .await
        .map_err(|error| db_error("insert local item title grams", error))?;
    }

    Ok(())
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
    replace_local_item_title_grams(transaction, item_id, item, &source_type, &source_key).await?;

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

async fn initialize_staged_local_inventory(
    connection: &mut sqlx::pool::PoolConnection<Sqlite>,
) -> Result<(), DatabaseError> {
    for statement in [
        "DROP TABLE IF EXISTS staged_local_inventory_changed_files",
        "DROP TABLE IF EXISTS staged_local_inventory_changed_title_grams",
        "DROP TABLE IF EXISTS staged_local_inventory_items",
        "DROP TABLE IF EXISTS staged_local_inventory_files",
        "DROP TABLE IF EXISTS staged_local_inventory_title_grams",
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
        CREATE TEMP TABLE staged_local_inventory_title_grams (
            source_type TEXT NOT NULL,
            source_key TEXT NOT NULL,
            media_type TEXT NOT NULL,
            gram TEXT NOT NULL,
            normalized_title TEXT NOT NULL,
            title TEXT NOT NULL
        )
        "#,
        r#"
        CREATE TEMP TABLE staged_local_inventory_changed_files (
            source_type TEXT NOT NULL,
            source_key TEXT NOT NULL,
            PRIMARY KEY (source_type, source_key)
        ) WITHOUT ROWID
        "#,
        r#"
        CREATE TEMP TABLE staged_local_inventory_changed_title_grams (
            source_type TEXT NOT NULL,
            source_key TEXT NOT NULL,
            PRIMARY KEY (source_type, source_key)
        ) WITHOUT ROWID
        "#,
        r#"
        CREATE INDEX staged_local_inventory_files_item_idx
            ON staged_local_inventory_files (source_type, source_key)
        "#,
        r#"
        CREATE INDEX staged_local_inventory_files_item_file_idx
            ON staged_local_inventory_files (source_type, source_key, file_index)
        "#,
        r#"
        CREATE INDEX staged_local_inventory_title_grams_item_idx
            ON staged_local_inventory_title_grams (source_type, source_key)
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

const STAGED_LOCAL_INVENTORY_CLEAR_STATEMENTS: [&str; 5] = [
    "DELETE FROM staged_local_inventory_changed_files",
    "DELETE FROM staged_local_inventory_changed_title_grams",
    "DELETE FROM staged_local_inventory_files",
    "DELETE FROM staged_local_inventory_title_grams",
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

    sqlx::query(
        "DELETE FROM staged_local_inventory_title_grams WHERE source_type = ? AND source_key = ?",
    )
    .bind(&source_type)
    .bind(&source_key)
    .execute(&mut **connection)
    .await
    .map_err(|error| db_error("replace staged local title grams", error))?;

    let normalized_title = normalized_lookup_title(item.title.as_str());
    for gram in local_item_title_grams(item.title.as_str()) {
        sqlx::query(
            r#"
            INSERT INTO staged_local_inventory_title_grams (
                source_type,
                source_key,
                media_type,
                gram,
                normalized_title,
                title
            )
            VALUES (?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(&source_type)
        .bind(&source_key)
        .bind(media_type_key(item.media_type))
        .bind(gram)
        .bind(normalized_title.as_str())
        .bind(item.title.as_str())
        .execute(&mut **connection)
        .await
        .map_err(|error| db_error("stage local title gram", error))?;
    }

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

async fn commit_staged_local_inventory(
    connection: &mut sqlx::pool::PoolConnection<Sqlite>,
    scope: &LocalInventoryScope,
) -> Result<u64, DatabaseError> {
    let mut transaction = connection
        .begin()
        .await
        .map_err(|error| db_error("begin local inventory transaction", error))?;

    if let LocalInventoryScope::Client { client_host } = scope {
        normalize_client_source_keys(&mut transaction, client_host).await?;
    }
    initialize_retained_keys(&mut transaction).await?;
    upsert_staged_local_inventory(&mut transaction).await?;
    insert_staged_retained_keys(&mut transaction).await?;
    let pruned = prune_local_items_not_retained(&mut transaction, scope).await?;

    clear_staged_local_inventory_in_transaction(&mut transaction).await?;
    clear_retained_keys(&mut transaction).await?;
    transaction
        .commit()
        .await
        .map_err(|error| db_error("commit local inventory transaction", error))?;

    Ok(pruned)
}

async fn mark_staged_local_inventory_changes(
    transaction: &mut Transaction<'_, Sqlite>,
) -> Result<(), DatabaseError> {
    sqlx::query(
        r#"
        INSERT OR IGNORE INTO staged_local_inventory_changed_title_grams (
            source_type,
            source_key
        )
        SELECT
            staged.source_type,
            staged.source_key
        FROM staged_local_inventory_items staged
        LEFT JOIN local_items existing
            ON existing.source_type = staged.source_type
           AND existing.source_key = staged.source_key
        WHERE existing.id IS NULL
           OR existing.title IS NOT staged.title
           OR existing.media_type IS NOT staged.media_type
        "#,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|error| db_error("mark changed local title grams", error))?;

    sqlx::query(
        r#"
        INSERT OR IGNORE INTO staged_local_inventory_changed_files (
            source_type,
            source_key
        )
        SELECT
            staged.source_type,
            staged.source_key
        FROM staged_local_inventory_items staged
        LEFT JOIN local_items existing
            ON existing.source_type = staged.source_type
           AND existing.source_key = staged.source_key
        WHERE existing.id IS NULL
           OR EXISTS (
                SELECT 1
                FROM staged_local_inventory_files staged_file
                LEFT JOIN local_files existing_file
                    ON existing_file.item_id = existing.id
                   AND existing_file.file_index = staged_file.file_index
                WHERE staged_file.source_type = staged.source_type
                  AND staged_file.source_key = staged.source_key
                  AND (
                        existing_file.item_id IS NULL
                     OR existing_file.relative_path IS NOT staged_file.relative_path
                     OR existing_file.file_name IS NOT staged_file.file_name
                     OR existing_file.size IS NOT staged_file.size
                     OR existing_file.mtime_ms IS NOT staged_file.mtime_ms
                  )
            )
           OR EXISTS (
                SELECT 1
                FROM local_files existing_file
                LEFT JOIN staged_local_inventory_files staged_file
                    ON staged_file.source_type = staged.source_type
                   AND staged_file.source_key = staged.source_key
                   AND staged_file.file_index = existing_file.file_index
                WHERE existing_file.item_id = existing.id
                  AND staged_file.source_type IS NULL
            )
        "#,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|error| db_error("mark changed local files", error))?;

    Ok(())
}

async fn upsert_staged_local_inventory(
    transaction: &mut Transaction<'_, Sqlite>,
) -> Result<(), DatabaseError> {
    mark_staged_local_inventory_changes(transaction).await?;

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
        WHERE local_items.title IS NOT excluded.title
           OR local_items.display_name IS NOT excluded.display_name
           OR local_items.media_type IS NOT excluded.media_type
           OR local_items.info_hash IS NOT excluded.info_hash
           OR local_items.path IS NOT excluded.path
           OR local_items.save_path IS NOT excluded.save_path
           OR local_items.total_size IS NOT excluded.total_size
           OR local_items.mtime_ms IS NOT excluded.mtime_ms
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
            INNER JOIN staged_local_inventory_changed_files changed
                ON changed.source_type = local_items.source_type
               AND changed.source_key = local_items.source_key
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
        INNER JOIN staged_local_inventory_changed_files changed
            ON changed.source_type = staged.source_type
           AND changed.source_key = staged.source_key
        INNER JOIN local_items
            ON local_items.source_type = staged.source_type
           AND local_items.source_key = staged.source_key
        "#,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|error| db_error("insert staged local files", error))?;

    sqlx::query(
        r#"
        DELETE FROM local_item_title_grams
        WHERE item_id IN (
            SELECT local_items.id
            FROM local_items
            INNER JOIN staged_local_inventory_changed_title_grams changed
                ON changed.source_type = local_items.source_type
               AND changed.source_key = local_items.source_key
        )
        "#,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|error| db_error("replace staged local title grams", error))?;

    sqlx::query(
        r#"
        INSERT OR IGNORE INTO local_item_title_grams (
            item_id,
            media_type,
            gram,
            normalized_title,
            title,
            source_type,
            source_key
        )
        SELECT
            local_items.id,
            staged_grams.media_type,
            staged_grams.gram,
            staged_grams.normalized_title,
            staged_grams.title,
            staged_grams.source_type,
            staged_grams.source_key
        FROM staged_local_inventory_title_grams staged_grams
        INNER JOIN staged_local_inventory_changed_title_grams changed
            ON changed.source_type = staged_grams.source_type
           AND changed.source_key = staged_grams.source_key
        INNER JOIN local_items
            ON local_items.source_type = staged_grams.source_type
           AND local_items.source_key = staged_grams.source_key
        "#,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|error| db_error("insert staged local title grams", error))?;

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
    let ranges = scope.source_key_ranges();
    if ranges.is_empty() {
        if matches!(scope, LocalInventoryScope::DataRoots { .. }) {
            return Ok(0);
        }
        let result = sqlx::query(
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
        .map_err(|error| db_error("prune missing local inventory", error))?;

        return Ok(result.rows_affected());
    }

    let mut pruned = 0u64;
    for range in ranges {
        let result = if let Some(end) = range.end {
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
        .map_err(|error| db_error("prune missing local inventory", error))?;

        pruned = pruned.saturating_add(result.rows_affected());
    }

    Ok(pruned)
}

async fn normalize_client_source_keys(
    transaction: &mut Transaction<'_, Sqlite>,
    client_host: &ClientHost,
) -> Result<(), DatabaseError> {
    let legacy_range = SourceKeyPrefixRange::new(format!("{}:", client_host.as_str()));

    loop {
        let rows = read_legacy_client_source_key_page(transaction, &legacy_range).await?;
        if rows.is_empty() {
            break;
        }

        for row in rows {
            let id: i64 = row.get("id");
            let old_source_key: String = row.get("source_key");
            let Some(row_source_key) = old_source_key.strip_prefix(legacy_range.start.as_str())
            else {
                continue;
            };

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
                    .bind(&new_source_key)
                    .bind(id)
                    .execute(&mut **transaction)
                    .await
                    .map_err(|error| {
                        db_error("normalize legacy client inventory source key", error)
                    })?;
                sqlx::query("UPDATE local_item_title_grams SET source_key = ? WHERE item_id = ?")
                    .bind(new_source_key)
                    .bind(id)
                    .execute(&mut **transaction)
                    .await
                    .map_err(|error| db_error("normalize legacy client title gram key", error))?;
            }
        }
    }

    Ok(())
}

async fn read_legacy_client_source_key_page(
    transaction: &mut Transaction<'_, Sqlite>,
    legacy_range: &SourceKeyPrefixRange,
) -> Result<Vec<SqliteRow>, DatabaseError> {
    let rows = if let Some(end) = legacy_range.end.as_deref() {
        sqlx::query(
            r#"
            SELECT id, source_key
            FROM local_items
            WHERE source_type = 'client'
              AND source_key >= ?
              AND source_key < ?
            ORDER BY source_key, id
            LIMIT ?
            "#,
        )
        .bind(legacy_range.start.as_str())
        .bind(end)
        .bind(LEGACY_CLIENT_SOURCE_KEY_NORMALIZE_PAGE_SIZE)
        .fetch_all(&mut **transaction)
        .await
    } else {
        sqlx::query(
            r#"
            SELECT id, source_key
            FROM local_items
            WHERE source_type = 'client'
              AND source_key >= ?
            ORDER BY source_key, id
            LIMIT ?
            "#,
        )
        .bind(legacy_range.start.as_str())
        .bind(LEGACY_CLIENT_SOURCE_KEY_NORMALIZE_PAGE_SIZE)
        .fetch_all(&mut **transaction)
        .await
    }
    .map_err(|error| db_error("read client inventory source key page", error))?;

    Ok(rows)
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
        AnnounceReason::DryRun => "dry_run",
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

fn dependency_health_snapshot_from_row(
    row: SqliteRow,
) -> Result<DependencyHealthSnapshot, DatabaseError> {
    Ok(DependencyHealthSnapshot {
        dependency_type: row.get("dependency_type"),
        dependency_name: DependencyName::new(row.get::<String, _>("dependency_name")).map_err(
            |error| DatabaseError::QueryFailed {
                operation: "read dependency name".to_owned(),
                message: error.to_string(),
            },
        )?,
        state: row.get("state"),
        reason: row.get("reason"),
        retry_after_ms: row.get("retry_after"),
        failure_count: failure_count_from_i64(row.get("failure_count"))?,
        checked_at_ms: row.get("checked_at"),
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

fn remote_candidate_cache_material_from_row(
    row: sqlx::sqlite::SqliteRow,
) -> Result<RemoteCandidateCacheMaterial, DatabaseError> {
    Ok(RemoteCandidateCacheMaterial {
        info_hash: row.get("info_hash"),
        torrent_cache_path: row
            .get::<Option<String>, _>("torrent_cache_path")
            .map(PathBuf::from),
    })
}

fn announce_candidate_material_from_row(
    row: sqlx::sqlite::SqliteRow,
) -> Result<AnnounceCandidateMaterial, DatabaseError> {
    let size = row
        .get::<Option<i64>, _>("size")
        .and_then(|size| u64::try_from(size).ok())
        .map(ByteSize::new);
    let attempt_count = row
        .get::<i64, _>("attempt_count")
        .try_into()
        .unwrap_or(u16::MAX);

    Ok(AnnounceCandidateMaterial {
        title: ItemTitle::new(row.get::<String, _>("title")).map_err(|error| {
            DatabaseError::QueryFailed {
                operation: "read announce candidate title".to_owned(),
                message: error.to_string(),
            }
        })?,
        tracker: TrackerName::new(row.get::<String, _>("tracker")).map_err(|error| {
            DatabaseError::QueryFailed {
                operation: "read announce candidate tracker".to_owned(),
                message: error.to_string(),
            }
        })?,
        guid: row.get("guid"),
        info_hash: row
            .get::<Option<String>, _>("info_hash")
            .map(InfoHash::new)
            .transpose()
            .map_err(|error| DatabaseError::QueryFailed {
                operation: "read announce candidate info hash".to_owned(),
                message: error.to_string(),
            })?,
        size,
        download_url: row
            .get::<Option<String>, _>("download_url")
            .map(DownloadUrl::new)
            .transpose()
            .map_err(|error| DatabaseError::QueryFailed {
                operation: "read announce candidate download URL".to_owned(),
                message: error.to_string(),
            })?,
        cookie: row.get("cookie"),
        attempt_count,
    })
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
            let prefix_without_last = prefix.get(..last_index)?;
            end.push_str(prefix_without_last);
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

#[cfg(test)]
mod tests;
