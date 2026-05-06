//! SQLite state, migrations, cache records, and paged persistence helpers.

use std::{
    borrow::Cow,
    collections::BTreeSet,
    future::Future,
    path::{Path, PathBuf},
};

use serde::Serialize;
use sqlx::{
    Decode, Row, Sqlite, SqlitePool, Type,
    query::Query,
    sqlite::{
        SqliteArguments, SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow,
    },
};
use tokio::runtime::{Builder, Handle, Runtime, RuntimeFlavor};

pub use crate::{Result, domain};
use crate::{
    SporosError,
    domain::{ClientLabel, Decision, File, LookupFields},
};

const DATABASE_FILE_NAME: &str = "sporos.db";
const SCHEMA_VERSION: i64 = 1;
const PRAGMAS: &str = "PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON;";
const REFRESH_DATA_ROOTS: &str = "data_roots";
const REFRESH_CLIENT_SEARCHEES: &str = "client_searchees";
const REFRESH_INDEXERS: &str = "indexers";
const REFRESH_TORRENT_DIR: &str = "torrent_dir";

#[derive(Debug, Clone, Copy)]
struct Migration {
    version: i64,
    sql: &'static str,
}

const MIGRATIONS: &[Migration] = &[Migration {
    version: SCHEMA_VERSION,
    sql: SCHEMA,
}];

/// SQLite database handle with schema helpers.
pub struct Database {
    runtime: Option<Runtime>,
    inner: AsyncDatabase,
}

/// Async SQLite database handle for sqlx-backed persistence call sites.
#[derive(Debug, Clone)]
pub struct AsyncDatabase {
    path: PathBuf,
    pool: SqlitePool,
}

impl Database {
    /// Open `<state_dir>/sporos.db`, enable WAL, and run migrations.
    pub fn open_app_dir(app_dir: &Path) -> crate::Result<Self> {
        Self::open(app_dir.join(DATABASE_FILE_NAME))
    }

    /// Open a database file, enable WAL, and run migrations.
    pub fn open(path: impl AsRef<Path>) -> crate::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| {
                persistence_message(format!("failed to build database runtime: {error}"))
            })?;
        let inner = block_on_runtime(&runtime, AsyncDatabase::open(path))?;
        Ok(Self {
            runtime: Some(runtime),
            inner,
        })
    }

    /// Run pending schema migrations and set SQLite pragmas.
    pub fn initialize(&self) -> crate::Result<()> {
        self.block_on(self.inner.initialize())
    }

    fn pool(&self) -> &SqlitePool {
        self.inner.pool()
    }

    fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future + Send,
        F::Output: Send,
    {
        block_on_runtime(
            self.runtime.as_ref().expect("database runtime is present"),
            future,
        )
    }

    /// Insert a searchee name if needed and return its stable id.
    pub fn get_or_insert_searchee(&self, name: &str) -> crate::Result<i64> {
        self.block_on(async {
            sqlx::query(
                "INSERT INTO searchee (name)
                 VALUES (?1)
                 ON CONFLICT(name) DO NOTHING",
            )
            .bind(name)
            .execute(self.pool())
            .await
            .map_err(sqlx_error)?;
            sqlx::query_scalar("SELECT id FROM searchee WHERE name = ?1")
                .bind(name)
                .fetch_one(self.pool())
                .await
                .map_err(sqlx_error)
        })
    }

    /// Insert or update a candidate decision row.
    pub fn upsert_decision(&self, record: &DecisionRecord<'_>) -> crate::Result<()> {
        self.block_on(async {
            sqlx::query(
                "INSERT INTO decision
                    (searchee_id, guid, info_hash, decision, first_seen, last_seen, fuzzy_size_factor)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(searchee_id, guid) DO UPDATE SET
                    info_hash = excluded.info_hash,
                    decision = excluded.decision,
                    last_seen = excluded.last_seen,
                    fuzzy_size_factor = excluded.fuzzy_size_factor",
            )
            .bind(record.searchee_id)
            .bind(record.guid)
            .bind(record.info_hash)
            .bind(record.decision.as_str())
            .bind(record.first_seen)
            .bind(record.last_seen)
            .bind(record.fuzzy_size_factor)
            .execute(self.pool())
            .await
            .map_err(sqlx_error)?;

            let decision_id: i64 = sqlx::query_scalar(
                "SELECT id FROM decision
                 WHERE searchee_id = ?1 AND guid = ?2",
            )
            .bind(record.searchee_id)
            .bind(record.guid)
            .fetch_one(self.pool())
            .await
            .map_err(sqlx_error)?;
            sqlx::query("DELETE FROM decision_guid_alias WHERE decision_id = ?1")
                .bind(decision_id)
                .execute(self.pool())
                .await
                .map_err(sqlx_error)?;
            if let (Some(alias), Some(info_hash)) =
                (decision_guid_alias(record.guid), record.info_hash)
            {
                sqlx::query(
                    "INSERT INTO decision_guid_alias
                        (alias, decision_id, info_hash, last_seen)
                     VALUES (?1, ?2, ?3, ?4)",
                )
                .bind(alias)
                .bind(decision_id)
                .bind(info_hash)
                .bind(record.last_seen)
                .execute(self.pool())
                .await
                .map_err(sqlx_error)?;
            }
            Ok(())
        })
    }

    /// Stream non-null GUID to info-hash mappings in bounded pages.
    pub fn guid_info_hash_page(
        &self,
        after_id: i64,
        limit: u32,
    ) -> crate::Result<Vec<GuidInfoHash>> {
        self.block_on(async {
            let rows = sqlx::query(
                "SELECT id, guid, info_hash
                 FROM decision
                 WHERE id > ?1 AND info_hash IS NOT NULL
                 ORDER BY id
                 LIMIT ?2",
            )
            .bind(after_id)
            .bind(i64::from(limit))
            .fetch_all(self.pool())
            .await
            .map_err(sqlx_error)?;
            Ok(rows
                .into_iter()
                .map(|row| GuidInfoHash {
                    id: row.get(0),
                    guid: row.get(1),
                    info_hash: row.get(2),
                })
                .collect())
        })
    }

    /// Read the generated API key from settings row `id = 0`.
    pub fn get_api_key(&self) -> crate::Result<Option<String>> {
        self.block_on(self.inner.get_api_key())
    }

    /// Persist the generated API key in settings row `id = 0`.
    pub fn set_api_key(&self, api_key: &str) -> crate::Result<()> {
        self.block_on(self.inner.set_api_key(api_key))
    }

    /// Insert or update one data-dir root row.
    pub fn upsert_data_root(&self, record: &DataRootRecord<'_>) -> crate::Result<()> {
        let lookup = record.lookup;
        self.block_on(async {
            sqlx::query(
                "INSERT INTO data
                    (path, title, search_key, media_type, season, episode, length, file_count, video_bytes, non_video_bytes)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
                 ON CONFLICT(path) DO UPDATE SET
                    title = excluded.title,
                    search_key = excluded.search_key,
                    media_type = excluded.media_type,
                    season = excluded.season,
                    episode = excluded.episode,
                    length = excluded.length,
                    file_count = excluded.file_count,
                    video_bytes = excluded.video_bytes,
                    non_video_bytes = excluded.non_video_bytes",
            )
            .bind(record.path)
            .bind(record.title)
            .bind(lookup.map(|fields| fields.search_key.as_str()))
            .bind(lookup.map(|fields| fields.media_type.as_str()))
            .bind(lookup.and_then(|fields| fields.season.map(i64::from)))
            .bind(lookup.and_then(|fields| fields.episode.map(i64::from)))
            .bind(lookup.map(|fields| i64::try_from(fields.length).unwrap_or(i64::MAX)))
            .bind(lookup.map(|fields| i64::try_from(fields.file_count).unwrap_or(i64::MAX)))
            .bind(lookup.map(|fields| i64::try_from(fields.video_bytes).unwrap_or(i64::MAX)))
            .bind(lookup.map(|fields| {
                i64::try_from(fields.non_video_bytes).unwrap_or(i64::MAX)
            }))
            .execute(self.pool())
            .await
            .map(|_| ())
            .map_err(sqlx_error)
        })
    }

    /// Refresh data-dir roots and prune data/ensemble rows no longer present.
    pub fn refresh_data_roots<'a>(
        &self,
        records: impl IntoIterator<Item = DataRootRecord<'a>>,
    ) -> crate::Result<usize> {
        let refresh_id = self.begin_data_root_refresh()?;
        for record in records {
            self.upsert_data_root(&record)?;
            self.mark_refreshed_data_root(&refresh_id, record.path)?;
        }
        self.finish_data_root_refresh(&refresh_id)
    }

    /// Start a bounded refresh for data-dir roots.
    pub fn begin_data_root_refresh(&self) -> crate::Result<String> {
        self.block_on(begin_refresh_run(self.pool(), REFRESH_DATA_ROOTS))
    }

    /// Mark one data-dir root as present during a bounded refresh.
    pub fn mark_refreshed_data_root(&self, refresh_id: &str, path: &str) -> crate::Result<()> {
        self.block_on(async {
            sqlx::query(
                "INSERT OR IGNORE INTO current_data_roots (refresh_id, path) VALUES (?1, ?2)",
            )
            .bind(refresh_id)
            .bind(path)
            .execute(self.pool())
            .await
            .map(|_| ())
            .map_err(sqlx_error)
        })
    }

    /// Prune data-dir rows absent from the current bounded refresh.
    pub fn finish_data_root_refresh(&self, refresh_id: &str) -> crate::Result<usize> {
        self.block_on(async {
            let mut transaction = self.pool().begin().await.map_err(sqlx_error)?;
            let overlapped =
                finish_refresh_run(&mut transaction, REFRESH_DATA_ROOTS, refresh_id).await?;
            let rows_removed = if overlapped {
                0
            } else {
                let result = sqlx::query(
                    "DELETE FROM data
                     WHERE path NOT IN (SELECT path FROM current_data_roots)",
                )
                .execute(&mut *transaction)
                .await
                .map_err(sqlx_error)?;
                rows_affected(result.rows_affected())?
            };
            sqlx::query("DELETE FROM current_data_roots WHERE refresh_id = ?1")
                .bind(refresh_id)
                .execute(&mut *transaction)
                .await
                .map_err(sqlx_error)?;
            transaction.commit().await.map_err(sqlx_error)?;
            Ok(rows_removed)
        })
    }

    /// Insert or update one client searchee cache row.
    pub fn upsert_client_searchee(&self, record: &ClientSearcheeRecord<'_>) -> crate::Result<()> {
        let files = files_json(record.files)?;
        let tags = labels_json(record.tags)?;
        let trackers = strings_json(record.trackers)?;
        let lookup = record.lookup;
        self.block_on(async {
            sqlx::query(
                "INSERT INTO client_searchee
                    (client_host, info_hash, name, title, files, length, save_path, category, tags, trackers, search_key, media_type, season, episode, file_count, video_bytes, non_video_bytes)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)
                 ON CONFLICT(client_host, info_hash) DO UPDATE SET
                    name = excluded.name,
                    title = excluded.title,
                    files = excluded.files,
                    length = excluded.length,
                    save_path = excluded.save_path,
                    category = excluded.category,
                    tags = excluded.tags,
                    trackers = excluded.trackers,
                    search_key = excluded.search_key,
                    media_type = excluded.media_type,
                    season = excluded.season,
                    episode = excluded.episode,
                    file_count = excluded.file_count,
                    video_bytes = excluded.video_bytes,
                    non_video_bytes = excluded.non_video_bytes",
            )
            .bind(record.client_host)
            .bind(record.info_hash)
            .bind(record.name)
            .bind(record.title)
            .bind(files)
            .bind(i64::try_from(record.length).unwrap_or(i64::MAX))
            .bind(record.save_path)
            .bind(record.category)
            .bind(tags)
            .bind(trackers)
            .bind(lookup.map(|fields| fields.search_key.as_str()))
            .bind(lookup.map(|fields| fields.media_type.as_str()))
            .bind(lookup.and_then(|fields| fields.season.map(i64::from)))
            .bind(lookup.and_then(|fields| fields.episode.map(i64::from)))
            .bind(lookup.map(|fields| i64::try_from(fields.file_count).unwrap_or(i64::MAX)))
            .bind(lookup.map(|fields| i64::try_from(fields.video_bytes).unwrap_or(i64::MAX)))
            .bind(lookup.map(|fields| {
                i64::try_from(fields.non_video_bytes).unwrap_or(i64::MAX)
            }))
            .execute(self.pool())
            .await
            .map(|_| ())
            .map_err(sqlx_error)
        })
    }

    /// Refresh one client's searchee rows and prune removed info hashes.
    pub fn refresh_client_searchees<'a>(
        &self,
        client_host: &str,
        records: impl IntoIterator<Item = ClientSearcheeRecord<'a>>,
    ) -> crate::Result<usize> {
        let refresh_id = self.begin_client_searchee_refresh()?;
        for record in records {
            self.upsert_client_searchee(&record)?;
            self.mark_refreshed_client_info_hash(&refresh_id, record.info_hash)?;
        }
        self.finish_client_searchee_refresh(&refresh_id, client_host)
    }

    /// Start a bounded refresh for one client's searchee rows.
    pub fn begin_client_searchee_refresh(&self) -> crate::Result<String> {
        self.block_on(begin_refresh_run(self.pool(), REFRESH_CLIENT_SEARCHEES))
    }

    /// Mark one info hash as present during a bounded client searchee refresh.
    pub fn mark_refreshed_client_info_hash(
        &self,
        refresh_id: &str,
        info_hash: &str,
    ) -> crate::Result<()> {
        self.block_on(async {
            sqlx::query(
                "INSERT OR IGNORE INTO current_client_info_hashes (refresh_id, info_hash)
                 VALUES (?1, ?2)",
            )
            .bind(refresh_id)
            .bind(info_hash)
            .execute(self.pool())
            .await
            .map(|_| ())
            .map_err(sqlx_error)
        })
    }

    /// Mark one ensemble path as present during a bounded client searchee refresh.
    pub fn mark_refreshed_client_ensemble_path(
        &self,
        refresh_id: &str,
        path: &str,
    ) -> crate::Result<()> {
        self.block_on(async {
            sqlx::query(
                "INSERT OR IGNORE INTO current_client_ensemble_paths (refresh_id, path)
                 VALUES (?1, ?2)",
            )
            .bind(refresh_id)
            .bind(path)
            .execute(self.pool())
            .await
            .map(|_| ())
            .map_err(sqlx_error)
        })
    }

    /// Prune rows absent from the current bounded client searchee refresh.
    pub fn finish_client_searchee_refresh(
        &self,
        refresh_id: &str,
        client_host: &str,
    ) -> crate::Result<usize> {
        self.block_on(async {
            let mut transaction = self.pool().begin().await.map_err(sqlx_error)?;
            let overlapped =
                finish_refresh_run(&mut transaction, REFRESH_CLIENT_SEARCHEES, refresh_id).await?;
            let rows_removed = if overlapped {
                0
            } else {
                sqlx::query(
                    "DELETE FROM client_ensemble
                     WHERE client_host = ?1
                     AND path NOT IN (SELECT path FROM current_client_ensemble_paths)",
                )
                .bind(client_host)
                .execute(&mut *transaction)
                .await
                .map_err(sqlx_error)?;
                sqlx::query(
                    "DELETE FROM client_ensemble
                     WHERE client_host = ?1
                     AND info_hash NOT IN (SELECT info_hash FROM current_client_info_hashes)",
                )
                .bind(client_host)
                .execute(&mut *transaction)
                .await
                .map_err(sqlx_error)?;
                let result = sqlx::query(
                    "DELETE FROM client_searchee
                     WHERE client_host = ?1
                     AND info_hash NOT IN (SELECT info_hash FROM current_client_info_hashes)",
                )
                .bind(client_host)
                .execute(&mut *transaction)
                .await
                .map_err(sqlx_error)?;
                rows_affected(result.rows_affected())?
            };
            sqlx::query("DELETE FROM current_client_info_hashes WHERE refresh_id = ?1")
                .bind(refresh_id)
                .execute(&mut *transaction)
                .await
                .map_err(sqlx_error)?;
            sqlx::query("DELETE FROM current_client_ensemble_paths WHERE refresh_id = ?1")
                .bind(refresh_id)
                .execute(&mut *transaction)
                .await
                .map_err(sqlx_error)?;
            transaction.commit().await.map_err(sqlx_error)?;
            Ok(rows_removed)
        })
    }

    /// Insert or update one ensemble row.
    pub fn upsert_ensemble(&self, record: &EnsembleRecord<'_>) -> crate::Result<()> {
        self.block_on(async {
            if let Some(client_host) = record.client_host {
                let info_hash = record.info_hash.ok_or_else(|| {
                    persistence_message("client ensemble rows require an info hash")
                })?;
                sqlx::query(
                    "INSERT INTO client_ensemble
                        (client_host, path, info_hash, ensemble, element)
                     VALUES (?1, ?2, ?3, ?4, ?5)
                     ON CONFLICT(client_host, path) DO UPDATE SET
                        info_hash = excluded.info_hash,
                        ensemble = excluded.ensemble,
                        element = excluded.element",
                )
                .bind(client_host)
                .bind(record.path)
                .bind(info_hash)
                .bind(record.ensemble)
                .bind(record.element)
                .execute(self.pool())
                .await
                .map(|_| ())
                .map_err(sqlx_error)
            } else {
                let data_root = sqlx::query_scalar::<_, String>(
                    "SELECT path FROM data
                     WHERE ?1 = path OR ?1 LIKE path || '/%'
                     ORDER BY length(path) DESC
                     LIMIT 1",
                )
                .bind(record.path)
                .fetch_optional(self.pool())
                .await
                .map_err(sqlx_error)?
                .ok_or_else(|| persistence_message("data ensemble rows require a data root"))?;
                sqlx::query(
                    "INSERT INTO data_ensemble (data_root, path, info_hash, ensemble, element)
                     VALUES (?1, ?2, ?3, ?4, ?5)
                     ON CONFLICT(path) DO UPDATE SET
                        data_root = excluded.data_root,
                        info_hash = excluded.info_hash,
                        ensemble = excluded.ensemble,
                        element = excluded.element",
                )
                .bind(data_root)
                .bind(record.path)
                .bind(record.info_hash)
                .bind(record.ensemble)
                .bind(record.element)
                .execute(self.pool())
                .await
                .map(|_| ())
                .map_err(sqlx_error)
            }
        })
    }

    /// Delete decision rows that have no cached torrent info hash.
    pub fn delete_null_decisions(&self) -> crate::Result<usize> {
        self.block_on(self.inner.delete_null_decisions())
    }

    /// Clear all search timestamp rows.
    pub fn clear_timestamps(&self) -> crate::Result<usize> {
        self.block_on(self.inner.clear_timestamps())
    }

    /// Clear one known cache table.
    pub fn clear_table(&self, table: CacheTable) -> crate::Result<usize> {
        self.block_on(self.inner.clear_table(table))
    }

    /// Clear persisted indexer failure status and retry timestamps.
    pub fn clear_indexer_failures(&self) -> crate::Result<usize> {
        self.block_on(self.inner.clear_indexer_failures())
    }

    /// Load indexer status rows for observability.
    pub fn indexer_health_rows(&self) -> crate::Result<Vec<IndexerHealthRow>> {
        self.block_on(self.inner.indexer_health_rows())
    }

    /// Record a safe remote endpoint breaker failure.
    pub fn record_endpoint_breaker_failure(
        &self,
        failure: &EndpointBreakerFailure<'_>,
    ) -> crate::Result<EndpointBreakerRow> {
        self.block_on(self.inner.record_endpoint_breaker_failure(failure))
    }

    /// Close one endpoint breaker after a successful probe.
    pub fn close_endpoint_breaker(
        &self,
        endpoint_key: &str,
        operation: &str,
        now: i64,
    ) -> crate::Result<()> {
        self.block_on(
            self.inner
                .close_endpoint_breaker(endpoint_key, operation, now),
        )
    }

    /// Return the currently open breaker for an endpoint operation.
    pub fn open_endpoint_breaker(
        &self,
        endpoint_key: &str,
        operation: &str,
        now: i64,
    ) -> crate::Result<Option<EndpointBreakerRow>> {
        self.block_on(
            self.inner
                .open_endpoint_breaker(endpoint_key, operation, now),
        )
    }

    /// Load endpoint breaker aggregate state for observability.
    pub fn endpoint_breaker_stats(&self, now: i64) -> crate::Result<EndpointBreakerStats> {
        self.block_on(self.inner.endpoint_breaker_stats(now))
    }

    /// Read a scheduler job's last run timestamp.
    pub fn read_last_run(&self, name: &str) -> crate::Result<Option<i64>> {
        self.block_on(self.inner.read_last_run(name))
    }

    /// Insert or update a scheduler job's last run timestamp.
    pub fn write_last_run(&self, name: &str, last_run: i64) -> crate::Result<()> {
        self.block_on(self.inner.write_last_run(name, last_run))
    }

    /// Insert or dedupe an active durable announce work row.
    pub fn insert_or_dedupe_announce_work(
        &self,
        record: &AnnounceWorkInsert<'_>,
    ) -> crate::Result<AnnounceWorkEnqueue> {
        self.block_on(self.inner.insert_or_dedupe_announce_work(record))
    }

    /// Insert or dedupe an active durable announce work row with an active queue bound.
    pub fn insert_or_dedupe_announce_work_bounded(
        &self,
        record: &AnnounceWorkInsert<'_>,
        max_active: u32,
    ) -> crate::Result<Option<AnnounceWorkEnqueue>> {
        self.block_on(
            self.inner
                .insert_or_dedupe_announce_work_bounded(record, max_active),
        )
    }

    /// Claim ready durable announce work and mark it running.
    pub fn claim_announce_work(
        &self,
        now: i64,
        lease_owner: &str,
        lease_timeout: i64,
        limit: u32,
    ) -> crate::Result<Vec<AnnounceWorkRecord>> {
        self.block_on(
            self.inner
                .claim_announce_work(now, lease_owner, lease_timeout, limit),
        )
    }

    /// Schedule claimed announce work for another attempt.
    pub fn schedule_announce_retry(&self, update: &AnnounceWorkRetry<'_>) -> crate::Result<bool> {
        self.block_on(self.inner.schedule_announce_retry(update))
    }

    /// Mark announce work as succeeded, terminally failed, or expired.
    pub fn finish_announce_work(&self, update: &AnnounceWorkFinish<'_>) -> crate::Result<bool> {
        self.block_on(self.inner.finish_announce_work(update))
    }

    /// Expire non-terminal announce work whose TTL has elapsed.
    pub fn expire_announce_work(
        &self,
        now: i64,
        limit: u32,
    ) -> crate::Result<Vec<AnnounceWorkRecord>> {
        self.block_on(self.inner.expire_announce_work(now, limit))
    }

    /// Delete terminal announce work older than the retention window.
    pub fn prune_terminal_announce_work(
        &self,
        now: i64,
        retention_millis: i64,
    ) -> crate::Result<usize> {
        self.block_on(
            self.inner
                .prune_terminal_announce_work(now, retention_millis),
        )
    }

    /// Return abandoned running work to a retryable state after lease timeout.
    pub fn release_stale_announce_leases(
        &self,
        now: i64,
        next_attempt_at: i64,
        limit: u32,
    ) -> crate::Result<Vec<AnnounceWorkRecord>> {
        self.block_on(
            self.inner
                .release_stale_announce_leases(now, next_attempt_at, limit),
        )
    }

    /// Load durable announce queue stats for status and metrics.
    pub fn announce_queue_stats(&self, now: i64) -> crate::Result<AnnounceQueueStats> {
        self.block_on(self.inner.announce_queue_stats(now))
    }

    /// Return a persisted indexer id for a configured URL.
    pub fn indexer_id(&self, url: &str) -> crate::Result<i64> {
        self.block_on(async {
            sqlx::query_scalar("SELECT id FROM indexer WHERE url = ?1")
                .bind(url)
                .fetch_one(self.pool())
                .await
                .map_err(sqlx_error)
        })
    }

    /// Synchronize configured Torznab indexers with persisted rows.
    pub fn sync_indexers<'a>(
        &self,
        configured: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> crate::Result<IndexerSyncStats> {
        let configured = configured.into_iter().collect::<Vec<_>>();
        self.block_on(async {
            let refresh_id = begin_refresh_run(self.pool(), REFRESH_INDEXERS).await?;
            let mut result = IndexerSyncStats::default();
            for (url, apikey) in configured {
                sqlx::query(
                    "INSERT OR IGNORE INTO current_indexer_urls (refresh_id, url)
                     VALUES (?1, ?2)",
                )
                    .bind(&refresh_id)
                    .bind(url)
                    .execute(self.pool())
                    .await
                    .map_err(sqlx_error)?;
                let changed = sqlx::query(
                    "UPDATE indexer
                     SET apikey = ?2,
                         active = 1,
                         search_cap = COALESCE(search_cap, 1),
                         tv_search_cap = COALESCE(tv_search_cap, 1),
                         movie_search_cap = COALESCE(movie_search_cap, 1),
                         music_search_cap = COALESCE(music_search_cap, 1),
                         audio_search_cap = COALESCE(audio_search_cap, 1),
                         book_search_cap = COALESCE(book_search_cap, 1),
                         tv_id_caps = COALESCE(tv_id_caps, '[]'),
                         movie_id_caps = COALESCE(movie_id_caps, '[]'),
                         cat_caps = COALESCE(cat_caps, '{\"movie\":false,\"tv\":false,\"anime\":false,\"xxx\":false,\"audio\":false,\"book\":false,\"additional\":false}'),
                         limits_caps = COALESCE(limits_caps, '{\"default\":100,\"max\":100}'),
                         status = CASE WHEN status = 'UNKNOWN_ERROR' THEN NULL ELSE status END
                     WHERE url = ?1",
                )
                .bind(url)
                .bind(apikey)
                .execute(self.pool())
                .await
                .map_err(sqlx_error)?;
                if changed.rows_affected() == 0 {
                    sqlx::query(
                        "INSERT INTO indexer
                            (url, apikey, active, search_cap, tv_search_cap, movie_search_cap,
                             music_search_cap, audio_search_cap, book_search_cap, tv_id_caps,
                             movie_id_caps, cat_caps, limits_caps)
                         VALUES
                            (?1, ?2, 1, 1, 1, 1, 1, 1, 1, '[]', '[]',
                             '{\"movie\":false,\"tv\":false,\"anime\":false,\"xxx\":false,\"audio\":false,\"book\":false,\"additional\":false}',
                             '{\"default\":100,\"max\":100}')",
                    )
                    .bind(url)
                    .bind(apikey)
                    .execute(self.pool())
                    .await
                    .map_err(sqlx_error)?;
                    result.inserted += 1;
                } else {
                    result.updated += rows_affected(changed.rows_affected())?;
                }
            }
            let mut transaction = self.pool().begin().await.map_err(sqlx_error)?;
            let overlapped =
                finish_refresh_run(&mut transaction, REFRESH_INDEXERS, &refresh_id).await?;
            if !overlapped {
                let deactivated = sqlx::query(
                    "UPDATE indexer
                     SET active = 0
                     WHERE active = 1
                     AND url NOT IN (SELECT url FROM current_indexer_urls)",
                )
                .execute(&mut *transaction)
                .await
                .map_err(sqlx_error)?;
                result.deactivated = rows_affected(deactivated.rows_affected())?;
            }
            sqlx::query("DELETE FROM current_indexer_urls WHERE refresh_id = ?1")
                .bind(&refresh_id)
                .execute(&mut *transaction)
                .await
                .map_err(sqlx_error)?;
            transaction.commit().await.map_err(sqlx_error)?;
            Ok(result)
        })
    }

    /// Persist parsed caps for an indexer row.
    pub fn update_indexer_caps(&self, record: &IndexerCapsRecord<'_>) -> crate::Result<()> {
        self.block_on(async {
            sqlx::query(
                "UPDATE indexer SET
                    search_cap = ?2,
                    tv_search_cap = ?3,
                    movie_search_cap = ?4,
                    music_search_cap = ?5,
                    audio_search_cap = ?6,
                    book_search_cap = ?7,
                    tv_id_caps = ?8,
                    movie_id_caps = ?9,
                    cat_caps = ?10,
                    limits_caps = ?11,
                    status = NULL,
                    retry_after = NULL
                 WHERE id = ?1",
            )
            .bind(record.indexer_id)
            .bind(record.search)
            .bind(record.tv_search)
            .bind(record.movie_search)
            .bind(record.music_search)
            .bind(record.audio_search)
            .bind(record.book_search)
            .bind(record.tv_ids)
            .bind(record.movie_ids)
            .bind(record.categories)
            .bind(record.limits)
            .execute(self.pool())
            .await
            .map(|_| ())
            .map_err(sqlx_error)
        })
    }

    /// Mark an indexer status and retry timestamp.
    pub fn set_indexer_status(
        &self,
        indexer_id: i64,
        status: Option<&str>,
        retry_after: Option<u64>,
    ) -> crate::Result<()> {
        self.block_on(async {
            sqlx::query("UPDATE indexer SET status = ?2, retry_after = ?3 WHERE id = ?1")
                .bind(indexer_id)
                .bind(status)
                .bind(retry_after.and_then(|value| i64::try_from(value).ok()))
                .execute(self.pool())
                .await
                .map(|_| ())
                .map_err(sqlx_error)
        })
    }

    /// Load enabled indexers for the current timestamp.
    pub fn enabled_indexers(&self, now_millis: u64) -> crate::Result<Vec<IndexerRow>> {
        self.block_on(async {
            let rows = sqlx::query(
                "SELECT id, url, apikey
                 FROM indexer
                 WHERE active = 1
                   AND search_cap = 1
                   AND (status IS NULL OR status = 'OK' OR retry_after < ?1)",
            )
            .bind(i64::try_from(now_millis).unwrap_or(i64::MAX))
            .fetch_all(self.pool())
            .await
            .map_err(sqlx_error)?;
            Ok(rows
                .into_iter()
                .map(|row| IndexerRow {
                    id: row.get(0),
                    url: row.get(1),
                    apikey: row.get(2),
                })
                .collect())
        })
    }

    /// Load enabled indexers and serialized caps for search.
    pub fn enabled_search_indexers(&self, now_millis: u64) -> crate::Result<Vec<SearchIndexerRow>> {
        self.block_on(async {
            let rows = sqlx::query(
                "SELECT id, url, apikey,
                        search_cap, tv_search_cap, movie_search_cap, music_search_cap,
                        audio_search_cap, book_search_cap, tv_id_caps, movie_id_caps,
                        cat_caps, limits_caps
                 FROM indexer
                 WHERE active = 1
                   AND search_cap = 1
                   AND (status IS NULL OR status = 'OK' OR retry_after < ?1)",
            )
            .bind(i64::try_from(now_millis).unwrap_or(i64::MAX))
            .fetch_all(self.pool())
            .await
            .map_err(sqlx_error)?;
            Ok(rows
                .into_iter()
                .map(|row| SearchIndexerRow {
                    id: row.get(0),
                    url: row.get(1),
                    apikey: row.get(2),
                    search: row.get(3),
                    tv_search: row.get(4),
                    movie_search: row.get(5),
                    music_search: row.get(6),
                    audio_search: row.get(7),
                    book_search: row.get(8),
                    tv_ids: row.get(9),
                    movie_ids: row.get(10),
                    categories: row.get(11),
                    limits: row.get(12),
                })
                .collect())
        })
    }

    /// Update an indexer display name.
    pub fn update_indexer_name(&self, indexer_id: i64, name: &str) -> crate::Result<()> {
        self.block_on(async {
            sqlx::query("UPDATE indexer SET name = ?2 WHERE id = ?1")
                .bind(indexer_id)
                .bind(name)
                .execute(self.pool())
                .await
                .map(|_| ())
                .map_err(sqlx_error)
        })
    }

    /// Update indexer tracker names with caller-encoded JSON.
    pub fn update_indexer_trackers_json(
        &self,
        indexer_id: i64,
        trackers: &str,
    ) -> crate::Result<()> {
        self.block_on(async {
            let incoming = serde_json::from_str::<Vec<String>>(trackers).map_err(|error| {
                persistence_message(format!("failed to parse indexer trackers JSON: {error}"))
            })?;
            let existing_json: Option<String> =
                sqlx::query_scalar("SELECT trackers FROM indexer WHERE id = ?1")
                    .bind(indexer_id)
                    .fetch_optional(self.pool())
                    .await
                    .map_err(sqlx_error)?
                    .flatten();
            let mut tracker_values = BTreeSet::new();
            if let Some(existing_json) = existing_json {
                let existing =
                    serde_json::from_str::<Vec<String>>(&existing_json).map_err(|error| {
                        persistence_message(format!(
                            "failed to parse stored indexer trackers JSON: {error}"
                        ))
                    })?;
                tracker_values.extend(existing);
            }
            let existing_child = sqlx::query_scalar::<_, String>(
                "SELECT tracker FROM indexer_tracker WHERE indexer_id = ?1",
            )
            .bind(indexer_id)
            .fetch_all(self.pool())
            .await
            .map_err(sqlx_error)?;
            tracker_values.extend(existing_child);
            tracker_values.extend(incoming);
            let tracker_values = tracker_values.into_iter().collect::<Vec<_>>();
            let encoded = serde_json::to_string(&tracker_values).map_err(|error| {
                persistence_message(format!(
                    "failed to serialize indexer trackers JSON: {error}"
                ))
            })?;
            sqlx::query("UPDATE indexer SET trackers = ?2 WHERE id = ?1")
                .bind(indexer_id)
                .bind(encoded)
                .execute(self.pool())
                .await
                .map_err(sqlx_error)?;
            sqlx::query("DELETE FROM indexer_tracker WHERE indexer_id = ?1")
                .bind(indexer_id)
                .execute(self.pool())
                .await
                .map_err(sqlx_error)?;
            for tracker in tracker_values {
                sqlx::query(
                    "INSERT OR IGNORE INTO indexer_tracker (indexer_id, tracker)
                     VALUES (?1, ?2)",
                )
                .bind(indexer_id)
                .bind(tracker)
                .execute(self.pool())
                .await
                .map_err(sqlx_error)?;
            }
            Ok(())
        })
    }

    /// Read a stored RSS cursor.
    pub fn read_rss_cursor(&self, indexer_id: i64) -> crate::Result<Option<String>> {
        self.block_on(async {
            sqlx::query_scalar("SELECT last_seen_guid FROM rss WHERE indexer_id = ?1")
                .bind(indexer_id)
                .fetch_optional(self.pool())
                .await
                .map_err(sqlx_error)
        })
    }

    /// Insert or update a stored RSS cursor.
    pub fn update_rss_cursor(&self, indexer_id: i64, guid: &str) -> crate::Result<()> {
        self.block_on(async {
            sqlx::query(
                "INSERT INTO rss (indexer_id, last_seen_guid)
                 VALUES (?1, ?2)
                 ON CONFLICT(indexer_id) DO UPDATE SET last_seen_guid = excluded.last_seen_guid",
            )
            .bind(indexer_id)
            .bind(guid)
            .execute(self.pool())
            .await
            .map(|_| ())
            .map_err(sqlx_error)
        })
    }

    /// Look up a cached decision by searchee and GUID.
    pub fn cached_decision(
        &self,
        searchee_id: i64,
        guid: &str,
    ) -> crate::Result<Option<CachedDecisionRecord>> {
        self.block_on(async {
            let row = sqlx::query(
                "SELECT decision, info_hash FROM decision
                 WHERE searchee_id = ?1 AND guid = ?2",
            )
            .bind(searchee_id)
            .bind(guid)
            .fetch_optional(self.pool())
            .await
            .map_err(sqlx_error)?;
            Ok(row.map(|row| CachedDecisionRecord {
                decision: row.get(0),
                info_hash: row.get(1),
            }))
        })
    }

    /// Look up a cached candidate info hash by exact GUID/link.
    pub fn decision_info_hash_by_guid(&self, key: &str) -> crate::Result<Option<String>> {
        self.block_on(async {
            sqlx::query_scalar(
                "SELECT info_hash FROM decision
                 WHERE guid = ?1 AND info_hash IS NOT NULL
                 ORDER BY id DESC LIMIT 1",
            )
            .bind(key)
            .fetch_optional(self.pool())
            .await
            .map_err(sqlx_error)
        })
    }

    /// Look up a cached candidate info hash by normalized tracker torrent id.
    pub fn decision_info_hash_by_tracker_id(&self, id: &str) -> crate::Result<Option<String>> {
        self.block_on(async {
            sqlx::query_scalar(decision_guid_alias_lookup_sql())
                .bind(decision_guid_alias_for_torrent_id(id))
                .fetch_optional(self.pool())
                .await
                .map_err(sqlx_error)
        })
    }

    /// Read a search timestamp row.
    pub fn read_timestamp(
        &self,
        searchee_id: i64,
        indexer_id: i64,
    ) -> crate::Result<Option<TimestampRecord>> {
        self.block_on(async {
            let row = sqlx::query(
                "SELECT first_searched, last_searched
                 FROM timestamp
                 WHERE searchee_id = ?1 AND indexer_id = ?2",
            )
            .bind(searchee_id)
            .bind(indexer_id)
            .fetch_optional(self.pool())
            .await
            .map_err(sqlx_error)?;
            Ok(row.map(|row| TimestampRecord {
                first_searched: row.get::<i64, _>(0).try_into().unwrap_or(0),
                last_searched: row.get::<i64, _>(1).try_into().unwrap_or(0),
            }))
        })
    }

    /// Insert or update a search timestamp row.
    pub fn update_timestamp(
        &self,
        searchee_id: i64,
        indexer_id: i64,
        now_millis: u64,
    ) -> crate::Result<()> {
        self.block_on(async {
            sqlx::query(
                "INSERT INTO timestamp (searchee_id, indexer_id, first_searched, last_searched)
                 VALUES (?1, ?2, ?3, ?3)
                 ON CONFLICT(searchee_id, indexer_id) DO UPDATE SET
                    last_searched = excluded.last_searched",
            )
            .bind(searchee_id)
            .bind(indexer_id)
            .bind(i64::try_from(now_millis).unwrap_or(i64::MAX))
            .execute(self.pool())
            .await
            .map(|_| ())
            .map_err(sqlx_error)
        })
    }

    /// Start a bounded refresh for torrent-dir rows.
    pub fn begin_torrent_dir_refresh(&self) -> crate::Result<String> {
        self.block_on(begin_refresh_run(self.pool(), REFRESH_TORRENT_DIR))
    }

    /// Mark one torrent-dir path as present during refresh.
    pub fn mark_refreshed_torrent_path(
        &self,
        refresh_id: &str,
        file_path: &str,
    ) -> crate::Result<()> {
        self.block_on(async {
            sqlx::query(
                "INSERT OR IGNORE INTO current_torrent_dir (refresh_id, file_path)
                 VALUES (?1, ?2)",
            )
            .bind(refresh_id)
            .bind(file_path)
            .execute(self.pool())
            .await
            .map(|_| ())
            .map_err(sqlx_error)
        })
    }

    /// Delete one torrent-dir cache row.
    pub fn delete_torrent_path(&self, file_path: &str) -> crate::Result<usize> {
        self.block_on(async {
            let result = sqlx::query("DELETE FROM torrent WHERE file_path = ?1")
                .bind(file_path)
                .execute(self.pool())
                .await
                .map_err(sqlx_error)?;
            rows_affected(result.rows_affected())
        })
    }

    /// Insert or update one torrent-dir cache row.
    pub fn upsert_torrent_path(
        &self,
        info_hash: &str,
        name: &str,
        file_path: &str,
    ) -> crate::Result<()> {
        self.block_on(async {
            sqlx::query(
                "INSERT INTO torrent (info_hash, name, file_path)
                 VALUES (?1, ?2, ?3)
                 ON CONFLICT(file_path) DO UPDATE SET
                    info_hash = excluded.info_hash,
                    name = excluded.name",
            )
            .bind(info_hash)
            .bind(name)
            .bind(file_path)
            .execute(self.pool())
            .await
            .map(|_| ())
            .map_err(sqlx_error)
        })
    }

    /// Prune torrent-dir rows absent from the current bounded refresh.
    pub fn finish_torrent_dir_refresh(&self, refresh_id: &str) -> crate::Result<usize> {
        self.block_on(async {
            let mut transaction = self.pool().begin().await.map_err(sqlx_error)?;
            let overlapped =
                finish_refresh_run(&mut transaction, REFRESH_TORRENT_DIR, refresh_id).await?;
            let rows_removed = if overlapped {
                0
            } else {
                let result = sqlx::query(
                    "DELETE FROM torrent
                     WHERE file_path NOT IN (SELECT file_path FROM current_torrent_dir)",
                )
                .execute(&mut *transaction)
                .await
                .map_err(sqlx_error)?;
                rows_affected(result.rows_affected())?
            };
            sqlx::query("DELETE FROM current_torrent_dir WHERE refresh_id = ?1")
                .bind(refresh_id)
                .execute(&mut *transaction)
                .await
                .map_err(sqlx_error)?;
            transaction.commit().await.map_err(sqlx_error)?;
            Ok(rows_removed)
        })
    }

    /// Load all client searchee cache rows for focused verification paths.
    pub fn client_searchee_rows(&self) -> crate::Result<Vec<ClientSearcheeCacheRecord>> {
        self.block_on(async {
            let rows = sqlx::query(
                "SELECT client_host, info_hash, name, title, files, save_path, category, tags, trackers
                 FROM client_searchee",
            )
            .fetch_all(self.pool())
            .await
            .map_err(sqlx_error)?;
            Ok(rows.into_iter().map(client_searchee_record).collect())
        })
    }

    /// Select one bounded page of likely client reverse lookup rows.
    pub fn reverse_lookup_client_page(
        &self,
        criteria: &ReverseLookupCriteria<'_>,
        after_rowid: i64,
        limit: i64,
    ) -> crate::Result<Vec<ReverseLookupClientRecord>> {
        if criteria.search_keys.is_empty() || limit <= 0 {
            return Ok(Vec::new());
        }
        self.block_on(async {
            let sql = reverse_lookup_client_sql(criteria.search_keys.len());
            let params = reverse_lookup_params(criteria, after_rowid, limit);
            let rows = bind_values(sqlx::query(&sql), &params)
                .fetch_all(self.pool())
                .await
                .map_err(sqlx_error)?;
            Ok(rows
                .into_iter()
                .map(|row| ReverseLookupClientRecord {
                    rowid: row.get(0),
                    client_host: row.get(1),
                    info_hash: row.get(2),
                    title: row.get(3),
                })
                .collect())
        })
    }

    /// Load distinct info hashes currently known to configured clients.
    pub fn client_info_hashes(&self) -> crate::Result<Vec<String>> {
        self.block_on(async {
            sqlx::query_scalar("SELECT DISTINCT info_hash FROM client_searchee")
                .fetch_all(self.pool())
                .await
                .map_err(sqlx_error)
        })
    }

    /// Select one bounded page of likely data-dir reverse lookup rows.
    pub fn reverse_lookup_data_page(
        &self,
        criteria: &ReverseLookupCriteria<'_>,
        after_rowid: i64,
        limit: i64,
    ) -> crate::Result<Vec<ReverseLookupDataRecord>> {
        if criteria.search_keys.is_empty() || limit <= 0 {
            return Ok(Vec::new());
        }
        self.block_on(async {
            let sql = reverse_lookup_data_sql(criteria.search_keys.len());
            let params = reverse_lookup_params(criteria, after_rowid, limit);
            let rows = bind_values(sqlx::query(&sql), &params)
                .fetch_all(self.pool())
                .await
                .map_err(sqlx_error)?;
            Ok(rows
                .into_iter()
                .map(|row| ReverseLookupDataRecord {
                    rowid: row.get(0),
                    path: row.get(1),
                    title: row.get(2),
                })
                .collect())
        })
    }

    /// Load virtual season and episode rows.
    pub fn ensemble_rows(
        &self,
        ensemble: &str,
        element: Option<&str>,
    ) -> crate::Result<Vec<EnsembleCacheRecord>> {
        self.block_on(async {
            let rows = if let Some(element) = element {
                sqlx::query(ensemble_data_sql(true))
                    .bind(ensemble)
                    .bind(element)
                    .fetch_all(self.pool())
                    .await
            } else {
                sqlx::query(ensemble_data_sql(false))
                    .bind(ensemble)
                    .fetch_all(self.pool())
                    .await
            }
            .map_err(sqlx_error)?;
            let mut records = rows
                .into_iter()
                .map(|row| EnsembleCacheRecord {
                    client_host: row.get(0),
                    path: row.get(1),
                    info_hash: row.get(2),
                })
                .collect::<Vec<_>>();
            let rows = if let Some(element) = element {
                sqlx::query(ensemble_client_sql(true))
                    .bind(ensemble)
                    .bind(element)
                    .fetch_all(self.pool())
                    .await
            } else {
                sqlx::query(ensemble_client_sql(false))
                    .bind(ensemble)
                    .fetch_all(self.pool())
                    .await
            }
            .map_err(sqlx_error)?;
            records.extend(rows.into_iter().map(|row| EnsembleCacheRecord {
                client_host: row.get(0),
                path: row.get(1),
                info_hash: row.get(2),
            }));
            Ok(records)
        })
    }

    /// Load one client searchee row by client host and info hash.
    pub fn client_searchee_by_hash(
        &self,
        client_host: &str,
        info_hash: &str,
    ) -> crate::Result<Option<ClientSearcheeCacheRecord>> {
        self.block_on(async {
            let row = sqlx::query(
                "SELECT client_host, info_hash, name, title, files, save_path, category, tags, trackers
                 FROM client_searchee
                 WHERE client_host = ?1 AND info_hash = ?2",
            )
            .bind(client_host)
            .bind(info_hash)
            .fetch_optional(self.pool())
            .await
            .map_err(sqlx_error)?;
            Ok(row.map(client_searchee_record))
        })
    }

    /// Load a bounded rowid/path page from data rows.
    pub fn data_rowid_path_page(
        &self,
        after_rowid: i64,
        limit: i64,
    ) -> crate::Result<Vec<RowidPath>> {
        self.rowid_path_page(
            "SELECT rowid, path FROM data
             WHERE rowid > ?1
             ORDER BY rowid
             LIMIT ?2",
            after_rowid,
            limit,
        )
    }

    /// Load a bounded rowid/path page from data-dir ensemble rows.
    pub fn data_ensemble_rowid_path_page(
        &self,
        after_rowid: i64,
        limit: i64,
    ) -> crate::Result<Vec<RowidPath>> {
        self.rowid_path_page(
            "SELECT rowid, path FROM data_ensemble
             WHERE rowid > ?1
             ORDER BY rowid
             LIMIT ?2",
            after_rowid,
            limit,
        )
    }

    fn rowid_path_page(
        &self,
        sql: &str,
        after_rowid: i64,
        limit: i64,
    ) -> crate::Result<Vec<RowidPath>> {
        self.block_on(async {
            let rows = sqlx::query(sql)
                .bind(after_rowid)
                .bind(limit)
                .fetch_all(self.pool())
                .await
                .map_err(sqlx_error)?;
            Ok(rows
                .into_iter()
                .map(|row| RowidPath {
                    rowid: row.get(0),
                    path: row.get(1),
                })
                .collect())
        })
    }

    /// Delete one data row by rowid.
    pub fn delete_data_rowid(&self, rowid: i64) -> crate::Result<usize> {
        self.block_on(async {
            let result = sqlx::query("DELETE FROM data WHERE rowid = ?1")
                .bind(rowid)
                .execute(self.pool())
                .await
                .map_err(sqlx_error)?;
            rows_affected(result.rows_affected())
        })
    }

    /// Delete one data-dir ensemble row by rowid.
    pub fn delete_ensemble_rowid(&self, rowid: i64) -> crate::Result<usize> {
        self.block_on(async {
            let result = sqlx::query("DELETE FROM data_ensemble WHERE rowid = ?1")
                .bind(rowid)
                .execute(self.pool())
                .await
                .map_err(sqlx_error)?;
            rows_affected(result.rows_affected())
        })
    }

    /// Return whether a recent cached decision exists for an info hash.
    pub fn recent_decision_exists(
        &self,
        info_hash: &str,
        cutoff_millis: i64,
    ) -> crate::Result<bool> {
        self.block_on(async {
            sqlx::query_scalar(
                "SELECT EXISTS(
                    SELECT 1 FROM decision
                    WHERE info_hash = ?1 AND last_seen >= ?2
                )",
            )
            .bind(info_hash)
            .bind(cutoff_millis)
            .fetch_one(self.pool())
            .await
            .map_err(sqlx_error)
        })
    }

    /// Stream distinct decision info hashes in stable bounded pages.
    pub fn decision_info_hash_page(
        &self,
        after_info_hash: Option<&str>,
        limit: i64,
    ) -> crate::Result<Vec<String>> {
        self.block_on(async {
            let rows = sqlx::query(
                "SELECT DISTINCT info_hash
                 FROM decision
                 WHERE info_hash IS NOT NULL
                 AND (?1 IS NULL OR info_hash > ?1)
                 ORDER BY info_hash
                 LIMIT ?2",
            )
            .bind(after_info_hash)
            .bind(limit)
            .fetch_all(self.pool())
            .await
            .map_err(sqlx_error)?;
            Ok(rows.into_iter().map(|row| row.get(0)).collect())
        })
    }

    /// Delete cached decisions for one info hash.
    pub fn delete_decisions_by_info_hash(&self, info_hash: &str) -> crate::Result<usize> {
        self.block_on(async {
            let result = sqlx::query("DELETE FROM decision WHERE info_hash = ?1")
                .bind(info_hash)
                .execute(self.pool())
                .await
                .map_err(sqlx_error)?;
            rows_affected(result.rows_affected())
        })
    }

    /// Load indexer tracker names with caller-decoded tracker JSON.
    pub fn indexer_tracker_rows(&self) -> crate::Result<Vec<IndexerTrackerRecord>> {
        self.block_on(async {
            let rows = sqlx::query(
                "SELECT i.id, COALESCE(i.name, 'UnknownTracker'), t.tracker
                 FROM indexer_tracker t
                 JOIN indexer i ON i.id = t.indexer_id
                 ORDER BY i.id, t.tracker",
            )
            .fetch_all(self.pool())
            .await
            .map_err(sqlx_error)?;
            let mut output = Vec::new();
            let mut current: Option<(i64, String, Vec<String>)> = None;
            for row in rows {
                let id = row.get::<i64, _>(0);
                let row_name = row.get::<String, _>(1);
                let tracker = row.get::<String, _>(2);
                match &mut current {
                    Some((current_id, _, trackers)) if *current_id == id => {
                        trackers.push(tracker);
                    }
                    Some(_) => {
                        let (_, name, trackers) = current.take().expect("current row");
                        output.push(IndexerTrackerRecord {
                            name,
                            trackers: serde_json::to_string(&trackers).map_err(|error| {
                                persistence_message(format!(
                                    "failed to serialize indexer trackers JSON: {error}"
                                ))
                            })?,
                        });
                        current = Some((id, row_name, vec![tracker]));
                    }
                    None => current = Some((id, row_name, vec![tracker])),
                }
            }
            if let Some((_, name, trackers)) = current {
                output.push(IndexerTrackerRecord {
                    name,
                    trackers: serde_json::to_string(&trackers).map_err(|error| {
                        persistence_message(format!(
                            "failed to serialize indexer trackers JSON: {error}"
                        ))
                    })?,
                });
            }

            let rows = sqlx::query(
                "SELECT COALESCE(name, 'UnknownTracker'), trackers
                 FROM indexer
                 WHERE trackers IS NOT NULL
                 AND NOT EXISTS (
                    SELECT 1 FROM indexer_tracker
                    WHERE indexer_tracker.indexer_id = indexer.id
                 )",
            )
            .fetch_all(self.pool())
            .await
            .map_err(sqlx_error)?;
            output.extend(rows.into_iter().map(|row| IndexerTrackerRecord {
                name: row.get(0),
                trackers: row.get(1),
            }));
            Ok(output)
        })
    }

    /// Execute raw SQL with positional parameters.
    #[doc(hidden)]
    pub fn execute_sql(&self, sql: &str, params: &[SqlValue<'_>]) -> crate::Result<usize> {
        self.block_on(async {
            let result = bind_values(sqlx::query(sql), params)
                .execute(self.pool())
                .await
                .map_err(sqlx_error)?;
            rows_affected(result.rows_affected())
        })
    }

    /// Read one raw SQL row.
    #[doc(hidden)]
    pub fn query_row<T>(
        &self,
        sql: &str,
        params: &[SqlValue<'_>],
        map: impl FnOnce(SqliteRow) -> T + Send,
    ) -> crate::Result<T>
    where
        T: Send,
    {
        self.block_on(async {
            bind_values(sqlx::query(sql), params)
                .fetch_one(self.pool())
                .await
                .map(map)
                .map_err(sqlx_error)
        })
    }

    /// Read one scalar raw SQL value.
    #[doc(hidden)]
    pub fn query_scalar<T>(&self, sql: &str, params: &[SqlValue<'_>]) -> crate::Result<T>
    where
        T: for<'r> Decode<'r, Sqlite> + Type<Sqlite> + Send + Unpin,
    {
        self.block_on(async {
            bind_values(sqlx::query(sql), params)
                .fetch_one(self.pool())
                .await
                .map(|row| row.get(0))
                .map_err(sqlx_error)
        })
    }

    /// Database path under a state directory.
    pub fn path_for_app_dir(app_dir: &Path) -> PathBuf {
        app_dir.join(DATABASE_FILE_NAME)
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}

impl AsyncDatabase {
    /// Open `<state_dir>/sporos.db` through the async sqlx boundary.
    pub async fn open_app_dir(app_dir: &Path) -> crate::Result<Self> {
        Self::open(Database::path_for_app_dir(app_dir)).await
    }

    /// Open a database file and expose a sqlx SQLite pool.
    pub async fn open(path: impl AsRef<Path>) -> crate::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let options = SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .foreign_keys(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .map_err(sqlx_error)?;

        let database = Self { path, pool };
        if let Err(error) = database.initialize().await {
            database.pool.close().await;
            return Err(error);
        }
        Ok(database)
    }

    /// Database file path used by this pool.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Access the raw sqlx pool for async persistence queries.
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// Run pending sqlx schema migrations and set SQLite pragmas.
    pub async fn initialize(&self) -> crate::Result<()> {
        sqlx::raw_sql(PRAGMAS)
            .execute(self.pool())
            .await
            .map_err(sqlx_error)?;

        let current_version = self.schema_version().await?;
        if current_version > SCHEMA_VERSION {
            return Err(persistence_message(format!(
                "database schema version {current_version} is newer than supported version {SCHEMA_VERSION}"
            )));
        }
        for migration in MIGRATIONS {
            if migration.version > current_version {
                sqlx::raw_sql(migration.sql)
                    .execute(self.pool())
                    .await
                    .map_err(sqlx_error)?;
                self.set_schema_version(migration.version).await?;
            }
        }
        Ok(())
    }

    /// Close all pool connections.
    pub async fn close(self) {
        self.pool.close().await;
    }

    /// Read the generated API key from settings row `id = 0`.
    pub async fn get_api_key(&self) -> crate::Result<Option<String>> {
        sqlx::query_scalar("SELECT apikey FROM settings WHERE id = 0")
            .fetch_optional(self.pool())
            .await
            .map_err(sqlx_error)
    }

    /// Persist the generated API key in settings row `id = 0`.
    pub async fn set_api_key(&self, api_key: &str) -> crate::Result<()> {
        sqlx::query(
            "INSERT INTO settings (id, apikey)
             VALUES (0, ?1)
             ON CONFLICT(id) DO UPDATE SET apikey = excluded.apikey",
        )
        .bind(api_key)
        .execute(self.pool())
        .await
        .map(|_| ())
        .map_err(sqlx_error)
    }

    /// Delete decision rows that have no cached torrent info hash.
    pub async fn delete_null_decisions(&self) -> crate::Result<usize> {
        let result = sqlx::query("DELETE FROM decision WHERE info_hash IS NULL")
            .execute(self.pool())
            .await
            .map_err(sqlx_error)?;
        rows_affected(result.rows_affected())
    }

    /// Clear all search timestamp rows.
    pub async fn clear_timestamps(&self) -> crate::Result<usize> {
        let result = sqlx::query("DELETE FROM timestamp")
            .execute(self.pool())
            .await
            .map_err(sqlx_error)?;
        rows_affected(result.rows_affected())
    }

    /// Clear one known cache table.
    pub async fn clear_table(&self, table: CacheTable) -> crate::Result<usize> {
        let sql = match table {
            CacheTable::Torrent => "DELETE FROM torrent",
            CacheTable::ClientSearchee => "DELETE FROM client_searchee",
            CacheTable::Data => "DELETE FROM data",
            CacheTable::Ensemble => {
                let data = sqlx::query("DELETE FROM data_ensemble")
                    .execute(self.pool())
                    .await
                    .map_err(sqlx_error)?;
                let client = sqlx::query("DELETE FROM client_ensemble")
                    .execute(self.pool())
                    .await
                    .map_err(sqlx_error)?;
                return rows_affected(data.rows_affected().saturating_add(client.rows_affected()));
            }
        };
        let result = sqlx::query(sql)
            .execute(self.pool())
            .await
            .map_err(sqlx_error)?;
        rows_affected(result.rows_affected())
    }

    /// Clear persisted indexer failure status and retry timestamps.
    pub async fn clear_indexer_failures(&self) -> crate::Result<usize> {
        let result = sqlx::query("UPDATE indexer SET status = NULL, retry_after = NULL")
            .execute(self.pool())
            .await
            .map_err(sqlx_error)?;
        rows_affected(result.rows_affected())
    }

    /// Load indexer status rows for observability.
    pub async fn indexer_health_rows(&self) -> crate::Result<Vec<IndexerHealthRow>> {
        let rows = sqlx::query(
            "SELECT id, url, active, status, retry_after
             FROM indexer
             ORDER BY id",
        )
        .fetch_all(self.pool())
        .await
        .map_err(sqlx_error)?;
        Ok(rows
            .into_iter()
            .map(|row| IndexerHealthRow {
                id: row.get(0),
                url: row.get(1),
                active: row.get(2),
                status: row.get(3),
                retry_after: row.get(4),
            })
            .collect())
    }

    /// Record a safe remote endpoint breaker failure.
    pub async fn record_endpoint_breaker_failure(
        &self,
        failure: &EndpointBreakerFailure<'_>,
    ) -> crate::Result<EndpointBreakerRow> {
        let existing = sqlx::query(
            "SELECT failure_count
             FROM endpoint_breaker
             WHERE endpoint_key = ?1 AND operation = ?2",
        )
        .bind(failure.endpoint_key)
        .bind(failure.operation)
        .fetch_optional(self.pool())
        .await
        .map_err(sqlx_error)?;
        let failure_count = existing
            .as_ref()
            .map(|row| row.get::<i64, _>(0))
            .unwrap_or(0)
            .saturating_add(1);
        let should_open = failure_count >= 2 || failure.retry_after.is_some();
        let retry_after = if should_open {
            Some(
                failure
                    .retry_after
                    .unwrap_or_else(|| failure.now.saturating_add(60_000)),
            )
        } else {
            failure.retry_after
        };
        let opened_at = if should_open { Some(failure.now) } else { None };
        sqlx::query(
            "INSERT INTO endpoint_breaker
                (endpoint_key, operation, state, failure_count, opened_at,
                 retry_after, updated_at, last_error_class, last_error_message)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
             ON CONFLICT(endpoint_key, operation) DO UPDATE SET
                state = excluded.state,
                failure_count = excluded.failure_count,
                opened_at = COALESCE(endpoint_breaker.opened_at, excluded.opened_at),
                retry_after = excluded.retry_after,
                updated_at = excluded.updated_at,
                last_error_class = excluded.last_error_class,
                last_error_message = excluded.last_error_message",
        )
        .bind(failure.endpoint_key)
        .bind(failure.operation)
        .bind(if should_open { "open" } else { "closed" })
        .bind(failure_count)
        .bind(opened_at)
        .bind(retry_after)
        .bind(failure.now)
        .bind(failure.error_class)
        .bind(failure.error_message)
        .execute(self.pool())
        .await
        .map_err(sqlx_error)?;
        self.endpoint_breaker_row(failure.endpoint_key, failure.operation)
            .await?
            .ok_or_else(|| persistence_message("endpoint breaker disappeared after update"))
    }

    /// Close one endpoint breaker after a successful probe.
    pub async fn close_endpoint_breaker(
        &self,
        endpoint_key: &str,
        operation: &str,
        now: i64,
    ) -> crate::Result<()> {
        sqlx::query(
            "INSERT INTO endpoint_breaker
                (endpoint_key, operation, state, failure_count, opened_at,
                 retry_after, updated_at, last_error_class, last_error_message)
             VALUES (?1, ?2, 'closed', 0, NULL, NULL, ?3, NULL, NULL)
             ON CONFLICT(endpoint_key, operation) DO UPDATE SET
                state = 'closed',
                failure_count = 0,
                opened_at = NULL,
                retry_after = NULL,
                updated_at = excluded.updated_at,
                last_error_class = NULL,
                last_error_message = NULL",
        )
        .bind(endpoint_key)
        .bind(operation)
        .bind(now)
        .execute(self.pool())
        .await
        .map(|_| ())
        .map_err(sqlx_error)
    }

    /// Return the currently open breaker for an endpoint operation.
    pub async fn open_endpoint_breaker(
        &self,
        endpoint_key: &str,
        operation: &str,
        now: i64,
    ) -> crate::Result<Option<EndpointBreakerRow>> {
        let row = self.endpoint_breaker_row(endpoint_key, operation).await?;
        Ok(row.filter(|row| row.state == "open" && row.retry_after.is_some_and(|at| at > now)))
    }

    /// Load endpoint breaker aggregate state for observability.
    pub async fn endpoint_breaker_stats(&self, now: i64) -> crate::Result<EndpointBreakerStats> {
        let row = sqlx::query(
            "SELECT
                COALESCE(SUM(CASE WHEN state = 'open' AND retry_after > ?1 THEN 1 ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN state = 'open' AND (retry_after IS NULL OR retry_after <= ?1) THEN 1 ELSE 0 END), 0),
                MIN(CASE WHEN state = 'open' AND retry_after > ?1 THEN retry_after END)
             FROM endpoint_breaker",
        )
        .bind(now)
        .fetch_one(self.pool())
        .await
        .map_err(sqlx_error)?;
        let last_error = sqlx::query(
            "SELECT endpoint_key, operation, last_error_class, last_error_message
             FROM endpoint_breaker
             WHERE last_error_class IS NOT NULL OR last_error_message IS NOT NULL
             ORDER BY updated_at DESC, endpoint_key, operation
             LIMIT 1",
        )
        .fetch_optional(self.pool())
        .await
        .map_err(sqlx_error)?;
        Ok(EndpointBreakerStats {
            open: row.get(0),
            half_open: row.get(1),
            next_retry_at: row.get(2),
            last_endpoint_key: last_error.as_ref().map(|row| row.get(0)),
            last_operation: last_error.as_ref().map(|row| row.get(1)),
            last_error_class: last_error.as_ref().and_then(|row| row.get(2)),
            last_error_message: last_error.as_ref().and_then(|row| row.get(3)),
        })
    }

    async fn endpoint_breaker_row(
        &self,
        endpoint_key: &str,
        operation: &str,
    ) -> crate::Result<Option<EndpointBreakerRow>> {
        let row = sqlx::query(
            "SELECT endpoint_key, operation, state, failure_count, opened_at,
                    retry_after, updated_at, last_error_class, last_error_message
             FROM endpoint_breaker
             WHERE endpoint_key = ?1 AND operation = ?2",
        )
        .bind(endpoint_key)
        .bind(operation)
        .fetch_optional(self.pool())
        .await
        .map_err(sqlx_error)?;
        Ok(row.map(endpoint_breaker_row))
    }

    /// Read a scheduler job's last run timestamp.
    pub async fn read_last_run(&self, name: &str) -> crate::Result<Option<i64>> {
        sqlx::query_scalar("SELECT last_run FROM job_log WHERE name = ?1")
            .bind(name)
            .fetch_optional(self.pool())
            .await
            .map_err(sqlx_error)
    }

    /// Insert or update a scheduler job's last run timestamp.
    pub async fn write_last_run(&self, name: &str, last_run: i64) -> crate::Result<()> {
        sqlx::query(
            "INSERT INTO job_log (name, last_run)
             VALUES (?1, ?2)
             ON CONFLICT(name) DO UPDATE SET last_run = excluded.last_run",
        )
        .bind(name)
        .bind(last_run)
        .execute(self.pool())
        .await
        .map(|_| ())
        .map_err(sqlx_error)
    }

    /// Insert or dedupe an active durable announce work row.
    pub async fn insert_or_dedupe_announce_work(
        &self,
        record: &AnnounceWorkInsert<'_>,
    ) -> crate::Result<AnnounceWorkEnqueue> {
        self.insert_or_dedupe_announce_work_with_limit(record, None)
            .await?
            .ok_or_else(|| persistence_message("unbounded announce work insert was rejected"))
    }

    /// Insert or dedupe an active durable announce work row with an active queue bound.
    pub async fn insert_or_dedupe_announce_work_bounded(
        &self,
        record: &AnnounceWorkInsert<'_>,
        max_active: u32,
    ) -> crate::Result<Option<AnnounceWorkEnqueue>> {
        self.insert_or_dedupe_announce_work_with_limit(record, Some(max_active))
            .await
    }

    async fn insert_or_dedupe_announce_work_with_limit(
        &self,
        record: &AnnounceWorkInsert<'_>,
        max_active: Option<u32>,
    ) -> crate::Result<Option<AnnounceWorkEnqueue>> {
        if let Some(work) = self
            .active_announce_work_by_dedupe(record.dedupe_key)
            .await?
        {
            return Ok(Some(AnnounceWorkEnqueue {
                work,
                inserted: false,
            }));
        }

        let sql = if max_active.is_some() {
            "INSERT INTO announce_work
                (work_id, dedupe_key, name, guid, link, tracker, cookie, status,
                 attempts, created_at, updated_at, next_attempt_at, expires_at)
             SELECT ?1, ?2, ?3, ?4, ?5, ?6, ?7, 'queued',
                 0, ?8, ?8, ?8, ?9
             WHERE (
                 SELECT COUNT(*)
                 FROM announce_work
                 WHERE status IN ('queued', 'retrying', 'running')
             ) < ?10"
        } else {
            "INSERT INTO announce_work
                (work_id, dedupe_key, name, guid, link, tracker, cookie, status,
                 attempts, created_at, updated_at, next_attempt_at, expires_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'queued',
                 0, ?8, ?8, ?8, ?9)"
        };
        let mut query = sqlx::query(sql)
            .bind(record.work_id)
            .bind(record.dedupe_key)
            .bind(record.name)
            .bind(record.guid)
            .bind(record.link)
            .bind(record.tracker)
            .bind(record.cookie)
            .bind(record.now)
            .bind(record.expires_at);
        if let Some(max_active) = max_active {
            query = query.bind(i64::from(max_active));
        }
        let result = query.execute(self.pool()).await;

        match result {
            Ok(result) => {
                if result.rows_affected() == 0 {
                    return Ok(self
                        .active_announce_work_by_dedupe(record.dedupe_key)
                        .await?
                        .map(|work| AnnounceWorkEnqueue {
                            work,
                            inserted: false,
                        }));
                }
                Ok(Some(AnnounceWorkEnqueue {
                    work: self
                        .announce_work_by_id(record.work_id)
                        .await?
                        .ok_or_else(|| persistence_message("inserted announce work disappeared"))?,
                    inserted: true,
                }))
            }
            Err(error) => {
                if let Some(work) = self
                    .active_announce_work_by_dedupe(record.dedupe_key)
                    .await?
                {
                    Ok(Some(AnnounceWorkEnqueue {
                        work,
                        inserted: false,
                    }))
                } else {
                    Err(sqlx_error(error))
                }
            }
        }
    }

    /// Find active durable announce work by dedupe key.
    pub async fn active_announce_work_by_dedupe_key(
        &self,
        dedupe_key: &str,
    ) -> crate::Result<Option<AnnounceWorkRecord>> {
        self.active_announce_work_by_dedupe(dedupe_key).await
    }

    /// Claim ready durable announce work and mark it running.
    pub async fn claim_announce_work(
        &self,
        now: i64,
        lease_owner: &str,
        lease_timeout: i64,
        limit: u32,
    ) -> crate::Result<Vec<AnnounceWorkRecord>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(
            "UPDATE announce_work
             SET status = 'running',
                 attempts = attempts + 1,
                 updated_at = ?1,
                 lease_owner = ?2,
                 lease_expires_at = ?1 + ?3
             WHERE work_id IN (
                 SELECT work_id
                 FROM announce_work
                 WHERE status IN ('queued', 'retrying')
                   AND next_attempt_at <= ?1
                   AND expires_at > ?1
                 ORDER BY next_attempt_at, created_at, work_id
                 LIMIT ?4
             )
             RETURNING work_id, dedupe_key, name, guid, link, tracker, cookie,
                 status, attempts, created_at, updated_at, next_attempt_at,
                 expires_at, lease_owner, lease_expires_at, last_error_class,
                 last_error_message, last_outcome_context",
        )
        .bind(now)
        .bind(lease_owner)
        .bind(lease_timeout)
        .bind(i64::from(limit))
        .fetch_all(self.pool())
        .await
        .map_err(sqlx_error)?;
        Ok(rows.into_iter().map(announce_work_record).collect())
    }

    /// Schedule claimed announce work for another attempt.
    pub async fn schedule_announce_retry(
        &self,
        update: &AnnounceWorkRetry<'_>,
    ) -> crate::Result<bool> {
        let result = sqlx::query(
            "UPDATE announce_work
             SET status = 'retrying',
                 updated_at = ?2,
                 next_attempt_at = ?3,
                 lease_owner = NULL,
                 lease_expires_at = NULL,
                 last_error_class = ?4,
                 last_error_message = ?5,
                 last_outcome_context = ?6
             WHERE work_id = ?1
               AND status = 'running'
               AND lease_owner = ?7",
        )
        .bind(update.work_id)
        .bind(update.now)
        .bind(update.next_attempt_at)
        .bind(update.error_class)
        .bind(update.error_message)
        .bind(update.outcome_context)
        .bind(update.lease_owner)
        .execute(self.pool())
        .await
        .map_err(sqlx_error)?;
        Ok(result.rows_affected() > 0)
    }

    /// Mark announce work as succeeded, terminally failed, or expired.
    pub async fn finish_announce_work(
        &self,
        update: &AnnounceWorkFinish<'_>,
    ) -> crate::Result<bool> {
        let result = sqlx::query(
            "UPDATE announce_work
             SET status = ?3,
                 updated_at = ?2,
                 lease_owner = NULL,
                 lease_expires_at = NULL,
                 last_error_class = ?4,
                 last_error_message = ?5,
                 last_outcome_context = ?6
             WHERE work_id = ?1
               AND status = 'running'
               AND lease_owner = ?7",
        )
        .bind(update.work_id)
        .bind(update.now)
        .bind(update.status.as_str())
        .bind(update.error_class)
        .bind(update.error_message)
        .bind(update.outcome_context)
        .bind(update.lease_owner)
        .execute(self.pool())
        .await
        .map_err(sqlx_error)?;
        Ok(result.rows_affected() > 0)
    }

    /// Expire non-terminal announce work whose TTL has elapsed.
    pub async fn expire_announce_work(
        &self,
        now: i64,
        limit: u32,
    ) -> crate::Result<Vec<AnnounceWorkRecord>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(
            "UPDATE announce_work
             SET status = 'expired',
                 updated_at = ?1,
                 lease_owner = NULL,
                 lease_expires_at = NULL,
                 last_error_class = NULL,
                 last_error_message = NULL,
                 last_outcome_context = 'ttl_expired'
             WHERE work_id IN (
                 SELECT work_id
                 FROM announce_work
                 WHERE status IN ('queued', 'retrying', 'running')
                   AND expires_at <= ?1
                 ORDER BY expires_at, created_at, work_id
                 LIMIT ?2
             )
             RETURNING work_id, dedupe_key, name, guid, link, tracker, cookie,
                 status, attempts, created_at, updated_at, next_attempt_at,
                 expires_at, lease_owner, lease_expires_at, last_error_class,
                 last_error_message, last_outcome_context",
        )
        .bind(now)
        .bind(i64::from(limit))
        .fetch_all(self.pool())
        .await
        .map_err(sqlx_error)?;
        Ok(rows.into_iter().map(announce_work_record).collect())
    }

    /// Delete terminal announce work older than the retention window.
    pub async fn prune_terminal_announce_work(
        &self,
        now: i64,
        retention_millis: i64,
    ) -> crate::Result<usize> {
        if retention_millis <= 0 {
            return Ok(0);
        }
        let cutoff = now.saturating_sub(retention_millis);
        let result = sqlx::query(
            "DELETE FROM announce_work
             WHERE status IN ('succeeded', 'terminal_failed', 'expired')
               AND updated_at < ?1",
        )
        .bind(cutoff)
        .execute(self.pool())
        .await
        .map_err(sqlx_error)?;
        rows_affected(result.rows_affected())
    }

    /// Return abandoned running work to a retryable state after lease timeout.
    pub async fn release_stale_announce_leases(
        &self,
        now: i64,
        next_attempt_at: i64,
        limit: u32,
    ) -> crate::Result<Vec<AnnounceWorkRecord>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(
            "UPDATE announce_work
             SET status = 'retrying',
                 updated_at = ?1,
                 next_attempt_at = ?2,
                 lease_owner = NULL,
                 lease_expires_at = NULL,
                 last_error_class = 'lease_timeout',
                 last_error_message = 'announce work lease expired',
                 last_outcome_context = 'released_after_lease_timeout'
             WHERE work_id IN (
                 SELECT work_id
                 FROM announce_work
                 WHERE status = 'running'
                   AND lease_expires_at <= ?1
                   AND expires_at > ?1
                 ORDER BY lease_expires_at, created_at, work_id
                 LIMIT ?3
             )
             RETURNING work_id, dedupe_key, name, guid, link, tracker, cookie,
                 status, attempts, created_at, updated_at, next_attempt_at,
                 expires_at, lease_owner, lease_expires_at, last_error_class,
                 last_error_message, last_outcome_context",
        )
        .bind(now)
        .bind(next_attempt_at)
        .bind(i64::from(limit))
        .fetch_all(self.pool())
        .await
        .map_err(sqlx_error)?;
        Ok(rows.into_iter().map(announce_work_record).collect())
    }

    /// Load durable announce queue stats for status and metrics.
    pub async fn announce_queue_stats(&self, now: i64) -> crate::Result<AnnounceQueueStats> {
        let row = sqlx::query(
            "SELECT
                COALESCE(SUM(CASE WHEN status IN ('queued', 'retrying') THEN 1 ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN status = 'succeeded' THEN 1 ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN status = 'terminal_failed' THEN 1 ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN status = 'expired' THEN 1 ELSE 0 END), 0),
                COALESCE(SUM(attempts), 0),
                COALESCE(SUM(CASE WHEN status = 'retrying' THEN 1 ELSE 0 END), 0),
                MIN(CASE WHEN status IN ('queued', 'retrying') THEN created_at END),
                MIN(CASE WHEN status = 'retrying' AND next_attempt_at > ?1 THEN next_attempt_at END)
             FROM announce_work",
        )
        .bind(now)
        .fetch_one(self.pool())
        .await
        .map_err(sqlx_error)?;
        let last_error = sqlx::query(
            "SELECT last_error_class, last_error_message, last_outcome_context
             FROM announce_work
             WHERE last_error_class IS NOT NULL OR last_error_message IS NOT NULL
             ORDER BY updated_at DESC, work_id DESC
             LIMIT 1",
        )
        .fetch_optional(self.pool())
        .await
        .map_err(sqlx_error)?;

        Ok(AnnounceQueueStats {
            backlog: row.get(0),
            running: row.get(1),
            succeeded: row.get(2),
            terminal_failed: row.get(3),
            expired: row.get(4),
            total_attempts: row.get(5),
            retry_scheduled: row.get(6),
            oldest_queued_at: row.get(7),
            next_retry_at: row.get(8),
            last_error_class: last_error.as_ref().and_then(|row| row.get(0)),
            last_error_message: last_error.as_ref().and_then(|row| row.get(1)),
            last_outcome_context: last_error.as_ref().and_then(|row| row.get(2)),
        })
    }

    async fn announce_work_by_id(
        &self,
        work_id: &str,
    ) -> crate::Result<Option<AnnounceWorkRecord>> {
        let sql = announce_work_select_sql("work_id = ?1");
        let row = sqlx::query(&sql)
            .bind(work_id)
            .fetch_optional(self.pool())
            .await
            .map_err(sqlx_error)?;
        Ok(row.map(announce_work_record))
    }

    async fn active_announce_work_by_dedupe(
        &self,
        dedupe_key: &str,
    ) -> crate::Result<Option<AnnounceWorkRecord>> {
        let sql = announce_work_select_sql(
            "dedupe_key = ?1 AND status IN ('queued', 'retrying', 'running')",
        );
        let row = sqlx::query(&sql)
            .bind(dedupe_key)
            .fetch_optional(self.pool())
            .await
            .map_err(sqlx_error)?;
        Ok(row.map(announce_work_record))
    }

    async fn schema_version(&self) -> crate::Result<i64> {
        sqlx::query_scalar("PRAGMA user_version")
            .fetch_one(self.pool())
            .await
            .map_err(sqlx_error)
    }

    async fn set_schema_version(&self, version: i64) -> crate::Result<()> {
        sqlx::query(&format!("PRAGMA user_version = {version}"))
            .execute(self.pool())
            .await
            .map(|_| ())
            .map_err(sqlx_error)
    }
}

/// Cache tables that may be cleared from maintenance commands.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum CacheTable {
    /// Cached torrent-dir metafile rows.
    Torrent,
    /// Cached torrent-client inventory rows.
    ClientSearchee,
    /// Cached data-dir root rows.
    Data,
    /// Cached virtual ensemble lookup rows.
    Ensemble,
}

/// Decision cache row for insertion.
#[derive(Debug, Clone, Copy)]
pub struct DecisionRecord<'a> {
    /// `searchee.id`.
    pub searchee_id: i64,
    /// Torznab GUID.
    pub guid: &'a str,
    /// Cached torrent info hash when available.
    pub info_hash: Option<&'a str>,
    /// Candidate assessment decision.
    pub decision: Decision,
    /// First seen timestamp in ms.
    pub first_seen: i64,
    /// Last seen timestamp in ms.
    pub last_seen: i64,
    /// Fuzzy size factor used for the decision.
    pub fuzzy_size_factor: f64,
}

/// GUID to info-hash mapping loaded from the decision cache.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct GuidInfoHash {
    /// Decision row id for paging.
    pub id: i64,
    /// Candidate GUID.
    pub guid: String,
    /// Cached info hash.
    pub info_hash: String,
}

/// Data-dir cache row.
#[derive(Debug, Clone, Copy)]
pub struct DataRootRecord<'a> {
    /// Absolute data-dir root path.
    pub path: &'a str,
    /// Parsed title.
    pub title: &'a str,
    /// Scalar facts used by bounded reverse lookup selectors.
    pub lookup: Option<&'a LookupFields>,
}

/// Client searchee cache row.
#[derive(Debug, Clone, Copy)]
pub struct ClientSearcheeRecord<'a> {
    /// Stable configured client host.
    pub client_host: &'a str,
    /// Torrent info hash.
    pub info_hash: &'a str,
    /// Original client torrent name.
    pub name: &'a str,
    /// Parsed title.
    pub title: &'a str,
    /// File tree serialized to JSON.
    pub files: &'a [File<'a>],
    /// Total length.
    pub length: u64,
    /// Client save path.
    pub save_path: &'a str,
    /// Optional category.
    pub category: Option<&'a str>,
    /// Client tags serialized to JSON.
    pub tags: &'a [ClientLabel<'a>],
    /// Tracker hosts serialized to JSON.
    pub trackers: &'a [std::borrow::Cow<'a, str>],
    /// Scalar facts used by bounded reverse lookup selectors.
    pub lookup: Option<&'a LookupFields>,
}

/// Ensemble row used for virtual seasons and reverse lookup.
#[derive(Debug, Clone, Copy)]
pub struct EnsembleRecord<'a> {
    /// Client host for client inventory, absent for data-dir rows.
    pub client_host: Option<&'a str>,
    /// Absolute largest-file path.
    pub path: &'a str,
    /// Source info hash when available.
    pub info_hash: Option<&'a str>,
    /// Normalized season/anime key.
    pub ensemble: &'a str,
    /// Episode number/date/release element.
    pub element: &'a str,
}

/// Row counts returned by an indexer synchronization.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub struct IndexerSyncStats {
    /// Newly inserted indexers.
    pub inserted: usize,
    /// Re-enabled or updated indexers.
    pub updated: usize,
    /// Indexers deactivated because they are no longer configured.
    pub deactivated: usize,
}

/// Serialized Torznab caps fields ready for persistence.
#[derive(Debug, Clone, Copy)]
pub struct IndexerCapsRecord<'a> {
    /// Indexer row id.
    pub indexer_id: i64,
    /// Basic search support.
    pub search: bool,
    /// TV search support.
    pub tv_search: bool,
    /// Movie search support.
    pub movie_search: bool,
    /// Music search support.
    pub music_search: bool,
    /// Audio search support.
    pub audio_search: bool,
    /// Book search support.
    pub book_search: bool,
    /// Serialized TV id caps.
    pub tv_ids: &'a str,
    /// Serialized movie id caps.
    pub movie_ids: &'a str,
    /// Serialized category caps.
    pub categories: &'a str,
    /// Serialized limit caps.
    pub limits: &'a str,
}

/// Enabled indexer row.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct IndexerRow {
    /// Database row id.
    pub id: i64,
    /// Base Torznab URL.
    pub url: String,
    /// API key.
    pub apikey: String,
}

/// Indexer status row safe for metrics and health reporting.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct IndexerHealthRow {
    /// Database row id.
    pub id: i64,
    /// Base Torznab URL without API key.
    pub url: String,
    /// Whether the indexer is active in the current config.
    pub active: bool,
    /// Persisted indexer status such as `OK` or `RATE_LIMITED`.
    pub status: Option<String>,
    /// Retry timestamp in milliseconds since the Unix epoch.
    pub retry_after: Option<i64>,
}

/// Durable endpoint breaker failure update.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EndpointBreakerFailure<'a> {
    /// Bounded endpoint identity, such as an indexer URL or tracker label.
    pub endpoint_key: &'a str,
    /// Bounded operation class.
    pub operation: &'a str,
    /// Update timestamp in milliseconds since the Unix epoch.
    pub now: i64,
    /// Absolute retry timestamp in milliseconds since the Unix epoch.
    pub retry_after: Option<i64>,
    /// Bounded error class.
    pub error_class: &'a str,
    /// Redacted error message.
    pub error_message: Option<&'a str>,
}

/// Durable endpoint breaker row.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EndpointBreakerRow {
    /// Bounded endpoint identity.
    pub endpoint_key: String,
    /// Bounded operation class.
    pub operation: String,
    /// Breaker state.
    pub state: String,
    /// Consecutive failure count.
    pub failure_count: i64,
    /// Timestamp when the breaker opened.
    pub opened_at: Option<i64>,
    /// Timestamp when the next probe is allowed.
    pub retry_after: Option<i64>,
    /// Last update timestamp.
    pub updated_at: i64,
    /// Bounded error class.
    pub last_error_class: Option<String>,
    /// Redacted error message.
    pub last_error_message: Option<String>,
}

/// Durable endpoint breaker aggregate state.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EndpointBreakerStats {
    /// Open breakers still cooling down.
    pub open: i64,
    /// Open breakers whose cooldown has elapsed.
    pub half_open: i64,
    /// Nearest future retry timestamp.
    pub next_retry_at: Option<i64>,
    /// Last endpoint key with error context.
    pub last_endpoint_key: Option<String>,
    /// Last operation with error context.
    pub last_operation: Option<String>,
    /// Last bounded error class.
    pub last_error_class: Option<String>,
    /// Last redacted error message.
    pub last_error_message: Option<String>,
}

/// Enabled search indexer row with serialized caps.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SearchIndexerRow {
    /// Database row id.
    pub id: i64,
    /// Base Torznab URL.
    pub url: String,
    /// API key.
    pub apikey: String,
    /// Basic search support.
    pub search: bool,
    /// TV search support.
    pub tv_search: bool,
    /// Movie search support.
    pub movie_search: bool,
    /// Music search support.
    pub music_search: bool,
    /// Audio search support.
    pub audio_search: bool,
    /// Book search support.
    pub book_search: bool,
    /// Serialized TV id caps.
    pub tv_ids: String,
    /// Serialized movie id caps.
    pub movie_ids: String,
    /// Serialized category caps.
    pub categories: String,
    /// Serialized limit caps.
    pub limits: String,
}

/// Cached decision row loaded by searchee and GUID.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CachedDecisionRecord {
    /// Persisted decision string.
    pub decision: String,
    /// Persisted candidate info hash when available.
    pub info_hash: Option<String>,
}

/// Search timestamp row.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct TimestampRecord {
    /// First search timestamp.
    pub first_searched: u64,
    /// Last search timestamp.
    pub last_searched: u64,
}

/// Durable announce work row for insertion.
#[derive(Debug, Clone, Copy)]
pub struct AnnounceWorkInsert<'a> {
    /// Stable work id returned to callers.
    pub work_id: &'a str,
    /// Active-work dedupe key.
    pub dedupe_key: &'a str,
    /// Remote release name.
    pub name: &'a str,
    /// Candidate GUID URL.
    pub guid: &'a str,
    /// Candidate download URL.
    pub link: &'a str,
    /// Source tracker.
    pub tracker: &'a str,
    /// Optional request cookie.
    pub cookie: Option<&'a str>,
    /// Acceptance timestamp in milliseconds since the Unix epoch.
    pub now: i64,
    /// Work expiry timestamp in milliseconds since the Unix epoch.
    pub expires_at: i64,
}

/// Durable announce work row loaded from SQLite.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AnnounceWorkRecord {
    /// Stable work id returned to callers.
    pub work_id: String,
    /// Active-work dedupe key.
    pub dedupe_key: String,
    /// Remote release name.
    pub name: String,
    /// Candidate GUID URL.
    pub guid: String,
    /// Candidate download URL.
    pub link: String,
    /// Source tracker.
    pub tracker: String,
    /// Optional request cookie.
    pub cookie: Option<String>,
    /// Current queue status.
    pub status: String,
    /// Number of claimed processing attempts.
    pub attempts: i64,
    /// Acceptance timestamp in milliseconds since the Unix epoch.
    pub created_at: i64,
    /// Last state transition timestamp in milliseconds since the Unix epoch.
    pub updated_at: i64,
    /// Next timestamp when queued work may be claimed.
    pub next_attempt_at: i64,
    /// Work expiry timestamp in milliseconds since the Unix epoch.
    pub expires_at: i64,
    /// Current lease owner for running work.
    pub lease_owner: Option<String>,
    /// Current lease expiry timestamp for running work.
    pub lease_expires_at: Option<i64>,
    /// Last bounded error class.
    pub last_error_class: Option<String>,
    /// Last redacted error message.
    pub last_error_message: Option<String>,
    /// Last bounded outcome context.
    pub last_outcome_context: Option<String>,
}

/// Result of inserting or deduping an announce work row.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AnnounceWorkEnqueue {
    /// Persisted work row.
    pub work: AnnounceWorkRecord,
    /// Whether this call inserted the row instead of returning active work.
    pub inserted: bool,
}

/// Retry transition for running announce work.
#[derive(Debug, Clone, Copy)]
pub struct AnnounceWorkRetry<'a> {
    /// Stable work id.
    pub work_id: &'a str,
    /// Lease owner allowed to transition this running work.
    pub lease_owner: &'a str,
    /// Transition timestamp.
    pub now: i64,
    /// Next claim timestamp.
    pub next_attempt_at: i64,
    /// Bounded error class.
    pub error_class: Option<&'a str>,
    /// Redacted error message.
    pub error_message: Option<&'a str>,
    /// Bounded outcome context.
    pub outcome_context: Option<&'a str>,
}

/// Terminal announce work status.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum AnnounceWorkTerminalStatus {
    /// Work completed successfully.
    Succeeded,
    /// Work reached a non-retryable failure.
    TerminalFailed,
    /// Work expired before completion.
    Expired,
}

impl AnnounceWorkTerminalStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::TerminalFailed => "terminal_failed",
            Self::Expired => "expired",
        }
    }
}

/// Terminal transition for announce work.
#[derive(Debug, Clone, Copy)]
pub struct AnnounceWorkFinish<'a> {
    /// Stable work id.
    pub work_id: &'a str,
    /// Lease owner allowed to transition this running work.
    pub lease_owner: &'a str,
    /// Transition timestamp.
    pub now: i64,
    /// Terminal status.
    pub status: AnnounceWorkTerminalStatus,
    /// Bounded error class.
    pub error_class: Option<&'a str>,
    /// Redacted error message.
    pub error_message: Option<&'a str>,
    /// Bounded outcome context.
    pub outcome_context: Option<&'a str>,
}

/// Durable announce queue aggregate state.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AnnounceQueueStats {
    /// Work waiting to be claimed.
    pub backlog: i64,
    /// Work currently leased to a worker.
    pub running: i64,
    /// Succeeded work rows retained for observability.
    pub succeeded: i64,
    /// Terminally failed work rows retained for observability.
    pub terminal_failed: i64,
    /// Expired work rows retained for observability.
    pub expired: i64,
    /// Total claimed attempts currently visible in retained rows.
    pub total_attempts: i64,
    /// Retry rows waiting for their next attempt.
    pub retry_scheduled: i64,
    /// Created timestamp for the oldest queued or retrying row.
    pub oldest_queued_at: Option<i64>,
    /// Nearest retry timestamp in the future.
    pub next_retry_at: Option<i64>,
    /// Last bounded error class.
    pub last_error_class: Option<String>,
    /// Last redacted error message.
    pub last_error_message: Option<String>,
    /// Last bounded outcome context.
    pub last_outcome_context: Option<String>,
}

/// Client searchee cache row.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ClientSearcheeCacheRecord {
    /// Client host.
    pub client_host: String,
    /// Torrent info hash.
    pub info_hash: String,
    /// Torrent name.
    pub name: String,
    /// Parsed title.
    pub title: String,
    /// Serialized files JSON.
    pub files: String,
    /// Client save path.
    pub save_path: String,
    /// Optional category.
    pub category: Option<String>,
    /// Serialized tags JSON.
    pub tags: Option<String>,
    /// Serialized tracker JSON.
    pub trackers: String,
}

/// Candidate-derived filters for paged reverse lookup selectors.
#[derive(Debug, Clone, Copy)]
pub struct ReverseLookupCriteria<'a> {
    /// Normalized candidate title keys.
    pub search_keys: &'a [String],
    /// Candidate media type string.
    pub media_type: Option<&'a str>,
    /// Candidate season when present.
    pub season: Option<u32>,
    /// Candidate episode when present.
    pub episode: Option<u32>,
    /// Inclusive lower byte bound.
    pub min_length: Option<u64>,
    /// Inclusive upper byte bound.
    pub max_length: Option<u64>,
}

/// Compact client reverse lookup selector row.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ReverseLookupClientRecord {
    /// SQLite rowid used for stable paging.
    pub rowid: i64,
    /// Client host needed for hydration.
    pub client_host: String,
    /// Info hash needed for hydration.
    pub info_hash: String,
    /// Parsed title used by the fuzzy Rust matcher.
    pub title: String,
}

/// Compact data-dir reverse lookup selector row.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ReverseLookupDataRecord {
    /// SQLite rowid used for stable paging.
    pub rowid: i64,
    /// Data path needed for hydration.
    pub path: String,
    /// Parsed title used by the fuzzy Rust matcher.
    pub title: String,
}

/// Ensemble cache row.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EnsembleCacheRecord {
    /// Optional client host.
    pub client_host: Option<String>,
    /// Cached path.
    pub path: String,
    /// Optional torrent info hash.
    pub info_hash: Option<String>,
}

/// Rowid/path pair used by cleanup pages.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RowidPath {
    /// SQLite rowid.
    pub rowid: i64,
    /// Cached filesystem path.
    pub path: String,
}

/// Indexer name plus serialized tracker JSON.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct IndexerTrackerRecord {
    /// Display name.
    pub name: String,
    /// Serialized tracker JSON.
    pub trackers: String,
}

/// Raw SQL bind value used by focused test fixtures.
#[derive(Debug, Clone)]
#[doc(hidden)]
pub enum SqlValue<'a> {
    /// Signed integer value.
    I64(i64),
    /// Unsigned integer value stored as signed SQLite integer.
    U64(u64),
    /// Floating point value.
    F64(f64),
    /// Text value.
    Text(Cow<'a, str>),
    /// SQL null.
    Null,
}

#[derive(Serialize)]
struct FileJson<'a> {
    name: &'a str,
    path: &'a str,
    length: u64,
}

const SCHEMA: &str = r#"
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS searchee (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS decision (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    searchee_id INTEGER NOT NULL REFERENCES searchee(id) ON DELETE CASCADE,
    guid TEXT NOT NULL,
    info_hash TEXT NULL,
    decision TEXT NOT NULL CHECK (decision IN (
        'MATCH',
        'MATCH_SIZE_ONLY',
        'MATCH_PARTIAL',
        'FUZZY_SIZE_MISMATCH',
        'SIZE_MISMATCH',
        'PARTIAL_SIZE_MISMATCH',
        'NO_DOWNLOAD_LINK',
        'DOWNLOAD_FAILED',
        'MAGNET_LINK',
        'RATE_LIMITED',
        'SAME_INFO_HASH',
        'INFO_HASH_ALREADY_EXISTS',
        'FILE_TREE_MISMATCH',
        'RELEASE_GROUP_MISMATCH',
        'BLOCKED_RELEASE',
        'PROPER_REPACK_MISMATCH',
        'RESOLUTION_MISMATCH',
        'SOURCE_MISMATCH'
    )),
    first_seen INTEGER NOT NULL CHECK (first_seen >= 0),
    last_seen INTEGER NOT NULL CHECK (last_seen >= 0),
    fuzzy_size_factor REAL NOT NULL CHECK (fuzzy_size_factor >= 0),
    CHECK (last_seen >= first_seen),
    UNIQUE(searchee_id, guid)
);
CREATE INDEX IF NOT EXISTS idx_decision_info_hash_guid ON decision(info_hash, guid);
CREATE INDEX IF NOT EXISTS idx_decision_info_hash ON decision(info_hash);
CREATE INDEX IF NOT EXISTS idx_decision_guid ON decision(guid);

CREATE TABLE IF NOT EXISTS decision_guid_alias (
    alias TEXT NOT NULL,
    decision_id INTEGER NOT NULL REFERENCES decision(id) ON DELETE CASCADE,
    info_hash TEXT NOT NULL,
    last_seen INTEGER NOT NULL CHECK (last_seen >= 0),
    PRIMARY KEY(alias, decision_id)
);
CREATE INDEX IF NOT EXISTS idx_decision_guid_alias_lookup
ON decision_guid_alias(alias, last_seen DESC, decision_id DESC);

CREATE TABLE IF NOT EXISTS torrent (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    info_hash TEXT NOT NULL,
    name TEXT NOT NULL,
    file_path TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS job_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    last_run INTEGER NOT NULL CHECK (last_run >= 0)
);

CREATE TABLE IF NOT EXISTS indexer (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NULL,
    url TEXT NOT NULL UNIQUE,
    apikey TEXT NOT NULL,
    trackers TEXT NULL,
    active INTEGER NOT NULL CHECK (active IN (0, 1)),
    status TEXT NULL,
    retry_after INTEGER NULL CHECK (retry_after IS NULL OR retry_after >= 0),
    search_cap INTEGER NULL CHECK (search_cap IS NULL OR search_cap IN (0, 1)),
    tv_search_cap INTEGER NULL CHECK (tv_search_cap IS NULL OR tv_search_cap IN (0, 1)),
    movie_search_cap INTEGER NULL CHECK (movie_search_cap IS NULL OR movie_search_cap IN (0, 1)),
    music_search_cap INTEGER NULL CHECK (music_search_cap IS NULL OR music_search_cap IN (0, 1)),
    audio_search_cap INTEGER NULL CHECK (audio_search_cap IS NULL OR audio_search_cap IN (0, 1)),
    book_search_cap INTEGER NULL CHECK (book_search_cap IS NULL OR book_search_cap IN (0, 1)),
    tv_id_caps TEXT NULL,
    movie_id_caps TEXT NULL,
    cat_caps TEXT NULL,
    limits_caps TEXT NULL
);

CREATE TABLE IF NOT EXISTS indexer_tracker (
    indexer_id INTEGER NOT NULL REFERENCES indexer(id) ON DELETE CASCADE,
    tracker TEXT NOT NULL,
    PRIMARY KEY(indexer_id, tracker)
);
CREATE INDEX IF NOT EXISTS idx_indexer_tracker_lookup
ON indexer_tracker(tracker, indexer_id);

CREATE TABLE IF NOT EXISTS endpoint_breaker (
    endpoint_key TEXT NOT NULL CHECK (length(endpoint_key) > 0 AND length(endpoint_key) <= 256),
    operation TEXT NOT NULL CHECK (length(operation) > 0 AND length(operation) <= 64),
    state TEXT NOT NULL CHECK (state IN ('closed', 'open')),
    failure_count INTEGER NOT NULL CHECK (failure_count >= 0),
    opened_at INTEGER NULL CHECK (opened_at IS NULL OR opened_at >= 0),
    retry_after INTEGER NULL CHECK (retry_after IS NULL OR retry_after >= 0),
    updated_at INTEGER NOT NULL CHECK (updated_at >= 0),
    last_error_class TEXT NULL CHECK (last_error_class IS NULL OR length(last_error_class) <= 64),
    last_error_message TEXT NULL CHECK (last_error_message IS NULL OR length(last_error_message) <= 512),
    PRIMARY KEY(endpoint_key, operation)
);
CREATE INDEX IF NOT EXISTS idx_endpoint_breaker_open
ON endpoint_breaker(state, retry_after, endpoint_key, operation);

CREATE TABLE IF NOT EXISTS timestamp (
    searchee_id INTEGER NOT NULL REFERENCES searchee(id) ON DELETE CASCADE,
    indexer_id INTEGER NOT NULL REFERENCES indexer(id) ON DELETE CASCADE,
    first_searched INTEGER NOT NULL CHECK (first_searched >= 0),
    last_searched INTEGER NOT NULL CHECK (last_searched >= 0),
    CHECK (last_searched >= first_searched),
    PRIMARY KEY(searchee_id, indexer_id)
);

CREATE TABLE IF NOT EXISTS settings (
    id INTEGER PRIMARY KEY CHECK (id = 0),
    apikey TEXT NULL
);

CREATE TABLE IF NOT EXISTS rss (
    indexer_id INTEGER PRIMARY KEY REFERENCES indexer(id) ON DELETE CASCADE,
    last_seen_guid TEXT NULL
);

CREATE TABLE IF NOT EXISTS announce_work (
    work_id TEXT PRIMARY KEY,
    dedupe_key TEXT NOT NULL,
    name TEXT NOT NULL,
    guid TEXT NOT NULL,
    link TEXT NOT NULL,
    tracker TEXT NOT NULL,
    cookie TEXT NULL,
    status TEXT NOT NULL CHECK (status IN (
        'queued',
        'running',
        'retrying',
        'succeeded',
        'terminal_failed',
        'expired'
    )),
    attempts INTEGER NOT NULL CHECK (attempts >= 0),
    created_at INTEGER NOT NULL CHECK (created_at >= 0),
    updated_at INTEGER NOT NULL CHECK (updated_at >= 0),
    next_attempt_at INTEGER NOT NULL CHECK (next_attempt_at >= 0),
    expires_at INTEGER NOT NULL CHECK (expires_at >= 0),
    lease_owner TEXT NULL,
    lease_expires_at INTEGER NULL CHECK (lease_expires_at IS NULL OR lease_expires_at >= 0),
    last_error_class TEXT NULL,
    last_error_message TEXT NULL,
    last_outcome_context TEXT NULL,
    CHECK (updated_at >= created_at),
    CHECK (expires_at >= created_at),
    CHECK (
        (status = 'running' AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
        OR (status != 'running' AND lease_owner IS NULL AND lease_expires_at IS NULL)
    )
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_announce_work_active_dedupe
ON announce_work(dedupe_key)
WHERE status IN ('queued', 'retrying', 'running');
CREATE INDEX IF NOT EXISTS idx_announce_work_ready
ON announce_work(status, next_attempt_at, expires_at, created_at, work_id)
WHERE status IN ('queued', 'retrying');
CREATE INDEX IF NOT EXISTS idx_announce_work_running_lease
ON announce_work(status, lease_expires_at, created_at, work_id)
WHERE status = 'running';
CREATE INDEX IF NOT EXISTS idx_announce_work_expiry
ON announce_work(status, expires_at, created_at, work_id);
CREATE INDEX IF NOT EXISTS idx_announce_work_terminal_retention
ON announce_work(status, updated_at, work_id)
WHERE status IN ('succeeded', 'terminal_failed', 'expired');
CREATE INDEX IF NOT EXISTS idx_announce_work_status
ON announce_work(status);

CREATE TABLE IF NOT EXISTS current_refresh_runs (
    scope TEXT NOT NULL,
    refresh_id TEXT PRIMARY KEY,
    overlapped INTEGER NOT NULL CHECK(overlapped IN (0, 1))
);

CREATE TABLE IF NOT EXISTS current_data_roots (
    refresh_id TEXT NOT NULL,
    path TEXT NOT NULL,
    PRIMARY KEY(refresh_id, path)
);

CREATE TABLE IF NOT EXISTS current_client_info_hashes (
    refresh_id TEXT NOT NULL,
    info_hash TEXT NOT NULL,
    PRIMARY KEY(refresh_id, info_hash)
);

CREATE TABLE IF NOT EXISTS current_client_ensemble_paths (
    refresh_id TEXT NOT NULL,
    path TEXT NOT NULL,
    PRIMARY KEY(refresh_id, path)
);

CREATE TABLE IF NOT EXISTS current_indexer_urls (
    refresh_id TEXT NOT NULL,
    url TEXT NOT NULL,
    PRIMARY KEY(refresh_id, url)
);

CREATE TABLE IF NOT EXISTS current_torrent_dir (
    refresh_id TEXT NOT NULL,
    file_path TEXT NOT NULL,
    PRIMARY KEY(refresh_id, file_path)
);

CREATE TABLE IF NOT EXISTS client_searchee (
    client_host TEXT NOT NULL,
    info_hash TEXT NOT NULL,
    name TEXT NOT NULL,
    title TEXT NOT NULL,
    files TEXT NOT NULL,
    length INTEGER NOT NULL CHECK (length >= 0),
    save_path TEXT NOT NULL,
    category TEXT NULL,
    tags TEXT NULL,
    trackers TEXT NOT NULL,
    search_key TEXT NULL,
    media_type TEXT NULL CHECK (media_type IS NULL OR media_type IN (
        'episode',
        'pack',
        'movie',
        'anime',
        'video',
        'audio',
        'book',
        'unknown'
    )),
    season INTEGER NULL CHECK (season IS NULL OR season >= 0),
    episode INTEGER NULL CHECK (episode IS NULL OR episode >= 0),
    file_count INTEGER NULL CHECK (file_count IS NULL OR file_count >= 0),
    video_bytes INTEGER NULL CHECK (video_bytes IS NULL OR video_bytes >= 0),
    non_video_bytes INTEGER NULL CHECK (non_video_bytes IS NULL OR non_video_bytes >= 0),
    PRIMARY KEY(client_host, info_hash)
);
CREATE INDEX IF NOT EXISTS idx_client_searchee_info_hash ON client_searchee(info_hash);
CREATE INDEX IF NOT EXISTS idx_client_searchee_lookup
ON client_searchee(search_key, media_type, season, episode, length);

CREATE TABLE IF NOT EXISTS data (
    path TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    search_key TEXT NULL,
    media_type TEXT NULL CHECK (media_type IS NULL OR media_type IN (
        'episode',
        'pack',
        'movie',
        'anime',
        'video',
        'audio',
        'book',
        'unknown'
    )),
    season INTEGER NULL CHECK (season IS NULL OR season >= 0),
    episode INTEGER NULL CHECK (episode IS NULL OR episode >= 0),
    length INTEGER NULL CHECK (length IS NULL OR length >= 0),
    file_count INTEGER NULL CHECK (file_count IS NULL OR file_count >= 0),
    video_bytes INTEGER NULL CHECK (video_bytes IS NULL OR video_bytes >= 0),
    non_video_bytes INTEGER NULL CHECK (non_video_bytes IS NULL OR non_video_bytes >= 0)
);
CREATE INDEX IF NOT EXISTS idx_data_lookup
ON data(search_key, media_type, season, episode, length);

CREATE TABLE IF NOT EXISTS data_ensemble (
    data_root TEXT NOT NULL REFERENCES data(path) ON DELETE CASCADE,
    path TEXT NOT NULL,
    info_hash TEXT NULL,
    ensemble TEXT NOT NULL,
    element TEXT NOT NULL,
    PRIMARY KEY(path)
);
CREATE INDEX IF NOT EXISTS idx_data_ensemble_root ON data_ensemble(data_root);
CREATE INDEX IF NOT EXISTS idx_data_ensemble_info_hash ON data_ensemble(info_hash);
CREATE INDEX IF NOT EXISTS idx_data_ensemble_lookup ON data_ensemble(ensemble, element);

CREATE TABLE IF NOT EXISTS client_ensemble (
    client_host TEXT NOT NULL,
    path TEXT NOT NULL,
    info_hash TEXT NOT NULL,
    ensemble TEXT NOT NULL,
    element TEXT NOT NULL,
    PRIMARY KEY(client_host, path),
    FOREIGN KEY(client_host, info_hash)
        REFERENCES client_searchee(client_host, info_hash)
        ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_client_ensemble_path ON client_ensemble(path);
CREATE INDEX IF NOT EXISTS idx_client_ensemble_info_hash ON client_ensemble(info_hash);
CREATE INDEX IF NOT EXISTS idx_client_ensemble_lookup ON client_ensemble(ensemble, element);
"#;

fn block_on_runtime<F>(runtime: &Runtime, future: F) -> F::Output
where
    F: Future + Send,
    F::Output: Send,
{
    match Handle::try_current().map(|handle| handle.runtime_flavor()) {
        Ok(RuntimeFlavor::MultiThread) => tokio::task::block_in_place(|| runtime.block_on(future)),
        Ok(_) => std::thread::scope(|scope| {
            scope
                .spawn(|| runtime.block_on(future))
                .join()
                .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
        }),
        Err(_) => runtime.block_on(future),
    }
}

fn bind_values<'q>(
    mut query: Query<'q, Sqlite, SqliteArguments<'q>>,
    params: &'q [SqlValue<'q>],
) -> Query<'q, Sqlite, SqliteArguments<'q>> {
    for param in params {
        query = match param {
            SqlValue::I64(value) => query.bind(*value),
            SqlValue::U64(value) => query.bind(i64::try_from(*value).unwrap_or(i64::MAX)),
            SqlValue::F64(value) => query.bind(*value),
            SqlValue::Text(value) => query.bind(value.as_ref()),
            SqlValue::Null => query.bind(Option::<i64>::None),
        };
    }
    query
}

fn placeholders(count: usize) -> String {
    std::iter::repeat_n("?", count)
        .collect::<Vec<_>>()
        .join(", ")
}

fn ensemble_data_sql(has_element: bool) -> &'static str {
    if has_element {
        "SELECT NULL AS client_host, path, info_hash
         FROM data_ensemble
         WHERE ensemble = ?1 AND element = ?2"
    } else {
        "SELECT NULL AS client_host, path, info_hash
         FROM data_ensemble
         WHERE ensemble = ?1"
    }
}

fn ensemble_client_sql(has_element: bool) -> &'static str {
    if has_element {
        "SELECT client_host, path, info_hash
         FROM client_ensemble
         WHERE ensemble = ?1 AND element = ?2"
    } else {
        "SELECT client_host, path, info_hash
         FROM client_ensemble
         WHERE ensemble = ?1"
    }
}

fn decision_guid_alias_lookup_sql() -> &'static str {
    "SELECT info_hash
     FROM decision_guid_alias
     WHERE alias = ?1
     ORDER BY last_seen DESC, decision_id DESC
     LIMIT 1"
}

fn decision_guid_alias(value: &str) -> Option<String> {
    let (_, suffix) = value.split_once("/torrent/")?;
    let id = suffix.split('/').next()?;
    (!id.is_empty()).then(|| decision_guid_alias_for_torrent_id(id))
}

fn decision_guid_alias_for_torrent_id(id: &str) -> String {
    format!("torrent:{id}")
}

fn reverse_lookup_client_sql(search_key_count: usize) -> String {
    let key_placeholders = placeholders(search_key_count);
    format!(
        "SELECT rowid, client_host, info_hash, title
         FROM (
            SELECT rowid, client_host, info_hash, title
            FROM client_searchee INDEXED BY idx_client_searchee_lookup
            WHERE rowid > ?
            AND search_key IN ({key_placeholders})
            AND (? IS NULL OR media_type IS NULL OR media_type = ?)
            AND (? IS NULL OR season IS NULL OR season = ?)
            AND (? IS NULL OR episode IS NULL OR episode = ?)
            AND (? IS NULL OR length >= ?)
            AND (? IS NULL OR length <= ?)
            UNION ALL
            SELECT rowid, client_host, info_hash, title
            FROM client_searchee
            WHERE rowid > ?
            AND search_key IS NULL
         )
         ORDER BY rowid
         LIMIT ?"
    )
}

fn reverse_lookup_data_sql(search_key_count: usize) -> String {
    let key_placeholders = placeholders(search_key_count);
    format!(
        "SELECT rowid, path, title
         FROM (
            SELECT rowid, path, title
            FROM data INDEXED BY idx_data_lookup
            WHERE rowid > ?
            AND search_key IN ({key_placeholders})
            AND (? IS NULL OR media_type IS NULL OR media_type = ?)
            AND (? IS NULL OR season IS NULL OR season = ?)
            AND (? IS NULL OR episode IS NULL OR episode = ?)
            AND (? IS NULL OR length IS NULL OR length >= ?)
            AND (? IS NULL OR length IS NULL OR length <= ?)
            UNION ALL
            SELECT rowid, path, title
            FROM data
            WHERE rowid > ?
            AND search_key IS NULL
         )
         ORDER BY rowid
         LIMIT ?"
    )
}

fn reverse_lookup_params<'a>(
    criteria: &'a ReverseLookupCriteria<'a>,
    after_rowid: i64,
    limit: i64,
) -> Vec<SqlValue<'a>> {
    let mut params = Vec::with_capacity(criteria.search_keys.len().saturating_add(13));
    params.push(SqlValue::I64(after_rowid));
    params.extend(
        criteria
            .search_keys
            .iter()
            .map(|key| SqlValue::Text(Cow::Borrowed(key.as_str()))),
    );
    push_text_pair(&mut params, criteria.media_type);
    push_i64_pair(&mut params, criteria.season.map(i64::from));
    push_i64_pair(&mut params, criteria.episode.map(i64::from));
    push_i64_pair(
        &mut params,
        criteria
            .min_length
            .map(|length| i64::try_from(length).unwrap_or(i64::MAX)),
    );
    push_i64_pair(
        &mut params,
        criteria
            .max_length
            .map(|length| i64::try_from(length).unwrap_or(i64::MAX)),
    );
    params.push(SqlValue::I64(after_rowid));
    params.push(SqlValue::I64(limit));
    params
}

fn push_text_pair<'a>(params: &mut Vec<SqlValue<'a>>, value: Option<&'a str>) {
    match value {
        Some(value) => {
            params.push(SqlValue::Text(Cow::Borrowed(value)));
            params.push(SqlValue::Text(Cow::Borrowed(value)));
        }
        None => {
            params.push(SqlValue::Null);
            params.push(SqlValue::Null);
        }
    }
}

fn push_i64_pair<'a>(params: &mut Vec<SqlValue<'a>>, value: Option<i64>) {
    match value {
        Some(value) => {
            params.push(SqlValue::I64(value));
            params.push(SqlValue::I64(value));
        }
        None => {
            params.push(SqlValue::Null);
            params.push(SqlValue::Null);
        }
    }
}

fn client_searchee_record(row: SqliteRow) -> ClientSearcheeCacheRecord {
    ClientSearcheeCacheRecord {
        client_host: row.get(0),
        info_hash: row.get(1),
        name: row.get(2),
        title: row.get(3),
        files: row.get(4),
        save_path: row.get(5),
        category: row.get(6),
        tags: row.get(7),
        trackers: row.get(8),
    }
}

fn announce_work_select_sql(predicate: &str) -> String {
    format!(
        "SELECT work_id, dedupe_key, name, guid, link, tracker, cookie,
            status, attempts, created_at, updated_at, next_attempt_at,
            expires_at, lease_owner, lease_expires_at, last_error_class,
            last_error_message, last_outcome_context
         FROM announce_work
         WHERE {predicate}
         ORDER BY created_at, work_id
         LIMIT 1"
    )
}

fn announce_work_record(row: SqliteRow) -> AnnounceWorkRecord {
    AnnounceWorkRecord {
        work_id: row.get(0),
        dedupe_key: row.get(1),
        name: row.get(2),
        guid: row.get(3),
        link: row.get(4),
        tracker: row.get(5),
        cookie: row.get(6),
        status: row.get(7),
        attempts: row.get(8),
        created_at: row.get(9),
        updated_at: row.get(10),
        next_attempt_at: row.get(11),
        expires_at: row.get(12),
        lease_owner: row.get(13),
        lease_expires_at: row.get(14),
        last_error_class: row.get(15),
        last_error_message: row.get(16),
        last_outcome_context: row.get(17),
    }
}

fn endpoint_breaker_row(row: SqliteRow) -> EndpointBreakerRow {
    EndpointBreakerRow {
        endpoint_key: row.get(0),
        operation: row.get(1),
        state: row.get(2),
        failure_count: row.get(3),
        opened_at: row.get(4),
        retry_after: row.get(5),
        updated_at: row.get(6),
        last_error_class: row.get(7),
        last_error_message: row.get(8),
    }
}

async fn begin_refresh_run(pool: &SqlitePool, scope: &str) -> crate::Result<String> {
    let mut transaction = pool.begin().await.map_err(sqlx_error)?;
    let refresh_id: String = sqlx::query_scalar("SELECT lower(hex(randomblob(16)))")
        .fetch_one(&mut *transaction)
        .await
        .map_err(sqlx_error)?;
    let active_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM current_refresh_runs WHERE scope = ?1")
            .bind(scope)
            .fetch_one(&mut *transaction)
            .await
            .map_err(sqlx_error)?;
    if active_count > 0 {
        sqlx::query("UPDATE current_refresh_runs SET overlapped = 1 WHERE scope = ?1")
            .bind(scope)
            .execute(&mut *transaction)
            .await
            .map_err(sqlx_error)?;
    }
    sqlx::query(
        "INSERT INTO current_refresh_runs (scope, refresh_id, overlapped)
         VALUES (?1, ?2, ?3)",
    )
    .bind(scope)
    .bind(&refresh_id)
    .bind(if active_count > 0 { 1_i64 } else { 0_i64 })
    .execute(&mut *transaction)
    .await
    .map_err(sqlx_error)?;
    transaction.commit().await.map_err(sqlx_error)?;
    Ok(refresh_id)
}

async fn finish_refresh_run(
    transaction: &mut sqlx::Transaction<'_, Sqlite>,
    scope: &str,
    refresh_id: &str,
) -> crate::Result<bool> {
    let overlapped = sqlx::query_scalar::<_, i64>(
        "SELECT overlapped FROM current_refresh_runs WHERE scope = ?1 AND refresh_id = ?2",
    )
    .bind(scope)
    .bind(refresh_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(sqlx_error)?
    .unwrap_or(1);
    sqlx::query("DELETE FROM current_refresh_runs WHERE scope = ?1 AND refresh_id = ?2")
        .bind(scope)
        .bind(refresh_id)
        .execute(&mut **transaction)
        .await
        .map_err(sqlx_error)?;
    Ok(overlapped != 0)
}

fn sqlx_error(error: sqlx::Error) -> SporosError {
    SporosError::Persistence {
        message: Cow::Owned(error.to_string()),
    }
}

fn rows_affected(rows: u64) -> crate::Result<usize> {
    usize::try_from(rows)
        .map_err(|error| persistence_message(format!("row count exceeds usize: {error}")))
}

fn persistence_message(message: impl Into<Cow<'static, str>>) -> SporosError {
    SporosError::Persistence {
        message: message.into(),
    }
}

fn files_json(files: &[File<'_>]) -> crate::Result<String> {
    let files = files
        .iter()
        .map(|file| FileJson {
            name: file.name.as_ref(),
            path: file.path.as_ref(),
            length: file.length,
        })
        .collect::<Vec<_>>();
    serde_json::to_string(&files)
        .map_err(|error| persistence_message(format!("failed to serialize files JSON: {error}")))
}

fn labels_json(labels: &[ClientLabel<'_>]) -> crate::Result<String> {
    let labels = labels.iter().map(ClientLabel::as_str).collect::<Vec<_>>();
    serde_json::to_string(&labels)
        .map_err(|error| persistence_message(format!("failed to serialize labels JSON: {error}")))
}

fn strings_json(values: &[std::borrow::Cow<'_, str>]) -> crate::Result<String> {
    let values = values
        .iter()
        .map(|value| value.as_ref())
        .collect::<Vec<_>>();
    serde_json::to_string(&values)
        .map_err(|error| persistence_message(format!("failed to serialize strings JSON: {error}")))
}

#[cfg(test)]
mod tests;
