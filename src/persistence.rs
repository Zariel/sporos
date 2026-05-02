//! SQLite state, migrations, cache records, and paged persistence helpers.

use std::{
    borrow::Cow,
    path::{Path, PathBuf},
};

use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;

use crate::{
    SporosError,
    domain::{ClientLabel, Decision, File},
};

const DATABASE_FILE_NAME: &str = "cross-seed.db";
const CURRENT_SCHEMA_VERSION: i64 = 2;
const PRAGMAS: &str = "PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON;";

#[derive(Debug, Clone, Copy)]
struct Migration {
    version: i64,
    sql: &'static str,
}

const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        sql: SCHEMA,
    },
    Migration {
        version: CURRENT_SCHEMA_VERSION,
        sql: ENSEMBLE_UNIQUE_KEY_MIGRATION,
    },
];

/// SQLite database handle with compatibility schema helpers.
pub struct Database {
    connection: Connection,
}

impl Database {
    /// Open `<appDir>/cross-seed.db`, enable WAL, and create the current
    /// unreleased schema directly.
    pub fn open_app_dir(app_dir: &Path) -> crate::Result<Self> {
        Self::open(app_dir.join(DATABASE_FILE_NAME))
    }

    /// Open a database file, enable WAL, and create the current unreleased schema.
    pub fn open(path: impl AsRef<Path>) -> crate::Result<Self> {
        let connection = Connection::open(path).map_err(sql_error)?;
        let database = Self { connection };
        database.initialize()?;
        Ok(database)
    }

    /// Access the raw connection for integration-specific queries.
    pub fn connection(&self) -> &Connection {
        &self.connection
    }

    /// Run pending Rust schema migrations and set SQLite pragmas.
    pub fn initialize(&self) -> crate::Result<()> {
        self.connection.execute_batch(PRAGMAS).map_err(sql_error)?;
        let current_version = self.schema_version()?;
        if current_version > CURRENT_SCHEMA_VERSION {
            return Err(persistence_message(format!(
                "database schema version {current_version} is newer than supported version {CURRENT_SCHEMA_VERSION}"
            )));
        }
        for migration in MIGRATIONS {
            if migration.version > current_version {
                self.connection
                    .execute_batch(migration.sql)
                    .map_err(sql_error)?;
                self.set_schema_version(migration.version)?;
            }
        }
        Ok(())
    }

    fn schema_version(&self) -> crate::Result<i64> {
        self.connection
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .map_err(sql_error)
    }

    fn set_schema_version(&self, version: i64) -> crate::Result<()> {
        self.connection
            .execute_batch(&format!("PRAGMA user_version = {version}"))
            .map_err(sql_error)
    }

    /// Insert a searchee name if needed and return its stable id.
    pub fn get_or_insert_searchee(&self, name: &str, now_millis: i64) -> crate::Result<i64> {
        self.connection
            .execute(
                "INSERT INTO searchee (name, first_searched, last_searched)
                 VALUES (?1, ?2, ?2)
                 ON CONFLICT(name) DO NOTHING",
                params![name, now_millis],
            )
            .map_err(sql_error)?;
        self.connection
            .query_row(
                "SELECT id FROM searchee WHERE name = ?1",
                params![name],
                |row| row.get(0),
            )
            .map_err(sql_error)
    }

    /// Insert or update a candidate decision row.
    pub fn upsert_decision(&self, record: &DecisionRecord<'_>) -> crate::Result<()> {
        self.connection
            .execute(
                "INSERT INTO decision
                    (searchee_id, guid, info_hash, decision, first_seen, last_seen, fuzzy_size_factor)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(searchee_id, guid) DO UPDATE SET
                    info_hash = excluded.info_hash,
                    decision = excluded.decision,
                    last_seen = excluded.last_seen,
                    fuzzy_size_factor = excluded.fuzzy_size_factor",
                params![
                    record.searchee_id,
                    record.guid,
                    record.info_hash,
                    record.decision.as_str(),
                    record.first_seen,
                    record.last_seen,
                    record.fuzzy_size_factor,
                ],
            )
            .map_err(sql_error)?;
        Ok(())
    }

    /// Stream non-null GUID to info-hash mappings in bounded pages.
    pub fn guid_info_hash_page(
        &self,
        after_id: i64,
        limit: u32,
    ) -> crate::Result<Vec<GuidInfoHash>> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT id, guid, info_hash
                 FROM decision
                 WHERE id > ?1 AND info_hash IS NOT NULL
                 ORDER BY id
                 LIMIT ?2",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(params![after_id, limit], |row| {
                Ok(GuidInfoHash {
                    id: row.get(0)?,
                    guid: row.get(1)?,
                    info_hash: row.get(2)?,
                })
            })
            .map_err(sql_error)?;

        collect_rows(rows)
    }

    /// Read the generated API key from settings row `id = 0`.
    pub fn get_api_key(&self) -> crate::Result<Option<String>> {
        self.connection
            .query_row("SELECT apikey FROM settings WHERE id = 0", [], |row| {
                row.get(0)
            })
            .optional()
            .map_err(sql_error)
    }

    /// Persist the generated API key in settings row `id = 0`.
    pub fn set_api_key(&self, api_key: &str) -> crate::Result<()> {
        self.connection
            .execute(
                "INSERT INTO settings (id, apikey)
                 VALUES (0, ?1)
                 ON CONFLICT(id) DO UPDATE SET apikey = excluded.apikey",
                params![api_key],
            )
            .map_err(sql_error)?;
        Ok(())
    }

    /// Insert or update one data-dir root row.
    pub fn upsert_data_root(&self, record: &DataRootRecord<'_>) -> crate::Result<()> {
        self.connection
            .execute(
                "INSERT INTO data (path, title)
                 VALUES (?1, ?2)
                 ON CONFLICT(path) DO UPDATE SET title = excluded.title",
                params![record.path, record.title],
            )
            .map_err(sql_error)?;
        Ok(())
    }

    /// Refresh data-dir roots and prune data/ensemble rows no longer present.
    pub fn refresh_data_roots<'a>(
        &self,
        records: impl IntoIterator<Item = DataRootRecord<'a>>,
    ) -> crate::Result<usize> {
        self.begin_data_root_refresh()?;
        for record in records {
            self.upsert_data_root(&record)?;
            self.mark_refreshed_data_root(record.path)?;
        }
        self.finish_data_root_refresh()
    }

    /// Start a bounded refresh for data-dir roots.
    pub fn begin_data_root_refresh(&self) -> crate::Result<()> {
        self.connection
            .execute_batch(
                "CREATE TEMP TABLE IF NOT EXISTS current_data_roots (
                    path TEXT PRIMARY KEY
                );
                DELETE FROM current_data_roots;",
            )
            .map_err(sql_error)
    }

    /// Mark one data-dir root as present during a bounded refresh.
    pub fn mark_refreshed_data_root(&self, path: &str) -> crate::Result<()> {
        self.connection
            .execute(
                "INSERT OR IGNORE INTO current_data_roots (path) VALUES (?1)",
                params![path],
            )
            .map(|_| ())
            .map_err(sql_error)
    }

    /// Prune data-dir rows absent from the current bounded refresh.
    pub fn finish_data_root_refresh(&self) -> crate::Result<usize> {
        self.connection
            .execute(
                "DELETE FROM ensemble
                 WHERE client_host IS NULL
                 AND NOT EXISTS (
                    SELECT 1 FROM current_data_roots
                    WHERE ensemble.path = current_data_roots.path
                    OR ensemble.path LIKE current_data_roots.path || '/%'
                 )",
                [],
            )
            .map_err(sql_error)?;
        self.connection
            .execute(
                "DELETE FROM data
                 WHERE path NOT IN (SELECT path FROM current_data_roots)",
                [],
            )
            .map_err(sql_error)
    }

    /// Insert or update one client searchee cache row.
    pub fn upsert_client_searchee(&self, record: &ClientSearcheeRecord<'_>) -> crate::Result<()> {
        let files = files_json(record.files)?;
        let tags = labels_json(record.tags)?;
        let trackers = strings_json(record.trackers)?;
        self.connection
            .execute(
                "INSERT INTO client_searchee
                    (client_host, info_hash, name, title, files, length, save_path, category, tags, trackers)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
                 ON CONFLICT(client_host, info_hash) DO UPDATE SET
                    name = excluded.name,
                    title = excluded.title,
                    files = excluded.files,
                    length = excluded.length,
                    save_path = excluded.save_path,
                    category = excluded.category,
                    tags = excluded.tags,
                    trackers = excluded.trackers",
                params![
                    record.client_host,
                    record.info_hash,
                    record.name,
                    record.title,
                    files,
                    record.length,
                    record.save_path,
                    record.category,
                    tags,
                    trackers,
                ],
            )
            .map_err(sql_error)?;
        Ok(())
    }

    /// Refresh one client's searchee rows and prune removed info hashes.
    pub fn refresh_client_searchees<'a>(
        &self,
        client_host: &str,
        records: impl IntoIterator<Item = ClientSearcheeRecord<'a>>,
    ) -> crate::Result<usize> {
        self.begin_client_searchee_refresh()?;
        for record in records {
            self.upsert_client_searchee(&record)?;
            self.mark_refreshed_client_info_hash(record.info_hash)?;
        }
        self.finish_client_searchee_refresh(client_host)
    }

    /// Start a bounded refresh for one client's searchee rows.
    pub fn begin_client_searchee_refresh(&self) -> crate::Result<()> {
        self.connection
            .execute_batch(
                "CREATE TEMP TABLE IF NOT EXISTS current_client_info_hashes (
                    info_hash TEXT PRIMARY KEY
                );
                CREATE TEMP TABLE IF NOT EXISTS current_client_ensemble_paths (
                    path TEXT PRIMARY KEY
                );
                DELETE FROM current_client_info_hashes;
                DELETE FROM current_client_ensemble_paths;",
            )
            .map_err(sql_error)
    }

    /// Mark one info hash as present during a bounded client searchee refresh.
    pub fn mark_refreshed_client_info_hash(&self, info_hash: &str) -> crate::Result<()> {
        self.connection
            .execute(
                "INSERT OR IGNORE INTO current_client_info_hashes (info_hash) VALUES (?1)",
                params![info_hash],
            )
            .map(|_| ())
            .map_err(sql_error)
    }

    /// Mark one ensemble path as present during a bounded client searchee refresh.
    pub fn mark_refreshed_client_ensemble_path(&self, path: &str) -> crate::Result<()> {
        self.connection
            .execute(
                "INSERT OR IGNORE INTO current_client_ensemble_paths (path) VALUES (?1)",
                params![path],
            )
            .map(|_| ())
            .map_err(sql_error)
    }

    /// Prune rows absent from the current bounded client searchee refresh.
    pub fn finish_client_searchee_refresh(&self, client_host: &str) -> crate::Result<usize> {
        self.connection
            .execute(
                "DELETE FROM ensemble
                 WHERE client_host = ?1
                 AND path NOT IN (SELECT path FROM current_client_ensemble_paths)",
                params![client_host],
            )
            .map_err(sql_error)?;
        self.connection
            .execute(
                "DELETE FROM ensemble
                 WHERE client_host = ?1
                 AND info_hash NOT IN (SELECT info_hash FROM current_client_info_hashes)",
                params![client_host],
            )
            .map_err(sql_error)?;
        self.connection
            .execute(
                "DELETE FROM client_searchee
                 WHERE client_host = ?1
                 AND info_hash NOT IN (SELECT info_hash FROM current_client_info_hashes)",
                params![client_host],
            )
            .map_err(sql_error)
    }

    /// Insert or update one ensemble row.
    pub fn upsert_ensemble(&self, record: &EnsembleRecord<'_>) -> crate::Result<()> {
        let updated = self
            .connection
            .execute(
                "UPDATE ensemble
                 SET info_hash = ?3, ensemble = ?4, element = ?5
                 WHERE path = ?2
                 AND (
                    client_host = ?1
                    OR (client_host IS NULL AND ?1 IS NULL)
                 )",
                params![
                    record.client_host,
                    record.path,
                    record.info_hash,
                    record.ensemble,
                    record.element,
                ],
            )
            .map_err(sql_error)?;
        if updated > 0 {
            return Ok(());
        }

        self.connection
            .execute(
                "INSERT INTO ensemble (client_host, path, info_hash, ensemble, element)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    record.client_host,
                    record.path,
                    record.info_hash,
                    record.ensemble,
                    record.element,
                ],
            )
            .map_err(sql_error)?;
        Ok(())
    }

    /// Database path under an app directory.
    pub fn path_for_app_dir(app_dir: &Path) -> PathBuf {
        app_dir.join(DATABASE_FILE_NAME)
    }
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
}

/// Ensemble row used for virtual seasons and reverse lookup.
#[derive(Debug, Clone, Copy)]
pub struct EnsembleRecord<'a> {
    /// Client host for client inventory, null for data-dir rows.
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
    name TEXT NOT NULL UNIQUE,
    first_searched INTEGER NOT NULL,
    last_searched INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS decision (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    searchee_id INTEGER NOT NULL REFERENCES searchee(id) ON DELETE CASCADE,
    guid TEXT NOT NULL,
    info_hash TEXT NULL,
    decision TEXT NOT NULL,
    first_seen INTEGER NOT NULL,
    last_seen INTEGER NOT NULL,
    fuzzy_size_factor REAL NOT NULL,
    UNIQUE(searchee_id, guid)
);
CREATE INDEX IF NOT EXISTS idx_decision_info_hash_guid ON decision(info_hash, guid);
CREATE INDEX IF NOT EXISTS idx_decision_info_hash ON decision(info_hash);
CREATE INDEX IF NOT EXISTS idx_decision_guid ON decision(guid);

CREATE TABLE IF NOT EXISTS torrent (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    info_hash TEXT NOT NULL,
    name TEXT NOT NULL,
    file_path TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS job_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    last_run INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS indexer (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NULL,
    url TEXT NOT NULL UNIQUE,
    apikey TEXT NOT NULL,
    trackers TEXT NULL,
    active INTEGER NOT NULL,
    status TEXT NULL,
    retry_after INTEGER NULL,
    search_cap INTEGER NULL,
    tv_search_cap INTEGER NULL,
    movie_search_cap INTEGER NULL,
    music_search_cap INTEGER NULL,
    audio_search_cap INTEGER NULL,
    book_search_cap INTEGER NULL,
    tv_id_caps TEXT NULL,
    movie_id_caps TEXT NULL,
    cat_caps TEXT NULL,
    limits_caps TEXT NULL
);

CREATE TABLE IF NOT EXISTS timestamp (
    searchee_id INTEGER NOT NULL REFERENCES searchee(id) ON DELETE CASCADE,
    indexer_id INTEGER NOT NULL REFERENCES indexer(id) ON DELETE CASCADE,
    first_searched INTEGER NOT NULL,
    last_searched INTEGER NOT NULL,
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

CREATE TABLE IF NOT EXISTS client_searchee (
    client_host TEXT NOT NULL,
    info_hash TEXT NOT NULL,
    name TEXT NOT NULL,
    title TEXT NOT NULL,
    files TEXT NOT NULL,
    length INTEGER NOT NULL,
    save_path TEXT NOT NULL,
    category TEXT NULL,
    tags TEXT NULL,
    trackers TEXT NOT NULL,
    PRIMARY KEY(client_host, info_hash)
);
CREATE INDEX IF NOT EXISTS idx_client_searchee_info_hash ON client_searchee(info_hash);

CREATE TABLE IF NOT EXISTS data (
    path TEXT PRIMARY KEY,
    title TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ensemble (
    client_host TEXT NULL,
    path TEXT NOT NULL,
    info_hash TEXT NULL,
    ensemble TEXT NOT NULL,
    element TEXT NOT NULL,
    PRIMARY KEY(client_host, path)
);
CREATE INDEX IF NOT EXISTS idx_ensemble_path ON ensemble(path);
CREATE INDEX IF NOT EXISTS idx_ensemble_info_hash ON ensemble(info_hash);
"#;

const ENSEMBLE_UNIQUE_KEY_MIGRATION: &str = r#"
DELETE FROM ensemble
WHERE client_host IS NULL
AND rowid NOT IN (
    SELECT MAX(rowid)
    FROM ensemble
    WHERE client_host IS NULL
    GROUP BY path
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_ensemble_data_path_key
ON ensemble(path)
WHERE client_host IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_ensemble_client_path_key
ON ensemble(client_host, path)
WHERE client_host IS NOT NULL;
"#;

fn collect_rows<T>(
    rows: rusqlite::MappedRows<'_, impl FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>>,
) -> crate::Result<Vec<T>> {
    let mut output = Vec::new();
    for row in rows {
        output.push(row.map_err(sql_error)?);
    }
    Ok(output)
}

fn sql_error(error: rusqlite::Error) -> SporosError {
    SporosError::Persistence {
        message: Cow::Owned(error.to_string()),
    }
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
mod tests {
    use super::{ClientSearcheeRecord, DataRootRecord, Database, DecisionRecord, EnsembleRecord};
    use crate::domain::{ClientLabel, Decision, File};
    use std::{
        borrow::Cow,
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    #[test]
    fn initializes_schema_with_wal_and_documented_tables() {
        let root = temp_path("schema");
        fs::create_dir_all(&root).expect("temp dir");
        let database = Database::open_app_dir(&root).expect("database");

        let journal_mode: String = database
            .connection()
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .expect("journal mode");
        assert_eq!(journal_mode.to_ascii_lowercase(), "wal");
        let user_version: i64 = database
            .connection()
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .expect("schema version");
        assert_eq!(user_version, super::CURRENT_SCHEMA_VERSION);

        for table in [
            "searchee",
            "decision",
            "torrent",
            "job_log",
            "indexer",
            "timestamp",
            "settings",
            "rss",
            "client_searchee",
            "data",
            "ensemble",
        ] {
            let count: i64 = database
                .connection()
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
                    [table],
                    |row| row.get(0),
                )
                .expect("table query");
            assert_eq!(count, 1, "{table}");
        }

        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn upserts_searchee_decisions_and_pages_guid_map() {
        let root = temp_path("decision");
        fs::create_dir_all(&root).expect("temp dir");
        let database = Database::open_app_dir(&root).expect("database");
        let searchee_id = database
            .get_or_insert_searchee("Example Show S01", 100)
            .expect("searchee");

        database
            .upsert_decision(&DecisionRecord {
                searchee_id,
                guid: "guid-1",
                info_hash: Some("0123456789abcdef0123456789abcdef01234567"),
                decision: Decision::Match,
                first_seen: 100,
                last_seen: 100,
                fuzzy_size_factor: 0.05,
            })
            .expect("decision");
        database
            .upsert_decision(&DecisionRecord {
                searchee_id,
                guid: "guid-1",
                info_hash: Some("fedcba9876543210fedcba9876543210fedcba98"),
                decision: Decision::MatchSizeOnly,
                first_seen: 100,
                last_seen: 200,
                fuzzy_size_factor: 0.1,
            })
            .expect("decision update");

        let page = database.guid_info_hash_page(0, 10).expect("page");

        assert_eq!(page.len(), 1);
        assert_eq!(page[0].guid, "guid-1");
        assert_eq!(
            page[0].info_hash,
            "fedcba9876543210fedcba9876543210fedcba98"
        );

        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn stores_generated_api_key_in_settings_row_zero() {
        let root = temp_path("settings");
        fs::create_dir_all(&root).expect("temp dir");
        let database = Database::open_app_dir(&root).expect("database");

        assert_eq!(database.get_api_key().expect("api key"), None);
        database
            .set_api_key("0123456789abcdef0123456789abcdef0123456789abcdef")
            .expect("set api key");

        assert_eq!(
            database.get_api_key().expect("api key"),
            Some("0123456789abcdef0123456789abcdef0123456789abcdef".to_owned())
        );

        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn refreshes_data_roots_and_prunes_missing_rows() {
        let root = temp_path("data-roots");
        fs::create_dir_all(&root).expect("temp dir");
        let database = Database::open_app_dir(&root).expect("database");
        database
            .refresh_data_roots([
                DataRootRecord {
                    path: "/data/one",
                    title: "One",
                },
                DataRootRecord {
                    path: "/data/two",
                    title: "Two",
                },
            ])
            .expect("refresh");
        database
            .upsert_ensemble(&EnsembleRecord {
                client_host: None,
                path: "/data/two/file.mkv",
                info_hash: None,
                ensemble: "show s01",
                element: "1",
            })
            .expect("ensemble");

        let removed = database
            .refresh_data_roots([DataRootRecord {
                path: "/data/one",
                title: "One Updated",
            }])
            .expect("refresh");

        assert_eq!(removed, 1);
        let title: String = database
            .connection()
            .query_row(
                "SELECT title FROM data WHERE path = '/data/one'",
                [],
                |row| row.get(0),
            )
            .expect("title");
        assert_eq!(title, "One Updated");
        let ensemble_count: i64 = database
            .connection()
            .query_row("SELECT COUNT(*) FROM ensemble", [], |row| row.get(0))
            .expect("ensemble count");
        assert_eq!(ensemble_count, 0);
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn upserts_data_dir_ensemble_rows_with_null_client_host() {
        let root = temp_path("ensemble-null-client");
        fs::create_dir_all(&root).expect("temp dir");
        let database = Database::open_app_dir(&root).expect("database");

        database
            .upsert_ensemble(&EnsembleRecord {
                client_host: None,
                path: "/data/show/file.mkv",
                info_hash: None,
                ensemble: "old show s01",
                element: "1",
            })
            .expect("ensemble");
        database
            .upsert_ensemble(&EnsembleRecord {
                client_host: None,
                path: "/data/show/file.mkv",
                info_hash: Some("0123456789abcdef0123456789abcdef01234567"),
                ensemble: "new show s01",
                element: "2",
            })
            .expect("ensemble update");

        let row: (i64, Option<String>, String, String) = database
            .connection()
            .query_row(
                "SELECT COUNT(*), info_hash, ensemble, element FROM ensemble",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .expect("ensemble row");
        assert_eq!(
            row,
            (
                1,
                Some("0123456789abcdef0123456789abcdef01234567".to_owned()),
                "new show s01".to_owned(),
                "2".to_owned()
            )
        );

        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn migration_deduplicates_null_client_ensemble_keys() {
        let root = temp_path("ensemble-null-client-migration");
        fs::create_dir_all(&root).expect("temp dir");
        let database_path = Database::path_for_app_dir(&root);
        let connection = rusqlite::Connection::open(&database_path).expect("raw database");
        connection.execute_batch(super::SCHEMA).expect("schema");
        connection
            .execute_batch(
                "INSERT INTO ensemble (client_host, path, info_hash, ensemble, element)
                 VALUES (NULL, '/data/show/file.mkv', NULL, 'old show s01', '1');
                 INSERT INTO ensemble (client_host, path, info_hash, ensemble, element)
                 VALUES (
                    NULL,
                    '/data/show/file.mkv',
                    '0123456789abcdef0123456789abcdef01234567',
                    'new show s01',
                    '2'
                 );
                 PRAGMA user_version = 1;",
            )
            .expect("legacy duplicate rows");
        drop(connection);

        let database = Database::open(&database_path).expect("migrated database");

        let row: (i64, Option<String>, String, String) = database
            .connection()
            .query_row(
                "SELECT COUNT(*), info_hash, ensemble, element FROM ensemble",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .expect("ensemble row");
        assert_eq!(
            row,
            (
                1,
                Some("0123456789abcdef0123456789abcdef01234567".to_owned()),
                "new show s01".to_owned(),
                "2".to_owned()
            )
        );

        let duplicate = database.connection().execute(
            "INSERT INTO ensemble (client_host, path, info_hash, ensemble, element)
             VALUES (NULL, '/data/show/file.mkv', NULL, 'duplicate show s01', '3')",
            [],
        );
        duplicate.expect_err("duplicate null-client ensemble key");

        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn stores_client_searchee_json_and_prunes_by_host() {
        let root = temp_path("client-searchees");
        fs::create_dir_all(&root).expect("temp dir");
        let database = Database::open_app_dir(&root).expect("database");
        let files = [File::new("Release/file.mkv", 42)];
        let tags = [ClientLabel::new("tag")];
        let trackers = [Cow::Borrowed("tracker.example")];
        database
            .refresh_client_searchees(
                "client",
                [ClientSearcheeRecord {
                    client_host: "client",
                    info_hash: "0123456789abcdef0123456789abcdef01234567",
                    name: "Release",
                    title: "Release",
                    files: &files,
                    length: 42,
                    save_path: "/downloads",
                    category: Some("tv"),
                    tags: &tags,
                    trackers: &trackers,
                }],
            )
            .expect("refresh");
        database
            .upsert_ensemble(&EnsembleRecord {
                client_host: Some("client"),
                path: "/downloads/file.mkv",
                info_hash: Some("0123456789abcdef0123456789abcdef01234567"),
                ensemble: "release",
                element: "1",
            })
            .expect("ensemble");

        let json: String = database
            .connection()
            .query_row("SELECT files FROM client_searchee", [], |row| row.get(0))
            .expect("files json");
        assert!(json.contains("Release/file.mkv"));

        let removed = database
            .refresh_client_searchees("client", [])
            .expect("prune");

        assert_eq!(removed, 1);
        let ensemble_count: i64 = database
            .connection()
            .query_row("SELECT COUNT(*) FROM ensemble", [], |row| row.get(0))
            .expect("ensemble count");
        assert_eq!(ensemble_count, 0);
        let _cleanup = fs::remove_dir_all(root);
    }

    fn temp_path(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        std::env::temp_dir().join(format!("sporos-db-{label}-{nanos}"))
    }
}
