//! SQLite state, migrations, cache records, and paged persistence helpers.

use std::{
    borrow::Cow,
    path::{Path, PathBuf},
};

use rusqlite::{Connection, OptionalExtension, params};

use crate::{SporosError, domain::Decision};

const DATABASE_FILE_NAME: &str = "cross-seed.db";

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

    /// Create schema objects and set SQLite pragmas.
    pub fn initialize(&self) -> crate::Result<()> {
        self.connection.execute_batch(SCHEMA).map_err(sql_error)?;
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::{Database, DecisionRecord};
    use crate::domain::Decision;
    use std::{
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

    fn temp_path(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        std::env::temp_dir().join(format!("sporos-db-{label}-{nanos}"))
    }
}
