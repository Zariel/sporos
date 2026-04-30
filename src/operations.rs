//! Maintenance operations for cache, indexer, API-key, diff, and tree commands.

use std::{borrow::Cow, fs, path::Path};

use crate::{
    SporosError,
    config::RuntimeConfig,
    domain::InfoHash,
    persistence::Database,
    torrent::{
        Bencode, BencodeValue, bdecode, bencode, parse_metafile, torrent_cache_dir,
        torrent_cache_path,
    },
};

const ONE_DAY_MILLIS: u64 = 86_400_000;
const THIRTY_DAYS_MILLIS: u64 = 30 * ONE_DAY_MILLIS;
const ONE_YEAR_MILLIS: u64 = 365 * ONE_DAY_MILLIS;

/// Result counts from cache cleanup.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct ClearCacheResult {
    /// Decision rows with null info hashes removed.
    pub decisions_removed: usize,
    /// Timestamp rows removed.
    pub timestamps_removed: usize,
}

/// Result counts from client cache cleanup.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct ClearClientCacheResult {
    /// Torrent-dir rows removed.
    pub torrents_removed: usize,
    /// Client searchee rows removed.
    pub client_searchees_removed: usize,
    /// Data-dir rows removed.
    pub data_removed: usize,
    /// Ensemble rows removed.
    pub ensemble_removed: usize,
}

/// Result from tracker URL replacement.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct TrackerUpdateResult {
    /// Cached torrent files inspected.
    pub files_seen: usize,
    /// Cached torrent files rewritten.
    pub files_updated: usize,
}

/// Result counts from daily cleanup.
#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct CleanupDbResult {
    /// Data-dir rows removed because paths no longer exist.
    pub data_rows_removed: usize,
    /// Ensemble rows removed because paths no longer exist.
    pub ensemble_rows_removed: usize,
    /// Torrent cache files removed because no recent decision references them.
    pub torrent_cache_files_removed: usize,
    /// Decision rows with null info hashes removed.
    pub null_decisions_removed: usize,
    /// Decision rows removed because their cache files are missing.
    pub missing_cache_decisions_removed: usize,
    /// Missing-cache decision cleanup was skipped by the catastrophic guard.
    pub catastrophic_decision_cleanup_skipped: bool,
    /// GUID to info-hash rows read while rebuilding the map.
    pub guid_info_hash_rows: usize,
}

/// Compact tree output for a parsed torrent.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TorrentTree {
    /// Torrent name.
    pub name: String,
    /// Info hash.
    pub info_hash: String,
    /// File paths and lengths.
    pub files: Vec<(String, u64)>,
}

/// Return configured, persisted, or newly generated API key in that order.
pub fn api_key(database: &Database, configured: Option<&str>) -> crate::Result<String> {
    if let Some(configured) = configured {
        return Ok(configured.to_owned());
    }
    if let Some(stored) = database.get_api_key()? {
        return Ok(stored);
    }
    reset_api_key(database)
}

/// Generate and persist a fresh API key.
pub fn reset_api_key(database: &Database) -> crate::Result<String> {
    let key = generate_api_key()?;
    database.set_api_key(&key)?;
    Ok(key)
}

/// Clear decision cache rows without cached torrents and search timestamps.
pub fn clear_cache(database: &Database) -> crate::Result<ClearCacheResult> {
    let decisions_removed = database
        .connection()
        .execute("DELETE FROM decision WHERE info_hash IS NULL", [])
        .map_err(persistence_error)?;
    let timestamps_removed = database
        .connection()
        .execute("DELETE FROM timestamp", [])
        .map_err(persistence_error)?;
    Ok(ClearCacheResult {
        decisions_removed,
        timestamps_removed,
    })
}

/// Clear cached client, torrent-dir, data-dir, and ensemble state.
pub fn clear_client_cache(database: &Database) -> crate::Result<ClearClientCacheResult> {
    let torrents_removed = database
        .connection()
        .execute("DELETE FROM torrent", [])
        .map_err(persistence_error)?;
    let client_searchees_removed = database
        .connection()
        .execute("DELETE FROM client_searchee", [])
        .map_err(persistence_error)?;
    let data_removed = database
        .connection()
        .execute("DELETE FROM data", [])
        .map_err(persistence_error)?;
    let ensemble_removed = database
        .connection()
        .execute("DELETE FROM ensemble", [])
        .map_err(persistence_error)?;
    Ok(ClearClientCacheResult {
        torrents_removed,
        client_searchees_removed,
        data_removed,
        ensemble_removed,
    })
}

/// Clear indexer failure status and retry timestamps.
pub fn clear_indexer_failures(database: &Database) -> crate::Result<usize> {
    database
        .connection()
        .execute("UPDATE indexer SET status = NULL, retry_after = NULL", [])
        .map_err(persistence_error)
}

/// Run daily database and torrent-cache cleanup.
pub fn cleanup_db(
    database: &Database,
    app_dir: &Path,
    config: &RuntimeConfig,
    now_millis: i64,
) -> crate::Result<CleanupDbResult> {
    let mut result = CleanupDbResult::default();
    if !config.data_dirs.is_empty() {
        result.data_rows_removed = prune_missing_data_rows(database)?;
        result.ensemble_rows_removed += prune_missing_ensemble_rows(database, true)?;
    }
    if config.season_from_episodes.is_some() {
        result.ensemble_rows_removed += prune_missing_ensemble_rows(database, false)?;
    }
    result.torrent_cache_files_removed =
        prune_unused_torrent_cache(database, app_dir, config, now_millis)?;
    result.null_decisions_removed = database
        .connection()
        .execute("DELETE FROM decision WHERE info_hash IS NULL", [])
        .map_err(persistence_error)?;
    let decision_cleanup = prune_missing_cache_decisions(database, app_dir)?;
    result.missing_cache_decisions_removed = decision_cleanup.removed;
    result.catastrophic_decision_cleanup_skipped = decision_cleanup.catastrophic_skipped;
    result.guid_info_hash_rows = rebuild_guid_info_hash_map(database)?;
    Ok(result)
}

/// Replace tracker URLs inside cached torrent files.
pub fn update_torrent_cache_trackers(
    app_dir: &Path,
    old_announce_url: &str,
    new_announce_url: &str,
) -> crate::Result<TrackerUpdateResult> {
    let cache_dir = torrent_cache_dir(app_dir);
    let mut result = TrackerUpdateResult {
        files_seen: 0,
        files_updated: 0,
    };
    let entries = match fs::read_dir(&cache_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(result),
        Err(error) => return Err(operation_error(format!("failed to read cache: {error}"))),
    };

    for entry in entries {
        let entry = entry.map_err(|error| operation_error(format!("cache entry: {error}")))?;
        let path = entry.path();
        if path.extension().and_then(std::ffi::OsStr::to_str) != Some("torrent") {
            continue;
        }
        result.files_seen += 1;
        let bytes = fs::read(&path)
            .map_err(|error| operation_error(format!("failed to read torrent: {error}")))?;
        if let Some(updated) = replace_torrent_tracker_urls(
            &bytes,
            old_announce_url.as_bytes(),
            new_announce_url.as_bytes(),
        )? {
            fs::write(&path, updated).map_err(|error| {
                operation_error(format!("failed to write updated torrent: {error}"))
            })?;
            result.files_updated += 1;
        }
    }

    Ok(result)
}

/// Parse and compare two torrent files by normalized metafile structure.
pub fn diff_torrents(left: &Path, right: &Path) -> crate::Result<Option<String>> {
    let left = parse_metafile(
        &fs::read(left)
            .map_err(|error| operation_error(format!("failed to read left torrent: {error}")))?,
    )?;
    let right = parse_metafile(
        &fs::read(right)
            .map_err(|error| operation_error(format!("failed to read right torrent: {error}")))?,
    )?;

    if left == right {
        Ok(None)
    } else {
        Ok(Some(format!("{left:#?}\n---\n{right:#?}")))
    }
}

/// Parse a torrent file and return displayable tree metadata.
pub fn torrent_tree(path: &Path) -> crate::Result<TorrentTree> {
    let metafile = parse_metafile(
        &fs::read(path)
            .map_err(|error| operation_error(format!("failed to read torrent: {error}")))?,
    )?;
    Ok(TorrentTree {
        name: metafile.name.into_owned(),
        info_hash: metafile.info_hash.as_str().to_owned(),
        files: metafile
            .files
            .into_iter()
            .map(|file| (file.path.into_owned(), file.length))
            .collect(),
    })
}

#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
struct MissingCacheDecisionCleanup {
    removed: usize,
    catastrophic_skipped: bool,
}

fn prune_missing_data_rows(database: &Database) -> crate::Result<usize> {
    let paths = string_column(database, "SELECT path FROM data")?;
    let mut removed = 0usize;
    for path in paths {
        if !Path::new(&path).exists() {
            removed += database
                .connection()
                .execute("DELETE FROM data WHERE path = ?1", [&path])
                .map_err(persistence_error)?;
        }
    }
    Ok(removed)
}

fn prune_missing_ensemble_rows(database: &Database, data_dir_only: bool) -> crate::Result<usize> {
    let sql = if data_dir_only {
        "SELECT rowid, path FROM ensemble WHERE client_host IS NULL"
    } else {
        "SELECT rowid, path FROM ensemble"
    };
    let rows = rowid_path_rows(database, sql)?;
    let mut removed = 0usize;
    for (rowid, path) in rows {
        if !Path::new(&path).exists() {
            removed += database
                .connection()
                .execute("DELETE FROM ensemble WHERE rowid = ?1", [rowid])
                .map_err(persistence_error)?;
        }
    }
    Ok(removed)
}

fn prune_unused_torrent_cache(
    database: &Database,
    app_dir: &Path,
    config: &RuntimeConfig,
    now_millis: i64,
) -> crate::Result<usize> {
    let cache_dir = torrent_cache_dir(app_dir);
    let entries = match fs::read_dir(&cache_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => {
            return Err(operation_error(format!(
                "failed to read torrent cache {}: {error}",
                cache_dir.display()
            )));
        }
    };
    let age = config
        .exclude_recent_search
        .map(|value| value.saturating_add(THIRTY_DAYS_MILLIS))
        .unwrap_or(0)
        .max(ONE_YEAR_MILLIS);
    let cutoff = now_millis.saturating_sub(i64::try_from(age).unwrap_or(i64::MAX));
    let mut removed = 0usize;
    for entry in entries {
        let entry =
            entry.map_err(|error| operation_error(format!("torrent cache entry: {error}")))?;
        let path = entry.path();
        let Some(info_hash) = cache_info_hash(&path) else {
            continue;
        };
        if !recent_decision_exists(database, &info_hash, cutoff)? {
            fs::remove_file(&path).map_err(|error| {
                operation_error(format!(
                    "failed to remove torrent cache file {}: {error}",
                    path.display()
                ))
            })?;
            removed += 1;
        }
    }
    Ok(removed)
}

fn prune_missing_cache_decisions(
    database: &Database,
    app_dir: &Path,
) -> crate::Result<MissingCacheDecisionCleanup> {
    let info_hashes = string_column(
        database,
        "SELECT DISTINCT info_hash FROM decision WHERE info_hash IS NOT NULL",
    )?;
    let missing = info_hashes
        .iter()
        .filter_map(|info_hash| {
            InfoHash::new(info_hash.as_str()).and_then(|hash| {
                (!torrent_cache_path(app_dir, &hash).exists()).then(|| info_hash.clone())
            })
        })
        .collect::<Vec<_>>();
    let valid_count = info_hashes
        .iter()
        .filter(|info_hash| InfoHash::new(info_hash.as_str()).is_some())
        .count();
    if valid_count > 0 && missing.len() == valid_count {
        return Ok(MissingCacheDecisionCleanup {
            removed: 0,
            catastrophic_skipped: true,
        });
    }

    let mut removed = 0usize;
    for info_hash in missing {
        removed += database
            .connection()
            .execute("DELETE FROM decision WHERE info_hash = ?1", [&info_hash])
            .map_err(persistence_error)?;
    }
    Ok(MissingCacheDecisionCleanup {
        removed,
        catastrophic_skipped: false,
    })
}

fn rebuild_guid_info_hash_map(database: &Database) -> crate::Result<usize> {
    let mut after_id = 0_i64;
    let mut count = 0usize;
    loop {
        let page = database.guid_info_hash_page(after_id, 1_000)?;
        let Some(last) = page.last() else {
            break;
        };
        after_id = last.id;
        count += page.len();
    }
    Ok(count)
}

fn recent_decision_exists(
    database: &Database,
    info_hash: &str,
    cutoff_millis: i64,
) -> crate::Result<bool> {
    database
        .connection()
        .query_row(
            "SELECT EXISTS(
                SELECT 1 FROM decision
                WHERE info_hash = ?1 AND last_seen >= ?2
            )",
            rusqlite::params![info_hash, cutoff_millis],
            |row| row.get::<_, bool>(0),
        )
        .map_err(persistence_error)
}

fn string_column(database: &Database, sql: &str) -> crate::Result<Vec<String>> {
    let mut statement = database
        .connection()
        .prepare(sql)
        .map_err(persistence_error)?;
    let rows = statement
        .query_map([], |row| row.get(0))
        .map_err(persistence_error)?;
    let mut output = Vec::new();
    for row in rows {
        output.push(row.map_err(persistence_error)?);
    }
    Ok(output)
}

fn rowid_path_rows(database: &Database, sql: &str) -> crate::Result<Vec<(i64, String)>> {
    let mut statement = database
        .connection()
        .prepare(sql)
        .map_err(persistence_error)?;
    let rows = statement
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .map_err(persistence_error)?;
    let mut output = Vec::new();
    for row in rows {
        output.push(row.map_err(persistence_error)?);
    }
    Ok(output)
}

fn cache_info_hash(path: &Path) -> Option<String> {
    let filename = path.file_name()?.to_str()?;
    let info_hash = filename.strip_suffix(".cached.torrent")?;
    InfoHash::new(info_hash).map(|hash| hash.to_string())
}

fn generate_api_key() -> crate::Result<String> {
    let mut bytes = [0_u8; 24];
    getrandom::fill(&mut bytes)
        .map_err(|error| operation_error(format!("failed to generate api key: {error}")))?;
    let mut output = String::with_capacity(48);
    for byte in bytes {
        output.push(hex_digit(byte >> 4));
        output.push(hex_digit(byte & 0x0f));
    }
    Ok(output)
}

fn replace_bytes(input: &[u8], from: &[u8], to: &[u8]) -> Vec<u8> {
    if from.is_empty() {
        return input.to_vec();
    }
    let mut output = Vec::with_capacity(input.len());
    let mut offset = 0;
    while offset < input.len() {
        let candidate = offset
            .checked_add(from.len())
            .and_then(|end| input.get(offset..end));
        if candidate == Some(from) {
            output.extend_from_slice(to);
            offset += from.len();
        } else if let Some(byte) = input.get(offset) {
            output.push(*byte);
            offset += 1;
        } else {
            break;
        }
    }
    output
}

fn hex_digit(nibble: u8) -> char {
    match nibble {
        0..=9 => char::from(b'0' + nibble),
        _ => char::from(b'a' + (nibble - 10)),
    }
}

fn replace_torrent_tracker_urls(
    input: &[u8],
    from: &[u8],
    to: &[u8],
) -> crate::Result<Option<Vec<u8>>> {
    if from.is_empty() {
        return Ok(None);
    }

    let mut decoded = bdecode(input)?;
    let BencodeValue::Dict(entries) = &mut decoded.value else {
        return Err(operation_error("cached torrent root must be a dictionary"));
    };

    let mut changed = false;
    for (key, value) in entries {
        match key.as_ref() {
            b"announce" => changed |= replace_bencode_bytes(value, from, to),
            b"announce-list" => changed |= replace_bencode_bytes_recursive(value, from, to),
            _ => {}
        }
    }

    Ok(changed.then(|| bencode(&decoded)))
}

fn persistence_error(error: rusqlite::Error) -> SporosError {
    SporosError::Persistence {
        message: Cow::Owned(error.to_string()),
    }
}

fn operation_error(message: impl Into<Cow<'static, str>>) -> SporosError {
    SporosError::Operation {
        message: message.into(),
    }
}

fn replace_bencode_bytes(value: &mut Bencode<'_>, from: &[u8], to: &[u8]) -> bool {
    let BencodeValue::Bytes(bytes) = &mut value.value else {
        return false;
    };
    let updated = replace_bytes(bytes.as_ref(), from, to);
    if updated == bytes.as_ref() {
        false
    } else {
        *bytes = Cow::Owned(updated);
        true
    }
}

fn replace_bencode_bytes_recursive(value: &mut Bencode<'_>, from: &[u8], to: &[u8]) -> bool {
    match &mut value.value {
        BencodeValue::Bytes(_) => replace_bencode_bytes(value, from, to),
        BencodeValue::List(items) => {
            let mut changed = false;
            for item in items {
                changed |= replace_bencode_bytes_recursive(item, from, to);
            }
            changed
        }
        BencodeValue::Integer(_) | BencodeValue::Dict(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        api_key, cleanup_db, clear_cache, clear_client_cache, clear_indexer_failures,
        reset_api_key, update_torrent_cache_trackers,
    };
    use crate::{
        config::{RawConfig, RuntimeConfig},
        domain::Decision,
        persistence::{Database, DecisionRecord},
    };
    use std::{
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    #[test]
    fn api_key_prefers_config_then_db_then_generated() {
        let root = temp_path("api");
        fs::create_dir_all(&root).expect("temp dir");
        let database = Database::open_app_dir(&root).expect("database");

        assert_eq!(
            api_key(&database, Some("configured-api-key")).expect("configured"),
            "configured-api-key"
        );
        let generated = api_key(&database, None).expect("generated");
        assert_eq!(generated.len(), 48);
        assert_eq!(api_key(&database, None).expect("stored"), generated);
        let reset = reset_api_key(&database).expect("reset");
        assert_eq!(reset.len(), 48);
        assert_ne!(reset, generated);

        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn clears_cache_tables() {
        let root = temp_path("clear-cache");
        fs::create_dir_all(&root).expect("temp dir");
        let database = Database::open_app_dir(&root).expect("database");
        let searchee_id = database
            .get_or_insert_searchee("name", 1)
            .expect("searchee");
        database
            .upsert_decision(&DecisionRecord {
                searchee_id,
                guid: "guid",
                info_hash: None,
                decision: crate::domain::Decision::NoDownloadLink,
                first_seen: 1,
                last_seen: 1,
                fuzzy_size_factor: 0.05,
            })
            .expect("decision");

        let result = clear_cache(&database).expect("clear");

        assert_eq!(result.decisions_removed, 1);
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn clears_client_cache_tables_and_indexer_failures() {
        let root = temp_path("client-cache");
        fs::create_dir_all(&root).expect("temp dir");
        let database = Database::open_app_dir(&root).expect("database");
        database
            .connection()
            .execute(
                "INSERT INTO indexer (url, apikey, active, status, retry_after)
                 VALUES ('https://indexer.example', 'key', 1, 'RATE_LIMITED', 100)",
                [],
            )
            .expect("indexer");

        let failures = clear_indexer_failures(&database).expect("failures");
        let client = clear_client_cache(&database).expect("client cache");

        assert_eq!(failures, 1);
        assert_eq!(client.torrents_removed, 0);
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn updates_cached_torrent_tracker_urls() {
        let root = temp_path("trackers");
        let cache_dir = root.join("torrent_cache");
        fs::create_dir_all(&cache_dir).expect("cache dir");
        let path = cache_dir.join("0123456789abcdef0123456789abcdef01234567.cached.torrent");
        fs::write(
            &path,
            b"d8:announce28:https://old.example/announce13:announce-listll28:https://old.example/announceeee",
        )
        .expect("write");

        let result = update_torrent_cache_trackers(
            &root,
            "https://old.example/announce",
            "https://longer-new.example/announce",
        )
        .expect("update");

        assert_eq!(result.files_seen, 1);
        assert_eq!(result.files_updated, 1);
        assert_eq!(
            fs::read(&path).expect("read"),
            b"d8:announce35:https://longer-new.example/announce13:announce-listll35:https://longer-new.example/announceeee"
        );
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn cleanup_prunes_cache_null_decisions_and_missing_paths() {
        let root = temp_path("cleanup");
        fs::create_dir_all(root.join("torrent_cache")).expect("cache dir");
        let database = Database::open_app_dir(&root).expect("database");
        let existing_data = root.join("data");
        fs::create_dir_all(&existing_data).expect("data dir");
        let missing_data = root.join("missing-data");
        let missing_ensemble = root.join("missing-episode.mkv");
        database
            .connection()
            .execute(
                "INSERT INTO data (path, title) VALUES (?1, 'Existing'), (?2, 'Missing')",
                rusqlite::params![
                    existing_data.to_string_lossy(),
                    missing_data.to_string_lossy()
                ],
            )
            .expect("data");
        database
            .connection()
            .execute(
                "INSERT INTO ensemble (client_host, path, info_hash, ensemble, element)
                 VALUES (NULL, ?1, NULL, 'show s01', 'e01')",
                [missing_ensemble.to_string_lossy()],
            )
            .expect("ensemble");
        let searchee_id = database
            .get_or_insert_searchee("name", 1)
            .expect("searchee");
        let old_hash = "0123456789012345678901234567890123456789";
        let recent_hash = "1111111111111111111111111111111111111111";
        let missing_hash = "2222222222222222222222222222222222222222";
        fs::write(
            root.join("torrent_cache")
                .join(format!("{old_hash}.cached.torrent")),
            b"old",
        )
        .expect("old cache");
        fs::write(
            root.join("torrent_cache")
                .join(format!("{recent_hash}.cached.torrent")),
            b"recent",
        )
        .expect("recent cache");
        let now = 800 * 86_400_000;
        insert_decision(&database, searchee_id, "old-guid", Some(old_hash), 1);
        insert_decision(
            &database,
            searchee_id,
            "recent-guid",
            Some(recent_hash),
            now,
        );
        insert_decision(
            &database,
            searchee_id,
            "missing-guid",
            Some(missing_hash),
            now,
        );
        insert_decision(&database, searchee_id, "null-guid", None, now);
        let config = RuntimeConfig::normalize(
            RawConfig {
                data_dirs: vec![existing_data],
                season_from_episodes: Some(1.0),
                ..RawConfig::default()
            },
            &root,
        )
        .expect("config");

        let result = cleanup_db(&database, &root, &config, now).expect("cleanup");

        assert_eq!(result.data_rows_removed, 1);
        assert_eq!(result.ensemble_rows_removed, 1);
        assert_eq!(result.torrent_cache_files_removed, 1);
        assert_eq!(result.null_decisions_removed, 1);
        assert_eq!(result.missing_cache_decisions_removed, 2);
        assert!(!result.catastrophic_decision_cleanup_skipped);
        assert_eq!(result.guid_info_hash_rows, 1);
        assert!(
            !root
                .join("torrent_cache")
                .join(format!("{old_hash}.cached.torrent"))
                .exists()
        );
        assert!(
            root.join("torrent_cache")
                .join(format!("{recent_hash}.cached.torrent"))
                .exists()
        );
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn cleanup_skips_catastrophic_missing_decision_prune() {
        let root = temp_path("cleanup-guard");
        fs::create_dir_all(&root).expect("root");
        let database = Database::open_app_dir(&root).expect("database");
        let searchee_id = database
            .get_or_insert_searchee("name", 1)
            .expect("searchee");
        insert_decision(
            &database,
            searchee_id,
            "missing-guid",
            Some("0123456789012345678901234567890123456789"),
            2_000_000,
        );
        let config = RuntimeConfig::normalize(RawConfig::default(), &root).expect("config");

        let result = cleanup_db(&database, &root, &config, 2_000_000).expect("cleanup");

        assert!(result.catastrophic_decision_cleanup_skipped);
        assert_eq!(result.missing_cache_decisions_removed, 0);
        let remaining: i64 = database
            .connection()
            .query_row("SELECT COUNT(*) FROM decision", [], |row| row.get(0))
            .expect("count");
        assert_eq!(remaining, 1);
        let _cleanup = fs::remove_dir_all(root);
    }

    fn insert_decision(
        database: &Database,
        searchee_id: i64,
        guid: &str,
        info_hash: Option<&str>,
        last_seen: i64,
    ) {
        database
            .upsert_decision(&DecisionRecord {
                searchee_id,
                guid,
                info_hash,
                decision: Decision::Match,
                first_seen: last_seen,
                last_seen,
                fuzzy_size_factor: 0.05,
            })
            .expect("decision");
    }

    fn temp_path(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        std::env::temp_dir().join(format!("sporos-ops-{label}-{}-{nanos}", std::process::id()))
    }
}
