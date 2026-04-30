//! External indexer, Torznab, Arr, and notification integrations.

use std::{
    borrow::Cow,
    collections::HashMap,
    fs,
    path::Path,
    thread,
    time::{Duration, Instant},
};

use filetime::FileTime;
use quick_xml::{Reader, events::Event};
use rusqlite::{OptionalExtension, params};
use url::Url;

use crate::{
    SporosError,
    domain::{Candidate, InfoHash, Metafile},
    persistence::Database,
    torrent::{parse_metafile, torrent_cache_path},
};

/// Sanitized Torznab configuration split into persisted URL and secret API key.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TorznabConfig {
    /// Sanitized `origin + pathname` ending in `/api`.
    pub url: String,
    /// API key extracted from the query string.
    pub apikey: String,
}

/// Result counts from syncing configured indexers with the database.
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
pub struct IndexerSyncResult {
    /// Newly inserted indexers.
    pub inserted: usize,
    /// Existing indexers reactivated or updated.
    pub updated: usize,
    /// Existing indexers deactivated because they are no longer configured.
    pub deactivated: usize,
}

/// Torznab category capability booleans.
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq, serde::Serialize)]
pub struct CategoryCaps {
    /// Movie categories.
    pub movie: bool,
    /// TV categories.
    pub tv: bool,
    /// Anime categories.
    pub anime: bool,
    /// Adult categories.
    pub xxx: bool,
    /// Audio categories.
    pub audio: bool,
    /// Book categories.
    pub book: bool,
    /// Other usable categories.
    pub additional: bool,
}

/// Torznab limit caps.
#[derive(Debug, Clone, Copy, Eq, PartialEq, serde::Serialize)]
pub struct LimitCaps {
    /// Default page size.
    pub default: u32,
    /// Maximum page size.
    pub max: u32,
}

impl Default for LimitCaps {
    fn default() -> Self {
        Self {
            default: 100,
            max: 100,
        }
    }
}

/// Parsed Torznab caps.
#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct TorznabCaps {
    /// Generic search support.
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
    /// Supported TV ID params.
    pub tv_ids: Vec<String>,
    /// Supported movie ID params.
    pub movie_ids: Vec<String>,
    /// Category support.
    pub categories: CategoryCaps,
    /// Limits.
    pub limits: LimitCaps,
}

/// Enabled indexer row for search and RSS flows.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EnabledIndexer {
    /// Database row id.
    pub id: i64,
    /// Sanitized URL.
    pub url: String,
    /// API key.
    pub apikey: String,
}

/// Retry behavior for candidate torrent downloads.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct SnatchOptions {
    /// Number of retries after the first attempt.
    pub retries: u32,
    /// Configured delay between attempts.
    pub delay: Duration,
    /// Per-request timeout.
    pub timeout: Option<Duration>,
}

impl Default for SnatchOptions {
    fn default() -> Self {
        Self {
            retries: 0,
            delay: Duration::ZERO,
            timeout: None,
        }
    }
}

/// Process-local failed snatch memory used to suppress repeated bad downloads.
#[derive(Debug, Default, Clone)]
pub struct SnatchHistory {
    failures: HashMap<String, SnatchFailure>,
}

impl SnatchHistory {
    /// Remove history older than `max_age`.
    pub fn prune_older_than(&mut self, max_age: Duration) {
        let now = Instant::now();
        self.failures.retain(|_, failure| {
            now.saturating_duration_since(failure.initial_failure_at) <= max_age
        });
    }

    /// Whether no failed snatches are remembered.
    pub fn is_empty(&self) -> bool {
        self.failures.is_empty()
    }

    fn clear(&mut self, key: &str) {
        self.failures.remove(key);
    }

    fn record_failure(&mut self, key: &str) {
        self.failures
            .entry(key.to_owned())
            .and_modify(|failure| failure.num_failures = failure.num_failures.saturating_add(1))
            .or_insert_with(|| SnatchFailure {
                initial_failure_at: Instant::now(),
                num_failures: 1,
            });
    }

    fn failure_count(&self, key: &str) -> u32 {
        self.failures
            .get(key)
            .map_or(0, |failure| failure.num_failures)
    }
}

#[derive(Debug, Clone)]
struct SnatchFailure {
    initial_failure_at: Instant,
    num_failures: u32,
}

/// Result of one candidate torrent snatch attempt.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SnatchResult {
    /// Valid torrent metafile and original bytes.
    Metafile {
        /// Parsed metafile.
        metafile: Metafile<'static>,
        /// Raw torrent bytes for cache writes.
        bytes: Vec<u8>,
    },
    /// Request aborted or timed out.
    Aborted,
    /// Download redirected to or started as a magnet URL.
    MagnetLink,
    /// Rate limited, optionally with retry timestamp.
    RateLimited { retry_after_millis: Option<u64> },
    /// Non-OK HTTP response or other unknown failure.
    UnknownError { retry_after_millis: Option<u64> },
    /// Response was not a valid torrent.
    InvalidContents,
}

/// Write a valid candidate torrent into the info-hash cache.
pub fn cache_torrent_file(app_dir: &Path, bytes: &[u8]) -> crate::Result<Metafile<'static>> {
    let metafile = parse_metafile(bytes)?;
    let path = torrent_cache_path(app_dir, &metafile.info_hash);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            integration_error(format!("failed to create torrent cache: {error}"))
        })?;
    }
    fs::write(&path, bytes)
        .map_err(|error| integration_error(format!("failed to write cached torrent: {error}")))?;
    Ok(metafile)
}

/// Read a cached torrent, update access time, and delete corrupted cache files.
pub fn get_cached_torrent(
    app_dir: &Path,
    info_hash: &InfoHash<'_>,
) -> crate::Result<Option<Metafile<'static>>> {
    let path = torrent_cache_path(app_dir, info_hash);
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(integration_error(format!(
                "failed to read cached torrent: {error}"
            )));
        }
    };
    match parse_metafile(&bytes) {
        Ok(metafile) => {
            let now = FileTime::now();
            let metadata = fs::metadata(&path).map_err(|error| {
                integration_error(format!("failed to stat cached torrent: {error}"))
            })?;
            let modified = FileTime::from_last_modification_time(&metadata);
            filetime::set_file_times(&path, now, modified).map_err(|error| {
                integration_error(format!("failed to touch cached torrent: {error}"))
            })?;
            Ok(Some(metafile))
        }
        Err(error) => {
            let _cleanup = fs::remove_file(&path);
            Err(error)
        }
    }
}

/// Download a candidate torrent with compatibility retry and failure-memory behavior.
pub fn snatch(
    candidate: &Candidate<'_>,
    options: SnatchOptions,
    history: &mut SnatchHistory,
) -> crate::Result<SnatchResult> {
    let Some(link) = candidate.link.as_deref() else {
        return Ok(SnatchResult::UnknownError {
            retry_after_millis: None,
        });
    };
    if history.failure_count(link) >= options.retries.saturating_add(1)
        || history.failure_count(candidate.tracker.as_ref())
            >= options.retries.saturating_mul(2).saturating_add(1)
    {
        return Ok(SnatchResult::UnknownError {
            retry_after_millis: None,
        });
    }

    let attempts = options.retries.saturating_add(1);
    for attempt in 0..attempts {
        let result = snatch_once(candidate, options.timeout)?;
        match result {
            SnatchResult::Metafile { .. } => {
                history.clear(link);
                history.clear(candidate.tracker.as_ref());
                return Ok(result);
            }
            SnatchResult::RateLimited { .. } | SnatchResult::MagnetLink => return Ok(result),
            SnatchResult::Aborted
            | SnatchResult::InvalidContents
            | SnatchResult::UnknownError { .. } => {
                history.record_failure(link);
                history.record_failure(candidate.tracker.as_ref());
                let remaining = attempts.saturating_sub(attempt).saturating_sub(1);
                if remaining == 0 {
                    return Ok(result);
                }
                let retry_after = retry_after_duration(&result);
                if retry_after_exceeds_window(retry_after, options.delay, remaining) {
                    return Ok(result);
                }
                let sleep_for = retry_after.unwrap_or(Duration::ZERO).max(options.delay);
                if !sleep_for.is_zero() {
                    thread::sleep(sleep_for);
                }
            }
        }
    }
    Ok(SnatchResult::UnknownError {
        retry_after_millis: None,
    })
}

/// Look up a cached candidate info hash by GUID, link, or tracker-specific URL id.
pub fn guid_lookup(
    database: &Database,
    guid: &str,
    link: Option<&str>,
) -> crate::Result<Option<String>> {
    for key in [Some(guid), link].into_iter().flatten() {
        if let Some(found) = lookup_decision_info_hash(database, key)? {
            return Ok(Some(found));
        }
    }
    if let Some(id) = link.and_then(tracker_torrent_id) {
        let like = format!("%/torrent/{id}/%");
        return database
            .connection()
            .query_row(
                "SELECT info_hash FROM decision
                 WHERE info_hash IS NOT NULL AND guid LIKE ?1
                 ORDER BY id DESC LIMIT 1",
                params![like],
                |row| row.get(0),
            )
            .optional()
            .map_err(persistence_error);
    }
    Ok(None)
}

/// Download a candidate torrent once and parse it if valid.
pub fn snatch_once(
    candidate: &Candidate<'_>,
    timeout: Option<Duration>,
) -> crate::Result<SnatchResult> {
    let Some(link) = candidate.link.as_deref() else {
        return Ok(SnatchResult::UnknownError {
            retry_after_millis: None,
        });
    };
    if link.starts_with("magnet:") {
        return Ok(SnatchResult::MagnetLink);
    }
    let mut builder = reqwest::blocking::Client::builder()
        .user_agent(format!("CrossSeed/{}", crate::VERSION))
        .redirect(reqwest::redirect::Policy::none());
    if let Some(timeout) = timeout {
        builder = builder.timeout(timeout);
    }
    let client = builder
        .build()
        .map_err(|error| integration_error(format!("failed to build HTTP client: {error}")))?;
    let mut request = client.get(link);
    if let Some(cookie) = candidate.cookie.as_deref() {
        request = request.header(reqwest::header::COOKIE, cookie);
    }
    let response = match request.send() {
        Ok(response) => response,
        Err(error) if error.is_timeout() || error.is_connect() => return Ok(SnatchResult::Aborted),
        Err(error) => {
            return Err(integration_error(format!(
                "failed to snatch torrent: {error}"
            )));
        }
    };
    if response.status().is_redirection()
        && response
            .headers()
            .get(reqwest::header::LOCATION)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|location| location.starts_with("magnet:"))
    {
        return Ok(SnatchResult::MagnetLink);
    }
    let retry_after = response
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(parse_retry_after_seconds)
        .map(|seconds| seconds.saturating_mul(1000));
    if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
        return Ok(SnatchResult::RateLimited {
            retry_after_millis: retry_after,
        });
    }
    if !response.status().is_success() {
        return Ok(SnatchResult::UnknownError {
            retry_after_millis: retry_after,
        });
    }
    if response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("application/rss+xml"))
    {
        return Ok(SnatchResult::InvalidContents);
    }
    let bytes = response
        .bytes()
        .map_err(|error| integration_error(format!("failed to read torrent response: {error}")))?
        .to_vec();
    match parse_metafile(&bytes) {
        Ok(metafile) => Ok(SnatchResult::Metafile { metafile, bytes }),
        Err(_) => Ok(SnatchResult::InvalidContents),
    }
}

/// Validate and sanitize a configured Torznab URL.
pub fn validate_torznab_url(value: &str) -> crate::Result<TorznabConfig> {
    let url = Url::parse(value)
        .map_err(|error| integration_error(format!("invalid Torznab URL {value:?}: {error}")))?;
    if !url.path().ends_with("/api") {
        return Err(integration_error("Torznab URL pathname must end in /api"));
    }
    let apikey = url
        .query_pairs()
        .find_map(|(key, value)| (key == "apikey").then(|| value.into_owned()))
        .filter(|value| !value.is_empty())
        .ok_or_else(|| integration_error("Torznab URL must include apikey query parameter"))?;
    let mut sanitized = url;
    sanitized.set_query(None);
    sanitized.set_fragment(None);
    Ok(TorznabConfig {
        url: sanitized.to_string(),
        apikey,
    })
}

/// Synchronize configured Torznab indexers with the database.
pub fn sync_torznab_indexers(
    database: &Database,
    configured: &[TorznabConfig],
) -> crate::Result<IndexerSyncResult> {
    let connection = database.connection();
    connection
        .execute_batch(
            "CREATE TEMP TABLE IF NOT EXISTS current_indexer_urls (
                url TEXT PRIMARY KEY
            );
            DELETE FROM current_indexer_urls;",
        )
        .map_err(persistence_error)?;
    let mut result = IndexerSyncResult::default();
    for indexer in configured {
        connection
            .execute(
                "INSERT OR IGNORE INTO current_indexer_urls (url) VALUES (?1)",
                params![indexer.url],
            )
            .map_err(persistence_error)?;
        let changed = connection
            .execute(
                "UPDATE indexer
                 SET apikey = ?2,
                     active = 1,
                     status = CASE WHEN status = 'UNKNOWN_ERROR' THEN NULL ELSE status END
                 WHERE url = ?1",
                params![indexer.url, indexer.apikey],
            )
            .map_err(persistence_error)?;
        if changed == 0 {
            connection
                .execute(
                    "INSERT INTO indexer (url, apikey, active)
                     VALUES (?1, ?2, 1)",
                    params![indexer.url, indexer.apikey],
                )
                .map_err(persistence_error)?;
            result.inserted += 1;
        } else {
            result.updated += changed;
        }
    }
    result.deactivated = connection
        .execute(
            "UPDATE indexer
             SET active = 0
             WHERE active = 1
             AND url NOT IN (SELECT url FROM current_indexer_urls)",
            [],
        )
        .map_err(persistence_error)?;
    Ok(result)
}

/// Persist parsed caps for an indexer row.
pub fn update_indexer_caps(
    database: &Database,
    indexer_id: i64,
    caps: &TorznabCaps,
) -> crate::Result<()> {
    database
        .connection()
        .execute(
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
            params![
                indexer_id,
                caps.search,
                caps.tv_search,
                caps.movie_search,
                caps.music_search,
                caps.audio_search,
                caps.book_search,
                serde_json::to_string(&caps.tv_ids).map_err(json_error)?,
                serde_json::to_string(&caps.movie_ids).map_err(json_error)?,
                serde_json::to_string(&caps.categories).map_err(json_error)?,
                serde_json::to_string(&caps.limits).map_err(json_error)?,
            ],
        )
        .map_err(persistence_error)?;
    Ok(())
}

/// Mark an indexer status and retry timestamp.
pub fn set_indexer_status(
    database: &Database,
    indexer_id: i64,
    status: Option<&str>,
    retry_after: Option<u64>,
) -> crate::Result<()> {
    database
        .connection()
        .execute(
            "UPDATE indexer SET status = ?2, retry_after = ?3 WHERE id = ?1",
            params![indexer_id, status, retry_after],
        )
        .map_err(persistence_error)?;
    Ok(())
}

/// Load enabled indexers for the current timestamp.
pub fn enabled_indexers(
    database: &Database,
    now_millis: u64,
) -> crate::Result<Vec<EnabledIndexer>> {
    let mut statement = database
        .connection()
        .prepare(
            "SELECT id, url, apikey
             FROM indexer
             WHERE active = 1
               AND search_cap = 1
               AND (status IS NULL OR status = 'OK' OR retry_after < ?1)",
        )
        .map_err(persistence_error)?;
    let rows = statement
        .query_map(params![now_millis], |row| {
            Ok(EnabledIndexer {
                id: row.get(0)?,
                url: row.get(1)?,
                apikey: row.get(2)?,
            })
        })
        .map_err(persistence_error)?;
    let mut output = Vec::new();
    for row in rows {
        output.push(row.map_err(persistence_error)?);
    }
    Ok(output)
}

/// Parse a Torznab caps XML response.
pub fn parse_torznab_caps(xml: &str) -> crate::Result<TorznabCaps> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    let mut caps = TorznabCaps {
        limits: LimitCaps::default(),
        ..TorznabCaps::default()
    };
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(event)) | Ok(Event::Empty(event)) => match event.name().as_ref() {
                b"searching" => {
                    for (key, value) in attributes(&event)? {
                        match key.as_str() {
                            "searchAvailable" => caps.search = bool_attr(&value),
                            "tv-searchAvailable" => caps.tv_search = bool_attr(&value),
                            "movie-searchAvailable" => caps.movie_search = bool_attr(&value),
                            "music-searchAvailable" => caps.music_search = bool_attr(&value),
                            "audio-searchAvailable" => caps.audio_search = bool_attr(&value),
                            "book-searchAvailable" => caps.book_search = bool_attr(&value),
                            _ => {}
                        }
                    }
                }
                b"tv-search" => {
                    caps.tv_ids = supported_params(&attributes(&event)?);
                }
                b"movie-search" => {
                    caps.movie_ids = supported_params(&attributes(&event)?);
                }
                b"category" => parse_category(&mut caps.categories, &attributes(&event)?),
                b"limits" => parse_limits(&mut caps.limits, &attributes(&event)?),
                _ => {}
            },
            Ok(Event::Eof) => break,
            Err(error) => {
                return Err(integration_error(format!(
                    "invalid Torznab caps XML: {error}"
                )));
            }
            _ => {}
        }
        buf.clear();
    }
    Ok(caps)
}

/// Fetch and parse Torznab caps for one indexer.
pub fn fetch_torznab_caps(indexer: &TorznabConfig) -> crate::Result<TorznabCaps> {
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(60))
        .user_agent(format!("CrossSeed/{}", crate::VERSION))
        .build()
        .map_err(|error| integration_error(format!("failed to build HTTP client: {error}")))?;
    let body = client
        .get(&indexer.url)
        .query(&[("apikey", indexer.apikey.as_str()), ("t", "caps")])
        .send()
        .and_then(reqwest::blocking::Response::error_for_status)
        .map_err(|error| integration_error(format!("failed to fetch Torznab caps: {error}")))?
        .text()
        .map_err(|error| integration_error(format!("failed to read Torznab caps: {error}")))?;
    parse_torznab_caps(&body)
}

fn attributes(event: &quick_xml::events::BytesStart<'_>) -> crate::Result<Vec<(String, String)>> {
    event
        .attributes()
        .map(|attribute| {
            let attribute = attribute
                .map_err(|error| integration_error(format!("invalid XML attribute: {error}")))?;
            Ok((
                String::from_utf8_lossy(attribute.key.as_ref()).into_owned(),
                String::from_utf8_lossy(attribute.value.as_ref()).into_owned(),
            ))
        })
        .collect()
}

fn supported_params(attributes: &[(String, String)]) -> Vec<String> {
    attributes
        .iter()
        .find_map(|(key, value)| (key == "supportedParams").then_some(value))
        .into_iter()
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect()
}

fn parse_category(caps: &mut CategoryCaps, attributes: &[(String, String)]) {
    let name = attributes
        .iter()
        .find_map(|(key, value)| (key == "name").then_some(value.as_str()))
        .unwrap_or_default()
        .to_ascii_lowercase();
    let id = attributes
        .iter()
        .find_map(|(key, value)| (key == "id").then_some(value.as_str()))
        .and_then(|value| value.parse::<u32>().ok());
    let movie = name.contains("movie");
    let tv = name.contains("tv");
    let anime = name.contains("anime");
    let xxx = name.contains("xxx");
    let audio = name.contains("audio") || name.contains("music");
    let book = name.contains("book");
    caps.movie |= movie;
    caps.tv |= tv;
    caps.anime |= anime;
    caps.xxx |= xxx;
    caps.audio |= audio;
    caps.book |= book;
    if !movie
        && !tv
        && !anime
        && !xxx
        && !audio
        && !book
        && id.is_some_and(|id| id < 100_000 && !(8000..=8999).contains(&id))
    {
        caps.additional = true;
    }
}

fn parse_limits(limits: &mut LimitCaps, attributes: &[(String, String)]) {
    for (key, value) in attributes {
        match key.as_str() {
            "default" => limits.default = value.parse().unwrap_or(limits.default),
            "max" => limits.max = value.parse().unwrap_or(limits.max),
            _ => {}
        }
    }
}

fn lookup_decision_info_hash(database: &Database, key: &str) -> crate::Result<Option<String>> {
    database
        .connection()
        .query_row(
            "SELECT info_hash FROM decision
             WHERE guid = ?1 AND info_hash IS NOT NULL
             ORDER BY id DESC LIMIT 1",
            params![key],
            |row| row.get(0),
        )
        .optional()
        .map_err(persistence_error)
}

fn tracker_torrent_id(link: &str) -> Option<String> {
    let parsed = Url::parse(link).ok()?;
    if !parsed.host_str()?.ends_with(".tv") {
        return None;
    }
    let segments = parsed.path_segments()?.collect::<Vec<_>>();
    let [.., "torrent", id, "group"] = segments.as_slice() else {
        return None;
    };
    (!id.is_empty()).then(|| (*id).to_owned())
}

fn parse_retry_after_seconds(value: &str) -> Option<u64> {
    value.trim().parse().ok()
}

fn retry_after_duration(result: &SnatchResult) -> Option<Duration> {
    match result {
        SnatchResult::RateLimited { retry_after_millis }
        | SnatchResult::UnknownError { retry_after_millis } => {
            retry_after_millis.map(Duration::from_millis)
        }
        SnatchResult::Metafile { .. }
        | SnatchResult::Aborted
        | SnatchResult::MagnetLink
        | SnatchResult::InvalidContents => None,
    }
}

fn retry_after_exceeds_window(
    retry_after: Option<Duration>,
    configured_delay: Duration,
    remaining_attempts: u32,
) -> bool {
    let Some(retry_after) = retry_after else {
        return false;
    };
    let window = configured_delay.saturating_mul(remaining_attempts);
    retry_after > window
}

fn bool_attr(value: &str) -> bool {
    matches!(value, "1" | "true" | "yes" | "True" | "TRUE")
}

fn integration_error(message: impl Into<Cow<'static, str>>) -> SporosError {
    SporosError::Integration {
        message: message.into(),
    }
}

fn persistence_error(error: rusqlite::Error) -> SporosError {
    SporosError::Persistence {
        message: Cow::Owned(error.to_string()),
    }
}

fn json_error(error: serde_json::Error) -> SporosError {
    SporosError::Persistence {
        message: Cow::Owned(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        SnatchHistory, SnatchOptions, SnatchResult, cache_torrent_file, enabled_indexers,
        get_cached_torrent, guid_lookup, parse_torznab_caps, set_indexer_status, snatch,
        snatch_once, sync_torznab_indexers, update_indexer_caps, validate_torznab_url,
    };
    use crate::{
        domain::{Candidate, Decision},
        persistence::{Database, DecisionRecord},
    };
    use std::{
        borrow::Cow,
        fs,
        io::{Read, Write},
        net::TcpListener,
        path::PathBuf,
        sync::{Arc, Mutex},
        thread,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    #[test]
    fn validates_and_sanitizes_torznab_urls() {
        let parsed =
            validate_torznab_url("https://indexer.example/api?apikey=secret&x=1").expect("url");

        assert_eq!(parsed.url, "https://indexer.example/api");
        assert_eq!(parsed.apikey, "secret");
        let _error =
            validate_torznab_url("https://indexer.example/search?apikey=secret").expect_err("path");
        let _error = validate_torznab_url("https://indexer.example/api").expect_err("apikey");
    }

    #[test]
    fn parses_caps_xml() {
        let caps = parse_torznab_caps(
            r#"
            <caps>
              <limits default="50" max="200" />
              <searching searchAvailable="yes" tv-searchAvailable="yes" movie-searchAvailable="no" />
              <tv-search supportedParams="q,season,ep,tvdbid" />
              <movie-search supportedParams="q,imdbid" />
              <categories>
                <category id="5000" name="TV" />
                <category id="2000" name="Movies" />
                <category id="7000" name="Books" />
                <category id="1000" name="Other" />
              </categories>
            </caps>
            "#,
        )
        .expect("caps");

        assert!(caps.search);
        assert!(caps.tv_search);
        assert!(!caps.movie_search);
        assert_eq!(caps.tv_ids, vec!["q", "season", "ep", "tvdbid"]);
        assert!(caps.categories.tv);
        assert!(caps.categories.movie);
        assert!(caps.categories.book);
        assert!(caps.categories.additional);
        assert_eq!(caps.limits.default, 50);
        assert_eq!(caps.limits.max, 200);
    }

    #[test]
    fn syncs_caps_and_enabled_indexers() {
        let root = temp_path("indexers");
        fs::create_dir_all(&root).expect("temp dir");
        let database = Database::open_app_dir(&root).expect("database");
        let first = validate_torznab_url("https://one.example/api?apikey=one").expect("one");
        let second = validate_torznab_url("https://two.example/api?apikey=two").expect("two");

        let result = sync_torznab_indexers(&database, &[first.clone(), second]).expect("sync");
        assert_eq!(result.inserted, 2);
        let result = sync_torznab_indexers(&database, std::slice::from_ref(&first)).expect("sync");
        assert_eq!(result.updated, 1);
        assert_eq!(result.deactivated, 1);

        let id: i64 = database
            .connection()
            .query_row(
                "SELECT id FROM indexer WHERE url = ?1",
                [&first.url],
                |row| row.get(0),
            )
            .expect("id");
        let caps = parse_torznab_caps(
            r#"<caps><searching searchAvailable="yes" /><categories><category id="5000" name="TV" /></categories></caps>"#,
        )
        .expect("caps");
        update_indexer_caps(&database, id, &caps).expect("update caps");
        assert_eq!(
            enabled_indexers(&database, 1_000).expect("enabled").len(),
            1
        );
        set_indexer_status(&database, id, Some("RATE_LIMITED"), Some(2_000)).expect("status");
        assert!(
            enabled_indexers(&database, 1_000)
                .expect("enabled")
                .is_empty()
        );
        assert_eq!(
            enabled_indexers(&database, 3_000).expect("enabled").len(),
            1
        );

        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn caches_reads_and_deletes_corrupted_torrents() {
        let root = temp_path("cache");
        fs::create_dir_all(&root).expect("temp dir");
        let bytes = torrent_bytes("Cached.Release", 10);

        let metafile = cache_torrent_file(&root, &bytes).expect("cache");
        let cached = get_cached_torrent(&root, &metafile.info_hash)
            .expect("read")
            .expect("cached");
        assert_eq!(cached.info_hash, metafile.info_hash);

        let path = crate::torrent::torrent_cache_path(&root, &metafile.info_hash);
        fs::write(&path, b"not a torrent").expect("corrupt");
        let _error = get_cached_torrent(&root, &metafile.info_hash).expect_err("corrupted");
        assert!(!path.exists());
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn guid_lookup_checks_guid_link_and_tracker_fallback() {
        let root = temp_path("guid");
        fs::create_dir_all(&root).expect("temp dir");
        let database = Database::open_app_dir(&root).expect("database");
        let searchee_id = database
            .get_or_insert_searchee("release", 1)
            .expect("searchee");
        database
            .upsert_decision(&DecisionRecord {
                searchee_id,
                guid: "guid-1",
                info_hash: Some("0123456789abcdef0123456789abcdef01234567"),
                decision: Decision::Match,
                first_seen: 1,
                last_seen: 1,
                fuzzy_size_factor: 0.05,
            })
            .expect("decision");
        database
            .upsert_decision(&DecisionRecord {
                searchee_id,
                guid: "https://tracker.tv/torrent/123/group",
                info_hash: Some("abcdef0123456789abcdef0123456789abcdef01"),
                decision: Decision::Match,
                first_seen: 1,
                last_seen: 1,
                fuzzy_size_factor: 0.05,
            })
            .expect("decision");

        assert_eq!(
            guid_lookup(&database, "guid-1", None)
                .expect("guid")
                .as_deref(),
            Some("0123456789abcdef0123456789abcdef01234567")
        );
        assert_eq!(
            guid_lookup(
                &database,
                "missing",
                Some("https://tracker.tv/torrent/123/group")
            )
            .expect("fallback")
            .as_deref(),
            Some("abcdef0123456789abcdef0123456789abcdef01")
        );
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn snatch_once_maps_http_results() {
        let magnet = Candidate::new(
            "Magnet.Release",
            "magnet-guid",
            Some("magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567"),
            "tracker",
        );
        assert_eq!(
            snatch_once(&magnet, None).expect("magnet"),
            SnatchResult::MagnetLink
        );

        let redirect = http_server(vec![http_response(
            "302 Found",
            &[(
                "Location",
                "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567",
            )],
            "",
        )]);
        let redirect_candidate = Candidate::new(
            "Redirect.Release",
            "redirect-guid",
            Some(redirect.url.clone()),
            "tracker",
        );
        assert_eq!(
            snatch_once(&redirect_candidate, None).expect("redirect"),
            SnatchResult::MagnetLink
        );
        redirect.join();

        let limited = http_server(vec![http_response(
            "429 Too Many Requests",
            &[("Retry-After", "2")],
            "",
        )]);
        let limited_candidate = Candidate::new(
            "Limited.Release",
            "limited-guid",
            Some(limited.url.clone()),
            "tracker",
        );
        assert_eq!(
            snatch_once(&limited_candidate, None).expect("limited"),
            SnatchResult::RateLimited {
                retry_after_millis: Some(2_000)
            }
        );
        limited.join();

        let rss = http_server(vec![http_response(
            "200 OK",
            &[("Content-Type", "application/rss+xml")],
            "<rss />",
        )]);
        let rss_candidate =
            Candidate::new("Rss.Release", "rss-guid", Some(rss.url.clone()), "tracker");
        assert_eq!(
            snatch_once(&rss_candidate, None).expect("rss"),
            SnatchResult::InvalidContents
        );
        rss.join();

        let torrent = torrent_bytes("Downloaded.Release", 10);
        let ok = http_server(vec![torrent_response(&torrent)]);
        let mut ok_candidate = Candidate::new(
            "Downloaded.Release",
            "ok-guid",
            Some(ok.url.clone()),
            "tracker",
        );
        ok_candidate.cookie = Some(Cow::Borrowed("session=secret"));
        let result = snatch_once(&ok_candidate, Some(Duration::from_secs(1))).expect("torrent");
        assert!(matches!(result, SnatchResult::Metafile { .. }));
        if let SnatchResult::Metafile { metafile, bytes } = result {
            assert_eq!(bytes, torrent);
            assert_eq!(metafile.name, "Downloaded.Release");
        }
        let requests = ok.join();
        let request = requests.first().expect("request").to_ascii_lowercase();
        assert!(request.contains("cookie: session=secret"));
    }

    #[test]
    fn snatch_retries_failures_and_clears_history_on_success() {
        let torrent = torrent_bytes("Retry.Release", 10);
        let server = http_server(vec![
            http_response("500 Internal Server Error", &[("Retry-After", "0")], ""),
            torrent_response(&torrent),
        ]);
        let candidate = Candidate::new(
            "Retry.Release",
            "retry-guid",
            Some(server.url.clone()),
            "tracker",
        );
        let options = SnatchOptions {
            retries: 1,
            delay: Duration::ZERO,
            timeout: Some(Duration::from_secs(1)),
        };
        let mut history = SnatchHistory::default();

        assert!(matches!(
            snatch(&candidate, options, &mut history).expect("snatch"),
            SnatchResult::Metafile { .. }
        ));
        assert!(history.is_empty());
        assert_eq!(server.join().len(), 2);
    }

    #[test]
    fn snatch_stops_when_retry_after_exceeds_retry_window() {
        let server = http_server(vec![http_response(
            "500 Internal Server Error",
            &[("Retry-After", "2")],
            "",
        )]);
        let candidate = Candidate::new(
            "Window.Release",
            "window-guid",
            Some(server.url.clone()),
            "tracker",
        );
        let options = SnatchOptions {
            retries: 1,
            delay: Duration::from_millis(1),
            timeout: Some(Duration::from_secs(1)),
        };
        let mut history = SnatchHistory::default();

        assert_eq!(
            snatch(&candidate, options, &mut history).expect("snatch"),
            SnatchResult::UnknownError {
                retry_after_millis: Some(2_000)
            }
        );
        assert_eq!(server.join().len(), 1);
    }

    fn torrent_bytes(name: &str, length: u64) -> Vec<u8> {
        format!(
            "d4:infod6:lengthi{length}e4:name{}:{name}12:piece lengthi1e6:pieces20:aaaaaaaaaaaaaaaaaaaaee",
            name.len()
        )
        .into_bytes()
    }

    fn torrent_response(bytes: &[u8]) -> String {
        let body = std::str::from_utf8(bytes).expect("ascii torrent fixture");
        http_response(
            "200 OK",
            &[("Content-Type", "application/x-bittorrent")],
            body,
        )
    }

    fn http_response(status: &str, headers: &[(&str, &str)], body: &str) -> String {
        let mut response = format!("HTTP/1.1 {status}\r\nContent-Length: {}\r\n", body.len());
        for (name, value) in headers {
            response.push_str(name);
            response.push_str(": ");
            response.push_str(value);
            response.push_str("\r\n");
        }
        response.push_str("\r\n");
        response.push_str(body);
        response
    }

    struct TestServer {
        url: String,
        requests: Arc<Mutex<Vec<String>>>,
        handle: thread::JoinHandle<()>,
    }

    impl TestServer {
        fn join(self) -> Vec<String> {
            self.handle.join().expect("server thread");
            Arc::try_unwrap(self.requests)
                .expect("requests still shared")
                .into_inner()
                .expect("requests lock")
        }
    }

    fn http_server(responses: Vec<String>) -> TestServer {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
        let url = format!("http://{}", listener.local_addr().expect("local addr"));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let server_requests = Arc::clone(&requests);
        let handle = thread::spawn(move || {
            for response in responses {
                let (mut stream, _) = listener.accept().expect("accept request");
                let mut buffer = [0; 4096];
                let read = stream.read(&mut buffer).expect("read request");
                server_requests
                    .lock()
                    .expect("requests lock")
                    .push(String::from_utf8_lossy(&buffer[..read]).into_owned());
                stream
                    .write_all(response.as_bytes())
                    .expect("write response");
            }
        });
        TestServer {
            url,
            requests,
            handle,
        }
    }

    fn temp_path(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        std::env::temp_dir().join(format!("sporos-integrations-{label}-{nanos}"))
    }
}
