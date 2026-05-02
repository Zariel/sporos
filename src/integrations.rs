//! External indexer, Torznab, Arr, and notification integrations.

use std::{
    borrow::Cow,
    collections::HashMap,
    fs,
    path::Path,
    sync::LazyLock,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use filetime::FileTime;
use quick_xml::{Reader, events::Event};
use regex::Regex;
use rusqlite::{OptionalExtension, params};
use url::{Url, form_urlencoded};

use crate::{
    SporosError,
    config::ApiIntegrationConfig,
    domain::{Candidate, InfoHash, MediaType, Metafile, Searchee},
    persistence::Database,
    torrent::{parse_metafile, torrent_cache_path},
};

static EPISODE_QUERY_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\bS(?P<season>\d{1,2})E(?P<episode>\d{1,3})\b")
        .expect("episode query regex compiles")
});
static SEASON_QUERY_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b(?:S(?P<s>\d{1,2})|Season[ ._\-]*(?P<season>\d{1,2}))\b")
        .expect("season query regex compiles")
});

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
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
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
#[derive(Debug, Clone, Copy, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
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

/// Indexer with parsed capabilities for search requests.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SearchIndexer {
    /// Database row id.
    pub id: i64,
    /// Sanitized URL.
    pub url: String,
    /// API key.
    pub apikey: String,
    /// Parsed caps.
    pub caps: TorznabCaps,
}

#[derive(Debug, Clone)]
struct RawSearchCaps {
    search: bool,
    tv_search: bool,
    movie_search: bool,
    music_search: bool,
    audio_search: bool,
    book_search: bool,
    tv_ids: String,
    movie_ids: String,
    categories: String,
    limits: String,
}

/// Optional Arr IDs available for Torznab ID searches.
#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct TorznabSearchIds {
    /// TVDB series id.
    pub tvdbid: Option<String>,
    /// TMDB movie/series id.
    pub tmdbid: Option<String>,
    /// IMDB title id.
    pub imdbid: Option<String>,
    /// TVMaze series id.
    pub tvmazeid: Option<String>,
}

/// Sonarr/Radarr service family.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ArrKind {
    /// Sonarr series parser.
    Sonarr,
    /// Radarr movie parser.
    Radarr,
}

/// Sanitized Arr parser configuration.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ArrConfig {
    /// Base URL without the API key query.
    pub url: String,
    /// API key extracted from the query string.
    pub apikey: String,
    /// Service kind.
    pub kind: ArrKind,
}

/// Arr lookup result IDs.
#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct ArrLookup {
    /// IDs found by Arr parsing.
    pub ids: TorznabSearchIds,
    /// Title sent to the Arr parser.
    pub query_title: String,
    /// Stable cache key including IDs.
    pub cache_key: String,
}

/// One generated Torznab query.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TorznabQuery {
    /// Query parameters excluding `apikey`.
    pub params: Vec<(String, String)>,
}

impl TorznabQuery {
    fn new<K, V>(params: Vec<(K, V)>) -> Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        Self {
            params: params
                .into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
        }
    }

    fn push(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.params.push((key.into(), value.into()));
    }
}

/// Search request behavior.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct TorznabSearchOptions {
    /// Per-request timeout.
    pub timeout: Option<Duration>,
    /// Delay between requests.
    pub delay: Duration,
    /// Maximum candidates to return.
    pub search_limit: Option<usize>,
    /// Current wall-clock time in milliseconds.
    pub now_millis: u64,
}

impl Default for TorznabSearchOptions {
    fn default() -> Self {
        Self {
            timeout: None,
            delay: Duration::ZERO,
            search_limit: None,
            now_millis: current_time_millis(),
        }
    }
}

/// RSS paging behavior.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct RssPagerOptions {
    /// Time elapsed since the previous RSS job run.
    pub time_since_last_run: Duration,
    /// Per-request timeout.
    pub timeout: Option<Duration>,
    /// Delay between page requests.
    pub delay: Duration,
    /// Current wall-clock time in milliseconds.
    pub now_millis: u64,
}

impl Default for RssPagerOptions {
    fn default() -> Self {
        Self {
            time_since_last_run: Duration::ZERO,
            timeout: None,
            delay: Duration::ZERO,
            now_millis: current_time_millis(),
        }
    }
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
    let mut current_link = link.to_owned();
    let mut response = None;
    for _ in 0..10 {
        let mut request = client.get(&current_link);
        if let Some(cookie) = candidate.cookie.as_deref() {
            request = request.header(reqwest::header::COOKIE, cookie);
        }
        let next = match request.send() {
            Ok(response) => response,
            Err(error) if error.is_timeout() || error.is_connect() => {
                return Ok(SnatchResult::Aborted);
            }
            Err(error) => {
                return Err(integration_error(format!(
                    "failed to snatch torrent: {error}"
                )));
            }
        };
        if next.status().is_redirection() {
            let Some(location) = next
                .headers()
                .get(reqwest::header::LOCATION)
                .and_then(|value| value.to_str().ok())
            else {
                response = Some(next);
                break;
            };
            if location.starts_with("magnet:") {
                return Ok(SnatchResult::MagnetLink);
            }
            current_link = next
                .url()
                .join(location)
                .map_err(|error| integration_error(format!("invalid redirect URL: {error}")))?
                .to_string();
            continue;
        }
        response = Some(next);
        break;
    }
    let Some(response) = response else {
        return Ok(SnatchResult::UnknownError {
            retry_after_millis: None,
        });
    };
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

/// Validate and sanitize a structured Torznab config entry.
pub fn validate_torznab_config(value: &ApiIntegrationConfig) -> crate::Result<TorznabConfig> {
    let mut url = Url::parse(&value.url).map_err(|error| {
        integration_error(format!("invalid Torznab URL {:?}: {error}", value.url))
    })?;
    if !url.path().ends_with("/api") {
        return Err(integration_error("Torznab URL pathname must end in /api"));
    }
    if value.api_key.is_empty() {
        return Err(integration_error("Torznab config must include api_key"));
    }
    if url
        .query_pairs()
        .any(|(key, _)| key == "apikey" || key == "api_key")
    {
        return Err(integration_error(
            "Torznab URL must not include api_key query parameters",
        ));
    }
    url.set_query(None);
    url.set_fragment(None);
    Ok(TorznabConfig {
        url: url.to_string(),
        apikey: value.api_key.clone(),
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

/// Load enabled indexers with parsed caps for search.
pub fn enabled_search_indexers(
    database: &Database,
    now_millis: u64,
) -> crate::Result<Vec<SearchIndexer>> {
    let mut statement = database
        .connection()
        .prepare(
            "SELECT id, url, apikey,
                    search_cap, tv_search_cap, movie_search_cap, music_search_cap,
                    audio_search_cap, book_search_cap, tv_id_caps, movie_id_caps,
                    cat_caps, limits_caps
             FROM indexer
             WHERE active = 1
               AND search_cap = 1
               AND (status IS NULL OR status = 'OK' OR retry_after < ?1)",
        )
        .map_err(persistence_error)?;
    let rows = statement
        .query_map(params![now_millis], |row| {
            let tv_ids: String = row.get(9)?;
            let movie_ids: String = row.get(10)?;
            let categories: String = row.get(11)?;
            let limits: String = row.get(12)?;
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                RawSearchCaps {
                    search: row.get(3)?,
                    tv_search: row.get(4)?,
                    movie_search: row.get(5)?,
                    music_search: row.get(6)?,
                    audio_search: row.get(7)?,
                    book_search: row.get(8)?,
                    tv_ids,
                    movie_ids,
                    categories,
                    limits,
                },
            ))
        })
        .map_err(persistence_error)?;
    let mut output = Vec::new();
    for row in rows {
        let (id, url, apikey, raw_caps) = row.map_err(persistence_error)?;
        output.push(SearchIndexer {
            id,
            url,
            apikey,
            caps: TorznabCaps {
                search: raw_caps.search,
                tv_search: raw_caps.tv_search,
                movie_search: raw_caps.movie_search,
                music_search: raw_caps.music_search,
                audio_search: raw_caps.audio_search,
                book_search: raw_caps.book_search,
                tv_ids: serde_json::from_str(&raw_caps.tv_ids).map_err(json_error)?,
                movie_ids: serde_json::from_str(&raw_caps.movie_ids).map_err(json_error)?,
                categories: serde_json::from_str(&raw_caps.categories).map_err(json_error)?,
                limits: serde_json::from_str(&raw_caps.limits).map_err(json_error)?,
            },
        });
    }
    Ok(output)
}

/// Whether an indexer can search a media type.
pub fn indexer_supports_media_type(media_type: MediaType, caps: &TorznabCaps) -> bool {
    match media_type {
        MediaType::Episode | MediaType::Pack => caps.tv_search || caps.categories.xxx,
        MediaType::Movie => caps.movie_search || caps.categories.xxx,
        MediaType::Anime | MediaType::Video => {
            caps.movie_search
                || caps.tv_search
                || caps.categories.movie
                || caps.categories.tv
                || caps.categories.anime
                || caps.categories.xxx
        }
        MediaType::Audio => caps.audio_search || caps.music_search || caps.categories.audio,
        MediaType::Book => caps.book_search || caps.categories.book,
        MediaType::Unknown => caps.categories.additional,
    }
}

/// Build Torznab query parameter sets for a searchee and indexer caps.
pub fn create_torznab_search_queries(
    searchee: &Searchee<'_>,
    caps: &TorznabCaps,
    ids: Option<&TorznabSearchIds>,
) -> Vec<TorznabQuery> {
    if !indexer_supports_media_type(searchee.media_type, caps) {
        return Vec::new();
    }
    let title = searchee.title.as_ref();
    match searchee.media_type {
        MediaType::Episode if caps.tv_search => {
            episode_query(title, caps, ids).into_iter().collect()
        }
        MediaType::Pack if caps.tv_search => season_query(title, caps, ids).into_iter().collect(),
        MediaType::Movie if caps.movie_search => vec![movie_query(title, caps, ids)],
        MediaType::Anime | MediaType::Video if caps.search => {
            let mut query = TorznabQuery::new(vec![("t", "search")]);
            query.push("q", cleaned_generic_query(title));
            vec![query]
        }
        MediaType::Audio | MediaType::Book | MediaType::Unknown if caps.search => {
            let mut query = TorznabQuery::new(vec![("t", "search")]);
            query.push("q", cleaned_generic_query(title));
            vec![query]
        }
        _ if caps.search => {
            let mut query = TorznabQuery::new(vec![("t", "search")]);
            query.push("q", cleaned_generic_query(title));
            vec![query]
        }
        _ => Vec::new(),
    }
}

/// Build the concrete request URL for an indexer/query pair.
pub fn torznab_request_url(indexer: &SearchIndexer, query: &TorznabQuery) -> crate::Result<String> {
    let mut url = Url::parse(&indexer.url)
        .map_err(|error| integration_error(format!("invalid Torznab URL: {error}")))?;
    let mut encoded = form_urlencoded::Serializer::new(String::new());
    encoded.append_pair("apikey", &indexer.apikey);
    for (key, value) in &query.params {
        encoded.append_pair(key, value);
    }
    url.set_query(Some(&encoded.finish()));
    Ok(url.to_string())
}

/// Validate and sanitize a Sonarr/Radarr URL with an `apikey` query parameter.
pub fn validate_arr_url(value: &str, kind: ArrKind) -> crate::Result<ArrConfig> {
    let url = Url::parse(value)
        .map_err(|error| integration_error(format!("invalid Arr URL: {error}")))?;
    let apikey = url
        .query_pairs()
        .find_map(|(key, value)| (key == "apikey").then(|| value.into_owned()))
        .filter(|value| !value.is_empty())
        .ok_or_else(|| integration_error("Arr URL must include apikey query parameter"))?;
    let mut sanitized = url;
    sanitized.set_query(None);
    sanitized.set_fragment(None);
    Ok(ArrConfig {
        url: sanitized.to_string().trim_end_matches('/').to_owned(),
        apikey,
        kind,
    })
}

/// Validate and sanitize a structured Sonarr/Radarr config entry.
pub fn validate_arr_config(
    value: &ApiIntegrationConfig,
    kind: ArrKind,
) -> crate::Result<ArrConfig> {
    let mut url = Url::parse(&value.url)
        .map_err(|error| integration_error(format!("invalid Arr URL: {error}")))?;
    if value.api_key.is_empty() {
        return Err(integration_error("Arr config must include api_key"));
    }
    if url
        .query_pairs()
        .any(|(key, _)| key == "apikey" || key == "api_key")
    {
        return Err(integration_error(
            "Arr URL must not include api_key query parameters",
        ));
    }
    url.set_query(None);
    url.set_fragment(None);
    Ok(ArrConfig {
        url: url.to_string().trim_end_matches('/').to_owned(),
        apikey: value.api_key.clone(),
        kind,
    })
}

/// Validate an Arr instance by checking `/api` for a JSON `current` field.
pub fn validate_arr_instance(config: &ArrConfig, timeout: Option<Duration>) -> crate::Result<()> {
    let client = arr_client(timeout)?;
    let response = client
        .get(format!("{}/api", config.url))
        .header("X-Api-Key", &config.apikey)
        .send()
        .and_then(reqwest::blocking::Response::error_for_status)
        .map_err(|error| integration_error(format!("failed to validate Arr instance: {error}")))?
        .text()
        .map_err(|error| {
            integration_error(format!("failed to read Arr validation response: {error}"))
        })?;
    let response: serde_json::Value = serde_json::from_str(&response).map_err(|error| {
        integration_error(format!("failed to read Arr validation JSON: {error}"))
    })?;
    if response.get("current").is_some() {
        Ok(())
    } else {
        Err(integration_error(
            "Arr validation response missing current field",
        ))
    }
}

/// Look up IDs by calling selected Sonarr/Radarr parse APIs.
pub fn lookup_arr_ids(
    configs: &[ArrConfig],
    searchee: &Searchee<'_>,
    timeout: Option<Duration>,
) -> crate::Result<Option<ArrLookup>> {
    let client = arr_client(timeout.or(Some(Duration::from_secs(30))))?;
    for config in arr_configs_for_media(configs, searchee.media_type) {
        let query_title = prepare_arr_title(searchee, config.kind);
        let response = client
            .get(format!("{}/api/v3/parse", config.url))
            .header("X-Api-Key", &config.apikey)
            .query(&[("title", query_title.as_str())])
            .send()
            .and_then(reqwest::blocking::Response::error_for_status)
            .map_err(|error| integration_error(format!("failed to parse title with Arr: {error}")))?
            .text()
            .map_err(|error| {
                integration_error(format!("failed to read Arr parse response: {error}"))
            })?;
        let response: serde_json::Value = serde_json::from_str(&response).map_err(|error| {
            integration_error(format!("failed to read Arr parse JSON: {error}"))
        })?;
        let ids = extract_arr_ids(&response);
        if arr_ids_present(&ids) {
            let cache_key = arr_search_cache_key(searchee.title.as_ref(), &ids);
            return Ok(Some(ArrLookup {
                ids,
                query_title,
                cache_key,
            }));
        }
    }
    Ok(None)
}

/// Filter found Arr IDs to those usable by an indexer's caps.
pub fn ids_for_torznab_caps(ids: &TorznabSearchIds, caps: &TorznabCaps) -> TorznabSearchIds {
    let supports = |key: &str| {
        caps.tv_ids.iter().any(|id| id == key) || caps.movie_ids.iter().any(|id| id == key)
    };
    TorznabSearchIds {
        tvdbid: supports("tvdbid").then(|| ids.tvdbid.clone()).flatten(),
        tmdbid: supports("tmdbid").then(|| ids.tmdbid.clone()).flatten(),
        imdbid: supports("imdbid").then(|| ids.imdbid.clone()).flatten(),
        tvmazeid: supports("tvmazeid").then(|| ids.tvmazeid.clone()).flatten(),
    }
}

/// Stable search cache key including Arr IDs to prevent stale ID reuse.
pub fn arr_search_cache_key(query: &str, ids: &TorznabSearchIds) -> String {
    format!(
        "{}|tvdbid={}|tmdbid={}|imdbid={}|tvmazeid={}",
        query,
        ids.tvdbid.as_deref().unwrap_or_default(),
        ids.tmdbid.as_deref().unwrap_or_default(),
        ids.imdbid.as_deref().unwrap_or_default(),
        ids.tvmazeid.as_deref().unwrap_or_default()
    )
}

/// Search one indexer with generated queries.
pub fn search_torznab_indexer(
    database: &Database,
    indexer: &SearchIndexer,
    queries: &[TorznabQuery],
    options: TorznabSearchOptions,
) -> crate::Result<Vec<Candidate<'static>>> {
    let mut builder =
        reqwest::blocking::Client::builder().user_agent(format!("CrossSeed/{}", crate::VERSION));
    if let Some(timeout) = options.timeout {
        builder = builder.timeout(timeout);
    }
    let client = builder
        .build()
        .map_err(|error| integration_error(format!("failed to build HTTP client: {error}")))?;
    let mut candidates = Vec::new();
    for (position, query) in queries.iter().enumerate() {
        if position > 0 && !options.delay.is_zero() {
            thread::sleep(options.delay);
        }
        let response = match client.get(torznab_request_url(indexer, query)?).send() {
            Ok(response) => response,
            Err(error) if error.is_timeout() => {
                snooze_indexer(
                    database,
                    indexer.id,
                    "UNKNOWN_ERROR",
                    None,
                    options.now_millis,
                )?;
                continue;
            }
            Err(error) => {
                return Err(integration_error(format!(
                    "failed to search Torznab indexer: {error}"
                )));
            }
        };
        if !response.status().is_success() {
            let retry_after =
                retry_after_millis(response.headers(), response.status(), options.now_millis);
            let status = if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
                "RATE_LIMITED"
            } else {
                "UNKNOWN_ERROR"
            };
            snooze_indexer(
                database,
                indexer.id,
                status,
                Some(retry_after),
                options.now_millis,
            )?;
            continue;
        }
        let body = response.text().map_err(|error| {
            integration_error(format!("failed to read Torznab response: {error}"))
        })?;
        let mut parsed = parse_torznab_rss(&body, indexer.id)?;
        if let Some(tracker) = parsed
            .iter()
            .map(|candidate| candidate.tracker.as_ref())
            .find(|tracker| *tracker != "UnknownTracker")
        {
            update_indexer_name(database, indexer.id, tracker)?;
        }
        candidates.append(&mut parsed);
        if let Some(limit) = options.search_limit {
            if candidates.len() >= limit {
                candidates.truncate(limit);
                break;
            }
        }
    }
    Ok(candidates)
}

/// Page one indexer's RSS feed and persist its newest cursor.
pub fn rss_pager(
    database: &Database,
    indexer: &SearchIndexer,
    options: RssPagerOptions,
) -> crate::Result<Vec<Candidate<'static>>> {
    const MAX_PAGES: u32 = 10;
    const MAX_CANDIDATES: usize = 10_000;

    let previous_guid = read_rss_cursor(database, indexer.id)?;
    let limit = indexer.caps.limits.max.max(1);
    let mut output = Vec::new();
    let mut first_seen_guid = None;
    let mut page_back_until = None;

    for page in 0..MAX_PAGES {
        let query = TorznabQuery {
            params: vec![
                ("t".to_owned(), "search".to_owned()),
                ("q".to_owned(), String::new()),
                ("limit".to_owned(), limit.to_string()),
                ("offset".to_owned(), page.saturating_mul(limit).to_string()),
            ],
        };
        let raw_page = search_torznab_indexer(
            database,
            indexer,
            &[query],
            TorznabSearchOptions {
                timeout: options.timeout,
                delay: Duration::ZERO,
                search_limit: None,
                now_millis: options.now_millis,
            },
        )?;
        if page == 0 {
            first_seen_guid = raw_page.first().map(|candidate| candidate.guid.to_string());
            page_back_until = raw_page
                .iter()
                .filter_map(|candidate| candidate.pub_date_millis)
                .max()
                .map(|newest| newest.saturating_sub(duration_millis(options.time_since_last_run)));
        }
        if raw_page.is_empty() {
            break;
        }

        let raw_len = raw_page.len();
        let mut reached_previous = false;
        let mut filtered = Vec::with_capacity(raw_page.len());
        for candidate in raw_page {
            if previous_guid
                .as_ref()
                .is_some_and(|guid| guid == candidate.guid.as_ref())
            {
                reached_previous = true;
                break;
            }
            filtered.push(candidate);
        }
        if !reached_previous && previous_guid.is_some() {
            if let Some(cutoff) = page_back_until {
                filtered.retain(|candidate| {
                    candidate
                        .pub_date_millis
                        .is_some_and(|pub_date| pub_date >= cutoff)
                });
            }
        }
        let filtered_len = filtered.len();
        output.append(&mut filtered);
        if output.len() >= MAX_CANDIDATES {
            output.truncate(MAX_CANDIDATES);
            break;
        }
        if reached_previous || filtered_len == 0 || filtered_len < raw_len {
            break;
        }
        if !options.delay.is_zero() {
            thread::sleep(options.delay);
        }
    }

    if let Some(guid) = first_seen_guid {
        update_rss_cursor(database, indexer.id, &guid)?;
    }
    Ok(output)
}

/// Page all enabled RSS feeds.
pub fn query_rss_feeds(
    database: &Database,
    indexers: &[SearchIndexer],
    options: RssPagerOptions,
) -> crate::Result<Vec<Candidate<'static>>> {
    let mut output = Vec::new();
    for indexer in indexers {
        output.extend(rss_pager(database, indexer, options)?);
    }
    Ok(output)
}

/// Parse a Torznab RSS response into candidates.
pub fn parse_torznab_rss(xml: &str, indexer_id: i64) -> crate::Result<Vec<Candidate<'static>>> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    let mut candidates = Vec::new();
    let mut item: Option<RssItem> = None;
    let mut current = Vec::new();
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(event)) => {
                current = event.name().as_ref().to_vec();
                if current == b"item" {
                    item = Some(RssItem::default());
                }
            }
            Ok(Event::End(event)) => {
                if event.name().as_ref() == b"item" {
                    if let Some(item) = item.take().and_then(|item| item.into_candidate(indexer_id))
                    {
                        candidates.push(item);
                    }
                }
                current.clear();
            }
            Ok(Event::Text(event)) => {
                if let Some(item) = &mut item {
                    let value = String::from_utf8_lossy(event.as_ref()).into_owned();
                    item.set(&current, value);
                }
            }
            Ok(Event::CData(event)) => {
                if let Some(item) = &mut item {
                    let value = String::from_utf8_lossy(event.as_ref()).into_owned();
                    item.set(&current, value);
                }
            }
            Ok(Event::Eof) => break,
            Err(error) => {
                return Err(integration_error(format!(
                    "invalid Torznab RSS XML: {error}"
                )));
            }
            _ => {}
        }
        buf.clear();
    }
    Ok(candidates)
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

fn arr_client(timeout: Option<Duration>) -> crate::Result<reqwest::blocking::Client> {
    let mut builder =
        reqwest::blocking::Client::builder().user_agent(format!("CrossSeed/{}", crate::VERSION));
    if let Some(timeout) = timeout {
        builder = builder.timeout(timeout);
    }
    builder
        .build()
        .map_err(|error| integration_error(format!("failed to build Arr HTTP client: {error}")))
}

fn arr_configs_for_media(configs: &[ArrConfig], media_type: MediaType) -> Vec<&ArrConfig> {
    let wanted: &[ArrKind] = match media_type {
        MediaType::Episode | MediaType::Pack => &[ArrKind::Sonarr],
        MediaType::Movie => &[ArrKind::Radarr],
        MediaType::Anime | MediaType::Video => &[ArrKind::Sonarr, ArrKind::Radarr],
        MediaType::Audio | MediaType::Book | MediaType::Unknown => &[],
    };
    wanted
        .iter()
        .flat_map(|kind| configs.iter().filter(move |config| config.kind == *kind))
        .collect()
}

fn prepare_arr_title(searchee: &Searchee<'_>, kind: ArrKind) -> String {
    match (searchee.media_type, kind) {
        (MediaType::Video | MediaType::Anime, ArrKind::Sonarr) => {
            format!("{} S00E00", cleaned_generic_query(searchee.title.as_ref()))
        }
        (MediaType::Video | MediaType::Anime, ArrKind::Radarr) => {
            cleaned_generic_query(searchee.title.as_ref())
        }
        _ => searchee.title.as_ref().to_owned(),
    }
}

fn extract_arr_ids(value: &serde_json::Value) -> TorznabSearchIds {
    TorznabSearchIds {
        tvdbid: find_json_id(value, &["tvdbId", "tvdbid"]),
        tmdbid: find_json_id(value, &["tmdbId", "tmdbid"]),
        imdbid: find_json_id(value, &["imdbId", "imdbid"]),
        tvmazeid: find_json_id(value, &["tvMazeId", "tvmazeId", "tvmazeid"]),
    }
}

fn find_json_id(value: &serde_json::Value, keys: &[&str]) -> Option<String> {
    match value {
        serde_json::Value::Object(object) => {
            for key in keys {
                if let Some(found) = object.get(*key).and_then(json_id_string) {
                    return Some(found);
                }
            }
            object.values().find_map(|value| find_json_id(value, keys))
        }
        serde_json::Value::Array(values) => {
            values.iter().find_map(|value| find_json_id(value, keys))
        }
        serde_json::Value::Null
        | serde_json::Value::Bool(_)
        | serde_json::Value::Number(_)
        | serde_json::Value::String(_) => None,
    }
}

fn json_id_string(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::Number(number) => number
            .as_u64()
            .filter(|value| *value > 0)
            .map(|value| value.to_string()),
        serde_json::Value::String(value) if !value.is_empty() && value != "0" => {
            Some(value.clone())
        }
        _ => None,
    }
}

fn arr_ids_present(ids: &TorznabSearchIds) -> bool {
    ids.tvdbid.is_some() || ids.tmdbid.is_some() || ids.imdbid.is_some() || ids.tvmazeid.is_some()
}

fn episode_query(
    title: &str,
    caps: &TorznabCaps,
    ids: Option<&TorznabSearchIds>,
) -> Option<TorznabQuery> {
    let captures = EPISODE_QUERY_REGEX.captures(title)?;
    let season = captures.name("season")?.as_str();
    let episode = captures.name("episode")?.as_str();
    let mut query = TorznabQuery::new(vec![("t", "tvsearch")]);
    append_supported_ids(&mut query, &caps.tv_ids, ids);
    if !query_has_id(&query) {
        query.push("q", strip_query_markers(title));
    }
    query.push("season", season);
    query.push("ep", episode);
    Some(query)
}

fn season_query(
    title: &str,
    caps: &TorznabCaps,
    ids: Option<&TorznabSearchIds>,
) -> Option<TorznabQuery> {
    let captures = SEASON_QUERY_REGEX.captures(title)?;
    let season = captures
        .name("s")
        .or_else(|| captures.name("season"))?
        .as_str();
    let mut query = TorznabQuery::new(vec![("t", "tvsearch")]);
    append_supported_ids(&mut query, &caps.tv_ids, ids);
    if !query_has_id(&query) {
        query.push("q", strip_query_markers(title));
    }
    query.push("season", season);
    Some(query)
}

fn movie_query(title: &str, caps: &TorznabCaps, ids: Option<&TorznabSearchIds>) -> TorznabQuery {
    let mut query = TorznabQuery::new(vec![("t", "movie")]);
    append_supported_ids(&mut query, &caps.movie_ids, ids);
    if !query_has_id(&query) {
        query.push("q", strip_query_markers(title));
    }
    query
}

fn append_supported_ids(
    query: &mut TorznabQuery,
    supported: &[String],
    ids: Option<&TorznabSearchIds>,
) {
    let Some(ids) = ids else {
        return;
    };
    for key in supported {
        let value = match key.as_str() {
            "tvdbid" => ids.tvdbid.as_deref(),
            "tmdbid" => ids.tmdbid.as_deref(),
            "imdbid" => ids.imdbid.as_deref(),
            "tvmazeid" => ids.tvmazeid.as_deref(),
            _ => None,
        };
        if let Some(value) = value {
            query.push(key, value);
        }
    }
}

fn query_has_id(query: &TorznabQuery) -> bool {
    query
        .params
        .iter()
        .any(|(key, _)| matches!(key.as_str(), "tvdbid" | "tmdbid" | "imdbid" | "tvmazeid"))
}

fn strip_query_markers(title: &str) -> String {
    let without_episode = EPISODE_QUERY_REGEX.replace_all(title, "");
    let without_season = SEASON_QUERY_REGEX.replace_all(&without_episode, "");
    cleaned_generic_query(&without_season)
}

fn cleaned_generic_query(title: &str) -> String {
    title
        .replace("WEB-DL", "WEBDL")
        .replace("web-dl", "webdl")
        .replace("Blu-ray", "Bluray")
        .replace("blu-ray", "bluray")
        .replace(['.', '_', '[', ']', '(', ')', '-'], " ")
        .split_whitespace()
        .filter(|token| !metadata_token(token))
        .collect::<Vec<_>>()
        .join(" ")
}

fn metadata_token(token: &str) -> bool {
    let lower = token.to_ascii_lowercase();
    matches!(
        lower.as_str(),
        "2160p"
            | "1080p"
            | "720p"
            | "480p"
            | "web"
            | "web-dl"
            | "webdl"
            | "webrip"
            | "hdtv"
            | "bluray"
            | "blu-ray"
            | "bdrip"
            | "brrip"
            | "remux"
            | "proper"
            | "repack"
            | "rerip"
    ) || lower
        .strip_prefix('v')
        .is_some_and(|rest| rest.bytes().all(|byte| byte.is_ascii_digit()))
}

fn snooze_indexer(
    database: &Database,
    indexer_id: i64,
    status: &str,
    retry_after: Option<u64>,
    now_millis: u64,
) -> crate::Result<()> {
    let fallback = if status == "RATE_LIMITED" {
        Duration::from_secs(60 * 60)
    } else {
        Duration::from_secs(10 * 60)
    };
    set_indexer_status(
        database,
        indexer_id,
        Some(status),
        Some(retry_after.unwrap_or_else(|| now_millis.saturating_add(duration_millis(fallback)))),
    )
}

fn retry_after_millis(
    headers: &reqwest::header::HeaderMap,
    status: reqwest::StatusCode,
    now_millis: u64,
) -> u64 {
    headers
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(parse_retry_after_seconds)
        .map(|seconds| now_millis.saturating_add(seconds.saturating_mul(1000)))
        .unwrap_or_else(|| {
            if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                now_millis.saturating_add(duration_millis(Duration::from_secs(60 * 60)))
            } else {
                now_millis.saturating_add(duration_millis(Duration::from_secs(10 * 60)))
            }
        })
}

fn update_indexer_name(database: &Database, indexer_id: i64, name: &str) -> crate::Result<()> {
    database
        .connection()
        .execute(
            "UPDATE indexer SET name = ?2 WHERE id = ?1",
            params![indexer_id, name],
        )
        .map_err(persistence_error)?;
    Ok(())
}

fn read_rss_cursor(database: &Database, indexer_id: i64) -> crate::Result<Option<String>> {
    database
        .connection()
        .query_row(
            "SELECT last_seen_guid FROM rss WHERE indexer_id = ?1",
            params![indexer_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(persistence_error)
}

fn update_rss_cursor(database: &Database, indexer_id: i64, guid: &str) -> crate::Result<()> {
    database
        .connection()
        .execute(
            "INSERT INTO rss (indexer_id, last_seen_guid)
             VALUES (?1, ?2)
             ON CONFLICT(indexer_id) DO UPDATE SET last_seen_guid = excluded.last_seen_guid",
            params![indexer_id, guid],
        )
        .map_err(persistence_error)?;
    Ok(())
}

#[derive(Debug, Default)]
struct RssItem {
    guid: Option<String>,
    title: Option<String>,
    link: Option<String>,
    size: Option<u64>,
    pub_date_millis: Option<u64>,
    tracker: Option<String>,
}

impl RssItem {
    fn set(&mut self, key: &[u8], value: String) {
        match key {
            b"guid" => self.guid = Some(value),
            b"title" => self.title = Some(value),
            b"link" => self.link = Some(value),
            b"size" => self.size = value.trim().parse().ok(),
            b"pubDate" => self.pub_date_millis = parse_rss_pub_date(&value),
            b"prowlarrindexer" | b"jackettindexer" | b"indexer" => self.tracker = Some(value),
            _ => {}
        }
    }

    fn into_candidate(self, indexer_id: i64) -> Option<Candidate<'static>> {
        let mut candidate = Candidate::new(
            self.title?,
            self.guid?,
            self.link,
            self.tracker.unwrap_or_else(|| "UnknownTracker".to_owned()),
        );
        candidate.size = self.size;
        candidate.pub_date_millis = self.pub_date_millis;
        candidate.indexer_id = Some(indexer_id);
        Some(candidate.into_owned())
    }
}

fn parse_rss_pub_date(value: &str) -> Option<u64> {
    let parts = value.split_whitespace().collect::<Vec<_>>();
    let start = if parts.first().is_some_and(|part| part.ends_with(',')) {
        1
    } else {
        0
    };
    let day = parts.get(start)?.parse::<i32>().ok()?;
    let month = month_number(parts.get(start + 1)?)?;
    let year = parts.get(start + 2)?.parse::<i32>().ok()?;
    let time = parts.get(start + 3)?;
    let mut time_parts = time.split(':');
    let hour = time_parts.next()?.parse::<i64>().ok()?;
    let minute = time_parts.next()?.parse::<i64>().ok()?;
    let second = time_parts.next()?.parse::<i64>().ok()?;
    let offset = parts
        .get(start + 4)
        .and_then(|zone| parse_zone_offset_seconds(zone))
        .unwrap_or(0);
    let days = days_from_civil(year, month, day);
    let seconds = days
        .saturating_mul(86_400)
        .saturating_add(hour.saturating_mul(3600))
        .saturating_add(minute.saturating_mul(60))
        .saturating_add(second)
        .saturating_sub(offset);
    u64::try_from(seconds)
        .ok()
        .map(|seconds| seconds.saturating_mul(1000))
}

fn month_number(value: &str) -> Option<i32> {
    match value {
        "Jan" => Some(1),
        "Feb" => Some(2),
        "Mar" => Some(3),
        "Apr" => Some(4),
        "May" => Some(5),
        "Jun" => Some(6),
        "Jul" => Some(7),
        "Aug" => Some(8),
        "Sep" => Some(9),
        "Oct" => Some(10),
        "Nov" => Some(11),
        "Dec" => Some(12),
        _ => None,
    }
}

fn parse_zone_offset_seconds(value: &str) -> Option<i64> {
    if matches!(value, "GMT" | "UTC" | "UT") {
        return Some(0);
    }
    let sign = match value.as_bytes().first()? {
        b'+' => 1,
        b'-' => -1,
        _ => return None,
    };
    let digits = value.get(1..)?;
    if digits.len() != 4 {
        return None;
    }
    let hours = digits.get(..2)?.parse::<i64>().ok()?;
    let minutes = digits.get(2..)?.parse::<i64>().ok()?;
    Some(sign * (hours * 3600 + minutes * 60))
}

fn days_from_civil(year: i32, month: i32, day: i32) -> i64 {
    let year = year - i32::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let month_adjusted = month + if month > 2 { -3 } else { 9 };
    let doy = (153 * month_adjusted + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    i64::from(era * 146_097 + doe - 719_468)
}

fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

fn current_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(duration_millis)
        .unwrap_or(0)
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
        ArrKind, CategoryCaps, LimitCaps, RssPagerOptions, SearchIndexer, SnatchHistory,
        SnatchOptions, SnatchResult, TorznabCaps, TorznabQuery, TorznabSearchIds,
        TorznabSearchOptions, arr_search_cache_key, cache_torrent_file,
        create_torznab_search_queries, enabled_indexers, get_cached_torrent, guid_lookup,
        ids_for_torznab_caps, lookup_arr_ids, parse_torznab_caps, parse_torznab_rss, rss_pager,
        search_torznab_indexer, set_indexer_status, snatch, snatch_once, sync_torznab_indexers,
        torznab_request_url, update_indexer_caps, validate_arr_instance, validate_arr_url,
        validate_torznab_url,
    };
    use crate::{
        domain::{Candidate, Decision, File, MediaType, Searchee},
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
    fn builds_media_aware_torznab_queries_and_urls() {
        let mut caps = TorznabCaps {
            search: true,
            tv_search: true,
            movie_search: true,
            tv_ids: vec!["tvdbid".to_owned()],
            movie_ids: vec!["imdbid".to_owned()],
            categories: CategoryCaps {
                tv: true,
                movie: true,
                ..CategoryCaps::default()
            },
            limits: LimitCaps::default(),
            ..TorznabCaps::default()
        };
        let mut episode = Searchee::from_files(
            "Example.Show.S01E02.1080p.WEB-DL-GRP",
            "Example.Show.S01E02",
            vec![File::new("Example.Show.S01E02.mkv", 10)],
        );
        episode.media_type = MediaType::Episode;
        let ids = TorznabSearchIds {
            tvdbid: Some("1234".to_owned()),
            ..TorznabSearchIds::default()
        };

        let queries = create_torznab_search_queries(&episode, &caps, Some(&ids));

        assert_eq!(queries.len(), 1);
        assert!(
            queries[0]
                .params
                .contains(&("t".to_owned(), "tvsearch".to_owned()))
        );
        assert!(
            queries[0]
                .params
                .contains(&("season".to_owned(), "01".to_owned()))
        );
        assert!(
            queries[0]
                .params
                .contains(&("ep".to_owned(), "02".to_owned()))
        );
        assert!(
            queries[0]
                .params
                .contains(&("tvdbid".to_owned(), "1234".to_owned()))
        );
        assert!(!queries[0].params.iter().any(|(key, _)| key == "q"));

        caps.tv_search = false;
        let queries = create_torznab_search_queries(&episode, &caps, Some(&ids));
        assert!(queries.is_empty());

        let indexer = SearchIndexer {
            id: 7,
            url: "https://indexer.example/api".to_owned(),
            apikey: "secret".to_owned(),
            caps,
        };
        let url = torznab_request_url(
            &indexer,
            &TorznabQuery {
                params: vec![
                    ("t".to_owned(), "search".to_owned()),
                    ("q".to_owned(), "a b".to_owned()),
                ],
            },
        )
        .expect("url");
        assert_eq!(
            url,
            "https://indexer.example/api?apikey=secret&t=search&q=a+b"
        );
    }

    #[test]
    fn parses_torznab_rss_candidates() {
        let candidates = parse_torznab_rss(
            r#"
            <rss><channel>
              <item>
                <title>Example.Release</title>
                <guid>guid-1</guid>
                <link>https://indexer.example/download/1</link>
                <size>12345</size>
                <pubDate>Thu, 01 Jan 1970 00:00:02 +0000</pubDate>
                <prowlarrindexer>TrackerOne</prowlarrindexer>
              </item>
              <item>
                <title>Other.Release</title>
                <guid>guid-2</guid>
                <link>https://indexer.example/download/2</link>
              </item>
            </channel></rss>
            "#,
            42,
        )
        .expect("rss");

        assert_eq!(candidates.len(), 2);
        assert_eq!(candidates[0].name, "Example.Release");
        assert_eq!(candidates[0].guid, "guid-1");
        assert_eq!(
            candidates[0].link.as_deref(),
            Some("https://indexer.example/download/1")
        );
        assert_eq!(candidates[0].size, Some(12_345));
        assert_eq!(candidates[0].pub_date_millis, Some(2_000));
        assert_eq!(candidates[0].tracker, "TrackerOne");
        assert_eq!(candidates[0].indexer_id, Some(42));
        assert_eq!(candidates[1].tracker, "UnknownTracker");
    }

    #[test]
    fn searches_torznab_and_updates_indexer_name() {
        let server = http_server(vec![http_response(
            "200 OK",
            &[("Content-Type", "application/rss+xml")],
            r#"<rss><channel>
              <item><title>One</title><guid>g1</guid><link>https://idx/1</link><indexer>NamedTracker</indexer></item>
              <item><title>Two</title><guid>g2</guid><link>https://idx/2</link><indexer>NamedTracker</indexer></item>
            </channel></rss>"#,
        )]);
        let root = temp_path("torznab-search");
        fs::create_dir_all(&root).expect("temp dir");
        let database = Database::open_app_dir(&root).expect("database");
        database
            .connection()
            .execute(
                "INSERT INTO indexer (url, apikey, active, search_cap) VALUES (?1, 'key', 1, 1)",
                [format!("{}/api", server.url)],
            )
            .expect("indexer");
        let id = database
            .connection()
            .query_row("SELECT id FROM indexer", [], |row| row.get(0))
            .expect("id");
        let indexer = SearchIndexer {
            id,
            url: format!("{}/api", server.url),
            apikey: "key".to_owned(),
            caps: TorznabCaps::default(),
        };

        let candidates = search_torznab_indexer(
            &database,
            &indexer,
            &[TorznabQuery {
                params: vec![("t".to_owned(), "search".to_owned())],
            }],
            TorznabSearchOptions {
                search_limit: Some(1),
                ..TorznabSearchOptions::default()
            },
        )
        .expect("search");

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].guid, "g1");
        let name: String = database
            .connection()
            .query_row("SELECT name FROM indexer WHERE id = ?1", [id], |row| {
                row.get(0)
            })
            .expect("name");
        assert_eq!(name, "NamedTracker");
        let requests = server.join();
        assert!(requests[0].contains("/api?apikey=key&t=search"));
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn search_snoozes_rate_limited_indexers() {
        let server = http_server(vec![http_response(
            "429 Too Many Requests",
            &[("Retry-After", "2")],
            "",
        )]);
        let root = temp_path("torznab-rate");
        fs::create_dir_all(&root).expect("temp dir");
        let database = Database::open_app_dir(&root).expect("database");
        database
            .connection()
            .execute(
                "INSERT INTO indexer (url, apikey, active, search_cap) VALUES (?1, 'key', 1, 1)",
                [format!("{}/api", server.url)],
            )
            .expect("indexer");
        let id = database
            .connection()
            .query_row("SELECT id FROM indexer", [], |row| row.get(0))
            .expect("id");
        let indexer = SearchIndexer {
            id,
            url: format!("{}/api", server.url),
            apikey: "key".to_owned(),
            caps: TorznabCaps::default(),
        };

        let candidates = search_torznab_indexer(
            &database,
            &indexer,
            &[TorznabQuery {
                params: vec![("t".to_owned(), "search".to_owned())],
            }],
            TorznabSearchOptions {
                now_millis: 1_000,
                ..TorznabSearchOptions::default()
            },
        )
        .expect("search");

        assert!(candidates.is_empty());
        let (status, retry_after): (String, u64) = database
            .connection()
            .query_row(
                "SELECT status, retry_after FROM indexer WHERE id = ?1",
                [id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("status");
        assert_eq!(status, "RATE_LIMITED");
        assert_eq!(retry_after, 3_000);
        server.join();
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn rss_pager_stops_at_previous_cursor_and_persists_newest_guid() {
        let server = http_server(vec![http_response(
            "200 OK",
            &[("Content-Type", "application/rss+xml")],
            r#"<rss><channel>
              <item><title>New</title><guid>new-guid</guid><link>https://idx/new</link><pubDate>Thu, 01 Jan 1970 00:00:10 +0000</pubDate></item>
              <item><title>Old</title><guid>old-guid</guid><link>https://idx/old</link><pubDate>Thu, 01 Jan 1970 00:00:09 +0000</pubDate></item>
            </channel></rss>"#,
        )]);
        let root = temp_path("rss-cursor");
        fs::create_dir_all(&root).expect("temp dir");
        let database = Database::open_app_dir(&root).expect("database");
        let indexer = insert_search_indexer(&database, &server.url, 2);
        database
            .connection()
            .execute(
                "INSERT INTO rss (indexer_id, last_seen_guid) VALUES (?1, 'old-guid')",
                [indexer.id],
            )
            .expect("cursor");

        let candidates = rss_pager(
            &database,
            &indexer,
            RssPagerOptions {
                time_since_last_run: Duration::from_secs(60),
                now_millis: 20_000,
                ..RssPagerOptions::default()
            },
        )
        .expect("rss");

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].guid, "new-guid");
        let cursor: String = database
            .connection()
            .query_row(
                "SELECT last_seen_guid FROM rss WHERE indexer_id = ?1",
                [indexer.id],
                |row| row.get(0),
            )
            .expect("cursor");
        assert_eq!(cursor, "new-guid");
        server.join();
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn rss_pager_uses_age_cutoff_when_previous_cursor_is_missing() {
        let server = http_server(vec![http_response(
            "200 OK",
            &[("Content-Type", "application/rss+xml")],
            r#"<rss><channel>
              <item><title>Fresh</title><guid>fresh</guid><link>https://idx/fresh</link><pubDate>Thu, 01 Jan 1970 00:00:10 +0000</pubDate></item>
              <item><title>Stale</title><guid>stale</guid><link>https://idx/stale</link><pubDate>Thu, 01 Jan 1970 00:00:04 +0000</pubDate></item>
            </channel></rss>"#,
        )]);
        let root = temp_path("rss-age");
        fs::create_dir_all(&root).expect("temp dir");
        let database = Database::open_app_dir(&root).expect("database");
        let indexer = insert_search_indexer(&database, &server.url, 2);
        database
            .connection()
            .execute(
                "INSERT INTO rss (indexer_id, last_seen_guid) VALUES (?1, 'missing')",
                [indexer.id],
            )
            .expect("cursor");

        let candidates = rss_pager(
            &database,
            &indexer,
            RssPagerOptions {
                time_since_last_run: Duration::from_secs(5),
                now_millis: 20_000,
                ..RssPagerOptions::default()
            },
        )
        .expect("rss");

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].guid, "fresh");
        server.join();
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn rss_pager_requests_offsets_until_empty_page() {
        let server = http_server(vec![
            http_response(
                "200 OK",
                &[("Content-Type", "application/rss+xml")],
                r#"<rss><channel>
                  <item><title>Only</title><guid>only</guid><link>https://idx/only</link><pubDate>Thu, 01 Jan 1970 00:00:10 +0000</pubDate></item>
                </channel></rss>"#,
            ),
            http_response(
                "200 OK",
                &[("Content-Type", "application/rss+xml")],
                "<rss><channel></channel></rss>",
            ),
        ]);
        let root = temp_path("rss-offsets");
        fs::create_dir_all(&root).expect("temp dir");
        let database = Database::open_app_dir(&root).expect("database");
        let indexer = insert_search_indexer(&database, &server.url, 1);

        let candidates = rss_pager(
            &database,
            &indexer,
            RssPagerOptions {
                now_millis: 20_000,
                ..RssPagerOptions::default()
            },
        )
        .expect("rss");

        assert_eq!(candidates.len(), 1);
        let requests = server.join();
        assert!(requests[0].contains("limit=1"));
        assert!(requests[0].contains("offset=0"));
        assert!(requests[1].contains("offset=1"));
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn validates_arr_urls_and_instances() {
        let server = http_server(vec![http_response(
            "200 OK",
            &[("Content-Type", "application/json")],
            r#"{"current":"4.0.0"}"#,
        )]);
        let config = validate_arr_url(
            &format!("{}/sonarr?apikey=secret&ignored=1#frag", server.url),
            ArrKind::Sonarr,
        )
        .expect("config");

        assert_eq!(config.url, format!("{}/sonarr", server.url));
        assert_eq!(config.apikey, "secret");
        assert_eq!(config.kind, ArrKind::Sonarr);
        validate_arr_instance(&config, Some(Duration::from_secs(1))).expect("validate");
        let requests = server.join();
        assert!(requests[0].contains("GET /sonarr/api "));
        assert!(
            requests[0]
                .to_ascii_lowercase()
                .contains("x-api-key: secret")
        );
    }

    #[test]
    fn looks_up_arr_ids_and_prepares_titles() {
        let server = http_server(vec![http_response(
            "200 OK",
            &[("Content-Type", "application/json")],
            r#"{"series":{"tvdbId":123,"tvMazeId":456,"imdbId":"tt123"}}"#,
        )]);
        let config = validate_arr_url(&format!("{}?apikey=secret", server.url), ArrKind::Sonarr)
            .expect("config");
        let mut searchee = Searchee::from_files(
            "Example.Show.S01E02",
            "Example.Show.S01E02",
            vec![File::new("Example.Show.S01E02.mkv", 10)],
        );
        searchee.media_type = MediaType::Episode;

        let lookup = lookup_arr_ids(&[config], &searchee, Some(Duration::from_secs(1)))
            .expect("lookup")
            .expect("ids");

        assert_eq!(lookup.query_title, "Example.Show.S01E02");
        assert_eq!(lookup.ids.tvdbid.as_deref(), Some("123"));
        assert_eq!(lookup.ids.tvmazeid.as_deref(), Some("456"));
        assert_eq!(lookup.ids.imdbid.as_deref(), Some("tt123"));
        assert!(lookup.cache_key.contains("tvdbid=123"));
        let requests = server.join();
        assert!(requests[0].contains("/api/v3/parse?title=Example.Show.S01E02"));
    }

    #[test]
    fn arr_video_lookup_tries_sonarr_then_filters_ids_for_caps() {
        let server = http_server(vec![http_response(
            "200 OK",
            &[("Content-Type", "application/json")],
            r#"{"series":{"tvdbId":777},"movie":{"tmdbId":888}}"#,
        )]);
        let sonarr = validate_arr_url(&format!("{}?apikey=secret", server.url), ArrKind::Sonarr)
            .expect("sonarr");
        let radarr = validate_arr_url("https://radarr.example?apikey=radarr", ArrKind::Radarr)
            .expect("radarr");
        let mut searchee = Searchee::from_files(
            "Loose.Video.1080p.WEB-DL-GRP",
            "Loose.Video.1080p.WEB-DL-GRP",
            vec![File::new("Loose.Video.1080p.WEB-DL-GRP.mkv", 10)],
        );
        searchee.media_type = MediaType::Video;

        let lookup = lookup_arr_ids(&[sonarr, radarr], &searchee, Some(Duration::from_secs(1)))
            .expect("lookup")
            .expect("ids");

        assert_eq!(lookup.query_title, "Loose Video GRP S00E00");
        assert_eq!(lookup.ids.tvdbid.as_deref(), Some("777"));
        let caps = TorznabCaps {
            tv_ids: vec!["tvdbid".to_owned()],
            movie_ids: Vec::new(),
            ..TorznabCaps::default()
        };
        let filtered = ids_for_torznab_caps(&lookup.ids, &caps);
        assert_eq!(filtered.tvdbid.as_deref(), Some("777"));
        assert_eq!(filtered.tmdbid, None);

        let changed = TorznabSearchIds {
            tvdbid: Some("778".to_owned()),
            ..TorznabSearchIds::default()
        };
        assert_ne!(
            arr_search_cache_key(searchee.title.as_ref(), &lookup.ids),
            arr_search_cache_key(searchee.title.as_ref(), &changed)
        );
        server.join();
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

        let redirected_torrent = torrent_bytes("Redirected.Download", 10);
        let http_redirect = http_server(vec![
            http_response("302 Found", &[("Location", "/download")], ""),
            torrent_response(&redirected_torrent),
        ]);
        let http_redirect_candidate = Candidate::new(
            "Redirected.Download",
            "http-redirect-guid",
            Some(http_redirect.url.clone()),
            "tracker",
        );
        let result =
            snatch_once(&http_redirect_candidate, Some(Duration::from_secs(1))).expect("redirect");
        assert!(matches!(result, SnatchResult::Metafile { .. }));
        if let SnatchResult::Metafile { metafile, bytes } = result {
            assert_eq!(bytes, redirected_torrent);
            assert_eq!(metafile.name, "Redirected.Download");
        }
        let requests = http_redirect.join();
        assert_eq!(requests.len(), 2);
        assert!(requests[1].starts_with("GET /download "));

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

    fn insert_search_indexer(database: &Database, server_url: &str, limit: u32) -> SearchIndexer {
        let url = format!("{server_url}/api");
        database
            .connection()
            .execute(
                "INSERT INTO indexer (url, apikey, active, search_cap) VALUES (?1, 'key', 1, 1)",
                [&url],
            )
            .expect("indexer");
        let id = database
            .connection()
            .query_row("SELECT id FROM indexer WHERE url = ?1", [&url], |row| {
                row.get(0)
            })
            .expect("id");
        SearchIndexer {
            id,
            url,
            apikey: "key".to_owned(),
            caps: TorznabCaps {
                limits: LimitCaps {
                    default: limit,
                    max: limit,
                },
                ..TorznabCaps::default()
            },
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
