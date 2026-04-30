//! External indexer, Torznab, Arr, and notification integrations.

use std::{borrow::Cow, time::Duration};

use quick_xml::{Reader, events::Event};
use rusqlite::params;
use url::Url;

use crate::{SporosError, persistence::Database};

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
        enabled_indexers, parse_torznab_caps, set_indexer_status, sync_torznab_indexers,
        update_indexer_caps, validate_torznab_url,
    };
    use crate::persistence::Database;
    use std::{
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    #[test]
    fn validates_and_sanitizes_torznab_urls() {
        let parsed =
            validate_torznab_url("https://indexer.example/api?apikey=secret&x=1").expect("url");

        assert_eq!(parsed.url, "https://indexer.example/api");
        assert_eq!(parsed.apikey, "secret");
        assert!(validate_torznab_url("https://indexer.example/search?apikey=secret").is_err());
        assert!(validate_torznab_url("https://indexer.example/api").is_err());
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

    fn temp_path(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        std::env::temp_dir().join(format!("sporos-integrations-{label}-{nanos}"))
    }
}
