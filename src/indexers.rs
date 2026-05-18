#![expect(
    clippy::let_underscore_must_use,
    reason = "mechanical clippy gate enablement leaves explicit cache cleanup handling to a linked lint-class bead"
)]
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use quick_xml::Reader;
use quick_xml::escape::{resolve_predefined_entity, unescape};
use quick_xml::events::{BytesCData, BytesRef, BytesStart, BytesText, Event};
use quick_xml::name::QName;
use reqwest::header::{CONTENT_TYPE, COOKIE, LOCATION, RETRY_AFTER, USER_AGENT};
use reqwest::{StatusCode, redirect};
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::config::{IndexersConfig, ProwlarrSourceConfig, ProwlarrTagMatch, TorznabIndexerConfig};
use crate::domain::{
    ByteSize, CandidateGuid, DependencyName, DownloadUrl, IndexerId, InfoHash, ItemTitle,
    MediaType, RemoteCandidate, TorrentMetafile, TrackerName,
};
use crate::matching::{TorznabSearchPlan, TorznabSearchType};
use crate::persistence::torrent_cache::cached_torrent_path;
use crate::runtime::backoff::{BackoffProbePolicy, JitteredBackoffPolicy};
use crate::secrets::{ApiKey, sanitize_url_for_logging};
use crate::torrent::parse_metafile;

static CACHE_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

const TORZNAB_RSS_MAX_BYTES: u64 = 8 * 1024 * 1024;
const PROWLARR_CATALOG_MAX_BYTES: u64 = 4 * 1024 * 1024;
const CANDIDATE_TORRENT_MAX_BYTES: u64 = 32 * 1024 * 1024;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ConfiguredTorznabIndexer {
    pub name: DependencyName,
    pub url: SanitizedTorznabUrl,
    pub api_key: Option<ApiKey>,
    pub api_key_source: ApiKeySource,
    pub enabled: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TorznabRegistry {
    indexers: Vec<ConfiguredTorznabIndexer>,
}

impl TorznabRegistry {
    pub fn from_config(config: &IndexersConfig) -> Result<Self, IndexerConfigError> {
        let mut seen_urls = BTreeSet::new();
        let mut indexers = Vec::with_capacity(config.torznab.len());

        for (name, indexer) in &config.torznab {
            let configured = configured_torznab_indexer(name, indexer)?;
            if !seen_urls.insert(configured.url.as_str().to_owned()) {
                return Err(IndexerConfigError::DuplicateUrl {
                    url: configured.url.as_str().to_owned(),
                });
            }
            indexers.push(configured);
        }

        Ok(Self { indexers })
    }

    pub fn indexers(&self) -> &[ConfiguredTorznabIndexer] {
        &self.indexers
    }

    pub fn is_empty(&self) -> bool {
        self.indexers.is_empty()
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SanitizedTorznabUrl(String);

impl SanitizedTorznabUrl {
    pub fn new(value: impl Into<String>) -> Result<Self, IndexerConfigError> {
        let sanitized = sanitize_torznab_url(&value.into())?;
        Ok(Self(sanitized))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SanitizedTorznabUrl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ApiKeySource {
    Direct,
    File(String),
    Env(String),
    UrlQuery,
    Missing,
}

impl ApiKeySource {
    pub fn storage_value(&self) -> String {
        match self {
            Self::Direct => "direct".to_owned(),
            Self::File(path) => format!("file:{path}"),
            Self::Env(name) => format!("env:{name}"),
            Self::UrlQuery => "url_query".to_owned(),
            Self::Missing => "missing".to_owned(),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum RetryAfter {
    DelayMs(i64),
    DeadlineMs(i64),
}

impl RetryAfter {
    pub fn deadline_ms(self, now_ms: i64) -> i64 {
        match self {
            Self::DelayMs(delay_ms) => now_ms.saturating_add(delay_ms.max(0)),
            Self::DeadlineMs(deadline_ms) => deadline_ms.max(now_ms),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum IndexerConfigError {
    InvalidName { message: String },
    InvalidUrl { message: String },
    DuplicateUrl { url: String },
}

impl fmt::Display for IndexerConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidName { message } => write!(formatter, "invalid indexer name: {message}"),
            Self::InvalidUrl { message } => write!(formatter, "invalid Torznab URL: {message}"),
            Self::DuplicateUrl { url } => write!(formatter, "duplicate Torznab URL `{url}`"),
        }
    }
}

impl std::error::Error for IndexerConfigError {}

#[derive(Debug, Clone)]
pub struct TorznabHttpClient {
    client: reqwest::Client,
    timeout: Duration,
}

impl TorznabHttpClient {
    pub fn new(timeout: Duration) -> Self {
        Self {
            client: reqwest::Client::new(),
            timeout,
        }
    }

    pub async fn search(
        &self,
        endpoint: &TorznabEndpoint,
        media_type: MediaType,
        plan: &TorznabSearchPlan,
        now_ms: i64,
    ) -> Result<Vec<RemoteCandidate>, TorznabRequestError> {
        if endpoint
            .retry_after_ms
            .is_some_and(|retry_after| retry_after > now_ms)
        {
            return Err(TorznabRequestError::Backoff {
                retry_after_ms: endpoint.retry_after_ms,
            });
        }
        if !endpoint.caps.supports_media_type(media_type) {
            return Ok(Vec::new());
        }

        let limit = plan.limit.min(endpoint.caps.limits.max);
        let response = self
            .request(endpoint, |params| {
                params.push(("t".to_owned(), plan.query.search_type.as_str().to_owned()));
                if let Some(q) = plan.query.q.as_deref() {
                    params.push(("q".to_owned(), q.to_owned()));
                }
                if let Some(season) = plan.query.season {
                    params.push(("season".to_owned(), season.to_string()));
                }
                if let Some(episode) = plan.query.episode {
                    params.push(("ep".to_owned(), episode.to_string()));
                }
                if let Some(imdb_id) = plan.query.ids.imdb_id.as_deref() {
                    params.push(("imdbid".to_owned(), imdb_id.to_owned()));
                }
                if let Some(tvdb_id) = plan.query.ids.tvdb_id.as_deref() {
                    params.push(("tvdbid".to_owned(), tvdb_id.to_owned()));
                }
                if let Some(tmdb_id) = plan.query.ids.tmdb_id.as_deref() {
                    params.push(("tmdbid".to_owned(), tmdb_id.to_owned()));
                }
                if let Some(tvmaze_id) = plan.query.ids.tvmaze_id.as_deref() {
                    params.push(("tvmazeid".to_owned(), tvmaze_id.to_owned()));
                }
                params.push(("limit".to_owned(), limit.to_string()));
            })
            .await?;

        parse_torznab_rss(&response, endpoint)
    }

    pub async fn rss(
        &self,
        endpoint: &TorznabEndpoint,
        options: RssPageOptions<'_>,
        now_ms: i64,
    ) -> Result<RssPageResult, TorznabRequestError> {
        if endpoint
            .retry_after_ms
            .is_some_and(|retry_after| retry_after > now_ms)
        {
            return Err(TorznabRequestError::Backoff {
                retry_after_ms: endpoint.retry_after_ms,
            });
        }

        let mut candidates = Vec::new();
        let mut new_last_seen_guid = None;
        for page in 0..options.max_pages {
            let limit = options.page_size.min(endpoint.caps.limits.max);
            let offset = u32::from(limit) * u32::from(page);
            let response = self
                .request(endpoint, |params| {
                    params.push((
                        "t".to_owned(),
                        TorznabSearchType::Search.as_str().to_owned(),
                    ));
                    params.push(("limit".to_owned(), limit.to_string()));
                    params.push(("offset".to_owned(), offset.to_string()));
                })
                .await?;
            let page_candidates = parse_torznab_rss(&response, endpoint)?;
            if page == 0 {
                new_last_seen_guid = page_candidates
                    .first()
                    .map(|candidate| candidate.guid.as_str().to_owned());
            }
            if page_candidates.is_empty() {
                break;
            }

            let mut should_stop = false;
            for candidate in page_candidates {
                if options
                    .last_seen_guid
                    .is_some_and(|guid| guid == candidate.guid.as_str())
                {
                    should_stop = true;
                    break;
                }
                if options.max_age_ms.is_some_and(|max_age_ms| {
                    candidate
                        .published_at_ms
                        .is_some_and(|published| now_ms.saturating_sub(published) > max_age_ms)
                }) {
                    should_stop = true;
                    break;
                }
                candidates.push(candidate);
                if candidates.len() >= usize::from(options.max_candidates) {
                    should_stop = true;
                    break;
                }
            }
            if should_stop {
                break;
            }
        }

        Ok(RssPageResult {
            candidates,
            new_last_seen_guid,
        })
    }

    pub async fn caps(
        &self,
        indexer: &ConfiguredTorznabIndexer,
    ) -> Result<TorznabCaps, TorznabRequestError> {
        let response = self
            .request_parts(
                indexer.url.as_str(),
                indexer
                    .api_key
                    .as_ref()
                    .map(|api_key| api_key.expose_secret()),
                |params| params.push(("t".to_owned(), "caps".to_owned())),
            )
            .await?;
        parse_torznab_caps(&response).map_err(|error| TorznabRequestError::InvalidXml {
            message: error.to_string(),
        })
    }

    pub async fn caps_endpoint(
        &self,
        endpoint: &TorznabEndpoint,
    ) -> Result<TorznabCaps, TorznabRequestError> {
        let response = self
            .request_parts(
                endpoint.url.as_str(),
                endpoint.api_key.as_deref(),
                |params| {
                    params.push(("t".to_owned(), "caps".to_owned()));
                },
            )
            .await?;
        parse_torznab_caps(&response).map_err(|error| TorznabRequestError::InvalidXml {
            message: error.to_string(),
        })
    }

    async fn request<F>(
        &self,
        endpoint: &TorznabEndpoint,
        build_params: F,
    ) -> Result<String, TorznabRequestError>
    where
        F: FnOnce(&mut Vec<(String, String)>),
    {
        self.request_parts(
            endpoint.url.as_str(),
            endpoint.api_key.as_deref(),
            build_params,
        )
        .await
    }

    async fn request_parts<F>(
        &self,
        url: &str,
        api_key: Option<&str>,
        build_params: F,
    ) -> Result<String, TorznabRequestError>
    where
        F: FnOnce(&mut Vec<(String, String)>),
    {
        let mut params = Vec::new();
        build_params(&mut params);
        if let Some(api_key) = api_key {
            params.push(("apikey".to_owned(), api_key.to_owned()));
        }

        let response = self
            .client
            .get(url)
            .query(&params)
            .timeout(self.timeout)
            .send()
            .await
            .map_err(TorznabRequestError::from_reqwest)?;
        let status = response.status();
        if !status.is_success() {
            let retry_after = response
                .headers()
                .get(RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .and_then(parse_retry_after);
            if status == StatusCode::TOO_MANY_REQUESTS {
                return Err(TorznabRequestError::RateLimited { retry_after });
            }
            return Err(TorznabRequestError::HttpStatus {
                status: status.as_u16(),
                retry_after,
            });
        }

        let bytes = read_torznab_response(response, TORZNAB_RSS_MAX_BYTES).await?;
        Ok(String::from_utf8_lossy(&bytes).into_owned())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TorznabEndpoint {
    pub indexer_id: IndexerId,
    pub name: DependencyName,
    pub url: SanitizedTorznabUrl,
    pub api_key: Option<String>,
    pub caps: TorznabCaps,
    pub retry_after_ms: Option<i64>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct RssPageOptions<'a> {
    pub last_seen_guid: Option<&'a str>,
    pub max_age_ms: Option<i64>,
    pub max_pages: u16,
    pub max_candidates: u16,
    pub page_size: u16,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RssPageResult {
    pub candidates: Vec<RemoteCandidate>,
    pub new_last_seen_guid: Option<String>,
}

#[derive(Clone, Eq, PartialEq)]
pub struct ProwlarrSource {
    pub name: DependencyName,
    pub url: String,
    pub api_key: ApiKey,
    pub api_key_source: ApiKeySource,
    pub tags: BTreeSet<String>,
    pub tag_match: ProwlarrTagMatch,
    pub include_untagged: bool,
}

impl ProwlarrSource {
    pub fn from_config(
        name: &str,
        config: &ProwlarrSourceConfig,
    ) -> Result<Option<Self>, ProwlarrConfigError> {
        if !config.enabled {
            return Ok(None);
        }
        let Some(api_key) = config.api_key.clone() else {
            return Err(ProwlarrConfigError::MissingApiKey {
                source: name.to_owned(),
            });
        };
        let name = DependencyName::new(name.to_owned()).map_err(|error| {
            ProwlarrConfigError::InvalidName {
                message: error.to_string(),
            }
        })?;
        Ok(Some(Self {
            name,
            url: config.url.trim_end_matches('/').to_owned(),
            api_key,
            api_key_source: prowlarr_api_key_source(config),
            tags: config.tags.iter().cloned().collect(),
            tag_match: config.tag_match,
            include_untagged: config.include_untagged,
        }))
    }
}

impl fmt::Debug for ProwlarrSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProwlarrSource")
            .field("name", &self.name)
            .field("url", &redacted_url_origin(&self.url))
            .field("api_key", &self.api_key)
            .field("api_key_source", &self.api_key_source)
            .field("tags", &self.tags)
            .field("tag_match", &self.tag_match)
            .field("include_untagged", &self.include_untagged)
            .finish()
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ProwlarrIndexer {
    pub source: DependencyName,
    pub prowlarr_id: i64,
    pub name: DependencyName,
    pub url: SanitizedTorznabUrl,
    pub api_key: Option<ApiKey>,
    pub api_key_source: ApiKeySource,
    pub tags: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ProwlarrHttpClient {
    client: reqwest::Client,
    timeout: Duration,
}

impl ProwlarrHttpClient {
    pub fn new(timeout: Duration) -> Self {
        Self {
            client: reqwest::Client::builder()
                .redirect(redirect::Policy::none())
                .build()
                .expect("redirect-disabled Prowlarr HTTP client should build"),
            timeout,
        }
    }

    pub async fn indexers(
        &self,
        source: &ProwlarrSource,
    ) -> Result<Vec<ProwlarrIndexer>, ProwlarrRequestError> {
        let tag_labels = self.tag_labels(source).await?;
        let bytes = self
            .get_bytes(source, "/api/v1/indexer", PROWLARR_CATALOG_MAX_BYTES)
            .await?;
        let rows =
            serde_json::from_slice::<Vec<ProwlarrIndexerResource>>(&bytes).map_err(|error| {
                ProwlarrRequestError::InvalidResponse {
                    message: error.to_string(),
                }
            })?;
        rows.into_iter()
            .filter_map(|row| {
                let tags = prowlarr_tag_values(&row.tags, &tag_labels);
                if source_accepts_prowlarr_tags(source, &tags) {
                    normalize_prowlarr_indexer(source, row, tags).transpose()
                } else {
                    None
                }
            })
            .collect()
    }

    async fn tag_labels(
        &self,
        source: &ProwlarrSource,
    ) -> Result<BTreeMap<i64, String>, ProwlarrRequestError> {
        if !source_needs_prowlarr_tag_labels(source) {
            return Ok(BTreeMap::new());
        }
        let bytes = self
            .get_bytes(source, "/api/v1/tag", PROWLARR_CATALOG_MAX_BYTES)
            .await?;
        let rows = serde_json::from_slice::<Vec<ProwlarrTagResource>>(&bytes).map_err(|error| {
            ProwlarrRequestError::InvalidResponse {
                message: error.to_string(),
            }
        })?;
        Ok(rows
            .into_iter()
            .filter(|row| !row.label.trim().is_empty())
            .map(|row| (row.id, row.label))
            .collect())
    }

    async fn get_bytes(
        &self,
        source: &ProwlarrSource,
        path: &str,
        limit: u64,
    ) -> Result<Vec<u8>, ProwlarrRequestError> {
        let url = format!("{}{}", source.url, path);
        let response = self
            .client
            .get(&url)
            .header("X-Api-Key", source.api_key.expose_secret())
            .header(USER_AGENT, concat!("Sporos/", env!("CARGO_PKG_VERSION")))
            .timeout(self.timeout)
            .send()
            .await
            .map_err(ProwlarrRequestError::from_reqwest)?;
        let status = response.status();
        if !status.is_success() {
            let retry_after = response
                .headers()
                .get(RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .and_then(parse_retry_after);
            return Err(ProwlarrRequestError::HttpStatus {
                status: status.as_u16(),
                retry_after,
            });
        }

        read_prowlarr_response(response, limit).await
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ProwlarrConfigError {
    InvalidName { message: String },
    MissingApiKey { source: String },
}

impl fmt::Display for ProwlarrConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidName { message } => {
                write!(formatter, "invalid Prowlarr source name: {message}")
            }
            Self::MissingApiKey { source } => {
                write!(formatter, "Prowlarr source `{source}` requires an API key")
            }
        }
    }
}

impl std::error::Error for ProwlarrConfigError {}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ProwlarrRequestError {
    HttpStatus {
        status: u16,
        retry_after: Option<RetryAfter>,
    },
    Timeout,
    Request {
        message: String,
    },
    InvalidResponse {
        message: String,
    },
    InvalidIndexer {
        message: String,
    },
    ResponseTooLarge {
        limit: u64,
    },
}

impl ProwlarrRequestError {
    fn from_reqwest(error: reqwest::Error) -> Self {
        if error.is_timeout() {
            Self::Timeout
        } else {
            Self::Request {
                message: sanitized_reqwest_error(error),
            }
        }
    }
}

impl fmt::Display for ProwlarrRequestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HttpStatus { status, .. } => {
                write!(formatter, "Prowlarr returned HTTP status {status}")
            }
            Self::Timeout => formatter.write_str("Prowlarr request timed out"),
            Self::Request { message } => write!(formatter, "Prowlarr request failed: {message}"),
            Self::InvalidResponse { message } => {
                write!(formatter, "invalid Prowlarr response: {message}")
            }
            Self::InvalidIndexer { message } => {
                write!(formatter, "invalid Prowlarr indexer: {message}")
            }
            Self::ResponseTooLarge { limit } => {
                write!(formatter, "Prowlarr response exceeded {limit} bytes")
            }
        }
    }
}

impl std::error::Error for ProwlarrRequestError {}

fn source_accepts_prowlarr_tags(source: &ProwlarrSource, tags: &[String]) -> bool {
    if source.tags.is_empty() {
        return source.include_untagged || !tags.is_empty();
    }
    if tags.is_empty() {
        return source.include_untagged;
    }
    match source.tag_match {
        ProwlarrTagMatch::Any => tags.iter().any(|tag| source.tags.contains(tag)),
        ProwlarrTagMatch::All => source.tags.iter().all(|tag| tags.contains(tag)),
    }
}

fn source_needs_prowlarr_tag_labels(source: &ProwlarrSource) -> bool {
    source.tags.iter().any(|tag| tag.parse::<i64>().is_err())
}

fn prowlarr_tag_values(tags: &[i64], labels: &BTreeMap<i64, String>) -> Vec<String> {
    tags.iter()
        .map(|tag| labels.get(tag).cloned().unwrap_or_else(|| tag.to_string()))
        .collect()
}

fn normalize_prowlarr_indexer(
    source: &ProwlarrSource,
    row: ProwlarrIndexerResource,
    tags: Vec<String>,
) -> Result<Option<ProwlarrIndexer>, ProwlarrRequestError> {
    if !row.enable
        || !row
            .protocol
            .as_deref()
            .is_some_and(|protocol| protocol.eq_ignore_ascii_case("torrent"))
        || (!row.supports_rss && !row.supports_search)
    {
        return Ok(None);
    }
    let name = DependencyName::new(row.name.clone()).map_err(|error| {
        ProwlarrRequestError::InvalidIndexer {
            message: format!("{}: {error}", row.name),
        }
    })?;
    let proxy_url = format!("{}/{}/api", source.url, row.id);
    Ok(Some(ProwlarrIndexer {
        source: source.name.clone(),
        prowlarr_id: row.id,
        name,
        url: SanitizedTorznabUrl::new(proxy_url).map_err(|error| {
            ProwlarrRequestError::InvalidIndexer {
                message: format!("{}: {error}", row.name),
            }
        })?,
        api_key: Some(source.api_key.clone()),
        api_key_source: source.api_key_source.clone(),
        tags,
    }))
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProwlarrIndexerResource {
    id: i64,
    name: String,
    enable: bool,
    protocol: Option<String>,
    #[serde(default)]
    supports_rss: bool,
    #[serde(default)]
    supports_search: bool,
    #[serde(default)]
    tags: Vec<i64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProwlarrTagResource {
    id: i64,
    label: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum TorznabRequestError {
    Backoff {
        retry_after_ms: Option<i64>,
    },
    RateLimited {
        retry_after: Option<RetryAfter>,
    },
    HttpStatus {
        status: u16,
        retry_after: Option<RetryAfter>,
    },
    Timeout,
    Request {
        message: String,
    },
    InvalidXml {
        message: String,
    },
    InvalidCandidate {
        message: String,
    },
    ResponseTooLarge {
        limit: u64,
    },
}

impl TorznabRequestError {
    fn from_reqwest(error: reqwest::Error) -> Self {
        if error.is_timeout() {
            Self::Timeout
        } else {
            Self::Request {
                message: sanitized_reqwest_error(error),
            }
        }
    }
}

impl fmt::Display for TorznabRequestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Backoff { .. } => formatter.write_str("indexer is in backoff"),
            Self::RateLimited { .. } => formatter.write_str("indexer returned a rate limit"),
            Self::HttpStatus { status, .. } => {
                write!(formatter, "indexer returned HTTP status {status}")
            }
            Self::Timeout => formatter.write_str("indexer request timed out"),
            Self::Request { message } => write!(formatter, "indexer request failed: {message}"),
            Self::InvalidXml { message } => write!(formatter, "invalid Torznab RSS: {message}"),
            Self::InvalidCandidate { message } => {
                write!(formatter, "invalid Torznab candidate: {message}")
            }
            Self::ResponseTooLarge { limit } => {
                write!(formatter, "indexer response exceeded {limit} bytes")
            }
        }
    }
}

impl std::error::Error for TorznabRequestError {}

#[derive(Debug, Clone)]
pub struct CandidateDownloadClient {
    client: reqwest::Client,
    timeout: Duration,
    allow_internal_targets: bool,
}

impl CandidateDownloadClient {
    pub fn new(timeout: Duration) -> Self {
        Self::with_resolver(timeout, Arc::new(SafeCandidateDownloadResolver::system()))
    }

    fn with_resolver(timeout: Duration, resolver: Arc<SafeCandidateDownloadResolver>) -> Self {
        Self {
            client: reqwest::Client::builder()
                .redirect(redirect::Policy::none())
                .no_proxy()
                .dns_resolver(resolver)
                .build()
                .expect("safe candidate download HTTP client should build"),
            timeout,
            allow_internal_targets: false,
        }
    }

    #[cfg(test)]
    pub(crate) fn allow_internal_for_tests(timeout: Duration) -> Self {
        Self {
            client: reqwest::Client::builder()
                .redirect(redirect::Policy::none())
                .no_proxy()
                .build()
                .expect("test candidate download HTTP client should build"),
            timeout,
            allow_internal_targets: true,
        }
    }

    pub async fn download_and_cache(
        &self,
        candidate: &RemoteCandidate,
        cache_dir: &Path,
        cookie: Option<&str>,
    ) -> Result<CachedCandidateTorrent, CandidateDownloadError> {
        if candidate.download_url.as_str().starts_with("magnet:") {
            return Err(CandidateDownloadError::MagnetLink);
        }
        if !self.allow_internal_targets {
            validate_candidate_download_url(&candidate.download_url)?;
        }

        let mut request = self
            .client
            .get(candidate.download_url.as_str())
            .header(USER_AGENT, concat!("Sporos/", env!("CARGO_PKG_VERSION")))
            .timeout(self.timeout);
        if let Some(cookie) = cookie {
            request = request.header(COOKIE, cookie);
        }

        let response = request
            .send()
            .await
            .map_err(CandidateDownloadError::from_reqwest)?;
        let status = response.status();
        if !status.is_success() {
            if status.is_redirection()
                && response
                    .headers()
                    .get(LOCATION)
                    .and_then(|value| value.to_str().ok())
                    .is_some_and(is_magnet_uri)
            {
                return Err(CandidateDownloadError::MagnetLink);
            }
            let retry_after = response
                .headers()
                .get(RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .and_then(parse_retry_after);
            if status == StatusCode::TOO_MANY_REQUESTS {
                return Err(CandidateDownloadError::RateLimited { retry_after });
            }
            return Err(CandidateDownloadError::HttpStatus {
                status: status.as_u16(),
                retry_after,
            });
        }
        if response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|content_type| content_type.contains("application/rss+xml"))
        {
            return Err(CandidateDownloadError::InvalidContents {
                message: "download returned RSS XML".to_owned(),
            });
        }

        let bytes = read_candidate_response(response, CANDIDATE_TORRENT_MAX_BYTES).await?;
        let parsed =
            parse_metafile(&bytes).map_err(|error| CandidateDownloadError::InvalidContents {
                message: error.to_string(),
            })?;
        let cache_path = cached_torrent_path(cache_dir, parsed.metafile.info_hash());
        write_cached_torrent(&cache_path, &bytes)?;

        let mut updated_candidate = candidate.clone();
        updated_candidate.info_hash = Some(parsed.metafile.info_hash().clone());
        updated_candidate.torrent_cache_path = Some(cache_path.clone());

        Ok(CachedCandidateTorrent {
            candidate: updated_candidate,
            metafile: parsed.metafile,
            tracker_hosts: parsed.tracker_hosts,
            cache_path,
        })
    }
}

#[derive(Debug)]
struct SafeCandidateDownloadResolver {
    hosts: Option<BTreeMap<String, Vec<SocketAddr>>>,
}

impl SafeCandidateDownloadResolver {
    fn system() -> Self {
        Self { hosts: None }
    }

    #[cfg(test)]
    fn from_static_hosts(hosts: BTreeMap<String, Vec<SocketAddr>>) -> Self {
        Self { hosts: Some(hosts) }
    }
}

impl reqwest::dns::Resolve for SafeCandidateDownloadResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let host = name.as_str().to_owned();
        if let Some(hosts) = self.hosts.as_ref() {
            return safe_candidate_download_addrs(host, hosts.get(name.as_str()).cloned());
        }

        Box::pin(async move {
            let lookup_host = host.clone();
            let addrs = tokio::net::lookup_host((lookup_host.as_str(), 0))
                .await
                .map_err(|error| Box::new(error) as Box<dyn std::error::Error + Send + Sync>)?;
            safe_candidate_download_addresses(host, addrs.collect())
        })
    }
}

fn safe_candidate_download_addrs(
    host: String,
    addrs: Option<Vec<SocketAddr>>,
) -> reqwest::dns::Resolving {
    Box::pin(async move { safe_candidate_download_addresses(host, addrs.unwrap_or_default()) })
}

fn safe_candidate_download_addresses(
    host: String,
    addrs: Vec<SocketAddr>,
) -> Result<reqwest::dns::Addrs, Box<dyn std::error::Error + Send + Sync>> {
    if addrs.is_empty() {
        let error = io::Error::new(
            io::ErrorKind::NotFound,
            format!("candidate download host `{host}` did not resolve"),
        );
        return Err(Box::new(error) as Box<dyn std::error::Error + Send + Sync>);
    }
    if addrs
        .iter()
        .map(SocketAddr::ip)
        .any(is_internal_candidate_download_ip)
    {
        let error = io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("candidate download host `{host}` resolved to an internal address"),
        );
        return Err(Box::new(error) as Box<dyn std::error::Error + Send + Sync>);
    }

    Ok(Box::new(addrs.into_iter()) as reqwest::dns::Addrs)
}

fn validate_candidate_download_url(url: &DownloadUrl) -> Result<(), CandidateDownloadError> {
    let parsed =
        reqwest::Url::parse(url.as_str()).map_err(|error| CandidateDownloadError::InvalidUrl {
            message: error.to_string(),
        })?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(CandidateDownloadError::InvalidUrl {
            message: "scheme must be http or https".to_owned(),
        });
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(CandidateDownloadError::InvalidUrl {
            message: "credentials are not allowed".to_owned(),
        });
    }
    let Some(host) = parsed.host_str() else {
        return Err(CandidateDownloadError::InvalidUrl {
            message: "host is required".to_owned(),
        });
    };
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    if host == "localhost" || host.ends_with(".localhost") {
        return Err(CandidateDownloadError::InvalidUrl {
            message: "internal hosts are not allowed".to_owned(),
        });
    }
    if ip_candidate_download_host_literal(&host)
        .parse::<IpAddr>()
        .is_ok_and(is_internal_candidate_download_ip)
    {
        return Err(CandidateDownloadError::InvalidUrl {
            message: "internal hosts are not allowed".to_owned(),
        });
    }

    Ok(())
}

fn ip_candidate_download_host_literal(host: &str) -> &str {
    host.strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host)
}

fn is_internal_candidate_download_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => is_internal_candidate_download_ipv4(ip),
        IpAddr::V6(ip) => is_internal_candidate_download_ipv6(ip),
    }
}

fn is_internal_candidate_download_ipv4(ip: Ipv4Addr) -> bool {
    let octets = ip.octets();
    ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local()
        || ip.is_unspecified()
        || octets[0] == 0
        || (octets[0] == 100 && (64..=127).contains(&octets[1]))
        || (octets[0] == 192 && octets[1] == 0 && octets[2] == 0)
        || (octets[0] == 192 && octets[1] == 0 && octets[2] == 2)
        || (octets[0] == 192 && octets[1] == 88 && octets[2] == 99)
        || (octets[0] == 198 && (octets[1] == 18 || octets[1] == 19))
        || (octets[0] == 198 && octets[1] == 51 && octets[2] == 100)
        || (octets[0] == 203 && octets[1] == 0 && octets[2] == 113)
        || octets[0] >= 224
}

fn is_internal_candidate_download_ipv6(ip: Ipv6Addr) -> bool {
    if let Some(ipv4) = ip.to_ipv4_mapped() {
        return is_internal_candidate_download_ipv4(ipv4);
    }

    let segments = ip.segments();
    ip.is_loopback()
        || ip.is_unspecified()
        || (segments[0] == 0x0064 && segments[1] == 0xff9b)
        || (segments[0] == 0x0100 && segments[1] == 0)
        || (segments[0] == 0x2001 && segments[1] < 0x0200)
        || (segments[0] == 0x2001 && segments[1] == 0x0db8)
        || segments[0] == 0x2002
        || (segments[0] & 0xfe00) == 0xfc00
        || (segments[0] & 0xffc0) == 0xfe80
        || (segments[0] & 0xffc0) == 0xfec0
        || (segments[0] & 0xff00) == 0xff00
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CachedCandidateTorrent {
    pub candidate: RemoteCandidate,
    pub metafile: TorrentMetafile,
    pub tracker_hosts: Vec<TrackerName>,
    pub cache_path: std::path::PathBuf,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct CandidateDownloadPolicy {
    pub max_failures: u16,
}

impl Default for CandidateDownloadPolicy {
    fn default() -> Self {
        Self { max_failures: 3 }
    }
}

impl CandidateDownloadPolicy {
    pub const fn should_attempt(self, prior_failures: u16) -> bool {
        prior_failures < self.max_failures
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct IndexerBackoffPolicy {
    pub base_delay_ms: i64,
    pub max_delay_ms: i64,
    pub jitter_ms: i64,
    pub recovery_probe_interval_ms: i64,
}

impl Default for IndexerBackoffPolicy {
    fn default() -> Self {
        Self {
            base_delay_ms: 10 * 60 * 1_000,
            max_delay_ms: 60 * 60 * 1_000,
            jitter_ms: 30_000,
            recovery_probe_interval_ms: 5 * 60 * 1_000,
        }
    }
}

impl IndexerBackoffPolicy {
    pub fn retry_after_deadline(
        self,
        now_ms: i64,
        consecutive_failures: u16,
        retry_after: Option<RetryAfter>,
        jitter_key: &str,
    ) -> i64 {
        self.retry_policy().retry_deadline_ms(
            now_ms,
            consecutive_failures,
            retry_after.map(|retry_after| retry_after.deadline_ms(now_ms)),
            jitter_key,
        )
    }

    pub fn should_probe(
        self,
        now_ms: i64,
        retry_after_ms: Option<i64>,
        last_probe_ms: Option<i64>,
        explicit_retry_after: bool,
    ) -> bool {
        BackoffProbePolicy {
            retry: self.retry_policy(),
            recovery_probe_interval_ms: self.recovery_probe_interval_ms,
        }
        .should_probe(now_ms, retry_after_ms, last_probe_ms, explicit_retry_after)
    }

    fn retry_policy(self) -> JitteredBackoffPolicy {
        JitteredBackoffPolicy {
            base_delay_ms: self.base_delay_ms,
            max_delay_ms: self.max_delay_ms,
            jitter_ms: self.jitter_ms,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum CandidateDownloadError {
    InvalidUrl {
        message: String,
    },
    RateLimited {
        retry_after: Option<RetryAfter>,
    },
    HttpStatus {
        status: u16,
        retry_after: Option<RetryAfter>,
    },
    MagnetLink,
    Timeout,
    Request {
        message: String,
    },
    InvalidContents {
        message: String,
    },
    ResponseTooLarge {
        limit: u64,
    },
    CacheWrite {
        path: std::path::PathBuf,
        message: String,
    },
}

impl CandidateDownloadError {
    fn from_reqwest(error: reqwest::Error) -> Self {
        if error.is_timeout() {
            Self::Timeout
        } else {
            Self::Request {
                message: sanitized_reqwest_error(error),
            }
        }
    }
}

impl fmt::Display for CandidateDownloadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidUrl { message } => {
                write!(formatter, "invalid candidate download URL: {message}")
            }
            Self::RateLimited { .. } => formatter.write_str("candidate download was rate limited"),
            Self::HttpStatus { status, .. } => {
                write!(
                    formatter,
                    "candidate download returned HTTP status {status}"
                )
            }
            Self::MagnetLink => formatter.write_str("candidate download is a magnet link"),
            Self::Timeout => formatter.write_str("candidate download timed out"),
            Self::Request { message } => write!(formatter, "candidate download failed: {message}"),
            Self::InvalidContents { message } => {
                write!(formatter, "invalid candidate torrent contents: {message}")
            }
            Self::ResponseTooLarge { limit } => {
                write!(
                    formatter,
                    "candidate download response exceeded {limit} bytes"
                )
            }
            Self::CacheWrite { path, message } => {
                write!(
                    formatter,
                    "write cached torrent {}: {message}",
                    path.display()
                )
            }
        }
    }
}

impl std::error::Error for CandidateDownloadError {}

pub fn parse_torznab_rss(
    xml: &str,
    endpoint: &TorznabEndpoint,
) -> Result<Vec<RemoteCandidate>, TorznabRequestError> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);
    let mut buffer = Vec::new();
    let mut candidates = Vec::new();
    let mut saw_rss = false;
    let mut item = None;
    let mut text_field = None;

    loop {
        match reader.read_event_into(&mut buffer) {
            Ok(Event::Start(element)) => {
                if element.name() == QName(b"rss") {
                    saw_rss = true;
                } else if element.name() == QName(b"item") {
                    item = Some(RssItemBuilder::default());
                } else if let Some(builder) = item.as_mut()
                    && element.name() == QName(b"enclosure")
                {
                    apply_enclosure_attributes(&reader, &element, builder)?;
                } else if item.is_some() {
                    text_field = rss_text_field(element.name());
                }
            }
            Ok(Event::Empty(element)) => {
                if let Some(builder) = item.as_mut() {
                    if element.name() == QName(b"enclosure") {
                        apply_enclosure_attributes(&reader, &element, builder)?;
                    } else if element.name() == QName(b"torznab:attr")
                        || element.name() == QName(b"attr")
                    {
                        let name = rss_attribute_value(&reader, &element, b"name")?
                            .unwrap_or_default()
                            .to_ascii_lowercase();
                        let value = rss_attribute_value(&reader, &element, b"value")?;
                        builder.apply_torznab_attr(&name, value.as_deref());
                    }
                }
            }
            Ok(Event::Text(text)) => {
                if let (Some(builder), Some(field)) = (item.as_mut(), text_field) {
                    let value = rss_text_value(&text)?;
                    builder.append_text(field, value);
                }
            }
            Ok(Event::CData(cdata)) => {
                if let (Some(builder), Some(field)) = (item.as_mut(), text_field) {
                    let value = rss_cdata_value(&cdata)?;
                    builder.append_text(field, value);
                }
            }
            Ok(Event::GeneralRef(reference)) => {
                if let (Some(builder), Some(field)) = (item.as_mut(), text_field) {
                    let value = rss_reference_value(&reference)?;
                    builder.append_text(field, value);
                }
            }
            Ok(Event::End(element)) => {
                if element.name() == QName(b"item") {
                    let Some(builder) = item.take() else {
                        return Err(TorznabRequestError::InvalidXml {
                            message: "item end without item start".to_owned(),
                        });
                    };
                    match builder.into_candidate(endpoint) {
                        Ok(candidate) => candidates.push(candidate),
                        Err(TorznabRequestError::InvalidCandidate { message }) => {
                            warn!(
                                indexer = %endpoint.name,
                                error = %message,
                                "skipping malformed Torznab RSS item"
                            );
                        }
                        Err(error) => return Err(error),
                    }
                }
                text_field = None;
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(error) => {
                return Err(TorznabRequestError::InvalidXml {
                    message: error.to_string(),
                });
            }
        }
        buffer.clear();
    }

    if !saw_rss {
        return Err(TorznabRequestError::InvalidXml {
            message: "missing rss root".to_owned(),
        });
    }

    Ok(candidates)
}

#[derive(Debug, Clone, Copy)]
enum RssTextField {
    Title,
    Guid,
    Link,
    PubDate,
    Size,
}

#[derive(Debug, Default)]
struct RssItemBuilder {
    title: Option<String>,
    guid: Option<String>,
    link: Option<String>,
    enclosure_url: Option<String>,
    size: Option<u64>,
    size_text: Option<String>,
    published_at_ms: Option<i64>,
    pub_date_text: Option<String>,
    info_hash: Option<String>,
}

impl RssItemBuilder {
    fn append_text(&mut self, field: RssTextField, value: String) {
        match field {
            RssTextField::Title => append_text(&mut self.title, value),
            RssTextField::Guid => append_text(&mut self.guid, value),
            RssTextField::Link => append_text(&mut self.link, value),
            RssTextField::PubDate => {
                append_text(&mut self.pub_date_text, value);
                self.published_at_ms = self.pub_date_text.as_deref().and_then(parse_http_date_ms);
            }
            RssTextField::Size => {
                append_text(&mut self.size_text, value);
                self.size = self
                    .size_text
                    .as_deref()
                    .and_then(|value| value.parse().ok());
            }
        }
    }

    fn apply_torznab_attr(&mut self, name: &str, value: Option<&str>) {
        match (name, value) {
            ("size", Some(value)) => self.size = value.parse().ok(),
            ("infohash", Some(value)) => self.info_hash = Some(value.to_owned()),
            ("magneturl", Some(value)) if self.link.is_none() => self.link = Some(value.to_owned()),
            _ => {}
        }
    }

    fn into_candidate(
        self,
        endpoint: &TorznabEndpoint,
    ) -> Result<RemoteCandidate, TorznabRequestError> {
        let link = self.link;
        let guid = self.guid.or_else(|| link.clone()).ok_or_else(|| {
            TorznabRequestError::InvalidCandidate {
                message: "candidate missing guid".to_owned(),
            }
        })?;
        let download_url = preferred_download_url(self.enclosure_url, link).ok_or_else(|| {
            TorznabRequestError::InvalidCandidate {
                message: "candidate missing download URL".to_owned(),
            }
        })?;
        let title = self
            .title
            .ok_or_else(|| TorznabRequestError::InvalidCandidate {
                message: "candidate missing title".to_owned(),
            })?;

        Ok(RemoteCandidate {
            id: None,
            indexer_id: endpoint.indexer_id,
            guid: CandidateGuid::new(guid).map_err(candidate_error)?,
            download_url: DownloadUrl::new(download_url).map_err(candidate_error)?,
            title: ItemTitle::new(title).map_err(candidate_error)?,
            tracker: TrackerName::new(endpoint.name.as_str().to_owned())
                .map_err(candidate_error)?,
            size: self.size.map(ByteSize::new),
            published_at_ms: self.published_at_ms,
            info_hash: self.info_hash.and_then(|value| InfoHash::new(value).ok()),
            torrent_cache_path: None,
        })
    }
}

fn apply_enclosure_attributes(
    reader: &Reader<&[u8]>,
    element: &BytesStart<'_>,
    builder: &mut RssItemBuilder,
) -> Result<(), TorznabRequestError> {
    if let Some(url) = rss_attribute_value(reader, element, b"url")? {
        builder.enclosure_url = Some(url);
    }
    if let Some(length) = rss_attribute_value(reader, element, b"length")? {
        builder.size = length.parse().ok();
    }
    Ok(())
}

fn preferred_download_url(enclosure_url: Option<String>, link: Option<String>) -> Option<String> {
    enclosure_url
        .filter(|url| !url.trim().is_empty())
        .or_else(|| link.filter(|url| !url.trim().is_empty()))
}

fn append_text(target: &mut Option<String>, value: String) {
    if let Some(existing) = target {
        existing.push_str(&value);
    } else {
        *target = Some(value);
    }
}

fn rss_text_field(name: QName<'_>) -> Option<RssTextField> {
    match name.as_ref() {
        b"title" => Some(RssTextField::Title),
        b"guid" => Some(RssTextField::Guid),
        b"link" => Some(RssTextField::Link),
        b"pubDate" => Some(RssTextField::PubDate),
        b"size" => Some(RssTextField::Size),
        _ => None,
    }
}

fn rss_attribute_value(
    reader: &Reader<&[u8]>,
    element: &BytesStart<'_>,
    key: &[u8],
) -> Result<Option<String>, TorznabRequestError> {
    for attribute in element.attributes() {
        let attribute = attribute.map_err(|error| TorznabRequestError::InvalidXml {
            message: error.to_string(),
        })?;
        if attribute.key == QName(key) {
            let value = attribute
                .decode_and_unescape_value(reader.decoder())
                .map_err(|error| TorznabRequestError::InvalidXml {
                    message: error.to_string(),
                })?;
            return Ok(Some(value.into_owned()));
        }
    }
    Ok(None)
}

fn rss_text_value(text: &BytesText<'_>) -> Result<String, TorznabRequestError> {
    let decoded = text
        .xml10_content()
        .map_err(|error| TorznabRequestError::InvalidXml {
            message: error.to_string(),
        })?;
    let value = unescape(&decoded).map_err(|error| TorznabRequestError::InvalidXml {
        message: error.to_string(),
    })?;
    Ok(value.into_owned())
}

fn rss_cdata_value(cdata: &BytesCData<'_>) -> Result<String, TorznabRequestError> {
    cdata
        .xml10_content()
        .map(|value| value.into_owned())
        .map_err(|error| TorznabRequestError::InvalidXml {
            message: error.to_string(),
        })
}

fn rss_reference_value(reference: &BytesRef<'_>) -> Result<String, TorznabRequestError> {
    if let Some(character) =
        reference
            .resolve_char_ref()
            .map_err(|error| TorznabRequestError::InvalidXml {
                message: error.to_string(),
            })?
    {
        return Ok(character.to_string());
    }
    let name = reference
        .decode()
        .map_err(|error| TorznabRequestError::InvalidXml {
            message: error.to_string(),
        })?;
    resolve_predefined_entity(&name)
        .map(str::to_owned)
        .ok_or_else(|| TorznabRequestError::InvalidXml {
            message: format!("unknown XML entity `{name}`"),
        })
}

fn candidate_error(error: impl std::error::Error) -> TorznabRequestError {
    TorznabRequestError::InvalidCandidate {
        message: error.to_string(),
    }
}

pub(crate) fn parse_retry_after(value: &str) -> Option<RetryAfter> {
    value
        .parse::<i64>()
        .ok()
        .map(|seconds| RetryAfter::DelayMs(seconds.saturating_mul(1_000)))
        .or_else(|| parse_http_date_ms(value).map(RetryAfter::DeadlineMs))
}

async fn read_torznab_response(
    mut response: reqwest::Response,
    limit: u64,
) -> Result<Vec<u8>, TorznabRequestError> {
    if response
        .content_length()
        .is_some_and(|length| length > limit)
    {
        return Err(TorznabRequestError::ResponseTooLarge { limit });
    }

    let mut body = Vec::with_capacity(
        response
            .content_length()
            .and_then(|length| usize::try_from(length).ok())
            .unwrap_or_default(),
    );
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(TorznabRequestError::from_reqwest)?
    {
        if !append_limited_body_chunk(&mut body, &chunk, limit) {
            return Err(TorznabRequestError::ResponseTooLarge { limit });
        }
    }
    Ok(body)
}

async fn read_prowlarr_response(
    mut response: reqwest::Response,
    limit: u64,
) -> Result<Vec<u8>, ProwlarrRequestError> {
    if response
        .content_length()
        .is_some_and(|length| length > limit)
    {
        return Err(ProwlarrRequestError::ResponseTooLarge { limit });
    }

    let mut body = Vec::with_capacity(
        response
            .content_length()
            .and_then(|length| usize::try_from(length).ok())
            .unwrap_or_default(),
    );
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(ProwlarrRequestError::from_reqwest)?
    {
        if !append_limited_body_chunk(&mut body, &chunk, limit) {
            return Err(ProwlarrRequestError::ResponseTooLarge { limit });
        }
    }
    Ok(body)
}

async fn read_candidate_response(
    mut response: reqwest::Response,
    limit: u64,
) -> Result<Vec<u8>, CandidateDownloadError> {
    if response
        .content_length()
        .is_some_and(|length| length > limit)
    {
        return Err(CandidateDownloadError::ResponseTooLarge { limit });
    }

    let mut body = Vec::with_capacity(
        response
            .content_length()
            .and_then(|length| usize::try_from(length).ok())
            .unwrap_or_default(),
    );
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(CandidateDownloadError::from_reqwest)?
    {
        if !append_limited_body_chunk(&mut body, &chunk, limit) {
            return Err(CandidateDownloadError::ResponseTooLarge { limit });
        }
    }
    Ok(body)
}

fn append_limited_body_chunk(body: &mut Vec<u8>, chunk: &[u8], limit: u64) -> bool {
    let next_len = body.len().saturating_add(chunk.len());
    if u64::try_from(next_len).unwrap_or(u64::MAX) > limit {
        return false;
    }
    body.extend_from_slice(chunk);
    true
}

fn parse_http_date_ms(value: &str) -> Option<i64> {
    let duration = httpdate::parse_http_date(value)
        .ok()?
        .duration_since(UNIX_EPOCH)
        .ok()?;
    i64::try_from(duration.as_millis()).ok()
}

fn write_cached_torrent(path: &Path, bytes: &[u8]) -> Result<(), CandidateDownloadError> {
    let parent = path
        .parent()
        .ok_or_else(|| CandidateDownloadError::CacheWrite {
            path: path.to_path_buf(),
            message: "cache path has no parent directory".to_owned(),
        })?;
    fs::create_dir_all(parent).map_err(|error| CandidateDownloadError::CacheWrite {
        path: parent.to_path_buf(),
        message: error.to_string(),
    })?;

    let (mut temporary_file, temporary) = create_cache_temp_file(path)?;
    if let Err(error) = temporary_file.write_all(bytes) {
        let _ = fs::remove_file(&temporary);
        return Err(CandidateDownloadError::CacheWrite {
            path: temporary,
            message: error.to_string(),
        });
    }
    drop(temporary_file);
    match fs::rename(&temporary, path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists && path.is_file() => {
            let _ = fs::remove_file(&temporary);
            Ok(())
        }
        Err(error) => {
            let _ = fs::remove_file(&temporary);
            Err(CandidateDownloadError::CacheWrite {
                path: path.to_path_buf(),
                message: error.to_string(),
            })
        }
    }
}

fn create_cache_temp_file(
    path: &Path,
) -> Result<(File, std::path::PathBuf), CandidateDownloadError> {
    for _ in 0..128 {
        let temporary = unique_cache_temp_path(path);
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
        {
            Ok(file) => return Ok((file, temporary)),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(CandidateDownloadError::CacheWrite {
                    path: temporary,
                    message: error.to_string(),
                });
            }
        }
    }

    Err(CandidateDownloadError::CacheWrite {
        path: path.to_path_buf(),
        message: "failed to create a unique temporary cache file".to_owned(),
    })
}

fn unique_cache_temp_path(path: &Path) -> std::path::PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let sequence = CACHE_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("candidate.torrent");
    let temp_name = format!(
        ".{file_name}.{}.{}.{}.tmp",
        std::process::id(),
        unique,
        sequence
    );
    path.with_file_name(temp_name)
}

fn configured_torznab_indexer(
    name: &str,
    config: &TorznabIndexerConfig,
) -> Result<ConfiguredTorznabIndexer, IndexerConfigError> {
    if url_has_apikey_query(&config.url) {
        return Err(IndexerConfigError::InvalidUrl {
            message: "URL query apikey is not supported; use api_key, api_key_file, or api_key_env"
                .to_owned(),
        });
    }
    let name =
        DependencyName::new(name.to_owned()).map_err(|error| IndexerConfigError::InvalidName {
            message: error.to_string(),
        })?;
    Ok(ConfiguredTorznabIndexer {
        name,
        url: SanitizedTorznabUrl::new(config.url.clone())?,
        api_key: config.api_key.clone(),
        api_key_source: api_key_source(config),
        enabled: true,
    })
}

fn api_key_source(config: &TorznabIndexerConfig) -> ApiKeySource {
    if config.api_key.is_some() {
        ApiKeySource::Direct
    } else if let Some(path) = &config.api_key_file {
        ApiKeySource::File(display_path(path))
    } else if let Some(name) = &config.api_key_env {
        ApiKeySource::Env(name.clone())
    } else {
        ApiKeySource::Missing
    }
}

fn prowlarr_api_key_source(config: &ProwlarrSourceConfig) -> ApiKeySource {
    if config.api_key.is_some() {
        ApiKeySource::Direct
    } else if let Some(path) = &config.api_key_file {
        ApiKeySource::File(display_path(path))
    } else if let Some(name) = &config.api_key_env {
        ApiKeySource::Env(name.clone())
    } else {
        ApiKeySource::Missing
    }
}

fn sanitize_torznab_url(value: &str) -> Result<String, IndexerConfigError> {
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed.chars().any(char::is_whitespace) {
        return Err(IndexerConfigError::InvalidUrl {
            message: "URL must not be empty or contain whitespace".to_owned(),
        });
    }
    let (scheme, after_scheme) =
        trimmed
            .split_once("://")
            .ok_or_else(|| IndexerConfigError::InvalidUrl {
                message: "URL must include http or https scheme".to_owned(),
            })?;
    if scheme != "http" && scheme != "https" {
        return Err(IndexerConfigError::InvalidUrl {
            message: "URL scheme must be http or https".to_owned(),
        });
    }
    let (authority, path_and_more) =
        after_scheme
            .split_once('/')
            .ok_or_else(|| IndexerConfigError::InvalidUrl {
                message: "URL must include /api path".to_owned(),
            })?;
    if authority.is_empty() || authority.contains('@') {
        return Err(IndexerConfigError::InvalidUrl {
            message: "URL authority must not be empty or include credentials".to_owned(),
        });
    }
    let path_with_leading_slash = format!("/{path_and_more}");
    let path = path_with_leading_slash
        .split(['?', '#'])
        .next()
        .unwrap_or("");
    if !path.ends_with("/api") {
        return Err(IndexerConfigError::InvalidUrl {
            message: "URL path must end in /api".to_owned(),
        });
    }

    Ok(format!("{scheme}://{authority}{path}"))
}

fn url_has_apikey_query(value: &str) -> bool {
    let Some((_base, query_and_fragment)) = value.split_once('?') else {
        return false;
    };
    let query = query_and_fragment.split('#').next().unwrap_or("");
    query
        .split('&')
        .filter_map(|part| part.split_once('='))
        .any(|(key, _value)| key.eq_ignore_ascii_case("apikey"))
}

fn sanitized_reqwest_error(error: reqwest::Error) -> String {
    let url = error.url().map(reqwest_error_origin);
    let mut message = error.without_url().to_string();
    if let Some(url) = url {
        message.push_str(" for ");
        message.push_str(&url);
    }
    message
}

fn reqwest_error_origin(url: &reqwest::Url) -> String {
    let mut origin = String::new();
    origin.push_str(url.scheme());
    origin.push_str("://");
    origin.push_str(url.host_str().unwrap_or("[unknown-host]"));
    if let Some(port) = url.port() {
        origin.push(':');
        origin.push_str(&port.to_string());
    }
    origin
}

fn redacted_url_origin(value: &str) -> String {
    reqwest::Url::parse(value)
        .map(|url| reqwest_error_origin(&url))
        .unwrap_or_else(|_error| sanitize_url_for_logging(value).to_string())
}

fn display_path(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn is_magnet_uri(value: &str) -> bool {
    value
        .split_once(':')
        .is_some_and(|(scheme, _rest)| scheme.eq_ignore_ascii_case("magnet"))
}

pub fn configured_torznab_by_name(
    config: &IndexersConfig,
) -> Result<BTreeMap<DependencyName, ConfiguredTorznabIndexer>, IndexerConfigError> {
    let registry = TorznabRegistry::from_config(config)?;
    Ok(registry
        .indexers
        .into_iter()
        .map(|indexer| (indexer.name.clone(), indexer))
        .collect())
}

#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct TorznabCaps {
    pub search: SearchCaps,
    pub categories: CategoryCaps,
    pub limits: TorznabLimits,
}

impl TorznabCaps {
    pub fn supports_media_type(&self, media_type: MediaType) -> bool {
        match media_type {
            MediaType::Episode | MediaType::SeasonPack => {
                self.search.tv_search || self.categories.tv || self.categories.xxx
            }
            MediaType::Movie => {
                self.search.movie_search || self.categories.movie || self.categories.xxx
            }
            MediaType::Anime | MediaType::Video => {
                self.search.tv_search
                    || self.search.movie_search
                    || self.categories.tv
                    || self.categories.movie
                    || self.categories.anime
                    || self.categories.xxx
            }
            MediaType::Audio => self.search.audio_search || self.categories.audio,
            MediaType::Book => self.categories.book,
            MediaType::Archive | MediaType::Unknown => self.search.generic_search,
        }
    }
}

#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct SearchCaps {
    pub generic_search: bool,
    pub tv_search: bool,
    pub movie_search: bool,
    pub audio_search: bool,
    pub supported_id_params: BTreeSet<String>,
}

#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct CategoryCaps {
    pub movie: bool,
    pub tv: bool,
    pub anime: bool,
    pub xxx: bool,
    pub audio: bool,
    pub book: bool,
    pub additional: bool,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct TorznabLimits {
    pub default: u16,
    pub max: u16,
}

impl Default for TorznabLimits {
    fn default() -> Self {
        Self {
            default: 100,
            max: 100,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum TorznabCapsError {
    InvalidXml { message: String },
    UnsupportedSearch,
}

impl fmt::Display for TorznabCapsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidXml { message } => {
                write!(formatter, "invalid Torznab caps XML: {message}")
            }
            Self::UnsupportedSearch => write!(formatter, "Torznab caps do not support search"),
        }
    }
}

impl std::error::Error for TorznabCapsError {}

pub fn parse_torznab_caps(xml: &str) -> Result<TorznabCaps, TorznabCapsError> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);
    let mut buffer = Vec::new();
    let mut caps = TorznabCaps::default();
    let mut saw_caps = false;

    loop {
        match reader.read_event_into(&mut buffer) {
            Ok(Event::Start(element)) | Ok(Event::Empty(element)) => {
                let name = element.name();
                if name == QName(b"caps") {
                    saw_caps = true;
                } else if name == QName(b"limits") {
                    parse_limits(&reader, &element, &mut caps)?;
                } else if is_search_element(name) {
                    parse_search_caps(&reader, &element, &mut caps)?;
                } else if name == QName(b"category") || name == QName(b"subcat") {
                    parse_category_caps(&reader, &element, &mut caps)?;
                }
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(error) => {
                return Err(TorznabCapsError::InvalidXml {
                    message: error.to_string(),
                });
            }
        }
        buffer.clear();
    }

    if !saw_caps {
        return Err(TorznabCapsError::InvalidXml {
            message: "missing caps root".to_owned(),
        });
    }
    if !caps.search.generic_search
        && !caps.search.tv_search
        && !caps.search.movie_search
        && !caps.search.audio_search
    {
        return Err(TorznabCapsError::UnsupportedSearch);
    }

    Ok(caps)
}

fn parse_limits(
    reader: &Reader<&[u8]>,
    element: &BytesStart<'_>,
    caps: &mut TorznabCaps,
) -> Result<(), TorznabCapsError> {
    let default = attribute_value(reader, element, b"default")?
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(100);
    let max = attribute_value(reader, element, b"max")?
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(100);
    caps.limits = TorznabLimits { default, max };
    Ok(())
}

fn parse_search_caps(
    reader: &Reader<&[u8]>,
    element: &BytesStart<'_>,
    caps: &mut TorznabCaps,
) -> Result<(), TorznabCapsError> {
    let available = attribute_value(reader, element, b"available")?
        .map(|value| matches!(value.as_str(), "yes" | "true" | "1"))
        .unwrap_or(false);
    match element.name() {
        QName(b"search") => caps.search.generic_search = available,
        QName(b"tv-search") => caps.search.tv_search = available,
        QName(b"movie-search") => caps.search.movie_search = available,
        QName(b"audio-search") => caps.search.audio_search = available,
        _ => {}
    }
    if available && let Some(params) = attribute_value(reader, element, b"supportedParams")? {
        for param in params
            .split(',')
            .map(str::trim)
            .filter(|param| !param.is_empty())
        {
            caps.search
                .supported_id_params
                .insert(param.to_ascii_lowercase());
        }
    }
    Ok(())
}

fn parse_category_caps(
    reader: &Reader<&[u8]>,
    element: &BytesStart<'_>,
    caps: &mut TorznabCaps,
) -> Result<(), TorznabCapsError> {
    let name = attribute_value(reader, element, b"name")?
        .unwrap_or_default()
        .to_ascii_lowercase();
    let id = attribute_value(reader, element, b"id")?.and_then(|value| value.parse::<u32>().ok());

    if name.contains("movie") {
        caps.categories.movie = true;
    } else if name.contains("tv") || name.contains("television") {
        caps.categories.tv = true;
    } else if name.contains("anime") {
        caps.categories.anime = true;
    } else if name.contains("xxx") {
        caps.categories.xxx = true;
    } else if name.contains("audio") || name.contains("music") {
        caps.categories.audio = true;
    } else if name.contains("book") {
        caps.categories.book = true;
    } else if id.is_some_and(is_additional_category) {
        caps.categories.additional = true;
    }

    Ok(())
}

fn is_search_element(name: QName<'_>) -> bool {
    matches!(
        name,
        QName(b"search") | QName(b"tv-search") | QName(b"movie-search") | QName(b"audio-search")
    )
}

fn is_additional_category(id: u32) -> bool {
    id < 100_000 && !(8_000..=8_999).contains(&id)
}

fn attribute_value(
    reader: &Reader<&[u8]>,
    element: &BytesStart<'_>,
    key: &[u8],
) -> Result<Option<String>, TorznabCapsError> {
    for attribute in element.attributes() {
        let attribute = attribute.map_err(|error| TorznabCapsError::InvalidXml {
            message: error.to_string(),
        })?;
        if attribute.key == QName(key) {
            let value = attribute
                .decode_and_unescape_value(reader.decoder())
                .map_err(|error| TorznabCapsError::InvalidXml {
                    message: error.to_string(),
                })?;
            return Ok(Some(value.into_owned()));
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener as StdTcpListener;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering as AtomicOrdering},
    };

    use crate::config::{IndexerTimeoutsConfig, IndexersConfig};
    use crate::matching::{SearchIds, TorznabSearchPlan, TorznabSearchQuery};
    use crate::secrets::ApiKey;
    use axum::Router;
    use axum::body::Body;
    use axum::http::{
        HeaderValue, Request, StatusCode as AxumStatusCode,
        header::{CONTENT_LENGTH, LOCATION},
    };
    use axum::response::{IntoResponse, Response};
    use axum::routing::get;
    use tokio::net::TcpListener;

    #[test]
    fn registry_sanitizes_urls_and_tracks_secret_sources() {
        let mut torznab = BTreeMap::new();
        torznab.insert(
            "main".to_owned(),
            TorznabIndexerConfig {
                url: "https://indexer.example//Case/api?t=caps#fragment".to_owned(),
                api_key: None,
                api_key_file: None,
                api_key_env: Some("MAIN_INDEXER_KEY".to_owned()),
            },
        );
        torznab.insert(
            "backup".to_owned(),
            TorznabIndexerConfig {
                url: "https://backup.example/prowlarr/api".to_owned(),
                api_key: Some(ApiKey::new("direct-secret").unwrap()),
                api_key_file: None,
                api_key_env: None,
            },
        );
        let config = IndexersConfig {
            default_timeouts: IndexerTimeoutsConfig::default(),
            torznab,
            prowlarr: Default::default(),
            arr: Default::default(),
        };

        let registry = TorznabRegistry::from_config(&config).unwrap();

        assert_eq!(2, registry.indexers().len());
        let main = registry
            .indexers()
            .iter()
            .find(|indexer| indexer.name.as_str() == "main")
            .unwrap();
        assert_eq!("https://indexer.example//Case/api", main.url.as_str());
        assert_eq!(
            ApiKeySource::Env("MAIN_INDEXER_KEY".to_owned()),
            main.api_key_source
        );
        assert!(!format!("{registry:?}").contains("secret"));
    }

    #[tokio::test]
    async fn request_errors_redact_secret_bearing_urls() {
        let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let torznab_url = format!(
            "http://url-user:url-password@{address}/api/path-token?apikey=secret&t=search#frag"
        );
        let candidate_url =
            format!("http://{address}/download/path-token?id=1&authkey=secret&torrent_pass=secret");

        let torznab_error = reqwest::Client::new()
            .get(&torznab_url)
            .timeout(Duration::from_millis(100))
            .send()
            .await
            .map_err(TorznabRequestError::from_reqwest)
            .unwrap_err()
            .to_string();
        let candidate_error = reqwest::Client::new()
            .get(&candidate_url)
            .timeout(Duration::from_millis(100))
            .send()
            .await
            .map_err(CandidateDownloadError::from_reqwest)
            .unwrap_err()
            .to_string();

        assert!(!torznab_error.contains("url-user"));
        assert!(!torznab_error.contains("url-password"));
        assert!(!torznab_error.contains("path-token"));
        assert!(!torznab_error.contains("apikey=secret"));
        assert!(torznab_error.contains(&format!("http://{address}")));
        assert!(!candidate_error.contains("path-token"));
        assert!(!candidate_error.contains("authkey=secret"));
        assert!(!candidate_error.contains("torrent_pass=secret"));
        assert!(candidate_error.contains(&format!("http://{address}")));
    }

    #[test]
    fn registry_rejects_url_query_api_keys() {
        let mut torznab = BTreeMap::new();
        torznab.insert(
            "main".to_owned(),
            TorznabIndexerConfig {
                url: "https://indexer.example/api?apikey=secret".to_owned(),
                api_key: None,
                api_key_file: None,
                api_key_env: None,
            },
        );
        let config = IndexersConfig {
            default_timeouts: IndexerTimeoutsConfig::default(),
            torznab,
            prowlarr: Default::default(),
            arr: Default::default(),
        };

        let error = TorznabRegistry::from_config(&config).unwrap_err();

        assert!(matches!(error, IndexerConfigError::InvalidUrl { .. }));
    }

    #[test]
    fn registry_rejects_duplicate_sanitized_urls() {
        let mut torznab = BTreeMap::new();
        for name in ["one", "two"] {
            torznab.insert(
                name.to_owned(),
                TorznabIndexerConfig {
                    url: "https://indexer.example/api?t=caps".to_owned(),
                    api_key: None,
                    api_key_file: None,
                    api_key_env: None,
                },
            );
        }
        let config = IndexersConfig {
            default_timeouts: IndexerTimeoutsConfig::default(),
            torznab,
            prowlarr: Default::default(),
            arr: Default::default(),
        };

        let error = TorznabRegistry::from_config(&config).unwrap_err();

        assert!(matches!(error, IndexerConfigError::DuplicateUrl { .. }));
    }

    #[test]
    fn registry_rejects_non_api_urls_and_credentials() {
        for url in [
            "https://indexer.example/rss",
            "ftp://indexer.example/api",
            "https://user:pass@indexer.example/api",
        ] {
            let error = SanitizedTorznabUrl::new(url).unwrap_err();
            assert!(matches!(error, IndexerConfigError::InvalidUrl { .. }));
        }
    }

    #[tokio::test]
    async fn search_client_sends_query_and_parses_rss_candidates() {
        let endpoint = test_endpoint(
            spawn_torznab_server(|request| async move {
                let query = request.uri().query().unwrap_or_default();
                if !query.contains("t=tvsearch")
                    || !query.contains("tvdbid=42")
                    || !query.contains("apikey=secret")
                    || !query.contains("limit=50")
                {
                    return (AxumStatusCode::BAD_REQUEST, "bad query".to_owned());
                }
                (
                    AxumStatusCode::OK,
                    search_rss("candidate-1", "Example S01E01"),
                )
            })
            .await,
        );
        let client = TorznabHttpClient::new(Duration::from_secs(5));
        let plan = TorznabSearchPlan {
            query: TorznabSearchQuery {
                search_type: TorznabSearchType::TvSearch,
                q: None,
                season: Some(1),
                episode: Some(1),
                ids: SearchIds {
                    tvdb_id: Some("42".to_owned()),
                    ..SearchIds::default()
                },
            },
            limit: 200,
        };

        let candidates = client
            .search(&endpoint, MediaType::Episode, &plan, 1_700_000_000_000)
            .await
            .unwrap();

        assert_eq!(1, candidates.len());
        assert_eq!("candidate-1", candidates[0].guid.as_str());
        assert_eq!("Example S01E01", candidates[0].title.as_str());
        assert_eq!(Some(ByteSize::new(1234)), candidates[0].size);
        assert_eq!(
            Some("0123456789abcdef0123456789abcdef01234567"),
            candidates[0].info_hash.as_ref().map(InfoHash::as_str)
        );
    }

    #[tokio::test]
    async fn prowlarr_client_fetches_and_filters_torznab_indexers() {
        let url = spawn_prowlarr_server(|request| async move {
            if request
                .headers()
                .get("x-api-key")
                .and_then(|value| value.to_str().ok())
                != Some("prowlarr-secret")
            {
                return (AxumStatusCode::UNAUTHORIZED, "missing key").into_response();
            }
            match request.uri().path() {
                "/api/v1/tag" => (
                    AxumStatusCode::OK,
                    r#"[{"id":1,"label":"movies"},{"id":2,"label":"hd"}]"#,
                )
                    .into_response(),
                "/api/v1/indexer" => (
                    AxumStatusCode::OK,
                    r#"
                [
                  {
                    "id": 101,
                    "name": "Movies",
                    "enable": true,
                    "protocol": "torrent",
                    "implementation": "Cardigann",
                    "indexerUrls": ["https://tracker.example"],
                    "supportsRss": true,
                    "supportsSearch": true,
                    "tags": [1, 2]
                  },
                  {
                    "id": 102,
                    "name": "Disabled",
                    "enable": false,
                    "protocol": "torrent",
                    "implementation": "Torznab",
                    "supportsRss": true,
                    "supportsSearch": true,
                    "tags": [1]
                  },
                  {
                    "id": 103,
                    "name": "Usenet",
                    "enable": true,
                    "protocol": "usenet",
                    "implementation": "Newznab",
                    "supportsRss": true,
                    "supportsSearch": true,
                    "tags": [1]
                  },
                  {
                    "id": 104,
                    "name": "Untagged",
                    "enable": true,
                    "protocol": "torrent",
                    "implementation": "Torznab",
                    "supportsRss": true,
                    "supportsSearch": true,
                    "tags": []
                  },
                  {
                    "id": 105,
                    "name": "No Search",
                    "enable": true,
                    "protocol": "torrent",
                    "implementation": "Cardigann",
                    "supportsRss": false,
                    "supportsSearch": false,
                    "tags": [1]
                  }
                ]
                "#,
                )
                    .into_response(),
                _ => (AxumStatusCode::NOT_FOUND, "bad path").into_response(),
            }
        })
        .await;
        let source_url = url.clone();
        let source = test_prowlarr_source(url, &["movies"], ProwlarrTagMatch::Any, false);
        let client = ProwlarrHttpClient::new(Duration::from_secs(5));

        let indexers = client.indexers(&source).await.unwrap();

        assert_eq!(1, indexers.len());
        assert_eq!(101, indexers[0].prowlarr_id);
        assert_eq!("Movies", indexers[0].name.as_str());
        assert_eq!(format!("{source_url}/101/api"), indexers[0].url.as_str());
        assert_eq!(
            Some("prowlarr-secret"),
            indexers[0].api_key.as_ref().map(ApiKey::expose_secret)
        );
        assert_eq!(ApiKeySource::Direct, indexers[0].api_key_source);
        assert_eq!(vec!["movies", "hd"], indexers[0].tags);
    }

    #[tokio::test]
    async fn prowlarr_client_applies_all_tags_and_include_untagged() {
        let url = spawn_prowlarr_server(|request| async move {
            match request.uri().path() {
                "/api/v1/tag" => (
                    AxumStatusCode::OK,
                    r#"[{"id":1,"label":"movies"},{"id":2,"label":"hd"}]"#,
                )
                    .into_response(),
                "/api/v1/indexer" => (
                    AxumStatusCode::OK,
                    r#"
                [
                  {
                    "id": 201,
                    "name": "Tagged",
                    "enable": true,
                    "protocol": "torrent",
                    "implementation": "Torznab",
                    "supportsRss": true,
                    "supportsSearch": true,
                    "tags": [1, 2]
                  },
                  {
                    "id": 202,
                    "name": "Partial",
                    "enable": true,
                    "protocol": "torrent",
                    "implementation": "Torznab",
                    "supportsRss": true,
                    "supportsSearch": true,
                    "tags": [1]
                  },
                  {
                    "id": 203,
                    "name": "Untagged",
                    "enable": true,
                    "protocol": "torrent",
                    "implementation": "Torznab",
                    "supportsRss": true,
                    "supportsSearch": true,
                    "tags": []
                  }
                ]
                "#,
                )
                    .into_response(),
                _ => (AxumStatusCode::NOT_FOUND, "bad path").into_response(),
            }
        })
        .await;
        let source = test_prowlarr_source(url, &["movies", "hd"], ProwlarrTagMatch::All, true);
        let client = ProwlarrHttpClient::new(Duration::from_secs(5));

        let indexers = client.indexers(&source).await.unwrap();
        let names = indexers
            .iter()
            .map(|indexer| indexer.name.as_str())
            .collect::<Vec<_>>();

        assert_eq!(vec!["Tagged", "Untagged"], names);
    }

    #[tokio::test]
    async fn prowlarr_client_maps_status_malformed_and_oversized_responses() {
        let status_url =
            spawn_prowlarr_server(|_request| async move { (AxumStatusCode::UNAUTHORIZED, "no") })
                .await;
        let malformed_url =
            spawn_prowlarr_server(|_request| async move { (AxumStatusCode::OK, "not json") }).await;
        let oversized_url = spawn_prowlarr_server(|_request| async move {
            oversized_response(PROWLARR_CATALOG_MAX_BYTES.saturating_add(1))
        })
        .await;
        let client = ProwlarrHttpClient::new(Duration::from_secs(5));

        let status = client
            .indexers(&test_prowlarr_source(
                status_url,
                &[],
                ProwlarrTagMatch::Any,
                true,
            ))
            .await
            .unwrap_err();
        let malformed = client
            .indexers(&test_prowlarr_source(
                malformed_url,
                &[],
                ProwlarrTagMatch::Any,
                true,
            ))
            .await
            .unwrap_err();
        let oversized = client
            .indexers(&test_prowlarr_source(
                oversized_url,
                &[],
                ProwlarrTagMatch::Any,
                true,
            ))
            .await
            .unwrap_err();

        assert!(matches!(
            status,
            ProwlarrRequestError::HttpStatus { status: 401, .. }
        ));
        assert!(matches!(
            malformed,
            ProwlarrRequestError::InvalidResponse { .. }
        ));
        assert!(matches!(
            oversized,
            ProwlarrRequestError::ResponseTooLarge {
                limit: PROWLARR_CATALOG_MAX_BYTES
            }
        ));
    }

    #[tokio::test]
    async fn prowlarr_client_does_not_forward_api_key_on_redirect() {
        let saw_redirected_key = Arc::new(AtomicBool::new(false));
        let target_saw_redirected_key = saw_redirected_key.clone();
        let target_url = spawn_prowlarr_server(move |request| {
            let target_saw_redirected_key = target_saw_redirected_key.clone();
            async move {
                if request.headers().get("x-api-key").is_some() {
                    target_saw_redirected_key.store(true, AtomicOrdering::Relaxed);
                }
                (AxumStatusCode::OK, "[]").into_response()
            }
        })
        .await;
        let redirect_url = target_url.clone();
        let source_url = spawn_prowlarr_server(move |_request| {
            let redirect_url = redirect_url.clone();
            async move {
                (
                    AxumStatusCode::FOUND,
                    [(
                        LOCATION,
                        HeaderValue::from_str(&format!("{redirect_url}/api/v1/indexer")).unwrap(),
                    )],
                    "",
                )
                    .into_response()
            }
        })
        .await;
        let client = ProwlarrHttpClient::new(Duration::from_secs(5));

        let error = client
            .indexers(&test_prowlarr_source(
                source_url,
                &[],
                ProwlarrTagMatch::Any,
                true,
            ))
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            ProwlarrRequestError::HttpStatus { status: 302, .. }
        ));
        assert!(!saw_redirected_key.load(AtomicOrdering::Relaxed));
    }

    #[tokio::test]
    async fn prowlarr_errors_redact_secret_bearing_urls() {
        let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let source = ProwlarrSource {
            name: DependencyName::new("main").unwrap(),
            url: format!("http://user:password@{address}/path-token?apikey=secret#frag"),
            api_key: ApiKey::new("prowlarr-secret").unwrap(),
            api_key_source: ApiKeySource::Direct,
            tags: BTreeSet::new(),
            tag_match: ProwlarrTagMatch::Any,
            include_untagged: true,
        };
        let client = ProwlarrHttpClient::new(Duration::from_millis(100));

        let error = client.indexers(&source).await.unwrap_err().to_string();
        let debug = format!("{source:?}");

        assert!(!error.contains("user"));
        assert!(!error.contains("password"));
        assert!(!error.contains("path-token"));
        assert!(!error.contains("apikey=secret"));
        assert!(error.contains(&format!("http://{address}")));
        assert!(!debug.contains("user"));
        assert!(!debug.contains("password"));
        assert!(!debug.contains("path-token"));
        assert!(!debug.contains("apikey=secret"));
        assert!(!debug.contains("prowlarr-secret"));
    }

    #[tokio::test]
    async fn search_client_sends_audio_queries_as_music() {
        let endpoint = test_endpoint(
            spawn_torznab_server(|request| async move {
                let query = request.uri().query().unwrap_or_default();
                if !query.contains("t=music")
                    || !query.contains("q=Example")
                    || !query.contains("apikey=secret")
                {
                    return (AxumStatusCode::BAD_REQUEST, "bad query".to_owned());
                }
                (
                    AxumStatusCode::OK,
                    search_rss("candidate-1", "Example Album"),
                )
            })
            .await,
        );
        let client = TorznabHttpClient::new(Duration::from_secs(5));
        let plan = TorznabSearchPlan {
            query: TorznabSearchQuery {
                search_type: TorznabSearchType::AudioSearch,
                q: Some("Example".to_owned()),
                season: None,
                episode: None,
                ids: SearchIds::default(),
            },
            limit: 50,
        };

        let candidates = client
            .search(&endpoint, MediaType::Audio, &plan, 1_700_000_000_000)
            .await
            .unwrap();

        assert_eq!(1, candidates.len());
        assert_eq!("candidate-1", candidates[0].guid.as_str());
    }

    #[tokio::test]
    async fn search_client_maps_rate_limits_and_malformed_rss() {
        let rate_limited = test_endpoint(
            spawn_torznab_server(|_request| async move {
                (AxumStatusCode::TOO_MANY_REQUESTS, "limited".to_owned())
            })
            .await,
        );
        let malformed = test_endpoint(
            spawn_torznab_server(
                |_request| async move { (AxumStatusCode::OK, "not rss".to_owned()) },
            )
            .await,
        );
        let client = TorznabHttpClient::new(Duration::from_secs(5));
        let plan = generic_plan();

        let limited = client
            .search(&rate_limited, MediaType::Movie, &plan, 1_700_000_000_000)
            .await
            .unwrap_err();
        let invalid = client
            .search(&malformed, MediaType::Movie, &plan, 1_700_000_000_000)
            .await
            .unwrap_err();

        assert!(matches!(limited, TorznabRequestError::RateLimited { .. }));
        assert!(matches!(invalid, TorznabRequestError::InvalidXml { .. }));
    }

    #[tokio::test]
    async fn search_client_rejects_oversized_rss_response() {
        let endpoint = test_endpoint(
            spawn_torznab_server(|_request| async move {
                oversized_response(TORZNAB_RSS_MAX_BYTES.saturating_add(1))
            })
            .await,
        );
        let client = TorznabHttpClient::new(Duration::from_secs(5));
        let plan = generic_plan();

        let error = client
            .search(&endpoint, MediaType::Movie, &plan, 1_700_000_000_000)
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                TorznabRequestError::ResponseTooLarge {
                    limit: TORZNAB_RSS_MAX_BYTES
                }
            ),
            "got {error:?}"
        );
    }

    #[tokio::test]
    async fn search_client_rejects_chunked_oversized_rss_response() {
        let endpoint = test_endpoint(spawn_chunked_response_server(
            "/api",
            TORZNAB_RSS_MAX_BYTES.saturating_add(1),
        ));
        let client = TorznabHttpClient::new(Duration::from_secs(5));
        let plan = generic_plan();

        let error = client
            .search(&endpoint, MediaType::Movie, &plan, 1_700_000_000_000)
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                TorznabRequestError::ResponseTooLarge {
                    limit: TORZNAB_RSS_MAX_BYTES
                }
            ),
            "got {error:?}"
        );
    }

    #[test]
    fn response_reader_rejects_oversized_chunks_without_content_length() {
        let mut body = Vec::new();

        assert!(append_limited_body_chunk(&mut body, b"12345678", 8));
        assert!(!append_limited_body_chunk(&mut body, b"9", 8));
        assert_eq!(b"12345678", body.as_slice());
    }

    #[tokio::test]
    async fn rss_client_stops_on_seen_guid_and_candidate_limit() {
        let endpoint = test_endpoint(
            spawn_torznab_server(|request| async move {
                let query = request.uri().query().unwrap_or_default();
                if query.contains("offset=0") {
                    (
                        AxumStatusCode::OK,
                        rss_items(&["newest", "seen", "ignored"]),
                    )
                } else {
                    (AxumStatusCode::OK, rss_items(&["later"]))
                }
            })
            .await,
        );
        let client = TorznabHttpClient::new(Duration::from_secs(5));

        let seen = client
            .rss(
                &endpoint,
                RssPageOptions {
                    last_seen_guid: Some("seen"),
                    max_age_ms: None,
                    max_pages: 5,
                    max_candidates: 10,
                    page_size: 50,
                },
                1_700_000_000_000,
            )
            .await
            .unwrap();
        let limited = client
            .rss(
                &endpoint,
                RssPageOptions {
                    last_seen_guid: None,
                    max_age_ms: None,
                    max_pages: 5,
                    max_candidates: 1,
                    page_size: 50,
                },
                1_700_000_000_000,
            )
            .await
            .unwrap();

        assert_eq!(Some("newest".to_owned()), seen.new_last_seen_guid);
        assert_eq!(vec!["newest"], candidate_guids(&seen.candidates));
        assert_eq!(vec!["newest"], candidate_guids(&limited.candidates));
    }

    #[test]
    fn rss_parser_unescapes_text_and_accepts_cdata() {
        let endpoint = test_endpoint("https://indexer.example/api".to_owned());
        let candidates = parse_torznab_rss(
            r#"
            <rss>
              <channel>
                <item>
                  <title><![CDATA[Example & Friends]]></title>
                  <guid>candidate-1</guid>
                  <link>https://indexer.example/download?id=1&amp;passkey=secret</link>
                </item>
              </channel>
            </rss>
            "#,
            &endpoint,
        )
        .unwrap();

        assert_eq!("candidate-1", candidates[0].guid.as_str());
        assert_eq!("Example & Friends", candidates[0].title.as_str());
        assert_eq!(
            "https://indexer.example/download?id=1&passkey=secret",
            candidates[0].download_url.as_str()
        );
    }

    #[test]
    fn rss_parser_prefers_enclosure_downloads_over_details_links() {
        let endpoint = test_endpoint("https://indexer.example/api".to_owned());
        let candidates = parse_torznab_rss(
            r#"
            <rss>
              <channel>
                <item>
                  <title>Example S01E01</title>
                  <guid>candidate-1</guid>
                  <link>https://indexer.example/details?id=1</link>
                  <enclosure url="https://indexer.example/download?id=1&amp;torrent=1" length="4321" type="application/x-bittorrent"/>
                </item>
              </channel>
            </rss>
            "#,
            &endpoint,
        )
        .unwrap();

        assert_eq!("candidate-1", candidates[0].guid.as_str());
        assert_eq!("Example S01E01", candidates[0].title.as_str());
        assert_eq!(
            "https://indexer.example/download?id=1&torrent=1",
            candidates[0].download_url.as_str()
        );
        assert_eq!(Some(ByteSize::new(4321)), candidates[0].size);
    }

    #[test]
    fn rss_parser_reads_non_self_closing_enclosure_downloads() {
        let endpoint = test_endpoint("https://indexer.example/api".to_owned());
        let candidates = parse_torznab_rss(
            r#"
            <rss>
              <channel>
                <item>
                  <title>Example S01E01</title>
                  <guid>candidate-1</guid>
                  <link>https://indexer.example/details?id=1</link>
                  <enclosure url="https://indexer.example/download?id=1" length="4321"></enclosure>
                </item>
              </channel>
            </rss>
            "#,
            &endpoint,
        )
        .unwrap();

        assert_eq!(
            "https://indexer.example/download?id=1",
            candidates[0].download_url.as_str()
        );
        assert_eq!(Some(ByteSize::new(4321)), candidates[0].size);
    }

    #[test]
    fn rss_parser_falls_back_to_link_without_enclosure() {
        let endpoint = test_endpoint("https://indexer.example/api".to_owned());
        let candidates = parse_torznab_rss(
            r#"
            <rss>
              <channel>
                <item>
                  <title>Example S01E02</title>
                  <guid>candidate-2</guid>
                  <link>https://indexer.example/download?id=2</link>
                </item>
              </channel>
            </rss>
            "#,
            &endpoint,
        )
        .unwrap();

        assert_eq!(
            "https://indexer.example/download?id=2",
            candidates[0].download_url.as_str()
        );
    }

    #[test]
    fn rss_parser_skips_malformed_items_without_losing_valid_candidates() {
        let endpoint = test_endpoint("https://indexer.example/api".to_owned());
        let candidates = parse_torznab_rss(
            r#"
            <rss>
              <channel>
                <item>
                  <title>Missing download</title>
                  <guid>bad-1</guid>
                </item>
                <item>
                  <guid>bad-2</guid>
                  <link>https://indexer.example/download?id=bad-2</link>
                </item>
                <item>
                  <title>Example S01E03</title>
                  <guid>candidate-3</guid>
                  <link>https://indexer.example/download?id=3</link>
                </item>
              </channel>
            </rss>
            "#,
            &endpoint,
        )
        .unwrap();

        assert_eq!(1, candidates.len());
        assert_eq!("candidate-3", candidates[0].guid.as_str());
        assert_eq!("Example S01E03", candidates[0].title.as_str());
    }

    #[test]
    fn rss_parser_preserves_hard_failures_for_malformed_documents() {
        let endpoint = test_endpoint("https://indexer.example/api".to_owned());
        let error = parse_torznab_rss(
            r#"
            <rss>
              <channel>
                <item>
                  <title>&unknown;</title>
                  <guid>candidate-1</guid>
                  <link>https://indexer.example/download?id=1</link>
                </item>
              </channel>
            </rss>
            "#,
            &endpoint,
        )
        .unwrap_err();

        assert!(matches!(error, TorznabRequestError::InvalidXml { .. }));
    }

    #[tokio::test]
    async fn candidate_download_caches_valid_torrents_atomically() {
        let url = spawn_torznab_server(|request| async move {
            let cookie = request
                .headers()
                .get(COOKIE)
                .and_then(|value| value.to_str().ok());
            let agent = request
                .headers()
                .get(USER_AGENT)
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default();
            if cookie != Some("sid=secret") || !agent.starts_with("Sporos/") {
                return (AxumStatusCode::BAD_REQUEST, "bad headers".to_owned());
            }
            (
                AxumStatusCode::OK,
                String::from_utf8(test_torrent_bytes()).unwrap(),
            )
        })
        .await;
        let cache_dir = unique_temp_dir("candidate-cache");
        let candidate = test_candidate(&url);
        let client = test_candidate_download_client();

        let cached = client
            .download_and_cache(&candidate, &cache_dir, Some("sid=secret"))
            .await
            .unwrap();

        assert_eq!(
            cached.metafile.info_hash(),
            &cached.candidate.info_hash.clone().unwrap()
        );
        assert_eq!(
            Some(cached.cache_path.clone()),
            cached.candidate.torrent_cache_path
        );
        assert_eq!(test_torrent_bytes(), fs::read(&cached.cache_path).unwrap());

        remove_temp_dir(&cache_dir);
    }

    #[tokio::test]
    async fn candidate_download_isolates_concurrent_cache_temp_writes() {
        let url = spawn_torznab_server(|_request| async move {
            (
                AxumStatusCode::OK,
                String::from_utf8(test_torrent_bytes()).unwrap(),
            )
        })
        .await;
        let cache_dir = unique_temp_dir("candidate-cache-concurrent");
        let candidate = test_candidate(&url);
        let client = test_candidate_download_client();

        let downloads = tokio::join!(
            client.download_and_cache(&candidate, &cache_dir, None),
            client.download_and_cache(&candidate, &cache_dir, None),
            client.download_and_cache(&candidate, &cache_dir, None),
            client.download_and_cache(&candidate, &cache_dir, None)
        );
        let cached = [
            downloads.0.unwrap(),
            downloads.1.unwrap(),
            downloads.2.unwrap(),
            downloads.3.unwrap(),
        ];

        for item in &cached {
            assert_eq!(test_torrent_bytes(), fs::read(&item.cache_path).unwrap());
        }
        assert!(fs::read_dir(&cache_dir).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .ends_with(".tmp")
        }));

        remove_temp_dir(&cache_dir);
    }

    #[test]
    fn cache_writer_isolates_parallel_distinct_temp_writes() {
        let cache_dir = unique_temp_dir("cache-writer-concurrent");
        let cache_path = cache_dir.join("candidate.torrent");
        let first = test_torrent_bytes();
        let second = alternate_test_torrent_bytes();
        let mut handles = Vec::new();

        for index in 0..32 {
            let cache_path = cache_path.clone();
            let bytes = if index % 2 == 0 {
                first.clone()
            } else {
                second.clone()
            };
            handles.push(std::thread::spawn(move || {
                write_cached_torrent(&cache_path, &bytes).unwrap();
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }
        let final_bytes = fs::read(&cache_path).unwrap();
        assert!(final_bytes == first || final_bytes == second);
        assert!(fs::read_dir(&cache_dir).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .ends_with(".tmp")
        }));

        remove_temp_dir(&cache_dir);
    }

    #[tokio::test]
    async fn candidate_download_maps_terminal_and_retryable_failures() {
        let rate_limited = test_candidate(
            &spawn_torznab_server(|_request| async move {
                (AxumStatusCode::TOO_MANY_REQUESTS, "limited".to_owned())
            })
            .await,
        );
        let invalid = test_candidate(
            &spawn_torznab_server(|_request| async move {
                (AxumStatusCode::OK, "not bencode".to_owned())
            })
            .await,
        );
        let rss = test_candidate(
            &spawn_torznab_server(|_request| async move {
                (
                    AxumStatusCode::OK,
                    [("content-type", "application/rss+xml")],
                    "<rss><channel></channel></rss>".to_owned(),
                )
            })
            .await,
        );
        let magnet = test_candidate("magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567");
        let cache_dir = unique_temp_dir("candidate-failures");
        let client = test_candidate_download_client();

        assert!(matches!(
            client
                .download_and_cache(&rate_limited, &cache_dir, None)
                .await
                .unwrap_err(),
            CandidateDownloadError::RateLimited { .. }
        ));
        assert!(matches!(
            client
                .download_and_cache(&invalid, &cache_dir, None)
                .await
                .unwrap_err(),
            CandidateDownloadError::InvalidContents { .. }
        ));
        assert!(matches!(
            client
                .download_and_cache(&rss, &cache_dir, None)
                .await
                .unwrap_err(),
            CandidateDownloadError::InvalidContents { .. }
        ));
        assert!(matches!(
            client
                .download_and_cache(&magnet, &cache_dir, None)
                .await
                .unwrap_err(),
            CandidateDownloadError::MagnetLink
        ));
        assert!(CandidateDownloadPolicy::default().should_attempt(2));
        assert!(!CandidateDownloadPolicy::default().should_attempt(3));

        remove_temp_dir(&cache_dir);
    }

    #[tokio::test]
    async fn candidate_download_maps_magnet_redirects() {
        let redirect = test_candidate(
            &spawn_torznab_server(|_request| async move {
                (
                    AxumStatusCode::FOUND,
                    [(
                        LOCATION,
                        "MAGNET:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567",
                    )],
                    "",
                )
                    .into_response()
            })
            .await,
        );
        let cache_dir = unique_temp_dir("candidate-magnet-redirect");
        let client = test_candidate_download_client();

        let error = client
            .download_and_cache(&redirect, &cache_dir, None)
            .await
            .unwrap_err();

        assert!(
            matches!(error, CandidateDownloadError::MagnetLink),
            "{error:?}"
        );

        remove_temp_dir(&cache_dir);
    }

    #[tokio::test]
    async fn candidate_download_does_not_follow_internal_redirects() {
        let reached_internal_target = Arc::new(AtomicBool::new(false));
        let target_reached_internal_target = Arc::clone(&reached_internal_target);
        let target_url = spawn_torznab_server(move |_request| {
            let target_reached_internal_target = Arc::clone(&target_reached_internal_target);
            async move {
                target_reached_internal_target.store(true, AtomicOrdering::Relaxed);
                (
                    AxumStatusCode::OK,
                    String::from_utf8(test_torrent_bytes()).unwrap(),
                )
                    .into_response()
            }
        })
        .await;
        let redirect = test_candidate(
            &spawn_torznab_server(move |_request| {
                let target_url = target_url.clone();
                async move {
                    (
                        AxumStatusCode::FOUND,
                        [(
                            LOCATION,
                            HeaderValue::from_str(&format!("{target_url}/download")).unwrap(),
                        )],
                        "",
                    )
                        .into_response()
                }
            })
            .await,
        );
        let cache_dir = unique_temp_dir("candidate-internal-redirect");
        let client = test_candidate_download_client();

        let error = client
            .download_and_cache(&redirect, &cache_dir, None)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            CandidateDownloadError::HttpStatus { status: 302, .. }
        ));
        assert!(!reached_internal_target.load(AtomicOrdering::Relaxed));

        remove_temp_dir(&cache_dir);
    }

    #[tokio::test]
    async fn candidate_download_rejects_internal_literal_urls_before_request() {
        let reached_internal_target = Arc::new(AtomicBool::new(false));
        let target_reached_internal_target = Arc::clone(&reached_internal_target);
        let target_url = spawn_torznab_server(move |_request| {
            let target_reached_internal_target = Arc::clone(&target_reached_internal_target);
            async move {
                target_reached_internal_target.store(true, AtomicOrdering::Relaxed);
                (
                    AxumStatusCode::OK,
                    String::from_utf8(test_torrent_bytes()).unwrap(),
                )
                    .into_response()
            }
        })
        .await;
        let cache_dir = unique_temp_dir("candidate-internal-literal");
        let client = CandidateDownloadClient::new(Duration::from_secs(5));

        for url in [
            target_url,
            "http://169.254.169.254/latest/meta-data".to_owned(),
            "http://198.18.0.1/download".to_owned(),
            "http://[2001:db8::1]/download".to_owned(),
            "http://[::ffff:127.0.0.1]/download".to_owned(),
        ] {
            let error = client
                .download_and_cache(&test_candidate(&url), &cache_dir, None)
                .await
                .unwrap_err();

            assert!(matches!(error, CandidateDownloadError::InvalidUrl { .. }));
        }
        assert!(!reached_internal_target.load(AtomicOrdering::Relaxed));

        remove_temp_dir(&cache_dir);
    }

    #[tokio::test]
    async fn candidate_download_rejects_internal_resolved_hosts() {
        let cache_dir = unique_temp_dir("candidate-internal-dns");
        let client = CandidateDownloadClient::with_resolver(
            Duration::from_secs(5),
            Arc::new(SafeCandidateDownloadResolver::from_static_hosts(
                BTreeMap::from([(
                    "rebind.example".to_owned(),
                    vec!["198.18.0.1:0".parse().unwrap()],
                )]),
            )),
        );

        let error = client
            .download_and_cache(
                &test_candidate("https://rebind.example/download"),
                &cache_dir,
                None,
            )
            .await
            .unwrap_err();

        assert!(matches!(error, CandidateDownloadError::Request { .. }));

        remove_temp_dir(&cache_dir);
    }

    #[tokio::test]
    async fn candidate_download_rejects_oversized_torrent_response() {
        let url = spawn_torznab_server(|_request| async move {
            oversized_response(CANDIDATE_TORRENT_MAX_BYTES.saturating_add(1))
        })
        .await;
        let cache_dir = unique_temp_dir("candidate-oversized");
        let candidate = test_candidate(&url);
        let client = test_candidate_download_client();

        let error = client
            .download_and_cache(&candidate, &cache_dir, None)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            CandidateDownloadError::ResponseTooLarge {
                limit: CANDIDATE_TORRENT_MAX_BYTES
            }
        ));
        remove_temp_dir(&cache_dir);
    }

    #[tokio::test]
    async fn candidate_download_rejects_chunked_oversized_torrent_response() {
        let url = spawn_chunked_response_server(
            "/download",
            CANDIDATE_TORRENT_MAX_BYTES.saturating_add(1),
        );
        let cache_dir = unique_temp_dir("candidate-chunked-oversized");
        let candidate = test_candidate(&url);
        let client = test_candidate_download_client();

        let error = client
            .download_and_cache(&candidate, &cache_dir, None)
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                CandidateDownloadError::ResponseTooLarge {
                    limit: CANDIDATE_TORRENT_MAX_BYTES
                }
            ),
            "got {error:?}"
        );
        remove_temp_dir(&cache_dir);
    }

    #[test]
    fn indexer_backoff_uses_retry_after_exponential_delay_and_recovery_probes() {
        let policy = IndexerBackoffPolicy {
            base_delay_ms: 1_000,
            max_delay_ms: 10_000,
            jitter_ms: 100,
            recovery_probe_interval_ms: 500,
        };

        assert_eq!(
            1_500,
            policy.retry_after_deadline(1_000, 3, Some(RetryAfter::DelayMs(500)), "main")
        );
        assert_eq!(
            6_000,
            policy.retry_after_deadline(1_000, 3, Some(RetryAfter::DeadlineMs(6_000)), "main")
        );
        assert_eq!(
            1_000,
            policy.retry_after_deadline(1_000, 3, Some(RetryAfter::DeadlineMs(500)), "main")
        );
        let main_deadline = policy.retry_after_deadline(1_000, 3, None, "main");
        let backup_deadline = policy.retry_after_deadline(1_000, 3, None, "backup");
        assert_eq!(
            main_deadline,
            policy.retry_after_deadline(1_000, 3, None, "main")
        );
        assert_ne!(main_deadline, backup_deadline);
        assert!((9_000..9_100).contains(&main_deadline));
        assert!((9_000..9_100).contains(&backup_deadline));
        assert!(!policy.should_probe(1_100, Some(2_000), Some(800), false));
        assert!(policy.should_probe(1_400, Some(2_000), Some(800), false));
        assert!(!policy.should_probe(1_400, Some(2_000), Some(800), true));
        assert!(policy.should_probe(2_100, Some(2_000), Some(2_000), true));
    }

    #[test]
    fn retry_after_parser_preserves_header_semantics() {
        assert_eq!(Some(RetryAfter::DelayMs(5_000)), parse_retry_after("5"));
        assert_eq!(
            Some(RetryAfter::DeadlineMs(5_000)),
            parse_retry_after("Thu, 01 Jan 1970 00:00:05 GMT")
        );
    }

    #[test]
    fn caps_parser_extracts_search_categories_and_limits() {
        let caps = parse_torznab_caps(
            r#"
            <caps>
              <limits default="50" max="200"/>
              <searching>
                <search available="yes" supportedParams="q"/>
                <tv-search available="yes" supportedParams="q,tvdbid,imdbid"/>
                <movie-search available="yes" supportedParams="q,imdbid"/>
                <audio-search available="yes" supportedParams="q,imdbid"/>
              </searching>
              <categories>
                <category id="2000" name="Movies"/>
                <category id="5000" name="TV"/>
                <category id="5070" name="Anime"/>
                <category id="3000" name="Audio"/>
                <category id="7020" name="Books"/>
                <category id="1010" name="Other"/>
              </categories>
            </caps>
            "#,
        )
        .unwrap();

        assert_eq!(
            TorznabLimits {
                default: 50,
                max: 200
            },
            caps.limits
        );
        assert!(caps.search.generic_search);
        assert!(caps.search.tv_search);
        assert!(caps.search.audio_search);
        assert!(caps.search.supported_id_params.contains("tvdbid"));
        assert!(caps.categories.movie);
        assert!(caps.categories.additional);
        assert!(caps.supports_media_type(MediaType::Episode));
        assert!(caps.supports_media_type(MediaType::Movie));
        assert!(caps.supports_media_type(MediaType::Audio));
    }

    #[test]
    fn caps_parser_defaults_limits_and_rejects_unsupported_search() {
        let audio_only = parse_torznab_caps(
            r#"
            <caps>
              <searching>
                <audio-search available="yes" supportedParams="q"/>
              </searching>
              <categories>
                <category id="3000" name="Audio"/>
              </categories>
            </caps>
            "#,
        )
        .unwrap();
        let error = parse_torznab_caps(
            r#"
            <caps>
              <searching>
                <search available="no"/>
              </searching>
            </caps>
            "#,
        )
        .unwrap_err();

        assert!(audio_only.search.audio_search);
        assert!(audio_only.supports_media_type(MediaType::Audio));
        assert_eq!(TorznabCapsError::UnsupportedSearch, error);
    }

    #[test]
    fn caps_parser_rejects_bad_xml() {
        let error = parse_torznab_caps("<caps><").unwrap_err();

        assert!(matches!(error, TorznabCapsError::InvalidXml { .. }));
    }

    async fn spawn_torznab_server<F, Fut, R>(handler: F) -> String
    where
        F: Fn(Request<Body>) -> Fut + Clone + Send + Sync + 'static,
        Fut: std::future::Future<Output = R> + Send + 'static,
        R: IntoResponse + Send + 'static,
    {
        let app = Router::new().route("/api", get(handler));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{address}/api")
    }

    fn test_candidate_download_client() -> CandidateDownloadClient {
        CandidateDownloadClient::allow_internal_for_tests(Duration::from_secs(5))
    }

    async fn spawn_prowlarr_server<F, Fut, R>(handler: F) -> String
    where
        F: Fn(Request<Body>) -> Fut + Clone + Send + Sync + 'static,
        Fut: std::future::Future<Output = R> + Send + 'static,
        R: IntoResponse + Send + 'static,
    {
        let app = Router::new()
            .route("/api/v1/indexer", get(handler.clone()))
            .route("/api/v1/tag", get(handler));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{address}")
    }

    fn test_prowlarr_source(
        url: String,
        tags: &[&str],
        tag_match: ProwlarrTagMatch,
        include_untagged: bool,
    ) -> ProwlarrSource {
        ProwlarrSource {
            name: DependencyName::new("main").unwrap(),
            url,
            api_key: ApiKey::new("prowlarr-secret").unwrap(),
            api_key_source: ApiKeySource::Direct,
            tags: tags.iter().map(|tag| (*tag).to_owned()).collect(),
            tag_match,
            include_untagged,
        }
    }

    fn test_endpoint(url: String) -> TorznabEndpoint {
        TorznabEndpoint {
            indexer_id: IndexerId::new(1).unwrap(),
            name: DependencyName::new("main").unwrap(),
            url: SanitizedTorznabUrl::new(url).unwrap(),
            api_key: Some("secret".to_owned()),
            caps: test_caps(),
            retry_after_ms: None,
        }
    }

    fn test_caps() -> TorznabCaps {
        TorznabCaps {
            search: SearchCaps {
                generic_search: true,
                tv_search: true,
                movie_search: true,
                audio_search: true,
                supported_id_params: ["tvdbid".to_owned()].into_iter().collect(),
            },
            categories: CategoryCaps {
                tv: true,
                movie: true,
                anime: false,
                xxx: false,
                audio: true,
                book: false,
                additional: false,
            },
            limits: TorznabLimits {
                default: 50,
                max: 50,
            },
        }
    }

    fn generic_plan() -> TorznabSearchPlan {
        TorznabSearchPlan {
            query: TorznabSearchQuery {
                search_type: TorznabSearchType::Search,
                q: Some("Example".to_owned()),
                season: None,
                episode: None,
                ids: SearchIds::default(),
            },
            limit: 50,
        }
    }

    fn search_rss(guid: &str, title: &str) -> String {
        format!(
            r#"
            <rss>
              <channel>
                <item>
                  <title>{title}</title>
                  <guid>{guid}</guid>
                  <link>https://indexer.example/download/{guid}</link>
                  <pubDate>Thu, 01 Jan 1970 00:00:01 GMT</pubDate>
                  <torznab:attr name="size" value="1234"/>
                  <torznab:attr name="infohash" value="0123456789abcdef0123456789abcdef01234567"/>
                </item>
              </channel>
            </rss>
            "#
        )
    }

    fn rss_items(guids: &[&str]) -> String {
        let mut items = String::new();
        for guid in guids {
            items.push_str(&search_item(guid));
        }
        format!("<rss><channel>{items}</channel></rss>")
    }

    fn search_item(guid: &str) -> String {
        format!(
            r#"
            <item>
              <title>{guid}</title>
              <guid>{guid}</guid>
              <link>https://indexer.example/download/{guid}</link>
              <pubDate>Thu, 01 Jan 1970 00:00:01 GMT</pubDate>
            </item>
            "#
        )
    }

    fn candidate_guids(candidates: &[RemoteCandidate]) -> Vec<&str> {
        candidates
            .iter()
            .map(|candidate| candidate.guid.as_str())
            .collect()
    }

    fn test_candidate(url: &str) -> RemoteCandidate {
        RemoteCandidate {
            id: None,
            indexer_id: IndexerId::new(1).unwrap(),
            guid: CandidateGuid::new(format!("guid-{url}")).unwrap(),
            download_url: DownloadUrl::new(url).unwrap(),
            title: ItemTitle::new("Example").unwrap(),
            tracker: TrackerName::new("main").unwrap(),
            size: None,
            published_at_ms: None,
            info_hash: None,
            torrent_cache_path: None,
        }
    }

    fn test_torrent_bytes() -> Vec<u8> {
        b"d8:announce32:https://tracker.example/announce4:infod6:lengthi12e4:name9:movie.mkv12:piece lengthi12e6:pieces20:aaaaaaaaaaaaaaaaaaaaee".to_vec()
    }

    fn alternate_test_torrent_bytes() -> Vec<u8> {
        b"d8:announce34:https://other.example:443/announce4:infod6:lengthi12e4:name9:movie.mkv12:piece lengthi12e6:pieces20:aaaaaaaaaaaaaaaaaaaaee".to_vec()
    }

    fn unique_temp_dir(label: &str) -> std::path::PathBuf {
        let unique = std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("sporos-{label}-{unique}"))
    }

    fn remove_temp_dir(path: &Path) {
        match fs::remove_dir_all(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => panic!("remove temp dir {}: {error}", path.display()),
        }
    }

    fn oversized_response(length: u64) -> Response {
        let body = vec![b'x'; usize::try_from(length).unwrap()];
        (
            AxumStatusCode::OK,
            [(
                CONTENT_LENGTH,
                HeaderValue::from_str(&length.to_string()).unwrap(),
            )],
            body,
        )
            .into_response()
    }

    fn spawn_chunked_response_server(path: &str, length: u64) -> String {
        let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0_u8; 4096];
            let _ = stream.read(&mut request);
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
                )
                .unwrap();
            write_chunked_body(&mut stream, length);
        });
        format!("http://{address}{path}")
    }

    fn write_chunked_body(stream: &mut std::net::TcpStream, length: u64) {
        let chunk = vec![b'x'; 8192];
        let mut remaining = length;
        while remaining > 0 {
            let size = usize::try_from(remaining.min(chunk.len() as u64)).unwrap();
            write!(stream, "{size:x}\r\n").unwrap();
            stream.write_all(&chunk[..size]).unwrap();
            stream.write_all(b"\r\n").unwrap();
            remaining -= u64::try_from(size).unwrap();
        }
        stream.write_all(b"0\r\n\r\n").unwrap();
    }
}
