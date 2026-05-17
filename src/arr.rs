#![cfg_attr(
    test,
    expect(
        clippy::let_underscore_must_use,
        reason = "test server teardown intentionally ignores post-test join outcome"
    )
)]

use std::collections::BTreeSet;
use std::fmt;
use std::time::Duration;

use reqwest::header::{RETRY_AFTER, USER_AGENT};
use reqwest::{StatusCode, redirect};
use serde::Deserialize;

use crate::config::{ArrInstanceConfig, ArrServicesConfig};
use crate::domain::{DependencyName, ItemTitle, MediaType};
use crate::indexers::{ApiKeySource, IndexerBackoffPolicy, RetryAfter, parse_retry_after};
use crate::matching::SearchIds;

const ARR_PARSE_MAX_BYTES: u64 = 1024 * 1024;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ArrKind {
    Sonarr,
    Radarr,
}

impl ArrKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sonarr => "sonarr",
            Self::Radarr => "radarr",
        }
    }
}

impl fmt::Display for ArrKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ConfiguredArrInstance {
    pub kind: ArrKind,
    pub name: DependencyName,
    pub url: SanitizedArrUrl,
    pub api_key: crate::secrets::ApiKey,
    pub api_key_source: ApiKeySource,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ArrRegistry {
    instances: Vec<ConfiguredArrInstance>,
}

impl ArrRegistry {
    pub fn from_config(config: &ArrServicesConfig) -> Result<Self, ArrConfigError> {
        let mut seen_urls = BTreeSet::new();
        let total = config.sonarr.len().saturating_add(config.radarr.len());
        let mut instances = Vec::with_capacity(total);

        collect_configured_instances(
            ArrKind::Sonarr,
            &config.sonarr,
            &mut seen_urls,
            &mut instances,
        )?;
        collect_configured_instances(
            ArrKind::Radarr,
            &config.radarr,
            &mut seen_urls,
            &mut instances,
        )?;

        Ok(Self { instances })
    }

    pub fn instances(&self) -> &[ConfiguredArrInstance] {
        &self.instances
    }

    pub fn is_empty(&self) -> bool {
        self.instances.is_empty()
    }
}

fn collect_configured_instances(
    kind: ArrKind,
    configured: &std::collections::BTreeMap<String, ArrInstanceConfig>,
    seen_urls: &mut BTreeSet<String>,
    instances: &mut Vec<ConfiguredArrInstance>,
) -> Result<(), ArrConfigError> {
    for (name, instance) in configured {
        let configured = configured_arr_instance(kind, name, instance)?;
        if !seen_urls.insert(configured.url.as_str().to_owned()) {
            return Err(ArrConfigError::DuplicateUrl {
                url: configured.url.as_str().to_owned(),
            });
        }
        instances.push(configured);
    }

    Ok(())
}

fn configured_arr_instance(
    kind: ArrKind,
    name: &str,
    instance: &ArrInstanceConfig,
) -> Result<ConfiguredArrInstance, ArrConfigError> {
    let name = DependencyName::new(format!("{}-{name}", kind.as_str())).map_err(|error| {
        ArrConfigError::InvalidName {
            message: error.to_string(),
        }
    })?;
    let api_key = instance
        .api_key
        .clone()
        .ok_or_else(|| ArrConfigError::MissingApiKey {
            name: name.as_str().to_owned(),
        })?;
    let api_key_source = if instance.api_key_file.is_some() {
        ApiKeySource::File(
            instance
                .api_key_file
                .as_ref()
                .map(|path| path.display().to_string())
                .unwrap_or_default(),
        )
    } else if let Some(env) = instance.api_key_env.as_ref() {
        ApiKeySource::Env(env.clone())
    } else {
        ApiKeySource::Direct
    };

    Ok(ConfiguredArrInstance {
        kind,
        name,
        url: SanitizedArrUrl::new(&instance.url)?,
        api_key,
        api_key_source,
    })
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SanitizedArrUrl(String);

impl SanitizedArrUrl {
    pub fn new(value: impl AsRef<str>) -> Result<Self, ArrConfigError> {
        let value = value.as_ref();
        let parsed = reqwest::Url::parse(value).map_err(|error| ArrConfigError::InvalidUrl {
            message: error.to_string(),
        })?;
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err(ArrConfigError::InvalidUrl {
                message: "Arr URL must use http or https".to_owned(),
            });
        }
        if !parsed.username().is_empty() || parsed.password().is_some() {
            return Err(ArrConfigError::InvalidUrl {
                message: "Arr URL must not include credentials".to_owned(),
            });
        }
        if parsed.query().is_some() {
            return Err(ArrConfigError::InvalidUrl {
                message: "Arr URL must not include query parameters".to_owned(),
            });
        }
        if parsed.fragment().is_some() {
            return Err(ArrConfigError::InvalidUrl {
                message: "Arr URL must not include fragments".to_owned(),
            });
        }
        let sanitized = parsed.as_str().trim_end_matches('/').to_owned();
        Ok(Self(sanitized))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SanitizedArrUrl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ArrConfigError {
    InvalidName { message: String },
    InvalidUrl { message: String },
    DuplicateUrl { url: String },
    MissingApiKey { name: String },
}

impl fmt::Display for ArrConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidName { message } => write!(formatter, "invalid Arr name: {message}"),
            Self::InvalidUrl { message } => write!(formatter, "invalid Arr URL: {message}"),
            Self::DuplicateUrl { url } => write!(formatter, "duplicate Arr URL `{url}`"),
            Self::MissingApiKey { name } => {
                write!(formatter, "Arr instance `{name}` is missing an API key")
            }
        }
    }
}

impl std::error::Error for ArrConfigError {}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ArrEndpoint {
    pub kind: ArrKind,
    pub name: DependencyName,
    pub url: SanitizedArrUrl,
    pub api_key: crate::secrets::ApiKey,
    pub retry_after_ms: Option<i64>,
    pub consecutive_failures: u16,
}

impl ArrEndpoint {
    pub fn from_configured(instance: &ConfiguredArrInstance) -> Self {
        Self {
            kind: instance.kind,
            name: instance.name.clone(),
            url: instance.url.clone(),
            api_key: instance.api_key.clone(),
            retry_after_ms: None,
            consecutive_failures: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ArrHttpClient {
    client: reqwest::Client,
    timeout: Duration,
    backoff: IndexerBackoffPolicy,
}

impl ArrHttpClient {
    pub fn new(timeout: Duration) -> Self {
        Self {
            client: reqwest::Client::builder()
                .redirect(redirect::Policy::none())
                .build()
                .expect("redirect-disabled Arr HTTP client should build"),
            timeout,
            backoff: IndexerBackoffPolicy::default(),
        }
    }

    pub fn with_backoff(mut self, backoff: IndexerBackoffPolicy) -> Self {
        self.backoff = backoff;
        self
    }

    pub async fn lookup_ids(
        &self,
        endpoints: &[ArrEndpoint],
        media_type: MediaType,
        title: &ItemTitle,
        now_ms: i64,
    ) -> ArrLookupResult {
        let mut attempts = Vec::new();
        for kind in arr_lookup_order(media_type) {
            for endpoint in endpoints
                .iter()
                .filter(|endpoint| endpoint.kind == *kind)
                .filter(|endpoint| endpoint_supports_media(endpoint.kind, media_type))
            {
                match self
                    .lookup_endpoint(endpoint, media_type, title, now_ms)
                    .await
                {
                    Ok(ids) if !ids.is_empty() => {
                        attempts.push(ArrLookupAttempt::found(endpoint, None));
                        return ArrLookupResult { ids, attempts };
                    }
                    Ok(_) => attempts.push(ArrLookupAttempt::empty(endpoint)),
                    Err(ArrRequestError::Backoff { retry_after_ms }) => {
                        attempts.push(ArrLookupAttempt::backoff(endpoint, retry_after_ms));
                    }
                    Err(error) => {
                        let retry_after_ms = self.backoff.retry_after_deadline(
                            now_ms,
                            endpoint.consecutive_failures.saturating_add(1),
                            error.retry_after(),
                            endpoint.name.as_str(),
                        );
                        attempts.push(ArrLookupAttempt::failure(
                            endpoint,
                            retry_after_ms,
                            error.to_string(),
                            error.is_unavailable(),
                        ));
                    }
                }
            }
        }

        ArrLookupResult {
            ids: SearchIds::default(),
            attempts,
        }
    }

    pub async fn lookup_endpoint(
        &self,
        endpoint: &ArrEndpoint,
        media_type: MediaType,
        title: &ItemTitle,
        now_ms: i64,
    ) -> Result<SearchIds, ArrRequestError> {
        if endpoint
            .retry_after_ms
            .is_some_and(|retry_after| retry_after > now_ms)
        {
            return Err(ArrRequestError::Backoff {
                retry_after_ms: endpoint.retry_after_ms,
            });
        }
        if !endpoint_supports_media(endpoint.kind, media_type) {
            return Ok(SearchIds::default());
        }

        let parse_title = arr_parse_title(endpoint.kind, media_type, title.as_str());
        let response = self
            .client
            .get(format!("{}/api/v3/parse", endpoint.url.as_str()))
            .header(USER_AGENT, concat!("Sporos/", env!("CARGO_PKG_VERSION")))
            .header("X-Api-Key", endpoint.api_key.expose_secret())
            .query(&[("title", parse_title)])
            .timeout(self.timeout)
            .send()
            .await
            .map_err(ArrRequestError::from_reqwest)?;
        let status = response.status();
        if !status.is_success() {
            let retry_after = response
                .headers()
                .get(RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .and_then(parse_retry_after);
            if status == StatusCode::TOO_MANY_REQUESTS {
                return Err(ArrRequestError::RateLimited { retry_after });
            }
            if matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN) {
                return Err(ArrRequestError::Unauthorized);
            }
            return Err(ArrRequestError::HttpStatus {
                status: status.as_u16(),
                retry_after,
            });
        }

        let bytes = read_arr_response(response).await?;
        let parsed = serde_json::from_slice::<ArrParseResponse>(&bytes).map_err(|error| {
            ArrRequestError::InvalidJson {
                message: error.to_string(),
            }
        })?;
        Ok(parsed.into_ids())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ArrLookupResult {
    pub ids: SearchIds,
    pub attempts: Vec<ArrLookupAttempt>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ArrLookupAttempt {
    pub kind: ArrKind,
    pub name: DependencyName,
    pub outcome: ArrLookupOutcome,
}

impl ArrLookupAttempt {
    fn found(endpoint: &ArrEndpoint, retry_after_ms: Option<i64>) -> Self {
        Self {
            kind: endpoint.kind,
            name: endpoint.name.clone(),
            outcome: ArrLookupOutcome::Found { retry_after_ms },
        }
    }

    fn empty(endpoint: &ArrEndpoint) -> Self {
        Self {
            kind: endpoint.kind,
            name: endpoint.name.clone(),
            outcome: ArrLookupOutcome::Empty,
        }
    }

    fn backoff(endpoint: &ArrEndpoint, retry_after_ms: Option<i64>) -> Self {
        Self {
            kind: endpoint.kind,
            name: endpoint.name.clone(),
            outcome: ArrLookupOutcome::Backoff { retry_after_ms },
        }
    }

    fn failure(
        endpoint: &ArrEndpoint,
        retry_after_ms: i64,
        reason: String,
        unavailable: bool,
    ) -> Self {
        Self {
            kind: endpoint.kind,
            name: endpoint.name.clone(),
            outcome: ArrLookupOutcome::Failure {
                retry_after_ms,
                reason,
                unavailable,
            },
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ArrLookupOutcome {
    Found {
        retry_after_ms: Option<i64>,
    },
    Empty,
    Backoff {
        retry_after_ms: Option<i64>,
    },
    Failure {
        retry_after_ms: i64,
        reason: String,
        unavailable: bool,
    },
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ArrRequestError {
    Backoff {
        retry_after_ms: Option<i64>,
    },
    RateLimited {
        retry_after: Option<RetryAfter>,
    },
    Unauthorized,
    HttpStatus {
        status: u16,
        retry_after: Option<RetryAfter>,
    },
    Timeout,
    Request {
        message: String,
    },
    InvalidJson {
        message: String,
    },
    ResponseTooLarge {
        limit: u64,
    },
}

impl ArrRequestError {
    fn from_reqwest(error: reqwest::Error) -> Self {
        if error.is_timeout() {
            Self::Timeout
        } else {
            Self::Request {
                message: crate::secrets::sanitize_url_for_logging(error.to_string()).to_string(),
            }
        }
    }

    fn retry_after(&self) -> Option<RetryAfter> {
        match self {
            Self::RateLimited { retry_after } | Self::HttpStatus { retry_after, .. } => {
                *retry_after
            }
            Self::Backoff { .. }
            | Self::Unauthorized
            | Self::Timeout
            | Self::Request { .. }
            | Self::InvalidJson { .. }
            | Self::ResponseTooLarge { .. } => None,
        }
    }

    fn is_unavailable(&self) -> bool {
        matches!(
            self,
            Self::Timeout | Self::Request { .. } | Self::ResponseTooLarge { .. }
        ) || matches!(self, Self::HttpStatus { status, .. } if *status >= 500)
    }
}

impl fmt::Display for ArrRequestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Backoff { .. } => formatter.write_str("Arr instance is in backoff"),
            Self::RateLimited { .. } => formatter.write_str("Arr instance returned a rate limit"),
            Self::Unauthorized => formatter.write_str("Arr instance rejected credentials"),
            Self::HttpStatus { status, .. } => {
                write!(formatter, "Arr instance returned HTTP status {status}")
            }
            Self::Timeout => formatter.write_str("Arr request timed out"),
            Self::Request { message } => write!(formatter, "Arr request failed: {message}"),
            Self::InvalidJson { message } => write!(formatter, "invalid Arr parse JSON: {message}"),
            Self::ResponseTooLarge { limit } => {
                write!(formatter, "Arr response exceeded {limit} bytes")
            }
        }
    }
}

impl std::error::Error for ArrRequestError {}

#[derive(Debug, Deserialize)]
struct ArrParseResponse {
    #[serde(default)]
    series: Option<ArrSeries>,
    #[serde(default)]
    movie: Option<ArrMovie>,
}

impl ArrParseResponse {
    fn into_ids(self) -> SearchIds {
        let mut ids = SearchIds::default();
        if let Some(series) = self.series {
            ids.tvdb_id = nonzero_i64(series.tvdb_id).map(|id| id.to_string());
            ids.tvmaze_id = nonzero_i64(series.tvmaze_id).map(|id| id.to_string());
            ids.imdb_id = nonempty_string(series.imdb_id);
        }
        if let Some(movie) = self.movie {
            ids.tmdb_id = nonzero_i64(movie.tmdb_id).map(|id| id.to_string());
            if ids.imdb_id.is_none() {
                ids.imdb_id = nonempty_string(movie.imdb_id);
            }
        }
        ids
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ArrSeries {
    #[serde(default, alias = "tvdb_id")]
    tvdb_id: Option<i64>,
    #[serde(default, alias = "tvMazeId", alias = "tvmaze_id")]
    tvmaze_id: Option<i64>,
    #[serde(default, alias = "imdb_id")]
    imdb_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ArrMovie {
    #[serde(default, alias = "tmdb_id")]
    tmdb_id: Option<i64>,
    #[serde(default, alias = "imdb_id")]
    imdb_id: Option<String>,
}

async fn read_arr_response(mut response: reqwest::Response) -> Result<Vec<u8>, ArrRequestError> {
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(ArrRequestError::from_reqwest)?
    {
        let next_len =
            bytes
                .len()
                .checked_add(chunk.len())
                .ok_or(ArrRequestError::ResponseTooLarge {
                    limit: ARR_PARSE_MAX_BYTES,
                })?;
        if u64::try_from(next_len).unwrap_or(u64::MAX) > ARR_PARSE_MAX_BYTES {
            return Err(ArrRequestError::ResponseTooLarge {
                limit: ARR_PARSE_MAX_BYTES,
            });
        }
        bytes.extend_from_slice(&chunk);
    }

    Ok(bytes)
}

fn endpoint_supports_media(kind: ArrKind, media_type: MediaType) -> bool {
    matches!(
        (kind, media_type),
        (ArrKind::Sonarr, MediaType::Episode | MediaType::SeasonPack)
            | (ArrKind::Sonarr, MediaType::Anime | MediaType::Video)
            | (ArrKind::Radarr, MediaType::Movie)
            | (ArrKind::Radarr, MediaType::Anime | MediaType::Video)
    )
}

fn arr_lookup_order(media_type: MediaType) -> &'static [ArrKind] {
    match media_type {
        MediaType::Episode | MediaType::SeasonPack => &[ArrKind::Sonarr],
        MediaType::Movie => &[ArrKind::Radarr],
        MediaType::Anime | MediaType::Video => &[ArrKind::Sonarr, ArrKind::Radarr],
        MediaType::Audio | MediaType::Book | MediaType::Archive | MediaType::Unknown => &[],
    }
}

fn arr_parse_title(kind: ArrKind, media_type: MediaType, title: &str) -> String {
    let stripped = strip_scene_metadata(title);
    if kind == ArrKind::Sonarr && matches!(media_type, MediaType::Anime | MediaType::Video) {
        format!("{stripped} S00E00")
    } else {
        stripped
    }
}

fn strip_scene_metadata(title: &str) -> String {
    let mut words = Vec::new();
    for word in title.split(['.', '_', ' ']).filter(|word| !word.is_empty()) {
        let lower = word.to_ascii_lowercase();
        if is_release_metadata_token(&lower) {
            break;
        }
        words.push(word);
    }
    if words.is_empty() {
        title.trim().to_owned()
    } else {
        words.join(" ")
    }
}

fn is_release_metadata_token(token: &str) -> bool {
    matches!(
        token,
        "480p"
            | "576p"
            | "720p"
            | "1080p"
            | "2160p"
            | "web"
            | "webrip"
            | "web-dl"
            | "hdtv"
            | "bluray"
            | "bdrip"
            | "dvdrip"
            | "x264"
            | "x265"
            | "h264"
            | "h265"
            | "hevc"
    )
}

fn nonzero_i64(value: Option<i64>) -> Option<i64> {
    value.filter(|value| *value > 0)
}

fn nonempty_string(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() || trimmed == "0" {
            None
        } else {
            Some(trimmed.to_owned())
        }
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::convert::Infallible;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

    use axum::Router;
    use axum::body::Body;
    use axum::http::header::LOCATION;
    use axum::http::{HeaderValue, Request, StatusCode as AxumStatusCode};
    use axum::response::{IntoResponse, Response};
    use axum::routing::get;
    use tokio::net::TcpListener;

    use super::*;
    use crate::config::ArrInstanceConfig;
    use crate::secrets::ApiKey;

    #[test]
    fn registry_sanitizes_urls_and_requires_secret_keys() {
        let mut sonarr = BTreeMap::new();
        sonarr.insert(
            "main".to_owned(),
            ArrInstanceConfig {
                url: "http://sonarr:8989/".to_owned(),
                api_key: Some(ApiKey::new("secret").unwrap()),
                api_key_file: None,
                api_key_env: None,
            },
        );
        let config = ArrServicesConfig {
            sonarr,
            radarr: BTreeMap::new(),
        };

        let registry = ArrRegistry::from_config(&config).unwrap();

        assert_eq!(1, registry.instances().len());
        assert_eq!("sonarr-main", registry.instances()[0].name.as_str());
        assert_eq!("http://sonarr:8989", registry.instances()[0].url.as_str());
        assert!(!format!("{registry:?}").contains("secret"));
    }

    #[test]
    fn registry_rejects_query_keys_missing_keys_and_duplicate_urls() {
        let missing = ArrServicesConfig {
            sonarr: [(
                "main".to_owned(),
                ArrInstanceConfig {
                    url: "http://sonarr:8989".to_owned(),
                    api_key: None,
                    api_key_file: None,
                    api_key_env: None,
                },
            )]
            .into_iter()
            .collect(),
            radarr: BTreeMap::new(),
        };
        let duplicate = ArrServicesConfig {
            sonarr: [(
                "main".to_owned(),
                ArrInstanceConfig {
                    url: "http://arr:8989".to_owned(),
                    api_key: Some(ApiKey::new("secret").unwrap()),
                    api_key_file: None,
                    api_key_env: None,
                },
            )]
            .into_iter()
            .collect(),
            radarr: [(
                "main".to_owned(),
                ArrInstanceConfig {
                    url: "http://arr:8989/".to_owned(),
                    api_key: Some(ApiKey::new("secret").unwrap()),
                    api_key_file: None,
                    api_key_env: None,
                },
            )]
            .into_iter()
            .collect(),
        };

        assert!(matches!(
            ArrRegistry::from_config(&missing).unwrap_err(),
            ArrConfigError::MissingApiKey { .. }
        ));
        assert!(matches!(
            SanitizedArrUrl::new("http://sonarr:8989?apikey=secret").unwrap_err(),
            ArrConfigError::InvalidUrl { .. }
        ));
        assert!(matches!(
            SanitizedArrUrl::new("http://sonarr:8989#apikey=secret").unwrap_err(),
            ArrConfigError::InvalidUrl { .. }
        ));
        assert!(matches!(
            ArrRegistry::from_config(&duplicate).unwrap_err(),
            ArrConfigError::DuplicateUrl { .. }
        ));
    }

    #[tokio::test]
    async fn client_sends_secret_header_and_decodes_sonarr_ids() {
        let endpoint = endpoint(
            ArrKind::Sonarr,
            spawn_arr_server(|request| async move {
                let has_key = request
                    .headers()
                    .get("X-Api-Key")
                    .and_then(|value| value.to_str().ok())
                    == Some("secret");
                let query = request.uri().query().unwrap_or_default();
                if !has_key || !query.contains("title=Example") {
                    return (AxumStatusCode::BAD_REQUEST, "{}".to_owned()).into_response();
                }
                (
                    AxumStatusCode::OK,
                    r#"{"series":{"tvdbId":42,"tvMazeId":84,"imdbId":"tt123"}}"#.to_owned(),
                )
                    .into_response()
            })
            .await,
        );
        let client = ArrHttpClient::new(Duration::from_secs(5));

        let ids = client
            .lookup_endpoint(
                &endpoint,
                MediaType::Episode,
                &ItemTitle::new("Example.1080p.WEB-DL").unwrap(),
                1_000,
            )
            .await
            .unwrap();

        assert_eq!(Some("42"), ids.tvdb_id.as_deref());
        assert_eq!(Some("84"), ids.tvmaze_id.as_deref());
        assert_eq!(Some("tt123"), ids.imdb_id.as_deref());
    }

    #[tokio::test]
    async fn client_does_not_forward_api_key_on_redirect() {
        let saw_redirected_key = Arc::new(AtomicBool::new(false));
        let target_saw_redirected_key = saw_redirected_key.clone();
        let target_url = spawn_arr_server(move |request| {
            let target_saw_redirected_key = target_saw_redirected_key.clone();
            async move {
                if request.headers().get("x-api-key").is_some() {
                    target_saw_redirected_key.store(true, AtomicOrdering::Relaxed);
                }
                (AxumStatusCode::OK, "{}").into_response()
            }
        })
        .await;
        let redirect_url = target_url.clone();
        let endpoint = endpoint(
            ArrKind::Sonarr,
            spawn_arr_server(move |_request| {
                let redirect_url = redirect_url.clone();
                async move {
                    (
                        AxumStatusCode::FOUND,
                        [(
                            LOCATION,
                            HeaderValue::from_str(&format!("{redirect_url}/api/v3/parse")).unwrap(),
                        )],
                        "",
                    )
                        .into_response()
                }
            })
            .await,
        );
        let client = ArrHttpClient::new(Duration::from_secs(5));

        let error = client
            .lookup_endpoint(
                &endpoint,
                MediaType::Episode,
                &ItemTitle::new("Example.1080p.WEB-DL").unwrap(),
                1_000,
            )
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            ArrRequestError::HttpStatus { status: 302, .. }
        ));
        assert!(!saw_redirected_key.load(AtomicOrdering::Relaxed));
    }

    #[tokio::test]
    async fn lookup_uses_media_order_and_records_backoff_failures() {
        let sonarr = endpoint(
            ArrKind::Sonarr,
            spawn_arr_server(|_request| async move {
                (
                    AxumStatusCode::TOO_MANY_REQUESTS,
                    [("Retry-After", "5")],
                    "{}",
                )
                    .into_response()
            })
            .await,
        );
        let radarr = endpoint(
            ArrKind::Radarr,
            spawn_arr_server(|_request| async move {
                (AxumStatusCode::OK, r#"{"movie":{"tmdbId":99}}"#).into_response()
            })
            .await,
        );
        let client =
            ArrHttpClient::new(Duration::from_secs(5)).with_backoff(IndexerBackoffPolicy {
                base_delay_ms: 100,
                max_delay_ms: 10_000,
                jitter_ms: 0,
                recovery_probe_interval_ms: 100,
            });

        let result = client
            .lookup_ids(
                &[radarr, sonarr],
                MediaType::Video,
                &ItemTitle::new("Example.Movie.2160p.WEB-DL").unwrap(),
                1_000,
            )
            .await;

        assert_eq!(Some("99"), result.ids.tmdb_id.as_deref());
        assert_eq!(2, result.attempts.len());
        assert!(matches!(
            result.attempts[0].outcome,
            ArrLookupOutcome::Failure {
                retry_after_ms: 6_000,
                unavailable: false,
                ..
            }
        ));
        assert!(matches!(
            result.attempts[1].outcome,
            ArrLookupOutcome::Found { .. }
        ));
    }

    #[tokio::test]
    async fn lookup_falls_back_after_empty_arr_ids() {
        let sonarr = endpoint(
            ArrKind::Sonarr,
            spawn_arr_server(|_request| async move {
                (AxumStatusCode::OK, r#"{"series":{"tvdbId":0}}"#).into_response()
            })
            .await,
        );
        let radarr = endpoint(
            ArrKind::Radarr,
            spawn_arr_server(|_request| async move {
                (AxumStatusCode::OK, r#"{"movie":{"tmdbId":99}}"#).into_response()
            })
            .await,
        );
        let client = ArrHttpClient::new(Duration::from_secs(5));

        let result = client
            .lookup_ids(
                &[sonarr, radarr],
                MediaType::Video,
                &ItemTitle::new("Example.Movie.2160p.WEB-DL").unwrap(),
                1_000,
            )
            .await;

        assert_eq!(Some("99"), result.ids.tmdb_id.as_deref());
        assert_eq!(2, result.attempts.len());
        assert!(matches!(
            result.attempts[0].outcome,
            ArrLookupOutcome::Empty
        ));
        assert!(matches!(
            result.attempts[1].outcome,
            ArrLookupOutcome::Found { .. }
        ));
    }

    #[tokio::test]
    async fn client_honors_endpoint_backoff_without_request() {
        let endpoint = ArrEndpoint {
            retry_after_ms: Some(5_000),
            ..endpoint(ArrKind::Radarr, "http://127.0.0.1:9".to_owned())
        };
        let client = ArrHttpClient::new(Duration::from_millis(10));

        let error = client
            .lookup_endpoint(
                &endpoint,
                MediaType::Movie,
                &ItemTitle::new("Example").unwrap(),
                1_000,
            )
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            ArrRequestError::Backoff {
                retry_after_ms: Some(5_000)
            }
        ));
    }

    #[test]
    fn request_error_classification_treats_outages_as_unavailable() {
        assert!(ArrRequestError::Timeout.is_unavailable());
        assert!(
            ArrRequestError::Request {
                message: "connection refused".to_owned()
            }
            .is_unavailable()
        );
        assert!(
            ArrRequestError::HttpStatus {
                status: 503,
                retry_after: None
            }
            .is_unavailable()
        );
        assert!(!ArrRequestError::Unauthorized.is_unavailable());
        assert!(
            !ArrRequestError::InvalidJson {
                message: "bad payload".to_owned()
            }
            .is_unavailable()
        );
    }

    #[test]
    fn parse_title_strips_video_metadata_and_appends_sonarr_generic_episode() {
        assert_eq!(
            "Example Show S01E02",
            arr_parse_title(
                ArrKind::Sonarr,
                MediaType::Episode,
                "Example.Show.S01E02.1080p.WEB-DL"
            )
        );
        assert_eq!(
            "Example Show S00E00",
            arr_parse_title(
                ArrKind::Sonarr,
                MediaType::Video,
                "Example.Show.2160p.WEB-DL"
            )
        );
    }

    fn endpoint(kind: ArrKind, url: String) -> ArrEndpoint {
        ArrEndpoint {
            kind,
            name: DependencyName::new(format!("{}-main", kind.as_str())).unwrap(),
            url: SanitizedArrUrl::new(url).unwrap(),
            api_key: ApiKey::new("secret").unwrap(),
            retry_after_ms: None,
            consecutive_failures: 0,
        }
    }

    async fn spawn_arr_server<F, Fut>(handler: F) -> String
    where
        F: Fn(Request<Body>) -> Fut + Clone + Send + Sync + 'static,
        Fut: std::future::Future<Output = Response> + Send + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new().route(
            "/api/v3/parse",
            get(move |request| {
                let handler = handler.clone();
                async move { Ok::<_, Infallible>(handler(request).await) }
            }),
        );
        let server = axum::serve(listener, app);
        tokio::spawn(async move {
            let _ = server.await;
        });

        format!("http://{address}")
    }
}
