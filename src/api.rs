//! Daemon HTTP API routing, authentication, and response mapping.

use std::{
    borrow::Cow,
    collections::BTreeMap,
    fs,
    net::IpAddr,
    path::{Path, PathBuf},
    time::UNIX_EPOCH,
};

use async_trait::async_trait;
use url::{Url, form_urlencoded};

use crate::{
    SporosError,
    domain::{ActionResult, Candidate, Decision, InjectionResult, SaveResult},
};

const AUTH_MESSAGE: &str = "Specify the API key in an X-Api-Key header or an apikey query param.";

/// Minimal HTTP method model used by the no-framework daemon router.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ApiMethod {
    /// GET request.
    Get,
    /// POST request.
    Post,
    /// Any method not explicitly supported by the API.
    Other,
}

/// Parsed request passed from the HTTP server into the API router.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ApiRequest {
    /// HTTP method.
    pub method: ApiMethod,
    /// Path without query string.
    pub path: String,
    /// Query pairs decoded from the URL.
    pub query: BTreeMap<String, String>,
    /// Lowercase header map.
    pub headers: BTreeMap<String, String>,
    /// Raw request body.
    pub body: String,
    /// Best-effort remote address for auth logging.
    pub remote_addr: Option<String>,
}

impl ApiRequest {
    /// Build a request from method, target URI, headers, and body.
    pub fn new(
        method: ApiMethod,
        target: &str,
        headers: BTreeMap<String, String>,
        body: impl Into<String>,
    ) -> Self {
        let (path, query) = split_target(target);
        Self {
            method,
            path,
            query,
            headers: headers
                .into_iter()
                .map(|(key, value)| (key.to_ascii_lowercase(), value))
                .collect(),
            body: body.into(),
            remote_addr: None,
        }
    }
}

/// Response returned by API routing.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ApiResponse {
    /// HTTP status code.
    pub status: u16,
    /// Plain-text response body.
    pub body: String,
}

impl ApiResponse {
    fn new(status: u16, body: impl Into<String>) -> Self {
        Self {
            status,
            body: body.into(),
        }
    }
}

/// Validated `/api/announce` request body.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AnnounceRequest {
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
}

impl AnnounceRequest {
    /// Convert to the shared candidate model.
    pub fn into_candidate(self) -> Candidate<'static> {
        let mut candidate = Candidate::new(self.name, self.guid, Some(self.link), self.tracker);
        candidate.cookie = self.cookie.map(Cow::Owned);
        candidate
    }
}

/// Validated `/api/webhook` request body.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WebhookRequest {
    /// Optional info hash criterion.
    pub info_hash: Option<String>,
    /// Optional canonical filesystem path criterion.
    pub path: Option<String>,
    /// Filesystem snapshot captured when the request path was validated.
    pub path_snapshot: Option<WebhookPathSnapshot>,
    /// Ignore cross-seed filtering.
    pub ignore_cross_seeds: bool,
    /// Override exclude_recent_search.
    pub ignore_exclude_recent_search: bool,
    /// Override exclude_older.
    pub ignore_exclude_older: bool,
    /// Disable blocklist.
    pub ignore_block_list: bool,
    /// Include single episodes.
    pub include_single_episodes: bool,
    /// Include non-video searchees.
    pub include_non_videos: bool,
}

/// Validated webhook path metadata used to detect replacement before work runs.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WebhookPathSnapshot {
    original: String,
    canonical: String,
    kind: WebhookPathKind,
    len: u64,
    modified_millis: Option<u64>,
}

impl WebhookPathSnapshot {
    pub(crate) fn capture(path: &str) -> crate::Result<Self> {
        let path_ref = Path::new(path);
        let metadata = fs::metadata(path_ref).map_err(|_error| api_error("path does not exist"))?;
        let canonical =
            fs::canonicalize(path_ref).map_err(|_error| api_error("path does not exist"))?;
        Ok(Self {
            original: path.to_owned(),
            canonical: path_to_string(canonical),
            kind: WebhookPathKind::from_metadata(&metadata),
            len: metadata.len(),
            modified_millis: modified_millis(&metadata),
        })
    }

    pub(crate) fn canonical(&self) -> &str {
        &self.canonical
    }

    pub(crate) fn revalidate(&self) -> crate::Result<()> {
        let path_ref = Path::new(&self.original);
        let metadata = fs::metadata(path_ref)
            .map_err(|_error| api_error("webhook path no longer exists before search"))?;
        let canonical = fs::canonicalize(path_ref)
            .map_err(|_error| api_error("webhook path no longer exists before search"))?;
        let current = Self {
            original: self.original.clone(),
            canonical: path_to_string(canonical),
            kind: WebhookPathKind::from_metadata(&metadata),
            len: metadata.len(),
            modified_millis: modified_millis(&metadata),
        };
        if &current == self {
            Ok(())
        } else {
            Err(api_error("webhook path changed before search"))
        }
    }
}

/// Coarse filesystem object kind for webhook path replacement checks.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum WebhookPathKind {
    File,
    Directory,
    Other,
}

impl WebhookPathKind {
    fn from_metadata(metadata: &fs::Metadata) -> Self {
        if metadata.is_file() {
            Self::File
        } else if metadata.is_dir() {
            Self::Directory
        } else {
            Self::Other
        }
    }
}

impl WebhookRequest {
    pub(crate) fn revalidate_path(&self) -> crate::Result<()> {
        if let Some(snapshot) = &self.path_snapshot {
            snapshot.revalidate()?;
        }
        Ok(())
    }
}

/// Validated `/api/job` request body.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct JobRequest {
    /// Job name.
    pub name: String,
    /// Override exclude_recent_search.
    pub ignore_exclude_recent_search: bool,
    /// Override exclude_older.
    pub ignore_exclude_older: bool,
}

/// Job endpoint result from the scheduler layer.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum JobResponse {
    /// Job was queued.
    Accepted(String),
    /// Job disabled by configuration.
    Disabled(String),
    /// Job already active.
    AlreadyRunning(String),
    /// Job is not eligible to run early.
    NotEligible(String),
}

/// Handler callbacks supplied by the daemon runtime.
#[async_trait]
pub trait ApiHandlers {
    /// Return authenticated service diagnostics.
    async fn status(&mut self) -> crate::Result<ApiResponse> {
        Ok(ApiResponse::new(200, "OK"))
    }
    /// Reverse-match an announce candidate.
    async fn announce(&mut self, request: AnnounceRequest) -> crate::Result<Option<ApiOutcome>>;
    /// Start webhook work after the immediate 204 response is selected.
    async fn webhook(&mut self, request: WebhookRequest) -> crate::Result<()>;
    /// Run a scheduled job ahead of schedule.
    async fn job(&mut self, request: JobRequest) -> crate::Result<JobResponse>;
}

/// API-compatible action outcome for announce response mapping.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct ApiOutcome {
    /// Candidate decision.
    pub decision: Decision,
    /// Optional action result.
    pub action_result: Option<ActionResult>,
}

/// Route one API request.
pub async fn handle_api_request<H: ApiHandlers + Send>(
    request: ApiRequest,
    api_key: &str,
    handlers: &mut H,
) -> crate::Result<ApiResponse> {
    if request.path == "/api/ping" {
        if let Some(response) = method_guard(request.method, ApiMethod::Get) {
            return Ok(response);
        }
        return Ok(ApiResponse::new(200, "OK"));
    }
    if !authorized(&request, api_key) {
        if let Some(client_addr) = logged_client_addr(&request) {
            tracing::warn!("unauthorized API request from {client_addr}");
        }
        return Ok(ApiResponse::new(401, AUTH_MESSAGE));
    }
    match request.path.as_str() {
        "/api/status" => {
            if let Some(response) = method_guard(request.method, ApiMethod::Get) {
                return Ok(response);
            }
            handlers.status().await
        }
        "/api/announce" => {
            if let Some(response) = method_guard(request.method, ApiMethod::Post) {
                return Ok(response);
            }
            let body = match parse_body_or_400(&request.body) {
                Ok(body) => body,
                Err(response) => return Ok(response),
            };
            let announce = match parse_announce_or_400(&body) {
                Ok(announce) => announce,
                Err(response) => return Ok(response),
            };
            let outcome = handlers.announce(announce).await?;
            Ok(announce_response(outcome))
        }
        "/api/webhook" => {
            if let Some(response) = method_guard(request.method, ApiMethod::Post) {
                return Ok(response);
            }
            let body = match parse_body_or_400(&request.body) {
                Ok(body) => body,
                Err(response) => return Ok(response),
            };
            let webhook = match parse_webhook_or_400(&body) {
                Ok(webhook) => webhook,
                Err(response) => return Ok(response),
            };
            handlers.webhook(webhook).await?;
            Ok(ApiResponse::new(204, ""))
        }
        "/api/job" => {
            if let Some(response) = method_guard(request.method, ApiMethod::Post) {
                return Ok(response);
            }
            let body = match parse_body_or_400(&request.body) {
                Ok(body) => body,
                Err(response) => return Ok(response),
            };
            let job = match parse_job_or_400(&body) {
                Ok(job) => job,
                Err(response) => return Ok(response),
            };
            Ok(job_response(handlers.job(job).await?))
        }
        _ => Ok(ApiResponse::new(404, "Not Found")),
    }
}

fn method_guard(actual: ApiMethod, expected: ApiMethod) -> Option<ApiResponse> {
    if actual == expected {
        None
    } else {
        Some(ApiResponse::new(405, "Method Not Allowed"))
    }
}

fn authorized(request: &ApiRequest, api_key: &str) -> bool {
    request
        .headers
        .get("x-api-key")
        .or_else(|| request.query.get("apikey"))
        .is_some_and(|value| value == api_key)
}

fn logged_client_addr(request: &ApiRequest) -> Option<Cow<'_, str>> {
    request
        .headers
        .get("x-forwarded-for")
        .and_then(|value| forwarded_client_addr(value))
        .map(Cow::Owned)
        .or_else(|| request.remote_addr.as_deref().map(Cow::Borrowed))
}

fn forwarded_client_addr(value: &str) -> Option<String> {
    value
        .split(',')
        .map(str::trim)
        .find(|addr| addr.parse::<IpAddr>().is_ok())
        .map(ToOwned::to_owned)
}

fn parse_body(body: &str) -> crate::Result<BTreeMap<String, serde_json::Value>> {
    if body.trim().is_empty() {
        return Ok(BTreeMap::new());
    }
    let trimmed = body.trim();
    if trimmed.starts_with('{') || trimmed.starts_with('[') {
        let value = serde_json::from_str::<serde_json::Value>(body)
            .map_err(|error| api_error(format!("malformed request body: {error}")))?;
        let Some(object) = value.as_object() else {
            return Err(api_error("request body must be an object"));
        };
        return Ok(object
            .iter()
            .map(|(key, value)| (key.clone(), normalize_body_value(key, value.clone())))
            .collect());
    }
    let mut output = BTreeMap::new();
    for (key, value) in form_urlencoded::parse(body.as_bytes()) {
        let key = key.into_owned();
        output.insert(
            key.clone(),
            normalize_body_value(&key, serde_json::Value::String(value.into_owned())),
        );
    }
    Ok(output)
}

fn parse_body_or_400(body: &str) -> Result<BTreeMap<String, serde_json::Value>, ApiResponse> {
    parse_body(body).map_err(|error| ApiResponse::new(400, error.to_string()))
}

fn parse_announce_or_400(
    body: &BTreeMap<String, serde_json::Value>,
) -> Result<AnnounceRequest, ApiResponse> {
    parse_announce(body).map_err(|error| ApiResponse::new(400, error.to_string()))
}

fn parse_webhook_or_400(
    body: &BTreeMap<String, serde_json::Value>,
) -> Result<WebhookRequest, ApiResponse> {
    parse_webhook(body).map_err(|error| ApiResponse::new(400, error.to_string()))
}

fn parse_job_or_400(body: &BTreeMap<String, serde_json::Value>) -> Result<JobRequest, ApiResponse> {
    parse_job(body).map_err(|error| ApiResponse::new(400, error.to_string()))
}

fn normalize_body_value(key: &str, value: serde_json::Value) -> serde_json::Value {
    match (key, value) {
        ("infoHash", serde_json::Value::String(value)) => {
            serde_json::Value::String(value.to_ascii_lowercase())
        }
        ("size", serde_json::Value::String(value)) => value
            .parse::<u64>()
            .map(serde_json::Value::from)
            .unwrap_or(serde_json::Value::String(value)),
        (_key, value) => value,
    }
}

fn path_to_string(path: PathBuf) -> String {
    path.to_string_lossy().into_owned()
}

fn modified_millis(metadata: &fs::Metadata) -> Option<u64> {
    metadata
        .modified()
        .ok()
        .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
}

fn parse_announce(body: &BTreeMap<String, serde_json::Value>) -> crate::Result<AnnounceRequest> {
    reject_unknown_fields(body, &["name", "guid", "link", "tracker", "cookie"])?;
    let name = required_string(body, "name")?;
    let guid = required_url(body, "guid")?;
    let link = required_url(body, "link")?;
    if guid != link {
        return Err(api_error("announce guid must equal link"));
    }
    let tracker = required_string(body, "tracker")?;
    let cookie = optional_string(body, "cookie");
    Ok(AnnounceRequest {
        name,
        guid,
        link,
        tracker,
        cookie,
    })
}

fn parse_webhook(body: &BTreeMap<String, serde_json::Value>) -> crate::Result<WebhookRequest> {
    reject_unknown_fields(
        body,
        &[
            "infoHash",
            "path",
            "ignoreCrossSeeds",
            "ignoreExcludeRecentSearch",
            "ignoreExcludeOlder",
            "ignoreBlockList",
            "includeSingleEpisodes",
            "includeNonVideos",
        ],
    )?;
    let info_hash = optional_string(body, "infoHash");
    let path = optional_string(body, "path");
    if info_hash.is_some() == path.is_some() {
        return Err(api_error("exactly one of infoHash or path is required"));
    }
    if info_hash
        .as_ref()
        .is_some_and(|info_hash| !is_hex_hash(info_hash))
    {
        return Err(api_error("infoHash must be 40 hex characters"));
    }
    let path_snapshot = path
        .as_deref()
        .map(WebhookPathSnapshot::capture)
        .transpose()?;
    let path = path_snapshot
        .as_ref()
        .map(|snapshot| snapshot.canonical().to_owned());
    Ok(WebhookRequest {
        info_hash,
        path,
        path_snapshot,
        ignore_cross_seeds: bool_field(body, "ignoreCrossSeeds"),
        ignore_exclude_recent_search: bool_field(body, "ignoreExcludeRecentSearch"),
        ignore_exclude_older: bool_field(body, "ignoreExcludeOlder"),
        ignore_block_list: bool_field(body, "ignoreBlockList"),
        include_single_episodes: bool_field(body, "includeSingleEpisodes"),
        include_non_videos: bool_field(body, "includeNonVideos"),
    })
}

fn parse_job(body: &BTreeMap<String, serde_json::Value>) -> crate::Result<JobRequest> {
    reject_unknown_fields(
        body,
        &["name", "ignoreExcludeRecentSearch", "ignoreExcludeOlder"],
    )?;
    let name = optional_string(body, "name").unwrap_or_else(|| "search".to_owned());
    if !matches!(
        name.as_str(),
        "rss" | "search" | "updateIndexerCaps" | "inject" | "cleanup"
    ) {
        return Err(api_error("invalid job name"));
    }
    Ok(JobRequest {
        name,
        ignore_exclude_recent_search: bool_field(body, "ignoreExcludeRecentSearch"),
        ignore_exclude_older: bool_field(body, "ignoreExcludeOlder"),
    })
}

fn reject_unknown_fields(
    body: &BTreeMap<String, serde_json::Value>,
    allowed: &[&str],
) -> crate::Result<()> {
    for key in body.keys() {
        if !allowed.contains(&key.as_str()) {
            return Err(api_error(format!("unknown request field: {key}")));
        }
    }
    Ok(())
}

fn announce_response(outcome: Option<ApiOutcome>) -> ApiResponse {
    let Some(outcome) = outcome else {
        return ApiResponse::new(204, "");
    };
    match outcome.action_result {
        Some(ActionResult::Save(SaveResult::Saved))
        | Some(ActionResult::Injection(InjectionResult::Injected))
        | Some(ActionResult::Injection(InjectionResult::Failure))
        | Some(ActionResult::Injection(InjectionResult::AlreadyExists)) => {
            ApiResponse::new(200, "")
        }
        Some(ActionResult::Injection(InjectionResult::TorrentNotComplete)) => {
            ApiResponse::new(202, "")
        }
        None if matches!(
            outcome.decision,
            Decision::InfoHashAlreadyExists | Decision::SameInfoHash
        ) =>
        {
            ApiResponse::new(200, "")
        }
        _ => ApiResponse::new(500, "Unexpected announce result"),
    }
}

fn job_response(response: JobResponse) -> ApiResponse {
    match response {
        JobResponse::Accepted(message) => ApiResponse::new(200, message),
        JobResponse::Disabled(message) => ApiResponse::new(404, message),
        JobResponse::AlreadyRunning(message) | JobResponse::NotEligible(message) => {
            ApiResponse::new(409, message)
        }
    }
}

fn required_string(body: &BTreeMap<String, serde_json::Value>, key: &str) -> crate::Result<String> {
    optional_string(body, key).ok_or_else(|| api_error(format!("{key} is required")))
}

fn required_url(body: &BTreeMap<String, serde_json::Value>, key: &str) -> crate::Result<String> {
    let value = required_string(body, key)?;
    Url::parse(&value).map_err(|error| api_error(format!("{key} must be a URL: {error}")))?;
    Ok(value)
}

fn optional_string(body: &BTreeMap<String, serde_json::Value>, key: &str) -> Option<String> {
    body.get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn bool_field(body: &BTreeMap<String, serde_json::Value>, key: &str) -> bool {
    body.get(key).is_some_and(|value| match value {
        serde_json::Value::Bool(value) => *value,
        serde_json::Value::String(value) => value == "true",
        _ => false,
    })
}

fn split_target(target: &str) -> (String, BTreeMap<String, String>) {
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let query = form_urlencoded::parse(query.as_bytes())
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect();
    (path.to_owned(), query)
}

fn is_hex_hash(value: &str) -> bool {
    value.len() == 40 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn api_error(message: impl Into<Cow<'static, str>>) -> SporosError {
    SporosError::Api {
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AnnounceRequest, ApiHandlers, ApiMethod, ApiOutcome, ApiRequest, JobRequest, JobResponse,
        WebhookRequest, handle_api_request,
    };
    use crate::domain::{ActionResult, Decision, InjectionResult};
    use std::{collections::BTreeMap, fs};

    #[tokio::test]
    async fn ping_skips_auth_and_status_requires_auth() {
        let mut handlers = TestHandlers::default();
        let ping = handle_api_request(
            ApiRequest::new(ApiMethod::Get, "/api/ping", BTreeMap::new(), ""),
            "secret",
            &mut handlers,
        )
        .await
        .expect("ping");
        assert_eq!(ping.status, 200);
        assert_eq!(ping.body, "OK");

        let unauthorized = handle_api_request(
            ApiRequest::new(ApiMethod::Get, "/api/status", BTreeMap::new(), ""),
            "secret",
            &mut handlers,
        )
        .await
        .expect("status");
        assert_eq!(unauthorized.status, 401);

        let status = handle_api_request(
            ApiRequest::new(
                ApiMethod::Get,
                "/api/status?apikey=secret",
                BTreeMap::new(),
                "",
            ),
            "secret",
            &mut handlers,
        )
        .await
        .expect("status");
        assert_eq!(status.status, 200);
        assert_eq!(status.body, "OK");
    }

    #[test]
    fn unauthorized_logging_prefers_valid_forwarded_client() {
        let mut request = ApiRequest::new(ApiMethod::Get, "/api/status", BTreeMap::new(), "");
        request.remote_addr = Some("10.0.0.4:50000".to_owned());
        request.headers.insert(
            "x-forwarded-for".to_owned(),
            "203.0.113.9, 10.0.0.4".to_owned(),
        );

        assert_eq!(
            super::logged_client_addr(&request).as_deref(),
            Some("203.0.113.9")
        );
    }

    #[test]
    fn unauthorized_logging_falls_back_to_socket_remote() {
        let mut request = ApiRequest::new(ApiMethod::Get, "/api/status", BTreeMap::new(), "");
        request.remote_addr = Some("10.0.0.4:50000".to_owned());
        request
            .headers
            .insert("x-forwarded-for".to_owned(), "unknown".to_owned());

        assert_eq!(
            super::logged_client_addr(&request).as_deref(),
            Some("10.0.0.4:50000")
        );
    }

    #[tokio::test]
    async fn announce_validates_body_and_maps_result() {
        let mut headers = BTreeMap::new();
        headers.insert("X-Api-Key".to_owned(), "secret".to_owned());
        let mut handlers = TestHandlers {
            announce_result: Some(ApiOutcome {
                decision: Decision::Match,
                action_result: Some(ActionResult::Injection(InjectionResult::TorrentNotComplete)),
            }),
            ..TestHandlers::default()
        };

        let response = handle_api_request(
            ApiRequest::new(
                ApiMethod::Post,
                "/api/announce",
                headers,
                r#"{"name":" Release ","guid":"https://idx/t","link":"https://idx/t","tracker":"Tracker"}"#,
            ),
            "secret",
            &mut handlers,
        )
        .await
        .expect("announce");

        assert_eq!(response.status, 202);
        assert_eq!(handlers.announces.len(), 1);
        assert_eq!(handlers.announces[0].name, "Release");
    }

    #[tokio::test]
    async fn webhook_accepts_form_body_and_returns_immediately() {
        let path = std::env::temp_dir().join("sporos-api-webhook-path");
        fs::write(&path, b"data").expect("path");
        let mut handlers = TestHandlers::default();

        let response = handle_api_request(
            ApiRequest::new(
                ApiMethod::Post,
                "/api/webhook?apikey=secret",
                BTreeMap::new(),
                format!(
                    "path={}&ignoreCrossSeeds=true&includeNonVideos=true",
                    path.display()
                ),
            ),
            "secret",
            &mut handlers,
        )
        .await
        .expect("webhook");

        assert_eq!(response.status, 204);
        assert_eq!(handlers.webhooks.len(), 1);
        assert!(handlers.webhooks[0].ignore_cross_seeds);
        assert!(handlers.webhooks[0].include_non_videos);
        let canonical = fs::canonicalize(&path)
            .expect("canonical")
            .to_string_lossy()
            .into_owned();
        assert_eq!(
            handlers.webhooks[0].path.as_deref(),
            Some(canonical.as_str())
        );
        assert!(handlers.webhooks[0].path_snapshot.is_some());
        let _cleanup = fs::remove_file(path);
    }

    #[tokio::test]
    async fn job_endpoint_maps_scheduler_responses() {
        let mut handlers = TestHandlers {
            job_response: JobResponse::AlreadyRunning("rss: already running".to_owned()),
            ..TestHandlers::default()
        };
        let response = handle_api_request(
            ApiRequest::new(
                ApiMethod::Post,
                "/api/job?apikey=secret",
                BTreeMap::new(),
                r#"{"name":"rss"}"#,
            ),
            "secret",
            &mut handlers,
        )
        .await
        .expect("job");

        assert_eq!(response.status, 409);
        assert_eq!(response.body, "rss: already running");
        assert_eq!(handlers.jobs[0].name, "rss");
    }

    #[tokio::test]
    async fn webhook_rejects_non_hex_info_hash() {
        let mut handlers = TestHandlers::default();

        let response = handle_api_request(
            ApiRequest::new(
                ApiMethod::Post,
                "/api/webhook?apikey=secret",
                BTreeMap::new(),
                r#"{"infoHash":"gggggggggggggggggggggggggggggggggggggggg"}"#,
            ),
            "secret",
            &mut handlers,
        )
        .await
        .expect("webhook");

        assert_eq!(response.status, 400);
        assert_eq!(
            response.body,
            "api error: infoHash must be 40 hex characters"
        );
        assert!(handlers.webhooks.is_empty());
    }

    #[tokio::test]
    async fn webhook_accepts_uppercase_hex_info_hash() {
        let mut handlers = TestHandlers::default();

        let response = handle_api_request(
            ApiRequest::new(
                ApiMethod::Post,
                "/api/webhook?apikey=secret",
                BTreeMap::new(),
                r#"{"infoHash":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}"#,
            ),
            "secret",
            &mut handlers,
        )
        .await
        .expect("webhook");

        assert_eq!(response.status, 204);
        assert_eq!(
            handlers.webhooks[0].info_hash.as_deref(),
            Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        );
    }

    #[tokio::test]
    async fn api_rejects_unknown_request_fields() {
        let mut headers = BTreeMap::new();
        headers.insert("X-Api-Key".to_owned(), "secret".to_owned());
        let path = std::env::temp_dir().join("sporos-api-strict-path");
        fs::write(&path, b"data").expect("path");
        let mut handlers = TestHandlers::default();

        let announce = handle_api_request(
            ApiRequest::new(
                ApiMethod::Post,
                "/api/announce",
                headers.clone(),
                r#"{"name":"Release","guid":"https://idx/t","link":"https://idx/t","tracker":"Tracker","unexpected":true}"#,
            ),
            "secret",
            &mut handlers,
        )
        .await
        .expect("announce");
        let webhook = handle_api_request(
            ApiRequest::new(
                ApiMethod::Post,
                "/api/webhook",
                headers.clone(),
                format!(r#"{{"path":"{}","unexpected":true}}"#, path.display()),
            ),
            "secret",
            &mut handlers,
        )
        .await
        .expect("webhook");
        let job = handle_api_request(
            ApiRequest::new(
                ApiMethod::Post,
                "/api/job",
                headers,
                r#"{"name":"rss","unexpected":true}"#,
            ),
            "secret",
            &mut handlers,
        )
        .await
        .expect("job");

        assert_eq!(announce.status, 400);
        assert_eq!(webhook.status, 400);
        assert_eq!(job.status, 400);
        assert!(announce.body.contains("unknown request field"));
        assert!(handlers.announces.is_empty());
        assert!(handlers.webhooks.is_empty());
        assert!(handlers.jobs.is_empty());
        let _cleanup = fs::remove_file(path);
    }

    #[derive(Debug)]
    struct TestHandlers {
        announces: Vec<AnnounceRequest>,
        webhooks: Vec<WebhookRequest>,
        jobs: Vec<JobRequest>,
        announce_result: Option<ApiOutcome>,
        job_response: JobResponse,
    }

    impl Default for TestHandlers {
        fn default() -> Self {
            Self {
                announces: Vec::new(),
                webhooks: Vec::new(),
                jobs: Vec::new(),
                announce_result: Some(ApiOutcome {
                    decision: Decision::Match,
                    action_result: Some(ActionResult::Injection(InjectionResult::Injected)),
                }),
                job_response: JobResponse::Accepted("search: running ahead of schedule".to_owned()),
            }
        }
    }

    #[async_trait::async_trait]
    impl ApiHandlers for TestHandlers {
        async fn announce(
            &mut self,
            request: AnnounceRequest,
        ) -> crate::Result<Option<ApiOutcome>> {
            self.announces.push(request);
            Ok(self.announce_result)
        }

        async fn webhook(&mut self, request: WebhookRequest) -> crate::Result<()> {
            self.webhooks.push(request);
            Ok(())
        }

        async fn job(&mut self, request: JobRequest) -> crate::Result<JobResponse> {
            self.jobs.push(request);
            Ok(self.job_response.clone())
        }
    }
}
