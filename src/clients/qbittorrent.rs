#![cfg_attr(
    test,
    expect(
        clippy::let_underscore_must_use,
        reason = "test HTTP fixture only drains enough bytes to observe the request"
    )
)]

use std::fmt;
use std::future::Future;
use std::path::PathBuf;
use std::time::Duration;

use reqwest::StatusCode;
use reqwest::header::{COOKIE, SET_COOKIE};
use reqwest::multipart::{Form, Part};
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::domain::{ByteSize, DisplayName, FileIndex, InfoHash, TorrentFile};
use crate::errors::TorrentClientError;
use crate::secrets::sanitize_url_for_logging;

const SPOROS_TAG: &str = "sporos";
const MIN_QBIT_VERSION: QbitVersion = QbitVersion {
    major: 4,
    minor: 3,
    patch: 1,
};
const QBIT_INVENTORY_PAGE_SIZE: usize = 500;
const QBIT_RESPONSE_MAX_BYTES: u64 = 64 * 1024 * 1024;

pub struct QbittorrentClient {
    client_name: String,
    base_url: String,
    username: Option<String>,
    password: Option<String>,
    timeout: Duration,
    client: reqwest::Client,
    cookie: Mutex<Option<String>>,
    version: Mutex<Option<QbitVersion>>,
}

impl fmt::Debug for QbittorrentClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QbittorrentClient")
            .field("client_name", &self.client_name)
            .field("base_url", &sanitize_url_for_logging(&self.base_url))
            .field("username", &redacted_option(self.username.as_ref()))
            .field("password", &redacted_option(self.password.as_ref()))
            .field("timeout", &self.timeout)
            .field("client", &"[REDACTED]")
            .field("cookie", &"[REDACTED]")
            .field("version", &"[cached]")
            .finish()
    }
}

impl QbittorrentClient {
    pub fn new(
        client_name: impl Into<String>,
        base_url: impl Into<String>,
        username: Option<String>,
        password: Option<String>,
        timeout: Duration,
    ) -> Self {
        Self {
            client_name: client_name.into(),
            base_url: base_url.into().trim_end_matches('/').to_owned(),
            username,
            password,
            timeout,
            client: reqwest::Client::new(),
            cookie: Mutex::new(None),
            version: Mutex::new(None),
        }
    }

    pub async fn validate(&self) -> Result<QbitVersion, TorrentClientError> {
        self.ensure_session().await?;
        let version = self.version().await?;
        if version < MIN_QBIT_VERSION {
            return Err(TorrentClientError::UnsupportedCapability {
                client: self.client_name.clone(),
                capability: format!("qBittorrent >= {MIN_QBIT_VERSION}"),
            });
        }
        self.create_tag(SPOROS_TAG).await?;
        Ok(version)
    }

    pub async fn version(&self) -> Result<QbitVersion, TorrentClientError> {
        if let Some(version) = *self.version.lock().await {
            return Ok(version);
        }

        let response = self.get_text("/api/v2/app/version").await?;
        let version = QbitVersion::parse(response.trim()).map_err(|message| {
            TorrentClientError::BadResponse {
                client: self.client_name.clone(),
                message,
            }
        })?;
        *self.version.lock().await = Some(version);
        Ok(version)
    }

    pub async fn list_inventory(&self) -> Result<Vec<QbitTorrent>, TorrentClientError> {
        let mut torrents = Vec::new();
        let mut offset = 0usize;
        loop {
            let page = self.inventory_page(offset).await?;
            let page_len = page.len();
            torrents.extend(page);
            if page_len < QBIT_INVENTORY_PAGE_SIZE {
                break;
            }
            offset = offset.saturating_add(QBIT_INVENTORY_PAGE_SIZE);
        }
        Ok(torrents)
    }

    pub async fn list_inventory_pages<F, Fut>(
        &self,
        mut on_page: F,
    ) -> Result<usize, TorrentClientError>
    where
        F: FnMut(Vec<QbitTorrent>) -> Fut,
        Fut: Future<Output = Result<(), TorrentClientError>>,
    {
        let mut total = 0usize;
        let mut offset = 0usize;
        loop {
            let page = self.inventory_page(offset).await?;
            let page_len = page.len();
            total = total.saturating_add(page_len);
            if page_len > 0 {
                on_page(page).await?;
            }
            if page_len < QBIT_INVENTORY_PAGE_SIZE {
                break;
            }
            offset = offset.saturating_add(QBIT_INVENTORY_PAGE_SIZE);
        }
        Ok(total)
    }

    pub async fn list_inventory_pages_until_shutdown<F, Fut, C, CFut>(
        &self,
        mut cancelled: C,
        mut on_page: F,
    ) -> Result<usize, TorrentClientError>
    where
        F: FnMut(Vec<QbitTorrent>) -> Fut,
        Fut: Future<Output = Result<(), TorrentClientError>>,
        C: FnMut() -> CFut,
        CFut: Future<Output = ()>,
    {
        let mut total = 0usize;
        let mut offset = 0usize;
        loop {
            let page = tokio::select! {
                biased;
                () = cancelled() => return Err(cancelled_error(&self.client_name)),
                page = self.inventory_page(offset) => page?,
            };
            let page_len = page.len();
            total = total.saturating_add(page_len);
            if page_len > 0 {
                tokio::select! {
                    biased;
                    () = cancelled() => return Err(cancelled_error(&self.client_name)),
                    result = on_page(page) => result?,
                }
            }
            if page_len < QBIT_INVENTORY_PAGE_SIZE {
                break;
            }
            offset = offset.saturating_add(QBIT_INVENTORY_PAGE_SIZE);
        }
        Ok(total)
    }

    pub async fn torrent_info(
        &self,
        info_hash: &InfoHash,
    ) -> Result<Option<QbitTorrent>, TorrentClientError> {
        let text = self
            .get_text(&format!(
                "/api/v2/torrents/info?hashes={}",
                info_hash.as_str()
            ))
            .await?;
        let mut torrents: Vec<QbitTorrent> =
            serde_json::from_str(&text).map_err(|error| TorrentClientError::BadResponse {
                client: self.client_name.clone(),
                message: error.to_string(),
            })?;
        Ok(torrents.pop())
    }

    async fn inventory_page(&self, offset: usize) -> Result<Vec<QbitTorrent>, TorrentClientError> {
        let text = self
            .get_text(&format!(
                "/api/v2/torrents/info?sort=hash&limit={QBIT_INVENTORY_PAGE_SIZE}&offset={offset}"
            ))
            .await?;
        serde_json::from_str(&text).map_err(|error| TorrentClientError::BadResponse {
            client: self.client_name.clone(),
            message: error.to_string(),
        })
    }

    pub async fn fetch_files(
        &self,
        info_hash: &InfoHash,
    ) -> Result<Vec<TorrentFile>, TorrentClientError> {
        let text = self
            .get_text(&format!(
                "/api/v2/torrents/files?hash={}",
                info_hash.as_str()
            ))
            .await?;
        let files: Vec<QbitFile> =
            serde_json::from_str(&text).map_err(|error| TorrentClientError::BadResponse {
                client: self.client_name.clone(),
                message: error.to_string(),
            })?;
        files
            .into_iter()
            .enumerate()
            .map(|(index, file)| {
                TorrentFile::new(
                    PathBuf::from(file.name),
                    ByteSize::new(file.size),
                    FileIndex::new(u32::try_from(index).map_err(|error| {
                        TorrentClientError::BadResponse {
                            client: self.client_name.clone(),
                            message: error.to_string(),
                        }
                    })?),
                )
                .map_err(|error| TorrentClientError::BadResponse {
                    client: self.client_name.clone(),
                    message: error.to_string(),
                })
            })
            .collect()
    }

    pub async fn fetch_files_until_shutdown<C, CFut>(
        &self,
        info_hash: &InfoHash,
        mut cancelled: C,
    ) -> Result<Vec<TorrentFile>, TorrentClientError>
    where
        C: FnMut() -> CFut,
        CFut: Future<Output = ()>,
    {
        tokio::select! {
            biased;
            () = cancelled() => Err(cancelled_error(&self.client_name)),
            files = self.fetch_files(info_hash) => files,
        }
    }

    pub async fn fetch_trackers(
        &self,
        info_hash: &InfoHash,
    ) -> Result<Vec<QbitTracker>, TorrentClientError> {
        let text = self
            .get_text(&format!(
                "/api/v2/torrents/trackers?hash={}",
                info_hash.as_str()
            ))
            .await?;
        serde_json::from_str(&text).map_err(|error| TorrentClientError::BadResponse {
            client: self.client_name.clone(),
            message: error.to_string(),
        })
    }

    pub async fn inject(&self, request: QbitAddTorrent<'_>) -> Result<(), TorrentClientError> {
        let version = self.version().await?;
        let paused_field = if version.uses_stop_start() {
            "stopped"
        } else {
            "paused"
        };
        let torrent_bytes = request.torrent_bytes.to_vec();
        let save_path = request.save_path.map(|path| path.display().to_string());
        let category = request.category.map(str::to_owned);
        let pause_for_recheck = request.pause_for_recheck;
        let content_layout = request.content_layout;
        self.post_multipart("/api/v2/torrents/add", || {
            add_torrent_form(
                &torrent_bytes,
                paused_field,
                pause_for_recheck,
                content_layout,
                save_path.as_deref(),
                category.as_deref(),
            )
        })
        .await
    }

    pub async fn create_tag(&self, tag: &str) -> Result<(), TorrentClientError> {
        self.post_form("/api/v2/torrents/createTags", &[("tags", tag)])
            .await
    }

    pub async fn create_category(
        &self,
        category: &str,
        save_path: Option<&PathBuf>,
    ) -> Result<(), TorrentClientError> {
        let save_path = save_path.map(|path| path.display().to_string());
        let mut fields = vec![("category".to_owned(), category.to_owned())];
        if let Some(save_path) = save_path {
            fields.push(("savePath".to_owned(), save_path));
        }
        self.post_owned_form("/api/v2/torrents/createCategory", &fields)
            .await
    }

    pub async fn recheck(&self, info_hash: &InfoHash) -> Result<(), TorrentClientError> {
        self.post_form(
            "/api/v2/torrents/recheck",
            &[("hashes", info_hash.as_str())],
        )
        .await
    }

    pub async fn resume(&self, info_hash: &InfoHash) -> Result<(), TorrentClientError> {
        let version = self.version().await?;
        let endpoint = if version.uses_stop_start() {
            "/api/v2/torrents/start"
        } else {
            "/api/v2/torrents/resume"
        };
        self.post_form(endpoint, &[("hashes", info_hash.as_str())])
            .await
    }

    pub async fn pause(&self, info_hash: &InfoHash) -> Result<(), TorrentClientError> {
        let version = self.version().await?;
        let endpoint = if version.uses_stop_start() {
            "/api/v2/torrents/stop"
        } else {
            "/api/v2/torrents/pause"
        };
        self.post_form(endpoint, &[("hashes", info_hash.as_str())])
            .await
    }

    async fn ensure_session(&self) -> Result<(), TorrentClientError> {
        if self.cookie.lock().await.is_some() {
            return Ok(());
        }
        self.login().await
    }

    async fn login(&self) -> Result<(), TorrentClientError> {
        let username = self.username.as_deref().unwrap_or_default();
        let password = self.password.as_deref().unwrap_or_default();
        let response = self
            .client
            .post(self.url("/api/v2/auth/login"))
            .timeout(self.timeout)
            .form(&[("username", username), ("password", password)])
            .send()
            .await
            .map_err(|error| unavailable(&self.client_name, error.to_string()))?;
        if response.status() == StatusCode::FORBIDDEN
            || response.status() == StatusCode::UNAUTHORIZED
        {
            return Err(TorrentClientError::Unauthorized {
                client: self.client_name.clone(),
            });
        }
        if !response.status().is_success() {
            return Err(unavailable(
                &self.client_name,
                format!("HTTP {}", response.status()),
            ));
        }
        let cookie = response
            .headers()
            .get(SET_COOKIE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| TorrentClientError::BadResponse {
                client: self.client_name.clone(),
                message: "login response did not include a session cookie".to_owned(),
            })?
            .to_owned();
        *self.cookie.lock().await = Some(cookie);
        Ok(())
    }

    async fn get_text(&self, path: &str) -> Result<String, TorrentClientError> {
        let response = self
            .send_with_session(|cookie| {
                let mut request = self.client.get(self.url(path)).timeout(self.timeout);
                if let Some(cookie) = cookie {
                    request = request.header(COOKIE, cookie);
                }
                request
            })
            .await?;
        read_client_text(response, &self.client_name, QBIT_RESPONSE_MAX_BYTES).await
    }

    async fn post_form(
        &self,
        path: &str,
        fields: &[(&str, &str)],
    ) -> Result<(), TorrentClientError> {
        let owned = fields
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect::<Vec<_>>();
        self.post_owned_form(path, &owned).await
    }

    async fn post_owned_form(
        &self,
        path: &str,
        fields: &[(String, String)],
    ) -> Result<(), TorrentClientError> {
        let response = self
            .send_with_session(|cookie| {
                let mut request = self
                    .client
                    .post(self.url(path))
                    .timeout(self.timeout)
                    .form(fields);
                if let Some(cookie) = cookie {
                    request = request.header(COOKIE, cookie);
                }
                request
            })
            .await?;
        drop(response);
        Ok(())
    }

    async fn post_multipart<F>(&self, path: &str, form: F) -> Result<(), TorrentClientError>
    where
        F: Fn() -> Form,
    {
        let response = self
            .send_with_session(|cookie| {
                let mut request = self.client.post(self.url(path)).timeout(self.timeout);
                if let Some(cookie) = cookie {
                    request = request.header(COOKIE, cookie);
                }
                request.multipart(form())
            })
            .await?;
        drop(response);
        Ok(())
    }

    async fn send_with_session<F>(&self, build: F) -> Result<reqwest::Response, TorrentClientError>
    where
        F: Fn(Option<&str>) -> reqwest::RequestBuilder,
    {
        self.ensure_session().await?;
        let cookie = self.cookie.lock().await.clone();
        let response = build(cookie.as_deref())
            .send()
            .await
            .map_err(|error| unavailable(&self.client_name, error.to_string()))?;
        if response.status() != StatusCode::FORBIDDEN
            && response.status() != StatusCode::UNAUTHORIZED
        {
            return success_response(&self.client_name, response);
        }

        *self.cookie.lock().await = None;
        self.login().await?;
        let cookie = self.cookie.lock().await.clone();
        let response = build(cookie.as_deref())
            .send()
            .await
            .map_err(|error| unavailable(&self.client_name, error.to_string()))?;
        success_response(&self.client_name, response)
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }
}

fn redacted_option(value: Option<&String>) -> Option<&'static str> {
    value.map(|_| "[REDACTED]")
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum QbitContentLayout {
    Original,
    Subfolder,
    NoSubfolder,
}

impl QbitContentLayout {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Original => "Original",
            Self::Subfolder => "Subfolder",
            Self::NoSubfolder => "NoSubfolder",
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct QbitAddTorrent<'a> {
    pub torrent_bytes: &'a [u8],
    pub save_path: Option<&'a PathBuf>,
    pub category: Option<&'a str>,
    pub pause_for_recheck: bool,
    pub content_layout: QbitContentLayout,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub struct QbitVersion {
    pub major: u64,
    pub minor: u64,
    pub patch: u64,
}

impl QbitVersion {
    pub fn parse(value: &str) -> Result<Self, String> {
        let version = value.trim().trim_start_matches('v');
        let mut parts = version.split(['.', '-']);
        let major = parse_version_part(value, parts.next(), "major")?;
        let minor = parse_version_part(value, parts.next(), "minor")?;
        let patch = parse_version_part(value, parts.next(), "patch")?;
        Ok(Self {
            major,
            minor,
            patch,
        })
    }

    pub const fn uses_stop_start(self) -> bool {
        self.major >= 5
    }
}

impl fmt::Display for QbitVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq)]
pub struct QbitTorrent {
    pub hash: String,
    pub name: String,
    pub save_path: Option<PathBuf>,
    pub content_path: Option<PathBuf>,
    pub category: Option<String>,
    pub tags: Option<String>,
    pub state: Option<String>,
    pub amount_left: Option<u64>,
    pub progress: Option<Progress>,
}

impl QbitTorrent {
    pub fn info_hash(&self, client: &str) -> Result<InfoHash, TorrentClientError> {
        InfoHash::new(&self.hash).map_err(|error| TorrentClientError::BadResponse {
            client: client.to_owned(),
            message: error.to_string(),
        })
    }

    pub fn display_name(&self, client: &str) -> Result<DisplayName, TorrentClientError> {
        DisplayName::new(&self.name).map_err(|error| TorrentClientError::BadResponse {
            client: client.to_owned(),
            message: error.to_string(),
        })
    }

    pub fn is_complete(&self) -> bool {
        self.amount_left == Some(0) || self.progress == Some(Progress::Complete)
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Progress {
    Complete,
    Incomplete,
}

impl<'de> Deserialize<'de> for Progress {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = f64::deserialize(deserializer)?;
        Ok(if (value - 1.0).abs() < f64::EPSILON {
            Self::Complete
        } else {
            Self::Incomplete
        })
    }
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq)]
pub struct QbitFile {
    pub name: String,
    pub size: u64,
    pub progress: Option<Progress>,
    pub priority: Option<i64>,
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq)]
pub struct QbitTracker {
    pub url: String,
    pub status: Option<i64>,
    pub msg: Option<String>,
}

fn parse_version_part(
    original: &str,
    part: Option<&str>,
    name: &'static str,
) -> Result<u64, String> {
    part.and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| format!("invalid qBittorrent version `{original}`: missing {name}"))
}

fn add_torrent_form(
    torrent_bytes: &[u8],
    paused_field: &str,
    pause_for_recheck: bool,
    content_layout: QbitContentLayout,
    save_path: Option<&str>,
    category: Option<&str>,
) -> Form {
    let mut form = Form::new()
        .part(
            "torrents",
            Part::bytes(torrent_bytes.to_vec()).file_name("candidate.torrent"),
        )
        .text(paused_field.to_owned(), pause_for_recheck.to_string())
        .text("skip_checking", (!pause_for_recheck).to_string())
        .text("tags", SPOROS_TAG.to_owned())
        .text("contentLayout", content_layout.as_str().to_owned());
    if let Some(save_path) = save_path {
        form = form.text("savepath", save_path.to_owned());
    }
    if let Some(category) = category {
        form = form.text("category", category.to_owned());
    }
    form
}

fn success_response(
    client: &str,
    response: reqwest::Response,
) -> Result<reqwest::Response, TorrentClientError> {
    if response.status().is_success() {
        Ok(response)
    } else {
        Err(unavailable(client, format!("HTTP {}", response.status())))
    }
}

async fn read_client_text(
    mut response: reqwest::Response,
    client: &str,
    limit: u64,
) -> Result<String, TorrentClientError> {
    if response
        .content_length()
        .is_some_and(|length| length > limit)
    {
        return Err(TorrentClientError::BadResponse {
            client: client.to_owned(),
            message: format!("response exceeded {limit} bytes"),
        });
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
        .map_err(|error| unavailable(client, error.to_string()))?
    {
        if !append_limited_body_chunk(&mut body, &chunk, limit) {
            return Err(TorrentClientError::BadResponse {
                client: client.to_owned(),
                message: format!("response exceeded {limit} bytes"),
            });
        }
    }

    String::from_utf8(body).map_err(|error| TorrentClientError::BadResponse {
        client: client.to_owned(),
        message: error.to_string(),
    })
}

fn append_limited_body_chunk(body: &mut Vec<u8>, chunk: &[u8], limit: u64) -> bool {
    let next_len = body.len().saturating_add(chunk.len());
    if u64::try_from(next_len).unwrap_or(u64::MAX) > limit {
        return false;
    }
    body.extend_from_slice(chunk);
    true
}

fn unavailable(client: &str, message: String) -> TorrentClientError {
    TorrentClientError::Unavailable {
        client: client.to_owned(),
        retry_after_ms: None,
        message,
    }
}

fn cancelled_error(client: &str) -> TorrentClientError {
    TorrentClientError::Cancelled {
        client: client.to_owned(),
        message: "shutdown requested".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::io::{Read, Write};
    use std::net::TcpListener as StdTcpListener;
    use std::sync::{Arc, Mutex as StdMutex};

    use axum::Router;
    use axum::body::{Body, to_bytes};
    use axum::http::{HeaderValue, Request, StatusCode as AxumStatusCode, header::CONTENT_LENGTH};
    use axum::response::{IntoResponse, Response};
    use axum::routing::{get, post};
    use tokio::net::TcpListener;

    use super::*;

    const SHA1: &str = "0123456789abcdef0123456789abcdef01234567";

    #[test]
    fn client_debug_redacts_credentials_urls_and_session_state() {
        let client = QbittorrentClient::new(
            "qbit",
            "https://url-user:url-pass@example.invalid/rpc?apikey=url-secret&ok=1#fragment",
            Some("login-user".to_owned()),
            Some("login-pass".to_owned()),
            Duration::from_secs(1),
        );

        let debug = format!("{client:?}");

        assert!(debug.contains("QbittorrentClient"));
        assert!(debug.contains("ok=1"));
        assert!(!debug.contains("url-user"));
        assert!(!debug.contains("url-pass"));
        assert!(!debug.contains("url-secret"));
        assert!(!debug.contains("fragment"));
        assert!(!debug.contains("login-user"));
        assert!(!debug.contains("login-pass"));
    }

    #[test]
    fn version_parsing_selects_v4_and_v5_control_endpoints() {
        let v4 = QbitVersion::parse("v4.6.7").unwrap();
        let v5 = QbitVersion::parse("5.0.0").unwrap();

        assert!(!v4.uses_stop_start());
        assert!(v5.uses_stop_start());
        assert!(v4 >= MIN_QBIT_VERSION);
    }

    #[test]
    fn torrent_info_maps_hash_name_and_completion() {
        let torrents: Vec<QbitTorrent> = serde_json::from_str(
            r#"[{
              "hash":"0123456789abcdef0123456789abcdef01234567",
              "name":"Example",
              "save_path":"/downloads",
              "content_path":"/downloads/Example",
              "category":"movies",
              "tags":"sporos",
              "state":"uploading",
              "amount_left":0,
              "progress":1.0
            }]"#,
        )
        .unwrap();

        assert_eq!(
            InfoHash::new(SHA1).unwrap(),
            torrents[0].info_hash("qbit").unwrap()
        );
        assert_eq!(
            "Example",
            torrents[0].display_name("qbit").unwrap().as_str()
        );
        assert_eq!(Some(PathBuf::from("/downloads")), torrents[0].save_path);
        assert!(
            torrents
                .iter()
                .any(|torrent| torrent.info_hash("qbit").unwrap().as_str() == SHA1)
        );
        assert!(torrents[0].is_complete());
    }

    #[test]
    fn file_rows_map_to_torrent_files() {
        let client = QbittorrentClient::new(
            "qbit",
            "http://127.0.0.1:1",
            None,
            None,
            Duration::from_secs(1),
        );
        let files: Vec<QbitFile> = serde_json::from_str(
            r#"[{"name":"Show/Episode.mkv","size":123,"progress":1.0,"priority":1}]"#,
        )
        .unwrap();
        let mapped = files
            .into_iter()
            .enumerate()
            .map(|(index, file)| {
                TorrentFile::new(
                    PathBuf::from(file.name),
                    ByteSize::new(file.size),
                    FileIndex::new(u32::try_from(index).unwrap()),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();

        assert_eq!(PathBuf::from("Show/Episode.mkv"), mapped[0].relative_path);
        assert_eq!(123, mapped[0].size.get());
        drop(client);
    }

    #[tokio::test]
    async fn client_logs_in_renews_session_and_uses_v5_start_stop() {
        let seen = Arc::new(StdMutex::new(Vec::<String>::new()));
        let seen_requests = seen.clone();
        let endpoint = spawn_qbit_server(move |request| {
            let seen = seen_requests.clone();
            async move {
                let path = request.uri().path().to_owned();
                let cookie = request
                    .headers()
                    .get(COOKIE)
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or_default()
                    .to_owned();
                seen.lock().unwrap().push(format!("{path}|{cookie}"));
                match path.as_str() {
                    "/api/v2/auth/login" => {
                        let body = to_bytes(request.into_body(), 65_536).await.unwrap();
                        assert!(
                            String::from_utf8(body.to_vec())
                                .unwrap()
                                .contains("username=user")
                        );
                        response_with_cookie(AxumStatusCode::OK, "Ok.", "SID=renewed")
                    }
                    "/api/v2/app/version" if cookie == "SID=expired" => {
                        (AxumStatusCode::FORBIDDEN, "expired").into_response()
                    }
                    "/api/v2/app/version" => (AxumStatusCode::OK, "5.0.0").into_response(),
                    "/api/v2/torrents/start" => (AxumStatusCode::OK, "").into_response(),
                    _ => (AxumStatusCode::NOT_FOUND, path).into_response(),
                }
            }
        })
        .await;
        let client = QbittorrentClient::new(
            "qbit",
            endpoint,
            Some("user".to_owned()),
            Some("pass".to_owned()),
            Duration::from_secs(5),
        );
        *client.cookie.lock().await = Some("SID=expired".to_owned());
        let hash = InfoHash::new(SHA1).unwrap();

        client.resume(&hash).await.unwrap();

        let seen = seen.lock().unwrap().join("\n");
        assert!(seen.contains("/api/v2/app/version|SID=expired"));
        assert!(seen.contains("/api/v2/auth/login|"));
        assert!(seen.contains("/api/v2/torrents/start|SID=renewed"));
    }

    #[tokio::test]
    async fn client_rejects_oversized_text_responses() {
        let endpoint = spawn_qbit_server(|request| async move {
            let path = request.uri().path().to_owned();
            match path.as_str() {
                "/api/v2/auth/login" => response_with_cookie(AxumStatusCode::OK, "Ok.", "SID=ok"),
                "/api/v2/app/version" => oversized_response(QBIT_RESPONSE_MAX_BYTES + 1),
                _ => (AxumStatusCode::NOT_FOUND, path).into_response(),
            }
        })
        .await;
        let client = QbittorrentClient::new("qbit", endpoint, None, None, Duration::from_secs(5));

        let error = client.version().await.unwrap_err();

        assert!(
            matches!(
                error,
                TorrentClientError::BadResponse { ref message, .. }
                    if message.contains("response exceeded")
            ),
            "got {error:?}"
        );
    }

    #[tokio::test]
    async fn client_rejects_chunked_oversized_text_responses() {
        let endpoint = spawn_chunked_response_server(QBIT_RESPONSE_MAX_BYTES + 1);
        let client = QbittorrentClient::new("qbit", endpoint, None, None, Duration::from_secs(5));
        *client.cookie.lock().await = Some("SID=ok".to_owned());

        let error = client.version().await.unwrap_err();

        assert!(
            matches!(
                error,
                TorrentClientError::BadResponse { ref message, .. }
                    if message.contains("response exceeded")
            ),
            "got {error:?}"
        );
    }

    #[test]
    fn client_reader_rejects_oversized_chunks_without_content_length() {
        let mut body = Vec::new();

        assert!(append_limited_body_chunk(&mut body, b"12345678", 8));
        assert!(!append_limited_body_chunk(&mut body, b"9", 8));
        assert_eq!(b"12345678", body.as_slice());
    }

    #[tokio::test]
    async fn client_posts_recheck_pause_and_v4_resume_endpoints() {
        let seen = Arc::new(StdMutex::new(Vec::<String>::new()));
        let seen_requests = seen.clone();
        let endpoint = spawn_qbit_server(move |request| {
            let seen = seen_requests.clone();
            async move {
                let path = request.uri().path().to_owned();
                seen.lock().unwrap().push(path.clone());
                match path.as_str() {
                    "/api/v2/auth/login" => {
                        response_with_cookie(AxumStatusCode::OK, "Ok.", "SID=ok")
                    }
                    "/api/v2/app/version" => (AxumStatusCode::OK, "4.6.0").into_response(),
                    "/api/v2/torrents/recheck"
                    | "/api/v2/torrents/pause"
                    | "/api/v2/torrents/resume" => (AxumStatusCode::OK, "").into_response(),
                    _ => (AxumStatusCode::NOT_FOUND, path).into_response(),
                }
            }
        })
        .await;
        let client = QbittorrentClient::new("qbit", endpoint, None, None, Duration::from_secs(5));
        let hash = InfoHash::new(SHA1).unwrap();

        client.recheck(&hash).await.unwrap();
        client.pause(&hash).await.unwrap();
        client.resume(&hash).await.unwrap();

        let seen = seen.lock().unwrap().join("\n");
        assert!(seen.contains("/api/v2/torrents/recheck"));
        assert!(seen.contains("/api/v2/torrents/pause"));
        assert!(seen.contains("/api/v2/torrents/resume"));
    }

    #[tokio::test]
    async fn client_injects_multipart_torrent_with_category_tag_and_layout() {
        let endpoint = spawn_qbit_server(|request| async move {
            let path = request.uri().path().to_owned();
            match path.as_str() {
                "/api/v2/auth/login" => response_with_cookie(AxumStatusCode::OK, "Ok.", "SID=ok"),
                "/api/v2/app/version" => (AxumStatusCode::OK, "4.6.0").into_response(),
                "/api/v2/torrents/add" => {
                    let body = to_bytes(request.into_body(), 1_000_000).await.unwrap();
                    let body = String::from_utf8_lossy(&body);
                    assert!(body.contains("candidate.torrent"));
                    assert!(body.contains("sporos"));
                    assert!(body.contains("category"));
                    assert!(body.contains("movies"));
                    assert!(body.contains("contentLayout"));
                    assert!(body.contains("Original"));
                    assert!(body.contains("paused"));
                    assert!(body.contains("name=\"paused\"\r\n\r\ntrue"));
                    assert!(body.contains("name=\"skip_checking\"\r\n\r\nfalse"));
                    (AxumStatusCode::OK, "").into_response()
                }
                _ => (AxumStatusCode::NOT_FOUND, path).into_response(),
            }
        })
        .await;
        let client = QbittorrentClient::new("qbit", endpoint, None, None, Duration::from_secs(5));
        let save_path = PathBuf::from("/downloads");

        client
            .inject(QbitAddTorrent {
                torrent_bytes: b"torrent-bytes",
                save_path: Some(&save_path),
                category: Some("movies"),
                pause_for_recheck: true,
                content_layout: QbitContentLayout::Original,
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn client_fetches_inventory_files_and_trackers() {
        let endpoint = spawn_qbit_server(|request| async move {
            let path = request.uri().path().to_owned();
            match path.as_str() {
                "/api/v2/auth/login" => response_with_cookie(AxumStatusCode::OK, "Ok.", "SID=ok"),
                "/api/v2/torrents/info" => (
                    AxumStatusCode::OK,
                    r#"[{"hash":"0123456789abcdef0123456789abcdef01234567","name":"Example","amount_left":0,"progress":1.0}]"#,
                )
                    .into_response(),
                "/api/v2/torrents/files" => (
                    AxumStatusCode::OK,
                    r#"[{"name":"Example/file.mkv","size":42,"progress":1.0,"priority":1}]"#,
                )
                    .into_response(),
                "/api/v2/torrents/trackers" => (
                    AxumStatusCode::OK,
                    r#"[{"url":"https://tracker.example/announce","status":2,"msg":""}]"#,
                )
                    .into_response(),
                _ => (AxumStatusCode::NOT_FOUND, path).into_response(),
            }
        })
        .await;
        let client = QbittorrentClient::new("qbit", endpoint, None, None, Duration::from_secs(5));
        let hash = InfoHash::new(SHA1).unwrap();

        let inventory = client.list_inventory().await.unwrap();
        let files = client.fetch_files(&hash).await.unwrap();
        let trackers = client.fetch_trackers(&hash).await.unwrap();

        assert_eq!("Example", inventory[0].name);
        assert_eq!(PathBuf::from("Example/file.mkv"), files[0].relative_path);
        assert_eq!("https://tracker.example/announce", trackers[0].url);
    }

    #[tokio::test]
    async fn client_pages_large_inventory_requests() {
        let seen_queries = Arc::new(StdMutex::new(Vec::<String>::new()));
        let seen_requests = seen_queries.clone();
        let endpoint = spawn_qbit_server(move |request| {
            let seen_queries = seen_requests.clone();
            async move {
                let path = request.uri().path().to_owned();
                match path.as_str() {
                    "/api/v2/auth/login" => {
                        response_with_cookie(AxumStatusCode::OK, "Ok.", "SID=ok")
                    }
                    "/api/v2/torrents/info" => {
                        let query = request.uri().query().unwrap_or_default().to_owned();
                        seen_queries.lock().unwrap().push(query.clone());
                        let limit = query_param(&query, "limit");
                        let offset = query_param(&query, "offset");
                        assert_eq!(QBIT_INVENTORY_PAGE_SIZE, limit);

                        let total = QBIT_INVENTORY_PAGE_SIZE + 1;
                        let count = total.saturating_sub(offset).min(limit);
                        (AxumStatusCode::OK, qbit_inventory_response(offset, count)).into_response()
                    }
                    _ => (AxumStatusCode::NOT_FOUND, path).into_response(),
                }
            }
        })
        .await;
        let client = QbittorrentClient::new("qbit", endpoint, None, None, Duration::from_secs(5));

        let inventory = client.list_inventory().await.unwrap();

        assert_eq!(QBIT_INVENTORY_PAGE_SIZE + 1, inventory.len());
        assert_eq!(
            vec![
                format!("sort=hash&limit={QBIT_INVENTORY_PAGE_SIZE}&offset=0"),
                format!(
                    "sort=hash&limit={QBIT_INVENTORY_PAGE_SIZE}&offset={QBIT_INVENTORY_PAGE_SIZE}"
                ),
            ],
            *seen_queries.lock().unwrap()
        );

        seen_queries.lock().unwrap().clear();
        let page_lengths = Arc::new(StdMutex::new(Vec::<usize>::new()));
        let streamed = client
            .list_inventory_pages({
                let page_lengths = page_lengths.clone();
                move |page| {
                    let page_lengths = page_lengths.clone();
                    async move {
                        page_lengths.lock().unwrap().push(page.len());
                        Ok(())
                    }
                }
            })
            .await
            .unwrap();

        assert_eq!(QBIT_INVENTORY_PAGE_SIZE + 1, streamed);
        assert_eq!(
            vec![QBIT_INVENTORY_PAGE_SIZE, 1],
            *page_lengths.lock().unwrap()
        );
        assert_eq!(
            vec![
                format!("sort=hash&limit={QBIT_INVENTORY_PAGE_SIZE}&offset=0"),
                format!(
                    "sort=hash&limit={QBIT_INVENTORY_PAGE_SIZE}&offset={QBIT_INVENTORY_PAGE_SIZE}"
                ),
            ],
            *seen_queries.lock().unwrap()
        );
    }

    #[tokio::test]
    async fn client_inventory_pages_stop_before_request_on_shutdown() {
        let requests = Arc::new(StdMutex::new(0usize));
        let seen_requests = requests.clone();
        let endpoint = spawn_qbit_server(move |_request| {
            let seen_requests = seen_requests.clone();
            async move {
                *seen_requests.lock().unwrap() += 1;
                (AxumStatusCode::OK, "[]").into_response()
            }
        })
        .await;
        let client = QbittorrentClient::new("qbit", endpoint, None, None, Duration::from_secs(5));
        let (shutdown, signal) = crate::runtime::shutdown::shutdown_channel();
        shutdown.cancel_now("test shutdown").unwrap();

        let error = client
            .list_inventory_pages_until_shutdown(
                || {
                    let mut signal = signal.clone();
                    async move {
                        let _ = signal.cancelled().await;
                    }
                },
                |_page| async { Ok(()) },
            )
            .await
            .unwrap_err();

        assert!(matches!(error, TorrentClientError::Cancelled { .. }));
        assert_eq!(0, *requests.lock().unwrap());
    }

    fn response_with_cookie(
        status: AxumStatusCode,
        body: &'static str,
        cookie: &'static str,
    ) -> Response {
        let mut response = (status, body).into_response();
        response
            .headers_mut()
            .insert(SET_COOKIE, cookie.parse().unwrap());
        response
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

    fn spawn_chunked_response_server(length: u64) -> String {
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
        format!("http://{address}")
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

    fn query_param(query: &str, name: &str) -> usize {
        query
            .split('&')
            .filter_map(|part| part.split_once('='))
            .find_map(|(key, value)| (key == name).then(|| value.parse().unwrap()))
            .unwrap()
    }

    fn qbit_inventory_response(start: usize, count: usize) -> String {
        let mut body = String::from("[");
        for index in 0..count {
            if index > 0 {
                body.push(',');
            }
            let id = start + index + 1;
            body.push_str(&format!(
                r#"{{"hash":"{id:040x}","name":"Example {id}","amount_left":0,"progress":1.0}}"#
            ));
        }
        body.push(']');
        body
    }

    async fn spawn_qbit_server<F, Fut, R>(handler: F) -> String
    where
        F: Fn(Request<Body>) -> Fut + Clone + Send + Sync + 'static,
        Fut: Future<Output = R> + Send + 'static,
        R: IntoResponse + Send + 'static,
    {
        let app = Router::new()
            .route("/api/v2/auth/login", post(handler.clone()))
            .route("/api/v2/app/version", get(handler.clone()))
            .route("/api/v2/torrents/info", get(handler.clone()))
            .route("/api/v2/torrents/files", get(handler.clone()))
            .route("/api/v2/torrents/trackers", get(handler.clone()))
            .route("/api/v2/torrents/add", post(handler.clone()))
            .route("/api/v2/torrents/recheck", post(handler.clone()))
            .route("/api/v2/torrents/resume", post(handler.clone()))
            .route("/api/v2/torrents/start", post(handler.clone()))
            .route("/api/v2/torrents/pause", post(handler.clone()))
            .route("/api/v2/torrents/stop", post(handler.clone()))
            .route("/api/v2/torrents/createTags", post(handler.clone()))
            .route("/api/v2/torrents/createCategory", post(handler));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{address}")
    }
}
