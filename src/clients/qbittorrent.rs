use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

use reqwest::StatusCode;
use reqwest::header::{COOKIE, SET_COOKIE};
use reqwest::multipart::{Form, Part};
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::domain::{ByteSize, DisplayName, FileIndex, InfoHash, TorrentFile};
use crate::errors::TorrentClientError;

const SPOROS_TAG: &str = "sporos";
const MIN_QBIT_VERSION: QbitVersion = QbitVersion {
    major: 4,
    minor: 3,
    patch: 1,
};

#[derive(Debug)]
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
        let text = self.get_text("/api/v2/torrents/info").await?;
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
        response
            .text()
            .await
            .map_err(|error| unavailable(&self.client_name, error.to_string()))
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

fn unavailable(client: &str, message: String) -> TorrentClientError {
    TorrentClientError::Unavailable {
        client: client.to_owned(),
        retry_after_ms: None,
        message,
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::sync::{Arc, Mutex as StdMutex};

    use axum::Router;
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode as AxumStatusCode};
    use axum::response::{IntoResponse, Response};
    use axum::routing::{get, post};
    use tokio::net::TcpListener;

    use super::*;

    const SHA1: &str = "0123456789abcdef0123456789abcdef01234567";

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
