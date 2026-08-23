use std::{fmt, time::Duration};

use reqwest::{
    Client, StatusCode, Url,
    header::{AUTHORIZATION, HeaderValue},
    multipart::{Form, Part},
};
use semver::Version;
use serde::Deserialize;
use thiserror::Error;

const MIN_APPLICATION_VERSION: Version = Version::new(5, 2, 0);
const MIN_WEB_API_VERSION: Version = Version::new(2, 14, 1);
const MAX_VERSION_BYTES: usize = 64;
const MAX_MUTATION_BYTES: usize = 4 * 1024;
const MAX_STATE_BYTES: usize = 1024 * 1024;

#[derive(Clone)]
pub struct ApiKey(HeaderValue);

impl ApiKey {
    pub fn new(value: &str) -> Result<Self, QbittorrentConfigError> {
        let suffix = value
            .strip_prefix("qbt_")
            .filter(|suffix| suffix.len() == 28)
            .filter(|suffix| suffix.bytes().all(|byte| byte.is_ascii_alphanumeric()))
            .ok_or(QbittorrentConfigError::InvalidApiKey)?;
        debug_assert_eq!(suffix.len(), 28);

        let mut header = HeaderValue::from_str(&format!("Bearer {value}"))
            .map_err(|_| QbittorrentConfigError::InvalidApiKey)?;
        header.set_sensitive(true);
        Ok(Self(header))
    }
}

impl fmt::Debug for ApiKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ApiKey([REDACTED])")
    }
}

#[derive(Debug, Error)]
pub enum QbittorrentConfigError {
    #[error("qBittorrent URL must be an HTTP(S) base URL without credentials, query, or fragment")]
    InvalidBaseUrl,
    #[error("qBittorrent API key must be qbt_ followed by 28 ASCII alphanumeric characters")]
    InvalidApiKey,
    #[error("could not construct the qBittorrent HTTP client")]
    Client(#[source] reqwest::Error),
}

#[derive(Debug, Error)]
pub enum QbittorrentError {
    #[error("qBittorrent request failed")]
    Request(#[source] reqwest::Error),
    #[error("qBittorrent returned HTTP {0}")]
    HttpStatus(StatusCode),
    #[error("qBittorrent response exceeded its {0}-byte limit")]
    ResponseTooLarge(usize),
    #[error("qBittorrent returned a malformed {kind} response")]
    MalformedResponse {
        kind: &'static str,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("unsupported qBittorrent version: application {application}, Web API {web_api}")]
    UnsupportedVersion {
        application: Version,
        web_api: Version,
    },
    #[error("qBittorrent rejected the torrent add")]
    AddRejected,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupportedVersions {
    pub application: Version,
    pub web_api: Version,
}

#[derive(Debug, Clone)]
pub struct AddTorrentRequest {
    pub torrent: Vec<u8>,
    pub filename: String,
    pub save_path: String,
    pub category: Option<String>,
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddSubmission {
    pub added_torrent_ids: Vec<String>,
    pub pending_count: u64,
}

#[derive(Debug, Deserialize)]
struct AddReceipt {
    success_count: u64,
    failure_count: u64,
    pending_count: u64,
    added_torrent_ids: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct TorrentState {
    pub hash: String,
    pub name: String,
    pub state: String,
    pub amount_left: u64,
    pub save_path: String,
    pub content_path: String,
    pub category: String,
    pub tags: String,
    pub auto_tmm: bool,
}

impl TorrentState {
    pub fn is_stopped(&self) -> bool {
        self.state.starts_with("stopped")
    }
}

#[derive(Debug, Clone)]
pub struct QbittorrentClient {
    client: Client,
    base_url: Url,
    authorization: ApiKey,
}

impl QbittorrentClient {
    pub fn new(base_url: Url, api_key: ApiKey) -> Result<Self, QbittorrentConfigError> {
        Self::with_timeout(base_url, api_key, Duration::from_secs(30))
    }

    pub fn with_timeout(
        base_url: Url,
        api_key: ApiKey,
        timeout: Duration,
    ) -> Result<Self, QbittorrentConfigError> {
        if !matches!(base_url.scheme(), "http" | "https")
            || base_url.cannot_be_a_base()
            || !base_url.username().is_empty()
            || base_url.password().is_some()
            || base_url.query().is_some()
            || base_url.fragment().is_some()
        {
            return Err(QbittorrentConfigError::InvalidBaseUrl);
        }

        let mut normalized = base_url;
        if !normalized.path().ends_with('/') {
            normalized.set_path(&format!("{}/", normalized.path()));
        }

        Ok(Self {
            client: Client::builder()
                .timeout(timeout)
                .build()
                .map_err(QbittorrentConfigError::Client)?,
            base_url: normalized,
            authorization: api_key,
        })
    }

    pub async fn validate_contract(&self) -> Result<SupportedVersions, QbittorrentError> {
        let application = self
            .version("api/v2/app/version", "application version")
            .await?;
        let web_api = self
            .version("api/v2/app/webapiVersion", "Web API version")
            .await?;

        if application < MIN_APPLICATION_VERSION || web_api < MIN_WEB_API_VERSION {
            return Err(QbittorrentError::UnsupportedVersion {
                application,
                web_api,
            });
        }

        Ok(SupportedVersions {
            application,
            web_api,
        })
    }

    pub async fn add_stopped(
        &self,
        request: AddTorrentRequest,
    ) -> Result<AddSubmission, QbittorrentError> {
        let mut form = Form::new()
            .part(
                "torrents",
                Part::bytes(request.torrent)
                    .file_name(request.filename)
                    .mime_str("application/x-bittorrent")
                    .expect("static MIME type is valid"),
            )
            .text("savepath", request.save_path)
            .text("stopped", "true")
            .text("skip_checking", "false")
            .text("contentLayout", "Original")
            .text("autoTMM", "false");

        if let Some(category) = request.category {
            form = form.text("category", category);
        }
        if !request.tags.is_empty() {
            form = form.text("tags", request.tags.join(","));
        }

        let response = self
            .request(reqwest::Method::POST, "api/v2/torrents/add")
            .multipart(form)
            .send()
            .await
            .map_err(QbittorrentError::Request)?;
        let body = checked_body(response, MAX_MUTATION_BYTES).await?;
        if body == b"Ok." {
            return Ok(AddSubmission {
                added_torrent_ids: Vec::new(),
                pending_count: 0,
            });
        }

        let receipt: AddReceipt = serde_json::from_slice(&body).map_err(|source| {
            QbittorrentError::MalformedResponse {
                kind: "add acknowledgement",
                source: Box::new(source),
            }
        })?;
        if receipt.failure_count != 0
            || receipt.success_count.checked_add(receipt.pending_count) != Some(1)
            || usize::try_from(receipt.success_count).ok() != Some(receipt.added_torrent_ids.len())
        {
            return Err(QbittorrentError::AddRejected);
        }
        Ok(AddSubmission {
            added_torrent_ids: receipt.added_torrent_ids,
            pending_count: receipt.pending_count,
        })
    }

    pub async fn torrent_state(
        &self,
        info_hash: &str,
    ) -> Result<Option<TorrentState>, QbittorrentError> {
        let response = self
            .request(reqwest::Method::GET, "api/v2/torrents/info")
            .query(&[("hashes", info_hash)])
            .send()
            .await
            .map_err(QbittorrentError::Request)?;
        let body = checked_body(response, MAX_STATE_BYTES).await?;
        let mut states: Vec<TorrentState> = serde_json::from_slice(&body).map_err(|source| {
            QbittorrentError::MalformedResponse {
                kind: "torrent state",
                source: Box::new(source),
            }
        })?;
        Ok(states.pop())
    }

    pub async fn stop(&self, info_hash: &str) -> Result<(), QbittorrentError> {
        let response = self
            .request(reqwest::Method::POST, "api/v2/torrents/stop")
            .form(&[("hashes", info_hash)])
            .send()
            .await
            .map_err(QbittorrentError::Request)?;
        let body = checked_body(response, MAX_MUTATION_BYTES).await?;
        if !body.is_empty() {
            return Err(malformed_text("stop acknowledgement"));
        }
        Ok(())
    }

    fn request(&self, method: reqwest::Method, endpoint: &str) -> reqwest::RequestBuilder {
        let url = self
            .base_url
            .join(endpoint)
            .expect("fixed qBittorrent endpoint is a valid relative URL");
        self.client
            .request(method, url)
            .header(AUTHORIZATION, self.authorization.0.clone())
    }

    async fn version(
        &self,
        endpoint: &str,
        kind: &'static str,
    ) -> Result<Version, QbittorrentError> {
        let response = self
            .request(reqwest::Method::GET, endpoint)
            .send()
            .await
            .map_err(QbittorrentError::Request)?;
        let body = checked_body(response, MAX_VERSION_BYTES).await?;
        let text =
            std::str::from_utf8(&body).map_err(|source| QbittorrentError::MalformedResponse {
                kind,
                source: Box::new(source),
            })?;
        Version::parse(text.trim().trim_start_matches('v')).map_err(|source| {
            QbittorrentError::MalformedResponse {
                kind,
                source: Box::new(source),
            }
        })
    }
}

async fn checked_body(
    mut response: reqwest::Response,
    limit: usize,
) -> Result<Vec<u8>, QbittorrentError> {
    let status = response.status();
    if !status.is_success() {
        return Err(QbittorrentError::HttpStatus(status));
    }
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err(QbittorrentError::ResponseTooLarge(limit));
    }

    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(QbittorrentError::Request)? {
        if body.len().saturating_add(chunk.len()) > limit {
            return Err(QbittorrentError::ResponseTooLarge(limit));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn malformed_text(kind: &'static str) -> QbittorrentError {
    QbittorrentError::MalformedResponse {
        kind,
        source: std::io::Error::new(std::io::ErrorKind::InvalidData, "unexpected response body")
            .into(),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Write},
        net::{TcpListener, TcpStream},
        sync::mpsc,
        thread,
        time::Duration,
    };

    use super::*;

    const API_KEY: &str = "qbt_0123456789abcdefghijklmnopqr";

    struct Reply {
        status: &'static str,
        content_type: &'static str,
        body: &'static [u8],
    }

    struct Request {
        head: String,
        body: Vec<u8>,
    }

    fn server(replies: Vec<Reply>) -> (Url, mpsc::Receiver<Request>, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake qBittorrent");
        let address = listener.local_addr().expect("fake address");
        let (requests_tx, requests_rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            for reply in replies {
                let (mut stream, _) = listener.accept().expect("accept request");
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .expect("set timeout");
                requests_tx
                    .send(read_request(&mut stream))
                    .expect("record request");
                write!(
                    stream,
                    "HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    reply.status,
                    reply.content_type,
                    reply.body.len()
                )
                .expect("write response head");
                stream.write_all(reply.body).expect("write response body");
            }
        });
        (
            Url::parse(&format!("http://{address}")).expect("fake URL"),
            requests_rx,
            handle,
        )
    }

    fn read_request(stream: &mut TcpStream) -> Request {
        let mut bytes = Vec::new();
        let mut chunk = [0_u8; 4096];
        let header_end = loop {
            let count = stream.read(&mut chunk).expect("read request");
            assert_ne!(count, 0, "request ended before headers");
            bytes.extend_from_slice(&chunk[..count]);
            if let Some(position) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                break position + 4;
            }
        };
        let head = String::from_utf8(bytes[..header_end].to_vec()).expect("ASCII request head");
        let content_length = head
            .lines()
            .find_map(|line| {
                line.to_ascii_lowercase()
                    .strip_prefix("content-length:")
                    .map(|value| value.trim().parse::<usize>().expect("content length"))
            })
            .unwrap_or(0);
        while bytes.len() - header_end < content_length {
            let count = stream.read(&mut chunk).expect("read request body");
            assert_ne!(count, 0, "request body ended early");
            bytes.extend_from_slice(&chunk[..count]);
        }
        Request {
            head,
            body: bytes[header_end..header_end + content_length].to_vec(),
        }
    }

    fn client(url: Url) -> QbittorrentClient {
        QbittorrentClient::new(url, ApiKey::new(API_KEY).expect("API key")).expect("client")
    }

    #[test]
    fn secrets_are_validated_and_redacted() {
        assert!(ApiKey::new("not-a-key").is_err());
        assert_eq!(
            format!("{:?}", ApiKey::new(API_KEY).expect("API key")),
            "ApiKey([REDACTED])"
        );
        let url = Url::parse("http://user:password@localhost/").expect("URL");
        assert!(matches!(
            QbittorrentClient::new(url, ApiKey::new(API_KEY).expect("API key")),
            Err(QbittorrentConfigError::InvalidBaseUrl)
        ));
    }

    #[tokio::test]
    async fn validates_bearer_authenticated_versions() {
        let (url, requests, server) = server(vec![
            Reply {
                status: "200 OK",
                content_type: "text/plain",
                body: b"v5.2.1",
            },
            Reply {
                status: "200 OK",
                content_type: "text/plain",
                body: b"2.14.1",
            },
        ]);

        assert_eq!(
            client(url).validate_contract().await.expect("versions"),
            SupportedVersions {
                application: Version::new(5, 2, 1),
                web_api: Version::new(2, 14, 1),
            }
        );
        for endpoint in ["/api/v2/app/version", "/api/v2/app/webapiVersion"] {
            let request = requests.recv().expect("request");
            assert!(
                request
                    .head
                    .starts_with(&format!("GET {endpoint} HTTP/1.1"))
            );
            assert!(
                request
                    .head
                    .contains(&format!("authorization: Bearer {API_KEY}"))
            );
        }
        server.join().expect("fake server");
    }

    #[tokio::test]
    async fn rejects_an_older_contract() {
        let (url, _requests, server) = server(vec![
            Reply {
                status: "200 OK",
                content_type: "text/plain",
                body: b"v5.1.9",
            },
            Reply {
                status: "200 OK",
                content_type: "text/plain",
                body: b"2.14.0",
            },
        ]);

        assert!(matches!(
            client(url).validate_contract().await,
            Err(QbittorrentError::UnsupportedVersion { .. })
        ));
        server.join().expect("fake server");
    }

    #[tokio::test]
    async fn adds_with_the_safe_5_2_field_set() {
        let (url, requests, server) = server(vec![Reply {
            status: "200 OK",
            content_type: "text/plain",
            body: b"Ok.",
        }]);
        let torrent = b"d4:infod4:name4:testee".to_vec();

        assert_eq!(
            client(url)
                .add_stopped(AddTorrentRequest {
                    torrent: torrent.clone(),
                    filename: "candidate.torrent".into(),
                    save_path: "/downloads/candidate".into(),
                    category: Some("cross-seed".into()),
                    tags: vec!["sporos".into(), "verified".into()],
                })
                .await
                .expect("add torrent"),
            AddSubmission {
                added_torrent_ids: Vec::new(),
                pending_count: 0,
            }
        );

        let request = requests.recv().expect("request");
        assert!(
            request
                .head
                .starts_with("POST /api/v2/torrents/add HTTP/1.1")
        );
        assert!(
            request
                .head
                .contains(&format!("authorization: Bearer {API_KEY}"))
        );
        let body = String::from_utf8_lossy(&request.body);
        for (name, value) in [
            ("savepath", "/downloads/candidate"),
            ("stopped", "true"),
            ("skip_checking", "false"),
            ("contentLayout", "Original"),
            ("autoTMM", "false"),
            ("category", "cross-seed"),
            ("tags", "sporos,verified"),
        ] {
            assert!(body.contains(&format!("name=\"{name}\"\r\n\r\n{value}\r\n")));
        }
        assert!(body.contains("name=\"torrents\"; filename=\"candidate.torrent\""));
        assert!(
            request
                .body
                .windows(torrent.len())
                .any(|window| window == torrent)
        );
        server.join().expect("fake server");
    }

    #[tokio::test]
    async fn accepts_a_detailed_add_receipt() {
        let (url, _requests, server) = server(vec![Reply {
            status: "200 OK",
            content_type: "application/json",
            body: br#"{"added_torrent_ids":["0123"],"failure_count":0,"pending_count":0,"success_count":1}"#,
        }]);

        let submission = client(url)
            .add_stopped(AddTorrentRequest {
                torrent: b"torrent".to_vec(),
                filename: "candidate.torrent".into(),
                save_path: "/downloads/candidate".into(),
                category: None,
                tags: Vec::new(),
            })
            .await
            .expect("detailed add receipt");
        assert_eq!(
            submission,
            AddSubmission {
                added_torrent_ids: vec!["0123".into()],
                pending_count: 0,
            }
        );
        server.join().expect("fake server");
    }

    #[tokio::test]
    async fn reads_and_stops_by_infohash() {
        let state = br#"[{"hash":"0123","name":"file.bin","state":"stoppedDL","amount_left":4,"save_path":"/downloads/candidate/","content_path":"/downloads/candidate/file.bin","category":"","tags":"","auto_tmm":false}]"#;
        let (url, requests, server) = server(vec![
            Reply {
                status: "200 OK",
                content_type: "application/json",
                body: state,
            },
            Reply {
                status: "200 OK",
                content_type: "text/plain",
                body: b"",
            },
        ]);
        let client = client(url);

        let state = client
            .torrent_state("0123")
            .await
            .expect("state response")
            .expect("torrent present");
        assert!(state.is_stopped());
        assert!(!state.auto_tmm);
        client.stop("0123").await.expect("stop torrent");

        let info = requests.recv().expect("info request");
        assert!(
            info.head
                .starts_with("GET /api/v2/torrents/info?hashes=0123 HTTP/1.1")
        );
        let stop = requests.recv().expect("stop request");
        assert!(stop.head.starts_with("POST /api/v2/torrents/stop HTTP/1.1"));
        assert_eq!(stop.body, b"hashes=0123");
        server.join().expect("fake server");
    }
}
