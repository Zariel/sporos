use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use reqwest::header::{HeaderName, HeaderValue};
use reqwest::{Client, StatusCode, Url};
use serde::{Deserialize, Serialize};
use sporos_matcher::parse_release;
use sqlx::Row;
use thiserror::Error;

use crate::config::{ArrInstance, ArrKind};
use crate::storage::Storage;

const API_KEY_HEADER: HeaderName = HeaderName::from_static("x-api-key");
const MAX_HISTORY_BYTES: usize = 2 * 1024 * 1024;
const HISTORY_PAGE_SIZE: usize = 100;
const NEGATIVE_CACHE_MS: i64 = 60 * 60 * 1_000;

#[derive(Clone)]
pub struct ApiKey(HeaderValue);

impl ApiKey {
    pub fn new(value: &str) -> Result<Self, ArrConfigError> {
        let mut value = HeaderValue::from_str(value).map_err(|_| ArrConfigError::InvalidApiKey)?;
        if value.is_empty() {
            return Err(ArrConfigError::InvalidApiKey);
        }
        value.set_sensitive(true);
        Ok(Self(value))
    }
}

impl fmt::Debug for ApiKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ApiKey([REDACTED])")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ArrIdentity {
    pub source_title: String,
    pub event_type: String,
    pub series_id: Option<i64>,
    pub episode_id: Option<i64>,
    pub movie_id: Option<i64>,
    pub title: Option<String>,
    pub tvdb_id: Option<i64>,
    pub tmdb_id: Option<i64>,
    pub imdb_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ArrClient {
    client: Client,
    base_url: Url,
    api_key: ApiKey,
}

pub struct ArrCacheEntry<'a> {
    pub source_id: [u8; 16],
    pub kind: ArrKind,
    pub instance_name: &'a str,
    pub source_hash: &'a str,
    pub identity: Option<&'a ArrIdentity>,
    pub fetched_at: i64,
    pub negative_expires_at: Option<i64>,
}

#[derive(Clone)]
pub(crate) struct ArrEnricher {
    storage: Arc<Storage>,
    instances: Arc<[EnrichmentInstance]>,
}

#[derive(Clone)]
struct EnrichmentInstance {
    kind: ArrKind,
    name: String,
    client: ArrClient,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct EnrichmentReport {
    pub queried: usize,
    pub cache_hits: usize,
    pub failed: usize,
    pub applied: bool,
}

impl ArrEnricher {
    pub(crate) fn new(
        storage: Arc<Storage>,
        instances: &[ArrInstance],
    ) -> Result<Self, ArrConfigError> {
        let instances = instances
            .iter()
            .map(|instance| {
                Ok(EnrichmentInstance {
                    kind: instance.kind,
                    name: instance.name.clone(),
                    client: ArrClient::new(
                        instance.url.clone(),
                        ApiKey::new(instance.api_key.expose())?,
                        instance.request_timeout,
                    )?,
                })
            })
            .collect::<Result<Vec<_>, ArrConfigError>>()?;
        Ok(Self {
            storage,
            instances: instances.into(),
        })
    }

    pub(crate) async fn enrich_source(
        &self,
        source_id: [u8; 16],
        now: i64,
    ) -> Result<EnrichmentReport, ArrError> {
        let row = sqlx::query(
            "SELECT qbit_id, release_json FROM sporos_qbit_torrent
             WHERE id = ? AND available = 1",
        )
        .bind(source_id.as_slice())
        .fetch_optional(self.storage.pool())
        .await?;
        let Some(row) = row else {
            return Ok(EnrichmentReport::default());
        };
        let Some(source_hash) = row.try_get::<Option<String>, _>("qbit_id")? else {
            return Ok(EnrichmentReport::default());
        };
        let Some(release_json) = row.try_get::<Option<String>, _>("release_json")? else {
            return Ok(EnrichmentReport::default());
        };
        let mut release: sporos_model::ReleaseDescriptor = serde_json::from_str(&release_json)?;
        let mut report = EnrichmentReport::default();
        let mut selected = None;
        for instance in self
            .instances
            .iter()
            .filter(|instance| relevant(instance.kind, release.kind))
        {
            let identity = match self.cached(source_id, instance, &source_hash, now).await? {
                CacheState::Hit(identity) => {
                    report.cache_hits += 1;
                    identity
                }
                CacheState::Miss => {
                    report.queried += 1;
                    match instance.client.history_by_download_id(&source_hash).await {
                        Ok(identity) => {
                            self.storage
                                .cache_arr_identity(&ArrCacheEntry {
                                    source_id,
                                    kind: instance.kind,
                                    instance_name: &instance.name,
                                    source_hash: &source_hash,
                                    identity: identity.as_ref(),
                                    fetched_at: now,
                                    negative_expires_at: identity
                                        .is_none()
                                        .then_some(now.saturating_add(NEGATIVE_CACHE_MS)),
                                })
                                .await?;
                            identity
                        }
                        Err(_) => {
                            report.failed += 1;
                            continue;
                        }
                    }
                }
            };
            if selected.is_none()
                && let Some(identity) = identity
                && let Some(model) = model_identity(instance, &identity)
            {
                selected = Some((identity, model));
            }
        }
        if let Some((identity, model)) = selected {
            for title in [
                identity.title.as_deref(),
                Some(identity.source_title.as_str()),
            ]
            .into_iter()
            .flatten()
            {
                let title = parse_release(title).primary_title;
                if title != release.primary_title && !release.alternate_titles.contains(&title) {
                    release.alternate_titles.push(title);
                }
            }
            release.arr_identity = Some(model);
            sqlx::query(
                "UPDATE sporos_qbit_torrent SET release_json = ?, arr_identity_json = ?
                 WHERE id = ?",
            )
            .bind(serde_json::to_string(&release)?)
            .bind(serde_json::to_string(&identity)?)
            .bind(source_id.as_slice())
            .execute(self.storage.pool())
            .await?;
            report.applied = true;
        }
        Ok(report)
    }

    async fn cached(
        &self,
        source_id: [u8; 16],
        instance: &EnrichmentInstance,
        source_hash: &str,
        now: i64,
    ) -> Result<CacheState, ArrError> {
        let kind = match instance.kind {
            ArrKind::Sonarr => "sonarr",
            ArrKind::Radarr => "radarr",
        };
        let row = sqlx::query(
            "SELECT source_hash, identity_json, negative_expires_at
             FROM sporos_arr_enrichment_cache
             WHERE source_id = ? AND instance_kind = ? AND instance_name = ?",
        )
        .bind(source_id.as_slice())
        .bind(kind)
        .bind(&instance.name)
        .fetch_optional(self.storage.pool())
        .await?;
        let Some(row) = row else {
            return Ok(CacheState::Miss);
        };
        if row.try_get::<String, _>("source_hash")? != source_hash {
            return Ok(CacheState::Miss);
        }
        if let Some(json) = row.try_get::<Option<String>, _>("identity_json")? {
            return Ok(CacheState::Hit(Some(serde_json::from_str(&json)?)));
        }
        if row
            .try_get::<Option<i64>, _>("negative_expires_at")?
            .is_some_and(|expires| expires > now)
        {
            return Ok(CacheState::Hit(None));
        }
        Ok(CacheState::Miss)
    }
}

enum CacheState {
    Hit(Option<ArrIdentity>),
    Miss,
}

fn relevant(kind: ArrKind, video: sporos_model::VideoKind) -> bool {
    match video {
        sporos_model::VideoKind::Movie | sporos_model::VideoKind::Disc => kind == ArrKind::Radarr,
        sporos_model::VideoKind::Episode
        | sporos_model::VideoKind::SeasonPack
        | sporos_model::VideoKind::DateEpisode
        | sporos_model::VideoKind::AbsoluteEpisode => kind == ArrKind::Sonarr,
        sporos_model::VideoKind::UnknownVideo => true,
    }
}

fn model_identity(
    instance: &EnrichmentInstance,
    identity: &ArrIdentity,
) -> Option<sporos_model::ArrIdentity> {
    let (kind, entity_id) = match instance.kind {
        ArrKind::Sonarr => (sporos_model::ArrKind::Series, identity.series_id?),
        ArrKind::Radarr => (sporos_model::ArrKind::Movie, identity.movie_id?),
    };
    Some(sporos_model::ArrIdentity {
        kind,
        instance: instance.name.clone(),
        entity_id,
        tvdb_id: identity.tvdb_id,
        tmdb_id: identity.tmdb_id,
        imdb_id: identity.imdb_id.clone(),
    })
}

impl ArrClient {
    pub fn new(base_url: Url, api_key: ApiKey, timeout: Duration) -> Result<Self, ArrConfigError> {
        if !matches!(base_url.scheme(), "http" | "https")
            || base_url.cannot_be_a_base()
            || !base_url.username().is_empty()
            || base_url.password().is_some()
            || base_url.query().is_some()
            || base_url.fragment().is_some()
        {
            return Err(ArrConfigError::InvalidBaseUrl);
        }
        let mut base_url = base_url;
        if !base_url.path().ends_with('/') {
            base_url.set_path(&format!("{}/", base_url.path()));
        }
        Ok(Self {
            client: Client::builder()
                .timeout(timeout)
                .build()
                .map_err(ArrConfigError::Client)?,
            base_url,
            api_key,
        })
    }

    pub async fn history_by_download_id(
        &self,
        download_id: &str,
    ) -> Result<Option<ArrIdentity>, ArrError> {
        let url = self
            .base_url
            .join("api/v3/history")
            .expect("fixed Arr endpoint is a valid relative URL");
        let response = self
            .client
            .get(url)
            .header(API_KEY_HEADER, self.api_key.0.clone())
            .query(&[
                ("page", "1"),
                ("pageSize", "100"),
                ("sortKey", "date"),
                ("sortDirection", "descending"),
                ("downloadId", download_id),
            ])
            .send()
            .await
            .map_err(ArrError::Request)?;
        let body = checked_body(response).await?;
        let page: HistoryPage = serde_json::from_slice(&body).map_err(ArrError::Malformed)?;
        if page.records.len() > HISTORY_PAGE_SIZE {
            return Err(ArrError::TooManyRecords);
        }
        Ok(page
            .records
            .into_iter()
            .find(|record| {
                record
                    .download_id
                    .as_deref()
                    .is_some_and(|value| value.eq_ignore_ascii_case(download_id))
            })
            .map(HistoryRecord::identity))
    }
}

#[derive(Debug, Deserialize)]
struct HistoryPage {
    #[serde(default)]
    records: Vec<HistoryRecord>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HistoryRecord {
    #[serde(default)]
    download_id: Option<String>,
    #[serde(default)]
    source_title: String,
    #[serde(default)]
    event_type: String,
    series_id: Option<i64>,
    episode_id: Option<i64>,
    movie_id: Option<i64>,
    series: Option<Entity>,
    movie: Option<Entity>,
}

impl HistoryRecord {
    fn identity(self) -> ArrIdentity {
        let entity = self.series.or(self.movie);
        ArrIdentity {
            source_title: self.source_title,
            event_type: self.event_type,
            series_id: self.series_id,
            episode_id: self.episode_id,
            movie_id: self.movie_id,
            title: entity.as_ref().and_then(|value| value.title.clone()),
            tvdb_id: entity.as_ref().and_then(|value| value.tvdb_id),
            tmdb_id: entity.as_ref().and_then(|value| value.tmdb_id),
            imdb_id: entity.and_then(|value| value.imdb_id),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Entity {
    title: Option<String>,
    tvdb_id: Option<i64>,
    tmdb_id: Option<i64>,
    imdb_id: Option<String>,
}

impl Storage {
    pub async fn cache_arr_identity(&self, entry: &ArrCacheEntry<'_>) -> Result<(), ArrError> {
        let kind = match entry.kind {
            ArrKind::Sonarr => "sonarr",
            ArrKind::Radarr => "radarr",
        };
        let identity_json = entry.identity.map(serde_json::to_string).transpose()?;
        sqlx::query(
            "INSERT INTO sporos_arr_enrichment_cache (
                source_id, instance_kind, instance_name, source_hash,
                identity_json, fetched_at, negative_expires_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(source_id, instance_kind, instance_name) DO UPDATE SET
                source_hash = excluded.source_hash,
                identity_json = excluded.identity_json,
                fetched_at = excluded.fetched_at,
                negative_expires_at = excluded.negative_expires_at",
        )
        .bind(entry.source_id.as_slice())
        .bind(kind)
        .bind(entry.instance_name)
        .bind(entry.source_hash)
        .bind(identity_json)
        .bind(entry.fetched_at)
        .bind(entry.negative_expires_at)
        .execute(self.pool())
        .await?;
        Ok(())
    }
}

async fn checked_body(mut response: reqwest::Response) -> Result<Vec<u8>, ArrError> {
    let status = response.status();
    if !status.is_success() {
        return Err(ArrError::HttpStatus(status));
    }
    if response
        .content_length()
        .is_some_and(|length| length > MAX_HISTORY_BYTES as u64)
    {
        return Err(ArrError::ResponseTooLarge);
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(ArrError::Request)? {
        if body.len().saturating_add(chunk.len()) > MAX_HISTORY_BYTES {
            return Err(ArrError::ResponseTooLarge);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

#[derive(Debug, Error)]
pub enum ArrConfigError {
    #[error("Arr URL must be an HTTP(S) base URL without credentials, query, or fragment")]
    InvalidBaseUrl,
    #[error("Arr API key is not a valid HTTP header value")]
    InvalidApiKey,
    #[error("could not construct the Arr HTTP client")]
    Client(#[source] reqwest::Error),
}

#[derive(Debug, Error)]
pub enum ArrError {
    #[error("Arr request failed")]
    Request(#[source] reqwest::Error),
    #[error("Arr returned HTTP {0}")]
    HttpStatus(StatusCode),
    #[error("Arr history response exceeded its byte limit")]
    ResponseTooLarge,
    #[error("Arr history response was malformed")]
    Malformed(#[source] serde_json::Error),
    #[error("Arr history response exceeded its record limit")]
    TooManyRecords,
    #[error("Arr cache operation failed")]
    Database(#[from] sqlx::Error),
    #[error("Arr identity serialization failed")]
    Serialize(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::mpsc;
    use std::thread;

    use tempfile::TempDir;

    use super::*;
    use crate::inventory::{InventoryChange, InventoryTorrent};
    use crate::storage::Storage;

    #[tokio::test]
    async fn reads_history_by_download_id_with_api_key_authentication() {
        let body = br#"{"records":[{"downloadId":"AABB","sourceTitle":"Release","eventType":"grabbed","seriesId":7,"episodeId":9,"series":{"title":"Show","tvdbId":11,"imdbId":"tt1"}}]}"#;
        let (url, request, server) = server(body);
        let client =
            ArrClient::new(url, ApiKey::new("secret").unwrap(), Duration::from_secs(5)).unwrap();

        let identity = client
            .history_by_download_id("aabb")
            .await
            .unwrap()
            .expect("history match");
        assert_eq!(identity.series_id, Some(7));
        assert_eq!(identity.episode_id, Some(9));
        assert_eq!(identity.title.as_deref(), Some("Show"));
        let request = request.recv().unwrap();
        assert!(request.contains("GET /api/v3/history?"));
        assert!(request.contains("downloadId=aabb"));
        assert!(request.contains("x-api-key: secret"));
        server.join().unwrap();
    }

    #[tokio::test]
    async fn caches_positive_and_negative_instance_results() {
        let directory = TempDir::new().unwrap();
        let storage = Storage::open(
            directory.path().join("sporos.lock"),
            directory.path().join("sporos.db"),
        )
        .await
        .unwrap();
        storage
            .project_qbit_batch(&[source()], 1, false, 1)
            .await
            .unwrap();
        let source_id = sqlx::query_scalar::<_, Vec<u8>>("SELECT id FROM sporos_qbit_torrent")
            .fetch_one(storage.pool())
            .await
            .unwrap()
            .try_into()
            .unwrap();
        let identity = ArrIdentity {
            source_title: "Release".to_owned(),
            event_type: "grabbed".to_owned(),
            series_id: Some(7),
            episode_id: Some(9),
            movie_id: None,
            title: Some("Show".to_owned()),
            tvdb_id: Some(11),
            tmdb_id: None,
            imdb_id: Some("tt1".to_owned()),
        };
        storage
            .cache_arr_identity(&ArrCacheEntry {
                source_id,
                kind: ArrKind::Sonarr,
                instance_name: "main",
                source_hash: id(),
                identity: Some(&identity),
                fetched_at: 2,
                negative_expires_at: None,
            })
            .await
            .unwrap();
        storage
            .cache_arr_identity(&ArrCacheEntry {
                source_id,
                kind: ArrKind::Radarr,
                instance_name: "movies",
                source_hash: id(),
                identity: None,
                fetched_at: 2,
                negative_expires_at: Some(3),
            })
            .await
            .unwrap();

        let counts = sqlx::query_as::<_, (i64, i64)>(
            "SELECT count(*), count(identity_json) FROM sporos_arr_enrichment_cache",
        )
        .fetch_one(storage.pool())
        .await
        .unwrap();
        assert_eq!(counts, (2, 1));
    }

    #[tokio::test]
    async fn applies_one_instance_while_another_is_unavailable() {
        let body = br#"{"records":[{"downloadId":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","sourceTitle":"Example.Show.S01E01","eventType":"grabbed","seriesId":7,"episodeId":9,"series":{"title":"Example Show","tvdbId":11,"imdbId":"tt1"}}]}"#;
        let (sonarr_url, _request, server) = server(body);
        let directory = TempDir::new().unwrap();
        let storage = Arc::new(
            Storage::open(
                directory.path().join("sporos.lock"),
                directory.path().join("sporos.db"),
            )
            .await
            .unwrap(),
        );
        storage
            .project_qbit_batch(&[source()], 1, false, 1)
            .await
            .unwrap();
        let source_id: [u8; 16] =
            sqlx::query_scalar::<_, Vec<u8>>("SELECT id FROM sporos_qbit_torrent")
                .fetch_one(storage.pool())
                .await
                .unwrap()
                .try_into()
                .unwrap();
        let instances = [
            crate::config::ArrInstance {
                kind: ArrKind::Sonarr,
                name: "shows".to_owned(),
                url: sonarr_url,
                api_key: crate::config::Secret::new("secret"),
                request_timeout: Duration::from_secs(2),
            },
            crate::config::ArrInstance {
                kind: ArrKind::Radarr,
                name: "movies".to_owned(),
                url: Url::parse("http://127.0.0.1:1/").unwrap(),
                api_key: crate::config::Secret::new("secret"),
                request_timeout: Duration::from_millis(50),
            },
        ];

        let report = ArrEnricher::new(Arc::clone(&storage), &instances)
            .unwrap()
            .enrich_source(source_id, 10)
            .await
            .unwrap();

        assert_eq!(report.queried, 2);
        assert_eq!(report.failed, 1);
        assert!(report.applied);
        let release_json = sqlx::query_scalar::<_, String>(
            "SELECT release_json FROM sporos_qbit_torrent WHERE id = ?",
        )
        .bind(source_id.as_slice())
        .fetch_one(storage.pool())
        .await
        .unwrap();
        let release: sporos_model::ReleaseDescriptor = serde_json::from_str(&release_json).unwrap();
        let identity = release.arr_identity.unwrap();
        assert_eq!(identity.instance, "shows");
        assert_eq!(identity.entity_id, 7);
        assert_eq!(identity.tvdb_id, Some(11));
        server.join().unwrap();
    }

    fn source() -> InventoryChange {
        InventoryTorrent {
            hash: id().to_owned(),
            infohash_v1: id().to_owned(),
            infohash_v2: String::new(),
            name: "release".to_owned(),
            total_size: 4,
            amount_left: 4,
            progress: 0.0,
            state: "stoppedDL".to_owned(),
            save_path: "/data".to_owned(),
            content_path: "/data/release".to_owned(),
            category: String::new(),
            tags: String::new(),
            added_on: 1,
            completion_on: 0,
        }
        .into()
    }

    fn id() -> &'static str {
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    }

    fn server(body: &'static [u8]) -> (Url, mpsc::Receiver<String>, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (sender, receiver) = mpsc::channel();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            sender.send(read_request(&mut stream)).unwrap();
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            )
            .unwrap();
            stream.write_all(body).unwrap();
        });
        (
            Url::parse(&format!("http://{address}")).unwrap(),
            receiver,
            handle,
        )
    }

    fn read_request(stream: &mut TcpStream) -> String {
        let mut bytes = Vec::new();
        let mut chunk = [0_u8; 1024];
        while !bytes.windows(4).any(|value| value == b"\r\n\r\n") {
            let count = stream.read(&mut chunk).unwrap();
            assert_ne!(count, 0);
            bytes.extend_from_slice(&chunk[..count]);
        }
        String::from_utf8(bytes).unwrap()
    }
}
