use std::collections::BTreeSet;
use std::io;
use std::time::Duration;

use futures_util::StreamExt;
use reqwest::header::{HeaderValue, RETRY_AFTER};
use reqwest::{StatusCode, Url};
use serde::Deserialize;
use serde_json::Value;
use sporos_model::{ReleaseDescriptor, VideoKind};
use sqlx::Row;
use thiserror::Error;
use tokio::io::BufReader;
use tokio_util::io::StreamReader;

use crate::config::Prowlarr;
use crate::storage::Storage;

const MAX_INDEXER_BYTES: usize = 8 * 1024 * 1024;
const MAX_TORZNAB_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone)]
pub(crate) struct ProwlarrClient {
    client: reqwest::Client,
    base_url: Url,
    api_key: HeaderValue,
    include_tags: BTreeSet<i64>,
    exclude_tags: BTreeSet<i64>,
    require_proxy_downloads: bool,
    max_results: usize,
    max_torrent_bytes: usize,
}

impl ProwlarrClient {
    pub(crate) fn new(
        settings: &Prowlarr,
        max_torrent_bytes: usize,
    ) -> Result<Self, ProwlarrError> {
        let mut api_key = HeaderValue::from_str(settings.api_key.expose())
            .map_err(|_| ProwlarrError::InvalidApiKey)?;
        api_key.set_sensitive(true);
        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(settings.request_timeout)
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .map_err(ProwlarrError::Client)?,
            base_url: settings.url.clone(),
            api_key,
            include_tags: settings.include_tags.iter().copied().collect(),
            exclude_tags: settings.exclude_tags.iter().copied().collect(),
            require_proxy_downloads: settings.require_proxy_downloads,
            max_results: settings.max_results_per_query,
            max_torrent_bytes,
        })
    }

    pub(crate) async fn indexers(&self) -> Result<Vec<ProjectedIndexer>, ProwlarrError> {
        let response = self
            .get(self.base_url.join("api/v1/indexer").expect("fixed path"))
            .await?;
        let body = checked_body(response, MAX_INDEXER_BYTES).await?;
        let resources: Vec<IndexerResource> = serde_json::from_slice(&body)
            .map_err(|source| ProwlarrError::Malformed("indexer projection", source))?;
        resources
            .into_iter()
            .map(|resource| self.project(resource))
            .collect()
    }

    pub(crate) async fn search(
        &self,
        indexer_id: i64,
        query: &SearchQuery,
    ) -> Result<Vec<crate::torznab::TorznabResult>, ProwlarrError> {
        let url = self
            .base_url
            .join(&format!("api/v1/indexer/{indexer_id}/newznab"))
            .expect("fixed Prowlarr search path");
        let response = self
            .client
            .get(url)
            .header("X-Api-Key", self.api_key.clone())
            .query(&query.parameters)
            .send()
            .await
            .map_err(ProwlarrError::Request)?;
        let response = checked_response(response, MAX_TORZNAB_BYTES)?;
        let mut received = 0_usize;
        let stream = response.bytes_stream().map(move |result| match result {
            Ok(bytes) if received.saturating_add(bytes.len()) <= MAX_TORZNAB_BYTES => {
                received += bytes.len();
                Ok(bytes)
            }
            Ok(_) => Err(io::Error::from(io::ErrorKind::FileTooLarge)),
            Err(error) => Err(io::Error::other(error)),
        });
        let mut results = Vec::new();
        crate::torznab::parse_torznab_async(
            BufReader::new(StreamReader::new(stream)),
            self.max_results,
            |result| {
                results.push(result);
                Ok(())
            },
        )
        .await?;
        Ok(results)
    }

    pub(crate) async fn download(
        &self,
        indexer_id: i64,
        raw_url: &str,
    ) -> Result<Vec<u8>, ProwlarrError> {
        let url = Url::parse(raw_url).map_err(|_| ProwlarrError::UnsafeDownloadUrl)?;
        let api_path = self
            .base_url
            .join(&format!("api/v1/indexer/{indexer_id}/download"))
            .expect("fixed Prowlarr download path");
        let legacy_path = self
            .base_url
            .join(&format!("{indexer_id}/download"))
            .expect("fixed legacy Prowlarr download path");
        if !same_origin(&url, &self.base_url)
            || (url.path() != api_path.path() && url.path() != legacy_path.path())
        {
            return Err(ProwlarrError::UnsafeDownloadUrl);
        }
        let response = self.get(url).await?;
        if response.status().is_redirection() {
            return Err(ProwlarrError::RedirectRejected);
        }
        checked_body(response, self.max_torrent_bytes).await
    }

    async fn get(&self, url: Url) -> Result<reqwest::Response, ProwlarrError> {
        self.client
            .get(url)
            .header("X-Api-Key", self.api_key.clone())
            .send()
            .await
            .map_err(ProwlarrError::Request)
    }

    fn project(&self, resource: IndexerResource) -> Result<ProjectedIndexer, ProwlarrError> {
        let name = resource
            .name
            .filter(|name| !name.is_empty())
            .ok_or(ProwlarrError::MalformedField("indexer name"))?;
        let tags: BTreeSet<_> = resource.tags.unwrap_or_default().into_iter().collect();
        let reason = if !resource.enable {
            Some("disabled")
        } else if resource.protocol != "torrent" {
            Some("unsupported_protocol")
        } else if !resource.supports_search {
            Some("search_unsupported")
        } else if self.require_proxy_downloads && resource.redirect {
            Some("redirect_enabled")
        } else if tags.iter().any(|tag| self.exclude_tags.contains(tag)) {
            Some("excluded_tag")
        } else if !self.include_tags.is_empty()
            && !tags.iter().any(|tag| self.include_tags.contains(tag))
        {
            Some("missing_include_tag")
        } else if resource
            .status
            .as_ref()
            .and_then(|status| status.get("disabledTill"))
            .is_some_and(|value| !value.is_null())
        {
            Some("temporarily_disabled")
        } else {
            None
        };
        Ok(ProjectedIndexer {
            id: resource.id,
            name,
            protocol: resource.protocol,
            enabled: resource.enable,
            supports_search: resource.supports_search,
            redirect: resource.redirect,
            priority: resource.priority,
            tags: tags.into_iter().collect(),
            capabilities: resource
                .capabilities
                .unwrap_or_else(|| Value::Object(Default::default())),
            eligible: reason.is_none(),
            ineligible_reason: reason.map(str::to_owned),
            status: resource.status,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct IndexerResource {
    id: i64,
    name: Option<String>,
    #[serde(default)]
    enable: bool,
    #[serde(default)]
    redirect: bool,
    #[serde(default)]
    supports_search: bool,
    #[serde(default)]
    protocol: String,
    #[serde(default)]
    priority: i64,
    tags: Option<Vec<i64>>,
    capabilities: Option<Value>,
    status: Option<Value>,
}

pub(crate) struct ProjectedIndexer {
    id: i64,
    name: String,
    protocol: String,
    enabled: bool,
    supports_search: bool,
    redirect: bool,
    priority: i64,
    tags: Vec<i64>,
    capabilities: Value,
    eligible: bool,
    ineligible_reason: Option<String>,
    status: Option<Value>,
}

impl Storage {
    pub(crate) async fn project_indexers(
        &self,
        indexers: &[ProjectedIndexer],
        now: i64,
    ) -> Result<(), ProwlarrError> {
        let mut transaction = self.pool().begin().await?;
        sqlx::query("UPDATE sporos_indexer SET eligible = 0, ineligible_reason = 'not_observed'")
            .execute(&mut *transaction)
            .await?;
        for indexer in indexers {
            sqlx::query(
                "INSERT INTO sporos_indexer (
                    prowlarr_id, name, protocol, enabled, supports_search, redirect,
                    priority, tags_json, capabilities_json, eligible,
                    ineligible_reason, status_json, refreshed_at
                 ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                 ON CONFLICT(prowlarr_id) DO UPDATE SET name = excluded.name,
                    protocol = excluded.protocol, enabled = excluded.enabled,
                    supports_search = excluded.supports_search, redirect = excluded.redirect,
                    priority = excluded.priority, tags_json = excluded.tags_json,
                    capabilities_json = excluded.capabilities_json, eligible = excluded.eligible,
                    ineligible_reason = excluded.ineligible_reason,
                    status_json = excluded.status_json, refreshed_at = excluded.refreshed_at",
            )
            .bind(indexer.id)
            .bind(&indexer.name)
            .bind(&indexer.protocol)
            .bind(i64::from(indexer.enabled))
            .bind(i64::from(indexer.supports_search))
            .bind(i64::from(indexer.redirect))
            .bind(indexer.priority)
            .bind(serde_json::to_string(&indexer.tags)?)
            .bind(serde_json::to_string(&indexer.capabilities)?)
            .bind(i64::from(indexer.eligible))
            .bind(&indexer.ineligible_reason)
            .bind(
                indexer
                    .status
                    .as_ref()
                    .map(serde_json::to_string)
                    .transpose()?,
            )
            .bind(now)
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    pub(crate) async fn indexer_query(
        &self,
        indexer_id: i64,
        release: &ReleaseDescriptor,
    ) -> Result<Option<SearchQuery>, ProwlarrError> {
        let row = sqlx::query(
            "SELECT capabilities_json FROM sporos_indexer
             WHERE prowlarr_id = ? AND eligible = 1",
        )
        .bind(indexer_id)
        .fetch_optional(self.pool())
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let capabilities: Value =
            serde_json::from_str(&row.try_get::<String, _>("capabilities_json")?)?;
        Ok(SearchQuery::for_release(release, &capabilities))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SearchQuery {
    parameters: Vec<(String, String)>,
}

impl SearchQuery {
    fn for_release(release: &ReleaseDescriptor, capabilities: &Value) -> Option<Self> {
        let (kind, field) = match release.kind {
            VideoKind::Movie => ("movie", "movieSearchParams"),
            VideoKind::Episode
            | VideoKind::SeasonPack
            | VideoKind::DateEpisode
            | VideoKind::AbsoluteEpisode => ("tvsearch", "tvSearchParams"),
            _ => ("search", "searchParams"),
        };
        let supported = capabilities
            .get(field)
            .and_then(Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(Value::as_str)
                    .collect::<BTreeSet<_>>()
            })
            .unwrap_or_default();
        let external = release.arr_identity.as_ref().and_then(|identity| {
            [
                ("imdbid", identity.imdb_id.clone()),
                ("tvdbid", identity.tvdb_id.map(|value| value.to_string())),
                ("tmdbid", identity.tmdb_id.map(|value| value.to_string())),
            ]
            .into_iter()
            .find(|(name, value)| supported.contains(*name) && value.is_some())
            .and_then(|(name, value)| value.map(|value| (name.to_owned(), value)))
        });
        if external.is_none() && !supported.contains("q") {
            return None;
        }
        let mut parameters = vec![("t".to_owned(), kind.to_owned())];
        if let Some(external) = external {
            parameters.push(external);
        } else {
            parameters.push(("q".to_owned(), release.primary_title.as_str().to_owned()));
        }
        for (name, value) in [
            ("year", release.year.map(|value| value.to_string())),
            ("season", release.season.map(|value| value.to_string())),
            ("ep", release.episode.map(|value| value.to_string())),
        ] {
            if supported.contains(name)
                && let Some(value) = value
            {
                parameters.push((name.to_owned(), value));
            }
        }
        parameters.push(("extended".to_owned(), "1".to_owned()));
        Some(Self { parameters })
    }
}

fn same_origin(left: &Url, right: &Url) -> bool {
    left.scheme() == right.scheme()
        && left.host_str() == right.host_str()
        && left.port_or_known_default() == right.port_or_known_default()
}

async fn checked_body(response: reqwest::Response, limit: usize) -> Result<Vec<u8>, ProwlarrError> {
    let mut response = checked_response(response, limit)?;
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(ProwlarrError::Request)? {
        if body.len().saturating_add(chunk.len()) > limit {
            return Err(ProwlarrError::ResponseTooLarge(limit));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn checked_response(
    response: reqwest::Response,
    limit: usize,
) -> Result<reqwest::Response, ProwlarrError> {
    let status = response.status();
    if status == StatusCode::TOO_MANY_REQUESTS {
        return Err(ProwlarrError::RateLimited {
            retry_after: response
                .headers()
                .get(RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok())
                .map(Duration::from_secs),
        });
    }
    if !status.is_success() {
        return Err(ProwlarrError::HttpStatus(status));
    }
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err(ProwlarrError::ResponseTooLarge(limit));
    }
    Ok(response)
}

#[derive(Debug, Error)]
pub(crate) enum ProwlarrError {
    #[error("Prowlarr API key is not a valid HTTP header")]
    InvalidApiKey,
    #[error("could not construct the Prowlarr HTTP client")]
    Client(#[source] reqwest::Error),
    #[error("Prowlarr request failed")]
    Request(#[source] reqwest::Error),
    #[error("Prowlarr returned HTTP {0}")]
    HttpStatus(StatusCode),
    #[error("Prowlarr rate limited the request")]
    RateLimited { retry_after: Option<Duration> },
    #[error("Prowlarr response exceeded its {0}-byte limit")]
    ResponseTooLarge(usize),
    #[error("Prowlarr returned malformed {0}")]
    Malformed(&'static str, #[source] serde_json::Error),
    #[error("Prowlarr response omitted {0}")]
    MalformedField(&'static str),
    #[error("Prowlarr download URL is not a same-origin proxy URL")]
    UnsafeDownloadUrl,
    #[error("Prowlarr download redirect was rejected")]
    RedirectRejected,
    #[error("Prowlarr returned invalid Torznab XML")]
    Torznab(#[from] crate::torznab::TorznabParseError),
    #[error("Prowlarr projection database operation failed")]
    Database(#[from] sqlx::Error),
    #[error("Prowlarr projection data is invalid")]
    Json(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use sporos_model::{NormalizedTitle, ReleaseDescriptor};

    #[test]
    fn query_uses_only_advertised_capabilities() {
        let mut release = ReleaseDescriptor::unknown(NormalizedTitle::from_normalized("show"));
        release.kind = VideoKind::Episode;
        release.year = Some(2024);
        release.season = Some(2);
        release.episode = Some(3);
        let capabilities = serde_json::json!({
            "tvSearchParams": ["q", "season", "ep"]
        });

        let query = SearchQuery::for_release(&release, &capabilities).unwrap();

        assert_eq!(
            query.parameters,
            [
                ("t".to_owned(), "tvsearch".to_owned()),
                ("q".to_owned(), "show".to_owned()),
                ("season".to_owned(), "2".to_owned()),
                ("ep".to_owned(), "3".to_owned()),
                ("extended".to_owned(), "1".to_owned()),
            ]
        );
    }

    #[test]
    fn query_rejects_an_indexer_without_text_search() {
        let release = ReleaseDescriptor::unknown(NormalizedTitle::from_normalized("movie"));
        assert!(SearchQuery::for_release(&release, &serde_json::json!({})).is_none());
    }

    #[test]
    fn query_prefers_an_advertised_arr_identifier() {
        let mut release = ReleaseDescriptor::unknown(NormalizedTitle::from_normalized("show"));
        release.kind = VideoKind::Episode;
        release.arr_identity = Some(sporos_model::ArrIdentity {
            kind: sporos_model::ArrKind::Series,
            instance: "main".to_owned(),
            entity_id: 7,
            tvdb_id: Some(11),
            tmdb_id: None,
            imdb_id: None,
        });

        let query = SearchQuery::for_release(
            &release,
            &serde_json::json!({"tvSearchParams": ["q", "tvdbid"]}),
        )
        .unwrap();

        assert_eq!(
            query.parameters,
            [
                ("t".to_owned(), "tvsearch".to_owned()),
                ("tvdbid".to_owned(), "11".to_owned()),
                ("extended".to_owned(), "1".to_owned()),
            ]
        );
    }
}
