use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::Path;

use crate::config::{IndexersConfig, TorznabIndexerConfig};
use crate::domain::DependencyName;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ConfiguredTorznabIndexer {
    pub name: DependencyName,
    pub url: SanitizedTorznabUrl,
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

fn configured_torznab_indexer(
    name: &str,
    config: &TorznabIndexerConfig,
) -> Result<ConfiguredTorznabIndexer, IndexerConfigError> {
    let name =
        DependencyName::new(name.to_owned()).map_err(|error| IndexerConfigError::InvalidName {
            message: error.to_string(),
        })?;
    Ok(ConfiguredTorznabIndexer {
        name,
        url: SanitizedTorznabUrl::new(config.url.clone())?,
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
    } else if url_has_apikey_query(&config.url) {
        ApiKeySource::UrlQuery
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

fn display_path(path: &Path) -> String {
    path.to_string_lossy().into_owned()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{IndexerTimeoutsConfig, IndexersConfig};
    use crate::secrets::ApiKey;

    #[test]
    fn registry_sanitizes_urls_and_tracks_secret_sources() {
        let mut torznab = BTreeMap::new();
        torznab.insert(
            "main".to_owned(),
            TorznabIndexerConfig {
                url: "https://indexer.example/api?apikey=secret&t=caps".to_owned(),
                api_key: None,
                api_key_file: None,
                api_key_env: None,
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
        };

        let registry = TorznabRegistry::from_config(&config).unwrap();

        assert_eq!(2, registry.indexers().len());
        let main = registry
            .indexers()
            .iter()
            .find(|indexer| indexer.name.as_str() == "main")
            .unwrap();
        assert_eq!("https://indexer.example/api", main.url.as_str());
        assert_eq!(ApiKeySource::UrlQuery, main.api_key_source);
        assert!(!format!("{registry:?}").contains("secret"));
    }

    #[test]
    fn registry_rejects_duplicate_sanitized_urls() {
        let mut torznab = BTreeMap::new();
        for name in ["one", "two"] {
            torznab.insert(
                name.to_owned(),
                TorznabIndexerConfig {
                    url: "https://indexer.example/api?apikey=secret".to_owned(),
                    api_key: None,
                    api_key_file: None,
                    api_key_env: None,
                },
            );
        }
        let config = IndexersConfig {
            default_timeouts: IndexerTimeoutsConfig::default(),
            torznab,
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
}
