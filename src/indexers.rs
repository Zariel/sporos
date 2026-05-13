use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::Path;

use quick_xml::Reader;
use quick_xml::events::{BytesStart, Event};
use quick_xml::name::QName;
use serde::{Deserialize, Serialize};

use crate::config::{IndexersConfig, TorznabIndexerConfig};
use crate::domain::{DependencyName, MediaType};

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

#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
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
pub struct SearchCaps {
    pub generic_search: bool,
    pub tv_search: bool,
    pub movie_search: bool,
    pub audio_search: bool,
    pub supported_id_params: BTreeSet<String>,
}

#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
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
    if !caps.search.generic_search && !caps.search.tv_search && !caps.search.movie_search {
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
    if available {
        if let Some(params) = attribute_value(reader, element, b"supportedParams")? {
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
        assert!(caps.search.supported_id_params.contains("tvdbid"));
        assert!(caps.categories.movie);
        assert!(caps.categories.additional);
        assert!(caps.supports_media_type(MediaType::Episode));
        assert!(caps.supports_media_type(MediaType::Movie));
        assert!(caps.supports_media_type(MediaType::Audio));
    }

    #[test]
    fn caps_parser_defaults_limits_and_rejects_unsupported_search() {
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

        assert_eq!(TorznabCapsError::UnsupportedSearch, error);
    }

    #[test]
    fn caps_parser_rejects_bad_xml() {
        let error = parse_torznab_caps("<caps><").unwrap_err();

        assert!(matches!(error, TorznabCapsError::InvalidXml { .. }));
    }
}
