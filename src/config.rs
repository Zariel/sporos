use std::collections::BTreeMap;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::errors::ConfigError;

pub const DEFAULT_CONFIG_PATH: &str = "./config.toml";

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SporosConfig {
    pub paths: PathsConfig,
    pub server: ServerConfig,
    pub torrent_clients: BTreeMap<String, TorrentClientConfig>,
    pub indexers: IndexersConfig,
    pub matching: MatchingConfig,
    pub scheduling: SchedulingConfig,
}

#[derive(Debug, Clone, Eq, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PathsConfig {
    pub database: PathBuf,
    pub torrent_cache_dir: PathBuf,
    pub output_dir: PathBuf,
    pub media_dirs: Vec<PathBuf>,
}

impl Default for PathsConfig {
    fn default() -> Self {
        Self {
            database: PathBuf::from("state/sporos.db"),
            torrent_cache_dir: PathBuf::from("cache/torrents"),
            output_dir: PathBuf::from("output"),
            media_dirs: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServerConfig {
    pub bind: SocketAddr,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: SocketAddr::from(([127, 0, 0, 1], 2468)),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TorrentClientConfig {
    pub kind: ConfigTorrentClientKind,
    pub url: String,
    pub username: Option<String>,
    pub password_file: Option<PathBuf>,
    pub default_save_path: PathBuf,
    pub label_field: Option<String>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfigTorrentClientKind {
    Qbittorrent,
    Rtorrent,
}

#[derive(Debug, Clone, Default, Eq, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct IndexersConfig {
    pub default_timeouts: IndexerTimeoutsConfig,
    pub torznab: BTreeMap<String, TorznabIndexerConfig>,
}

#[derive(Debug, Clone, Eq, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct IndexerTimeoutsConfig {
    pub search: String,
    pub download: String,
}

impl Default for IndexerTimeoutsConfig {
    fn default() -> Self {
        Self {
            search: "120s".to_owned(),
            download: "30s".to_owned(),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TorznabIndexerConfig {
    pub url: String,
    pub api_key_file: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MatchingConfig {
    pub mode: MatchingMode,
    pub fuzzy_size_threshold: f64,
    pub include_single_episodes: bool,
    pub include_non_video: bool,
    pub season_from_episodes: f64,
}

impl Default for MatchingConfig {
    fn default() -> Self {
        Self {
            mode: MatchingMode::Partial,
            fuzzy_size_threshold: 0.02,
            include_single_episodes: false,
            include_non_video: false,
            season_from_episodes: 1.0,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MatchingMode {
    Exact,
    Partial,
}

#[derive(Debug, Clone, Eq, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SchedulingConfig {
    pub rss_interval: String,
    pub search_interval: String,
    pub indexer_caps_interval: String,
    pub cleanup_interval: String,
}

impl Default for SchedulingConfig {
    fn default() -> Self {
        Self {
            rss_interval: "30m".to_owned(),
            search_interval: "24h".to_owned(),
            indexer_caps_interval: "24h".to_owned(),
            cleanup_interval: "24h".to_owned(),
        }
    }
}

pub fn load_config(path: impl AsRef<Path>) -> Result<SporosConfig, ConfigError> {
    let path = path.as_ref();
    let contents = fs::read_to_string(path).map_err(|error| ConfigError::UnreadableFile {
        path: path.to_path_buf(),
        message: error.to_string(),
    })?;

    parse_config(&contents)
}

pub fn parse_config(contents: &str) -> Result<SporosConfig, ConfigError> {
    toml::from_str(contents).map_err(|error| ConfigError::InvalidField {
        field: "config",
        reason: error.to_string(),
    })
}

pub const CONFIG_SCHEMA: &str = r#"sporos config schema

[paths]
database = "path"
torrent_cache_dir = "path"
output_dir = "path"
media_dirs = ["path", "..."]

[server]
bind = "127.0.0.1:2468"

[torrent_clients.<name>]
kind = "qbittorrent|rtorrent"
url = "http://client.example"
username = "optional"
password_file = "optional path"
default_save_path = "path"
label_field = "optional rtorrent custom field"

[indexers.default_timeouts]
search = "120s"
download = "30s"

[indexers.torznab.<name>]
url = "https://indexer.example/api"
api_key_file = "optional path"

[matching]
mode = "exact|partial"
fuzzy_size_threshold = 0.02
include_single_episodes = false
include_non_video = false
season_from_episodes = 1.0

[scheduling]
rss_interval = "30m"
search_interval = "24h"
indexer_caps_interval = "24h"
cleanup_interval = "24h"
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_typed_toml_config() {
        let config = parse_config(
            r#"
            [paths]
            database = "/data/state/sporos.db"
            torrent_cache_dir = "/data/cache/torrents"
            output_dir = "/data/output"
            media_dirs = ["/media/movies"]

            [server]
            bind = "0.0.0.0:2468"

            [torrent_clients.qbit_main]
            kind = "qbittorrent"
            url = "http://qbittorrent:8080"
            username = "sporos"
            password_file = "/var/run/secrets/qbit-password"
            default_save_path = "/downloads"

            [indexers.torznab.main]
            url = "https://indexer.example/api"
            api_key_file = "/var/run/secrets/indexer-api-key"
            "#,
        )
        .unwrap();

        assert_eq!(
            "0.0.0.0:2468".parse::<SocketAddr>().unwrap(),
            config.server.bind
        );
        assert_eq!(1, config.torrent_clients.len());
        assert_eq!(1, config.indexers.torznab.len());
    }

    #[test]
    fn rejects_unsupported_compatibility_keys() {
        let error = parse_config(
            r#"
            [paths]
            base_dir = "/hidden"
            "#,
        )
        .unwrap_err();

        assert!(error.to_string().contains("unknown field"));
        assert!(error.to_string().contains("base_dir"));
    }

    #[test]
    fn schema_documents_sporos_native_surface() {
        assert!(CONFIG_SCHEMA.contains("sporos config schema"));
        assert!(CONFIG_SCHEMA.contains("[torrent_clients.<name>]"));
        assert!(CONFIG_SCHEMA.contains("[indexers.torznab.<name>]"));
        assert!(!CONFIG_SCHEMA.contains("SPOROS"));
    }
}
