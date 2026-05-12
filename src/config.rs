use std::collections::BTreeMap;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::Deserialize;
use toml::Value;

use crate::announce::AnnounceQueueConfig;
use crate::errors::ConfigError;

pub const DEFAULT_CONFIG_PATH: &str = "./config.toml";
static WRITE_PROBE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SporosConfig {
    pub paths: PathsConfig,
    pub server: ServerConfig,
    pub torrent_clients: BTreeMap<String, TorrentClientConfig>,
    pub indexers: IndexersConfig,
    pub matching: MatchingConfig,
    pub inventory: InventoryConfig,
    pub scheduling: SchedulingConfig,
    pub announce: AnnounceQueueConfig,
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

#[derive(Debug, Clone, Copy, Eq, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct InventoryConfig {
    pub media_scan_max_depth: u16,
}

impl Default for InventoryConfig {
    fn default() -> Self {
        Self {
            media_scan_max_depth: 3,
        }
    }
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

    let cwd = std::env::current_dir().map_err(|error| ConfigError::InvalidField {
        field: "paths",
        reason: format!("cannot resolve current working directory: {error}"),
    })?;

    parse_startup_config(&contents, cwd)
}

pub fn parse_config(contents: &str) -> Result<SporosConfig, ConfigError> {
    let config: SporosConfig =
        toml::from_str(contents).map_err(|error| ConfigError::InvalidField {
            field: "config",
            reason: error.to_string(),
        })?;

    config
        .announce
        .validate()
        .map_err(|error| ConfigError::InvalidField {
            field: "announce",
            reason: error.to_string(),
        })?;

    Ok(config)
}

pub fn parse_startup_config(
    contents: &str,
    cwd: impl AsRef<Path>,
) -> Result<SporosConfig, ConfigError> {
    let raw: Value = toml::from_str(contents).map_err(|error| ConfigError::InvalidField {
        field: "config",
        reason: error.to_string(),
    })?;
    let supplied_paths = SuppliedPaths::from_toml(&raw);
    let mut config = parse_config(contents)?;

    config.paths.resolve(cwd.as_ref(), supplied_paths)?;
    config.paths.prepare_local_state()?;
    config.paths.validate_media_dirs()?;

    Ok(config)
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
struct SuppliedPaths {
    database: bool,
    torrent_cache_dir: bool,
    output_dir: bool,
    media_dirs: bool,
}

impl SuppliedPaths {
    fn from_toml(raw: &Value) -> Self {
        let Some(paths) = raw.get("paths").and_then(Value::as_table) else {
            return Self::default();
        };

        Self {
            database: paths.contains_key("database"),
            torrent_cache_dir: paths.contains_key("torrent_cache_dir"),
            output_dir: paths.contains_key("output_dir"),
            media_dirs: paths.contains_key("media_dirs"),
        }
    }
}

impl PathsConfig {
    fn resolve(&mut self, cwd: &Path, supplied: SuppliedPaths) -> Result<(), ConfigError> {
        self.database = resolve_path("paths.database", &self.database, cwd, supplied.database)?;
        self.torrent_cache_dir = resolve_path(
            "paths.torrent_cache_dir",
            &self.torrent_cache_dir,
            cwd,
            supplied.torrent_cache_dir,
        )?;
        self.output_dir = resolve_path(
            "paths.output_dir",
            &self.output_dir,
            cwd,
            supplied.output_dir,
        )?;

        if supplied.media_dirs {
            for media_dir in &self.media_dirs {
                reject_relative_operator_path("paths.media_dirs", media_dir)?;
            }
        }

        self.media_dirs = self
            .media_dirs
            .iter()
            .map(|path| absolutize(path, cwd))
            .collect();

        Ok(())
    }

    fn prepare_local_state(&mut self) -> Result<(), ConfigError> {
        self.database = prepare_database_path(&self.database)?;
        self.torrent_cache_dir =
            prepare_directory("paths.torrent_cache_dir", &self.torrent_cache_dir)?;
        self.output_dir = prepare_directory("paths.output_dir", &self.output_dir)?;

        Ok(())
    }

    fn validate_media_dirs(&mut self) -> Result<(), ConfigError> {
        let mut validated = Vec::with_capacity(self.media_dirs.len());
        for media_dir in &self.media_dirs {
            let absolute = media_dir.canonicalize().map_err(|error| {
                path_error(
                    "paths.media_dirs",
                    format!("cannot resolve {}: {error}", media_dir.display()),
                )
            })?;
            let metadata = absolute.metadata().map_err(|error| {
                path_error(
                    "paths.media_dirs",
                    format!("cannot inspect {}: {error}", absolute.display()),
                )
            })?;

            if !metadata.is_dir() {
                return Err(path_error(
                    "paths.media_dirs",
                    format!("{} is not a directory", absolute.display()),
                ));
            }

            fs::read_dir(&absolute).map_err(|error| {
                path_error(
                    "paths.media_dirs",
                    format!("cannot read {}: {error}", absolute.display()),
                )
            })?;

            validated.push(absolute);
        }
        self.media_dirs = validated;

        Ok(())
    }
}

fn resolve_path(
    field: &'static str,
    path: &Path,
    cwd: &Path,
    operator_supplied: bool,
) -> Result<PathBuf, ConfigError> {
    if operator_supplied {
        reject_relative_operator_path(field, path)?;
    }

    Ok(absolutize(path, cwd))
}

fn reject_relative_operator_path(field: &'static str, path: &Path) -> Result<(), ConfigError> {
    if path.is_absolute() {
        return Ok(());
    }

    Err(path_error(
        field,
        format!(
            "operator-supplied filesystem paths must be absolute: {}",
            path.display()
        ),
    ))
}

fn absolutize(path: &Path, cwd: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        cwd.join(path)
    }
}

fn prepare_database_path(path: &Path) -> Result<PathBuf, ConfigError> {
    let parent = path.parent().ok_or_else(|| {
        path_error(
            "paths.database",
            format!("database path has no parent directory: {}", path.display()),
        )
    })?;
    let file_name = path.file_name().ok_or_else(|| {
        path_error(
            "paths.database",
            format!("database path has no file name: {}", path.display()),
        )
    })?;

    create_dir("paths.database", parent)?;
    let parent = parent.canonicalize().map_err(|error| {
        path_error(
            "paths.database",
            format!("cannot resolve {}: {error}", parent.display()),
        )
    })?;
    ensure_directory_writable("paths.database", &parent)?;

    Ok(parent.join(file_name))
}

fn prepare_directory(field: &'static str, path: &Path) -> Result<PathBuf, ConfigError> {
    create_dir(field, path)?;
    let path = path.canonicalize().map_err(|error| {
        path_error(field, format!("cannot resolve {}: {error}", path.display()))
    })?;

    let metadata = path.metadata().map_err(|error| {
        path_error(field, format!("cannot inspect {}: {error}", path.display()))
    })?;
    if !metadata.is_dir() {
        return Err(path_error(
            field,
            format!("{} is not a directory", path.display()),
        ));
    }

    ensure_directory_writable(field, &path)?;

    Ok(path)
}

fn create_dir(field: &'static str, path: &Path) -> Result<(), ConfigError> {
    fs::create_dir_all(path)
        .map_err(|error| path_error(field, format!("cannot create {}: {error}", path.display())))
}

fn ensure_directory_writable(field: &'static str, path: &Path) -> Result<(), ConfigError> {
    let probe_id = WRITE_PROBE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let probe = path.join(format!(
        "sporos-write-test-{}-{probe_id}",
        std::process::id()
    ));

    fs::write(&probe, b"")
        .map_err(|error| path_error(field, format!("cannot write {}: {error}", probe.display())))?;
    fs::remove_file(&probe).map_err(|error| {
        path_error(field, format!("cannot remove {}: {error}", probe.display()))
    })?;

    Ok(())
}

fn path_error(field: &'static str, reason: impl Into<String>) -> ConfigError {
    ConfigError::InvalidField {
        field,
        reason: reason.into(),
    }
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

[inventory]
media_scan_max_depth = 3

[scheduling]
rss_interval = "30m"
search_interval = "24h"
indexer_caps_interval = "24h"
cleanup_interval = "24h"

[announce]
max_pending = 1000
worker_concurrency = 2
claim_batch_size = 10
lease_duration_secs = 300
lease_renewal_secs = 120
default_ttl_secs = 86400
retry_initial_delay_secs = 30
retry_max_delay_secs = 3600
retry_jitter_ratio = 0.2
success_retention_secs = 604800
failure_retention_secs = 1209600
"#;

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

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
    fn startup_config_resolves_default_paths_under_cwd() {
        let cwd = unique_temp_dir("defaults");
        let config = parse_startup_config("", &cwd).unwrap();
        let cwd = cwd.canonicalize().unwrap();

        assert_eq!(cwd.join("state/sporos.db"), config.paths.database);
        assert_eq!(cwd.join("cache/torrents"), config.paths.torrent_cache_dir);
        assert_eq!(cwd.join("output"), config.paths.output_dir);
        assert!(config.paths.database.parent().unwrap().is_dir());
        assert!(config.paths.torrent_cache_dir.is_dir());
        assert!(config.paths.output_dir.is_dir());

        fs::remove_dir_all(cwd).unwrap();
    }

    #[test]
    fn startup_config_rejects_relative_operator_state_paths() {
        let cwd = unique_temp_dir("relative-state");
        let error = parse_startup_config(
            r#"
            [paths]
            database = "state/sporos.db"
            "#,
            &cwd,
        )
        .unwrap_err();

        assert!(error.to_string().contains("paths.database"));
        assert!(error.to_string().contains("must be absolute"));

        fs::remove_dir_all(cwd).unwrap();
    }

    #[test]
    fn startup_config_creates_configured_state_directories() {
        let cwd = unique_temp_dir("configured-state");
        let database = cwd.join("state/nested/sporos.db");
        let torrent_cache_dir = cwd.join("cache/torrents");
        let output_dir = cwd.join("output");
        let contents = format!(
            r#"
            [paths]
            database = "{}"
            torrent_cache_dir = "{}"
            output_dir = "{}"
            "#,
            database.display(),
            torrent_cache_dir.display(),
            output_dir.display()
        );

        let config = parse_startup_config(&contents, &cwd).unwrap();
        let expected_database = database
            .parent()
            .unwrap()
            .canonicalize()
            .unwrap()
            .join("sporos.db");

        assert_eq!(expected_database, config.paths.database);
        assert!(database.parent().unwrap().is_dir());
        assert!(config.paths.torrent_cache_dir.is_dir());
        assert!(config.paths.output_dir.is_dir());

        fs::remove_dir_all(cwd).unwrap();
    }

    #[test]
    fn startup_config_validates_media_dirs_without_creating_them() {
        let cwd = unique_temp_dir("media");
        let media_dir = cwd.join("movies");
        fs::create_dir(&media_dir).unwrap();
        let missing_dir = cwd.join("missing");
        let contents = format!(
            r#"
            [paths]
            media_dirs = ["{}", "{}"]
            "#,
            media_dir.display(),
            missing_dir.display()
        );

        let error = parse_startup_config(&contents, &cwd).unwrap_err();

        assert!(error.to_string().contains("paths.media_dirs"));
        assert!(!missing_dir.exists());

        fs::remove_dir_all(cwd).unwrap();
    }

    #[test]
    fn startup_config_rejects_relative_media_dirs() {
        let cwd = unique_temp_dir("relative-media");
        let error = parse_startup_config(
            r#"
            [paths]
            media_dirs = ["media/movies"]
            "#,
            &cwd,
        )
        .unwrap_err();

        assert!(error.to_string().contains("paths.media_dirs"));
        assert!(error.to_string().contains("must be absolute"));

        fs::remove_dir_all(cwd).unwrap();
    }

    #[test]
    fn schema_documents_sporos_native_surface() {
        assert!(CONFIG_SCHEMA.contains("sporos config schema"));
        assert!(CONFIG_SCHEMA.contains("[torrent_clients.<name>]"));
        assert!(CONFIG_SCHEMA.contains("[indexers.torznab.<name>]"));
        assert!(CONFIG_SCHEMA.contains("[inventory]"));
        assert!(!CONFIG_SCHEMA.contains("SPOROS"));
    }

    fn unique_temp_dir(label: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("sporos-config-test-{label}-{nanos}"));
        fs::create_dir_all(&path).unwrap();
        path
    }
}
