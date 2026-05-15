use std::collections::BTreeMap;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::Deserialize;
use toml::Value;

use crate::announce::AnnounceQueueConfig;
use crate::errors::ConfigError;
use crate::secrets::{ApiKey, ApiToken, Password};

pub const DEFAULT_CONFIG_PATH: &str = "./config.toml";
const ENV_PREFIX: &str = "SPOROS__";
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

#[derive(Debug, Clone, Eq, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServerConfig {
    pub bind: SocketAddr,
    pub api_token: Option<ApiToken>,
    pub api_token_file: Option<PathBuf>,
    pub api_token_env: Option<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: SocketAddr::from(([127, 0, 0, 1], 2468)),
            api_token: None,
            api_token_file: None,
            api_token_env: None,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TorrentClientConfig {
    pub kind: ConfigTorrentClientKind,
    pub url: String,
    pub username: Option<String>,
    pub password: Option<Password>,
    pub password_file: Option<PathBuf>,
    pub password_env: Option<String>,
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
    pub api_key: Option<ApiKey>,
    pub api_key_file: Option<PathBuf>,
    pub api_key_env: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MatchingConfig {
    pub mode: MatchingMode,
    pub fuzzy_size_threshold: f64,
    pub include_single_episodes: bool,
    pub include_non_video: bool,
    pub season_from_episodes: f64,
    pub recent_search_cooldown_secs: Option<u64>,
    pub first_search_window_secs: Option<u64>,
}

impl Default for MatchingConfig {
    fn default() -> Self {
        Self {
            mode: MatchingMode::Partial,
            fuzzy_size_threshold: 0.02,
            include_single_episodes: false,
            include_non_video: false,
            season_from_episodes: 1.0,
            recent_search_cooldown_secs: Some(3 * 24 * 60 * 60),
            first_search_window_secs: Some(7 * 24 * 60 * 60),
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
    pub saved_retry_interval: String,
    pub cleanup_interval: String,
}

impl Default for SchedulingConfig {
    fn default() -> Self {
        Self {
            rss_interval: "30m".to_owned(),
            search_interval: "24h".to_owned(),
            indexer_caps_interval: "24h".to_owned(),
            saved_retry_interval: "30m".to_owned(),
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

    parse_startup_config_with_env(&contents, cwd, std::env::vars())
}

pub fn parse_config(contents: &str) -> Result<SporosConfig, ConfigError> {
    parse_config_with_env(contents, std::iter::empty::<(String, String)>())
}

pub fn parse_config_with_env<I>(contents: &str, env: I) -> Result<SporosConfig, ConfigError>
where
    I: IntoIterator<Item = (String, String)>,
{
    let (config, _raw) = parse_config_value(contents, env)?;
    Ok(config)
}

pub fn parse_startup_config(
    contents: &str,
    cwd: impl AsRef<Path>,
) -> Result<SporosConfig, ConfigError> {
    parse_startup_config_with_env(contents, cwd, std::iter::empty::<(String, String)>())
}

pub fn parse_startup_config_with_env<I>(
    contents: &str,
    cwd: impl AsRef<Path>,
    env: I,
) -> Result<SporosConfig, ConfigError>
where
    I: IntoIterator<Item = (String, String)>,
{
    let (mut config, raw) = parse_config_value(contents, env)?;
    let supplied_paths = SuppliedPaths::from_toml(&raw);

    config.paths.resolve(cwd.as_ref(), supplied_paths)?;
    config.paths.prepare_local_state()?;
    config.paths.validate_media_dirs()?;
    resolve_secret_files(&mut config)?;
    validate_server_auth(&config)?;

    Ok(config)
}

fn parse_config_value<I>(contents: &str, env: I) -> Result<(SporosConfig, Value), ConfigError>
where
    I: IntoIterator<Item = (String, String)>,
{
    let env = env.into_iter().collect::<BTreeMap<_, _>>();
    let mut raw = parse_raw_config(contents)?;
    apply_env_overrides(&mut raw, &env)?;
    let mut config: SporosConfig =
        raw.clone()
            .try_into()
            .map_err(|error: toml::de::Error| ConfigError::InvalidField {
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
    resolve_secret_env(&mut config, &env)?;

    Ok((config, raw))
}

fn parse_raw_config(contents: &str) -> Result<Value, ConfigError> {
    if contents.trim().is_empty() {
        return Ok(Value::Table(toml::Table::new()));
    }

    toml::from_str(contents).map_err(|error| ConfigError::InvalidField {
        field: "config",
        reason: error.to_string(),
    })
}

fn apply_env_overrides(raw: &mut Value, env: &BTreeMap<String, String>) -> Result<(), ConfigError> {
    for (key, value) in env {
        let Some(suffix) = key.strip_prefix(ENV_PREFIX) else {
            continue;
        };
        let path = env_key_path(key, suffix)?;
        reject_array_env_path(key, &path)?;
        let value = parse_env_scalar(key, value)?;
        insert_env_value(raw, &path, value, key)?;
    }

    Ok(())
}

fn env_key_path(key: &str, suffix: &str) -> Result<Vec<String>, ConfigError> {
    if suffix.is_empty() {
        return Err(env_error(key, "missing config path after SPOROS__"));
    }

    suffix
        .split("__")
        .map(|segment| env_segment_to_key(key, segment))
        .collect()
}

fn env_segment_to_key(key: &str, segment: &str) -> Result<String, ConfigError> {
    if segment.is_empty() {
        return Err(env_error(key, "empty path segment"));
    }
    if segment.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(env_error(key, "indexed env overrides are not supported"));
    }
    if !segment
        .bytes()
        .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(env_error(
            key,
            "path segments must be uppercase ASCII, digits, or underscores",
        ));
    }

    Ok(segment.to_ascii_lowercase())
}

fn reject_array_env_path(key: &str, path: &[String]) -> Result<(), ConfigError> {
    if path == ["paths", "media_dirs"] {
        return Err(env_error(
            key,
            "array config values are not settable through env",
        ));
    }

    Ok(())
}

fn parse_env_scalar(key: &str, value: &str) -> Result<Value, ConfigError> {
    let document = format!("value = {value}");
    let parsed = toml::from_str::<Value>(&document)
        .ok()
        .and_then(|value| value.get("value").cloned())
        .unwrap_or_else(|| Value::String(value.to_owned()));

    match parsed {
        Value::Array(_) | Value::Table(_) => {
            Err(env_error(key, "env overrides must be scalar values"))
        }
        value => Ok(value),
    }
}

fn insert_env_value(
    current: &mut Value,
    path: &[String],
    value: Value,
    key: &str,
) -> Result<(), ConfigError> {
    let Some((segment, rest)) = path.split_first() else {
        return Err(env_error(key, "missing config path"));
    };
    let Value::Table(table) = current else {
        return Err(env_error(
            key,
            "cannot override inside a scalar config value",
        ));
    };

    if rest.is_empty() {
        if matches!(table.get(segment), Some(Value::Array(_))) {
            return Err(env_error(
                key,
                "array config values are not settable through env",
            ));
        }
        table.insert(segment.clone(), value);
        return Ok(());
    }

    let child = table
        .entry(segment.clone())
        .or_insert_with(|| Value::Table(toml::Table::new()));
    if matches!(child, Value::Array(_)) {
        return Err(env_error(key, "indexed env overrides are not supported"));
    }

    insert_env_value(child, rest, value, key)
}

fn resolve_secret_env(
    config: &mut SporosConfig,
    env: &BTreeMap<String, String>,
) -> Result<(), ConfigError> {
    if config.server.api_token.is_none() {
        if let Some(env_name) = nonempty_secret_env(
            "server.api_token_env",
            "server",
            &config.server.api_token_env,
        )? {
            let value = secret_env_value(env, env_name, "server.api_token_env", "server")?;
            config.server.api_token = Some(
                ApiToken::new(value.clone())
                    .map_err(|source| ConfigError::InvalidSecret { source })?,
            );
        }
    }
    for (name, client) in &mut config.torrent_clients {
        if client.password.is_none() {
            if let Some(env_name) =
                nonempty_secret_env("torrent_clients.password_env", name, &client.password_env)?
            {
                let value = secret_env_value(env, env_name, "torrent_clients.password_env", name)?;
                client.password = Some(
                    Password::new(value.clone())
                        .map_err(|source| ConfigError::InvalidSecret { source })?,
                );
            }
        }
    }
    for (name, indexer) in &mut config.indexers.torznab {
        if indexer.api_key.is_none() {
            if let Some(env_name) =
                nonempty_secret_env("indexers.torznab.api_key_env", name, &indexer.api_key_env)?
            {
                let value = secret_env_value(env, env_name, "indexers.torznab.api_key_env", name)?;
                indexer.api_key = Some(
                    ApiKey::new(value.clone())
                        .map_err(|source| ConfigError::InvalidSecret { source })?,
                );
            }
        }
    }

    Ok(())
}

fn resolve_secret_files(config: &mut SporosConfig) -> Result<(), ConfigError> {
    if config.server.api_token.is_none() {
        if let Some(path) = &config.server.api_token_file {
            let value = secret_file_value("server.api_token_file", "server", path)?;
            config.server.api_token =
                Some(ApiToken::new(value).map_err(|source| ConfigError::InvalidSecret { source })?);
        }
    }
    for (name, client) in &mut config.torrent_clients {
        if client.password.is_none() {
            if let Some(path) = &client.password_file {
                let value = secret_file_value("torrent_clients.password_file", name, path)?;
                client.password = Some(
                    Password::new(value).map_err(|source| ConfigError::InvalidSecret { source })?,
                );
            }
        }
    }
    for (name, indexer) in &mut config.indexers.torznab {
        if indexer.api_key.is_none() {
            if let Some(path) = &indexer.api_key_file {
                let value = secret_file_value("indexers.torznab.api_key_file", name, path)?;
                indexer.api_key = Some(
                    ApiKey::new(value).map_err(|source| ConfigError::InvalidSecret { source })?,
                );
            }
        }
    }

    Ok(())
}

pub(crate) fn validate_server_auth(config: &SporosConfig) -> Result<(), ConfigError> {
    if config.server.bind.ip().is_loopback() || config.server.api_token.is_some() {
        return Ok(());
    }

    Err(ConfigError::InvalidField {
        field: "server.api_token",
        reason: format!(
            "non-loopback bind {} requires api_token, api_token_file, or api_token_env",
            config.server.bind
        ),
    })
}

fn secret_file_value(field: &'static str, name: &str, path: &Path) -> Result<String, ConfigError> {
    let value = fs::read_to_string(path).map_err(|error| ConfigError::UnreadableSecretFile {
        field,
        path: path.to_path_buf(),
        message: format!("{name}: {error}"),
    })?;

    Ok(value.trim_end_matches(['\r', '\n']).to_owned())
}

fn nonempty_secret_env<'a>(
    field: &'static str,
    name: &str,
    value: &'a Option<String>,
) -> Result<Option<&'a str>, ConfigError> {
    let Some(value) = value.as_deref() else {
        return Ok(None);
    };
    if value.trim().is_empty() {
        return Err(ConfigError::InvalidField {
            field,
            reason: format!("{name} references an empty environment variable name"),
        });
    }

    Ok(Some(value))
}

fn secret_env_value<'a>(
    env: &'a BTreeMap<String, String>,
    env_name: &str,
    field: &'static str,
    config_name: &str,
) -> Result<&'a String, ConfigError> {
    env.get(env_name).ok_or_else(|| ConfigError::InvalidField {
        field,
        reason: format!("{config_name} references unset environment variable {env_name}"),
    })
}

fn env_error(key: &str, reason: impl Into<String>) -> ConfigError {
    ConfigError::InvalidField {
        field: "environment",
        reason: format!("{key}: {}", reason.into()),
    }
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
api_token = "optional local-development bearer token"
api_token_file = "optional path"
api_token_env = "optional env var containing bearer token"

[torrent_clients.<name>]
kind = "qbittorrent|rtorrent"
url = "http://client.example"
username = "optional"
password = "optional local-development secret"
password_file = "optional path"
password_env = "optional env var containing password"
default_save_path = "path"
label_field = "optional rtorrent custom field"

[indexers.default_timeouts]
search = "120s"
download = "30s"

[indexers.torznab.<name>]
url = "https://indexer.example/api"
api_key = "optional local-development secret"
api_key_file = "optional path"
api_key_env = "optional env var containing api key"

[environment overrides]
SPOROS__SERVER__BIND = "0.0.0.0:2468"
SPOROS__SERVER__API_TOKEN_FILE = "/var/run/secrets/sporos-api-token"
SPOROS__PATHS__DATABASE = "/data/state/sporos.db"
SPOROS__MATCHING__FUZZY_SIZE_THRESHOLD = "0.02"
SPOROS__TORRENT_CLIENTS__QBIT_MAIN__URL = "http://qbittorrent:8080"
SPOROS__TORRENT_CLIENTS__QBIT_MAIN__PASSWORD_FILE = "/var/run/secrets/qbit-password"
SPOROS__INDEXERS__TORZNAB__EXAMPLE__API_KEY_FILE = "/var/run/secrets/indexer-api-key"

[matching]
mode = "exact|partial"
fuzzy_size_threshold = 0.02
include_single_episodes = false
include_non_video = false
season_from_episodes = 1.0
recent_search_cooldown_secs = 259200
first_search_window_secs = 604800

[inventory]
media_scan_max_depth = 3

[scheduling]
rss_interval = "30m"
search_interval = "24h"
indexer_caps_interval = "24h"
saved_retry_interval = "30m"
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
    use std::path::{Path, PathBuf};

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
        assert_eq!(
            Some("/var/run/secrets/qbit-password"),
            config
                .torrent_clients
                .get("qbit_main")
                .and_then(|client| client.password_file.as_deref())
                .and_then(Path::to_str)
        );
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
    fn startup_config_env_overrides_resolve_container_style_paths() {
        let cwd = unique_temp_dir("env-container");
        let database = cwd.join("data/state/sporos.db");
        let torrent_cache_dir = cwd.join("data/cache/torrents");
        let output_dir = cwd.join("data/output");

        let config = parse_startup_config_with_env(
            "",
            &cwd,
            vec![
                (
                    "SPOROS__PATHS__DATABASE".to_owned(),
                    database.display().to_string(),
                ),
                (
                    "SPOROS__PATHS__TORRENT_CACHE_DIR".to_owned(),
                    torrent_cache_dir.display().to_string(),
                ),
                (
                    "SPOROS__PATHS__OUTPUT_DIR".to_owned(),
                    output_dir.display().to_string(),
                ),
            ],
        )
        .unwrap();

        assert_eq!(
            database
                .parent()
                .unwrap()
                .canonicalize()
                .unwrap()
                .join("sporos.db"),
            config.paths.database
        );
        assert!(config.paths.torrent_cache_dir.is_dir());
        assert!(config.paths.output_dir.is_dir());

        fs::remove_dir_all(cwd).unwrap();
    }

    #[test]
    fn startup_config_rejects_relative_env_paths() {
        let cwd = unique_temp_dir("relative-env");
        let error = parse_startup_config_with_env(
            "",
            &cwd,
            vec![(
                "SPOROS__PATHS__DATABASE".to_owned(),
                "state/sporos.db".to_owned(),
            )],
        )
        .unwrap_err();

        assert!(error.to_string().contains("paths.database"));
        assert!(error.to_string().contains("must be absolute"));

        fs::remove_dir_all(cwd).unwrap();
    }

    #[test]
    fn startup_config_rejects_external_bind_without_api_token() {
        let cwd = unique_temp_dir("external-bind");
        let contents = format!(
            r#"
            [paths]
            database = "{}/state/sporos.db"
            torrent_cache_dir = "{}/cache/torrents"
            output_dir = "{}/output"

            [server]
            bind = "0.0.0.0:2468"
            "#,
            cwd.display(),
            cwd.display(),
            cwd.display()
        );

        let error = parse_startup_config(&contents, &cwd).unwrap_err();

        assert!(error.to_string().contains("server.api_token"));
        assert!(error.to_string().contains("non-loopback bind"));

        fs::remove_dir_all(cwd).unwrap();
    }

    #[test]
    fn startup_config_loads_secret_file_values() {
        let cwd = unique_temp_dir("secret-files");
        let api_token_file = cwd.join("api-token");
        let password_file = cwd.join("qbit-password");
        let api_key_file = cwd.join("indexer-api-key");
        fs::write(&api_token_file, "server-secret\n").unwrap();
        fs::write(&password_file, "super-secret\n").unwrap();
        fs::write(&api_key_file, "api-secret\r\n").unwrap();
        let contents = format!(
            r#"
            [paths]
            database = "{}/state/sporos.db"
            torrent_cache_dir = "{}/cache/torrents"
            output_dir = "{}/output"

            [server]
            bind = "0.0.0.0:2468"
            api_token_file = "{}"

            [torrent_clients.qbit_main]
            kind = "qbittorrent"
            url = "http://qbittorrent:8080"
            password_file = "{}"
            default_save_path = "/downloads"

            [indexers.torznab.example]
            url = "https://indexer.example/api"
            api_key_file = "{}"
            "#,
            cwd.display(),
            cwd.display(),
            cwd.display(),
            api_token_file.display(),
            password_file.display(),
            api_key_file.display()
        );

        let config = parse_startup_config(&contents, &cwd).unwrap();
        let client = &config.torrent_clients["qbit_main"];
        let indexer = &config.indexers.torznab["example"];

        assert_eq!(
            Some("super-secret"),
            client.password.as_ref().map(Password::expose_secret)
        );
        assert_eq!(
            Some("server-secret"),
            config
                .server
                .api_token
                .as_ref()
                .map(ApiToken::expose_secret)
        );
        assert_eq!(
            Some("api-secret"),
            indexer.api_key.as_ref().map(ApiKey::expose_secret)
        );
        assert!(!format!("{:?}", config.server).contains("server-secret"));
        assert!(!format!("{client:?}").contains("super-secret"));
        assert!(!format!("{indexer:?}").contains("api-secret"));

        fs::remove_dir_all(cwd).unwrap();
    }

    #[test]
    fn startup_config_rejects_unreadable_secret_files() {
        let cwd = unique_temp_dir("missing-secret");
        let missing_file = cwd.join("missing-password");
        let contents = format!(
            r#"
            [paths]
            database = "{}/state/sporos.db"
            torrent_cache_dir = "{}/cache/torrents"
            output_dir = "{}/output"

            [torrent_clients.qbit_main]
            kind = "qbittorrent"
            url = "http://qbittorrent:8080"
            password_file = "{}"
            default_save_path = "/downloads"
            "#,
            cwd.display(),
            cwd.display(),
            cwd.display(),
            missing_file.display()
        );

        let error = parse_startup_config(&contents, &cwd).unwrap_err();

        assert!(error.to_string().contains("torrent_clients.password_file"));
        assert!(error.to_string().contains("missing-password"));

        fs::remove_dir_all(cwd).unwrap();
    }

    #[test]
    fn schema_documents_sporos_native_surface() {
        assert!(CONFIG_SCHEMA.contains("sporos config schema"));
        assert!(CONFIG_SCHEMA.contains("[torrent_clients.<name>]"));
        assert!(CONFIG_SCHEMA.contains("[indexers.torznab.<name>]"));
        assert!(CONFIG_SCHEMA.contains("[inventory]"));
        assert!(CONFIG_SCHEMA.contains("SPOROS__SERVER__BIND"));
        assert!(CONFIG_SCHEMA.contains("SPOROS__TORRENT_CLIENTS__QBIT_MAIN__URL"));
    }

    #[test]
    fn environment_overrides_scalar_fields_before_typed_parse() {
        let config = parse_config_with_env(
            r#"
            [paths]
            database = "/data/state/sporos.db"

            [server]
            bind = "127.0.0.1:2468"
            api_token_env = "SPOROS_API_TOKEN"

            [matching]
            fuzzy_size_threshold = 0.02
            include_non_video = false
            recent_search_cooldown_secs = 259200

            [announce]
            max_pending = 1000
            "#,
            vec![
                ("SPOROS__SERVER__BIND".to_owned(), "0.0.0.0:9876".to_owned()),
                (
                    "SPOROS__MATCHING__FUZZY_SIZE_THRESHOLD".to_owned(),
                    "0.05".to_owned(),
                ),
                (
                    "SPOROS__MATCHING__INCLUDE_NON_VIDEO".to_owned(),
                    "true".to_owned(),
                ),
                (
                    "SPOROS__MATCHING__RECENT_SEARCH_COOLDOWN_SECS".to_owned(),
                    "86400".to_owned(),
                ),
                ("SPOROS__ANNOUNCE__MAX_PENDING".to_owned(), "42".to_owned()),
                ("SPOROS_API_TOKEN".to_owned(), "api-token".to_owned()),
            ],
        )
        .unwrap();

        assert_eq!(
            "0.0.0.0:9876".parse::<SocketAddr>().unwrap(),
            config.server.bind
        );
        assert!((config.matching.fuzzy_size_threshold - 0.05).abs() < f64::EPSILON);
        assert!(config.matching.include_non_video);
        assert_eq!(Some(86_400), config.matching.recent_search_cooldown_secs);
        assert_eq!(42, config.announce.max_pending);
        assert_eq!(
            Some("api-token"),
            config
                .server
                .api_token
                .as_ref()
                .map(ApiToken::expose_secret)
        );
    }

    #[test]
    fn environment_overrides_keyed_table_scalars_and_secrets() {
        let config = parse_config_with_env(
            r#"
            [torrent_clients.qbit_main]
            kind = "qbittorrent"
            url = "http://old:8080"
            default_save_path = "/downloads"
            password_env = "QBIT_PASSWORD"

            [indexers.torznab.example]
            url = "https://old.example/api"
            api_key_env = "INDEXER_API_KEY"
            "#,
            vec![
                (
                    "SPOROS__TORRENT_CLIENTS__QBIT_MAIN__URL".to_owned(),
                    "http://qbittorrent:8080".to_owned(),
                ),
                (
                    "SPOROS__INDEXERS__TORZNAB__EXAMPLE__URL".to_owned(),
                    "https://indexer.example/api".to_owned(),
                ),
                ("QBIT_PASSWORD".to_owned(), "super-secret".to_owned()),
                ("INDEXER_API_KEY".to_owned(), "api-secret".to_owned()),
            ],
        )
        .unwrap();
        let client = &config.torrent_clients["qbit_main"];
        let indexer = &config.indexers.torznab["example"];

        assert_eq!("http://qbittorrent:8080", client.url);
        assert_eq!(
            Some("super-secret"),
            client.password.as_ref().map(Password::expose_secret)
        );
        assert_eq!(
            Some("[REDACTED]".to_owned()),
            client.password.as_ref().map(ToString::to_string)
        );
        assert_eq!("https://indexer.example/api", indexer.url);
        assert_eq!(
            Some("api-secret"),
            indexer.api_key.as_ref().map(ApiKey::expose_secret)
        );
    }

    #[test]
    fn environment_rejects_arrays_and_indexed_paths() {
        let media_error = parse_config_with_env(
            "",
            vec![(
                "SPOROS__PATHS__MEDIA_DIRS".to_owned(),
                "[\"/media\"]".to_owned(),
            )],
        )
        .unwrap_err();
        let indexed_error = parse_config_with_env(
            "",
            vec![(
                "SPOROS__TORRENT_CLIENTS__0__URL".to_owned(),
                "http://client".to_owned(),
            )],
        )
        .unwrap_err();

        assert!(media_error.to_string().contains("array config values"));
        assert!(indexed_error.to_string().contains("indexed env overrides"));
    }

    #[test]
    fn direct_toml_secret_values_are_redacted() {
        let config = parse_config(
            r#"
            [torrent_clients.qbit_main]
            kind = "qbittorrent"
            url = "http://qbittorrent:8080"
            password = "dev-secret"
            default_save_path = "/downloads"

            [indexers.torznab.example]
            url = "https://indexer.example/api"
            api_key = "api-secret"
            "#,
        )
        .unwrap();
        let client = &config.torrent_clients["qbit_main"];
        let indexer = &config.indexers.torznab["example"];

        assert_eq!(
            Some("dev-secret"),
            client.password.as_ref().map(Password::expose_secret)
        );
        assert!(!format!("{client:?}").contains("dev-secret"));
        assert_eq!(
            Some("api-secret"),
            indexer.api_key.as_ref().map(ApiKey::expose_secret)
        );
        assert!(!format!("{indexer:?}").contains("api-secret"));
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
