use std::collections::BTreeMap;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Deserializer};
use toml::Value;

use crate::announce::AnnounceQueueConfig;
use crate::errors::ConfigError;
use crate::secrets::{ApiKey, ApiToken, Password};

pub const DEFAULT_CONFIG_PATH: &str = "./config.toml";
pub const DEFAULT_INJECTION_METADATA: &str = "sporos";
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
    #[serde(default)]
    pub default_category: Option<String>,
    #[serde(
        default = "default_injection_tags",
        deserialize_with = "deserialize_string_list"
    )]
    pub default_tags: Vec<String>,
    #[serde(default = "default_injection_label")]
    pub default_label: String,
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
    pub prowlarr: BTreeMap<String, ProwlarrSourceConfig>,
    pub arr: ArrServicesConfig,
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

#[derive(Debug, Clone, Eq, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProwlarrSourceConfig {
    pub enabled: bool,
    #[serde(alias = "base_url")]
    pub url: String,
    pub api_key: Option<ApiKey>,
    pub api_key_file: Option<PathBuf>,
    pub api_key_env: Option<String>,
    pub update_interval: String,
    pub tags: Vec<String>,
    pub tag_match: ProwlarrTagMatch,
    pub include_untagged: bool,
    pub refresh_on_startup: bool,
    pub required: bool,
    pub remove_policy: ProwlarrRemovePolicy,
}

impl Default for ProwlarrSourceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            url: String::new(),
            api_key: None,
            api_key_file: None,
            api_key_env: None,
            update_interval: "24h".to_owned(),
            tags: Vec::new(),
            tag_match: ProwlarrTagMatch::Any,
            include_untagged: true,
            refresh_on_startup: true,
            required: false,
            remove_policy: ProwlarrRemovePolicy::Deactivate,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProwlarrTagMatch {
    #[default]
    Any,
    All,
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProwlarrRemovePolicy {
    #[default]
    Deactivate,
    Ignore,
}

#[derive(Debug, Clone, Default, Eq, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ArrServicesConfig {
    pub sonarr: BTreeMap<String, ArrInstanceConfig>,
    pub radarr: BTreeMap<String, ArrInstanceConfig>,
}

#[derive(Debug, Clone, Eq, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArrInstanceConfig {
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
    pub client_inventory_interval: String,
    pub indexer_caps_interval: String,
    pub saved_retry_interval: String,
    pub cleanup_interval: String,
}

impl Default for SchedulingConfig {
    fn default() -> Self {
        Self {
            client_inventory_interval: "24h".to_owned(),
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
    validate_secret_source_counts(&config)?;
    validate_torrent_clients(&config)?;
    validate_prowlarr_sources(&config, &raw)?;
    validate_arr_secret_source_counts(&config)?;
    resolve_secret_env(&mut config, &env)?;
    validate_integration_api_keys(&config)?;

    Ok((config, raw))
}

fn default_injection_label() -> String {
    DEFAULT_INJECTION_METADATA.to_owned()
}

fn default_injection_tags() -> Vec<String> {
    vec![DEFAULT_INJECTION_METADATA.to_owned()]
}

fn deserialize_string_list<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StringList {
        String(String),
        List(Vec<String>),
    }

    match StringList::deserialize(deserializer)? {
        StringList::String(value) => {
            Ok(value.split(',').map(str::trim).map(str::to_owned).collect())
        }
        StringList::List(values) => Ok(values),
    }
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
        insert_env_value(raw, &path, &path, value, key)?;
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
    full_path: &[String],
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
        if matches!(table.get(segment), Some(Value::Array(_)))
            && !is_torrent_client_default_tags_path(full_path)
        {
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

    insert_env_value(child, rest, full_path, value, key)
}

fn is_torrent_client_default_tags_path(path: &[String]) -> bool {
    matches!(
        path,
        [section, _client_name, field]
            if section == "torrent_clients" && field == "default_tags"
    )
}

fn resolve_secret_env(
    config: &mut SporosConfig,
    env: &BTreeMap<String, String>,
) -> Result<(), ConfigError> {
    if config.server.api_token.is_none()
        && let Some(env_name) = nonempty_secret_env(
            "server.api_token_env",
            "server",
            &config.server.api_token_env,
        )?
    {
        let value = secret_env_value(env, env_name, "server.api_token_env", "server")?;
        config.server.api_token = Some(
            ApiToken::new(value.clone()).map_err(|source| ConfigError::InvalidSecret { source })?,
        );
    }
    for (name, client) in &mut config.torrent_clients {
        if client.password.is_none()
            && let Some(env_name) =
                nonempty_secret_env("torrent_clients.password_env", name, &client.password_env)?
        {
            let value = secret_env_value(env, env_name, "torrent_clients.password_env", name)?;
            client.password = Some(
                Password::new(value.clone())
                    .map_err(|source| ConfigError::InvalidSecret { source })?,
            );
        }
    }
    for (name, indexer) in &mut config.indexers.torznab {
        if indexer.api_key.is_none()
            && let Some(env_name) =
                nonempty_secret_env("indexers.torznab.api_key_env", name, &indexer.api_key_env)?
        {
            let value = secret_env_value(env, env_name, "indexers.torznab.api_key_env", name)?;
            indexer.api_key = Some(
                ApiKey::new(value.clone())
                    .map_err(|source| ConfigError::InvalidSecret { source })?,
            );
        }
    }
    for (name, source) in &mut config.indexers.prowlarr {
        if source.api_key.is_none()
            && let Some(env_name) =
                nonempty_secret_env("indexers.prowlarr.api_key_env", name, &source.api_key_env)?
        {
            if !source.enabled {
                continue;
            }
            let value = secret_env_value(env, env_name, "indexers.prowlarr.api_key_env", name)?;
            source.api_key = Some(
                ApiKey::new(value.clone())
                    .map_err(|source| ConfigError::InvalidSecret { source })?,
            );
        }
    }
    resolve_arr_secret_env(
        "indexers.arr.sonarr.api_key_env",
        &mut config.indexers.arr.sonarr,
        env,
    )?;
    resolve_arr_secret_env(
        "indexers.arr.radarr.api_key_env",
        &mut config.indexers.arr.radarr,
        env,
    )?;

    Ok(())
}

fn resolve_secret_files(config: &mut SporosConfig) -> Result<(), ConfigError> {
    if config.server.api_token.is_none()
        && let Some(path) = &config.server.api_token_file
    {
        let value = secret_file_value("server.api_token_file", "server", path)?;
        config.server.api_token =
            Some(ApiToken::new(value).map_err(|source| ConfigError::InvalidSecret { source })?);
    }
    for (name, client) in &mut config.torrent_clients {
        if client.password.is_none()
            && let Some(path) = &client.password_file
        {
            let value = secret_file_value("torrent_clients.password_file", name, path)?;
            client.password =
                Some(Password::new(value).map_err(|source| ConfigError::InvalidSecret { source })?);
        }
    }
    for (name, indexer) in &mut config.indexers.torznab {
        if indexer.api_key.is_none()
            && let Some(path) = &indexer.api_key_file
        {
            let value = secret_file_value("indexers.torznab.api_key_file", name, path)?;
            indexer.api_key =
                Some(ApiKey::new(value).map_err(|source| ConfigError::InvalidSecret { source })?);
        }
    }
    for (name, source) in &mut config.indexers.prowlarr {
        if source.api_key.is_none()
            && let Some(path) = &source.api_key_file
        {
            if !source.enabled {
                continue;
            }
            let value = secret_file_value("indexers.prowlarr.api_key_file", name, path)?;
            source.api_key =
                Some(ApiKey::new(value).map_err(|source| ConfigError::InvalidSecret { source })?);
        }
    }
    resolve_arr_secret_files(
        "indexers.arr.sonarr.api_key_file",
        &mut config.indexers.arr.sonarr,
    )?;
    resolve_arr_secret_files(
        "indexers.arr.radarr.api_key_file",
        &mut config.indexers.arr.radarr,
    )?;

    Ok(())
}

fn resolve_arr_secret_env(
    field: &'static str,
    instances: &mut BTreeMap<String, ArrInstanceConfig>,
    env: &BTreeMap<String, String>,
) -> Result<(), ConfigError> {
    for (name, instance) in instances {
        if instance.api_key.is_none()
            && let Some(env_name) = nonempty_secret_env(field, name, &instance.api_key_env)?
        {
            let value = secret_env_value(env, env_name, field, name)?;
            instance.api_key = Some(
                ApiKey::new(value.clone())
                    .map_err(|source| ConfigError::InvalidSecret { source })?,
            );
        }
    }

    Ok(())
}

fn resolve_arr_secret_files(
    field: &'static str,
    instances: &mut BTreeMap<String, ArrInstanceConfig>,
) -> Result<(), ConfigError> {
    for (name, instance) in instances {
        if instance.api_key.is_none()
            && let Some(path) = &instance.api_key_file
        {
            let value = secret_file_value(field, name, path)?;
            instance.api_key =
                Some(ApiKey::new(value).map_err(|source| ConfigError::InvalidSecret { source })?);
        }
    }

    Ok(())
}

fn validate_prowlarr_sources(config: &SporosConfig, raw: &Value) -> Result<(), ConfigError> {
    for (name, source) in &config.indexers.prowlarr {
        if name.trim().is_empty() {
            return Err(ConfigError::InvalidField {
                field: "indexers.prowlarr",
                reason: "source names must not be empty".to_owned(),
            });
        }
        if source.enabled || prowlarr_source_field_supplied(raw, name, &["url", "base_url"]) {
            validate_http_url("indexers.prowlarr.url", name, &source.url)?;
        }
        if source.enabled || prowlarr_source_field_supplied(raw, name, &["update_interval"]) {
            validate_interval(
                "indexers.prowlarr.update_interval",
                name,
                &source.update_interval,
            )?;
        }
        validate_secret_source_count(
            "indexers.prowlarr.api_key",
            name,
            [
                ("api_key", source.api_key.is_some()),
                ("api_key_file", source.api_key_file.is_some()),
                ("api_key_env", source.api_key_env.is_some()),
            ],
        )?;
        for tag in &source.tags {
            if tag.trim().is_empty() {
                return Err(ConfigError::InvalidField {
                    field: "indexers.prowlarr.tags",
                    reason: format!("{name} contains an empty tag"),
                });
            }
        }
    }

    Ok(())
}

fn validate_torrent_clients(config: &SporosConfig) -> Result<(), ConfigError> {
    for (name, client) in &config.torrent_clients {
        if let Some(category) = &client.default_category {
            validate_injection_metadata_value(
                "torrent_clients.default_category",
                name,
                "category",
                category,
                false,
            )?;
        }
        validate_injection_metadata_value(
            "torrent_clients.default_label",
            name,
            "label",
            &client.default_label,
            false,
        )?;
        for tag in &client.default_tags {
            validate_injection_metadata_value(
                "torrent_clients.default_tags",
                name,
                "tag",
                tag,
                true,
            )?;
        }
    }

    Ok(())
}

fn validate_injection_metadata_value(
    field: &'static str,
    client_name: &str,
    value_name: &str,
    value: &str,
    reject_comma: bool,
) -> Result<(), ConfigError> {
    if value.trim().is_empty() {
        return Err(ConfigError::InvalidField {
            field,
            reason: format!("{client_name} contains an empty {value_name}"),
        });
    }
    if value.chars().any(char::is_control) {
        return Err(ConfigError::InvalidField {
            field,
            reason: format!("{client_name} {value_name} contains a control character"),
        });
    }
    if reject_comma && value.contains(',') {
        return Err(ConfigError::InvalidField {
            field,
            reason: format!("{client_name} {value_name} must not contain commas"),
        });
    }

    Ok(())
}

fn validate_integration_api_keys(config: &SporosConfig) -> Result<(), ConfigError> {
    for (name, source) in &config.indexers.prowlarr {
        if source.enabled
            && !has_api_key_source(&source.api_key, &source.api_key_file, &source.api_key_env)
        {
            return Err(missing_api_key_source("indexers.prowlarr.api_key", name));
        }
    }
    validate_arr_api_key_sources("indexers.arr.sonarr.api_key", &config.indexers.arr.sonarr)?;
    validate_arr_api_key_sources("indexers.arr.radarr.api_key", &config.indexers.arr.radarr)?;

    Ok(())
}

fn validate_secret_source_counts(config: &SporosConfig) -> Result<(), ConfigError> {
    validate_secret_source_count(
        "server.api_token",
        "server",
        [
            ("api_token", config.server.api_token.is_some()),
            ("api_token_file", config.server.api_token_file.is_some()),
            ("api_token_env", config.server.api_token_env.is_some()),
        ],
    )?;
    for (name, client) in &config.torrent_clients {
        validate_secret_source_count(
            "torrent_clients.password",
            name,
            [
                ("password", client.password.is_some()),
                ("password_file", client.password_file.is_some()),
                ("password_env", client.password_env.is_some()),
            ],
        )?;
    }
    for (name, indexer) in &config.indexers.torznab {
        validate_secret_source_count(
            "indexers.torznab.api_key",
            name,
            [
                ("api_key", indexer.api_key.is_some()),
                ("api_key_file", indexer.api_key_file.is_some()),
                ("api_key_env", indexer.api_key_env.is_some()),
            ],
        )?;
    }

    Ok(())
}

fn validate_arr_secret_source_counts(config: &SporosConfig) -> Result<(), ConfigError> {
    validate_arr_secret_source_count("indexers.arr.sonarr.api_key", &config.indexers.arr.sonarr)?;
    validate_arr_secret_source_count("indexers.arr.radarr.api_key", &config.indexers.arr.radarr)
}

fn validate_arr_secret_source_count(
    field: &'static str,
    instances: &BTreeMap<String, ArrInstanceConfig>,
) -> Result<(), ConfigError> {
    for (name, instance) in instances {
        validate_secret_source_count(
            field,
            name,
            [
                ("api_key", instance.api_key.is_some()),
                ("api_key_file", instance.api_key_file.is_some()),
                ("api_key_env", instance.api_key_env.is_some()),
            ],
        )?;
    }

    Ok(())
}

fn validate_arr_api_key_sources(
    field: &'static str,
    instances: &BTreeMap<String, ArrInstanceConfig>,
) -> Result<(), ConfigError> {
    for (name, instance) in instances {
        validate_arr_url(name, &instance.url)?;
        if !has_api_key_source(
            &instance.api_key,
            &instance.api_key_file,
            &instance.api_key_env,
        ) {
            return Err(missing_api_key_source(field, name));
        }
    }

    Ok(())
}

fn validate_arr_url(name: &str, value: &str) -> Result<(), ConfigError> {
    let parsed = reqwest::Url::parse(value).map_err(|error| ConfigError::InvalidField {
        field: "indexers.arr.url",
        reason: format!("{name} has invalid URL: {error}"),
    })?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(ConfigError::InvalidField {
            field: "indexers.arr.url",
            reason: format!("{name} URL must use http or https"),
        });
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(ConfigError::InvalidField {
            field: "indexers.arr.url",
            reason: format!("{name} URL must not include credentials"),
        });
    }
    if parsed.query().is_some() {
        return Err(ConfigError::InvalidField {
            field: "indexers.arr.url",
            reason: format!("{name} URL must not include query parameters"),
        });
    }
    if parsed.fragment().is_some() {
        return Err(ConfigError::InvalidField {
            field: "indexers.arr.url",
            reason: format!("{name} URL must not include fragments"),
        });
    }
    Ok(())
}

fn has_api_key_source(
    direct: &Option<ApiKey>,
    file: &Option<PathBuf>,
    env: &Option<String>,
) -> bool {
    direct.is_some() || file.is_some() || env.is_some()
}

fn missing_api_key_source(field: &'static str, name: &str) -> ConfigError {
    ConfigError::InvalidField {
        field,
        reason: format!("{name} must configure api_key, api_key_file, or api_key_env"),
    }
}

fn prowlarr_source_field_supplied(raw: &Value, name: &str, fields: &[&str]) -> bool {
    raw.get("indexers")
        .and_then(Value::as_table)
        .and_then(|indexers| indexers.get("prowlarr"))
        .and_then(Value::as_table)
        .and_then(|prowlarr| prowlarr.get(name))
        .and_then(Value::as_table)
        .is_some_and(|source| fields.iter().any(|field| source.contains_key(*field)))
}

fn validate_http_url(field: &'static str, name: &str, value: &str) -> Result<(), ConfigError> {
    let parsed = reqwest::Url::parse(value).map_err(|error| ConfigError::InvalidField {
        field,
        reason: format!("{name}: {error}"),
    })?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(ConfigError::InvalidField {
            field,
            reason: format!("{name}: URL scheme must be http or https"),
        });
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(ConfigError::InvalidField {
            field,
            reason: format!("{name}: URL userinfo is not supported"),
        });
    }
    if parsed.query().is_some() {
        return Err(ConfigError::InvalidField {
            field,
            reason: format!("{name}: URL query parameters are not supported"),
        });
    }
    if parsed.fragment().is_some() {
        return Err(ConfigError::InvalidField {
            field,
            reason: format!("{name}: URL fragments are not supported"),
        });
    }

    Ok(())
}

fn validate_interval(field: &'static str, name: &str, value: &str) -> Result<(), ConfigError> {
    let trimmed = value.trim();
    let split_at = trimmed
        .find(|character: char| !character.is_ascii_digit())
        .ok_or_else(|| ConfigError::InvalidField {
            field,
            reason: format!("{name}: {value} is missing a duration unit"),
        })?;
    let (amount, unit) = trimmed.split_at(split_at);
    let amount = amount
        .parse::<i64>()
        .map_err(|error| ConfigError::InvalidField {
            field,
            reason: format!("{name}: {error}"),
        })?;
    if amount <= 0 {
        return Err(ConfigError::InvalidField {
            field,
            reason: format!("{name}: interval must be positive"),
        });
    }
    let multiplier = match unit {
        "s" => 1_000_i64,
        "m" => 60_000_i64,
        "h" => 3_600_000_i64,
        "d" => 86_400_000_i64,
        _ => {
            return Err(ConfigError::InvalidField {
                field,
                reason: format!("{name}: unsupported duration unit {unit}"),
            });
        }
    };
    amount
        .checked_mul(multiplier)
        .ok_or_else(|| ConfigError::InvalidField {
            field,
            reason: format!("{name}: interval is too large"),
        })?;

    Ok(())
}

fn validate_secret_source_count(
    field: &'static str,
    name: &str,
    sources: [(&str, bool); 3],
) -> Result<(), ConfigError> {
    let count = sources
        .iter()
        .filter(|(_source, configured)| *configured)
        .count();
    if count <= 1 {
        return Ok(());
    }
    let names = sources.map(|(source, _configured)| source).join(", or ");

    Err(ConfigError::InvalidField {
        field,
        reason: format!("{name} must use only one of {names}"),
    })
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
default_category = "optional qbittorrent category"
default_tags = ["sporos"]
default_label = "sporos"
label_field = "optional rtorrent custom field"

[indexers.default_timeouts]
search = "120s"
download = "30s"

[indexers.torznab.<name>]
url = "https://indexer.example/api"
api_key = "optional local-development secret"
api_key_file = "optional path"
api_key_env = "optional env var containing api key"

[indexers.prowlarr.<name>]
enabled = true
url = "https://prowlarr.example"
api_key = "optional local-development secret"
api_key_file = "optional path"
api_key_env = "optional env var containing api key"
update_interval = "24h"
tags = ["optional", "tag"]
tag_match = "any|all"
include_untagged = true
refresh_on_startup = true
required = false
remove_policy = "deactivate|ignore"

[indexers.arr.sonarr.<name>]
url = "http://sonarr:8989"
api_key = "optional local-development secret"
api_key_file = "optional path"
api_key_env = "optional env var containing api key"

[indexers.arr.radarr.<name>]
url = "http://radarr:7878"
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
SPOROS__TORRENT_CLIENTS__QBIT_MAIN__DEFAULT_CATEGORY = "cross-seed"
SPOROS__TORRENT_CLIENTS__QBIT_MAIN__DEFAULT_TAGS = "cross-seed,sporos"
SPOROS__TORRENT_CLIENTS__RTORRENT_MAIN__DEFAULT_LABEL = "cross-seed"
SPOROS__INDEXERS__TORZNAB__EXAMPLE__API_KEY_FILE = "/var/run/secrets/indexer-api-key"
SPOROS__INDEXERS__PROWLARR__MAIN__API_KEY_FILE = "/var/run/secrets/prowlarr-api-key"
SPOROS__INDEXERS__ARR__SONARR__MAIN__API_KEY_FILE = "/var/run/secrets/sonarr-api-key"

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
client_inventory_interval = "24h"
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
        let client = &config.torrent_clients["qbit_main"];
        assert_eq!(None, client.default_category);
        assert_eq!(
            vec![DEFAULT_INJECTION_METADATA.to_owned()],
            client.default_tags
        );
        assert_eq!(DEFAULT_INJECTION_METADATA, client.default_label);
    }

    #[test]
    fn parses_injection_metadata_settings() {
        let config = parse_config(
            r#"
            [torrent_clients.qbit_main]
            kind = "qbittorrent"
            url = "http://qbittorrent:8080"
            default_save_path = "/downloads"
            default_category = "cross-seed"
            default_tags = ["cross-seed", "sporos"]

            [torrent_clients.rtorrent_main]
            kind = "rtorrent"
            url = "http://rtorrent:5000/RPC2"
            default_save_path = "/downloads"
            default_label = "cross-seed"
            label_field = "custom1"
            "#,
        )
        .unwrap();
        let qbit = &config.torrent_clients["qbit_main"];
        let rtorrent = &config.torrent_clients["rtorrent_main"];

        assert_eq!(Some("cross-seed"), qbit.default_category.as_deref());
        assert_eq!(
            vec!["cross-seed".to_owned(), "sporos".to_owned()],
            qbit.default_tags
        );
        assert_eq!("cross-seed", rtorrent.default_label);
    }

    #[test]
    fn rejects_invalid_injection_metadata_settings() {
        for (contents, expected) in [
            (
                r#"
                [torrent_clients.qbit_main]
                kind = "qbittorrent"
                url = "http://qbittorrent:8080"
                default_save_path = "/downloads"
                default_category = " "
                "#,
                "empty category",
            ),
            (
                r#"
                [torrent_clients.qbit_main]
                kind = "qbittorrent"
                url = "http://qbittorrent:8080"
                default_save_path = "/downloads"
                default_tags = ["cross-seed", ""]
                "#,
                "empty tag",
            ),
            (
                r#"
                [torrent_clients.qbit_main]
                kind = "qbittorrent"
                url = "http://qbittorrent:8080"
                default_save_path = "/downloads"
                default_tags = ["bad,tag"]
                "#,
                "commas",
            ),
            (
                r#"
                [torrent_clients.rtorrent_main]
                kind = "rtorrent"
                url = "http://rtorrent:5000/RPC2"
                default_save_path = "/downloads"
                default_label = ""
                "#,
                "empty label",
            ),
        ] {
            let error = parse_config(contents).unwrap_err();

            assert!(
                error.to_string().contains(expected),
                "{error} did not contain {expected}"
            );
        }
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
    fn rejects_removed_rss_interval() {
        let error = parse_config(
            r#"
            [scheduling]
            rss_interval = "30m"
            "#,
        )
        .unwrap_err();

        assert!(error.to_string().contains("unknown field"));
        assert!(error.to_string().contains("rss_interval"));
    }

    #[test]
    fn rejects_unsupported_scheduled_search_interval() {
        let error = parse_config(
            r#"
            [scheduling]
            search_interval = "24h"
            "#,
        )
        .unwrap_err();

        assert!(error.to_string().contains("unknown field"));
        assert!(error.to_string().contains("search_interval"));
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
        let prowlarr_api_key_file = cwd.join("prowlarr-api-key");
        let sonarr_api_key_file = cwd.join("sonarr-api-key");
        fs::write(&api_token_file, "server-secret\n").unwrap();
        fs::write(&password_file, "super-secret\n").unwrap();
        fs::write(&api_key_file, "api-secret\r\n").unwrap();
        fs::write(&prowlarr_api_key_file, "prowlarr-secret\n").unwrap();
        fs::write(&sonarr_api_key_file, "sonarr-secret\n").unwrap();
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

            [indexers.prowlarr.main]
            url = "https://prowlarr.example"
            api_key_file = "{}"

            [indexers.arr.sonarr.main]
            url = "http://sonarr:8989"
            api_key_file = "{}"
            "#,
            cwd.display(),
            cwd.display(),
            cwd.display(),
            api_token_file.display(),
            password_file.display(),
            api_key_file.display(),
            prowlarr_api_key_file.display(),
            sonarr_api_key_file.display()
        );

        let config = parse_startup_config(&contents, &cwd).unwrap();
        let client = &config.torrent_clients["qbit_main"];
        let indexer = &config.indexers.torznab["example"];
        let prowlarr = &config.indexers.prowlarr["main"];
        let sonarr = &config.indexers.arr.sonarr["main"];

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
        assert_eq!(
            Some("prowlarr-secret"),
            prowlarr.api_key.as_ref().map(ApiKey::expose_secret)
        );
        assert_eq!(
            Some("sonarr-secret"),
            sonarr.api_key.as_ref().map(ApiKey::expose_secret)
        );
        assert!(!format!("{:?}", config.server).contains("server-secret"));
        assert!(!format!("{client:?}").contains("super-secret"));
        assert!(!format!("{indexer:?}").contains("api-secret"));
        assert!(!format!("{prowlarr:?}").contains("prowlarr-secret"));
        assert!(!format!("{sonarr:?}").contains("sonarr-secret"));

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
    fn startup_config_ignores_disabled_prowlarr_secret_files() {
        let cwd = unique_temp_dir("disabled-prowlarr-secret");
        let missing_secret = cwd.join("future-prowlarr-api-key");
        let contents = format!(
            r#"
            [paths]
            database = "{}/state/sporos.db"
            torrent_cache_dir = "{}/cache/torrents"
            output_dir = "{}/output"

            [indexers.prowlarr.future]
            enabled = false
            api_key_file = "{}"
            "#,
            cwd.display(),
            cwd.display(),
            cwd.display(),
            missing_secret.display()
        );

        let config = parse_startup_config(&contents, &cwd).unwrap();
        let source = &config.indexers.prowlarr["future"];

        assert_eq!(None, source.api_key);
        assert_eq!(
            Some(missing_secret.as_path()),
            source.api_key_file.as_deref()
        );

        fs::remove_dir_all(cwd).unwrap();
    }

    #[test]
    fn schema_documents_sporos_native_surface() {
        assert!(CONFIG_SCHEMA.contains("sporos config schema"));
        assert!(CONFIG_SCHEMA.contains("[torrent_clients.<name>]"));
        assert!(CONFIG_SCHEMA.contains("[indexers.torznab.<name>]"));
        assert!(CONFIG_SCHEMA.contains("[indexers.prowlarr.<name>]"));
        assert!(CONFIG_SCHEMA.contains("[inventory]"));
        assert!(!CONFIG_SCHEMA.contains("rss_interval"));
        assert!(CONFIG_SCHEMA.contains("default_tags = [\"sporos\"]"));
        assert!(CONFIG_SCHEMA.contains("SPOROS__SERVER__BIND"));
        assert!(CONFIG_SCHEMA.contains("SPOROS__TORRENT_CLIENTS__QBIT_MAIN__URL"));
        assert!(CONFIG_SCHEMA.contains("SPOROS__TORRENT_CLIENTS__QBIT_MAIN__DEFAULT_TAGS"));
        assert!(CONFIG_SCHEMA.contains("SPOROS__TORRENT_CLIENTS__RTORRENT_MAIN__DEFAULT_LABEL"));
        assert!(CONFIG_SCHEMA.contains("SPOROS__INDEXERS__PROWLARR__MAIN__API_KEY_FILE"));
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

            [indexers.prowlarr.main]
            url = "https://old-prowlarr.example"
            api_key_env = "PROWLARR_API_KEY"

            [indexers.arr.radarr.main]
            url = "http://old-radarr:7878"
            api_key_env = "RADARR_API_KEY"
            "#,
            vec![
                (
                    "SPOROS__TORRENT_CLIENTS__QBIT_MAIN__URL".to_owned(),
                    "http://qbittorrent:8080".to_owned(),
                ),
                (
                    "SPOROS__TORRENT_CLIENTS__QBIT_MAIN__DEFAULT_CATEGORY".to_owned(),
                    "cross-seed".to_owned(),
                ),
                (
                    "SPOROS__TORRENT_CLIENTS__QBIT_MAIN__DEFAULT_TAGS".to_owned(),
                    "cross-seed,sporos".to_owned(),
                ),
                (
                    "SPOROS__INDEXERS__TORZNAB__EXAMPLE__URL".to_owned(),
                    "https://indexer.example/api".to_owned(),
                ),
                (
                    "SPOROS__INDEXERS__PROWLARR__MAIN__URL".to_owned(),
                    "https://prowlarr.example".to_owned(),
                ),
                (
                    "SPOROS__INDEXERS__ARR__RADARR__MAIN__URL".to_owned(),
                    "http://radarr:7878".to_owned(),
                ),
                ("QBIT_PASSWORD".to_owned(), "super-secret".to_owned()),
                ("INDEXER_API_KEY".to_owned(), "api-secret".to_owned()),
                ("PROWLARR_API_KEY".to_owned(), "prowlarr-secret".to_owned()),
                ("RADARR_API_KEY".to_owned(), "radarr-secret".to_owned()),
            ],
        )
        .unwrap();
        let client = &config.torrent_clients["qbit_main"];
        let indexer = &config.indexers.torznab["example"];
        let prowlarr = &config.indexers.prowlarr["main"];
        let radarr = &config.indexers.arr.radarr["main"];

        assert_eq!("http://qbittorrent:8080", client.url);
        assert_eq!(Some("cross-seed"), client.default_category.as_deref());
        assert_eq!(
            vec!["cross-seed".to_owned(), "sporos".to_owned()],
            client.default_tags
        );
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
        assert_eq!("https://prowlarr.example", prowlarr.url);
        assert_eq!(
            Some("prowlarr-secret"),
            prowlarr.api_key.as_ref().map(ApiKey::expose_secret)
        );
        assert_eq!("http://radarr:7878", radarr.url);
        assert_eq!(
            Some("radarr-secret"),
            radarr.api_key.as_ref().map(ApiKey::expose_secret)
        );
    }

    #[test]
    fn environment_overrides_default_tags_array_with_comma_list() {
        let config = parse_config_with_env(
            r#"
            [torrent_clients.qbit_main]
            kind = "qbittorrent"
            url = "http://qbittorrent:8080"
            default_save_path = "/downloads"
            default_tags = ["sporos"]
            "#,
            vec![(
                "SPOROS__TORRENT_CLIENTS__QBIT_MAIN__DEFAULT_TAGS".to_owned(),
                "cross-seed,sporos".to_owned(),
            )],
        )
        .unwrap();
        let client = &config.torrent_clients["qbit_main"];

        assert_eq!(
            vec!["cross-seed".to_owned(), "sporos".to_owned()],
            client.default_tags
        );
    }

    #[test]
    fn parses_prowlarr_sources_with_defaults_and_policies() {
        let config = parse_config(
            r#"
            [indexers.prowlarr.main]
            url = "https://prowlarr.example"
            api_key = "prowlarr-secret"
            update_interval = "30m"
            tags = ["movies", "hd"]
            tag_match = "all"
            include_untagged = false
            refresh_on_startup = false
            required = true
            remove_policy = "ignore"

            [indexers.prowlarr.backup]
            base_url = "http://backup-prowlarr.example"
            api_key = "backup-secret"
            "#,
        )
        .unwrap();
        let main = &config.indexers.prowlarr["main"];
        let backup = &config.indexers.prowlarr["backup"];

        assert!(main.enabled);
        assert_eq!("https://prowlarr.example", main.url);
        assert_eq!(
            Some("prowlarr-secret"),
            main.api_key.as_ref().map(ApiKey::expose_secret)
        );
        assert_eq!("30m", main.update_interval);
        assert_eq!(vec!["movies".to_owned(), "hd".to_owned()], main.tags);
        assert_eq!(ProwlarrTagMatch::All, main.tag_match);
        assert!(!main.include_untagged);
        assert!(!main.refresh_on_startup);
        assert!(main.required);
        assert_eq!(ProwlarrRemovePolicy::Ignore, main.remove_policy);
        assert_eq!("http://backup-prowlarr.example", backup.url);
        assert_eq!(
            Some("backup-secret"),
            backup.api_key.as_ref().map(ApiKey::expose_secret)
        );
        assert_eq!("24h", backup.update_interval);
        assert_eq!(ProwlarrTagMatch::Any, backup.tag_match);
        assert_eq!(ProwlarrRemovePolicy::Deactivate, backup.remove_policy);
    }

    #[test]
    fn disabled_prowlarr_sources_can_be_placeholders() {
        let config = parse_config(
            r#"
            [indexers.prowlarr.future]
            enabled = false
            api_key_env = "FUTURE_PROWLARR_API_KEY"
            "#,
        )
        .unwrap();
        let source = &config.indexers.prowlarr["future"];

        assert!(!source.enabled);
        assert_eq!("", source.url);
        assert_eq!(None, source.api_key);
        assert_eq!(
            Some("FUTURE_PROWLARR_API_KEY"),
            source.api_key_env.as_deref()
        );
        assert_eq!("24h", source.update_interval);

        for (contents, expected) in [
            (
                r#"
                [indexers.prowlarr.future]
                enabled = false
                url = "file:///tmp/prowlarr"
                "#,
                "URL scheme",
            ),
            (
                r#"
                [indexers.prowlarr.future]
                enabled = false
                update_interval = "0m"
                "#,
                "interval must be positive",
            ),
        ] {
            let error = parse_config(contents).unwrap_err();

            assert!(
                error.to_string().contains(expected),
                "{error} did not contain {expected}"
            );
        }
    }

    #[test]
    fn env_disabled_prowlarr_sources_can_be_placeholders() {
        let config = parse_config_with_env(
            "",
            vec![(
                "SPOROS__INDEXERS__PROWLARR__FUTURE__ENABLED".to_owned(),
                "false".to_owned(),
            )],
        )
        .unwrap();
        let source = &config.indexers.prowlarr["future"];

        assert!(!source.enabled);
        assert_eq!("", source.url);

        let error = parse_config_with_env(
            "",
            vec![
                (
                    "SPOROS__INDEXERS__PROWLARR__FUTURE__ENABLED".to_owned(),
                    "false".to_owned(),
                ),
                (
                    "SPOROS__INDEXERS__PROWLARR__FUTURE__URL".to_owned(),
                    "file:///tmp/prowlarr".to_owned(),
                ),
            ],
        )
        .unwrap_err();

        assert!(error.to_string().contains("URL scheme"));
    }

    #[test]
    fn rejects_invalid_prowlarr_sources() {
        for (contents, expected) in [
            (
                r#"
                [indexers.prowlarr.""]
                url = "https://prowlarr.example"
                "#,
                "source names",
            ),
            (
                r#"
                [indexers.prowlarr.main]
                url = "file:///tmp/prowlarr"
                "#,
                "URL scheme",
            ),
            (
                r#"
                [indexers.prowlarr.main]
                url = "https://user:secret@prowlarr.example"
                "#,
                "userinfo",
            ),
            (
                r#"
                [indexers.prowlarr.main]
                url = "https://prowlarr.example?apikey=secret"
                "#,
                "query parameters",
            ),
            (
                r#"
                [indexers.prowlarr.main]
                url = "https://prowlarr.example#secret"
                "#,
                "fragments",
            ),
            (
                r#"
                [indexers.prowlarr.main]
                url = "https://prowlarr.example"
                update_interval = "0m"
                "#,
                "interval must be positive",
            ),
            (
                r#"
                [indexers.prowlarr.main]
                url = "https://prowlarr.example"
                api_key = "direct"
                api_key_file = "/var/run/secrets/prowlarr"
                "#,
                "only one",
            ),
            (
                r#"
                [indexers.prowlarr.main]
                url = "https://prowlarr.example"
                tags = [""]
                "#,
                "empty tag",
            ),
        ] {
            let error = parse_config(contents).unwrap_err();

            assert!(
                error.to_string().contains(expected),
                "{error} did not contain {expected}"
            );
        }

        let tag_policy = parse_config(
            r#"
            [indexers.prowlarr.main]
            url = "https://prowlarr.example"
            tag_match = "xor"
            "#,
        )
        .unwrap_err();

        assert!(tag_policy.to_string().contains("tag_match"));
    }

    #[test]
    fn enabled_prowlarr_sources_require_api_key_source() {
        let error = parse_config(
            r#"
            [indexers.prowlarr.main]
            url = "https://prowlarr.example"
            "#,
        )
        .unwrap_err();

        assert!(error.to_string().contains("indexers.prowlarr.api_key"));
        assert!(error.to_string().contains("api_key_file"));
    }

    #[test]
    fn arr_instances_require_api_key_source() {
        let missing = parse_config(
            r#"
            [indexers.arr.sonarr.main]
            url = "http://sonarr:8989"
            "#,
        )
        .unwrap_err();
        let duplicate = parse_config(
            r#"
            [indexers.arr.radarr.main]
            url = "http://radarr:7878"
            api_key = "direct"
            api_key_env = "RADARR_API_KEY"
            "#,
        )
        .unwrap_err();

        assert!(missing.to_string().contains("indexers.arr.sonarr.api_key"));
        assert!(missing.to_string().contains("api_key_file"));
        assert!(
            duplicate
                .to_string()
                .contains("indexers.arr.radarr.api_key")
        );
        assert!(duplicate.to_string().contains("only one"));
    }

    #[test]
    fn server_rejects_duplicate_api_token_sources() {
        let error = parse_config(
            r#"
            [server]
            api_token = "direct"
            api_token_file = "/var/run/secrets/sporos-api-token"
            "#,
        )
        .unwrap_err();

        assert!(error.to_string().contains("server.api_token"));
        assert!(error.to_string().contains("only one"));
        assert!(error.to_string().contains("api_token_env"));
    }

    #[test]
    fn torrent_clients_reject_duplicate_password_sources() {
        let error = parse_config(
            r#"
            [torrent_clients.qbit_main]
            kind = "qbittorrent"
            url = "http://qbittorrent:8080"
            password = "direct"
            password_env = "QBIT_PASSWORD"
            default_save_path = "/downloads"
            "#,
        )
        .unwrap_err();

        assert!(error.to_string().contains("torrent_clients.password"));
        assert!(error.to_string().contains("only one"));
        assert!(error.to_string().contains("password_file"));
    }

    #[test]
    fn torznab_indexers_reject_duplicate_api_key_sources() {
        let error = parse_config(
            r#"
            [indexers.torznab.example]
            url = "https://indexer.example/api"
            api_key = "direct"
            api_key_file = "/var/run/secrets/indexer-api-key"
            "#,
        )
        .unwrap_err();

        assert!(error.to_string().contains("indexers.torznab.api_key"));
        assert!(error.to_string().contains("only one"));
        assert!(error.to_string().contains("api_key_env"));
    }

    #[test]
    fn arr_instances_reject_runtime_invalid_urls() {
        for (url, expected) in [
            ("file:///var/lib/sonarr", "http or https"),
            (
                "http://user:pass@sonarr:8989",
                "must not include credentials",
            ),
            ("http://sonarr:8989?apikey=secret", "query parameters"),
            ("http://sonarr:8989#apikey=secret", "fragments"),
        ] {
            let error = parse_config(&format!(
                r#"
                [indexers.arr.sonarr.main]
                url = "{url}"
                api_key = "direct"
                "#
            ))
            .unwrap_err();

            assert!(
                error.to_string().contains(expected),
                "{error} did not contain {expected}"
            );
        }
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

            [indexers.prowlarr.main]
            url = "https://prowlarr.example"
            api_key = "prowlarr-secret"

            [indexers.arr.sonarr.main]
            url = "http://sonarr:8989"
            api_key = "sonarr-secret"
            "#,
        )
        .unwrap();
        let client = &config.torrent_clients["qbit_main"];
        let indexer = &config.indexers.torznab["example"];
        let prowlarr = &config.indexers.prowlarr["main"];
        let sonarr = &config.indexers.arr.sonarr["main"];

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
        assert_eq!(
            Some("prowlarr-secret"),
            prowlarr.api_key.as_ref().map(ApiKey::expose_secret)
        );
        assert!(!format!("{prowlarr:?}").contains("prowlarr-secret"));
        assert_eq!(
            Some("sonarr-secret"),
            sonarr.api_key.as_ref().map(ApiKey::expose_secret)
        );
        assert!(!format!("{sonarr:?}").contains("sonarr-secret"));
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
