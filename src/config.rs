use std::collections::BTreeMap;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Deserializer};
use toml::Value;

use crate::announce::AnnounceQueueConfig;
use crate::errors::ConfigError;
use crate::secrets::{ApiKey, ApiToken, NotificationToken, Password};

pub const DEFAULT_CONFIG_PATH: &str = "./config.toml";
pub const DEFAULT_INJECTION_METADATA: &str = "sporos";
pub const MAX_RUNTIME_WORKER_THREADS: usize = 256;
pub const MAX_RUNTIME_BLOCKING_THREADS: usize = 512;
pub const DEFAULT_SEARCH_QUEUE_LIMIT: usize = 100;
pub const DEFAULT_INJECTION_QUEUE_LIMIT: usize = 100;
pub const DEFAULT_INDEXING_QUEUE_LIMIT: usize = 50;
pub const DEFAULT_NOTIFICATION_QUEUE_LIMIT: usize = 500;
pub const DEFAULT_SEARCH_WORKER_CONCURRENCY: usize = 4;
pub const DEFAULT_MANUAL_SEARCH_PER_INDEXER_RESULT_LIMIT: usize = 1_000;
pub const DEFAULT_MANUAL_SEARCH_WORKFLOW_RESULT_LIMIT: usize = 10_000;
pub const MAX_RUNTIME_QUEUE_LIMIT: usize = 1_000_000;
pub const MAX_SEARCH_WORKER_CONCURRENCY: usize = 256;
pub const MAX_MANUAL_SEARCH_RESULT_LIMIT: usize = 1_000_000;
pub const MAX_NOTIFICATION_RETRY_ATTEMPTS: u8 = 10;
const ENV_PREFIX: &str = "SPOROS__";
static WRITE_PROBE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SporosConfig {
    pub paths: PathsConfig,
    pub server: ServerConfig,
    pub runtime: RuntimeConfig,
    pub torrent_clients: BTreeMap<String, TorrentClientConfig>,
    pub indexers: IndexersConfig,
    pub matching: MatchingConfig,
    pub inventory: InventoryConfig,
    pub injection: InjectionConfig,
    pub scheduling: SchedulingConfig,
    pub announce: AnnounceQueueConfig,
    pub notifications: NotificationsConfig,
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
#[serde(default, deny_unknown_fields)]
pub struct RuntimeConfig {
    pub worker_threads: Option<usize>,
    pub max_blocking_threads: Option<usize>,
    pub search_queue_limit: usize,
    pub indexing_queue_limit: usize,
    pub notification_queue_limit: usize,
    pub search_worker_concurrency: usize,
    pub manual_search_per_indexer_result_limit: usize,
    pub manual_search_workflow_result_limit: usize,
}

#[derive(Debug, Clone, Default, Eq, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NotificationsConfig {
    pub endpoints: BTreeMap<String, NotificationEndpointConfig>,
}

#[derive(Debug, Clone, Eq, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NotificationEndpointConfig {
    pub url: String,
    pub token: Option<NotificationToken>,
    pub token_file: Option<PathBuf>,
    pub token_env: Option<String>,
    pub timeout: String,
    pub retry_max_attempts: u8,
    pub retry_initial_delay: String,
    pub retry_max_delay: String,
}

impl Default for NotificationEndpointConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            token: None,
            token_file: None,
            token_env: None,
            timeout: "300s".to_owned(),
            retry_max_attempts: 3,
            retry_initial_delay: "1s".to_owned(),
            retry_max_delay: "30s".to_owned(),
        }
    }
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            worker_threads: None,
            max_blocking_threads: None,
            search_queue_limit: DEFAULT_SEARCH_QUEUE_LIMIT,
            indexing_queue_limit: DEFAULT_INDEXING_QUEUE_LIMIT,
            notification_queue_limit: DEFAULT_NOTIFICATION_QUEUE_LIMIT,
            search_worker_concurrency: DEFAULT_SEARCH_WORKER_CONCURRENCY,
            manual_search_per_indexer_result_limit: DEFAULT_MANUAL_SEARCH_PER_INDEXER_RESULT_LIMIT,
            manual_search_workflow_result_limit: DEFAULT_MANUAL_SEARCH_WORKFLOW_RESULT_LIMIT,
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

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct InjectionConfig {
    pub link_type: Option<InjectionLinkTypeConfig>,
    pub link_dirs: Vec<PathBuf>,
    pub flat_linking: bool,
    pub recheck: AutoResumePolicyConfig,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InjectionLinkTypeConfig {
    Hardlink,
    Symlink,
    Reflink,
    ReflinkOrCopy,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AutoResumePolicyConfig {
    pub skip_recheck: bool,
    pub max_remaining_bytes: u64,
    pub min_completion_percent: Option<f64>,
    pub max_remaining_percent: Option<f64>,
    pub ignore_non_relevant_files_to_resume: bool,
    pub non_relevant_max_remaining_bytes: u64,
    pub piece_slack_multiplier: u64,
    pub poll_interval_ms: u64,
    pub max_resume_wait_ms: u64,
    pub below_threshold_action: BelowThresholdActionConfig,
}

impl Default for AutoResumePolicyConfig {
    fn default() -> Self {
        Self {
            skip_recheck: false,
            max_remaining_bytes: 0,
            min_completion_percent: None,
            max_remaining_percent: None,
            ignore_non_relevant_files_to_resume: false,
            non_relevant_max_remaining_bytes: 200 * 1024 * 1024,
            piece_slack_multiplier: 2,
            poll_interval_ms: 5_000,
            max_resume_wait_ms: 60 * 60 * 1_000,
            below_threshold_action: BelowThresholdActionConfig::InjectPaused,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BelowThresholdActionConfig {
    InjectAndStart,
    #[default]
    InjectPaused,
    RejectWithoutInjecting,
}

#[derive(Debug, Clone, Eq, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SchedulingConfig {
    pub client_inventory_interval: String,
    pub media_inventory_interval: String,
    pub indexer_caps_interval: String,
    pub saved_retry_interval: String,
    pub cleanup_interval: String,
}

impl Default for SchedulingConfig {
    fn default() -> Self {
        Self {
            client_inventory_interval: "24h".to_owned(),
            media_inventory_interval: "24h".to_owned(),
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
    validate_runtime_threads(&config)?;
    validate_notifications_config(&config)?;
    validate_secret_source_counts(&config)?;
    validate_torrent_clients(&config)?;
    validate_injection_config(&config)?;
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
    for (name, endpoint) in &mut config.notifications.endpoints {
        if endpoint.token.is_none()
            && let Some(env_name) = nonempty_secret_env(
                "notifications.endpoints.token_env",
                name,
                &endpoint.token_env,
            )?
        {
            let value = secret_env_value(env, env_name, "notifications.endpoints.token_env", name)?;
            endpoint.token = Some(
                NotificationToken::new(value.clone())
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
    for (name, endpoint) in &mut config.notifications.endpoints {
        if endpoint.token.is_none()
            && let Some(path) = &endpoint.token_file
        {
            let value = secret_file_value("notifications.endpoints.token_file", name, path)?;
            endpoint.token = Some(
                NotificationToken::new(value)
                    .map_err(|source| ConfigError::InvalidSecret { source })?,
            );
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
        validate_http_url("torrent_clients.url", name, &client.url)?;
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

fn validate_injection_config(config: &SporosConfig) -> Result<(), ConfigError> {
    if config.injection.link_type.is_some() && config.injection.link_dirs.is_empty() {
        return Err(ConfigError::InvalidField {
            field: "injection.link_dirs",
            reason: "link_dirs must not be empty when link_type is configured".to_owned(),
        });
    }
    let recheck = &config.injection.recheck;
    validate_percent(
        "injection.recheck.min_completion_percent",
        "min_completion_percent",
        recheck.min_completion_percent,
        false,
    )?;
    validate_percent(
        "injection.recheck.max_remaining_percent",
        "max_remaining_percent",
        recheck.max_remaining_percent,
        true,
    )?;
    if recheck.poll_interval_ms == 0 {
        return Err(ConfigError::InvalidField {
            field: "injection.recheck.poll_interval_ms",
            reason: "poll_interval_ms must be positive".to_owned(),
        });
    }
    if recheck.piece_slack_multiplier == 0 {
        return Err(ConfigError::InvalidField {
            field: "injection.recheck.piece_slack_multiplier",
            reason: "piece_slack_multiplier must be positive".to_owned(),
        });
    }
    if recheck.max_resume_wait_ms == 0 {
        return Err(ConfigError::InvalidField {
            field: "injection.recheck.max_resume_wait_ms",
            reason: "max_resume_wait_ms must be positive".to_owned(),
        });
    }

    Ok(())
}

fn validate_runtime_threads(config: &SporosConfig) -> Result<(), ConfigError> {
    validate_optional_usize_range(
        "runtime.worker_threads",
        config.runtime.worker_threads,
        MAX_RUNTIME_WORKER_THREADS,
    )?;
    validate_optional_usize_range(
        "runtime.max_blocking_threads",
        config.runtime.max_blocking_threads,
        MAX_RUNTIME_BLOCKING_THREADS,
    )?;
    validate_usize_range(
        "runtime.search_queue_limit",
        config.runtime.search_queue_limit,
        MAX_RUNTIME_QUEUE_LIMIT,
    )?;
    validate_usize_range(
        "runtime.indexing_queue_limit",
        config.runtime.indexing_queue_limit,
        MAX_RUNTIME_QUEUE_LIMIT,
    )?;
    validate_usize_range(
        "runtime.notification_queue_limit",
        config.runtime.notification_queue_limit,
        MAX_RUNTIME_QUEUE_LIMIT,
    )?;
    validate_usize_range(
        "runtime.search_worker_concurrency",
        config.runtime.search_worker_concurrency,
        MAX_SEARCH_WORKER_CONCURRENCY,
    )?;
    validate_usize_range(
        "runtime.manual_search_per_indexer_result_limit",
        config.runtime.manual_search_per_indexer_result_limit,
        MAX_MANUAL_SEARCH_RESULT_LIMIT,
    )?;
    validate_usize_range(
        "runtime.manual_search_workflow_result_limit",
        config.runtime.manual_search_workflow_result_limit,
        MAX_MANUAL_SEARCH_RESULT_LIMIT,
    )
}

fn validate_notifications_config(config: &SporosConfig) -> Result<(), ConfigError> {
    for (name, endpoint) in &config.notifications.endpoints {
        if name.trim().is_empty() {
            return Err(ConfigError::InvalidField {
                field: "notifications.endpoints",
                reason: "endpoint names must not be empty".to_owned(),
            });
        }
        validate_http_url("notifications.endpoints.url", name, &endpoint.url)?;
        validate_interval("notifications.endpoints.timeout", name, &endpoint.timeout)?;
        validate_interval(
            "notifications.endpoints.retry_initial_delay",
            name,
            &endpoint.retry_initial_delay,
        )?;
        validate_interval(
            "notifications.endpoints.retry_max_delay",
            name,
            &endpoint.retry_max_delay,
        )?;
        validate_secret_source_count(
            "notifications.endpoints.token",
            name,
            [
                ("token", endpoint.token.is_some()),
                ("token_file", endpoint.token_file.is_some()),
                ("token_env", endpoint.token_env.is_some()),
            ],
        )?;
        if !(1..=MAX_NOTIFICATION_RETRY_ATTEMPTS).contains(&endpoint.retry_max_attempts) {
            return Err(ConfigError::InvalidField {
                field: "notifications.endpoints.retry_max_attempts",
                reason: format!(
                    "{name}: retry_max_attempts must be between 1 and {MAX_NOTIFICATION_RETRY_ATTEMPTS}"
                ),
            });
        }
        let initial_delay_ms = interval_ms(
            "notifications.endpoints.retry_initial_delay",
            name,
            &endpoint.retry_initial_delay,
        )?;
        let max_delay_ms = interval_ms(
            "notifications.endpoints.retry_max_delay",
            name,
            &endpoint.retry_max_delay,
        )?;
        if max_delay_ms < initial_delay_ms {
            return Err(ConfigError::InvalidField {
                field: "notifications.endpoints.retry_max_delay",
                reason: format!("{name}: retry_max_delay must be at least retry_initial_delay"),
            });
        }
    }

    Ok(())
}

fn validate_optional_usize_range(
    field: &'static str,
    value: Option<usize>,
    max: usize,
) -> Result<(), ConfigError> {
    let Some(value) = value else {
        return Ok(());
    };
    if (1..=max).contains(&value) {
        return Ok(());
    }

    Err(ConfigError::InvalidField {
        field,
        reason: format!("must be between 1 and {max} when configured"),
    })
}

fn validate_usize_range(field: &'static str, value: usize, max: usize) -> Result<(), ConfigError> {
    if (1..=max).contains(&value) {
        return Ok(());
    }

    Err(ConfigError::InvalidField {
        field,
        reason: format!("must be between 1 and {max}"),
    })
}

fn validate_percent(
    field: &'static str,
    name: &str,
    value: Option<f64>,
    allow_zero: bool,
) -> Result<(), ConfigError> {
    let Some(value) = value else {
        return Ok(());
    };
    let lower_bound_valid = if allow_zero {
        value >= 0.0
    } else {
        value > 0.0
    };
    if value.is_finite() && lower_bound_valid && value <= 100.0 {
        return Ok(());
    }

    let lower_bound = if allow_zero {
        "at least 0"
    } else {
        "greater than 0"
    };
    Err(ConfigError::InvalidField {
        field,
        reason: format!("{name} must be {lower_bound} and at most 100"),
    })
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
    interval_ms(field, name, value).map(|_millis| ())
}

fn interval_ms(field: &'static str, name: &str, value: &str) -> Result<i64, ConfigError> {
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
        })
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

[runtime]
worker_threads = "optional 1-256 integer; defaults to Tokio"
max_blocking_threads = "optional 1-512 integer; defaults to Tokio"
search_queue_limit = 100
indexing_queue_limit = 50
notification_queue_limit = 500
search_worker_concurrency = 4
manual_search_per_indexer_result_limit = 1000
manual_search_workflow_result_limit = 10000

[notifications.endpoints.<name>]
url = "https://hooks.example/sporos"
token = "optional local-development bearer token"
token_file = "optional path"
token_env = "optional env var containing bearer token"
timeout = "300s"
retry_max_attempts = 3
retry_initial_delay = "1s"
retry_max_delay = "30s"

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
SPOROS__RUNTIME__WORKER_THREADS = "4"
SPOROS__RUNTIME__MAX_BLOCKING_THREADS = "64"
SPOROS__RUNTIME__SEARCH_QUEUE_LIMIT = "100"
SPOROS__RUNTIME__INDEXING_QUEUE_LIMIT = "50"
SPOROS__RUNTIME__NOTIFICATION_QUEUE_LIMIT = "500"
SPOROS__RUNTIME__SEARCH_WORKER_CONCURRENCY = "4"
SPOROS__RUNTIME__MANUAL_SEARCH_PER_INDEXER_RESULT_LIMIT = "1000"
SPOROS__RUNTIME__MANUAL_SEARCH_WORKFLOW_RESULT_LIMIT = "10000"
SPOROS__NOTIFICATIONS__ENDPOINTS__MAIN__URL = "https://hooks.example/sporos"
SPOROS__NOTIFICATIONS__ENDPOINTS__MAIN__TOKEN_FILE = "/var/run/secrets/notification-token"
SPOROS__MATCHING__FUZZY_SIZE_THRESHOLD = "0.02"
SPOROS__INJECTION__RECHECK__SKIP_RECHECK = "false"
SPOROS__INJECTION__RECHECK__MAX_REMAINING_BYTES = "104857600"
SPOROS__INJECTION__RECHECK__MIN_COMPLETION_PERCENT = "85.0"
SPOROS__INJECTION__RECHECK__MAX_REMAINING_PERCENT = "15.0"
SPOROS__INJECTION__RECHECK__IGNORE_NON_RELEVANT_FILES_TO_RESUME = "true"
SPOROS__INJECTION__RECHECK__NON_RELEVANT_MAX_REMAINING_BYTES = "209715200"
SPOROS__INJECTION__RECHECK__PIECE_SLACK_MULTIPLIER = "2"
SPOROS__INJECTION__RECHECK__POLL_INTERVAL_MS = "5000"
SPOROS__INJECTION__RECHECK__MAX_RESUME_WAIT_MS = "3600000"
SPOROS__INJECTION__RECHECK__BELOW_THRESHOLD_ACTION = "inject_paused"
SPOROS__INJECTION__LINK_TYPE = "hardlink"
SPOROS__INJECTION__FLAT_LINKING = "true"
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

[injection]
link_type = "optional hardlink|symlink|reflink|reflink_or_copy"
link_dirs = ["optional path", "..."]
flat_linking = false

[injection.recheck]
skip_recheck = false
max_remaining_bytes = 0
min_completion_percent = "optional 0-100"
max_remaining_percent = "optional 0-100"
ignore_non_relevant_files_to_resume = false
non_relevant_max_remaining_bytes = 209715200
piece_slack_multiplier = 2
poll_interval_ms = 5000
max_resume_wait_ms = 3600000
below_threshold_action = "inject_and_start|inject_paused|reject_without_injecting"

[scheduling]
client_inventory_interval = "24h"
media_inventory_interval = "24h"
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
remote_candidate_retention_secs = 2592000
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

            [runtime]
            worker_threads = 4
            max_blocking_threads = 64
            search_queue_limit = 250
            indexing_queue_limit = 75
            notification_queue_limit = 800
            search_worker_concurrency = 8
            manual_search_per_indexer_result_limit = 333
            manual_search_workflow_result_limit = 444

            [notifications.endpoints.ops]
            url = "https://hooks.example/sporos"
            token = "notification-secret"
            timeout = "30s"
            retry_max_attempts = 4
            retry_initial_delay = "2s"
            retry_max_delay = "20s"

            [torrent_clients.qbit_main]
            kind = "qbittorrent"
            url = "http://qbittorrent:8080"
            username = "sporos"
            password_file = "/var/run/secrets/qbit-password"
            default_save_path = "/downloads"

            [indexers.torznab.main]
            url = "https://indexer.example/api"
            api_key_file = "/var/run/secrets/indexer-api-key"

            [injection]
            link_type = "hardlink"
            link_dirs = ["/links/fast", "/links/slow"]
            flat_linking = true

            [scheduling]
            client_inventory_interval = "12h"
            media_inventory_interval = "6h"
            indexer_caps_interval = "3h"
            saved_retry_interval = "15m"
            cleanup_interval = "2h"
            "#,
        )
        .unwrap();

        assert_eq!(
            "0.0.0.0:2468".parse::<SocketAddr>().unwrap(),
            config.server.bind
        );
        assert_eq!(Some(4), config.runtime.worker_threads);
        assert_eq!(Some(64), config.runtime.max_blocking_threads);
        assert_eq!(250, config.runtime.search_queue_limit);
        assert_eq!(75, config.runtime.indexing_queue_limit);
        assert_eq!(800, config.runtime.notification_queue_limit);
        assert_eq!(8, config.runtime.search_worker_concurrency);
        assert_eq!(333, config.runtime.manual_search_per_indexer_result_limit);
        assert_eq!(444, config.runtime.manual_search_workflow_result_limit);
        let endpoint = &config.notifications.endpoints["ops"];
        assert_eq!("https://hooks.example/sporos", endpoint.url);
        assert_eq!(
            Some("notification-secret"),
            endpoint
                .token
                .as_ref()
                .map(NotificationToken::expose_secret)
        );
        assert_eq!("30s", endpoint.timeout);
        assert_eq!(4, endpoint.retry_max_attempts);
        assert_eq!("2s", endpoint.retry_initial_delay);
        assert_eq!("20s", endpoint.retry_max_delay);
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
        assert_eq!(
            Some(InjectionLinkTypeConfig::Hardlink),
            config.injection.link_type
        );
        assert_eq!(
            vec![PathBuf::from("/links/fast"), PathBuf::from("/links/slow")],
            config.injection.link_dirs
        );
        assert!(config.injection.flat_linking);
        assert!(!config.injection.recheck.skip_recheck);
        assert_eq!(0, config.injection.recheck.max_remaining_bytes);
        assert_eq!(None, config.injection.recheck.min_completion_percent);
        assert_eq!(None, config.injection.recheck.max_remaining_percent);
        assert_eq!(
            200 * 1024 * 1024,
            config.injection.recheck.non_relevant_max_remaining_bytes
        );
        assert_eq!(2, config.injection.recheck.piece_slack_multiplier);
        assert_eq!(5_000, config.injection.recheck.poll_interval_ms);
        assert_eq!(60 * 60 * 1_000, config.injection.recheck.max_resume_wait_ms);
        assert_eq!(
            BelowThresholdActionConfig::InjectPaused,
            config.injection.recheck.below_threshold_action
        );
        assert_eq!("12h", config.scheduling.client_inventory_interval);
        assert_eq!("6h", config.scheduling.media_inventory_interval);
        assert_eq!("3h", config.scheduling.indexer_caps_interval);
        assert_eq!("15m", config.scheduling.saved_retry_interval);
        assert_eq!("2h", config.scheduling.cleanup_interval);
    }

    #[test]
    fn runtime_thread_counts_default_to_tokio_policy() {
        let config = parse_config("").unwrap();

        assert_eq!(None, config.runtime.worker_threads);
        assert_eq!(None, config.runtime.max_blocking_threads);
        assert_eq!(
            DEFAULT_SEARCH_QUEUE_LIMIT,
            config.runtime.search_queue_limit
        );
        assert_eq!(
            DEFAULT_INDEXING_QUEUE_LIMIT,
            config.runtime.indexing_queue_limit
        );
        assert_eq!(
            DEFAULT_NOTIFICATION_QUEUE_LIMIT,
            config.runtime.notification_queue_limit
        );
        assert_eq!(
            DEFAULT_SEARCH_WORKER_CONCURRENCY,
            config.runtime.search_worker_concurrency
        );
        assert_eq!(
            DEFAULT_MANUAL_SEARCH_PER_INDEXER_RESULT_LIMIT,
            config.runtime.manual_search_per_indexer_result_limit
        );
        assert_eq!(
            DEFAULT_MANUAL_SEARCH_WORKFLOW_RESULT_LIMIT,
            config.runtime.manual_search_workflow_result_limit
        );
        assert!(config.notifications.endpoints.is_empty());
    }

    #[test]
    fn announce_ttl_and_cleanup_bounds_are_validated_from_toml() {
        let error = parse_config(
            r#"
            [announce]
            default_ttl_secs = 604801
            "#,
        )
        .unwrap_err();

        assert!(matches!(
            error,
            ConfigError::InvalidField {
                field: "announce",
                ..
            }
        ));
        assert!(error.to_string().contains("default_ttl_secs"));
        let retry_boundary_error = parse_config(
            r#"
            [announce]
            retry_max_delay_secs = 3600
            default_ttl_secs = 3600
            "#,
        )
        .unwrap_err();
        assert!(
            retry_boundary_error
                .to_string()
                .contains("default_ttl_secs")
        );
        parse_config(
            r#"
            [announce]
            default_ttl_secs = 604800
            success_retention_secs = 2592000
            failure_retention_secs = 2592000
            remote_candidate_retention_secs = 7776000
            "#,
        )
        .unwrap();
    }

    #[test]
    fn runtime_rejects_out_of_range_thread_counts() {
        let worker_error = parse_config(
            r#"
            [runtime]
            worker_threads = 0
            "#,
        )
        .unwrap_err();
        assert!(worker_error.to_string().contains("runtime.worker_threads"));

        let blocking_error = parse_config(
            r#"
            [runtime]
            max_blocking_threads = 0
            "#,
        )
        .unwrap_err();
        assert!(
            blocking_error
                .to_string()
                .contains("runtime.max_blocking_threads")
        );

        let oversized_worker_error = parse_config(
            r#"
            [runtime]
            worker_threads = 257
            "#,
        )
        .unwrap_err();
        assert!(
            oversized_worker_error
                .to_string()
                .contains("between 1 and 256")
        );

        let oversized_blocking_error = parse_config(
            r#"
            [runtime]
            max_blocking_threads = 513
            "#,
        )
        .unwrap_err();
        assert!(
            oversized_blocking_error
                .to_string()
                .contains("between 1 and 512")
        );
    }

    #[test]
    fn runtime_rejects_out_of_range_queue_and_worker_limits() {
        for (field, value) in [
            ("search_queue_limit", "0"),
            ("indexing_queue_limit", "0"),
            ("notification_queue_limit", "0"),
            ("search_worker_concurrency", "0"),
            ("manual_search_per_indexer_result_limit", "0"),
            ("manual_search_workflow_result_limit", "0"),
        ] {
            let error = parse_config(&format!(
                r#"
                [runtime]
                {field} = {value}
                "#
            ))
            .unwrap_err();
            assert!(error.to_string().contains(&format!("runtime.{field}")));
        }

        let queue_error = parse_config(
            r#"
            [runtime]
            search_queue_limit = 1000001
            "#,
        )
        .unwrap_err();
        assert!(queue_error.to_string().contains("between 1 and 1000000"));

        let worker_error = parse_config(
            r#"
            [runtime]
            search_worker_concurrency = 257
            "#,
        )
        .unwrap_err();
        assert!(worker_error.to_string().contains("between 1 and 256"));

        let search_cap_error = parse_config(
            r#"
            [runtime]
            manual_search_workflow_result_limit = 1000001
            "#,
        )
        .unwrap_err();
        assert!(
            search_cap_error
                .to_string()
                .contains("between 1 and 1000000")
        );
    }

    #[test]
    fn notifications_parse_endpoints_and_redact_tokens() {
        let config = parse_config(
            r#"
            [notifications.endpoints.ops]
            url = "https://hooks.example/sporos"
            token = "notification-secret"
            timeout = "45s"
            retry_max_attempts = 2
            retry_initial_delay = "5s"
            retry_max_delay = "30s"
            "#,
        )
        .unwrap();
        let endpoint = &config.notifications.endpoints["ops"];

        assert_eq!("https://hooks.example/sporos", endpoint.url);
        assert_eq!(
            Some("notification-secret"),
            endpoint
                .token
                .as_ref()
                .map(NotificationToken::expose_secret)
        );
        assert_eq!("45s", endpoint.timeout);
        assert_eq!(2, endpoint.retry_max_attempts);
        assert_eq!("5s", endpoint.retry_initial_delay);
        assert_eq!("30s", endpoint.retry_max_delay);
        assert!(!format!("{endpoint:?}").contains("notification-secret"));
    }

    #[test]
    fn notifications_reject_invalid_endpoint_config() {
        for (field, value, expected) in [
            (
                "url",
                "\"http://user:pass@hooks.example/sporos\"",
                "userinfo",
            ),
            (
                "url",
                "\"https://hooks.example/sporos?token=secret\"",
                "query",
            ),
            ("timeout", "\"0s\"", "interval must be positive"),
            ("retry_max_attempts", "0", "retry_max_attempts"),
            (
                "retry_initial_delay",
                "\"1ms\"",
                "unsupported duration unit",
            ),
        ] {
            let endpoint = if field == "url" {
                format!(
                    r#"
                    [notifications.endpoints.ops]
                    url = {value}
                    "#
                )
            } else {
                format!(
                    r#"
                    [notifications.endpoints.ops]
                    url = "https://hooks.example/sporos"
                    {field} = {value}
                    "#
                )
            };
            let error = parse_config(&endpoint).unwrap_err();

            assert!(
                error.to_string().contains(expected),
                "{error} did not contain {expected}"
            );
        }

        let max_before_initial = parse_config(
            r#"
            [notifications.endpoints.ops]
            url = "https://hooks.example/sporos"
            retry_initial_delay = "30s"
            retry_max_delay = "5s"
            "#,
        )
        .unwrap_err();
        assert!(max_before_initial.to_string().contains("retry_max_delay"));
    }

    #[test]
    fn notifications_reject_duplicate_token_sources() {
        let error = parse_config(
            r#"
            [notifications.endpoints.ops]
            url = "https://hooks.example/sporos"
            token = "direct"
            token_env = "SPOROS_NOTIFICATION_TOKEN"
            "#,
        )
        .unwrap_err();

        assert!(error.to_string().contains("notifications.endpoints.token"));
        assert!(error.to_string().contains("only one"));
        assert!(error.to_string().contains("token_file"));
    }

    #[test]
    fn runtime_thread_counts_support_env_overrides() {
        let config = parse_config_with_env(
            "",
            [
                ("SPOROS__RUNTIME__WORKER_THREADS".to_owned(), "3".to_owned()),
                (
                    "SPOROS__RUNTIME__MAX_BLOCKING_THREADS".to_owned(),
                    "16".to_owned(),
                ),
                (
                    "SPOROS__RUNTIME__SEARCH_QUEUE_LIMIT".to_owned(),
                    "250".to_owned(),
                ),
                (
                    "SPOROS__RUNTIME__INDEXING_QUEUE_LIMIT".to_owned(),
                    "75".to_owned(),
                ),
                (
                    "SPOROS__RUNTIME__NOTIFICATION_QUEUE_LIMIT".to_owned(),
                    "800".to_owned(),
                ),
                (
                    "SPOROS__RUNTIME__SEARCH_WORKER_CONCURRENCY".to_owned(),
                    "8".to_owned(),
                ),
                (
                    "SPOROS__RUNTIME__MANUAL_SEARCH_PER_INDEXER_RESULT_LIMIT".to_owned(),
                    "333".to_owned(),
                ),
                (
                    "SPOROS__RUNTIME__MANUAL_SEARCH_WORKFLOW_RESULT_LIMIT".to_owned(),
                    "444".to_owned(),
                ),
            ],
        )
        .unwrap();

        assert_eq!(Some(3), config.runtime.worker_threads);
        assert_eq!(Some(16), config.runtime.max_blocking_threads);
        assert_eq!(250, config.runtime.search_queue_limit);
        assert_eq!(75, config.runtime.indexing_queue_limit);
        assert_eq!(800, config.runtime.notification_queue_limit);
        assert_eq!(8, config.runtime.search_worker_concurrency);
        assert_eq!(333, config.runtime.manual_search_per_indexer_result_limit);
        assert_eq!(444, config.runtime.manual_search_workflow_result_limit);
    }

    #[test]
    fn parses_auto_resume_policy_settings() {
        let config = parse_config(
            r#"
            [injection.recheck]
            skip_recheck = true
            max_remaining_bytes = 104857600
            min_completion_percent = 85.5
            max_remaining_percent = 15.0
            ignore_non_relevant_files_to_resume = true
            non_relevant_max_remaining_bytes = 10485760
            piece_slack_multiplier = 3
            poll_interval_ms = 2500
            max_resume_wait_ms = 120000
            below_threshold_action = "reject_without_injecting"
            "#,
        )
        .unwrap();
        let policy = &config.injection.recheck;

        assert!(policy.skip_recheck);
        assert_eq!(104_857_600, policy.max_remaining_bytes);
        assert_eq!(Some(85.5), policy.min_completion_percent);
        assert_eq!(Some(15.0), policy.max_remaining_percent);
        assert!(policy.ignore_non_relevant_files_to_resume);
        assert_eq!(10_485_760, policy.non_relevant_max_remaining_bytes);
        assert_eq!(3, policy.piece_slack_multiplier);
        assert_eq!(2_500, policy.poll_interval_ms);
        assert_eq!(120_000, policy.max_resume_wait_ms);
        assert_eq!(
            BelowThresholdActionConfig::RejectWithoutInjecting,
            policy.below_threshold_action
        );
    }

    #[test]
    fn rejects_invalid_auto_resume_policy_settings() {
        for (contents, expected) in [
            (
                r#"
                [injection.recheck]
                min_completion_percent = 0.0
                "#,
                "min_completion_percent",
            ),
            (
                r#"
                [injection.recheck]
                max_remaining_percent = -0.1
                "#,
                "max_remaining_percent",
            ),
            (
                r#"
                [injection.recheck]
                poll_interval_ms = 0
                "#,
                "poll_interval_ms",
            ),
            (
                r#"
                [injection.recheck]
                piece_slack_multiplier = 0
                "#,
                "piece_slack_multiplier",
            ),
            (
                r#"
                [injection.recheck]
                max_resume_wait_ms = 0
                "#,
                "max_resume_wait_ms",
            ),
            (
                r#"
                [injection.recheck]
                below_threshold_action = "maybe"
                "#,
                "below_threshold_action",
            ),
        ] {
            let error = parse_config(contents).unwrap_err();

            assert!(
                error.to_string().contains(expected),
                "{error} did not contain {expected}"
            );
        }

        let zero_remaining_percent = parse_config(
            r#"
            [injection.recheck]
            max_remaining_percent = 0.0
            "#,
        )
        .unwrap();

        assert_eq!(
            Some(0.0),
            zero_remaining_percent
                .injection
                .recheck
                .max_remaining_percent
        );
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
    fn injection_link_policy_defaults_to_disabled() {
        let config = parse_config("").unwrap();

        assert_eq!(None, config.injection.link_type);
        assert!(config.injection.link_dirs.is_empty());
        assert!(!config.injection.flat_linking);
    }

    #[test]
    fn parses_injection_link_policy_settings() {
        for (link_type, expected) in [
            ("hardlink", InjectionLinkTypeConfig::Hardlink),
            ("symlink", InjectionLinkTypeConfig::Symlink),
            ("reflink", InjectionLinkTypeConfig::Reflink),
            ("reflink_or_copy", InjectionLinkTypeConfig::ReflinkOrCopy),
        ] {
            let config = parse_config(&format!(
                r#"
                [injection]
                link_type = "{link_type}"
                link_dirs = ["/links"]
                flat_linking = true
                "#
            ))
            .unwrap();

            assert_eq!(Some(expected), config.injection.link_type);
            assert_eq!(vec![PathBuf::from("/links")], config.injection.link_dirs);
            assert!(config.injection.flat_linking);
        }
    }

    #[test]
    fn rejects_invalid_injection_link_policy_settings() {
        for (contents, expected) in [
            (
                r#"
                [injection]
                link_type = "junction"
                link_dirs = ["/links"]
                "#,
                "unknown variant",
            ),
            (
                r#"
                [injection]
                link_type = "hardlink"
                link_dirs = []
                "#,
                "link_dirs must not be empty",
            ),
            (
                r#"
                [injection]
                link_type = "hardlink"
                "#,
                "link_dirs must not be empty",
            ),
        ] {
            let error = parse_config(contents).unwrap_err();
            assert!(
                error.to_string().contains(expected),
                "{error:?} did not contain {expected:?}"
            );
        }
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
        let notification_token_file = cwd.join("notification-token");
        fs::write(&api_token_file, "server-secret\n").unwrap();
        fs::write(&password_file, "super-secret\n").unwrap();
        fs::write(&api_key_file, "api-secret\r\n").unwrap();
        fs::write(&prowlarr_api_key_file, "prowlarr-secret\n").unwrap();
        fs::write(&sonarr_api_key_file, "sonarr-secret\n").unwrap();
        fs::write(&notification_token_file, "notification-secret\n").unwrap();
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

            [notifications.endpoints.ops]
            url = "https://hooks.example/sporos"
            token_file = "{}"
            "#,
            cwd.display(),
            cwd.display(),
            cwd.display(),
            api_token_file.display(),
            password_file.display(),
            api_key_file.display(),
            prowlarr_api_key_file.display(),
            sonarr_api_key_file.display(),
            notification_token_file.display()
        );

        let config = parse_startup_config(&contents, &cwd).unwrap();
        let client = &config.torrent_clients["qbit_main"];
        let indexer = &config.indexers.torznab["example"];
        let prowlarr = &config.indexers.prowlarr["main"];
        let sonarr = &config.indexers.arr.sonarr["main"];
        let endpoint = &config.notifications.endpoints["ops"];

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
        assert_eq!(
            Some("notification-secret"),
            endpoint
                .token
                .as_ref()
                .map(NotificationToken::expose_secret)
        );
        assert!(!format!("{:?}", config.server).contains("server-secret"));
        assert!(!format!("{client:?}").contains("super-secret"));
        assert!(!format!("{indexer:?}").contains("api-secret"));
        assert!(!format!("{prowlarr:?}").contains("prowlarr-secret"));
        assert!(!format!("{sonarr:?}").contains("sonarr-secret"));
        assert!(!format!("{endpoint:?}").contains("notification-secret"));

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
        assert!(CONFIG_SCHEMA.contains("[notifications.endpoints.<name>]"));
        assert!(CONFIG_SCHEMA.contains("[inventory]"));
        assert!(CONFIG_SCHEMA.contains("[injection]"));
        assert!(CONFIG_SCHEMA.contains("link_type"));
        assert!(CONFIG_SCHEMA.contains("link_dirs"));
        assert!(CONFIG_SCHEMA.contains("flat_linking"));
        assert!(CONFIG_SCHEMA.contains("[injection.recheck]"));
        assert!(!CONFIG_SCHEMA.contains("rss_interval"));
        assert!(CONFIG_SCHEMA.contains("media_inventory_interval"));
        assert!(CONFIG_SCHEMA.contains("default_tags = [\"sporos\"]"));
        assert!(CONFIG_SCHEMA.contains("below_threshold_action"));
        assert!(CONFIG_SCHEMA.contains("SPOROS__SERVER__BIND"));
        assert!(CONFIG_SCHEMA.contains("SPOROS__INJECTION__LINK_TYPE"));
        assert!(CONFIG_SCHEMA.contains("SPOROS__INJECTION__FLAT_LINKING"));
        assert!(CONFIG_SCHEMA.contains("SPOROS__INJECTION__RECHECK__SKIP_RECHECK"));
        assert!(CONFIG_SCHEMA.contains("SPOROS__INJECTION__RECHECK__MIN_COMPLETION_PERCENT"));
        assert!(CONFIG_SCHEMA.contains("SPOROS__INJECTION__RECHECK__MAX_REMAINING_PERCENT"));
        assert!(CONFIG_SCHEMA.contains("SPOROS__INJECTION__RECHECK__POLL_INTERVAL_MS"));
        assert!(CONFIG_SCHEMA.contains("SPOROS__INJECTION__RECHECK__MAX_RESUME_WAIT_MS"));
        assert!(CONFIG_SCHEMA.contains("SPOROS__INJECTION__RECHECK__BELOW_THRESHOLD_ACTION"));
        assert!(CONFIG_SCHEMA.contains("SPOROS__TORRENT_CLIENTS__QBIT_MAIN__URL"));
        assert!(CONFIG_SCHEMA.contains("SPOROS__TORRENT_CLIENTS__QBIT_MAIN__DEFAULT_TAGS"));
        assert!(CONFIG_SCHEMA.contains("SPOROS__TORRENT_CLIENTS__RTORRENT_MAIN__DEFAULT_LABEL"));
        assert!(CONFIG_SCHEMA.contains("SPOROS__INDEXERS__PROWLARR__MAIN__API_KEY_FILE"));
        assert!(CONFIG_SCHEMA.contains("SPOROS__NOTIFICATIONS__ENDPOINTS__MAIN__TOKEN_FILE"));
    }

    #[test]
    fn system_config_template_documents_link_policy() {
        let template = include_str!("../docker/system/config/sporos.toml.template");

        assert!(template.contains("[injection]"));
        assert!(template.contains("link_type"));
        assert!(template.contains("link_dirs"));
        assert!(template.contains("flat_linking"));
        assert!(template.contains("media_inventory_interval"));
    }

    #[test]
    fn operator_guide_documents_notification_operations() {
        let guide = include_str!("../docs/operators/operator-guide.md");
        let configuration = include_str!("../docs/configuration.md");
        let guide_example = notification_example(guide);
        let configuration_example = notification_example(configuration);
        let config = parse_config(guide_example).unwrap();
        let endpoint = &config.notifications.endpoints["ops"];

        assert_eq!(configuration_example.trim(), guide_example.trim());
        assert_eq!("https://hooks.example/sporos", endpoint.url);
        assert_eq!(
            Some(Path::new("/var/run/secrets/notification-token")),
            endpoint.token_file.as_deref()
        );
        assert_eq!(None, endpoint.token_env);
        assert_eq!(None, endpoint.token);
        assert_eq!("30s", endpoint.timeout);
        assert_eq!(3, endpoint.retry_max_attempts);
        assert_eq!("1s", endpoint.retry_initial_delay);
        assert_eq!("30s", endpoint.retry_max_delay);

        for expected in [
            "runtime.notification_queue_limit",
            "POST /v1/notifications/test",
            "sporos_notification_requests_total",
            "sporos_notification_request_duration_seconds",
            "sporos_dependency_health_state",
        ] {
            assert!(guide.contains(expected), "missing {expected}");
        }
    }

    fn notification_example(document: &str) -> &str {
        for block in document.split("```toml\n").skip(1) {
            let Some((body, _rest)) = block.split_once("\n```") else {
                continue;
            };
            if body.starts_with("[notifications.endpoints.ops]") {
                return body;
            }
        }
        panic!("missing notification TOML example");
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
                (
                    "SPOROS__INJECTION__RECHECK__MIN_COMPLETION_PERCENT".to_owned(),
                    "85.0".to_owned(),
                ),
                (
                    "SPOROS__INJECTION__RECHECK__BELOW_THRESHOLD_ACTION".to_owned(),
                    "reject_without_injecting".to_owned(),
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
        assert_eq!(Some(85.0), config.injection.recheck.min_completion_percent);
        assert_eq!(
            BelowThresholdActionConfig::RejectWithoutInjecting,
            config.injection.recheck.below_threshold_action
        );
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

            [notifications.endpoints.ops]
            url = "https://old-hooks.example/sporos"
            token_env = "SPOROS_NOTIFICATION_TOKEN"
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
                (
                    "SPOROS__NOTIFICATIONS__ENDPOINTS__OPS__URL".to_owned(),
                    "https://hooks.example/sporos".to_owned(),
                ),
                ("QBIT_PASSWORD".to_owned(), "super-secret".to_owned()),
                ("INDEXER_API_KEY".to_owned(), "api-secret".to_owned()),
                ("PROWLARR_API_KEY".to_owned(), "prowlarr-secret".to_owned()),
                ("RADARR_API_KEY".to_owned(), "radarr-secret".to_owned()),
                (
                    "SPOROS_NOTIFICATION_TOKEN".to_owned(),
                    "notification-secret".to_owned(),
                ),
            ],
        )
        .unwrap();
        let client = &config.torrent_clients["qbit_main"];
        let indexer = &config.indexers.torznab["example"];
        let prowlarr = &config.indexers.prowlarr["main"];
        let radarr = &config.indexers.arr.radarr["main"];
        let endpoint = &config.notifications.endpoints["ops"];

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
        assert_eq!("https://hooks.example/sporos", endpoint.url);
        assert_eq!(
            Some("notification-secret"),
            endpoint
                .token
                .as_ref()
                .map(NotificationToken::expose_secret)
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
    fn torrent_clients_reject_secret_bearing_urls() {
        for (url, expected) in [
            ("file:///var/lib/qbittorrent", "http or https"),
            (
                "http://user:pass@qbittorrent:8080",
                "userinfo is not supported",
            ),
            ("http://qbittorrent:8080?token=secret", "query parameters"),
            ("http://qbittorrent:8080#token=secret", "fragments"),
        ] {
            let error = parse_config(&format!(
                r#"
                [torrent_clients.qbit_main]
                kind = "qbittorrent"
                url = "{url}"
                default_save_path = "/downloads"
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
