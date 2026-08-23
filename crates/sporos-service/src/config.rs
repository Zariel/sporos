use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use reqwest::Url;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

const DEFAULT_PATH: &str = "/config/sporos.toml";
const MAX_SECRET_BYTES: u64 = 64 * 1024;

#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    pub(crate) fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn expose(&self) -> &str {
        &self.0
    }

    pub fn resolve(
        field: &str,
        direct: Option<String>,
        file: Option<PathBuf>,
    ) -> Result<Self, ConfigError> {
        resolve_secret(field, direct, file)
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Secret([REDACTED])")
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub server: Server,
    pub auth: Auth,
    pub runtime: Runtime,
    pub limits: Limits,
    pub logging: Logging,
    pub metrics: Metrics,
    pub qbittorrent: Option<Qbittorrent>,
}

#[derive(Debug, Clone)]
pub struct Qbittorrent {
    pub url: Url,
    pub api_key: Secret,
    pub request_timeout: Duration,
    pub sync_interval: Duration,
    pub full_reconcile_interval: Duration,
    pub inventory_batch_size: usize,
    pub database_batch_size: usize,
    pub inventory_stale_after: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Server {
    pub bind: SocketAddr,
    #[serde(with = "duration")]
    pub request_timeout: Duration,
    #[serde(with = "duration")]
    pub shutdown_grace: Duration,
    pub autobrr_body_limit_bytes: usize,
    pub admin_body_limit_bytes: usize,
}

impl Default for Server {
    fn default() -> Self {
        Self {
            bind: "0.0.0.0:8080".parse().expect("valid default bind"),
            request_timeout: Duration::from_secs(30),
            shutdown_grace: Duration::from_secs(30),
            autobrr_body_limit_bytes: 12 * 1024 * 1024,
            admin_body_limit_bytes: 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Auth {
    pub webhook_token: Secret,
    pub admin_token: Secret,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Runtime {
    pub data_dir: PathBuf,
    pub database_path: PathBuf,
    pub lock_path: PathBuf,
    #[serde(with = "duration")]
    pub lock_wait: Duration,
}

impl Default for Runtime {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("/data"),
            database_path: PathBuf::from("/data/sporos.db"),
            lock_path: PathBuf::from("/data/sporos.lock"),
            lock_wait: Duration::ZERO,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Limits {
    pub max_http_requests: usize,
    pub max_candidate_workflows: usize,
    pub max_search_workflows: usize,
    pub max_indexer_requests: usize,
    pub max_filesystem_operations: usize,
    pub max_uploads: usize,
    pub outbox_batch_size: usize,
    pub internal_channel_capacity: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_http_requests: 16,
            max_candidate_workflows: 8,
            max_search_workflows: 4,
            max_indexer_requests: 4,
            max_filesystem_operations: 4,
            max_uploads: 4,
            outbox_batch_size: 32,
            internal_channel_capacity: 128,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Logging {
    pub format: LogFormat,
    pub level: String,
}

impl Default for Logging {
    fn default() -> Self {
        Self {
            format: LogFormat::Json,
            level: "info".to_owned(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LogFormat {
    Json,
    Pretty,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Metrics {
    pub enabled: bool,
}

impl Default for Metrics {
    fn default() -> Self {
        Self { enabled: true }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct RawConfig {
    server: Server,
    auth: RawAuth,
    runtime: Runtime,
    limits: Limits,
    logging: Logging,
    metrics: Metrics,
    qbittorrent: Option<RawQbittorrent>,
    prowlarr: Option<ServiceConfig>,
    arr: ArrConfig,
    paths: PathsConfig,
    sources: SourceFilters,
    matching: MatchingConfig,
    injection: InjectionConfig,
    data_scan: DataScanConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawQbittorrent {
    url: String,
    api_key: Option<String>,
    api_key_file: Option<PathBuf>,
    #[serde(with = "duration")]
    request_timeout: Duration,
    #[serde(with = "duration")]
    sync_interval: Duration,
    #[serde(with = "duration")]
    full_reconcile_interval: Duration,
    inventory_batch_size: usize,
    database_batch_size: usize,
    #[serde(with = "duration")]
    inventory_stale_after: Duration,
}

impl Default for RawQbittorrent {
    fn default() -> Self {
        Self {
            url: String::new(),
            api_key: None,
            api_key_file: None,
            request_timeout: Duration::from_secs(30),
            sync_interval: Duration::from_secs(10),
            full_reconcile_interval: Duration::from_secs(6 * 60 * 60),
            inventory_batch_size: 500,
            database_batch_size: 200,
            inventory_stale_after: Duration::from_secs(5 * 60),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct RawAuth {
    webhook_token: Option<String>,
    webhook_token_file: Option<PathBuf>,
    admin_token: Option<String>,
    admin_token_file: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ServiceConfig {
    url: String,
    api_key: Option<String>,
    api_key_file: Option<PathBuf>,
    #[serde(with = "duration")]
    request_timeout: Duration,
    #[serde(flatten)]
    options: BTreeMap<String, toml::Value>,
}

impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            api_key: None,
            api_key_file: None,
            request_timeout: Duration::from_secs(30),
            options: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct ArrConfig {
    sonarr: BTreeMap<String, ServiceConfig>,
    radarr: BTreeMap<String, ServiceConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct PathsConfig {
    link_root: Option<PathBuf>,
    rewrite: Vec<PathRewrite>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PathRewrite {
    name: String,
    remote: PathBuf,
    local: PathBuf,
    services: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct SourceFilters {
    include_categories: Vec<String>,
    exclude_categories: Vec<String>,
    include_tags: Vec<String>,
    exclude_tags: Vec<String>,
    exclude_sporos_managed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct MatchingConfig {
    mode: String,
    season_from_episodes: bool,
    preflight_size_tolerance: f64,
    max_torrent_bytes: usize,
    max_files_per_torrent: usize,
    max_path_bytes: usize,
    #[serde(with = "duration")]
    pending_source_timeout: Duration,
    video_extensions: Vec<String>,
    optional_extensions: Vec<String>,
}

impl Default for MatchingConfig {
    fn default() -> Self {
        Self {
            mode: "partial".to_owned(),
            season_from_episodes: true,
            preflight_size_tolerance: 0.02,
            max_torrent_bytes: 8 * 1024 * 1024,
            max_files_per_torrent: 100_000,
            max_path_bytes: 4096,
            pending_source_timeout: Duration::from_secs(7 * 24 * 60 * 60),
            video_extensions: Vec::new(),
            optional_extensions: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct InjectionConfig {
    dry_run: bool,
    category_template: String,
    tag_templates: Vec<String>,
    inherit_source_category: bool,
    inherit_source_tags: bool,
    resume: ResumeConfig,
}

impl Default for InjectionConfig {
    fn default() -> Self {
        Self {
            dry_run: false,
            category_template: "sporos".to_owned(),
            tag_templates: Vec::new(),
            inherit_source_category: false,
            inherit_source_tags: false,
            resume: ResumeConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ResumeConfig {
    mode: String,
    combine: String,
}

impl Default for ResumeConfig {
    fn default() -> Self {
        Self {
            mode: "complete_only".to_owned(),
            combine: "and".to_owned(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct DataScanConfig {
    roots: BTreeMap<String, DataRoot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DataRoot {
    path: PathBuf,
    max_depth: usize,
    max_releases: usize,
    max_files_per_release: usize,
}

impl Config {
    pub fn load() -> Result<Self, ConfigError> {
        let configured_path = std::env::var_os("SPOROS_CONFIG").map(PathBuf::from);
        let path = configured_path
            .as_deref()
            .unwrap_or_else(|| Path::new(DEFAULT_PATH));
        let required = configured_path.is_some();
        load(path, required, std::env::vars())
    }
}

fn load(
    path: &Path,
    required: bool,
    environment: impl IntoIterator<Item = (String, String)>,
) -> Result<Config, ConfigError> {
    let mut value = toml::Value::try_from(RawConfig::default())?;
    match fs::read_to_string(path) {
        Ok(contents) => merge(&mut value, toml::from_str(&contents)?),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && !required => {}
        Err(source) => {
            return Err(ConfigError::ReadConfig {
                path: path.to_owned(),
                source,
            });
        }
    }

    for (key, raw) in environment {
        let Some(path) = key.strip_prefix("SPOROS__") else {
            continue;
        };
        let path = path
            .split("__")
            .map(str::to_ascii_lowercase)
            .collect::<Vec<_>>();
        if path.iter().any(String::is_empty) {
            return Err(ConfigError::InvalidEnvironmentKey(key));
        }
        let parsed = toml::from_str::<toml::Table>(&format!("value = {raw}"))
            .map_err(|source| ConfigError::InvalidEnvironmentValue {
                key: key.clone(),
                source,
            })?
            .remove("value")
            .expect("environment wrapper has value");
        set_path(&mut value, &path, parsed, &key)?;
    }

    let raw: RawConfig = value.try_into()?;
    validate_positive(&raw)?;
    let webhook_token = resolve_secret(
        "auth.webhook_token",
        raw.auth.webhook_token,
        raw.auth.webhook_token_file,
    )?;
    let admin_token = resolve_secret(
        "auth.admin_token",
        raw.auth.admin_token,
        raw.auth.admin_token_file,
    )?;
    if webhook_token == admin_token {
        return Err(ConfigError::SharedAuthToken);
    }

    let qbittorrent = raw
        .qbittorrent
        .map(|service| -> Result<Qbittorrent, ConfigError> {
            let url = Url::parse(&service.url).map_err(|_| ConfigError::QbittorrentUrl)?;
            let api_key =
                resolve_secret("qbittorrent.api_key", service.api_key, service.api_key_file)?;
            Ok(Qbittorrent {
                url,
                api_key,
                request_timeout: service.request_timeout,
                sync_interval: service.sync_interval,
                full_reconcile_interval: service.full_reconcile_interval,
                inventory_batch_size: service.inventory_batch_size,
                database_batch_size: service.database_batch_size,
                inventory_stale_after: service.inventory_stale_after,
            })
        })
        .transpose()?;
    validate_optional_secrets(raw.prowlarr.as_ref(), "prowlarr.api_key")?;
    for (kind, services) in [("sonarr", &raw.arr.sonarr), ("radarr", &raw.arr.radarr)] {
        for (name, service) in services {
            validate_optional_secrets(service.into(), &format!("arr.{kind}.{name}.api_key"))?;
        }
    }

    Ok(Config {
        server: raw.server,
        auth: Auth {
            webhook_token,
            admin_token,
        },
        runtime: raw.runtime,
        limits: raw.limits,
        logging: raw.logging,
        metrics: raw.metrics,
        qbittorrent,
    })
}

fn validate_positive(config: &RawConfig) -> Result<(), ConfigError> {
    let mut values = vec![
        (
            "server.autobrr_body_limit_bytes",
            config.server.autobrr_body_limit_bytes,
        ),
        (
            "server.admin_body_limit_bytes",
            config.server.admin_body_limit_bytes,
        ),
        ("limits.max_http_requests", config.limits.max_http_requests),
        (
            "limits.max_candidate_workflows",
            config.limits.max_candidate_workflows,
        ),
        (
            "limits.max_search_workflows",
            config.limits.max_search_workflows,
        ),
        (
            "limits.max_indexer_requests",
            config.limits.max_indexer_requests,
        ),
        (
            "limits.max_filesystem_operations",
            config.limits.max_filesystem_operations,
        ),
        ("limits.max_uploads", config.limits.max_uploads),
        ("limits.outbox_batch_size", config.limits.outbox_batch_size),
        (
            "limits.internal_channel_capacity",
            config.limits.internal_channel_capacity,
        ),
    ];
    if let Some(qbittorrent) = &config.qbittorrent {
        values.extend([
            (
                "qbittorrent.inventory_batch_size",
                qbittorrent.inventory_batch_size,
            ),
            (
                "qbittorrent.database_batch_size",
                qbittorrent.database_batch_size,
            ),
        ]);
        if qbittorrent.inventory_batch_size > 500 {
            return Err(ConfigError::LimitTooLarge {
                field: "qbittorrent.inventory_batch_size",
                maximum: 500,
            });
        }
        if qbittorrent.database_batch_size > 500 {
            return Err(ConfigError::LimitTooLarge {
                field: "qbittorrent.database_batch_size",
                maximum: 500,
            });
        }
        for (field, value) in [
            ("qbittorrent.request_timeout", qbittorrent.request_timeout),
            ("qbittorrent.sync_interval", qbittorrent.sync_interval),
            (
                "qbittorrent.full_reconcile_interval",
                qbittorrent.full_reconcile_interval,
            ),
            (
                "qbittorrent.inventory_stale_after",
                qbittorrent.inventory_stale_after,
            ),
        ] {
            if value.is_zero() {
                return Err(ConfigError::ZeroLimit(field));
            }
        }
    }
    if let Some((field, _)) = values.into_iter().find(|(_, value)| *value == 0) {
        return Err(ConfigError::ZeroLimit(field));
    }
    Ok(())
}

fn validate_optional_secrets(
    service: Option<&ServiceConfig>,
    field: &str,
) -> Result<(), ConfigError> {
    let Some(service) = service else {
        return Ok(());
    };
    let _ = resolve_secret(field, service.api_key.clone(), service.api_key_file.clone())?;
    Ok(())
}

fn resolve_secret(
    field: &str,
    direct: Option<String>,
    file: Option<PathBuf>,
) -> Result<Secret, ConfigError> {
    match (direct, file) {
        (Some(_), Some(_)) => Err(ConfigError::ConflictingSecretSources(field.to_owned())),
        (None, None) => Err(ConfigError::MissingSecret(field.to_owned())),
        (Some(value), None) => secret(field, value),
        (None, Some(path)) => read_secret(field, &path),
    }
}

fn secret(field: &str, value: String) -> Result<Secret, ConfigError> {
    if value.is_empty() {
        Err(ConfigError::EmptySecret(field.to_owned()))
    } else if value.len() as u64 > MAX_SECRET_BYTES {
        Err(ConfigError::SecretTooLarge(field.to_owned()))
    } else {
        Ok(Secret::new(value))
    }
}

fn read_secret(field: &str, path: &Path) -> Result<Secret, ConfigError> {
    let metadata = fs::symlink_metadata(path).map_err(|source| ConfigError::ReadSecret {
        field: field.to_owned(),
        path: path.to_owned(),
        source,
    })?;
    if !metadata.file_type().is_file() {
        return Err(ConfigError::SecretNotRegular {
            field: field.to_owned(),
            path: path.to_owned(),
        });
    }
    if metadata.len() > MAX_SECRET_BYTES {
        return Err(ConfigError::SecretTooLarge(field.to_owned()));
    }
    let mut value = fs::read_to_string(path).map_err(|source| ConfigError::ReadSecret {
        field: field.to_owned(),
        path: path.to_owned(),
        source,
    })?;
    if value.ends_with('\n') {
        value.pop();
        if value.ends_with('\r') {
            value.pop();
        }
    }
    secret(field, value)
}

fn merge(base: &mut toml::Value, overlay: toml::Value) {
    match (base, overlay) {
        (toml::Value::Table(base), toml::Value::Table(overlay)) => {
            for (key, value) in overlay {
                match base.get_mut(&key) {
                    Some(existing) => merge(existing, value),
                    None => {
                        base.insert(key, value);
                    }
                }
            }
        }
        (base, overlay) => *base = overlay,
    }
}

fn set_path(
    root: &mut toml::Value,
    path: &[String],
    value: toml::Value,
    environment_key: &str,
) -> Result<(), ConfigError> {
    let mut current = root;
    for segment in &path[..path.len() - 1] {
        let table = current
            .as_table_mut()
            .ok_or_else(|| ConfigError::UnknownEnvironmentKey(environment_key.to_owned()))?;
        current = table
            .entry(segment)
            .or_insert_with(|| toml::Value::Table(toml::Table::new()));
    }
    let table = current
        .as_table_mut()
        .ok_or_else(|| ConfigError::UnknownEnvironmentKey(environment_key.to_owned()))?;
    let leaf = path.last().expect("environment path is not empty");
    table.insert(leaf.clone(), value);
    Ok(())
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read configuration file {path}")]
    ReadConfig {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid TOML configuration")]
    Toml(#[from] toml::de::Error),
    #[error("failed to construct default configuration")]
    Serialize(#[from] toml::ser::Error),
    #[error("invalid environment key {0}")]
    InvalidEnvironmentKey(String),
    #[error("unknown environment key {0}")]
    UnknownEnvironmentKey(String),
    #[error("invalid TOML value in environment key {key}")]
    InvalidEnvironmentValue {
        key: String,
        #[source]
        source: toml::de::Error,
    },
    #[error("{0} must be greater than zero")]
    ZeroLimit(&'static str),
    #[error("{field} must not exceed {maximum}")]
    LimitTooLarge { field: &'static str, maximum: usize },
    #[error("invalid qBittorrent URL")]
    QbittorrentUrl,
    #[error("{0} must specify either a direct value or a file")]
    MissingSecret(String),
    #[error("{0} cannot specify both a direct value and a file")]
    ConflictingSecretSources(String),
    #[error("{0} cannot be empty")]
    EmptySecret(String),
    #[error("{0} exceeds the secret size limit")]
    SecretTooLarge(String),
    #[error("secret file for {field} is not a regular file: {path}")]
    SecretNotRegular { field: String, path: PathBuf },
    #[error("failed to read secret file for {field}: {path}")]
    ReadSecret {
        field: String,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("webhook and administrative tokens must differ")]
    SharedAuthToken,
}

mod duration {
    use super::*;

    pub fn serialize<S>(value: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format_duration(*value))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        parse_duration(&value).map_err(serde::de::Error::custom)
    }

    fn format_duration(value: Duration) -> String {
        if value.subsec_nanos() == 0 {
            format!("{}s", value.as_secs())
        } else {
            format!("{}ms", value.as_millis())
        }
    }

    fn parse_duration(value: &str) -> Result<Duration, &'static str> {
        let (number, unit) = value
            .find(|character: char| !character.is_ascii_digit())
            .map(|index| value.split_at(index))
            .ok_or("duration requires a unit")?;
        let number = number.parse::<u64>().map_err(|_| "invalid duration")?;
        let seconds = match unit {
            "ms" => return Ok(Duration::from_millis(number)),
            "s" => number,
            "m" => number.checked_mul(60).ok_or("duration overflow")?,
            "h" => number.checked_mul(60 * 60).ok_or("duration overflow")?,
            "d" => number
                .checked_mul(24 * 60 * 60)
                .ok_or("duration overflow")?,
            _ => return Err("unsupported duration unit"),
        };
        Ok(Duration::from_secs(seconds))
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::symlink;

    use tempfile::TempDir;

    use super::*;

    #[test]
    fn applies_file_then_environment() {
        let directory = TempDir::new().expect("create temporary directory");
        let path = directory.path().join("sporos.toml");
        fs::write(
            &path,
            r#"
                [server]
                bind = "127.0.0.1:9000"
                [auth]
                webhook_token = "webhook"
                admin_token = "admin"
            "#,
        )
        .expect("write configuration");

        let config = load(
            &path,
            true,
            [(
                "SPOROS__SERVER__BIND".to_owned(),
                "\"127.0.0.1:9001\"".to_owned(),
            )],
        )
        .expect("load configuration");

        assert_eq!(config.server.bind, "127.0.0.1:9001".parse().unwrap());
        assert_eq!(config.auth.webhook_token.expose(), "webhook");
        assert_eq!(config.runtime.database_path, Path::new("/data/sporos.db"));
    }

    #[test]
    fn accepts_environment_only_secrets() {
        let directory = TempDir::new().expect("create temporary directory");
        let config = load(
            &directory.path().join("missing.toml"),
            false,
            [
                (
                    "SPOROS__AUTH__WEBHOOK_TOKEN".to_owned(),
                    "\"webhook\"".to_owned(),
                ),
                (
                    "SPOROS__AUTH__ADMIN_TOKEN".to_owned(),
                    "\"admin\"".to_owned(),
                ),
            ],
        )
        .expect("load environment configuration");

        assert_eq!(config.auth.webhook_token.expose(), "webhook");
        assert_eq!(config.auth.admin_token.expose(), "admin");
    }

    #[test]
    fn loads_bounded_qbittorrent_settings() {
        let config = load_config(
            r#"
                [auth]
                webhook_token = "webhook"
                admin_token = "admin"
                [qbittorrent]
                url = "http://qbittorrent:8080"
                api_key = "qbt_0123456789abcdefghijklmnopqr"
                sync_interval = "15s"
                inventory_batch_size = 400
                database_batch_size = 100
            "#,
        )
        .expect("load qBittorrent settings");
        let qbittorrent = config.qbittorrent.expect("qBittorrent configured");

        assert_eq!(qbittorrent.url.as_str(), "http://qbittorrent:8080/");
        assert_eq!(qbittorrent.sync_interval, Duration::from_secs(15));
        assert_eq!(qbittorrent.inventory_batch_size, 400);
        assert_eq!(qbittorrent.database_batch_size, 100);
        assert!(!format!("{qbittorrent:?}").contains(qbittorrent.api_key.expose()));
    }

    #[test]
    fn rejects_unbounded_qbittorrent_batches() {
        let error = load_config(
            r#"
                [auth]
                webhook_token = "webhook"
                admin_token = "admin"
                [qbittorrent]
                url = "http://qbittorrent:8080"
                api_key = "secret"
                inventory_batch_size = 501
            "#,
        )
        .expect_err("reject unbounded inventory pages");

        assert!(matches!(error, ConfigError::LimitTooLarge { .. }));
    }

    #[test]
    fn reads_regular_secret_files_and_one_newline() {
        let directory = TempDir::new().expect("create temporary directory");
        let webhook = directory.path().join("webhook");
        let admin = directory.path().join("admin");
        fs::write(&webhook, "webhook\n\n").expect("write webhook token");
        fs::write(&admin, "admin\r\n").expect("write admin token");
        let path = directory.path().join("sporos.toml");
        fs::write(
            &path,
            format!(
                "[auth]\nwebhook_token_file = {:?}\nadmin_token_file = {:?}\n",
                webhook.display().to_string(),
                admin.display().to_string()
            ),
        )
        .expect("write configuration");

        let config = load(&path, true, []).expect("load configuration");
        assert_eq!(config.auth.webhook_token.expose(), "webhook\n");
        assert_eq!(config.auth.admin_token.expose(), "admin");
        assert_eq!(
            format!("{:?}", config.auth),
            "Auth { webhook_token: Secret([REDACTED]), admin_token: Secret([REDACTED]) }"
        );
    }

    #[test]
    fn rejects_secret_source_conflicts() {
        let error = load_config(
            "[auth]\nwebhook_token = \"webhook\"\nwebhook_token_file = \"token\"\nadmin_token = \"admin\"\n",
        )
        .expect_err("reject conflicting secret sources");
        assert!(matches!(error, ConfigError::ConflictingSecretSources(_)));
    }

    #[test]
    fn rejects_shared_auth_tokens() {
        let error = load_config("[auth]\nwebhook_token = \"shared\"\nadmin_token = \"shared\"\n")
            .expect_err("reject shared auth tokens");
        assert!(matches!(error, ConfigError::SharedAuthToken));
    }

    #[test]
    fn rejects_symlinked_secret_files() {
        let directory = TempDir::new().expect("create temporary directory");
        let target = directory.path().join("target");
        let link = directory.path().join("link");
        fs::write(&target, "webhook").expect("write secret");
        symlink(&target, &link).expect("create symlink");

        let error = read_secret("auth.webhook_token", &link).expect_err("reject symlink");
        assert!(matches!(error, ConfigError::SecretNotRegular { .. }));
    }

    #[test]
    fn rejects_unknown_and_unquoted_environment_values() {
        let error =
            load_config_with_env("SPOROS__SERVER__TYPO", "1").expect_err("reject unknown key");
        assert!(matches!(error, ConfigError::Toml(_)));

        let error = load_config_with_env("SPOROS__SERVER__BIND", "127.0.0.1:9001")
            .expect_err("reject non-TOML string");
        assert!(matches!(error, ConfigError::InvalidEnvironmentValue { .. }));
    }

    fn load_config(contents: &str) -> Result<Config, ConfigError> {
        let directory = TempDir::new().expect("create temporary directory");
        let path = directory.path().join("sporos.toml");
        fs::write(&path, contents).expect("write configuration");
        load(&path, true, [])
    }

    fn load_config_with_env(key: &str, value: &str) -> Result<Config, ConfigError> {
        let directory = TempDir::new().expect("create temporary directory");
        let path = directory.path().join("sporos.toml");
        fs::write(
            &path,
            "[auth]\nwebhook_token = \"webhook\"\nadmin_token = \"admin\"\n",
        )
        .expect("write configuration");
        load(&path, true, [(key.to_owned(), value.to_owned())])
    }
}
