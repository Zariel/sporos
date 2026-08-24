use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use reqwest::Url;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sporos_model::MatchingPolicy;
use thiserror::Error;

const DEFAULT_PATH: &str = "/config/sporos.toml";
const MAX_SECRET_BYTES: u64 = 64 * 1024;

#[derive(Clone)]
pub struct Secret(String);

impl Secret {
    pub(crate) fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn expose(&self) -> &str {
        &self.0
    }

    pub fn parse(field: &str, value: String) -> Result<Self, ConfigError> {
        secret(field, value)
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
    pub prowlarr: Option<Prowlarr>,
    pub arr: Vec<ArrInstance>,
    pub sources: SourceFilters,
    pub matching: Matching,
    pub injection: Injection,
    pub paths: Paths,
    pub data_roots: BTreeMap<String, DataRoot>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArrKind {
    Sonarr,
    Radarr,
}

#[derive(Debug, Clone)]
pub struct ArrInstance {
    pub kind: ArrKind,
    pub name: String,
    pub url: Url,
    pub api_key: Secret,
    pub request_timeout: Duration,
}

#[derive(Debug, Clone)]
pub struct Qbittorrent {
    pub url: Url,
    pub api_key: Option<Secret>,
    pub request_timeout: Duration,
    pub sync_interval: Duration,
    pub full_reconcile_interval: Duration,
    pub inventory_batch_size: usize,
    pub database_batch_size: usize,
    pub inventory_stale_after: Duration,
}

#[derive(Debug, Clone)]
pub struct Prowlarr {
    pub url: Url,
    pub api_key: Secret,
    pub request_timeout: Duration,
    pub refresh_interval: Duration,
    pub include_tags: Vec<i64>,
    pub exclude_tags: Vec<i64>,
    pub require_proxy_downloads: bool,
    pub max_results_per_query: usize,
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
    pub api_key: Option<Secret>,
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
    prowlarr: Option<RawProwlarr>,
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
            request_timeout: Duration::from_secs(30),
            sync_interval: Duration::from_secs(10),
            full_reconcile_interval: Duration::from_secs(6 * 60 * 60),
            inventory_batch_size: 500,
            database_batch_size: 200,
            inventory_stale_after: Duration::from_secs(5 * 60),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawProwlarr {
    url: String,
    api_key: Option<String>,
    #[serde(with = "duration")]
    request_timeout: Duration,
    #[serde(with = "duration")]
    refresh_interval: Duration,
    include_tags: Vec<i64>,
    exclude_tags: Vec<i64>,
    require_proxy_downloads: bool,
    max_results_per_query: usize,
}

impl Default for RawProwlarr {
    fn default() -> Self {
        Self {
            url: String::new(),
            api_key: None,
            request_timeout: Duration::from_secs(30),
            refresh_interval: Duration::from_secs(5 * 60),
            include_tags: Vec::new(),
            exclude_tags: Vec::new(),
            require_proxy_downloads: true,
            max_results_per_query: 100,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct RawAuth {
    api_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ServiceConfig {
    url: String,
    api_key: Option<String>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct PathsConfig {
    link_root: Option<PathBuf>,
    rewrite: Vec<PathRewrite>,
}

impl Default for PathsConfig {
    fn default() -> Self {
        Self {
            link_root: Some(PathBuf::from("/data/links")),
            rewrite: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PathRewrite {
    pub name: String,
    pub remote: PathBuf,
    pub local: PathBuf,
    pub services: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Paths {
    pub link_root: PathBuf,
    pub rewrite: Vec<PathRewrite>,
}

impl Default for Paths {
    fn default() -> Self {
        Self {
            link_root: PathBuf::from("/data/links"),
            rewrite: Vec::new(),
        }
    }
}

impl Paths {
    pub fn qbit_link_root(&self) -> Option<PathBuf> {
        self.local_to_remote("qbittorrent", &self.link_root)
    }

    pub fn remote_to_local(&self, service: &str, path: &Path) -> Option<PathBuf> {
        rewrite_path(&self.rewrite, service, path, |rewrite| {
            (&rewrite.remote, &rewrite.local)
        })
    }

    pub fn local_to_remote(&self, service: &str, path: &Path) -> Option<PathBuf> {
        rewrite_path(&self.rewrite, service, path, |rewrite| {
            (&rewrite.local, &rewrite.remote)
        })
    }
}

fn rewrite_path<'a>(
    rewrites: &'a [PathRewrite],
    service: &str,
    path: &Path,
    direction: impl Fn(&'a PathRewrite) -> (&'a Path, &'a Path),
) -> Option<PathBuf> {
    rewrites
        .iter()
        .filter(|rewrite| {
            rewrite
                .services
                .iter()
                .any(|candidate| candidate == service)
        })
        .filter_map(|rewrite| {
            let (from, to) = direction(rewrite);
            path.strip_prefix(from)
                .ok()
                .map(|suffix| (from.components().count(), to.join(suffix)))
        })
        .max_by_key(|(length, _)| *length)
        .map(|(_, rewritten)| rewritten)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SourceFilters {
    pub include_categories: Vec<String>,
    pub exclude_categories: Vec<String>,
    pub include_tags: Vec<String>,
    pub exclude_tags: Vec<String>,
    pub exclude_sporos_managed: bool,
}

impl Default for SourceFilters {
    fn default() -> Self {
        Self {
            include_categories: Vec::new(),
            exclude_categories: Vec::new(),
            include_tags: Vec::new(),
            exclude_tags: vec!["no-sporos".to_owned()],
            exclude_sporos_managed: true,
        }
    }
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
    max_assignment_files: usize,
    max_candidate_edges: usize,
    max_assignment_component_files: usize,
    max_assignment_operations: u64,
    #[serde(with = "duration")]
    pending_source_timeout: Duration,
    video_extensions: Vec<String>,
    optional_extensions: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Matching {
    pub policy: MatchingPolicy,
    pub preflight_size_tolerance: f64,
    pub max_torrent_bytes: usize,
    pub max_files_per_torrent: usize,
    pub max_path_bytes: usize,
    #[serde(with = "duration")]
    pub pending_source_timeout: Duration,
}

impl Default for Matching {
    fn default() -> Self {
        matching(&MatchingConfig::default()).expect("default matching configuration is valid")
    }
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
            max_assignment_files: 4_096,
            max_candidate_edges: 100_000,
            max_assignment_component_files: 128,
            max_assignment_operations: 50_000_000,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Injection {
    pub dry_run: bool,
    pub category_template: String,
    pub tag_templates: Vec<String>,
    pub inherit_source_category: bool,
    pub inherit_source_tags: bool,
    pub resume: ResumePolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum ResumePolicy {
    Never,
    CompleteOnly,
    Threshold {
        max_missing_bytes: Option<u64>,
        min_present_ratio_ppm: Option<u32>,
        combine: ThresholdCombination,
    },
    Always,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ThresholdCombination {
    And,
    Or,
}

impl Default for Injection {
    fn default() -> Self {
        let raw = InjectionConfig::default();
        Self {
            dry_run: raw.dry_run,
            category_template: raw.category_template,
            tag_templates: raw.tag_templates,
            inherit_source_category: raw.inherit_source_category,
            inherit_source_tags: raw.inherit_source_tags,
            resume: resume_policy(&raw.resume).expect("default resume policy is valid"),
        }
    }
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
    max_missing_bytes: Option<u64>,
    min_present_ratio: Option<f64>,
}

impl Default for ResumeConfig {
    fn default() -> Self {
        Self {
            mode: "complete_only".to_owned(),
            combine: "and".to_owned(),
            max_missing_bytes: None,
            min_present_ratio: None,
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
pub struct DataRoot {
    pub path: PathBuf,
    pub max_depth: usize,
    pub max_releases: usize,
    pub max_files_per_release: usize,
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
        let parsed = if path.last().is_some_and(|field| field == "api_key") {
            toml::Value::String(raw)
        } else {
            toml::from_str::<toml::Table>(&format!("value = {raw}"))
                .map_err(|source| ConfigError::InvalidEnvironmentValue {
                    key: key.clone(),
                    source,
                })?
                .remove("value")
                .expect("environment wrapper has value")
        };
        set_path(&mut value, &path, parsed, &key)?;
    }

    let raw: RawConfig = value.try_into()?;
    validate_positive(&raw)?;
    let matching = matching(&raw.matching)?;
    let injection = injection(&raw.injection)?;
    let paths = Paths {
        link_root: raw
            .paths
            .link_root
            .clone()
            .expect("default paths include a link root"),
        rewrite: raw.paths.rewrite.clone(),
    };
    validate_paths(&paths)?;
    validate_data_roots(&raw.data_scan.roots)?;
    let api_key = optional_secret("auth.api_key", raw.auth.api_key)?;

    let qbittorrent = raw
        .qbittorrent
        .map(|service| -> Result<Qbittorrent, ConfigError> {
            let url = Url::parse(&service.url).map_err(|_| ConfigError::QbittorrentUrl)?;
            let api_key = optional_secret("qbittorrent.api_key", service.api_key)?;
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
    let prowlarr = raw
        .prowlarr
        .map(|service| -> Result<Prowlarr, ConfigError> {
            let url = service_url(&service.url).ok_or(ConfigError::ProwlarrUrl)?;
            let api_key = required_secret("prowlarr.api_key", service.api_key)?;
            Ok(Prowlarr {
                url,
                api_key,
                request_timeout: service.request_timeout,
                refresh_interval: service.refresh_interval,
                include_tags: service.include_tags,
                exclude_tags: service.exclude_tags,
                require_proxy_downloads: service.require_proxy_downloads,
                max_results_per_query: service.max_results_per_query,
            })
        })
        .transpose()?;
    if raw.arr.sonarr.len().saturating_add(raw.arr.radarr.len()) > 32 {
        return Err(ConfigError::TooManyArrInstances);
    }
    let mut arr = Vec::new();
    for (kind_name, kind, services) in [
        ("sonarr", ArrKind::Sonarr, raw.arr.sonarr),
        ("radarr", ArrKind::Radarr, raw.arr.radarr),
    ] {
        for (name, service) in services {
            if name.is_empty()
                || name.len() > 64
                || !name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
            {
                return Err(ConfigError::ArrName {
                    kind: kind_name,
                    name,
                });
            }
            let url = Url::parse(&service.url).map_err(|_| ConfigError::ArrUrl {
                kind: kind_name,
                name: name.clone(),
            })?;
            let api_key =
                required_secret(&format!("arr.{kind_name}.{name}.api_key"), service.api_key)?;
            arr.push(ArrInstance {
                kind,
                name,
                url,
                api_key,
                request_timeout: service.request_timeout,
            });
        }
    }

    Ok(Config {
        server: raw.server,
        auth: Auth { api_key },
        runtime: raw.runtime,
        limits: raw.limits,
        logging: raw.logging,
        metrics: raw.metrics,
        qbittorrent,
        prowlarr,
        arr,
        sources: raw.sources,
        matching,
        injection,
        paths,
        data_roots: raw.data_scan.roots,
    })
}

fn validate_data_roots(roots: &BTreeMap<String, DataRoot>) -> Result<(), ConfigError> {
    for (name, root) in roots {
        if name.is_empty()
            || name.len() > 64
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(ConfigError::DataRootName(name.clone()));
        }
        if !root.path.is_absolute() {
            return Err(ConfigError::DataRootPath(name.clone()));
        }
        if root.max_depth > 16
            || !(1..=1_000_000).contains(&root.max_releases)
            || !(1..=100_000).contains(&root.max_files_per_release)
        {
            return Err(ConfigError::DataRootLimits(name.clone()));
        }
    }
    Ok(())
}

fn validate_paths(paths: &Paths) -> Result<(), ConfigError> {
    if !paths.link_root.is_absolute() {
        return Err(ConfigError::AbsolutePath("paths.link_root"));
    }
    for rewrite in &paths.rewrite {
        if rewrite.name.is_empty() || !rewrite.remote.is_absolute() || !rewrite.local.is_absolute()
        {
            return Err(ConfigError::PathRewrite);
        }
    }
    for (index, left) in paths.rewrite.iter().enumerate() {
        if paths.rewrite[index + 1..].iter().any(|right| {
            (left.local == right.local || left.remote == right.remote)
                && left
                    .services
                    .iter()
                    .any(|service| right.services.contains(service))
        }) {
            return Err(ConfigError::AmbiguousPathRewrite);
        }
    }
    Ok(())
}

fn injection(config: &InjectionConfig) -> Result<Injection, ConfigError> {
    if config.category_template.len() > 256
        || config.tag_templates.len() > 64
        || config
            .tag_templates
            .iter()
            .any(|template| template.len() > 256)
    {
        return Err(ConfigError::TemplateLimit);
    }
    if crate::template::validate(&config.category_template).is_err()
        || config
            .tag_templates
            .iter()
            .any(|template| crate::template::validate(template).is_err())
    {
        return Err(ConfigError::TemplateSyntax);
    }
    Ok(Injection {
        dry_run: config.dry_run,
        category_template: config.category_template.clone(),
        tag_templates: config.tag_templates.clone(),
        inherit_source_category: config.inherit_source_category,
        inherit_source_tags: config.inherit_source_tags,
        resume: resume_policy(&config.resume)?,
    })
}

fn resume_policy(config: &ResumeConfig) -> Result<ResumePolicy, ConfigError> {
    let combine = match config.combine.as_str() {
        "and" => ThresholdCombination::And,
        "or" => ThresholdCombination::Or,
        _ => return Err(ConfigError::ThresholdCombination),
    };
    let min_present_ratio_ppm = config
        .min_present_ratio
        .map(|ratio| {
            if ratio.is_finite() && (0.0..=1.0).contains(&ratio) {
                Ok((ratio * 1_000_000.0).round() as u32)
            } else {
                Err(ConfigError::PresentRatio)
            }
        })
        .transpose()?;
    match config.mode.as_str() {
        "never" => Ok(ResumePolicy::Never),
        "complete_only" => Ok(ResumePolicy::CompleteOnly),
        "always" => Ok(ResumePolicy::Always),
        "threshold" if config.max_missing_bytes.is_some() || min_present_ratio_ppm.is_some() => {
            Ok(ResumePolicy::Threshold {
                max_missing_bytes: config.max_missing_bytes,
                min_present_ratio_ppm,
                combine,
            })
        }
        "threshold" => Err(ConfigError::EmptyThreshold),
        _ => Err(ConfigError::ResumeMode),
    }
}

fn matching(config: &MatchingConfig) -> Result<Matching, ConfigError> {
    let (allow_flexible, allow_partial) = match config.mode.as_str() {
        "strict" => (false, false),
        "flexible" => (true, false),
        "partial" => (true, true),
        _ => return Err(ConfigError::MatchingMode),
    };
    if !(0.0..=1.0).contains(&config.preflight_size_tolerance) {
        return Err(ConfigError::PreflightTolerance);
    }
    if config.max_assignment_files == 0
        || config.max_assignment_files > 100_000
        || config.max_candidate_edges == 0
        || config.max_candidate_edges > 1_000_000
        || config.max_assignment_component_files == 0
        || config.max_assignment_component_files > 1_024
        || config.max_assignment_component_files > config.max_assignment_files
        || config.max_assignment_operations == 0
        || config.max_assignment_operations > 1_000_000_000
    {
        return Err(ConfigError::MatcherBudget);
    }
    let defaults = MatchingPolicy::default();
    Ok(Matching {
        policy: MatchingPolicy {
            allow_flexible,
            allow_partial,
            allow_season_from_episodes: config.season_from_episodes,
            primary_video_extensions: if config.video_extensions.is_empty() {
                defaults.primary_video_extensions
            } else {
                normalized_extensions(&config.video_extensions)?
            },
            optional_extensions: if config.optional_extensions.is_empty() {
                defaults.optional_extensions
            } else {
                normalized_extensions(&config.optional_extensions)?
            },
            optional_path_components: defaults.optional_path_components,
            max_assignment_files: config.max_assignment_files,
            max_candidate_edges: config.max_candidate_edges,
            max_assignment_component_files: config.max_assignment_component_files,
            max_assignment_operations: config.max_assignment_operations,
        },
        preflight_size_tolerance: config.preflight_size_tolerance,
        max_torrent_bytes: config.max_torrent_bytes,
        max_files_per_torrent: config.max_files_per_torrent,
        max_path_bytes: config.max_path_bytes,
        pending_source_timeout: config.pending_source_timeout,
    })
}

fn normalized_extensions(values: &[String]) -> Result<Vec<String>, ConfigError> {
    let mut normalized = Vec::with_capacity(values.len());
    for value in values {
        let value = value
            .strip_prefix('.')
            .unwrap_or(value)
            .to_ascii_lowercase();
        if value.is_empty()
            || value.len() > 16
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        {
            return Err(ConfigError::FileExtension);
        }
        if !normalized.contains(&value) {
            normalized.push(value);
        }
    }
    Ok(normalized)
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
        (
            "matching.max_torrent_bytes",
            config.matching.max_torrent_bytes,
        ),
        (
            "matching.max_files_per_torrent",
            config.matching.max_files_per_torrent,
        ),
        ("matching.max_path_bytes", config.matching.max_path_bytes),
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
    if let Some(prowlarr) = &config.prowlarr {
        if prowlarr.max_results_per_query == 0 {
            return Err(ConfigError::ZeroLimit("prowlarr.max_results_per_query"));
        }
        if prowlarr.max_results_per_query > 1_000 {
            return Err(ConfigError::LimitTooLarge {
                field: "prowlarr.max_results_per_query",
                maximum: 1_000,
            });
        }
        for (field, value) in [
            ("prowlarr.request_timeout", prowlarr.request_timeout),
            ("prowlarr.refresh_interval", prowlarr.refresh_interval),
        ] {
            if value.is_zero() {
                return Err(ConfigError::ZeroLimit(field));
            }
        }
    }
    for (field, value, maximum) in [
        (
            "matching.max_torrent_bytes",
            config.matching.max_torrent_bytes,
            8 * 1024 * 1024,
        ),
        (
            "matching.max_files_per_torrent",
            config.matching.max_files_per_torrent,
            1_000_000,
        ),
        (
            "matching.max_path_bytes",
            config.matching.max_path_bytes,
            16_384,
        ),
    ] {
        if value > maximum {
            return Err(ConfigError::LimitTooLarge { field, maximum });
        }
    }
    if let Some((field, _)) = values.into_iter().find(|(_, value)| *value == 0) {
        return Err(ConfigError::ZeroLimit(field));
    }
    Ok(())
}

fn service_url(value: &str) -> Option<Url> {
    let mut url = Url::parse(value).ok()?;
    if !matches!(url.scheme(), "http" | "https")
        || url.cannot_be_a_base()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return None;
    }
    if !url.path().ends_with('/') {
        url.set_path(&format!("{}/", url.path()));
    }
    Some(url)
}

fn optional_secret(field: &str, value: Option<String>) -> Result<Option<Secret>, ConfigError> {
    value.map(|value| secret(field, value)).transpose()
}

fn required_secret(field: &str, value: Option<String>) -> Result<Secret, ConfigError> {
    optional_secret(field, value)?.ok_or_else(|| ConfigError::MissingSecret(field.to_owned()))
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
    #[error("invalid Prowlarr URL")]
    ProwlarrUrl,
    #[error("matching.mode must be strict, flexible, or partial")]
    MatchingMode,
    #[error("matching.preflight_size_tolerance must be between zero and one")]
    PreflightTolerance,
    #[error("matching assignment budgets must be positive and internally consistent")]
    MatcherBudget,
    #[error("matching file extensions must be short alphanumeric values")]
    FileExtension,
    #[error("{0} must be an absolute path")]
    AbsolutePath(&'static str),
    #[error("path rewrites require a name and absolute local and remote prefixes")]
    PathRewrite,
    #[error("data root name {0:?} is invalid")]
    DataRootName(String),
    #[error("data root {0:?} must use an absolute path")]
    DataRootPath(String),
    #[error("data root {0:?} has unsafe scan limits")]
    DataRootLimits(String),
    #[error("path rewrites cannot have equal local prefixes for the same service")]
    AmbiguousPathRewrite,
    #[error("category and tag templates exceed their configured limits")]
    TemplateLimit,
    #[error("category or tag template uses unsupported syntax or variables")]
    TemplateSyntax,
    #[error("injection.resume.mode is invalid")]
    ResumeMode,
    #[error("injection.resume.combine must be and or or")]
    ThresholdCombination,
    #[error("injection.resume.min_present_ratio must be between zero and one")]
    PresentRatio,
    #[error("threshold resume policy requires at least one threshold")]
    EmptyThreshold,
    #[error("invalid {kind} URL for Arr instance {name}")]
    ArrUrl { kind: &'static str, name: String },
    #[error("too many Arr instances; at most 32 are supported")]
    TooManyArrInstances,
    #[error("invalid {kind} instance name {name:?}")]
    ArrName { kind: &'static str, name: String },
    #[error("required secret {0} is not configured")]
    MissingSecret(String),
    #[error("{0} cannot be empty")]
    EmptySecret(String),
    #[error("{0} exceeds the secret size limit")]
    SecretTooLarge(String),
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
                api_key = "file-secret"
            "#,
        )
        .expect("write configuration");

        let config = load(
            &path,
            true,
            [
                (
                    "SPOROS__SERVER__BIND".to_owned(),
                    "\"127.0.0.1:9001\"".to_owned(),
                ),
                (
                    "SPOROS__AUTH__API_KEY".to_owned(),
                    "environment-secret".to_owned(),
                ),
            ],
        )
        .expect("load configuration");

        assert_eq!(config.server.bind, "127.0.0.1:9001".parse().unwrap());
        assert_eq!(config.auth.api_key.unwrap().expose(), "environment-secret");
        assert_eq!(config.runtime.database_path, Path::new("/data/sporos.db"));
    }

    #[test]
    fn loads_optional_api_key() {
        let directory = TempDir::new().expect("create temporary directory");
        let config = load(
            &directory.path().join("missing.toml"),
            false,
            [("SPOROS__AUTH__API_KEY".to_owned(), "api secret".to_owned())],
        )
        .expect("load environment configuration");

        assert_eq!(config.auth.api_key.unwrap().expose(), "api secret");

        let config = load(&directory.path().join("missing.toml"), false, [])
            .expect("load without API authentication");
        assert!(config.auth.api_key.is_none());

        let config =
            load_config("[auth]\napi_key = \"config-secret\"\n").expect("load configured API key");
        assert_eq!(config.auth.api_key.unwrap().expose(), "config-secret");
    }

    #[test]
    fn loads_bounded_qbittorrent_settings() {
        let config = load_config(
            r#"
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
        assert_eq!(
            qbittorrent.api_key.as_ref().unwrap().expose(),
            "qbt_0123456789abcdefghijklmnopqr"
        );
        assert!(!format!("{qbittorrent:?}").contains("qbt_0123456789abcdefghijklmnopqr"));
    }

    #[test]
    fn loads_typed_candidate_policy() {
        let config = load_config(
            r#"
                [sources]
                include_categories = ["tv"]
                [matching]
                mode = "flexible"
                season_from_episodes = false
                preflight_size_tolerance = 0.05
                video_extensions = [".MKV", "mp4"]
                [injection]
                dry_run = true
                [paths]
                link_root = "/srv/sporos/links"
                [[paths.rewrite]]
                name = "qbit"
                local = "/srv/sporos"
                remote = "/downloads/sporos"
                services = ["qbittorrent"]
                [injection.resume]
                mode = "threshold"
                min_present_ratio = 0.10
                combine = "or"
            "#,
        )
        .expect("load candidate policy");

        assert_eq!(config.sources.include_categories, ["tv"]);
        assert!(config.sources.exclude_sporos_managed);
        assert!(config.matching.policy.allow_flexible);
        assert!(!config.matching.policy.allow_partial);
        assert!(!config.matching.policy.allow_season_from_episodes);
        assert_eq!(
            config.matching.policy.primary_video_extensions,
            ["mkv", "mp4"]
        );
        assert_eq!(config.matching.preflight_size_tolerance, 0.05);
        assert!(config.injection.dry_run);
        assert_eq!(
            config.paths.qbit_link_root(),
            Some(PathBuf::from("/downloads/sporos/links"))
        );
        assert_eq!(
            config.paths.remote_to_local(
                "qbittorrent",
                Path::new("/downloads/sporos/source/file.mkv")
            ),
            Some(PathBuf::from("/srv/sporos/source/file.mkv"))
        );
        assert_eq!(
            config
                .paths
                .remote_to_local("qbittorrent", Path::new("/unapproved/file.mkv")),
            None
        );
        assert_eq!(
            config.injection.resume,
            ResumePolicy::Threshold {
                max_missing_bytes: None,
                min_present_ratio_ppm: Some(100_000),
                combine: ThresholdCombination::Or,
            }
        );
    }

    #[test]
    fn rejects_unbounded_or_unknown_candidate_policy() {
        for invalid in [
            "[matching]\nmode = \"guess\"",
            "[matching]\npreflight_size_tolerance = 1.1",
            "[matching]\nmax_torrent_bytes = 67108865",
            "[matching]\nmax_assignment_files = 0",
            "[matching]\nmax_assignment_files = 10\nmax_assignment_component_files = 11",
            "[matching]\nmax_candidate_edges = 1000001",
            "[matching]\nmax_assignment_operations = 1000000001",
            "[matching]\nvideo_extensions = [\"../mkv\"]",
            "[paths]\nlink_root = \"relative\"",
            "[injection.resume]\nmode = \"threshold\"",
            "[injection.resume]\nmode = \"threshold\"\nmin_present_ratio = 1.1",
        ] {
            let config = format!("{invalid}\n");
            assert!(load_config(&config).is_err(), "accepted {invalid}");
        }
    }

    #[test]
    fn rejects_unbounded_qbittorrent_batches() {
        let error = load_config(
            r#"
                [qbittorrent]
                url = "http://qbittorrent:8080"
                inventory_batch_size = 501
            "#,
        )
        .expect_err("reject unbounded inventory pages");

        assert!(matches!(error, ConfigError::LimitTooLarge { .. }));
    }

    #[test]
    fn loads_named_optional_arr_instances() {
        let config = load_config(
            r#"
                [arr.sonarr.main]
                url = "http://sonarr:8989"
                api_key = "sonarr-secret"
                request_timeout = "15s"
                [arr.radarr.movies]
                url = "http://radarr:7878"
                api_key = "radarr-secret"
            "#,
        )
        .expect("load Arr instances");

        assert_eq!(config.arr.len(), 2);
        assert_eq!(config.arr[0].kind, ArrKind::Sonarr);
        assert_eq!(config.arr[0].name, "main");
        assert_eq!(config.arr[0].request_timeout, Duration::from_secs(15));
        assert_eq!(config.arr[1].kind, ArrKind::Radarr);
        assert!(!format!("{:?}", config.arr).contains("sonarr-secret"));
    }

    #[test]
    fn loads_downstream_keys_from_environment_overrides() {
        let directory = TempDir::new().expect("create temporary directory");
        let path = directory.path().join("sporos.toml");
        fs::write(
            &path,
            r#"
                [qbittorrent]
                url = "http://qbittorrent:8080"
                [prowlarr]
                url = "http://prowlarr:9696"
                [arr.sonarr.main]
                url = "http://sonarr:8989"
            "#,
        )
        .expect("write configuration");

        let config = load(
            &path,
            true,
            [
                (
                    "SPOROS__QBITTORRENT__API_KEY".to_owned(),
                    "qbt_0123456789abcdefghijklmnopqr".to_owned(),
                ),
                (
                    "SPOROS__PROWLARR__API_KEY".to_owned(),
                    "prowlarr-secret".to_owned(),
                ),
                (
                    "SPOROS__ARR__SONARR__MAIN__API_KEY".to_owned(),
                    "sonarr-secret".to_owned(),
                ),
            ],
        )
        .expect("load environment overrides");

        assert_eq!(
            config.qbittorrent.unwrap().api_key.unwrap().expose(),
            "qbt_0123456789abcdefghijklmnopqr"
        );
        assert_eq!(config.prowlarr.unwrap().api_key.expose(), "prowlarr-secret");
        assert_eq!(config.arr[0].api_key.expose(), "sonarr-secret");
    }

    #[test]
    fn loads_only_named_bounded_data_roots() {
        let config = load_config(
            r#"
                [data_scan.roots.media]
                path = "/media/library"
                max_depth = 2
                max_releases = 1000
                max_files_per_release = 100
            "#,
        )
        .expect("load data root");

        assert_eq!(config.data_roots["media"].path, Path::new("/media/library"));
        for invalid in [
            "[data_scan.roots.'../media']\npath = \"/media\"\nmax_depth = 1\nmax_releases = 1\nmax_files_per_release = 1",
            "[data_scan.roots.media]\npath = \"relative\"\nmax_depth = 1\nmax_releases = 1\nmax_files_per_release = 1",
            "[data_scan.roots.media]\npath = \"/media\"\nmax_depth = 17\nmax_releases = 1\nmax_files_per_release = 1",
        ] {
            let input = format!("{invalid}\n");
            assert!(load_config(&input).is_err(), "accepted {invalid}");
        }
    }

    #[test]
    fn loads_bounded_prowlarr_settings() {
        let config = load_config(
            r#"
                [prowlarr]
                url = "http://prowlarr:9696/base"
                api_key = "prowlarr-secret"
                request_timeout = "15s"
                refresh_interval = "2m"
                include_tags = [1, 2]
                exclude_tags = [3]
                require_proxy_downloads = true
                max_results_per_query = 25
            "#,
        )
        .expect("load Prowlarr settings");
        let prowlarr = config.prowlarr.expect("Prowlarr configured");

        assert_eq!(prowlarr.url.as_str(), "http://prowlarr:9696/base/");
        assert_eq!(prowlarr.request_timeout, Duration::from_secs(15));
        assert_eq!(prowlarr.refresh_interval, Duration::from_secs(120));
        assert_eq!(prowlarr.include_tags, [1, 2]);
        assert_eq!(prowlarr.exclude_tags, [3]);
        assert_eq!(prowlarr.max_results_per_query, 25);
        assert!(!format!("{prowlarr:?}").contains("prowlarr-secret"));
    }

    #[test]
    fn requires_keys_for_authenticated_services() {
        let directory = TempDir::new().expect("create temporary directory");
        let path = directory.path().join("sporos.toml");
        fs::write(&path, "[prowlarr]\nurl = \"http://prowlarr:9696\"\n")
            .expect("write configuration");

        let error = load(&path, true, []).expect_err("require Prowlarr API key");
        assert!(matches!(error, ConfigError::MissingSecret(name) if name == "prowlarr.api_key"));

        fs::write(&path, "[arr.sonarr.main]\nurl = \"http://sonarr:8989\"\n")
            .expect("write configuration");
        let error = load(&path, true, []).expect_err("require Arr API key");
        assert!(
            matches!(error, ConfigError::MissingSecret(name) if name == "arr.sonarr.main.api_key")
        );
    }

    #[test]
    fn permits_unauthenticated_qbittorrent() {
        let directory = TempDir::new().expect("create temporary directory");
        let path = directory.path().join("sporos.toml");
        fs::write(&path, "[qbittorrent]\nurl = \"http://qbittorrent:8080\"\n")
            .expect("write configuration");

        let config = load(&path, true, []).expect("load qBittorrent without authentication");
        assert!(config.qbittorrent.unwrap().api_key.is_none());
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
        fs::write(&path, "").expect("write configuration");
        load(&path, true, [(key.to_owned(), value.to_owned())])
    }
}
