//! Configuration discovery, schema validation, and runtime config assembly.

use std::{
    borrow::Cow,
    collections::BTreeSet,
    env,
    ffi::OsStr,
    fs::{self, OpenOptions},
    io::Write,
    net::IpAddr,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use regex::Regex;
use serde::{Deserialize, Deserializer, de};

use crate::SporosError;
pub use crate::{Result, domain};

const APP_DIR_NAME: &str = "cross-seed";
const CONFIG_FILE_NAME: &str = "config.toml";
const CONFIG_FILE_ENV: &str = "SPOROS__CONFIG_FILE";
const DEFAULT_DELAY_SECONDS: u64 = 30;
const DEFAULT_PORT: u16 = 2468;
const DEFAULT_MAX_DATA_DEPTH: u32 = 2;
const DEFAULT_FUZZY_SIZE_THRESHOLD: f64 = 0.05;
const DEFAULT_SNATCH_RETRIES: u32 = 2;
const MAX_SNATCH_RETRIES: u32 = 10;
const MAX_AUTO_RESUME_DOWNLOAD_BYTES: u64 = 52_428_800;
const MIN_RSS_CADENCE_MILLIS: u64 = 10 * 60 * 1_000;
const MAX_RSS_CADENCE_MILLIS: u64 = 2 * 60 * 60 * 1_000;
const MIN_SEARCH_CADENCE_MILLIS: u64 = 24 * 60 * 60 * 1_000;

/// Match mode after configured text has been parsed.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum MatchMode {
    /// Exact file-tree match only.
    Strict,
    /// Size-only matching may be accepted.
    Flexible,
    /// Partial matching may be accepted.
    Partial,
}

impl MatchMode {
    /// Parse configured match-mode text.
    pub fn parse(value: &str) -> crate::Result<Self> {
        match value {
            "strict" => Ok(Self::Strict),
            "flexible" => Ok(Self::Flexible),
            "partial" => Ok(Self::Partial),
            _ => Err(config_error(format!("invalid match_mode: {value}"))),
        }
    }
}

/// Action mode for matched candidates.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Action {
    /// Save matched torrents to `output_dir`.
    Save,
    /// Inject matched torrents into a client.
    Inject,
}

impl Action {
    /// Parse action text.
    pub fn parse(value: &str) -> crate::Result<Self> {
        match value {
            "save" => Ok(Self::Save),
            "inject" => Ok(Self::Inject),
            _ => Err(config_error(format!("invalid action: {value}"))),
        }
    }
}

/// Link type for linked injection.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum LinkType {
    /// Symbolic links.
    Symlink,
    /// Hard links.
    Hardlink,
    /// Reflinks.
    Reflink,
    /// Reflink when possible, copy otherwise.
    ReflinkOrCopy,
}

impl LinkType {
    /// Parse link-type text.
    pub fn parse(value: &str) -> crate::Result<Self> {
        match value {
            "symlink" => Ok(Self::Symlink),
            "hardlink" => Ok(Self::Hardlink),
            "reflink" => Ok(Self::Reflink),
            "reflink_or_copy" => Ok(Self::ReflinkOrCopy),
            _ => Err(config_error(format!("invalid link_type: {value}"))),
        }
    }
}

/// Parsed torrent-client entry.
#[derive(Debug, Default, Clone, Eq, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TorrentClientConfig {
    /// Client adapter type.
    pub kind: String,
    /// Whether the client is readonly.
    pub readonly: bool,
    /// Client URL string.
    pub url: String,
}

impl TorrentClientConfig {
    /// Parse CLI shorthand `<type>:[readonly:]<url>`.
    pub fn parse(value: &str) -> crate::Result<Self> {
        let (kind, rest) = value
            .split_once(':')
            .ok_or_else(|| config_error("torrent client entry missing URL"))?;
        let (readonly, url) = if let Some(url) = rest.strip_prefix("readonly:") {
            (true, url)
        } else {
            (false, rest)
        };

        let config = Self {
            kind: kind.to_owned(),
            readonly,
            url: url.to_owned(),
        };
        config.validate()?;
        Ok(config)
    }

    /// Validate a structured TOML or CLI torrent-client entry.
    pub fn validate(&self) -> crate::Result<()> {
        if self.kind.is_empty() {
            return Err(config_error("torrent client entry missing kind"));
        }
        if self.url.is_empty() {
            return Err(config_error("torrent client entry missing url"));
        }
        match self.kind.as_str() {
            "qbittorrent" | "rtorrent" | "transmission" | "deluge" => {}
            _ => {
                return Err(config_error(format!(
                    "unsupported torrent client: {}",
                    self.kind
                )));
            }
        }
        if !self.url.contains("://") {
            return Err(config_error("torrent client URL must include a scheme"));
        }
        Ok(())
    }
}

/// Structured API-backed integration config entry.
#[derive(Debug, Default, Clone, Eq, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ApiIntegrationConfig {
    /// Base service URL without API-key query parameters.
    pub url: String,
    /// API key kept separately from the URL for redaction and persistence.
    pub api_key: String,
}

impl ApiIntegrationConfig {
    /// Validate fields common to structured integrations.
    pub fn validate(&self, label: &str) -> crate::Result<()> {
        if self.url.is_empty() {
            return Err(config_error(format!("{label} entry missing url")));
        }
        if self.api_key.is_empty() {
            return Err(config_error(format!("{label} entry missing api_key")));
        }
        if self.url.contains("apikey=") || self.url.contains("api_key=") {
            return Err(config_error(format!(
                "{label} url must not include api_key query parameters"
            )));
        }
        Ok(())
    }
}

/// Raw options before defaults and cross-option validation.
#[derive(Debug, Default, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RawConfig {
    /// Writable runtime state directory.
    pub state_dir: Option<PathBuf>,
    /// SQLite database path.
    pub database_path: Option<PathBuf>,
    /// Delay in seconds.
    pub delay: Option<u64>,
    /// Torznab indexers.
    pub torznab: Vec<ApiIntegrationConfig>,
    /// Use client torrents as searchees.
    pub use_client_torrents: Option<bool>,
    /// Data directories.
    pub data_dirs: Vec<PathBuf>,
    /// Match mode text.
    pub match_mode: Option<String>,
    /// Skip recheck.
    pub skip_recheck: Option<bool>,
    /// Auto resume max download bytes.
    pub auto_resume_max_download: Option<u64>,
    /// Ignore non-relevant files to resume.
    pub ignore_non_relevant_files_to_resume: Option<bool>,
    /// Link category.
    pub link_category: Option<String>,
    /// Link dirs.
    pub link_dirs: Vec<PathBuf>,
    /// Link type text.
    pub link_type: Option<String>,
    /// Flat linking.
    pub flat_linking: Option<bool>,
    /// Max data depth.
    pub max_data_depth: Option<u32>,
    /// Torrent dir.
    pub torrent_dir: Option<PathBuf>,
    /// Output dir.
    pub output_dir: Option<PathBuf>,
    /// Inject dir.
    pub inject_dir: Option<PathBuf>,
    /// Ignore saved torrent titles.
    pub ignore_titles: Option<bool>,
    /// Include single episodes.
    pub include_single_episodes: Option<bool>,
    /// Include non-video searchees.
    pub include_non_videos: Option<bool>,
    /// Fuzzy size threshold.
    pub fuzzy_size_threshold: Option<f64>,
    /// Season from episodes ratio.
    #[serde(deserialize_with = "deserialize_optional_ratio")]
    pub season_from_episodes: Option<f64>,
    /// Exclude older duration in ms.
    #[serde(deserialize_with = "deserialize_optional_duration")]
    pub exclude_older: Option<u64>,
    /// Exclude recent search duration in ms.
    #[serde(deserialize_with = "deserialize_optional_duration")]
    pub exclude_recent_search: Option<u64>,
    /// Action text.
    pub action: Option<String>,
    /// Torrent-client entries.
    pub torrent_clients: Vec<TorrentClientConfig>,
    /// Duplicate categories.
    pub duplicate_categories: Option<bool>,
    /// Notification URLs.
    pub notification_webhook_urls: Vec<String>,
    /// Daemon port.
    pub port: Option<Option<u16>>,
    /// Daemon host.
    pub host: Option<IpAddr>,
    /// RSS cadence in ms.
    #[serde(deserialize_with = "deserialize_optional_duration")]
    pub rss_cadence: Option<u64>,
    /// Search cadence in ms.
    #[serde(deserialize_with = "deserialize_optional_duration")]
    pub search_cadence: Option<u64>,
    /// Snatch timeout in ms.
    #[serde(deserialize_with = "deserialize_optional_duration")]
    pub snatch_timeout: Option<u64>,
    /// Snatch retries after the first attempt.
    pub snatch_retries: Option<u32>,
    /// Search timeout in ms.
    #[serde(deserialize_with = "deserialize_optional_duration")]
    pub search_timeout: Option<u64>,
    /// Search limit.
    pub search_limit: Option<u32>,
    /// Logging format text.
    pub log_format: Option<String>,
    /// Logging level text.
    pub log_level: Option<String>,
    /// Verbose logs.
    pub verbose: Option<bool>,
    /// Hidden targeted torrent paths.
    pub torrents: Option<Vec<PathBuf>>,
    /// Blocklist entries.
    pub block_list: Vec<String>,
    /// API key.
    pub api_key: Option<String>,
    /// Sonarr instances.
    pub sonarr: Vec<ApiIntegrationConfig>,
    /// Radarr instances.
    pub radarr: Vec<ApiIntegrationConfig>,
}

/// Runtime config after defaults and validation.
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    /// Writable runtime state directory.
    pub state_dir: PathBuf,
    /// SQLite database path.
    pub database_path: PathBuf,
    /// Delay in seconds.
    pub delay: u64,
    /// Torznab indexers.
    pub torznab: Vec<ApiIntegrationConfig>,
    /// Use client torrents as searchees.
    pub use_client_torrents: bool,
    /// Data directories.
    pub data_dirs: Vec<PathBuf>,
    /// Match mode.
    pub match_mode: MatchMode,
    /// Skip recheck.
    pub skip_recheck: bool,
    /// Auto resume max download bytes.
    pub auto_resume_max_download: u64,
    /// Ignore non-relevant files to resume.
    pub ignore_non_relevant_files_to_resume: bool,
    /// Link category.
    pub link_category: Option<String>,
    /// Link dirs.
    pub link_dirs: Vec<PathBuf>,
    /// Link type.
    pub link_type: LinkType,
    /// Flat linking.
    pub flat_linking: bool,
    /// Max data depth.
    pub max_data_depth: u32,
    /// Torrent dir.
    pub torrent_dir: Option<PathBuf>,
    /// Output dir.
    pub output_dir: PathBuf,
    /// Inject dir.
    pub inject_dir: Option<PathBuf>,
    /// Ignore saved torrent titles.
    pub ignore_titles: Option<bool>,
    /// Include single episodes.
    pub include_single_episodes: bool,
    /// Include non-video searchees.
    pub include_non_videos: bool,
    /// Fuzzy size threshold.
    pub fuzzy_size_threshold: f64,
    /// Season from episodes ratio.
    pub season_from_episodes: Option<f64>,
    /// Exclude older duration in ms.
    pub exclude_older: Option<u64>,
    /// Exclude recent search duration in ms.
    pub exclude_recent_search: Option<u64>,
    /// Action.
    pub action: Action,
    /// Torrent clients.
    pub torrent_clients: Vec<TorrentClientConfig>,
    /// Duplicate categories.
    pub duplicate_categories: bool,
    /// Notification URLs.
    pub notification_webhook_urls: Vec<String>,
    /// Daemon port. `None` preserves `--no-port`.
    pub port: Option<u16>,
    /// Daemon host.
    pub host: Option<IpAddr>,
    /// RSS cadence in ms.
    pub rss_cadence: Option<u64>,
    /// Search cadence in ms.
    pub search_cadence: Option<u64>,
    /// Snatch timeout in ms.
    pub snatch_timeout: Option<u64>,
    /// Snatch retries after the first attempt.
    pub snatch_retries: u32,
    /// Search timeout in ms.
    pub search_timeout: Option<u64>,
    /// Search limit.
    pub search_limit: Option<u32>,
    /// Logging format.
    pub log_format: Option<String>,
    /// Logging level.
    pub log_level: Option<String>,
    /// Verbose logs.
    pub verbose: bool,
    /// Hidden targeted torrent paths.
    pub torrents: Option<Vec<PathBuf>>,
    /// Blocklist entries.
    pub block_list: Vec<String>,
    /// API key.
    pub api_key: Option<String>,
    /// Sonarr instances.
    pub sonarr: Vec<ApiIntegrationConfig>,
    /// Radarr instances.
    pub radarr: Vec<ApiIntegrationConfig>,
}

impl RuntimeConfig {
    /// Normalize raw options and run documented cross-option checks.
    pub fn normalize(raw: RawConfig, default_state_dir: &Path) -> crate::Result<Self> {
        let link_dirs = raw.link_dirs;
        let notification_webhook_urls = raw.notification_webhook_urls;
        let torrent_clients = raw.torrent_clients;
        let state_dir = raw
            .state_dir
            .unwrap_or_else(|| default_state_dir.to_owned());
        let database_path = raw
            .database_path
            .unwrap_or_else(|| state_dir.join("sporos.db"));

        for client in &torrent_clients {
            client.validate()?;
        }
        for indexer in &raw.torznab {
            indexer.validate("torznab")?;
        }
        for arr in &raw.sonarr {
            arr.validate("sonarr")?;
        }
        for arr in &raw.radarr {
            arr.validate("radarr")?;
        }
        let config = Self {
            state_dir: state_dir.clone(),
            database_path,
            delay: raw.delay.unwrap_or(DEFAULT_DELAY_SECONDS),
            torznab: raw.torznab,
            use_client_torrents: raw.use_client_torrents.unwrap_or(false),
            data_dirs: raw.data_dirs,
            match_mode: MatchMode::parse(raw.match_mode.as_deref().unwrap_or("strict"))?,
            skip_recheck: raw.skip_recheck.unwrap_or(true),
            auto_resume_max_download: raw.auto_resume_max_download.unwrap_or(0),
            ignore_non_relevant_files_to_resume: raw
                .ignore_non_relevant_files_to_resume
                .unwrap_or(false),
            link_category: raw.link_category,
            link_dirs,
            link_type: LinkType::parse(raw.link_type.as_deref().unwrap_or("symlink"))?,
            flat_linking: raw.flat_linking.unwrap_or(false),
            max_data_depth: raw.max_data_depth.unwrap_or(DEFAULT_MAX_DATA_DEPTH),
            torrent_dir: raw.torrent_dir,
            output_dir: raw
                .output_dir
                .unwrap_or_else(|| state_dir.join("cross-seeds")),
            inject_dir: raw.inject_dir,
            ignore_titles: raw.ignore_titles,
            include_single_episodes: raw.include_single_episodes.unwrap_or(false),
            include_non_videos: raw.include_non_videos.unwrap_or(false),
            fuzzy_size_threshold: raw
                .fuzzy_size_threshold
                .unwrap_or(DEFAULT_FUZZY_SIZE_THRESHOLD),
            season_from_episodes: raw.season_from_episodes,
            exclude_older: raw.exclude_older,
            exclude_recent_search: raw.exclude_recent_search,
            action: Action::parse(raw.action.as_deref().unwrap_or("save"))?,
            torrent_clients,
            duplicate_categories: raw.duplicate_categories.unwrap_or(false),
            notification_webhook_urls,
            port: raw.port.unwrap_or(Some(DEFAULT_PORT)),
            host: raw.host,
            rss_cadence: raw.rss_cadence,
            search_cadence: raw.search_cadence,
            snatch_timeout: raw.snatch_timeout,
            snatch_retries: raw.snatch_retries.unwrap_or(DEFAULT_SNATCH_RETRIES),
            search_timeout: raw.search_timeout,
            search_limit: raw.search_limit,
            log_format: raw.log_format,
            log_level: raw.log_level,
            verbose: raw.verbose.unwrap_or(false),
            torrents: raw.torrents,
            block_list: raw.block_list,
            api_key: raw.api_key,
            sonarr: raw.sonarr,
            radarr: raw.radarr,
        };

        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> crate::Result<()> {
        if self.delay < DEFAULT_DELAY_SECONDS {
            return Err(config_error("delay must be at least 30 seconds"));
        }
        if self.snatch_retries > MAX_SNATCH_RETRIES {
            return Err(config_error("snatch_retries must be <= 10"));
        }
        if let Some(log_format) = &self.log_format {
            validate_log_format(log_format)?;
        }
        if let Some(log_level) = &self.log_level {
            validate_log_level(log_level)?;
        }
        if self.max_data_depth == 0 {
            return Err(config_error("max_data_depth must be at least 1"));
        }
        if !(self.fuzzy_size_threshold > 0.0 && self.fuzzy_size_threshold <= 1.0) {
            return Err(config_error("fuzzy_size_threshold must be > 0 and <= 1"));
        }
        if self.auto_resume_max_download > MAX_AUTO_RESUME_DOWNLOAD_BYTES {
            return Err(config_error(
                "auto_resume_max_download exceeds 52428800 bytes",
            ));
        }
        if let Some(api_key) = &self.api_key {
            if api_key.len() < 24 {
                return Err(config_error("api_key must be at least 24 characters"));
            }
        }
        if self.torrent_dir.is_some() && self.use_client_torrents {
            return Err(config_error(
                "torrent_dir cannot be used with use_client_torrents",
            ));
        }
        if self.use_client_torrents && self.torrent_clients.is_empty() {
            return Err(config_error("use_client_torrents requires torrent_clients"));
        }
        let mut torrent_client_urls = BTreeSet::new();
        for client in &self.torrent_clients {
            if !torrent_client_urls.insert(client.url.as_str()) {
                return Err(config_error("duplicate torrent client URL"));
            }
        }
        if self.action == Action::Inject && self.torrent_clients.is_empty() {
            return Err(config_error("action inject requires torrent_clients"));
        }
        if self.action == Action::Inject
            && self.torrent_clients.iter().all(|client| client.readonly)
        {
            return Err(config_error("action inject requires a non-readonly client"));
        }
        if self.torrent_clients.len() > 1 && self.torrent_dir.is_some() {
            return Err(config_error(
                "multiple clients cannot be combined with torrent_dir",
            ));
        }
        if self.torrent_clients.len() > 1
            && self
                .torrents
                .as_ref()
                .is_some_and(|items| !items.is_empty())
        {
            return Err(config_error(
                "multiple clients cannot be combined with --torrents",
            ));
        }
        if self.torrent_clients.len() > 1 && !self.data_dirs.is_empty() && self.link_dirs.is_empty()
        {
            return Err(config_error(
                "multiple clients plus data_dirs require link_dirs",
            ));
        }
        if self.inject_dir.is_some() && self.action != Action::Inject {
            return Err(config_error("inject_dir is only valid with action inject"));
        }
        if self.action == Action::Inject
            && matches!(self.match_mode, MatchMode::Flexible | MatchMode::Partial)
            && self.link_dirs.is_empty()
        {
            return Err(config_error(
                "injecting with flexible or partial match_mode requires link_dirs",
            ));
        }
        if let Some(season_from_episodes) = self.season_from_episodes {
            if !(season_from_episodes > 0.0 && season_from_episodes <= 1.0) {
                return Err(config_error("season_from_episodes must be > 0 and <= 1"));
            }
            if season_from_episodes < 1.0 && self.match_mode != MatchMode::Partial {
                return Err(config_error(
                    "season_from_episodes below 1 requires match_mode partial",
                ));
            }
            if self.action == Action::Inject && self.link_dirs.is_empty() {
                return Err(config_error(
                    "season_from_episodes with action inject requires link_dirs",
                ));
            }
        }
        if !dev_mode_enabled() {
            if let Some(rss_cadence) = self.rss_cadence {
                if !(MIN_RSS_CADENCE_MILLIS..=MAX_RSS_CADENCE_MILLIS).contains(&rss_cadence) {
                    return Err(config_error(
                        "rss_cadence must be between 10 minutes and 2 hours",
                    ));
                }
            }
            if self
                .search_cadence
                .is_some_and(|search_cadence| search_cadence < MIN_SEARCH_CADENCE_MILLIS)
            {
                return Err(config_error("search_cadence must be at least 1 day"));
            }
        }
        if let (Some(search_cadence), Some(exclude_recent_search)) =
            (self.search_cadence, self.exclude_recent_search)
        {
            if exclude_recent_search < search_cadence.saturating_mul(3) {
                return Err(config_error(
                    "exclude_recent_search must be at least 3x search_cadence",
                ));
            }
        }
        if self.search_cadence.is_some() {
            let (Some(exclude_older), Some(exclude_recent_search)) =
                (self.exclude_older, self.exclude_recent_search)
            else {
                return Err(config_error(
                    "scheduled search requires exclude_older and exclude_recent_search",
                ));
            };
            if exclude_older < exclude_recent_search.saturating_mul(2)
                || exclude_older > exclude_recent_search.saturating_mul(5)
            {
                return Err(config_error(
                    "exclude_older must be between 2x and 5x exclude_recent_search",
                ));
            }
        }
        if (self.search_cadence.is_some() || self.rss_cadence.is_some())
            && self.fuzzy_size_threshold > 0.1
        {
            return Err(config_error(
                "scheduled search/rss requires fuzzy_size_threshold <= 0.1",
            ));
        }
        if (self.search_cadence.is_some() || self.rss_cadence.is_some())
            && self.torrent_dir.is_none()
            && !self.use_client_torrents
            && self.data_dirs.is_empty()
        {
            return Err(config_error(
                "scheduled search/rss requires torrent_dir, use_client_torrents, or data_dirs",
            ));
        }
        if has_nested_paths(
            std::iter::once(self.output_dir.clone())
                .chain(self.link_dirs.iter().cloned())
                .chain(self.data_dirs.iter().cloned())
                .chain(self.torrent_dir.iter().cloned()),
        ) {
            return Err(config_error(
                "link_dirs, data_dirs, torrent_dir, and output_dir cannot be nested",
            ));
        }
        validate_block_list(&self.block_list)?;

        Ok(())
    }
}

/// Parse raw `config.toml` source into typed raw options.
pub fn raw_config_from_source(source: &str) -> crate::Result<RawConfig> {
    raw_config_from_named_source(Path::new(CONFIG_FILE_NAME), source)
}

fn raw_config_from_named_source(path: &Path, source: &str) -> crate::Result<RawConfig> {
    toml::from_str(source)
        .map_err(|error| config_error(format!("failed to parse {}: {error}", path.display())))
}

/// Minimal representation of discovered `config.toml`.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FileConfig {
    /// Full path to the config file.
    pub path: PathBuf,
    /// Raw TOML source when the file exists.
    pub source: Option<String>,
}

/// Selected config file path plus the compatibility app directory used for state.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ConfigFileTarget {
    /// Compatibility app directory for database, cache, and default output paths.
    pub app_dir: PathBuf,
    /// Exact config file path to read or generate.
    pub path: PathBuf,
    /// Whether the path came from `--config` or `SPOROS__CONFIG_FILE`.
    pub explicit: bool,
}

/// Resolve and create the compatibility app directory.
pub fn app_dir() -> crate::Result<PathBuf> {
    // CONFIG_DIR is retained as compatibility input and container state location.
    let dir = if let Some(config_dir) = env::var_os("CONFIG_DIR") {
        PathBuf::from(config_dir)
    } else if cfg!(windows) {
        env::var_os("LOCALAPPDATA")
            .map(PathBuf::from)
            .ok_or_else(|| config_error("LOCALAPPDATA is not set"))?
            .join(APP_DIR_NAME)
    } else {
        env::var_os("HOME")
            .map(PathBuf::from)
            .ok_or_else(|| config_error("HOME is not set"))?
            .join(".cross-seed")
    };

    fs::create_dir_all(&dir)
        .map_err(|error| config_error(format!("failed to create app directory: {error}")))?;
    verify_read_write_dir(&dir)?;
    Ok(dir)
}

/// Resolve config-file selection from CLI, env, or the local default.
pub fn selected_config_file(cli_path: Option<&Path>) -> crate::Result<ConfigFileTarget> {
    let app_dir = app_dir()?;
    config_file_from_sources(cli_path, env::var_os(CONFIG_FILE_ENV).as_deref(), &app_dir)
}

/// Resolve config-file selection with explicit source precedence.
pub fn config_file_from_sources(
    cli_path: Option<&Path>,
    env_path: Option<&OsStr>,
    app_dir: &Path,
) -> crate::Result<ConfigFileTarget> {
    if let Some(path) = cli_path {
        if path.as_os_str().is_empty() {
            return Err(config_error("--config cannot be empty"));
        }
        return Ok(ConfigFileTarget {
            app_dir: app_dir.to_owned(),
            path: path.to_owned(),
            explicit: true,
        });
    }

    if let Some(path) = env_path {
        if path.to_string_lossy().is_empty() {
            return Err(config_error(format!("{CONFIG_FILE_ENV} cannot be empty")));
        }
        return Ok(ConfigFileTarget {
            app_dir: app_dir.to_owned(),
            path: PathBuf::from(path),
            explicit: true,
        });
    }

    Ok(ConfigFileTarget {
        app_dir: app_dir.to_owned(),
        path: config_path(app_dir),
        explicit: false,
    })
}

/// Path to `config.toml` under the app directory.
pub fn config_path(app_dir: &Path) -> PathBuf {
    app_dir.join(CONFIG_FILE_NAME)
}

/// Load raw config-file source if present.
pub fn get_selected_file_config(target: &ConfigFileTarget) -> crate::Result<FileConfig> {
    let source = match fs::read_to_string(&target.path) {
        Ok(source) => Some(source),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if target.explicit {
                return Err(config_error(format!(
                    "config file not found: {}",
                    target.path.display()
                )));
            }
            if env::var_os("DOCKER_ENV").is_some() {
                generate_config_file(&target.path)?;
                Some(fs::read_to_string(&target.path).map_err(|error| {
                    config_error(format!(
                        "failed to read generated config file {}: {error}",
                        target.path.display()
                    ))
                })?)
            } else {
                None
            }
        }
        Err(error) => {
            return Err(config_error(format!(
                "failed to read config file {}: {error}",
                target.path.display()
            )));
        }
    };

    Ok(FileConfig {
        path: target.path.clone(),
        source,
    })
}

/// Load raw `config.toml` source if present under the app directory.
pub fn get_file_config(app_dir: &Path) -> crate::Result<FileConfig> {
    let target = ConfigFileTarget {
        app_dir: app_dir.to_owned(),
        path: config_path(app_dir),
        explicit: false,
    };
    get_selected_file_config(&target)
}

/// Load and parse the selected config file when present.
pub fn load_selected_raw_config(target: &ConfigFileTarget) -> crate::Result<RawConfig> {
    let file_config = get_selected_file_config(target)?;
    match file_config.source {
        Some(source) => raw_config_from_named_source(&file_config.path, &source),
        None => Ok(RawConfig::default()),
    }
}

/// Load and parse `config.toml` when present under the app directory.
pub fn load_file_raw_config(app_dir: &Path) -> crate::Result<RawConfig> {
    let target = ConfigFileTarget {
        app_dir: app_dir.to_owned(),
        path: config_path(app_dir),
        explicit: false,
    };
    load_selected_raw_config(&target)
}

/// Generate a starter config file if one does not exist.
pub fn generate_config(app_dir: &Path) -> crate::Result<PathBuf> {
    generate_config_file(&config_path(app_dir))
}

/// Generate a starter config file at an explicit path if one does not exist.
pub fn generate_config_file(path: &Path) -> crate::Result<PathBuf> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)
            .map_err(|error| config_error(format!("failed to create config directory: {error}")))?;
    }
    if path.exists() {
        return Ok(path.to_owned());
    }
    fs::write(path, config_template())
        .map_err(|error| config_error(format!("failed to write config file: {error}")))?;
    Ok(path.to_owned())
}

/// Starter config template.
pub const fn config_template() -> &'static str {
    "torznab = []\nuse_client_torrents = true\ndata_dirs = []\ntorrent_clients = []\n"
}

/// Parse simple duration strings used by CLI/config options.
pub fn parse_duration_millis(value: &str) -> crate::Result<u64> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(config_error("duration cannot be empty"));
    }
    if let Ok(number) = trimmed.parse::<u64>() {
        return Ok(number);
    }

    let split_at = trimmed
        .find(|character: char| !character.is_ascii_digit())
        .ok_or_else(|| config_error(format!("invalid duration: {value}")))?;
    let (number, unit) = trimmed.split_at(split_at);
    let number = number
        .parse::<u64>()
        .map_err(|error| config_error(format!("invalid duration number: {error}")))?;
    let multiplier = match unit.trim() {
        "ms" => 1,
        "s" | "sec" | "secs" | "second" | "seconds" => 1_000,
        "m" | "min" | "mins" | "minute" | "minutes" => 60_000,
        "h" | "hr" | "hrs" | "hour" | "hours" => 3_600_000,
        "d" | "day" | "days" => 86_400_000,
        _ => return Err(config_error(format!("invalid duration unit: {unit}"))),
    };

    number
        .checked_mul(multiplier)
        .ok_or_else(|| config_error("duration is too large"))
}

/// Apply supported `SPOROS__` scalar environment overrides.
pub fn apply_env_overrides(raw: &mut RawConfig) -> crate::Result<()> {
    apply_env_overrides_from(env::vars(), raw)
}

/// Apply supported env overrides from an explicit iterator for tests.
pub fn apply_env_overrides_from(
    values: impl IntoIterator<Item = (String, String)>,
    raw: &mut RawConfig,
) -> crate::Result<()> {
    for (key, value) in values {
        match key.as_str() {
            CONFIG_FILE_ENV => {}
            "SPOROS__API_KEY" => raw.api_key = Some(value),
            "SPOROS__LOG_FORMAT" => raw.log_format = Some(value),
            "SPOROS__LOG_LEVEL" => raw.log_level = Some(value),
            "SPOROS__LISTEN_HOST" => {
                raw.host = Some(value.parse().map_err(|error| {
                    config_error(format!("invalid SPOROS__LISTEN_HOST: {error}"))
                })?);
            }
            "SPOROS__LISTEN_PORT" => {
                raw.port = Some(Some(value.parse().map_err(|error| {
                    config_error(format!("invalid SPOROS__LISTEN_PORT: {error}"))
                })?));
            }
            "SPOROS__RSS_CADENCE" => raw.rss_cadence = Some(parse_duration_millis(&value)?),
            "SPOROS__SEARCH_CADENCE" => raw.search_cadence = Some(parse_duration_millis(&value)?),
            _ => {}
        }
    }
    Ok(())
}

fn verify_read_write_dir(path: &Path) -> crate::Result<()> {
    let metadata = fs::metadata(path)
        .map_err(|error| config_error(format!("failed to stat app directory: {error}")))?;
    if !metadata.is_dir() {
        return Err(config_error(format!(
            "app directory is not a directory: {}",
            path.display()
        )));
    }

    let probe = create_unique_probe(path, ".sporos-write-test")
        .map_err(|error| config_error(format!("app directory is not writable: {error}")))?;
    fs::remove_file(&probe)
        .map_err(|error| config_error(format!("failed to remove app directory probe: {error}")))?;
    Ok(())
}

fn create_unique_probe(dir: &Path, prefix: &str) -> std::io::Result<PathBuf> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    for attempt in 0..128 {
        let path = dir.join(format!("{prefix}-{}-{nanos}-{attempt}", std::process::id()));
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(mut file) => {
                file.write_all(b"test")?;
                return Ok(path);
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        "failed to allocate unique probe path",
    ))
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum DurationConfigValue {
    Integer(u64),
    String(String),
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RatioConfigValue {
    Number(f64),
}

fn deserialize_optional_duration<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    match Option::<DurationConfigValue>::deserialize(deserializer)? {
        None => Ok(None),
        Some(DurationConfigValue::Integer(value)) => Ok(Some(value)),
        Some(DurationConfigValue::String(value)) => parse_duration_millis(&value)
            .map(Some)
            .map_err(de::Error::custom),
    }
}

fn deserialize_optional_ratio<'de, D>(deserializer: D) -> std::result::Result<Option<f64>, D::Error>
where
    D: Deserializer<'de>,
{
    match Option::<RatioConfigValue>::deserialize(deserializer)? {
        None => Ok(None),
        Some(RatioConfigValue::Number(value)) => Ok(Some(value)),
    }
}

fn has_nested_paths(paths: impl Iterator<Item = PathBuf>) -> bool {
    let mut paths = paths.collect::<Vec<_>>();
    paths.sort();
    for (index, parent) in paths.iter().enumerate() {
        for child in paths.iter().skip(index + 1) {
            if child.starts_with(parent) || parent.starts_with(child) {
                return true;
            }
        }
    }
    false
}

fn validate_block_list(entries: &[String]) -> crate::Result<()> {
    let mut size_below = None;
    let mut size_above = None;
    for entry in entries {
        let trimmed = entry.trim();
        if trimmed.is_empty() {
            continue;
        }
        let (kind, value) = trimmed.split_once(':').ok_or_else(|| {
            config_error(format!(
                "invalid block_list entry {trimmed:?}: expected <type>:<value>"
            ))
        })?;
        match kind {
            "name" | "folder" | "category" | "tag" | "tracker" => {}
            "name_regex" | "folder_regex" => {
                Regex::new(value).map_err(|error| {
                    config_error(format!(
                        "invalid block_list {kind} entry {trimmed:?}: {error}"
                    ))
                })?;
            }
            "info_hash" => {
                if crate::domain::InfoHash::new(value.to_ascii_lowercase()).is_none() {
                    return Err(config_error(format!(
                        "invalid block_list info_hash entry {trimmed:?}"
                    )));
                }
            }
            "size_below" => {
                if size_below
                    .replace(parse_blocklist_size(trimmed, value)?)
                    .is_some()
                {
                    return Err(config_error("block_list allows only one size_below entry"));
                }
            }
            "size_above" => {
                if size_above
                    .replace(parse_blocklist_size(trimmed, value)?)
                    .is_some()
                {
                    return Err(config_error("block_list allows only one size_above entry"));
                }
            }
            _ => {
                return Err(config_error(format!(
                    "invalid block_list entry type {kind:?}; use explicit snake_case blocklist types"
                )));
            }
        }
    }
    if let (Some(below), Some(above)) = (size_below, size_above) {
        if below > above {
            return Err(config_error("block_list requires size_below <= size_above"));
        }
    }
    Ok(())
}

fn validate_log_format(value: &str) -> crate::Result<()> {
    match value.trim().to_ascii_lowercase().as_str() {
        "text" | "plain" | "pretty" | "json" => Ok(()),
        _ => Err(config_error(format!("invalid log_format: {value}"))),
    }
}

fn validate_log_level(value: &str) -> crate::Result<()> {
    match value.trim().to_ascii_lowercase().as_str() {
        "trace" | "verbose" | "debug" | "info" | "warn" | "warning" | "error" => Ok(()),
        _ => Err(config_error(format!("invalid log_level: {value}"))),
    }
}

fn parse_blocklist_size(entry: &str, value: &str) -> crate::Result<u64> {
    value
        .parse::<u64>()
        .map_err(|error| config_error(format!("invalid block_list size entry {entry:?}: {error}")))
}

fn config_error(message: impl Into<Cow<'static, str>>) -> SporosError {
    SporosError::configuration(message)
}

fn dev_mode_enabled() -> bool {
    env::var_os("DEV").is_some()
        || env::var("SPOROS_ENV")
            .map(|value| matches!(value.to_ascii_lowercase().as_str(), "dev" | "development"))
            .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::{
        Action, ApiIntegrationConfig, LinkType, MatchMode, RawConfig, RuntimeConfig,
        TorrentClientConfig, apply_env_overrides_from, config_file_from_sources,
        load_selected_raw_config, parse_duration_millis, raw_config_from_source,
    };
    use std::{
        ffi::OsStr,
        fs,
        path::{Path, PathBuf},
        time::{SystemTime, UNIX_EPOCH},
    };

    #[test]
    fn normalizes_defaults_and_supported_names() {
        let raw = RawConfig {
            match_mode: Some("strict".to_owned()),
            link_type: Some("reflink_or_copy".to_owned()),
            link_dirs: vec!["/links".into()],
            notification_webhook_urls: vec!["https://notify.example".to_owned()],
            torrent_clients: vec![
                TorrentClientConfig::parse("qbittorrent:http://localhost:8080").expect("client"),
            ],
            ..RawConfig::default()
        };

        let config = RuntimeConfig::normalize(raw, Path::new("/config")).expect("valid config");

        assert_eq!(config.match_mode, MatchMode::Strict);
        assert_eq!(config.link_type, LinkType::ReflinkOrCopy);
        assert_eq!(config.link_dirs, vec![Path::new("/links")]);
        assert_eq!(
            config.notification_webhook_urls,
            vec!["https://notify.example"]
        );
        assert_eq!(config.snatch_retries, 2);
        assert_eq!(config.torrent_clients[0].kind, "qbittorrent");
    }

    #[test]
    fn defaults_runtime_paths_from_state_dir() {
        let config =
            RuntimeConfig::normalize(RawConfig::default(), Path::new("/state")).expect("config");

        assert_eq!(config.state_dir, Path::new("/state"));
        assert_eq!(config.database_path, Path::new("/state/sporos.db"));
        assert_eq!(config.output_dir, Path::new("/state/cross-seeds"));
    }

    #[test]
    fn explicit_state_and_database_paths_do_not_follow_config_file_location() {
        let raw = RawConfig {
            state_dir: Some("/writable/state".into()),
            database_path: Some("/writable/db/custom.sqlite".into()),
            ..RawConfig::default()
        };

        let config =
            RuntimeConfig::normalize(raw, Path::new("/readonly/config-parent")).expect("config");

        assert_eq!(config.state_dir, Path::new("/writable/state"));
        assert_eq!(
            config.database_path,
            Path::new("/writable/db/custom.sqlite")
        );
        assert_eq!(config.output_dir, Path::new("/writable/state/cross-seeds"));
    }

    #[test]
    fn validates_snatch_retries_bound() {
        let config = RuntimeConfig::normalize(
            RawConfig {
                snatch_retries: Some(11),
                ..RawConfig::default()
            },
            Path::new("/config"),
        )
        .expect_err("snatch retry bound");

        assert!(config.to_string().contains("snatch_retries"));
    }

    #[test]
    fn rejects_unsupported_match_mode_names() {
        for match_mode in ["safe", "risky"] {
            let error = RuntimeConfig::normalize(
                RawConfig {
                    match_mode: Some(match_mode.to_owned()),
                    ..RawConfig::default()
                },
                Path::new("/config"),
            )
            .expect_err("unsupported match mode rejected");

            assert!(error.to_string().contains("invalid match_mode"));
        }
    }

    #[test]
    fn parses_readonly_torrent_client_entries() {
        let client = TorrentClientConfig::parse(
            "rtorrent:readonly:http://username:password@localhost:1234/RPC2",
        )
        .expect("client parses");

        assert_eq!(client.kind, "rtorrent");
        assert!(client.readonly);
        assert_eq!(client.url, "http://username:password@localhost:1234/RPC2");
    }

    #[test]
    fn validates_incompatible_source_options() {
        let raw = RawConfig {
            torrent_dir: Some("/torrents".into()),
            use_client_torrents: Some(true),
            torrent_clients: vec![
                TorrentClientConfig::parse("qbittorrent:http://localhost:8080").expect("client"),
            ],
            ..RawConfig::default()
        };

        let error = RuntimeConfig::normalize(raw, Path::new("/config")).expect_err("invalid");

        assert!(error.to_string().contains("torrent_dir cannot be used"));
    }

    #[test]
    fn validates_scheduled_rss_requires_a_local_source() {
        let raw = RawConfig {
            rss_cadence: Some(900_000),
            fuzzy_size_threshold: Some(0.1),
            ..RawConfig::default()
        };

        let error = RuntimeConfig::normalize(raw, Path::new("/config")).expect_err("invalid");

        assert!(error.to_string().contains("scheduled search/rss"));
    }

    #[test]
    fn validates_scheduled_rss_cadence_bounds() {
        let too_low = RawConfig {
            rss_cadence: Some(60_000),
            fuzzy_size_threshold: Some(0.1),
            data_dirs: vec!["/data".into()],
            ..RawConfig::default()
        };
        let error = RuntimeConfig::normalize(too_low, Path::new("/config")).expect_err("low");
        assert!(error.to_string().contains("rss_cadence"));

        let too_high = RawConfig {
            rss_cadence: Some(3 * 60 * 60 * 1_000),
            fuzzy_size_threshold: Some(0.1),
            data_dirs: vec!["/data".into()],
            ..RawConfig::default()
        };
        let error = RuntimeConfig::normalize(too_high, Path::new("/config")).expect_err("high");
        assert!(error.to_string().contains("rss_cadence"));
    }

    #[test]
    fn validates_scheduled_search_cadence_minimum() {
        let raw = RawConfig {
            search_cadence: Some(60_000),
            fuzzy_size_threshold: Some(0.1),
            data_dirs: vec!["/data".into()],
            ..RawConfig::default()
        };

        let error = RuntimeConfig::normalize(raw, Path::new("/config")).expect_err("invalid");

        assert!(error.to_string().contains("search_cadence"));
    }

    #[test]
    fn validates_inject_requires_writable_client() {
        let raw = RawConfig {
            action: Some("inject".to_owned()),
            torrent_clients: vec![
                TorrentClientConfig::parse("qbittorrent:readonly:http://localhost:8080")
                    .expect("client"),
            ],
            ..RawConfig::default()
        };

        let error = RuntimeConfig::normalize(raw, Path::new("/config")).expect_err("invalid");

        assert!(error.to_string().contains("non-readonly client"));
    }

    #[test]
    fn rejects_duplicate_torrent_client_urls() {
        let raw = RawConfig {
            torrent_clients: vec![
                TorrentClientConfig::parse("qbittorrent:http://localhost:8080").expect("client"),
                TorrentClientConfig::parse("qbittorrent:http://localhost:8080").expect("client"),
            ],
            ..RawConfig::default()
        };

        let error = RuntimeConfig::normalize(raw, Path::new("/config")).expect_err("invalid");

        assert!(error.to_string().contains("duplicate torrent client URL"));
    }

    #[test]
    fn parses_duration_units() {
        assert_eq!(parse_duration_millis("30s").expect("duration"), 30_000);
        assert_eq!(
            parse_duration_millis("10 minutes").expect("duration"),
            600_000
        );
        assert_eq!(parse_duration_millis("42").expect("duration"), 42);
    }

    #[test]
    fn parses_enums() {
        assert_eq!(
            MatchMode::parse("flexible").expect("mode"),
            MatchMode::Flexible
        );
        assert_eq!(Action::parse("inject").expect("action"), Action::Inject);
    }

    #[test]
    fn app_dir_write_probe_does_not_clobber_existing_probe_name() {
        let root = temp_path("config-probe-collision");
        fs::create_dir_all(&root).expect("root");
        let existing_probe = root.join(".sporos-write-test");
        fs::write(&existing_probe, b"user data").expect("existing probe");

        super::verify_read_write_dir(&root).expect("writable app dir");

        assert_eq!(
            fs::read(existing_probe).expect("existing probe"),
            b"user data"
        );
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn loads_config_toml() {
        let raw = raw_config_from_source(
            r#"
            use_client_torrents = true
            data_dirs = ["/data"]
            match_mode = "flexible"
            exclude_older = "2 days"
            exclude_recent_search = 86400000
            season_from_episodes = 0.75
            snatch_retries = 4
            log_format = "json"
            log_level = "debug"
            link_dirs = ["/links"]
            state_dir = "/state"
            database_path = "/state/sporos.db"

            [[torznab]]
            url = "https://indexer.example/api"
            api_key = "secret"

            [[torrent_clients]]
            kind = "qbittorrent"
            url = "http://localhost:8080"
            "#,
        )
        .expect("config parses");

        assert_eq!(
            raw.torznab,
            vec![ApiIntegrationConfig {
                url: "https://indexer.example/api".to_owned(),
                api_key: "secret".to_owned(),
            }]
        );
        assert_eq!(raw.use_client_torrents, Some(true));
        assert_eq!(raw.data_dirs, vec![Path::new("/data")]);
        assert_eq!(raw.match_mode.as_deref(), Some("flexible"));
        assert_eq!(raw.exclude_older, Some(172_800_000));
        assert_eq!(raw.exclude_recent_search, Some(86_400_000));
        assert_eq!(raw.season_from_episodes, Some(0.75));
        assert_eq!(raw.snatch_retries, Some(4));
        assert_eq!(raw.log_format.as_deref(), Some("json"));
        assert_eq!(raw.log_level.as_deref(), Some("debug"));
        assert_eq!(raw.state_dir.as_deref(), Some(Path::new("/state")));
        assert_eq!(
            raw.database_path.as_deref(),
            Some(Path::new("/state/sporos.db"))
        );
        assert_eq!(
            raw.torrent_clients,
            vec![TorrentClientConfig {
                kind: "qbittorrent".to_owned(),
                readonly: false,
                url: "http://localhost:8080".to_owned(),
            }]
        );
        assert_eq!(raw.link_dirs, vec![Path::new("/links")]);
    }

    #[test]
    fn selects_explicit_config_file_before_env_and_default() {
        let app_dir = Path::new("/state");
        let target = config_file_from_sources(
            Some(Path::new("/cli/config.toml")),
            Some(OsStr::new("/env/config.toml")),
            app_dir,
        )
        .expect("selected");

        assert_eq!(target.app_dir, app_dir);
        assert_eq!(target.path, Path::new("/cli/config.toml"));
        assert!(target.explicit);
    }

    #[test]
    fn selects_env_config_file_before_local_default() {
        let app_dir = Path::new("/state");
        let target = config_file_from_sources(None, Some(OsStr::new("/env/config.toml")), app_dir)
            .expect("selected");

        assert_eq!(target.app_dir, app_dir);
        assert_eq!(target.path, Path::new("/env/config.toml"));
        assert!(target.explicit);
    }

    #[test]
    fn keeps_local_default_config_file_compatible() {
        let app_dir = Path::new("/state");
        let target = config_file_from_sources(None, None, app_dir).expect("selected");

        assert_eq!(target.app_dir, app_dir);
        assert_eq!(target.path, Path::new("/state/config.toml"));
        assert!(!target.explicit);
    }

    #[test]
    fn reports_explicit_config_read_errors_with_path() {
        let target = config_file_from_sources(
            Some(Path::new("/definitely/missing/sporos.toml")),
            None,
            Path::new("/state"),
        )
        .expect("selected");

        let error = load_selected_raw_config(&target).expect_err("missing file");

        assert!(error.to_string().contains("config file not found"));
        assert!(
            error
                .to_string()
                .contains("/definitely/missing/sporos.toml")
        );
    }

    #[test]
    fn applies_namespaced_scalar_env_overrides() {
        let mut raw = RawConfig {
            api_key: Some("file-file-file-file-file".to_owned()),
            port: Some(Some(1111)),
            ..RawConfig::default()
        };

        apply_env_overrides_from(
            [
                (
                    "SPOROS__API_KEY".to_owned(),
                    "env-env-env-env-env-env".to_owned(),
                ),
                ("SPOROS__LISTEN_HOST".to_owned(), "127.0.0.1".to_owned()),
                ("SPOROS__LISTEN_PORT".to_owned(), "2222".to_owned()),
                ("SPOROS__LOG_FORMAT".to_owned(), "json".to_owned()),
                ("SPOROS__LOG_LEVEL".to_owned(), "debug".to_owned()),
                ("SPOROS__RSS_CADENCE".to_owned(), "30 minutes".to_owned()),
                ("SPOROS__SEARCH_CADENCE".to_owned(), "1 day".to_owned()),
                (
                    "SPOROS__TORZNAB".to_owned(),
                    "https://indexer.example/api?apikey=ignored".to_owned(),
                ),
            ],
            &mut raw,
        )
        .expect("env overrides");

        assert_eq!(raw.api_key.as_deref(), Some("env-env-env-env-env-env"));
        assert_eq!(raw.host, Some("127.0.0.1".parse().expect("ip")));
        assert_eq!(raw.port, Some(Some(2222)));
        assert_eq!(raw.log_format.as_deref(), Some("json"));
        assert_eq!(raw.log_level.as_deref(), Some("debug"));
        assert_eq!(raw.rss_cadence, Some(1_800_000));
        assert_eq!(raw.search_cadence, Some(86_400_000));
        assert!(raw.torznab.is_empty());
    }

    #[test]
    fn rejects_invalid_scalar_env_overrides() {
        for (key, value) in [
            ("SPOROS__LISTEN_HOST", "not an ip"),
            ("SPOROS__LISTEN_PORT", "not a port"),
            ("SPOROS__RSS_CADENCE", "not a duration"),
            ("SPOROS__SEARCH_CADENCE", "not a duration"),
        ] {
            let mut raw = RawConfig::default();
            let error = apply_env_overrides_from([(key.to_owned(), value.to_owned())], &mut raw)
                .expect_err("invalid env rejected");

            assert!(error.to_string().contains("configuration error"));
        }
    }

    #[test]
    fn validates_configured_logging_scalars() {
        for raw in [
            RawConfig {
                log_format: Some("xml".to_owned()),
                ..RawConfig::default()
            },
            RawConfig {
                log_level: Some("crate=debug".to_owned()),
                ..RawConfig::default()
            },
        ] {
            let error = RuntimeConfig::normalize(raw, Path::new("/config"))
                .expect_err("invalid logging config");

            assert!(error.to_string().contains("log_"));
        }
    }

    #[test]
    fn rejects_boolean_duration_and_ratio_values() {
        for source in [
            "exclude_older = false",
            "exclude_recent_search = true",
            "rss_cadence = false",
            "search_cadence = true",
            "snatch_timeout = false",
            "search_timeout = true",
            "season_from_episodes = false",
            "season_from_episodes = true",
        ] {
            let error = raw_config_from_source(source).expect_err("boolean value rejected");

            assert!(error.to_string().contains("failed to parse config.toml"));
        }
    }

    #[test]
    fn rejects_camel_case_config_keys() {
        let error = raw_config_from_source("useClientTorrents = true")
            .expect_err("camelCase key is rejected");

        assert!(error.to_string().contains("failed to parse config.toml"));
    }

    #[test]
    fn rejects_string_torrent_client_config_entries() {
        let error =
            raw_config_from_source(r#"torrent_clients = ["qbittorrent:http://localhost:8080"]"#)
                .expect_err("string client entries are rejected");

        assert!(error.to_string().contains("failed to parse config.toml"));
    }

    #[test]
    fn rejects_string_integration_config_entries() {
        let error =
            raw_config_from_source(r#"torznab = ["https://indexer.example/api?apikey=secret"]"#)
                .expect_err("string integration entries are rejected");

        assert!(error.to_string().contains("failed to parse config.toml"));
    }

    #[test]
    fn validates_blocklist_entries() {
        let raw = RawConfig {
            block_list: vec![
                "name:blocked".to_owned(),
                "name_regex:(?i)blocked".to_owned(),
                "folder:/downloads".to_owned(),
                "folder_regex:/downloads/.+".to_owned(),
                "category:tv".to_owned(),
                "tag:".to_owned(),
                "tracker:tracker.example".to_owned(),
                "info_hash:0123456789abcdef0123456789abcdef01234567".to_owned(),
                "size_below:10".to_owned(),
                "size_above:100".to_owned(),
            ],
            ..RawConfig::default()
        };

        let config = RuntimeConfig::normalize(raw, Path::new("/config")).expect("valid");

        assert_eq!(config.block_list.len(), 10);
    }

    #[test]
    fn rejects_unsupported_blocklist_entries() {
        let raw = RawConfig {
            block_list: vec!["folderRegex:/downloads".to_owned()],
            ..RawConfig::default()
        };

        let error = RuntimeConfig::normalize(raw, Path::new("/config")).expect_err("invalid");

        assert!(error.to_string().contains("invalid block_list entry type"));
    }

    #[test]
    fn rejects_invalid_blocklist_size_bounds() {
        let raw = RawConfig {
            block_list: vec!["size_below:100".to_owned(), "size_above:10".to_owned()],
            ..RawConfig::default()
        };

        let error = RuntimeConfig::normalize(raw, Path::new("/config")).expect_err("invalid");

        assert!(error.to_string().contains("size_below <= size_above"));
    }

    fn temp_path(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        std::env::temp_dir().join(format!("sporos-{label}-{nanos}"))
    }
}
