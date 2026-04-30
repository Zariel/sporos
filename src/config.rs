//! Configuration discovery, schema validation, and runtime config assembly.

use std::{
    borrow::Cow,
    collections::BTreeMap,
    env, fs,
    net::IpAddr,
    path::{Path, PathBuf},
};

use crate::SporosError;

const APP_DIR_NAME: &str = "cross-seed";
const CONFIG_FILE_NAME: &str = "config.js";
const DEFAULT_DELAY_SECONDS: u64 = 30;
const DEFAULT_PORT: u16 = 2468;
const DEFAULT_MAX_DATA_DEPTH: u32 = 2;
const DEFAULT_FUZZY_SIZE_THRESHOLD: f64 = 0.05;
const MAX_AUTO_RESUME_DOWNLOAD_BYTES: u64 = 52_428_800;

/// Match mode after deprecated names have been normalized.
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
    /// Normalize current and deprecated config spellings.
    pub fn parse(value: &str) -> crate::Result<Self> {
        match value {
            "strict" | "safe" => Ok(Self::Strict),
            "flexible" | "risky" => Ok(Self::Flexible),
            "partial" => Ok(Self::Partial),
            _ => Err(config_error(format!("invalid matchMode: {value}"))),
        }
    }
}

/// Action mode for matched candidates.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Action {
    /// Save matched torrents to `outputDir`.
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
            "reflinkOrCopy" => Ok(Self::ReflinkOrCopy),
            _ => Err(config_error(format!("invalid linkType: {value}"))),
        }
    }
}

/// Parsed torrent-client entry.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TorrentClientConfig {
    /// Client adapter type.
    pub kind: String,
    /// Whether the client is readonly.
    pub readonly: bool,
    /// Client URL string.
    pub url: String,
}

impl TorrentClientConfig {
    /// Parse `<type>:[readonly:]<url>`.
    pub fn parse(value: &str) -> crate::Result<Self> {
        let (kind, rest) = value
            .split_once(':')
            .ok_or_else(|| config_error("torrent client entry missing URL"))?;
        if kind.is_empty() {
            return Err(config_error("torrent client entry missing type"));
        }

        let (readonly, url) = if let Some(url) = rest.strip_prefix("readonly:") {
            (true, url)
        } else {
            (false, rest)
        };

        match kind {
            "qbittorrent" | "rtorrent" | "transmission" | "deluge" => {}
            _ => return Err(config_error(format!("unsupported torrent client: {kind}"))),
        }

        if !url.contains("://") {
            return Err(config_error("torrent client URL must include a scheme"));
        }

        Ok(Self {
            kind: kind.to_owned(),
            readonly,
            url: url.to_owned(),
        })
    }
}

/// Deprecated config fields that map into current options.
#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct DeprecatedConfig {
    /// Deprecated singular link dir.
    pub link_dir: Option<PathBuf>,
    /// Deprecated singular notification URL.
    pub notification_webhook_url: Option<String>,
    /// Deprecated qBittorrent URL.
    pub qbittorrent_url: Option<String>,
    /// Deprecated rTorrent URL.
    pub rtorrent_rpc_url: Option<String>,
    /// Deprecated Transmission URL.
    pub transmission_rpc_url: Option<String>,
    /// Deprecated Deluge URL.
    pub deluge_rpc_url: Option<String>,
}

/// Raw options before defaults and cross-option validation.
#[derive(Debug, Default, Clone)]
pub struct RawConfig {
    /// Delay in seconds.
    pub delay: Option<u64>,
    /// Torznab URLs.
    pub torznab: Vec<String>,
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
    pub season_from_episodes: Option<f64>,
    /// Exclude older duration in ms.
    pub exclude_older: Option<u64>,
    /// Exclude recent search duration in ms.
    pub exclude_recent_search: Option<u64>,
    /// Action text.
    pub action: Option<String>,
    /// Torrent-client entries.
    pub torrent_clients: Vec<String>,
    /// Duplicate categories.
    pub duplicate_categories: Option<bool>,
    /// Notification URLs.
    pub notification_webhook_urls: Vec<String>,
    /// Daemon port.
    pub port: Option<Option<u16>>,
    /// Daemon host.
    pub host: Option<IpAddr>,
    /// RSS cadence in ms.
    pub rss_cadence: Option<u64>,
    /// Search cadence in ms.
    pub search_cadence: Option<u64>,
    /// Snatch timeout in ms.
    pub snatch_timeout: Option<u64>,
    /// Search timeout in ms.
    pub search_timeout: Option<u64>,
    /// Search limit.
    pub search_limit: Option<u32>,
    /// Verbose logs.
    pub verbose: Option<bool>,
    /// Hidden targeted torrent paths.
    pub torrents: Option<Vec<PathBuf>>,
    /// Blocklist entries.
    pub block_list: Vec<String>,
    /// API key.
    pub api_key: Option<String>,
    /// Sonarr URLs.
    pub sonarr: Vec<String>,
    /// Radarr URLs.
    pub radarr: Vec<String>,
    /// Deprecated fields.
    pub deprecated: DeprecatedConfig,
}

/// Runtime config after defaults and validation.
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    /// Delay in seconds.
    pub delay: u64,
    /// Torznab URLs.
    pub torznab: Vec<String>,
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
    /// Search timeout in ms.
    pub search_timeout: Option<u64>,
    /// Search limit.
    pub search_limit: Option<u32>,
    /// Verbose logs.
    pub verbose: bool,
    /// Hidden targeted torrent paths.
    pub torrents: Option<Vec<PathBuf>>,
    /// Blocklist entries.
    pub block_list: Vec<String>,
    /// API key.
    pub api_key: Option<String>,
    /// Sonarr URLs.
    pub sonarr: Vec<String>,
    /// Radarr URLs.
    pub radarr: Vec<String>,
}

impl RuntimeConfig {
    /// Normalize raw options and run documented cross-option checks.
    pub fn normalize(raw: RawConfig, app_dir: &Path) -> crate::Result<Self> {
        let mut link_dirs = raw.link_dirs;
        if link_dirs.is_empty() {
            if let Some(link_dir) = raw.deprecated.link_dir {
                link_dirs.push(link_dir);
            }
        }

        let mut notification_webhook_urls = raw.notification_webhook_urls;
        if notification_webhook_urls.is_empty() {
            if let Some(url) = raw.deprecated.notification_webhook_url {
                notification_webhook_urls.push(url);
            }
        }

        let mut torrent_clients = raw.torrent_clients;
        if torrent_clients.is_empty() {
            push_deprecated_client(
                &mut torrent_clients,
                "qbittorrent",
                raw.deprecated.qbittorrent_url,
            );
            push_deprecated_client(
                &mut torrent_clients,
                "rtorrent",
                raw.deprecated.rtorrent_rpc_url,
            );
            push_deprecated_client(
                &mut torrent_clients,
                "transmission",
                raw.deprecated.transmission_rpc_url,
            );
            push_deprecated_client(
                &mut torrent_clients,
                "deluge",
                raw.deprecated.deluge_rpc_url,
            );
        }

        let parsed_clients = torrent_clients
            .iter()
            .map(|client| TorrentClientConfig::parse(client))
            .collect::<crate::Result<Vec<_>>>()?;
        let config = Self {
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
                .unwrap_or_else(|| app_dir.join("cross-seeds")),
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
            torrent_clients: parsed_clients,
            duplicate_categories: raw.duplicate_categories.unwrap_or(false),
            notification_webhook_urls,
            port: raw.port.unwrap_or(Some(DEFAULT_PORT)),
            host: raw.host,
            rss_cadence: raw.rss_cadence,
            search_cadence: raw.search_cadence,
            snatch_timeout: raw.snatch_timeout,
            search_timeout: raw.search_timeout,
            search_limit: raw.search_limit,
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
        if self.max_data_depth == 0 {
            return Err(config_error("maxDataDepth must be at least 1"));
        }
        if !(self.fuzzy_size_threshold > 0.0 && self.fuzzy_size_threshold <= 1.0) {
            return Err(config_error("fuzzySizeThreshold must be > 0 and <= 1"));
        }
        if self.auto_resume_max_download > MAX_AUTO_RESUME_DOWNLOAD_BYTES {
            return Err(config_error("autoResumeMaxDownload exceeds 52428800 bytes"));
        }
        if let Some(api_key) = &self.api_key {
            if api_key.len() < 24 {
                return Err(config_error("apiKey must be at least 24 characters"));
            }
        }
        if self.torrent_dir.is_some() && self.use_client_torrents {
            return Err(config_error(
                "torrentDir cannot be used with useClientTorrents",
            ));
        }
        if self.use_client_torrents && self.torrent_clients.is_empty() {
            return Err(config_error("useClientTorrents requires torrentClients"));
        }
        if self.action == Action::Inject && self.torrent_clients.is_empty() {
            return Err(config_error("action inject requires torrentClients"));
        }
        if self.action == Action::Inject
            && self.torrent_clients.iter().all(|client| client.readonly)
        {
            return Err(config_error("action inject requires a non-readonly client"));
        }
        if self.torrent_clients.len() > 1 && self.torrent_dir.is_some() {
            return Err(config_error(
                "multiple clients cannot be combined with torrentDir",
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
                "multiple clients plus dataDirs require linkDirs",
            ));
        }
        if self.inject_dir.is_some() && self.action != Action::Inject {
            return Err(config_error("injectDir is only valid with action inject"));
        }
        if self.action == Action::Inject
            && matches!(self.match_mode, MatchMode::Flexible | MatchMode::Partial)
            && self.link_dirs.is_empty()
        {
            return Err(config_error(
                "injecting with flexible or partial matchMode requires linkDirs",
            ));
        }
        if let Some(season_from_episodes) = self.season_from_episodes {
            if !(season_from_episodes > 0.0 && season_from_episodes <= 1.0) {
                return Err(config_error("seasonFromEpisodes must be > 0 and <= 1"));
            }
            if season_from_episodes < 1.0 && self.match_mode != MatchMode::Partial {
                return Err(config_error(
                    "seasonFromEpisodes below 1 requires matchMode partial",
                ));
            }
            if self.action == Action::Inject && self.link_dirs.is_empty() {
                return Err(config_error(
                    "seasonFromEpisodes with action inject requires linkDirs",
                ));
            }
        }
        if let (Some(search_cadence), Some(exclude_recent_search)) =
            (self.search_cadence, self.exclude_recent_search)
        {
            if exclude_recent_search < search_cadence.saturating_mul(3) {
                return Err(config_error(
                    "excludeRecentSearch must be at least 3x searchCadence",
                ));
            }
        }
        if self.search_cadence.is_some() {
            let (Some(exclude_older), Some(exclude_recent_search)) =
                (self.exclude_older, self.exclude_recent_search)
            else {
                return Err(config_error(
                    "scheduled search requires excludeOlder and excludeRecentSearch",
                ));
            };
            if exclude_older < exclude_recent_search.saturating_mul(2)
                || exclude_older > exclude_recent_search.saturating_mul(5)
            {
                return Err(config_error(
                    "excludeOlder must be between 2x and 5x excludeRecentSearch",
                ));
            }
        }
        if (self.search_cadence.is_some() || self.rss_cadence.is_some())
            && self.fuzzy_size_threshold > 0.1
        {
            return Err(config_error(
                "scheduled search/rss requires fuzzySizeThreshold <= 0.1",
            ));
        }
        if self.search_cadence.is_some()
            && self.torrent_dir.is_none()
            && !self.use_client_torrents
            && self.data_dirs.is_empty()
        {
            return Err(config_error(
                "scheduled search requires torrentDir, useClientTorrents, or dataDirs",
            ));
        }
        if has_nested_paths(
            std::iter::once(self.output_dir.clone())
                .chain(self.link_dirs.iter().cloned())
                .chain(self.data_dirs.iter().cloned())
                .chain(self.torrent_dir.iter().cloned()),
        ) {
            return Err(config_error(
                "linkDirs, dataDirs, torrentDir, and outputDir cannot be nested",
            ));
        }

        Ok(())
    }
}

/// Parse raw `config.js` source into typed raw options.
pub fn raw_config_from_source(source: &str) -> crate::Result<RawConfig> {
    let object_source = exported_object_source(source)?;
    let mut parser = JsConfigParser::new(&object_source);
    let value = parser.parse_value()?;
    parser.finish()?;

    let JsConfigValue::Object(object) = value else {
        return Err(config_error("config.js default export must be an object"));
    };

    raw_config_from_object(&object)
}

/// Minimal representation of discovered `config.js`.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FileConfig {
    /// Full path to the config file.
    pub path: PathBuf,
    /// Raw JavaScript source when the file exists.
    pub source: Option<String>,
}

/// Resolve and create the cross-seed app directory.
pub fn app_dir() -> crate::Result<PathBuf> {
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

/// Path to `config.js` under the app directory.
pub fn config_path(app_dir: &Path) -> PathBuf {
    app_dir.join(CONFIG_FILE_NAME)
}

/// Load raw `config.js` source if present.
pub fn get_file_config(app_dir: &Path) -> crate::Result<FileConfig> {
    let path = config_path(app_dir);
    let source = match fs::read_to_string(&path) {
        Ok(source) => Some(source),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if env::var_os("DOCKER_ENV").is_some() {
                generate_config(app_dir)?;
                Some(fs::read_to_string(&path).map_err(|error| {
                    config_error(format!("failed to read generated config.js: {error}"))
                })?)
            } else {
                None
            }
        }
        Err(error) => return Err(config_error(format!("failed to read config.js: {error}"))),
    };

    Ok(FileConfig { path, source })
}

/// Load and parse `config.js` when present.
pub fn load_file_raw_config(app_dir: &Path) -> crate::Result<RawConfig> {
    let file_config = get_file_config(app_dir)?;
    match file_config.source {
        Some(source) => raw_config_from_source(&source),
        None => Ok(RawConfig::default()),
    }
}

/// Generate a starter config file if one does not exist.
pub fn generate_config(app_dir: &Path) -> crate::Result<PathBuf> {
    fs::create_dir_all(app_dir)
        .map_err(|error| config_error(format!("failed to create app directory: {error}")))?;
    let path = config_path(app_dir);
    if path.exists() {
        return Ok(path);
    }
    fs::write(&path, config_template())
        .map_err(|error| config_error(format!("failed to write config.js: {error}")))?;
    Ok(path)
}

/// Starter config template.
pub const fn config_template() -> &'static str {
    "export default {\n  torznab: [],\n  useClientTorrents: true,\n  dataDirs: [],\n  torrentClients: [],\n};\n"
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

fn verify_read_write_dir(path: &Path) -> crate::Result<()> {
    let metadata = fs::metadata(path)
        .map_err(|error| config_error(format!("failed to stat app directory: {error}")))?;
    if !metadata.is_dir() {
        return Err(config_error(format!(
            "app directory is not a directory: {}",
            path.display()
        )));
    }

    let probe = path.join(".sporos-write-test");
    fs::write(&probe, b"test")
        .map_err(|error| config_error(format!("app directory is not writable: {error}")))?;
    fs::remove_file(&probe)
        .map_err(|error| config_error(format!("failed to remove app directory probe: {error}")))?;
    Ok(())
}

fn push_deprecated_client(clients: &mut Vec<String>, kind: &str, url: Option<String>) {
    if let Some(url) = url {
        clients.push(format!("{kind}:{url}"));
    }
}

fn raw_config_from_object(object: &BTreeMap<String, JsConfigValue>) -> crate::Result<RawConfig> {
    Ok(RawConfig {
        delay: optional_u64(object, "delay")?,
        torznab: string_array(object, "torznab")?,
        use_client_torrents: optional_bool(object, "useClientTorrents")?,
        data_dirs: path_array(object, "dataDirs")?,
        match_mode: optional_string(object, "matchMode")?,
        skip_recheck: optional_bool(object, "skipRecheck")?,
        auto_resume_max_download: optional_u64(object, "autoResumeMaxDownload")?,
        ignore_non_relevant_files_to_resume: optional_bool(
            object,
            "ignoreNonRelevantFilesToResume",
        )?,
        link_category: optional_string(object, "linkCategory")?,
        link_dirs: path_array(object, "linkDirs")?,
        link_type: optional_string(object, "linkType")?,
        flat_linking: optional_bool(object, "flatLinking")?,
        max_data_depth: optional_u32(object, "maxDataDepth")?,
        torrent_dir: optional_path(object, "torrentDir")?,
        output_dir: optional_path(object, "outputDir")?,
        inject_dir: optional_path(object, "injectDir")?,
        ignore_titles: optional_bool(object, "ignoreTitles")?,
        include_single_episodes: optional_bool(object, "includeSingleEpisodes")?,
        include_non_videos: optional_bool(object, "includeNonVideos")?,
        fuzzy_size_threshold: optional_f64(object, "fuzzySizeThreshold")?,
        season_from_episodes: optional_ratio_or_false(object, "seasonFromEpisodes")?,
        exclude_older: optional_duration_or_false(object, "excludeOlder")?,
        exclude_recent_search: optional_duration_or_false(object, "excludeRecentSearch")?,
        action: optional_string(object, "action")?,
        torrent_clients: string_array(object, "torrentClients")?,
        duplicate_categories: optional_bool(object, "duplicateCategories")?,
        notification_webhook_urls: string_array(object, "notificationWebhookUrls")?,
        rss_cadence: optional_duration_or_false(object, "rssCadence")?,
        search_cadence: optional_duration_or_false(object, "searchCadence")?,
        snatch_timeout: optional_duration_or_false(object, "snatchTimeout")?,
        search_timeout: optional_duration_or_false(object, "searchTimeout")?,
        search_limit: optional_u32(object, "searchLimit")?,
        verbose: optional_bool(object, "verbose")?,
        block_list: string_array(object, "blockList")?,
        api_key: optional_string(object, "apiKey")?,
        sonarr: string_array(object, "sonarr")?,
        radarr: string_array(object, "radarr")?,
        deprecated: DeprecatedConfig {
            link_dir: optional_path(object, "linkDir")?,
            notification_webhook_url: optional_string(object, "notificationWebhookUrl")?,
            qbittorrent_url: optional_string(object, "qbittorrentUrl")?,
            rtorrent_rpc_url: optional_string(object, "rtorrentRpcUrl")?,
            transmission_rpc_url: optional_string(object, "transmissionRpcUrl")?,
            deluge_rpc_url: optional_string(object, "delugeRpcUrl")?,
        },
        ..RawConfig::default()
    })
}

fn exported_object_source(source: &str) -> crate::Result<String> {
    let source = strip_comments(source);
    let trimmed = source.trim();
    let body = trimmed
        .strip_prefix("export default")
        .or_else(|| trimmed.strip_prefix("module.exports ="))
        .unwrap_or(trimmed)
        .trim()
        .trim_end_matches(';')
        .trim();

    if body.starts_with('{') {
        Ok(body.to_owned())
    } else {
        Err(config_error(
            "config.js must export an object literal with export default",
        ))
    }
}

#[derive(Debug, Clone, PartialEq)]
enum JsConfigValue {
    Null,
    Bool(bool),
    Number(f64),
    String(String),
    Array(Vec<JsConfigValue>),
    Object(BTreeMap<String, JsConfigValue>),
}

struct JsConfigParser<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> JsConfigParser<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            input: input.as_bytes(),
            offset: 0,
        }
    }

    fn parse_value(&mut self) -> crate::Result<JsConfigValue> {
        self.skip_ws();
        match self.peek() {
            Some(b'{') => self.parse_object(),
            Some(b'[') => self.parse_array(),
            Some(b'"' | b'\'') => self.parse_string().map(JsConfigValue::String),
            Some(b't') => {
                self.expect_keyword("true")?;
                Ok(JsConfigValue::Bool(true))
            }
            Some(b'f') => {
                self.expect_keyword("false")?;
                Ok(JsConfigValue::Bool(false))
            }
            Some(b'n') => {
                self.expect_keyword("null")?;
                Ok(JsConfigValue::Null)
            }
            Some(b'-' | b'0'..=b'9') => self.parse_number().map(JsConfigValue::Number),
            Some(byte) => Err(config_error(format!(
                "unexpected config token '{}' at byte {}",
                char::from(byte),
                self.offset
            ))),
            None => Err(config_error("unexpected end of config")),
        }
    }

    fn parse_object(&mut self) -> crate::Result<JsConfigValue> {
        self.expect_byte(b'{')?;
        let mut object = BTreeMap::new();
        loop {
            self.skip_ws();
            if self.consume_if(b'}') {
                break;
            }
            let key = self.parse_key()?;
            self.skip_ws();
            self.expect_byte(b':')?;
            let value = self.parse_value()?;
            object.insert(key, value);
            self.skip_ws();
            if self.consume_if(b',') {
                continue;
            }
            self.expect_byte(b'}')?;
            break;
        }
        Ok(JsConfigValue::Object(object))
    }

    fn parse_array(&mut self) -> crate::Result<JsConfigValue> {
        self.expect_byte(b'[')?;
        let mut values = Vec::new();
        loop {
            self.skip_ws();
            if self.consume_if(b']') {
                break;
            }
            values.push(self.parse_value()?);
            self.skip_ws();
            if self.consume_if(b',') {
                continue;
            }
            self.expect_byte(b']')?;
            break;
        }
        Ok(JsConfigValue::Array(values))
    }

    fn parse_key(&mut self) -> crate::Result<String> {
        self.skip_ws();
        match self.peek() {
            Some(b'"' | b'\'') => self.parse_string(),
            Some(byte) if is_identifier_start(byte) => self.parse_identifier(),
            _ => Err(config_error(format!(
                "expected object key at byte {}",
                self.offset
            ))),
        }
    }

    fn parse_string(&mut self) -> crate::Result<String> {
        let quote = self
            .next()
            .ok_or_else(|| config_error("unexpected end of string"))?;
        let mut output = String::new();
        while let Some(byte) = self.next() {
            if byte == quote {
                return Ok(output);
            }
            if byte == b'\\' {
                let escaped = self
                    .next()
                    .ok_or_else(|| config_error("unterminated escape sequence"))?;
                match escaped {
                    b'"' => output.push('"'),
                    b'\'' => output.push('\''),
                    b'\\' => output.push('\\'),
                    b'/' => output.push('/'),
                    b'b' => output.push('\u{0008}'),
                    b'f' => output.push('\u{000c}'),
                    b'n' => output.push('\n'),
                    b'r' => output.push('\r'),
                    b't' => output.push('\t'),
                    _ => {
                        return Err(config_error(format!(
                            "unsupported escape sequence: \\{}",
                            char::from(escaped)
                        )));
                    }
                }
            } else {
                output.push(char::from(byte));
            }
        }
        Err(config_error("unterminated string"))
    }

    fn parse_identifier(&mut self) -> crate::Result<String> {
        let start = self.offset;
        while self.peek().is_some_and(is_identifier_continue) {
            self.offset += 1;
        }
        let bytes = self
            .input
            .get(start..self.offset)
            .ok_or_else(|| config_error("invalid identifier range"))?;
        String::from_utf8(bytes.to_vec())
            .map_err(|error| config_error(format!("invalid identifier: {error}")))
    }

    fn parse_number(&mut self) -> crate::Result<f64> {
        let start = self.offset;
        let _negative = self.consume_if(b'-');
        self.skip_digits();
        if self.consume_if(b'.') {
            self.skip_digits();
        }
        if self.consume_if(b'e') || self.consume_if(b'E') {
            if !self.consume_if(b'+') {
                let _minus = self.consume_if(b'-');
            }
            self.skip_digits();
        }
        let bytes = self
            .input
            .get(start..self.offset)
            .ok_or_else(|| config_error("invalid number range"))?;
        let text = std::str::from_utf8(bytes)
            .map_err(|error| config_error(format!("invalid number text: {error}")))?;
        text.parse::<f64>()
            .map_err(|error| config_error(format!("invalid number: {error}")))
    }

    fn finish(&mut self) -> crate::Result<()> {
        self.skip_ws();
        if self.offset == self.input.len() {
            Ok(())
        } else {
            Err(config_error(format!(
                "trailing config input at byte {}",
                self.offset
            )))
        }
    }

    fn skip_digits(&mut self) {
        while self.peek().is_some_and(|byte| byte.is_ascii_digit()) {
            self.offset += 1;
        }
    }

    fn skip_ws(&mut self) {
        while self.peek().is_some_and(|byte| byte.is_ascii_whitespace()) {
            self.offset += 1;
        }
    }

    fn expect_keyword(&mut self, keyword: &str) -> crate::Result<()> {
        let end = self
            .offset
            .checked_add(keyword.len())
            .ok_or_else(|| config_error("keyword offset overflow"))?;
        if self.input.get(self.offset..end) == Some(keyword.as_bytes()) {
            self.offset = end;
            Ok(())
        } else {
            Err(config_error(format!(
                "expected keyword {keyword} at byte {}",
                self.offset
            )))
        }
    }

    fn expect_byte(&mut self, expected: u8) -> crate::Result<()> {
        match self.next() {
            Some(actual) if actual == expected => Ok(()),
            Some(actual) => Err(config_error(format!(
                "expected '{}' at byte {}, found '{}'",
                char::from(expected),
                self.offset.saturating_sub(1),
                char::from(actual)
            ))),
            None => Err(config_error(format!(
                "expected '{}' at end of config",
                char::from(expected)
            ))),
        }
    }

    fn consume_if(&mut self, expected: u8) -> bool {
        if self.peek() == Some(expected) {
            self.offset += 1;
            true
        } else {
            false
        }
    }

    fn peek(&self) -> Option<u8> {
        self.input.get(self.offset).copied()
    }

    fn next(&mut self) -> Option<u8> {
        let byte = self.peek()?;
        self.offset += 1;
        Some(byte)
    }
}

fn strip_comments(source: &str) -> String {
    let mut output = String::with_capacity(source.len());
    let mut chars = source.chars().peekable();
    let mut quote = None;
    while let Some(character) = chars.next() {
        if let Some(active_quote) = quote {
            output.push(character);
            if character == '\\' {
                if let Some(escaped) = chars.next() {
                    output.push(escaped);
                }
            } else if character == active_quote {
                quote = None;
            }
            continue;
        }

        if character == '"' || character == '\'' {
            quote = Some(character);
            output.push(character);
            continue;
        }

        if character == '/' && chars.peek() == Some(&'/') {
            let _slash = chars.next();
            for next in chars.by_ref() {
                if next == '\n' {
                    output.push('\n');
                    break;
                }
            }
        } else if character == '/' && chars.peek() == Some(&'*') {
            let _star = chars.next();
            let mut previous = '\0';
            for next in chars.by_ref() {
                if previous == '*' && next == '/' {
                    break;
                }
                previous = next;
            }
        } else {
            output.push(character);
        }
    }
    output
}

fn optional_string(
    object: &BTreeMap<String, JsConfigValue>,
    key: &str,
) -> crate::Result<Option<String>> {
    match object.get(key) {
        Some(JsConfigValue::String(value)) => Ok(Some(value.clone())),
        Some(JsConfigValue::Null) | None => Ok(None),
        Some(_) => Err(config_error(format!("{key} must be a string"))),
    }
}

fn optional_path(
    object: &BTreeMap<String, JsConfigValue>,
    key: &str,
) -> crate::Result<Option<PathBuf>> {
    Ok(optional_string(object, key)?.map(PathBuf::from))
}

fn optional_bool(
    object: &BTreeMap<String, JsConfigValue>,
    key: &str,
) -> crate::Result<Option<bool>> {
    match object.get(key) {
        Some(JsConfigValue::Bool(value)) => Ok(Some(*value)),
        Some(JsConfigValue::Null) | None => Ok(None),
        Some(_) => Err(config_error(format!("{key} must be a boolean"))),
    }
}

fn optional_f64(object: &BTreeMap<String, JsConfigValue>, key: &str) -> crate::Result<Option<f64>> {
    match object.get(key) {
        Some(JsConfigValue::Number(value)) => Ok(Some(*value)),
        Some(JsConfigValue::Null) | None => Ok(None),
        Some(_) => Err(config_error(format!("{key} must be a number"))),
    }
}

fn optional_u64(object: &BTreeMap<String, JsConfigValue>, key: &str) -> crate::Result<Option<u64>> {
    optional_f64(object, key)?.map(checked_u64).transpose()
}

fn optional_u32(object: &BTreeMap<String, JsConfigValue>, key: &str) -> crate::Result<Option<u32>> {
    optional_u64(object, key)?
        .map(|value| u32::try_from(value).map_err(|error| config_error(format!("{key}: {error}"))))
        .transpose()
}

fn optional_duration_or_false(
    object: &BTreeMap<String, JsConfigValue>,
    key: &str,
) -> crate::Result<Option<u64>> {
    match object.get(key) {
        Some(JsConfigValue::Bool(false)) | Some(JsConfigValue::Null) | None => Ok(None),
        Some(JsConfigValue::Number(value)) => checked_u64(*value).map(Some),
        Some(JsConfigValue::String(value)) => parse_duration_millis(value).map(Some),
        Some(_) => Err(config_error(format!(
            "{key} must be a duration string, number, false, or null",
        ))),
    }
}

fn optional_ratio_or_false(
    object: &BTreeMap<String, JsConfigValue>,
    key: &str,
) -> crate::Result<Option<f64>> {
    match object.get(key) {
        Some(JsConfigValue::Bool(false)) | Some(JsConfigValue::Null) | None => Ok(None),
        Some(JsConfigValue::Bool(true)) => Ok(Some(1.0)),
        Some(JsConfigValue::Number(value)) => Ok(Some(*value)),
        Some(_) => Err(config_error(format!(
            "{key} must be a number, false, or null"
        ))),
    }
}

fn string_array(object: &BTreeMap<String, JsConfigValue>, key: &str) -> crate::Result<Vec<String>> {
    match object.get(key) {
        Some(JsConfigValue::Array(items)) => items
            .iter()
            .map(|item| match item {
                JsConfigValue::String(value) => Ok(value.clone()),
                _ => Err(config_error(format!("{key} entries must be strings"))),
            })
            .collect(),
        Some(JsConfigValue::Null) | None => Ok(Vec::new()),
        Some(_) => Err(config_error(format!("{key} must be an array"))),
    }
}

fn path_array(object: &BTreeMap<String, JsConfigValue>, key: &str) -> crate::Result<Vec<PathBuf>> {
    Ok(string_array(object, key)?
        .into_iter()
        .map(PathBuf::from)
        .collect())
}

fn checked_u64(value: f64) -> crate::Result<u64> {
    if value.is_finite() && value >= 0.0 && value.fract() == 0.0 {
        value
            .to_string()
            .parse::<u64>()
            .map_err(|error| config_error(format!("number must fit in u64: {error}")))
    } else {
        Err(config_error("number must be a nonnegative integer"))
    }
}

fn is_identifier_start(byte: u8) -> bool {
    byte == b'_' || byte == b'$' || byte.is_ascii_alphabetic()
}

fn is_identifier_continue(byte: u8) -> bool {
    is_identifier_start(byte) || byte.is_ascii_digit()
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

fn config_error(message: impl Into<Cow<'static, str>>) -> SporosError {
    SporosError::configuration(message)
}

#[cfg(test)]
mod tests {
    use super::{
        Action, DeprecatedConfig, LinkType, MatchMode, RawConfig, RuntimeConfig,
        TorrentClientConfig, parse_duration_millis, raw_config_from_source,
    };
    use std::path::Path;

    #[test]
    fn normalizes_defaults_and_deprecated_names() {
        let raw = RawConfig {
            match_mode: Some("safe".to_owned()),
            link_type: Some("reflinkOrCopy".to_owned()),
            deprecated: DeprecatedConfig {
                link_dir: Some("/links".into()),
                notification_webhook_url: Some("https://notify.example".to_owned()),
                qbittorrent_url: Some("http://localhost:8080".to_owned()),
                ..DeprecatedConfig::default()
            },
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
        assert_eq!(config.torrent_clients[0].kind, "qbittorrent");
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
            torrent_clients: vec!["qbittorrent:http://localhost:8080".to_owned()],
            ..RawConfig::default()
        };

        let error = RuntimeConfig::normalize(raw, Path::new("/config")).expect_err("invalid");

        assert!(error.to_string().contains("torrentDir cannot be used"));
    }

    #[test]
    fn validates_inject_requires_writable_client() {
        let raw = RawConfig {
            action: Some("inject".to_owned()),
            torrent_clients: vec!["qbittorrent:readonly:http://localhost:8080".to_owned()],
            ..RawConfig::default()
        };

        let error = RuntimeConfig::normalize(raw, Path::new("/config")).expect_err("invalid");

        assert!(error.to_string().contains("non-readonly client"));
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
            MatchMode::parse("risky").expect("mode"),
            MatchMode::Flexible
        );
        assert_eq!(Action::parse("inject").expect("action"), Action::Inject);
    }

    #[test]
    fn loads_export_default_config_object() {
        let raw = raw_config_from_source(
            r#"
            // existing configs are ESM object literals
            export default {
              torznab: ["https://indexer.example/api"],
              useClientTorrents: true,
              dataDirs: ["/data"],
              matchMode: "risky",
              excludeOlder: "2 days",
              excludeRecentSearch: false,
              torrentClients: [
                "qbittorrent:http://localhost:8080",
              ],
              linkDir: "/links",
            };
            "#,
        )
        .expect("config parses");

        assert_eq!(raw.torznab, vec!["https://indexer.example/api"]);
        assert_eq!(raw.use_client_torrents, Some(true));
        assert_eq!(raw.data_dirs, vec![Path::new("/data")]);
        assert_eq!(raw.match_mode.as_deref(), Some("risky"));
        assert_eq!(raw.exclude_older, Some(172_800_000));
        assert_eq!(raw.exclude_recent_search, None);
        assert_eq!(
            raw.torrent_clients,
            vec!["qbittorrent:http://localhost:8080"]
        );
        assert_eq!(
            raw.deprecated.link_dir.as_deref(),
            Some(Path::new("/links"))
        );
    }
}
