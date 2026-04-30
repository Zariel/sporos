//! Torrent-client adapter boundary and client mutation operations.

use std::{borrow::Cow, collections::BTreeMap, path::PathBuf, time::Duration};

use reqwest::blocking::multipart;
use url::Url;

use crate::{
    SporosError,
    config::TorrentClientConfig,
    domain::{
        ClientLabel, ClientTorrentMetadata, Decision, File, InfoHash, InjectionResult, Metafile,
        Searchee, TorrentClientKind, TorrentClientMetadata,
    },
    search::parsed_name_and_media,
};

/// Normalized torrent-client adapter identity.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ClientIdentity {
    /// Base URL from configuration.
    pub url: String,
    /// Metadata shared with searchees and action selection.
    pub metadata: TorrentClientMetadata<'static>,
}

/// Torrent row returned by a client inventory call.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ClientTorrent<'a> {
    /// Torrent info hash.
    pub info_hash: InfoHash<'a>,
    /// Client display name.
    pub name: Cow<'a, str>,
    /// Torrent file tree.
    pub files: Vec<File<'a>>,
    /// Client save path.
    pub save_path: Cow<'a, str>,
    /// Optional category or label.
    pub category: Option<ClientLabel<'a>>,
    /// Optional tags or labels.
    pub tags: Vec<ClientLabel<'a>>,
    /// Sanitized tracker hosts.
    pub trackers: Vec<Cow<'a, str>>,
    /// Whether the torrent is complete.
    pub complete: bool,
    /// Whether the client is currently hash-checking it.
    pub checking: bool,
}

impl<'a> ClientTorrent<'a> {
    /// Convert any borrowed storage into owned storage.
    pub fn into_owned(self) -> ClientTorrent<'static> {
        ClientTorrent {
            info_hash: self.info_hash.into_owned(),
            name: Cow::Owned(self.name.into_owned()),
            files: self.files.into_iter().map(File::into_owned).collect(),
            save_path: Cow::Owned(self.save_path.into_owned()),
            category: self.category.map(ClientLabel::into_owned),
            tags: self.tags.into_iter().map(ClientLabel::into_owned).collect(),
            trackers: self
                .trackers
                .into_iter()
                .map(|tracker| Cow::Owned(tracker.into_owned()))
                .collect(),
            complete: self.complete,
            checking: self.checking,
        }
    }
}

/// Result from mapping a client inventory to searchees.
#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct ClientSearcheeResult {
    /// Searchable torrent-client searchees.
    pub searchees: Vec<Searchee<'static>>,
    /// Torrents skipped because their metadata could not form a valid searchee.
    pub skipped: usize,
}

/// Download-dir lookup options.
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
pub struct DownloadDirOptions {
    /// Require a complete source torrent.
    pub only_completed: bool,
}

/// Injection request options shared by all adapters.
#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct InjectionOptions {
    /// Destination directory passed to the client when linking or data-dir injection chooses one.
    pub destination_dir: Option<PathBuf>,
    /// Category or label to assign.
    pub category: Option<ClientLabel<'static>>,
    /// Tags or labels to assign.
    pub tags: Vec<ClientLabel<'static>>,
    /// Add paused/stopped before recheck.
    pub paused: bool,
    /// Skip client-side hash checking where the adapter supports it.
    pub skip_checking: bool,
}

/// Resume loop behavior shared by adapters.
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
pub struct ResumeOptions {
    /// Perform one check/resume pass instead of the full background loop.
    pub check_once: bool,
}

/// Torrent bytes ready to inject.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NewTorrent<'a> {
    /// Parsed metadata for matching and policy decisions.
    pub metafile: Metafile<'a>,
    /// Original `.torrent` bytes.
    pub bytes: Cow<'a, [u8]>,
}

/// Error codes used by shared client-selection logic.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ClientErrorCode {
    /// Torrent is not present in the client.
    NotFound,
    /// The selected client is readonly.
    Readonly,
    /// The client cannot safely resolve a complete source.
    TorrentNotComplete,
    /// Adapter or configuration does not support the requested operation.
    Unsupported,
}

/// Common synchronous torrent-client adapter contract.
pub trait TorrentClient {
    /// Static adapter identity.
    fn metadata(&self) -> &TorrentClientMetadata<'_>;

    /// Whether a torrent exists in the client.
    fn is_torrent_in_client(&self, info_hash: &InfoHash<'_>) -> crate::Result<bool>;

    /// Whether a torrent is complete.
    fn is_torrent_complete(&self, info_hash: &InfoHash<'_>) -> crate::Result<bool>;

    /// Whether a torrent is hash-checking.
    fn is_torrent_checking(&self, info_hash: &InfoHash<'_>) -> crate::Result<bool>;

    /// Return the complete client inventory.
    fn get_all_torrents(&self) -> crate::Result<Vec<ClientTorrent<'static>>>;

    /// Map client inventory to searchable searchees.
    fn get_client_searchees(&self) -> crate::Result<ClientSearcheeResult> {
        let mut result = ClientSearcheeResult::default();
        for torrent in self.get_all_torrents()? {
            match client_torrent_to_searchee(self.metadata(), torrent) {
                Some(searchee) => result.searchees.push(searchee),
                None => result.skipped += 1,
            }
        }
        Ok(result)
    }

    /// Resolve the download directory for a torrent.
    fn get_download_dir(
        &self,
        metafile: &Metafile<'_>,
        options: DownloadDirOptions,
    ) -> crate::Result<Result<PathBuf, ClientErrorCode>>;

    /// Return known download directories keyed by info hash.
    fn get_all_download_dirs(&self) -> crate::Result<BTreeMap<String, PathBuf>>;

    /// Add a candidate torrent to the client.
    fn inject(
        &self,
        new_torrent: &NewTorrent<'_>,
        searchee: &Searchee<'_>,
        decision: Decision,
        options: &InjectionOptions,
    ) -> crate::Result<InjectionResult>;

    /// Trigger a hash check.
    fn recheck_torrent(&self, info_hash: &InfoHash<'_>) -> crate::Result<()>;

    /// Resume or start after injection/recheck policy allows it.
    fn resume_injection(
        &self,
        metafile: &Metafile<'_>,
        decision: Decision,
        options: ResumeOptions,
    ) -> crate::Result<()>;

    /// Validate adapter-specific configuration.
    fn validate_config(&self) -> crate::Result<()>;
}

/// qBittorrent Web API adapter.
pub struct QbittorrentClient {
    identity: ClientIdentity,
    base_url: String,
    username: String,
    password: String,
    client: reqwest::blocking::Client,
}

impl QbittorrentClient {
    /// Build a qBittorrent adapter from normalized identity metadata.
    pub fn new(identity: ClientIdentity, timeout: Option<Duration>) -> crate::Result<Self> {
        let mut url = Url::parse(&identity.url)
            .map_err(|error| client_error(format!("invalid qBittorrent URL: {error}")))?;
        let username = url.username().to_owned();
        let password = url.password().unwrap_or_default().to_owned();
        url.set_username("")
            .map_err(|()| client_error("failed to sanitize qBittorrent username"))?;
        url.set_password(None)
            .map_err(|()| client_error("failed to sanitize qBittorrent password"))?;
        let mut builder = reqwest::blocking::Client::builder()
            .cookie_store(true)
            .user_agent(format!("CrossSeed/{}", crate::VERSION));
        if let Some(timeout) = timeout {
            builder = builder.timeout(timeout);
        }
        let client = builder.build().map_err(|error| {
            client_error(format!("failed to build qBittorrent client: {error}"))
        })?;
        Ok(Self {
            identity,
            base_url: url.to_string().trim_end_matches('/').to_owned(),
            username,
            password,
            client,
        })
    }

    fn login(&self) -> crate::Result<()> {
        let response = self
            .client
            .post(self.api_url("/api/v2/auth/login"))
            .form(&[
                ("username", self.username.as_str()),
                ("password", self.password.as_str()),
            ])
            .send()
            .map_err(|error| client_error(format!("qBittorrent login failed: {error}")))?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(client_error(format!(
                "qBittorrent login returned {}",
                response.status()
            )))
        }
    }

    fn api_url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    fn get_text(&self, path: &str) -> crate::Result<String> {
        self.login()?;
        self.client
            .get(self.api_url(path))
            .send()
            .and_then(reqwest::blocking::Response::error_for_status)
            .map_err(|error| client_error(format!("qBittorrent request failed: {error}")))?
            .text()
            .map_err(|error| client_error(format!("failed to read qBittorrent response: {error}")))
    }

    fn post_form(&self, path: &str, form: &[(&str, &str)]) -> crate::Result<()> {
        self.login()?;
        self.client
            .post(self.api_url(path))
            .form(form)
            .send()
            .and_then(reqwest::blocking::Response::error_for_status)
            .map_err(|error| client_error(format!("qBittorrent mutation failed: {error}")))?;
        Ok(())
    }

    fn torrent_info(&self, info_hash: &InfoHash<'_>) -> crate::Result<Option<QbTorrentInfo>> {
        self.login()?;
        let response = self
            .client
            .get(self.api_url("/api/v2/torrents/info"))
            .query(&[("hashes", info_hash.as_str())])
            .send()
            .and_then(reqwest::blocking::Response::error_for_status)
            .map_err(|error| client_error(format!("qBittorrent info request failed: {error}")))?
            .text()
            .map_err(|error| client_error(format!("failed to read qBittorrent info: {error}")))?;
        let mut torrents = parse_qb_torrents(&response)?;
        Ok(torrents.pop())
    }

    fn torrent_files(&self, info_hash: &str) -> crate::Result<Vec<File<'static>>> {
        self.login()?;
        let response = self
            .client
            .get(self.api_url("/api/v2/torrents/files"))
            .query(&[("hash", info_hash)])
            .send()
            .and_then(reqwest::blocking::Response::error_for_status)
            .map_err(|error| client_error(format!("qBittorrent files request failed: {error}")))?
            .text()
            .map_err(|error| client_error(format!("failed to read qBittorrent files: {error}")))?;
        parse_qb_files(&response)
    }

    fn torrent_trackers(&self, info_hash: &str) -> crate::Result<Vec<Cow<'static, str>>> {
        self.login()?;
        let response = self
            .client
            .get(self.api_url("/api/v2/torrents/trackers"))
            .query(&[("hash", info_hash)])
            .send()
            .and_then(reqwest::blocking::Response::error_for_status)
            .map_err(|error| client_error(format!("qBittorrent trackers request failed: {error}")))?
            .text()
            .map_err(|error| {
                client_error(format!("failed to read qBittorrent trackers: {error}"))
            })?;
        parse_qb_trackers(&response)
    }

    fn post_hash_action(
        &self,
        primary: &str,
        fallback: &str,
        info_hash: &InfoHash<'_>,
    ) -> crate::Result<()> {
        self.login()?;
        let form = [("hashes", info_hash.as_str())];
        let primary = self.client.post(self.api_url(primary)).form(&form).send();
        match primary {
            Ok(response) if response.status().is_success() => Ok(()),
            _ => self.post_form(fallback, &form),
        }
    }
}

impl TorrentClient for QbittorrentClient {
    fn metadata(&self) -> &TorrentClientMetadata<'_> {
        &self.identity.metadata
    }

    fn is_torrent_in_client(&self, info_hash: &InfoHash<'_>) -> crate::Result<bool> {
        Ok(self.torrent_info(info_hash)?.is_some())
    }

    fn is_torrent_complete(&self, info_hash: &InfoHash<'_>) -> crate::Result<bool> {
        Ok(self
            .torrent_info(info_hash)?
            .is_some_and(|torrent| torrent.complete()))
    }

    fn is_torrent_checking(&self, info_hash: &InfoHash<'_>) -> crate::Result<bool> {
        Ok(self
            .torrent_info(info_hash)?
            .is_some_and(|torrent| torrent.checking()))
    }

    fn get_all_torrents(&self) -> crate::Result<Vec<ClientTorrent<'static>>> {
        let body = self.get_text("/api/v2/torrents/info")?;
        let mut output = Vec::new();
        for torrent in parse_qb_torrents(&body)? {
            let files = self.torrent_files(&torrent.hash)?;
            let trackers = self.torrent_trackers(&torrent.hash)?;
            let Some(info_hash) = InfoHash::new(torrent.hash.clone()) else {
                continue;
            };
            let complete = torrent.complete();
            let checking = torrent.checking();
            output.push(ClientTorrent {
                info_hash: info_hash.into_owned(),
                name: Cow::Owned(torrent.name),
                files,
                save_path: Cow::Owned(torrent.save_path),
                category: torrent.category.map(ClientLabel::new),
                tags: split_qb_tags(torrent.tags),
                trackers,
                complete,
                checking,
            });
        }
        Ok(output)
    }

    fn get_download_dir(
        &self,
        metafile: &Metafile<'_>,
        options: DownloadDirOptions,
    ) -> crate::Result<Result<PathBuf, ClientErrorCode>> {
        let Some(torrent) = self.torrent_info(&metafile.info_hash)? else {
            return Ok(Err(ClientErrorCode::NotFound));
        };
        if options.only_completed && !torrent.complete() {
            return Ok(Err(ClientErrorCode::TorrentNotComplete));
        }
        Ok(Ok(PathBuf::from(torrent.save_path)))
    }

    fn get_all_download_dirs(&self) -> crate::Result<BTreeMap<String, PathBuf>> {
        let body = self.get_text("/api/v2/torrents/info")?;
        Ok(parse_qb_torrents(&body)?
            .into_iter()
            .map(|torrent| (torrent.hash, PathBuf::from(torrent.save_path)))
            .collect())
    }

    fn inject(
        &self,
        new_torrent: &NewTorrent<'_>,
        _searchee: &Searchee<'_>,
        _decision: Decision,
        options: &InjectionOptions,
    ) -> crate::Result<InjectionResult> {
        ensure_writable(self)?;
        self.login()?;
        let mut form = multipart::Form::new().part(
            "torrents",
            multipart::Part::bytes(new_torrent.bytes.clone().into_owned())
                .file_name(format!("{}.torrent", new_torrent.metafile.info_hash)),
        );
        if let Some(destination) = &options.destination_dir {
            form = form.text("savepath", destination.display().to_string());
        }
        if let Some(category) = &options.category {
            form = form.text("category", category.as_str().to_owned());
        }
        if !options.tags.is_empty() {
            form = form.text(
                "tags",
                options
                    .tags
                    .iter()
                    .map(ClientLabel::as_str)
                    .collect::<Vec<_>>()
                    .join(","),
            );
        }
        form = form
            .text("paused", options.paused.to_string())
            .text("skip_checking", options.skip_checking.to_string())
            .text("contentLayout", "Original");
        self.client
            .post(self.api_url("/api/v2/torrents/add"))
            .multipart(form)
            .send()
            .and_then(reqwest::blocking::Response::error_for_status)
            .map_err(|error| client_error(format!("qBittorrent add failed: {error}")))?;
        Ok(InjectionResult::Injected)
    }

    fn recheck_torrent(&self, info_hash: &InfoHash<'_>) -> crate::Result<()> {
        self.post_hash_action(
            "/api/v2/torrents/recheck",
            "/api/v2/torrents/recheck",
            info_hash,
        )
    }

    fn resume_injection(
        &self,
        metafile: &Metafile<'_>,
        _decision: Decision,
        _options: ResumeOptions,
    ) -> crate::Result<()> {
        self.post_hash_action(
            "/api/v2/torrents/start",
            "/api/v2/torrents/resume",
            &metafile.info_hash,
        )
    }

    fn validate_config(&self) -> crate::Result<()> {
        self.login()?;
        let version = self.get_text("/api/v2/app/version")?;
        if !qb_version_at_least(&version, 4, 3, 1) {
            return Err(client_error(format!(
                "qBittorrent version {version} is below 4.3.1"
            )));
        }
        let _preferences = self.get_text("/api/v2/app/preferences")?;
        Ok(())
    }
}

/// Build client identities from config order and URL host/path rules.
pub fn client_identities(configs: &[TorrentClientConfig]) -> crate::Result<Vec<ClientIdentity>> {
    let mut host_counts = BTreeMap::<String, usize>::new();
    let parsed = configs
        .iter()
        .map(|config| {
            let url = Url::parse(&config.url).map_err(|error| {
                client_error(format!(
                    "invalid torrent client URL {:?}: {error}",
                    config.url
                ))
            })?;
            let host = url
                .host_str()
                .ok_or_else(|| client_error("torrent client URL must include a host"))?
                .to_owned();
            *host_counts.entry(host.clone()).or_default() += 1;
            Ok((config, url, host))
        })
        .collect::<crate::Result<Vec<_>>>()?;

    parsed
        .into_iter()
        .enumerate()
        .map(|(priority, (config, url, host))| {
            let client_host = if host_counts.get(&host).copied().unwrap_or_default() > 1 {
                format!("{}{}", host, normalized_client_path(url.path()))
            } else {
                host
            };
            Ok(ClientIdentity {
                url: config.url.clone(),
                metadata: TorrentClientMetadata::new(
                    client_host,
                    priority as u16,
                    parse_client_kind(&config.kind)?,
                    config.readonly,
                    config.kind.clone(),
                ),
            })
        })
        .collect()
}

/// Select the writable client that should receive an injection.
pub fn select_injection_client<'a>(
    clients: &'a [&'a dyn TorrentClient],
    searchee: &Searchee<'_>,
) -> crate::Result<Option<&'a dyn TorrentClient>> {
    if clients.len() == 1 {
        let client = clients.first().copied();
        return client
            .map(|client| ensure_writable(client).map(|()| Some(client)))
            .unwrap_or(Ok(None));
    }

    if let Some(host) = searchee.client.as_ref().map(|client| client.host.as_ref()) {
        if let Some(client) = clients
            .iter()
            .copied()
            .find(|client| client.metadata().host.as_ref() == host)
        {
            return ensure_writable(client).map(|()| Some(client));
        }
    }

    clients
        .iter()
        .copied()
        .filter(|client| !client.metadata().readonly)
        .min_by_key(|client| client.metadata().priority)
        .map_or(Ok(None), |client| Ok(Some(client)))
}

/// Convert one client inventory item into a searchable searchee.
pub fn client_torrent_to_searchee(
    metadata: &TorrentClientMetadata<'_>,
    torrent: ClientTorrent<'_>,
) -> Option<Searchee<'static>> {
    let (title, media_type) = parsed_name_and_media(&torrent.name, &torrent.files, None);
    let title = title.into_owned();
    let mut searchee = Searchee::from_files(
        torrent.name.into_owned(),
        title,
        torrent
            .files
            .into_iter()
            .map(File::into_owned)
            .collect::<Vec<_>>(),
    );
    searchee.info_hash = Some(torrent.info_hash.into_owned());
    searchee.media_type = media_type;
    searchee.client = Some(ClientTorrentMetadata::new(
        metadata.host.as_ref().to_owned(),
        torrent.save_path.into_owned(),
        torrent.category.map(ClientLabel::into_owned),
        torrent
            .tags
            .into_iter()
            .map(ClientLabel::into_owned)
            .collect(),
        torrent
            .trackers
            .into_iter()
            .map(|tracker| Cow::Owned(tracker.into_owned()))
            .collect(),
    ));
    Some(searchee.into_owned())
}

/// Check whether an adapter can be used as an injection target.
pub fn ensure_writable(client: &dyn TorrentClient) -> crate::Result<()> {
    if client.metadata().readonly {
        Err(client_error(format!(
            "torrent client {} is readonly",
            client.metadata().host
        )))
    } else {
        Ok(())
    }
}

#[derive(Debug, serde::Deserialize)]
struct QbTorrentInfo {
    hash: String,
    name: String,
    #[serde(rename = "save_path", default)]
    save_path: String,
    #[serde(default)]
    category: Option<String>,
    #[serde(default)]
    tags: Option<String>,
    #[serde(default)]
    progress: f64,
    #[serde(default)]
    state: String,
}

impl QbTorrentInfo {
    fn complete(&self) -> bool {
        self.progress >= 1.0
            || matches!(
                self.state.as_str(),
                "uploading" | "stalledUP" | "forcedUP" | "queuedUP"
            )
    }

    fn checking(&self) -> bool {
        self.state.to_ascii_lowercase().contains("check")
    }
}

#[derive(Debug, serde::Deserialize)]
struct QbFileInfo {
    name: String,
    size: u64,
}

#[derive(Debug, serde::Deserialize)]
struct QbTrackerInfo {
    url: String,
}

fn parse_qb_torrents(body: &str) -> crate::Result<Vec<QbTorrentInfo>> {
    serde_json::from_str(body)
        .map_err(|error| client_error(format!("failed to parse qBittorrent torrents: {error}")))
}

fn parse_qb_files(body: &str) -> crate::Result<Vec<File<'static>>> {
    let files = serde_json::from_str::<Vec<QbFileInfo>>(body)
        .map_err(|error| client_error(format!("failed to parse qBittorrent files: {error}")))?;
    Ok(files
        .into_iter()
        .map(|file| File::new(file.name, file.size))
        .collect())
}

fn parse_qb_trackers(body: &str) -> crate::Result<Vec<Cow<'static, str>>> {
    let trackers = serde_json::from_str::<Vec<QbTrackerInfo>>(body)
        .map_err(|error| client_error(format!("failed to parse qBittorrent trackers: {error}")))?;
    Ok(trackers
        .into_iter()
        .filter_map(|tracker| tracker_host(&tracker.url))
        .map(Cow::Owned)
        .collect())
}

fn tracker_host(value: &str) -> Option<String> {
    Url::parse(value)
        .ok()
        .and_then(|url| url.host_str().map(str::to_owned))
}

fn split_qb_tags(tags: Option<String>) -> Vec<ClientLabel<'static>> {
    tags.unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|tag| !tag.is_empty())
        .map(|tag| ClientLabel::new(tag.to_owned()))
        .collect()
}

fn qb_version_at_least(version: &str, major: u32, minor: u32, patch: u32) -> bool {
    let version = version.trim_start_matches('v');
    let parts = version
        .split('.')
        .take(3)
        .map(|part| {
            part.chars()
                .take_while(|character| character.is_ascii_digit())
                .collect::<String>()
                .parse::<u32>()
                .unwrap_or(0)
        })
        .collect::<Vec<_>>();
    let current = (
        parts.first().copied().unwrap_or_default(),
        parts.get(1).copied().unwrap_or_default(),
        parts.get(2).copied().unwrap_or_default(),
    );
    current >= (major, minor, patch)
}

fn parse_client_kind(value: &str) -> crate::Result<TorrentClientKind> {
    match value {
        "qbittorrent" => Ok(TorrentClientKind::QBittorrent),
        "rtorrent" => Ok(TorrentClientKind::RTorrent),
        "transmission" => Ok(TorrentClientKind::Transmission),
        "deluge" => Ok(TorrentClientKind::Deluge),
        _ => Err(client_error(format!("unsupported torrent client: {value}"))),
    }
}

fn normalized_client_path(path: &str) -> String {
    let path = path.trim_end_matches('/');
    if path.is_empty() || path == "/" {
        String::new()
    } else if path.starts_with('/') {
        path.to_owned()
    } else {
        format!("/{path}")
    }
}

fn client_error(message: impl Into<Cow<'static, str>>) -> SporosError {
    SporosError::TorrentClient {
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ClientTorrent, DownloadDirOptions, InjectionOptions, NewTorrent, QbittorrentClient,
        ResumeOptions, TorrentClient, client_identities, client_torrent_to_searchee,
        select_injection_client,
    };
    use crate::{
        config::TorrentClientConfig,
        domain::{
            ClientLabel, Decision, File, InfoHash, InjectionResult, MediaType, Metafile, Searchee,
            TorrentClientKind, TorrentClientMetadata,
        },
    };
    use std::{
        borrow::Cow,
        collections::BTreeMap,
        io::{Read, Write},
        net::TcpListener,
        path::PathBuf,
        thread,
        time::Duration,
    };

    #[test]
    fn derives_client_hosts_from_unique_host_or_path() {
        let unique = client_identities(&[
            TorrentClientConfig::parse("qbittorrent:http://qb.example:8080").expect("client"),
            TorrentClientConfig::parse("rtorrent:http://rt.example/RPC2").expect("client"),
        ])
        .expect("identities");

        assert_eq!(unique[0].metadata.host, "qb.example");
        assert_eq!(unique[0].metadata.priority, 0);
        assert_eq!(unique[0].metadata.kind, TorrentClientKind::QBittorrent);
        assert_eq!(unique[1].metadata.host, "rt.example");

        let duplicate = client_identities(&[
            TorrentClientConfig::parse("qbittorrent:http://shared.example/qb").expect("client"),
            TorrentClientConfig::parse("transmission:http://shared.example/transmission")
                .expect("client"),
        ])
        .expect("identities");

        assert_eq!(duplicate[0].metadata.host, "shared.example/qb");
        assert_eq!(duplicate[1].metadata.host, "shared.example/transmission");
    }

    #[test]
    fn maps_client_torrent_to_searchee_metadata() {
        let metadata = TorrentClientMetadata::new(
            "client-a",
            0,
            TorrentClientKind::QBittorrent,
            false,
            "qBittorrent",
        );
        let torrent = ClientTorrent {
            info_hash: InfoHash::from_validated("0123456789abcdef0123456789abcdef01234567"),
            name: Cow::Borrowed("Example.Show.S01E01"),
            files: vec![File::new("Example.Show.S01E01.mkv", 10)],
            save_path: Cow::Borrowed("/downloads"),
            category: Some(ClientLabel::new("tv")),
            tags: vec![ClientLabel::new("tag")],
            trackers: vec![Cow::Borrowed("tracker.example")],
            complete: true,
            checking: false,
        };

        let searchee = client_torrent_to_searchee(&metadata, torrent).expect("searchee");

        assert_eq!(searchee.title, "Example.Show.S01E01");
        assert_eq!(searchee.media_type, MediaType::Episode);
        assert_eq!(
            searchee.client.as_ref().map(|client| client.host.as_ref()),
            Some("client-a")
        );
        assert_eq!(
            searchee
                .client
                .as_ref()
                .and_then(|client| client.category.as_ref())
                .map(ClientLabel::as_str),
            Some("tv")
        );
    }

    #[test]
    fn selects_writable_injection_client_by_rules() {
        let readonly = FakeClient::new("readonly", 0, true);
        let writable = FakeClient::new("writable", 1, false);
        let preferred = FakeClient::new("preferred", 0, false);
        let mut searchee =
            Searchee::from_files("Release", "Release", vec![File::new("file.mkv", 1)]);
        searchee.client = Some(crate::domain::ClientTorrentMetadata::new(
            "preferred",
            "/downloads",
            None,
            Vec::new(),
            Vec::<Cow<'static, str>>::new(),
        ));

        let clients: [&dyn TorrentClient; 3] = [&readonly, &writable, &preferred];
        let selected = select_injection_client(&clients, &searchee)
            .expect("select")
            .expect("client");

        assert_eq!(selected.metadata().host, "preferred");

        let data_source =
            Searchee::from_files("Release", "Release", vec![File::new("file.mkv", 1)]);
        let fallback_clients: [&dyn TorrentClient; 2] = [&readonly, &writable];
        let selected = select_injection_client(&fallback_clients, &data_source)
            .expect("select")
            .expect("client");

        assert_eq!(selected.metadata().host, "writable");
        assert!(select_injection_client(&[&readonly], &data_source).is_err());
    }

    #[test]
    fn qbittorrent_validates_version_and_preferences() {
        let server = http_server(vec![
            http_response("200 OK", "Ok."),
            http_response("200 OK", "Ok."),
            http_response("200 OK", "v4.6.2"),
            http_response("200 OK", "Ok."),
            http_response("200 OK", r#"{"save_path":"/downloads"}"#),
        ]);
        let client = qb_client(&server.url);

        client.validate_config().expect("validate");

        let requests = server.join();
        assert!(
            requests
                .iter()
                .any(|request| request.contains("POST /api/v2/auth/login "))
        );
        assert!(
            requests
                .iter()
                .any(|request| request.contains("GET /api/v2/app/version "))
        );
        assert!(
            requests
                .iter()
                .any(|request| request.contains("GET /api/v2/app/preferences "))
        );
    }

    #[test]
    fn qbittorrent_maps_inventory_files_and_trackers() {
        let hash = "0123456789abcdef0123456789abcdef01234567";
        let server = http_server(vec![
            http_response("200 OK", "Ok."),
            http_response(
                "200 OK",
                &format!(
                    r#"[{{"hash":"{hash}","name":"Example.Show.S01E01","save_path":"/downloads","category":"tv","tags":"tag, cross-seed","progress":1.0,"state":"uploading"}}]"#
                ),
            ),
            http_response("200 OK", "Ok."),
            http_response(
                "200 OK",
                r#"[{"name":"Example.Show.S01E01.mkv","size":123}]"#,
            ),
            http_response("200 OK", "Ok."),
            http_response("200 OK", r#"[{"url":"https://tracker.example/announce"}]"#),
        ]);
        let client = qb_client(&server.url);

        let torrents = client.get_all_torrents().expect("inventory");

        assert_eq!(torrents.len(), 1);
        assert_eq!(torrents[0].info_hash.as_str(), hash);
        assert_eq!(torrents[0].files[0].path, "Example.Show.S01E01.mkv");
        assert_eq!(
            torrents[0].category.as_ref().map(ClientLabel::as_str),
            Some("tv")
        );
        assert_eq!(torrents[0].tags.len(), 2);
        assert_eq!(torrents[0].trackers[0], "tracker.example");
        assert!(torrents[0].complete);
        let requests = server.join();
        assert!(requests.iter().any(|request| {
            request.contains(
                "GET /api/v2/torrents/files?hash=0123456789abcdef0123456789abcdef01234567 ",
            )
        }));
    }

    #[test]
    fn qbittorrent_injects_with_multipart_add_and_starts() {
        let server = http_server(vec![
            http_response("200 OK", "Ok."),
            http_response("200 OK", ""),
            http_response("200 OK", "Ok."),
            http_response("200 OK", ""),
        ]);
        let client = qb_client(&server.url);
        let bytes = torrent_bytes("Inject.Release", 10);
        let metafile = crate::torrent::parse_metafile(&bytes).expect("metafile");
        let new_torrent = NewTorrent {
            metafile,
            bytes: Cow::Owned(bytes),
        };
        let searchee = Searchee::from_files("Inject.Release", "Inject.Release", Vec::new());

        let result = client
            .inject(
                &new_torrent,
                &searchee,
                Decision::Match,
                &InjectionOptions {
                    destination_dir: Some(PathBuf::from("/linked")),
                    category: Some(ClientLabel::new("tv")),
                    tags: vec![ClientLabel::new("cross-seed")],
                    paused: true,
                    skip_checking: true,
                },
            )
            .expect("inject");
        client
            .resume_injection(
                &new_torrent.metafile,
                Decision::Match,
                ResumeOptions::default(),
            )
            .expect("resume");

        assert_eq!(result, InjectionResult::Injected);
        let requests = server.join();
        assert!(
            requests
                .iter()
                .any(|request| request.contains("POST /api/v2/torrents/add "))
        );
        assert!(
            requests
                .iter()
                .any(|request| request.contains("POST /api/v2/torrents/start "))
        );
    }

    struct FakeClient {
        metadata: TorrentClientMetadata<'static>,
    }

    impl FakeClient {
        fn new(host: &str, priority: u16, readonly: bool) -> Self {
            Self {
                metadata: TorrentClientMetadata::new(
                    host.to_owned(),
                    priority,
                    TorrentClientKind::QBittorrent,
                    readonly,
                    "fake",
                ),
            }
        }
    }

    impl TorrentClient for FakeClient {
        fn metadata(&self) -> &TorrentClientMetadata<'_> {
            &self.metadata
        }

        fn is_torrent_in_client(&self, _info_hash: &InfoHash<'_>) -> crate::Result<bool> {
            Ok(false)
        }

        fn is_torrent_complete(&self, _info_hash: &InfoHash<'_>) -> crate::Result<bool> {
            Ok(false)
        }

        fn is_torrent_checking(&self, _info_hash: &InfoHash<'_>) -> crate::Result<bool> {
            Ok(false)
        }

        fn get_all_torrents(&self) -> crate::Result<Vec<ClientTorrent<'static>>> {
            Ok(Vec::new())
        }

        fn get_download_dir(
            &self,
            _metafile: &Metafile<'_>,
            _options: DownloadDirOptions,
        ) -> crate::Result<Result<PathBuf, super::ClientErrorCode>> {
            Ok(Err(super::ClientErrorCode::NotFound))
        }

        fn get_all_download_dirs(&self) -> crate::Result<BTreeMap<String, PathBuf>> {
            Ok(BTreeMap::new())
        }

        fn inject(
            &self,
            _new_torrent: &NewTorrent<'_>,
            _searchee: &Searchee<'_>,
            _decision: Decision,
            _options: &InjectionOptions,
        ) -> crate::Result<InjectionResult> {
            Ok(InjectionResult::Injected)
        }

        fn recheck_torrent(&self, _info_hash: &InfoHash<'_>) -> crate::Result<()> {
            Ok(())
        }

        fn resume_injection(
            &self,
            _metafile: &Metafile<'_>,
            _decision: Decision,
            _options: ResumeOptions,
        ) -> crate::Result<()> {
            Ok(())
        }

        fn validate_config(&self) -> crate::Result<()> {
            Ok(())
        }
    }

    fn qb_client(base_url: &str) -> QbittorrentClient {
        let identity =
            client_identities(&[
                TorrentClientConfig::parse(&format!("qbittorrent:{base_url}")).expect("config"),
            ])
            .expect("identity")
            .into_iter()
            .next()
            .expect("identity");
        QbittorrentClient::new(identity, Some(Duration::from_secs(1))).expect("client")
    }

    struct TestHttpServer {
        url: String,
        handle: thread::JoinHandle<Vec<String>>,
    }

    impl TestHttpServer {
        fn join(self) -> Vec<String> {
            self.handle.join().expect("server joins")
        }
    }

    fn http_server(responses: Vec<String>) -> TestHttpServer {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
        let url = format!("http://{}", listener.local_addr().expect("local addr"));
        let handle = thread::spawn(move || {
            let mut requests = Vec::new();
            for response in responses {
                let (mut stream, _) = listener.accept().expect("accept");
                let mut buf = [0_u8; 8192];
                let read = stream.read(&mut buf).expect("read");
                requests.push(String::from_utf8_lossy(&buf[..read]).into_owned());
                stream.write_all(response.as_bytes()).expect("write");
            }
            requests
        });
        TestHttpServer { url, handle }
    }

    fn http_response(status: &str, body: &str) -> String {
        format!(
            "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
    }

    fn torrent_bytes(name: &str, length: u64) -> Vec<u8> {
        format!(
            "d4:infod6:lengthi{length}e4:name{}:{name}12:piece lengthi1e6:pieces20:aaaaaaaaaaaaaaaaaaaaee",
            name.len()
        )
        .into_bytes()
    }
}
