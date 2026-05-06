//! Torrent-client adapter boundary and client mutation operations.

use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
    future::Future,
    path::{Path, PathBuf},
    time::Duration,
};

use async_trait::async_trait;
use tokio::runtime::Builder;
use url::Url;

mod deluge;
mod qbittorrent;
mod rtorrent;
mod transmission;

pub use deluge::DelugeClient;
pub use qbittorrent::QbittorrentClient;
pub use rtorrent::RtorrentClient;
pub use transmission::TransmissionClient;

use crate::{
    SporosError,
    config::{RuntimeConfig, TorrentClientConfig},
    domain::{
        ClientLabel, ClientTorrentMetadata, Decision, File, InfoHash, InjectionResult, Metafile,
        Searchee, TorrentClientKind, TorrentClientMetadata,
    },
    retry::{RetryClass, classify_reqwest_error},
    search::parsed_name_and_media,
};

pub const CLIENT_INVENTORY_PAGE_SIZE: usize = 1_000;
/// Maximum active qBittorrent file-list requests during cleanup refresh.
pub const QB_TORRENT_FILES_CONCURRENCY_LIMIT: usize = 1;
const INJECTION_CONFIRM_ATTEMPTS: usize = 5;
const INJECTION_CONFIRM_BASE_DELAY: Duration = Duration::from_millis(100);
const INJECTION_CONFIRM_MAX_DELAY: Duration = Duration::from_millis(500);

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
    /// Derive a duplicate category from the source torrent category.
    pub duplicate_categories: bool,
    /// Add paused/stopped before recheck.
    pub paused: bool,
    /// Skip client-side hash checking where the adapter supports it.
    pub skip_checking: bool,
}

/// Resume loop behavior shared by adapters.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct ResumeOptions {
    /// Perform one check/resume pass instead of the full background loop.
    pub check_once: bool,
    /// Maximum bytes still missing before the torrent may be resumed.
    pub max_remaining_bytes: u64,
    /// Allow resume policy to account for non-relevant missing files.
    pub ignore_non_relevant_files: bool,
}

impl Default for ResumeOptions {
    fn default() -> Self {
        Self {
            check_once: false,
            max_remaining_bytes: u64::MAX,
            ignore_non_relevant_files: false,
        }
    }
}

/// Torrent bytes ready to inject.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NewTorrent<'a> {
    /// Parsed metadata for matching and policy decisions.
    pub metafile: Metafile<'a>,
    /// Original `.torrent` bytes.
    pub bytes: Cow<'a, [u8]>,
}

fn duplicate_category_label(
    searchee: &Searchee<'_>,
    options: &InjectionOptions,
) -> Option<ClientLabel<'static>> {
    if !options.duplicate_categories {
        return None;
    }
    let category = searchee
        .client
        .as_ref()
        .and_then(|client| client.category.as_ref())?;
    Some(ClientLabel::new(format!("{}.sporos", category.as_str())))
}

fn qbit_category_and_tags(
    searchee: &Searchee<'_>,
    options: &InjectionOptions,
) -> (Option<ClientLabel<'static>>, Vec<ClientLabel<'static>>) {
    let duplicate = duplicate_category_label(searchee, options);
    let mut tags = options.tags.clone();
    if let (Some(_category), Some(duplicate)) = (&options.category, duplicate.as_ref()) {
        if !tags
            .iter()
            .any(|tag| tag.as_str().eq_ignore_ascii_case(duplicate.as_str()))
        {
            tags.push(duplicate.clone());
        }
    }
    (options.category.clone().or(duplicate), tags)
}

fn primary_client_label(
    searchee: &Searchee<'_>,
    options: &InjectionOptions,
) -> Option<ClientLabel<'static>> {
    options
        .category
        .clone()
        .or_else(|| duplicate_category_label(searchee, options))
        .or_else(|| options.tags.first().cloned())
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

    /// Visit client inventory without requiring callers to retain it.
    fn for_each_torrent(
        &self,
        visitor: &mut dyn FnMut(ClientTorrent<'static>) -> crate::Result<()>,
    ) -> crate::Result<()> {
        let _visitor = visitor;
        Err(client_error(
            "torrent client does not support streaming inventory",
        ))
    }

    /// Return the complete client inventory.
    fn get_all_torrents(&self) -> crate::Result<Vec<ClientTorrent<'static>>> {
        let mut output = Vec::new();
        self.for_each_torrent(&mut |torrent| {
            output.push(torrent);
            Ok(())
        })?;
        Ok(output)
    }

    /// Map client inventory to searchable searchees.
    fn get_client_searchees(&self) -> crate::Result<ClientSearcheeResult> {
        let mut result = ClientSearcheeResult::default();
        let metadata = self.metadata().clone().into_owned();
        self.for_each_torrent(&mut |torrent| {
            match client_torrent_to_searchee(&metadata, torrent) {
                Some(searchee) => result.searchees.push(searchee),
                None => result.skipped += 1,
            }
            Ok(())
        })?;
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

    /// Visit download directories until the predicate finds a compatible target.
    fn has_matching_download_dir(
        &self,
        predicate: &mut dyn FnMut(&Path) -> crate::Result<bool>,
    ) -> crate::Result<bool> {
        for download_dir in self.get_all_download_dirs()?.values() {
            if predicate(download_dir)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Return bytes still missing for one torrent when the adapter can tell.
    fn remaining_bytes(&self, _metafile: &Metafile<'_>) -> crate::Result<Option<u64>> {
        Ok(None)
    }

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

/// Async-shaped torrent-client API for Tokio orchestration boundaries.
#[async_trait(?Send)]
pub trait AsyncTorrentClient {
    /// Check whether one info hash exists in the client.
    async fn is_torrent_in_client_async(&self, info_hash: &InfoHash<'_>) -> crate::Result<bool>;

    /// Check whether one info hash is complete in the client.
    async fn is_torrent_complete_async(&self, info_hash: &InfoHash<'_>) -> crate::Result<bool>;

    /// Check whether one info hash is hash-checking.
    async fn is_torrent_checking_async(&self, info_hash: &InfoHash<'_>) -> crate::Result<bool>;

    /// Visit all torrents with adapter-specific paging where available.
    async fn for_each_torrent_async(
        &self,
        visitor: &mut dyn FnMut(ClientTorrent<'static>) -> crate::Result<()>,
    ) -> crate::Result<()>;

    /// Map client inventory to searchable searchees.
    async fn get_client_searchees_async(&self) -> crate::Result<ClientSearcheeResult>;

    /// Resolve the download directory for a torrent.
    async fn get_download_dir_async(
        &self,
        metafile: &Metafile<'_>,
        options: DownloadDirOptions,
    ) -> crate::Result<Result<PathBuf, ClientErrorCode>>;

    /// Return bytes still missing for one torrent when the adapter can tell.
    async fn remaining_bytes_async(&self, metafile: &Metafile<'_>) -> crate::Result<Option<u64>>;

    /// Add a candidate torrent to the client.
    async fn inject_async(
        &self,
        new_torrent: &NewTorrent<'_>,
        searchee: &Searchee<'_>,
        decision: Decision,
        options: &InjectionOptions,
    ) -> crate::Result<InjectionResult>;

    /// Trigger a hash check.
    async fn recheck_torrent_async(&self, info_hash: &InfoHash<'_>) -> crate::Result<()>;

    /// Resume or start after injection/recheck policy allows it.
    async fn resume_injection_async(
        &self,
        metafile: &Metafile<'_>,
        decision: Decision,
        options: ResumeOptions,
    ) -> crate::Result<()>;

    /// Validate adapter-specific configuration.
    async fn validate_config_async(&self) -> crate::Result<()>;
}

#[async_trait(?Send)]
impl<T> AsyncTorrentClient for T
where
    T: TorrentClient + ?Sized,
{
    async fn is_torrent_in_client_async(&self, info_hash: &InfoHash<'_>) -> crate::Result<bool> {
        self.is_torrent_in_client(info_hash)
    }

    async fn is_torrent_complete_async(&self, info_hash: &InfoHash<'_>) -> crate::Result<bool> {
        self.is_torrent_complete(info_hash)
    }

    async fn is_torrent_checking_async(&self, info_hash: &InfoHash<'_>) -> crate::Result<bool> {
        self.is_torrent_checking(info_hash)
    }

    async fn for_each_torrent_async(
        &self,
        visitor: &mut dyn FnMut(ClientTorrent<'static>) -> crate::Result<()>,
    ) -> crate::Result<()> {
        self.for_each_torrent(visitor)
    }

    async fn get_client_searchees_async(&self) -> crate::Result<ClientSearcheeResult> {
        self.get_client_searchees()
    }

    async fn get_download_dir_async(
        &self,
        metafile: &Metafile<'_>,
        options: DownloadDirOptions,
    ) -> crate::Result<Result<PathBuf, ClientErrorCode>> {
        self.get_download_dir(metafile, options)
    }

    async fn remaining_bytes_async(&self, metafile: &Metafile<'_>) -> crate::Result<Option<u64>> {
        self.remaining_bytes(metafile)
    }

    async fn inject_async(
        &self,
        new_torrent: &NewTorrent<'_>,
        searchee: &Searchee<'_>,
        decision: Decision,
        options: &InjectionOptions,
    ) -> crate::Result<InjectionResult> {
        self.inject(new_torrent, searchee, decision, options)
    }

    async fn recheck_torrent_async(&self, info_hash: &InfoHash<'_>) -> crate::Result<()> {
        self.recheck_torrent(info_hash)
    }

    async fn resume_injection_async(
        &self,
        metafile: &Metafile<'_>,
        decision: Decision,
        options: ResumeOptions,
    ) -> crate::Result<()> {
        self.resume_injection(metafile, decision, options)
    }

    async fn validate_config_async(&self) -> crate::Result<()> {
        self.validate_config()
    }
}

fn resume_with_policy<F>(
    client: &dyn TorrentClient,
    metafile: &Metafile<'_>,
    options: ResumeOptions,
    mut resume: F,
) -> crate::Result<()>
where
    F: FnMut() -> crate::Result<()>,
{
    if options.max_remaining_bytes == u64::MAX && !options.check_once {
        return resume();
    }
    let mut attempts = if options.check_once { 1 } else { 360 };
    while attempts > 0 {
        attempts -= 1;
        if client.is_torrent_checking(&metafile.info_hash)? {
            if attempts > 0 {
                block_on_client_delay(Duration::from_secs(10))?;
            }
            continue;
        }
        if options.max_remaining_bytes == u64::MAX {
            return resume();
        }
        let Some(remaining) = client.remaining_bytes(metafile)? else {
            tracing::warn!(
                info_hash = %metafile.info_hash,
                max_remaining_bytes = options.max_remaining_bytes,
                ignore_non_relevant_files = options.ignore_non_relevant_files,
                "torrent client cannot report remaining bytes for auto-resume policy"
            );
            return Ok(());
        };
        if remaining <= options.max_remaining_bytes {
            return resume();
        }
        tracing::warn!(
            info_hash = %metafile.info_hash,
            remaining_bytes = remaining,
            max_remaining_bytes = options.max_remaining_bytes,
            ignore_non_relevant_files = options.ignore_non_relevant_files,
            "torrent remains above auto-resume threshold"
        );
        return Ok(());
    }
    Ok(())
}

fn confirm_injection(
    client: &dyn TorrentClient,
    info_hash: &InfoHash<'_>,
) -> crate::Result<InjectionResult> {
    let mut delay = INJECTION_CONFIRM_BASE_DELAY;
    for attempt in 0..INJECTION_CONFIRM_ATTEMPTS {
        if client.is_torrent_in_client(info_hash)? {
            return Ok(InjectionResult::Injected);
        }
        if attempt + 1 < INJECTION_CONFIRM_ATTEMPTS {
            block_on_client_delay(delay)?;
            delay = delay.saturating_mul(2).min(INJECTION_CONFIRM_MAX_DELAY);
        }
    }
    tracing::warn!(
        info_hash = %info_hash,
        "torrent client did not confirm injected torrent before timeout"
    );
    Ok(InjectionResult::Failure)
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

    let identities = parsed
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
        .collect::<crate::Result<Vec<_>>>()?;
    let mut seen = BTreeSet::new();
    for identity in &identities {
        if !seen.insert(identity.metadata.host.as_ref().to_owned()) {
            return Err(client_error(format!(
                "duplicate torrent client identity: {}",
                identity.metadata.host
            )));
        }
    }
    Ok(identities)
}

/// Build configured torrent-client adapters.
pub fn build_torrent_clients(
    configs: &[TorrentClientConfig],
    timeout: Option<Duration>,
) -> crate::Result<Vec<Box<dyn TorrentClient>>> {
    build_torrent_clients_with_torrent_dir(configs, timeout, None)
}

/// Build configured torrent-client adapters with qBittorrent torrent_dir context.
pub fn build_torrent_clients_with_torrent_dir(
    configs: &[TorrentClientConfig],
    timeout: Option<Duration>,
    torrent_dir: Option<&Path>,
) -> crate::Result<Vec<Box<dyn TorrentClient>>> {
    let identities = client_identities(configs)?;
    identities
        .into_iter()
        .map(|identity| {
            let client: Box<dyn TorrentClient> = match identity.metadata.kind {
                TorrentClientKind::QBittorrent => Box::new(
                    QbittorrentClient::new(identity, timeout)?
                        .with_torrent_dir(torrent_dir.map(Path::to_path_buf)),
                ),
                TorrentClientKind::RTorrent => Box::new(RtorrentClient::new(identity, timeout)?),
                TorrentClientKind::Transmission => {
                    Box::new(TransmissionClient::new(identity, timeout)?)
                }
                TorrentClientKind::Deluge => Box::new(DelugeClient::new(identity, timeout)?),
            };
            Ok(client)
        })
        .collect()
}

/// Validate configured torrent clients during full-runtime startup.
pub fn validate_configured_torrent_clients(config: &RuntimeConfig) -> crate::Result<()> {
    let clients = build_torrent_clients_with_torrent_dir(
        &config.torrent_clients,
        config.search_timeout.map(Duration::from_millis),
        config.torrent_dir.as_deref(),
    )?;
    for client in clients {
        client.validate_config()?;
    }
    Ok(())
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

fn tracker_host(value: &str) -> Option<String> {
    Url::parse(value)
        .ok()
        .and_then(|url| url.host_str().map(str::to_owned))
}

fn base64_encode(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    fn encode_char(index: u8) -> char {
        TABLE
            .get(usize::from(index))
            .copied()
            .map(char::from)
            .unwrap_or('=')
    }

    let mut output = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let Some(&first) = chunk.first() else {
            continue;
        };
        let second = chunk.get(1).copied().unwrap_or_default();
        let third = chunk.get(2).copied().unwrap_or_default();

        output.push(encode_char(first >> 2));
        output.push(encode_char(((first & 0b0000_0011) << 4) | (second >> 4)));
        if chunk.len() > 1 {
            output.push(encode_char(((second & 0b0000_1111) << 2) | (third >> 6)));
        } else {
            output.push('=');
        }
        if chunk.len() > 2 {
            output.push(encode_char(third & 0b0011_1111));
        } else {
            output.push('=');
        }
    }
    output
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

fn client_error_retryable(error: &reqwest::Error) -> bool {
    matches!(classify_reqwest_error(error), RetryClass::Retryable { .. })
}

fn block_on_client<F, T>(future: F) -> crate::Result<T>
where
    F: Future<Output = T>,
{
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| client_error(format!("failed to build client runtime: {error}")))?;
    Ok(runtime.block_on(future))
}

fn block_on_client_delay(delay: Duration) -> crate::Result<()> {
    block_on_client(async {
        tokio::time::sleep(delay).await;
    })
}

#[cfg(test)]
mod tests;
