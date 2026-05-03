//! Torrent-client adapter boundary and client mutation operations.

use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
    fs,
    future::Future,
    path::{Path, PathBuf},
    sync::Mutex,
    time::Duration,
};

use async_trait::async_trait;
use quick_xml::{Reader, events::Event};
use reqwest::header::CONTENT_TYPE;
use reqwest::multipart;
use tokio::runtime::Builder;
use url::Url;

use crate::{
    SporosError,
    config::{RuntimeConfig, TorrentClientConfig},
    domain::{
        ClientLabel, ClientTorrentMetadata, Decision, File, InfoHash, InjectionResult, Metafile,
        Searchee, TorrentClientKind, TorrentClientMetadata,
    },
    retry::{RetryClass, RetryPolicy, classify_reqwest_error},
    search::parsed_name_and_media,
};

/// qBittorrent inventory page size used by bounded refresh paths.
pub const CLIENT_INVENTORY_PAGE_SIZE: usize = 1_000;
/// Maximum active qBittorrent file-list requests during cleanup refresh.
pub const QB_TORRENT_FILES_CONCURRENCY_LIMIT: usize = 1;

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
    /// Derive a duplicate cross-seed category from the source torrent category.
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
    Some(ClientLabel::new(format!(
        "{}.cross-seed",
        category.as_str()
    )))
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

/// qBittorrent Web API adapter.
pub struct QbittorrentClient {
    identity: ClientIdentity,
    base_url: String,
    username: String,
    password: String,
    torrent_dir: Option<PathBuf>,
    client: reqwest::Client,
    session_valid: Mutex<bool>,
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
        let mut builder = reqwest::Client::builder()
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
            torrent_dir: None,
            client,
            session_valid: Mutex::new(false),
        })
    }

    /// Attach a configured qBittorrent torrent directory for startup validation.
    pub fn with_torrent_dir(mut self, torrent_dir: Option<PathBuf>) -> Self {
        self.torrent_dir = torrent_dir;
        self
    }

    fn login(&self) -> crate::Result<()> {
        if self.session_valid()? {
            return Ok(());
        }
        self.force_login()
    }

    fn session_valid(&self) -> crate::Result<bool> {
        self.session_valid
            .lock()
            .map(|session_valid| *session_valid)
            .map_err(|error| client_error(format!("qBittorrent session lock failed: {error}")))
    }

    fn set_session_valid(&self, valid: bool) -> crate::Result<()> {
        self.session_valid
            .lock()
            .map(|mut session_valid| *session_valid = valid)
            .map_err(|error| client_error(format!("qBittorrent session lock failed: {error}")))
    }

    fn force_login(&self) -> crate::Result<()> {
        let response = block_on_client(async {
            self.client
                .post(self.api_url("/api/v2/auth/login"))
                .form(&[
                    ("username", self.username.as_str()),
                    ("password", self.password.as_str()),
                ])
                .send()
                .await
        })?
        .map_err(|error| client_error(format!("qBittorrent login failed: {error}")))?;
        if response.status().is_success() {
            self.set_session_valid(true)?;
            Ok(())
        } else {
            self.set_session_valid(false)?;
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
        self.request_text("request", || self.client.get(self.api_url(path)))
    }

    fn post_form(&self, path: &str, form: &[(&str, &str)]) -> crate::Result<()> {
        self.request_unit("mutation", || {
            self.client.post(self.api_url(path)).form(form)
        })
    }

    fn torrent_info(&self, info_hash: &InfoHash<'_>) -> crate::Result<Option<QbTorrentInfo>> {
        let response = self.request_text("info", || {
            self.client
                .get(self.api_url("/api/v2/torrents/info"))
                .query(&[("hashes", info_hash.as_str())])
        })?;
        let mut torrents = parse_qb_torrents(&response)?;
        Ok(torrents.pop())
    }

    fn torrent_page(&self, offset: usize, limit: usize) -> crate::Result<Vec<QbTorrentInfo>> {
        let response = self.request_text("info", || {
            self.client
                .get(self.api_url("/api/v2/torrents/info"))
                .query(&[("offset", offset.to_string()), ("limit", limit.to_string())])
        })?;
        parse_qb_torrents(&response)
    }

    fn torrent_files(&self, info_hash: &str) -> crate::Result<Vec<File<'static>>> {
        let response = self.request_text("files", || {
            self.client
                .get(self.api_url("/api/v2/torrents/files"))
                .query(&[("hash", info_hash)])
        })?;
        parse_qb_files(&response)
    }

    fn client_torrent_from_qb(
        &self,
        torrent: QbTorrentInfo,
    ) -> crate::Result<Option<ClientTorrent<'static>>> {
        let Some(info_hash) = InfoHash::new(torrent.hash.clone()) else {
            return Ok(None);
        };
        let complete = torrent.complete();
        let checking = torrent.checking();
        let files = self.torrent_files(&torrent.hash)?;
        let trackers = self.torrent_trackers(&torrent.hash)?;
        Ok(Some(ClientTorrent {
            info_hash: info_hash.into_owned(),
            name: Cow::Owned(torrent.name),
            files,
            save_path: Cow::Owned(torrent.save_path),
            category: torrent.category.map(ClientLabel::new),
            tags: split_qb_tags(torrent.tags),
            trackers,
            complete,
            checking,
        }))
    }

    fn visit_qb_torrent_batch(
        &self,
        batch: &mut Vec<QbTorrentInfo>,
        visitor: &mut dyn FnMut(ClientTorrent<'static>) -> crate::Result<()>,
    ) -> crate::Result<()> {
        for torrent in batch.drain(..) {
            if let Some(torrent) = self.client_torrent_from_qb(torrent)? {
                visitor(torrent)?;
            }
        }
        Ok(())
    }

    fn torrent_trackers(&self, info_hash: &str) -> crate::Result<Vec<Cow<'static, str>>> {
        let response = self.request_text("trackers", || {
            self.client
                .get(self.api_url("/api/v2/torrents/trackers"))
                .query(&[("hash", info_hash)])
        })?;
        parse_qb_trackers(&response)
    }

    fn post_hash_action(
        &self,
        primary: &str,
        fallback: &str,
        info_hash: &InfoHash<'_>,
    ) -> crate::Result<()> {
        let form = [("hashes", info_hash.as_str())];
        let primary = self.request_unit("hash_action", || {
            self.client.post(self.api_url(primary)).form(&form)
        });
        match primary {
            Ok(()) => Ok(()),
            _ => self.post_form(fallback, &form),
        }
    }

    fn request_text<F>(&self, kind: &'static str, build: F) -> crate::Result<String>
    where
        F: Fn() -> reqwest::RequestBuilder,
    {
        self.request_with_retry(
            kind,
            |response| async move { response.error_for_status()?.text().await },
            build,
        )
    }

    fn request_unit<F>(&self, kind: &'static str, build: F) -> crate::Result<()>
    where
        F: Fn() -> reqwest::RequestBuilder,
    {
        self.request_with_retry(
            kind,
            |response| async move { response.error_for_status().map(|_| ()) },
            build,
        )
    }

    fn request_with_retry<T, F, P, Fut>(
        &self,
        kind: &'static str,
        parse: P,
        build: F,
    ) -> crate::Result<T>
    where
        F: Fn() -> reqwest::RequestBuilder,
        P: Fn(reqwest::Response) -> Fut,
        Fut: Future<Output = Result<T, reqwest::Error>>,
    {
        let policy = RetryPolicy::idempotent();
        let mut retried_auth = false;
        for attempt in 1..=policy.max_attempts {
            self.login()?;
            let result = block_on_client(async {
                let response = build().send().await?;
                parse(response).await
            })?;
            match result {
                Ok(value) => return Ok(value),
                Err(error) if is_qb_auth_error(&error) && !retried_auth => {
                    self.set_session_valid(false)?;
                    self.force_login()?;
                    retried_auth = true;
                }
                Err(error) if client_error_retryable(&error) && attempt < policy.max_attempts => {
                    tracing::debug!(
                        client = %self.base_url,
                        kind,
                        attempt,
                        max_attempts = policy.max_attempts,
                        error = %error,
                        "retrying qBittorrent request",
                    );
                    let delay = policy.delay_for_retry(attempt);
                    if !delay.is_zero() {
                        block_on_client_delay(delay)?;
                    }
                }
                Err(error) => {
                    return Err(client_error(format!(
                        "qBittorrent {kind} request failed: {error}"
                    )));
                }
            }
        }
        Err(client_error(format!(
            "qBittorrent {kind} request retry attempts exhausted"
        )))
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
            if let Some(torrent) = self.client_torrent_from_qb(torrent)? {
                output.push(torrent);
            }
        }
        Ok(output)
    }

    fn for_each_torrent(
        &self,
        visitor: &mut dyn FnMut(ClientTorrent<'static>) -> crate::Result<()>,
    ) -> crate::Result<()> {
        let mut offset = 0usize;
        loop {
            let page = self.torrent_page(offset, CLIENT_INVENTORY_PAGE_SIZE)?;
            let page_len = page.len();
            let mut active_batch = Vec::with_capacity(QB_TORRENT_FILES_CONCURRENCY_LIMIT);
            for torrent in page {
                active_batch.push(torrent);
                if active_batch.len() == QB_TORRENT_FILES_CONCURRENCY_LIMIT {
                    self.visit_qb_torrent_batch(&mut active_batch, visitor)?;
                }
            }
            self.visit_qb_torrent_batch(&mut active_batch, visitor)?;
            if page_len < CLIENT_INVENTORY_PAGE_SIZE {
                break;
            }
            offset = offset.saturating_add(CLIENT_INVENTORY_PAGE_SIZE);
        }
        Ok(())
    }

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

    fn has_matching_download_dir(
        &self,
        predicate: &mut dyn FnMut(&Path) -> crate::Result<bool>,
    ) -> crate::Result<bool> {
        let mut offset = 0usize;
        loop {
            let page = self.torrent_page(offset, CLIENT_INVENTORY_PAGE_SIZE)?;
            let page_len = page.len();
            for torrent in page {
                if predicate(Path::new(&torrent.save_path))? {
                    return Ok(true);
                }
            }
            if page_len < CLIENT_INVENTORY_PAGE_SIZE {
                return Ok(false);
            }
            offset = offset.saturating_add(CLIENT_INVENTORY_PAGE_SIZE);
        }
    }

    fn remaining_bytes(&self, metafile: &Metafile<'_>) -> crate::Result<Option<u64>> {
        let Some(torrent) = self.torrent_info(&metafile.info_hash)? else {
            return Ok(None);
        };
        Ok(Some(if torrent.complete() {
            0
        } else {
            torrent.amount_left.unwrap_or(metafile.length)
        }))
    }

    fn inject(
        &self,
        new_torrent: &NewTorrent<'_>,
        searchee: &Searchee<'_>,
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
        let (category, tags) = qbit_category_and_tags(searchee, options);
        if let Some(category) = &category {
            form = form.text("category", category.as_str().to_owned());
        }
        if !tags.is_empty() {
            form = form.text(
                "tags",
                tags.iter()
                    .map(ClientLabel::as_str)
                    .collect::<Vec<_>>()
                    .join(","),
            );
        }
        form = form
            .text("paused", options.paused.to_string())
            .text("skip_checking", options.skip_checking.to_string())
            .text("contentLayout", "Original");
        block_on_client(async {
            self.client
                .post(self.api_url("/api/v2/torrents/add"))
                .multipart(form)
                .send()
                .await?
                .error_for_status()
        })?
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
        options: ResumeOptions,
    ) -> crate::Result<()> {
        resume_with_policy(self, metafile, options, || {
            self.post_hash_action(
                "/api/v2/torrents/start",
                "/api/v2/torrents/resume",
                &metafile.info_hash,
            )
        })
    }

    fn validate_config(&self) -> crate::Result<()> {
        self.login()?;
        let version = self.get_text("/api/v2/app/version")?;
        if !qb_version_at_least(&version, 4, 3, 1) {
            return Err(client_error(format!(
                "qBittorrent version {version} is below 4.3.1"
            )));
        }
        let preferences = self.get_text("/api/v2/app/preferences")?;
        let preferences = parse_qb_preferences(&preferences)?;
        if let Some(torrent_dir) = &self.torrent_dir {
            if qb_uses_sqlite_resume_data(&preferences) {
                return Err(client_error(
                    "qBittorrent torrent_dir cannot use SQLite resume-data mode",
                ));
            }
            validate_qb_fastresume_dir(torrent_dir)?;
        }
        self.post_form("/api/v2/torrents/createTags", &[("tags", "cross-seed")])?;
        Ok(())
    }
}

/// Transmission RPC adapter.
pub struct TransmissionClient {
    identity: ClientIdentity,
    rpc_url: String,
    client: reqwest::Client,
}

impl TransmissionClient {
    /// Build a Transmission adapter from normalized identity metadata.
    pub fn new(identity: ClientIdentity, timeout: Option<Duration>) -> crate::Result<Self> {
        let mut builder =
            reqwest::Client::builder().user_agent(format!("CrossSeed/{}", crate::VERSION));
        if let Some(timeout) = timeout {
            builder = builder.timeout(timeout);
        }
        let client = builder.build().map_err(|error| {
            client_error(format!("failed to build Transmission client: {error}"))
        })?;
        Ok(Self {
            rpc_url: identity.url.clone(),
            identity,
            client,
        })
    }

    fn rpc(&self, body: serde_json::Value) -> crate::Result<serde_json::Value> {
        let retry_safe =
            body.get("method").and_then(serde_json::Value::as_str) != Some("torrent-add");
        let body = body.to_string();
        let text = self.rpc_text("transmission", retry_safe, || {
            let body = body.clone();
            async move {
                let response = match self
                    .client
                    .post(&self.rpc_url)
                    .header(CONTENT_TYPE, "application/json")
                    .body(body.clone())
                    .send()
                    .await
                {
                    Ok(response) => response,
                    Err(error) => return Ok(Err(error)),
                };
                let response = if response.status() == reqwest::StatusCode::CONFLICT {
                    let Some(session_id) = response
                        .headers()
                        .get("X-Transmission-Session-Id")
                        .and_then(|value| value.to_str().ok())
                    else {
                        return Err(client_error("Transmission session id missing"));
                    };
                    match self
                        .client
                        .post(&self.rpc_url)
                        .header("X-Transmission-Session-Id", session_id.to_owned())
                        .header(CONTENT_TYPE, "application/json")
                        .body(body)
                        .send()
                        .await
                    {
                        Ok(response) => response,
                        Err(error) => return Ok(Err(error)),
                    }
                } else {
                    response
                };
                let response = match response.error_for_status() {
                    Ok(response) => response,
                    Err(error) => return Ok(Err(error)),
                };
                Ok(response.text().await)
            }
        })?;
        let value = serde_json::from_str::<serde_json::Value>(&text)
            .map_err(|error| client_error(format!("failed to parse Transmission RPC: {error}")))?;
        if value.get("result").and_then(serde_json::Value::as_str) == Some("success") {
            Ok(value)
        } else {
            Err(client_error(format!(
                "Transmission RPC result was {}",
                value
                    .get("result")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("unknown")
            )))
        }
    }

    fn rpc_text<F, Fut>(
        &self,
        kind: &'static str,
        retry_safe: bool,
        request: F,
    ) -> crate::Result<String>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = crate::Result<Result<String, reqwest::Error>>>,
    {
        let policy = RetryPolicy::idempotent();
        let max_attempts = if retry_safe { policy.max_attempts } else { 1 };
        for attempt in 1..=max_attempts {
            let result = block_on_client(request())??;
            match result {
                Ok(text) => return Ok(text),
                Err(error) if client_error_retryable(&error) && attempt < max_attempts => {
                    tracing::debug!(
                        client = %self.rpc_url,
                        kind,
                        attempt,
                        max_attempts,
                        error = %error,
                        "retrying torrent client request",
                    );
                    let delay = policy.delay_for_retry(attempt);
                    if !delay.is_zero() {
                        block_on_client_delay(delay)?;
                    }
                }
                Err(error) => {
                    return Err(client_error(format!("{kind} RPC request failed: {error}")));
                }
            }
        }
        Err(client_error(format!("{kind} RPC retry attempts exhausted")))
    }

    fn torrent_get_fields(
        &self,
        ids: Option<&[String]>,
        fields: &[&str],
    ) -> crate::Result<Vec<TransmissionTorrent>> {
        let mut arguments = serde_json::Map::new();
        arguments.insert("fields".to_owned(), serde_json::json!(fields));
        if let Some(ids) = ids {
            arguments.insert("ids".to_owned(), serde_json::json!(ids));
        }
        let response = self.rpc(serde_json::json!({
            "method": "torrent-get",
            "arguments": arguments
        }))?;
        let torrents = response
            .get("arguments")
            .and_then(|arguments| arguments.get("torrents"))
            .cloned()
            .unwrap_or_else(|| serde_json::json!([]));
        serde_json::from_value(torrents).map_err(|error| {
            client_error(format!("failed to parse Transmission torrents: {error}"))
        })
    }

    fn torrent_get(&self, ids: Option<&[String]>) -> crate::Result<Vec<TransmissionTorrent>> {
        self.torrent_get_fields(
            ids,
            &[
                "hashString",
                "name",
                "downloadDir",
                "files",
                "trackers",
                "labels",
                "percentDone",
                "leftUntilDone",
                "status",
            ],
        )
    }

    fn torrent_hashes(&self) -> crate::Result<Vec<String>> {
        Ok(self
            .torrent_get_fields(None, &["hashString"])?
            .into_iter()
            .map(|torrent| torrent.hash_string)
            .filter(|hash| InfoHash::new(hash.clone()).is_some())
            .collect())
    }

    fn torrent_info(&self, info_hash: &InfoHash<'_>) -> crate::Result<Option<TransmissionTorrent>> {
        Ok(self
            .torrent_get(Some(&[info_hash.as_str().to_owned()]))?
            .into_iter()
            .next())
    }

    fn torrent_action(&self, method: &str, info_hash: &InfoHash<'_>) -> crate::Result<()> {
        self.rpc(serde_json::json!({
            "method": method,
            "arguments": { "ids": [info_hash.as_str()] }
        }))?;
        Ok(())
    }

    fn client_torrent_from_transmission(
        torrent: TransmissionTorrent,
    ) -> Option<ClientTorrent<'static>> {
        let info_hash = InfoHash::new(torrent.hash_string.clone())?;
        let complete = torrent.complete();
        let checking = torrent.checking();
        Some(ClientTorrent {
            info_hash: info_hash.into_owned(),
            name: Cow::Owned(torrent.name),
            files: torrent
                .files
                .into_iter()
                .map(|file| File::new(file.name, file.length))
                .collect(),
            save_path: Cow::Owned(torrent.download_dir),
            category: None,
            tags: torrent.labels.into_iter().map(ClientLabel::new).collect(),
            trackers: torrent
                .trackers
                .into_iter()
                .filter_map(|tracker| tracker_host(&tracker.announce))
                .map(Cow::Owned)
                .collect(),
            complete,
            checking,
        })
    }
}

impl TorrentClient for TransmissionClient {
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
        let mut output = Vec::new();
        self.for_each_torrent(&mut |torrent| {
            output.push(torrent);
            Ok(())
        })?;
        Ok(output)
    }

    fn for_each_torrent(
        &self,
        visitor: &mut dyn FnMut(ClientTorrent<'static>) -> crate::Result<()>,
    ) -> crate::Result<()> {
        for hash in self.torrent_hashes()? {
            for torrent in self.torrent_get(Some(&[hash]))? {
                if let Some(torrent) = Self::client_torrent_from_transmission(torrent) {
                    visitor(torrent)?;
                }
            }
        }
        Ok(())
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
        Ok(Ok(PathBuf::from(torrent.download_dir)))
    }

    fn get_all_download_dirs(&self) -> crate::Result<BTreeMap<String, PathBuf>> {
        Ok(self
            .torrent_get(None)?
            .into_iter()
            .map(|torrent| (torrent.hash_string, PathBuf::from(torrent.download_dir)))
            .collect())
    }

    fn has_matching_download_dir(
        &self,
        predicate: &mut dyn FnMut(&Path) -> crate::Result<bool>,
    ) -> crate::Result<bool> {
        for hash in self.torrent_hashes()? {
            for torrent in self.torrent_get_fields(Some(&[hash]), &["hashString", "downloadDir"])? {
                if predicate(Path::new(&torrent.download_dir))? {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    fn remaining_bytes(&self, metafile: &Metafile<'_>) -> crate::Result<Option<u64>> {
        let Some(torrent) = self.torrent_info(&metafile.info_hash)? else {
            return Ok(None);
        };
        Ok(Some(if torrent.complete() {
            0
        } else {
            torrent.left_until_done.unwrap_or(metafile.length)
        }))
    }

    fn inject(
        &self,
        new_torrent: &NewTorrent<'_>,
        _searchee: &Searchee<'_>,
        _decision: Decision,
        options: &InjectionOptions,
    ) -> crate::Result<InjectionResult> {
        ensure_writable(self)?;
        let mut arguments = serde_json::Map::new();
        arguments.insert(
            "metainfo".to_owned(),
            serde_json::Value::String(base64_encode(new_torrent.bytes.as_ref())),
        );
        arguments.insert("paused".to_owned(), serde_json::Value::Bool(options.paused));
        if let Some(destination) = &options.destination_dir {
            arguments.insert(
                "download-dir".to_owned(),
                serde_json::Value::String(destination.display().to_string()),
            );
        }
        let labels = options
            .category
            .iter()
            .chain(options.tags.iter())
            .map(ClientLabel::as_str)
            .collect::<Vec<_>>();
        if !labels.is_empty() {
            arguments.insert("labels".to_owned(), serde_json::json!(labels));
        }
        self.rpc(serde_json::json!({
            "method": "torrent-add",
            "arguments": arguments
        }))?;
        if options.paused {
            self.torrent_action("torrent-stop", &new_torrent.metafile.info_hash)?;
        }
        Ok(InjectionResult::Injected)
    }

    fn recheck_torrent(&self, info_hash: &InfoHash<'_>) -> crate::Result<()> {
        self.torrent_action("torrent-verify", info_hash)
    }

    fn resume_injection(
        &self,
        metafile: &Metafile<'_>,
        _decision: Decision,
        options: ResumeOptions,
    ) -> crate::Result<()> {
        resume_with_policy(self, metafile, options, || {
            self.torrent_action("torrent-start", &metafile.info_hash)
        })
    }

    fn validate_config(&self) -> crate::Result<()> {
        self.rpc(serde_json::json!({ "method": "session-get" }))?;
        Ok(())
    }
}

/// Deluge Web JSON-RPC adapter.
pub struct DelugeClient {
    identity: ClientIdentity,
    rpc_url: String,
    password: String,
    client: reqwest::Client,
}

impl DelugeClient {
    /// Build a Deluge adapter from normalized identity metadata.
    pub fn new(identity: ClientIdentity, timeout: Option<Duration>) -> crate::Result<Self> {
        let mut url = Url::parse(&identity.url)
            .map_err(|error| client_error(format!("invalid Deluge URL: {error}")))?;
        let password = url.password().unwrap_or("deluge").to_owned();
        url.set_username("")
            .map_err(|()| client_error("failed to sanitize Deluge username"))?;
        url.set_password(None)
            .map_err(|()| client_error("failed to sanitize Deluge password"))?;
        let mut builder = reqwest::Client::builder()
            .cookie_store(true)
            .user_agent(format!("CrossSeed/{}", crate::VERSION));
        if let Some(timeout) = timeout {
            builder = builder.timeout(timeout);
        }
        let client = builder
            .build()
            .map_err(|error| client_error(format!("failed to build Deluge client: {error}")))?;
        let base_url = url.to_string().trim_end_matches('/').to_owned();
        let rpc_url = if base_url.ends_with("/json") {
            base_url
        } else {
            format!("{base_url}/json")
        };
        Ok(Self {
            identity,
            rpc_url,
            password,
            client,
        })
    }

    fn rpc(&self, method: &str, params: serde_json::Value) -> crate::Result<serde_json::Value> {
        let retry_safe = method != "core.add_torrent_file";
        let body = serde_json::json!({
            "method": method,
            "params": params,
            "id": 1
        })
        .to_string();
        let text = self.rpc_text("deluge", retry_safe, || {
            let body = body.clone();
            async move {
                let response = match self
                    .client
                    .post(&self.rpc_url)
                    .header(CONTENT_TYPE, "application/json")
                    .body(body)
                    .send()
                    .await
                {
                    Ok(response) => response,
                    Err(error) => return Ok(Err(error)),
                };
                let response = match response.error_for_status() {
                    Ok(response) => response,
                    Err(error) => return Ok(Err(error)),
                };
                Ok(response.text().await)
            }
        })?;
        let response = serde_json::from_str::<DelugeRpcResponse>(&text)
            .map_err(|error| client_error(format!("failed to parse Deluge RPC: {error}")))?;
        if let Some(error) = response.error {
            return Err(client_error(format!("Deluge RPC {method} failed: {error}")));
        }
        Ok(response.result.unwrap_or(serde_json::Value::Null))
    }

    fn rpc_text<F, Fut>(
        &self,
        kind: &'static str,
        retry_safe: bool,
        request: F,
    ) -> crate::Result<String>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = crate::Result<Result<String, reqwest::Error>>>,
    {
        let policy = RetryPolicy::idempotent();
        let max_attempts = if retry_safe { policy.max_attempts } else { 1 };
        for attempt in 1..=max_attempts {
            let result = block_on_client(request())??;
            match result {
                Ok(text) => return Ok(text),
                Err(error) if client_error_retryable(&error) && attempt < max_attempts => {
                    tracing::debug!(
                        client = %self.rpc_url,
                        kind,
                        attempt,
                        max_attempts,
                        error = %error,
                        "retrying torrent client request",
                    );
                    let delay = policy.delay_for_retry(attempt);
                    if !delay.is_zero() {
                        block_on_client_delay(delay)?;
                    }
                }
                Err(error) => {
                    return Err(client_error(format!("{kind} RPC request failed: {error}")));
                }
            }
        }
        Err(client_error(format!("{kind} RPC retry attempts exhausted")))
    }

    fn login(&self) -> crate::Result<()> {
        let result = self.rpc("auth.login", serde_json::json!([self.password]))?;
        if result.as_bool() == Some(true) {
            Ok(())
        } else {
            Err(client_error("Deluge authentication failed"))
        }
    }

    fn ensure_connected(&self) -> crate::Result<()> {
        self.login()?;
        if self.rpc("web.connected", serde_json::json!([]))?.as_bool() == Some(true) {
            return Ok(());
        }

        let hosts = self.rpc("web.get_hosts", serde_json::json!([]))?;
        let host_id = hosts
            .as_array()
            .and_then(|hosts| hosts.first())
            .and_then(serde_json::Value::as_array)
            .and_then(|host| host.first())
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| client_error("Deluge Web returned no hosts"))?;
        let connected = self.rpc("web.connect", serde_json::json!([host_id]))?;
        if connected.as_bool() == Some(false) {
            Err(client_error("Deluge host connection failed"))
        } else {
            Ok(())
        }
    }

    fn update_ui_fields(
        &self,
        ids: Option<&[String]>,
        fields: &[&str],
    ) -> crate::Result<Vec<DelugeTorrent>> {
        self.ensure_connected()?;
        let mut filter = serde_json::Map::new();
        if let Some(ids) = ids {
            filter.insert("id".to_owned(), serde_json::json!(ids));
        }
        let response = self.rpc("web.update_ui", serde_json::json!([fields, filter]))?;
        let Some(torrents) = response
            .get("torrents")
            .and_then(serde_json::Value::as_object)
        else {
            return Ok(Vec::new());
        };
        torrents
            .iter()
            .map(|(id, value)| {
                let mut torrent =
                    serde_json::from_value::<DelugeTorrent>(value.clone()).map_err(|error| {
                        client_error(format!("failed to parse Deluge torrent: {error}"))
                    })?;
                if torrent.hash.is_empty() {
                    torrent.hash.clone_from(id);
                }
                Ok(torrent)
            })
            .collect()
    }

    fn update_ui(&self, ids: Option<&[String]>) -> crate::Result<Vec<DelugeTorrent>> {
        self.update_ui_fields(
            ids,
            &[
                "name",
                "hash",
                "save_path",
                "files",
                "tracker_host",
                "label",
                "progress",
                "total_remaining",
                "state",
            ],
        )
    }

    fn torrent_hashes(&self) -> crate::Result<Vec<String>> {
        Ok(self
            .update_ui_fields(None, &["hash"])?
            .into_iter()
            .map(|torrent| torrent.hash)
            .filter(|hash| InfoHash::new(hash.as_str()).is_some())
            .collect())
    }

    fn torrent_info(&self, info_hash: &InfoHash<'_>) -> crate::Result<Option<DelugeTorrent>> {
        Ok(self
            .update_ui(Some(&[info_hash.as_str().to_owned()]))?
            .into_iter()
            .next())
    }

    fn torrent_action(&self, method: &str, info_hash: &InfoHash<'_>) -> crate::Result<()> {
        self.ensure_connected()?;
        self.rpc(method, serde_json::json!([[info_hash.as_str()]]))?;
        Ok(())
    }

    fn label_torrent(
        &self,
        info_hash: &InfoHash<'_>,
        label: &ClientLabel<'_>,
    ) -> crate::Result<()> {
        let label = label.as_str();
        let labels = self.rpc("label.get_labels", serde_json::json!([]))?;
        let label_exists = labels
            .as_array()
            .is_some_and(|labels| labels.iter().any(|value| value.as_str() == Some(label)));
        if !label_exists {
            self.rpc("label.add", serde_json::json!([label]))?;
        }
        self.rpc(
            "label.set_torrent",
            serde_json::json!([info_hash.as_str(), label]),
        )?;
        Ok(())
    }

    fn client_torrent_from_deluge(torrent: DelugeTorrent) -> Option<ClientTorrent<'static>> {
        let info_hash = InfoHash::new(torrent.hash.clone())?;
        let complete = torrent.complete();
        let checking = torrent.checking();
        let tracker =
            (!torrent.tracker_host.is_empty()).then_some(Cow::Owned(torrent.tracker_host));
        Some(ClientTorrent {
            info_hash: info_hash.into_owned(),
            name: Cow::Owned(torrent.name),
            files: torrent
                .files
                .into_iter()
                .map(|file| File::new(file.path, file.size))
                .collect(),
            save_path: Cow::Owned(torrent.save_path),
            category: torrent
                .label
                .filter(|label| !label.is_empty())
                .map(ClientLabel::new),
            tags: Vec::new(),
            trackers: tracker.into_iter().collect(),
            complete,
            checking,
        })
    }
}

impl TorrentClient for DelugeClient {
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
        let mut output = Vec::new();
        self.for_each_torrent(&mut |torrent| {
            output.push(torrent);
            Ok(())
        })?;
        Ok(output)
    }

    fn for_each_torrent(
        &self,
        visitor: &mut dyn FnMut(ClientTorrent<'static>) -> crate::Result<()>,
    ) -> crate::Result<()> {
        for hash in self.torrent_hashes()? {
            for torrent in self.update_ui(Some(&[hash]))? {
                if let Some(torrent) = Self::client_torrent_from_deluge(torrent) {
                    visitor(torrent)?;
                }
            }
        }
        Ok(())
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
        Ok(self
            .update_ui(None)?
            .into_iter()
            .map(|torrent| (torrent.hash, PathBuf::from(torrent.save_path)))
            .collect())
    }

    fn has_matching_download_dir(
        &self,
        predicate: &mut dyn FnMut(&Path) -> crate::Result<bool>,
    ) -> crate::Result<bool> {
        for hash in self.torrent_hashes()? {
            for torrent in self.update_ui_fields(Some(&[hash]), &["hash", "save_path"])? {
                if predicate(Path::new(&torrent.save_path))? {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    fn remaining_bytes(&self, metafile: &Metafile<'_>) -> crate::Result<Option<u64>> {
        let Some(torrent) = self.torrent_info(&metafile.info_hash)? else {
            return Ok(None);
        };
        Ok(Some(if torrent.complete() {
            0
        } else {
            torrent.total_remaining.unwrap_or(metafile.length)
        }))
    }

    fn inject(
        &self,
        new_torrent: &NewTorrent<'_>,
        searchee: &Searchee<'_>,
        _decision: Decision,
        options: &InjectionOptions,
    ) -> crate::Result<InjectionResult> {
        ensure_writable(self)?;
        self.ensure_connected()?;
        let mut add_options = serde_json::Map::new();
        add_options.insert(
            "add_paused".to_owned(),
            serde_json::Value::Bool(options.paused),
        );
        if let Some(destination) = &options.destination_dir {
            add_options.insert(
                "download_location".to_owned(),
                serde_json::Value::String(destination.display().to_string()),
            );
        }
        self.rpc(
            "core.add_torrent_file",
            serde_json::json!([
                format!("{}.torrent", new_torrent.metafile.info_hash),
                base64_encode(new_torrent.bytes.as_ref()),
                add_options
            ]),
        )?;

        let label = primary_client_label(searchee, options);
        if let Some(label) = label {
            self.label_torrent(&new_torrent.metafile.info_hash, &label)?;
        }
        if options.paused {
            self.rpc(
                "core.pause_torrent",
                serde_json::json!([[new_torrent.metafile.info_hash.as_str()]]),
            )?;
        }
        Ok(InjectionResult::Injected)
    }

    fn recheck_torrent(&self, info_hash: &InfoHash<'_>) -> crate::Result<()> {
        self.torrent_action("core.force_recheck", info_hash)
    }

    fn resume_injection(
        &self,
        metafile: &Metafile<'_>,
        _decision: Decision,
        options: ResumeOptions,
    ) -> crate::Result<()> {
        resume_with_policy(self, metafile, options, || {
            self.torrent_action("core.resume_torrent", &metafile.info_hash)
        })
    }

    fn validate_config(&self) -> crate::Result<()> {
        self.ensure_connected()?;
        let plugins = self.rpc("core.get_enabled_plugins", serde_json::json!([]))?;
        let label_enabled = plugins.as_array().is_some_and(|plugins| {
            plugins
                .iter()
                .any(|plugin| plugin.as_str() == Some("Label"))
        });
        if label_enabled {
            Ok(())
        } else {
            Err(client_error("Deluge Label plugin is not enabled"))
        }
    }
}

/// rTorrent XML-RPC adapter.
pub struct RtorrentClient {
    identity: ClientIdentity,
    rpc_url: String,
    username: String,
    password: Option<String>,
    client: reqwest::Client,
}

impl RtorrentClient {
    /// Build an rTorrent adapter from normalized identity metadata.
    pub fn new(identity: ClientIdentity, timeout: Option<Duration>) -> crate::Result<Self> {
        let mut url = Url::parse(&identity.url)
            .map_err(|error| client_error(format!("invalid rTorrent URL: {error}")))?;
        let username = url.username().to_owned();
        let password = url.password().map(str::to_owned);
        url.set_username("")
            .map_err(|()| client_error("failed to sanitize rTorrent username"))?;
        url.set_password(None)
            .map_err(|()| client_error("failed to sanitize rTorrent password"))?;
        let mut builder =
            reqwest::Client::builder().user_agent(format!("CrossSeed/{}", crate::VERSION));
        if let Some(timeout) = timeout {
            builder = builder.timeout(timeout);
        }
        let client = builder
            .build()
            .map_err(|error| client_error(format!("failed to build rTorrent client: {error}")))?;
        Ok(Self {
            identity,
            rpc_url: url.to_string(),
            username,
            password,
            client,
        })
    }

    fn rpc(&self, method: &str, params: &[RtXmlParam]) -> crate::Result<RtXmlValue> {
        let retry_safe = !matches!(method, "load.raw" | "load.raw_start");
        let body = rt_xml_call(method, params);
        let text = self.rpc_text(retry_safe, || {
            let body = body.clone();
            async move {
                let mut request = self
                    .client
                    .post(&self.rpc_url)
                    .header(CONTENT_TYPE, "text/xml")
                    .body(body);
                if let Some(password) = &self.password {
                    request = request.basic_auth(&self.username, Some(password));
                }
                let response = match request.send().await {
                    Ok(response) => response,
                    Err(error) => return Ok(Err(error)),
                };
                let response = match response.error_for_status() {
                    Ok(response) => response,
                    Err(error) => return Ok(Err(error)),
                };
                Ok(response.text().await)
            }
        })?;
        rt_parse_response(&text)
    }

    fn rpc_text<F, Fut>(&self, retry_safe: bool, request: F) -> crate::Result<String>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = crate::Result<Result<String, reqwest::Error>>>,
    {
        let policy = RetryPolicy::idempotent();
        let max_attempts = if retry_safe { policy.max_attempts } else { 1 };
        for attempt in 1..=max_attempts {
            let result = block_on_client(request())??;
            match result {
                Ok(text) => return Ok(text),
                Err(error) if client_error_retryable(&error) && attempt < max_attempts => {
                    tracing::debug!(
                        client = %self.rpc_url,
                        kind = "rtorrent",
                        attempt,
                        max_attempts,
                        error = %error,
                        "retrying torrent client request",
                    );
                    let delay = policy.delay_for_retry(attempt);
                    if !delay.is_zero() {
                        block_on_client_delay(delay)?;
                    }
                }
                Err(error) => {
                    return Err(client_error(format!(
                        "rTorrent XML-RPC request failed: {error}"
                    )));
                }
            }
        }
        Err(client_error("rTorrent XML-RPC retry attempts exhausted"))
    }

    fn hashes(&self) -> crate::Result<Vec<String>> {
        let value = self.rpc("download_list", &[])?;
        Ok(value
            .into_array()
            .into_iter()
            .filter_map(RtXmlValue::into_string)
            .collect())
    }

    fn torrent_info(&self, info_hash: &InfoHash<'_>) -> crate::Result<Option<RtTorrent>> {
        if self
            .hashes()?
            .iter()
            .any(|hash| hash.eq_ignore_ascii_case(info_hash.as_str()))
        {
            self.fetch_torrent(info_hash.as_str()).map(Some)
        } else {
            Ok(None)
        }
    }

    fn fetch_torrent(&self, hash: &str) -> crate::Result<RtTorrent> {
        let calls = [
            rt_call("d.name", &[RtXmlParam::String(hash.to_owned())]),
            rt_call("d.directory", &[RtXmlParam::String(hash.to_owned())]),
            rt_call("d.left_bytes", &[RtXmlParam::String(hash.to_owned())]),
            rt_call("d.hashing", &[RtXmlParam::String(hash.to_owned())]),
            rt_call("d.complete", &[RtXmlParam::String(hash.to_owned())]),
            rt_call("d.is_multi_file", &[RtXmlParam::String(hash.to_owned())]),
            rt_call("d.is_active", &[RtXmlParam::String(hash.to_owned())]),
            rt_call("d.custom1", &[RtXmlParam::String(hash.to_owned())]),
            rt_call(
                "f.multicall",
                &[
                    RtXmlParam::String(hash.to_owned()),
                    RtXmlParam::String(String::new()),
                    RtXmlParam::String("f.path=".to_owned()),
                    RtXmlParam::String("f.size_bytes=".to_owned()),
                ],
            ),
            rt_call(
                "t.multicall",
                &[
                    RtXmlParam::String(hash.to_owned()),
                    RtXmlParam::String(String::new()),
                    RtXmlParam::String("t.url=".to_owned()),
                    RtXmlParam::String("t.group=".to_owned()),
                ],
            ),
        ];
        let values = self
            .rpc(
                "system.multicall",
                &[RtXmlParam::Array(calls.into_iter().collect())],
            )?
            .into_array();
        Ok(RtTorrent {
            name: rt_wrapped_string(values.first()),
            directory: rt_wrapped_string(values.get(1)),
            left_bytes: rt_wrapped_i64(values.get(2)),
            hashing: rt_wrapped_bool(values.get(3)),
            complete: rt_wrapped_bool(values.get(4)),
            _multi_file: rt_wrapped_bool(values.get(5)),
            label: rt_wrapped_string(values.get(7)),
            files: rt_wrapped_array(values.get(8))
                .into_iter()
                .filter_map(rt_file_row)
                .collect(),
            trackers: rt_wrapped_array(values.get(9))
                .into_iter()
                .filter_map(rt_tracker_row)
                .collect(),
        })
    }

    fn client_torrent_from_rtorrent(
        hash: String,
        torrent: RtTorrent,
    ) -> Option<ClientTorrent<'static>> {
        let info_hash = InfoHash::new(hash)?;
        let complete = torrent.complete();
        let checking = torrent.checking();
        let tags = (!torrent.label.is_empty()).then_some(ClientLabel::new(torrent.label));
        Some(ClientTorrent {
            info_hash: info_hash.into_owned(),
            name: Cow::Owned(torrent.name),
            files: torrent.files,
            save_path: Cow::Owned(torrent.directory),
            category: None,
            tags: tags.into_iter().collect(),
            trackers: torrent.trackers,
            complete,
            checking,
        })
    }

    fn action(&self, method: &str, info_hash: &InfoHash<'_>) -> crate::Result<()> {
        self.rpc(method, &[RtXmlParam::String(info_hash.as_str().to_owned())])?;
        Ok(())
    }
}

impl TorrentClient for RtorrentClient {
    fn metadata(&self) -> &TorrentClientMetadata<'_> {
        &self.identity.metadata
    }

    fn is_torrent_in_client(&self, info_hash: &InfoHash<'_>) -> crate::Result<bool> {
        Ok(self.hashes()?.iter().any(|hash| hash == info_hash.as_str()))
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
        let mut output = Vec::new();
        self.for_each_torrent(&mut |torrent| {
            output.push(torrent);
            Ok(())
        })?;
        Ok(output)
    }

    fn for_each_torrent(
        &self,
        visitor: &mut dyn FnMut(ClientTorrent<'static>) -> crate::Result<()>,
    ) -> crate::Result<()> {
        for hash in self.hashes()? {
            let Some(_) = InfoHash::new(hash.as_str()) else {
                continue;
            };
            let torrent = self.fetch_torrent(&hash)?;
            if let Some(torrent) = Self::client_torrent_from_rtorrent(hash, torrent) {
                visitor(torrent)?;
            }
        }
        Ok(())
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
        Ok(Ok(PathBuf::from(torrent.directory)))
    }

    fn get_all_download_dirs(&self) -> crate::Result<BTreeMap<String, PathBuf>> {
        let mut output = BTreeMap::new();
        for hash in self.hashes()? {
            let torrent = self.fetch_torrent(&hash)?;
            output.insert(hash, PathBuf::from(torrent.directory));
        }
        Ok(output)
    }

    fn has_matching_download_dir(
        &self,
        predicate: &mut dyn FnMut(&Path) -> crate::Result<bool>,
    ) -> crate::Result<bool> {
        for hash in self.hashes()? {
            let torrent = self.fetch_torrent(&hash)?;
            if predicate(Path::new(&torrent.directory))? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn remaining_bytes(&self, metafile: &Metafile<'_>) -> crate::Result<Option<u64>> {
        let Some(torrent) = self.torrent_info(&metafile.info_hash)? else {
            return Ok(None);
        };
        Ok(Some(if torrent.complete() {
            0
        } else {
            u64::try_from(torrent.left_bytes).unwrap_or(metafile.length)
        }))
    }

    fn inject(
        &self,
        new_torrent: &NewTorrent<'_>,
        _searchee: &Searchee<'_>,
        _decision: Decision,
        options: &InjectionOptions,
    ) -> crate::Result<InjectionResult> {
        ensure_writable(self)?;
        let method = if options.paused {
            "load.raw"
        } else {
            "load.raw_start"
        };
        let mut params = vec![
            RtXmlParam::String(String::new()),
            RtXmlParam::Base64(base64_encode(new_torrent.bytes.as_ref())),
        ];
        if let Some(destination) = &options.destination_dir {
            params.push(RtXmlParam::String(format!(
                "d.directory.set={}",
                destination.display()
            )));
        }
        self.rpc(method, &params)?;
        let label = options
            .tags
            .first()
            .or(options.category.as_ref())
            .map(ClientLabel::as_str)
            .unwrap_or("cross-seed");
        self.rpc(
            "d.custom1.set",
            &[
                RtXmlParam::String(new_torrent.metafile.info_hash.as_str().to_owned()),
                RtXmlParam::String(label.to_owned()),
            ],
        )?;
        if options.paused {
            self.action("d.pause", &new_torrent.metafile.info_hash)?;
        }
        Ok(InjectionResult::Injected)
    }

    fn recheck_torrent(&self, info_hash: &InfoHash<'_>) -> crate::Result<()> {
        self.action("d.check_hash", info_hash)
    }

    fn resume_injection(
        &self,
        metafile: &Metafile<'_>,
        _decision: Decision,
        options: ResumeOptions,
    ) -> crate::Result<()> {
        resume_with_policy(self, metafile, options, || {
            self.action("d.resume", &metafile.info_hash)
        })
    }

    fn validate_config(&self) -> crate::Result<()> {
        self.rpc("download_list", &[])?;
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
    amount_left: Option<u64>,
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

fn parse_qb_preferences(body: &str) -> crate::Result<serde_json::Value> {
    serde_json::from_str(body).map_err(|error| {
        client_error(format!(
            "failed to parse qBittorrent preferences response: {error}"
        ))
    })
}

fn qb_uses_sqlite_resume_data(preferences: &serde_json::Value) -> bool {
    let Some(value) = preferences.get("resume_data_storage_type") else {
        return false;
    };
    value
        .as_str()
        .is_some_and(|value| value.to_ascii_lowercase().contains("sqlite"))
        || value.as_i64() == Some(1)
}

fn validate_qb_fastresume_dir(torrent_dir: &Path) -> crate::Result<()> {
    let mut saw_fastresume = false;
    for entry in fs::read_dir(torrent_dir)
        .map_err(|error| client_error(format!("failed to read torrent_dir: {error}")))?
    {
        let entry = entry
            .map_err(|error| client_error(format!("failed to read torrent_dir entry: {error}")))?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if path.extension().and_then(|extension| extension.to_str()) == Some("fastresume") {
            saw_fastresume = true;
            continue;
        }
        if path.extension().and_then(|extension| extension.to_str()) == Some("torrent") {
            let fastresume = path.with_extension("fastresume");
            if !fastresume.is_file() {
                return Err(client_error(format!(
                    "qBittorrent torrent_dir entry {} is missing a .fastresume sidecar",
                    path.display()
                )));
            }
        }
    }
    if saw_fastresume {
        Ok(())
    } else {
        Err(client_error(
            "qBittorrent torrent_dir requires .fastresume files",
        ))
    }
}

#[derive(Debug, serde::Deserialize)]
struct TransmissionTorrent {
    #[serde(rename = "hashString")]
    hash_string: String,
    #[serde(default)]
    name: String,
    #[serde(rename = "downloadDir", default)]
    download_dir: String,
    #[serde(default)]
    files: Vec<TransmissionFile>,
    #[serde(default)]
    trackers: Vec<TransmissionTracker>,
    #[serde(default)]
    labels: Vec<String>,
    #[serde(rename = "percentDone", default)]
    percent_done: f64,
    #[serde(rename = "leftUntilDone", default)]
    left_until_done: Option<u64>,
    #[serde(default)]
    status: i64,
}

impl TransmissionTorrent {
    fn complete(&self) -> bool {
        self.percent_done >= 1.0 || self.status == 6
    }

    fn checking(&self) -> bool {
        self.status == 2
    }
}

#[derive(Debug, serde::Deserialize)]
struct TransmissionFile {
    name: String,
    length: u64,
}

#[derive(Debug, serde::Deserialize)]
struct TransmissionTracker {
    announce: String,
}

#[derive(Debug, serde::Deserialize)]
struct DelugeRpcResponse {
    #[serde(default)]
    result: Option<serde_json::Value>,
    #[serde(default)]
    error: Option<serde_json::Value>,
}

#[derive(Debug, serde::Deserialize)]
struct DelugeTorrent {
    #[serde(default)]
    hash: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    save_path: String,
    #[serde(default)]
    files: Vec<DelugeFile>,
    #[serde(default)]
    tracker_host: String,
    #[serde(default)]
    label: Option<String>,
    #[serde(default)]
    progress: f64,
    #[serde(default)]
    total_remaining: Option<u64>,
    #[serde(default)]
    state: String,
}

impl DelugeTorrent {
    fn complete(&self) -> bool {
        self.progress >= 100.0 || self.state.eq_ignore_ascii_case("seeding")
    }

    fn checking(&self) -> bool {
        self.state.to_ascii_lowercase().contains("check")
    }
}

#[derive(Debug, serde::Deserialize)]
struct DelugeFile {
    path: String,
    size: u64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum RtXmlParam {
    String(String),
    Base64(String),
    Array(Vec<RtXmlParam>),
    Struct(Vec<(String, RtXmlParam)>),
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum RtXmlValue {
    String(String),
    I64(i64),
    Bool(bool),
    Array(Vec<RtXmlValue>),
    Struct(BTreeMap<String, RtXmlValue>),
}

impl RtXmlValue {
    fn into_array(self) -> Vec<RtXmlValue> {
        match self {
            Self::Array(values) => values,
            _ => Vec::new(),
        }
    }

    fn into_string(self) -> Option<String> {
        match self {
            Self::String(value) => Some(value),
            Self::I64(value) => Some(value.to_string()),
            _ => None,
        }
    }

    fn as_i64(&self) -> i64 {
        match self {
            Self::I64(value) => *value,
            Self::String(value) => value.parse::<i64>().unwrap_or_default(),
            Self::Bool(value) => i64::from(*value),
            _ => 0,
        }
    }

    fn as_bool(&self) -> bool {
        match self {
            Self::Bool(value) => *value,
            Self::I64(value) => *value != 0,
            Self::String(value) => value == "1" || value.eq_ignore_ascii_case("true"),
            _ => false,
        }
    }

    fn as_string(&self) -> String {
        match self {
            Self::String(value) => value.clone(),
            Self::I64(value) => value.to_string(),
            Self::Bool(value) => i64::from(*value).to_string(),
            _ => String::new(),
        }
    }
}

#[derive(Debug)]
struct RtTorrent {
    name: String,
    directory: String,
    left_bytes: i64,
    hashing: bool,
    complete: bool,
    _multi_file: bool,
    label: String,
    files: Vec<File<'static>>,
    trackers: Vec<Cow<'static, str>>,
}

impl RtTorrent {
    fn complete(&self) -> bool {
        self.complete || self.left_bytes == 0
    }

    fn checking(&self) -> bool {
        self.hashing
    }
}

fn rt_call(method: &str, params: &[RtXmlParam]) -> RtXmlParam {
    RtXmlParam::Struct(vec![
        (
            "methodName".to_owned(),
            RtXmlParam::String(method.to_owned()),
        ),
        ("params".to_owned(), RtXmlParam::Array(params.to_vec())),
    ])
}

fn rt_wrapped_value(value: Option<&RtXmlValue>) -> Option<&RtXmlValue> {
    match value {
        Some(RtXmlValue::Array(values)) => values.first(),
        other => other,
    }
}

fn rt_wrapped_string(value: Option<&RtXmlValue>) -> String {
    rt_wrapped_value(value)
        .map(RtXmlValue::as_string)
        .unwrap_or_default()
}

fn rt_wrapped_i64(value: Option<&RtXmlValue>) -> i64 {
    rt_wrapped_value(value)
        .map(RtXmlValue::as_i64)
        .unwrap_or_default()
}

fn rt_wrapped_bool(value: Option<&RtXmlValue>) -> bool {
    rt_wrapped_value(value).is_some_and(RtXmlValue::as_bool)
}

fn rt_wrapped_array(value: Option<&RtXmlValue>) -> Vec<RtXmlValue> {
    match rt_wrapped_value(value) {
        Some(RtXmlValue::Array(values)) => values.clone(),
        _ => Vec::new(),
    }
}

fn rt_file_row(value: RtXmlValue) -> Option<File<'static>> {
    let values = value.into_array();
    let path = values.first().map(RtXmlValue::as_string)?;
    let size = values
        .get(1)
        .map(RtXmlValue::as_i64)
        .and_then(|value| u64::try_from(value).ok())
        .unwrap_or_default();
    Some(File::new(path, size))
}

fn rt_tracker_row(value: RtXmlValue) -> Option<Cow<'static, str>> {
    let values = value.into_array();
    let url = values
        .first()
        .map(RtXmlValue::as_string)
        .unwrap_or_default();
    if let Some(host) = tracker_host(&url) {
        return Some(Cow::Owned(host));
    }
    values
        .get(1)
        .map(RtXmlValue::as_string)
        .filter(|group| !group.is_empty())
        .map(Cow::Owned)
}

fn rt_xml_call(method: &str, params: &[RtXmlParam]) -> String {
    let mut output = String::from("<?xml version=\"1.0\"?><methodCall><methodName>");
    output.push_str(&xml_escape(method));
    output.push_str("</methodName><params>");
    for param in params {
        output.push_str("<param>");
        rt_push_param(&mut output, param);
        output.push_str("</param>");
    }
    output.push_str("</params></methodCall>");
    output
}

fn rt_push_param(output: &mut String, param: &RtXmlParam) {
    output.push_str("<value>");
    match param {
        RtXmlParam::String(value) => {
            output.push_str("<string>");
            output.push_str(&xml_escape(value));
            output.push_str("</string>");
        }
        RtXmlParam::Base64(value) => {
            output.push_str("<base64>");
            output.push_str(value);
            output.push_str("</base64>");
        }
        RtXmlParam::Array(values) => {
            output.push_str("<array><data>");
            for value in values {
                rt_push_param(output, value);
            }
            output.push_str("</data></array>");
        }
        RtXmlParam::Struct(entries) => {
            output.push_str("<struct>");
            for (name, value) in entries {
                output.push_str("<member><name>");
                output.push_str(&xml_escape(name));
                output.push_str("</name>");
                rt_push_param(output, value);
                output.push_str("</member>");
            }
            output.push_str("</struct>");
        }
    }
    output.push_str("</value>");
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn rt_parse_response(xml: &str) -> crate::Result<RtXmlValue> {
    let mut parser = RtXmlParser::new(xml);
    parser.parse_response()
}

struct RtXmlParser<'a> {
    reader: Reader<&'a [u8]>,
    buf: Vec<u8>,
}

impl<'a> RtXmlParser<'a> {
    fn new(xml: &'a str) -> Self {
        let mut reader = Reader::from_str(xml);
        reader.config_mut().trim_text(true);
        Self {
            reader,
            buf: Vec::new(),
        }
    }

    fn parse_response(&mut self) -> crate::Result<RtXmlValue> {
        loop {
            match self.reader.read_event_into(&mut self.buf) {
                Ok(Event::Start(event)) if event.name().as_ref() == b"value" => {
                    self.buf.clear();
                    return self.parse_value();
                }
                Ok(Event::Eof) => return Err(client_error("empty rTorrent XML-RPC response")),
                Err(error) => {
                    return Err(client_error(format!(
                        "invalid rTorrent XML-RPC response: {error}"
                    )));
                }
                _ => {}
            }
            self.buf.clear();
        }
    }

    fn parse_value(&mut self) -> crate::Result<RtXmlValue> {
        let mut text = String::new();
        loop {
            match self.reader.read_event_into(&mut self.buf) {
                Ok(Event::Start(event)) => {
                    let name = event.name().as_ref().to_vec();
                    self.buf.clear();
                    return match name.as_slice() {
                        b"array" => self.parse_array(),
                        b"struct" => self.parse_struct(),
                        b"string" | b"base64" => self.read_typed_string(&name),
                        b"int" | b"i4" | b"i8" => self.read_typed_i64(&name),
                        b"boolean" => self.read_typed_bool(&name),
                        _ => self.read_typed_string(&name),
                    };
                }
                Ok(Event::Text(event)) => {
                    text.push_str(&String::from_utf8_lossy(event.as_ref()));
                }
                Ok(Event::CData(event)) => {
                    text.push_str(&String::from_utf8_lossy(event.as_ref()));
                }
                Ok(Event::End(event)) if event.name().as_ref() == b"value" => {
                    self.buf.clear();
                    return Ok(RtXmlValue::String(text));
                }
                Ok(Event::Eof) => return Err(client_error("unterminated XML-RPC value")),
                Err(error) => return Err(client_error(format!("invalid XML-RPC value: {error}"))),
                _ => {}
            }
            self.buf.clear();
        }
    }

    fn parse_array(&mut self) -> crate::Result<RtXmlValue> {
        let mut values = Vec::new();
        loop {
            match self.reader.read_event_into(&mut self.buf) {
                Ok(Event::Start(event)) if event.name().as_ref() == b"value" => {
                    self.buf.clear();
                    values.push(self.parse_value()?);
                }
                Ok(Event::End(event)) if event.name().as_ref() == b"array" => {
                    self.buf.clear();
                    return Ok(RtXmlValue::Array(values));
                }
                Ok(Event::Eof) => return Err(client_error("unterminated XML-RPC array")),
                Err(error) => return Err(client_error(format!("invalid XML-RPC array: {error}"))),
                _ => {}
            }
            self.buf.clear();
        }
    }

    fn parse_struct(&mut self) -> crate::Result<RtXmlValue> {
        let mut entries = BTreeMap::new();
        loop {
            match self.reader.read_event_into(&mut self.buf) {
                Ok(Event::Start(event)) if event.name().as_ref() == b"member" => {
                    self.buf.clear();
                    if let Some((name, value)) = self.parse_member()? {
                        entries.insert(name, value);
                    }
                }
                Ok(Event::End(event)) if event.name().as_ref() == b"struct" => {
                    self.buf.clear();
                    return Ok(RtXmlValue::Struct(entries));
                }
                Ok(Event::Eof) => return Err(client_error("unterminated XML-RPC struct")),
                Err(error) => return Err(client_error(format!("invalid XML-RPC struct: {error}"))),
                _ => {}
            }
            self.buf.clear();
        }
    }

    fn parse_member(&mut self) -> crate::Result<Option<(String, RtXmlValue)>> {
        let mut name = None;
        let mut value = None;
        loop {
            match self.reader.read_event_into(&mut self.buf) {
                Ok(Event::Start(event)) if event.name().as_ref() == b"name" => {
                    let end = event.name().as_ref().to_vec();
                    self.buf.clear();
                    name = Some(self.read_text_until(&end)?);
                }
                Ok(Event::Start(event)) if event.name().as_ref() == b"value" => {
                    self.buf.clear();
                    value = Some(self.parse_value()?);
                }
                Ok(Event::End(event)) if event.name().as_ref() == b"member" => {
                    self.buf.clear();
                    return Ok(name.zip(value));
                }
                Ok(Event::Eof) => return Err(client_error("unterminated XML-RPC member")),
                Err(error) => return Err(client_error(format!("invalid XML-RPC member: {error}"))),
                _ => {}
            }
            self.buf.clear();
        }
    }

    fn read_typed_string(&mut self, end: &[u8]) -> crate::Result<RtXmlValue> {
        self.read_text_until(end).map(RtXmlValue::String)
    }

    fn read_typed_i64(&mut self, end: &[u8]) -> crate::Result<RtXmlValue> {
        Ok(RtXmlValue::I64(
            self.read_text_until(end)?
                .parse::<i64>()
                .unwrap_or_default(),
        ))
    }

    fn read_typed_bool(&mut self, end: &[u8]) -> crate::Result<RtXmlValue> {
        Ok(RtXmlValue::Bool(matches!(
            self.read_text_until(end)?.as_str(),
            "1" | "true" | "True"
        )))
    }

    fn read_text_until(&mut self, end: &[u8]) -> crate::Result<String> {
        let mut text = String::new();
        loop {
            match self.reader.read_event_into(&mut self.buf) {
                Ok(Event::Text(event)) => {
                    text.push_str(&String::from_utf8_lossy(event.as_ref()));
                }
                Ok(Event::CData(event)) => {
                    text.push_str(&String::from_utf8_lossy(event.as_ref()));
                }
                Ok(Event::End(event)) if event.name().as_ref() == end => {
                    self.buf.clear();
                    return Ok(text);
                }
                Ok(Event::Eof) => return Err(client_error("unterminated XML-RPC text")),
                Err(error) => return Err(client_error(format!("invalid XML-RPC text: {error}"))),
                _ => {}
            }
            self.buf.clear();
        }
    }
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

fn is_qb_auth_error(error: &reqwest::Error) -> bool {
    error.status().is_some_and(|status| {
        status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN
    })
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
mod tests {
    use super::{
        AsyncTorrentClient, ClientErrorCode, ClientTorrent, DelugeClient, DownloadDirOptions,
        InjectionOptions, NewTorrent, QbittorrentClient, ResumeOptions, RtorrentClient,
        TorrentClient, TransmissionClient, client_identities, client_torrent_to_searchee,
        select_injection_client,
    };
    use crate::{
        config::TorrentClientConfig,
        domain::{
            ClientLabel, ClientTorrentMetadata, Decision, File, InfoHash, InjectionResult,
            MediaType, Metafile, Searchee, TorrentClientKind, TorrentClientMetadata,
        },
    };
    use std::{
        borrow::Cow,
        collections::BTreeMap,
        fs,
        io::{Read, Write},
        net::TcpListener,
        path::{Path, PathBuf},
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

        let error = client_identities(&[
            TorrentClientConfig::parse("qbittorrent:http://shared.example/qb").expect("client"),
            TorrentClientConfig::parse("transmission:http://shared.example/qb").expect("client"),
        ])
        .expect_err("duplicate identity");
        assert!(
            error
                .to_string()
                .contains("duplicate torrent client identity")
        );
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

    #[tokio::test]
    async fn async_client_facade_preserves_trait_behavior() {
        let client = FakeClient::new("async", 0, false);
        let info_hash = InfoHash::new("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").expect("hash");

        assert!(
            !client
                .is_torrent_in_client_async(&info_hash)
                .await
                .expect("in client")
        );
        assert!(
            !client
                .is_torrent_complete_async(&info_hash)
                .await
                .expect("complete")
        );
        assert_eq!(TorrentClient::metadata(&client).host.as_ref(), "async");
        client.validate_config_async().await.expect("validate");
    }

    #[test]
    fn qbittorrent_validates_version_and_preferences() {
        let server = http_server(vec![
            http_response("200 OK", "Ok."),
            http_response("200 OK", "v4.6.2"),
            http_response("200 OK", r#"{"save_path":"/downloads"}"#),
            http_response("200 OK", ""),
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
        assert!(
            requests
                .iter()
                .any(|request| request.contains("POST /api/v2/torrents/createTags "))
        );
        assert!(
            requests
                .iter()
                .any(|request| request.contains("tags=cross-seed"))
        );
    }

    #[test]
    fn qbittorrent_rejects_sqlite_resume_data_with_torrent_dir() {
        let root = temp_path("qb-sqlite-resume");
        fs::create_dir_all(&root).expect("torrent dir");
        fs::write(
            root.join("0123456789abcdef0123456789abcdef01234567.fastresume"),
            b"",
        )
        .expect("fastresume");
        let server = http_server(vec![
            http_response("200 OK", "Ok."),
            http_response("200 OK", "v5.0.0"),
            http_response("200 OK", r#"{"resume_data_storage_type":"SQLite"}"#),
        ]);
        let client = qb_client(&server.url).with_torrent_dir(Some(root));

        let error = client.validate_config().expect_err("sqlite rejected");

        assert!(error.to_string().contains("SQLite resume-data mode"));
        let _requests = server.join();
    }

    #[test]
    fn qbittorrent_requires_fastresume_sidecars_with_torrent_dir() {
        let root = temp_path("qb-fastresume-sidecar");
        fs::create_dir_all(&root).expect("torrent dir");
        fs::write(
            root.join("0123456789abcdef0123456789abcdef01234567.torrent"),
            b"",
        )
        .expect("torrent");
        let server = http_server(vec![
            http_response("200 OK", "Ok."),
            http_response("200 OK", "v4.6.2"),
            http_response("200 OK", r#"{"resume_data_storage_type":"Legacy"}"#),
        ]);
        let client = qb_client(&server.url).with_torrent_dir(Some(root));

        let error = client.validate_config().expect_err("missing fastresume");

        assert!(error.to_string().contains("missing a .fastresume sidecar"));
        let _requests = server.join();
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
            http_response(
                "200 OK",
                r#"[{"name":"Example.Show.S01E01.mkv","size":123}]"#,
            ),
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
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.contains("POST /api/v2/auth/login "))
                .count(),
            1
        );
    }

    #[test]
    fn qbittorrent_relogs_after_auth_failure() {
        let hash = "0123456789abcdef0123456789abcdef01234567";
        let server = http_server(vec![
            http_response("200 OK", "Ok."),
            http_response("403 Forbidden", "Forbidden"),
            http_response("200 OK", "Ok."),
            http_response(
                "200 OK",
                &format!(
                    r#"[{{"hash":"{hash}","name":"Example","save_path":"/downloads","progress":1.0,"state":"uploading"}}]"#
                ),
            ),
        ]);
        let client = qb_client(&server.url);
        let metafile = Metafile::from_files(
            InfoHash::new(hash).expect("hash").into_owned(),
            "Example".to_owned(),
            "Example".to_owned(),
            42,
            vec![File::new("Example.mkv", 42)],
        );

        let remaining = client.remaining_bytes(&metafile).expect("remaining");

        assert_eq!(remaining, Some(0));
        let requests = server.join();
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.contains("POST /api/v2/auth/login "))
                .count(),
            2
        );
    }

    #[test]
    fn qbittorrent_retries_transient_info_status() {
        let hash = "0123456789abcdef0123456789abcdef01234567";
        let server = http_server(vec![
            http_response("200 OK", "Ok."),
            http_response("502 Bad Gateway", ""),
            http_response(
                "200 OK",
                &format!(
                    r#"[{{"hash":"{hash}","name":"Example","save_path":"/downloads","progress":0.5,"amount_left":42,"state":"downloading"}}]"#
                ),
            ),
        ]);
        let client = qb_client(&server.url);
        let metafile = Metafile::from_files(
            InfoHash::new(hash).expect("hash").into_owned(),
            "Example".to_owned(),
            "Example".to_owned(),
            42,
            vec![File::new("Example.mkv", 42)],
        );

        let remaining = client.remaining_bytes(&metafile).expect("remaining");

        assert_eq!(remaining, Some(42));
        let requests = server.join();
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.contains("GET /api/v2/torrents/info?hashes="))
                .count(),
            2
        );
    }

    #[test]
    fn qbittorrent_visits_inventory_with_paged_file_backpressure() {
        let hash = "0123456789abcdef0123456789abcdef01234567";
        let server = http_server(vec![
            http_response("200 OK", "Ok."),
            http_response(
                "200 OK",
                &format!(
                    r#"[{{"hash":"{hash}","name":"Example.Show.S01E01","save_path":"/downloads","progress":1.0,"state":"uploading"}}]"#
                ),
            ),
            http_response(
                "200 OK",
                r#"[{"name":"Example.Show.S01E01.mkv","size":123}]"#,
            ),
            http_response("200 OK", r#"[]"#),
        ]);
        let client = qb_client(&server.url);
        let mut seen = 0usize;

        client
            .for_each_torrent(&mut |torrent| {
                assert_eq!(torrent.info_hash.as_str(), hash);
                seen += 1;
                Ok(())
            })
            .expect("inventory");

        assert_eq!(seen, 1);
        let requests = server.join();
        assert!(
            requests.iter().any(|request| {
                request.contains("GET /api/v2/torrents/info?offset=0&limit=1000 ")
            })
        );
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.contains("GET /api/v2/torrents/files?hash="))
                .count(),
            1
        );
    }

    #[test]
    fn qbittorrent_client_searchees_use_paged_inventory() {
        let hash = "0123456789abcdef0123456789abcdef01234567";
        let server = http_server(vec![
            http_response("200 OK", "Ok."),
            http_response(
                "200 OK",
                &format!(
                    r#"[{{"hash":"{hash}","name":"Example.Show.S01E01","save_path":"/downloads","progress":1.0,"state":"uploading"}}]"#
                ),
            ),
            http_response(
                "200 OK",
                r#"[{"name":"Example.Show.S01E01.mkv","size":123}]"#,
            ),
            http_response("200 OK", r#"[]"#),
        ]);
        let client = qb_client(&server.url);

        let result = client.get_client_searchees().expect("searchees");

        assert_eq!(result.searchees.len(), 1);
        assert_eq!(result.skipped, 0);
        assert_eq!(
            result.searchees[0].info_hash.as_ref().map(InfoHash::as_str),
            Some(hash)
        );
        let requests = server.join();
        assert!(
            requests.iter().any(|request| {
                request.contains("GET /api/v2/torrents/info?offset=0&limit=1000 ")
            })
        );
        assert!(
            !requests
                .iter()
                .any(|request| request.contains("GET /api/v2/torrents/info "))
        );
    }

    #[test]
    fn qbittorrent_download_dir_lookup_stops_at_first_match() {
        let hash = "0123456789abcdef0123456789abcdef01234567";
        let server = http_server(vec![
            http_response("200 OK", "Ok."),
            http_response(
                "200 OK",
                &format!(
                    r#"[{{"hash":"{hash}","name":"Example","save_path":"/downloads","progress":1.0,"state":"uploading"}}]"#
                ),
            ),
        ]);
        let client = qb_client(&server.url);
        let mut seen = Vec::new();

        let found = client
            .has_matching_download_dir(&mut |download_dir| {
                seen.push(download_dir.to_path_buf());
                Ok(download_dir == Path::new("/downloads"))
            })
            .expect("lookup");

        assert!(found);
        assert_eq!(seen, vec![PathBuf::from("/downloads")]);
        let requests = server.join();
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.contains("GET /api/v2/torrents/info?offset="))
                .count(),
            1
        );
    }

    #[test]
    fn qbittorrent_remaining_bytes_uses_single_info_lookup() {
        let hash = "0123456789abcdef0123456789abcdef01234567";
        let server = http_server(vec![
            http_response("200 OK", "Ok."),
            http_response(
                "200 OK",
                &format!(
                    r#"[{{"hash":"{hash}","name":"Example","save_path":"/downloads","progress":0.5,"amount_left":42,"state":"downloading"}}]"#
                ),
            ),
        ]);
        let client = qb_client(&server.url);
        let metafile = Metafile::from_files(
            InfoHash::new(hash).expect("hash").into_owned(),
            "Example".to_owned(),
            "Example".to_owned(),
            42,
            vec![File::new("Example.mkv", 42)],
        );

        let remaining = client.remaining_bytes(&metafile).expect("remaining");

        assert_eq!(remaining, Some(42));
        let requests = server.join();
        assert!(requests.iter().any(|request| {
            request.contains(
                "GET /api/v2/torrents/info?hashes=0123456789abcdef0123456789abcdef01234567 ",
            )
        }));
        assert!(
            requests
                .iter()
                .all(|request| !request.contains("/api/v2/torrents/files"))
        );
    }

    #[test]
    fn transmission_remaining_bytes_uses_single_info_lookup() {
        let hash = "0123456789abcdef0123456789abcdef01234567";
        let server = http_server(vec![
            http_response_with_headers("409 Conflict", &[("X-Transmission-Session-Id", "sid")], ""),
            http_response(
                "200 OK",
                &format!(
                    r#"{{"result":"success","arguments":{{"torrents":[{{"hashString":"{hash}","name":"Example","downloadDir":"/downloads","files":[],"trackers":[],"labels":[],"percentDone":0.5,"leftUntilDone":7,"status":4}}]}}}}"#
                ),
            ),
        ]);
        let client = transmission_client(&server.url);
        let metafile = Metafile::from_files(
            InfoHash::new(hash).expect("hash").into_owned(),
            "Example".to_owned(),
            "Example".to_owned(),
            42,
            vec![File::new("Example.mkv", 42)],
        );

        let remaining = client.remaining_bytes(&metafile).expect("remaining");

        assert_eq!(remaining, Some(7));
        let requests = server.join();
        assert_eq!(requests.len(), 2);
        assert!(requests[1].contains(r#""ids":["0123456789abcdef0123456789abcdef01234567"]"#));
        assert!(requests[1].contains("leftUntilDone"));
    }

    #[test]
    fn transmission_download_dir_lookup_stops_at_first_match() {
        let first = "0123456789abcdef0123456789abcdef01234567";
        let second = "89abcdef012345670123456789abcdef01234567";
        let server = http_server(vec![
            http_response_with_headers("409 Conflict", &[("X-Transmission-Session-Id", "sid")], ""),
            http_response(
                "200 OK",
                &format!(
                    r#"{{"result":"success","arguments":{{"torrents":[{{"hashString":"{first}"}},{{"hashString":"{second}"}}]}}}}"#
                ),
            ),
            http_response(
                "200 OK",
                &format!(
                    r#"{{"result":"success","arguments":{{"torrents":[{{"hashString":"{first}","downloadDir":"/match"}}]}}}}"#
                ),
            ),
        ]);
        let client = transmission_client(&server.url);

        let found = client
            .has_matching_download_dir(&mut |download_dir| Ok(download_dir == Path::new("/match")))
            .expect("lookup");

        assert!(found);
        let requests = server.join();
        assert_eq!(requests.len(), 3);
        assert!(requests[2].contains(r#""ids":["0123456789abcdef0123456789abcdef01234567"]"#));
        assert!(
            !requests
                .iter()
                .skip(2)
                .any(|request| request.contains(second))
        );
    }

    #[test]
    fn deluge_remaining_bytes_uses_single_info_lookup() {
        let hash = "0123456789abcdef0123456789abcdef01234567";
        let server = http_server(vec![
            deluge_response("true"),
            deluge_response("true"),
            deluge_response(&format!(
                r#"{{"torrents":{{"{hash}":{{"name":"Example","save_path":"/downloads","files":[],"tracker_host":"","label":"","progress":50.0,"total_remaining":7,"state":"Downloading"}}}}}}"#
            )),
        ]);
        let client = deluge_client(&server.url);
        let metafile = Metafile::from_files(
            InfoHash::new(hash).expect("hash").into_owned(),
            "Example".to_owned(),
            "Example".to_owned(),
            42,
            vec![File::new("Example.mkv", 42)],
        );

        let remaining = client.remaining_bytes(&metafile).expect("remaining");

        assert_eq!(remaining, Some(7));
        let requests = server.join();
        assert_eq!(requests.len(), 3);
        assert!(requests[2].contains(r#""id":["0123456789abcdef0123456789abcdef01234567"]"#));
        assert!(requests[2].contains("total_remaining"));
    }

    #[test]
    fn deluge_download_dir_lookup_stops_at_first_match() {
        let first = "0123456789abcdef0123456789abcdef01234567";
        let second = "89abcdef012345670123456789abcdef01234567";
        let server = http_server(vec![
            deluge_response("true"),
            deluge_response("true"),
            deluge_response(&format!(
                r#"{{"torrents":{{"{first}":{{"hash":"{first}"}},"{second}":{{"hash":"{second}"}}}}}}"#
            )),
            deluge_response("true"),
            deluge_response("true"),
            deluge_response(&format!(
                r#"{{"torrents":{{"{first}":{{"hash":"{first}","save_path":"/match"}}}}}}"#
            )),
        ]);
        let client = deluge_client(&server.url);

        let found = client
            .has_matching_download_dir(&mut |download_dir| Ok(download_dir == Path::new("/match")))
            .expect("lookup");

        assert!(found);
        let requests = server.join();
        assert_eq!(requests.len(), 6);
        assert!(requests[5].contains(r#""id":["0123456789abcdef0123456789abcdef01234567"]"#));
        assert!(
            !requests
                .iter()
                .skip(3)
                .any(|request| request.contains(second))
        );
    }

    #[test]
    fn qbittorrent_contracts_presence_state_and_download_dir() {
        let hash = "0123456789abcdef0123456789abcdef01234567";
        let info = format!(
            r#"[{{"hash":"{hash}","name":"Example.Show.S01E01","save_path":"/downloads/show","progress":0.5,"state":"checkingUP"}}]"#
        );
        let server = http_server(vec![
            http_response("200 OK", "Ok."),
            http_response("200 OK", &info),
            http_response("200 OK", &info),
            http_response("200 OK", &info),
            http_response("200 OK", &info),
            http_response("200 OK", &info),
        ]);
        let client = qb_client(&server.url);
        let metafile = Metafile::from_files(
            InfoHash::from_validated(hash),
            "Example.Show.S01E01",
            "Example.Show.S01E01",
            16_384,
            vec![File::new("Example.Show.S01E01.mkv", 123)],
        );

        assert!(
            client
                .is_torrent_in_client(&metafile.info_hash)
                .expect("present")
        );
        assert!(
            !client
                .is_torrent_complete(&metafile.info_hash)
                .expect("complete")
        );
        assert!(
            client
                .is_torrent_checking(&metafile.info_hash)
                .expect("checking")
        );
        assert_eq!(
            client
                .get_download_dir(
                    &metafile,
                    DownloadDirOptions {
                        only_completed: true,
                    },
                )
                .expect("download dir"),
            Err(ClientErrorCode::TorrentNotComplete)
        );
        assert_eq!(
            client
                .get_download_dir(
                    &metafile,
                    DownloadDirOptions {
                        only_completed: false,
                    },
                )
                .expect("download dir")
                .expect("path"),
            PathBuf::from("/downloads/show")
        );

        let requests = server.join();
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.contains("GET /api/v2/torrents/info?hashes="))
                .count(),
            5
        );
    }

    #[test]
    fn qbittorrent_injects_with_multipart_add_and_starts() {
        let server = http_server(vec![
            http_response("200 OK", "Ok."),
            http_response("200 OK", ""),
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
                    duplicate_categories: false,
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

    #[test]
    fn qbittorrent_injects_duplicate_source_category() {
        let server = http_server(vec![
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
        let mut searchee = Searchee::from_files("Inject.Release", "Inject.Release", Vec::new());
        searchee.client = Some(ClientTorrentMetadata::new(
            "client",
            "/downloads",
            Some(ClientLabel::new("movies")),
            Vec::new(),
            Vec::new(),
        ));

        client
            .inject(
                &new_torrent,
                &searchee,
                Decision::Match,
                &InjectionOptions {
                    destination_dir: None,
                    category: None,
                    tags: Vec::new(),
                    duplicate_categories: true,
                    paused: false,
                    skip_checking: true,
                },
            )
            .expect("inject");

        let requests = server.join();
        let add = requests
            .iter()
            .find(|request| request.contains("POST /api/v2/torrents/add "))
            .expect("add request");
        assert!(add.contains("movies.cross-seed"));
    }

    #[test]
    fn transmission_negotiates_session_and_maps_inventory() {
        let hash = "0123456789abcdef0123456789abcdef01234567";
        let body = format!(
            r#"{{"result":"success","arguments":{{"torrents":[{{"hashString":"{hash}","name":"Example.Show.S01E01","downloadDir":"/downloads","files":[{{"name":"Example.Show.S01E01.mkv","length":123}}],"trackers":[{{"announce":"https://tracker.example/announce"}}],"labels":["tv","cross-seed"],"percentDone":1.0,"status":6}}]}}}}"#
        );
        let server = http_server(vec![
            http_response_with_headers("409 Conflict", &[("X-Transmission-Session-Id", "sid")], ""),
            http_response("200 OK", &body),
            http_response("200 OK", &body),
        ]);
        let client = transmission_client(&server.url);

        let torrents = client.get_all_torrents().expect("inventory");

        assert_eq!(torrents.len(), 1);
        assert_eq!(torrents[0].info_hash.as_str(), hash);
        assert_eq!(torrents[0].save_path, "/downloads");
        assert_eq!(torrents[0].files[0].path, "Example.Show.S01E01.mkv");
        assert_eq!(torrents[0].tags.len(), 2);
        assert_eq!(torrents[0].trackers[0], "tracker.example");
        assert!(torrents[0].complete);
        assert!(!torrents[0].checking);
        let requests = server.join();
        assert_eq!(requests.len(), 3);
        assert!(!requests[0].contains("X-Transmission-Session-Id"));
        assert!(
            requests[1]
                .to_ascii_lowercase()
                .contains("x-transmission-session-id: sid")
        );
        assert!(requests[1].contains(r#""method":"torrent-get""#));
    }

    #[test]
    fn transmission_retries_transient_reads() {
        let hash = "0123456789abcdef0123456789abcdef01234567";
        let body = format!(
            r#"{{"result":"success","arguments":{{"torrents":[{{"hashString":"{hash}","name":"Example","downloadDir":"/downloads","files":[],"trackers":[],"labels":[],"percentDone":1.0,"status":6}}]}}}}"#
        );
        let server = http_server(vec![
            http_response("502 Bad Gateway", ""),
            http_response_with_headers("409 Conflict", &[("X-Transmission-Session-Id", "sid")], ""),
            http_response("200 OK", &body),
            http_response("200 OK", &body),
        ]);
        let client = transmission_client(&server.url);

        let torrents = client.get_all_torrents().expect("inventory");

        assert_eq!(torrents.len(), 1);
        let requests = server.join();
        assert_eq!(requests.len(), 4);
    }

    #[test]
    fn transmission_injects_and_starts() {
        let server = http_server(vec![
            http_response("200 OK", r#"{"result":"success","arguments":{}}"#),
            http_response("200 OK", r#"{"result":"success","arguments":{}}"#),
            http_response("200 OK", r#"{"result":"success","arguments":{}}"#),
            http_response("200 OK", r#"{"result":"success","arguments":{}}"#),
        ]);
        let client = transmission_client(&server.url);
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
                    duplicate_categories: false,
                    paused: true,
                    skip_checking: true,
                },
            )
            .expect("inject");
        client
            .recheck_torrent(&new_torrent.metafile.info_hash)
            .expect("recheck");
        client
            .resume_injection(
                &new_torrent.metafile,
                Decision::Match,
                ResumeOptions::default(),
            )
            .expect("resume");

        assert_eq!(result, InjectionResult::Injected);
        let requests = server.join();
        assert_eq!(requests.len(), 4);
        assert!(requests[0].contains(r#""method":"torrent-add""#));
        assert!(requests[0].contains(r#""download-dir":"/linked""#));
        assert!(requests[0].contains(r#""labels":["tv","cross-seed"]"#));
        assert!(requests[0].contains(r#""paused":true"#));
        assert!(requests[1].contains(r#""method":"torrent-stop""#));
        assert!(requests[2].contains(r#""method":"torrent-verify""#));
        assert!(requests[3].contains(r#""method":"torrent-start""#));
    }

    #[test]
    fn deluge_connects_and_maps_inventory() {
        let hash = "0123456789abcdef0123456789abcdef01234567";
        let body = format!(
            r#"{{"torrents":{{"{hash}":{{"name":"Example.Show.S01E01","save_path":"/downloads","files":[{{"path":"Example.Show.S01E01.mkv","size":123}}],"tracker_host":"tracker.example","label":"tv","progress":100.0,"state":"Seeding"}}}}}}"#
        );
        let server = http_server(vec![
            deluge_response("true"),
            deluge_response("true"),
            deluge_response(&body),
            deluge_response("true"),
            deluge_response("true"),
            deluge_response(&body),
        ]);
        let client = deluge_client(&server.url);

        let torrents = client.get_all_torrents().expect("inventory");

        assert_eq!(torrents.len(), 1);
        assert_eq!(torrents[0].info_hash.as_str(), hash);
        assert_eq!(torrents[0].save_path, "/downloads");
        assert_eq!(torrents[0].files[0].path, "Example.Show.S01E01.mkv");
        assert_eq!(
            torrents[0].category.as_ref().map(ClientLabel::as_str),
            Some("tv")
        );
        assert_eq!(torrents[0].trackers[0], "tracker.example");
        assert!(torrents[0].complete);
        let requests = server.join();
        assert_eq!(requests.len(), 6);
        assert!(requests[0].contains(r#""method":"auth.login""#));
        assert!(requests[1].contains(r#""method":"web.connected""#));
        assert!(requests[2].contains(r#""method":"web.update_ui""#));
    }

    #[test]
    fn deluge_retries_transient_reads() {
        let hash = "0123456789abcdef0123456789abcdef01234567";
        let body = format!(
            r#"{{"torrents":{{"{hash}":{{"name":"Example","save_path":"/downloads","files":[],"tracker_host":"","label":"","progress":100.0,"state":"Seeding"}}}}}}"#
        );
        let server = http_server(vec![
            http_response("503 Service Unavailable", ""),
            deluge_response("true"),
            deluge_response("true"),
            deluge_response(&body),
            deluge_response("true"),
            deluge_response("true"),
            deluge_response(&body),
        ]);
        let client = deluge_client(&server.url);

        let torrents = client.get_all_torrents().expect("inventory");

        assert_eq!(torrents.len(), 1);
        let requests = server.join();
        assert_eq!(requests.len(), 7);
    }

    #[test]
    fn deluge_injects_labels_rechecks_and_resumes() {
        let server = http_server(vec![
            deluge_response("true"),
            deluge_response("true"),
            deluge_response("true"),
            deluge_response("[]"),
            deluge_response("true"),
            deluge_response("true"),
            deluge_response("true"),
            deluge_response("true"),
            deluge_response("true"),
            deluge_response("true"),
            deluge_response("true"),
            deluge_response("true"),
            deluge_response("true"),
        ]);
        let client = deluge_client(&server.url);
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
                    duplicate_categories: false,
                    paused: true,
                    skip_checking: true,
                },
            )
            .expect("inject");
        client
            .recheck_torrent(&new_torrent.metafile.info_hash)
            .expect("recheck");
        client
            .resume_injection(
                &new_torrent.metafile,
                Decision::Match,
                ResumeOptions::default(),
            )
            .expect("resume");

        assert_eq!(result, InjectionResult::Injected);
        let requests = server.join();
        assert_eq!(requests.len(), 13);
        assert!(requests[2].contains(r#""method":"core.add_torrent_file""#));
        assert!(requests[2].contains(r#""download_location":"/linked""#));
        assert!(requests[3].contains(r#""method":"label.get_labels""#));
        assert!(requests[4].contains(r#""method":"label.add""#));
        assert!(requests[5].contains(r#""method":"label.set_torrent""#));
        assert!(requests[6].contains(r#""method":"core.pause_torrent""#));
        assert!(requests[9].contains(r#""method":"core.force_recheck""#));
        assert!(requests[12].contains(r#""method":"core.resume_torrent""#));
    }

    #[test]
    fn deluge_injects_duplicate_source_category_label() {
        let server = http_server(vec![
            deluge_response("true"),
            deluge_response("true"),
            deluge_response("true"),
            deluge_response("[]"),
            deluge_response("true"),
            deluge_response("true"),
        ]);
        let client = deluge_client(&server.url);
        let bytes = torrent_bytes("Inject.Release", 10);
        let metafile = crate::torrent::parse_metafile(&bytes).expect("metafile");
        let new_torrent = NewTorrent {
            metafile,
            bytes: Cow::Owned(bytes),
        };
        let mut searchee = Searchee::from_files("Inject.Release", "Inject.Release", Vec::new());
        searchee.client = Some(ClientTorrentMetadata::new(
            "client",
            "/downloads",
            Some(ClientLabel::new("movies")),
            Vec::new(),
            Vec::new(),
        ));

        client
            .inject(
                &new_torrent,
                &searchee,
                Decision::Match,
                &InjectionOptions {
                    destination_dir: None,
                    category: None,
                    tags: Vec::new(),
                    duplicate_categories: true,
                    paused: false,
                    skip_checking: true,
                },
            )
            .expect("inject");

        let requests = server.join();
        assert!(requests[4].contains("movies.cross-seed"));
        assert!(requests[5].contains("movies.cross-seed"));
    }

    #[test]
    fn rtorrent_maps_inventory_files_and_trackers() {
        let hash = "0123456789abcdef0123456789abcdef01234567";
        let server = http_server(vec![
            rt_response(&rt_array(&[rt_string(hash)])),
            rt_response(&rt_array(&[
                rt_array(&[rt_string("Example.Show.S01E01")]),
                rt_array(&[rt_string("/downloads")]),
                rt_array(&[rt_int(0)]),
                rt_array(&[rt_bool(false)]),
                rt_array(&[rt_bool(true)]),
                rt_array(&[rt_bool(false)]),
                rt_array(&[rt_bool(false)]),
                rt_array(&[rt_string("cross-seed")]),
                rt_array(&[rt_array(&[rt_array(&[
                    rt_string("Example.Show.S01E01.mkv"),
                    rt_int(123),
                ])])]),
                rt_array(&[rt_array(&[rt_array(&[
                    rt_string("https://tracker.example/announce"),
                    rt_string("tracker-group"),
                ])])]),
            ])),
        ]);
        let client = rtorrent_client(&server.url);

        let torrents = client.get_all_torrents().expect("inventory");

        assert_eq!(torrents.len(), 1);
        assert_eq!(torrents[0].info_hash.as_str(), hash);
        assert_eq!(torrents[0].save_path, "/downloads");
        assert_eq!(torrents[0].files[0].path, "Example.Show.S01E01.mkv");
        assert_eq!(torrents[0].tags[0].as_str(), "cross-seed");
        assert_eq!(torrents[0].trackers[0], "tracker.example");
        assert!(torrents[0].complete);
        let requests = server.join();
        assert_eq!(requests.len(), 2);
        assert!(requests[0].contains("<methodName>download_list</methodName>"));
        assert!(requests[1].contains("<methodName>system.multicall</methodName>"));
        assert!(requests[1].contains("f.multicall"));
        assert!(requests[1].contains("t.multicall"));
    }

    #[test]
    fn rtorrent_retries_transient_reads() {
        let hash = "0123456789abcdef0123456789abcdef01234567";
        let server = http_server(vec![
            http_response("502 Bad Gateway", ""),
            rt_response(&rt_array(&[rt_string(hash)])),
            rt_response(&rt_array(&[
                rt_array(&[rt_string("Example")]),
                rt_array(&[rt_string("/downloads")]),
                rt_array(&[rt_int(0)]),
                rt_array(&[rt_bool(false)]),
                rt_array(&[rt_bool(true)]),
                rt_array(&[rt_bool(false)]),
                rt_array(&[rt_bool(false)]),
                rt_array(&[rt_string("cross-seed")]),
                rt_array(&[rt_array(&[])]),
                rt_array(&[rt_array(&[])]),
            ])),
        ]);
        let client = rtorrent_client(&server.url);

        let torrents = client.get_all_torrents().expect("inventory");

        assert_eq!(torrents.len(), 1);
        let requests = server.join();
        assert_eq!(requests.len(), 3);
    }

    #[test]
    fn rtorrent_injects_labels_rechecks_and_resumes() {
        let server = http_server(vec![
            rt_response(&rt_string("")),
            rt_response(&rt_bool(true)),
            rt_response(&rt_bool(true)),
            rt_response(&rt_bool(true)),
            rt_response(&rt_bool(true)),
        ]);
        let client = rtorrent_client(&server.url);
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
                    category: None,
                    tags: vec![ClientLabel::new("cross-seed")],
                    duplicate_categories: false,
                    paused: true,
                    skip_checking: true,
                },
            )
            .expect("inject");
        client
            .recheck_torrent(&new_torrent.metafile.info_hash)
            .expect("recheck");
        client
            .resume_injection(
                &new_torrent.metafile,
                Decision::Match,
                ResumeOptions::default(),
            )
            .expect("resume");

        assert_eq!(result, InjectionResult::Injected);
        let requests = server.join();
        assert_eq!(requests.len(), 5);
        assert!(requests[0].contains("<methodName>load.raw</methodName>"));
        assert!(requests[0].contains("<base64>"));
        assert!(requests[0].contains("d.directory.set=/linked"));
        assert!(requests[1].contains("<methodName>d.custom1.set</methodName>"));
        assert!(requests[2].contains("<methodName>d.pause</methodName>"));
        assert!(requests[3].contains("<methodName>d.check_hash</methodName>"));
        assert!(requests[4].contains("<methodName>d.resume</methodName>"));
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

    fn temp_path(name: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "sporos-clients-{name}-{}-{nanos}",
            std::process::id(),
        ))
    }

    fn transmission_client(base_url: &str) -> TransmissionClient {
        let identity =
            client_identities(&[
                TorrentClientConfig::parse(&format!("transmission:{base_url}")).expect("config"),
            ])
            .expect("identity")
            .into_iter()
            .next()
            .expect("identity");
        TransmissionClient::new(identity, Some(Duration::from_secs(1))).expect("client")
    }

    fn deluge_client(base_url: &str) -> DelugeClient {
        let identity =
            client_identities(&[
                TorrentClientConfig::parse(&format!("deluge:{base_url}")).expect("config")
            ])
            .expect("identity")
            .into_iter()
            .next()
            .expect("identity");
        DelugeClient::new(identity, Some(Duration::from_secs(1))).expect("client")
    }

    fn rtorrent_client(base_url: &str) -> RtorrentClient {
        let identity =
            client_identities(&[
                TorrentClientConfig::parse(&format!("rtorrent:{base_url}")).expect("config")
            ])
            .expect("identity")
            .into_iter()
            .next()
            .expect("identity");
        RtorrentClient::new(identity, Some(Duration::from_secs(1))).expect("client")
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
        http_response_with_headers(status, &[], body)
    }

    fn http_response_with_headers(status: &str, headers: &[(&str, &str)], body: &str) -> String {
        let extra_headers = headers
            .iter()
            .map(|(name, value)| format!("{name}: {value}\r\n"))
            .collect::<String>();
        format!(
            "HTTP/1.1 {status}\r\n{extra_headers}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
    }

    fn deluge_response(result: &str) -> String {
        http_response(
            "200 OK",
            &format!(r#"{{"result":{result},"error":null,"id":1}}"#),
        )
    }

    fn rt_response(value: &str) -> String {
        http_response(
            "200 OK",
            &format!(
                "<?xml version=\"1.0\"?><methodResponse><params><param><value>{value}</value></param></params></methodResponse>"
            ),
        )
    }

    fn rt_string(value: &str) -> String {
        format!("<string>{value}</string>")
    }

    fn rt_int(value: i64) -> String {
        format!("<i8>{value}</i8>")
    }

    fn rt_bool(value: bool) -> String {
        format!("<boolean>{}</boolean>", i64::from(value))
    }

    fn rt_array(values: &[String]) -> String {
        let values = values
            .iter()
            .map(|value| format!("<value>{value}</value>"))
            .collect::<String>();
        format!("<array><data>{values}</data></array>")
    }

    fn torrent_bytes(name: &str, length: u64) -> Vec<u8> {
        format!(
            "d4:infod6:lengthi{length}e4:name{}:{name}12:piece lengthi1e6:pieces20:aaaaaaaaaaaaaaaaaaaaee",
            name.len()
        )
        .into_bytes()
    }
}
