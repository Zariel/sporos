use std::{
    borrow::Cow,
    collections::BTreeMap,
    fs,
    future::Future,
    path::{Path, PathBuf},
    sync::Mutex,
    time::Duration,
};

use reqwest::multipart;
use url::Url;

use super::{
    CLIENT_INVENTORY_PAGE_SIZE, ClientErrorCode, ClientIdentity, ClientSearcheeResult,
    ClientTorrent, DownloadDirOptions, InjectionOptions, NewTorrent,
    QB_TORRENT_FILES_CONCURRENCY_LIMIT, ResumeOptions, TorrentClient, block_on_client,
    block_on_client_delay, client_error, client_error_retryable, client_torrent_to_searchee,
    confirm_injection, ensure_writable, qbit_category_and_tags, resume_with_policy, tracker_host,
};
use crate::{
    domain::{
        ClientLabel, Decision, File, InfoHash, InjectionResult, Metafile, Searchee,
        TorrentClientMetadata,
    },
    retry::RetryPolicy,
};

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
        confirm_injection(self, &new_torrent.metafile.info_hash)
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

fn is_qb_auth_error(error: &reqwest::Error) -> bool {
    error.status().is_some_and(|status| {
        status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN
    })
}
