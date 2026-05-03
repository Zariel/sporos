use std::{
    borrow::Cow,
    collections::BTreeMap,
    future::Future,
    path::{Path, PathBuf},
    time::Duration,
};

use reqwest::header::CONTENT_TYPE;
use url::Url;

use super::{
    ClientErrorCode, ClientIdentity, ClientTorrent, DownloadDirOptions, InjectionOptions,
    NewTorrent, ResumeOptions, TorrentClient, base64_encode, block_on_client,
    block_on_client_delay, client_error, client_error_retryable, confirm_injection,
    ensure_writable, primary_client_label, resume_with_policy,
};
use crate::{
    domain::{
        ClientLabel, Decision, File, InfoHash, InjectionResult, Metafile, Searchee,
        TorrentClientMetadata,
    },
    retry::RetryPolicy,
};

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
        let result = confirm_injection(self, &new_torrent.metafile.info_hash)?;
        if result != InjectionResult::Injected {
            return Ok(result);
        }

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
        Ok(result)
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
