use std::{
    borrow::Cow,
    collections::BTreeMap,
    future::Future,
    path::{Path, PathBuf},
    time::Duration,
};

use reqwest::header::CONTENT_TYPE;

use super::{
    ClientErrorCode, ClientIdentity, ClientTorrent, DownloadDirOptions, InjectionOptions,
    NewTorrent, ResumeOptions, TorrentClient, base64_encode, block_on_client,
    block_on_client_delay, client_error, client_error_retryable, confirm_injection,
    ensure_writable, resume_with_policy, tracker_host,
};
use crate::{
    domain::{
        ClientLabel, Decision, File, InfoHash, InjectionResult, Metafile, Searchee,
        TorrentClientMetadata,
    },
    retry::RetryPolicy,
};

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
        let result = confirm_injection(self, &new_torrent.metafile.info_hash)?;
        if result != InjectionResult::Injected {
            return Ok(result);
        }
        if options.paused {
            self.torrent_action("torrent-stop", &new_torrent.metafile.info_hash)?;
        }
        Ok(result)
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
