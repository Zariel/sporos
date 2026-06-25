use std::collections::BTreeMap;
use std::error::Error as StdError;
use std::fmt;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use quick_xml::Reader;
use quick_xml::escape::{escape, resolve_predefined_entity, unescape};
use quick_xml::events::{BytesCData, BytesRef, BytesText, Event};
use quick_xml::name::QName;
use reqwest::StatusCode;
use reqwest::header::CONTENT_TYPE;
use reqwest::redirect::Policy;

use crate::domain::{ByteSize, DisplayName, FileIndex, InfoHash, TorrentFile};
use crate::errors::TorrentClientError;
use crate::runtime::backoff::{RetryDecision, RetryErrorKind, retry_transient_io};
use crate::secrets::sanitize_url_for_logging;

const RTORRENT_RESPONSE_MAX_BYTES: u64 = 64 * 1024 * 1024;
const INVENTORY_METHODS: &[&str] = &[
    "d.name",
    "d.directory",
    "d.left_bytes",
    "d.hashing",
    "d.complete",
    "d.is_multi_file",
    "d.is_active",
    "d.custom1",
];
const RTORRENT_INVENTORY_CHUNK_SIZE: usize = 256;

#[derive(Clone)]
pub struct RtorrentClient {
    client_name: String,
    endpoint: String,
    timeout: Duration,
    client: reqwest::Client,
}

impl fmt::Debug for RtorrentClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RtorrentClient")
            .field("client_name", &self.client_name)
            .field("endpoint", &sanitize_url_for_logging(&self.endpoint))
            .field("timeout", &self.timeout)
            .field("client", &"[REDACTED]")
            .finish()
    }
}

impl RtorrentClient {
    pub fn new(
        client_name: impl Into<String>,
        endpoint: impl Into<String>,
        timeout: Duration,
    ) -> Self {
        let client_name = client_name.into();
        Self {
            client_name: client_name.clone(),
            endpoint: endpoint.into(),
            timeout,
            client: torrent_client_http_client(),
        }
    }

    pub async fn validate(&self) -> Result<(), TorrentClientError> {
        self.call_read("download_list", Vec::new())
            .await
            .map(|_| ())
    }

    pub async fn list_inventory(&self) -> Result<Vec<RtorrentDownload>, TorrentClientError> {
        let response = self.call_text_read("download_list", Vec::new()).await?;
        let mut hash_chunks =
            DownloadHashChunks::new(&self.client_name, &response, RTORRENT_INVENTORY_CHUNK_SIZE);
        let mut downloads = Vec::new();
        while let Some(chunk) = hash_chunks.next_chunk()? {
            let response = self
                .call_read("system.multicall", vec![inventory_multicall_param(&chunk)])
                .await?;
            downloads.extend(parse_inventory_response(
                &self.client_name,
                &chunk,
                &response,
            )?);
        }
        Ok(downloads)
    }

    pub async fn list_inventory_chunks<F, Fut>(
        &self,
        mut on_chunk: F,
    ) -> Result<usize, TorrentClientError>
    where
        F: FnMut(Vec<RtorrentDownload>) -> Fut,
        Fut: Future<Output = Result<(), TorrentClientError>>,
    {
        let response = self.call_text_read("download_list", Vec::new()).await?;
        let mut hash_chunks =
            DownloadHashChunks::new(&self.client_name, &response, RTORRENT_INVENTORY_CHUNK_SIZE);
        let mut total = 0usize;
        while let Some(chunk) = hash_chunks.next_chunk()? {
            let response = self
                .call_read("system.multicall", vec![inventory_multicall_param(&chunk)])
                .await?;
            let downloads = parse_inventory_response(&self.client_name, &chunk, &response)?;
            let chunk_len = downloads.len();
            total = total.saturating_add(chunk_len);
            if chunk_len > 0 {
                on_chunk(downloads).await?;
            }
        }
        Ok(total)
    }

    pub async fn list_inventory_chunks_until_shutdown<F, Fut, C, CFut>(
        &self,
        mut cancelled: C,
        mut on_chunk: F,
    ) -> Result<usize, TorrentClientError>
    where
        F: FnMut(Vec<RtorrentDownload>) -> Fut,
        Fut: Future<Output = Result<(), TorrentClientError>>,
        C: FnMut() -> CFut,
        CFut: Future<Output = ()>,
    {
        let response = tokio::select! {
            biased;
            () = cancelled() => return Err(cancelled_error(&self.client_name)),
            response = self.call_text_read("download_list", Vec::new()) => response?,
        };
        let mut hash_chunks =
            DownloadHashChunks::new(&self.client_name, &response, RTORRENT_INVENTORY_CHUNK_SIZE);
        let mut total = 0usize;
        while let Some(chunk) = hash_chunks.next_chunk()? {
            let response = tokio::select! {
                biased;
                () = cancelled() => return Err(cancelled_error(&self.client_name)),
                response = self.call_read("system.multicall", vec![inventory_multicall_param(&chunk)]) => response?,
            };
            let downloads = parse_inventory_response(&self.client_name, &chunk, &response)?;
            let chunk_len = downloads.len();
            total = total.saturating_add(chunk_len);
            if chunk_len > 0 {
                tokio::select! {
                    biased;
                    () = cancelled() => return Err(cancelled_error(&self.client_name)),
                    result = on_chunk(downloads) => result?,
                }
            }
        }
        Ok(total)
    }

    pub async fn download_info(
        &self,
        info_hash: &InfoHash,
    ) -> Result<Option<RtorrentDownload>, TorrentClientError> {
        let response = match self
            .call_read(
                "system.multicall",
                vec![inventory_multicall_param(std::slice::from_ref(info_hash))],
            )
            .await
        {
            Ok(response) => response,
            Err(error) if is_missing_download_error(&error) => return Ok(None),
            Err(error) => return Err(error),
        };
        parse_optional_inventory_response(&self.client_name, info_hash, &response)
    }

    pub async fn fetch_files(
        &self,
        info_hash: &InfoHash,
    ) -> Result<Vec<TorrentFile>, TorrentClientError> {
        let response = self
            .call_read(
                "f.multicall",
                vec![
                    XmlRpcValue::String(info_hash.as_str().to_owned()),
                    XmlRpcValue::String(String::new()),
                    XmlRpcValue::String("f.path=".to_owned()),
                    XmlRpcValue::String("f.size_bytes=".to_owned()),
                ],
            )
            .await?;
        parse_files_response(&self.client_name, &response)
    }

    pub async fn fetch_files_until_shutdown<C, CFut>(
        &self,
        info_hash: &InfoHash,
        mut cancelled: C,
    ) -> Result<Vec<TorrentFile>, TorrentClientError>
    where
        C: FnMut() -> CFut,
        CFut: Future<Output = ()>,
    {
        tokio::select! {
            biased;
            () = cancelled() => Err(cancelled_error(&self.client_name)),
            files = self.fetch_files(info_hash) => files,
        }
    }

    pub async fn fetch_trackers(
        &self,
        info_hash: &InfoHash,
    ) -> Result<Vec<RtorrentTracker>, TorrentClientError> {
        let response = self
            .call_read(
                "t.multicall",
                vec![
                    XmlRpcValue::String(info_hash.as_str().to_owned()),
                    XmlRpcValue::String(String::new()),
                    XmlRpcValue::String("t.url=".to_owned()),
                    XmlRpcValue::String("t.group=".to_owned()),
                ],
            )
            .await?;
        parse_trackers_response(&self.client_name, &response)
    }

    pub async fn inject(
        &self,
        torrent_bytes: &[u8],
        save_path: Option<&Path>,
        label: &str,
        start: bool,
    ) -> Result<(), TorrentClientError> {
        let method = if start { "load.raw_start" } else { "load.raw" };
        self.call(method, injection_params(torrent_bytes, save_path, label))
            .await
            .map(|_| ())
    }

    pub async fn set_label(
        &self,
        info_hash: &InfoHash,
        label: &str,
    ) -> Result<(), TorrentClientError> {
        retry_transient_io(
            "d.custom1.set",
            |_attempt| async {
                self.call(
                    "d.custom1.set",
                    vec![
                        XmlRpcValue::String(info_hash.as_str().to_owned()),
                        XmlRpcValue::String(label.to_owned()),
                    ],
                )
                .await
            },
            classify_rtorrent_idempotent_error,
        )
        .await
        .map(|_| ())
    }

    pub async fn recheck(&self, info_hash: &InfoHash) -> Result<(), TorrentClientError> {
        retry_transient_io(
            "d.check_hash",
            |_attempt| async {
                self.call(
                    "d.check_hash",
                    vec![XmlRpcValue::String(info_hash.as_str().to_owned())],
                )
                .await
            },
            classify_rtorrent_idempotent_error,
        )
        .await
        .map(|_| ())
    }

    pub async fn resume(&self, info_hash: &InfoHash) -> Result<(), TorrentClientError> {
        retry_transient_io(
            "d.resume",
            |_attempt| async {
                self.call(
                    "d.resume",
                    vec![XmlRpcValue::String(info_hash.as_str().to_owned())],
                )
                .await
            },
            classify_rtorrent_idempotent_error,
        )
        .await
        .map(|_| ())
    }

    pub async fn pause(&self, info_hash: &InfoHash) -> Result<(), TorrentClientError> {
        retry_transient_io(
            "d.pause",
            |_attempt| async {
                self.call(
                    "d.pause",
                    vec![XmlRpcValue::String(info_hash.as_str().to_owned())],
                )
                .await
            },
            classify_rtorrent_idempotent_error,
        )
        .await
        .map(|_| ())
    }

    async fn call(
        &self,
        method: &str,
        params: Vec<XmlRpcValue>,
    ) -> Result<XmlRpcValue, TorrentClientError> {
        let body = self.call_text(method, params).await?;
        parse_method_response(&self.client_name, &body)
    }

    async fn call_read(
        &self,
        method: &str,
        params: Vec<XmlRpcValue>,
    ) -> Result<XmlRpcValue, TorrentClientError> {
        let body = self.call_text_read(method, params).await?;
        parse_method_response(&self.client_name, &body)
    }

    async fn call_text_read(
        &self,
        method: &str,
        params: Vec<XmlRpcValue>,
    ) -> Result<String, TorrentClientError> {
        retry_transient_io(
            method,
            |_attempt| async { self.call_text(method, params.clone()).await },
            classify_rtorrent_idempotent_error,
        )
        .await
    }

    async fn call_text(
        &self,
        method: &str,
        params: Vec<XmlRpcValue>,
    ) -> Result<String, TorrentClientError> {
        let response = self
            .client
            .post(&self.endpoint)
            .timeout(self.timeout)
            .header(CONTENT_TYPE, "text/xml")
            .body(build_method_call(method, &params))
            .send()
            .await
            .map_err(|error| unavailable(&self.client_name, request_error_message(error)))?;

        if response.status() == StatusCode::UNAUTHORIZED {
            return Err(TorrentClientError::Unauthorized {
                client: self.client_name.clone(),
            });
        }
        if !response.status().is_success() {
            return Err(unavailable(
                &self.client_name,
                format!("HTTP {}", response.status()),
            ));
        }

        read_client_text(response, &self.client_name, RTORRENT_RESPONSE_MAX_BYTES).await
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RtorrentDownload {
    pub info_hash: InfoHash,
    pub name: DisplayName,
    pub directory: PathBuf,
    pub left_bytes: ByteSize,
    pub hashing: bool,
    pub complete: bool,
    pub is_multi_file: bool,
    pub is_active: bool,
    pub label: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RtorrentTracker {
    pub url: String,
    pub group: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum XmlRpcValue {
    String(String),
    Int(i64),
    Bool(bool),
    Base64(Vec<u8>),
    Array(Vec<XmlRpcValue>),
    Struct(BTreeMap<String, XmlRpcValue>),
    Nil,
}

impl XmlRpcValue {
    fn as_array<'a>(
        &'a self,
        client: &str,
        field: &str,
    ) -> Result<&'a [XmlRpcValue], TorrentClientError> {
        match self {
            Self::Array(values) => Ok(values),
            _ => Err(bad_response(client, format!("{field} is not an array"))),
        }
    }

    fn as_string<'a>(&'a self, client: &str, field: &str) -> Result<&'a str, TorrentClientError> {
        match self {
            Self::String(value) => Ok(value),
            _ => Err(bad_response(client, format!("{field} is not a string"))),
        }
    }

    fn as_i64(&self, client: &str, field: &str) -> Result<i64, TorrentClientError> {
        match self {
            Self::Int(value) => Ok(*value),
            Self::Bool(value) => Ok(i64::from(*value)),
            _ => Err(bad_response(client, format!("{field} is not an integer"))),
        }
    }

    fn as_bool(&self, client: &str, field: &str) -> Result<bool, TorrentClientError> {
        Ok(self.as_i64(client, field)? != 0)
    }
}

pub fn build_method_call(method: &str, params: &[XmlRpcValue]) -> String {
    let mut xml = String::from(r#"<?xml version="1.0"?><methodCall><methodName>"#);
    xml.push_str(&escape(method));
    xml.push_str("</methodName><params>");
    for param in params {
        xml.push_str("<param>");
        push_value(&mut xml, param);
        xml.push_str("</param>");
    }
    xml.push_str("</params></methodCall>");
    xml
}

pub fn build_inventory_multicall(hashes: &[InfoHash]) -> String {
    build_method_call("system.multicall", &[inventory_multicall_param(hashes)])
}

pub fn build_injection_call(
    torrent_bytes: &[u8],
    save_path: Option<&Path>,
    label: &str,
    start: bool,
) -> String {
    let method = if start { "load.raw_start" } else { "load.raw" };
    build_method_call(method, &injection_params(torrent_bytes, save_path, label))
}

pub fn parse_method_response(client: &str, xml: &str) -> Result<XmlRpcValue, TorrentClientError> {
    let mut reader = Reader::from_str(xml);

    loop {
        match reader.read_event() {
            Ok(Event::Start(start)) if start.name() == QName(b"value") => {
                return parse_value(&mut reader);
            }
            Ok(Event::Start(start)) if start.name() == QName(b"fault") => {
                let fault = parse_fault(&mut reader)?;
                return Err(fault_error(client, &fault));
            }
            Ok(Event::Eof) => return Err(bad_response(client, "missing XML-RPC value")),
            Ok(_) => {}
            Err(error) => return Err(bad_response(client, error.to_string())),
        }
    }
}

struct DownloadHashChunks<'a> {
    client: &'a str,
    reader: Reader<&'a [u8]>,
    chunk_size: usize,
    in_array: bool,
    in_array_data: bool,
    in_hash_value: bool,
    typed_value: bool,
    finished: bool,
}

impl<'a> DownloadHashChunks<'a> {
    fn new(client: &'a str, xml: &'a str, chunk_size: usize) -> Self {
        // rTorrent exposes download_list and d.multicall2 over a whole view,
        // not offset ranges. Keep the unavoidable XML-RPC response body as the
        // memory floor, but avoid retaining the full parsed hash list.
        Self {
            client,
            reader: Reader::from_str(xml),
            chunk_size: chunk_size.max(1),
            in_array: false,
            in_array_data: false,
            in_hash_value: false,
            typed_value: false,
            finished: false,
        }
    }

    fn next_chunk(&mut self) -> Result<Option<Vec<InfoHash>>, TorrentClientError> {
        if self.finished {
            return Ok(None);
        }

        let mut chunk = Vec::with_capacity(self.chunk_size);
        loop {
            match self.reader.read_event() {
                Ok(Event::Start(start)) if start.name() == QName(b"fault") => {
                    let fault = parse_fault(&mut self.reader)?;
                    return Err(fault_error(self.client, &fault));
                }
                Ok(Event::Start(start)) if start.name() == QName(b"array") && !self.in_array => {
                    self.in_array = true;
                }
                Ok(Event::Start(start)) if start.name() == QName(b"data") && self.in_array => {
                    self.in_array_data = true;
                }
                Ok(Event::Start(start))
                    if start.name() == QName(b"value") && self.in_array_data =>
                {
                    self.in_hash_value = true;
                    self.typed_value = false;
                }
                Ok(Event::Start(start))
                    if start.name() == QName(b"string")
                        && self.in_hash_value
                        && !self.typed_value =>
                {
                    self.typed_value = true;
                    let value = read_text_until(&mut self.reader, QName(b"string"))?;
                    self.push_hash(&mut chunk, value)?;
                }
                Ok(Event::Start(start)) if self.in_hash_value && !self.typed_value => {
                    return Err(bad_response(
                        self.client,
                        format!(
                            "download hash uses unsupported XML-RPC type `{}`",
                            String::from_utf8_lossy(start.name().as_ref())
                        ),
                    ));
                }
                Ok(Event::Text(text)) if self.in_hash_value && !self.typed_value => {
                    let value = xml_text_value(&text)?;
                    if !value.trim().is_empty() {
                        self.push_hash(&mut chunk, value)?;
                        self.typed_value = true;
                    }
                }
                Ok(Event::CData(cdata)) if self.in_hash_value && !self.typed_value => {
                    self.push_hash(&mut chunk, xml_cdata_value(&cdata)?)?;
                    self.typed_value = true;
                }
                Ok(Event::GeneralRef(reference)) if self.in_hash_value && !self.typed_value => {
                    self.push_hash(&mut chunk, xml_reference_value(&reference)?)?;
                    self.typed_value = true;
                }
                Ok(Event::End(end)) if end.name() == QName(b"value") && self.in_hash_value => {
                    if !self.typed_value {
                        return Err(bad_response(self.client, "empty download hash value"));
                    }
                    self.in_hash_value = false;
                }
                Ok(Event::End(end)) if end.name() == QName(b"data") && self.in_array_data => {
                    self.in_array_data = false;
                }
                Ok(Event::End(end)) if end.name() == QName(b"array") && self.in_array => {
                    self.finished = true;
                    return Ok((!chunk.is_empty()).then_some(chunk));
                }
                Ok(Event::Eof) => {
                    return Err(bad_response(
                        self.client,
                        "unterminated download_list result",
                    ));
                }
                Ok(_) => {}
                Err(error) => return Err(bad_response(self.client, error.to_string())),
            }

            if chunk.len() >= self.chunk_size {
                return Ok(Some(chunk));
            }
        }
    }

    fn push_hash(
        &self,
        chunk: &mut Vec<InfoHash>,
        value: String,
    ) -> Result<(), TorrentClientError> {
        let hash =
            InfoHash::new(&value).map_err(|error| bad_response(self.client, error.to_string()))?;
        chunk.push(hash);
        Ok(())
    }
}

pub fn parse_inventory_response(
    client: &str,
    hashes: &[InfoHash],
    response: &XmlRpcValue,
) -> Result<Vec<RtorrentDownload>, TorrentClientError> {
    let values = response.as_array(client, "system.multicall result")?;
    let expected = hashes.len().saturating_mul(INVENTORY_METHODS.len());
    if values.len() != expected {
        return Err(bad_response(
            client,
            format!("expected {expected} inventory values, got {}", values.len()),
        ));
    }

    let mut downloads = Vec::with_capacity(hashes.len());
    for (hash_index, info_hash) in hashes.iter().enumerate() {
        let offset = hash_index.saturating_mul(INVENTORY_METHODS.len());
        let fields = multicall_fields(client, values, offset, INVENTORY_METHODS.len())?;
        let [
            name,
            directory,
            left_bytes,
            hashing,
            complete,
            is_multi_file,
            is_active,
            label,
        ] = fields.as_slice()
        else {
            return Err(bad_response(
                client,
                "inventory multicall field count mismatch",
            ));
        };
        downloads.push(RtorrentDownload {
            info_hash: info_hash.clone(),
            name: DisplayName::new(name.as_string(client, "d.name")?)
                .map_err(|error| bad_response(client, error.to_string()))?,
            directory: PathBuf::from(directory.as_string(client, "d.directory")?),
            left_bytes: ByteSize::new(nonnegative_u64(
                client,
                left_bytes.as_i64(client, "d.left_bytes")?,
            )?),
            hashing: hashing.as_bool(client, "d.hashing")?,
            complete: complete.as_bool(client, "d.complete")?,
            is_multi_file: is_multi_file.as_bool(client, "d.is_multi_file")?,
            is_active: is_active.as_bool(client, "d.is_active")?,
            label: nonempty_string(label.as_string(client, "d.custom1")?),
        });
    }
    Ok(downloads)
}

fn parse_optional_inventory_response(
    client: &str,
    info_hash: &InfoHash,
    response: &XmlRpcValue,
) -> Result<Option<RtorrentDownload>, TorrentClientError> {
    let values = response.as_array(client, "system.multicall result")?;
    let expected = INVENTORY_METHODS.len();
    if values.len() != expected {
        return Err(bad_response(
            client,
            format!("expected {expected} inventory values, got {}", values.len()),
        ));
    }
    if values.iter().all(is_missing_download_fault_value) {
        return Ok(None);
    }
    if values.iter().any(is_missing_download_fault_value) {
        return Err(bad_response(
            client,
            "partial missing download response from system.multicall",
        ));
    }

    let downloads = parse_inventory_response(client, std::slice::from_ref(info_hash), response)?;
    Ok(downloads.into_iter().next())
}

pub fn parse_files_response(
    client: &str,
    response: &XmlRpcValue,
) -> Result<Vec<TorrentFile>, TorrentClientError> {
    let rows = response.as_array(client, "f.multicall result")?;
    rows.iter()
        .enumerate()
        .map(|(index, row)| {
            let fields = row.as_array(client, "f.multicall row")?;
            if fields.len() < 2 {
                return Err(bad_response(
                    client,
                    "f.multicall row has fewer than two fields",
                ));
            }
            let [path, size, ..] = fields else {
                return Err(bad_response(
                    client,
                    "f.multicall row has fewer than two fields",
                ));
            };
            TorrentFile::new(
                PathBuf::from(path.as_string(client, "f.path")?),
                ByteSize::new(nonnegative_u64(
                    client,
                    size.as_i64(client, "f.size_bytes")?,
                )?),
                FileIndex::new(
                    u32::try_from(index)
                        .map_err(|error| bad_response(client, error.to_string()))?,
                ),
            )
            .map_err(|error| bad_response(client, error.to_string()))
        })
        .collect()
}

pub fn parse_trackers_response(
    client: &str,
    response: &XmlRpcValue,
) -> Result<Vec<RtorrentTracker>, TorrentClientError> {
    let rows = response.as_array(client, "t.multicall result")?;
    rows.iter()
        .map(|row| {
            let fields = row.as_array(client, "t.multicall row")?;
            if fields.len() < 2 {
                return Err(bad_response(
                    client,
                    "t.multicall row has fewer than two fields",
                ));
            }
            let [url, group, ..] = fields else {
                return Err(bad_response(
                    client,
                    "t.multicall row has fewer than two fields",
                ));
            };
            Ok(RtorrentTracker {
                url: url.as_string(client, "t.url")?.to_owned(),
                group: group.as_string(client, "t.group")?.to_owned(),
            })
        })
        .collect()
}

fn inventory_multicall_param(hashes: &[InfoHash]) -> XmlRpcValue {
    XmlRpcValue::Array(
        hashes
            .iter()
            .flat_map(|hash| {
                INVENTORY_METHODS.iter().map(|method| {
                    let mut call = BTreeMap::new();
                    call.insert(
                        "methodName".to_owned(),
                        XmlRpcValue::String((*method).to_owned()),
                    );
                    call.insert(
                        "params".to_owned(),
                        XmlRpcValue::Array(vec![XmlRpcValue::String(hash.as_str().to_owned())]),
                    );
                    XmlRpcValue::Struct(call)
                })
            })
            .collect(),
    )
}

fn injection_params(
    torrent_bytes: &[u8],
    save_path: Option<&Path>,
    label: &str,
) -> Vec<XmlRpcValue> {
    let mut params = vec![
        XmlRpcValue::String(String::new()),
        XmlRpcValue::Base64(torrent_bytes.to_vec()),
        XmlRpcValue::String(format!("d.custom1.set={label}")),
    ];
    if let Some(save_path) = save_path {
        params.push(XmlRpcValue::String(format!(
            "d.directory.set={}",
            save_path.display()
        )));
    }
    params
}

fn push_value(xml: &mut String, value: &XmlRpcValue) {
    xml.push_str("<value>");
    match value {
        XmlRpcValue::String(value) => {
            xml.push_str("<string>");
            xml.push_str(&escape(value));
            xml.push_str("</string>");
        }
        XmlRpcValue::Int(value) => {
            xml.push_str("<i8>");
            xml.push_str(&value.to_string());
            xml.push_str("</i8>");
        }
        XmlRpcValue::Bool(value) => {
            xml.push_str("<boolean>");
            xml.push_str(if *value { "1" } else { "0" });
            xml.push_str("</boolean>");
        }
        XmlRpcValue::Base64(value) => {
            xml.push_str("<base64>");
            xml.push_str(&BASE64.encode(value));
            xml.push_str("</base64>");
        }
        XmlRpcValue::Array(values) => {
            xml.push_str("<array><data>");
            for value in values {
                push_value(xml, value);
            }
            xml.push_str("</data></array>");
        }
        XmlRpcValue::Struct(fields) => {
            xml.push_str("<struct>");
            for (name, value) in fields {
                xml.push_str("<member><name>");
                xml.push_str(&escape(name));
                xml.push_str("</name>");
                push_value(xml, value);
                xml.push_str("</member>");
            }
            xml.push_str("</struct>");
        }
        XmlRpcValue::Nil => xml.push_str("<nil/>"),
    }
    xml.push_str("</value>");
}

fn parse_fault(reader: &mut Reader<&[u8]>) -> Result<XmlRpcValue, TorrentClientError> {
    loop {
        match reader.read_event() {
            Ok(Event::Start(start)) if start.name() == QName(b"value") => {
                return parse_value(reader);
            }
            Ok(Event::End(end)) if end.name() == QName(b"fault") => {
                return Ok(XmlRpcValue::String("unknown XML-RPC fault".to_owned()));
            }
            Ok(Event::Eof) => return Err(bad_response("rtorrent", "unterminated XML-RPC fault")),
            Ok(_) => {}
            Err(error) => return Err(bad_response("rtorrent", error.to_string())),
        }
    }
}

fn parse_value(reader: &mut Reader<&[u8]>) -> Result<XmlRpcValue, TorrentClientError> {
    let mut text = String::new();
    loop {
        match reader.read_event() {
            Ok(Event::Start(start)) if start.name() == QName(b"string") => {
                return Ok(XmlRpcValue::String(read_text_until(
                    reader,
                    QName(b"string"),
                )?));
            }
            Ok(Event::Start(start)) if matches!(start.name().as_ref(), b"int" | b"i4" | b"i8") => {
                let end = start.name();
                let value = read_text_until(reader, end)?;
                return value
                    .parse::<i64>()
                    .map(XmlRpcValue::Int)
                    .map_err(|error| bad_response("rtorrent", error.to_string()));
            }
            Ok(Event::Start(start)) if start.name() == QName(b"boolean") => {
                let value = read_text_until(reader, QName(b"boolean"))?;
                return match value.as_str() {
                    "0" => Ok(XmlRpcValue::Bool(false)),
                    "1" => Ok(XmlRpcValue::Bool(true)),
                    _ => Err(bad_response(
                        "rtorrent",
                        format!("invalid boolean `{value}`"),
                    )),
                };
            }
            Ok(Event::Start(start)) if start.name() == QName(b"base64") => {
                let value = read_text_until(reader, QName(b"base64"))?;
                return BASE64
                    .decode(value.trim())
                    .map(XmlRpcValue::Base64)
                    .map_err(|error| bad_response("rtorrent", error.to_string()));
            }
            Ok(Event::Start(start)) if start.name() == QName(b"array") => {
                return parse_array(reader);
            }
            Ok(Event::Start(start)) if start.name() == QName(b"struct") => {
                return parse_struct(reader);
            }
            Ok(Event::Empty(start)) if start.name() == QName(b"nil") => {
                return Ok(XmlRpcValue::Nil);
            }
            Ok(Event::Text(value)) => text.push_str(&xml_text_value(&value)?),
            Ok(Event::CData(value)) => text.push_str(&xml_cdata_value(&value)?),
            Ok(Event::GeneralRef(value)) => text.push_str(&xml_reference_value(&value)?),
            Ok(Event::End(end)) if end.name() == QName(b"value") => {
                return Ok(XmlRpcValue::String(text));
            }
            Ok(Event::Eof) => return Err(bad_response("rtorrent", "unterminated XML-RPC value")),
            Ok(_) => {}
            Err(error) => return Err(bad_response("rtorrent", error.to_string())),
        }
    }
}

fn parse_array(reader: &mut Reader<&[u8]>) -> Result<XmlRpcValue, TorrentClientError> {
    let mut values = Vec::new();
    loop {
        match reader.read_event() {
            Ok(Event::Start(start)) if start.name() == QName(b"value") => {
                values.push(parse_value(reader)?);
            }
            Ok(Event::End(end)) if end.name() == QName(b"array") => {
                return Ok(XmlRpcValue::Array(values));
            }
            Ok(Event::Eof) => return Err(bad_response("rtorrent", "unterminated XML-RPC array")),
            Ok(_) => {}
            Err(error) => return Err(bad_response("rtorrent", error.to_string())),
        }
    }
}

fn parse_struct(reader: &mut Reader<&[u8]>) -> Result<XmlRpcValue, TorrentClientError> {
    let mut fields = BTreeMap::new();
    let mut name = None;
    loop {
        match reader.read_event() {
            Ok(Event::Start(start)) if start.name() == QName(b"name") => {
                name = Some(read_text_until(reader, QName(b"name"))?);
            }
            Ok(Event::Start(start)) if start.name() == QName(b"value") => {
                let field = name
                    .take()
                    .ok_or_else(|| bad_response("rtorrent", "XML-RPC struct value without name"))?;
                fields.insert(field, parse_value(reader)?);
            }
            Ok(Event::End(end)) if end.name() == QName(b"struct") => {
                return Ok(XmlRpcValue::Struct(fields));
            }
            Ok(Event::Eof) => return Err(bad_response("rtorrent", "unterminated XML-RPC struct")),
            Ok(_) => {}
            Err(error) => return Err(bad_response("rtorrent", error.to_string())),
        }
    }
}

fn read_text_until(
    reader: &mut Reader<&[u8]>,
    end: QName<'_>,
) -> Result<String, TorrentClientError> {
    let mut value = String::new();
    loop {
        match reader.read_event() {
            Ok(Event::Text(text)) => value.push_str(&xml_text_value(&text)?),
            Ok(Event::CData(cdata)) => value.push_str(&xml_cdata_value(&cdata)?),
            Ok(Event::GeneralRef(reference)) => value.push_str(&xml_reference_value(&reference)?),
            Ok(Event::End(name)) if name.name() == end => return Ok(value),
            Ok(Event::Eof) => return Err(bad_response("rtorrent", "unterminated XML-RPC text")),
            Ok(_) => {}
            Err(error) => return Err(bad_response("rtorrent", error.to_string())),
        }
    }
}

fn xml_text_value(text: &BytesText<'_>) -> Result<String, TorrentClientError> {
    let decoded = text
        .xml10_content()
        .map_err(|error| bad_response("rtorrent", error.to_string()))?;
    let value = unescape(&decoded).map_err(|error| bad_response("rtorrent", error.to_string()))?;
    Ok(value.into_owned())
}

fn xml_cdata_value(cdata: &BytesCData<'_>) -> Result<String, TorrentClientError> {
    cdata
        .xml10_content()
        .map(|value| value.into_owned())
        .map_err(|error| bad_response("rtorrent", error.to_string()))
}

fn xml_reference_value(reference: &BytesRef<'_>) -> Result<String, TorrentClientError> {
    if let Some(character) = reference
        .resolve_char_ref()
        .map_err(|error| bad_response("rtorrent", error.to_string()))?
    {
        return Ok(character.to_string());
    }
    let name = reference
        .decode()
        .map_err(|error| bad_response("rtorrent", error.to_string()))?;
    resolve_predefined_entity(&name)
        .map(str::to_owned)
        .ok_or_else(|| bad_response("rtorrent", format!("unknown XML entity `{name}`")))
}

fn multicall_fields<'a>(
    client: &str,
    values: &'a [XmlRpcValue],
    offset: usize,
    count: usize,
) -> Result<Vec<&'a XmlRpcValue>, TorrentClientError> {
    let mut fields = Vec::with_capacity(count);
    for index in offset..offset.saturating_add(count) {
        let row = values
            .get(index)
            .ok_or_else(|| bad_response(client, "missing system.multicall row"))?;
        let row_values = row.as_array(client, "system.multicall row")?;
        let value = row_values
            .first()
            .ok_or_else(|| bad_response(client, "empty system.multicall row"))?;
        fields.push(value);
    }
    Ok(fields)
}

fn nonnegative_u64(client: &str, value: i64) -> Result<u64, TorrentClientError> {
    u64::try_from(value).map_err(|error| bad_response(client, error.to_string()))
}

fn nonempty_string(value: &str) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_owned())
}

fn fault_error(client: &str, fault: &XmlRpcValue) -> TorrentClientError {
    let message = fault_message(fault)
        .map(str::to_owned)
        .unwrap_or_else(|| format!("{fault:?}"));
    let lower = message.to_ascii_lowercase();
    if lower.contains("method") && (lower.contains("not found") || lower.contains("unknown")) {
        TorrentClientError::UnsupportedCapability {
            client: client.to_owned(),
            capability: message,
        }
    } else {
        bad_response(client, message)
    }
}

fn fault_message(fault: &XmlRpcValue) -> Option<&str> {
    match fault {
        XmlRpcValue::Struct(fields) => fields.get("faultString").and_then(|value| match value {
            XmlRpcValue::String(value) => Some(value.as_str()),
            _ => None,
        }),
        _ => None,
    }
}

fn is_missing_download_fault_value(value: &XmlRpcValue) -> bool {
    fault_message(value).is_some_and(is_missing_download_message)
}

fn is_missing_download_error(error: &TorrentClientError) -> bool {
    matches!(
        error,
        TorrentClientError::BadResponse { message, .. }
            if is_missing_download_message(message)
    )
}

fn is_missing_download_message(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    (lower.contains("could not find") || lower.contains("not found"))
        && (lower.contains("info-hash")
            || lower.contains("info hash")
            || lower.contains("download")
            || lower.contains("torrent"))
}

async fn read_client_text(
    mut response: reqwest::Response,
    client: &str,
    limit: u64,
) -> Result<String, TorrentClientError> {
    if response
        .content_length()
        .is_some_and(|length| length > limit)
    {
        return Err(bad_response(
            client,
            format!("response exceeded {limit} bytes"),
        ));
    }

    let mut body = Vec::with_capacity(
        response
            .content_length()
            .and_then(|length| usize::try_from(length).ok())
            .unwrap_or_default(),
    );
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| unavailable(client, request_error_message(error)))?
    {
        let next_len = body.len().saturating_add(chunk.len());
        if u64::try_from(next_len).unwrap_or(u64::MAX) > limit {
            return Err(bad_response(
                client,
                format!("response exceeded {limit} bytes"),
            ));
        }
        body.extend_from_slice(&chunk);
    }

    String::from_utf8(body).map_err(|error| bad_response(client, error.to_string()))
}

fn unavailable(client: &str, message: String) -> TorrentClientError {
    TorrentClientError::Unavailable {
        client: client.to_owned(),
        retry_after_ms: None,
        message,
    }
}

fn classify_rtorrent_idempotent_error(error: &TorrentClientError) -> RetryDecision {
    match error {
        TorrentClientError::Unavailable { message, .. } if is_transient_http_error(message) => {
            RetryDecision::retry(RetryErrorKind::TransientNetwork)
        }
        TorrentClientError::Unavailable { message, .. } if is_transport_error(message) => {
            RetryDecision::retry(RetryErrorKind::TransientNetwork)
        }
        TorrentClientError::Unavailable { .. } => {
            RetryDecision::do_not_retry(RetryErrorKind::BadRequest)
        }
        TorrentClientError::Unauthorized { .. } => {
            RetryDecision::do_not_retry(RetryErrorKind::Authentication)
        }
        TorrentClientError::Cancelled { .. } => {
            RetryDecision::do_not_retry(RetryErrorKind::Cancelled)
        }
        TorrentClientError::BadResponse { .. } | TorrentClientError::ApiChanged { .. } => {
            RetryDecision::do_not_retry(RetryErrorKind::InvalidResponse)
        }
        TorrentClientError::UnsupportedCapability { .. } => {
            RetryDecision::do_not_retry(RetryErrorKind::Unsupported)
        }
    }
}

fn is_transient_http_error(message: &str) -> bool {
    message.starts_with("HTTP 408")
        || message.starts_with("HTTP 429")
        || message.starts_with("HTTP 502")
        || message.starts_with("HTTP 503")
        || message.starts_with("HTTP 504")
}

fn is_transport_error(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("error sending request")
        || lower.contains("error decoding response body")
        || lower.contains("request timed out")
        || lower.contains("connection reset")
        || lower.contains("connection refused")
        || lower.contains("connection closed")
        || lower.contains("connection aborted")
        || lower.contains("broken pipe")
        || lower.contains("timed out")
}

fn request_error_message(error: reqwest::Error) -> String {
    let url = error
        .url()
        .map(|url| sanitize_url_for_logging(url.as_str()).to_string());
    let error = error.without_url();
    let mut message = error.to_string();
    let mut source = StdError::source(&error);
    while let Some(error) = source {
        message.push_str(": ");
        message.push_str(&error.to_string());
        source = error.source();
    }
    if let Some(url) = url {
        message.push_str(" for url (");
        message.push_str(&url);
        message.push(')');
    }
    sanitize_url_for_logging(message).to_string()
}

fn torrent_client_http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .redirect(Policy::none())
        .no_proxy()
        .build()
        .expect("static rTorrent HTTP client policy should build")
}

fn cancelled_error(client: &str) -> TorrentClientError {
    TorrentClientError::Cancelled {
        client: client.to_owned(),
        message: "shutdown requested".to_owned(),
    }
}

fn bad_response(client: &str, message: impl Into<String>) -> TorrentClientError {
    TorrentClientError::BadResponse {
        client: client.to_owned(),
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::io::{Read, Write};
    use std::net::TcpListener as StdTcpListener;
    use std::process::Command;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex as StdMutex};
    use std::time::Duration as StdDuration;

    use axum::Router;
    use axum::body::{Body, to_bytes};
    use axum::http::{
        HeaderValue, Request, StatusCode as AxumStatusCode,
        header::{CONTENT_LENGTH, LOCATION},
    };
    use axum::response::{IntoResponse, Response};
    use axum::routing::post;
    use tokio::net::TcpListener;

    use super::*;

    const SHA1: &str = "0123456789abcdef0123456789abcdef01234567";

    #[test]
    fn client_debug_redacts_secret_bearing_endpoint() {
        let client = RtorrentClient::new(
            "rtorrent",
            "https://url-user:url-pass@example.invalid/RPC2?token=url-secret&ok=1#fragment",
            Duration::from_secs(1),
        );

        let debug = format!("{client:?}");

        assert!(debug.contains("RtorrentClient"));
        assert!(debug.contains("ok=1"));
        assert!(!debug.contains("url-user"));
        assert!(!debug.contains("url-pass"));
        assert!(!debug.contains("url-secret"));
        assert!(!debug.contains("fragment"));
    }

    #[test]
    fn inventory_multicall_payload_contains_rtorrent_fields() {
        let hash = InfoHash::new(SHA1).unwrap();
        let xml = build_inventory_multicall(&[hash]);

        assert!(xml.contains("<methodName>system.multicall</methodName>"));
        assert!(xml.contains("<name>methodName</name><value><string>d.name</string></value>"));
        assert!(xml.contains("<string>d.custom1</string>"));
        assert!(xml.contains(SHA1));
    }

    #[tokio::test]
    async fn client_does_not_follow_rpc_redirects() {
        let endpoint = spawn_rtorrent_server(|_request| async move {
            let mut response = Response::new(Body::empty());
            *response.status_mut() = AxumStatusCode::FOUND;
            response.headers_mut().insert(
                LOCATION,
                HeaderValue::from_static("http://127.0.0.1:1/leaked"),
            );
            response
        })
        .await;
        let client = RtorrentClient::new("rtorrent", endpoint, Duration::from_secs(5));

        let error = client.validate().await.unwrap_err();

        assert!(error.to_string().contains("HTTP 302"));
    }

    #[tokio::test]
    async fn request_errors_include_source_chain_and_sanitize_url() {
        let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let error = torrent_client_http_client()
            .get(format!(
                "http://user:password@{address}/RPC2?apikey=secret&ok=1"
            ))
            .send()
            .await
            .unwrap_err();

        let message = request_error_message(error);

        assert!(message.contains("error sending request"));
        assert!(message.contains("tcp connect error"));
        assert!(message.contains("127.0.0.1"));
        assert!(message.contains("ok=1"));
        assert!(!message.contains("user"));
        assert!(!message.contains("password"));
        assert!(!message.contains("secret"));
    }

    #[tokio::test]
    async fn client_ignores_environment_http_proxy() {
        const CHILD_MODE: &str = "SPOROS_RTORRENT_NO_PROXY_CHILD";
        const ENDPOINT_ENV: &str = "SPOROS_RTORRENT_NO_PROXY_ENDPOINT";
        if std::env::var(CHILD_MODE).ok().as_deref() == Some("1") {
            let endpoint =
                std::env::var(ENDPOINT_ENV).expect("child proxy test should receive endpoint");
            let client = RtorrentClient::new("rtorrent", endpoint, Duration::from_secs(2));
            client.validate().await.unwrap();
            return;
        }

        let endpoint = spawn_rtorrent_no_proxy_target();
        let proxy = ProxyProbe::spawn();
        let status = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("clients::rtorrent::tests::client_ignores_environment_http_proxy")
            .arg("--nocapture")
            .env(CHILD_MODE, "1")
            .env(ENDPOINT_ENV, endpoint)
            .env("HTTP_PROXY", proxy.url())
            .env("HTTPS_PROXY", proxy.url())
            .env("ALL_PROXY", proxy.url())
            .env("NO_PROXY", "")
            .env("http_proxy", proxy.url())
            .env("https_proxy", proxy.url())
            .env("all_proxy", proxy.url())
            .env("no_proxy", "")
            .status()
            .unwrap();

        proxy.stop();
        assert!(status.success());
        assert_eq!(0, proxy.requests());
    }

    #[test]
    fn injection_payload_uses_raw_methods_and_sets_label() {
        let save_path = PathBuf::from("/downloads/prepared");
        let stopped = build_injection_call(b"torrent", Some(&save_path), "cross-seed", false);
        let started = build_injection_call(b"torrent", Some(&save_path), "cross-seed", true);

        assert!(stopped.contains("<methodName>load.raw</methodName>"));
        assert!(started.contains("<methodName>load.raw_start</methodName>"));
        assert_xml_order(
            &stopped,
            &[
                "<string></string>",
                "<base64>dG9ycmVudA==</base64>",
                "<string>d.custom1.set=cross-seed</string>",
                "<string>d.directory.set=/downloads/prepared</string>",
            ],
        );
    }

    #[tokio::test]
    async fn client_injects_with_requested_save_path() {
        let endpoint = spawn_rtorrent_server(|request| async move {
            let body = to_bytes(request.into_body(), 65_536).await.unwrap();
            let body = String::from_utf8(body.to_vec()).unwrap();
            if body.contains("<methodName>load.raw_start</methodName>")
                && xml_contains_order(
                    &body,
                    &[
                        "<string></string>",
                        "<base64>dG9ycmVudA==</base64>",
                        "<string>d.custom1.set=cross-seed</string>",
                        "<string>d.directory.set=/downloads/prepared</string>",
                    ],
                )
            {
                return (AxumStatusCode::OK, xml_response("<i8>0</i8>"));
            }
            (AxumStatusCode::BAD_REQUEST, body)
        })
        .await;
        let client = RtorrentClient::new("rtorrent", endpoint, Duration::from_secs(5));
        let save_path = PathBuf::from("/downloads/prepared");

        client
            .inject(b"torrent", Some(&save_path), "cross-seed", true)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn client_injects_paused_for_recheck() {
        let endpoint = spawn_rtorrent_server(|request| async move {
            let body = to_bytes(request.into_body(), 65_536).await.unwrap();
            let body = String::from_utf8(body.to_vec()).unwrap();
            if body.contains("<methodName>load.raw</methodName>")
                && !body.contains("<methodName>load.raw_start</methodName>")
            {
                return (AxumStatusCode::OK, xml_response("<i8>0</i8>"));
            }
            (AxumStatusCode::BAD_REQUEST, body)
        })
        .await;
        let client = RtorrentClient::new("rtorrent", endpoint, Duration::from_secs(5));

        client
            .inject(b"torrent", None, "sporos", false)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn client_does_not_retry_transient_injection_failure() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let server_attempts = attempts.clone();
        let endpoint = spawn_rtorrent_server(move |_request| {
            let attempts = server_attempts.clone();
            async move {
                attempts.fetch_add(1, Ordering::Relaxed);
                (AxumStatusCode::SERVICE_UNAVAILABLE, "try again")
            }
        })
        .await;
        let client = RtorrentClient::new("rtorrent", endpoint, Duration::from_secs(5));

        let error = client
            .inject(b"torrent-bytes", None, "sporos", false)
            .await
            .unwrap_err();

        assert!(matches!(error, TorrentClientError::Unavailable { .. }));
        assert_eq!(1, attempts.load(Ordering::Relaxed));
    }

    fn assert_xml_order(xml: &str, needles: &[&str]) {
        assert!(
            xml_contains_order(xml, needles),
            "expected XML to contain {needles:?} in order, got {xml}"
        );
    }

    fn xml_contains_order(xml: &str, needles: &[&str]) -> bool {
        let mut remaining = xml;
        for needle in needles {
            let Some(index) = remaining.find(needle) else {
                return false;
            };
            let next_index = index + needle.len();
            let Some(next_remaining) = remaining.get(next_index..) else {
                return false;
            };
            remaining = next_remaining;
        }
        true
    }

    #[test]
    fn response_parser_decodes_xmlrpc_values_and_entities() {
        let value = parse_method_response(
            "rtorrent",
            r#"
            <methodResponse><params><param><value><array><data>
              <value><string>Show &amp; Tell</string></value>
              <value><i8>42</i8></value>
              <value><boolean>1</boolean></value>
            </data></array></value></param></params></methodResponse>
            "#,
        )
        .unwrap();

        assert_eq!(
            XmlRpcValue::Array(vec![
                XmlRpcValue::String("Show & Tell".to_owned()),
                XmlRpcValue::Int(42),
                XmlRpcValue::Bool(true),
            ]),
            value
        );
    }

    #[test]
    fn download_hash_parser_returns_bounded_chunks() {
        let xml = xml_response(&download_list_xml(RTORRENT_INVENTORY_CHUNK_SIZE + 2));
        let mut chunks = DownloadHashChunks::new("rtorrent", &xml, RTORRENT_INVENTORY_CHUNK_SIZE);

        let first = chunks.next_chunk().unwrap().unwrap();
        let second = chunks.next_chunk().unwrap().unwrap();

        assert_eq!(RTORRENT_INVENTORY_CHUNK_SIZE, first.len());
        assert_eq!(2, second.len());
        assert!(chunks.next_chunk().unwrap().is_none());
        assert_eq!(
            "0000000000000000000000000000000000000001",
            first[0].as_str()
        );
    }

    #[test]
    fn inventory_response_maps_download_metadata() {
        let hash = InfoHash::new(SHA1).unwrap();
        let rows = XmlRpcValue::Array(vec![
            row(XmlRpcValue::String("Example".to_owned())),
            row(XmlRpcValue::String("/downloads".to_owned())),
            row(XmlRpcValue::Int(0)),
            row(XmlRpcValue::Int(0)),
            row(XmlRpcValue::Int(1)),
            row(XmlRpcValue::Int(1)),
            row(XmlRpcValue::Int(0)),
            row(XmlRpcValue::String("sporos".to_owned())),
        ]);

        let downloads =
            parse_inventory_response("rtorrent", std::slice::from_ref(&hash), &rows).unwrap();

        assert_eq!(hash, downloads[0].info_hash);
        assert_eq!("Example", downloads[0].name.as_str());
        assert_eq!(PathBuf::from("/downloads"), downloads[0].directory);
        assert_eq!(0, downloads[0].left_bytes.get());
        assert!(downloads[0].complete);
        assert!(downloads.iter().any(|download| download.info_hash == hash));
        assert_eq!(Some("sporos".to_owned()), downloads[0].label);
    }

    #[test]
    fn optional_inventory_response_maps_missing_download_fault_to_none() {
        let hash = InfoHash::new(SHA1).unwrap();
        let rows = XmlRpcValue::Array(
            std::iter::repeat_with(missing_download_fault)
                .take(INVENTORY_METHODS.len())
                .collect(),
        );

        let download = parse_optional_inventory_response("rtorrent", &hash, &rows).unwrap();

        assert!(download.is_none());
    }

    #[test]
    fn optional_inventory_response_maps_rtorrent_not_found_fault_to_none() {
        let hash = InfoHash::new(SHA1).unwrap();
        let rows = XmlRpcValue::Array(
            std::iter::repeat_with(|| {
                XmlRpcValue::Struct(BTreeMap::from([(
                    "faultString".to_owned(),
                    XmlRpcValue::String("invalid parameters: info-hash not found".to_owned()),
                )]))
            })
            .take(INVENTORY_METHODS.len())
            .collect(),
        );

        let download = parse_optional_inventory_response("rtorrent", &hash, &rows).unwrap();

        assert!(download.is_none());
    }

    #[test]
    fn file_and_tracker_responses_map_typed_rows() {
        let files = parse_files_response(
            "rtorrent",
            &XmlRpcValue::Array(vec![XmlRpcValue::Array(vec![
                XmlRpcValue::String("Show/Episode.mkv".to_owned()),
                XmlRpcValue::Int(123),
            ])]),
        )
        .unwrap();
        let trackers = parse_trackers_response(
            "rtorrent",
            &XmlRpcValue::Array(vec![XmlRpcValue::Array(vec![
                XmlRpcValue::String("https://tracker.example/announce".to_owned()),
                XmlRpcValue::String("tracker.example".to_owned()),
            ])]),
        )
        .unwrap();

        assert_eq!(PathBuf::from("Show/Episode.mkv"), files[0].relative_path);
        assert_eq!(123, files[0].size.get());
        assert_eq!("https://tracker.example/announce", trackers[0].url);
        assert_eq!("tracker.example", trackers[0].group);
    }

    #[test]
    fn xmlrpc_fault_maps_missing_methods_to_unsupported_capability() {
        let error = parse_method_response(
            "rtorrent",
            r#"
            <methodResponse><fault><value><struct>
              <member><name>faultString</name><value><string>Method not found: d.tags</string></value></member>
            </struct></value></fault></methodResponse>
            "#,
        )
        .unwrap_err();

        assert!(matches!(
            error,
            TorrentClientError::UnsupportedCapability { .. }
        ));
    }

    #[tokio::test]
    async fn client_posts_xmlrpc_payloads_and_parses_inventory() {
        let endpoint = spawn_rtorrent_server(|request| async move {
            let body = to_bytes(request.into_body(), 65_536).await.unwrap();
            let body = String::from_utf8(body.to_vec()).unwrap();
            if body.contains("<methodName>download_list</methodName>") {
                return (
                    AxumStatusCode::OK,
                    xml_response(r#"<array><data><value><string>0123456789abcdef0123456789abcdef01234567</string></value></data></array>"#),
                );
            }
            if body.contains("<methodName>system.multicall</methodName>")
                && body.contains("d.custom1")
            {
                return (AxumStatusCode::OK, inventory_xml_response());
            }
            (AxumStatusCode::BAD_REQUEST, body)
        })
        .await;
        let client = RtorrentClient::new("rtorrent", endpoint, Duration::from_secs(5));

        let inventory = client.list_inventory().await.unwrap();

        assert_eq!(1, inventory.len());
        assert_eq!("Example", inventory[0].name.as_str());
    }

    #[tokio::test]
    async fn client_download_info_treats_missing_hash_as_absent() {
        let endpoint = spawn_rtorrent_server(|request| async move {
            let body = to_bytes(request.into_body(), 65_536).await.unwrap();
            let body = String::from_utf8(body.to_vec()).unwrap();
            if body.contains("<methodName>system.multicall</methodName>")
                && body.contains("d.custom1")
            {
                return (AxumStatusCode::OK, missing_inventory_xml_response());
            }
            (AxumStatusCode::BAD_REQUEST, body)
        })
        .await;
        let client = RtorrentClient::new("rtorrent", endpoint, Duration::from_secs(5));
        let hash = InfoHash::new(SHA1).unwrap();

        assert!(client.download_info(&hash).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn client_retries_transient_inventory_file_reads() {
        let file_attempts = Arc::new(AtomicUsize::new(0));
        let server_file_attempts = file_attempts.clone();
        let endpoint = spawn_rtorrent_server(move |request| {
            let file_attempts = server_file_attempts.clone();
            async move {
                let body = to_bytes(request.into_body(), 65_536).await.unwrap();
                let body = String::from_utf8(body.to_vec()).unwrap();
                if body.contains("<methodName>f.multicall</methodName>") {
                    if file_attempts.fetch_add(1, Ordering::Relaxed) == 0 {
                        return (AxumStatusCode::SERVICE_UNAVAILABLE, "try again").into_response();
                    }
                    return (
                        AxumStatusCode::OK,
                        xml_response(
                            r#"<array><data><value><array><data>
                              <value><string>Show/Episode.mkv</string></value>
                              <value><i8>123</i8></value>
                            </data></array></value></data></array>"#,
                        ),
                    )
                        .into_response();
                }
                (AxumStatusCode::BAD_REQUEST, body).into_response()
            }
        })
        .await;
        let client = RtorrentClient::new("rtorrent", endpoint, Duration::from_secs(5));
        let hash = InfoHash::new(SHA1).unwrap();

        let files = client.fetch_files(&hash).await.unwrap();

        assert_eq!(PathBuf::from("Show/Episode.mkv"), files[0].relative_path);
        assert_eq!(2, file_attempts.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn client_does_not_retry_terminal_inventory_http_statuses() {
        let download_list_attempts = Arc::new(AtomicUsize::new(0));
        let server_download_list_attempts = download_list_attempts.clone();
        let endpoint = spawn_rtorrent_server(move |_request| {
            let attempts = server_download_list_attempts.clone();
            async move {
                attempts.fetch_add(1, Ordering::Relaxed);
                (AxumStatusCode::BAD_REQUEST, "bad request").into_response()
            }
        })
        .await;
        let client = RtorrentClient::new("rtorrent", endpoint, Duration::from_secs(5));

        let error = client.list_inventory().await.unwrap_err();

        assert!(matches!(
            error,
            TorrentClientError::Unavailable { message, .. } if message == "HTTP 400 Bad Request"
        ));
        assert_eq!(1, download_list_attempts.load(Ordering::Relaxed));

        let multicall_attempts = Arc::new(AtomicUsize::new(0));
        let server_multicall_attempts = multicall_attempts.clone();
        let endpoint = spawn_rtorrent_server(move |request| {
            let attempts = server_multicall_attempts.clone();
            async move {
                let body = to_bytes(request.into_body(), 65_536).await.unwrap();
                let body = String::from_utf8(body.to_vec()).unwrap();
                if body.contains("<methodName>download_list</methodName>") {
                    return (
                        AxumStatusCode::OK,
                        xml_response(
                            r#"<array><data><value><string>0123456789abcdef0123456789abcdef01234567</string></value></data></array>"#,
                        ),
                    )
                        .into_response();
                }
                if body.contains("<methodName>system.multicall</methodName>") {
                    attempts.fetch_add(1, Ordering::Relaxed);
                    return (AxumStatusCode::NOT_FOUND, "missing method").into_response();
                }
                (AxumStatusCode::BAD_REQUEST, body).into_response()
            }
        })
        .await;
        let client = RtorrentClient::new("rtorrent", endpoint, Duration::from_secs(5));

        let error = client.list_inventory().await.unwrap_err();

        assert!(matches!(
            error,
            TorrentClientError::Unavailable { message, .. } if message == "HTTP 404 Not Found"
        ));
        assert_eq!(1, multicall_attempts.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn client_rejects_oversized_xmlrpc_response() {
        let endpoint = spawn_rtorrent_server(|_request| async move {
            oversized_response(RTORRENT_RESPONSE_MAX_BYTES + 1)
        })
        .await;
        let client = RtorrentClient::new("rtorrent", endpoint, Duration::from_secs(5));

        let error = client.validate().await.unwrap_err();

        assert!(matches!(
            error,
            TorrentClientError::BadResponse { message, .. }
                if message.contains("response exceeded")
        ));
    }

    #[tokio::test]
    async fn client_rejects_chunked_oversized_xmlrpc_response() {
        let endpoint = spawn_chunked_response_server("/RPC2", RTORRENT_RESPONSE_MAX_BYTES + 1);
        let client = RtorrentClient::new("rtorrent", endpoint, Duration::from_secs(5));

        let error = client.validate().await.unwrap_err();

        assert!(matches!(
            error,
            TorrentClientError::BadResponse { message, .. }
                if message.contains("response exceeded")
        ));
    }

    #[tokio::test]
    async fn client_chunks_large_inventory_multicalls() {
        let seen_chunks = Arc::new(StdMutex::new(Vec::<usize>::new()));
        let seen_requests = seen_chunks.clone();
        let endpoint = spawn_rtorrent_server(move |request| {
            let seen_chunks = seen_requests.clone();
            async move {
                let body = to_bytes(request.into_body(), 5_000_000).await.unwrap();
                let body = String::from_utf8(body.to_vec()).unwrap();
                if body.contains("<methodName>download_list</methodName>") {
                    return (
                        AxumStatusCode::OK,
                        xml_response(&download_list_xml(RTORRENT_INVENTORY_CHUNK_SIZE + 1)),
                    );
                }
                if body.contains("<methodName>system.multicall</methodName>")
                    && body.contains("d.custom1")
                {
                    let chunk_size = body.matches("<string>d.name</string>").count();
                    seen_chunks.lock().unwrap().push(chunk_size);
                    return (
                        AxumStatusCode::OK,
                        xml_response(&inventory_rows_xml(chunk_size)),
                    );
                }
                (AxumStatusCode::BAD_REQUEST, body)
            }
        })
        .await;
        let client = RtorrentClient::new("rtorrent", endpoint, Duration::from_secs(5));

        let inventory = client.list_inventory().await.unwrap();

        assert_eq!(RTORRENT_INVENTORY_CHUNK_SIZE + 1, inventory.len());
        assert_eq!(
            vec![RTORRENT_INVENTORY_CHUNK_SIZE, 1],
            *seen_chunks.lock().unwrap()
        );

        seen_chunks.lock().unwrap().clear();
        let streamed_chunks = Arc::new(StdMutex::new(Vec::<usize>::new()));
        let streamed = client
            .list_inventory_chunks({
                let streamed_chunks = streamed_chunks.clone();
                move |chunk| {
                    let streamed_chunks = streamed_chunks.clone();
                    async move {
                        streamed_chunks.lock().unwrap().push(chunk.len());
                        Ok(())
                    }
                }
            })
            .await
            .unwrap();

        assert_eq!(RTORRENT_INVENTORY_CHUNK_SIZE + 1, streamed);
        assert_eq!(
            vec![RTORRENT_INVENTORY_CHUNK_SIZE, 1],
            *streamed_chunks.lock().unwrap()
        );
        assert_eq!(
            vec![RTORRENT_INVENTORY_CHUNK_SIZE, 1],
            *seen_chunks.lock().unwrap()
        );
    }

    #[tokio::test]
    async fn client_inventory_chunks_stop_before_request_on_shutdown() {
        let requests = Arc::new(StdMutex::new(0usize));
        let seen_requests = requests.clone();
        let endpoint = spawn_rtorrent_server(move |_request| {
            let seen_requests = seen_requests.clone();
            async move {
                *seen_requests.lock().unwrap() += 1;
                (
                    AxumStatusCode::OK,
                    xml_response("<array><data></data></array>"),
                )
            }
        })
        .await;
        let client = RtorrentClient::new("rtorrent", endpoint, Duration::from_secs(5));
        let (shutdown, signal) = crate::runtime::shutdown::shutdown_channel();
        shutdown.cancel_now("test shutdown").unwrap();

        let error = client
            .list_inventory_chunks_until_shutdown(
                || {
                    let mut signal = signal.clone();
                    async move {
                        signal.cancelled().await;
                    }
                },
                |_chunk| async { Ok(()) },
            )
            .await
            .unwrap_err();

        assert!(matches!(error, TorrentClientError::Cancelled { .. }));
        assert_eq!(0, *requests.lock().unwrap());
    }

    #[tokio::test]
    async fn client_posts_label_recheck_pause_and_resume_methods() {
        let endpoint = spawn_rtorrent_server(|request| async move {
            let body = to_bytes(request.into_body(), 65_536).await.unwrap();
            let body = String::from_utf8(body.to_vec()).unwrap();
            if body.contains("<methodName>d.custom1.set</methodName>") {
                assert!(body.contains("<string>cross-seed</string>"));
                return (AxumStatusCode::OK, xml_response("<i8>0</i8>"));
            }
            for method in ["d.check_hash", "d.pause", "d.resume"] {
                if body.contains(&format!("<methodName>{method}</methodName>")) {
                    return (AxumStatusCode::OK, xml_response("<i8>0</i8>"));
                }
            }
            (AxumStatusCode::BAD_REQUEST, body)
        })
        .await;
        let client = RtorrentClient::new("rtorrent", endpoint, Duration::from_secs(5));
        let hash = InfoHash::new(SHA1).unwrap();

        client.set_label(&hash, "cross-seed").await.unwrap();
        client.recheck(&hash).await.unwrap();
        client.pause(&hash).await.unwrap();
        client.resume(&hash).await.unwrap();
    }

    #[tokio::test]
    async fn client_retries_idempotent_mutations() {
        let attempts = Arc::new(StdMutex::new(BTreeMap::<String, usize>::new()));
        let server_attempts = attempts.clone();
        let endpoint = spawn_rtorrent_server(move |request| {
            let attempts = server_attempts.clone();
            async move {
                let body = to_bytes(request.into_body(), 65_536).await.unwrap();
                let body = String::from_utf8(body.to_vec()).unwrap();
                for method in ["d.custom1.set", "d.check_hash", "d.pause", "d.resume"] {
                    if body.contains(&format!("<methodName>{method}</methodName>")) {
                        let mut attempts = attempts.lock().unwrap();
                        let attempt = attempts.entry(method.to_owned()).or_insert(0);
                        *attempt += 1;
                        if *attempt == 1 {
                            return (AxumStatusCode::SERVICE_UNAVAILABLE, "try again")
                                .into_response();
                        }
                        return (AxumStatusCode::OK, xml_response("<i8>0</i8>")).into_response();
                    }
                }
                (AxumStatusCode::BAD_REQUEST, body).into_response()
            }
        })
        .await;
        let client = RtorrentClient::new("rtorrent", endpoint, Duration::from_secs(5));
        let hash = InfoHash::new(SHA1).unwrap();

        client.set_label(&hash, "cross-seed").await.unwrap();
        client.recheck(&hash).await.unwrap();
        client.pause(&hash).await.unwrap();
        client.resume(&hash).await.unwrap();

        let attempts = attempts.lock().unwrap();
        assert_eq!(Some(&2), attempts.get("d.custom1.set"));
        assert_eq!(Some(&2), attempts.get("d.check_hash"));
        assert_eq!(Some(&2), attempts.get("d.pause"));
        assert_eq!(Some(&2), attempts.get("d.resume"));
    }

    fn row(value: XmlRpcValue) -> XmlRpcValue {
        XmlRpcValue::Array(vec![value])
    }

    fn missing_download_fault() -> XmlRpcValue {
        XmlRpcValue::Struct(BTreeMap::from([(
            "faultString".to_owned(),
            XmlRpcValue::String("Could not find info-hash.".to_owned()),
        )]))
    }

    fn xml_response(value: &str) -> String {
        format!(
            "<methodResponse><params><param><value>{value}</value></param></params></methodResponse>"
        )
    }

    fn oversized_response(length: u64) -> Response {
        let body = vec![b'x'; usize::try_from(length).unwrap()];
        (
            AxumStatusCode::OK,
            [(
                CONTENT_LENGTH,
                HeaderValue::from_str(&length.to_string()).unwrap(),
            )],
            body,
        )
            .into_response()
    }

    fn spawn_chunked_response_server(path: &str, length: u64) -> String {
        let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0_u8; 4096];
            drop(stream.read(&mut request));
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: text/xml\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
                )
                .unwrap();
            write_chunked_body(&mut stream, length);
        });
        format!("http://{address}{path}")
    }

    fn write_chunked_body(stream: &mut std::net::TcpStream, length: u64) {
        let mut remaining = length;
        while remaining > 0 {
            let size = usize::try_from(remaining.min(8192)).unwrap();
            let chunk = vec![b'x'; size];
            write!(stream, "{size:x}\r\n").unwrap();
            stream.write_all(&chunk).unwrap();
            stream.write_all(b"\r\n").unwrap();
            remaining -= u64::try_from(size).unwrap();
        }
        stream.write_all(b"0\r\n\r\n").unwrap();
    }

    fn inventory_xml_response() -> String {
        xml_response(
            r#"<array><data>
              <value><array><data><value><string>Example</string></value></data></array></value>
              <value><array><data><value><string>/downloads</string></value></data></array></value>
              <value><array><data><value><i8>0</i8></value></data></array></value>
              <value><array><data><value><i8>0</i8></value></data></array></value>
              <value><array><data><value><i8>1</i8></value></data></array></value>
              <value><array><data><value><i8>1</i8></value></data></array></value>
              <value><array><data><value><i8>0</i8></value></data></array></value>
              <value><array><data><value><string>sporos</string></value></data></array></value>
            </data></array>"#,
        )
    }

    fn missing_inventory_xml_response() -> String {
        let mut body = String::from("<array><data>");
        for _ in INVENTORY_METHODS {
            body.push_str(
                r#"<value><struct>
                  <member><name>faultString</name><value><string>Could not find info-hash.</string></value></member>
                </struct></value>"#,
            );
        }
        body.push_str("</data></array>");
        xml_response(&body)
    }

    fn download_list_xml(count: usize) -> String {
        let mut body = String::from("<array><data>");
        for index in 0..count {
            body.push_str(&format!(
                r#"<value><string>{:040x}</string></value>"#,
                index + 1
            ));
        }
        body.push_str("</data></array>");
        body
    }

    fn inventory_rows_xml(count: usize) -> String {
        let mut body = String::from("<array><data>");
        for index in 0..count {
            let name = format!("Example {}", index + 1);
            for value in [
                format!("<string>{name}</string>"),
                "<string>/downloads</string>".to_owned(),
                "<i8>0</i8>".to_owned(),
                "<i8>0</i8>".to_owned(),
                "<i8>1</i8>".to_owned(),
                "<i8>1</i8>".to_owned(),
                "<i8>0</i8>".to_owned(),
                "<string>sporos</string>".to_owned(),
            ] {
                body.push_str("<value><array><data><value>");
                body.push_str(&value);
                body.push_str("</value></data></array></value>");
            }
        }
        body.push_str("</data></array>");
        body
    }

    async fn spawn_rtorrent_server<F, Fut, R>(handler: F) -> String
    where
        F: Fn(Request<Body>) -> Fut + Clone + Send + Sync + 'static,
        Fut: Future<Output = R> + Send + 'static,
        R: IntoResponse + Send + 'static,
    {
        let app = Router::new().route("/RPC2", post(handler));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app.into_make_service())
                .await
                .unwrap();
        });
        format!("http://{address}/RPC2")
    }

    fn spawn_rtorrent_no_proxy_target() -> String {
        let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0_u8; 2048];
            drop(stream.read(&mut request));
            let body = xml_response("<array><data></data></array>");
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/xml\r\nContent-Length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).unwrap();
        });
        format!("http://{address}/RPC2")
    }

    struct ProxyProbe {
        address: std::net::SocketAddr,
        requests: Arc<AtomicUsize>,
        stop: Arc<AtomicBool>,
    }

    impl ProxyProbe {
        fn spawn() -> Self {
            let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let address = listener.local_addr().unwrap();
            let requests = Arc::new(AtomicUsize::new(0));
            let stop = Arc::new(AtomicBool::new(false));
            let thread_requests = Arc::clone(&requests);
            let thread_stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                while !thread_stop.load(Ordering::SeqCst) {
                    match listener.accept() {
                        Ok((mut stream, _)) => {
                            thread_requests.fetch_add(1, Ordering::SeqCst);
                            let mut request = [0_u8; 1024];
                            drop(stream.read(&mut request));
                            drop(stream.write_all(
                                b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n",
                            ));
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(StdDuration::from_millis(10));
                        }
                        Err(_) => break,
                    }
                }
            });
            Self {
                address,
                requests,
                stop,
            }
        }

        fn url(&self) -> String {
            format!("http://{}", self.address)
        }

        fn requests(&self) -> usize {
            self.requests.load(Ordering::SeqCst)
        }

        fn stop(&self) {
            self.stop.store(true, Ordering::SeqCst);
        }
    }
}
