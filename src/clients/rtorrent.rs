use std::{
    borrow::Cow,
    collections::BTreeMap,
    future::Future,
    path::{Path, PathBuf},
    time::Duration,
};

use quick_xml::{Reader, events::Event};
use reqwest::header::CONTENT_TYPE;
use url::Url;

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
        let result = confirm_injection(self, &new_torrent.metafile.info_hash)?;
        if result != InjectionResult::Injected {
            return Ok(result);
        }
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
        Ok(result)
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
