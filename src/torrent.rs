//! Torrent parsing, metafile normalization, cache naming, and inventory walks.

use std::{borrow::Cow, str};

use sha1::{Digest, Sha1};

use crate::{
    SporosError,
    domain::{ClientLabel, File, InfoHash, Metafile},
};

type DictEntry<'a> = (Cow<'a, [u8]>, Bencode<'a>);
type DictEntries<'a> = [DictEntry<'a>];

/// Borrowed bencode value with its original byte span.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Bencode<'a> {
    /// Parsed value.
    pub value: BencodeValue<'a>,
    /// Start byte offset in the original input.
    pub start: usize,
    /// End byte offset in the original input.
    pub end: usize,
}

/// Bencode value variants.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum BencodeValue<'a> {
    /// Integer value.
    Integer(i64),
    /// Raw byte string.
    Bytes(Cow<'a, [u8]>),
    /// Ordered list.
    List(Vec<Bencode<'a>>),
    /// Ordered dictionary entries.
    Dict(Vec<DictEntry<'a>>),
}

impl<'a> Bencode<'a> {
    /// Construct an owned byte string value.
    pub fn bytes(bytes: impl Into<Cow<'a, [u8]>>) -> Self {
        Self {
            value: BencodeValue::Bytes(bytes.into()),
            start: 0,
            end: 0,
        }
    }

    /// Construct an integer value.
    pub const fn integer(value: i64) -> Self {
        Self {
            value: BencodeValue::Integer(value),
            start: 0,
            end: 0,
        }
    }

    /// Construct a list value.
    pub fn list(items: Vec<Bencode<'a>>) -> Self {
        Self {
            value: BencodeValue::List(items),
            start: 0,
            end: 0,
        }
    }

    /// Construct a dictionary value.
    pub fn dict(entries: Vec<DictEntry<'a>>) -> Self {
        Self {
            value: BencodeValue::Dict(entries),
            start: 0,
            end: 0,
        }
    }

    fn as_dict(&self) -> Option<&DictEntries<'a>> {
        match &self.value {
            BencodeValue::Dict(entries) => Some(entries),
            _ => None,
        }
    }

    fn as_bytes(&self) -> Option<&[u8]> {
        match &self.value {
            BencodeValue::Bytes(bytes) => Some(bytes.as_ref()),
            _ => None,
        }
    }

    fn as_integer(&self) -> Option<i64> {
        match self.value {
            BencodeValue::Integer(value) => Some(value),
            _ => None,
        }
    }
}

/// Decode one complete bencode value.
pub fn bdecode(input: &[u8]) -> crate::Result<Bencode<'_>> {
    let mut parser = Parser { input, offset: 0 };
    let value = parser.parse_value()?;

    if parser.offset == input.len() {
        Ok(value)
    } else {
        Err(torrent_error(format!(
            "trailing bencode data at byte {}",
            parser.offset
        )))
    }
}

/// Encode a bencode value, preserving dictionary entry order.
pub fn bencode(value: &Bencode<'_>) -> Vec<u8> {
    let mut output = Vec::new();
    encode_into(value, &mut output);
    output
}

/// Parse a `.torrent` file and normalize it into a [`Metafile`].
pub fn parse_metafile(input: &[u8]) -> crate::Result<Metafile<'static>> {
    let decoded = bdecode(input)?;
    let root = decoded
        .as_dict()
        .ok_or_else(|| torrent_error("torrent root must be a dictionary"))?;
    let info = dict_get(root, b"info").ok_or_else(|| torrent_error("torrent missing info"))?;
    let info_dict = info
        .as_dict()
        .ok_or_else(|| torrent_error("torrent info must be a dictionary"))?;

    let info_bytes = input
        .get(info.start..info.end)
        .ok_or_else(|| torrent_error("invalid info dictionary byte range"))?;
    let info_hash = InfoHash::from_validated(hex_sha1(info_bytes));
    let name = required_utf8(dict_get(info_dict, b"name"), "info.name")?;
    let piece_length =
        required_nonnegative_integer(dict_get(info_dict, b"piece length"), "info.piece length")?;
    let files = normalized_files(name.as_ref(), info_dict)?;
    let trackers = tracker_hosts(root);

    let mut metafile = Metafile::from_files(info_hash, name.clone(), name, piece_length, files);
    metafile.trackers = trackers;

    Ok(metafile.into_owned())
}

/// Apply qBittorrent fastresume metadata to an already parsed metafile.
pub fn apply_qbittorrent_fastresume_metadata(
    metafile: &mut Metafile<'static>,
    input: &[u8],
) -> crate::Result<()> {
    let decoded = bdecode(input)?;
    let root = decoded
        .as_dict()
        .ok_or_else(|| torrent_error("fastresume root must be a dictionary"))?;

    if let Some(category) = optional_utf8(dict_get(root, b"qBt-category"))? {
        metafile.category = Some(ClientLabel::new(category.into_owned()));
    }

    if let Some(tags) = dict_get(root, b"qBt-tags") {
        metafile.tags = fastresume_tags(tags)?;
    }

    let trackers = tracker_hosts(root);
    if !trackers.is_empty() {
        metafile.trackers = trackers;
    }

    Ok(())
}

struct Parser<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> Parser<'a> {
    fn parse_value(&mut self) -> crate::Result<Bencode<'a>> {
        let byte = self
            .peek()
            .ok_or_else(|| torrent_error("unexpected end of bencode input"))?;

        match byte {
            b'i' => self.parse_integer(),
            b'l' => self.parse_list(),
            b'd' => self.parse_dict(),
            b'0'..=b'9' => self.parse_bytes(),
            _ => Err(torrent_error(format!(
                "invalid bencode byte '{}' at byte {}",
                char::from(byte),
                self.offset
            ))),
        }
    }

    fn parse_integer(&mut self) -> crate::Result<Bencode<'a>> {
        let start = self.offset;
        self.offset += 1;
        let value_start = self.offset;

        while self.peek().is_some_and(|byte| byte != b'e') {
            self.offset += 1;
        }

        self.expect(b'e')?;
        let value_bytes = self.slice(value_start, self.offset - 1)?;
        let value_text = str::from_utf8(value_bytes)
            .map_err(|_error| torrent_error(format!("integer at byte {start} is not utf-8")))?;
        let value = value_text
            .parse::<i64>()
            .map_err(|_error| torrent_error(format!("invalid integer at byte {start}")))?;

        Ok(Bencode {
            value: BencodeValue::Integer(value),
            start,
            end: self.offset,
        })
    }

    fn parse_bytes(&mut self) -> crate::Result<Bencode<'a>> {
        let start = self.offset;
        let length_start = self.offset;

        while self.peek().is_some_and(|byte| byte.is_ascii_digit()) {
            self.offset += 1;
        }

        self.expect(b':')?;
        let length_bytes = self.slice(length_start, self.offset - 1)?;
        let length_text = str::from_utf8(length_bytes).map_err(|_error| {
            torrent_error(format!("byte string length at byte {start} is not utf-8"))
        })?;
        let length = length_text.parse::<usize>().map_err(|_error| {
            torrent_error(format!("invalid byte string length at byte {start}"))
        })?;
        let value_start = self.offset;
        let end = value_start
            .checked_add(length)
            .ok_or_else(|| torrent_error("byte string length overflow"))?;

        if end > self.input.len() {
            return Err(torrent_error(format!(
                "byte string at byte {start} extends past input"
            )));
        }

        let bytes = self.slice(value_start, end)?;
        self.offset = end;
        Ok(Bencode {
            value: BencodeValue::Bytes(Cow::Borrowed(bytes)),
            start,
            end,
        })
    }

    fn parse_list(&mut self) -> crate::Result<Bencode<'a>> {
        let start = self.offset;
        self.offset += 1;
        let mut items = Vec::new();

        while self.peek().is_some_and(|byte| byte != b'e') {
            items.push(self.parse_value()?);
        }

        self.expect(b'e')?;
        Ok(Bencode {
            value: BencodeValue::List(items),
            start,
            end: self.offset,
        })
    }

    fn parse_dict(&mut self) -> crate::Result<Bencode<'a>> {
        let start = self.offset;
        self.offset += 1;
        let mut entries = Vec::new();

        while self.peek().is_some_and(|byte| byte != b'e') {
            let key = self.parse_bytes()?;
            let key = match key.value {
                BencodeValue::Bytes(bytes) => bytes,
                _ => return Err(torrent_error("dictionary key must be bytes")),
            };
            let value = self.parse_value()?;
            entries.push((key, value));
        }

        self.expect(b'e')?;
        Ok(Bencode {
            value: BencodeValue::Dict(entries),
            start,
            end: self.offset,
        })
    }

    fn peek(&self) -> Option<u8> {
        self.input.get(self.offset).copied()
    }

    fn expect(&mut self, expected: u8) -> crate::Result<()> {
        match self.peek() {
            Some(actual) if actual == expected => {
                self.offset += 1;
                Ok(())
            }
            Some(actual) => Err(torrent_error(format!(
                "expected '{}' at byte {}, found '{}'",
                char::from(expected),
                self.offset,
                char::from(actual)
            ))),
            None => Err(torrent_error(format!(
                "expected '{}' at end of input",
                char::from(expected)
            ))),
        }
    }

    fn slice(&self, start: usize, end: usize) -> crate::Result<&'a [u8]> {
        self.input
            .get(start..end)
            .ok_or_else(|| torrent_error("invalid bencode byte range"))
    }
}

fn encode_into(value: &Bencode<'_>, output: &mut Vec<u8>) {
    match &value.value {
        BencodeValue::Integer(integer) => {
            output.extend_from_slice(b"i");
            output.extend_from_slice(integer.to_string().as_bytes());
            output.extend_from_slice(b"e");
        }
        BencodeValue::Bytes(bytes) => {
            output.extend_from_slice(bytes.len().to_string().as_bytes());
            output.extend_from_slice(b":");
            output.extend_from_slice(bytes.as_ref());
        }
        BencodeValue::List(items) => {
            output.extend_from_slice(b"l");
            for item in items {
                encode_into(item, output);
            }
            output.extend_from_slice(b"e");
        }
        BencodeValue::Dict(entries) => {
            output.extend_from_slice(b"d");
            for (key, value) in entries {
                output.extend_from_slice(key.len().to_string().as_bytes());
                output.extend_from_slice(b":");
                output.extend_from_slice(key.as_ref());
                encode_into(value, output);
            }
            output.extend_from_slice(b"e");
        }
    }
}

fn normalized_files(name: &str, info: &DictEntries<'_>) -> crate::Result<Vec<File<'static>>> {
    if let Some(files_value) = dict_get(info, b"files") {
        let BencodeValue::List(file_entries) = &files_value.value else {
            return Err(torrent_error("info.files must be a list"));
        };

        let mut files = Vec::with_capacity(file_entries.len());
        for file_entry in file_entries {
            let file_dict = file_entry
                .as_dict()
                .ok_or_else(|| torrent_error("info.files entry must be a dictionary"))?;
            let length =
                required_nonnegative_integer(dict_get(file_dict, b"length"), "file.length")?;
            let path_value = dict_get(file_dict, b"path")
                .or_else(|| dict_get(file_dict, b"path.utf-8"))
                .ok_or_else(|| torrent_error("file missing path"))?;
            let path_segments = path_segments(path_value)?;
            let mut path = String::with_capacity(
                name.len()
                    + path_segments.iter().map(String::len).sum::<usize>()
                    + path_segments.len(),
            );
            path.push_str(name);
            for segment in path_segments {
                path.push('/');
                path.push_str(&segment);
            }
            files.push(File::new(path, length));
        }

        files.sort_by(|left, right| left.path.cmp(&right.path));
        Ok(files)
    } else {
        let length = required_nonnegative_integer(dict_get(info, b"length"), "info.length")?;
        Ok(vec![File::new(name.to_owned(), length)])
    }
}

fn path_segments(value: &Bencode<'_>) -> crate::Result<Vec<String>> {
    let BencodeValue::List(segments) = &value.value else {
        return Err(torrent_error("file path must be a list"));
    };

    let mut normalized = Vec::with_capacity(segments.len());
    for segment in segments {
        let bytes = segment
            .as_bytes()
            .ok_or_else(|| torrent_error("file path segment must be bytes"))?;
        let segment = String::from_utf8_lossy(bytes);
        if segment.is_empty() {
            normalized.push("_".to_owned());
        } else {
            normalized.push(segment.into_owned());
        }
    }

    Ok(normalized)
}

fn tracker_hosts(root: &DictEntries<'_>) -> Vec<Cow<'static, str>> {
    let mut trackers = Vec::new();

    if let Some(announce_list) = dict_get(root, b"announce-list") {
        collect_tracker_hosts(announce_list, &mut trackers);
    }
    if let Some(trackers_value) = dict_get(root, b"trackers") {
        collect_tracker_hosts(trackers_value, &mut trackers);
    }
    if let Some(announce) = dict_get(root, b"announce") {
        collect_tracker_hosts(announce, &mut trackers);
    }

    trackers.dedup();
    trackers
}

fn collect_tracker_hosts(value: &Bencode<'_>, trackers: &mut Vec<Cow<'static, str>>) {
    match &value.value {
        BencodeValue::Bytes(bytes) => {
            if let Some(host) = sanitize_tracker_host(bytes.as_ref()) {
                if !trackers.iter().any(|existing| existing.as_ref() == host) {
                    trackers.push(Cow::Owned(host));
                }
            }
        }
        BencodeValue::List(items) => {
            for item in items {
                collect_tracker_hosts(item, trackers);
            }
        }
        BencodeValue::Integer(_) | BencodeValue::Dict(_) => {}
    }
}

fn sanitize_tracker_host(bytes: &[u8]) -> Option<String> {
    let text = str::from_utf8(bytes).ok()?.trim();
    let without_scheme = text.split_once("://").map_or(text, |(_, rest)| rest);
    let without_credentials = without_scheme
        .rsplit_once('@')
        .map_or(without_scheme, |(_, rest)| rest);
    let host_port = without_credentials
        .split(['/', '?', '#'])
        .next()
        .unwrap_or_default();
    let host = host_port
        .strip_prefix('[')
        .and_then(|value| value.split_once(']').map(|(host, _)| host))
        .unwrap_or_else(|| host_port.split(':').next().unwrap_or_default())
        .trim()
        .to_ascii_lowercase();

    if host.is_empty() { None } else { Some(host) }
}

fn fastresume_tags(value: &Bencode<'_>) -> crate::Result<Vec<ClientLabel<'static>>> {
    match &value.value {
        BencodeValue::Bytes(bytes) => {
            let text = String::from_utf8_lossy(bytes);
            Ok(text
                .split(',')
                .map(str::trim)
                .filter(|tag| !tag.is_empty())
                .map(|tag| ClientLabel::new(tag.to_owned()))
                .collect())
        }
        BencodeValue::List(items) => {
            let mut tags = Vec::with_capacity(items.len());
            for item in items {
                if let Some(tag) = optional_utf8(Some(item))? {
                    if !tag.trim().is_empty() {
                        tags.push(ClientLabel::new(tag.trim().to_owned()));
                    }
                }
            }
            Ok(tags)
        }
        BencodeValue::Integer(_) | BencodeValue::Dict(_) => Err(torrent_error(
            "fastresume qBt-tags must be bytes or a list of bytes",
        )),
    }
}

fn dict_get<'a>(entries: &'a DictEntries<'a>, key: &[u8]) -> Option<&'a Bencode<'a>> {
    entries
        .iter()
        .find_map(|(entry_key, value)| (entry_key.as_ref() == key).then_some(value))
}

fn required_utf8(value: Option<&Bencode<'_>>, field: &str) -> crate::Result<Cow<'static, str>> {
    optional_utf8(value)?.ok_or_else(|| torrent_error(format!("torrent missing {field}")))
}

fn optional_utf8(value: Option<&Bencode<'_>>) -> crate::Result<Option<Cow<'static, str>>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let bytes = value
        .as_bytes()
        .ok_or_else(|| torrent_error("expected byte string"))?;

    Ok(Some(Cow::Owned(
        String::from_utf8_lossy(bytes).into_owned(),
    )))
}

fn required_nonnegative_integer(value: Option<&Bencode<'_>>, field: &str) -> crate::Result<u64> {
    let integer = value
        .and_then(Bencode::as_integer)
        .ok_or_else(|| torrent_error(format!("torrent missing {field}")))?;
    u64::try_from(integer).map_err(|_error| torrent_error(format!("{field} must be nonnegative")))
}

fn hex_sha1(bytes: &[u8]) -> String {
    let digest = Sha1::digest(bytes);
    let mut output = String::with_capacity(40);
    for byte in digest {
        output.push(hex_digit(byte >> 4));
        output.push(hex_digit(byte & 0x0f));
    }
    output
}

fn hex_digit(nibble: u8) -> char {
    match nibble {
        0..=9 => char::from(b'0' + nibble),
        _ => char::from(b'a' + (nibble - 10)),
    }
}

fn torrent_error(message: impl Into<Cow<'static, str>>) -> SporosError {
    SporosError::Torrent {
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::{Bencode, apply_qbittorrent_fastresume_metadata, bdecode, bencode, parse_metafile};
    use std::borrow::Cow;

    #[test]
    fn bdecode_tracks_original_spans_and_bencode_round_trips() {
        let decoded = bdecode(b"d4:infod4:name4:Teste5:otheri1ee").expect("valid bencode");
        let root = decoded.as_dict().expect("root dict");
        let info = root
            .iter()
            .find_map(|(key, value)| (key.as_ref() == b"info").then_some(value))
            .expect("info dict");

        assert_eq!(&b"d4:name4:Teste"[..], &b"d4:name4:Teste"[..]);
        assert_eq!(
            &b"d4:infod4:name4:Teste5:otheri1ee"[info.start..info.end],
            b"d4:name4:Teste"
        );
        assert_eq!(bencode(&decoded), b"d4:infod4:name4:Teste5:otheri1ee");
    }

    #[test]
    fn parse_single_file_torrent_normalizes_metafile() {
        let input = include_bytes!("../tests/fixtures/memory/torrents/representative.torrent");
        let input = input.strip_suffix(b"\n").unwrap_or(input);
        let metafile = parse_metafile(input).expect("fixture parses");

        assert_eq!(metafile.name, "Example.Release.2024.1080p");
        assert_eq!(metafile.title, "Example.Release.2024.1080p");
        assert_eq!(metafile.length, 123_456);
        assert_eq!(metafile.piece_length, 262_144);
        assert_eq!(metafile.files.len(), 1);
        assert_eq!(metafile.files[0].path, "Example.Release.2024.1080p");
        assert_eq!(metafile.trackers, vec![Cow::Borrowed("tracker.example")]);
        assert_eq!(
            metafile.info_hash.as_str(),
            "af64c6808eef72403c2491c27b52b04b0771ba5b"
        );
    }

    #[test]
    fn parse_multi_file_torrent_prefixes_name_and_sorts_paths() {
        let input = b"d13:announce-listll28:https://one.example/announceel28:https://two.example/announceee4:infod5:filesld6:lengthi20e4:pathl6:Season6:02.mkveed6:lengthi10e4:pathl0:6:01.mkveee4:name12:Example.Show12:piece lengthi16384e6:pieces20:aaaaaaaaaaaaaaaaaaaaee";
        let metafile = parse_metafile(input).expect("torrent parses");

        assert_eq!(metafile.files.len(), 2);
        assert_eq!(metafile.files[0].path, "Example.Show/Season/02.mkv");
        assert_eq!(metafile.files[0].length, 20);
        assert_eq!(metafile.files[1].path, "Example.Show/_/01.mkv");
        assert_eq!(metafile.length, 30);
        assert_eq!(
            metafile.trackers,
            vec![Cow::Borrowed("one.example"), Cow::Borrowed("two.example")]
        );
    }

    #[test]
    fn fastresume_metadata_updates_category_tags_and_trackers() {
        let input =
            b"d4:infod6:lengthi1e4:name4:Test12:piece lengthi1e6:pieces20:aaaaaaaaaaaaaaaaaaaaee";
        let mut metafile = parse_metafile(input).expect("torrent parses");
        let fastresume = b"d8:trackersl32:https://tracker.example/announcee12:qBt-category2:tv8:qBt-tags14:cross-seed, 4ke";

        apply_qbittorrent_fastresume_metadata(&mut metafile, fastresume)
            .expect("fastresume parses");

        assert_eq!(metafile.category.expect("category").as_str(), "tv");
        assert_eq!(metafile.tags.len(), 2);
        assert_eq!(metafile.tags[0].as_str(), "cross-seed");
        assert_eq!(metafile.tags[1].as_str(), "4k");
        assert_eq!(metafile.trackers, vec![Cow::Borrowed("tracker.example")]);
    }

    #[test]
    fn bencode_can_encode_constructed_values() {
        let value = Bencode::dict(vec![
            (
                Cow::Borrowed(b"cow".as_slice()),
                Bencode::bytes(Cow::Borrowed(b"moo".as_slice())),
            ),
            (Cow::Borrowed(b"n".as_slice()), Bencode::integer(7)),
        ]);

        assert_eq!(bencode(&value), b"d3:cow3:moo1:ni7ee");
    }
}

#[cfg(test)]
mod fuzz_tests {
    use super::{Bencode, BencodeValue, bdecode, bencode, parse_metafile};
    use proptest::{collection, prelude::*};
    use std::borrow::Cow;

    fn arb_bencode(depth: u32) -> BoxedStrategy<Bencode<'static>> {
        let leaf = prop_oneof![
            any::<i64>().prop_map(Bencode::integer),
            collection::vec(any::<u8>(), 0..64).prop_map(|bytes| Bencode::bytes(Cow::Owned(bytes))),
        ];

        leaf.prop_recursive(depth, 64, 8, |inner| {
            prop_oneof![
                collection::vec(inner.clone(), 0..8).prop_map(Bencode::list),
                collection::vec((collection::vec(any::<u8>(), 0..16), inner), 0..8).prop_map(
                    |entries| Bencode::dict(
                        entries
                            .into_iter()
                            .map(|(key, value)| (Cow::Owned(key), value))
                            .collect(),
                    ),
                ),
            ]
        })
        .boxed()
    }

    fn arb_release_name() -> impl Strategy<Value = String> {
        "[A-Za-z0-9._ -]{1,32}"
    }

    fn arb_path_segments() -> impl Strategy<Value = Vec<String>> {
        collection::vec("[A-Za-z0-9._ -]{0,16}", 1..5)
    }

    fn same_bencode_value(left: &Bencode<'_>, right: &Bencode<'_>) -> bool {
        match (&left.value, &right.value) {
            (BencodeValue::Integer(left), BencodeValue::Integer(right)) => left == right,
            (BencodeValue::Bytes(left), BencodeValue::Bytes(right)) => {
                left.as_ref() == right.as_ref()
            }
            (BencodeValue::List(left), BencodeValue::List(right)) => {
                left.len() == right.len()
                    && left
                        .iter()
                        .zip(right)
                        .all(|(left, right)| same_bencode_value(left, right))
            }
            (BencodeValue::Dict(left), BencodeValue::Dict(right)) => {
                left.len() == right.len()
                    && left.iter().zip(right).all(
                        |((left_key, left_value), (right_key, right_value))| {
                            left_key.as_ref() == right_key.as_ref()
                                && same_bencode_value(left_value, right_value)
                        },
                    )
            }
            _ => false,
        }
    }

    fn torrent_with_files(name: String, files: Vec<(u64, Vec<String>)>) -> Bencode<'static> {
        let file_entries = files
            .into_iter()
            .map(|(length, segments)| {
                Bencode::dict(vec![
                    (
                        Cow::Borrowed(b"length".as_slice()),
                        Bencode::integer(length as i64),
                    ),
                    (
                        Cow::Borrowed(b"path".as_slice()),
                        Bencode::list(
                            segments
                                .into_iter()
                                .map(|segment| Bencode::bytes(Cow::Owned(segment.into_bytes())))
                                .collect(),
                        ),
                    ),
                ])
            })
            .collect();

        Bencode::dict(vec![
            (
                Cow::Borrowed(b"announce".as_slice()),
                Bencode::bytes(Cow::Borrowed(
                    b"https://tracker.example/announce".as_slice(),
                )),
            ),
            (
                Cow::Borrowed(b"info".as_slice()),
                Bencode::dict(vec![
                    (
                        Cow::Borrowed(b"files".as_slice()),
                        Bencode::list(file_entries),
                    ),
                    (
                        Cow::Borrowed(b"name".as_slice()),
                        Bencode::bytes(Cow::Owned(name.into_bytes())),
                    ),
                    (
                        Cow::Borrowed(b"piece length".as_slice()),
                        Bencode::integer(16_384),
                    ),
                    (
                        Cow::Borrowed(b"pieces".as_slice()),
                        Bencode::bytes(Cow::Owned(vec![b'a'; 20])),
                    ),
                ]),
            ),
        ])
    }

    proptest! {
        #[test]
        fn arbitrary_remote_bytes_do_not_panic(input in collection::vec(any::<u8>(), 0..4096)) {
            let _result = bdecode(&input);
        }

        #[test]
        fn generated_bencode_round_trips(value in arb_bencode(4)) {
            let encoded = bencode(&value);
            let decoded = bdecode(&encoded).expect("generated bencode should decode");

            prop_assert!(same_bencode_value(&value, &decoded));
            prop_assert_eq!(bencode(&decoded), encoded);
        }

        #[test]
        fn generated_torrent_metafiles_normalize_paths(
            name in arb_release_name(),
            files in collection::vec((0_u64..1_000_000, arb_path_segments()), 1..16),
        ) {
            let torrent = torrent_with_files(name.clone(), files);
            let encoded = bencode(&torrent);
            let metafile = parse_metafile(&encoded).expect("generated torrent should parse");

            prop_assert!(!metafile.files.is_empty());
            prop_assert_eq!(metafile.length, metafile.files.iter().map(|file| file.length).sum::<u64>());
            prop_assert!(metafile.files.windows(2).all(|window| window[0].path <= window[1].path));
            prop_assert!(metafile.files.iter().all(|file| file.path.starts_with(&name)));
            prop_assert!(metafile.files.iter().all(|file| !file.path.contains("//")));
        }
    }
}
