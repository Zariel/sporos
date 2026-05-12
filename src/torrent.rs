use std::collections::BTreeSet;
use std::path::{Component, Path, PathBuf};
use std::str;

use bendy::decoding::{Decoder, DictDecoder, Error as BencodeError, ListDecoder, Object};
use sha1::{Digest, Sha1};

use crate::domain::{
    ByteSize, DisplayName, FileIndex, InfoHash, TorrentFile, TorrentMetafile, TrackerName,
};
use crate::errors::TorrentParseError;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ParsedMetafile {
    pub metafile: TorrentMetafile,
    pub tracker_hosts: Vec<TrackerName>,
}

pub fn parse_metafile(bytes: &[u8]) -> Result<ParsedMetafile, TorrentParseError> {
    let mut decoder = Decoder::new(bytes).with_max_depth(16);

    let (info_raw, tracker_urls) = {
        let root = decoder
            .next_object()
            .map_err(bencode_error)?
            .ok_or_else(|| invalid_bencode("torrent file is empty"))?;

        match root {
            Object::Dict(root) => parse_root(root)?,
            _ => return Err(invalid_bencode("torrent root must be a dictionary")),
        }
    };

    if decoder.next_object().map_err(bencode_error)?.is_some() {
        return Err(invalid_bencode("torrent file contains trailing bencode"));
    }

    let info_hash = info_hash(&info_raw)?;
    let (name, files) = parse_info(&info_raw)?;
    let metafile = TorrentMetafile::new(
        info_hash,
        DisplayName::new(name).map_err(|source| TorrentParseError::InvalidMetafile { source })?,
        files,
    )
    .map_err(|source| TorrentParseError::InvalidMetafile { source })?;
    let tracker_hosts = tracker_hosts(&tracker_urls)?;

    Ok(ParsedMetafile {
        metafile,
        tracker_hosts,
    })
}

fn parse_root(mut root: DictDecoder<'_, '_>) -> Result<(Vec<u8>, Vec<String>), TorrentParseError> {
    let mut info_raw = None;
    let mut tracker_urls = Vec::new();

    while let Some((key, value)) = root.next_pair().map_err(bencode_error)? {
        match key {
            b"announce" => tracker_urls.push(bytes_to_string(bytes_object(value)?)?),
            b"announce-list" => parse_announce_list(list_object(value)?, &mut tracker_urls)?,
            b"info" => {
                let info = dict_object(value)?;
                info_raw = Some(info.into_raw().map_err(bencode_error)?.to_vec());
            }
            _ => {}
        }
    }

    let info_raw = info_raw.ok_or(TorrentParseError::MissingInfoDictionary)?;
    Ok((info_raw, tracker_urls))
}

fn parse_announce_list(
    mut tiers: ListDecoder<'_, '_>,
    tracker_urls: &mut Vec<String>,
) -> Result<(), TorrentParseError> {
    while let Some(tier) = tiers.next_object().map_err(bencode_error)? {
        match tier {
            Object::List(mut tier) => {
                while let Some(url) = tier.next_object().map_err(bencode_error)? {
                    tracker_urls.push(bytes_to_string(bytes_object(url)?)?);
                }
            }
            Object::Bytes(url) => tracker_urls.push(bytes_to_string(url)?),
            _ => {
                return Err(invalid_bencode(
                    "announce-list entries must be strings or lists",
                ));
            }
        }
    }

    Ok(())
}

fn parse_info(bytes: &[u8]) -> Result<(String, Vec<TorrentFile>), TorrentParseError> {
    let mut decoder = Decoder::new(bytes).with_max_depth(16);
    let info = decoder
        .next_object()
        .map_err(bencode_error)?
        .ok_or(TorrentParseError::MissingInfoDictionary)?;

    let Object::Dict(mut info) = info else {
        return Err(TorrentParseError::MissingInfoDictionary);
    };

    let mut name = None;
    let mut single_file_length = None;
    let mut multi_file_entries = None;

    while let Some((key, value)) = info.next_pair().map_err(bencode_error)? {
        match key {
            b"name" => name = Some(bytes_to_string(bytes_object(value)?)?),
            b"length" => single_file_length = Some(parse_u64(integer_object(value)?)?),
            b"files" => multi_file_entries = Some(parse_files(list_object(value)?)?),
            _ => {}
        }
    }

    let name = name.ok_or_else(|| unsupported_layout("torrent info dictionary is missing name"))?;
    let mut files = if let Some(files) = multi_file_entries {
        files
    } else {
        let length = single_file_length
            .ok_or_else(|| unsupported_layout("single-file torrent is missing length"))?;
        vec![
            TorrentFile::new(
                normalized_relative_path([name.as_str()])?,
                ByteSize::new(length),
                FileIndex::new(0),
            )
            .map_err(|source| TorrentParseError::InvalidMetafile { source })?,
        ]
    };

    files.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    Ok((name, files))
}

fn parse_files(mut files: ListDecoder<'_, '_>) -> Result<Vec<TorrentFile>, TorrentParseError> {
    let mut parsed = Vec::new();
    let mut file_index = 0;

    while let Some(file) = files.next_object().map_err(bencode_error)? {
        let Object::Dict(file) = file else {
            return Err(unsupported_layout(
                "multi-file entries must be dictionaries",
            ));
        };
        parsed.push(parse_file(file, FileIndex::new(file_index))?);
        file_index += 1;
    }

    if parsed.is_empty() {
        return Err(unsupported_layout("multi-file torrent has no files"));
    }

    Ok(parsed)
}

fn parse_file(
    mut file: DictDecoder<'_, '_>,
    file_index: FileIndex,
) -> Result<TorrentFile, TorrentParseError> {
    let mut length = None;
    let mut path = None;

    while let Some((key, value)) = file.next_pair().map_err(bencode_error)? {
        match key {
            b"length" => length = Some(parse_u64(integer_object(value)?)?),
            b"path" => path = Some(parse_path_list(list_object(value)?)?),
            _ => {}
        }
    }

    let length = length.ok_or_else(|| unsupported_layout("file entry is missing length"))?;
    let path = path.ok_or_else(|| unsupported_layout("file entry is missing path"))?;

    TorrentFile::new(path, ByteSize::new(length), file_index)
        .map_err(|source| TorrentParseError::InvalidMetafile { source })
}

fn parse_path_list(mut path: ListDecoder<'_, '_>) -> Result<PathBuf, TorrentParseError> {
    let mut segments = Vec::new();

    while let Some(segment) = path.next_object().map_err(bencode_error)? {
        segments.push(bytes_to_string(bytes_object(segment)?)?);
    }

    normalized_relative_path(segments.iter().map(String::as_str))
}

fn normalized_relative_path<'a>(
    segments: impl IntoIterator<Item = &'a str>,
) -> Result<PathBuf, TorrentParseError> {
    let mut normalized = PathBuf::new();

    for segment in segments {
        if segment.trim().is_empty() {
            return Err(unsupported_layout("torrent path contains an empty segment"));
        }

        let path = Path::new(segment);
        if path.is_absolute() {
            return Err(unsupported_layout("torrent path must be relative"));
        }

        for component in path.components() {
            match component {
                Component::Normal(part) => normalized.push(part),
                Component::CurDir
                | Component::ParentDir
                | Component::RootDir
                | Component::Prefix(_) => {
                    return Err(unsupported_layout(
                        "torrent path contains an unsafe segment",
                    ));
                }
            }
        }
    }

    if normalized.as_os_str().is_empty() {
        return Err(unsupported_layout("torrent path must not be empty"));
    }

    Ok(normalized)
}

fn tracker_hosts(urls: &[String]) -> Result<Vec<TrackerName>, TorrentParseError> {
    let hosts = urls
        .iter()
        .filter_map(|url| tracker_host(url))
        .collect::<BTreeSet<_>>();

    hosts
        .into_iter()
        .map(TrackerName::new)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| TorrentParseError::InvalidMetafile { source })
}

fn tracker_host(url: &str) -> Option<String> {
    let trimmed = url.trim();
    if trimmed.is_empty() {
        return None;
    }

    let after_scheme = trimmed
        .split_once("://")
        .map_or(trimmed, |(_scheme, rest)| rest);
    let authority = after_scheme
        .split(['/', '?', '#'])
        .next()
        .unwrap_or_default();
    let without_userinfo = authority
        .rsplit_once('@')
        .map_or(authority, |(_userinfo, host)| host);
    let host = strip_port(without_userinfo)
        .trim()
        .trim_matches('.')
        .to_ascii_lowercase();

    if host.is_empty() { None } else { Some(host) }
}

fn strip_port(authority: &str) -> &str {
    if authority.starts_with('[') {
        return authority
            .split_once(']')
            .map_or(authority, |(host, _port)| host.trim_start_matches('['));
    }

    authority
        .split_once(':')
        .map_or(authority, |(host, _port)| host)
}

fn info_hash(info_raw: &[u8]) -> Result<InfoHash, TorrentParseError> {
    let mut hasher = Sha1::new();
    hasher.update(info_raw);
    let digest = hasher.finalize();
    let mut hex = String::with_capacity(40);
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut hex, "{byte:02x}").map_err(|error| TorrentParseError::InvalidBencode {
            message: error.to_string(),
        })?;
    }

    InfoHash::new(hex).map_err(|source| TorrentParseError::InvalidInfoHash { source })
}

fn bytes_object<'a>(value: Object<'_, 'a>) -> Result<&'a [u8], TorrentParseError> {
    match value {
        Object::Bytes(bytes) => Ok(bytes),
        _ => Err(invalid_bencode("expected bencode byte string")),
    }
}

fn integer_object<'a>(value: Object<'_, 'a>) -> Result<&'a str, TorrentParseError> {
    match value {
        Object::Integer(integer) => Ok(integer),
        _ => Err(invalid_bencode("expected bencode integer")),
    }
}

fn list_object<'obj, 'ser>(
    value: Object<'obj, 'ser>,
) -> Result<ListDecoder<'obj, 'ser>, TorrentParseError> {
    match value {
        Object::List(list) => Ok(list),
        _ => Err(invalid_bencode("expected bencode list")),
    }
}

fn dict_object<'obj, 'ser>(
    value: Object<'obj, 'ser>,
) -> Result<DictDecoder<'obj, 'ser>, TorrentParseError> {
    match value {
        Object::Dict(dict) => Ok(dict),
        _ => Err(invalid_bencode("expected bencode dictionary")),
    }
}

fn bytes_to_string(bytes: &[u8]) -> Result<String, TorrentParseError> {
    str::from_utf8(bytes)
        .map(str::to_owned)
        .map_err(|error| unsupported_layout(format!("torrent contains non-UTF-8 text: {error}")))
}

fn parse_u64(integer: &str) -> Result<u64, TorrentParseError> {
    integer.parse::<u64>().map_err(|error| {
        unsupported_layout(format!(
            "torrent integer must be an unsigned 64-bit value: {error}"
        ))
    })
}

fn bencode_error(error: BencodeError) -> TorrentParseError {
    TorrentParseError::InvalidBencode {
        message: error.to_string(),
    }
}

fn invalid_bencode(message: impl Into<String>) -> TorrentParseError {
    TorrentParseError::InvalidBencode {
        message: message.into(),
    }
}

fn unsupported_layout(message: impl Into<String>) -> TorrentParseError {
    TorrentParseError::UnsupportedLayout {
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_single_file_torrent_and_hashes_info_dictionary() {
        let first =
            b"d8:announce32:https://tracker.example/announce4:infod6:lengthi12e4:name9:movie.mkvee";
        let second = b"d8:announce34:https://other.example:443/announce4:infod6:lengthi12e4:name9:movie.mkvee";

        let first = parse_metafile(first).unwrap();
        let second = parse_metafile(second).unwrap();

        assert_eq!(first.metafile.info_hash, second.metafile.info_hash);
        assert_eq!("movie.mkv", first.metafile.name.as_str());
        assert_eq!(12, first.metafile.total_size.get());
        assert_eq!(
            PathBuf::from("movie.mkv"),
            first.metafile.files[0].relative_path
        );
        assert_eq!(vec!["tracker.example"], tracker_strings(&first));
        assert_eq!(vec!["other.example"], tracker_strings(&second));
    }

    #[test]
    fn parses_multi_file_torrent_with_sorted_normalized_files_and_tracker_hosts() {
        let torrent = b"d13:announce-listll34:https://tracker-b.example/announceel28:udp://tracker-a.example:6969ee4:infod5:filesld6:lengthi5e4:pathl5:z.mkveed6:lengthi7e4:pathl5:a.mkveee4:name7:Exampleee";

        let parsed = parse_metafile(torrent).unwrap();

        assert_eq!(12, parsed.metafile.total_size.get());
        assert_eq!(
            PathBuf::from("a.mkv"),
            parsed.metafile.files[0].relative_path
        );
        assert_eq!(
            PathBuf::from("z.mkv"),
            parsed.metafile.files[1].relative_path
        );
        assert_eq!(1, parsed.metafile.files[0].file_index.get());
        assert_eq!(0, parsed.metafile.files[1].file_index.get());
        assert_eq!(
            vec!["tracker-a.example", "tracker-b.example"],
            tracker_strings(&parsed)
        );
    }

    #[test]
    fn rejects_empty_and_unsafe_path_segments() {
        let empty = b"d4:infod5:filesld6:lengthi1e4:pathl0:eee4:name4:bad eee";
        let parent = b"d4:infod5:filesld6:lengthi1e4:pathl2:..8:file.mkveee4:name4:bad eee";

        parse_metafile(empty).unwrap_err();
        parse_metafile(parent).unwrap_err();
    }

    #[test]
    fn rejects_malformed_inputs() {
        parse_metafile(b"not bencode").unwrap_err();
        parse_metafile(b"d4:infod4:name5:emptyee").unwrap_err();
    }

    fn tracker_strings(parsed: &ParsedMetafile) -> Vec<&str> {
        parsed
            .tracker_hosts
            .iter()
            .map(TrackerName::as_str)
            .collect()
    }
}
