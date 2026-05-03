//! Torrent-file operations for cache rewrite, diff, and tree commands.

use std::{borrow::Cow, fs, path::Path};

use crate::torrent::{Bencode, BencodeValue, bdecode, bencode, parse_metafile, torrent_cache_dir};

use super::{TorrentTree, TrackerUpdateResult, operation_error};

/// Replace tracker URLs inside cached torrent files.
pub fn update_torrent_cache_trackers(
    app_dir: &Path,
    old_announce_url: &str,
    new_announce_url: &str,
) -> crate::Result<TrackerUpdateResult> {
    let cache_dir = torrent_cache_dir(app_dir);
    let mut result = TrackerUpdateResult {
        files_seen: 0,
        files_updated: 0,
    };
    let entries = match fs::read_dir(&cache_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(result),
        Err(error) => return Err(operation_error(format!("failed to read cache: {error}"))),
    };

    for entry in entries {
        let entry = entry.map_err(|error| operation_error(format!("cache entry: {error}")))?;
        let path = entry.path();
        if path.extension().and_then(std::ffi::OsStr::to_str) != Some("torrent") {
            continue;
        }
        result.files_seen += 1;
        let bytes = fs::read(&path)
            .map_err(|error| operation_error(format!("failed to read torrent: {error}")))?;
        if let Some(updated) = replace_torrent_tracker_urls(
            &bytes,
            old_announce_url.as_bytes(),
            new_announce_url.as_bytes(),
        )? {
            fs::write(&path, updated).map_err(|error| {
                operation_error(format!("failed to write updated torrent: {error}"))
            })?;
            result.files_updated += 1;
        }
    }

    Ok(result)
}

/// Parse and compare two torrent files by normalized metafile structure.
pub fn diff_torrents(left: &Path, right: &Path) -> crate::Result<Option<String>> {
    let left = parse_metafile(
        &fs::read(left)
            .map_err(|error| operation_error(format!("failed to read left torrent: {error}")))?,
    )?;
    let right = parse_metafile(
        &fs::read(right)
            .map_err(|error| operation_error(format!("failed to read right torrent: {error}")))?,
    )?;

    if left == right {
        Ok(None)
    } else {
        Ok(Some(format!("{left:#?}\n---\n{right:#?}")))
    }
}

/// Parse a torrent file and return displayable tree metadata.
pub fn torrent_tree(path: &Path) -> crate::Result<TorrentTree> {
    let metafile = parse_metafile(
        &fs::read(path)
            .map_err(|error| operation_error(format!("failed to read torrent: {error}")))?,
    )?;
    Ok(TorrentTree {
        name: metafile.name.into_owned(),
        info_hash: metafile.info_hash.as_str().to_owned(),
        files: metafile
            .files
            .into_iter()
            .map(|file| (file.path.into_owned(), file.length))
            .collect(),
    })
}

fn replace_bytes(input: &[u8], from: &[u8], to: &[u8]) -> Vec<u8> {
    if from.is_empty() {
        return input.to_vec();
    }
    let mut output = Vec::with_capacity(input.len());
    let mut offset = 0;
    while offset < input.len() {
        let candidate = offset
            .checked_add(from.len())
            .and_then(|end| input.get(offset..end));
        if candidate == Some(from) {
            output.extend_from_slice(to);
            offset += from.len();
        } else if let Some(byte) = input.get(offset) {
            output.push(*byte);
            offset += 1;
        } else {
            break;
        }
    }
    output
}

fn replace_torrent_tracker_urls(
    input: &[u8],
    from: &[u8],
    to: &[u8],
) -> crate::Result<Option<Vec<u8>>> {
    if from.is_empty() {
        return Ok(None);
    }

    let mut decoded = bdecode(input)?;
    let BencodeValue::Dict(entries) = &mut decoded.value else {
        return Err(operation_error("cached torrent root must be a dictionary"));
    };

    let mut changed = false;
    for (key, value) in entries {
        match key.as_ref() {
            b"announce" => changed |= replace_bencode_bytes(value, from, to),
            b"announce-list" => changed |= replace_bencode_bytes_recursive(value, from, to),
            _ => {}
        }
    }

    Ok(changed.then(|| bencode(&decoded)))
}

fn replace_bencode_bytes(value: &mut Bencode<'_>, from: &[u8], to: &[u8]) -> bool {
    let BencodeValue::Bytes(bytes) = &mut value.value else {
        return false;
    };
    let updated = replace_bytes(bytes.as_ref(), from, to);
    if updated == bytes.as_ref() {
        false
    } else {
        *bytes = Cow::Owned(updated);
        true
    }
}

fn replace_bencode_bytes_recursive(value: &mut Bencode<'_>, from: &[u8], to: &[u8]) -> bool {
    match &mut value.value {
        BencodeValue::Bytes(_) => replace_bencode_bytes(value, from, to),
        BencodeValue::List(items) => {
            let mut changed = false;
            for item in items {
                changed |= replace_bencode_bytes_recursive(item, from, to);
            }
            changed
        }
        BencodeValue::Integer(_) | BencodeValue::Dict(_) => false,
    }
}
