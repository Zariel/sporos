use std::error::Error;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::{collections::BTreeMap, sync::MutexGuard};

use crate::domain::{InfoHash, MediaType};

pub const CACHED_TORRENT_SUFFIX: &str = ".cached.torrent";
pub const SAVED_TORRENT_SUFFIX: &str = ".torrent";
pub const MAX_TORRENT_OUTPUT_PATH_BYTES: usize = 255;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TorrentOutputMetadata {
    pub media_type: MediaType,
    pub tracker: String,
    pub name: String,
    pub info_hash: InfoHash,
    pub cached: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum TorrentCachePathError {
    UnsafeComponent { field: &'static str, value: String },
    MalformedFilename { filename: String, reason: String },
    PathBudgetTooSmall { output_dir: PathBuf },
}

impl fmt::Display for TorrentCachePathError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsafeComponent { field, value } => {
                write!(formatter, "unsafe torrent output {field}: {value}")
            }
            Self::MalformedFilename { filename, reason } => {
                write!(
                    formatter,
                    "malformed torrent filename `{filename}`: {reason}"
                )
            }
            Self::PathBudgetTooSmall { output_dir } => write!(
                formatter,
                "output directory leaves no filename budget under {} bytes: {}",
                MAX_TORRENT_OUTPUT_PATH_BYTES,
                output_dir.display()
            ),
        }
    }
}

impl Error for TorrentCachePathError {}

/// Returns the canonical cached torrent path for a parsed info hash.
///
/// Cached torrents are named `<info_hash>.cached.torrent` directly under the
/// configured torrent cache directory, making lookup independent of tracker or
/// candidate title metadata.
pub fn cached_torrent_path(cache_dir: &Path, info_hash: &InfoHash) -> PathBuf {
    cache_dir.join(format!("{}{CACHED_TORRENT_SUFFIX}", info_hash.as_str()))
}

pub fn with_cached_torrent_path_lock<T>(path: &Path, action: impl FnOnce() -> T) -> T {
    let lock = cached_torrent_path_lock(path);
    let _release = CachePathLockRelease {
        path: path.to_path_buf(),
        lock: Arc::clone(&lock),
    };
    let _guard = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    action()
}

type CachePathLockMap = Mutex<BTreeMap<PathBuf, Arc<Mutex<()>>>>;

struct CachePathLockRelease {
    path: PathBuf,
    lock: Arc<Mutex<()>>,
}

impl Drop for CachePathLockRelease {
    fn drop(&mut self) {
        release_cached_torrent_path_lock(&self.path, &self.lock);
    }
}

fn cached_torrent_path_locks() -> &'static CachePathLockMap {
    static LOCKS: OnceLock<CachePathLockMap> = OnceLock::new();
    LOCKS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

fn lock_map() -> MutexGuard<'static, BTreeMap<PathBuf, Arc<Mutex<()>>>> {
    cached_torrent_path_locks()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn cached_torrent_path_lock(path: &Path) -> Arc<Mutex<()>> {
    let mut locks = lock_map();
    Arc::clone(
        locks
            .entry(path.to_path_buf())
            .or_insert_with(|| Arc::new(Mutex::new(()))),
    )
}

fn release_cached_torrent_path_lock(path: &Path, lock: &Arc<Mutex<()>>) {
    let mut locks = lock_map();
    if Arc::strong_count(lock) == 3 {
        locks.remove(path);
    }
}

#[cfg(test)]
fn cached_torrent_path_lock_is_tracked(path: &Path) -> bool {
    lock_map().contains_key(path)
}

/// Parses a cached torrent filename and returns its info hash.
///
/// This intentionally accepts only the cache filename, not a path, so callers
/// cannot accidentally treat traversal or nested paths as cache entries.
pub fn parse_cached_torrent_filename(file_name: &str) -> Result<InfoHash, TorrentCachePathError> {
    reject_pathlike_filename(file_name)?;
    let Some(info_hash) = file_name.strip_suffix(CACHED_TORRENT_SUFFIX) else {
        return Err(malformed(file_name, "missing .cached.torrent suffix"));
    };
    InfoHash::new(info_hash).map_err(|error| malformed(file_name, error.to_string()))
}

/// Builds a saved torrent output path and truncates the title component when
/// needed to keep the full path within the conservative 255-byte budget.
pub fn torrent_output_path(
    output_dir: &Path,
    metadata: &TorrentOutputMetadata,
) -> Result<PathBuf, TorrentCachePathError> {
    Ok(output_dir.join(torrent_output_filename(output_dir, metadata)?))
}

/// Builds a parseable saved torrent filename from typed metadata.
///
/// The format is `[media_type][tracker]safe-name[info_hash].torrent`, with
/// `.cached.torrent` used for restore/cache output. Components are sanitized
/// before formatting so the result is a single path-safe filename.
pub fn torrent_output_filename(
    output_dir: &Path,
    metadata: &TorrentOutputMetadata,
) -> Result<String, TorrentCachePathError> {
    let media_type = media_type_key(metadata.media_type);
    let tracker = sanitize_component("tracker", &metadata.tracker)?;
    let name = sanitize_component("name", &metadata.name)?;
    let suffix = if metadata.cached {
        CACHED_TORRENT_SUFFIX
    } else {
        SAVED_TORRENT_SUFFIX
    };
    let fixed = format!(
        "[{media_type}][{tracker}][{}]{suffix}",
        metadata.info_hash.as_str()
    );
    let budget = filename_budget(output_dir, fixed.len())?;
    let name = truncate_utf8(&name, budget);

    Ok(format!(
        "[{media_type}][{tracker}]{name}[{}]{suffix}",
        metadata.info_hash.as_str()
    ))
}

/// Parses metadata encoded by `torrent_output_filename`.
pub fn parse_torrent_output_filename(
    file_name: &str,
) -> Result<TorrentOutputMetadata, TorrentCachePathError> {
    reject_pathlike_filename(file_name)?;
    let (stem, cached) = if let Some(stem) = file_name.strip_suffix(CACHED_TORRENT_SUFFIX) {
        (stem, true)
    } else if let Some(stem) = file_name.strip_suffix(SAVED_TORRENT_SUFFIX) {
        (stem, false)
    } else {
        return Err(malformed(file_name, "missing torrent suffix"));
    };

    let (media_type, rest) = parse_bracketed(file_name, stem, "media type")?;
    let media_type = parse_media_type(file_name, media_type)?;
    let (tracker, rest) = parse_bracketed(file_name, rest, "tracker")?;
    let hash_start = rest
        .rfind('[')
        .ok_or_else(|| malformed(file_name, "missing info hash"))?;
    let hash_end = rest
        .get(hash_start..)
        .and_then(|value| value.rfind(']').map(|end| hash_start + end))
        .ok_or_else(|| malformed(file_name, "missing info hash terminator"))?;
    if hash_end + 1 != rest.len() {
        return Err(malformed(file_name, "unexpected text after info hash"));
    }

    let name = rest
        .get(..hash_start)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| malformed(file_name, "missing safe name"))?;
    let info_hash = rest
        .get(hash_start + 1..hash_end)
        .ok_or_else(|| malformed(file_name, "missing info hash"))?;

    Ok(TorrentOutputMetadata {
        media_type,
        tracker: tracker.to_owned(),
        name: name.to_owned(),
        info_hash: InfoHash::new(info_hash)
            .map_err(|error| malformed(file_name, error.to_string()))?,
        cached,
    })
}

fn parse_bracketed<'a>(
    filename: &str,
    value: &'a str,
    field: &'static str,
) -> Result<(&'a str, &'a str), TorrentCachePathError> {
    let Some(rest) = value.strip_prefix('[') else {
        return Err(malformed(filename, format!("missing {field} opener")));
    };
    let Some(end) = rest.find(']') else {
        return Err(malformed(filename, format!("missing {field} terminator")));
    };
    let component = rest
        .get(..end)
        .filter(|component| !component.is_empty())
        .ok_or_else(|| malformed(filename, format!("empty {field}")))?;
    let rest = rest
        .get(end + 1..)
        .ok_or_else(|| malformed(filename, format!("invalid {field} boundary")))?;
    Ok((component, rest))
}

fn parse_media_type(filename: &str, value: &str) -> Result<MediaType, TorrentCachePathError> {
    match value {
        "episode" => Ok(MediaType::Episode),
        "season_pack" => Ok(MediaType::SeasonPack),
        "movie" => Ok(MediaType::Movie),
        "anime" => Ok(MediaType::Anime),
        "video" => Ok(MediaType::Video),
        "audio" => Ok(MediaType::Audio),
        "book" => Ok(MediaType::Book),
        "archive" => Ok(MediaType::Archive),
        "unknown" => Ok(MediaType::Unknown),
        _ => Err(malformed(filename, format!("invalid media type `{value}`"))),
    }
}

fn media_type_key(media_type: MediaType) -> &'static str {
    match media_type {
        MediaType::Episode => "episode",
        MediaType::SeasonPack => "season_pack",
        MediaType::Movie => "movie",
        MediaType::Anime => "anime",
        MediaType::Video => "video",
        MediaType::Audio => "audio",
        MediaType::Book => "book",
        MediaType::Archive => "archive",
        MediaType::Unknown => "unknown",
    }
}

fn sanitize_component(field: &'static str, value: &str) -> Result<String, TorrentCachePathError> {
    if value.contains("..") {
        return Err(TorrentCachePathError::UnsafeComponent {
            field,
            value: value.to_owned(),
        });
    }

    let mut sanitized = String::with_capacity(value.len());
    for character in value.trim().chars() {
        if character.is_control()
            || matches!(
                character,
                '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' | '[' | ']'
            )
        {
            sanitized.push('_');
        } else {
            sanitized.push(character);
        }
    }

    let sanitized = sanitized
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim_matches(|character| matches!(character, '.' | ' ' | '_'))
        .to_owned();

    if sanitized.is_empty() || sanitized == "." || sanitized == ".." || sanitized.contains("..") {
        return Err(TorrentCachePathError::UnsafeComponent {
            field,
            value: value.to_owned(),
        });
    }

    Ok(sanitized)
}

fn reject_pathlike_filename(file_name: &str) -> Result<(), TorrentCachePathError> {
    if file_name.contains('/') || file_name.contains('\\') || file_name.contains("..") {
        return Err(malformed(
            file_name,
            "filename must not contain path traversal",
        ));
    }
    Ok(())
}

fn filename_budget(output_dir: &Path, fixed_len: usize) -> Result<usize, TorrentCachePathError> {
    let dir_len = output_dir.to_string_lossy().len();
    let reserved = dir_len.saturating_add(1).saturating_add(fixed_len);
    let budget = MAX_TORRENT_OUTPUT_PATH_BYTES.saturating_sub(reserved);
    if budget == 0 {
        Err(TorrentCachePathError::PathBudgetTooSmall {
            output_dir: output_dir.to_path_buf(),
        })
    } else {
        Ok(budget)
    }
}

fn truncate_utf8(value: &str, max_bytes: usize) -> String {
    let mut truncated = String::new();
    for character in value.chars() {
        if truncated.len() + character.len_utf8() > max_bytes {
            break;
        }
        truncated.push(character);
    }
    truncated.trim_end().to_owned()
}

fn malformed(filename: impl Into<String>, reason: impl Into<String>) -> TorrentCachePathError {
    TorrentCachePathError::MalformedFilename {
        filename: filename.into(),
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SHA1: &str = "0123456789abcdef0123456789abcdef01234567";
    const SHA1_ALT: &str = "fedcba9876543210fedcba9876543210fedcba98";

    #[test]
    fn cached_torrent_paths_are_info_hash_addressed() {
        let info_hash = InfoHash::new(SHA1).unwrap();
        let path = cached_torrent_path(Path::new("/cache/torrents"), &info_hash);
        let parsed = parse_cached_torrent_filename(
            "0123456789abcdef0123456789abcdef01234567.cached.torrent",
        )
        .unwrap();

        assert_eq!(
            PathBuf::from(
                "/cache/torrents/0123456789abcdef0123456789abcdef01234567.cached.torrent"
            ),
            path
        );
        assert_eq!(info_hash, parsed);
    }

    #[test]
    fn cached_torrent_path_lock_is_released_after_panic() {
        let path = Path::new("/cache/torrents/panic.cached.torrent");

        let result = std::panic::catch_unwind(|| {
            with_cached_torrent_path_lock(path, || panic!("cache lock panic test"));
        });

        assert!(result.is_err());
        assert!(!cached_torrent_path_lock_is_tracked(path));
    }

    #[test]
    fn output_filename_round_trips_metadata() {
        let metadata = TorrentOutputMetadata {
            media_type: MediaType::Movie,
            tracker: "tracker.example".to_owned(),
            name: "Example Movie: 2024 / WEB-DL".to_owned(),
            info_hash: InfoHash::new(SHA1).unwrap(),
            cached: false,
        };

        let filename = torrent_output_filename(Path::new("/out"), &metadata).unwrap();
        let parsed = parse_torrent_output_filename(&filename).unwrap();

        assert_eq!(
            "[movie][tracker.example]Example Movie_ 2024 _ WEB-DL[0123456789abcdef0123456789abcdef01234567].torrent",
            filename
        );
        assert_eq!(MediaType::Movie, parsed.media_type);
        assert_eq!("tracker.example", parsed.tracker);
        assert_eq!("Example Movie_ 2024 _ WEB-DL", parsed.name);
        assert_eq!(metadata.info_hash, parsed.info_hash);
        assert!(!parsed.cached);
    }

    #[test]
    fn cached_output_filename_round_trips() {
        let metadata = TorrentOutputMetadata {
            media_type: MediaType::Episode,
            tracker: "tv.example".to_owned(),
            name: "Show S01E01".to_owned(),
            info_hash: InfoHash::new(SHA1_ALT).unwrap(),
            cached: true,
        };

        let filename = torrent_output_filename(Path::new("/out"), &metadata).unwrap();
        let parsed = parse_torrent_output_filename(&filename).unwrap();

        assert!(filename.ends_with(CACHED_TORRENT_SUFFIX));
        assert_eq!(metadata, parsed);
    }

    #[test]
    fn output_filename_truncates_name_to_path_budget_on_utf8_boundary() {
        let metadata = TorrentOutputMetadata {
            media_type: MediaType::Video,
            tracker: "tracker".to_owned(),
            name: "長".repeat(200),
            info_hash: InfoHash::new(SHA1).unwrap(),
            cached: false,
        };

        let path = torrent_output_path(Path::new("/output"), &metadata).unwrap();
        let filename = path.file_name().and_then(|name| name.to_str()).unwrap();

        assert!(path.to_string_lossy().len() <= MAX_TORRENT_OUTPUT_PATH_BYTES);
        assert!(filename.ends_with(SAVED_TORRENT_SUFFIX));
        parse_torrent_output_filename(filename).unwrap();
    }

    #[test]
    fn output_filename_rejects_traversal_and_malformed_metadata() {
        let metadata = TorrentOutputMetadata {
            media_type: MediaType::Book,
            tracker: "tracker".to_owned(),
            name: "../bad".to_owned(),
            info_hash: InfoHash::new(SHA1).unwrap(),
            cached: false,
        };

        assert!(matches!(
            torrent_output_filename(Path::new("/out"), &metadata),
            Err(TorrentCachePathError::UnsafeComponent { .. })
        ));
        parse_torrent_output_filename("[bogus][tracker]Name[0123].torrent").unwrap_err();
        parse_torrent_output_filename("../[movie][tracker]Name[0123].torrent").unwrap_err();
        parse_cached_torrent_filename("not-a-hash.cached.torrent").unwrap_err();
    }
}
