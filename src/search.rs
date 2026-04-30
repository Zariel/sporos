//! Searchee discovery, filtering, Torznab queries, RSS, and announce workflows.

use std::{
    borrow::Cow,
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    sync::LazyLock,
    time::UNIX_EPOCH,
};

use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use regex::Regex;
use rusqlite::params;
use walkdir::{DirEntry, WalkDir};

use crate::{
    SporosError,
    domain::{File, MediaType, Searchee},
    persistence::Database,
    torrent::parse_metafile,
};

static EPISODE_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)(?P<title>.*?)[ ._\-\[]+s(?P<season>\d{1,2})[ ._\-\]]*e(?P<episode>\d{1,3})\b")
        .expect("episode regex compiles")
});
static ALT_EPISODE_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)(?P<title>.*?)[ ._\-]+(?P<season>\d{1,2})x(?P<episode>\d{1,3})\b")
        .expect("alternate episode regex compiles")
});
static DATED_EPISODE_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)(?P<title>.*?)[ ._\-]+(?P<date>(?:19|20)\d{2}[ ._\-](?:0?[1-9]|1[0-2])[ ._\-](?:0?[1-9]|[12]\d|3[01]))\b")
        .expect("dated episode regex compiles")
});
static SEASON_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)(?:^|[ ._\-\[])(?:s(?P<s>\d{1,2})|season[ ._\-]*(?P<season>\d{1,2}))(?:\b|[ ._\-\]])",
    )
    .expect("season regex compiles")
});
static SHORT_SEASON_FOLDER_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)^(?:s\d{1,2}|season[ ._\-]*\d{1,2})$").expect("short season regex compiles")
});
static MOVIE_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\b(?:19|20)\d{2}\b").expect("movie regex compiles"));
static ANIME_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)(?P<title>.+?)(?:[ ._\-]+| - )(?P<episode>\d{1,4})(?:v\d+)?(?:\b|[ ._\-\]])")
        .expect("anime regex compiles")
});
static RESOLUTION_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b(?P<value>2160p|1080p|720p|480p|4k|uhd)\b")
        .expect("resolution regex compiles")
});
static SOURCE_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b(?P<value>bluray|blu-ray|bdrip|brrip|web-dl|webdl|webrip|web|hdtv|hdrip|dvdrip|remux)\b")
        .expect("source regex compiles")
});
static PROPER_REPACK_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\b(?:proper|repack|rerip|real|v\d+)\b").expect("proper regex compiles")
});
static ALT_TITLE_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?P<title>.+?)\s+\((?P<alternate>[^()]+)\)")
        .expect("alternate title regex compiles")
});

const VIDEO_EXTENSIONS: &[&str] = &[
    "mkv", "mp4", "avi", "mov", "wmv", "flv", "m4v", "mpg", "mpeg", "ts", "webm",
];
const VIDEO_DISC_EXTENSIONS: &[&str] = &["iso", "vob", "m2ts", "mts"];
const AUDIO_EXTENSIONS: &[&str] = &["mp3", "flac", "m4a", "aac", "ogg", "opus", "wav", "alac"];
const BOOK_EXTENSIONS: &[&str] = &["epub", "mobi", "azw", "azw3", "pdf", "cbr", "cbz"];

/// Active recursive data-dir watcher. Dropping it stops watching.
pub struct DataDirWatchState {
    watcher: RecommendedWatcher,
    roots: Vec<PathBuf>,
}

/// Result counts from indexing a torrentDir.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct TorrentDirIndexResult {
    /// `.torrent` files seen in the directory.
    pub files_seen: usize,
    /// Torrents parsed and upserted.
    pub torrents_indexed: usize,
    /// Existing rows pruned because their files disappeared or became invalid.
    pub torrents_removed: usize,
    /// Files that could not be read or parsed.
    pub files_failed: usize,
}

/// Parse and index every `.torrent` in a torrentDir, then prune removed files.
pub fn index_torrent_dir(
    database: &Database,
    torrent_dir: &Path,
) -> crate::Result<TorrentDirIndexResult> {
    let connection = database.connection();
    connection
        .execute_batch(
            "CREATE TEMP TABLE IF NOT EXISTS current_torrent_dir (
                file_path TEXT PRIMARY KEY
            );
            DELETE FROM current_torrent_dir;",
        )
        .map_err(persistence_error)?;

    let mut result = TorrentDirIndexResult {
        files_seen: 0,
        torrents_indexed: 0,
        torrents_removed: 0,
        files_failed: 0,
    };
    let entries = match fs::read_dir(torrent_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            result.torrents_removed = connection
                .execute("DELETE FROM torrent", [])
                .map_err(persistence_error)?;
            return Ok(result);
        }
        Err(error) => {
            return Err(search_error(format!(
                "failed to read torrentDir {}: {error}",
                torrent_dir.display()
            )));
        }
    };

    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                tracing::debug!("skipping torrentDir entry: {error}");
                result.files_failed += 1;
                continue;
            }
        };
        let path = entry.path();
        if path.extension().and_then(std::ffi::OsStr::to_str) != Some("torrent") {
            continue;
        }
        result.files_seen += 1;
        let file_path = path.display().to_string();
        connection
            .execute(
                "INSERT OR IGNORE INTO current_torrent_dir (file_path) VALUES (?1)",
                params![file_path],
            )
            .map_err(persistence_error)?;

        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) => {
                tracing::debug!("failed to read torrentDir file {}: {error}", path.display());
                connection
                    .execute(
                        "DELETE FROM torrent WHERE file_path = ?1",
                        params![file_path],
                    )
                    .map_err(persistence_error)?;
                result.files_failed += 1;
                continue;
            }
        };
        match parse_metafile(&bytes) {
            Ok(metafile) => {
                connection
                    .execute(
                        "INSERT INTO torrent (info_hash, name, file_path)
                         VALUES (?1, ?2, ?3)
                         ON CONFLICT(file_path) DO UPDATE SET
                            info_hash = excluded.info_hash,
                            name = excluded.name",
                        params![
                            metafile.info_hash.as_str(),
                            metafile.name.as_ref(),
                            file_path
                        ],
                    )
                    .map_err(persistence_error)?;
                result.torrents_indexed += 1;
            }
            Err(error) => {
                tracing::debug!(
                    "failed to parse torrentDir file {}: {error}",
                    path.display()
                );
                connection
                    .execute(
                        "DELETE FROM torrent WHERE file_path = ?1",
                        params![file_path],
                    )
                    .map_err(persistence_error)?;
                result.files_failed += 1;
            }
        }
    }

    result.torrents_removed = connection
        .execute(
            "DELETE FROM torrent
             WHERE file_path NOT IN (SELECT file_path FROM current_torrent_dir)",
            [],
        )
        .map_err(persistence_error)?;
    Ok(result)
}

impl DataDirWatchState {
    /// Watched data-dir roots.
    pub fn roots(&self) -> &[PathBuf] {
        &self.roots
    }

    /// Keep the watcher handle observable without exposing notify internals.
    pub fn is_active(&self) -> bool {
        let _watcher = &self.watcher;
        true
    }
}

/// Start one recursive watcher over all configured data dirs.
pub fn watch_data_dirs<F>(data_dirs: &[PathBuf], handler: F) -> crate::Result<DataDirWatchState>
where
    F: FnMut(notify::Result<notify::Event>) + Send + 'static,
{
    let mut watcher = notify::recommended_watcher(handler)
        .map_err(|error| search_error(format!("failed to create data-dir watcher: {error}")))?;
    for data_dir in data_dirs {
        watcher
            .watch(data_dir, RecursiveMode::Recursive)
            .map_err(|error| {
                search_error(format!("failed to watch {}: {error}", data_dir.display()))
            })?;
    }
    Ok(DataDirWatchState {
        watcher,
        roots: data_dirs.to_vec(),
    })
}

/// Discover candidate release roots below a data-dir using a bounded walk.
pub fn find_potential_nested_roots(root: &Path, max_depth: u32) -> crate::Result<Vec<PathBuf>> {
    let mut roots = BTreeSet::new();
    for entry in WalkDir::new(root)
        .max_depth(max_depth as usize + 1)
        .follow_links(false)
        .into_iter()
        .filter_entry(|entry| !ignored_directory(entry))
    {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                tracing::debug!("skipping data-dir entry: {error}");
                continue;
            }
        };
        if !entry.file_type().is_file() || !path_has_extension(entry.path(), VIDEO_EXTENSIONS) {
            continue;
        }
        if let Some(parent) = entry.path().parent() {
            roots.insert(parent.to_path_buf());
        }
    }

    let mut roots = roots.into_iter().collect::<Vec<_>>();
    roots.sort_by(|left, right| {
        right
            .components()
            .count()
            .cmp(&left.components().count())
            .then_with(|| left.cmp(right))
    });
    Ok(roots)
}

/// Recompute affected parent roots for a changed path up to `maxDataDepth`.
pub fn affected_roots_for_changed_path(
    data_dir: &Path,
    changed_path: &Path,
    max_depth: u32,
) -> Vec<PathBuf> {
    let mut affected = Vec::new();
    let mut current = if changed_path.is_dir() {
        changed_path.to_path_buf()
    } else {
        changed_path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| changed_path.to_path_buf())
    };

    for _ in 0..=max_depth {
        if current.starts_with(data_dir) {
            affected.push(current.clone());
        }
        if current == data_dir {
            break;
        }
        let Some(parent) = current.parent() else {
            break;
        };
        current = parent.to_path_buf();
    }
    affected
}

/// Build a searchee from a data-dir file or release root.
pub fn create_searchee_from_path(path: &Path) -> crate::Result<Option<Searchee<'static>>> {
    let mut files = Vec::new();
    let mut newest_mtime = None;
    gather_files(path, &mut files, &mut newest_mtime)?;
    if files.is_empty() {
        return Ok(None);
    }

    files.sort_by(|left, right| left.path.cmp(&right.path));
    let name = path
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .map(str::to_owned)
        .unwrap_or_else(|| path.display().to_string());
    let Some(parsed) = parse_title(&name, &files, path.to_str()) else {
        return Ok(None);
    };

    let mut searchee = Searchee::from_files(name, parsed.title, files);
    searchee.path = Some(Cow::Owned(path.display().to_string()));
    searchee.mtime_millis = newest_mtime;
    searchee.media_type = parsed.media_type;
    Ok(Some(searchee.into_owned()))
}

/// Discover and build data-dir searchees in bounded batches.
pub fn data_dir_searchees(
    data_dirs: &[PathBuf],
    max_depth: u32,
) -> crate::Result<Vec<Searchee<'static>>> {
    let mut searchees = Vec::new();
    for data_dir in data_dirs {
        for root in find_potential_nested_roots(data_dir, max_depth)? {
            if let Some(searchee) = create_searchee_from_path(&root)? {
                searchees.push(searchee);
            }
        }
    }
    Ok(searchees)
}

/// Parsed title and metadata used by search grouping and compatibility checks.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ParsedTitle {
    /// Searchable compatibility title.
    pub title: String,
    /// Media type inferred from title and file extensions.
    pub media_type: MediaType,
    /// Release group suffix when present.
    pub release_group: Option<String>,
    /// Resolution marker such as `1080p`.
    pub resolution: Option<String>,
    /// Source marker such as `WEB-DL` or `BluRay`.
    pub source: Option<String>,
    /// Whether the name carries proper/repack/version metadata.
    pub proper_repack: bool,
    /// Parenthesized alternate title metadata when present.
    pub alternate_title: Option<String>,
}

/// Infer a media type using the documented compatibility order.
pub fn get_media_type(title: &str, files: &[File<'_>]) -> MediaType {
    if episode_match(title).is_some() {
        return MediaType::Episode;
    }
    if SEASON_REGEX.is_match(title) {
        return MediaType::Pack;
    }
    if files
        .iter()
        .any(|file| extension_in(file, VIDEO_EXTENSIONS))
    {
        if MOVIE_REGEX.is_match(title) {
            MediaType::Movie
        } else if ANIME_REGEX.is_match(title) {
            MediaType::Anime
        } else {
            MediaType::Video
        }
    } else if files
        .iter()
        .any(|file| extension_in(file, VIDEO_DISC_EXTENSIONS))
    {
        if MOVIE_REGEX.is_match(title) {
            MediaType::Movie
        } else {
            MediaType::Video
        }
    } else if files.iter().any(|file| extension_is(file, "rar")) {
        if MOVIE_REGEX.is_match(title) {
            MediaType::Movie
        } else if files
            .iter()
            .any(|file| extension_in(file, AUDIO_EXTENSIONS))
        {
            MediaType::Audio
        } else if files.iter().any(|file| extension_in(file, BOOK_EXTENSIONS)) {
            MediaType::Book
        } else {
            MediaType::Unknown
        }
    } else if files
        .iter()
        .any(|file| extension_in(file, AUDIO_EXTENSIONS))
    {
        MediaType::Audio
    } else if files.iter().any(|file| extension_in(file, BOOK_EXTENSIONS)) {
        MediaType::Book
    } else {
        MediaType::Unknown
    }
}

/// Parse a torrent, folder, or file name into a searchable title.
pub fn parse_title(name: &str, files: &[File<'_>], path: Option<&str>) -> Option<ParsedTitle> {
    let short_season_folder = SHORT_SEASON_FOLDER_REGEX.is_match(name.trim());
    let has_video = files.iter().any(|file| {
        extension_in(file, VIDEO_EXTENSIONS) || extension_in(file, VIDEO_DISC_EXTENSIONS)
    });

    if !short_season_folder
        && (name.chars().any(|character| character.is_ascii_digit()) || !has_video)
    {
        return Some(parsed_title(name, name, files));
    }

    if has_video {
        if let Some(parsed) = parse_from_video_files(name, files, path, short_season_folder) {
            return Some(parsed);
        }
    }

    if short_season_folder {
        None
    } else {
        Some(parsed_title(name, name, files))
    }
}

fn parse_from_video_files(
    name: &str,
    files: &[File<'_>],
    path: Option<&str>,
    short_season_folder: bool,
) -> Option<ParsedTitle> {
    let mut parsed_files = files
        .iter()
        .filter(|file| {
            extension_in(file, VIDEO_EXTENSIONS) || extension_in(file, VIDEO_DISC_EXTENSIONS)
        })
        .filter_map(|file| parse_video_name(file.name.as_ref()))
        .collect::<Vec<_>>();
    if parsed_files.is_empty() {
        return None;
    }

    parsed_files.sort_by(|left, right| left.title.cmp(&right.title));
    let first = parsed_files.first()?;
    let title = if short_season_folder {
        let season = season_number(name).or(first.season)?;
        let parent = parent_title(path)?;
        format!("{parent} S{season:02}")
    } else if parsed_files.len() > 1
        && parsed_files.iter().all(|item| {
            item.title.eq_ignore_ascii_case(&first.title)
                && item.season == first.season
                && item.dated_key.is_none()
        })
    {
        format!("{} S{:02}", clean_title(&first.title), first.season?)
    } else if let Some(dated_key) = &first.dated_key {
        format!("{} {dated_key}", clean_title(&first.title))
    } else if let (Some(season), Some(episode)) = (first.season, first.episode) {
        format!("{} S{season:02}E{episode:02}", clean_title(&first.title))
    } else {
        return None;
    };

    let mut parsed = parsed_title(name, &title, files);
    if parsed.resolution.is_none() {
        parsed.resolution = agreed_meta(&parsed_files, |item| item.resolution.as_deref());
    }
    if parsed.source.is_none() {
        parsed.source = agreed_meta(&parsed_files, |item| item.source.as_deref());
    }
    if parsed.release_group.is_none() {
        parsed.release_group = agreed_meta(&parsed_files, |item| item.release_group.as_deref());
    }
    parsed.proper_repack |= parsed_files.iter().all(|item| item.proper_repack);
    Some(parsed)
}

fn parsed_title(name: &str, title: &str, files: &[File<'_>]) -> ParsedTitle {
    ParsedTitle {
        title: title.to_owned(),
        media_type: get_media_type(title, files),
        release_group: release_group(name),
        resolution: capture_value(&RESOLUTION_REGEX, name),
        source: capture_value(&SOURCE_REGEX, name),
        proper_repack: PROPER_REPACK_REGEX.is_match(name),
        alternate_title: ALT_TITLE_REGEX
            .captures(name)
            .and_then(|captures| captures.name("alternate"))
            .map(|alternate| normalize_spaces(alternate.as_str())),
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct VideoTitle {
    title: String,
    season: Option<u32>,
    episode: Option<u32>,
    dated_key: Option<String>,
    release_group: Option<String>,
    resolution: Option<String>,
    source: Option<String>,
    proper_repack: bool,
}

fn parse_video_name(name: &str) -> Option<VideoTitle> {
    let stem = strip_extension(name);
    if let Some(captures) = episode_match(stem) {
        return Some(VideoTitle {
            title: capture_string(&captures, "title")?,
            season: capture_u32(&captures, "season"),
            episode: capture_u32(&captures, "episode"),
            dated_key: None,
            release_group: release_group(stem),
            resolution: capture_value(&RESOLUTION_REGEX, stem),
            source: capture_value(&SOURCE_REGEX, stem),
            proper_repack: PROPER_REPACK_REGEX.is_match(stem),
        });
    }
    if let Some(captures) = DATED_EPISODE_REGEX.captures(stem) {
        return Some(VideoTitle {
            title: capture_string(&captures, "title")?,
            season: None,
            episode: None,
            dated_key: capture_string(&captures, "date").map(|date| date.replace(['.', '_'], "-")),
            release_group: release_group(stem),
            resolution: capture_value(&RESOLUTION_REGEX, stem),
            source: capture_value(&SOURCE_REGEX, stem),
            proper_repack: PROPER_REPACK_REGEX.is_match(stem),
        });
    }
    if let Some(captures) = ANIME_REGEX.captures(stem) {
        return Some(VideoTitle {
            title: capture_string(&captures, "title")?,
            season: None,
            episode: capture_u32(&captures, "episode"),
            dated_key: None,
            release_group: release_group(stem),
            resolution: capture_value(&RESOLUTION_REGEX, stem),
            source: capture_value(&SOURCE_REGEX, stem),
            proper_repack: PROPER_REPACK_REGEX.is_match(stem),
        });
    }
    None
}

fn episode_match(name: &str) -> Option<regex::Captures<'_>> {
    EPISODE_REGEX
        .captures(name)
        .or_else(|| ALT_EPISODE_REGEX.captures(name))
}

fn extension_in(file: &File<'_>, extensions: &[&str]) -> bool {
    extension(file)
        .as_deref()
        .is_some_and(|extension| extensions.contains(&extension))
}

fn path_has_extension(path: &Path, extensions: &[&str]) -> bool {
    path.extension()
        .and_then(std::ffi::OsStr::to_str)
        .map(str::to_ascii_lowercase)
        .is_some_and(|extension| extensions.contains(&extension.as_str()))
}

fn ignored_directory(entry: &DirEntry) -> bool {
    if !entry.file_type().is_dir() {
        return false;
    }
    let name = entry.file_name().to_string_lossy().to_ascii_lowercase();
    [
        "sample",
        "proof",
        "bdmv",
        "bdrom",
        "certificate",
        "video_ts",
    ]
    .iter()
    .any(|ignored| name.contains(ignored))
}

fn gather_files(
    path: &Path,
    output: &mut Vec<File<'static>>,
    newest_mtime: &mut Option<u64>,
) -> crate::Result<()> {
    let metadata = fs::metadata(path)
        .map_err(|error| search_error(format!("failed to stat {}: {error}", path.display())))?;
    if metadata.is_file() {
        push_file(path, &metadata, output, newest_mtime);
        return Ok(());
    }

    for entry in WalkDir::new(path)
        .follow_links(false)
        .into_iter()
        .filter_entry(|entry| !ignored_directory(entry))
    {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                tracing::debug!("skipping data-dir file: {error}");
                continue;
            }
        };
        if !entry.file_type().is_file() {
            continue;
        }
        let metadata = match entry.metadata() {
            Ok(metadata) => metadata,
            Err(error) => {
                tracing::debug!("skipping data-dir metadata: {error}");
                continue;
            }
        };
        push_file(entry.path(), &metadata, output, newest_mtime);
    }
    Ok(())
}

fn push_file(
    path: &Path,
    metadata: &fs::Metadata,
    output: &mut Vec<File<'static>>,
    newest_mtime: &mut Option<u64>,
) {
    if let Ok(modified) = metadata.modified() {
        if let Ok(duration) = modified.duration_since(UNIX_EPOCH) {
            let millis = duration.as_millis().min(u128::from(u64::MAX)) as u64;
            *newest_mtime = Some(newest_mtime.map_or(millis, |current| current.max(millis)));
        }
    }
    output.push(File::new(path.display().to_string(), metadata.len()));
}

fn extension_is(file: &File<'_>, expected: &str) -> bool {
    extension(file).as_deref() == Some(expected)
}

fn extension(file: &File<'_>) -> Option<String> {
    Path::new(file.name.as_ref())
        .extension()
        .and_then(std::ffi::OsStr::to_str)
        .map(str::to_ascii_lowercase)
}

fn strip_extension(name: &str) -> &str {
    Path::new(name)
        .file_stem()
        .and_then(std::ffi::OsStr::to_str)
        .unwrap_or(name)
}

fn season_number(name: &str) -> Option<u32> {
    SEASON_REGEX.captures(name).and_then(|captures| {
        captures
            .name("s")
            .or_else(|| captures.name("season"))
            .and_then(|value| value.as_str().parse().ok())
    })
}

fn parent_title(path: Option<&str>) -> Option<String> {
    let path = Path::new(path?);
    path.parent()
        .and_then(Path::file_name)
        .and_then(std::ffi::OsStr::to_str)
        .map(clean_title)
        .filter(|title| !title.is_empty())
}

fn clean_title(title: &str) -> String {
    normalize_spaces(title.trim_matches(|character: char| {
        character == '.' || character == '_' || character == '-' || character.is_ascii_whitespace()
    }))
}

fn normalize_spaces(value: &str) -> String {
    value
        .replace(['.', '_'], " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn capture_value(regex: &Regex, value: &str) -> Option<String> {
    regex
        .captures(value)
        .and_then(|captures| captures.name("value").or_else(|| captures.name("group")))
        .map(|matched| normalize_spaces(matched.as_str()))
}

fn release_group(value: &str) -> Option<String> {
    let trimmed = value.trim().trim_end_matches(']');
    if value.ends_with(']') {
        if let Some((_, group)) = trimmed.rsplit_once('[') {
            return valid_group(group).then(|| normalize_spaces(group));
        }
    }
    trimmed
        .rsplit_once('-')
        .and_then(|(_, group)| valid_group(group).then(|| normalize_spaces(group)))
}

fn valid_group(group: &str) -> bool {
    let len = group.len();
    (2..=32).contains(&len)
        && group
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

fn capture_string(captures: &regex::Captures<'_>, name: &str) -> Option<String> {
    captures
        .name(name)
        .map(|matched| matched.as_str().to_owned())
}

fn capture_u32(captures: &regex::Captures<'_>, name: &str) -> Option<u32> {
    captures
        .name(name)
        .and_then(|matched| matched.as_str().parse().ok())
}

fn agreed_meta(
    parsed_files: &[VideoTitle],
    value: impl Fn(&VideoTitle) -> Option<&str>,
) -> Option<String> {
    let first = value(parsed_files.first()?)?;
    parsed_files
        .iter()
        .all(|item| value(item).is_some_and(|item_value| item_value.eq_ignore_ascii_case(first)))
        .then(|| first.to_owned())
}

fn search_error(message: impl Into<Cow<'static, str>>) -> SporosError {
    SporosError::Search {
        message: message.into(),
    }
}

fn persistence_error(error: rusqlite::Error) -> SporosError {
    SporosError::Persistence {
        message: Cow::Owned(error.to_string()),
    }
}

/// Apply parsed title metadata to a domain object represented by title and media fields.
pub fn parsed_name_and_media<'a>(
    name: &'a str,
    files: &[File<'_>],
    path: Option<&str>,
) -> (Cow<'a, str>, MediaType) {
    parse_title(name, files, path)
        .map(|parsed| (Cow::Owned(parsed.title), parsed.media_type))
        .unwrap_or((Cow::Borrowed(name), MediaType::Unknown))
}

#[cfg(test)]
mod tests {
    use super::{
        affected_roots_for_changed_path, create_searchee_from_path, find_potential_nested_roots,
        get_media_type, index_torrent_dir, parse_title,
    };
    use crate::{
        domain::{File, MediaType},
        persistence::Database,
    };
    use std::{
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    #[test]
    fn classifies_media_type_in_documented_order() {
        assert_eq!(get_media_type("Show S01E02", &[]), MediaType::Episode);
        assert_eq!(get_media_type("Show Season 2", &[]), MediaType::Pack);
        assert_eq!(
            get_media_type("Movie 2020", &[File::new("Movie.2020.mkv", 10)]),
            MediaType::Movie
        );
        assert_eq!(
            get_media_type("Album", &[File::new("track.flac", 10)]),
            MediaType::Audio
        );
        assert_eq!(
            get_media_type("Book", &[File::new("book.epub", 10)]),
            MediaType::Book
        );
        assert_eq!(
            get_media_type("Archive", &[File::new("data.bin", 10)]),
            MediaType::Unknown
        );
    }

    #[test]
    fn keeps_digit_names_as_compatibility_titles() {
        let parsed = parse_title(
            "Example.Show.S01E02.1080p.WEB-DL-GROUP",
            &[File::new("Example.Show.S01E02.1080p.WEB-DL-GROUP.mkv", 10)],
            None,
        )
        .expect("title parses");

        assert_eq!(parsed.title, "Example.Show.S01E02.1080p.WEB-DL-GROUP");
        assert_eq!(parsed.media_type, MediaType::Episode);
        assert_eq!(parsed.resolution.as_deref(), Some("1080p"));
        assert_eq!(parsed.source.as_deref(), Some("WEB-DL"));
        assert_eq!(parsed.release_group.as_deref(), Some("GROUP"));
    }

    #[test]
    fn infers_episode_title_from_video_file() {
        let parsed = parse_title(
            "Example Show",
            &[File::new("Example.Show.S01E02.1080p.WEB-DL-GROUP.mkv", 10)],
            None,
        )
        .expect("title parses");

        assert_eq!(parsed.title, "Example Show S01E02");
        assert_eq!(parsed.media_type, MediaType::Episode);
        assert_eq!(parsed.resolution.as_deref(), Some("1080p"));
        assert_eq!(parsed.source.as_deref(), Some("WEB-DL"));
        assert_eq!(parsed.release_group.as_deref(), Some("GROUP"));
    }

    #[test]
    fn infers_short_season_folder_from_parent_path() {
        let parsed = parse_title(
            "Season 2",
            &[
                File::new("Episode.One.S02E01.mkv", 10),
                File::new("Episode.Two.S02E02.mkv", 10),
            ],
            Some("/media/Example Show (2020)/Season 2"),
        )
        .expect("season parses");

        assert_eq!(parsed.title, "Example Show (2020) S02");
        assert_eq!(parsed.media_type, MediaType::Pack);
    }

    #[test]
    fn skips_short_season_folder_without_parent_title() {
        assert!(
            parse_title("Season 2", &[File::new("Episode.One.S02E01.mkv", 10)], None).is_none()
        );
    }

    #[test]
    fn discovers_nested_roots_deepest_first_and_ignores_samples() {
        let root = temp_path("nested-roots");
        fs::create_dir_all(root.join("Show/Season 1")).expect("season dir");
        fs::create_dir_all(root.join("Show/Sample")).expect("sample dir");
        fs::write(root.join("Show/Season 1/Show.S01E01.mkv"), b"video").expect("episode");
        fs::write(root.join("Show/Sample/sample.mkv"), b"sample").expect("sample");
        fs::write(root.join("readme.txt"), b"text").expect("text");

        let roots = find_potential_nested_roots(&root, 2).expect("roots");

        assert_eq!(roots, vec![root.join("Show/Season 1")]);
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn creates_data_dir_searchee_with_title_mtime_and_files() {
        let root = temp_path("searchee");
        let release = root.join("Example Show");
        fs::create_dir_all(&release).expect("root");
        let episode = release.join("Example.Show.S01E02.mkv");
        let subtitle = release.join("Example.Show.S01E02.srt");
        fs::write(&episode, b"video bytes").expect("episode");
        fs::write(&subtitle, b"sub").expect("subtitle");

        let searchee = create_searchee_from_path(&release)
            .expect("create")
            .expect("searchee");

        assert_eq!(searchee.title, "Example Show S01E02");
        assert_eq!(searchee.media_type, MediaType::Episode);
        assert_eq!(searchee.files.len(), 2);
        assert!(searchee.mtime_millis.is_some());
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn changed_path_maps_to_parents_within_max_depth() {
        let data_dir = PathBuf::from("/data");
        let changed = PathBuf::from("/data/show/season/episode.mkv");

        let affected = affected_roots_for_changed_path(&data_dir, &changed, 2);

        assert_eq!(
            affected,
            vec![
                PathBuf::from("/data/show/season"),
                PathBuf::from("/data/show"),
                PathBuf::from("/data")
            ]
        );
    }

    #[test]
    fn indexes_torrent_dir_and_prunes_removed_files() {
        let root = temp_path("torrent-dir");
        let torrent_dir = root.join("torrents");
        fs::create_dir_all(&torrent_dir).expect("torrent dir");
        let first = torrent_dir.join("first.torrent");
        let second = torrent_dir.join("second.torrent");
        fs::write(&first, torrent_bytes("First.Release", 10)).expect("first");
        fs::write(&second, torrent_bytes("Second.Release", 20)).expect("second");
        let database = Database::open_app_dir(&root).expect("database");

        let result = index_torrent_dir(&database, &torrent_dir).expect("index");

        assert_eq!(result.files_seen, 2);
        assert_eq!(result.torrents_indexed, 2);
        let count: i64 = database
            .connection()
            .query_row("SELECT COUNT(*) FROM torrent", [], |row| row.get(0))
            .expect("count");
        assert_eq!(count, 2);

        fs::remove_file(second).expect("remove second");
        fs::write(&first, torrent_bytes("First.Changed", 30)).expect("change first");
        let result = index_torrent_dir(&database, &torrent_dir).expect("reindex");

        assert_eq!(result.files_seen, 1);
        assert_eq!(result.torrents_indexed, 1);
        assert_eq!(result.torrents_removed, 1);
        let names = database
            .connection()
            .query_row(
                "SELECT name FROM torrent WHERE file_path = ?1",
                [&first.display().to_string()],
                |row| row.get::<_, String>(0),
            )
            .expect("name");
        assert_eq!(names, "First.Changed");
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn invalid_torrent_dir_files_remove_stale_rows() {
        let root = temp_path("torrent-dir-invalid");
        let torrent_dir = root.join("torrents");
        fs::create_dir_all(&torrent_dir).expect("torrent dir");
        let path = torrent_dir.join("stale.torrent");
        fs::write(&path, torrent_bytes("Stale.Release", 10)).expect("torrent");
        let database = Database::open_app_dir(&root).expect("database");
        index_torrent_dir(&database, &torrent_dir).expect("index");

        fs::write(&path, b"not bencode").expect("invalid");
        let result = index_torrent_dir(&database, &torrent_dir).expect("reindex");

        assert_eq!(result.files_seen, 1);
        assert_eq!(result.files_failed, 1);
        let count: i64 = database
            .connection()
            .query_row("SELECT COUNT(*) FROM torrent", [], |row| row.get(0))
            .expect("count");
        assert_eq!(count, 0);
        let _cleanup = fs::remove_dir_all(root);
    }

    fn torrent_bytes(name: &str, length: u64) -> Vec<u8> {
        format!(
            "d4:infod6:lengthi{length}e4:name{}:{name}12:piece lengthi1e6:pieces20:aaaaaaaaaaaaaaaaaaaaee",
            name.len()
        )
        .into_bytes()
    }

    fn temp_path(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        std::env::temp_dir().join(format!("sporos-search-{label}-{nanos}"))
    }
}
