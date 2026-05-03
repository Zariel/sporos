//! Title and media-type parsing for searchees and torrent metadata.

use std::{borrow::Cow, path::Path, sync::LazyLock};

use crate::domain::{File, MediaType};
use regex::Regex;

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

pub(super) const VIDEO_EXTENSIONS: &[&str] = &[
    "mkv", "mp4", "avi", "mov", "wmv", "flv", "m4v", "mpg", "mpeg", "ts", "webm",
];
const VIDEO_DISC_EXTENSIONS: &[&str] = &["iso", "vob", "m2ts", "mts"];
const AUDIO_EXTENSIONS: &[&str] = &["mp3", "flac", "m4a", "aac", "ogg", "opus", "wav", "alac"];
const BOOK_EXTENSIONS: &[&str] = &["epub", "mobi", "azw", "azw3", "pdf", "cbr", "cbz"];

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

pub(super) fn episode_match(name: &str) -> Option<regex::Captures<'_>> {
    EPISODE_REGEX
        .captures(name)
        .or_else(|| ALT_EPISODE_REGEX.captures(name))
}

fn extension_in(file: &File<'_>, extensions: &[&str]) -> bool {
    extension(file)
        .as_deref()
        .is_some_and(|extension| extensions.contains(&extension))
}

pub(super) fn is_video_file(file: &File<'_>) -> bool {
    extension_in(file, VIDEO_EXTENSIONS) || extension_in(file, VIDEO_DISC_EXTENSIONS)
}

pub(super) fn is_episode_match(title: &str) -> bool {
    EPISODE_REGEX.is_match(title)
}

pub(super) fn is_season_match(title: &str) -> bool {
    SEASON_REGEX.is_match(title)
}

pub(super) fn season_captures(title: &str) -> Option<regex::Captures<'_>> {
    SEASON_REGEX.captures(title)
}

pub(super) fn normalized_query_key(title: &str) -> String {
    title
        .chars()
        .map(|character| {
            if character.is_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                ' '
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(".")
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

pub(super) fn season_number(name: &str) -> Option<u32> {
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

pub(super) fn clean_title(title: &str) -> String {
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

pub(super) fn capture_u32(captures: &regex::Captures<'_>, name: &str) -> Option<u32> {
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
