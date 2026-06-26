use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::UNIX_EPOCH;

use regex::Regex;

use crate::domain::{
    ByteSize, DisplayName, DomainError, FileIndex, ItemTitle, LocalFile, LocalItem,
    LocalItemSource, MediaType,
};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct InventoryScanOptions {
    pub max_depth: u16,
}

impl Default for InventoryScanOptions {
    fn default() -> Self {
        Self { max_depth: 3 }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ScannedLocalItem {
    pub item: LocalItem,
    pub files: Vec<LocalFile>,
}

#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct InventoryScanReport {
    pub items: Vec<ScannedLocalItem>,
    pub failures: Vec<InventoryScanFailure>,
}

#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct InventoryScanStreamReport {
    pub scanned_items: usize,
    pub failures: Vec<InventoryScanFailure>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct InventoryScanFailure {
    pub path: PathBuf,
    pub kind: InventoryScanFailureKind,
    pub message: String,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum InventoryScanFailureKind {
    Metadata,
    ReadDirectory,
    NonUtf8Path,
    Domain,
    Overflow,
}

impl fmt::Display for InventoryScanFailureKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            Self::Metadata => "metadata",
            Self::ReadDirectory => "read directory",
            Self::NonUtf8Path => "non-UTF-8 path",
            Self::Domain => "domain",
            Self::Overflow => "overflow",
        };
        formatter.write_str(label)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ParsedMediaTitle {
    pub raw_name: String,
    pub search_title: String,
    pub media_type: MediaType,
    pub season: Option<u16>,
    pub episode: Option<u16>,
    pub air_date: Option<AirDate>,
    pub year: Option<u16>,
    pub release_group: Option<String>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct AirDate {
    pub year: u16,
    pub month: u8,
    pub day: u8,
}

pub fn parse_media_title(name: &str, file_paths: &[PathBuf]) -> ParsedMediaTitle {
    parse_media_title_from_paths(name, file_paths.iter().map(PathBuf::as_path))
}

fn parse_media_title_from_paths<'a>(
    name: &str,
    file_paths: impl IntoIterator<Item = &'a Path>,
) -> ParsedMediaTitle {
    let release_group = parse_release_group(name);
    let normalized = normalize_title_input(strip_release_group(name));

    if let Some(episode) = parse_numbered_episode(&normalized) {
        let title = title_before(&normalized, episode.start);
        return ParsedMediaTitle {
            raw_name: name.to_owned(),
            search_title: format!(
                "{} S{:02}E{:02}",
                fallback_title(&title, &normalized),
                episode.season,
                episode.episode
            ),
            media_type: MediaType::Episode,
            season: Some(episode.season),
            episode: Some(episode.episode),
            air_date: None,
            year: None,
            release_group,
        };
    }

    if let Some(date) = parse_dated_episode(&normalized) {
        let title = title_before(&normalized, date.start);
        return ParsedMediaTitle {
            raw_name: name.to_owned(),
            search_title: format!(
                "{} {:04}-{:02}-{:02}",
                fallback_title(&title, &normalized),
                date.date.year,
                date.date.month,
                date.date.day
            ),
            media_type: MediaType::Episode,
            season: None,
            episode: None,
            air_date: Some(date.date),
            year: Some(date.date.year),
            release_group,
        };
    }

    if let Some(season) = parse_season_pack(&normalized) {
        let title = title_before(&normalized, season.start);
        return ParsedMediaTitle {
            raw_name: name.to_owned(),
            search_title: format!(
                "{} S{:02}",
                fallback_title(&title, &normalized),
                season.season
            ),
            media_type: MediaType::SeasonPack,
            season: Some(season.season),
            episode: None,
            air_date: None,
            year: None,
            release_group,
        };
    }

    let media_type = classify_media_type_from_paths(name, file_paths);
    let year = parse_movie_year(&normalized);
    let anime_episode = parse_anime_episode(&normalized, release_group.as_deref());
    let search_title = match (media_type, year, anime_episode) {
        (MediaType::Movie, Some(year), _) => {
            format!("{} {year}", title_before_year(&normalized, year))
        }
        (MediaType::Anime, _, Some(episode)) => {
            format!(
                "{} {:02}",
                title_before(&normalized, episode.start),
                episode.episode
            )
        }
        _ => strip_trailing_metadata(&normalized),
    };

    ParsedMediaTitle {
        raw_name: name.to_owned(),
        search_title: fallback_title(&search_title, &normalized),
        media_type,
        season: None,
        episode: anime_episode.map(|episode| episode.episode),
        air_date: None,
        year,
        release_group,
    }
}

pub fn classify_media_type_from_name(name: &str, file_paths: &[PathBuf]) -> MediaType {
    classify_media_type_from_paths(name, file_paths.iter().map(PathBuf::as_path))
}

fn classify_media_type_from_paths<'a>(
    name: &str,
    file_paths: impl IntoIterator<Item = &'a Path>,
) -> MediaType {
    let normalized = normalize_title_input(strip_release_group(name));
    if parse_numbered_episode(&normalized).is_some() || parse_dated_episode(&normalized).is_some() {
        return MediaType::Episode;
    }
    if parse_season_pack(&normalized).is_some() {
        return MediaType::SeasonPack;
    }

    let extensions = FileExtensions::from_paths(file_paths);
    if extensions.has_video {
        if parse_movie_year(&normalized).is_some() {
            MediaType::Movie
        } else if parse_anime_episode(&normalized, parse_release_group(name).as_deref()).is_some() {
            MediaType::Anime
        } else {
            MediaType::Video
        }
    } else if extensions.has_video_disc {
        if parse_movie_year(&normalized).is_some() {
            MediaType::Movie
        } else {
            MediaType::Video
        }
    } else if extensions.has_rar && parse_movie_year(&normalized).is_some() {
        MediaType::Movie
    } else if extensions.has_audio {
        MediaType::Audio
    } else if extensions.has_book {
        MediaType::Book
    } else if extensions.has_archive && !extensions.has_rar {
        MediaType::Archive
    } else {
        MediaType::Unknown
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct ScannedFile {
    relative_path: PathBuf,
    size: ByteSize,
    mtime_ms: Option<i64>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct EpisodeMatch {
    start: usize,
    season: u16,
    episode: u16,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct DateMatch {
    start: usize,
    date: AirDate,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct SeasonMatch {
    start: usize,
    season: u16,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct AnimeEpisode {
    start: usize,
    episode: u16,
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
struct RootDiscoveryOutcome {
    emitted: bool,
    keep_going: bool,
}

#[derive(Debug, Clone)]
pub struct InventoryScanner {
    options: InventoryScanOptions,
}

impl InventoryScanner {
    pub const fn new(options: InventoryScanOptions) -> Self {
        Self { options }
    }

    pub fn scan_media_dirs(&self, media_dirs: &[PathBuf]) -> InventoryScanReport {
        let mut items = Vec::new();
        let report = self.scan_media_dirs_with(media_dirs, |item| {
            items.push(item);
            true
        });

        InventoryScanReport {
            items,
            failures: report.failures,
        }
    }

    pub fn scan_media_dirs_with<F>(
        &self,
        media_dirs: &[PathBuf],
        mut on_item: F,
    ) -> InventoryScanStreamReport
    where
        F: FnMut(ScannedLocalItem) -> bool,
    {
        self.scan_media_dirs_until(media_dirs, || true, &mut on_item)
    }

    pub fn scan_media_dirs_until<F, C>(
        &self,
        media_dirs: &[PathBuf],
        mut should_continue: C,
        mut on_item: F,
    ) -> InventoryScanStreamReport
    where
        F: FnMut(ScannedLocalItem) -> bool,
        C: FnMut() -> bool,
    {
        let mut report = InventoryScanReport::default();
        let mut scanned_items = 0usize;
        for media_dir in media_dirs {
            if !should_continue() {
                break;
            }
            let outcome = self.discover_roots_until(
                media_dir,
                &mut report,
                &mut should_continue,
                &mut |root, report, should_continue| {
                    if !should_continue() {
                        return false;
                    }
                    if let Some(item) = self.scan_item_root_until(root, report, should_continue) {
                        scanned_items = scanned_items.saturating_add(1);
                        on_item(item)
                    } else {
                        true
                    }
                },
            );
            if !outcome.keep_going {
                break;
            }
        }
        InventoryScanStreamReport {
            scanned_items,
            failures: report.failures,
        }
    }

    pub fn scan_item_roots_until<F, C>(
        &self,
        item_roots: &[PathBuf],
        mut should_continue: C,
        mut on_item: F,
    ) -> InventoryScanStreamReport
    where
        F: FnMut(ScannedLocalItem) -> bool,
        C: FnMut() -> bool,
    {
        let mut report = InventoryScanReport::default();
        let mut scanned_items = 0usize;
        for item_root in item_roots {
            if !should_continue() {
                break;
            }
            if let Some(item) =
                self.scan_item_root_until(item_root, &mut report, &mut should_continue)
            {
                scanned_items = scanned_items.saturating_add(1);
                if !on_item(item) {
                    break;
                }
            }
        }
        InventoryScanStreamReport {
            scanned_items,
            failures: report.failures,
        }
    }

    fn discover_roots_until<F, C>(
        &self,
        root: &Path,
        report: &mut InventoryScanReport,
        should_continue: &mut C,
        on_root: &mut F,
    ) -> RootDiscoveryOutcome
    where
        F: FnMut(&Path, &mut InventoryScanReport, &mut C) -> bool,
        C: FnMut() -> bool,
    {
        if !should_continue() {
            return RootDiscoveryOutcome {
                emitted: false,
                keep_going: false,
            };
        }

        let Ok(metadata) = fs::symlink_metadata(root) else {
            push_io_failure(
                report,
                root,
                InventoryScanFailureKind::Metadata,
                "read metadata",
            );
            return RootDiscoveryOutcome {
                emitted: false,
                keep_going: true,
            };
        };

        if metadata.is_file() {
            return if is_video_file(root) {
                RootDiscoveryOutcome {
                    emitted: true,
                    keep_going: on_root(root, report, should_continue),
                }
            } else {
                RootDiscoveryOutcome {
                    emitted: false,
                    keep_going: true,
                }
            };
        }

        if !metadata.is_dir() {
            return RootDiscoveryOutcome {
                emitted: false,
                keep_going: true,
            };
        }

        self.discover_directory_roots_until(root, 0, true, report, should_continue, on_root)
    }

    fn discover_directory_roots_until<F, C>(
        &self,
        dir: &Path,
        depth: u16,
        is_scan_root: bool,
        report: &mut InventoryScanReport,
        should_continue: &mut C,
        on_root: &mut F,
    ) -> RootDiscoveryOutcome
    where
        F: FnMut(&Path, &mut InventoryScanReport, &mut C) -> bool,
        C: FnMut() -> bool,
    {
        if !should_continue() {
            return RootDiscoveryOutcome {
                emitted: false,
                keep_going: false,
            };
        }
        if !is_scan_root && should_ignore_dir(dir) {
            return RootDiscoveryOutcome {
                emitted: false,
                keep_going: true,
            };
        }

        if !path_has_utf8_name(dir) {
            push_failure(
                report,
                dir,
                InventoryScanFailureKind::NonUtf8Path,
                "directory name is not valid UTF-8",
            );
            return RootDiscoveryOutcome {
                emitted: false,
                keep_going: true,
            };
        }

        let Ok(entries) = fs::read_dir(dir) else {
            push_io_failure(
                report,
                dir,
                InventoryScanFailureKind::ReadDirectory,
                "read directory",
            );
            return RootDiscoveryOutcome {
                emitted: false,
                keep_going: true,
            };
        };

        let mut child_emitted = false;
        for entry in entries {
            if !should_continue() {
                return RootDiscoveryOutcome {
                    emitted: child_emitted,
                    keep_going: false,
                };
            }
            let Ok(entry) = entry else {
                push_io_failure(
                    report,
                    dir,
                    InventoryScanFailureKind::ReadDirectory,
                    "read directory entry",
                );
                continue;
            };
            let path = entry.path();
            let Ok(metadata) = entry.metadata() else {
                push_io_failure(
                    report,
                    &path,
                    InventoryScanFailureKind::Metadata,
                    "read metadata",
                );
                continue;
            };

            if metadata.is_dir() && depth < self.options.max_depth {
                let outcome = self.discover_directory_roots_until(
                    &path,
                    depth + 1,
                    false,
                    report,
                    should_continue,
                    on_root,
                );
                child_emitted |= outcome.emitted;
                if !outcome.keep_going {
                    return RootDiscoveryOutcome {
                        emitted: child_emitted,
                        keep_going: false,
                    };
                }
            }
        }

        if child_emitted {
            return RootDiscoveryOutcome {
                emitted: true,
                keep_going: true,
            };
        }

        self.discover_direct_file_roots_until(dir, is_scan_root, report, should_continue, on_root)
    }

    fn discover_direct_file_roots_until<F, C>(
        &self,
        dir: &Path,
        is_scan_root: bool,
        report: &mut InventoryScanReport,
        should_continue: &mut C,
        on_root: &mut F,
    ) -> RootDiscoveryOutcome
    where
        F: FnMut(&Path, &mut InventoryScanReport, &mut C) -> bool,
        C: FnMut() -> bool,
    {
        if !should_continue() {
            return RootDiscoveryOutcome {
                emitted: false,
                keep_going: false,
            };
        }
        let Ok(entries) = fs::read_dir(dir) else {
            push_io_failure(
                report,
                dir,
                InventoryScanFailureKind::ReadDirectory,
                "read directory",
            );
            return RootDiscoveryOutcome {
                emitted: false,
                keep_going: true,
            };
        };

        let mut emitted = false;
        for entry in entries {
            if !should_continue() {
                return RootDiscoveryOutcome {
                    emitted,
                    keep_going: false,
                };
            }
            let Ok(entry) = entry else {
                push_io_failure(
                    report,
                    dir,
                    InventoryScanFailureKind::ReadDirectory,
                    "read directory entry",
                );
                continue;
            };
            let path = entry.path();
            let Ok(metadata) = entry.metadata() else {
                push_io_failure(
                    report,
                    &path,
                    InventoryScanFailureKind::Metadata,
                    "read metadata",
                );
                continue;
            };

            if !metadata.is_file() || !is_video_file(&path) {
                continue;
            }

            let root = if is_scan_root {
                path
            } else {
                dir.to_path_buf()
            };
            emitted = true;
            if !on_root(&root, report, should_continue) {
                return RootDiscoveryOutcome {
                    emitted,
                    keep_going: false,
                };
            }
            if !is_scan_root {
                return RootDiscoveryOutcome {
                    emitted: true,
                    keep_going: true,
                };
            }
        }

        RootDiscoveryOutcome {
            emitted,
            keep_going: true,
        }
    }

    fn scan_item_root_until<C>(
        &self,
        root: &Path,
        report: &mut InventoryScanReport,
        should_continue: &mut C,
    ) -> Option<ScannedLocalItem>
    where
        C: FnMut() -> bool,
    {
        let display_name = root.file_name().and_then(|name| name.to_str())?;
        let mut files = Vec::new();
        let completed = collect_video_files_until(
            root,
            root,
            self.options.max_depth,
            &mut files,
            report,
            should_continue,
        );
        if !completed {
            return None;
        }
        if files.is_empty() {
            return None;
        }

        let total_size = total_size(&files, root, report)?;
        let newest_mtime = files.iter().filter_map(|file| file.mtime_ms).max();
        let parsed_title = parse_media_title_from_paths(
            display_name,
            files.iter().map(|file| file.relative_path.as_path()),
        );
        let item = LocalItem {
            id: None,
            source: LocalItemSource::DataRoot {
                path: root.to_path_buf(),
            },
            title: ItemTitle::new(parsed_title.search_title).ok()?,
            display_name: DisplayName::new(display_name).ok()?,
            media_type: parsed_title.media_type,
            info_hash: None,
            path: Some(root.to_path_buf()),
            save_path: None,
            total_size,
            mtime_ms: newest_mtime,
        };

        let mut local_files = Vec::with_capacity(files.len());
        for (index, file) in files.into_iter().enumerate() {
            let Ok(index) = u32::try_from(index) else {
                push_failure(
                    report,
                    root,
                    InventoryScanFailureKind::Overflow,
                    "too many files under one local item",
                );
                return None;
            };
            let mtime_ms = file.mtime_ms;
            match LocalFile::new(None, file.relative_path, file.size, FileIndex::new(index)) {
                Ok(file) => local_files.push(file.with_mtime_ms(mtime_ms)),
                Err(error) => {
                    push_domain_failure(report, root, error);
                }
            }
        }

        if local_files.is_empty() {
            None
        } else {
            Some(ScannedLocalItem {
                item,
                files: local_files,
            })
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
struct FileExtensions {
    has_video: bool,
    has_video_disc: bool,
    has_rar: bool,
    has_audio: bool,
    has_book: bool,
    has_archive: bool,
}

impl FileExtensions {
    fn from_paths<'a>(file_paths: impl IntoIterator<Item = &'a Path>) -> Self {
        let mut extensions = Self::default();
        for path in file_paths {
            let Some(extension) = path.extension().and_then(|extension| extension.to_str()) else {
                continue;
            };
            match extension.to_ascii_lowercase().as_str() {
                "mkv" | "mp4" | "avi" | "mov" | "m4v" | "ts" | "wmv" | "flv" | "webm" => {
                    extensions.has_video = true;
                }
                "m2ts" | "ifo" | "vob" | "bup" => {
                    extensions.has_video_disc = true;
                }
                "rar" => {
                    extensions.has_rar = true;
                    extensions.has_archive = true;
                }
                "zip" | "7z" | "tar" | "gz" => {
                    extensions.has_archive = true;
                }
                "mp3" | "flac" | "m4a" | "aac" | "ogg" | "opus" | "wav" => {
                    extensions.has_audio = true;
                }
                "epub" | "mobi" | "azw3" | "pdf" | "cbz" | "cbr" => {
                    extensions.has_book = true;
                }
                _ => {}
            }
        }
        extensions
    }
}

fn parse_numbered_episode(value: &str) -> Option<EpisodeMatch> {
    let captures = episode_regex()
        .captures(value)
        .or_else(|| season_space_episode_regex().captures(value))?;
    Some(EpisodeMatch {
        start: captures.get(0)?.start(),
        season: captures.name("season")?.as_str().parse().ok()?,
        episode: captures.name("episode")?.as_str().parse().ok()?,
    })
}

fn parse_dated_episode(value: &str) -> Option<DateMatch> {
    let captures = dated_episode_regex().captures(value)?;
    Some(DateMatch {
        start: captures.get(0)?.start(),
        date: AirDate {
            year: captures.name("year")?.as_str().parse().ok()?,
            month: captures.name("month")?.as_str().parse().ok()?,
            day: captures.name("day")?.as_str().parse().ok()?,
        },
    })
}

fn parse_season_pack(value: &str) -> Option<SeasonMatch> {
    let captures = season_regex().captures(value)?;
    let season = captures
        .name("season")
        .or_else(|| captures.name("season_word"))?;
    Some(SeasonMatch {
        start: captures.get(0)?.start(),
        season: season.as_str().parse().ok()?,
    })
}

fn parse_movie_year(value: &str) -> Option<u16> {
    let captures = movie_year_regex().captures(value)?;
    captures.name("year")?.as_str().parse().ok()
}

fn parse_anime_episode(value: &str, release_group: Option<&str>) -> Option<AnimeEpisode> {
    let captures = anime_episode_regex().captures(value)?;
    let marker = captures.get(0)?;
    if release_group.is_none() && !value.contains("anime") && !value.contains("sub") {
        return None;
    }
    Some(AnimeEpisode {
        start: marker.start(),
        episode: captures.name("episode")?.as_str().parse().ok()?,
    })
}

fn parse_release_group(value: &str) -> Option<String> {
    let captures = bracketed_group_regex()
        .captures(value)
        .or_else(|| scene_group_regex().captures(value))?;
    let group = captures.name("group")?.as_str().trim();
    if is_bad_group(group) {
        None
    } else {
        Some(group.to_owned())
    }
}

fn strip_release_group(value: &str) -> &str {
    if let Some(captures) = bracketed_group_regex().captures(value)
        && let (Some(match_), Some(group)) = (captures.get(0), captures.name("group"))
        && match_.start() == 0
        && !is_bad_group(group.as_str())
    {
        return value.get(match_.end()..).unwrap_or(value).trim_start();
    }

    if let Some(captures) = scene_group_regex().captures(value)
        && let (Some(match_), Some(group)) = (captures.get(0), captures.name("group"))
        && match_.start() == 0
        && !is_bad_group(group.as_str())
    {
        return value.get(match_.end()..).unwrap_or(value).trim_start();
    }

    value
}

fn is_bad_group(group: &str) -> bool {
    matches!(
        group.to_ascii_lowercase().as_str(),
        "x264"
            | "x265"
            | "h264"
            | "h265"
            | "hevc"
            | "av1"
            | "aac"
            | "dts"
            | "truehd"
            | "1080p"
            | "2160p"
            | "720p"
            | "bluray"
            | "web-dl"
            | "webrip"
    )
}

fn normalize_title_input(value: &str) -> String {
    let mut normalized = String::with_capacity(value.len());
    for character in value.chars() {
        if matches!(character, '.' | '_' | '-' | '[' | ']' | '(' | ')') {
            normalized.push(' ');
        } else {
            normalized.push(character);
        }
    }
    normalized
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_owned()
}

fn title_before(value: &str, index: usize) -> String {
    value
        .get(..index)
        .unwrap_or(value)
        .split_whitespace()
        .filter(|token| !is_title_metadata_token(token))
        .collect::<Vec<_>>()
        .join(" ")
}

fn title_before_year(value: &str, year: u16) -> String {
    let year = year.to_string();
    value
        .split_whitespace()
        .take_while(|token| *token != year)
        .filter(|token| !is_title_metadata_token(token))
        .collect::<Vec<_>>()
        .join(" ")
}

fn strip_trailing_metadata(value: &str) -> String {
    value
        .split_whitespace()
        .take_while(|token| !is_title_metadata_token(token))
        .collect::<Vec<_>>()
        .join(" ")
}

fn fallback_title(candidate: &str, fallback: &str) -> String {
    let candidate = candidate.trim();
    if candidate.is_empty() {
        fallback.to_owned()
    } else {
        candidate.to_owned()
    }
}

fn is_title_metadata_token(token: &str) -> bool {
    matches!(
        token.to_ascii_lowercase().as_str(),
        "480p"
            | "576p"
            | "720p"
            | "1080p"
            | "2160p"
            | "4k"
            | "web"
            | "webdl"
            | "web-dl"
            | "webrip"
            | "bluray"
            | "brrip"
            | "hdtv"
            | "dvdrip"
            | "x264"
            | "x265"
            | "h264"
            | "h265"
            | "hevc"
            | "av1"
            | "proper"
            | "repack"
            | "extended"
    )
}

fn episode_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(r"(?i)(?:^|\b)S(?P<season>\d{1,2})\s*E(?P<episode>\d{1,3})(?:\s*E\d{1,3})?")
            .expect("episode regex should compile")
    })
}

fn season_space_episode_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(r"(?i)(?:^|\b)S(?P<season>\d{1,2})\s+(?P<episode>\d{2})(?:\b|$)")
            .expect("season-space-episode regex should compile")
    })
}

fn dated_episode_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(
            r"(?P<year>19\d{2}|20\d{2})\s+(?P<month>1[0-2]|0?[1-9])\s+(?P<day>3[01]|[12]\d|0?[1-9])(?:\b|$)",
        )
        .expect("dated episode regex should compile")
    })
}

fn season_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(
            r"(?i)(?:^|\b)(?:S(?P<season>\d{1,2})|Season\s+(?P<season_word>\d{1,2}))(?:\b|$)",
        )
        .expect("season regex should compile")
    })
}

fn movie_year_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(r"(?:^|\b)(?P<year>19\d{2}|20\d{2})(?:\b|$)")
            .expect("movie year regex should compile")
    })
}

fn anime_episode_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(r"(?:^|\b)(?P<episode>\d{2,3})(?:v\d+)?(?:\b|$)")
            .expect("anime episode regex should compile")
    })
}

fn bracketed_group_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(r"^\[(?P<group>[^\]]{2,32})\]\s*").expect("bracketed group regex should compile")
    })
}

fn scene_group_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(r"^(?P<group>[A-Za-z0-9][A-Za-z0-9._-]{1,31})\s+-\s+")
            .expect("scene group regex should compile")
    })
}

fn collect_video_files_until<C>(
    root: &Path,
    current: &Path,
    remaining_depth: u16,
    files: &mut Vec<ScannedFile>,
    report: &mut InventoryScanReport,
    should_continue: &mut C,
) -> bool
where
    C: FnMut() -> bool,
{
    if !should_continue() {
        return false;
    }

    let Ok(metadata) = fs::symlink_metadata(current) else {
        push_io_failure(
            report,
            current,
            InventoryScanFailureKind::Metadata,
            "read metadata",
        );
        return true;
    };

    if metadata.is_file() {
        collect_one_file(root, current, &metadata, files, report);
        return true;
    }

    if !metadata.is_dir() || should_ignore_dir(current) {
        return true;
    }

    let Ok(entries) = fs::read_dir(current) else {
        push_io_failure(
            report,
            current,
            InventoryScanFailureKind::ReadDirectory,
            "read directory",
        );
        return true;
    };

    for entry in entries {
        if !should_continue() {
            return false;
        }
        let Ok(entry) = entry else {
            push_io_failure(
                report,
                current,
                InventoryScanFailureKind::ReadDirectory,
                "read directory entry",
            );
            continue;
        };
        let path = entry.path();
        let Ok(metadata) = entry.metadata() else {
            push_io_failure(
                report,
                &path,
                InventoryScanFailureKind::Metadata,
                "read metadata",
            );
            continue;
        };

        if metadata.is_file() {
            collect_one_file(root, &path, &metadata, files, report);
        } else if metadata.is_dir()
            && remaining_depth > 0
            && !collect_video_files_until(
                root,
                &path,
                remaining_depth - 1,
                files,
                report,
                should_continue,
            )
        {
            return false;
        }
    }
    true
}

fn collect_one_file(
    root: &Path,
    path: &Path,
    metadata: &fs::Metadata,
    files: &mut Vec<ScannedFile>,
    report: &mut InventoryScanReport,
) {
    if !is_video_file(path) {
        return;
    }
    if !path_has_utf8_name(path) {
        push_failure(
            report,
            path,
            InventoryScanFailureKind::NonUtf8Path,
            "file name is not valid UTF-8",
        );
        return;
    }

    let relative_path = if root == path {
        match path.file_name() {
            Some(name) => PathBuf::from(name),
            None => {
                push_failure(
                    report,
                    path,
                    InventoryScanFailureKind::Domain,
                    "file path has no file name",
                );
                return;
            }
        }
    } else {
        match path.strip_prefix(root) {
            Ok(relative_path) => relative_path.to_path_buf(),
            Err(error) => {
                push_failure(
                    report,
                    path,
                    InventoryScanFailureKind::Domain,
                    format!("file is not under item root: {error}"),
                );
                return;
            }
        }
    };

    files.push(ScannedFile {
        relative_path,
        size: ByteSize::new(metadata.len()),
        mtime_ms: metadata
            .modified()
            .ok()
            .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
            .and_then(|duration| i64::try_from(duration.as_millis()).ok()),
    });
}

fn total_size(
    files: &[ScannedFile],
    root: &Path,
    report: &mut InventoryScanReport,
) -> Option<ByteSize> {
    let mut total = 0_u64;
    for file in files {
        let Some(next_total) = total.checked_add(file.size.get()) else {
            push_failure(
                report,
                root,
                InventoryScanFailureKind::Overflow,
                "local item file sizes exceed u64",
            );
            return None;
        };
        total = next_total;
    }
    Some(ByteSize::new(total))
}

fn is_video_file(path: &Path) -> bool {
    let Some(extension) = path.extension().and_then(|extension| extension.to_str()) else {
        return false;
    };
    matches!(
        extension.to_ascii_lowercase().as_str(),
        "mkv" | "mp4" | "avi" | "mov" | "m4v" | "ts" | "m2ts" | "wmv" | "flv" | "webm"
    )
}

fn should_ignore_dir(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return true;
    };
    let name = name.to_ascii_lowercase();
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

fn path_has_utf8_name(path: &Path) -> bool {
    path.file_name().is_some_and(|name| name.to_str().is_some())
}

fn push_io_failure(
    report: &mut InventoryScanReport,
    path: &Path,
    kind: InventoryScanFailureKind,
    operation: &'static str,
) {
    let message = match fs::metadata(path) {
        Ok(_) => operation.to_owned(),
        Err(error) => format!("{operation}: {error}"),
    };
    push_failure(report, path, kind, message);
}

fn push_domain_failure(report: &mut InventoryScanReport, path: &Path, error: DomainError) {
    push_failure(
        report,
        path,
        InventoryScanFailureKind::Domain,
        error.to_string(),
    );
}

fn push_failure(
    report: &mut InventoryScanReport,
    path: &Path,
    kind: InventoryScanFailureKind,
    message: impl Into<String>,
) {
    report.failures.push(InventoryScanFailure {
        path: path.to_path_buf(),
        kind,
        message: message.into(),
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn parser_classifies_episode_and_season_precedence() {
        let episode = parse_media_title("My.Show.S01E01.1080p", &[]);
        let multi_episode = parse_media_title("My.Show.S01E01E02.1080p", &[]);
        let episode_with_season_text = parse_media_title("My.Show.S01E01.Season.1", &[]);
        let short_season_pack = parse_media_title("My.Show.S01", &[]);
        let season_pack = parse_media_title("My.Show.Season.2", &[]);
        let spaced_episode = parse_media_title("My.Show.S01.02", &[]);

        assert_eq!(MediaType::Episode, episode.media_type);
        assert_eq!("My Show S01E01", episode.search_title);
        assert_eq!(Some(1), episode.season);
        assert_eq!(Some(1), episode.episode);
        assert_eq!(MediaType::Episode, multi_episode.media_type);
        assert_eq!("My Show S01E01", multi_episode.search_title);
        assert_eq!(MediaType::Episode, episode_with_season_text.media_type);
        assert_eq!("My Show S01E01", episode_with_season_text.search_title);
        assert_eq!(MediaType::SeasonPack, short_season_pack.media_type);
        assert_eq!("My Show S01", short_season_pack.search_title);
        assert_eq!(MediaType::SeasonPack, season_pack.media_type);
        assert_eq!("My Show S02", season_pack.search_title);
        assert_eq!(MediaType::Episode, spaced_episode.media_type);
        assert_eq!("My Show S01E02", spaced_episode.search_title);
    }

    #[test]
    fn parser_handles_dated_episodes_and_movie_years() {
        let dated = parse_media_title("Daily.Show.2024.01.31.720p", &[]);
        let movie = parse_media_title(
            "Example.Movie.(2023).1080p",
            &[PathBuf::from("Example.Movie.2023.mkv")],
        );
        let bracketed = parse_media_title(
            "Another.Movie.[2022].WEB-DL",
            &[PathBuf::from("Another.Movie.2022.MP4")],
        );
        let year_only = parse_media_title(
            "Example.Show.2024.1080p",
            &[PathBuf::from("Example.Show.2024.mkv")],
        );

        assert_eq!(MediaType::Episode, dated.media_type);
        assert_eq!("Daily Show 2024-01-31", dated.search_title);
        assert_eq!(
            Some(AirDate {
                year: 2024,
                month: 1,
                day: 31
            }),
            dated.air_date
        );
        assert_eq!(MediaType::Movie, movie.media_type);
        assert_eq!("Example Movie 2023", movie.search_title);
        assert_eq!(Some(2023), movie.year);
        assert_eq!(MediaType::Movie, bracketed.media_type);
        assert_eq!("Another Movie 2022", bracketed.search_title);
        assert_eq!(MediaType::Movie, year_only.media_type);
        assert_eq!(None, year_only.episode);
    }

    #[test]
    fn parser_handles_anime_scene_prefixes_and_bad_groups() {
        let anime = parse_media_title(
            "[SubsPlease] Frieren - 03 (1080p)",
            &[PathBuf::from("Frieren.03.mkv")],
        );
        let bad_group = parse_media_title(
            "x264 - Example.Movie.2020.1080p",
            &[PathBuf::from("Example.Movie.2020.mkv")],
        );

        assert_eq!(MediaType::Anime, anime.media_type);
        assert_eq!(Some("SubsPlease".to_owned()), anime.release_group);
        assert_eq!("Frieren 03", anime.search_title);
        assert_eq!(Some(3), anime.episode);
        assert_eq!(None, bad_group.release_group);
        assert_eq!("Example Movie 2020", bad_group.search_title);
    }

    #[test]
    fn media_type_detection_preserves_rar_fallthrough_and_archive_classification() {
        assert_eq!(
            MediaType::Movie,
            classify_media_type_from_name("Movie.2020", &[PathBuf::from("movie.rar")])
        );
        assert_eq!(
            MediaType::Audio,
            classify_media_type_from_name(
                "Album.Release",
                &[PathBuf::from("album.rar"), PathBuf::from("track.flac")]
            )
        );
        assert_eq!(
            MediaType::Audio,
            classify_media_type_from_name(
                "Mixed.Release",
                &[
                    PathBuf::from("mixed.rar"),
                    PathBuf::from("track.flac"),
                    PathBuf::from("book.epub")
                ]
            )
        );
        assert_eq!(
            MediaType::Book,
            classify_media_type_from_name(
                "Book.Release",
                &[PathBuf::from("book.rar"), PathBuf::from("book.epub")]
            )
        );
        assert_eq!(
            MediaType::Unknown,
            classify_media_type_from_name("Archive.Release", &[PathBuf::from("archive.rar")])
        );
        assert_eq!(
            MediaType::Archive,
            classify_media_type_from_name("Archive.Release", &[PathBuf::from("archive.zip")])
        );
    }

    #[test]
    fn media_type_detection_handles_video_disc_uppercase_and_title_regex_wins() {
        assert_eq!(
            MediaType::Movie,
            classify_media_type_from_name("Movie.2021", &[PathBuf::from("BDMV/INDEX.IFO")])
        );
        assert_eq!(
            MediaType::Video,
            classify_media_type_from_name("Disc.Release", &[PathBuf::from("BDMV/STREAM.M2TS")])
        );
        assert_eq!(
            MediaType::Video,
            classify_media_type_from_name("Disc.Release", &[PathBuf::from("BDMV/BACKUP.BUP")])
        );
        assert_eq!(
            MediaType::Video,
            classify_media_type_from_name("Concert.Release", &[PathBuf::from("VIDEO_TS/VTS.VOB")])
        );
        assert_eq!(
            MediaType::Movie,
            classify_media_type_from_name(
                "Movie.2021",
                &[PathBuf::from("cover.mp3"), PathBuf::from("movie.mkv")]
            )
        );
        assert_eq!(
            MediaType::Video,
            classify_media_type_from_name("Generic.Release", &[PathBuf::from("GENERIC.MKV")])
        );
        assert_eq!(
            MediaType::Episode,
            classify_media_type_from_name("Show.S01E01", &[PathBuf::from("track.mp3")])
        );
    }

    #[test]
    fn scan_media_dirs_builds_items_and_ignores_noise_dirs() {
        let root = unique_temp_dir("basic");
        let release = root.join("Movie.2024.1080p");
        fs::create_dir_all(&release).unwrap();
        write_file(&release.join("movie.mkv"), 10);
        write_file(&release.join("notes.txt"), 20);

        for ignored in [
            "sample",
            "proof",
            "BDMV",
            "bdrom",
            "CERTIFICATE",
            "VIDEO_TS",
        ] {
            let ignored_dir = release.join(ignored);
            fs::create_dir_all(&ignored_dir).unwrap();
            write_file(&ignored_dir.join("ignored.mkv"), 30);
        }

        let scanner = InventoryScanner::new(InventoryScanOptions { max_depth: 3 });
        let report = scanner.scan_media_dirs(std::slice::from_ref(&root));

        assert!(report.failures.is_empty());
        assert_eq!(1, report.items.len());
        let scanned = &report.items[0];
        assert_eq!("Movie.2024.1080p", scanned.item.display_name.as_str());
        assert_eq!(ByteSize::new(10), scanned.item.total_size);
        assert_eq!(Some(release), scanned.item.path);
        assert_eq!(1, scanned.files.len());
        assert_eq!(PathBuf::from("movie.mkv"), scanned.files[0].relative_path);

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn scan_media_dirs_with_streams_items_without_collecting_report_items() {
        let root = unique_temp_dir("stream");
        let first = root.join("First.2024.1080p");
        let second = root.join("Second.2024.1080p");
        fs::create_dir_all(&first).unwrap();
        fs::create_dir_all(&second).unwrap();
        write_file(&first.join("first.mkv"), 10);
        write_file(&second.join("second.mkv"), 20);
        let scanner = InventoryScanner::new(InventoryScanOptions { max_depth: 3 });
        let mut names = Vec::new();

        let report = scanner.scan_media_dirs_with(std::slice::from_ref(&root), |item| {
            names.push(item.item.display_name.as_str().to_owned());
            true
        });
        names.sort();

        assert!(report.failures.is_empty());
        assert_eq!(2, report.scanned_items);
        assert_eq!(
            vec![
                "First.2024.1080p".to_owned(),
                "Second.2024.1080p".to_owned()
            ],
            names
        );

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn scan_media_dirs_with_stops_after_callback_returns_false() {
        let root = unique_temp_dir("stream-stop");
        let first = root.join("First.2024.1080p");
        let second = root.join("Second.2024.1080p");
        fs::create_dir_all(&first).unwrap();
        fs::create_dir_all(&second).unwrap();
        write_file(&first.join("first.mkv"), 10);
        write_file(&second.join("second.mkv"), 20);
        let scanner = InventoryScanner::new(InventoryScanOptions { max_depth: 3 });
        let mut names = Vec::new();

        let report = scanner.scan_media_dirs_with(std::slice::from_ref(&root), |item| {
            names.push(item.item.display_name.as_str().to_owned());
            false
        });

        assert!(report.failures.is_empty());
        assert_eq!(1, report.scanned_items);
        assert_eq!(1, names.len());

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn scan_media_dirs_with_stops_after_direct_file_callback_returns_false() {
        let root = unique_temp_dir("stream-stop-direct");
        fs::create_dir_all(&root).unwrap();
        write_file(&root.join("first.mkv"), 10);
        write_file(&root.join("second.mkv"), 20);
        let scanner = InventoryScanner::new(InventoryScanOptions { max_depth: 3 });
        let mut names = Vec::new();

        let report = scanner.scan_media_dirs_with(std::slice::from_ref(&root), |item| {
            names.push(item.item.display_name.as_str().to_owned());
            false
        });

        assert!(report.failures.is_empty());
        assert_eq!(1, report.scanned_items);
        assert_eq!(1, names.len());

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn scan_media_dirs_until_stops_during_directory_walk() {
        let root = unique_temp_dir("stream-cancel");
        for index in 0..64 {
            let release = root.join(format!("Release.{index:02}.2024"));
            fs::create_dir_all(&release).unwrap();
            write_file(&release.join("movie.mkv"), 10);
        }
        let scanner = InventoryScanner::new(InventoryScanOptions { max_depth: 3 });
        let mut checks = 0usize;
        let mut emitted = false;

        let report = scanner.scan_media_dirs_until(
            std::slice::from_ref(&root),
            || {
                checks = checks.saturating_add(1);
                checks <= 3
            },
            |_item| {
                emitted = true;
                true
            },
        );

        assert_eq!(0, report.scanned_items);
        assert!(!emitted);
        assert!(checks > 3);

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn scan_media_dirs_until_does_not_emit_partial_file_walk() {
        let root = unique_temp_dir("stream-cancel-files");
        let release = root.join("Movie.2024.1080p");
        fs::create_dir_all(&release).unwrap();
        for index in 0..64 {
            write_file(&release.join(format!("part-{index:02}.mkv")), 10);
        }
        let scanner = InventoryScanner::new(InventoryScanOptions { max_depth: 3 });
        let mut checks = 0usize;
        let mut emitted = false;

        let report = scanner.scan_media_dirs_until(
            std::slice::from_ref(&root),
            || {
                checks = checks.saturating_add(1);
                checks <= 8
            },
            |_item| {
                emitted = true;
                true
            },
        );

        assert_eq!(0, report.scanned_items);
        assert!(!emitted);
        assert!(checks > 8);

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn scan_media_dirs_respects_configured_depth() {
        let root = unique_temp_dir("depth");
        let shallow = root.join("Shallow");
        let deep = root.join("A/B/C/D");
        fs::create_dir_all(&shallow).unwrap();
        fs::create_dir_all(&deep).unwrap();
        write_file(&shallow.join("shallow.mkv"), 10);
        write_file(&deep.join("deep.mkv"), 10);

        let scanner = InventoryScanner::new(InventoryScanOptions { max_depth: 1 });
        let report = scanner.scan_media_dirs(std::slice::from_ref(&root));

        assert_eq!(1, report.items.len());
        assert_eq!("Shallow", report.items[0].item.display_name.as_str());

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn scan_media_dirs_handles_deleted_or_unreadable_roots() {
        let root = unique_temp_dir("deleted");
        let missing = root.join("missing");
        let scanner = InventoryScanner::new(InventoryScanOptions::default());
        let report = scanner.scan_media_dirs(std::slice::from_ref(&missing));

        assert!(report.items.is_empty());
        assert_eq!(1, report.failures.len());
        assert_eq!(missing, report.failures[0].path);
        assert_eq!(InventoryScanFailureKind::Metadata, report.failures[0].kind);

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn scan_media_dirs_handles_large_release_directories() {
        let root = unique_temp_dir("large");
        let release = root.join("Large.Release");
        fs::create_dir_all(&release).unwrap();
        for index in 0..300 {
            write_file(&release.join(format!("episode-{index:03}.mkv")), 1);
        }

        let scanner = InventoryScanner::new(InventoryScanOptions::default());
        let report = scanner.scan_media_dirs(std::slice::from_ref(&root));

        assert_eq!(1, report.items.len());
        assert_eq!(300, report.items[0].files.len());
        assert_eq!(ByteSize::new(300), report.items[0].item.total_size);
        assert_eq!(MediaType::Video, report.items[0].item.media_type);

        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(all(unix, not(target_os = "macos")))]
    #[test]
    fn scan_media_dirs_skips_non_utf8_file_names() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        let root = unique_temp_dir("non-utf8");
        let release = root.join("Release");
        fs::create_dir_all(&release).unwrap();
        write_file(&release.join("valid.mkv"), 1);
        let invalid_name = OsString::from_vec(vec![b'b', b'a', b'd', 0xff, b'.', b'm', b'k', b'v']);
        write_file(&release.join(invalid_name), 1);

        let scanner = InventoryScanner::new(InventoryScanOptions::default());
        let report = scanner.scan_media_dirs(std::slice::from_ref(&root));

        assert_eq!(1, report.items.len());
        assert_eq!(1, report.items[0].files.len());
        assert!(
            report
                .failures
                .iter()
                .any(|failure| failure.kind == InventoryScanFailureKind::NonUtf8Path)
        );

        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn scan_media_dirs_reports_permission_failures_and_continues() {
        use std::os::unix::fs::PermissionsExt;

        let root = unique_temp_dir("permission");
        let denied = root.join("Denied");
        let allowed = root.join("Allowed");
        fs::create_dir_all(&denied).unwrap();
        fs::create_dir_all(&allowed).unwrap();
        write_file(&denied.join("hidden.mkv"), 1);
        write_file(&allowed.join("visible.mkv"), 1);
        let original_permissions = fs::metadata(&denied).unwrap().permissions();
        fs::set_permissions(&denied, fs::Permissions::from_mode(0o000)).unwrap();

        if fs::read_dir(&denied).is_ok() {
            fs::set_permissions(&denied, original_permissions).unwrap();
            fs::remove_dir_all(root).unwrap();
            return;
        }

        let scanner = InventoryScanner::new(InventoryScanOptions::default());
        let report = scanner.scan_media_dirs(std::slice::from_ref(&root));

        fs::set_permissions(&denied, original_permissions).unwrap();

        assert_eq!(1, report.items.len());
        assert_eq!("Allowed", report.items[0].item.display_name.as_str());
        assert!(
            report
                .failures
                .iter()
                .any(|failure| failure.kind == InventoryScanFailureKind::ReadDirectory)
        );

        fs::remove_dir_all(root).unwrap();
    }

    fn unique_temp_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("sporos-inventory-test-{label}-{nanos}"));
        fs::create_dir_all(&path).unwrap();
        path
    }

    fn write_file(path: &Path, size: usize) {
        let mut file = File::create(path).unwrap();
        let bytes = vec![b'x'; size];
        file.write_all(&bytes).unwrap();
    }
}
