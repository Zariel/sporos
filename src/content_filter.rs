use std::path::Path;

use regex::Regex;

use crate::domain::{ByteSize, InfoHash, LocalFile, LocalItem, LocalItemSource, MediaType};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ContentFilterConfig {
    pub blocklist: Vec<BlocklistRule>,
    pub blocklist_only: bool,
    pub include_single_episodes: bool,
    pub include_non_videos: bool,
    pub fuzzy_size_threshold: Permille,
    pub ignore_existing_seed_markers: bool,
    pub allow_season_pack_episodes: bool,
    pub allow_season_specials: bool,
    pub allow_non_standard_naming: bool,
}

impl Default for ContentFilterConfig {
    fn default() -> Self {
        Self {
            blocklist: Vec::new(),
            blocklist_only: false,
            include_single_episodes: false,
            include_non_videos: false,
            fuzzy_size_threshold: Permille::new(20),
            ignore_existing_seed_markers: true,
            allow_season_pack_episodes: false,
            allow_season_specials: false,
            allow_non_standard_naming: false,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct Permille(u16);

impl Permille {
    pub const fn new(value: u16) -> Self {
        Self(if value > 1_000 { 1_000 } else { value })
    }

    pub const fn get(self) -> u16 {
        self.0
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum BlocklistRule {
    NameSubstring(String),
    NameRegex(String),
    FolderSubstring(String),
    Tag(String),
    TrackerHost(String),
    InfoHash(InfoHash),
    SizeBelow(ByteSize),
    SizeAbove(ByteSize),
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ContentFilterContext {
    Search,
    ReverseLookup,
    Announcement,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ContentFilterDecision {
    Accepted,
    Rejected(ContentFilterReason),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ContentFilterReason {
    BlockedRelease { rule: BlocklistRule },
    DataDirSeasonPackEpisode,
    SingleEpisode,
    NonVideoRatio,
    ExistingExternalSeed,
    ArrRoot,
    SeasonSpecial,
    NonStandardNaming,
}

#[derive(Debug, Clone, Copy)]
pub struct ContentFilterSubject<'a> {
    pub item: &'a LocalItem,
    pub files: &'a [LocalFile],
    pub metadata: ContentMetadata<'a>,
    pub context: ContentFilterContext,
}

#[derive(Debug, Clone, Copy)]
pub struct CandidateBlocklistSubject<'a> {
    pub display_name: &'a str,
    pub tracker_hosts: &'a [&'a str],
    pub info_hash: Option<&'a InfoHash>,
    pub size: Option<ByteSize>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ContentMetadata<'a> {
    pub tags: &'a [&'a str],
    pub tracker_hosts: &'a [&'a str],
    pub category: Option<&'a str>,
    pub link_category: Option<&'a str>,
}

pub fn filter_content(
    subject: ContentFilterSubject<'_>,
    config: &ContentFilterConfig,
) -> ContentFilterDecision {
    if let Some(rule) = config.blocklist.iter().find(|rule| rule.matches(&subject)) {
        return ContentFilterDecision::Rejected(ContentFilterReason::BlockedRelease {
            rule: rule.clone(),
        });
    }

    if config.blocklist_only {
        return ContentFilterDecision::Accepted;
    }

    if !config.allow_season_pack_episodes && is_data_dir_single_file_in_season_pack(&subject) {
        return ContentFilterDecision::Rejected(ContentFilterReason::DataDirSeasonPackEpisode);
    }

    if !config.include_single_episodes
        && subject.context != ContentFilterContext::Announcement
        && subject.item.media_type == MediaType::Episode
    {
        return ContentFilterDecision::Rejected(ContentFilterReason::SingleEpisode);
    }

    if !config.include_non_videos && non_video_ratio_exceeds(subject.files, config) {
        return ContentFilterDecision::Rejected(ContentFilterReason::NonVideoRatio);
    }

    if config.ignore_existing_seed_markers && has_external_seed_marker(subject.metadata) {
        return ContentFilterDecision::Rejected(ContentFilterReason::ExistingExternalSeed);
    }

    if is_arr_root(&subject) {
        return ContentFilterDecision::Rejected(ContentFilterReason::ArrRoot);
    }

    if !config.allow_season_specials && is_season_special(subject.files) {
        return ContentFilterDecision::Rejected(ContentFilterReason::SeasonSpecial);
    }

    if !config.allow_non_standard_naming
        && subject.context != ContentFilterContext::Announcement
        && has_non_standard_episode_naming(&subject)
    {
        return ContentFilterDecision::Rejected(ContentFilterReason::NonStandardNaming);
    }

    ContentFilterDecision::Accepted
}

impl BlocklistRule {
    pub fn matches_candidate(&self, subject: CandidateBlocklistSubject<'_>) -> bool {
        match self {
            Self::NameSubstring(value) => subject.display_name.contains(value),
            Self::NameRegex(pattern) => Regex::new(pattern)
                .map(|regex| regex.is_match(subject.display_name))
                .unwrap_or(false),
            Self::TrackerHost(value) => {
                subject.tracker_hosts.iter().any(|tracker| tracker == value)
            }
            Self::InfoHash(value) => subject.info_hash == Some(value),
            Self::SizeBelow(value) => subject.size.is_some_and(|size| size.get() < value.get()),
            Self::SizeAbove(value) => subject.size.is_some_and(|size| size.get() > value.get()),
            Self::FolderSubstring(_) | Self::Tag(_) => false,
        }
    }

    fn matches(&self, subject: &ContentFilterSubject<'_>) -> bool {
        match self {
            Self::NameSubstring(value) => subject.item.display_name.as_str().contains(value),
            Self::NameRegex(pattern) => Regex::new(pattern)
                .map(|regex| regex.is_match(subject.item.display_name.as_str()))
                .unwrap_or(false),
            Self::FolderSubstring(value) => subject
                .item
                .path
                .as_ref()
                .and_then(|path| path.parent())
                .is_some_and(|path| path_to_string(path).contains(value)),
            Self::Tag(value) => {
                if value.is_empty() {
                    subject.metadata.tags.is_empty()
                } else {
                    subject.metadata.tags.iter().any(|tag| tag == value)
                }
            }
            Self::TrackerHost(value) => subject
                .metadata
                .tracker_hosts
                .iter()
                .any(|tracker| tracker == value),
            Self::InfoHash(value) => subject.item.info_hash.as_ref() == Some(value),
            Self::SizeBelow(value) => subject.item.total_size.get() < value.get(),
            Self::SizeAbove(value) => subject.item.total_size.get() > value.get(),
        }
    }
}

fn is_data_dir_single_file_in_season_pack(subject: &ContentFilterSubject<'_>) -> bool {
    matches!(subject.item.source, LocalItemSource::DataRoot { .. })
        && subject.item.media_type == MediaType::Episode
        && subject.files.len() == 1
        && subject
            .item
            .path
            .as_ref()
            .and_then(|path| path.parent())
            .and_then(Path::file_name)
            .and_then(|name| name.to_str())
            .is_some_and(looks_like_season_pack_name)
}

fn non_video_ratio_exceeds(files: &[LocalFile], config: &ContentFilterConfig) -> bool {
    let total = files.iter().map(|file| file.size.get()).sum::<u64>();
    if total == 0 {
        return false;
    }
    let non_video = files
        .iter()
        .filter(|file| !is_video_path(&file.relative_path))
        .map(|file| file.size.get())
        .sum::<u64>();

    non_video.saturating_mul(1_000)
        > total.saturating_mul(u64::from(config.fuzzy_size_threshold.get()))
}

fn has_external_seed_marker(metadata: ContentMetadata<'_>) -> bool {
    metadata
        .link_category
        .is_some_and(has_external_seed_suffix_or_value)
        || metadata
            .category
            .is_some_and(has_external_seed_suffix_or_value)
        || metadata
            .tags
            .iter()
            .any(|tag| has_external_seed_suffix_or_value(tag))
}

fn has_external_seed_suffix_or_value(value: &str) -> bool {
    let marker = external_seed_marker();
    value == marker || value.ends_with(&format!(".{marker}"))
}

fn external_seed_marker() -> String {
    ["cross", "-", "seed"].concat()
}

fn is_arr_root(subject: &ContentFilterSubject<'_>) -> bool {
    matches!(subject.item.source, LocalItemSource::DataRoot { .. })
        && subject.item.media_type == MediaType::Unknown
        && subject.files.is_empty()
}

fn is_season_special(files: &[LocalFile]) -> bool {
    files.iter().any(|file| {
        file.relative_path.components().any(|component| {
            component
                .as_os_str()
                .to_str()
                .map(|name| {
                    let normalized = name.to_ascii_lowercase();
                    normalized == "specials"
                        || normalized == "season 0"
                        || normalized == "season 00"
                        || normalized == "s00"
                })
                .unwrap_or(false)
        })
    })
}

fn has_non_standard_episode_naming(subject: &ContentFilterSubject<'_>) -> bool {
    subject.item.media_type == MediaType::Episode
        && !Regex::new(r"(?i)\bs\d{1,2}e\d{1,3}\b")
            .map(|regex| regex.is_match(subject.item.display_name.as_str()))
            .unwrap_or(false)
}

fn looks_like_season_pack_name(name: &str) -> bool {
    let normalized = name.to_ascii_lowercase();
    normalized.contains("season") || normalized.contains("s01") || normalized.contains("s02")
}

fn is_video_path(path: &Path) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .map(|extension| {
            matches!(
                extension.to_ascii_lowercase().as_str(),
                "mkv" | "mp4" | "avi" | "mov" | "m4v" | "ts" | "webm" | "wmv"
            )
        })
        .unwrap_or(false)
}

fn path_to_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::domain::{DisplayName, FileIndex, ItemTitle, LocalItemId, SourceKey};

    #[test]
    fn blocklist_rules_cover_name_regex_folder_tag_tracker_hash_and_size() {
        let hash = InfoHash::new("0123456789abcdef0123456789abcdef01234567").unwrap();
        let item = local_item("Blocked.Release", MediaType::Movie)
            .with_path(PathBuf::from("/media/blocked/Blocked.Release"))
            .with_hash(hash.clone())
            .with_size(50);
        let files = vec![local_file("movie.mkv", 50)];

        for rule in [
            BlocklistRule::NameSubstring("Blocked".to_owned()),
            BlocklistRule::NameRegex("Blocked[.]Release".to_owned()),
            BlocklistRule::FolderSubstring("blocked".to_owned()),
            BlocklistRule::Tag("bad".to_owned()),
            BlocklistRule::TrackerHost("tracker.example".to_owned()),
            BlocklistRule::InfoHash(hash),
            BlocklistRule::SizeBelow(ByteSize::new(100)),
            BlocklistRule::SizeAbove(ByteSize::new(10)),
        ] {
            let config = ContentFilterConfig {
                blocklist: vec![rule.clone()],
                ..ContentFilterConfig::default()
            };
            let decision = filter_content(
                ContentFilterSubject {
                    item: &item.0,
                    files: &files,
                    metadata: ContentMetadata {
                        tags: &["bad"],
                        tracker_hosts: &["tracker.example"],
                        category: None,
                        link_category: None,
                    },
                    context: ContentFilterContext::Search,
                },
                &config,
            );

            assert_eq!(
                ContentFilterDecision::Rejected(ContentFilterReason::BlockedRelease { rule }),
                decision
            );
        }
    }

    #[test]
    fn blocklist_name_and_size_rules_use_strict_boundaries() {
        let item = local_item("Blocked.Release", MediaType::Movie).with_size(50);
        let files = vec![local_file("movie.mkv", 50)];

        for rule in [
            BlocklistRule::NameSubstring("blocked".to_owned()),
            BlocklistRule::SizeBelow(ByteSize::new(50)),
            BlocklistRule::SizeAbove(ByteSize::new(50)),
        ] {
            let decision = filter_content(
                subject(
                    &item.0,
                    &files,
                    ContentMetadata::default(),
                    ContentFilterContext::Search,
                ),
                &ContentFilterConfig {
                    blocklist: vec![rule],
                    include_non_videos: true,
                    ..ContentFilterConfig::default()
                },
            );

            assert_eq!(ContentFilterDecision::Accepted, decision);
        }
    }

    #[test]
    fn candidate_blocklist_rules_use_available_metadata() {
        let hash = InfoHash::new("0123456789abcdef0123456789abcdef01234567").unwrap();
        let subject = CandidateBlocklistSubject {
            display_name: "Blocked.Release",
            tracker_hosts: &["tracker.example"],
            info_hash: Some(&hash),
            size: Some(ByteSize::new(50)),
        };

        for rule in [
            BlocklistRule::NameSubstring("Blocked".to_owned()),
            BlocklistRule::NameRegex("Blocked[.]Release".to_owned()),
            BlocklistRule::TrackerHost("tracker.example".to_owned()),
            BlocklistRule::InfoHash(hash.clone()),
            BlocklistRule::SizeBelow(ByteSize::new(100)),
            BlocklistRule::SizeAbove(ByteSize::new(10)),
        ] {
            assert!(rule.matches_candidate(subject));
        }

        for rule in [
            BlocklistRule::FolderSubstring("blocked".to_owned()),
            BlocklistRule::Tag("bad".to_owned()),
            BlocklistRule::SizeBelow(ByteSize::new(50)),
            BlocklistRule::SizeAbove(ByteSize::new(50)),
        ] {
            assert!(!rule.matches_candidate(subject));
        }
    }

    #[test]
    fn empty_tag_blocklist_matches_absent_tags() {
        let item = local_item("Untagged", MediaType::Movie);
        let decision = filter_content(
            subject(
                &item.0,
                &[],
                ContentMetadata::default(),
                ContentFilterContext::Search,
            ),
            &ContentFilterConfig {
                blocklist: vec![BlocklistRule::Tag(String::new())],
                ..ContentFilterConfig::default()
            },
        );

        assert!(matches!(
            decision,
            ContentFilterDecision::Rejected(ContentFilterReason::BlockedRelease { .. })
        ));
    }

    #[test]
    fn blocklist_only_accepts_after_blocklist_check() {
        let item = local_item("Show.S01E01", MediaType::Episode);
        let files = vec![local_file("Show.S01E01.mkv", 10)];
        let decision = filter_content(
            subject(
                &item.0,
                &files,
                ContentMetadata::default(),
                ContentFilterContext::Search,
            ),
            &ContentFilterConfig {
                blocklist_only: true,
                ..ContentFilterConfig::default()
            },
        );

        assert_eq!(ContentFilterDecision::Accepted, decision);
    }

    #[test]
    fn filters_single_episodes_except_announcements() {
        let item = local_item("Show.S01E01", MediaType::Episode);
        let files = vec![local_file("Show.S01E01.mkv", 10)];

        let search = filter_content(
            subject(
                &item.0,
                &files,
                ContentMetadata::default(),
                ContentFilterContext::Search,
            ),
            &ContentFilterConfig::default(),
        );
        let announce = filter_content(
            subject(
                &item.0,
                &files,
                ContentMetadata::default(),
                ContentFilterContext::Announcement,
            ),
            &ContentFilterConfig::default(),
        );

        assert_eq!(
            ContentFilterDecision::Rejected(ContentFilterReason::SingleEpisode),
            search
        );
        assert_eq!(ContentFilterDecision::Accepted, announce);
    }

    #[test]
    fn filters_non_video_ratio_with_strict_boundary() {
        let item = local_item("Mixed", MediaType::Movie).with_size(100);
        let at_boundary = vec![local_file("movie.mkv", 98), local_file("extras.nfo", 2)];
        let above_boundary = vec![local_file("movie.mkv", 97), local_file("extras.nfo", 3)];
        let config = ContentFilterConfig::default();

        assert_eq!(
            ContentFilterDecision::Accepted,
            filter_content(
                subject(
                    &item.0,
                    &at_boundary,
                    ContentMetadata::default(),
                    ContentFilterContext::Search
                ),
                &config,
            )
        );
        assert_eq!(
            ContentFilterDecision::Rejected(ContentFilterReason::NonVideoRatio),
            filter_content(
                subject(
                    &item.0,
                    &above_boundary,
                    ContentMetadata::default(),
                    ContentFilterContext::Search
                ),
                &config,
            )
        );
    }

    #[test]
    fn filters_external_seed_only_when_client_metadata_exists() {
        let item = local_item("Movie", MediaType::Movie);
        let files = vec![local_file("movie.mkv", 10)];
        let no_metadata = filter_content(
            subject(
                &item.0,
                &files,
                ContentMetadata::default(),
                ContentFilterContext::Search,
            ),
            &ContentFilterConfig::default(),
        );
        let category_name = format!("movies.{}", external_seed_marker());
        let category = filter_content(
            subject(
                &item.0,
                &files,
                ContentMetadata {
                    tags: &[],
                    tracker_hosts: &[],
                    category: Some(&category_name),
                    link_category: None,
                },
                ContentFilterContext::Search,
            ),
            &ContentFilterConfig::default(),
        );

        assert_eq!(ContentFilterDecision::Accepted, no_metadata);
        assert_eq!(
            ContentFilterDecision::Rejected(ContentFilterReason::ExistingExternalSeed),
            category
        );
    }

    #[test]
    fn filters_season_specials_and_non_standard_episode_names() {
        let standard = local_item("Show.S01E01", MediaType::Episode);
        let non_standard = local_item("Show Episode One", MediaType::Episode);
        let specials = vec![local_file("Specials/episode.mkv", 10)];

        assert_eq!(
            ContentFilterDecision::Rejected(ContentFilterReason::SeasonSpecial),
            filter_content(
                subject(
                    &standard.0,
                    &specials,
                    ContentMetadata::default(),
                    ContentFilterContext::Announcement
                ),
                &ContentFilterConfig::default(),
            )
        );
        assert_eq!(
            ContentFilterDecision::Rejected(ContentFilterReason::SingleEpisode),
            filter_content(
                subject(
                    &non_standard.0,
                    &[],
                    ContentMetadata::default(),
                    ContentFilterContext::Search
                ),
                &ContentFilterConfig::default(),
            )
        );
        assert_eq!(
            ContentFilterDecision::Rejected(ContentFilterReason::NonStandardNaming),
            filter_content(
                subject(
                    &non_standard.0,
                    &[],
                    ContentMetadata::default(),
                    ContentFilterContext::Search
                ),
                &ContentFilterConfig {
                    include_single_episodes: true,
                    ..ContentFilterConfig::default()
                },
            )
        );
    }

    #[test]
    fn filters_data_dir_single_file_inside_season_pack_folder() {
        let item = local_item("Show.S01E01", MediaType::Episode)
            .with_source(LocalItemSource::DataRoot {
                path: PathBuf::from("/media/Show Season 1/Show.S01E01"),
            })
            .with_path(PathBuf::from("/media/Show Season 1/Show.S01E01"));
        let files = vec![local_file("Show.S01E01.mkv", 10)];

        let decision = filter_content(
            subject(
                &item.0,
                &files,
                ContentMetadata::default(),
                ContentFilterContext::Announcement,
            ),
            &ContentFilterConfig::default(),
        );

        assert_eq!(
            ContentFilterDecision::Rejected(ContentFilterReason::DataDirSeasonPackEpisode),
            decision
        );
    }

    #[test]
    fn filters_arr_style_data_roots_without_release_files() {
        let item = local_item("Series Root", MediaType::Unknown)
            .with_source(LocalItemSource::DataRoot {
                path: PathBuf::from("/media/Series Root"),
            })
            .with_path(PathBuf::from("/media/Series Root"));

        let decision = filter_content(
            subject(
                &item.0,
                &[],
                ContentMetadata::default(),
                ContentFilterContext::Search,
            ),
            &ContentFilterConfig::default(),
        );

        assert_eq!(
            ContentFilterDecision::Rejected(ContentFilterReason::ArrRoot),
            decision
        );
    }

    #[derive(Debug, Clone)]
    struct TestItem(LocalItem);

    impl TestItem {
        fn with_hash(mut self, hash: InfoHash) -> Self {
            self.0.info_hash = Some(hash);
            self
        }

        fn with_path(mut self, path: PathBuf) -> Self {
            self.0.path = Some(path);
            self
        }

        fn with_size(mut self, size: u64) -> Self {
            self.0.total_size = ByteSize::new(size);
            self
        }

        fn with_source(mut self, source: LocalItemSource) -> Self {
            self.0.source = source;
            self
        }
    }

    fn local_item(name: &str, media_type: MediaType) -> TestItem {
        TestItem(LocalItem {
            id: None,
            source: LocalItemSource::Virtual {
                source_key: SourceKey::new(name).unwrap(),
            },
            title: ItemTitle::new(name).unwrap(),
            display_name: DisplayName::new(name).unwrap(),
            media_type,
            info_hash: None,
            path: None,
            save_path: None,
            total_size: ByteSize::new(10),
            mtime_ms: None,
        })
    }

    fn local_file(path: &str, size: u64) -> LocalFile {
        LocalFile::new(
            Some(LocalItemId::new(1).unwrap()),
            PathBuf::from(path),
            ByteSize::new(size),
            FileIndex::new(0),
        )
        .unwrap()
    }

    fn subject<'a>(
        item: &'a LocalItem,
        files: &'a [LocalFile],
        metadata: ContentMetadata<'a>,
        context: ContentFilterContext,
    ) -> ContentFilterSubject<'a> {
        ContentFilterSubject {
            item,
            files,
            metadata,
            context,
        }
    }
}
