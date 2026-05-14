use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::OnceLock;

use regex::Regex;

use crate::content_filter::{BlocklistRule, CandidateBlocklistSubject};
use crate::domain::{
    ByteSize, IndexerId, InfoHash, LocalFile, LocalItem, LocalItemSource, MediaType,
    RemoteCandidate, TorrentFile, TorrentMetafile,
};
use crate::indexers::TorznabCaps;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum FileTreeMatchMode {
    Strict,
    Flexible,
    Partial,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FileTreeMatchConfig {
    pub mode: FileTreeMatchMode,
    pub fuzzy_size_threshold: f64,
    pub season_from_episodes: f64,
}

impl Default for FileTreeMatchConfig {
    fn default() -> Self {
        Self {
            mode: FileTreeMatchMode::Strict,
            fuzzy_size_threshold: 0.02,
            season_from_episodes: 1.0,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum FileTreeDecision {
    Match,
    MatchSizeOnly,
    MatchPartial,
    SizeMismatch,
    PartialSizeMismatch,
    FileTreeMismatch,
}

impl FileTreeDecision {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Match => "MATCH",
            Self::MatchSizeOnly => "MATCH_SIZE_ONLY",
            Self::MatchPartial => "MATCH_PARTIAL",
            Self::SizeMismatch => "SIZE_MISMATCH",
            Self::PartialSizeMismatch => "PARTIAL_SIZE_MISMATCH",
            Self::FileTreeMismatch => "FILE_TREE_MISMATCH",
        }
    }

    pub const fn is_actionable(self) -> bool {
        matches!(self, Self::Match | Self::MatchSizeOnly | Self::MatchPartial)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FileTreeAssessment {
    pub decision: FileTreeDecision,
    pub matched_size: ByteSize,
    pub matched_ratio: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CandidatePrecheckConfig {
    pub fuzzy_size_threshold: f64,
    pub season_from_episodes: f64,
    pub include_single_episodes: bool,
    pub blocklist: Vec<BlocklistRule>,
}

impl Default for CandidatePrecheckConfig {
    fn default() -> Self {
        Self {
            fuzzy_size_threshold: FileTreeMatchConfig::default().fuzzy_size_threshold,
            season_from_episodes: FileTreeMatchConfig::default().season_from_episodes,
            include_single_episodes: false,
            blocklist: Vec::new(),
        }
    }
}

impl From<FileTreeMatchConfig> for CandidatePrecheckConfig {
    fn from(config: FileTreeMatchConfig) -> Self {
        Self {
            fuzzy_size_threshold: config.fuzzy_size_threshold,
            season_from_episodes: config.season_from_episodes,
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CandidatePrecheckInput<'a> {
    pub title: &'a str,
    pub download_url: Option<&'a str>,
    pub tracker: Option<&'a str>,
    pub size: Option<ByteSize>,
    pub info_hash: Option<&'a InfoHash>,
}

impl<'a> CandidatePrecheckInput<'a> {
    pub fn from_remote_candidate(candidate: &'a RemoteCandidate) -> Self {
        Self {
            title: candidate.title.as_str(),
            download_url: Some(candidate.download_url.as_str()),
            tracker: Some(candidate.tracker.as_str()),
            size: candidate.size,
            info_hash: candidate.info_hash.as_ref(),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum CandidatePrecheckDecision {
    Accepted,
    Rejected(CandidatePrecheckReason),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum CandidatePrecheckReason {
    ReleaseGroupMismatch,
    ResolutionMismatch,
    SourceMismatch,
    ProperRepackMismatch,
    FuzzySizeMismatch,
    MissingDownloadLink,
    SameInfoHash,
    InfoHashAlreadyExists,
    BlockedRelease { rule: BlocklistRule },
    SingleEpisodeForSeasonPack,
}

impl CandidatePrecheckReason {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ReleaseGroupMismatch => "RELEASE_GROUP_MISMATCH",
            Self::ResolutionMismatch => "RESOLUTION_MISMATCH",
            Self::SourceMismatch => "SOURCE_MISMATCH",
            Self::ProperRepackMismatch => "PROPER_REPACK_MISMATCH",
            Self::FuzzySizeMismatch => "FUZZY_SIZE_MISMATCH",
            Self::MissingDownloadLink => "MISSING_DOWNLOAD_LINK",
            Self::SameInfoHash => "SAME_INFO_HASH",
            Self::InfoHashAlreadyExists => "INFO_HASH_ALREADY_EXISTS",
            Self::BlockedRelease { .. } => "BLOCKED_RELEASE",
            Self::SingleEpisodeForSeasonPack => "FILE_TREE_MISMATCH",
        }
    }
}

pub fn precheck_candidate(
    local_item: &LocalItem,
    candidate: CandidatePrecheckInput<'_>,
    owned_info_hashes: &[InfoHash],
    config: &CandidatePrecheckConfig,
) -> CandidatePrecheckDecision {
    let local_metadata = ParsedReleaseMetadata::from_title(local_item.display_name.as_str());
    let candidate_metadata = ParsedReleaseMetadata::from_title(candidate.title);

    if comparable_mismatch(
        local_metadata.release_group.as_deref(),
        candidate_metadata.release_group.as_deref(),
    ) {
        return reject(CandidatePrecheckReason::ReleaseGroupMismatch);
    }

    if comparable_mismatch(local_metadata.resolution, candidate_metadata.resolution) {
        return reject(CandidatePrecheckReason::ResolutionMismatch);
    }

    if comparable_mismatch(local_metadata.source, candidate_metadata.source) {
        return reject(CandidatePrecheckReason::SourceMismatch);
    }

    if local_metadata.has_proper_repack != candidate_metadata.has_proper_repack {
        return reject(CandidatePrecheckReason::ProperRepackMismatch);
    }

    if candidate
        .size
        .is_some_and(|size| !candidate_size_in_bounds(local_item, size, config))
    {
        return reject(CandidatePrecheckReason::FuzzySizeMismatch);
    }

    if !candidate
        .download_url
        .is_some_and(|url| !url.trim().is_empty())
    {
        return reject(CandidatePrecheckReason::MissingDownloadLink);
    }

    if local_item
        .info_hash
        .as_ref()
        .is_some_and(|local_hash| candidate.info_hash == Some(local_hash))
    {
        return reject(CandidatePrecheckReason::SameInfoHash);
    }

    if let Some(candidate_hash) = candidate.info_hash
        && owned_info_hashes
            .iter()
            .any(|owned_hash| owned_hash == candidate_hash)
    {
        return reject(CandidatePrecheckReason::InfoHashAlreadyExists);
    }

    let tracker_hosts = candidate.tracker.into_iter().collect::<Vec<_>>();
    if let Some(rule) = config.blocklist.iter().find(|rule| {
        rule.matches_candidate(CandidateBlocklistSubject {
            display_name: candidate.title,
            tracker_hosts: &tracker_hosts,
            info_hash: candidate.info_hash,
            size: candidate.size,
        })
    }) {
        return reject(CandidatePrecheckReason::BlockedRelease { rule: rule.clone() });
    }

    if !config.include_single_episodes
        && local_item.media_type == MediaType::SeasonPack
        && parse_episode_metadata(candidate.title).episode.is_some()
    {
        return reject(CandidatePrecheckReason::SingleEpisodeForSeasonPack);
    }

    CandidatePrecheckDecision::Accepted
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct SearchCadenceConfig {
    pub recent_search_cooldown_ms: Option<u64>,
    pub first_search_window_ms: Option<u64>,
}

impl Default for SearchCadenceConfig {
    fn default() -> Self {
        Self {
            recent_search_cooldown_ms: Some(3 * 24 * 60 * 60 * 1_000),
            first_search_window_ms: Some(7 * 24 * 60 * 60 * 1_000),
        }
    }
}

impl SearchCadenceConfig {
    pub fn from_seconds(
        recent_search_cooldown_secs: Option<u64>,
        first_search_window_secs: Option<u64>,
    ) -> Self {
        Self {
            recent_search_cooldown_ms: recent_search_cooldown_secs
                .map(|seconds| seconds.saturating_mul(1_000)),
            first_search_window_ms: first_search_window_secs
                .map(|seconds| seconds.saturating_mul(1_000)),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct SearchCadenceIndexer<'a> {
    pub indexer_id: IndexerId,
    pub enabled: bool,
    pub caps: &'a TorznabCaps,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct SearchHistoryEntry {
    pub indexer_id: IndexerId,
    pub first_searched_at_ms: i64,
    pub last_searched_at_ms: i64,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum SearchCadenceDecision {
    Searchable(SearchCadenceSearchReason),
    Skipped(SearchCadenceSkipReason),
}

impl SearchCadenceDecision {
    pub const fn is_searchable(self) -> bool {
        matches!(self, Self::Searchable(_))
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum SearchCadenceSearchReason {
    MissingCompatibleIndexerHistory,
    VirtualSourceChanged,
    CadenceDue,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum SearchCadenceSkipReason {
    NoCompatibleIndexers,
    RecentlySearched,
    FirstSearchWindowExpired,
}

#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct SearchIds {
    pub imdb_id: Option<String>,
    pub tvdb_id: Option<String>,
    pub tmdb_id: Option<String>,
}

impl SearchIds {
    pub fn is_empty(&self) -> bool {
        self.imdb_id.is_none() && self.tvdb_id.is_none() && self.tmdb_id.is_none()
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SearchPlanningItem<'a> {
    pub item: &'a LocalItem,
    pub ids: SearchIds,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SearchGroup<'a> {
    pub cache_key: SearchCacheKey,
    pub items: Vec<SearchPlanningItem<'a>>,
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SearchCacheKey {
    value: String,
}

impl SearchCacheKey {
    pub fn as_str(&self) -> &str {
        &self.value
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TorznabSearchPlan {
    pub query: TorznabSearchQuery,
    pub limit: u16,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TorznabSearchQuery {
    pub search_type: TorznabSearchType,
    pub q: Option<String>,
    pub season: Option<u16>,
    pub episode: Option<u16>,
    pub ids: SearchIds,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum TorznabSearchType {
    Search,
    TvSearch,
    MovieSearch,
}

impl TorznabSearchType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Search => "search",
            Self::TvSearch => "tvsearch",
            Self::MovieSearch => "movie",
        }
    }
}

pub fn group_search_items<'a>(items: Vec<SearchPlanningItem<'a>>) -> Vec<SearchGroup<'a>> {
    let mut groups = BTreeMap::<SearchCacheKey, Vec<SearchPlanningItem<'a>>>::new();
    for item in items {
        groups
            .entry(search_cache_key(item.item, &item.ids))
            .or_default()
            .push(item);
    }

    groups
        .into_iter()
        .map(|(cache_key, items)| SearchGroup { cache_key, items })
        .collect()
}

pub fn search_cache_key(item: &LocalItem, ids: &SearchIds) -> SearchCacheKey {
    let title = normalize_search_key(item.title.as_str());
    let metadata = parsed_search_metadata(item);
    let mut value = match (metadata.season, metadata.episode) {
        (Some(season), Some(episode)) => format!("{title}.s{season:02}.e{episode:02}"),
        (Some(season), None) => format!("{title}.s{season:02}"),
        _ => title,
    };
    if let Some(imdb_id) = ids.imdb_id.as_deref() {
        value.push_str("|imdb:");
        value.push_str(&normalize_search_key(imdb_id));
    }
    if let Some(tvdb_id) = ids.tvdb_id.as_deref() {
        value.push_str("|tvdb:");
        value.push_str(&normalize_search_key(tvdb_id));
    }
    if let Some(tmdb_id) = ids.tmdb_id.as_deref() {
        value.push_str("|tmdb:");
        value.push_str(&normalize_search_key(tmdb_id));
    }
    SearchCacheKey { value }
}

pub fn plan_torznab_search(
    item: &LocalItem,
    ids: &SearchIds,
    caps: &TorznabCaps,
) -> Option<TorznabSearchPlan> {
    if !caps.supports_media_type(item.media_type) {
        return None;
    }
    let metadata = parsed_search_metadata(item);
    let query = match item.media_type {
        MediaType::Episode | MediaType::SeasonPack => tv_query(item, ids, caps, metadata),
        MediaType::Movie => movie_query(item, ids, caps),
        MediaType::Anime | MediaType::Video | MediaType::Audio | MediaType::Book => {
            generic_query(item, ids, caps)
        }
        MediaType::Archive | MediaType::Unknown => generic_query(item, ids, caps),
    }?;

    Some(TorznabSearchPlan {
        query,
        limit: caps.limits.max,
    })
}

pub fn evaluate_search_cadence(
    item: &LocalItem,
    indexers: &[SearchCadenceIndexer<'_>],
    history: &[SearchHistoryEntry],
    now_ms: i64,
    config: SearchCadenceConfig,
) -> SearchCadenceDecision {
    let compatible_indexers = indexers
        .iter()
        .filter(|indexer| indexer.enabled && cadence_can_search(item, indexer.caps));

    let mut earliest_first = None;
    let mut earliest_last = None;
    let history_by_indexer = history
        .iter()
        .map(|entry| (entry.indexer_id, entry))
        .collect::<HashMap<_, _>>();

    let mut compatible_count = 0_usize;
    for indexer in compatible_indexers {
        compatible_count += 1;
        let Some(entry) = history_by_indexer.get(&indexer.indexer_id) else {
            return SearchCadenceDecision::Searchable(
                SearchCadenceSearchReason::MissingCompatibleIndexerHistory,
            );
        };
        earliest_first = Some(min_timestamp(earliest_first, entry.first_searched_at_ms));
        earliest_last = Some(min_timestamp(earliest_last, entry.last_searched_at_ms));
    }

    if compatible_count == 0 {
        return SearchCadenceDecision::Skipped(SearchCadenceSkipReason::NoCompatibleIndexers);
    }

    let Some(earliest_first) = earliest_first else {
        return SearchCadenceDecision::Searchable(
            SearchCadenceSearchReason::MissingCompatibleIndexerHistory,
        );
    };
    let Some(earliest_last) = earliest_last else {
        return SearchCadenceDecision::Searchable(
            SearchCadenceSearchReason::MissingCompatibleIndexerHistory,
        );
    };

    if matches!(item.source, LocalItemSource::Virtual { .. })
        && item
            .mtime_ms
            .is_some_and(|newest_source_mtime| newest_source_mtime > earliest_last)
    {
        return SearchCadenceDecision::Searchable(SearchCadenceSearchReason::VirtualSourceChanged);
    }

    if let Some(cooldown_ms) = config.recent_search_cooldown_ms {
        let cutoff = timestamp_cutoff(now_ms, cooldown_ms);
        if earliest_last > cutoff {
            return SearchCadenceDecision::Skipped(SearchCadenceSkipReason::RecentlySearched);
        }
    }

    if let Some(window_ms) = config.first_search_window_ms {
        let cutoff = timestamp_cutoff(now_ms, window_ms);
        if earliest_first < cutoff {
            return SearchCadenceDecision::Skipped(
                SearchCadenceSkipReason::FirstSearchWindowExpired,
            );
        }
    }

    SearchCadenceDecision::Searchable(SearchCadenceSearchReason::CadenceDue)
}

fn cadence_can_search(item: &LocalItem, caps: &TorznabCaps) -> bool {
    caps.supports_media_type(item.media_type)
        && plan_torznab_search(item, &SearchIds::default(), caps).is_some()
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
struct ParsedSearchMetadata {
    season: Option<u16>,
    episode: Option<u16>,
}

fn tv_query(
    item: &LocalItem,
    ids: &SearchIds,
    caps: &TorznabCaps,
    metadata: ParsedSearchMetadata,
) -> Option<TorznabSearchQuery> {
    if caps.search.tv_search {
        let ids = supported_ids(ids, caps, &["tvdbid", "imdbid"]);
        return Some(TorznabSearchQuery {
            search_type: TorznabSearchType::TvSearch,
            q: if ids.is_empty() {
                Some(item.title.as_str().to_owned())
            } else {
                None
            },
            season: metadata.season,
            episode: metadata.episode,
            ids,
        });
    }
    generic_query(item, ids, caps)
}

fn movie_query(
    item: &LocalItem,
    ids: &SearchIds,
    caps: &TorznabCaps,
) -> Option<TorznabSearchQuery> {
    if caps.search.movie_search {
        let ids = supported_ids(ids, caps, &["imdbid", "tmdbid"]);
        return Some(TorznabSearchQuery {
            search_type: TorznabSearchType::MovieSearch,
            q: if ids.is_empty() {
                Some(item.title.as_str().to_owned())
            } else {
                None
            },
            season: None,
            episode: None,
            ids,
        });
    }
    generic_query(item, ids, caps)
}

fn generic_query(
    item: &LocalItem,
    ids: &SearchIds,
    caps: &TorznabCaps,
) -> Option<TorznabSearchQuery> {
    if !caps.search.generic_search {
        return None;
    }
    Some(TorznabSearchQuery {
        search_type: TorznabSearchType::Search,
        q: Some(item.title.as_str().to_owned()),
        season: None,
        episode: None,
        ids: supported_ids(ids, caps, &["imdbid", "tvdbid", "tmdbid"]),
    })
}

fn supported_ids(ids: &SearchIds, caps: &TorznabCaps, priority: &[&str]) -> SearchIds {
    let mut supported = SearchIds::default();
    for key in priority {
        if !caps.search.supported_id_params.contains(*key) {
            continue;
        }
        match *key {
            "imdbid" => supported.imdb_id = ids.imdb_id.clone(),
            "tvdbid" => supported.tvdb_id = ids.tvdb_id.clone(),
            "tmdbid" => supported.tmdb_id = ids.tmdb_id.clone(),
            _ => {}
        }
        if !supported.is_empty() {
            return supported;
        }
    }
    supported
}

fn parsed_search_metadata(item: &LocalItem) -> ParsedSearchMetadata {
    let title = item.title.as_str();
    match item.media_type {
        MediaType::Episode => parse_episode_metadata(title),
        MediaType::SeasonPack => parse_season_metadata(title),
        _ => ParsedSearchMetadata::default(),
    }
}

fn parse_episode_metadata(title: &str) -> ParsedSearchMetadata {
    let lower = title.to_ascii_lowercase();
    let bytes = lower.as_bytes();
    for index in 0..bytes.len() {
        if bytes.get(index) != Some(&b's') {
            continue;
        }
        let season_start = index + 1;
        let Some((season, after_season)) = parse_digits(&lower, season_start, 2) else {
            continue;
        };
        if bytes.get(after_season) != Some(&b'e') {
            continue;
        }
        let Some((episode, _after_episode)) = parse_digits(&lower, after_season + 1, 3) else {
            continue;
        };
        return ParsedSearchMetadata {
            season: Some(season),
            episode: Some(episode),
        };
    }
    ParsedSearchMetadata::default()
}

fn parse_season_metadata(title: &str) -> ParsedSearchMetadata {
    let lower = title.to_ascii_lowercase();
    let bytes = lower.as_bytes();
    for index in 0..bytes.len() {
        if bytes.get(index) != Some(&b's') {
            continue;
        }
        let Some((season, _after_season)) = parse_digits(&lower, index + 1, 2) else {
            continue;
        };
        return ParsedSearchMetadata {
            season: Some(season),
            episode: None,
        };
    }
    ParsedSearchMetadata::default()
}

fn parse_digits(value: &str, start: usize, max_len: usize) -> Option<(u16, usize)> {
    let mut end = start;
    for byte in value.as_bytes().iter().skip(start).take(max_len) {
        if !byte.is_ascii_digit() {
            break;
        }
        end += 1;
    }
    if end == start {
        return None;
    }
    value
        .get(start..end)?
        .parse()
        .ok()
        .map(|number| (number, end))
}

#[derive(Debug, Clone, Default, Eq, PartialEq)]
struct ParsedReleaseMetadata {
    release_group: Option<String>,
    resolution: Option<ReleaseResolution>,
    source: Option<ReleaseSource>,
    has_proper_repack: bool,
}

impl ParsedReleaseMetadata {
    fn from_title(title: &str) -> Self {
        Self {
            release_group: parse_release_group(title),
            resolution: parse_release_resolution(title),
            source: parse_release_source(title),
            has_proper_repack: has_proper_repack(title),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum ReleaseResolution {
    P720,
    P1080,
    P2160,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum ReleaseSource {
    WebDl,
    WebRip,
    Bluray,
    Hdtv,
    Dvd,
    Remux,
}

fn comparable_mismatch<T: PartialEq>(left: Option<T>, right: Option<T>) -> bool {
    matches!((left, right), (Some(left), Some(right)) if left != right)
}

fn candidate_size_in_bounds(
    local_item: &LocalItem,
    candidate_size: ByteSize,
    config: &CandidatePrecheckConfig,
) -> bool {
    let length = local_item.total_size.get() as f64;
    let factor = fuzzy_size_factor(local_item, config);
    let lower = length - factor * length;
    let upper = length + factor * length;
    let candidate_size = candidate_size.get() as f64;
    candidate_size >= lower && candidate_size <= upper
}

fn fuzzy_size_factor(local_item: &LocalItem, config: &CandidatePrecheckConfig) -> f64 {
    if matches!(local_item.source, LocalItemSource::Virtual { .. }) {
        1.0 - config.season_from_episodes
    } else {
        config.fuzzy_size_threshold
    }
    .clamp(0.0, 1.0)
}

fn reject(reason: CandidatePrecheckReason) -> CandidatePrecheckDecision {
    CandidatePrecheckDecision::Rejected(reason)
}

fn parse_release_group(title: &str) -> Option<String> {
    bracketed_group_regex()
        .captures(title)
        .or_else(|| leading_group_regex().captures(title))
        .or_else(|| trailing_group_regex().captures(title))
        .and_then(|captures| captures.name("group"))
        .map(|group| group.as_str().trim())
        .filter(|group| !is_bad_release_group(group))
        .map(|group| group.to_ascii_lowercase())
}

fn parse_release_resolution(title: &str) -> Option<ReleaseResolution> {
    release_resolution_regex()
        .captures(title)
        .and_then(|captures| captures.name("resolution"))
        .and_then(
            |resolution| match resolution.as_str().to_ascii_lowercase().as_str() {
                "720p" => Some(ReleaseResolution::P720),
                "1080p" => Some(ReleaseResolution::P1080),
                "2160p" => Some(ReleaseResolution::P2160),
                _ => None,
            },
        )
}

fn parse_release_source(title: &str) -> Option<ReleaseSource> {
    release_source_regex()
        .captures(title)
        .and_then(|captures| captures.name("source"))
        .map(|source| normalize_release_token(source.as_str()))
        .and_then(|source| match source.as_str() {
            "webdl" => Some(ReleaseSource::WebDl),
            "webrip" => Some(ReleaseSource::WebRip),
            "bluray" | "bdrip" | "brrip" => Some(ReleaseSource::Bluray),
            "hdtv" => Some(ReleaseSource::Hdtv),
            "dvdrip" => Some(ReleaseSource::Dvd),
            "remux" => Some(ReleaseSource::Remux),
            _ => None,
        })
}

fn has_proper_repack(title: &str) -> bool {
    proper_repack_regex().is_match(title)
}

fn normalize_release_token(value: &str) -> String {
    value
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

fn is_bad_release_group(group: &str) -> bool {
    matches!(
        normalize_release_token(group).as_str(),
        "x264"
            | "x265"
            | "h264"
            | "h265"
            | "hevc"
            | "av1"
            | "aac"
            | "dts"
            | "truehd"
            | "720p"
            | "1080p"
            | "2160p"
            | "bluray"
            | "webdl"
            | "webrip"
            | "dl"
            | "rip"
            | "ray"
    )
}

fn bracketed_group_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(r"^\[(?P<group>[^\]]{2,32})\]\s*").expect("group regex should compile")
    })
}

fn leading_group_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(r"^(?P<group>[A-Za-z0-9][A-Za-z0-9._-]{1,31})\s+-\s+")
            .expect("leading group regex should compile")
    })
}

fn trailing_group_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(r"-(?P<group>[A-Za-z0-9][A-Za-z0-9._]{1,31})$")
            .expect("trailing group regex should compile")
    })
}

fn release_resolution_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(r"(?i)(?:^|[ ._\-\[\]()])(?P<resolution>2160p|1080p|720p)(?:$|[ ._\-\[\]()])")
            .expect("resolution regex should compile")
    })
}

fn release_source_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(
            r"(?i)(?:^|[ ._\-\[\]()])(?P<source>web[ ._-]?dl|web[ ._-]?rip|blu[ ._-]?ray|bdrip|brrip|hdtv|dvd[ ._-]?rip|remux)(?:$|[ ._\-\[\]()])",
        )
        .expect("source regex should compile")
    })
}

fn proper_repack_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(r"(?i)(?:^|[ ._\-\[\]()])(?:proper|repack)(?:$|[ ._\-\[\]()])")
            .expect("proper/repack regex should compile")
    })
}

fn normalize_search_key(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                '.'
            }
        })
        .fold(String::new(), |mut normalized, character| {
            if character == '.' && normalized.ends_with('.') {
                return normalized;
            }
            normalized.push(character);
            normalized
        })
        .trim_matches('.')
        .to_owned()
}

fn min_timestamp(current: Option<i64>, candidate: i64) -> i64 {
    current.map_or(candidate, |current| current.min(candidate))
}

fn timestamp_cutoff(now_ms: i64, duration_ms: u64) -> i64 {
    now_ms.saturating_sub(i64::try_from(duration_ms).unwrap_or(i64::MAX))
}

pub fn assess_file_tree(
    local_item: &LocalItem,
    local_files: &[LocalFile],
    candidate: &TorrentMetafile,
    config: FileTreeMatchConfig,
) -> FileTreeAssessment {
    let virtual_item = matches!(local_item.source, LocalItemSource::Virtual { .. });
    let exact = exact_tree_matches(local_files, &candidate.files, virtual_item);
    if exact {
        return assessment(
            FileTreeDecision::Match,
            candidate.total_size,
            full_ratio(candidate.total_size),
        );
    }

    let size_pairing = pair_by_size_prefer_name(local_files, &candidate.files);
    let size_only = size_pairing.matched_files == candidate.files.len();
    match config.mode {
        FileTreeMatchMode::Strict => {
            if size_only {
                assessment(
                    FileTreeDecision::FileTreeMismatch,
                    size_pairing.matched_size,
                    ratio(size_pairing.matched_size, candidate.total_size),
                )
            } else {
                assessment(
                    FileTreeDecision::SizeMismatch,
                    size_pairing.matched_size,
                    ratio(size_pairing.matched_size, candidate.total_size),
                )
            }
        }
        FileTreeMatchMode::Flexible => {
            if size_only {
                assessment(
                    FileTreeDecision::MatchSizeOnly,
                    size_pairing.matched_size,
                    full_ratio(candidate.total_size),
                )
            } else {
                assessment(
                    FileTreeDecision::SizeMismatch,
                    size_pairing.matched_size,
                    ratio(size_pairing.matched_size, candidate.total_size),
                )
            }
        }
        FileTreeMatchMode::Partial => partial_assessment(
            local_item,
            local_files,
            candidate,
            config,
            size_only,
            size_pairing,
        ),
    }
}

fn partial_assessment(
    local_item: &LocalItem,
    local_files: &[LocalFile],
    candidate: &TorrentMetafile,
    config: FileTreeMatchConfig,
    size_only: bool,
    size_pairing: SizePairing,
) -> FileTreeAssessment {
    if size_only {
        return assessment(
            FileTreeDecision::MatchSizeOnly,
            size_pairing.matched_size,
            full_ratio(candidate.total_size),
        );
    }

    let min_ratio = min_size_ratio(local_item, config);
    let size_gate = partial_size_gate(local_files, &candidate.files);
    let size_gate_ratio = ratio(size_gate, candidate.total_size);
    if size_gate_ratio < min_ratio {
        return assessment(
            FileTreeDecision::PartialSizeMismatch,
            size_gate,
            size_gate_ratio,
        );
    }

    let piece_ratio = piece_ratio(size_pairing.matched_size, candidate);
    if piece_ratio >= min_ratio {
        assessment(
            FileTreeDecision::MatchPartial,
            size_pairing.matched_size,
            piece_ratio,
        )
    } else {
        assessment(
            FileTreeDecision::FileTreeMismatch,
            size_pairing.matched_size,
            piece_ratio,
        )
    }
}

fn exact_tree_matches(
    local_files: &[LocalFile],
    candidate_files: &[TorrentFile],
    virtual_item: bool,
) -> bool {
    if virtual_item {
        let mut local =
            local_files
                .iter()
                .fold(HashMap::<(&str, u64), usize>::new(), |mut counts, file| {
                    *counts
                        .entry((file.file_name.as_str(), file.size.get()))
                        .or_default() += 1;
                    counts
                });
        candidate_files
            .iter()
            .all(|file| decrement_count(&mut local, (file.file_name.as_str(), file.size.get())))
    } else {
        let mut local =
            local_files
                .iter()
                .fold(HashMap::<(&Path, u64), usize>::new(), |mut counts, file| {
                    *counts
                        .entry((file.relative_path.as_path(), file.size.get()))
                        .or_default() += 1;
                    counts
                });
        candidate_files.iter().all(|file| {
            decrement_count(&mut local, (file.relative_path.as_path(), file.size.get()))
        })
    }
}

fn decrement_count<K: Eq + std::hash::Hash>(counts: &mut HashMap<K, usize>, key: K) -> bool {
    let Some(count) = counts.get_mut(&key) else {
        return false;
    };
    *count -= 1;
    if *count == 0 {
        counts.remove(&key);
    }
    true
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct SizePairing {
    matched_files: usize,
    matched_size: ByteSize,
}

fn pair_by_size_prefer_name(
    local_files: &[LocalFile],
    candidate_files: &[TorrentFile],
) -> SizePairing {
    let mut used = vec![false; local_files.len()];
    let mut matched_files = 0;
    let mut matched_size = 0;
    let mut candidates = candidate_files.iter().collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        left.relative_path
            .cmp(&right.relative_path)
            .then_with(|| left.file_index.get().cmp(&right.file_index.get()))
    });

    for candidate in candidates {
        let selected = local_files
            .iter()
            .enumerate()
            .filter(|(index, local)| {
                used.get(*index).is_some_and(|is_used| !*is_used) && local.size == candidate.size
            })
            .min_by(|(_, left), (_, right)| {
                same_name_rank(left.file_name.as_str(), candidate.file_name.as_str())
                    .cmp(&same_name_rank(
                        right.file_name.as_str(),
                        candidate.file_name.as_str(),
                    ))
                    .then_with(|| left.relative_path.cmp(&right.relative_path))
                    .then_with(|| left.file_index.get().cmp(&right.file_index.get()))
            })
            .map(|(index, _)| index);

        if let Some(slot) = selected.and_then(|index| used.get_mut(index)) {
            *slot = true;
            matched_files += 1;
            matched_size += candidate.size.get();
        }
    }

    SizePairing {
        matched_files,
        matched_size: ByteSize::new(matched_size),
    }
}

fn same_name_rank(left: &str, right: &str) -> u8 {
    u8::from(left != right)
}

fn partial_size_gate(local_files: &[LocalFile], candidate_files: &[TorrentFile]) -> ByteSize {
    let local_sizes = local_files
        .iter()
        .map(|file| file.size.get())
        .collect::<HashSet<_>>();
    ByteSize::new(
        candidate_files
            .iter()
            .filter(|file| local_sizes.contains(&file.size.get()))
            .map(|file| file.size.get())
            .sum(),
    )
}

fn piece_ratio(matched_size: ByteSize, candidate: &TorrentMetafile) -> f64 {
    let piece_length = candidate
        .piece_length
        .unwrap_or(candidate.total_size)
        .get()
        .max(1);
    let total_pieces = candidate.total_size.get().div_ceil(piece_length);
    if total_pieces == 0 {
        return 1.0;
    }
    let available_pieces = matched_size.get() / piece_length;
    available_pieces as f64 / total_pieces as f64
}

fn min_size_ratio(local_item: &LocalItem, config: FileTreeMatchConfig) -> f64 {
    if matches!(local_item.source, LocalItemSource::Virtual { .. }) {
        config.season_from_episodes
    } else {
        1.0 - config.fuzzy_size_threshold
    }
    .clamp(0.0, 1.0)
}

fn ratio(size: ByteSize, total: ByteSize) -> f64 {
    if total.get() == 0 {
        full_ratio(total)
    } else {
        size.get() as f64 / total.get() as f64
    }
}

fn full_ratio(total: ByteSize) -> f64 {
    if total.get() == 0 { 0.0 } else { 1.0 }
}

fn assessment(
    decision: FileTreeDecision,
    matched_size: ByteSize,
    matched_ratio: f64,
) -> FileTreeAssessment {
    FileTreeAssessment {
        decision,
        matched_size,
        matched_ratio,
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::domain::{
        DisplayName, FileIndex, InfoHash, ItemTitle, LocalItem, MediaType, SourceKey,
    };
    use crate::indexers::{CategoryCaps, SearchCaps, TorznabLimits};

    #[test]
    fn query_grouping_deduplicates_titles_and_keeps_distinct_ids() {
        let movie = search_item("Example Movie 2026", MediaType::Movie);
        let same_movie = search_item("Example.Movie.2026", MediaType::Movie);
        let different_id_movie = search_item("Example Movie 2026", MediaType::Movie);

        let groups = group_search_items(vec![
            SearchPlanningItem {
                item: &movie,
                ids: SearchIds {
                    imdb_id: Some("tt123".to_owned()),
                    ..SearchIds::default()
                },
            },
            SearchPlanningItem {
                item: &same_movie,
                ids: SearchIds {
                    imdb_id: Some("tt123".to_owned()),
                    ..SearchIds::default()
                },
            },
            SearchPlanningItem {
                item: &different_id_movie,
                ids: SearchIds {
                    imdb_id: Some("tt999".to_owned()),
                    ..SearchIds::default()
                },
            },
        ]);

        assert_eq!(2, groups.len());
        assert_eq!(2, groups[0].items.len());
        assert_ne!(groups[0].cache_key, groups[1].cache_key);
        assert!(groups[0].cache_key.as_str().contains("imdb:tt123"));
        assert!(groups[1].cache_key.as_str().contains("imdb:tt999"));
    }

    #[test]
    fn query_planning_covers_common_media_types_and_ids() {
        let caps = all_caps();
        let episode = search_item("My Show S01E02", MediaType::Episode);
        let pack = search_item("My Show S01", MediaType::SeasonPack);
        let movie = search_item("Example Movie 2026", MediaType::Movie);
        let anime = search_item("Anime Show 03", MediaType::Anime);
        let book = search_item("Great Book", MediaType::Book);
        let video = search_item("Generic Video", MediaType::Video);

        let episode_plan = plan_torznab_search(
            &episode,
            &SearchIds {
                tvdb_id: Some("777".to_owned()),
                ..SearchIds::default()
            },
            &caps,
        )
        .unwrap();
        let pack_plan = plan_torznab_search(&pack, &SearchIds::default(), &caps).unwrap();
        let movie_plan = plan_torznab_search(
            &movie,
            &SearchIds {
                imdb_id: Some("tt123".to_owned()),
                ..SearchIds::default()
            },
            &caps,
        )
        .unwrap();
        let anime_plan = plan_torznab_search(&anime, &SearchIds::default(), &caps).unwrap();
        let book_plan = plan_torznab_search(&book, &SearchIds::default(), &caps).unwrap();
        let video_plan = plan_torznab_search(&video, &SearchIds::default(), &caps).unwrap();

        assert_eq!(TorznabSearchType::TvSearch, episode_plan.query.search_type);
        assert_eq!("tvsearch", episode_plan.query.search_type.as_str());
        assert_eq!(Some(1), episode_plan.query.season);
        assert_eq!(Some(2), episode_plan.query.episode);
        assert_eq!(Some("777"), episode_plan.query.ids.tvdb_id.as_deref());
        assert_eq!(Some(1), pack_plan.query.season);
        assert_eq!(TorznabSearchType::MovieSearch, movie_plan.query.search_type);
        assert_eq!(Some("tt123"), movie_plan.query.ids.imdb_id.as_deref());
        assert_eq!(TorznabSearchType::Search, anime_plan.query.search_type);
        assert_eq!(Some("Anime Show 03"), anime_plan.query.q.as_deref());
        assert_eq!(TorznabSearchType::Search, book_plan.query.search_type);
        assert_eq!(TorznabSearchType::Search, video_plan.query.search_type);
        assert_eq!(200, episode_plan.limit);
    }

    #[test]
    fn query_planning_respects_media_support_and_id_fallback() {
        let movie = search_item("Example Movie 2026", MediaType::Movie);
        let episode = search_item("My Show S01E02", MediaType::Episode);
        let movie_only_caps = TorznabCaps {
            search: SearchCaps {
                generic_search: true,
                movie_search: true,
                supported_id_params: ["imdbid".to_owned()].into_iter().collect(),
                ..SearchCaps::default()
            },
            categories: CategoryCaps {
                movie: true,
                ..CategoryCaps::default()
            },
            limits: TorznabLimits {
                default: 50,
                max: 50,
            },
        };
        let generic_caps = TorznabCaps {
            search: SearchCaps {
                generic_search: true,
                supported_id_params: ["imdbid".to_owned()].into_iter().collect(),
                ..SearchCaps::default()
            },
            categories: CategoryCaps {
                tv: true,
                movie: true,
                ..CategoryCaps::default()
            },
            limits: TorznabLimits::default(),
        };

        assert!(plan_torznab_search(&episode, &SearchIds::default(), &movie_only_caps).is_none());
        let fallback = plan_torznab_search(
            &movie,
            &SearchIds {
                imdb_id: Some("tt123".to_owned()),
                tmdb_id: Some("999".to_owned()),
                ..SearchIds::default()
            },
            &generic_caps,
        )
        .unwrap();

        assert_eq!(TorznabSearchType::Search, fallback.query.search_type);
        assert_eq!(Some("Example Movie 2026"), fallback.query.q.as_deref());
        assert_eq!(Some("tt123"), fallback.query.ids.imdb_id.as_deref());
        assert_eq!(None, fallback.query.ids.tmdb_id);
    }

    #[test]
    fn cadence_filters_use_earliest_compatible_history() {
        let item = search_item("Example Movie", MediaType::Movie);
        let caps = all_caps();
        let unsupported = no_movie_caps();
        let indexers = vec![
            cadence_indexer(1, true, &caps),
            cadence_indexer(2, true, &caps),
            cadence_indexer(3, false, &caps),
            cadence_indexer(4, true, &unsupported),
        ];

        let due = evaluate_search_cadence(
            &item,
            &indexers,
            &[
                history(1, 900, 900),
                history(2, 900, 700),
                history(3, 1, 1),
                history(4, 1, 1),
            ],
            1_000,
            SearchCadenceConfig {
                recent_search_cooldown_ms: Some(200),
                first_search_window_ms: None,
            },
        );
        let recent = evaluate_search_cadence(
            &item,
            &indexers,
            &[history(1, 900, 900), history(2, 900, 850)],
            1_000,
            SearchCadenceConfig {
                recent_search_cooldown_ms: Some(200),
                first_search_window_ms: None,
            },
        );
        let old = evaluate_search_cadence(
            &item,
            &indexers[..2],
            &[history(1, 600, 600), history(2, 100, 600)],
            1_000,
            SearchCadenceConfig {
                recent_search_cooldown_ms: None,
                first_search_window_ms: Some(500),
            },
        );

        assert_eq!(
            SearchCadenceDecision::Searchable(SearchCadenceSearchReason::CadenceDue),
            due
        );
        assert_eq!(
            SearchCadenceDecision::Skipped(SearchCadenceSkipReason::RecentlySearched),
            recent
        );
        assert_eq!(
            SearchCadenceDecision::Skipped(SearchCadenceSkipReason::FirstSearchWindowExpired),
            old
        );
    }

    #[test]
    fn cadence_new_indexer_and_virtual_changes_make_items_searchable() {
        let item = search_item("Example Movie", MediaType::Movie);
        let virtual_item = virtual_item();
        let caps = all_caps();
        let indexers = vec![
            cadence_indexer(1, true, &caps),
            cadence_indexer(2, true, &caps),
        ];
        let config = SearchCadenceConfig {
            recent_search_cooldown_ms: Some(1_000),
            first_search_window_ms: Some(500),
        };

        let missing_history =
            evaluate_search_cadence(&item, &indexers, &[history(1, 1, 1)], 1_000, config);
        let changed_virtual = evaluate_search_cadence(
            &virtual_item,
            &indexers,
            &[history(1, 900, 900), history(2, 900, 850)],
            1_000,
            config,
        );

        assert_eq!(
            SearchCadenceDecision::Searchable(
                SearchCadenceSearchReason::MissingCompatibleIndexerHistory
            ),
            missing_history
        );
        assert_eq!(
            SearchCadenceDecision::Searchable(SearchCadenceSearchReason::VirtualSourceChanged),
            changed_virtual
        );
    }

    #[test]
    fn cadence_skips_without_enabled_compatible_indexers() {
        let item = search_item("Example Movie", MediaType::Movie);
        let caps = no_movie_caps();
        let indexers = vec![cadence_indexer(1, true, &caps)];

        let decision =
            evaluate_search_cadence(&item, &indexers, &[], 1_000, SearchCadenceConfig::default());

        assert_eq!(
            SearchCadenceDecision::Skipped(SearchCadenceSkipReason::NoCompatibleIndexers),
            decision
        );
    }

    #[test]
    fn cadence_skips_indexers_without_searchable_caps() {
        let item = search_item("Example Movie", MediaType::Movie);
        let caps = TorznabCaps {
            search: SearchCaps::default(),
            categories: CategoryCaps {
                movie: true,
                ..CategoryCaps::default()
            },
            limits: TorznabLimits::default(),
        };
        let indexers = vec![cadence_indexer(1, true, &caps)];

        let decision =
            evaluate_search_cadence(&item, &indexers, &[], 1_000, SearchCadenceConfig::default());

        assert_eq!(
            SearchCadenceDecision::Skipped(SearchCadenceSkipReason::NoCompatibleIndexers),
            decision
        );
    }

    #[test]
    fn candidate_precheck_runs_metadata_gates_in_documented_order() {
        let mut local = search_item("[GRP] Show.S01E01.1080p.WEB-DL", MediaType::Episode);
        local.total_size = ByteSize::new(100);
        let candidate = CandidatePrecheckInput {
            title: "Show.S01E01.720p.HDTV-OTHER",
            size: Some(ByteSize::new(10_000)),
            download_url: None,
            ..candidate_input("Show.S01E01.720p.HDTV-OTHER")
        };

        let decision =
            precheck_candidate(&local, candidate, &[], &CandidatePrecheckConfig::default());

        assert_eq!(
            CandidatePrecheckDecision::Rejected(CandidatePrecheckReason::ReleaseGroupMismatch),
            decision
        );
        assert_eq!("RELEASE_GROUP_MISMATCH", rejected_reason(decision).as_str());
    }

    #[test]
    fn candidate_precheck_rejects_resolution_source_and_proper_mismatches() {
        let local = search_item("Show.S01E01.1080p.WEB-DL-GRP", MediaType::Episode);

        assert_eq!(
            CandidatePrecheckReason::ResolutionMismatch,
            rejected_reason(precheck_candidate(
                &local,
                candidate_input("Show.S01E01.720p.WEB-DL-GRP"),
                &[],
                &CandidatePrecheckConfig::default(),
            ))
        );
        assert_eq!(
            CandidatePrecheckReason::SourceMismatch,
            rejected_reason(precheck_candidate(
                &local,
                candidate_input("Show.S01E01.1080p.HDTV-GRP"),
                &[],
                &CandidatePrecheckConfig::default(),
            ))
        );
        assert_eq!(
            CandidatePrecheckReason::ProperRepackMismatch,
            rejected_reason(precheck_candidate(
                &local,
                candidate_input("Show.S01E01.1080p.WEB-DL.REPACK-GRP"),
                &[],
                &CandidatePrecheckConfig::default(),
            ))
        );
    }

    #[test]
    fn candidate_precheck_applies_fuzzy_size_boundaries() {
        let mut local = search_item("Movie.2026.1080p.WEB-DL-GRP", MediaType::Movie);
        local.total_size = ByteSize::new(100);
        let config = CandidatePrecheckConfig {
            fuzzy_size_threshold: 0.2,
            ..CandidatePrecheckConfig::default()
        };

        for accepted_size in [80, 100, 120] {
            assert_eq!(
                CandidatePrecheckDecision::Accepted,
                precheck_candidate(
                    &local,
                    CandidatePrecheckInput {
                        size: Some(ByteSize::new(accepted_size)),
                        ..candidate_input("Movie.2026.1080p.WEB-DL-GRP")
                    },
                    &[],
                    &config,
                )
            );
        }

        for rejected_size in [79, 121] {
            assert_eq!(
                CandidatePrecheckReason::FuzzySizeMismatch,
                rejected_reason(precheck_candidate(
                    &local,
                    CandidatePrecheckInput {
                        size: Some(ByteSize::new(rejected_size)),
                        ..candidate_input("Movie.2026.1080p.WEB-DL-GRP")
                    },
                    &[],
                    &config,
                ))
            );
        }

        let mut virtual_pack = virtual_item();
        virtual_pack.total_size = ByteSize::new(100);
        let virtual_config = CandidatePrecheckConfig {
            season_from_episodes: 0.75,
            ..CandidatePrecheckConfig::default()
        };

        assert_eq!(
            CandidatePrecheckDecision::Accepted,
            precheck_candidate(
                &virtual_pack,
                CandidatePrecheckInput {
                    size: Some(ByteSize::new(75)),
                    ..candidate_input("Show.S01.1080p.WEB-DL-GRP")
                },
                &[],
                &virtual_config,
            )
        );
        assert_eq!(
            CandidatePrecheckReason::FuzzySizeMismatch,
            rejected_reason(precheck_candidate(
                &virtual_pack,
                CandidatePrecheckInput {
                    size: Some(ByteSize::new(74)),
                    ..candidate_input("Show.S01.1080p.WEB-DL-GRP")
                },
                &[],
                &virtual_config,
            ))
        );
    }

    #[test]
    fn candidate_precheck_rejects_missing_download_links() {
        let local = search_item("Movie.2026.1080p.WEB-DL-GRP", MediaType::Movie);

        for download_url in [None, Some("   ")] {
            assert_eq!(
                CandidatePrecheckReason::MissingDownloadLink,
                rejected_reason(precheck_candidate(
                    &local,
                    CandidatePrecheckInput {
                        download_url,
                        ..candidate_input("Movie.2026.1080p.WEB-DL-GRP")
                    },
                    &[],
                    &CandidatePrecheckConfig::default(),
                ))
            );
        }
    }

    #[test]
    fn candidate_precheck_rejects_same_and_owned_info_hashes() {
        let same_hash = InfoHash::new("0123456789abcdef0123456789abcdef01234567").unwrap();
        let owned_hash = InfoHash::new("ffffffffffffffffffffffffffffffffffffffff").unwrap();
        let mut local = search_item("Movie.2026.1080p.WEB-DL-GRP", MediaType::Movie);
        local.info_hash = Some(same_hash.clone());

        assert_eq!(
            CandidatePrecheckReason::SameInfoHash,
            rejected_reason(precheck_candidate(
                &local,
                CandidatePrecheckInput {
                    info_hash: Some(&same_hash),
                    ..candidate_input("Movie.2026.1080p.WEB-DL-GRP")
                },
                &[owned_hash.clone()],
                &CandidatePrecheckConfig::default(),
            ))
        );
        assert_eq!(
            CandidatePrecheckReason::InfoHashAlreadyExists,
            rejected_reason(precheck_candidate(
                &local,
                CandidatePrecheckInput {
                    info_hash: Some(&owned_hash),
                    ..candidate_input("Movie.2026.1080p.WEB-DL-GRP")
                },
                &[owned_hash.clone()],
                &CandidatePrecheckConfig::default(),
            ))
        );
    }

    #[test]
    fn candidate_precheck_rejects_blocklisted_candidates() {
        let local = search_item("Movie.2026.1080p.WEB-DL-GRP", MediaType::Movie);
        let rule = BlocklistRule::TrackerHost("tracker.example".to_owned());
        let config = CandidatePrecheckConfig {
            blocklist: vec![rule.clone()],
            ..CandidatePrecheckConfig::default()
        };

        assert_eq!(
            CandidatePrecheckReason::BlockedRelease { rule },
            rejected_reason(precheck_candidate(
                &local,
                candidate_input("Movie.2026.1080p.WEB-DL-GRP"),
                &[],
                &config,
            ))
        );
    }

    #[test]
    fn candidate_precheck_rejects_single_episodes_for_season_pack() {
        let local = search_item("Show.S01", MediaType::SeasonPack);
        let candidate = candidate_input("Show.S01E01.1080p.WEB-DL-GRP");

        assert_eq!(
            CandidatePrecheckReason::SingleEpisodeForSeasonPack,
            rejected_reason(precheck_candidate(
                &local,
                candidate,
                &[],
                &CandidatePrecheckConfig::default(),
            ))
        );
        assert_eq!(
            CandidatePrecheckDecision::Accepted,
            precheck_candidate(
                &local,
                candidate,
                &[],
                &CandidatePrecheckConfig {
                    include_single_episodes: true,
                    ..CandidatePrecheckConfig::default()
                },
            )
        );
    }

    #[test]
    fn exact_match_requires_paths_and_sizes_for_real_items() {
        let local_item = data_root_item();
        let local_files = vec![local_file("Example/a.mkv", 10, 0)];
        let candidate = torrent(
            vec![torrent_file("Example/a.mkv", 10, 0)],
            Some(ByteSize::new(4)),
        );

        let result = assess_file_tree(
            &local_item,
            &local_files,
            &candidate,
            FileTreeMatchConfig::default(),
        );

        assert_eq!(FileTreeDecision::Match, result.decision);
        assert_eq!("MATCH", result.decision.as_str());
        assert!(result.decision.is_actionable());
        assert_eq!(ByteSize::new(10), result.matched_size);
        assert_float_eq(1.0, result.matched_ratio);
    }

    #[test]
    fn flexible_mode_returns_size_only_with_deterministic_duplicate_ties() {
        let local_item = data_root_item();
        let local_files = vec![
            local_file("Local/z.mkv", 10, 2),
            local_file("Local/a.mkv", 10, 1),
        ];
        let candidate = torrent(
            vec![
                torrent_file("Candidate/a.mkv", 10, 0),
                torrent_file("Candidate/z.mkv", 10, 1),
            ],
            Some(ByteSize::new(4)),
        );

        let result = assess_file_tree(
            &local_item,
            &local_files,
            &candidate,
            FileTreeMatchConfig {
                mode: FileTreeMatchMode::Flexible,
                ..FileTreeMatchConfig::default()
            },
        );

        assert_eq!(FileTreeDecision::MatchSizeOnly, result.decision);
        assert_eq!(ByteSize::new(20), result.matched_size);
    }

    #[test]
    fn strict_mode_distinguishes_tree_and_size_mismatches() {
        let local_item = data_root_item();
        let local_files = vec![local_file("Local/a.mkv", 10, 0)];
        let size_only_candidate = torrent(vec![torrent_file("Other/a.mkv", 10, 0)], None);
        let size_mismatch_candidate = torrent(vec![torrent_file("Other/a.mkv", 20, 0)], None);
        let candidate_with_extra_file = torrent(
            vec![
                torrent_file("Local/a.mkv", 10, 0),
                torrent_file("Local/b.mkv", 10, 1),
            ],
            None,
        );
        let candidate_subset = torrent(vec![torrent_file("Local/a.mkv", 10, 0)], None);

        let tree_result = assess_file_tree(
            &local_item,
            &local_files,
            &size_only_candidate,
            FileTreeMatchConfig::default(),
        );
        let size_result = assess_file_tree(
            &local_item,
            &local_files,
            &size_mismatch_candidate,
            FileTreeMatchConfig::default(),
        );
        let extra_candidate_result = assess_file_tree(
            &local_item,
            &local_files,
            &candidate_with_extra_file,
            FileTreeMatchConfig::default(),
        );
        let extra_local_result = assess_file_tree(
            &local_item,
            &[
                local_file("Local/a.mkv", 10, 0),
                local_file("Local/b.mkv", 10, 1),
            ],
            &candidate_subset,
            FileTreeMatchConfig::default(),
        );

        assert_eq!(FileTreeDecision::FileTreeMismatch, tree_result.decision);
        assert_eq!(FileTreeDecision::SizeMismatch, size_result.decision);
        assert_eq!(
            FileTreeDecision::SizeMismatch,
            extra_candidate_result.decision
        );
        assert_eq!(FileTreeDecision::Match, extra_local_result.decision);
    }

    #[test]
    fn partial_mode_reports_size_gate_and_piece_gate_failures() {
        let local_item = data_root_item();
        let config = FileTreeMatchConfig {
            mode: FileTreeMatchMode::Partial,
            fuzzy_size_threshold: 0.5,
            season_from_episodes: 1.0,
        };
        let no_size_candidate = torrent(
            vec![
                torrent_file("Candidate/a.mkv", 40, 0),
                torrent_file("Candidate/b.mkv", 60, 1),
            ],
            Some(ByteSize::new(25)),
        );
        let piece_gate_candidate = torrent(
            vec![
                torrent_file("Candidate/a.mkv", 30, 0),
                torrent_file("Candidate/b.mkv", 30, 1),
                torrent_file("Candidate/c.mkv", 40, 2),
            ],
            Some(ByteSize::new(40)),
        );

        let size_result = assess_file_tree(
            &local_item,
            &[local_file("Local/a.mkv", 10, 0)],
            &no_size_candidate,
            config,
        );
        let tree_result = assess_file_tree(
            &local_item,
            &[local_file("Local/a.mkv", 30, 0)],
            &piece_gate_candidate,
            config,
        );

        assert_eq!(FileTreeDecision::PartialSizeMismatch, size_result.decision);
        assert_eq!(FileTreeDecision::FileTreeMismatch, tree_result.decision);
    }

    #[test]
    fn partial_mode_accepts_piece_ratio_threshold() {
        let local_item = data_root_item();
        let local_files = vec![
            local_file("Local/a.mkv", 40, 0),
            local_file("Local/b.mkv", 40, 1),
        ];
        let candidate = torrent(
            vec![
                torrent_file("Candidate/a.mkv", 40, 0),
                torrent_file("Candidate/b.mkv", 40, 1),
                torrent_file("Candidate/c.mkv", 20, 2),
            ],
            Some(ByteSize::new(20)),
        );

        let result = assess_file_tree(
            &local_item,
            &local_files,
            &candidate,
            FileTreeMatchConfig {
                mode: FileTreeMatchMode::Partial,
                fuzzy_size_threshold: 0.25,
                season_from_episodes: 1.0,
            },
        );

        assert_eq!(FileTreeDecision::MatchPartial, result.decision);
        assert_eq!(ByteSize::new(80), result.matched_size);
        assert_float_eq(0.8, result.matched_ratio);
    }

    #[test]
    fn partial_mode_respects_fuzzy_size_boundary() {
        let local_item = data_root_item();
        let config = FileTreeMatchConfig {
            mode: FileTreeMatchMode::Partial,
            fuzzy_size_threshold: 0.2,
            season_from_episodes: 1.0,
        };
        let accepted_candidate = torrent(
            vec![
                torrent_file("Candidate/a.mkv", 80, 0),
                torrent_file("Candidate/b.mkv", 20, 1),
            ],
            Some(ByteSize::new(20)),
        );
        let rejected_candidate = torrent(
            vec![
                torrent_file("Candidate/a.mkv", 79, 0),
                torrent_file("Candidate/b.mkv", 21, 1),
            ],
            Some(ByteSize::new(20)),
        );

        let accepted = assess_file_tree(
            &local_item,
            &[local_file("Local/a.mkv", 80, 0)],
            &accepted_candidate,
            config,
        );
        let rejected = assess_file_tree(
            &local_item,
            &[local_file("Local/a.mkv", 79, 0)],
            &rejected_candidate,
            config,
        );

        assert_eq!(FileTreeDecision::MatchPartial, accepted.decision);
        assert_eq!(FileTreeDecision::PartialSizeMismatch, rejected.decision);
    }

    #[test]
    fn virtual_items_match_by_file_name_and_length() {
        let local_item = virtual_item();
        let local_files = vec![local_file("Real/S01E01.mkv", 10, 0)];
        let candidate = torrent(vec![torrent_file("Show/S01E01.mkv", 10, 0)], None);
        let wrong_name = torrent(vec![torrent_file("Show/S01E02.mkv", 10, 0)], None);

        let result = assess_file_tree(
            &local_item,
            &local_files,
            &candidate,
            FileTreeMatchConfig::default(),
        );
        let wrong_name_result = assess_file_tree(
            &local_item,
            &local_files,
            &wrong_name,
            FileTreeMatchConfig::default(),
        );

        assert_eq!(FileTreeDecision::Match, result.decision);
        assert_eq!(
            FileTreeDecision::FileTreeMismatch,
            wrong_name_result.decision
        );
    }

    fn data_root_item() -> LocalItem {
        LocalItem {
            id: None,
            source: LocalItemSource::DataRoot {
                path: PathBuf::from("/media/example"),
            },
            title: ItemTitle::new("Example").unwrap(),
            display_name: DisplayName::new("Example").unwrap(),
            media_type: crate::domain::MediaType::Movie,
            info_hash: None,
            path: Some(PathBuf::from("/media/example")),
            save_path: None,
            total_size: ByteSize::new(10),
            mtime_ms: Some(1_700_000_000_000),
        }
    }

    fn search_item(title: &str, media_type: MediaType) -> LocalItem {
        LocalItem {
            id: None,
            source: LocalItemSource::DataRoot {
                path: PathBuf::from("/media/example"),
            },
            title: ItemTitle::new(title).unwrap(),
            display_name: DisplayName::new(title).unwrap(),
            media_type,
            info_hash: None,
            path: Some(PathBuf::from("/media/example")),
            save_path: None,
            total_size: ByteSize::new(10),
            mtime_ms: Some(1_700_000_000_000),
        }
    }

    fn all_caps() -> TorznabCaps {
        TorznabCaps {
            search: SearchCaps {
                generic_search: true,
                tv_search: true,
                movie_search: true,
                audio_search: true,
                supported_id_params: ["imdbid".to_owned(), "tvdbid".to_owned()]
                    .into_iter()
                    .collect(),
            },
            categories: CategoryCaps {
                movie: true,
                tv: true,
                anime: true,
                xxx: false,
                audio: true,
                book: true,
                additional: true,
            },
            limits: TorznabLimits {
                default: 100,
                max: 200,
            },
        }
    }

    fn no_movie_caps() -> TorznabCaps {
        TorznabCaps {
            search: SearchCaps {
                generic_search: true,
                tv_search: true,
                ..SearchCaps::default()
            },
            categories: CategoryCaps {
                tv: true,
                ..CategoryCaps::default()
            },
            limits: TorznabLimits::default(),
        }
    }

    fn cadence_indexer<'a>(
        indexer_id: u64,
        enabled: bool,
        caps: &'a TorznabCaps,
    ) -> SearchCadenceIndexer<'a> {
        SearchCadenceIndexer {
            indexer_id: IndexerId::new(indexer_id).unwrap(),
            enabled,
            caps,
        }
    }

    fn history(
        indexer_id: u64,
        first_searched_at_ms: i64,
        last_searched_at_ms: i64,
    ) -> SearchHistoryEntry {
        SearchHistoryEntry {
            indexer_id: IndexerId::new(indexer_id).unwrap(),
            first_searched_at_ms,
            last_searched_at_ms,
        }
    }

    fn virtual_item() -> LocalItem {
        LocalItem {
            id: None,
            source: LocalItemSource::Virtual {
                source_key: SourceKey::new("show-s01").unwrap(),
            },
            title: ItemTitle::new("Show S01").unwrap(),
            display_name: DisplayName::new("Show S01").unwrap(),
            media_type: crate::domain::MediaType::SeasonPack,
            info_hash: None,
            path: None,
            save_path: None,
            total_size: ByteSize::new(10),
            mtime_ms: Some(1_700_000_000_000),
        }
    }

    fn candidate_input(title: &str) -> CandidatePrecheckInput<'_> {
        CandidatePrecheckInput {
            title,
            download_url: Some("https://indexer.example/download/1"),
            tracker: Some("tracker.example"),
            size: Some(ByteSize::new(10)),
            info_hash: None,
        }
    }

    fn rejected_reason(decision: CandidatePrecheckDecision) -> CandidatePrecheckReason {
        match decision {
            CandidatePrecheckDecision::Rejected(reason) => reason,
            CandidatePrecheckDecision::Accepted => panic!("candidate precheck accepted"),
        }
    }

    fn local_file(path: &str, size: u64, index: u32) -> LocalFile {
        LocalFile::new(
            Some(crate::domain::LocalItemId::new(1).unwrap()),
            PathBuf::from(path),
            ByteSize::new(size),
            FileIndex::new(index),
        )
        .unwrap()
    }

    fn torrent_file(path: &str, size: u64, index: u32) -> TorrentFile {
        TorrentFile::new(
            PathBuf::from(path),
            ByteSize::new(size),
            FileIndex::new(index),
        )
        .unwrap()
    }

    fn torrent(files: Vec<TorrentFile>, piece_length: Option<ByteSize>) -> TorrentMetafile {
        TorrentMetafile::new_with_piece_length(
            InfoHash::new("0123456789abcdef0123456789abcdef01234567").unwrap(),
            DisplayName::new("Candidate").unwrap(),
            files,
            piece_length,
        )
        .unwrap()
    }

    fn assert_float_eq(expected: f64, actual: f64) {
        assert!(
            (expected - actual).abs() < f64::EPSILON,
            "expected {expected}, got {actual}"
        );
    }
}
