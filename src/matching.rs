use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;

use crate::domain::{
    ByteSize, LocalFile, LocalItem, LocalItemSource, MediaType, TorrentFile, TorrentMetafile,
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

        assert_eq!(FileTreeDecision::FileTreeMismatch, tree_result.decision);
        assert_eq!(FileTreeDecision::SizeMismatch, size_result.decision);
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
