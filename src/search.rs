//! Searchee discovery, filtering, Torznab queries, RSS, and announce workflows.

use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    sync::{Arc, LazyLock, Mutex},
    time::{Duration, Instant, UNIX_EPOCH},
};

use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use regex::Regex;
use serde::Deserialize;
use tokio::runtime::Builder;
use walkdir::{DirEntry, WalkDir};

use crate::{
    SporosError,
    domain::{
        ActionResult, Candidate, ClientLabel, ClientTorrentMetadata, File, InfoHash, Label,
        LookupFields, MediaType, Searchee,
    },
    integrations::{
        ArrConfig, CategoryCaps, SearchIndexer, SnatchHistory, SnatchOptions, TorznabCaps,
        TorznabQuery, TorznabSearchIds, TorznabSearchOptions, arr_search_cache_key,
        create_torznab_search_queries, ids_for_torznab_caps, lookup_arr_ids,
        search_torznab_indexer, set_indexer_status,
    },
    matching::{Assessment, AssessmentOptions, CandidateAssessmentContext, assess_candidate},
    persistence::{
        ClientSearcheeCacheRecord, Database, EnsembleCacheRecord, ReverseLookupCriteria,
    },
    torrent::parse_metafile,
};

const REVERSE_LOOKUP_PAGE_SIZE: i64 = 256;

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
const EIGHT_DAYS_MILLIS: u64 = 8 * 24 * 60 * 60 * 1000;

/// Active recursive data-dir watcher. Dropping it stops watching.
pub struct DataDirWatchState {
    watcher: RecommendedWatcher,
    roots: Vec<PathBuf>,
}

/// Result counts from indexing a torrent_dir.
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

/// Parsed blocklist.
#[derive(Debug)]
pub struct Blocklist {
    rules: Vec<BlocklistRule>,
}

impl Blocklist {
    /// Parse configured blocklist strings.
    pub fn parse(entries: &[String]) -> crate::Result<Self> {
        let mut rules = Vec::with_capacity(entries.len());
        let mut size_below = None;
        let mut size_above = None;
        for entry in entries {
            let trimmed = entry.trim();
            if trimmed.is_empty() {
                continue;
            }
            let (kind, value) = trimmed.split_once(':').ok_or_else(|| {
                search_error(format!(
                    "invalid block_list entry {trimmed:?}: expected <type>:<value>"
                ))
            })?;
            let rule = if kind == "name_regex" {
                BlocklistRule::NameRegex(Regex::new(value).map_err(|error| {
                    search_error(format!(
                        "invalid block_list name_regex entry {trimmed:?}: {error}"
                    ))
                })?)
            } else if kind == "folder_regex" {
                BlocklistRule::FolderRegex(Regex::new(value).map_err(|error| {
                    search_error(format!(
                        "invalid block_list folder_regex entry {trimmed:?}: {error}"
                    ))
                })?)
            } else if kind == "name" {
                BlocklistRule::NameContains(value.to_ascii_lowercase())
            } else if kind == "category" {
                BlocklistRule::Category(value.to_ascii_lowercase())
            } else if kind == "tag" {
                BlocklistRule::Tag(value.to_ascii_lowercase())
            } else if kind == "tracker" {
                BlocklistRule::Tracker(value.to_ascii_lowercase())
            } else if kind == "folder" {
                BlocklistRule::FolderContains(value.to_ascii_lowercase())
            } else if kind == "info_hash" {
                let info_hash = crate::domain::InfoHash::new(value.to_ascii_lowercase())
                    .ok_or_else(|| {
                        search_error(format!("invalid block_list info_hash entry {trimmed:?}"))
                    })?;
                BlocklistRule::InfoHash(info_hash.to_string())
            } else if kind == "size_below" {
                if size_below
                    .replace(parse_blocklist_size(trimmed, value)?)
                    .is_some()
                {
                    return Err(search_error("block_list allows only one size_below entry"));
                }
                BlocklistRule::SizeBelow(size_below.unwrap_or_default())
            } else if kind == "size_above" {
                if size_above
                    .replace(parse_blocklist_size(trimmed, value)?)
                    .is_some()
                {
                    return Err(search_error("block_list allows only one size_above entry"));
                }
                BlocklistRule::SizeAbove(size_above.unwrap_or_default())
            } else {
                return Err(search_error(format!(
                    "invalid block_list entry type {kind:?}; use explicit snake_case blocklist types"
                )));
            };
            rules.push(rule);
        }
        if let (Some(below), Some(above)) = (size_below, size_above) {
            if below > above {
                return Err(search_error("block_list requires size_below <= size_above"));
            }
        }
        Ok(Self { rules })
    }

    /// Whether any rule matches a searchee.
    pub fn matches_searchee(&self, searchee: &Searchee<'_>) -> bool {
        self.rules.iter().any(|rule| rule.matches(searchee))
    }
}

#[derive(Debug)]
enum BlocklistRule {
    NameContains(String),
    NameRegex(Regex),
    Category(String),
    Tag(String),
    Tracker(String),
    FolderContains(String),
    FolderRegex(Regex),
    InfoHash(String),
    SizeBelow(u64),
    SizeAbove(u64),
}

impl BlocklistRule {
    fn matches(&self, searchee: &Searchee<'_>) -> bool {
        match self {
            Self::NameContains(value) => {
                contains_ignore_case(searchee.name.as_ref(), value)
                    || contains_ignore_case(searchee.title.as_ref(), value)
            }
            Self::NameRegex(regex) => {
                regex.is_match(searchee.name.as_ref()) || regex.is_match(searchee.title.as_ref())
            }
            Self::Category(value) => searchee
                .client
                .as_ref()
                .and_then(|client| client.category.as_ref())
                .is_some_and(|category| eq_ignore_case(category.as_str(), value)),
            Self::Tag(value) => searchee.client.as_ref().is_some_and(|client| {
                if value.is_empty() {
                    client.tags.is_empty()
                } else {
                    client
                        .tags
                        .iter()
                        .any(|tag| eq_ignore_case(tag.as_str(), value))
                }
            }),
            Self::Tracker(value) => searchee.client.as_ref().is_some_and(|client| {
                client
                    .trackers
                    .iter()
                    .any(|tracker| eq_ignore_case(tracker.as_ref(), value))
            }),
            Self::FolderContains(value) => searchee
                .path
                .as_ref()
                .is_some_and(|path| contains_ignore_case(path.as_ref(), value)),
            Self::FolderRegex(regex) => searchee
                .path
                .as_ref()
                .is_some_and(|path| regex.is_match(path.as_ref())),
            Self::InfoHash(value) => searchee
                .info_hash
                .as_ref()
                .is_some_and(|info_hash| info_hash.as_str().eq_ignore_ascii_case(value)),
            Self::SizeBelow(value) => searchee.length < *value,
            Self::SizeAbove(value) => searchee.length > *value,
        }
    }
}

fn parse_blocklist_size(entry: &str, value: &str) -> crate::Result<u64> {
    value
        .parse::<u64>()
        .map_err(|error| search_error(format!("invalid block_list size entry {entry:?}: {error}")))
}

/// Content filter options for search, webhook, RSS, and announce flows.
#[derive(Debug, Clone)]
pub struct ContentFilterOptions<'a> {
    /// Parsed blocklist.
    pub blocklist: &'a Blocklist,
    /// Accept after blocklist checks.
    pub blocklist_only: bool,
    /// Include single episode searchees.
    pub include_single_episodes: bool,
    /// Include releases with non-video bytes over the fuzzy threshold.
    pub include_non_videos: bool,
    /// Fuzzy size threshold used for non-video ratio checks.
    pub fuzzy_size_threshold: f64,
    /// Reject known cross-seed client entries.
    pub ignore_cross_seeds: bool,
    /// Configured link category used by cross-seed detection.
    pub link_category: Option<&'a str>,
    /// Current workflow label.
    pub label: Option<Label>,
}

/// Reasons a searchee can be rejected before search or reverse matching.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ContentFilterRejection {
    /// Name, folder, tracker, category, or tag matched the blocklist.
    Blocklisted,
    /// Data-dir single episode inside a season-pack folder is disabled.
    DataDirSingleEpisodeInSeasonPack,
    /// Single episodes are disabled for this workflow.
    SingleEpisode,
    /// Non-video bytes exceed the configured threshold.
    NonVideoRatio,
    /// Client metadata identifies an existing cross-seed.
    CrossSeed,
    /// Data-dir root appears to be an Arr library folder rather than a release.
    ArrRoot,
    /// Season 0 or Specials folder.
    Specials,
    /// Search/webhook searchee has non-standard episode or season naming.
    NonStandardNaming,
}

/// Timestamp state for one searchee/indexer pair.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct TimestampDecision {
    /// First searched timestamp.
    pub first_searched: u64,
    /// Last searched timestamp.
    pub last_searched: u64,
}

/// Parsed media capability flags for one indexer.
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
pub struct MediaCapabilities {
    /// TV search capability.
    pub tv: bool,
    /// Movie search capability.
    pub movie: bool,
    /// Music/audio search capability.
    pub audio: bool,
    /// Book search capability.
    pub book: bool,
    /// Generic search fallback.
    pub generic: bool,
}

/// One matched candidate that reached the configured action hook.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PipelineAction<'a> {
    /// Workflow label for notifications and action side effects.
    pub label: Label,
    /// Local item being cross-seeded.
    pub searchee: &'a Searchee<'a>,
    /// Remote candidate that matched the searchee.
    pub candidate: &'a Candidate<'a>,
    /// Conservative assessment result.
    pub assessment: &'a Assessment,
}

/// Persisted result from one candidate assessment and optional action.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PipelineAttempt {
    /// Workflow label.
    pub label: Label,
    /// Local searchee title.
    pub searchee_title: String,
    /// Remote candidate release name.
    pub candidate_name: String,
    /// Remote candidate GUID.
    pub candidate_guid: String,
    /// Candidate info hashes from a matched metafile.
    pub candidate_info_hashes: Vec<String>,
    /// Candidate or metafile tracker names.
    pub trackers: Vec<String>,
    /// Candidate decision.
    pub decision: crate::domain::Decision,
    /// Action outcome when a match was dispatched.
    pub action_result: Option<ActionResult>,
    /// Searchee category when known.
    pub searchee_category: Option<String>,
    /// Searchee tags when known.
    pub searchee_tags: Vec<String>,
    /// Searchee trackers when known.
    pub searchee_trackers: Vec<String>,
    /// Searchee byte length.
    pub searchee_length: u64,
    /// Searchee client host when sourced from a torrent client.
    pub searchee_client_host: Option<String>,
    /// Searchee local info hash when known.
    pub searchee_info_hash: Option<String>,
    /// Searchee filesystem path when known.
    pub searchee_path: Option<String>,
    /// Searchee source type.
    pub searchee_source_type: String,
}

/// Summary returned by bulk search and targeted search flows.
#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct PipelineSummary {
    /// Searchees considered after source discovery.
    pub searchees_seen: usize,
    /// Searchees rejected by content filters.
    pub searchees_filtered: usize,
    /// Real indexer searches performed.
    pub indexer_searches: usize,
    /// Remote candidates assessed.
    pub candidates_assessed: usize,
    /// Match/action attempts.
    pub attempts: Vec<PipelineAttempt>,
}

/// Shared candidate cache used by a bulk search batch.
#[derive(Debug, Default, Clone)]
pub struct CandidateSearchCache {
    entries: BTreeMap<(String, i64), CachedCandidates>,
    indexer_search_counts: BTreeMap<i64, usize>,
    last_indexer_search: Option<Instant>,
}

impl CandidateSearchCache {
    fn remaining_search_limit(&self, indexer_id: i64, limit: usize) -> usize {
        limit.saturating_sub(
            self.indexer_search_counts
                .get(&indexer_id)
                .copied()
                .unwrap_or_default(),
        )
    }

    fn record_indexer_candidates(&mut self, indexer_id: i64, count: usize) {
        *self.indexer_search_counts.entry(indexer_id).or_default() += count;
    }

    fn wait_for_indexer_delay(&mut self, delay: Duration) -> crate::Result<()> {
        if delay.is_zero() {
            return Ok(());
        }
        let Some(last_search) = self.last_indexer_search else {
            return Ok(());
        };
        let remaining = delay.saturating_sub(last_search.elapsed());
        if !remaining.is_zero() {
            block_on_delay(remaining)?;
        }
        Ok(())
    }

    fn mark_indexer_search(&mut self) {
        self.last_indexer_search = Some(Instant::now());
    }
}

#[derive(Debug, Clone)]
struct CachedCandidates {
    ids_key: String,
    candidates: Vec<Candidate<'static>>,
}

/// Runtime settings for search orchestration.
pub struct SearchPipelineOptions<'a> {
    /// Workflow label, normally `search` or `webhook`.
    pub label: Label,
    /// Content filters applied before searching.
    pub filter: ContentFilterOptions<'a>,
    /// Candidate assessment options.
    pub assessment: AssessmentOptions<'a>,
    /// Candidate snatch retry behavior.
    pub snatch: SnatchOptions,
    /// Torznab request options.
    pub torznab: TorznabSearchOptions,
    /// Optional Arr parser instances.
    pub arr_configs: &'a [ArrConfig],
    /// Arr parser timeout.
    pub arr_timeout: Option<Duration>,
    /// Optional virtual season creation.
    pub virtual_season: Option<VirtualSeasonOptions>,
    /// Skip searchee/indexer pairs searched before this age window.
    pub exclude_older: Option<u64>,
    /// Skip searchee/indexer pairs searched inside this recent window.
    pub exclude_recent_search: Option<u64>,
}

/// Shared runtime dependencies for bulk and targeted search flows.
pub struct SearchPipelineRuntime<'a, 'b> {
    /// SQLite state.
    pub database: &'a Database,
    /// Application directory containing cached torrents.
    pub app_dir: &'a Path,
    /// Pipeline settings.
    pub options: &'a SearchPipelineOptions<'a>,
    /// Per-batch shared candidate cache.
    pub cache: &'b mut CandidateSearchCache,
}

static SHARED_REVERSE_LOOKUP_PERMIT: LazyLock<Arc<Mutex<()>>> =
    LazyLock::new(|| Arc::new(Mutex::new(())));

/// One-permit shared gate for RSS and announce reverse lookups.
#[derive(Debug, Clone)]
pub struct ReverseLookupGate {
    permit: Arc<Mutex<()>>,
}

impl ReverseLookupGate {
    /// Create a handle to the shared reverse lookup gate.
    pub fn new() -> Self {
        Self {
            permit: Arc::clone(&SHARED_REVERSE_LOOKUP_PERMIT),
        }
    }
}

impl Default for ReverseLookupGate {
    fn default() -> Self {
        Self::new()
    }
}

/// Runtime dependencies for RSS, announce, and webhook reverse lookup.
pub struct ReverseLookupRuntime<'a> {
    /// One-permit concurrency gate.
    pub gate: &'a ReverseLookupGate,
    /// SQLite state.
    pub database: &'a Database,
    /// Application directory containing cached torrents.
    pub app_dir: &'a Path,
    /// Pipeline settings.
    pub options: &'a SearchPipelineOptions<'a>,
}

/// Configured searchee inputs for source selection.
#[derive(Debug, Default)]
pub struct SearcheeSources<'a> {
    /// Explicit `.torrent` paths, when a targeted CLI/API request supplied them.
    pub torrents: Option<&'a [PathBuf]>,
    /// Whether configured torrent-client inventory should be used.
    pub use_client_torrents: bool,
    /// Already-loaded torrent-client searchees from client adapters.
    pub client_searchees: &'a [Searchee<'static>],
    /// Configured torrent directory fallback.
    pub torrent_dir: Option<&'a Path>,
    /// Configured data directories.
    pub data_dirs: &'a [PathBuf],
    /// Maximum data-dir walk depth.
    pub max_data_depth: u32,
}

/// Choose and load searchee sources using the documented precedence.
pub fn find_all_searchees(
    sources: &SearcheeSources<'_>,
    label: Label,
) -> crate::Result<Vec<Searchee<'static>>> {
    let mut output = Vec::new();
    if let Some(torrents) = sources.torrents {
        for path in torrents {
            if let Some(searchee) = torrent_file_searchee(path, label)? {
                output.push(searchee);
            }
        }
    } else if sources.use_client_torrents {
        output.extend(
            sources
                .client_searchees
                .iter()
                .cloned()
                .map(|mut searchee| {
                    searchee.label = Some(label);
                    searchee
                }),
        );
    } else if let Some(torrent_dir) = sources.torrent_dir {
        for entry in fs::read_dir(torrent_dir)
            .map_err(|error| search_error(format!("failed to read torrent_dir: {error}")))?
        {
            let entry = entry.map_err(|error| {
                search_error(format!("failed to read torrent_dir entry: {error}"))
            })?;
            let path = entry.path();
            if path.extension().and_then(std::ffi::OsStr::to_str) == Some("torrent") {
                if let Some(searchee) = torrent_file_searchee(&path, label)? {
                    output.push(searchee);
                }
            }
        }
    }

    for_each_data_dir_searchee(sources.data_dirs, sources.max_data_depth, |mut searchee| {
        searchee.label = Some(label);
        output.push(searchee);
        Ok(())
    })?;
    Ok(output)
}

/// Find searchable searchees from already-loaded sources and configured data dirs.
pub fn find_searchable_searchees(
    mut real_searchees: Vec<Searchee<'static>>,
    data_dirs: &[PathBuf],
    max_depth: u32,
    options: &SearchPipelineOptions<'_>,
) -> crate::Result<Vec<Searchee<'static>>> {
    for_each_data_dir_searchee(data_dirs, max_depth, |searchee| {
        real_searchees.push(searchee);
        Ok(())
    })?;
    let virtuals = options
        .virtual_season
        .map(|virtual_options| create_virtual_season_searchees(&real_searchees, virtual_options))
        .unwrap_or_default();
    let mut ensemble = real_searchees;
    ensemble.extend(virtuals);
    Ok(filter_duplicate_searchees(
        ensemble
            .into_iter()
            .filter(|searchee| filter_by_content(searchee, &options.filter).is_none())
            .collect(),
    ))
}

/// Run bulk search over a set of searchees and dispatch matched candidates.
pub fn bulk_search<A, N>(
    runtime: &mut SearchPipelineRuntime<'_, '_>,
    searchees: &[Searchee<'static>],
    indexers: &[SearchIndexer],
    mut action: A,
    mut notify: N,
) -> crate::Result<PipelineSummary>
where
    A: FnMut(&PipelineAction<'_>) -> crate::Result<Option<ActionResult>>,
    N: FnMut(&PipelineAttempt) -> crate::Result<()>,
{
    let mut summary = PipelineSummary {
        searchees_seen: searchees.len(),
        ..PipelineSummary::default()
    };
    let mut snatch_history = SnatchHistory::default();

    for searchee in searchees {
        let database = runtime.database;
        let options = runtime.options;
        if filter_by_content(searchee, &options.filter).is_some() {
            summary.searchees_filtered += 1;
            continue;
        }
        let searchee_id = database
            .get_or_insert_searchee(searchee.title.as_ref(), options.torznab.now_millis as i64)?;
        let group_key = search_group_key(searchee);
        let arr_lookup = lookup_arr_ids(options.arr_configs, searchee, options.arr_timeout)?;
        let arr_ids = arr_lookup.as_ref().map(|lookup| &lookup.ids);
        let ids_key = arr_lookup
            .as_ref()
            .map(|lookup| arr_search_cache_key(&group_key, &lookup.ids))
            .unwrap_or_else(|| group_key.clone());

        for indexer in indexers {
            if timestamp_excludes(
                read_timestamp(database, searchee_id, indexer.id)?,
                options.torznab.now_millis,
                options.exclude_older,
                options.exclude_recent_search,
                searchee.mtime_millis,
            ) {
                continue;
            }
            let search_result = cached_or_search_candidates(
                CandidateSearchRequest {
                    database,
                    indexer,
                    searchee,
                    arr_ids,
                    group_key: &group_key,
                    ids_key: &ids_key,
                    options,
                },
                runtime.cache,
                &mut summary,
            )?;
            for candidate in search_result.candidates {
                let attempt = assess_and_dispatch(
                    database,
                    runtime.app_dir,
                    options,
                    searchee,
                    &candidate,
                    &mut snatch_history,
                    &mut action,
                )?;
                summary.candidates_assessed += 1;
                if attempt.decision == crate::domain::Decision::RateLimited {
                    set_indexer_status(
                        database,
                        indexer.id,
                        Some("RATE_LIMITED"),
                        Some(options.torznab.now_millis.saturating_add(60 * 60 * 1000)),
                    )?;
                }
                notify(&attempt)?;
                summary.attempts.push(attempt);
            }
            if search_result.update_timestamp {
                update_timestamp(
                    database,
                    searchee_id,
                    indexer.id,
                    options.torznab.now_millis,
                )?;
            }
        }
    }

    Ok(summary)
}

/// Run a targeted find-on-other-sites search for one webhook/API searchee.
pub fn find_on_other_sites<A, N>(
    runtime: &mut SearchPipelineRuntime<'_, '_>,
    searchee: Searchee<'static>,
    indexers: &[SearchIndexer],
    action: A,
    notify: N,
) -> crate::Result<PipelineSummary>
where
    A: FnMut(&PipelineAction<'_>) -> crate::Result<Option<ActionResult>>,
    N: FnMut(&PipelineAttempt) -> crate::Result<()>,
{
    bulk_search(runtime, &[searchee], indexers, action, notify)
}

/// Reverse-match one RSS, announce, or webhook candidate against local searchees.
pub fn check_new_candidate_match<A, N>(
    runtime: &ReverseLookupRuntime<'_>,
    candidate: &Candidate<'static>,
    local_searchees: &[Searchee<'static>],
    mut action: A,
    mut notify: N,
) -> crate::Result<Option<PipelineAttempt>>
where
    A: FnMut(&PipelineAction<'_>) -> crate::Result<Option<ActionResult>>,
    N: FnMut(&PipelineAttempt) -> crate::Result<()>,
{
    let _permit = runtime
        .gate
        .permit
        .lock()
        .map_err(|_error| search_error("reverse lookup gate was poisoned"))?;
    let mut snatch_history = SnatchHistory::default();
    let mut candidates = reverse_lookup_candidates(runtime, candidate, local_searchees)?;
    sort_reverse_lookup_searchees(&mut candidates);

    let mut best: Option<PipelineAttempt> = None;
    for searchee in candidates {
        let attempt = assess_and_dispatch(
            runtime.database,
            runtime.app_dir,
            runtime.options,
            &searchee,
            candidate,
            &mut snatch_history,
            &mut action,
        )?;
        notify(&attempt)?;
        if attempt
            .action_result
            .is_some_and(crate::domain::ActionResult::accepted)
            || matches!(
                attempt.decision,
                crate::domain::Decision::InfoHashAlreadyExists
                    | crate::domain::Decision::SameInfoHash
            )
        {
            return Ok(Some(attempt));
        }
        if best_failure(&attempt, best.as_ref()) {
            best = Some(attempt);
        }
    }
    Ok(best)
}

/// Reverse-match a batch of RSS or announce candidates with the same one-permit gate.
pub fn check_new_candidate_matches<A, N>(
    runtime: &ReverseLookupRuntime<'_>,
    candidates: &[Candidate<'static>],
    local_searchees: &[Searchee<'static>],
    mut action: A,
    mut notify: N,
) -> crate::Result<Vec<PipelineAttempt>>
where
    A: FnMut(&PipelineAction<'_>) -> crate::Result<Option<ActionResult>>,
    N: FnMut(&PipelineAttempt) -> crate::Result<()>,
{
    let mut attempts = Vec::new();
    for candidate in candidates {
        if let Some(attempt) = check_new_candidate_match(
            runtime,
            candidate,
            local_searchees,
            &mut action,
            &mut notify,
        )? {
            attempts.push(attempt);
        }
    }
    Ok(attempts)
}

/// Return likely local searchees for a remote candidate name.
pub fn reverse_lookup_searchees(
    candidate: &Candidate<'_>,
    local_searchees: &[Searchee<'static>],
    filter: &ContentFilterOptions<'_>,
) -> Vec<Searchee<'static>> {
    let keys = reverse_lookup_keys(candidate.name.as_ref());
    let mut output = local_searchees
        .iter()
        .filter(|searchee| filter_by_content(searchee, filter).is_none())
        .filter(|searchee| {
            keys.iter()
                .any(|key| fuzzy_title_match(key, &search_group_key(searchee)))
        })
        .cloned()
        .collect::<Vec<_>>();
    output = filter_duplicate_searchees(output);
    sort_reverse_lookup_searchees(&mut output);
    output
}

fn reverse_lookup_candidates(
    runtime: &ReverseLookupRuntime<'_>,
    candidate: &Candidate<'_>,
    local_searchees: &[Searchee<'static>],
) -> crate::Result<Vec<Searchee<'static>>> {
    let keys = reverse_lookup_keys(candidate.name.as_ref());
    let mut output = reverse_lookup_searchees(candidate, local_searchees, &runtime.options.filter);
    output.extend(reverse_lookup_client_rows(
        runtime.database,
        candidate,
        &keys,
        &runtime.options.filter,
    )?);
    output.extend(reverse_lookup_data_rows(
        runtime.database,
        &keys,
        &runtime.options.filter,
    )?);
    output.extend(reverse_lookup_ensemble_rows(
        runtime.database,
        candidate,
        &runtime.options.filter,
        runtime.options.virtual_season,
    )?);
    output = filter_duplicate_searchees(output);
    sort_reverse_lookup_searchees(&mut output);
    Ok(output)
}

/// Options for virtual season construction.
#[derive(Debug, Clone, Copy)]
pub struct VirtualSeasonOptions {
    /// Required episode ratio against the highest episode number.
    pub season_from_episodes: f64,
    /// Apply production freshness and minimum-count filters.
    pub use_filters: bool,
    /// Current time in milliseconds for age filtering.
    pub now_millis: u64,
}

/// Episode row materialized into the ensemble cache.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EpisodeEnsemble {
    /// Representative file path for the episode.
    pub path: String,
    /// Normalized season key.
    pub ensemble: String,
    /// Episode number within the season.
    pub element: String,
}

/// Remove duplicate searchees from a set already believed to describe the same media.
pub fn filter_duplicate_searchees(mut searchees: Vec<Searchee<'static>>) -> Vec<Searchee<'static>> {
    searchees.sort_by(|left, right| {
        right
            .info_hash
            .is_some()
            .cmp(&left.info_hash.is_some())
            .then_with(|| left.title.cmp(&right.title))
            .then_with(|| left.length.cmp(&right.length))
    });
    let mut filtered = Vec::with_capacity(searchees.len());
    'outer: for searchee in searchees {
        for existing in &filtered {
            if duplicate_searchee(existing, &searchee) {
                continue 'outer;
            }
        }
        filtered.push(searchee);
    }
    filtered
}

/// Build the lowercased search grouping key used for cached search reuse.
pub fn search_group_key(searchee: &Searchee<'_>) -> String {
    let caps = search_group_caps();
    if let Some(query) = create_torznab_search_queries(searchee, &caps, None).first() {
        let key = torznab_query_group_key(query);
        if !key.is_empty() {
            return key;
        }
    }
    normalized_query_key(searchee.title.as_ref())
}

fn search_group_caps() -> TorznabCaps {
    TorznabCaps {
        search: true,
        tv_search: true,
        movie_search: true,
        music_search: true,
        audio_search: true,
        book_search: true,
        tv_ids: Vec::new(),
        movie_ids: Vec::new(),
        categories: CategoryCaps {
            movie: true,
            tv: true,
            anime: true,
            xxx: true,
            audio: true,
            book: true,
            additional: true,
        },
        limits: Default::default(),
    }
}

fn torznab_query_group_key(query: &TorznabQuery) -> String {
    let mut parts = Vec::new();
    if let Some(query) = torznab_param(query, "q") {
        let query = normalized_query_key(query);
        if !query.is_empty() {
            parts.push(query);
        }
    }
    if let Some(season) = torznab_param(query, "season") {
        let season = normalized_query_key(season);
        if !season.is_empty() {
            parts.push(format!("s{season}"));
        }
    }
    if let Some(episode) = torznab_param(query, "ep") {
        let episode = normalized_query_key(episode);
        if !episode.is_empty() {
            parts.push(format!("e{episode}"));
        }
    }
    parts.join(".")
}

fn torznab_param<'a>(query: &'a TorznabQuery, name: &str) -> Option<&'a str> {
    query
        .params
        .iter()
        .find_map(|(key, value)| (key == name).then_some(value.as_str()))
}

/// Return true when timestamp history should skip this searchee/indexer.
pub fn timestamp_excludes(
    timestamp: Option<TimestampDecision>,
    now_millis: u64,
    exclude_older: Option<u64>,
    exclude_recent_search: Option<u64>,
    virtual_mtime_millis: Option<u64>,
) -> bool {
    let Some(timestamp) = timestamp else {
        return false;
    };
    if virtual_mtime_millis.is_some_and(|mtime| mtime > timestamp.last_searched) {
        return false;
    }
    if exclude_older.is_some_and(|age| timestamp.first_searched < now_millis.saturating_sub(age)) {
        return true;
    }
    if exclude_recent_search
        .is_some_and(|age| timestamp.last_searched > now_millis.saturating_sub(age))
    {
        return true;
    }
    false
}

/// Check whether an indexer can search the searchee media type.
pub fn indexer_supports_media(media_type: MediaType, caps: MediaCapabilities) -> bool {
    match media_type {
        MediaType::Episode | MediaType::Pack | MediaType::Anime => caps.tv || caps.generic,
        MediaType::Movie | MediaType::Video => caps.movie || caps.generic,
        MediaType::Audio => caps.audio || caps.generic,
        MediaType::Book => caps.book || caps.generic,
        MediaType::Unknown => caps.generic,
    }
}

/// Build virtual season searchees from episode searchees.
pub fn create_virtual_season_searchees(
    searchees: &[Searchee<'_>],
    options: VirtualSeasonOptions,
) -> Vec<Searchee<'static>> {
    let existing_seasons = searchees
        .iter()
        .filter(|searchee| searchee.media_type == MediaType::Pack)
        .filter_map(|searchee| season_key_from_title(searchee.title.as_ref()))
        .collect::<BTreeSet<_>>();
    let mut groups: BTreeMap<SeasonKey, BTreeMap<u32, EpisodeChoice>> = BTreeMap::new();

    for searchee in searchees {
        if searchee.media_type != MediaType::Episode {
            continue;
        }
        let Some((key, episode)) = episode_key_from_title(searchee.title.as_ref()) else {
            continue;
        };
        if options.use_filters && existing_seasons.contains(&key) {
            continue;
        }
        let Some(file) = searchee.files.iter().max_by_key(|file| file.length) else {
            continue;
        };
        let choice = EpisodeChoice {
            file: file.clone().into_owned(),
            length: file.length,
            mtime_millis: searchee.mtime_millis,
            client_host: searchee
                .client
                .as_ref()
                .map(|client| client.host.as_ref().to_owned()),
        };
        groups
            .entry(key)
            .or_default()
            .entry(episode)
            .and_modify(|existing| {
                if choice.length > existing.length
                    || (choice.length == existing.length
                        && choice.mtime_millis.unwrap_or(u64::MAX)
                            < existing.mtime_millis.unwrap_or(u64::MAX))
                {
                    *existing = choice.clone();
                }
            })
            .or_insert(choice);
    }

    let mut virtuals = Vec::new();
    for (key, episodes) in groups {
        let Some(highest_episode) = episodes.keys().next_back().copied() else {
            continue;
        };
        if highest_episode == 0 {
            continue;
        }
        let ratio = episodes.len() as f64 / f64::from(highest_episode);
        let newest_mtime = episodes
            .values()
            .filter_map(|episode| episode.mtime_millis)
            .max();
        if options.use_filters
            && (episodes.len() < 3
                || ratio < options.season_from_episodes
                || newest_mtime.is_some_and(|mtime| {
                    options.now_millis.saturating_sub(mtime) < EIGHT_DAYS_MILLIS
                }))
        {
            continue;
        }

        let files = episodes
            .values()
            .map(|episode| episode.file.clone())
            .collect::<Vec<_>>();
        let title = format!("{} S{:02}", key.title, key.season);
        let mut searchee = Searchee::from_files(title.clone(), title, files);
        searchee.media_type = MediaType::Pack;
        searchee.mtime_millis = newest_mtime;
        if let Some(host) = choose_virtual_client_host(episodes.values()) {
            searchee.client = Some(ClientTorrentMetadata::new(
                host,
                "",
                None,
                Vec::new(),
                Vec::new(),
            ));
        }
        virtuals.push(searchee.into_owned());
    }
    virtuals
}

/// Build an ensemble-cache row from an episode searchee.
pub fn episode_ensemble(searchee: &Searchee<'_>) -> Option<EpisodeEnsemble> {
    if searchee.media_type != MediaType::Episode {
        return None;
    }
    let (key, episode) = episode_key_from_title(searchee.title.as_ref())?;
    let file = searchee.files.iter().max_by_key(|file| file.length)?;
    Some(EpisodeEnsemble {
        path: searchee_file_path(searchee, file),
        ensemble: format!("{} S{:02}", key.title, key.season),
        element: format!("{episode:02}"),
    })
}

/// Apply documented content filters and return a rejection reason when filtered.
pub fn filter_by_content(
    searchee: &Searchee<'_>,
    options: &ContentFilterOptions<'_>,
) -> Option<ContentFilterRejection> {
    if options.blocklist.matches_searchee(searchee) {
        return Some(ContentFilterRejection::Blocklisted);
    }
    if options.blocklist_only {
        return None;
    }
    if data_dir_single_episode_in_season_pack(searchee) && !options.include_single_episodes {
        return Some(ContentFilterRejection::DataDirSingleEpisodeInSeasonPack);
    }
    if searchee.media_type == MediaType::Episode
        && !options.include_single_episodes
        && options.label != Some(Label::Announce)
    {
        return Some(ContentFilterRejection::SingleEpisode);
    }
    if !options.include_non_videos && non_video_ratio(searchee) > options.fuzzy_size_threshold {
        return Some(ContentFilterRejection::NonVideoRatio);
    }
    if options.ignore_cross_seeds && is_cross_seed(searchee, options.link_category) {
        return Some(ContentFilterRejection::CrossSeed);
    }
    if looks_like_arr_root(searchee) {
        return Some(ContentFilterRejection::ArrRoot);
    }
    if is_specials(searchee) {
        return Some(ContentFilterRejection::Specials);
    }
    if matches!(options.label, Some(Label::Search | Label::Webhook))
        && non_standard_naming(searchee)
    {
        return Some(ContentFilterRejection::NonStandardNaming);
    }
    None
}

/// Parse and index every `.torrent` in a torrent_dir, then prune removed files.
pub fn index_torrent_dir(
    database: &Database,
    torrent_dir: &Path,
) -> crate::Result<TorrentDirIndexResult> {
    database.begin_torrent_dir_refresh()?;

    let mut result = TorrentDirIndexResult {
        files_seen: 0,
        torrents_indexed: 0,
        torrents_removed: 0,
        files_failed: 0,
    };
    let entries = match fs::read_dir(torrent_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            result.torrents_removed =
                database.clear_table(crate::persistence::CacheTable::Torrent)?;
            return Ok(result);
        }
        Err(error) => {
            return Err(search_error(format!(
                "failed to read torrent_dir {}: {error}",
                torrent_dir.display()
            )));
        }
    };

    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                tracing::debug!("skipping torrent_dir entry: {error}");
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
        database.mark_refreshed_torrent_path(&file_path)?;

        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) => {
                tracing::debug!(
                    "failed to read torrent_dir file {}: {error}",
                    path.display()
                );
                database.delete_torrent_path(&file_path)?;
                result.files_failed += 1;
                continue;
            }
        };
        match parse_metafile(&bytes) {
            Ok(metafile) => {
                database.upsert_torrent_path(
                    metafile.info_hash.as_str(),
                    metafile.name.as_ref(),
                    &file_path,
                )?;
                result.torrents_indexed += 1;
            }
            Err(error) => {
                tracing::debug!(
                    "failed to parse torrent_dir file {}: {error}",
                    path.display()
                );
                database.delete_torrent_path(&file_path)?;
                result.files_failed += 1;
            }
        }
    }

    result.torrents_removed = database.finish_torrent_dir_refresh()?;
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
    let mut roots = Vec::new();
    for_each_potential_nested_root(root, max_depth, |root| {
        roots.push(root.to_path_buf());
        Ok(())
    })?;
    Ok(roots)
}

/// Stream candidate release roots below a data-dir without retaining the tree.
pub fn for_each_potential_nested_root<F>(
    root: &Path,
    max_depth: u32,
    mut handle: F,
) -> crate::Result<()>
where
    F: FnMut(&Path) -> crate::Result<()>,
{
    visit_potential_nested_roots(root, max_depth, &mut handle)
}

fn visit_potential_nested_roots<F>(
    root: &Path,
    remaining_depth: u32,
    handle: &mut F,
) -> crate::Result<()>
where
    F: FnMut(&Path) -> crate::Result<()>,
{
    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(error) => {
            tracing::debug!("skipping data-dir entry {}: {error}", root.display());
            return Ok(());
        }
    };
    let mut has_video = false;
    let mut child_dirs = Vec::new();
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                tracing::debug!("skipping data-dir entry: {error}");
                continue;
            }
        };
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(error) => {
                tracing::debug!("skipping data-dir metadata: {error}");
                continue;
            }
        };
        if file_type.is_dir() {
            if remaining_depth > 0 && !ignored_directory_name(&entry.file_name()) {
                child_dirs.push(entry.path());
            }
            continue;
        }
        if file_type.is_file() && path_has_extension(&entry.path(), VIDEO_EXTENSIONS) {
            has_video = true;
        }
    }
    child_dirs.sort();
    for child in child_dirs {
        visit_potential_nested_roots(&child, remaining_depth - 1, handle)?;
    }
    if has_video {
        handle(root)?;
    }
    Ok(())
}

/// Recompute affected parent roots for a changed path up to `max_data_depth`.
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
    for_each_data_dir_searchee(data_dirs, max_depth, |searchee| {
        searchees.push(searchee);
        Ok(())
    })?;
    Ok(searchees)
}

/// Discover and handle data-dir searchees one at a time.
pub fn for_each_data_dir_searchee<F>(
    data_dirs: &[PathBuf],
    max_depth: u32,
    mut handle: F,
) -> crate::Result<usize>
where
    F: FnMut(Searchee<'static>) -> crate::Result<()>,
{
    let mut seen = 0usize;
    for data_dir in data_dirs {
        for_each_potential_nested_root(data_dir, max_depth, |root| {
            if let Some(searchee) = create_searchee_from_path(root)? {
                handle(searchee)?;
                seen = seen.saturating_add(1);
            }
            Ok(())
        })?;
    }
    Ok(seen)
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

fn searchee_file_path(searchee: &Searchee<'_>, file: &File<'_>) -> String {
    let path = Path::new(file.path.as_ref());
    if path.is_absolute() {
        return path.to_string_lossy().into_owned();
    }
    if let Some(client) = &searchee.client {
        return Path::new(client.save_path.as_ref())
            .join(path)
            .to_string_lossy()
            .into_owned();
    }
    if let Some(source) = &searchee.path {
        let source = Path::new(source.as_ref());
        let root = if source.is_dir() {
            source
        } else {
            source.parent().unwrap_or(source)
        };
        return root.join(path).to_string_lossy().into_owned();
    }
    file.path.to_string()
}

fn extension_in(file: &File<'_>, extensions: &[&str]) -> bool {
    extension(file)
        .as_deref()
        .is_some_and(|extension| extensions.contains(&extension))
}

fn is_video_file(file: &File<'_>) -> bool {
    extension_in(file, VIDEO_EXTENSIONS) || extension_in(file, VIDEO_DISC_EXTENSIONS)
}

fn non_video_ratio(searchee: &Searchee<'_>) -> f64 {
    if searchee.length == 0 {
        return 0.0;
    }
    let non_video = searchee
        .files
        .iter()
        .filter(|file| !is_video_file(file))
        .map(|file| file.length)
        .sum::<u64>();
    non_video as f64 / searchee.length as f64
}

fn data_dir_single_episode_in_season_pack(searchee: &Searchee<'_>) -> bool {
    searchee.source() == crate::domain::SearcheeSource::DataDir
        && searchee.files.len() == 1
        && searchee.media_type == MediaType::Episode
        && searchee
            .path
            .as_ref()
            .is_some_and(|path| SEASON_REGEX.is_match(path.as_ref()))
}

fn is_cross_seed(searchee: &Searchee<'_>, link_category: Option<&str>) -> bool {
    let Some(client) = &searchee.client else {
        return false;
    };
    client.category.as_ref().is_some_and(|category| {
        label_is_cross_seed(category)
            || link_category
                .is_some_and(|link_category| category.as_str().eq_ignore_ascii_case(link_category))
    }) || client.tags.iter().any(label_is_cross_seed)
}

fn label_is_cross_seed(label: &ClientLabel<'_>) -> bool {
    let value = label.as_str();
    value.eq_ignore_ascii_case("cross-seed")
        || value
            .to_ascii_lowercase()
            .strip_suffix(".cross-seed")
            .is_some()
}

fn looks_like_arr_root(searchee: &Searchee<'_>) -> bool {
    if searchee.source() != crate::domain::SearcheeSource::DataDir {
        return false;
    }
    let Some(path) = &searchee.path else {
        return false;
    };
    let name = Path::new(path.as_ref())
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .unwrap_or(path.as_ref());
    !name.chars().any(|character| character.is_ascii_digit())
        && searchee.media_type == MediaType::Video
        && searchee
            .files
            .iter()
            .filter(|file| is_video_file(file))
            .count()
            > 3
}

fn is_specials(searchee: &Searchee<'_>) -> bool {
    let haystack = format!(
        "{} {} {}",
        searchee.name,
        searchee.title,
        searchee.path.as_deref().unwrap_or_default()
    )
    .to_ascii_lowercase();
    haystack.contains("specials")
        || haystack.contains("season 0")
        || haystack.contains("season.0")
        || haystack.contains("s00")
}

fn non_standard_naming(searchee: &Searchee<'_>) -> bool {
    matches!(searchee.media_type, MediaType::Episode | MediaType::Pack)
        && !EPISODE_REGEX.is_match(searchee.title.as_ref())
        && !SEASON_REGEX.is_match(searchee.title.as_ref())
}

fn duplicate_searchee(left: &Searchee<'_>, right: &Searchee<'_>) -> bool {
    left.title == right.title
        && left.length == right.length
        && left.files.len() == right.files.len()
        && client_host(left) == client_host(right)
        && sorted_lengths(left) == sorted_lengths(right)
}

fn client_host<'a>(searchee: &'a Searchee<'_>) -> Option<&'a str> {
    searchee.client.as_ref().map(|client| client.host.as_ref())
}

fn sorted_lengths(searchee: &Searchee<'_>) -> Vec<u64> {
    let mut lengths = searchee
        .files
        .iter()
        .map(|file| file.length)
        .collect::<Vec<_>>();
    lengths.sort_unstable();
    lengths
}

fn normalized_query_key(title: &str) -> String {
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

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
struct SeasonKey {
    title: String,
    season: u32,
}

#[derive(Debug, Clone)]
struct EpisodeChoice {
    file: File<'static>,
    length: u64,
    mtime_millis: Option<u64>,
    client_host: Option<String>,
}

fn episode_key_from_title(title: &str) -> Option<(SeasonKey, u32)> {
    let captures = episode_match(title)?;
    let title = clean_title(captures.name("title")?.as_str());
    let season = capture_u32(&captures, "season")?;
    let episode = capture_u32(&captures, "episode")?;
    Some((SeasonKey { title, season }, episode))
}

fn season_key_from_title(title: &str) -> Option<SeasonKey> {
    let captures = SEASON_REGEX.captures(title)?;
    let matched = captures.get(0)?;
    let title = clean_title(title.get(..matched.start())?);
    let season = season_number(matched.as_str())?;
    Some(SeasonKey { title, season })
}

fn choose_virtual_client_host<'a>(
    episodes: impl Iterator<Item = &'a EpisodeChoice>,
) -> Option<String> {
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for episode in episodes {
        if let Some(host) = &episode.client_host {
            *counts.entry(host.clone()).or_default() += 1;
        }
    }
    counts
        .into_iter()
        .max_by(|left, right| left.1.cmp(&right.1).then_with(|| right.0.cmp(&left.0)))
        .map(|(host, _)| host)
}

fn contains_ignore_case(haystack: &str, needle_lower: &str) -> bool {
    haystack.to_ascii_lowercase().contains(needle_lower)
}

fn eq_ignore_case(left: &str, right_lower: &str) -> bool {
    left.eq_ignore_ascii_case(right_lower)
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
    ignored_directory_name(entry.file_name())
}

fn ignored_directory_name(name: &std::ffi::OsStr) -> bool {
    let name = name.to_string_lossy().to_ascii_lowercase();
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

fn torrent_file_searchee(path: &Path, label: Label) -> crate::Result<Option<Searchee<'static>>> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) => {
            tracing::debug!("skipping torrent file {}: {error}", path.display());
            return Ok(None);
        }
    };
    let metafile = match parse_metafile(&bytes) {
        Ok(metafile) => metafile,
        Err(error) => {
            tracing::debug!("skipping invalid torrent file {}: {error}", path.display());
            return Ok(None);
        }
    };
    let mut searchee = Searchee::from_files(
        metafile.name.into_owned(),
        metafile.title.into_owned(),
        metafile.files,
    );
    searchee.info_hash = Some(metafile.info_hash);
    searchee.media_type = metafile.media_type;
    searchee.label = Some(label);
    Ok(Some(searchee))
}

struct CandidateSearchRequest<'a> {
    database: &'a Database,
    indexer: &'a SearchIndexer,
    searchee: &'a Searchee<'a>,
    arr_ids: Option<&'a TorznabSearchIds>,
    group_key: &'a str,
    ids_key: &'a str,
    options: &'a SearchPipelineOptions<'a>,
}

struct CandidateSearchResult {
    candidates: Vec<Candidate<'static>>,
    update_timestamp: bool,
}

fn cached_or_search_candidates(
    request: CandidateSearchRequest<'_>,
    cache: &mut CandidateSearchCache,
    summary: &mut PipelineSummary,
) -> crate::Result<CandidateSearchResult> {
    let cache_key = (request.group_key.to_owned(), request.indexer.id);
    if let Some(cached) = cache.entries.get(&cache_key) {
        if cached.ids_key == request.ids_key {
            return Ok(CandidateSearchResult {
                candidates: cached.candidates.clone(),
                update_timestamp: true,
            });
        }
    }

    let mut torznab_options = request.options.torznab;
    if let Some(limit) = request.options.torznab.search_limit {
        let remaining = cache.remaining_search_limit(request.indexer.id, limit);
        if remaining == 0 {
            return Ok(CandidateSearchResult {
                candidates: Vec::new(),
                update_timestamp: false,
            });
        }
        torznab_options.search_limit = Some(remaining);
    }

    let indexer_ids = request
        .arr_ids
        .map(|ids| ids_for_torznab_caps(ids, &request.indexer.caps));
    let queries = create_torznab_search_queries(
        request.searchee,
        &request.indexer.caps,
        indexer_ids.as_ref(),
    );
    if queries.is_empty() {
        cache.entries.insert(
            cache_key,
            CachedCandidates {
                ids_key: request.ids_key.to_owned(),
                candidates: Vec::new(),
            },
        );
        return Ok(CandidateSearchResult {
            candidates: Vec::new(),
            update_timestamp: true,
        });
    }

    cache.wait_for_indexer_delay(request.options.torznab.delay)?;
    let candidates =
        search_torznab_indexer(request.database, request.indexer, &queries, torznab_options);
    cache.mark_indexer_search();
    let candidates = candidates?;
    cache.record_indexer_candidates(request.indexer.id, candidates.len());
    summary.indexer_searches += 1;
    cache.entries.insert(
        cache_key,
        CachedCandidates {
            ids_key: request.ids_key.to_owned(),
            candidates: candidates.clone(),
        },
    );
    Ok(CandidateSearchResult {
        candidates,
        update_timestamp: true,
    })
}

fn assess_and_dispatch<A>(
    database: &Database,
    app_dir: &Path,
    options: &SearchPipelineOptions<'_>,
    searchee: &Searchee<'_>,
    candidate: &Candidate<'_>,
    snatch_history: &mut SnatchHistory,
    action: &mut A,
) -> crate::Result<PipelineAttempt>
where
    A: FnMut(&PipelineAction<'_>) -> crate::Result<Option<ActionResult>>,
{
    let context = CandidateAssessmentContext {
        database,
        app_dir,
        options: &options.assessment,
        snatch_options: options.snatch,
        now_millis: options.torznab.now_millis as i64,
    };
    let assessment = assess_candidate(&context, candidate, searchee, snatch_history)?;
    let action_result = if assessment.decision.is_match() {
        action(&PipelineAction {
            label: options.label,
            searchee,
            candidate,
            assessment: &assessment,
        })?
    } else {
        None
    };
    Ok(PipelineAttempt {
        label: options.label,
        searchee_title: searchee.title.to_string(),
        candidate_name: candidate.name.to_string(),
        candidate_guid: candidate.guid.to_string(),
        candidate_info_hashes: assessment
            .metafile
            .as_ref()
            .map(|metafile| vec![metafile.info_hash.to_string()])
            .unwrap_or_default(),
        trackers: notification_trackers(candidate, assessment.metafile.as_ref()),
        decision: assessment.decision,
        action_result,
        searchee_category: searchee
            .client
            .as_ref()
            .and_then(|client| client.category.as_ref())
            .map(ToString::to_string),
        searchee_tags: searchee
            .client
            .as_ref()
            .map(|client| client.tags.iter().map(ToString::to_string).collect())
            .unwrap_or_default(),
        searchee_trackers: searchee
            .client
            .as_ref()
            .map(|client| client.trackers.iter().map(ToString::to_string).collect())
            .unwrap_or_default(),
        searchee_length: searchee.length,
        searchee_client_host: searchee
            .client
            .as_ref()
            .map(|client| client.host.to_string()),
        searchee_info_hash: searchee.info_hash.as_ref().map(ToString::to_string),
        searchee_path: searchee.path.as_ref().map(ToString::to_string),
        searchee_source_type: searchee.source().as_str().to_owned(),
    })
}

fn notification_trackers(
    candidate: &Candidate<'_>,
    metafile: Option<&crate::domain::Metafile<'_>>,
) -> Vec<String> {
    let trackers = metafile
        .map(|metafile| {
            metafile
                .trackers
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if trackers.is_empty() {
        vec![candidate.tracker.to_string()]
    } else {
        trackers
    }
}

fn read_timestamp(
    database: &Database,
    searchee_id: i64,
    indexer_id: i64,
) -> crate::Result<Option<TimestampDecision>> {
    Ok(database
        .read_timestamp(searchee_id, indexer_id)?
        .map(|record| TimestampDecision {
            first_searched: record.first_searched,
            last_searched: record.last_searched,
        }))
}

fn update_timestamp(
    database: &Database,
    searchee_id: i64,
    indexer_id: i64,
    now_millis: u64,
) -> crate::Result<()> {
    database.update_timestamp(searchee_id, indexer_id, now_millis)
}

fn reverse_lookup_keys(name: &str) -> Vec<String> {
    let mut keys = vec![normalized_query_key(
        parse_title(name, &[], None)
            .map(|parsed| parsed.title)
            .unwrap_or_else(|| name.to_owned())
            .as_str(),
    )];
    let stripped = strip_bracketed_metadata(name);
    let stripped_key = normalized_query_key(&stripped);
    if !stripped_key.is_empty() && !keys.iter().any(|key| key == &stripped_key) {
        keys.push(stripped_key);
    }
    keys
}

fn reverse_lookup_client_rows(
    database: &Database,
    candidate: &Candidate<'_>,
    keys: &[String],
    filter: &ContentFilterOptions<'_>,
) -> crate::Result<Vec<Searchee<'static>>> {
    let mut output = Vec::new();
    let criteria = reverse_lookup_criteria(candidate, keys, filter);
    let mut after_rowid = 0_i64;
    loop {
        let rows = database.reverse_lookup_client_page(
            &criteria,
            after_rowid,
            REVERSE_LOOKUP_PAGE_SIZE,
        )?;
        let Some(last) = rows.last() else {
            break;
        };
        after_rowid = last.rowid;
        for row in rows {
            if !reverse_keys_match(keys, &row.title) {
                continue;
            }
            if let Some(searchee) =
                client_searchee_by_hash(database, &row.client_host, &row.info_hash)?
            {
                if filter_by_content(&searchee, filter).is_none() {
                    output.push(searchee);
                }
            }
        }
    }
    Ok(output)
}

fn reverse_lookup_criteria<'a>(
    candidate: &Candidate<'_>,
    keys: &'a [String],
    filter: &ContentFilterOptions<'_>,
) -> ReverseLookupCriteria<'a> {
    let parsed_title = parse_title(candidate.name.as_ref(), &[], None);
    let title = parsed_title
        .as_ref()
        .map(|parsed| parsed.title.as_str())
        .unwrap_or(candidate.name.as_ref());
    let (season, episode) = if let Some((season_key, episode)) = episode_key_from_title(title) {
        (Some(season_key.season), Some(episode))
    } else if let Some(season_key) = season_key_from_title(title) {
        (Some(season_key.season), None)
    } else {
        (None, None)
    };
    let (min_length, max_length) = candidate.size.map_or((None, None), |size| {
        let factor = filter.fuzzy_size_threshold;
        let min = saturated_f64_length((size as f64 / (1.0 + factor)).floor());
        let max = if factor >= 1.0 {
            u64::MAX
        } else {
            saturated_f64_length((size as f64 / (1.0 - factor)).ceil())
        };
        (Some(min), Some(max))
    });

    ReverseLookupCriteria {
        search_keys: keys,
        media_type: parsed_title
            .as_ref()
            .map(|parsed| parsed.media_type.as_str()),
        season,
        episode,
        min_length,
        max_length,
    }
}

#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "reverse lookup length bounds are range-checked before saturating to u64"
)]
fn saturated_f64_length(value: f64) -> u64 {
    if !value.is_finite() || value <= 0.0 {
        0
    } else if value >= u64::MAX as f64 {
        u64::MAX
    } else {
        value as u64
    }
}

fn reverse_lookup_data_rows(
    database: &Database,
    keys: &[String],
    filter: &ContentFilterOptions<'_>,
) -> crate::Result<Vec<Searchee<'static>>> {
    let mut output = Vec::new();
    for row in database.data_rows()? {
        if !reverse_keys_match(keys, &row.title) {
            continue;
        }
        match create_searchee_from_path(Path::new(&row.path)) {
            Ok(Some(searchee)) if filter_by_content(&searchee, filter).is_none() => {
                output.push(searchee);
            }
            Ok(_) => {}
            Err(error) => {
                tracing::debug!(
                    "skipping stale reverse lookup data path {}: {error}",
                    row.path
                )
            }
        }
    }
    Ok(output)
}

fn reverse_lookup_ensemble_rows(
    database: &Database,
    candidate: &Candidate<'_>,
    filter: &ContentFilterOptions<'_>,
    virtual_options: Option<VirtualSeasonOptions>,
) -> crate::Result<Vec<Searchee<'static>>> {
    let mut output = Vec::new();
    if let Some((ensemble, element)) = candidate_episode_key(candidate.name.as_ref()) {
        output.extend(ensemble_rows(database, &ensemble, Some(&element), filter)?);
    }
    if let (Some(ensemble), Some(options)) = (
        candidate_season_key(candidate.name.as_ref()),
        virtual_options.map(|mut options| {
            options.use_filters = false;
            options
        }),
    ) {
        let episodes = ensemble_rows(database, &ensemble, None, filter)?;
        output.extend(create_virtual_season_searchees(&episodes, options));
    }
    Ok(output)
}

fn ensemble_rows(
    database: &Database,
    ensemble: &str,
    element: Option<&str>,
    filter: &ContentFilterOptions<'_>,
) -> crate::Result<Vec<Searchee<'static>>> {
    let mut output = Vec::new();
    for row in database.ensemble_rows(ensemble, element)? {
        if let Some(searchee) = EnsembleCacheRow::from(row).into_searchee(database)? {
            if filter_by_content(&searchee, filter).is_none() {
                output.push(searchee);
            }
        }
    }
    Ok(output)
}

fn reverse_keys_match(keys: &[String], title: &str) -> bool {
    let mut searchee = Searchee::from_files(title.to_owned(), title.to_owned(), Vec::new());
    searchee.media_type = get_media_type(title, &[]);
    let local_key = search_group_key(&searchee);
    keys.iter().any(|key| fuzzy_title_match(key, &local_key))
}

fn candidate_episode_key(name: &str) -> Option<(String, String)> {
    let title = parse_title(name, &[], None)
        .map(|parsed| parsed.title)
        .unwrap_or_else(|| name.to_owned());
    let (key, episode) = episode_key_from_title(&title)?;
    Some((
        format!("{} S{:02}", key.title, key.season),
        format!("{episode:02}"),
    ))
}

fn candidate_season_key(name: &str) -> Option<String> {
    let title = parse_title(name, &[], None)
        .map(|parsed| parsed.title)
        .unwrap_or_else(|| name.to_owned());
    let key = season_key_from_title(&title)?;
    Some(format!("{} S{:02}", key.title, key.season))
}

#[derive(Debug)]
struct ClientSearcheeCacheRow {
    client_host: String,
    info_hash: String,
    name: String,
    title: String,
    files: String,
    save_path: String,
    category: Option<String>,
    tags: Option<String>,
    trackers: String,
}

impl From<ClientSearcheeCacheRecord> for ClientSearcheeCacheRow {
    fn from(record: ClientSearcheeCacheRecord) -> Self {
        Self {
            client_host: record.client_host,
            info_hash: record.info_hash,
            name: record.name,
            title: record.title,
            files: record.files,
            save_path: record.save_path,
            category: record.category,
            tags: record.tags,
            trackers: record.trackers,
        }
    }
}

impl ClientSearcheeCacheRow {
    fn into_searchee(self) -> crate::Result<Option<Searchee<'static>>> {
        let Some(info_hash) = InfoHash::new(self.info_hash) else {
            return Ok(None);
        };
        let files = files_from_json(&self.files)?;
        let tags = labels_from_json(self.tags.as_deref())?;
        let trackers = strings_from_json(&self.trackers)?;
        let category = self.category.map(ClientLabel::new);
        let mut searchee = Searchee::from_files(self.name, self.title, files);
        searchee.media_type = get_media_type(searchee.title.as_ref(), &searchee.files);
        searchee.info_hash = Some(info_hash);
        searchee.client = Some(ClientTorrentMetadata::new(
            self.client_host,
            self.save_path,
            category,
            tags,
            trackers,
        ));
        Ok(Some(searchee))
    }
}

#[derive(Debug)]
struct EnsembleCacheRow {
    client_host: Option<String>,
    path: String,
    info_hash: Option<String>,
}

impl From<EnsembleCacheRecord> for EnsembleCacheRow {
    fn from(record: EnsembleCacheRecord) -> Self {
        Self {
            client_host: record.client_host,
            path: record.path,
            info_hash: record.info_hash,
        }
    }
}

impl EnsembleCacheRow {
    fn into_searchee(self, database: &Database) -> crate::Result<Option<Searchee<'static>>> {
        if let (Some(client_host), Some(info_hash)) = (self.client_host, self.info_hash) {
            return client_searchee_by_hash(database, &client_host, &info_hash);
        }
        match create_searchee_from_path(Path::new(&self.path)) {
            Ok(searchee) => Ok(searchee),
            Err(error) => {
                tracing::debug!(
                    "skipping stale reverse lookup ensemble path {}: {error}",
                    self.path
                );
                Ok(None)
            }
        }
    }
}

fn client_searchee_by_hash(
    database: &Database,
    client_host: &str,
    info_hash: &str,
) -> crate::Result<Option<Searchee<'static>>> {
    database
        .client_searchee_by_hash(client_host, info_hash)?
        .map(ClientSearcheeCacheRow::from)
        .map(ClientSearcheeCacheRow::into_searchee)
        .transpose()
        .map(Option::flatten)
}

#[derive(Deserialize)]
struct StoredFile {
    name: String,
    path: String,
    length: u64,
}

fn files_from_json(value: &str) -> crate::Result<Vec<File<'static>>> {
    serde_json::from_str::<Vec<StoredFile>>(value)
        .map_err(|error| search_error(format!("failed to parse cached files JSON: {error}")))
        .map(|files| {
            files
                .into_iter()
                .map(|file| File::with_name(file.name, file.path, file.length))
                .collect()
        })
}

fn labels_from_json(value: Option<&str>) -> crate::Result<Vec<ClientLabel<'static>>> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    serde_json::from_str::<Vec<String>>(value)
        .map_err(|error| search_error(format!("failed to parse cached labels JSON: {error}")))
        .map(|labels| labels.into_iter().map(ClientLabel::new).collect())
}

fn strings_from_json(value: &str) -> crate::Result<Vec<Cow<'static, str>>> {
    serde_json::from_str::<Vec<String>>(value)
        .map_err(|error| search_error(format!("failed to parse cached strings JSON: {error}")))
        .map(|values| values.into_iter().map(Cow::Owned).collect())
}

fn strip_bracketed_metadata(name: &str) -> String {
    let mut output = String::with_capacity(name.len());
    let mut depth = 0_u32;
    for character in name.chars() {
        match character {
            '(' | '[' => depth = depth.saturating_add(1),
            ')' | ']' => depth = depth.saturating_sub(1),
            _ if depth == 0 => output.push(character),
            _ => {}
        }
    }
    output
}

fn fuzzy_title_match(candidate_key: &str, local_key: &str) -> bool {
    if candidate_key == local_key {
        return true;
    }
    let max_distance = candidate_key.len().max(local_key.len()) / 3;
    levenshtein_at_most(candidate_key, local_key, max_distance)
        .is_some_and(|distance| distance <= candidate_key.len().min(local_key.len()) / 3)
}

fn levenshtein_at_most(left: &str, right: &str, max_distance: usize) -> Option<usize> {
    let left = left.chars().collect::<Vec<_>>();
    let right = right.chars().collect::<Vec<_>>();
    if left.len().abs_diff(right.len()) > max_distance {
        return None;
    }
    let mut previous = (0..=right.len()).collect::<Vec<_>>();
    let mut current = vec![0; right.len() + 1];
    for (left_index, left_char) in left.iter().enumerate() {
        *current.get_mut(0)? = left_index + 1;
        let mut row_min = *current.first()?;
        for (right_index, right_char) in right.iter().enumerate() {
            let cost = usize::from(left_char != right_char);
            let insert = previous.get(right_index + 1)?.saturating_add(1);
            let delete = current.get(right_index)?.saturating_add(1);
            let replace = previous.get(right_index)?.saturating_add(cost);
            let cell = insert.min(delete).min(replace);
            *current.get_mut(right_index + 1)? = cell;
            row_min = row_min.min(cell);
        }
        if row_min > max_distance {
            return None;
        }
        std::mem::swap(&mut previous, &mut current);
    }
    previous
        .last()
        .copied()
        .filter(|distance| *distance <= max_distance)
}

fn sort_reverse_lookup_searchees(searchees: &mut [Searchee<'static>]) {
    searchees.sort_by(|left, right| {
        source_priority(left)
            .cmp(&source_priority(right))
            .then_with(|| right.files.len().cmp(&left.files.len()))
            .then_with(|| left.title.cmp(&right.title))
    });
}

fn source_priority(searchee: &Searchee<'_>) -> u8 {
    match searchee.source() {
        crate::domain::SearcheeSource::TorrentClient => 0,
        crate::domain::SearcheeSource::TorrentFile => 1,
        crate::domain::SearcheeSource::DataDir => 2,
        crate::domain::SearcheeSource::Virtual => 3,
    }
}

fn best_failure(attempt: &PipelineAttempt, current: Option<&PipelineAttempt>) -> bool {
    let rank = failure_rank(attempt.decision);
    current.is_none_or(|current| rank < failure_rank(current.decision))
}

fn failure_rank(decision: crate::domain::Decision) -> u8 {
    match decision {
        crate::domain::Decision::RateLimited => 0,
        crate::domain::Decision::DownloadFailed | crate::domain::Decision::MagnetLink => 1,
        crate::domain::Decision::NoDownloadLink => 2,
        crate::domain::Decision::FuzzySizeMismatch
        | crate::domain::Decision::SizeMismatch
        | crate::domain::Decision::PartialSizeMismatch
        | crate::domain::Decision::FileTreeMismatch => 3,
        crate::domain::Decision::ReleaseGroupMismatch
        | crate::domain::Decision::ProperRepackMismatch
        | crate::domain::Decision::ResolutionMismatch
        | crate::domain::Decision::SourceMismatch => 4,
        crate::domain::Decision::BlockedRelease => 5,
        crate::domain::Decision::SameInfoHash
        | crate::domain::Decision::InfoHashAlreadyExists
        | crate::domain::Decision::Match
        | crate::domain::Decision::MatchSizeOnly
        | crate::domain::Decision::MatchPartial => 6,
    }
}

fn search_error(message: impl Into<Cow<'static, str>>) -> SporosError {
    SporosError::Search {
        message: message.into(),
    }
}

fn block_on_delay(delay: Duration) -> crate::Result<()> {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| search_error(format!("failed to build delay runtime: {error}")))?;
    runtime.block_on(async {
        tokio::time::sleep(delay).await;
    });
    Ok(())
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

/// Build persisted scalar facts for reverse lookup selectors.
pub fn lookup_fields(searchee: &Searchee<'_>) -> LookupFields {
    let (season, episode) =
        if let Some((season_key, episode)) = episode_key_from_title(searchee.title.as_ref()) {
            (Some(season_key.season), Some(episode))
        } else if let Some(season_key) = season_key_from_title(searchee.title.as_ref()) {
            (Some(season_key.season), None)
        } else {
            (None, None)
        };
    let video_bytes = searchee
        .files
        .iter()
        .filter(|file| is_video_file(file))
        .map(|file| file.length)
        .sum::<u64>();
    let non_video_bytes = searchee.length.saturating_sub(video_bytes);

    LookupFields {
        search_key: normalized_query_key(searchee.title.as_ref()),
        media_type: searchee.media_type,
        season,
        episode,
        length: searchee.length,
        file_count: searchee.files.len(),
        video_bytes,
        non_video_bytes,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Blocklist, CachedCandidates, CandidateSearchCache, ContentFilterOptions,
        ContentFilterRejection, MediaCapabilities, ReverseLookupRuntime, SearchPipelineOptions,
        SearchPipelineRuntime, SearcheeSources, TimestampDecision, VirtualSeasonOptions,
        affected_roots_for_changed_path, bulk_search, check_new_candidate_match,
        create_searchee_from_path, create_virtual_season_searchees, filter_by_content,
        filter_duplicate_searchees, find_all_searchees, find_potential_nested_roots,
        find_searchable_searchees, get_media_type, index_torrent_dir, indexer_supports_media,
        lookup_fields, parse_title, reverse_lookup_searchees, search_group_key, timestamp_excludes,
    };
    use crate::{
        domain::{
            ActionResult, Candidate, ClientLabel, ClientTorrentMetadata, Decision, File, Label,
            MediaType, SaveResult, Searchee,
        },
        integrations::{SearchIndexer, SnatchOptions, TorznabCaps, TorznabSearchOptions},
        matching::AssessmentOptions,
        persistence::{ClientSearcheeRecord, Database, DecisionRecord, SqlValue},
    };
    use std::{
        borrow::Cow,
        collections::BTreeSet,
        fs,
        io::{Read, Write},
        net::TcpListener,
        path::PathBuf,
        sync::{Arc, Mutex},
        thread,
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
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
        fs::write(root.join("Show/Season 1/Show.S01E02.mkv"), b"video").expect("episode");
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
            .query_scalar("SELECT COUNT(*) FROM torrent", &[])
            .expect("count");
        assert_eq!(count, 2);

        fs::remove_file(second).expect("remove second");
        fs::write(&first, torrent_bytes("First.Changed", 30)).expect("change first");
        let result = index_torrent_dir(&database, &torrent_dir).expect("reindex");

        assert_eq!(result.files_seen, 1);
        assert_eq!(result.torrents_indexed, 1);
        assert_eq!(result.torrents_removed, 1);
        let names: String = database
            .query_scalar(
                "SELECT name FROM torrent WHERE file_path = ?1",
                &[SqlValue::Text(Cow::Owned(first.display().to_string()))],
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
            .query_scalar("SELECT COUNT(*) FROM torrent", &[])
            .expect("count");
        assert_eq!(count, 0);
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn source_selection_prefers_explicit_torrents_and_adds_data_dirs() {
        let root = temp_path("source-selection");
        let torrent_dir = root.join("torrents");
        let data_dir = root.join("data");
        let release = data_dir.join("Example Show");
        fs::create_dir_all(&torrent_dir).expect("torrent dir");
        fs::create_dir_all(&release).expect("release dir");
        let explicit = root.join("explicit.torrent");
        let ignored = torrent_dir.join("ignored.torrent");
        fs::write(&explicit, torrent_bytes("Explicit.Release", 10)).expect("explicit");
        fs::write(&ignored, torrent_bytes("Ignored.Release", 10)).expect("ignored");
        fs::write(release.join("Example.Show.S01E01.mkv"), b"video").expect("video");

        let searchees = find_all_searchees(
            &SearcheeSources {
                torrents: Some(std::slice::from_ref(&explicit)),
                use_client_torrents: false,
                client_searchees: &[],
                torrent_dir: Some(&torrent_dir),
                data_dirs: std::slice::from_ref(&data_dir),
                max_data_depth: 2,
            },
            Label::Webhook,
        )
        .expect("sources");

        assert!(searchees.iter().any(|item| item.name == "Explicit.Release"));
        assert!(!searchees.iter().any(|item| item.name == "Ignored.Release"));
        assert!(
            searchees
                .iter()
                .any(|item| item.source() == crate::domain::SearcheeSource::DataDir)
        );
        assert!(
            searchees
                .iter()
                .all(|item| item.label == Some(Label::Webhook))
        );
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn parses_typed_blocklist_entries() {
        let blocklist = Blocklist::parse(&[
            "name:bad.release".to_owned(),
            "name_regex:(?i)evil".to_owned(),
            "category:blocked".to_owned(),
            "tag:".to_owned(),
            "tracker:tracker.example".to_owned(),
            "folder_regex:/downloads".to_owned(),
            "info_hash:0123456789abcdef0123456789abcdef01234567".to_owned(),
            "size_below:20".to_owned(),
        ])
        .expect("blocklist");
        let mut searchee = Searchee::from_files("Good", "Good", vec![File::new("Good.mkv", 10)]);
        searchee.client = Some(ClientTorrentMetadata::new(
            "client",
            "/downloads",
            Some(ClientLabel::new("blocked")),
            Vec::new(),
            vec!["tracker.example".into()],
        ));
        searchee.path = Some("/downloads/Good".into());
        searchee.info_hash =
            crate::domain::InfoHash::new("0123456789abcdef0123456789abcdef01234567");

        assert!(blocklist.matches_searchee(&searchee));
    }

    #[test]
    fn rejects_legacy_blocklist_entries() {
        let error = Blocklist::parse(&["folderRegex:/downloads".to_owned()])
            .expect_err("legacy entry rejected");

        assert!(error.to_string().contains("invalid block_list entry type"));
    }

    #[test]
    fn validates_blocklist_size_pair() {
        let error = Blocklist::parse(&["size_below:20".to_owned(), "size_above:10".to_owned()])
            .expect_err("inverted size range rejected");

        assert!(error.to_string().contains("size_below <= size_above"));
    }

    #[test]
    fn content_filter_rejects_blocklisted_and_single_episode() {
        let blocklist = Blocklist::parse(&["name:blocked".to_owned()]).expect("blocklist");
        let options = filter_options(&blocklist);
        let mut blocked = Searchee::from_files(
            "Blocked.Release",
            "Blocked.Release",
            vec![File::new("Blocked.mkv", 10)],
        );
        blocked.media_type = MediaType::Video;

        assert_eq!(
            filter_by_content(&blocked, &options),
            Some(ContentFilterRejection::Blocklisted)
        );

        let empty = Blocklist::parse(&[]).expect("empty");
        let options = filter_options(&empty);
        let mut episode = Searchee::from_files(
            "Show.S01E02",
            "Show S01E02",
            vec![File::new("Show.S01E02.mkv", 10)],
        );
        episode.media_type = MediaType::Episode;

        assert_eq!(
            filter_by_content(&episode, &options),
            Some(ContentFilterRejection::SingleEpisode)
        );
    }

    #[test]
    fn announce_allows_single_episode_but_non_video_ratio_can_reject() {
        let empty = Blocklist::parse(&[]).expect("empty");
        let mut options = filter_options(&empty);
        options.label = Some(Label::Announce);
        let mut searchee = Searchee::from_files(
            "Show.S01E02",
            "Show S01E02",
            vec![File::new("Show.S01E02.mkv", 10), File::new("extra.nfo", 10)],
        );
        searchee.media_type = MediaType::Episode;

        assert_eq!(
            filter_by_content(&searchee, &options),
            Some(ContentFilterRejection::NonVideoRatio)
        );
    }

    #[test]
    fn content_filter_rejects_cross_seed_and_specials() {
        let empty = Blocklist::parse(&[]).expect("empty");
        let mut options = filter_options(&empty);
        options.ignore_cross_seeds = true;
        let mut searchee =
            Searchee::from_files("Release", "Release", vec![File::new("Release.mkv", 10)]);
        searchee.media_type = MediaType::Video;
        searchee.client = Some(ClientTorrentMetadata::new(
            "client",
            "/downloads",
            Some(ClientLabel::new("tv.cross-seed")),
            vec![ClientLabel::new("tag")],
            Vec::<Cow<'static, str>>::new(),
        ));

        assert_eq!(
            filter_by_content(&searchee, &options),
            Some(ContentFilterRejection::CrossSeed)
        );

        let mut specials = Searchee::from_files(
            "Show Specials",
            "Show Specials",
            vec![File::new("Show.S00E01.mkv", 10)],
        );
        specials.media_type = MediaType::Episode;
        let mut options = filter_options(&empty);
        options.include_single_episodes = true;
        assert_eq!(
            filter_by_content(&specials, &options),
            Some(ContentFilterRejection::Specials)
        );
    }

    fn filter_options<'a>(blocklist: &'a Blocklist) -> ContentFilterOptions<'a> {
        ContentFilterOptions {
            blocklist,
            blocklist_only: false,
            include_single_episodes: false,
            include_non_videos: false,
            fuzzy_size_threshold: 0.05,
            ignore_cross_seeds: false,
            link_category: None,
            label: Some(Label::Search),
        }
    }

    fn pipeline_options<'a>(
        blocklist: &'a Blocklist,
        exclude: &'a BTreeSet<String>,
        _root: &PathBuf,
        label: Label,
    ) -> SearchPipelineOptions<'a> {
        SearchPipelineOptions {
            label,
            filter: ContentFilterOptions {
                label: Some(label),
                ..filter_options(blocklist)
            },
            assessment: AssessmentOptions {
                match_mode: crate::config::MatchMode::Strict,
                fuzzy_size_threshold: 0.05,
                season_from_episodes: 1.0,
                include_single_episodes: true,
                info_hashes_to_exclude: exclude,
                blocklist,
            },
            snatch: SnatchOptions::default(),
            torznab: TorznabSearchOptions {
                now_millis: 1_000,
                ..TorznabSearchOptions::default()
            },
            arr_configs: &[],
            arr_timeout: None,
            virtual_season: None,
            exclude_older: None,
            exclude_recent_search: None,
        }
    }

    fn episode_searchee(episode: u32, mtime_millis: u64) -> Searchee<'static> {
        let title = format!("Example Show S01E{episode:02}");
        let mut searchee = Searchee::from_files(
            title.clone(),
            title,
            vec![File::new(format!("Example.Show.S01E{episode:02}.mkv"), 100)],
        );
        searchee.media_type = MediaType::Episode;
        searchee.mtime_millis = Some(mtime_millis);
        searchee
    }

    #[test]
    fn duplicate_filter_prefers_info_hash_sources() {
        let mut with_hash =
            Searchee::from_files("Release A", "Same Title", vec![File::new("a.mkv", 10)]);
        with_hash.info_hash =
            crate::domain::InfoHash::new("0123456789abcdef0123456789abcdef01234567");
        let duplicate =
            Searchee::from_files("Release B", "Same Title", vec![File::new("b.mkv", 10)]);

        let filtered = filter_duplicate_searchees(vec![duplicate, with_hash]);

        assert_eq!(filtered.len(), 1);
        assert!(filtered[0].info_hash.is_some());
    }

    #[test]
    fn timestamp_filter_honors_recent_old_and_virtual_freshness() {
        let timestamp = TimestampDecision {
            first_searched: 1_000,
            last_searched: 9_000,
        };

        assert!(timestamp_excludes(
            Some(timestamp),
            10_000,
            None,
            Some(2_000),
            None
        ));
        assert!(timestamp_excludes(
            Some(timestamp),
            10_000,
            Some(5_000),
            None,
            None
        ));
        assert!(!timestamp_excludes(
            Some(timestamp),
            10_000,
            Some(5_000),
            Some(2_000),
            Some(9_500)
        ));
        assert!(!timestamp_excludes(
            None,
            10_000,
            Some(5_000),
            Some(2_000),
            None
        ));
    }

    #[test]
    fn media_caps_and_group_key_are_stable() {
        let caps = MediaCapabilities {
            tv: true,
            ..MediaCapabilities::default()
        };
        assert!(indexer_supports_media(MediaType::Episode, caps));
        assert!(!indexer_supports_media(MediaType::Movie, caps));

        let mut searchee = Searchee::from_files(
            "Example.Show.S01E02.1080p",
            "Example Show S01E02",
            vec![File::new("Example.Show.S01E02.mkv", 10)],
        );
        searchee.media_type = MediaType::Episode;
        assert_eq!(search_group_key(&searchee), "example.show.s01.e02");
        let mut decorated = Searchee::from_files(
            "Example.Show.S01E02.1080p.WEB-DL",
            "Example Show S01E02 1080p WEB-DL",
            vec![File::new("Example.Show.S01E02.1080p.WEB-DL.mkv", 10)],
        );
        decorated.media_type = MediaType::Episode;
        assert_eq!(search_group_key(&decorated), search_group_key(&searchee));
    }

    #[test]
    fn searchable_pipeline_filters_virtuals_and_dispatches_cached_candidates() {
        let root = temp_path("bulk-pipeline");
        fs::create_dir_all(&root).expect("temp dir");
        let database = Database::open_app_dir(&root).expect("database");
        let blocklist = Blocklist::parse(&[]).expect("blocklist");
        let exclude = BTreeSet::new();
        let mut options = pipeline_options(&blocklist, &exclude, &root, Label::Search);
        options.filter.include_single_episodes = true;
        options.virtual_season = Some(VirtualSeasonOptions {
            season_from_episodes: 0.5,
            use_filters: true,
            now_millis: 1_000 + 9 * 24 * 60 * 60 * 1000,
        });

        let searchees = (1..=3)
            .map(|episode| episode_searchee(episode, 1_000))
            .collect::<Vec<_>>();
        let searchable =
            find_searchable_searchees(searchees, &[], 1, &options).expect("searchable");

        assert_eq!(searchable.len(), 4);
        assert!(
            searchable
                .iter()
                .any(|item| item.media_type == MediaType::Pack)
        );

        let target = searchable
            .iter()
            .find(|item| item.title.as_ref() == "Example Show S01E01")
            .expect("target");
        let searchee_id = database
            .get_or_insert_searchee(target.title.as_ref(), 1_000)
            .expect("searchee");
        database
            .upsert_decision(&DecisionRecord {
                searchee_id,
                guid: "guid-1",
                info_hash: None,
                decision: Decision::Match,
                first_seen: 1_000,
                last_seen: 1_000,
                fuzzy_size_factor: 0.05,
            })
            .expect("decision");

        let mut cache = CandidateSearchCache::default();
        cache.entries.insert(
            (search_group_key(target), 7),
            CachedCandidates {
                ids_key: search_group_key(target),
                candidates: vec![Candidate::new(
                    "Example.Show.S01E01",
                    "guid-1",
                    None::<String>,
                    "tracker",
                )],
            },
        );
        let indexer = SearchIndexer {
            id: 7,
            url: "https://indexer.example/api".to_owned(),
            apikey: "secret".to_owned(),
            caps: TorznabCaps {
                search: true,
                tv_search: true,
                ..TorznabCaps::default()
            },
        };
        database
            .execute_sql(
                "INSERT INTO indexer (id, url, apikey, active)
                 VALUES (?1, ?2, ?3, 1)",
                &[
                    SqlValue::I64(indexer.id),
                    SqlValue::Text(Cow::Borrowed(indexer.url.as_str())),
                    SqlValue::Text(Cow::Borrowed(indexer.apikey.as_str())),
                ],
            )
            .expect("indexer");
        let mut actions = 0;
        let mut notifications = 0;
        let mut runtime = SearchPipelineRuntime {
            database: &database,
            app_dir: &root,
            options: &options,
            cache: &mut cache,
        };
        let summary = bulk_search(
            &mut runtime,
            std::slice::from_ref(target),
            &[indexer],
            |action| {
                assert_eq!(action.label, Label::Search);
                assert_eq!(action.assessment.decision, Decision::Match);
                actions += 1;
                Ok(Some(ActionResult::Save(SaveResult::Saved)))
            },
            |_| {
                notifications += 1;
                Ok(())
            },
        )
        .expect("bulk search");

        assert_eq!(summary.indexer_searches, 0);
        assert_eq!(summary.candidates_assessed, 1);
        assert_eq!(
            summary.attempts[0].action_result,
            Some(ActionResult::Save(SaveResult::Saved))
        );
        assert_eq!(actions, 1);
        assert_eq!(notifications, 1);
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn bulk_search_waits_between_real_indexer_requests() {
        let server = http_server(vec![
            rss_response("<rss><channel></channel></rss>"),
            rss_response("<rss><channel></channel></rss>"),
        ]);
        let root = temp_path("bulk-search-delay");
        fs::create_dir_all(&root).expect("temp dir");
        let database = Database::open_app_dir(&root).expect("database");
        let blocklist = Blocklist::parse(&[]).expect("blocklist");
        let exclude = BTreeSet::new();
        let mut options = pipeline_options(&blocklist, &exclude, &root, Label::Search);
        options.filter.include_single_episodes = true;
        options.filter.include_non_videos = true;
        options.torznab.delay = Duration::from_millis(20);
        let indexer = SearchIndexer {
            id: 7,
            url: format!("{}/api", server.url),
            apikey: "secret".to_owned(),
            caps: TorznabCaps {
                tv_search: true,
                ..TorznabCaps::default()
            },
        };
        database
            .execute_sql(
                "INSERT INTO indexer (id, url, apikey, active)
                 VALUES (?1, ?2, ?3, 1)",
                &[
                    SqlValue::I64(indexer.id),
                    SqlValue::Text(Cow::Borrowed(indexer.url.as_str())),
                    SqlValue::Text(Cow::Borrowed(indexer.apikey.as_str())),
                ],
            )
            .expect("indexer");
        let searchees = vec![episode_searchee(1, 1_000), episode_searchee(2, 1_000)];
        let mut cache = CandidateSearchCache::default();
        let mut runtime = SearchPipelineRuntime {
            database: &database,
            app_dir: &root,
            options: &options,
            cache: &mut cache,
        };

        let summary = bulk_search(
            &mut runtime,
            &searchees,
            &[indexer],
            |_| Ok(None),
            |_| Ok(()),
        )
        .expect("bulk search");

        assert_eq!(summary.indexer_searches, 2);
        let requests = server.join();
        assert_eq!(requests.len(), 2);
        assert!(requests[0].raw.contains("/api?apikey=secret&t=tvsearch"));
        assert!(
            requests[1]
                .accepted_at
                .duration_since(requests[0].accepted_at)
                >= options.torznab.delay
        );
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn bulk_search_counts_search_limit_per_indexer_batch() {
        let server = http_server(vec![rss_response(
            r#"<rss><channel>
              <item><title>Example.Show.S01E01</title><guid>guid-1</guid><indexer>tracker</indexer></item>
            </channel></rss>"#,
        )]);
        let root = temp_path("bulk-search-limit");
        fs::create_dir_all(&root).expect("temp dir");
        let database = Database::open_app_dir(&root).expect("database");
        let blocklist = Blocklist::parse(&[]).expect("blocklist");
        let exclude = BTreeSet::new();
        let mut options = pipeline_options(&blocklist, &exclude, &root, Label::Search);
        options.filter.include_single_episodes = true;
        options.filter.include_non_videos = true;
        options.torznab.search_limit = Some(1);
        let indexer = SearchIndexer {
            id: 7,
            url: format!("{}/api", server.url),
            apikey: "secret".to_owned(),
            caps: TorznabCaps {
                tv_search: true,
                ..TorznabCaps::default()
            },
        };
        database
            .execute_sql(
                "INSERT INTO indexer (id, url, apikey, active)
                 VALUES (?1, ?2, ?3, 1)",
                &[
                    SqlValue::I64(indexer.id),
                    SqlValue::Text(Cow::Borrowed(indexer.url.as_str())),
                    SqlValue::Text(Cow::Borrowed(indexer.apikey.as_str())),
                ],
            )
            .expect("indexer");
        let searchees = vec![episode_searchee(1, 1_000), episode_searchee(2, 1_000)];
        let mut cache = CandidateSearchCache::default();
        let mut runtime = SearchPipelineRuntime {
            database: &database,
            app_dir: &root,
            options: &options,
            cache: &mut cache,
        };

        let summary = bulk_search(
            &mut runtime,
            &searchees,
            &[indexer],
            |_| Ok(None),
            |_| Ok(()),
        )
        .expect("bulk search");

        assert_eq!(summary.indexer_searches, 1);
        assert_eq!(summary.candidates_assessed, 1);
        let requests = server.join();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].raw.contains("/api?apikey=secret&t=tvsearch"));
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn reverse_lookup_filters_sorts_and_stops_after_success() {
        let root = temp_path("reverse-pipeline");
        fs::create_dir_all(&root).expect("temp dir");
        let database = Database::open_app_dir(&root).expect("database");
        let blocklist = Blocklist::parse(&[]).expect("blocklist");
        let exclude = BTreeSet::new();
        let mut options = pipeline_options(&blocklist, &exclude, &root, Label::Rss);
        options.filter.include_single_episodes = true;
        let candidate =
            Candidate::new("Example.Show.S01E01", "guid-rss", None::<String>, "tracker");
        let mut client = episode_searchee(1, 1_000);
        client.client = Some(ClientTorrentMetadata::new(
            "client-a",
            "/downloads",
            None,
            Vec::new(),
            Vec::<Cow<'static, str>>::new(),
        ));
        let unrelated = Searchee::from_files(
            "Other.Movie.2020",
            "Other Movie 2020",
            vec![File::new("movie.mkv", 1)],
        );
        let local = vec![unrelated, client];

        let matches = reverse_lookup_searchees(&candidate, &local, &options.filter);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].title, "Example Show S01E01");

        let searchee_id = database
            .get_or_insert_searchee(matches[0].title.as_ref(), 1_000)
            .expect("searchee");
        database
            .upsert_decision(&DecisionRecord {
                searchee_id,
                guid: "guid-rss",
                info_hash: None,
                decision: Decision::MatchSizeOnly,
                first_seen: 1_000,
                last_seen: 1_000,
                fuzzy_size_factor: 0.05,
            })
            .expect("decision");

        let gate = super::ReverseLookupGate::new();
        let mut actions = 0;
        let runtime = ReverseLookupRuntime {
            gate: &gate,
            database: &database,
            app_dir: &root,
            options: &options,
        };
        let attempt = check_new_candidate_match(
            &runtime,
            &candidate,
            &local,
            |_| {
                actions += 1;
                Ok(Some(ActionResult::Save(SaveResult::Saved)))
            },
            |_| Ok(()),
        )
        .expect("reverse lookup")
        .expect("attempt");

        assert_eq!(attempt.decision, Decision::MatchSizeOnly);
        assert_eq!(actions, 1);
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn reverse_lookup_gates_share_one_runtime_permit() {
        let first = super::ReverseLookupGate::new();
        let second = super::ReverseLookupGate::new();
        let _held = first.permit.lock().expect("first gate");

        assert!(second.permit.try_lock().is_err());
    }

    #[test]
    fn reverse_lookup_uses_cached_client_rows() {
        let root = temp_path("reverse-client-cache");
        fs::create_dir_all(&root).expect("temp dir");
        let database = Database::open_app_dir(&root).expect("database");
        let blocklist = Blocklist::parse(&[]).expect("blocklist");
        let exclude = BTreeSet::new();
        let mut options = pipeline_options(&blocklist, &exclude, &root, Label::Rss);
        options.filter.include_single_episodes = true;
        let files = [File::new("Example.Show.S01E01.mkv", 10)];
        let mut cached =
            Searchee::from_files("Example.Show.S01E01", "Example Show S01E01", files.to_vec());
        cached.media_type = MediaType::Episode;
        let lookup = lookup_fields(&cached);
        database
            .upsert_client_searchee(&ClientSearcheeRecord {
                client_host: "client-a",
                info_hash: "0123456789abcdef0123456789abcdef01234567",
                name: "Example.Show.S01E01",
                title: "Example Show S01E01",
                files: &files,
                length: 10,
                save_path: "/downloads",
                category: None,
                tags: &[],
                trackers: &[Cow::Borrowed("tracker.example")],
                lookup: Some(&lookup),
            })
            .expect("client searchee");
        database
            .execute_sql(
                "INSERT INTO client_searchee
                    (client_host, info_hash, name, title, files, length, save_path, trackers, search_key, media_type, season, episode, file_count, video_bytes, non_video_bytes)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
                &[
                    SqlValue::Text(Cow::Borrowed("client-a")),
                    SqlValue::Text(Cow::Borrowed("fedcba9876543210fedcba9876543210fedcba98")),
                    SqlValue::Text(Cow::Borrowed("Example.Show.S01E01.poison")),
                    SqlValue::Text(Cow::Borrowed("Example Show S01E01")),
                    SqlValue::Text(Cow::Borrowed("not-json")),
                    SqlValue::I64(10),
                    SqlValue::Text(Cow::Borrowed("/downloads")),
                    SqlValue::Text(Cow::Borrowed("[]")),
                    SqlValue::Text(Cow::Borrowed("other.show.s01e01")),
                    SqlValue::Text(Cow::Borrowed("episode")),
                    SqlValue::I64(1),
                    SqlValue::I64(1),
                    SqlValue::I64(1),
                    SqlValue::I64(10),
                    SqlValue::I64(0),
                ],
            )
            .expect("poison row");
        let searchee_id = database
            .get_or_insert_searchee("Example Show S01E01", 1_000)
            .expect("searchee");
        database
            .upsert_decision(&DecisionRecord {
                searchee_id,
                guid: "guid-db",
                info_hash: None,
                decision: Decision::MatchSizeOnly,
                first_seen: 1_000,
                last_seen: 1_000,
                fuzzy_size_factor: 0.05,
            })
            .expect("decision");
        let gate = super::ReverseLookupGate::new();
        let runtime = ReverseLookupRuntime {
            gate: &gate,
            database: &database,
            app_dir: &root,
            options: &options,
        };
        let candidate = Candidate::new("Example.Show.S01E01", "guid-db", None::<String>, "tracker");
        let mut actions = 0;

        let attempt = check_new_candidate_match(
            &runtime,
            &candidate,
            &[],
            |_| {
                actions += 1;
                Ok(Some(ActionResult::Save(SaveResult::Saved)))
            },
            |_| Ok(()),
        )
        .expect("reverse lookup")
        .expect("attempt");

        assert_eq!(attempt.decision, Decision::MatchSizeOnly);
        assert_eq!(attempt.searchee_client_host.as_deref(), Some("client-a"));
        assert_eq!(actions, 1);
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn builds_virtual_season_from_episode_searchees() {
        let episodes = (1..=3)
            .map(|episode| {
                let title = format!("Example Show S01E{episode:02}");
                let mut searchee = Searchee::from_files(
                    title.clone(),
                    title,
                    vec![File::new(format!("Example.Show.S01E{episode:02}.mkv"), 100)],
                );
                searchee.media_type = MediaType::Episode;
                searchee.mtime_millis = Some(1_000);
                searchee.client = Some(ClientTorrentMetadata::new(
                    "client-a",
                    "/downloads",
                    None,
                    Vec::new(),
                    Vec::<Cow<'static, str>>::new(),
                ));
                searchee
            })
            .collect::<Vec<_>>();

        let virtuals = create_virtual_season_searchees(
            &episodes,
            VirtualSeasonOptions {
                season_from_episodes: 0.5,
                use_filters: true,
                now_millis: 1_000 + 9 * 24 * 60 * 60 * 1000,
            },
        );

        assert_eq!(virtuals.len(), 1);
        assert_eq!(virtuals[0].title, "Example Show S01");
        assert_eq!(virtuals[0].media_type, MediaType::Pack);
        assert_eq!(virtuals[0].length, 300);
        assert_eq!(
            virtuals[0]
                .client
                .as_ref()
                .map(|client| client.host.as_ref()),
            Some("client-a")
        );
    }

    #[test]
    fn virtual_seasons_respect_existing_pack_ratio_and_age() {
        let mut pack = Searchee::from_files(
            "Example Show S01",
            "Example Show S01",
            vec![File::new("pack.mkv", 1)],
        );
        pack.media_type = MediaType::Pack;
        let mut episode = Searchee::from_files(
            "Example Show S01E01",
            "Example Show S01E01",
            vec![File::new("e1.mkv", 1)],
        );
        episode.media_type = MediaType::Episode;
        episode.mtime_millis = Some(1_000);

        assert!(
            create_virtual_season_searchees(
                &[pack, episode],
                VirtualSeasonOptions {
                    season_from_episodes: 0.5,
                    use_filters: true,
                    now_millis: 1_000 + 9 * 24 * 60 * 60 * 1000,
                },
            )
            .is_empty()
        );
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

    fn rss_response(body: &str) -> String {
        let mut response = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n", body.len());
        response.push_str("Content-Type: application/rss+xml\r\n\r\n");
        response.push_str(body);
        response
    }

    #[derive(Debug)]
    struct TestRequest {
        raw: String,
        accepted_at: Instant,
    }

    struct TestServer {
        url: String,
        requests: Arc<Mutex<Vec<TestRequest>>>,
        handle: thread::JoinHandle<()>,
    }

    impl TestServer {
        fn join(self) -> Vec<TestRequest> {
            self.handle.join().expect("server thread");
            Arc::try_unwrap(self.requests)
                .expect("requests still shared")
                .into_inner()
                .expect("requests lock")
        }
    }

    fn http_server(responses: Vec<String>) -> TestServer {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
        let url = format!("http://{}", listener.local_addr().expect("local addr"));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let server_requests = Arc::clone(&requests);
        let handle = thread::spawn(move || {
            for response in responses {
                let (mut stream, _) = listener.accept().expect("accept request");
                let mut buffer = [0; 4096];
                let read = stream.read(&mut buffer).expect("read request");
                server_requests
                    .lock()
                    .expect("requests lock")
                    .push(TestRequest {
                        raw: String::from_utf8_lossy(&buffer[..read]).into_owned(),
                        accepted_at: Instant::now(),
                    });
                stream
                    .write_all(response.as_bytes())
                    .expect("write response");
            }
        });
        TestServer {
            url,
            requests,
            handle,
        }
    }
}
