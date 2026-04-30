//! Candidate assessment, file-tree comparison, and decision mapping.

use std::{borrow::Cow, collections::BTreeSet, path::Path};

use rusqlite::{OptionalExtension, params};

use crate::{
    SporosError,
    config::MatchMode,
    domain::{
        Candidate, ClientTorrentMetadata, Decision, File, InfoHash, MediaType, Metafile, Searchee,
        SearcheeSource,
    },
    integrations::{
        SnatchHistory, SnatchOptions, SnatchResult, cache_torrent_file, get_cached_torrent,
        guid_lookup, snatch,
    },
    persistence::{Database, DecisionRecord},
    search::{Blocklist, parse_title},
};

/// Options controlling conservative candidate assessment.
#[derive(Debug)]
pub struct AssessmentOptions<'a> {
    /// Configured file-tree match mode.
    pub match_mode: MatchMode,
    /// Fuzzy size threshold.
    pub fuzzy_size_threshold: f64,
    /// Virtual season pack ratio used as the minimum match ratio.
    pub season_from_episodes: f64,
    /// Whether single-episode candidates can match season packs.
    pub include_single_episodes: bool,
    /// Known local or excluded info hashes.
    pub info_hashes_to_exclude: &'a BTreeSet<String>,
    /// Parsed blocklist rules.
    pub blocklist: &'a Blocklist,
}

/// Runtime dependencies and settings for assessing a remote candidate.
pub struct CandidateAssessmentContext<'a> {
    /// SQLite state.
    pub database: &'a Database,
    /// Application directory containing the torrent cache.
    pub app_dir: &'a Path,
    /// Conservative assessment options.
    pub options: &'a AssessmentOptions<'a>,
    /// Candidate snatch retry options.
    pub snatch_options: SnatchOptions,
    /// Current timestamp in milliseconds.
    pub now_millis: i64,
}

/// Result returned by candidate assessment.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Assessment {
    /// Conservative decision.
    pub decision: Decision,
    /// Parsed candidate metafile when one was available.
    pub metafile: Option<Metafile<'static>>,
    /// Whether the metafile came from the torrent cache.
    pub meta_cached: bool,
    /// Human-readable reason for verbose logs.
    pub reason: String,
}

impl Assessment {
    fn new(
        decision: Decision,
        metafile: Option<Metafile<'static>>,
        meta_cached: bool,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            decision,
            metafile,
            meta_cached,
            reason: reason.into(),
        }
    }
}

/// Assess a remote candidate, using cached decisions and torrent cache when possible.
pub fn assess_candidate(
    context: &CandidateAssessmentContext<'_>,
    candidate: &Candidate<'_>,
    searchee: &Searchee<'_>,
    history: &mut SnatchHistory,
) -> crate::Result<Assessment> {
    let database = context.database;
    let app_dir = context.app_dir;
    let options = context.options;
    let now_millis = context.now_millis;
    let fuzzy_size_factor = fuzzy_size_factor(searchee, options);
    let searchee_id = database.get_or_insert_searchee(searchee.title.as_ref(), now_millis)?;

    if let Some(cached) = cached_decision(database, searchee_id, candidate.guid.as_ref())? {
        if let Some(info_hash) = InfoHash::new(cached.info_hash.clone().unwrap_or_default()) {
            if let Some(metafile) = get_cached_torrent(app_dir, &info_hash)? {
                let assessment =
                    assess_metafile(&metafile, searchee, options, true, fuzzy_size_factor);
                if options.info_hashes_to_exclude.contains(info_hash.as_str()) {
                    return Ok(Assessment::new(
                        Decision::InfoHashAlreadyExists,
                        Some(metafile),
                        true,
                        "cached candidate info hash is already present locally",
                    ));
                }
                return Ok(assessment);
            }
        } else {
            return Ok(Assessment::new(
                cached.decision,
                None,
                false,
                "reused persisted candidate decision",
            ));
        }
    }

    if let Some(info_hash) =
        guid_lookup(database, candidate.guid.as_ref(), candidate.link.as_deref())?
            .and_then(InfoHash::new)
    {
        if let Some(metafile) = get_cached_torrent(app_dir, &info_hash)? {
            if options.info_hashes_to_exclude.contains(info_hash.as_str()) {
                let assessment = Assessment::new(
                    Decision::InfoHashAlreadyExists,
                    Some(metafile),
                    true,
                    "cached GUID/link info hash is already present locally",
                );
                persist_decision(
                    database,
                    searchee_id,
                    candidate,
                    &assessment,
                    now_millis,
                    fuzzy_size_factor,
                )?;
                return Ok(assessment);
            }
            let assessment = assess_metafile(&metafile, searchee, options, true, fuzzy_size_factor);
            persist_decision(
                database,
                searchee_id,
                candidate,
                &assessment,
                now_millis,
                fuzzy_size_factor,
            )?;
            return Ok(assessment);
        }
    }

    let predownload = predownload_rejection(candidate, searchee, fuzzy_size_factor);
    let assessment = if let Some(assessment) = predownload {
        assessment
    } else if options.blocklist.matches_searchee(searchee) {
        Assessment::new(
            Decision::BlockedRelease,
            None,
            false,
            "searchee matched blocklist",
        )
    } else {
        match snatch(candidate, context.snatch_options, history)? {
            SnatchResult::Metafile { bytes, .. } => {
                let metafile = cache_torrent_file(app_dir, &bytes)?;
                update_indexer_trackers(database, candidate.indexer_id, &metafile.trackers)?;
                assess_metafile(&metafile, searchee, options, false, fuzzy_size_factor)
            }
            SnatchResult::MagnetLink => Assessment::new(
                Decision::MagnetLink,
                None,
                false,
                "candidate download redirected to a magnet link",
            ),
            SnatchResult::RateLimited { .. } => Assessment::new(
                Decision::RateLimited,
                None,
                false,
                "candidate download hit a rate limit",
            ),
            SnatchResult::Aborted
            | SnatchResult::UnknownError { .. }
            | SnatchResult::InvalidContents => Assessment::new(
                Decision::DownloadFailed,
                None,
                false,
                "candidate torrent could not be downloaded and parsed",
            ),
        }
    };

    persist_decision(
        database,
        searchee_id,
        candidate,
        &assessment,
        now_millis,
        fuzzy_size_factor,
    )?;
    Ok(assessment)
}

/// Assess an already parsed candidate metafile.
pub fn assess_metafile(
    metafile: &Metafile<'_>,
    searchee: &Searchee<'_>,
    options: &AssessmentOptions<'_>,
    meta_cached: bool,
    fuzzy_size_factor: f64,
) -> Assessment {
    if searchee
        .info_hash
        .as_ref()
        .is_some_and(|info_hash| info_hash == &metafile.info_hash)
    {
        return Assessment::new(
            Decision::SameInfoHash,
            Some(metafile.clone().into_owned()),
            meta_cached,
            "candidate info hash matches searchee info hash",
        );
    }
    if options
        .info_hashes_to_exclude
        .contains(metafile.info_hash.as_str())
    {
        return Assessment::new(
            Decision::InfoHashAlreadyExists,
            Some(metafile.clone().into_owned()),
            meta_cached,
            "candidate info hash is already present locally",
        );
    }
    if options
        .blocklist
        .matches_searchee(&metafile_searchee(metafile))
    {
        return Assessment::new(
            Decision::BlockedRelease,
            Some(metafile.clone().into_owned()),
            meta_cached,
            "candidate metafile matched blocklist",
        );
    }
    if searchee.media_type == MediaType::Pack
        && metafile.media_type == MediaType::Episode
        && !options.include_single_episodes
    {
        return Assessment::new(
            Decision::FileTreeMismatch,
            Some(metafile.clone().into_owned()),
            meta_cached,
            "single-episode candidate cannot match season-pack searchee",
        );
    }

    let decision = file_tree_decision(metafile, searchee, options, fuzzy_size_factor);
    let reason = decision_reason(decision);
    Assessment::new(
        decision,
        Some(metafile.clone().into_owned()),
        meta_cached,
        reason,
    )
}

fn predownload_rejection(
    candidate: &Candidate<'_>,
    searchee: &Searchee<'_>,
    fuzzy_size_factor: f64,
) -> Option<Assessment> {
    let candidate_title = parse_title(candidate.name.as_ref(), &[], None);
    let searchee_title = parse_title(
        searchee.name.as_ref(),
        &searchee.files,
        searchee.path.as_deref(),
    );

    if both_present_differ(
        candidate_title
            .as_ref()
            .and_then(|parsed| parsed.release_group.as_deref()),
        searchee_title
            .as_ref()
            .and_then(|parsed| parsed.release_group.as_deref()),
    ) {
        return Some(Assessment::new(
            Decision::ReleaseGroupMismatch,
            None,
            false,
            "candidate release group differs from searchee release group",
        ));
    }
    if both_present_differ(
        candidate_title
            .as_ref()
            .and_then(|parsed| parsed.resolution.as_deref())
            .filter(|resolution| strict_resolution(resolution)),
        searchee_title
            .as_ref()
            .and_then(|parsed| parsed.resolution.as_deref())
            .filter(|resolution| strict_resolution(resolution)),
    ) {
        return Some(Assessment::new(
            Decision::ResolutionMismatch,
            None,
            false,
            "candidate resolution differs from searchee resolution",
        ));
    }
    if both_present_differ(
        candidate_title
            .as_ref()
            .and_then(|parsed| parsed.source.as_deref()),
        searchee_title
            .as_ref()
            .and_then(|parsed| parsed.source.as_deref()),
    ) {
        return Some(Assessment::new(
            Decision::SourceMismatch,
            None,
            false,
            "candidate source differs from searchee source",
        ));
    }
    if candidate_title
        .as_ref()
        .is_some_and(|parsed| parsed.proper_repack)
        != searchee_title
            .as_ref()
            .is_some_and(|parsed| parsed.proper_repack)
    {
        return Some(Assessment::new(
            Decision::ProperRepackMismatch,
            None,
            false,
            "candidate proper/repack marker differs from searchee",
        ));
    }
    if candidate
        .size
        .is_some_and(|size| !size_within_fuzzy_bounds(size, searchee.length, fuzzy_size_factor))
    {
        return Some(Assessment::new(
            Decision::FuzzySizeMismatch,
            None,
            false,
            "candidate size is outside fuzzy bounds",
        ));
    }
    if candidate.link.is_none() {
        return Some(Assessment::new(
            Decision::NoDownloadLink,
            None,
            false,
            "candidate has no download link",
        ));
    }
    None
}

fn file_tree_decision(
    metafile: &Metafile<'_>,
    searchee: &Searchee<'_>,
    options: &AssessmentOptions<'_>,
    fuzzy_size_factor: f64,
) -> Decision {
    if exact_file_tree_matches(&metafile.files, searchee) {
        return Decision::Match;
    }
    let size_only = size_only_matches(&metafile.files, &searchee.files);
    if size_only && options.match_mode != MatchMode::Strict {
        return Decision::MatchSizeOnly;
    }
    if options.match_mode == MatchMode::Partial {
        let min_ratio = min_size_ratio(searchee, options, fuzzy_size_factor);
        if size_match_ratio(&metafile.files, &searchee.files, metafile.length) < min_ratio {
            return Decision::PartialSizeMismatch;
        }
        if partial_piece_ratio(metafile, &searchee.files) >= min_ratio {
            return Decision::MatchPartial;
        }
        return Decision::FileTreeMismatch;
    }
    if options.match_mode == MatchMode::Strict && size_only {
        Decision::FileTreeMismatch
    } else {
        Decision::SizeMismatch
    }
}

fn exact_file_tree_matches(candidate_files: &[File<'_>], searchee: &Searchee<'_>) -> bool {
    candidate_files.iter().all(|candidate| {
        searchee.files.iter().any(|local| {
            local.length == candidate.length
                && if searchee.source() == SearcheeSource::Virtual {
                    local.name.eq_ignore_ascii_case(candidate.name.as_ref())
                } else {
                    local.path == candidate.path
                }
        })
    })
}

fn size_only_matches(candidate_files: &[File<'_>], searchee_files: &[File<'_>]) -> bool {
    matched_pairs(candidate_files, searchee_files).len() == candidate_files.len()
}

fn size_match_ratio(
    candidate_files: &[File<'_>],
    searchee_files: &[File<'_>],
    candidate_length: u64,
) -> f64 {
    if candidate_length == 0 {
        return 0.0;
    }
    let matched = matched_pairs(candidate_files, searchee_files)
        .iter()
        .map(|(candidate, _)| candidate.length)
        .sum::<u64>();
    matched as f64 / candidate_length as f64
}

fn partial_piece_ratio(metafile: &Metafile<'_>, searchee_files: &[File<'_>]) -> f64 {
    if metafile.length == 0 || metafile.piece_length == 0 {
        return 0.0;
    }
    let matched = matched_pairs(&metafile.files, searchee_files)
        .iter()
        .map(|(candidate, _)| candidate.length)
        .sum::<u64>();
    let total_pieces = metafile.length.div_ceil(metafile.piece_length);
    let available_pieces = matched / metafile.piece_length;
    available_pieces as f64 / total_pieces as f64
}

fn matched_pairs<'a, 'b>(
    candidate_files: &'a [File<'_>],
    searchee_files: &'b [File<'_>],
) -> Vec<(&'a File<'a>, &'b File<'b>)> {
    let mut available = searchee_files.iter().enumerate().collect::<Vec<_>>();
    let mut matched = Vec::new();

    for candidate in candidate_files {
        let Some((available_index, _)) = available
            .iter()
            .enumerate()
            .filter(|(_, (_, file))| file.length == candidate.length)
            .max_by_key(|(_, (_, file))| file.name.eq_ignore_ascii_case(candidate.name.as_ref()))
        else {
            continue;
        };
        let (_, local) = available.swap_remove(available_index);
        matched.push((candidate, local));
    }
    matched
}

fn fuzzy_size_factor(searchee: &Searchee<'_>, options: &AssessmentOptions<'_>) -> f64 {
    if searchee.source() == SearcheeSource::Virtual {
        1.0 - options.season_from_episodes
    } else {
        options.fuzzy_size_threshold
    }
}

fn min_size_ratio(
    searchee: &Searchee<'_>,
    options: &AssessmentOptions<'_>,
    fuzzy_size_factor: f64,
) -> f64 {
    if searchee.source() == SearcheeSource::Virtual {
        options.season_from_episodes
    } else {
        1.0 - fuzzy_size_factor
    }
}

fn size_within_fuzzy_bounds(candidate_size: u64, searchee_length: u64, factor: f64) -> bool {
    let length = searchee_length as f64;
    let candidate_size = candidate_size as f64;
    candidate_size >= length - factor * length && candidate_size <= length + factor * length
}

fn metafile_searchee(metafile: &Metafile<'_>) -> Searchee<'static> {
    let mut searchee = Searchee::from_files(
        metafile.name.as_ref().to_owned(),
        metafile.title.as_ref().to_owned(),
        metafile
            .files
            .iter()
            .cloned()
            .map(File::into_owned)
            .collect(),
    );
    searchee.info_hash = Some(metafile.info_hash.clone().into_owned());
    searchee.media_type = metafile.media_type;
    searchee.client = Some(ClientTorrentMetadata::new(
        "",
        "",
        metafile
            .category
            .clone()
            .map(crate::domain::ClientLabel::into_owned),
        metafile
            .tags
            .iter()
            .cloned()
            .map(crate::domain::ClientLabel::into_owned)
            .collect(),
        metafile
            .trackers
            .iter()
            .map(|tracker| Cow::Owned(tracker.as_ref().to_owned()))
            .collect(),
    ));
    searchee.into_owned()
}

fn cached_decision(
    database: &Database,
    searchee_id: i64,
    guid: &str,
) -> crate::Result<Option<CachedDecision>> {
    database
        .connection()
        .query_row(
            "SELECT decision, info_hash FROM decision
             WHERE searchee_id = ?1 AND guid = ?2",
            params![searchee_id, guid],
            |row| {
                let decision: String = row.get(0)?;
                Ok(CachedDecision {
                    decision: Decision::parse(&decision).unwrap_or(Decision::DownloadFailed),
                    info_hash: row.get(1)?,
                })
            },
        )
        .optional()
        .map_err(persistence_error)
}

#[derive(Debug)]
struct CachedDecision {
    decision: Decision,
    info_hash: Option<String>,
}

fn persist_decision(
    database: &Database,
    searchee_id: i64,
    candidate: &Candidate<'_>,
    assessment: &Assessment,
    now_millis: i64,
    fuzzy_size_factor: f64,
) -> crate::Result<()> {
    database.upsert_decision(&DecisionRecord {
        searchee_id,
        guid: candidate.guid.as_ref(),
        info_hash: assessment
            .metafile
            .as_ref()
            .map(|metafile| metafile.info_hash.as_str()),
        decision: assessment.decision,
        first_seen: now_millis,
        last_seen: now_millis,
        fuzzy_size_factor,
    })
}

fn update_indexer_trackers(
    database: &Database,
    indexer_id: Option<i64>,
    trackers: &[Cow<'_, str>],
) -> crate::Result<()> {
    let Some(indexer_id) = indexer_id else {
        return Ok(());
    };
    if trackers.is_empty() {
        return Ok(());
    }
    let unique = trackers
        .iter()
        .map(|tracker| tracker.as_ref())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let encoded = serde_json::to_string(&unique).map_err(|error| SporosError::Persistence {
        message: Cow::Owned(error.to_string()),
    })?;
    database
        .connection()
        .execute(
            "UPDATE indexer SET trackers = ?2 WHERE id = ?1",
            params![indexer_id, encoded],
        )
        .map_err(persistence_error)?;
    Ok(())
}

fn both_present_differ(left: Option<&str>, right: Option<&str>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => !left.eq_ignore_ascii_case(right),
        _ => false,
    }
}

fn strict_resolution(value: &str) -> bool {
    matches!(
        value.to_ascii_lowercase().as_str(),
        "2160p" | "1080p" | "720p"
    )
}

fn decision_reason(decision: Decision) -> &'static str {
    match decision {
        Decision::Match => "candidate file tree exactly matches searchee",
        Decision::MatchSizeOnly => "candidate file sizes match searchee",
        Decision::MatchPartial => "candidate has enough locally available pieces",
        Decision::PartialSizeMismatch => "too little candidate data is available locally",
        Decision::FileTreeMismatch => "candidate file tree does not match searchee",
        Decision::SizeMismatch => "candidate file sizes do not match searchee",
        Decision::FuzzySizeMismatch
        | Decision::NoDownloadLink
        | Decision::DownloadFailed
        | Decision::MagnetLink
        | Decision::RateLimited
        | Decision::SameInfoHash
        | Decision::InfoHashAlreadyExists
        | Decision::BlockedRelease
        | Decision::ReleaseGroupMismatch
        | Decision::ProperRepackMismatch
        | Decision::ResolutionMismatch
        | Decision::SourceMismatch => decision.as_str(),
    }
}

fn persistence_error(error: rusqlite::Error) -> SporosError {
    SporosError::Persistence {
        message: Cow::Owned(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::{AssessmentOptions, CandidateAssessmentContext, assess_candidate, assess_metafile};
    use crate::{
        config::MatchMode,
        domain::{Candidate, Decision, File, InfoHash, MediaType, Metafile, Searchee},
        integrations::{SnatchHistory, SnatchOptions},
        persistence::Database,
        search::Blocklist,
        torrent::torrent_cache_path,
    };
    use std::{
        collections::BTreeSet,
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    #[test]
    fn assesses_exact_size_only_and_partial_file_trees() {
        let blocklist = Blocklist::parse(&[]).expect("blocklist");
        let strict_exclusions = BTreeSet::new();
        let strict_options = options(&blocklist, MatchMode::Strict, false, &strict_exclusions);
        let searchee = searchee(
            "Example.Show.S01",
            "Example.Show.S01",
            vec![
                File::new("Example.Show.S01/E01.mkv", 100),
                File::new("Example.Show.S01/E02.mkv", 100),
            ],
        );
        let exact = metafile(
            "1111111111111111111111111111111111111111",
            "Example.Show.S01",
            vec![
                File::new("Example.Show.S01/E01.mkv", 100),
                File::new("Example.Show.S01/E02.mkv", 100),
            ],
        );

        assert_eq!(
            assess_metafile(&exact, &searchee, &strict_options, false, 0.05).decision,
            Decision::Match
        );

        let renamed = metafile(
            "2222222222222222222222222222222222222222",
            "Example.Show.S01",
            vec![
                File::new("Renamed/Part1.mkv", 100),
                File::new("Renamed/Part2.mkv", 100),
            ],
        );
        assert_eq!(
            assess_metafile(&renamed, &searchee, &strict_options, false, 0.05).decision,
            Decision::FileTreeMismatch
        );

        let flexible_exclusions = BTreeSet::new();
        let flexible = options(&blocklist, MatchMode::Flexible, false, &flexible_exclusions);
        assert_eq!(
            assess_metafile(&renamed, &searchee, &flexible, false, 0.05).decision,
            Decision::MatchSizeOnly
        );

        let partial_exclusions = BTreeSet::new();
        let partial = options(&blocklist, MatchMode::Partial, false, &partial_exclusions);
        let partial_meta = metafile(
            "3333333333333333333333333333333333333333",
            "Example.Show.S01",
            vec![
                File::new("Example.Show.S01/E01.mkv", 100),
                File::new("Example.Show.S01/E03.mkv", 100),
                File::new("Example.Show.S01/E04.mkv", 100),
            ],
        );
        assert_eq!(
            assess_metafile(&partial_meta, &searchee, &partial, false, 0.05).decision,
            Decision::MatchPartial
        );
    }

    #[test]
    fn applies_file_tree_non_match_fallbacks() {
        let blocklist = Blocklist::parse(&[]).expect("blocklist");
        let exclusions = BTreeSet::new();
        let strict = options(&blocklist, MatchMode::Strict, false, &exclusions);
        let partial = options(&blocklist, MatchMode::Partial, false, &exclusions);
        let mut searchee = searchee(
            "Example.Show.S01",
            "Example.Show.S01",
            vec![
                File::new("Example.Show.S01/E01.mkv", 950),
                File::new("Example.Show.S01/E02.mkv", 100),
            ],
        );
        searchee.info_hash = Some(InfoHash::from_validated(
            "9999999999999999999999999999999999999999",
        ));

        let strict_size_mismatch = metafile(
            "5555555555555555555555555555555555555555",
            "Example.Show.S01",
            vec![File::new("Example.Show.S01/E01.mkv", 90)],
        );
        assert_eq!(
            assess_metafile(&strict_size_mismatch, &searchee, &strict, false, 0.05).decision,
            Decision::SizeMismatch
        );

        let partial_size_mismatch = metafile(
            "6666666666666666666666666666666666666666",
            "Example.Show.S01",
            vec![
                File::new("Example.Show.S01/E01.mkv", 100),
                File::new("Example.Show.S01/E03.mkv", 100),
                File::new("Example.Show.S01/E04.mkv", 100),
            ],
        );
        assert_eq!(
            assess_metafile(&partial_size_mismatch, &searchee, &partial, false, 0.05).decision,
            Decision::PartialSizeMismatch
        );

        let partial_piece_mismatch = metafile_with_piece_length(
            "7777777777777777777777777777777777777777",
            "Example.Show.S01",
            512,
            vec![
                File::new("Example.Show.S01/E01.mkv", 950),
                File::new("Example.Show.S01/E03.mkv", 50),
            ],
        );
        assert_eq!(
            assess_metafile(&partial_piece_mismatch, &searchee, &partial, false, 0.05).decision,
            Decision::FileTreeMismatch
        );
    }

    #[test]
    fn virtual_exact_matching_uses_file_names_not_paths() {
        let blocklist = Blocklist::parse(&[]).expect("blocklist");
        let exclusions = BTreeSet::new();
        let options = options(&blocklist, MatchMode::Strict, false, &exclusions);
        let searchee = Searchee::from_files(
            "Example.Show.S01",
            "Example.Show.S01",
            vec![File::new("/library/Example.Show/Season 01/E01.mkv", 100)],
        );
        assert_eq!(searchee.source(), crate::domain::SearcheeSource::Virtual);
        let candidate = metafile(
            "8888888888888888888888888888888888888888",
            "Example.Show.S01",
            vec![File::new("Example.Show.S01/E01.mkv", 100)],
        );

        assert_eq!(
            assess_metafile(&candidate, &searchee, &options, false, 0.05).decision,
            Decision::Match
        );
    }

    #[test]
    fn size_matching_prefers_same_names_for_duplicate_lengths() {
        let candidates = vec![
            File::new("candidate/episode-one.mkv", 100),
            File::new("candidate/episode-two.mkv", 100),
        ];
        let locals = vec![
            File::with_name("episode-two.mkv", "local/random-a.mkv", 100),
            File::with_name("episode-one.mkv", "local/random-b.mkv", 100),
        ];

        let pairs = super::matched_pairs(&candidates, &locals);

        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0].0.name, pairs[0].1.name);
        assert_eq!(pairs[1].0.name, pairs[1].1.name);
    }

    #[test]
    fn rejects_conservative_candidate_predownload_mismatches() {
        let blocklist = Blocklist::parse(&[]).expect("blocklist");
        let exclusions = BTreeSet::new();
        let options = options(&blocklist, MatchMode::Strict, false, &exclusions);
        let searchee = searchee(
            "Example.Show.S01E01.1080p.WEB-DL-GRP",
            "Example.Show.S01E01.1080p.WEB-DL-GRP",
            vec![File::new("Example.Show.S01E01.1080p.WEB-DL-GRP.mkv", 100)],
        );
        let database_root = temp_path("predownload");
        fs::create_dir_all(&database_root).expect("temp dir");
        let database = Database::open_app_dir(&database_root).expect("database");
        let mut history = SnatchHistory::default();

        let first_candidate = candidate("Example.Show.S01E01.720p.WEB-DL-GRP", Some(100));
        let mut context = CandidateAssessmentContext {
            database: &database,
            app_dir: &database_root,
            options: &options,
            snatch_options: SnatchOptions::default(),
            now_millis: 1,
        };
        let assessment = assess_candidate(&context, &first_candidate, &searchee, &mut history)
            .expect("assessment");
        assert_eq!(assessment.decision, Decision::ResolutionMismatch);

        let second_candidate = candidate("Example.Show.S01E01.1080p.WEB-DL-GRP", Some(200));
        context.now_millis = 2;
        let assessment = assess_candidate(&context, &second_candidate, &searchee, &mut history)
            .expect("assessment");
        assert_eq!(assessment.decision, Decision::FuzzySizeMismatch);
        let _cleanup = fs::remove_dir_all(database_root);
    }

    #[test]
    fn persists_decisions_and_reuses_cached_torrents_by_guid() {
        let blocklist = Blocklist::parse(&[]).expect("blocklist");
        let exclusions = BTreeSet::new();
        let options = options(&blocklist, MatchMode::Strict, false, &exclusions);
        let root = temp_path("cache-assess");
        fs::create_dir_all(&root).expect("temp dir");
        let database = Database::open_app_dir(&root).expect("database");
        let searchee = searchee(
            "Cached.Release",
            "Cached.Release",
            vec![File::new("Cached.Release", 10)],
        );
        let bytes = torrent_bytes("Cached.Release", 10);
        let metafile = crate::integrations::cache_torrent_file(&root, &bytes).expect("cache");
        let searchee_id = database
            .get_or_insert_searchee(searchee.title.as_ref(), 1)
            .expect("searchee");
        database
            .upsert_decision(&crate::persistence::DecisionRecord {
                searchee_id,
                guid: "guid-1",
                info_hash: Some(metafile.info_hash.as_str()),
                decision: Decision::Match,
                first_seen: 1,
                last_seen: 1,
                fuzzy_size_factor: 0.05,
            })
            .expect("decision");
        let mut history = SnatchHistory::default();
        let candidate = Candidate::new(
            "Cached.Release",
            "guid-1",
            Some("http://127.0.0.1:9/t"),
            "tracker",
        );

        let context = CandidateAssessmentContext {
            database: &database,
            app_dir: &root,
            options: &options,
            snatch_options: SnatchOptions::default(),
            now_millis: 2,
        };
        let assessment =
            assess_candidate(&context, &candidate, &searchee, &mut history).expect("assessment");

        assert_eq!(assessment.decision, Decision::Match);
        assert!(assessment.meta_cached);
        assert!(torrent_cache_path(&root, &metafile.info_hash).exists());
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn rejects_same_existing_blocklisted_and_single_episode_candidates() {
        let blocklist = Blocklist::parse(&["name:blocked".to_owned()]).expect("blocklist");
        let mut excluded = BTreeSet::new();
        excluded.insert("2222222222222222222222222222222222222222".to_owned());
        let options = options(&blocklist, MatchMode::Flexible, false, &excluded);
        let mut searchee = searchee(
            "Example.Show.S01",
            "Example.Show.S01",
            vec![File::new("Example.Show.S01/E01.mkv", 100)],
        );
        searchee.info_hash = Some(InfoHash::from_validated(
            "1111111111111111111111111111111111111111",
        ));

        let same = metafile(
            "1111111111111111111111111111111111111111",
            "Example.Show.S01",
            vec![File::new("Example.Show.S01/E01.mkv", 100)],
        );
        assert_eq!(
            assess_metafile(&same, &searchee, &options, false, 0.05).decision,
            Decision::SameInfoHash
        );

        let existing = metafile(
            "2222222222222222222222222222222222222222",
            "Example.Show.S01",
            vec![File::new("Example.Show.S01/E01.mkv", 100)],
        );
        assert_eq!(
            assess_metafile(&existing, &searchee, &options, false, 0.05).decision,
            Decision::InfoHashAlreadyExists
        );

        let blocked = metafile(
            "3333333333333333333333333333333333333333",
            "Blocked.Release",
            vec![File::new("Blocked.Release.mkv", 100)],
        );
        assert_eq!(
            assess_metafile(&blocked, &searchee, &options, false, 0.05).decision,
            Decision::BlockedRelease
        );

        let episode = metafile(
            "4444444444444444444444444444444444444444",
            "Example.Show.S01E01",
            vec![File::new("Example.Show.S01E01.mkv", 100)],
        );
        assert_eq!(
            assess_metafile(&episode, &searchee, &options, false, 0.05).decision,
            Decision::FileTreeMismatch
        );
    }

    fn options<'a>(
        blocklist: &'a Blocklist,
        match_mode: MatchMode,
        include_single_episodes: bool,
        info_hashes_to_exclude: &'a BTreeSet<String>,
    ) -> AssessmentOptions<'a> {
        AssessmentOptions {
            match_mode,
            fuzzy_size_threshold: 0.05,
            season_from_episodes: 0.5,
            include_single_episodes,
            info_hashes_to_exclude,
            blocklist,
        }
    }

    fn searchee(name: &str, title: &str, files: Vec<File<'static>>) -> Searchee<'static> {
        let mut searchee = Searchee::from_files(name.to_owned(), title.to_owned(), files);
        searchee.media_type = if title.contains("S01E") {
            MediaType::Episode
        } else {
            MediaType::Pack
        };
        searchee.into_owned()
    }

    fn metafile(hash: &str, name: &str, files: Vec<File<'static>>) -> Metafile<'static> {
        metafile_with_piece_length(hash, name, 50, files)
    }

    fn metafile_with_piece_length(
        hash: &str,
        name: &str,
        piece_length: u64,
        files: Vec<File<'static>>,
    ) -> Metafile<'static> {
        let mut metafile = Metafile::from_files(
            InfoHash::from_validated(hash.to_owned()),
            name.to_owned(),
            name.to_owned(),
            piece_length,
            files,
        );
        metafile.media_type = if name.contains("S01E") {
            MediaType::Episode
        } else {
            MediaType::Pack
        };
        metafile
    }

    fn candidate(name: &str, size: Option<u64>) -> Candidate<'static> {
        let mut candidate = Candidate::new(
            name.to_owned(),
            format!("guid-{name}"),
            Some("http://127.0.0.1:9/t".to_owned()),
            "tracker",
        );
        candidate.size = size;
        candidate
    }

    fn torrent_bytes(name: &str, length: u64) -> Vec<u8> {
        format!(
            "d4:infod6:lengthi{length}e4:name{}:{name}12:piece lengthi50e6:pieces20:aaaaaaaaaaaaaaaaaaaaee",
            name.len()
        )
        .into_bytes()
    }

    fn temp_path(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        std::env::temp_dir().join(format!("sporos-matching-{label}-{nanos}"))
    }
}
