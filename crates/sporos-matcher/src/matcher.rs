use std::collections::{BTreeMap, BTreeSet, VecDeque};

use sporos_model::{
    FileMapping, LocalSourceFile, LocalSourceManifest, MatchDecision, MatchEvidence, MatchMode,
    MatchOutcome, MatchReason, MatchingPolicy, Ratio, ReleaseDescriptor, SourceId, SourceKind,
    TorrentFile, TorrentManifest, VideoKind,
};
use unicode_normalization::UnicodeNormalization;

use crate::release::{episode_keys, normalize_title};

const SCORE_EXACT_PATH: u32 = 1_000_000;
const SCORE_EXACT_BASENAME: u32 = 100_000;
const SCORE_EPISODE: u32 = 10_000;
const SCORE_PRIMARY_EXTENSION: u32 = 1_000;
const SCORE_NORMALIZED_BASENAME: u32 = 100;
const SCORE_DIRECTORY: u32 = 10;
const SCORE_ROLE: u32 = 1;

pub struct MatchRequest<'a> {
    pub candidate: &'a TorrentManifest,
    pub candidate_release: &'a ReleaseDescriptor,
    pub sources: &'a [LocalSourceManifest],
    pub policy: &'a MatchingPolicy,
}

pub trait Matcher {
    fn evaluate(&self, request: &MatchRequest<'_>) -> MatchDecision;
}

#[derive(Debug, Default)]
pub struct PureMatcher;

impl Matcher for PureMatcher {
    fn evaluate(&self, request: &MatchRequest<'_>) -> MatchDecision {
        let evidence = base_evidence(request);
        if let Some(reason) = validate_manifest(request.candidate) {
            return rejection(reason, evidence);
        }

        if request.sources.iter().any(|source| {
            (request.candidate.hashes.v1.is_some()
                && request.candidate.hashes.v1 == source.hashes.v1)
                || (request.candidate.hashes.v2.is_some()
                    && request.candidate.hashes.v2 == source.hashes.v2)
        }) {
            return MatchDecision {
                outcome: MatchOutcome::AlreadyPresent,
                mode: None,
                reason: MatchReason::AlreadyPresent,
                source_ids: Vec::new(),
                mappings: Vec::new(),
                mapped_bytes: 0,
                missing_bytes: required_bytes(request),
                present_ratio: Ratio::ZERO,
                requires_recheck: false,
                evidence,
            };
        }
        if input_budget_exceeded(request) {
            return rejection(MatchReason::MatcherBudgetExceeded, evidence);
        }
        let mut sources: Vec<_> = request
            .sources
            .iter()
            .filter(|source| source.available)
            .collect();
        sources.sort_by_key(|source| source.id);

        let mut conflicts = Vec::new();
        let mut choices = Vec::new();
        for source in &sources {
            match compatible(request.candidate_release, &source.release, false) {
                Ok(()) => match evaluate_source(request, source) {
                    SourceEvaluation::Decision(decision) => choices.push(decision),
                    SourceEvaluation::Ambiguous => {
                        return rejection(MatchReason::AmbiguousFileMapping, evidence);
                    }
                    SourceEvaluation::NoMatch(reason) => conflicts.push(reason),
                },
                Err(reason) => conflicts.push(reason),
            }
        }

        if request.policy.allow_season_from_episodes
            && request.candidate_release.kind == VideoKind::SeasonPack
        {
            match evaluate_season(request, &sources) {
                SourceEvaluation::Decision(decision) => choices.push(decision),
                SourceEvaluation::Ambiguous => {
                    return rejection(MatchReason::AmbiguousFileMapping, evidence);
                }
                SourceEvaluation::NoMatch(reason) => conflicts.push(reason),
            }
        }

        choose(choices).unwrap_or_else(|| no_match(best_reason(&conflicts), evidence, request))
    }
}

enum SourceEvaluation {
    Decision(MatchDecision),
    Ambiguous,
    NoMatch(MatchReason),
}

fn evaluate_source(request: &MatchRequest<'_>, source: &LocalSourceManifest) -> SourceEvaluation {
    let candidate_files = candidate_files(request.candidate);
    let source_files: Vec<_> = source.files.iter().map(|file| (source.id, file)).collect();
    let assignment = match assign(&candidate_files, &source_files, request.policy) {
        Ok(assignment) => assignment,
        Err(AssignmentError::BudgetExceeded) => {
            return SourceEvaluation::NoMatch(MatchReason::MatcherBudgetExceeded);
        }
    };
    if assignment.ambiguous {
        return SourceEvaluation::Ambiguous;
    }
    let mappings = build_mappings(&candidate_files, &source_files, &assignment);
    let required: Vec<_> = candidate_files
        .iter()
        .filter(|file| required(file, request.policy))
        .collect();
    let mapped_ordinals: BTreeSet<_> = mappings
        .iter()
        .map(|mapping| mapping.candidate_ordinal)
        .collect();
    let all_required = required
        .iter()
        .all(|file| mapped_ordinals.contains(&file.ordinal));
    let exact_required = required.iter().all(|file| {
        mappings.iter().any(|mapping| {
            mapping.candidate_ordinal == file.ordinal
                && mapping.candidate_path == mapping.source_path
        })
    });
    let mapped_primary = mappings
        .iter()
        .filter(|mapping| {
            candidate_files
                .iter()
                .find(|file| file.ordinal == mapping.candidate_ordinal)
                .is_some_and(|file| primary(file, request.policy))
        })
        .count();

    let mode = if all_required && exact_required {
        Some(MatchMode::Strict)
    } else if all_required && request.policy.allow_flexible {
        Some(MatchMode::Flexible)
    } else if mapped_primary > 0 && request.policy.allow_partial {
        Some(MatchMode::Partial)
    } else {
        None
    };
    let Some(mode) = mode else {
        return SourceEvaluation::NoMatch(file_failure_reason(
            &candidate_files,
            &source_files,
            request.policy,
        ));
    };
    SourceEvaluation::Decision(decision(request, mode, mappings, 1))
}

fn evaluate_season(
    request: &MatchRequest<'_>,
    sources: &[&LocalSourceManifest],
) -> SourceEvaluation {
    let season = request.candidate_release.season;
    let episode_sources: Vec<_> = sources
        .iter()
        .filter(|source| {
            compatible(request.candidate_release, &source.release, true).is_ok()
                && source.release.kind == VideoKind::Episode
                && source.release.season == season
        })
        .flat_map(|source| source.files.iter().map(|file| (source.id, file)))
        .collect();
    if episode_sources.is_empty() {
        return SourceEvaluation::NoMatch(MatchReason::NoPrimaryVideoOverlap);
    }
    let candidate_files = candidate_files(request.candidate);
    let assignment = match assign_season(&candidate_files, &episode_sources, request.policy) {
        Ok(assignment) => assignment,
        Err(AssignmentError::BudgetExceeded) => {
            return SourceEvaluation::NoMatch(MatchReason::MatcherBudgetExceeded);
        }
    };
    if assignment.ambiguous {
        return SourceEvaluation::Ambiguous;
    }
    let mappings = build_mappings(&candidate_files, &episode_sources, &assignment);
    if !mappings.iter().any(|mapping| {
        candidate_files
            .iter()
            .find(|file| file.ordinal == mapping.candidate_ordinal)
            .is_some_and(|file| primary(file, request.policy))
    }) {
        return SourceEvaluation::NoMatch(MatchReason::NoPrimaryVideoOverlap);
    }
    let source_count = mappings
        .iter()
        .map(|mapping| mapping.source_id)
        .collect::<BTreeSet<_>>()
        .len();
    SourceEvaluation::Decision(decision(
        request,
        MatchMode::SeasonFromEpisodes,
        mappings,
        source_count,
    ))
}

fn candidate_files(manifest: &TorrentManifest) -> Vec<&TorrentFile> {
    let mut files: Vec<_> = manifest.files.iter().filter(|file| !file.padding).collect();
    files.sort_by_key(|file| file.ordinal);
    files
}

struct Assignment {
    pairs: Vec<(usize, usize, u32)>,
    ambiguous: bool,
}

#[derive(Debug)]
enum AssignmentError {
    BudgetExceeded,
}

fn assign(
    candidates: &[&TorrentFile],
    sources: &[(SourceId, &LocalSourceFile)],
    policy: &MatchingPolicy,
) -> Result<Assignment, AssignmentError> {
    assign_with(candidates, sources, policy, |candidate, source, policy| {
        edge_score(candidate, source, policy, false)
    })
}

fn assign_season(
    candidates: &[&TorrentFile],
    sources: &[(SourceId, &LocalSourceFile)],
    policy: &MatchingPolicy,
) -> Result<Assignment, AssignmentError> {
    assign_with(candidates, sources, policy, |candidate, source, policy| {
        edge_score(candidate, source, policy, true)
    })
}

fn assign_with(
    candidates: &[&TorrentFile],
    sources: &[(SourceId, &LocalSourceFile)],
    policy: &MatchingPolicy,
    score: impl Fn(&TorrentFile, &LocalSourceFile, &MatchingPolicy) -> Option<u32>,
) -> Result<Assignment, AssignmentError> {
    if candidates.len() > policy.max_assignment_files
        || sources.len() > policy.max_assignment_files.saturating_sub(candidates.len())
    {
        return Err(AssignmentError::BudgetExceeded);
    }
    let mut source_buckets: BTreeMap<BucketKey, Vec<usize>> = BTreeMap::new();
    for (index, (_, source)) in sources.iter().enumerate() {
        source_buckets
            .entry(bucket(
                source.size,
                &source.path,
                primary_path(&source.path, policy),
            ))
            .or_default()
            .push(index);
    }
    let mut edges = vec![Vec::new(); candidates.len()];
    let mut reverse = vec![Vec::new(); sources.len()];
    let mut edge_count = 0_usize;
    for (candidate_index, candidate) in candidates.iter().enumerate() {
        let key = bucket(candidate.size, &candidate.path, primary(candidate, policy));
        for &source_index in source_buckets.get(&key).into_iter().flatten() {
            if let Some(value) = score(candidate, sources[source_index].1, policy) {
                edge_count = edge_count
                    .checked_add(1)
                    .ok_or(AssignmentError::BudgetExceeded)?;
                if edge_count > policy.max_candidate_edges {
                    return Err(AssignmentError::BudgetExceeded);
                }
                edges[candidate_index].push((source_index, value));
                reverse[source_index].push(candidate_index);
            }
        }
    }
    let score_unit = u128::from(
        edges
            .iter()
            .flatten()
            .map(|(_, score)| u64::from(*score))
            .sum::<u64>(),
    ) + 1;
    let primary_bytes = candidates
        .iter()
        .filter(|candidate| primary(candidate, policy))
        .fold(0_u128, |total, candidate| {
            total + u128::from(candidate.size)
        });
    let required_unit = (primary_bytes + 1) * score_unit;
    let mut candidate_seen = vec![false; candidates.len()];
    let mut source_seen = vec![false; sources.len()];
    let mut pairs = Vec::new();
    let mut operations = 0_u128;
    for start in 0..candidates.len() {
        if candidate_seen[start] || edges[start].is_empty() {
            continue;
        }
        let (component_candidates, component_sources) = component(
            start,
            &edges,
            &reverse,
            &mut candidate_seen,
            &mut source_seen,
        );
        if component_candidates.len() + component_sources.len()
            > policy.max_assignment_component_files
            || component_candidates.len() + component_sources.len() > policy.max_assignment_files
        {
            return Err(AssignmentError::BudgetExceeded);
        }
        let rows = component_candidates.len() as u128;
        let columns = (component_sources.len() + component_candidates.len()) as u128;
        let alternatives = component_candidates.len().min(component_sources.len()) as u128 + 1;
        operations = operations.saturating_add(alternatives * rows * rows * columns);
        if operations > u128::from(policy.max_assignment_operations) {
            return Err(AssignmentError::BudgetExceeded);
        }
        let source_positions: BTreeMap<_, _> = component_sources
            .iter()
            .enumerate()
            .map(|(position, source)| (*source, position))
            .collect();
        let mut raw = vec![vec![None; component_sources.len()]; component_candidates.len()];
        let mut weights = vec![vec![None; component_sources.len()]; component_candidates.len()];
        for (row, &candidate_index) in component_candidates.iter().enumerate() {
            for &(source_index, value) in &edges[candidate_index] {
                let column = source_positions[&source_index];
                raw[row][column] = Some(value);
                weights[row][column] = Some(
                    u128::from(value)
                        + if primary(candidates[candidate_index], policy) {
                            u128::from(candidates[candidate_index].size) * score_unit
                        } else {
                            0
                        }
                        + if required(candidates[candidate_index], policy) {
                            required_unit
                        } else {
                            0
                        },
                );
            }
        }
        let (total, selected) = optimal_assignment(&weights, None);
        if selected.iter().any(|&(candidate, source)| {
            let (alternative, _) = optimal_assignment(&weights, Some((candidate, source)));
            alternative == total
        }) {
            return Ok(Assignment {
                pairs: Vec::new(),
                ambiguous: true,
            });
        }
        pairs.extend(selected.into_iter().map(|(candidate, source)| {
            (
                component_candidates[candidate],
                component_sources[source],
                raw[candidate][source].expect("selected edge has a score"),
            )
        }));
    }
    Ok(Assignment {
        pairs,
        ambiguous: false,
    })
}

fn input_budget_exceeded(request: &MatchRequest<'_>) -> bool {
    let candidate_files = request
        .candidate
        .files
        .iter()
        .filter(|file| !file.padding)
        .count();
    if candidate_files > request.policy.max_assignment_files {
        return true;
    }
    request
        .sources
        .iter()
        .filter(|source| source.available)
        .try_fold(candidate_files, |total, source| {
            total.checked_add(source.files.len()).filter(|total| {
                source.files.len()
                    <= request
                        .policy
                        .max_assignment_files
                        .saturating_sub(candidate_files)
                    && *total <= request.policy.max_assignment_files
            })
        })
        .is_none()
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct BucketKey {
    size: u64,
    primary: bool,
    extension: Option<String>,
}

fn bucket(size: u64, path: &str, primary: bool) -> BucketKey {
    BucketKey {
        size,
        primary,
        extension: (!primary).then(|| extension(path).unwrap_or("").to_ascii_lowercase()),
    }
}

fn component(
    start: usize,
    edges: &[Vec<(usize, u32)>],
    reverse: &[Vec<usize>],
    candidate_seen: &mut [bool],
    source_seen: &mut [bool],
) -> (Vec<usize>, Vec<usize>) {
    let mut candidates = Vec::new();
    let mut sources = Vec::new();
    let mut queue = VecDeque::from([(true, start)]);
    candidate_seen[start] = true;
    while let Some((candidate_side, index)) = queue.pop_front() {
        if candidate_side {
            candidates.push(index);
            for &(source, _) in &edges[index] {
                if !source_seen[source] {
                    source_seen[source] = true;
                    queue.push_back((false, source));
                }
            }
        } else {
            sources.push(index);
            for &candidate in &reverse[index] {
                if !candidate_seen[candidate] {
                    candidate_seen[candidate] = true;
                    queue.push_back((true, candidate));
                }
            }
        }
    }
    candidates.sort_unstable();
    sources.sort_unstable();
    (candidates, sources)
}

fn optimal_assignment(
    weights: &[Vec<Option<u128>>],
    banned: Option<(usize, usize)>,
) -> (u128, Vec<(usize, usize)>) {
    let rows = weights.len();
    if rows == 0 {
        return (0, Vec::new());
    }
    let source_columns = weights.first().map_or(0, Vec::len);
    let columns = source_columns + rows;
    let mut maximum = 0_u128;
    for (row, values) in weights.iter().enumerate() {
        for (column, weight) in values.iter().enumerate() {
            if banned != Some((row, column)) {
                maximum = maximum.max(weight.unwrap_or(0));
            }
        }
    }
    assert!(maximum <= i128::MAX as u128, "matcher objective overflow");
    let mut costs = vec![vec![0_i128; columns]; rows];
    for row in 0..rows {
        for column in 0..source_columns {
            if banned != Some((row, column)) {
                costs[row][column] = -i128::try_from(weights[row][column].unwrap_or(0))
                    .expect("matcher objective was bounded");
            }
        }
    }
    let assignment = hungarian(&costs);
    let pairs: Vec<_> = assignment
        .into_iter()
        .enumerate()
        .filter_map(|(row, column)| {
            (column < source_columns
                && banned != Some((row, column))
                && weights[row][column].is_some())
            .then_some((row, column))
        })
        .collect();
    let total = pairs
        .iter()
        .map(|&(row, column)| weights[row][column].expect("selected edges exist"))
        .sum();
    (total, pairs)
}

// Rectangular Hungarian assignment. Each candidate also receives a private
// zero-cost dummy column, so unmatched files are represented explicitly.
fn hungarian(costs: &[Vec<i128>]) -> Vec<usize> {
    let rows = costs.len();
    let columns = costs[0].len();
    let mut row_potential = vec![0_i128; rows + 1];
    let mut column_potential = vec![0_i128; columns + 1];
    let mut column_row = vec![0_usize; columns + 1];
    let mut previous = vec![0_usize; columns + 1];
    for row in 1..=rows {
        column_row[0] = row;
        let mut column = 0;
        let mut minimum = vec![i128::MAX; columns + 1];
        let mut used = vec![false; columns + 1];
        loop {
            used[column] = true;
            let current_row = column_row[column];
            let mut delta = i128::MAX;
            let mut next = 0;
            for candidate_column in 1..=columns {
                if used[candidate_column] {
                    continue;
                }
                let reduced = costs[current_row - 1][candidate_column - 1]
                    - row_potential[current_row]
                    - column_potential[candidate_column];
                if reduced < minimum[candidate_column] {
                    minimum[candidate_column] = reduced;
                    previous[candidate_column] = column;
                }
                if minimum[candidate_column] < delta {
                    delta = minimum[candidate_column];
                    next = candidate_column;
                }
            }
            for candidate_column in 0..=columns {
                if used[candidate_column] {
                    row_potential[column_row[candidate_column]] += delta;
                    column_potential[candidate_column] -= delta;
                } else {
                    minimum[candidate_column] -= delta;
                }
            }
            column = next;
            if column_row[column] == 0 {
                break;
            }
        }
        loop {
            let prior = previous[column];
            column_row[column] = column_row[prior];
            column = prior;
            if column == 0 {
                break;
            }
        }
    }
    let mut assignment = vec![columns; rows];
    for column in 1..=columns {
        if column_row[column] != 0 {
            assignment[column_row[column] - 1] = column - 1;
        }
    }
    assignment
}

fn edge_score(
    candidate: &TorrentFile,
    source: &LocalSourceFile,
    policy: &MatchingPolicy,
    season_mode: bool,
) -> Option<u32> {
    if candidate.size != source.size {
        return None;
    }
    let candidate_primary = primary(candidate, policy);
    let source_primary = primary_path(&source.path, policy);
    if candidate_primary != source_primary {
        return None;
    }
    let candidate_extension = extension(&candidate.path);
    let source_extension = extension(&source.path);
    if !candidate_primary && candidate_extension != source_extension {
        return None;
    }
    let candidate_keys = episode_keys(&candidate.path);
    let source_keys = episode_keys(&source.path);
    let episode_agreement = !candidate_keys.is_empty()
        && !source_keys.is_empty()
        && candidate_keys.iter().any(|key| source_keys.contains(key));
    if !candidate_keys.is_empty() && !source_keys.is_empty() && !episode_agreement {
        return None;
    }
    if season_mode && candidate_primary && !episode_agreement {
        return None;
    }

    let exact_path = candidate.path == source.path;
    let candidate_basename = basename(&candidate.path);
    let source_basename = basename(&source.path);
    let exact_basename = candidate_basename == source_basename;
    let normalized_basename =
        normalize_title(stem(candidate_basename)) == normalize_title(stem(source_basename));
    let directory_agreement = parent_basename(&candidate.path)
        .zip(parent_basename(&source.path))
        .is_some_and(|(candidate, source)| normalize_title(candidate) == normalize_title(source));
    Some(
        SCORE_ROLE
            + u32::from(exact_path) * SCORE_EXACT_PATH
            + u32::from(exact_basename) * SCORE_EXACT_BASENAME
            + u32::from(episode_agreement) * SCORE_EPISODE
            + u32::from(candidate_primary && candidate_extension == source_extension)
                * SCORE_PRIMARY_EXTENSION
            + u32::from(normalized_basename) * SCORE_NORMALIZED_BASENAME
            + u32::from(directory_agreement) * SCORE_DIRECTORY,
    )
}

fn build_mappings(
    candidates: &[&TorrentFile],
    sources: &[(SourceId, &LocalSourceFile)],
    assignment: &Assignment,
) -> Vec<FileMapping> {
    let mut mappings: Vec<_> = assignment
        .pairs
        .iter()
        .map(|&(candidate_index, source_index, score)| {
            let candidate = candidates[candidate_index];
            let (source_id, source) = sources[source_index];
            FileMapping {
                candidate_ordinal: candidate.ordinal,
                source_id,
                source_file_id: source.id,
                candidate_path: candidate.path.clone(),
                source_path: source.path.clone(),
                size: candidate.size,
                score,
            }
        })
        .collect();
    mappings.sort_by_key(|mapping| mapping.candidate_ordinal);
    mappings
}

fn decision(
    request: &MatchRequest<'_>,
    mode: MatchMode,
    mappings: Vec<FileMapping>,
    compatible_sources: usize,
) -> MatchDecision {
    let mapped_bytes = mappings.iter().map(|mapping| mapping.size).sum();
    let total = required_bytes(request);
    let mapped_required = mappings
        .iter()
        .filter(|mapping| {
            request
                .candidate
                .files
                .iter()
                .find(|file| file.ordinal == mapping.candidate_ordinal)
                .is_some_and(|file| required(file, request.policy))
        })
        .map(|mapping| mapping.size)
        .sum::<u64>();
    let mut source_ids: Vec<_> = mappings.iter().map(|mapping| mapping.source_id).collect();
    source_ids.sort();
    source_ids.dedup();
    let mut evidence = base_evidence(request);
    evidence.compatible_sources = compatible_sources;
    evidence.mapped_files = mappings.len();
    evidence.mapped_primary_files = mappings
        .iter()
        .filter(|mapping| {
            request
                .candidate
                .files
                .iter()
                .find(|file| file.ordinal == mapping.candidate_ordinal)
                .is_some_and(|file| primary(file, request.policy))
        })
        .count();
    evidence.mapped_primary_bytes = mappings
        .iter()
        .filter(|mapping| {
            request
                .candidate
                .files
                .iter()
                .find(|file| file.ordinal == mapping.candidate_ordinal)
                .is_some_and(|file| primary(file, request.policy))
        })
        .map(|mapping| mapping.size)
        .sum();
    evidence.exact_path_mappings = mappings
        .iter()
        .filter(|mapping| mapping.candidate_path == mapping.source_path)
        .count();
    evidence.qbit_sources = source_ids
        .iter()
        .filter(|source_id| {
            request.sources.iter().any(|source| {
                source.id == **source_id && source.kind == SourceKind::QbittorrentTorrent
            })
        })
        .count();
    MatchDecision {
        outcome: MatchOutcome::Match,
        mode: Some(mode),
        reason: match mode {
            MatchMode::Strict => MatchReason::MatchStrict,
            MatchMode::Flexible => MatchReason::MatchFlexible,
            MatchMode::Partial => MatchReason::MatchPartial,
            MatchMode::SeasonFromEpisodes => MatchReason::MatchSeasonFromEpisodes,
        },
        source_ids,
        mappings,
        mapped_bytes,
        missing_bytes: total.saturating_sub(mapped_required),
        present_ratio: Ratio::from_bytes(mapped_required, total),
        requires_recheck: true,
        evidence,
    }
}

fn choose(mut choices: Vec<MatchDecision>) -> Option<MatchDecision> {
    choices.sort_by_key(|decision| std::cmp::Reverse(quality(decision)));
    let first = choices.first()?;
    if choices.get(1).is_some_and(|second| {
        quality(first) == quality(second) && first.mappings != second.mappings
    }) {
        let evidence = first.evidence.clone();
        return Some(rejection(MatchReason::AmbiguousFileMapping, evidence));
    }
    Some(choices.remove(0))
}

fn quality(decision: &MatchDecision) -> (u8, u64, usize, std::cmp::Reverse<usize>, usize, usize) {
    let rank = match decision.mode {
        Some(MatchMode::Strict) => 4,
        Some(MatchMode::Flexible) => 3,
        Some(MatchMode::Partial) => 2,
        Some(MatchMode::SeasonFromEpisodes) => 1,
        None => 0,
    };
    (
        rank,
        decision.evidence.mapped_primary_bytes,
        decision.evidence.exact_path_mappings,
        std::cmp::Reverse(decision.source_ids.len()),
        decision.evidence.qbit_sources,
        decision.mappings.len(),
    )
}

fn compatible(
    candidate: &ReleaseDescriptor,
    source: &ReleaseDescriptor,
    season_mode: bool,
) -> Result<(), MatchReason> {
    let same_arr_identity = matches!(
        (&candidate.arr_identity, &source.arr_identity),
        (Some(candidate), Some(source))
            if candidate.kind == source.kind
                && candidate.instance == source.instance
                && candidate.entity_id == source.entity_id
    );
    if let (Some(candidate), Some(source)) = (&candidate.arr_identity, &source.arr_identity)
        && (candidate.kind != source.kind
            || candidate.instance != source.instance
            || candidate.entity_id != source.entity_id)
    {
        return Err(MatchReason::SeriesConflict);
    }
    if candidate.kind != VideoKind::UnknownVideo
        && source.kind != VideoKind::UnknownVideo
        && candidate.kind != source.kind
        && !(season_mode
            && candidate.kind == VideoKind::SeasonPack
            && source.kind == VideoKind::Episode)
    {
        return Err(MatchReason::MediaTypeConflict);
    }
    if !same_arr_identity && !titles_overlap(candidate, source) {
        return Err(MatchReason::TitleConflict);
    }
    if let (Some(candidate), Some(source)) = (candidate.year, source.year)
        && candidate != source
    {
        return Err(MatchReason::YearConflict);
    }
    if let (Some(candidate), Some(source)) = (candidate.season, source.season)
        && candidate != source
    {
        return Err(MatchReason::SeasonConflict);
    }
    if !season_mode
        && candidate.kind == VideoKind::Episode
        && source.kind == VideoKind::Episode
        && (candidate.episode, candidate.episode_end) != (source.episode, source.episode_end)
    {
        return Err(MatchReason::EpisodeConflict);
    }
    if let (Some(candidate), Some(source)) = (candidate.air_date, source.air_date)
        && candidate != source
    {
        return Err(MatchReason::DateConflict);
    }
    if let (Some(candidate), Some(source)) = (candidate.absolute_episode, source.absolute_episode)
        && candidate != source
    {
        return Err(MatchReason::EpisodeConflict);
    }
    Ok(())
}

fn titles_overlap(candidate: &ReleaseDescriptor, source: &ReleaseDescriptor) -> bool {
    let candidate_titles = std::iter::once(&candidate.primary_title)
        .chain(candidate.alternate_titles.iter())
        .collect::<BTreeSet<_>>();
    std::iter::once(&source.primary_title)
        .chain(source.alternate_titles.iter())
        .any(|title| candidate_titles.contains(title))
}

fn validate_manifest(manifest: &TorrentManifest) -> Option<MatchReason> {
    if manifest.files.is_empty() || manifest.piece_length == Some(0) {
        return Some(MatchReason::UnsupportedTorrent);
    }
    let mut ordinals = BTreeSet::new();
    let mut paths = BTreeSet::new();
    for file in &manifest.files {
        if !ordinals.insert(file.ordinal) || unsafe_path(&file.path) {
            return Some(MatchReason::UnsafeTorrentPath);
        }
        let normalized: String = file.path.nfc().collect();
        if !paths.insert(normalized) {
            return Some(MatchReason::UnsafeTorrentPath);
        }
    }
    for path in &paths {
        let mut prefix = String::new();
        let component_count = path.split('/').count();
        for (index, component) in path.split('/').enumerate() {
            if index > 0 {
                prefix.push('/');
            }
            prefix.push_str(component);
            if index + 1 < component_count && paths.contains(&prefix) {
                return Some(MatchReason::UnsafeTorrentPath);
            }
        }
    }
    None
}

fn unsafe_path(path: &str) -> bool {
    path.is_empty()
        || path.starts_with('/')
        || path.starts_with("//")
        || path.contains('\\')
        || path.contains('\0')
        || path.as_bytes().get(1) == Some(&b':')
        || path.split('/').any(|component| {
            component.is_empty()
                || matches!(component, "." | "..")
                || matches!(component.nfc().collect::<String>().as_str(), "." | "..")
        })
}

fn required(file: &TorrentFile, policy: &MatchingPolicy) -> bool {
    !file.padding && !optional(&file.path, policy)
}

fn primary(file: &TorrentFile, policy: &MatchingPolicy) -> bool {
    !file.padding && !optional(&file.path, policy) && primary_path(&file.path, policy)
}

fn primary_path(path: &str, policy: &MatchingPolicy) -> bool {
    let upper = path.to_ascii_uppercase();
    upper.starts_with("BDMV/")
        || upper.contains("/BDMV/")
        || upper.starts_with("VIDEO_TS/")
        || upper.contains("/VIDEO_TS/")
        || extension(path).is_some_and(|extension| {
            policy
                .primary_video_extensions
                .iter()
                .any(|configured| configured.eq_ignore_ascii_case(extension))
        })
}

fn optional(path: &str, policy: &MatchingPolicy) -> bool {
    let basename = basename(path);
    let stem = stem(basename);
    stem.eq_ignore_ascii_case("sample")
        || extension(path).is_some_and(|extension| {
            policy
                .optional_extensions
                .iter()
                .any(|configured| configured.eq_ignore_ascii_case(extension))
        })
        || path.split('/').any(|component| {
            policy
                .optional_path_components
                .iter()
                .any(|configured| configured.eq_ignore_ascii_case(component))
        })
}

fn basename(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

fn parent_basename(path: &str) -> Option<&str> {
    path.rsplit_once('/')
        .map(|(parent, _)| parent.rsplit('/').next().unwrap_or(parent))
}

fn extension(path: &str) -> Option<&str> {
    basename(path)
        .rsplit_once('.')
        .map(|(_, extension)| extension)
}

fn stem(path: &str) -> &str {
    path.rsplit_once('.').map_or(path, |(stem, _)| stem)
}

fn base_evidence(request: &MatchRequest<'_>) -> MatchEvidence {
    MatchEvidence {
        candidate_files: request.candidate.files.len(),
        required_files: request
            .candidate
            .files
            .iter()
            .filter(|file| required(file, request.policy))
            .count(),
        primary_files: request
            .candidate
            .files
            .iter()
            .filter(|file| primary(file, request.policy))
            .count(),
        ..MatchEvidence::default()
    }
}

fn required_bytes(request: &MatchRequest<'_>) -> u64 {
    request
        .candidate
        .files
        .iter()
        .filter(|file| required(file, request.policy))
        .map(|file| file.size)
        .sum()
}

fn file_failure_reason(
    candidates: &[&TorrentFile],
    sources: &[(SourceId, &LocalSourceFile)],
    policy: &MatchingPolicy,
) -> MatchReason {
    if candidates.iter().any(|candidate| {
        required(candidate, policy)
            && sources.iter().any(|(_, source)| {
                (candidate.path == source.path
                    || basename(&candidate.path) == basename(&source.path))
                    && candidate.size != source.size
            })
    }) {
        MatchReason::FileSizeConflict
    } else {
        MatchReason::NoPrimaryVideoOverlap
    }
}

fn best_reason(reasons: &[MatchReason]) -> MatchReason {
    const PRIORITY: &[MatchReason] = &[
        MatchReason::MatcherBudgetExceeded,
        MatchReason::SeriesConflict,
        MatchReason::MediaTypeConflict,
        MatchReason::SeasonConflict,
        MatchReason::EpisodeConflict,
        MatchReason::DateConflict,
        MatchReason::YearConflict,
        MatchReason::TitleConflict,
        MatchReason::FileSizeConflict,
        MatchReason::NoPrimaryVideoOverlap,
    ];
    PRIORITY
        .iter()
        .find(|reason| reasons.contains(reason))
        .copied()
        .unwrap_or(MatchReason::NoPlausibleSource)
}

fn rejection(reason: MatchReason, evidence: MatchEvidence) -> MatchDecision {
    MatchDecision {
        outcome: MatchOutcome::Rejected,
        mode: None,
        reason,
        source_ids: Vec::new(),
        mappings: Vec::new(),
        mapped_bytes: 0,
        missing_bytes: 0,
        present_ratio: Ratio::ZERO,
        requires_recheck: false,
        evidence,
    }
}

fn no_match(
    reason: MatchReason,
    evidence: MatchEvidence,
    request: &MatchRequest<'_>,
) -> MatchDecision {
    MatchDecision {
        outcome: MatchOutcome::NoMatch,
        mode: None,
        reason,
        source_ids: Vec::new(),
        mappings: Vec::new(),
        mapped_bytes: 0,
        missing_bytes: required_bytes(request),
        present_ratio: Ratio::ZERO,
        requires_recheck: false,
        evidence,
    }
}

#[cfg(test)]
mod tests {
    use sporos_model::{
        ArrIdentity, ArrKind, LocalSourceFile, MatchingPolicy, SourceId, TorrentFile,
    };

    use crate::parse_release;

    use super::{AssignmentError, assign, compatible, hungarian};

    #[test]
    fn arr_identity_establishes_title_identity() {
        let mut candidate = parse_release("Localized Series S01E01");
        let mut source = parse_release("Original Series S01E01");
        let identity = ArrIdentity {
            kind: ArrKind::Series,
            instance: "sonarr".to_owned(),
            entity_id: 42,
            tvdb_id: Some(123),
            tmdb_id: None,
            imdb_id: None,
        };
        candidate.arr_identity = Some(identity.clone());
        source.arr_identity = Some(identity);

        assert!(compatible(&candidate, &source, false).is_ok());
    }

    #[test]
    fn rectangular_assignment_can_leave_rows_unmatched() {
        let assignment = hungarian(&[vec![-10, 0, 0], vec![-5, 0, 0]]);
        assert_eq!(assignment[0], 0);
        assert_ne!(assignment[1], 0);
    }

    #[test]
    fn repeated_episode_sizes_split_into_sparse_components() {
        let candidates: Vec<_> = (0..50)
            .map(|index| candidate(index, &format!("Show.S01E{index:02}.mkv"), 100))
            .collect();
        let sources: Vec<_> = (0..50)
            .map(|index| source(index, &format!("Other.Show.S01E{index:02}.mkv"), 100))
            .collect();
        let candidate_refs: Vec<_> = candidates.iter().collect();
        let source_refs: Vec<_> = sources
            .iter()
            .map(|file| (SourceId::from_bytes([1; 16]), file))
            .collect();
        let policy = MatchingPolicy {
            max_assignment_component_files: 4,
            ..MatchingPolicy::default()
        };

        let assignment = assign(&candidate_refs, &source_refs, &policy).expect("sparse assignment");
        assert_eq!(assignment.pairs.len(), 50);
    }

    #[test]
    fn thousands_of_identical_sidecars_stop_at_the_edge_budget() {
        let candidates: Vec<_> = (0..2_000)
            .map(|index| candidate(index, &format!("candidate-{index}/same.srt"), 10))
            .collect();
        let sources: Vec<_> = (0..2_000)
            .map(|index| source(index, &format!("source-{index}/same.srt"), 10))
            .collect();
        let candidate_refs: Vec<_> = candidates.iter().collect();
        let source_refs: Vec<_> = sources
            .iter()
            .map(|file| (SourceId::from_bytes([1; 16]), file))
            .collect();
        let policy = MatchingPolicy {
            max_candidate_edges: 10_000,
            ..MatchingPolicy::default()
        };

        assert!(matches!(
            assign(&candidate_refs, &source_refs, &policy),
            Err(AssignmentError::BudgetExceeded)
        ));
    }

    #[test]
    fn assignment_file_budget_is_checked_before_bucketing() {
        let candidates: Vec<_> = (0..1_000)
            .map(|index| candidate(index, &format!("candidate-{index}.mkv"), 10))
            .collect();
        let sources: Vec<_> = (0..5_000)
            .map(|index| source(index, &format!("source-{index}.mkv"), 11))
            .collect();
        let candidate_refs: Vec<_> = candidates.iter().collect();
        let source_refs: Vec<_> = sources
            .iter()
            .map(|file| (SourceId::from_bytes([1; 16]), file))
            .collect();
        let policy = MatchingPolicy {
            max_assignment_files: 1_100,
            ..MatchingPolicy::default()
        };

        assert!(matches!(
            assign(&candidate_refs, &source_refs, &policy),
            Err(AssignmentError::BudgetExceeded)
        ));
    }

    #[test]
    fn ambiguous_components_stop_at_the_component_budget() {
        let candidates: Vec<_> = (0..5)
            .map(|index| candidate(index, &format!("candidate-{index}.mkv"), 10))
            .collect();
        let sources: Vec<_> = (0..5)
            .map(|index| source(index, &format!("source-{index}.mkv"), 10))
            .collect();
        let candidate_refs: Vec<_> = candidates.iter().collect();
        let source_refs: Vec<_> = sources
            .iter()
            .map(|file| (SourceId::from_bytes([1; 16]), file))
            .collect();
        let policy = MatchingPolicy {
            max_assignment_component_files: 8,
            ..MatchingPolicy::default()
        };

        assert!(matches!(
            assign(&candidate_refs, &source_refs, &policy),
            Err(AssignmentError::BudgetExceeded)
        ));
    }

    fn candidate(ordinal: u32, path: &str, size: u64) -> TorrentFile {
        TorrentFile {
            ordinal,
            path: path.to_owned(),
            size,
            padding: false,
        }
    }

    fn source(id: u32, path: &str, size: u64) -> LocalSourceFile {
        LocalSourceFile {
            id: u64::from(id),
            path: path.to_owned(),
            size,
            device_id: None,
            inode: None,
        }
    }
}
