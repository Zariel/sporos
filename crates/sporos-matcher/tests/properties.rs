use proptest::prelude::*;
use sporos_matcher::{MatchRequest, Matcher, PureMatcher, parse_release};
use sporos_model::{
    InfoHashes, LocalSourceFile, LocalSourceManifest, MatchOutcome, MatchReason, MatchingPolicy,
    SourceId, SourceKind, TorrentFile, TorrentManifest,
};

proptest! {
    #[test]
    fn logical_input_permutations_do_not_change_decisions(
        sizes in prop::collection::btree_set(1_u64..1_000_000, 1..=12)
    ) {
        let sizes: Vec<_> = sizes.into_iter().collect();
        let candidate = manifest(&sizes);
        let release = parse_release("Permutation.Show.S01");
        let first = source(1, &sizes, false);
        let second = source(2, &sizes, true);
        let policy = MatchingPolicy::default();
        let matcher = PureMatcher;

        let sources = vec![first.clone(), second.clone()];
        let forward = matcher.evaluate(&MatchRequest {
            candidate: &candidate,
            candidate_release: &release,
            sources: &sources,
            policy: &policy,
        });
        let sources = vec![second, first];
        let reverse = matcher.evaluate(&MatchRequest {
            candidate: &candidate,
            candidate_release: &release,
            sources: &sources,
            policy: &policy,
        });
        prop_assert_eq!(&forward, &reverse);

        let mut candidate_reversed = candidate.clone();
        candidate_reversed.files.reverse();
        let reordered = matcher.evaluate(&MatchRequest {
            candidate: &candidate_reversed,
            candidate_release: &release,
            sources: &sources,
            policy: &policy,
        });
        prop_assert_eq!(&forward, &reordered);
    }

    #[test]
    fn successful_mappings_are_exact_size_and_one_to_one(
        sizes in prop::collection::btree_set(1_u64..1_000_000, 1..=16)
    ) {
        let sizes: Vec<_> = sizes.into_iter().collect();
        let candidate = manifest(&sizes);
        let release = parse_release("Permutation.Show.S01");
        let sources = vec![source(1, &sizes, false)];
        let policy = MatchingPolicy::default();
        let decision = PureMatcher.evaluate(&MatchRequest {
            candidate: &candidate,
            candidate_release: &release,
            sources: &sources,
            policy: &policy,
        });
        prop_assert_eq!(decision.outcome, MatchOutcome::Match);
        let source_files: std::collections::BTreeSet<_> = decision
            .mappings
            .iter()
            .map(|mapping| (mapping.source_id, mapping.source_file_id))
            .collect();
        prop_assert_eq!(source_files.len(), decision.mappings.len());
        for mapping in decision.mappings {
            let candidate = candidate.files.iter()
                .find(|file| file.ordinal == mapping.candidate_ordinal)
                .expect("mapped candidate exists");
            let source = sources[0].files.iter()
                .find(|file| file.id == mapping.source_file_id)
                .expect("mapped source exists");
            prop_assert_eq!(candidate.size, source.size);
            prop_assert_eq!(mapping.size, candidate.size);
        }
    }
}

#[test]
fn equal_best_paths_are_rejected() {
    let candidate = manifest(&[1_000]);
    let release = parse_release("Permutation.Show.S01");
    let mut source = source(1, &[1_000], true);
    source.files[0].path = "other/episode-a.mkv".to_owned();
    source.files.push(LocalSourceFile {
        id: 2,
        path: "other/episode-b.mkv".to_owned(),
        size: 1_000,
        device_id: Some(1),
        inode: Some(2),
    });
    let decision = PureMatcher.evaluate(&MatchRequest {
        candidate: &candidate,
        candidate_release: &release,
        sources: &[source],
        policy: &MatchingPolicy::default(),
    });
    assert_eq!(decision.outcome, MatchOutcome::Rejected);
    assert_eq!(decision.reason, MatchReason::AmbiguousFileMapping);
}

#[test]
fn aggregate_budget_rejects_many_large_similar_sources() {
    let candidate = manifest(&(1..=20).collect::<Vec<_>>());
    let release = parse_release("Permutation.Show.S01");
    let sources: Vec<_> = (1..=64)
        .map(|seed| source(seed, &vec![1_000; 100], false))
        .collect();
    let decision = PureMatcher.evaluate(&MatchRequest {
        candidate: &candidate,
        candidate_release: &release,
        sources: &sources,
        policy: &MatchingPolicy::default(),
    });

    assert_eq!(decision.outcome, MatchOutcome::Rejected);
    assert_eq!(decision.reason, MatchReason::MatcherBudgetExceeded);
}

fn manifest(sizes: &[u64]) -> TorrentManifest {
    TorrentManifest {
        hashes: InfoHashes::default(),
        files: sizes
            .iter()
            .enumerate()
            .map(|(index, size)| TorrentFile {
                ordinal: index as u32,
                path: format!("Permutation.Show.S01E{:02}.mkv", index + 1),
                size: *size,
                padding: false,
            })
            .collect(),
        piece_length: Some(1_048_576),
        piece_files: Vec::new(),
    }
}

fn source(seed: u8, sizes: &[u64], reverse: bool) -> LocalSourceManifest {
    let mut files: Vec<_> = sizes
        .iter()
        .enumerate()
        .map(|(index, size)| LocalSourceFile {
            id: index as u64,
            path: format!("Permutation.Show.S01E{:02}.mkv", index + 1),
            size: *size,
            device_id: Some(u64::from(seed)),
            inode: Some(index as u64),
        })
        .collect();
    if reverse {
        files.reverse();
    }
    LocalSourceManifest {
        id: SourceId::from_bytes([seed; 16]),
        kind: SourceKind::QbittorrentTorrent,
        release: parse_release("Permutation.Show.S01"),
        hashes: InfoHashes::default(),
        files,
        available: true,
    }
}
