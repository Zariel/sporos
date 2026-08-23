use std::{hint::black_box, time::Instant};

use sporos_matcher::{MatchRequest, Matcher, PureMatcher, parse_release};
use sporos_model::{
    InfoHashes, LocalSourceFile, LocalSourceManifest, MatchingPolicy, SourceId, SourceKind,
    TorrentFile, TorrentManifest,
};

fn main() {
    let candidate = TorrentManifest {
        hashes: InfoHashes::default(),
        files: (0..20)
            .map(|index| TorrentFile {
                ordinal: index,
                path: format!("Benchmark.Show.S01E{:02}.mkv", index + 1),
                size: 1_000_000 + u64::from(index),
                padding: false,
            })
            .collect(),
        piece_length: Some(1_048_576),
    };
    let release = parse_release("Benchmark.Show.S01");
    let sources: Vec<_> = (0..8)
        .map(|source| LocalSourceManifest {
            id: SourceId::from_bytes([source + 1; 16]),
            kind: SourceKind::QbittorrentTorrent,
            release: release.clone(),
            hashes: InfoHashes::default(),
            files: (0..20)
                .map(|index| LocalSourceFile {
                    id: index,
                    path: format!("Benchmark.Show.S01E{:02}.mkv", index + 1),
                    size: 1_000_000 + index,
                    device_id: Some(u64::from(source)),
                    inode: Some(index),
                })
                .collect(),
            available: true,
        })
        .collect();
    let policy = MatchingPolicy::default();
    let matcher = PureMatcher;
    let request = MatchRequest {
        candidate: &candidate,
        candidate_release: &release,
        sources: &sources,
        policy: &policy,
    };

    let iterations = 1_000;
    let started = Instant::now();
    for _ in 0..iterations {
        black_box(matcher.evaluate(black_box(&request)));
    }
    let elapsed = started.elapsed();
    println!(
        "matcher_20_files_8_sources: {iterations} iterations in {elapsed:?} ({:?}/iteration)",
        elapsed / iterations
    );
}
