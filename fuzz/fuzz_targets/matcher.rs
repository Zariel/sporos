#![no_main]

use libfuzzer_sys::fuzz_target;
use serde::Deserialize;
use sporos_matcher::{MatchRequest, Matcher, PureMatcher};
use sporos_model::{LocalSourceManifest, MatchingPolicy, ReleaseDescriptor, TorrentManifest};

#[derive(Deserialize)]
struct Input {
    candidate: TorrentManifest,
    candidate_release: ReleaseDescriptor,
    sources: Vec<LocalSourceManifest>,
    policy: MatchingPolicy,
}

fuzz_target!(|bytes: &[u8]| {
    let Ok(input) = serde_json::from_slice::<Input>(bytes) else {
        return;
    };
    if input.candidate.files.len() > 100 || input.sources.len() > 20 {
        return;
    }
    let file_count: usize = input.sources.iter().map(|source| source.files.len()).sum();
    if file_count > 200 {
        return;
    }
    let _ = PureMatcher.evaluate(&MatchRequest {
        candidate: &input.candidate,
        candidate_release: &input.candidate_release,
        sources: &input.sources,
        policy: &input.policy,
    });
});
