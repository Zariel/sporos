use serde::Deserialize;
use sporos_matcher::{MatchRequest, Matcher, PureMatcher, normalize_title, parse_release};
use sporos_model::{
    ArrIdentity, ArrKind, InfoHashes, LocalSourceFile, LocalSourceManifest, MatchMode,
    MatchOutcome, MatchReason, MatchingPolicy, SourceId, SourceKind, TorrentFile, TorrentManifest,
};

#[derive(Debug, Deserialize)]
struct Case {
    name: String,
    candidate_release: String,
    #[serde(default)]
    candidate_alternate_titles: Vec<String>,
    candidate_arr: Option<i64>,
    candidate_hash: Option<u8>,
    candidate_hash_v2: Option<u8>,
    candidate_files: Vec<FileFixture>,
    sources: Vec<SourceFixture>,
    expect: (MatchOutcome, Option<MatchMode>, MatchReason, usize),
    expect_source: Option<u8>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum FileFixture {
    File((String, u64)),
    Flagged((String, u64, bool)),
}

impl FileFixture {
    fn parts(&self) -> (&str, u64, bool) {
        match self {
            Self::File((path, size)) => (path, *size, false),
            Self::Flagged((path, size, padding)) => (path, *size, *padding),
        }
    }
}

#[derive(Debug, Deserialize)]
struct SourceFixture {
    release: String,
    hash: Option<u8>,
    hash_v2: Option<u8>,
    arr: Option<i64>,
    kind: Option<SourceKind>,
    #[serde(default = "available")]
    available: bool,
    files: Vec<FileFixture>,
}

#[test]
fn approved_corpus() {
    let cases: Vec<Case> =
        serde_json::from_str(include_str!("fixtures/cases.json")).expect("valid matcher corpus");
    for case in cases {
        let candidate = manifest(
            &case.candidate_files,
            case.candidate_hash,
            case.candidate_hash_v2,
        );
        let mut candidate_release = parse_release(&case.candidate_release);
        candidate_release.alternate_titles = case
            .candidate_alternate_titles
            .iter()
            .map(|title| normalize_title(title))
            .collect();
        candidate_release.arr_identity = case.candidate_arr.map(arr_identity);
        let sources: Vec<_> = case
            .sources
            .iter()
            .enumerate()
            .map(|(index, source)| {
                let mut release = parse_release(&source.release);
                release.arr_identity = source.arr.map(arr_identity);
                LocalSourceManifest {
                    id: source_id(index),
                    kind: source.kind.unwrap_or(SourceKind::QbittorrentTorrent),
                    release,
                    hashes: hashes(source.hash, source.hash_v2),
                    files: source
                        .files
                        .iter()
                        .enumerate()
                        .map(|(file_index, file)| {
                            let (path, size, _) = file.parts();
                            LocalSourceFile {
                                id: file_index as u64,
                                path: path.to_owned(),
                                size,
                                device_id: Some(index as u64 + 1),
                                inode: Some(file_index as u64 + 1),
                            }
                        })
                        .collect(),
                    available: source.available,
                }
            })
            .collect();
        let policy = MatchingPolicy::default();
        let decision = PureMatcher.evaluate(&MatchRequest {
            candidate: &candidate,
            candidate_release: &candidate_release,
            sources: &sources,
            policy: &policy,
        });
        assert_eq!(decision.outcome, case.expect.0, "{} outcome", case.name);
        assert_eq!(decision.mode, case.expect.1, "{} mode", case.name);
        assert_eq!(decision.reason, case.expect.2, "{} reason", case.name);
        assert_eq!(
            decision.mappings.len(),
            case.expect.3,
            "{} mapping count",
            case.name
        );
        if let Some(expected) = case.expect_source {
            assert_eq!(
                decision.source_ids,
                vec![source_id(usize::from(expected - 1))],
                "{} selected source",
                case.name
            );
        }
    }
}

fn manifest(files: &[FileFixture], hash: Option<u8>, hash_v2: Option<u8>) -> TorrentManifest {
    TorrentManifest {
        hashes: hashes(hash, hash_v2),
        files: files
            .iter()
            .enumerate()
            .map(|(index, file)| {
                let (path, size, padding) = file.parts();
                TorrentFile {
                    ordinal: index as u32,
                    path: path.to_owned(),
                    size,
                    padding,
                }
            })
            .collect(),
        piece_length: Some(1_048_576),
    }
}

fn hashes(v1: Option<u8>, v2: Option<u8>) -> InfoHashes {
    InfoHashes {
        v1: v1.map(|seed| [seed; 20]),
        v2: v2.map(|seed| [seed; 32]),
    }
}

fn available() -> bool {
    true
}

fn source_id(index: usize) -> SourceId {
    let mut bytes = [0_u8; 16];
    bytes[15] = index as u8 + 1;
    SourceId::from_bytes(bytes)
}

fn arr_identity(entity_id: i64) -> ArrIdentity {
    ArrIdentity {
        kind: ArrKind::Series,
        instance: "fixture".to_owned(),
        entity_id,
    }
}
