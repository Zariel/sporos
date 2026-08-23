//! Stable domain types shared by Sporos components.

mod id;
mod matching;

pub use id::{PolicySnapshotId, SourceId, TaskId, TaskKey};
pub use matching::{
    ArrIdentity, ArrKind, Date, FileMapping, InfoHashes, LocalSourceFile, LocalSourceManifest,
    MatchDecision, MatchEvidence, MatchMode, MatchOutcome, MatchReason, MatchingPolicy,
    NormalizedTitle, Ratio, ReleaseDescriptor, SourceKind, TorrentFile, TorrentManifest, VideoKind,
};
