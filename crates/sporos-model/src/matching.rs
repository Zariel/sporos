use serde::{Deserialize, Serialize};

use crate::SourceId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VideoKind {
    Movie,
    Episode,
    SeasonPack,
    DateEpisode,
    AbsoluteEpisode,
    Disc,
    UnknownVideo,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Date {
    pub year: u16,
    pub month: u8,
    pub day: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NormalizedTitle(String);

impl NormalizedTitle {
    pub fn from_normalized(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArrKind {
    Movie,
    Series,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArrIdentity {
    pub kind: ArrKind,
    pub instance: String,
    pub entity_id: i64,
    #[serde(default)]
    pub tvdb_id: Option<i64>,
    #[serde(default)]
    pub tmdb_id: Option<i64>,
    #[serde(default)]
    pub imdb_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseDescriptor {
    pub kind: VideoKind,
    pub primary_title: NormalizedTitle,
    #[serde(default)]
    pub alternate_titles: Vec<NormalizedTitle>,
    pub year: Option<u16>,
    pub season: Option<u16>,
    pub episode: Option<u16>,
    pub episode_end: Option<u16>,
    pub absolute_episode: Option<u32>,
    pub air_date: Option<Date>,
    pub edition: Option<String>,
    pub source: Option<String>,
    pub resolution: Option<String>,
    pub video_codec: Option<String>,
    pub hdr: Option<String>,
    pub audio: Option<String>,
    pub release_group: Option<String>,
    pub arr_identity: Option<ArrIdentity>,
}

impl ReleaseDescriptor {
    pub fn unknown(title: NormalizedTitle) -> Self {
        Self {
            kind: VideoKind::UnknownVideo,
            primary_title: title,
            alternate_titles: Vec::new(),
            year: None,
            season: None,
            episode: None,
            episode_end: None,
            absolute_episode: None,
            air_date: None,
            edition: None,
            source: None,
            resolution: None,
            video_codec: None,
            hdr: None,
            audio: None,
            release_group: None,
            arr_identity: None,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InfoHashes {
    pub v1: Option<[u8; 20]>,
    pub v2: Option<[u8; 32]>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TorrentFile {
    pub ordinal: u32,
    pub path: String,
    pub size: u64,
    #[serde(default)]
    pub padding: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TorrentPieceFile {
    pub file_ordinal: Option<u32>,
    pub offset: u64,
    pub size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TorrentManifest {
    pub hashes: InfoHashes,
    pub files: Vec<TorrentFile>,
    pub piece_length: Option<u64>,
    #[serde(default)]
    pub piece_files: Vec<TorrentPieceFile>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceKind {
    QbittorrentTorrent,
    DataDirectoryRelease,
    DataDirectoryFile,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalSourceFile {
    pub id: u64,
    pub path: String,
    pub size: u64,
    pub device_id: Option<u64>,
    pub inode: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalSourceManifest {
    pub id: SourceId,
    pub kind: SourceKind,
    pub release: ReleaseDescriptor,
    pub hashes: InfoHashes,
    pub files: Vec<LocalSourceFile>,
    pub available: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MatchingPolicy {
    pub allow_flexible: bool,
    pub allow_partial: bool,
    pub allow_season_from_episodes: bool,
    pub primary_video_extensions: Vec<String>,
    pub optional_extensions: Vec<String>,
    pub optional_path_components: Vec<String>,
}

impl Default for MatchingPolicy {
    fn default() -> Self {
        Self {
            allow_flexible: true,
            allow_partial: true,
            allow_season_from_episodes: true,
            primary_video_extensions: [
                "mkv", "mp4", "m4v", "avi", "ts", "m2ts", "mov", "wmv", "webm", "iso",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect(),
            optional_extensions: [
                "nfo", "txt", "srt", "ass", "ssa", "sub", "idx", "jpg", "jpeg", "png", "sfv",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect(),
            optional_path_components: ["sample", "proof", "screenshots"]
                .into_iter()
                .map(str::to_owned)
                .collect(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MatchOutcome {
    Match,
    NoMatch,
    AlreadyPresent,
    Rejected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MatchMode {
    Strict,
    Flexible,
    Partial,
    SeasonFromEpisodes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MatchReason {
    MatchStrict,
    MatchFlexible,
    MatchPartial,
    MatchSeasonFromEpisodes,
    AlreadyPresent,
    NoPlausibleSource,
    MediaTypeConflict,
    TitleConflict,
    YearConflict,
    SeriesConflict,
    SeasonConflict,
    EpisodeConflict,
    DateConflict,
    NoPrimaryVideoOverlap,
    FileSizeConflict,
    AmbiguousFileMapping,
    UnsupportedTorrent,
    UnsafeTorrentPath,
    HardlinkDeviceMismatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Ratio(u32);

impl Ratio {
    pub const ZERO: Self = Self(0);
    pub const ONE: Self = Self(1_000_000);

    pub const fn from_ppm(ppm: u32) -> Option<Self> {
        if ppm <= 1_000_000 {
            Some(Self(ppm))
        } else {
            None
        }
    }

    pub const fn as_ppm(self) -> u32 {
        self.0
    }

    pub fn from_bytes(present: u64, total: u64) -> Self {
        if total == 0 {
            return Self::ZERO;
        }
        let ppm = (u128::from(present) * 1_000_000 / u128::from(total)) as u32;
        Self(ppm)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileMapping {
    pub candidate_ordinal: u32,
    pub source_id: SourceId,
    pub source_file_id: u64,
    pub candidate_path: String,
    pub source_path: String,
    pub size: u64,
    pub score: u32,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MatchEvidence {
    pub candidate_files: usize,
    pub required_files: usize,
    pub primary_files: usize,
    pub mapped_files: usize,
    pub mapped_primary_files: usize,
    pub mapped_primary_bytes: u64,
    pub exact_path_mappings: usize,
    pub compatible_sources: usize,
    pub qbit_sources: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MatchDecision {
    pub outcome: MatchOutcome,
    pub mode: Option<MatchMode>,
    pub reason: MatchReason,
    pub source_ids: Vec<SourceId>,
    pub mappings: Vec<FileMapping>,
    pub mapped_bytes: u64,
    pub missing_bytes: u64,
    pub present_ratio: Ratio,
    pub requires_recheck: bool,
    pub evidence: MatchEvidence,
}

#[cfg(test)]
mod tests {
    use super::Ratio;

    #[test]
    fn ratio_uses_bounded_integer_arithmetic() {
        assert_eq!(Ratio::from_bytes(1, 3).as_ppm(), 333_333);
        assert_eq!(Ratio::from_bytes(u64::MAX, u64::MAX), Ratio::ONE);
        assert_eq!(Ratio::from_bytes(0, 0), Ratio::ZERO);
        assert!(Ratio::from_ppm(1_000_001).is_none());
    }
}
