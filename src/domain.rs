use std::fmt;
use std::path::{Path, PathBuf};
use std::str::FromStr;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DomainError {
    EmptyField { field: &'static str },
    InvalidInfoHash { value: String },
    InvalidPath { field: &'static str, value: PathBuf },
    InvalidRatio,
    EmptyFiles,
}

impl fmt::Display for DomainError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyField { field } => write!(formatter, "{field} must not be empty"),
            Self::InvalidInfoHash { value } => {
                write!(formatter, "{value} is not a valid info hash")
            }
            Self::InvalidPath { field, value } => {
                write!(
                    formatter,
                    "{field} is not a valid relative path: {}",
                    value.display()
                )
            }
            Self::InvalidRatio => {
                write!(formatter, "match ratio must be finite and between 0 and 1")
            }
            Self::EmptyFiles => {
                write!(formatter, "torrent metafile must contain at least one file")
            }
        }
    }
}

impl std::error::Error for DomainError {}

type DomainResult<T> = Result<T, DomainError>;

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
struct NonEmptyText(String);

impl NonEmptyText {
    fn new(field: &'static str, value: impl Into<String>) -> DomainResult<Self> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(DomainError::EmptyField { field });
        }

        Ok(Self(value))
    }

    fn as_str(&self) -> &str {
        &self.0
    }
}

macro_rules! text_newtype {
    ($name:ident, $field:literal) => {
        #[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
        pub struct $name(NonEmptyText);

        impl $name {
            pub fn new(value: impl Into<String>) -> DomainResult<Self> {
                NonEmptyText::new($field, value).map(Self)
            }

            pub fn as_str(&self) -> &str {
                self.0.as_str()
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
            }
        }
    };
}

text_newtype!(CandidateGuid, "candidate guid");
text_newtype!(ClientHost, "client host");
text_newtype!(DependencyName, "dependency name");
text_newtype!(DisplayName, "display name");
text_newtype!(DownloadUrl, "download url");
text_newtype!(FileName, "file name");
text_newtype!(ItemTitle, "item title");
text_newtype!(JobName, "job name");
text_newtype!(ReasonText, "reason");
text_newtype!(SourceKey, "source key");
text_newtype!(TrackerName, "tracker name");

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct InfoHash(String);

impl InfoHash {
    pub fn new(value: impl Into<String>) -> DomainResult<Self> {
        let value = value.into();
        let trimmed = value.trim();
        let is_supported_length = matches!(trimmed.len(), 40 | 64);
        let is_hex = trimmed.bytes().all(|byte| byte.is_ascii_hexdigit());

        if !is_supported_length || !is_hex {
            return Err(DomainError::InvalidInfoHash { value });
        }

        Ok(Self(trimmed.to_ascii_lowercase()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn algorithm(&self) -> InfoHashAlgorithm {
        if self.0.len() == 40 {
            InfoHashAlgorithm::Sha1
        } else {
            InfoHashAlgorithm::Sha256
        }
    }
}

impl AsRef<str> for InfoHash {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for InfoHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for InfoHash {
    type Err = DomainError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum InfoHashAlgorithm {
    Sha1,
    Sha256,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct ByteSize(u64);

impl ByteSize {
    pub const fn new(bytes: u64) -> Self {
        Self(bytes)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct FileIndex(u32);

impl FileIndex {
    pub const fn new(index: u32) -> Self {
        Self(index)
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MatchRatio(f64);

impl MatchRatio {
    pub fn new(value: f64) -> DomainResult<Self> {
        if !(0.0..=1.0).contains(&value) || !value.is_finite() {
            return Err(DomainError::InvalidRatio);
        }

        Ok(Self(value))
    }

    pub const fn get(self) -> f64 {
        self.0
    }
}

macro_rules! positive_id {
    ($name:ident, $field:literal) => {
        #[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
        pub struct $name(u64);

        impl $name {
            pub fn new(value: u64) -> DomainResult<Self> {
                if value == 0 {
                    return Err(DomainError::EmptyField { field: $field });
                }

                Ok(Self(value))
            }

            pub const fn get(self) -> u64 {
                self.0
            }
        }
    };
}

positive_id!(IndexerId, "indexer id");
positive_id!(LocalItemId, "local item id");
positive_id!(RemoteCandidateId, "remote candidate id");

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum MediaType {
    Episode,
    SeasonPack,
    Movie,
    Anime,
    Video,
    Audio,
    Book,
    Archive,
    Unknown,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum LocalItemSource {
    Client {
        client_host: ClientHost,
        source_key: SourceKey,
    },
    TorrentCache {
        path: PathBuf,
    },
    DataRoot {
        path: PathBuf,
    },
    Virtual {
        source_key: SourceKey,
    },
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct LocalItem {
    pub id: Option<LocalItemId>,
    pub source: LocalItemSource,
    pub title: ItemTitle,
    pub display_name: DisplayName,
    pub media_type: MediaType,
    pub info_hash: Option<InfoHash>,
    pub path: Option<PathBuf>,
    pub save_path: Option<PathBuf>,
    pub total_size: ByteSize,
    pub mtime_ms: Option<i64>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct LocalFile {
    pub item_id: Option<LocalItemId>,
    pub relative_path: PathBuf,
    pub file_name: FileName,
    pub size: ByteSize,
    pub mtime_ms: Option<i64>,
    pub file_index: FileIndex,
}

impl LocalFile {
    pub fn new(
        item_id: Option<LocalItemId>,
        relative_path: PathBuf,
        size: ByteSize,
        file_index: FileIndex,
    ) -> DomainResult<Self> {
        let file_name = file_name_from_relative_path("local file relative path", &relative_path)?;

        Ok(Self {
            item_id,
            relative_path,
            file_name,
            size,
            mtime_ms: None,
            file_index,
        })
    }

    pub const fn with_mtime_ms(mut self, mtime_ms: Option<i64>) -> Self {
        self.mtime_ms = mtime_ms;
        self
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct RemoteCandidate {
    pub id: Option<RemoteCandidateId>,
    pub indexer_id: IndexerId,
    pub guid: CandidateGuid,
    pub download_url: DownloadUrl,
    pub title: ItemTitle,
    pub tracker: TrackerName,
    pub size: Option<ByteSize>,
    pub published_at_ms: Option<i64>,
    pub info_hash: Option<InfoHash>,
    pub torrent_cache_path: Option<PathBuf>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct TorrentFile {
    pub relative_path: PathBuf,
    pub file_name: FileName,
    pub size: ByteSize,
    pub file_index: FileIndex,
}

impl TorrentFile {
    pub fn new(
        relative_path: PathBuf,
        size: ByteSize,
        file_index: FileIndex,
    ) -> DomainResult<Self> {
        let file_name = file_name_from_relative_path("torrent file relative path", &relative_path)?;

        Ok(Self {
            relative_path,
            file_name,
            size,
            file_index,
        })
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct TorrentMetafile {
    pub info_hash: InfoHash,
    pub name: DisplayName,
    pub files: Vec<TorrentFile>,
    pub total_size: ByteSize,
    pub piece_length: Option<ByteSize>,
}

impl TorrentMetafile {
    pub fn new(
        info_hash: InfoHash,
        name: DisplayName,
        files: Vec<TorrentFile>,
    ) -> DomainResult<Self> {
        Self::new_with_piece_length(info_hash, name, files, None)
    }

    pub fn new_with_piece_length(
        info_hash: InfoHash,
        name: DisplayName,
        files: Vec<TorrentFile>,
        piece_length: Option<ByteSize>,
    ) -> DomainResult<Self> {
        if files.is_empty() {
            return Err(DomainError::EmptyFiles);
        }

        let total_size = ByteSize::new(files.iter().map(|file| file.size.get()).sum());

        Ok(Self {
            info_hash,
            name,
            files,
            total_size,
            piece_length,
        })
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum MatchDecision {
    Exact,
    SizeOnly,
    Partial,
    NoMatch,
    Rejected,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum DecisionReason {
    AlreadyExists,
    BlockedRelease,
    CandidateInvalid,
    FileTreeMatched,
    FuzzySizeMismatch,
    InfoHashAlreadyExists,
    MissingDownloadLink,
    NameMismatch,
    PartialOverlap,
    PolicyRejected,
    ProperRepackMismatch,
    ReleaseGroupMismatch,
    ResolutionMismatch,
    SameInfoHash,
    SingleEpisodeForSeasonPack,
    SizeMatched,
    SourceIncomplete,
    SourceMismatch,
    UnsupportedLayout,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CandidateAssessment {
    pub decision: MatchDecision,
    pub reason: DecisionReason,
    pub matched_size: Option<ByteSize>,
    pub matched_ratio: Option<MatchRatio>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum JobState {
    Pending,
    Running,
    Waiting,
    Succeeded,
    Failed,
    Disabled,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum DependencyState {
    Unknown,
    Healthy {
        checked_at_ms: i64,
    },
    Degraded {
        reason: ReasonText,
        retry_after_ms: Option<i64>,
    },
    Unavailable {
        reason: ReasonText,
        retry_after_ms: Option<i64>,
    },
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum InjectionOutcome {
    Injected,
    Saved,
    AlreadyExists,
    SourceIncomplete,
    Failed,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum TorrentClientKind {
    Qbittorrent,
    Rtorrent,
}

fn file_name_from_relative_path(
    field: &'static str,
    relative_path: &Path,
) -> DomainResult<FileName> {
    if relative_path.as_os_str().is_empty() || relative_path.is_absolute() {
        return Err(DomainError::InvalidPath {
            field,
            value: relative_path.to_path_buf(),
        });
    }

    let Some(file_name) = relative_path.file_name().and_then(|value| value.to_str()) else {
        return Err(DomainError::InvalidPath {
            field,
            value: relative_path.to_path_buf(),
        });
    };

    FileName::new(file_name)
}

pub mod dto {
    #[derive(Debug, Clone, Eq, PartialEq)]
    pub struct AnnouncementRequest {
        pub name: String,
        pub guid: String,
        pub download_url: String,
        pub tracker: String,
        pub cookie: Option<String>,
        pub size: Option<u64>,
    }

    #[derive(Debug, Clone, Eq, PartialEq)]
    pub struct RemoteCandidate {
        pub indexer_id: u64,
        pub guid: String,
        pub download_url: String,
        pub title: String,
        pub tracker: String,
        pub size: Option<u64>,
        pub published_at_ms: Option<i64>,
        pub info_hash: Option<String>,
    }
}

impl TryFrom<dto::RemoteCandidate> for RemoteCandidate {
    type Error = DomainError;

    fn try_from(value: dto::RemoteCandidate) -> Result<Self, Self::Error> {
        Ok(Self {
            id: None,
            indexer_id: IndexerId::new(value.indexer_id)?,
            guid: CandidateGuid::new(value.guid)?,
            download_url: DownloadUrl::new(value.download_url)?,
            title: ItemTitle::new(value.title)?,
            tracker: TrackerName::new(value.tracker)?,
            size: value.size.map(ByteSize::new),
            published_at_ms: value.published_at_ms,
            info_hash: value.info_hash.map(InfoHash::new).transpose()?,
            torrent_cache_path: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn info_hash_accepts_hex_sha1_and_normalizes_case() {
        let hash = InfoHash::new("0123456789ABCDEF0123456789ABCDEF01234567").unwrap();

        assert_eq!("0123456789abcdef0123456789abcdef01234567", hash.as_str());
        assert_eq!(InfoHashAlgorithm::Sha1, hash.algorithm());
    }

    #[test]
    fn info_hash_rejects_bad_values() {
        assert_eq!(
            Err(DomainError::InvalidInfoHash {
                value: "not-a-hash".to_owned()
            }),
            InfoHash::new("not-a-hash")
        );
        assert_eq!(
            Err(DomainError::InvalidInfoHash {
                value: "g123456789abcdef0123456789abcdef01234567".to_owned()
            }),
            InfoHash::new("g123456789abcdef0123456789abcdef01234567")
        );
    }

    #[test]
    fn text_newtypes_reject_empty_values() {
        assert_eq!(
            Err(DomainError::EmptyField {
                field: "item title"
            }),
            ItemTitle::new("  ")
        );
    }

    #[test]
    fn match_ratio_must_be_finite_unit_interval() {
        assert_eq!(
            0.25_f64.to_bits(),
            MatchRatio::new(0.25).unwrap().get().to_bits()
        );
        assert_eq!(Err(DomainError::InvalidRatio), MatchRatio::new(1.01));
        assert_eq!(Err(DomainError::InvalidRatio), MatchRatio::new(f64::NAN));
    }

    #[test]
    fn torrent_files_must_use_relative_utf8_paths_with_file_names() {
        let file = TorrentFile::new(
            PathBuf::from("Season 01/Episode 01.mkv"),
            ByteSize::new(128),
            FileIndex::new(0),
        )
        .unwrap();

        assert_eq!("Episode 01.mkv", file.file_name.as_str());
        assert_eq!(
            Err(DomainError::InvalidPath {
                field: "torrent file relative path",
                value: PathBuf::from("/tmp/file.mkv")
            }),
            TorrentFile::new(
                PathBuf::from("/tmp/file.mkv"),
                ByteSize::new(1),
                FileIndex::new(0)
            )
        );
        assert_eq!(
            Err(DomainError::InvalidPath {
                field: "torrent file relative path",
                value: PathBuf::new()
            }),
            TorrentFile::new(PathBuf::new(), ByteSize::new(1), FileIndex::new(0))
        );
    }

    #[test]
    fn torrent_metafile_requires_at_least_one_file_and_totals_size() {
        let hash = InfoHash::new("0123456789abcdef0123456789abcdef01234567").unwrap();
        let name = DisplayName::new("Example").unwrap();
        let file = TorrentFile::new(
            PathBuf::from("Example.mkv"),
            ByteSize::new(20),
            FileIndex::new(0),
        )
        .unwrap();

        let metafile = TorrentMetafile::new(hash.clone(), name.clone(), vec![file]).unwrap();

        assert_eq!(20, metafile.total_size.get());
        assert_eq!(
            Err(DomainError::EmptyFiles),
            TorrentMetafile::new(hash, name, Vec::new())
        );
    }

    #[test]
    fn remote_candidate_dto_validates_into_domain_model() {
        let candidate = RemoteCandidate::try_from(dto::RemoteCandidate {
            indexer_id: 1,
            guid: "guid-1".to_owned(),
            download_url: "https://indexer.example/download/1".to_owned(),
            title: "Example".to_owned(),
            tracker: "tracker.example".to_owned(),
            size: Some(42),
            published_at_ms: Some(1_700_000_000_000),
            info_hash: Some("0123456789abcdef0123456789abcdef01234567".to_owned()),
        })
        .unwrap();

        assert_eq!(Some(42), candidate.size.map(ByteSize::get));
        assert_eq!(
            Some(InfoHashAlgorithm::Sha1),
            candidate.info_hash.map(|hash| hash.algorithm())
        );

        let invalid = RemoteCandidate::try_from(dto::RemoteCandidate {
            indexer_id: 0,
            guid: "guid-1".to_owned(),
            download_url: "https://indexer.example/download/1".to_owned(),
            title: "Example".to_owned(),
            tracker: "tracker.example".to_owned(),
            size: None,
            published_at_ms: None,
            info_hash: None,
        });

        assert_eq!(
            Err(DomainError::EmptyField {
                field: "indexer id"
            }),
            invalid
        );
    }
}
