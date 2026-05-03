//! Domain models shared across searchees, candidates, metafiles, and decisions.

use std::{borrow::Cow, fmt};

/// Byte length used for torrent files and aggregate torrent sizes.
pub type ByteLength = u64;

/// Millisecond timestamp used at API and persistence boundaries.
pub type TimestampMillis = u64;

/// A v1 BitTorrent info hash.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct InfoHash<'a>(Cow<'a, str>);

impl<'a> InfoHash<'a> {
    /// Build an info hash if it is a 40-character hexadecimal string.
    pub fn new(value: impl Into<Cow<'a, str>>) -> Option<Self> {
        let value = value.into();
        if value.len() == 40 && value.as_bytes().iter().all(u8::is_ascii_hexdigit) {
            Some(Self(value))
        } else {
            None
        }
    }

    /// Build an info hash from a caller that has already validated it.
    pub fn from_validated(value: impl Into<Cow<'a, str>>) -> Self {
        Self(value.into())
    }

    /// Return the canonical hash text.
    pub fn as_str(&self) -> &str {
        self.0.as_ref()
    }

    /// Convert any borrowed storage into owned storage.
    pub fn into_owned(self) -> InfoHash<'static> {
        InfoHash(Cow::Owned(self.0.into_owned()))
    }
}

impl fmt::Display for InfoHash<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Normalized file entry used by searchees and parsed metafiles.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct File<'a> {
    /// Basename of the file.
    pub name: Cow<'a, str>,
    /// Path relative to the torrent save path, or an absolute data-dir path
    /// when the file needs to be linked directly.
    pub path: Cow<'a, str>,
    /// File length in bytes.
    pub length: ByteLength,
}

impl<'a> File<'a> {
    /// Build a file entry and derive the basename from the path.
    pub fn new(path: impl Into<Cow<'a, str>>, length: ByteLength) -> Self {
        let path = path.into();
        let name = file_name(path.as_ref()).to_owned();

        Self {
            name: Cow::Owned(name),
            path,
            length,
        }
    }

    /// Build a file entry when the caller already has a normalized basename.
    pub fn with_name(
        name: impl Into<Cow<'a, str>>,
        path: impl Into<Cow<'a, str>>,
        length: ByteLength,
    ) -> Self {
        Self {
            name: name.into(),
            path: path.into(),
            length,
        }
    }

    /// Convert any borrowed storage into owned storage.
    pub fn into_owned(self) -> File<'static> {
        File {
            name: Cow::Owned(self.name.into_owned()),
            path: Cow::Owned(self.path.into_owned()),
            length: self.length,
        }
    }
}

/// Workflow source label carried through searches, RSS, announces, webhooks,
/// and saved-torrent injection.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum Label {
    /// Bulk search workflow.
    Search,
    /// RSS workflow.
    Rss,
    /// Saved torrent injection workflow.
    Inject,
    /// Announce API workflow.
    Announce,
    /// Webhook API workflow.
    Webhook,
}

impl Label {
    /// String representation used by workflow APIs and persisted records.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Search => "search",
            Self::Rss => "rss",
            Self::Inject => "inject",
            Self::Announce => "announce",
            Self::Webhook => "webhook",
        }
    }
}

impl fmt::Display for Label {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Media type used for indexer capability matching and output filenames.
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum MediaType {
    /// Single TV episode.
    Episode,
    /// TV season or multi-episode pack.
    Pack,
    /// Movie release.
    Movie,
    /// Anime release.
    Anime,
    /// Generic video release.
    Video,
    /// Audio or music release.
    Audio,
    /// Book or ebook release.
    Book,
    /// Unknown or unsupported release.
    #[default]
    Unknown,
}

impl MediaType {
    /// String representation used by filenames and integration metadata.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Episode => "episode",
            Self::Pack => "pack",
            Self::Movie => "movie",
            Self::Anime => "anime",
            Self::Video => "video",
            Self::Audio => "audio",
            Self::Book => "book",
            Self::Unknown => "unknown",
        }
    }
}

impl fmt::Display for MediaType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Persisted scalar facts used to shortlist reverse lookup rows before
/// hydrating full file trees.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct LookupFields {
    /// Normalized title key used by SQLite selectors.
    pub search_key: String,
    /// Parsed media type.
    pub media_type: MediaType,
    /// Parsed season when present.
    pub season: Option<u32>,
    /// Parsed episode when present.
    pub episode: Option<u32>,
    /// Total byte length.
    pub length: ByteLength,
    /// Number of files in the normalized tree.
    pub file_count: usize,
    /// Bytes attributed to video-like files.
    pub video_bytes: ByteLength,
    /// Bytes attributed to non-video files.
    pub non_video_bytes: ByteLength,
}

/// Torrent-client family behind an adapter.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum TorrentClientKind {
    /// qBittorrent Web API.
    QBittorrent,
    /// rTorrent XML-RPC.
    RTorrent,
    /// Transmission RPC.
    Transmission,
    /// Deluge RPC.
    Deluge,
}

impl TorrentClientKind {
    /// String representation used by configuration and diagnostics.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::QBittorrent => "qbittorrent",
            Self::RTorrent => "rtorrent",
            Self::Transmission => "transmission",
            Self::Deluge => "deluge",
        }
    }
}

impl fmt::Display for TorrentClientKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// A category, tag, or client label value.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ClientLabel<'a>(Cow<'a, str>);

impl<'a> ClientLabel<'a> {
    /// Build a client label value.
    pub fn new(value: impl Into<Cow<'a, str>>) -> Self {
        Self(value.into())
    }

    /// Return the label text.
    pub fn as_str(&self) -> &str {
        self.0.as_ref()
    }

    /// Convert any borrowed storage into owned storage.
    pub fn into_owned(self) -> ClientLabel<'static> {
        ClientLabel(Cow::Owned(self.0.into_owned()))
    }
}

impl fmt::Display for ClientLabel<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Static metadata for one configured torrent-client adapter.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TorrentClientMetadata<'a> {
    /// Stable configured client identity.
    pub host: Cow<'a, str>,
    /// User configuration order. Lower values are preferred.
    pub priority: u16,
    /// Adapter family.
    pub kind: TorrentClientKind,
    /// Whether this client can only be used as a searchee source.
    pub readonly: bool,
    /// Human-readable adapter label.
    pub label: Cow<'a, str>,
}

impl<'a> TorrentClientMetadata<'a> {
    /// Build client adapter metadata.
    pub fn new(
        host: impl Into<Cow<'a, str>>,
        priority: u16,
        kind: TorrentClientKind,
        readonly: bool,
        label: impl Into<Cow<'a, str>>,
    ) -> Self {
        Self {
            host: host.into(),
            priority,
            kind,
            readonly,
            label: label.into(),
        }
    }

    /// Convert any borrowed storage into owned storage.
    pub fn into_owned(self) -> TorrentClientMetadata<'static> {
        TorrentClientMetadata {
            host: Cow::Owned(self.host.into_owned()),
            priority: self.priority,
            kind: self.kind,
            readonly: self.readonly,
            label: Cow::Owned(self.label.into_owned()),
        }
    }
}

/// Metadata attached to a torrent discovered from a torrent client.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ClientTorrentMetadata<'a> {
    /// Stable configured client identity.
    pub host: Cow<'a, str>,
    /// Client save path for this torrent.
    pub save_path: Cow<'a, str>,
    /// qBittorrent/Deluge category or label.
    pub category: Option<ClientLabel<'a>>,
    /// qBittorrent/rTorrent/Transmission tags or labels.
    pub tags: Vec<ClientLabel<'a>>,
    /// Sanitized tracker hosts.
    pub trackers: Vec<Cow<'a, str>>,
}

impl<'a> ClientTorrentMetadata<'a> {
    /// Build torrent metadata from a client inventory row.
    pub fn new(
        host: impl Into<Cow<'a, str>>,
        save_path: impl Into<Cow<'a, str>>,
        category: Option<ClientLabel<'a>>,
        tags: Vec<ClientLabel<'a>>,
        trackers: Vec<Cow<'a, str>>,
    ) -> Self {
        Self {
            host: host.into(),
            save_path: save_path.into(),
            category,
            tags,
            trackers,
        }
    }

    /// Convert any borrowed storage into owned storage.
    pub fn into_owned(self) -> ClientTorrentMetadata<'static> {
        ClientTorrentMetadata {
            host: Cow::Owned(self.host.into_owned()),
            save_path: Cow::Owned(self.save_path.into_owned()),
            category: self.category.map(ClientLabel::into_owned),
            tags: self.tags.into_iter().map(ClientLabel::into_owned).collect(),
            trackers: self
                .trackers
                .into_iter()
                .map(|tracker| Cow::Owned(tracker.into_owned()))
                .collect(),
        }
    }
}

/// Source classification inferred from populated searchee fields.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum SearcheeSource {
    /// Torrent loaded in a configured client.
    TorrentClient,
    /// `.torrent` file from `torrent_dir`.
    TorrentFile,
    /// Real file or folder from `data_dirs`.
    DataDir,
    /// Synthetic season pack assembled from episode files.
    Virtual,
}

impl SearcheeSource {
    /// String representation used in notifications and diagnostics.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TorrentClient => "torrentClient",
            Self::TorrentFile => "torrentFile",
            Self::DataDir => "dataDir",
            Self::Virtual => "virtual",
        }
    }
}

/// Local data that can be searched for cross-seeds.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Searchee<'a> {
    /// Optional local torrent info hash.
    pub info_hash: Option<InfoHash<'a>>,
    /// Optional data-dir file or folder path.
    pub path: Option<Cow<'a, str>>,
    /// Normalized file list.
    pub files: Vec<File<'a>>,
    /// Original torrent, folder, or file name.
    pub name: Cow<'a, str>,
    /// Parsed search and matching title.
    pub title: Cow<'a, str>,
    /// Total byte length.
    pub length: ByteLength,
    /// Newest data-dir file modification time.
    pub mtime_millis: Option<TimestampMillis>,
    /// Optional torrent-client inventory metadata.
    pub client: Option<ClientTorrentMetadata<'a>>,
    /// Workflow label that produced this searchee.
    pub label: Option<Label>,
    /// Parsed media type.
    pub media_type: MediaType,
}

impl<'a> Searchee<'a> {
    /// Build a searchee from normalized files and compute aggregate length.
    pub fn from_files(
        name: impl Into<Cow<'a, str>>,
        title: impl Into<Cow<'a, str>>,
        mut files: Vec<File<'a>>,
    ) -> Self {
        files.sort_by(|left, right| left.path.cmp(&right.path));
        let length = files.iter().map(|file| file.length).sum();

        Self {
            info_hash: None,
            path: None,
            files,
            name: name.into(),
            title: title.into(),
            length,
            mtime_millis: None,
            client: None,
            label: None,
            media_type: MediaType::Unknown,
        }
    }

    /// Classify the searchee source using the compatibility field rules.
    pub fn source(&self) -> SearcheeSource {
        if self.client.is_some() {
            SearcheeSource::TorrentClient
        } else if self.info_hash.is_some() {
            SearcheeSource::TorrentFile
        } else if self.path.is_some() {
            SearcheeSource::DataDir
        } else {
            SearcheeSource::Virtual
        }
    }

    /// Convert any borrowed storage into owned storage.
    pub fn into_owned(self) -> Searchee<'static> {
        Searchee {
            info_hash: self.info_hash.map(InfoHash::into_owned),
            path: self.path.map(|path| Cow::Owned(path.into_owned())),
            files: self.files.into_iter().map(File::into_owned).collect(),
            name: Cow::Owned(self.name.into_owned()),
            title: Cow::Owned(self.title.into_owned()),
            length: self.length,
            mtime_millis: self.mtime_millis,
            client: self.client.map(ClientTorrentMetadata::into_owned),
            label: self.label,
            media_type: self.media_type,
        }
    }
}

/// Remote indexer result that may point at a downloadable torrent.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Candidate<'a> {
    /// Remote release name.
    pub name: Cow<'a, str>,
    /// Torznab item GUID.
    pub guid: Cow<'a, str>,
    /// Download URL when the indexer provided one.
    pub link: Option<Cow<'a, str>>,
    /// Source tracker or indexer name.
    pub tracker: Cow<'a, str>,
    /// Optional Torznab size.
    pub size: Option<ByteLength>,
    /// Optional publication timestamp.
    pub pub_date_millis: Option<TimestampMillis>,
    /// Optional persisted indexer row id.
    pub indexer_id: Option<i64>,
    /// Optional request cookie.
    pub cookie: Option<Cow<'a, str>>,
}

impl<'a> Candidate<'a> {
    /// Build a candidate with the required Torznab identity fields.
    pub fn new(
        name: impl Into<Cow<'a, str>>,
        guid: impl Into<Cow<'a, str>>,
        link: Option<impl Into<Cow<'a, str>>>,
        tracker: impl Into<Cow<'a, str>>,
    ) -> Self {
        Self {
            name: name.into(),
            guid: guid.into(),
            link: link.map(Into::into),
            tracker: tracker.into(),
            size: None,
            pub_date_millis: None,
            indexer_id: None,
            cookie: None,
        }
    }

    /// Convert any borrowed storage into owned storage.
    pub fn into_owned(self) -> Candidate<'static> {
        Candidate {
            name: Cow::Owned(self.name.into_owned()),
            guid: Cow::Owned(self.guid.into_owned()),
            link: self.link.map(|link| Cow::Owned(link.into_owned())),
            tracker: Cow::Owned(self.tracker.into_owned()),
            size: self.size,
            pub_date_millis: self.pub_date_millis,
            indexer_id: self.indexer_id,
            cookie: self.cookie.map(|cookie| Cow::Owned(cookie.into_owned())),
        }
    }
}

/// Parsed `.torrent` model after bencode normalization.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Metafile<'a> {
    /// SHA1 info hash over the bencoded `info` dictionary.
    pub info_hash: InfoHash<'a>,
    /// Torrent name.
    pub name: Cow<'a, str>,
    /// Parsed matching title.
    pub title: Cow<'a, str>,
    /// Total torrent length.
    pub length: ByteLength,
    /// Piece length from the torrent info dictionary.
    pub piece_length: ByteLength,
    /// Sorted normalized file tree.
    pub files: Vec<File<'a>>,
    /// Sanitized tracker hosts.
    pub trackers: Vec<Cow<'a, str>>,
    /// Optional category from client metadata.
    pub category: Option<ClientLabel<'a>>,
    /// Optional tags from client metadata.
    pub tags: Vec<ClientLabel<'a>>,
    /// Parsed media type.
    pub media_type: MediaType,
}

impl<'a> Metafile<'a> {
    /// Build a metafile from parser output and compute aggregate length.
    pub fn from_files(
        info_hash: InfoHash<'a>,
        name: impl Into<Cow<'a, str>>,
        title: impl Into<Cow<'a, str>>,
        piece_length: ByteLength,
        mut files: Vec<File<'a>>,
    ) -> Self {
        files.sort_by(|left, right| left.path.cmp(&right.path));
        let length = files.iter().map(|file| file.length).sum();

        Self {
            info_hash,
            name: name.into(),
            title: title.into(),
            length,
            piece_length,
            files,
            trackers: Vec::new(),
            category: None,
            tags: Vec::new(),
            media_type: MediaType::Unknown,
        }
    }

    /// Convert any borrowed storage into owned storage.
    pub fn into_owned(self) -> Metafile<'static> {
        Metafile {
            info_hash: self.info_hash.into_owned(),
            name: Cow::Owned(self.name.into_owned()),
            title: Cow::Owned(self.title.into_owned()),
            length: self.length,
            piece_length: self.piece_length,
            files: self.files.into_iter().map(File::into_owned).collect(),
            trackers: self
                .trackers
                .into_iter()
                .map(|tracker| Cow::Owned(tracker.into_owned()))
                .collect(),
            category: self.category.map(ClientLabel::into_owned),
            tags: self.tags.into_iter().map(ClientLabel::into_owned).collect(),
            media_type: self.media_type,
        }
    }
}

/// Candidate assessment result.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum Decision {
    /// Exact path/name and size match.
    Match,
    /// All sizes match, but names or paths may differ.
    MatchSizeOnly,
    /// Enough files match to seed partially.
    MatchPartial,
    /// Torznab size was outside the preliminary size window.
    FuzzySizeMismatch,
    /// File sizes do not match enough for strict or flexible mode.
    SizeMismatch,
    /// Too little candidate size is locally available for partial mode.
    PartialSizeMismatch,
    /// Candidate has no usable download link.
    NoDownloadLink,
    /// Downloading or parsing the torrent failed.
    DownloadFailed,
    /// Download redirected to an unsupported magnet URL.
    MagnetLink,
    /// Download or search hit a tracker rate limit.
    RateLimited,
    /// Candidate and searchee are the same torrent.
    SameInfoHash,
    /// Candidate info hash is already in local inventory.
    InfoHashAlreadyExists,
    /// Size-only or exact tree conditions failed.
    FileTreeMismatch,
    /// Parsed release group differs.
    ReleaseGroupMismatch,
    /// Searchee or candidate matched blocklist.
    BlockedRelease,
    /// One side is repack/proper/versioned and the other is not.
    ProperRepackMismatch,
    /// Strict 2160/1080/720 marker differs.
    ResolutionMismatch,
    /// Parsed streaming/source marker differs.
    SourceMismatch,
}

impl Decision {
    /// String representation used by persisted decision rows.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Match => "MATCH",
            Self::MatchSizeOnly => "MATCH_SIZE_ONLY",
            Self::MatchPartial => "MATCH_PARTIAL",
            Self::FuzzySizeMismatch => "FUZZY_SIZE_MISMATCH",
            Self::SizeMismatch => "SIZE_MISMATCH",
            Self::PartialSizeMismatch => "PARTIAL_SIZE_MISMATCH",
            Self::NoDownloadLink => "NO_DOWNLOAD_LINK",
            Self::DownloadFailed => "DOWNLOAD_FAILED",
            Self::MagnetLink => "MAGNET_LINK",
            Self::RateLimited => "RATE_LIMITED",
            Self::SameInfoHash => "SAME_INFO_HASH",
            Self::InfoHashAlreadyExists => "INFO_HASH_ALREADY_EXISTS",
            Self::FileTreeMismatch => "FILE_TREE_MISMATCH",
            Self::ReleaseGroupMismatch => "RELEASE_GROUP_MISMATCH",
            Self::BlockedRelease => "BLOCKED_RELEASE",
            Self::ProperRepackMismatch => "PROPER_REPACK_MISMATCH",
            Self::ResolutionMismatch => "RESOLUTION_MISMATCH",
            Self::SourceMismatch => "SOURCE_MISMATCH",
        }
    }

    /// Whether the decision should continue to the action stage.
    pub const fn is_match(self) -> bool {
        matches!(self, Self::Match | Self::MatchSizeOnly | Self::MatchPartial)
    }

    /// Parse a persisted decision value.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "MATCH" => Some(Self::Match),
            "MATCH_SIZE_ONLY" => Some(Self::MatchSizeOnly),
            "MATCH_PARTIAL" => Some(Self::MatchPartial),
            "FUZZY_SIZE_MISMATCH" => Some(Self::FuzzySizeMismatch),
            "SIZE_MISMATCH" => Some(Self::SizeMismatch),
            "PARTIAL_SIZE_MISMATCH" => Some(Self::PartialSizeMismatch),
            "NO_DOWNLOAD_LINK" => Some(Self::NoDownloadLink),
            "DOWNLOAD_FAILED" => Some(Self::DownloadFailed),
            "MAGNET_LINK" => Some(Self::MagnetLink),
            "RATE_LIMITED" => Some(Self::RateLimited),
            "SAME_INFO_HASH" => Some(Self::SameInfoHash),
            "INFO_HASH_ALREADY_EXISTS" => Some(Self::InfoHashAlreadyExists),
            "FILE_TREE_MISMATCH" => Some(Self::FileTreeMismatch),
            "RELEASE_GROUP_MISMATCH" => Some(Self::ReleaseGroupMismatch),
            "BLOCKED_RELEASE" => Some(Self::BlockedRelease),
            "PROPER_REPACK_MISMATCH" => Some(Self::ProperRepackMismatch),
            "RESOLUTION_MISMATCH" => Some(Self::ResolutionMismatch),
            "SOURCE_MISMATCH" => Some(Self::SourceMismatch),
            _ => None,
        }
    }
}

impl fmt::Display for Decision {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Result of saving a matched torrent file to `output_dir`.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum SaveResult {
    /// Torrent was written to `output_dir`.
    Saved,
}

impl SaveResult {
    /// String representation used by logs and API mapping.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Saved => "SAVED",
        }
    }
}

/// Result of injecting a matched torrent into a client.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum InjectionResult {
    /// Torrent was added to a client.
    Injected,
    /// Injection failed and the torrent was saved for retry where possible.
    Failure,
    /// Candidate info hash was already in a client.
    AlreadyExists,
    /// Source searchee is not complete enough for safe injection.
    TorrentNotComplete,
}

impl InjectionResult {
    /// String representation used by logs and API mapping.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Injected => "INJECTED",
            Self::Failure => "FAILURE",
            Self::AlreadyExists => "ALREADY_EXISTS",
            Self::TorrentNotComplete => "TORRENT_NOT_COMPLETE",
        }
    }
}

/// Top-level action outcome.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ActionResult {
    /// Save-mode result.
    Save(SaveResult),
    /// Inject-mode result.
    Injection(InjectionResult),
}

impl ActionResult {
    /// Whether the action means the candidate was accepted by save or inject.
    pub const fn accepted(self) -> bool {
        matches!(
            self,
            Self::Save(SaveResult::Saved) | Self::Injection(InjectionResult::Injected)
        )
    }
}

fn file_name(path: &str) -> &str {
    match path.rsplit(['/', '\\']).next() {
        Some(name) if !name.is_empty() => name,
        _ => path,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ActionResult, Candidate, ClientLabel, ClientTorrentMetadata, Decision, File, InfoHash,
        InjectionResult, Label, MediaType, Metafile, SaveResult, Searchee, SearcheeSource,
        TorrentClientKind, TorrentClientMetadata,
    };
    use std::borrow::Cow;

    #[test]
    fn file_derives_name_from_normalized_path() {
        let file = File::new("Example.Show/Season 01/Episode.mkv", 42);

        assert_eq!(file.name, "Episode.mkv");
        assert_eq!(file.path, "Example.Show/Season 01/Episode.mkv");
        assert_eq!(file.length, 42);
    }

    #[test]
    fn info_hash_accepts_only_v1_hex_values() {
        assert!(InfoHash::new("0123456789abcdef0123456789ABCDEF01234567").is_some());
        assert!(InfoHash::new("not-a-hash").is_none());
    }

    #[test]
    fn searchee_source_follows_compatibility_fields() {
        let mut searchee = Searchee::from_files(
            "Example.Show.S01",
            "Example Show S01",
            vec![File::new("Example.Show.S01/E01.mkv", 10)],
        );

        assert_eq!(searchee.source(), SearcheeSource::Virtual);

        searchee.path = Some(Cow::Borrowed("/data/Example.Show.S01"));
        assert_eq!(searchee.source(), SearcheeSource::DataDir);

        searchee.info_hash = Some(InfoHash::from_validated(
            "0123456789abcdef0123456789abcdef01234567",
        ));
        searchee.path = None;
        assert_eq!(searchee.source(), SearcheeSource::TorrentFile);

        searchee.client = Some(ClientTorrentMetadata::new(
            "qb.local",
            "/downloads",
            Some(ClientLabel::new("tv")),
            vec![ClientLabel::new("cross-seed")],
            vec![Cow::Borrowed("tracker.example")],
        ));
        assert_eq!(searchee.source(), SearcheeSource::TorrentClient);
    }

    #[test]
    fn constructors_sort_files_and_compute_total_length() {
        let hash = InfoHash::from_validated("0123456789abcdef0123456789abcdef01234567");
        let metafile = Metafile::from_files(
            hash,
            "Example.Show.S01",
            "Example Show S01",
            262_144,
            vec![
                File::new("Example.Show.S01/E02.mkv", 20),
                File::new("Example.Show.S01/E01.mkv", 10),
            ],
        );

        assert_eq!(metafile.length, 30);
        assert_eq!(metafile.files[0].path, "Example.Show.S01/E01.mkv");
        assert_eq!(metafile.files[1].path, "Example.Show.S01/E02.mkv");
    }

    #[test]
    fn decision_strings_match_persisted_values() {
        assert_eq!(Decision::Match.as_str(), "MATCH");
        assert_eq!(Decision::MatchSizeOnly.as_str(), "MATCH_SIZE_ONLY");
        assert_eq!(Decision::MatchPartial.as_str(), "MATCH_PARTIAL");
        assert!(Decision::Match.is_match());
        assert!(!Decision::SizeMismatch.is_match());
    }

    #[test]
    fn action_results_expose_compatibility_names() {
        assert_eq!(SaveResult::Saved.as_str(), "SAVED");
        assert_eq!(InjectionResult::Injected.as_str(), "INJECTED");
        assert_eq!(InjectionResult::Failure.as_str(), "FAILURE");
        assert!(ActionResult::Save(SaveResult::Saved).accepted());
        assert!(ActionResult::Injection(InjectionResult::Injected).accepted());
        assert!(!ActionResult::Injection(InjectionResult::AlreadyExists).accepted());
    }

    #[test]
    fn labels_media_types_and_client_kinds_use_contract_text() {
        let client = TorrentClientMetadata::new(
            "qb.local",
            0,
            TorrentClientKind::QBittorrent,
            false,
            "qBittorrent",
        );
        let candidate = Candidate::new(
            "Example.Show.S01",
            "guid",
            Some("https://tracker/t"),
            "Tracker",
        );

        assert_eq!(Label::Announce.as_str(), "announce");
        assert_eq!(MediaType::Episode.as_str(), "episode");
        assert_eq!(client.kind.as_str(), "qbittorrent");
        assert_eq!(candidate.link.as_deref(), Some("https://tracker/t"));
    }
}
