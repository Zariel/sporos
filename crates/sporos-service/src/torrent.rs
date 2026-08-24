use magpie_bt_bencode::{DecodeOptions, decode_with};
use magpie_bt_metainfo::{FileListV1, FileTreeNode, InfoHash, MetaInfo};
use thiserror::Error;

const MAX_TORRENT_BYTES: usize = 8 * 1024 * 1024;
const MAX_BENCODE_DEPTH: u32 = 32;
const MAX_BENCODE_NODES: u32 = 100_000;
const MAX_FILES: usize = 100_000;
const MAX_PATH_COMPONENTS: usize = 64;
const MAX_COMPONENT_BYTES: usize = 255;
const MAX_PATH_BYTES: usize = 4_096;
const MAX_MANIFEST_PATH_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Default)]
pub struct TorrentParser;

impl TorrentParser {
    pub fn parse<'a>(&self, input: &'a [u8]) -> Result<ParsedTorrent<'a>, TorrentParseError> {
        if input.len() > MAX_TORRENT_BYTES {
            return Err(TorrentParseError::LimitExceeded("torrent bytes"));
        }

        let mut options = DecodeOptions::default();
        options.max_depth = MAX_BENCODE_DEPTH;
        options.max_nodes = MAX_BENCODE_NODES;
        let tree = decode_with(input, options)?;
        drop(tree);

        let metainfo = magpie_bt_metainfo::parse(input)?;
        parse_metainfo(metainfo)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TorrentVersion {
    V1,
    V2,
    Hybrid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TorrentFile<'a> {
    path: Vec<&'a [u8]>,
    length: u64,
    pieces_root: Option<[u8; 32]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PieceFile {
    file_ordinal: Option<u32>,
    offset: u64,
    length: u64,
}

impl PieceFile {
    pub const fn file_ordinal(self) -> Option<u32> {
        self.file_ordinal
    }

    pub const fn offset(self) -> u64 {
        self.offset
    }

    pub const fn length(self) -> u64 {
        self.length
    }
}

impl<'a> TorrentFile<'a> {
    pub fn path(&self) -> &[&'a [u8]] {
        &self.path
    }

    pub const fn length(&self) -> u64 {
        self.length
    }

    pub const fn pieces_root(&self) -> Option<[u8; 32]> {
        self.pieces_root
    }
}

#[derive(Debug, Clone)]
pub struct ParsedTorrent<'a> {
    version: TorrentVersion,
    name: &'a [u8],
    info_bytes: &'a [u8],
    piece_length: u64,
    v1_hash: Option<[u8; 20]>,
    v2_hash: Option<[u8; 32]>,
    v1_pieces: Option<&'a [u8]>,
    files: Vec<TorrentFile<'a>>,
    piece_files: Vec<PieceFile>,
}

impl<'a> ParsedTorrent<'a> {
    pub const fn version(&self) -> TorrentVersion {
        self.version
    }

    pub const fn name(&self) -> &'a [u8] {
        self.name
    }

    pub const fn info_bytes(&self) -> &'a [u8] {
        self.info_bytes
    }

    pub const fn piece_length(&self) -> u64 {
        self.piece_length
    }

    pub const fn v1_hash(&self) -> Option<[u8; 20]> {
        self.v1_hash
    }

    pub const fn v2_hash(&self) -> Option<[u8; 32]> {
        self.v2_hash
    }

    pub const fn v1_pieces(&self) -> Option<&'a [u8]> {
        self.v1_pieces
    }

    pub fn files(&self) -> &[TorrentFile<'a>] {
        &self.files
    }

    pub fn piece_files(&self) -> &[PieceFile] {
        &self.piece_files
    }
}

#[derive(Debug, Error)]
pub enum TorrentParseError {
    #[error("torrent exceeds the {0} limit")]
    LimitExceeded(&'static str),
    #[error("invalid bencode structure")]
    Bencode(#[from] magpie_bt_bencode::DecodeError),
    #[error("invalid torrent metainfo")]
    Metainfo(#[from] magpie_bt_metainfo::ParseError),
    #[error("torrent contains an unsafe path")]
    UnsafePath,
    #[error("torrent contains duplicate or prefix-conflicting paths")]
    ConflictingPaths,
    #[error("torrent piece hashes do not describe its file lengths")]
    InconsistentPieces,
    #[error("hybrid torrent v1 and v2 file layouts differ")]
    InconsistentHybrid,
}

fn parse_metainfo<'a>(metainfo: MetaInfo<'a>) -> Result<ParsedTorrent<'a>, TorrentParseError> {
    validate_component(metainfo.info.name)?;

    let v1_view = metainfo
        .info
        .v1
        .as_ref()
        .map(|v1| v1_files(metainfo.info.name, &v1.files))
        .transpose()?;
    if let (Some(v1), Some(view)) = (&metainfo.info.v1, &v1_view) {
        let total_length = view
            .piece_files
            .last()
            .map_or(0, |file| file.offset.saturating_add(file.length));
        let expected = total_length.div_ceil(metainfo.info.piece_length);
        if usize::try_from(expected).ok() != Some(v1.pieces.len() / 20) {
            return Err(TorrentParseError::InconsistentPieces);
        }
    }
    let v2_view = metainfo
        .info
        .v2
        .as_ref()
        .map(|v2| {
            v2_files(
                metainfo.info.name,
                metainfo.info.piece_length,
                &v2.file_tree,
            )
        })
        .transpose()?;

    if let (Some(v1), Some(v2)) = (&v1_view, &v2_view)
        && !hybrid_layout_matches(v1, v2)
    {
        return Err(TorrentParseError::InconsistentHybrid);
    }
    let view = match (v1_view, v2_view) {
        (Some(v1), Some(v2)) => FileView {
            files: v2.files,
            piece_files: v1.piece_files,
        },
        (Some(v1), None) => v1,
        (None, Some(v2)) => v2,
        (None, None) => unreachable!("metainfo has a v1 or v2 view"),
    };
    let version = match (&metainfo.info.v1, &metainfo.info.v2) {
        (Some(_), None) => TorrentVersion::V1,
        (None, Some(_)) => TorrentVersion::V2,
        (Some(_), Some(_)) => TorrentVersion::Hybrid,
        (None, None) => unreachable!("metainfo parser rejects an empty version"),
    };
    let (v1_hash, v2_hash) = match metainfo.info_hash {
        InfoHash::V1(v1) => (Some(v1), None),
        InfoHash::V2(v2) => (None, Some(v2)),
        InfoHash::Hybrid { v1, v2 } => (Some(v1), Some(v2)),
    };

    Ok(ParsedTorrent {
        version,
        name: metainfo.info.name,
        info_bytes: metainfo.info_bytes,
        piece_length: metainfo.info.piece_length,
        v1_hash,
        v2_hash,
        v1_pieces: metainfo.info.v1.map(|v1| v1.pieces),
        files: view.files,
        piece_files: view.piece_files,
    })
}

struct FileView<'a> {
    files: Vec<TorrentFile<'a>>,
    piece_files: Vec<PieceFile>,
}

fn hybrid_layout_matches(v1: &FileView<'_>, v2: &FileView<'_>) -> bool {
    v1.files.len() == v2.files.len()
        && v1
            .files
            .iter()
            .zip(&v2.files)
            .all(|(v1, v2)| v1.path == v2.path && v1.length == v2.length)
        && v1
            .piece_files
            .iter()
            .filter(|file| file.file_ordinal.is_some())
            .eq(v2.piece_files.iter())
}

fn v1_files<'a>(
    name: &'a [u8],
    layout: &FileListV1<'a>,
) -> Result<FileView<'a>, TorrentParseError> {
    let raw = match layout {
        FileListV1::Single { length } => vec![(vec![name], *length, false)],
        FileListV1::Multi { files } => files
            .iter()
            .map(|file| {
                (
                    std::iter::once(name)
                        .chain(file.path.iter().copied())
                        .collect(),
                    file.length,
                    file.path
                        .first()
                        .is_some_and(|component| *component == b".pad"),
                )
            })
            .collect(),
    };
    let raw_files: Vec<_> = raw
        .iter()
        .map(|(path, length, _)| TorrentFile {
            path: path.clone(),
            length: *length,
            pieces_root: None,
        })
        .collect();
    validate_files(&raw_files)?;
    let mut files = Vec::new();
    let mut piece_files = Vec::with_capacity(raw.len());
    let mut offset = 0_u64;
    for (path, length, padding) in raw {
        let file_ordinal = if padding {
            None
        } else {
            let ordinal = u32::try_from(files.len())
                .map_err(|_| TorrentParseError::LimitExceeded("file count"))?;
            files.push(TorrentFile {
                path,
                length,
                pieces_root: None,
            });
            Some(ordinal)
        };
        piece_files.push(PieceFile {
            file_ordinal,
            offset,
            length,
        });
        offset = offset
            .checked_add(length)
            .ok_or(TorrentParseError::InconsistentPieces)?;
    }
    Ok(FileView { files, piece_files })
}

fn v2_files<'a>(
    name: &'a [u8],
    piece_length: u64,
    tree: &FileTreeNode<'a>,
) -> Result<FileView<'a>, TorrentParseError> {
    let mut files = Vec::new();
    let mut path = Vec::new();
    flatten_v2(tree, &mut path, &mut files);
    let single_file = files.len() == 1 && files[0].path.len() == 1;
    if !single_file {
        for file in &mut files {
            file.path.insert(0, name);
        }
    }
    validate_files(&files)?;
    let mut piece_files = Vec::with_capacity(files.len());
    let mut end = 0_u64;
    for (ordinal, file) in files.iter().enumerate() {
        let offset = align_piece(end, piece_length)?;
        piece_files.push(PieceFile {
            file_ordinal: Some(
                u32::try_from(ordinal)
                    .map_err(|_| TorrentParseError::LimitExceeded("file count"))?,
            ),
            offset,
            length: file.length,
        });
        end = offset
            .checked_add(file.length)
            .ok_or(TorrentParseError::InconsistentPieces)?;
    }
    Ok(FileView { files, piece_files })
}

fn align_piece(offset: u64, piece_length: u64) -> Result<u64, TorrentParseError> {
    let remainder = offset % piece_length;
    if remainder == 0 {
        Ok(offset)
    } else {
        offset
            .checked_add(piece_length - remainder)
            .ok_or(TorrentParseError::InconsistentPieces)
    }
}

fn flatten_v2<'a>(
    node: &FileTreeNode<'a>,
    path: &mut Vec<&'a [u8]>,
    files: &mut Vec<TorrentFile<'a>>,
) {
    match node {
        FileTreeNode::File {
            length,
            pieces_root,
        } => files.push(TorrentFile {
            path: path.clone(),
            length: *length,
            pieces_root: *pieces_root,
        }),
        FileTreeNode::Dir(children) => {
            for (name, child) in children {
                path.push(*name);
                flatten_v2(child, path, files);
                path.pop();
            }
        }
    }
}

fn validate_files(files: &[TorrentFile<'_>]) -> Result<(), TorrentParseError> {
    if files.is_empty() || files.len() > MAX_FILES {
        return Err(TorrentParseError::LimitExceeded("file count"));
    }

    let mut manifest_path_bytes = 0_usize;
    for file in files {
        if file.path.is_empty() || file.path.len() > MAX_PATH_COMPONENTS {
            return Err(TorrentParseError::LimitExceeded("path components"));
        }
        let mut path_bytes = file.path.len() - 1;
        for component in &file.path {
            validate_component(component)?;
            path_bytes = path_bytes
                .checked_add(component.len())
                .ok_or(TorrentParseError::LimitExceeded("path bytes"))?;
        }
        if path_bytes > MAX_PATH_BYTES {
            return Err(TorrentParseError::LimitExceeded("path bytes"));
        }
        manifest_path_bytes = manifest_path_bytes
            .checked_add(path_bytes)
            .ok_or(TorrentParseError::LimitExceeded("manifest path bytes"))?;
        if manifest_path_bytes > MAX_MANIFEST_PATH_BYTES {
            return Err(TorrentParseError::LimitExceeded("manifest path bytes"));
        }
    }

    let mut paths: Vec<_> = files.iter().map(|file| file.path.as_slice()).collect();
    paths.sort_unstable();
    for pair in paths.windows(2) {
        if pair[1].starts_with(pair[0]) {
            return Err(TorrentParseError::ConflictingPaths);
        }
    }
    Ok(())
}

fn validate_component(component: &[u8]) -> Result<(), TorrentParseError> {
    if component.is_empty()
        || component.len() > MAX_COMPONENT_BYTES
        || component == b"."
        || component == b".."
        || component.contains(&b'/')
        || component.contains(&0)
    {
        return Err(TorrentParseError::UnsafePath);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const PIECES: &str = "aaaaaaaaaaaaaaaaaaaa";
    const PIECES_ROOT: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    #[test]
    fn parses_v1_and_preserves_raw_info() {
        let bytes =
            format!("d4:infod6:lengthi13e4:name5:hello12:piece lengthi16384e6:pieces20:{PIECES}ee");
        let parsed = TorrentParser.parse(bytes.as_bytes()).expect("parse v1");

        assert_eq!(parsed.version(), TorrentVersion::V1);
        assert_eq!(parsed.name(), b"hello");
        assert_eq!(parsed.piece_length(), 16_384);
        assert_eq!(parsed.files()[0].path(), &[b"hello".as_slice()]);
        assert_eq!(parsed.files()[0].length(), 13);
        assert_eq!(
            parsed.v1_hash(),
            Some(magpie_bt_metainfo::sha1(parsed.info_bytes()))
        );
        assert!(
            bytes
                .as_bytes()
                .windows(parsed.info_bytes().len())
                .any(|span| span == parsed.info_bytes())
        );
    }

    #[test]
    fn parses_v2() {
        let bytes = format!(
            "d4:infod9:file treed5:hellod0:d6:lengthi13e11:pieces root32:{PIECES_ROOT}eee12:meta versioni2e4:name5:hello12:piece lengthi16384eee"
        );
        let parsed = TorrentParser.parse(bytes.as_bytes()).expect("parse v2");

        assert_eq!(parsed.version(), TorrentVersion::V2);
        assert_eq!(parsed.files()[0].path(), &[b"hello".as_slice()]);
        assert_eq!(parsed.files()[0].length(), 13);
        assert_eq!(parsed.files()[0].pieces_root(), Some([b'b'; 32]));
        assert_eq!(
            parsed.v2_hash(),
            Some(magpie_bt_metainfo::sha256(parsed.info_bytes()))
        );
    }

    #[test]
    fn prepends_the_qbittorrent_root_for_multifile_v2() {
        let bytes = format!(
            "d4:infod9:file treed5:a.bind0:d6:lengthi10000e11:pieces root32:{PIECES_ROOT}ee5:b.bind0:d6:lengthi10000e11:pieces root32:{PIECES_ROOT}eee12:meta versioni2e4:name4:root12:piece lengthi16384eee"
        );
        let parsed = TorrentParser.parse(bytes.as_bytes()).expect("parse v2");

        assert_eq!(
            parsed.files()[0].path(),
            &[b"root".as_slice(), b"a.bin".as_slice()]
        );
        assert_eq!(
            parsed.files()[1].path(),
            &[b"root".as_slice(), b"b.bin".as_slice()]
        );
        assert_eq!(parsed.piece_files()[0].offset(), 0);
        assert_eq!(parsed.piece_files()[1].offset(), 16_384);
    }

    #[test]
    fn parses_hybrid() {
        let bytes = format!(
            "d4:infod9:file treed5:hellod0:d6:lengthi13e11:pieces root32:{PIECES_ROOT}eee6:lengthi13e12:meta versioni2e4:name5:hello12:piece lengthi16384e6:pieces20:{PIECES}ee"
        );
        let parsed = TorrentParser.parse(bytes.as_bytes()).expect("parse hybrid");

        assert_eq!(parsed.version(), TorrentVersion::Hybrid);
        assert!(parsed.v1_hash().is_some());
        assert!(parsed.v2_hash().is_some());
        assert_eq!(parsed.v1_pieces(), Some(PIECES.as_bytes()));
    }

    #[test]
    fn validates_and_preserves_hybrid_padding_layout() {
        let bytes = hybrid_multifile("b.bin");
        let parsed = TorrentParser
            .parse(bytes.as_bytes())
            .expect("parse aligned hybrid");

        assert_eq!(parsed.files().len(), 2);
        assert_eq!(parsed.piece_files().len(), 3);
        assert_eq!(parsed.piece_files()[0].file_ordinal(), Some(0));
        assert_eq!(parsed.piece_files()[1].file_ordinal(), None);
        assert_eq!(parsed.piece_files()[1].offset(), 10_000);
        assert_eq!(parsed.piece_files()[1].length(), 6_384);
        assert_eq!(parsed.piece_files()[2].file_ordinal(), Some(1));
        assert_eq!(parsed.piece_files()[2].offset(), 16_384);
    }

    #[test]
    fn rejects_disagreeing_hybrid_views() {
        let bytes = hybrid_multifile("c.bin");
        assert!(matches!(
            TorrentParser.parse(bytes.as_bytes()),
            Err(TorrentParseError::InconsistentHybrid)
        ));
    }

    fn hybrid_multifile(v1_second_path: &str) -> String {
        format!(
            "d4:infod9:file treed5:a.bind0:d6:lengthi10000e11:pieces root32:{PIECES_ROOT}ee5:b.bind0:d6:lengthi10000e11:pieces root32:{PIECES_ROOT}eee5:filesld6:lengthi10000e4:pathl5:a.bineed6:lengthi6384e4:pathl4:.pad4:6384eed6:lengthi10000e4:pathl{}:{}eee12:meta versioni2e4:name4:root12:piece lengthi16384e6:pieces40:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaee",
            v1_second_path.len(),
            v1_second_path,
        )
    }

    #[test]
    fn rejects_duplicate_dictionary_keys() {
        let bytes = b"d4:infod4:name5:helloe4:infod4:name5:helloee";
        assert!(matches!(
            TorrentParser.parse(bytes),
            Err(TorrentParseError::Bencode(_))
        ));
    }

    #[test]
    fn rejects_excessive_depth_before_metainfo_allocation() {
        let mut bytes = vec![b'l'; MAX_BENCODE_DEPTH as usize + 2];
        bytes.extend(std::iter::repeat_n(b'e', MAX_BENCODE_DEPTH as usize + 2));
        assert!(matches!(
            TorrentParser.parse(&bytes),
            Err(TorrentParseError::Bencode(_))
        ));
    }

    #[test]
    fn rejects_excessive_structural_allocation() {
        let mut bytes = Vec::with_capacity(MAX_BENCODE_NODES as usize * 2 + 2);
        bytes.push(b'l');
        for _ in 0..MAX_BENCODE_NODES {
            bytes.extend_from_slice(b"0:");
        }
        bytes.push(b'e');

        assert!(matches!(
            TorrentParser.parse(&bytes),
            Err(TorrentParseError::Bencode(_))
        ));
    }

    #[test]
    fn rejects_parent_components() {
        let bytes = format!(
            "d4:infod5:filesld6:lengthi13e4:pathl2:..5:videoeee4:name4:root12:piece lengthi16384e6:pieces20:{PIECES}ee"
        );
        assert!(matches!(
            TorrentParser.parse(bytes.as_bytes()),
            Err(TorrentParseError::UnsafePath)
        ));
    }

    #[test]
    fn rejects_file_directory_prefix_collisions() {
        let bytes = format!(
            "d4:infod5:filesld6:lengthi1e4:pathl1:aeed6:lengthi1e4:pathl1:a1:beee4:name4:root12:piece lengthi16384e6:pieces20:{PIECES}ee"
        );
        assert!(matches!(
            TorrentParser.parse(bytes.as_bytes()),
            Err(TorrentParseError::ConflictingPaths)
        ));
    }

    #[test]
    fn rejects_inconsistent_v1_piece_count() {
        let bytes = b"d4:infod6:lengthi16385e4:name5:hello12:piece lengthi16384e6:pieces20:aaaaaaaaaaaaaaaaaaaaee";
        assert!(matches!(
            TorrentParser.parse(bytes),
            Err(TorrentParseError::InconsistentPieces)
        ));
    }
}
