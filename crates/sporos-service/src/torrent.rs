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
}

fn parse_metainfo<'a>(metainfo: MetaInfo<'a>) -> Result<ParsedTorrent<'a>, TorrentParseError> {
    validate_component(metainfo.info.name)?;

    let v1_files = metainfo
        .info
        .v1
        .as_ref()
        .map(|v1| v1_files(metainfo.info.name, &v1.files))
        .transpose()?;
    if let (Some(v1), Some(files)) = (&metainfo.info.v1, &v1_files) {
        let total_length = files.iter().try_fold(0_u64, |total, file| {
            total
                .checked_add(file.length)
                .ok_or(TorrentParseError::InconsistentPieces)
        })?;
        let expected = total_length.div_ceil(metainfo.info.piece_length);
        if usize::try_from(expected).ok() != Some(v1.pieces.len() / 20) {
            return Err(TorrentParseError::InconsistentPieces);
        }
    }
    let v2_files = metainfo
        .info
        .v2
        .as_ref()
        .map(|v2| v2_files(&v2.file_tree))
        .transpose()?;

    let files = v2_files.or(v1_files).expect("metainfo has a v1 or v2 view");
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
        files,
    })
}

fn v1_files<'a>(
    name: &'a [u8],
    layout: &FileListV1<'a>,
) -> Result<Vec<TorrentFile<'a>>, TorrentParseError> {
    let files = match layout {
        FileListV1::Single { length } => vec![TorrentFile {
            path: vec![name],
            length: *length,
            pieces_root: None,
        }],
        FileListV1::Multi { files } => files
            .iter()
            .map(|file| TorrentFile {
                path: std::iter::once(name)
                    .chain(file.path.iter().copied())
                    .collect(),
                length: file.length,
                pieces_root: None,
            })
            .collect(),
    };
    validate_files(&files)?;
    Ok(files)
}

fn v2_files<'a>(tree: &FileTreeNode<'a>) -> Result<Vec<TorrentFile<'a>>, TorrentParseError> {
    let mut files = Vec::new();
    let mut path = Vec::new();
    flatten_v2(tree, &mut path, &mut files);
    validate_files(&files)?;
    Ok(files)
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
