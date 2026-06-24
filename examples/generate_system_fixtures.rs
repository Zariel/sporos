use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use sha1::{Digest, Sha1};

const ANNOUNCE_URL: &str = "http://example.invalid/announce";
const CREATED_BY: &str = "sporos system fixture generator";
const PIECE_LENGTH: u64 = 16 * 1024;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("docker/system/fixtures"));
    generate(&output)?;
    Ok(())
}

fn generate(root: &Path) -> io::Result<()> {
    if root.exists() {
        fs::remove_dir_all(root)?;
    }
    fs::create_dir_all(root.join("torrents"))?;
    fs::create_dir_all(root.join("media/qbittorrent"))?;
    fs::create_dir_all(root.join("media/qbittorrent-candidate"))?;
    fs::create_dir_all(root.join("media/rtorrent"))?;

    let qbit_source_files = vec![FixtureFile::new(
        ["Sporos qBittorrent Fixture.mkv"],
        bytes(4096, b"qbit-visible-media"),
    )];
    let qbit_candidate_files = vec![FixtureFile::new(
        ["Sporos qBittorrent Fixture.mkv"],
        bytes(4096, b"qbit-candidate-media"),
    )];
    let rtorrent_files = vec![
        FixtureFile::new(
            ["Season 01", "Sporos rTorrent Fixture S01E01.mkv"],
            bytes(3072, b"rtorrent-episode-media"),
        ),
        FixtureFile::new(
            ["Extras", "sample.txt"],
            bytes(257, b"rtorrent-extra-sample"),
        ),
    ];

    write_media(
        root,
        "qbittorrent",
        "Sporos qBittorrent Fixture",
        &qbit_source_files,
    )?;
    write_media(
        root,
        "qbittorrent-candidate",
        "Sporos qBittorrent Candidate",
        &qbit_candidate_files,
    )?;
    write_media(root, "rtorrent", "Sporos rTorrent Fixture", &rtorrent_files)?;

    let fixtures = [
        TorrentFixture::new(
            "qbittorrent-source",
            "qBittorrent source",
            "sporos-qbittorrent-source",
            "Sporos qBittorrent Fixture",
            &qbit_source_files,
        ),
        TorrentFixture::new(
            "qbittorrent-candidate",
            "qBittorrent candidate",
            "sporos-qbittorrent-candidate",
            "Sporos qBittorrent Candidate",
            &qbit_candidate_files,
        ),
        TorrentFixture::new(
            "rtorrent-source",
            "rTorrent source",
            "sporos-rtorrent-source",
            "Sporos rTorrent Fixture",
            &rtorrent_files,
        ),
        TorrentFixture::new(
            "rtorrent-candidate",
            "rTorrent candidate",
            "sporos-rtorrent-candidate",
            "Sporos rTorrent Fixture",
            &rtorrent_files,
        ),
    ];

    let mut manifest_entries = Vec::new();
    for fixture in fixtures {
        let torrent = fixture.torrent_bytes();
        let info_hash = sha1_hex(&fixture.info_bytes());
        let path = root
            .join("torrents")
            .join(format!("{}.torrent", fixture.slug));
        fs::write(&path, torrent)?;
        manifest_entries.push(ManifestEntry {
            slug: fixture.slug,
            label: fixture.label,
            source_marker: fixture.source_marker,
            name: fixture.name,
            torrent_path: format!("torrents/{}.torrent", fixture.slug),
            media_root: fixture.media_root(),
            info_hash,
            files: fixture.file_manifest(),
        });
    }

    fs::write(root.join("manifest.json"), manifest_json(&manifest_entries))?;
    fs::write(root.join("README.md"), readme(&manifest_entries))?;
    Ok(())
}

fn write_media(
    root: &Path,
    client: &str,
    fixture_root: &str,
    files: &[FixtureFile],
) -> io::Result<()> {
    for file in files {
        let path = root
            .join("media")
            .join(client)
            .join(fixture_root)
            .join(file.path.join("/"));
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, &file.contents)?;
    }
    Ok(())
}

#[derive(Clone)]
struct FixtureFile {
    path: Vec<&'static str>,
    contents: Vec<u8>,
}

impl FixtureFile {
    fn new<const N: usize>(path: [&'static str; N], contents: Vec<u8>) -> Self {
        Self {
            path: path.into_iter().collect(),
            contents,
        }
    }
}

struct TorrentFixture<'a> {
    slug: &'static str,
    label: &'static str,
    source_marker: &'static str,
    name: &'static str,
    files: &'a [FixtureFile],
}

impl<'a> TorrentFixture<'a> {
    const fn new(
        slug: &'static str,
        label: &'static str,
        source_marker: &'static str,
        name: &'static str,
        files: &'a [FixtureFile],
    ) -> Self {
        Self {
            slug,
            label,
            source_marker,
            name,
            files,
        }
    }

    fn torrent_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"d");
        bencode_bytes(&mut bytes, b"announce");
        bencode_bytes(&mut bytes, ANNOUNCE_URL.as_bytes());
        bencode_bytes(&mut bytes, b"created by");
        bencode_bytes(&mut bytes, CREATED_BY.as_bytes());
        bencode_bytes(&mut bytes, b"info");
        bytes.extend_from_slice(&self.info_bytes());
        bytes.extend_from_slice(b"e");
        bytes
    }

    fn info_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"d");
        bencode_bytes(&mut bytes, b"files");
        bytes.extend_from_slice(b"l");
        for file in self.files {
            bytes.extend_from_slice(b"d");
            bencode_bytes(&mut bytes, b"length");
            bencode_int(&mut bytes, file.contents.len() as u64);
            bencode_bytes(&mut bytes, b"path");
            bytes.extend_from_slice(b"l");
            for segment in &file.path {
                bencode_bytes(&mut bytes, segment.as_bytes());
            }
            bytes.extend_from_slice(b"e");
            bytes.extend_from_slice(b"e");
        }
        bytes.extend_from_slice(b"e");
        bencode_bytes(&mut bytes, b"name");
        bencode_bytes(&mut bytes, self.name.as_bytes());
        bencode_bytes(&mut bytes, b"piece length");
        bencode_int(&mut bytes, PIECE_LENGTH);
        bencode_bytes(&mut bytes, b"pieces");
        bencode_bytes(&mut bytes, &self.pieces());
        bencode_bytes(&mut bytes, b"private");
        bencode_int(&mut bytes, 1);
        bencode_bytes(&mut bytes, b"source");
        bencode_bytes(&mut bytes, self.source_marker.as_bytes());
        bytes.extend_from_slice(b"e");
        bytes
    }

    fn pieces(&self) -> Vec<u8> {
        let mut pieces = Vec::new();
        for chunk in self
            .files
            .iter()
            .flat_map(|file| file.contents.iter().copied())
            .collect::<Vec<_>>()
            .chunks(PIECE_LENGTH as usize)
        {
            pieces.extend_from_slice(&Sha1::digest(chunk));
        }
        pieces
    }

    fn media_root(&self) -> &'static str {
        if self.slug == "qbittorrent-candidate" {
            "media/qbittorrent-candidate"
        } else if self.slug.starts_with("qbittorrent") {
            "media/qbittorrent"
        } else {
            "media/rtorrent"
        }
    }

    fn file_manifest(&self) -> Vec<FileManifest> {
        self.files
            .iter()
            .map(|file| FileManifest {
                path: file.path.join("/"),
                size: file.contents.len(),
                sha1: sha1_hex(&file.contents),
            })
            .collect()
    }
}

struct ManifestEntry {
    slug: &'static str,
    label: &'static str,
    source_marker: &'static str,
    name: &'static str,
    torrent_path: String,
    media_root: &'static str,
    info_hash: String,
    files: Vec<FileManifest>,
}

struct FileManifest {
    path: String,
    size: usize,
    sha1: String,
}

fn bytes(len: usize, seed: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    let mut counter = 0_u64;
    while out.len() < len {
        let mut block = Vec::new();
        block.extend_from_slice(seed);
        block.extend_from_slice(&counter.to_be_bytes());
        out.extend_from_slice(&Sha1::digest(block));
        counter = counter.saturating_add(1);
    }
    out.truncate(len);
    out
}

fn bencode_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(bytes.len().to_string().as_bytes());
    out.extend_from_slice(b":");
    out.extend_from_slice(bytes);
}

fn bencode_int(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(b"i");
    out.extend_from_slice(value.to_string().as_bytes());
    out.extend_from_slice(b"e");
}

fn sha1_hex(bytes: &[u8]) -> String {
    Sha1::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn manifest_json(entries: &[ManifestEntry]) -> String {
    let mut out = String::from("{\n  \"announce\": \"");
    out.push_str(ANNOUNCE_URL);
    out.push_str("\",\n  \"piece_length\": ");
    out.push_str(&PIECE_LENGTH.to_string());
    out.push_str(",\n  \"fixtures\": [\n");
    for (index, entry) in entries.iter().enumerate() {
        if index > 0 {
            out.push_str(",\n");
        }
        out.push_str("    {\n");
        out.push_str(&format!("      \"slug\": \"{}\",\n", entry.slug));
        out.push_str(&format!("      \"label\": \"{}\",\n", entry.label));
        out.push_str(&format!(
            "      \"source_marker\": \"{}\",\n",
            entry.source_marker
        ));
        out.push_str(&format!("      \"name\": \"{}\",\n", entry.name));
        out.push_str(&format!(
            "      \"torrent_path\": \"{}\",\n",
            entry.torrent_path
        ));
        out.push_str(&format!(
            "      \"media_root\": \"{}\",\n",
            entry.media_root
        ));
        out.push_str(&format!("      \"info_hash\": \"{}\",\n", entry.info_hash));
        out.push_str("      \"files\": [\n");
        for (file_index, file) in entry.files.iter().enumerate() {
            if file_index > 0 {
                out.push_str(",\n");
            }
            out.push_str(&format!(
                "        {{ \"path\": \"{}\", \"size\": {}, \"sha1\": \"{}\" }}",
                file.path, file.size, file.sha1
            ));
        }
        out.push_str("\n      ]\n    }");
    }
    out.push_str("\n  ]\n}\n");
    out
}

fn readme(entries: &[ManifestEntry]) -> String {
    let mut out = String::from(
        "# Real Client Torrent Fixtures\n\n\
         Generated by `cargo run --example generate_system_fixtures -- docker/system/fixtures`.\n\n\
         The torrents use `http://example.invalid/announce`; they contain no real tracker URLs, \
         passkeys, cookies, or credentials. Source and candidate torrents keep matching visible \
         file trees and file sizes while changing their private `source` marker, which gives them \
         distinct info hashes. The qBittorrent candidate also uses a distinct torrent root name and \
         piece data so pinned qBittorrent releases accept it next to the loaded source fixture.\n\n\
         | Fixture | Torrent | Info hash | Media root |\n\
         | --- | --- | --- | --- |\n",
    );
    for entry in entries {
        out.push_str(&format!(
            "| {} | `{}` | `{}` | `{}` |\n",
            entry.label, entry.torrent_path, entry.info_hash, entry.media_root
        ));
    }
    out.push_str("\n## File Trees\n\n");
    for entry in entries {
        out.push_str(&format!("### {}\n\n", entry.label));
        for file in &entry.files {
            out.push_str(&format!(
                "- `{}`: {} bytes, sha1 `{}`\n",
                file.path, file.size, file.sha1
            ));
        }
        out.push('\n');
    }
    out
}
