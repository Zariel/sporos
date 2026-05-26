use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use bendy::decoding::{Decoder, Object};
use serde::Deserialize;
use sha1::{Digest, Sha1};
use sporos::torrent::parse_metafile;

#[derive(Debug, Deserialize)]
struct Manifest {
    announce: String,
    piece_length: u64,
    fixtures: Vec<Fixture>,
}

#[derive(Debug, Deserialize)]
struct Fixture {
    slug: String,
    name: String,
    torrent_path: PathBuf,
    media_root: PathBuf,
    info_hash: String,
    files: Vec<FixtureFile>,
}

#[derive(Debug, Deserialize)]
struct FixtureFile {
    path: PathBuf,
    size: u64,
    sha1: String,
}

#[test]
fn real_client_torrent_fixtures_are_deterministic_and_tiny() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("docker/system/fixtures");
    let manifest_bytes =
        fs::read(root.join("manifest.json")).expect("fixture manifest should be readable");
    let manifest: Manifest =
        serde_json::from_slice(&manifest_bytes).expect("fixture manifest should be valid JSON");

    assert_eq!("http://example.invalid/announce", manifest.announce);
    assert_eq!(16 * 1024, manifest.piece_length);
    assert_eq!(4, manifest.fixtures.len());

    let by_slug: BTreeMap<_, _> = manifest
        .fixtures
        .iter()
        .map(|fixture| (fixture.slug.as_str(), fixture))
        .collect();
    assert_pair_equivalent_and_distinct(&by_slug, "qbittorrent-source", "qbittorrent-candidate");
    assert_pair_equivalent_and_distinct(&by_slug, "rtorrent-source", "rtorrent-candidate");
    let search_xml =
        fs::read_to_string(root.parent().unwrap().join("torznab/www/search.xml")).unwrap();
    let compose = fs::read_to_string(root.parent().unwrap().join("compose.yml")).unwrap();
    let nginx = fs::read_to_string(root.parent().unwrap().join("torznab/nginx.conf")).unwrap();
    assert_search_fixture_matches_manifest(&search_xml, by_slug["qbittorrent-candidate"], "4096");
    assert_search_fixture_matches_manifest(&search_xml, by_slug["rtorrent-candidate"], "3329");
    assert_torznab_mounts_do_not_nest_read_only_document_root(&compose, &nginx);

    for fixture in &manifest.fixtures {
        let torrent =
            fs::read(root.join(&fixture.torrent_path)).expect("fixture torrent should be readable");
        assert!(torrent.len() < 2 * 1024);
        assert!(
            !torrent
                .windows(b"passkey".len())
                .any(|window| window == b"passkey")
        );
        assert!(
            !torrent
                .windows(b"secret".len())
                .any(|window| window == b"secret")
        );
        assert!(
            !torrent
                .windows(b"tracker.".len())
                .any(|window| window == b"tracker.")
        );

        let parsed = parse_metafile(&torrent).expect("fixture torrent should parse");
        assert_eq!(fixture.info_hash, parsed.metafile.info_hash().as_str());
        assert_eq!(
            Some(manifest.piece_length),
            parsed.metafile.piece_length().map(|size| size.get())
        );
        assert_eq!(fixture.name, parsed.metafile.name().as_str());

        let mut parsed_files: Vec<_> = parsed
            .metafile
            .files()
            .iter()
            .map(|file| (file.relative_path.clone(), file.size.get()))
            .collect();
        parsed_files.sort();

        let mut manifest_files: Vec<_> = fixture
            .files
            .iter()
            .map(|file| (file.path.clone(), file.size))
            .collect();
        manifest_files.sort();
        assert_eq!(manifest_files, parsed_files);

        let mut piece_input = Vec::new();
        for file in &fixture.files {
            let media = fs::read(
                root.join(&fixture.media_root)
                    .join(&fixture.name)
                    .join(&file.path),
            )
            .expect("fixture media file should be readable");
            assert_eq!(file.size as usize, media.len());
            assert_eq!(file.sha1, sha1_hex(&media));
            piece_input.extend_from_slice(&media);
        }
        assert_eq!(
            torrent_pieces(&torrent),
            expected_pieces(&piece_input, manifest.piece_length)
        );
    }
}

fn assert_torznab_mounts_do_not_nest_read_only_document_root(compose: &str, nginx: &str) {
    assert!(compose.contains("./torznab/www:/usr/share/nginx/html:ro"));
    assert!(compose.contains("./fixtures/torrents:/usr/share/nginx/torrents:ro"));
    assert!(!compose.contains("./fixtures/torrents:/usr/share/nginx/html/torrents"));
    assert!(nginx.contains("alias /usr/share/nginx/torrents/;"));
}

fn assert_search_fixture_matches_manifest(
    search_xml: &str,
    fixture: &Fixture,
    expected_size: &str,
) {
    let slug = fixture.slug.as_str();
    assert!(search_xml.contains(&format!("<title>{}</title>", fixture.name)));
    assert!(search_xml.contains(&format!("<guid>sporos-{slug}</guid>")));
    assert!(search_xml.contains(&format!(
        "url=\"http://torznab-fixture:8080/torrents/{slug}.torrent\""
    )));
    assert!(search_xml.contains(&format!("name=\"size\" value=\"{expected_size}\"")));
    assert!(search_xml.contains(&format!(
        "name=\"infohash\" value=\"{}\"",
        fixture.info_hash
    )));
}

fn assert_pair_equivalent_and_distinct(
    fixtures: &BTreeMap<&str, &Fixture>,
    source: &str,
    candidate: &str,
) {
    let source = fixtures
        .get(source)
        .expect("source fixture should exist in manifest");
    let candidate = fixtures
        .get(candidate)
        .expect("candidate fixture should exist in manifest");
    assert_ne!(source.info_hash, candidate.info_hash);
    assert_eq!(source.media_root, candidate.media_root);

    let source_files: Vec<_> = source
        .files
        .iter()
        .map(|file| (&file.path, file.size))
        .collect();
    let candidate_files: Vec<_> = candidate
        .files
        .iter()
        .map(|file| (&file.path, file.size))
        .collect();
    assert_eq!(source_files, candidate_files);
}

fn sha1_hex(bytes: &[u8]) -> String {
    Sha1::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn expected_pieces(bytes: &[u8], piece_length: u64) -> Vec<u8> {
    bytes
        .chunks(usize::try_from(piece_length).expect("piece length should fit usize"))
        .flat_map(|chunk| Sha1::digest(chunk).to_vec())
        .collect()
}

fn torrent_pieces(torrent: &[u8]) -> Vec<u8> {
    let mut decoder = Decoder::new(torrent).with_max_depth(16);
    let root = decoder
        .next_object()
        .expect("fixture torrent root should decode")
        .and_then(|object| match object {
            Object::Dict(dict) => Some(dict),
            _ => None,
        })
        .expect("fixture torrent root should be a dictionary");
    let mut root = root;
    while let Some((key, value)) = root
        .next_pair()
        .expect("fixture torrent root entries should decode")
    {
        if key != b"info" {
            continue;
        }
        let Some(mut info) = (match value {
            Object::Dict(dict) => Some(dict),
            _ => None,
        }) else {
            return Vec::new();
        };
        while let Some((key, value)) = info
            .next_pair()
            .expect("fixture torrent info entries should decode")
        {
            if key == b"pieces" {
                return match value {
                    Object::Bytes(bytes) => bytes.to_vec(),
                    _ => Vec::new(),
                };
            }
        }
    }
    Vec::new()
}
