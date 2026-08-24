use std::io::Read;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::time::Duration;

use reqwest::Url;
use sporos_service::{
    hardlink::{HardlinkMaterializer, PlannedLink},
    inventory::{
        InventoryChange, parse_files, parse_inventory, parse_main_data, parse_piece_states,
    },
    qbittorrent::{AddTorrentRequest, ApiKey, QbittorrentClient, QbittorrentError},
    torrent::TorrentParser,
};

#[tokio::test]
#[ignore = "requires scripts/test-qbittorrent"]
async fn stopped_add_is_safe_on_qbittorrent_5_2() {
    let base_url = Url::parse(&required_env("SPOROS_QBITTORRENT_URL")).expect("qBittorrent URL");
    let api_key = required_env("SPOROS_QBITTORRENT_API_KEY");
    let client = QbittorrentClient::new(
        base_url.clone(),
        Some(ApiKey::new(&api_key).expect("qBittorrent API key")),
    )
    .expect("qBittorrent client");

    let wrong_key = QbittorrentClient::new(
        base_url,
        Some(ApiKey::new("qbt_abcdefghijklmnopqrstuvwxyz01").expect("fixture API key")),
    )
    .expect("unauthenticated client");
    assert!(matches!(
        wrong_key.validate_contract().await,
        Err(QbittorrentError::HttpStatus(status)) if status.as_u16() == 403
    ));

    let versions = client
        .validate_contract()
        .await
        .expect("supported contract");
    eprintln!(
        "validated qBittorrent {} with Web API {}",
        versions.application, versions.web_api
    );

    let mut hashes = Vec::new();
    for case in cases() {
        hashes.push(verify_case(&client, case).await);
    }
    verify_inventory_reads(&client, &hashes).await;
}

async fn verify_case(client: &QbittorrentClient, case: Case) -> String {
    let parsed = TorrentParser
        .parse(&case.torrent)
        .unwrap_or_else(|error| panic!("parse {} fixture: {error}", case.name));
    let parsed_info_hash = hex(parsed
        .v1_hash()
        .map(Vec::from)
        .or_else(|| parsed.v2_hash().map(Vec::from))
        .expect("torrent identity")
        .as_slice());
    let parsed_files: Vec<_> = parsed
        .files()
        .iter()
        .map(|file| {
            (
                file.path()
                    .iter()
                    .map(|component| std::str::from_utf8(component).expect("UTF-8 fixture"))
                    .collect::<Vec<_>>()
                    .join("/"),
                file.length(),
            )
        })
        .collect();
    let save_path = format!("/downloads/{}", case.name);
    let tag = format!("sporos-phase0-{}", case.name);
    let submission = client
        .add_stopped(AddTorrentRequest {
            torrent: case.torrent,
            filename: format!("{}.torrent", case.name),
            save_path: save_path.clone(),
            category: None,
            tags: vec![tag.clone()],
        })
        .await
        .unwrap_or_else(|error| panic!("{} stopped add: {error}", case.name));
    let info_hash = submission
        .added_torrent_ids
        .first()
        .cloned()
        .unwrap_or(parsed_info_hash);
    eprintln!("verifying {} as {}", case.name, info_hash);

    let first_state = poll_visible(client, &info_hash).await;
    assert_safe_state(&first_state.state);
    client.stop(&info_hash).await.expect("explicit stop");
    let state = poll_stopped(client, &info_hash).await;
    assert!(!state.auto_tmm, "automatic management was enabled");
    assert_eq!(state.amount_left, case.length, "{} length", case.name);
    assert_eq!(state.tags, tag, "{} tags", case.name);
    assert_eq!(state.save_path, save_path, "{} save path", case.name);
    assert_eq!(
        state.content_path,
        format!("{save_path}/{}", case.content),
        "{} content path",
        case.name
    );

    if let Some(files) = case.materialize {
        materialize_and_recheck(client, case.name, &info_hash, &parsed_files, files).await;
    }

    client
        .stop(&info_hash)
        .await
        .expect("idempotent explicit stop");
    let state = poll_stopped(client, &info_hash).await;
    assert!(state.is_stopped(), "torrent did not remain stopped");
    info_hash
}

async fn materialize_and_recheck(
    client: &QbittorrentClient,
    case_name: &str,
    info_hash: &str,
    parsed_files: &[(String, u64)],
    files: Vec<Vec<u8>>,
) {
    let downloads = PathBuf::from(required_env("SPOROS_QBITTORRENT_DOWNLOADS"));
    let source_root = PathBuf::from(required_env("SPOROS_QBITTORRENT_SOURCES")).join(case_name);
    std::fs::create_dir_all(&source_root).expect("create source root");
    let mut links = Vec::new();
    for (index, ((destination, length), content)) in parsed_files.iter().zip(files).enumerate() {
        assert_eq!(*length, content.len() as u64);
        let candidate_path = downloads.join(case_name).join(destination);
        assert!(
            !candidate_path.exists(),
            "{} materialised data before verification",
            case_name
        );
        let source_relative = PathBuf::from(format!("{index}.source"));
        std::fs::write(source_root.join(&source_relative), &content).expect("write source fixture");
        links.push(PlannedLink {
            source_root: source_root.clone(),
            source_relative,
            destination_relative: PathBuf::from(destination),
            expected_size: *length,
            expected_device: None,
            expected_inode: None,
        });
    }
    let materialized = HardlinkMaterializer::open(&downloads)
        .expect("open download root")
        .materialize(Path::new(case_name), &links)
        .expect("materialize candidate tree");
    assert_eq!(materialized.len(), links.len());
    for (destination, _) in parsed_files {
        let mut directory = downloads.join(case_name).join(destination);
        directory.pop();
        while directory.starts_with(downloads.join(case_name)) {
            std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o777))
                .expect("make bind-mounted fixture traversable");
            if !directory.pop() {
                break;
            }
        }
    }

    client
        .force_recheck(info_hash)
        .await
        .expect("force recheck");
    let state = poll_complete(client, info_hash).await;
    assert_eq!(state.amount_left, 0, "{} amount left", case_name);
    assert_eq!(state.progress, 1.0, "{} progress", case_name);
    let body = client.piece_states(info_hash).await.expect("piece states");
    let (count, complete) = tokio::task::spawn_blocking(move || {
        let mut complete = true;
        let count = parse_piece_states(body, u64::MAX, 100, |state| {
            complete &= state == 2;
            Ok(())
        })?;
        Ok::<_, sporos_service::inventory::InventoryParseError>((count, complete))
    })
    .await
    .expect("join piece-state parser")
    .expect("parse piece states");
    assert!(count > 0 && complete, "{} piece states", case_name);

    for (index, (destination, _)) in parsed_files.iter().enumerate() {
        let source = std::fs::metadata(source_root.join(format!("{index}.source")))
            .expect("source metadata");
        let destination = std::fs::metadata(downloads.join(case_name).join(destination))
            .expect("destination metadata");
        assert_eq!(source.dev(), destination.dev());
        assert_eq!(source.ino(), destination.ino());
    }
}

async fn verify_inventory_reads(client: &QbittorrentClient, hashes: &[String]) {
    let main_body = client.sync_main_data(0).await.expect("full main data");
    let (main, identities) = tokio::task::spawn_blocking(move || {
        let mut identities = Vec::new();
        let main = parse_main_data(main_body, u64::MAX, |change| {
            if let InventoryChange::Upsert { qbit_id, delta } = change {
                assert!(
                    delta
                        .infohash_v1
                        .as_deref()
                        .is_some_and(|hash| !hash.is_empty())
                        || delta
                            .infohash_v2
                            .as_deref()
                            .is_some_and(|hash| !hash.is_empty())
                );
                identities.push(qbit_id);
            }
            Ok(())
        })?;
        Ok::<_, sporos_service::inventory::InventoryParseError>((main, identities))
    })
    .await
    .expect("join main-data parser")
    .expect("parse full main data");
    assert!(main.full_update);
    assert_eq!(main.changed, hashes.len());

    let page_body = client.inventory_page(0, 500).await.expect("inventory page");
    let (count, page_hashes) = tokio::task::spawn_blocking(move || {
        let mut page_hashes = Vec::new();
        let count = parse_inventory(page_body, u64::MAX, |torrent| {
            assert!(!torrent.infohash_v1.is_empty() || !torrent.infohash_v2.is_empty());
            page_hashes.push(torrent.hash);
            Ok(())
        })?;
        Ok::<_, sporos_service::inventory::InventoryParseError>((count, page_hashes))
    })
    .await
    .expect("join inventory parser")
    .expect("parse inventory page");
    assert_eq!(count, hashes.len());
    assert_eq!(page_hashes, identities);

    for hash in hashes {
        let body = client.torrent_files(hash).await.expect("torrent files");
        let count = tokio::task::spawn_blocking(move || {
            let mut next = 0_usize;
            parse_files(body, u64::MAX, 100_000, |file| {
                assert_eq!(file.index, next);
                next += 1;
                Ok(())
            })
        })
        .await
        .expect("join file-list parser")
        .expect("parse torrent files");
        assert!(count > 0);
    }
}

struct Case {
    name: &'static str,
    torrent: Vec<u8>,
    content: &'static str,
    length: u64,
    materialize: Option<Vec<Vec<u8>>>,
}

fn cases() -> Vec<Case> {
    let pieces = "00000000000000000000";
    let hybrid_pieces = "0000000000000000000000000000000000000000";
    let root = "11111111111111111111111111111111";
    vec![
        Case {
            name: "v1-single",
            torrent: format!(
                "d4:infod6:lengthi4e4:name11:v1-file.bin12:piece lengthi16384e6:pieces20:{pieces}ee"
            )
            .into_bytes(),
            content: "v1-file.bin",
            length: 4,
            materialize: None,
        },
        Case {
            name: "v1-multi",
            torrent: format!(
                "d4:infod5:filesld6:lengthi4e4:pathl5:a.bineed6:lengthi5e4:pathl5:b.bineee4:name7:v1-root12:piece lengthi16384e6:pieces20:{pieces}ee"
            )
            .into_bytes(),
            content: "v1-root",
            length: 9,
            materialize: None,
        },
        Case {
            name: "v2-single",
            torrent: format!(
                "d4:infod9:file treed11:v2-file.bind0:d6:lengthi4e11:pieces root32:{root}eee12:meta versioni2e4:name11:v2-file.bin12:piece lengthi16384eee"
            )
            .into_bytes(),
            content: "v2-file.bin",
            length: 4,
            materialize: None,
        },
        Case {
            name: "v2-multi",
            torrent: format!(
                "d4:infod9:file treed5:a.bind0:d6:lengthi4e11:pieces root32:{root}ee5:b.bind0:d6:lengthi5e11:pieces root32:{root}eee12:meta versioni2e4:name7:v2-root12:piece lengthi16384eee"
            )
            .into_bytes(),
            content: "v2-root",
            length: 9,
            materialize: None,
        },
        Case {
            name: "hybrid-single",
            torrent: format!(
                "d4:infod9:file treed15:hybrid-file.bind0:d6:lengthi4e11:pieces root32:{root}eee6:lengthi4e12:meta versioni2e4:name15:hybrid-file.bin12:piece lengthi16384e6:pieces20:{pieces}ee"
            )
            .into_bytes(),
            content: "hybrid-file.bin",
            length: 4,
            materialize: None,
        },
        Case {
            name: "hybrid-multi",
            torrent: format!(
                "d4:infod9:file treed5:a.bind0:d6:lengthi4e11:pieces root32:{root}ee5:b.bind0:d6:lengthi5e11:pieces root32:{root}eee5:filesld6:lengthi4e4:pathl5:a.bineed4:attr1:p6:lengthi16380e4:pathl4:.pad5:16380eed6:lengthi5e4:pathl5:b.bineee12:meta versioni2e4:name11:hybrid-root12:piece lengthi16384e6:pieces40:{hybrid_pieces}ee"
            )
            .into_bytes(),
            content: "hybrid-root",
            length: 9,
            materialize: None,
        },
        Case {
            name: "v2-materialized",
            torrent: valid_v2_multifile(b"aaaa", b"bbbbb", "v2-materialized"),
            content: "v2-materialized",
            length: 9,
            materialize: Some(vec![b"aaaa".to_vec(), b"bbbbb".to_vec()]),
        },
        Case {
            name: "hybrid-materialized",
            torrent: valid_hybrid_multifile(b"aaaa", b"bbbbb", "hybrid-materialized"),
            content: "hybrid-materialized",
            length: 9,
            materialize: Some(vec![b"aaaa".to_vec(), b"bbbbb".to_vec()]),
        },
    ]
}

fn valid_v2_multifile(first: &[u8], second: &[u8], name: &str) -> Vec<u8> {
    let mut torrent = b"d4:infod9:file treed5:a.bind0:d6:lengthi4e11:pieces root32:".to_vec();
    torrent.extend_from_slice(&magpie_bt_metainfo::sha256(first));
    torrent.extend_from_slice(b"ee5:b.bind0:d6:lengthi5e11:pieces root32:");
    torrent.extend_from_slice(&magpie_bt_metainfo::sha256(second));
    torrent.extend_from_slice(b"eee12:meta versioni2e4:name");
    push_bytes(&mut torrent, name.as_bytes());
    torrent.extend_from_slice(b"12:piece lengthi16384eee");
    torrent
}

fn valid_hybrid_multifile(first: &[u8], second: &[u8], name: &str) -> Vec<u8> {
    let mut torrent = b"d4:infod9:file treed5:a.bind0:d6:lengthi4e11:pieces root32:".to_vec();
    torrent.extend_from_slice(&magpie_bt_metainfo::sha256(first));
    torrent.extend_from_slice(b"ee5:b.bind0:d6:lengthi5e11:pieces root32:");
    torrent.extend_from_slice(&magpie_bt_metainfo::sha256(second));
    torrent.extend_from_slice(b"eee5:filesld6:lengthi4e4:pathl5:a.bineed4:attr1:p6:lengthi16380e4:pathl4:.pad5:16380eed6:lengthi5e4:pathl5:b.bineee12:meta versioni2e4:name");
    push_bytes(&mut torrent, name.as_bytes());
    torrent.extend_from_slice(b"12:piece lengthi16384e6:pieces40:");
    let mut first_piece = Vec::with_capacity(16_384);
    first_piece.extend_from_slice(first);
    first_piece.resize(16_384, 0);
    torrent.extend_from_slice(&magpie_bt_metainfo::sha1(&first_piece));
    torrent.extend_from_slice(&magpie_bt_metainfo::sha1(second));
    torrent.extend_from_slice(b"ee");
    torrent
}

fn push_bytes(output: &mut Vec<u8>, value: &[u8]) {
    output.extend_from_slice(value.len().to_string().as_bytes());
    output.push(b':');
    output.extend_from_slice(value);
}

async fn poll_visible(
    client: &QbittorrentClient,
    info_hash: &str,
) -> sporos_service::qbittorrent::TorrentState {
    for _ in 0..100 {
        if let Some(state) = client
            .torrent_state(info_hash)
            .await
            .expect("torrent state")
        {
            return state;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("torrent did not become visible within ten seconds");
}

async fn poll_stopped(
    client: &QbittorrentClient,
    info_hash: &str,
) -> sporos_service::qbittorrent::TorrentState {
    for _ in 0..100 {
        if let Some(state) = client
            .torrent_state(info_hash)
            .await
            .expect("torrent state")
        {
            assert_safe_state(&state.state);
            if state.is_stopped() {
                return state;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("torrent did not stop within ten seconds");
}

async fn poll_complete(
    client: &QbittorrentClient,
    info_hash: &str,
) -> sporos_service::qbittorrent::TorrentState {
    let mut last = None;
    for _ in 0..200 {
        if let Some(state) = client
            .torrent_state(info_hash)
            .await
            .expect("torrent state")
        {
            if !state.is_checking() && state.amount_left == 0 && state.progress == 1.0 {
                return state;
            }
            last = Some(state);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let pieces = match client.piece_states(info_hash).await {
        Ok(mut body) => tokio::task::spawn_blocking(move || {
            let mut bytes = Vec::new();
            body.read_to_end(&mut bytes).map(|_| bytes)
        })
        .await
        .ok()
        .and_then(Result::ok),
        Err(_) => None,
    };
    panic!(
        "torrent did not complete recheck within twenty seconds: state={last:?} pieces={pieces:?}"
    );
}

fn assert_safe_state(state: &str) {
    assert!(
        state.starts_with("stopped") || state == "checkingResumeData",
        "torrent entered unsafe state {state}"
    );
}

fn required_env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} is set by scripts/test-qbittorrent"))
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(DIGITS[(byte >> 4) as usize] as char);
        output.push(DIGITS[(byte & 0xf) as usize] as char);
    }
    output
}
