use std::time::Duration;

use reqwest::Url;
use sporos_service::{
    inventory::{InventoryChange, parse_files, parse_inventory, parse_main_data},
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
        ApiKey::new(&api_key).expect("qBittorrent API key"),
    )
    .expect("qBittorrent client");

    let wrong_key = QbittorrentClient::new(
        base_url,
        ApiKey::new("qbt_abcdefghijklmnopqrstuvwxyz01").expect("fixture API key"),
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

    client
        .stop(&info_hash)
        .await
        .expect("idempotent explicit stop");
    let state = poll_stopped(client, &info_hash).await;
    assert!(state.is_stopped(), "torrent did not remain stopped");
    info_hash
}

async fn verify_inventory_reads(client: &QbittorrentClient, hashes: &[String]) {
    let main_body = client.sync_main_data(0).await.expect("full main data");
    let mut identities = Vec::new();
    let main = parse_main_data(main_body.as_slice(), u64::MAX, |change| {
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
    })
    .expect("parse full main data");
    assert!(main.full_update);
    assert_eq!(main.changed, hashes.len());

    let page_body = client.inventory_page(0, 500).await.expect("inventory page");
    let mut page_hashes = Vec::new();
    let count = parse_inventory(page_body.as_slice(), u64::MAX, |torrent| {
        assert!(!torrent.infohash_v1.is_empty() || !torrent.infohash_v2.is_empty());
        page_hashes.push(torrent.hash);
        Ok(())
    })
    .expect("parse inventory page");
    assert_eq!(count, hashes.len());
    assert_eq!(page_hashes, identities);

    for hash in hashes {
        let body = client.torrent_files(hash).await.expect("torrent files");
        let mut next = 0_usize;
        let count = parse_files(body.as_slice(), u64::MAX, 100_000, |file| {
            assert_eq!(file.index, next);
            next += 1;
            Ok(())
        })
        .expect("parse torrent files");
        assert!(count > 0);
    }
}

struct Case {
    name: &'static str,
    torrent: Vec<u8>,
    content: &'static str,
    length: u64,
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
        },
        Case {
            name: "v1-multi",
            torrent: format!(
                "d4:infod5:filesld6:lengthi4e4:pathl5:a.bineed6:lengthi5e4:pathl5:b.bineee4:name7:v1-root12:piece lengthi16384e6:pieces20:{pieces}ee"
            )
            .into_bytes(),
            content: "v1-root",
            length: 9,
        },
        Case {
            name: "v2-single",
            torrent: format!(
                "d4:infod9:file treed11:v2-file.bind0:d6:lengthi4e11:pieces root32:{root}eee12:meta versioni2e4:name11:v2-file.bin12:piece lengthi16384eee"
            )
            .into_bytes(),
            content: "v2-file.bin",
            length: 4,
        },
        Case {
            name: "v2-multi",
            torrent: format!(
                "d4:infod9:file treed5:a.bind0:d6:lengthi4e11:pieces root32:{root}ee5:b.bind0:d6:lengthi5e11:pieces root32:{root}eee12:meta versioni2e4:name7:v2-root12:piece lengthi16384eee"
            )
            .into_bytes(),
            content: "v2-root",
            length: 9,
        },
        Case {
            name: "hybrid-single",
            torrent: format!(
                "d4:infod9:file treed15:hybrid-file.bind0:d6:lengthi4e11:pieces root32:{root}eee6:lengthi4e12:meta versioni2e4:name15:hybrid-file.bin12:piece lengthi16384e6:pieces20:{pieces}ee"
            )
            .into_bytes(),
            content: "hybrid-file.bin",
            length: 4,
        },
        Case {
            name: "hybrid-multi",
            torrent: format!(
                "d4:infod9:file treed5:a.bind0:d6:lengthi4e11:pieces root32:{root}ee5:b.bind0:d6:lengthi5e11:pieces root32:{root}eee5:filesld6:lengthi4e4:pathl5:a.bineed4:attr1:p6:lengthi16380e4:pathl4:.pad5:16380eed6:lengthi5e4:pathl5:b.bineee12:meta versioni2e4:name11:hybrid-root12:piece lengthi16384e6:pieces40:{hybrid_pieces}ee"
            )
            .into_bytes(),
            content: "hybrid-root",
            length: 9,
        },
    ]
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
