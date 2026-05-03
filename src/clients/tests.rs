use super::{
    AsyncTorrentClient, ClientErrorCode, ClientTorrent, DelugeClient, DownloadDirOptions,
    InjectionOptions, NewTorrent, QbittorrentClient, ResumeOptions, RtorrentClient, TorrentClient,
    TransmissionClient, client_identities, client_torrent_to_searchee, select_injection_client,
};
use crate::{
    config::TorrentClientConfig,
    domain::{
        ClientLabel, ClientTorrentMetadata, Decision, File, InfoHash, InjectionResult, MediaType,
        Metafile, Searchee, TorrentClientKind, TorrentClientMetadata,
    },
};
use std::{
    borrow::Cow,
    collections::BTreeMap,
    fs,
    io::{Read, Write},
    net::TcpListener,
    path::{Path, PathBuf},
    thread,
    time::Duration,
};

#[test]
fn derives_client_hosts_from_unique_host_or_path() {
    let unique = client_identities(&[
        TorrentClientConfig::parse("qbittorrent:http://qb.example:8080").expect("client"),
        TorrentClientConfig::parse("rtorrent:http://rt.example/RPC2").expect("client"),
    ])
    .expect("identities");

    assert_eq!(unique[0].metadata.host, "qb.example");
    assert_eq!(unique[0].metadata.priority, 0);
    assert_eq!(unique[0].metadata.kind, TorrentClientKind::QBittorrent);
    assert_eq!(unique[1].metadata.host, "rt.example");

    let duplicate = client_identities(&[
        TorrentClientConfig::parse("qbittorrent:http://shared.example/qb").expect("client"),
        TorrentClientConfig::parse("transmission:http://shared.example/transmission")
            .expect("client"),
    ])
    .expect("identities");

    assert_eq!(duplicate[0].metadata.host, "shared.example/qb");
    assert_eq!(duplicate[1].metadata.host, "shared.example/transmission");

    let error = client_identities(&[
        TorrentClientConfig::parse("qbittorrent:http://shared.example/qb").expect("client"),
        TorrentClientConfig::parse("transmission:http://shared.example/qb").expect("client"),
    ])
    .expect_err("duplicate identity");
    assert!(
        error
            .to_string()
            .contains("duplicate torrent client identity")
    );
}

#[test]
fn maps_client_torrent_to_searchee_metadata() {
    let metadata = TorrentClientMetadata::new(
        "client-a",
        0,
        TorrentClientKind::QBittorrent,
        false,
        "qBittorrent",
    );
    let torrent = ClientTorrent {
        info_hash: InfoHash::from_validated("0123456789abcdef0123456789abcdef01234567"),
        name: Cow::Borrowed("Example.Show.S01E01"),
        files: vec![File::new("Example.Show.S01E01.mkv", 10)],
        save_path: Cow::Borrowed("/downloads"),
        category: Some(ClientLabel::new("tv")),
        tags: vec![ClientLabel::new("tag")],
        trackers: vec![Cow::Borrowed("tracker.example")],
        complete: true,
        checking: false,
    };

    let searchee = client_torrent_to_searchee(&metadata, torrent).expect("searchee");

    assert_eq!(searchee.title, "Example.Show.S01E01");
    assert_eq!(searchee.media_type, MediaType::Episode);
    assert_eq!(
        searchee.client.as_ref().map(|client| client.host.as_ref()),
        Some("client-a")
    );
    assert_eq!(
        searchee
            .client
            .as_ref()
            .and_then(|client| client.category.as_ref())
            .map(ClientLabel::as_str),
        Some("tv")
    );
}

#[test]
fn selects_writable_injection_client_by_rules() {
    let readonly = FakeClient::new("readonly", 0, true);
    let writable = FakeClient::new("writable", 1, false);
    let preferred = FakeClient::new("preferred", 0, false);
    let mut searchee = Searchee::from_files("Release", "Release", vec![File::new("file.mkv", 1)]);
    searchee.client = Some(crate::domain::ClientTorrentMetadata::new(
        "preferred",
        "/downloads",
        None,
        Vec::new(),
        Vec::<Cow<'static, str>>::new(),
    ));

    let clients: [&dyn TorrentClient; 3] = [&readonly, &writable, &preferred];
    let selected = select_injection_client(&clients, &searchee)
        .expect("select")
        .expect("client");

    assert_eq!(selected.metadata().host, "preferred");

    let data_source = Searchee::from_files("Release", "Release", vec![File::new("file.mkv", 1)]);
    let fallback_clients: [&dyn TorrentClient; 2] = [&readonly, &writable];
    let selected = select_injection_client(&fallback_clients, &data_source)
        .expect("select")
        .expect("client");

    assert_eq!(selected.metadata().host, "writable");
    assert!(select_injection_client(&[&readonly], &data_source).is_err());
}

#[tokio::test]
async fn async_client_facade_preserves_trait_behavior() {
    let client = FakeClient::new("async", 0, false);
    let info_hash = InfoHash::new("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").expect("hash");

    assert!(
        !client
            .is_torrent_in_client_async(&info_hash)
            .await
            .expect("in client")
    );
    assert!(
        !client
            .is_torrent_complete_async(&info_hash)
            .await
            .expect("complete")
    );
    assert_eq!(TorrentClient::metadata(&client).host.as_ref(), "async");
    client.validate_config_async().await.expect("validate");
}

#[test]
fn qbittorrent_validates_version_and_preferences() {
    let server = http_server(vec![
        http_response("200 OK", "Ok."),
        http_response("200 OK", "v4.6.2"),
        http_response("200 OK", r#"{"save_path":"/downloads"}"#),
        http_response("200 OK", ""),
    ]);
    let client = qb_client(&server.url);

    client.validate_config().expect("validate");

    let requests = server.join();
    assert!(
        requests
            .iter()
            .any(|request| request.contains("POST /api/v2/auth/login "))
    );
    assert!(
        requests
            .iter()
            .any(|request| request.contains("GET /api/v2/app/version "))
    );
    assert!(
        requests
            .iter()
            .any(|request| request.contains("GET /api/v2/app/preferences "))
    );
    assert!(
        requests
            .iter()
            .any(|request| request.contains("POST /api/v2/torrents/createTags "))
    );
    assert!(
        requests
            .iter()
            .any(|request| request.contains("tags=cross-seed"))
    );
}

#[test]
fn qbittorrent_rejects_sqlite_resume_data_with_torrent_dir() {
    let root = temp_path("qb-sqlite-resume");
    fs::create_dir_all(&root).expect("torrent dir");
    fs::write(
        root.join("0123456789abcdef0123456789abcdef01234567.fastresume"),
        b"",
    )
    .expect("fastresume");
    let server = http_server(vec![
        http_response("200 OK", "Ok."),
        http_response("200 OK", "v5.0.0"),
        http_response("200 OK", r#"{"resume_data_storage_type":"SQLite"}"#),
    ]);
    let client = qb_client(&server.url).with_torrent_dir(Some(root));

    let error = client.validate_config().expect_err("sqlite rejected");

    assert!(error.to_string().contains("SQLite resume-data mode"));
    let _requests = server.join();
}

#[test]
fn qbittorrent_requires_fastresume_sidecars_with_torrent_dir() {
    let root = temp_path("qb-fastresume-sidecar");
    fs::create_dir_all(&root).expect("torrent dir");
    fs::write(
        root.join("0123456789abcdef0123456789abcdef01234567.torrent"),
        b"",
    )
    .expect("torrent");
    let server = http_server(vec![
        http_response("200 OK", "Ok."),
        http_response("200 OK", "v4.6.2"),
        http_response("200 OK", r#"{"resume_data_storage_type":"Legacy"}"#),
    ]);
    let client = qb_client(&server.url).with_torrent_dir(Some(root));

    let error = client.validate_config().expect_err("missing fastresume");

    assert!(error.to_string().contains("missing a .fastresume sidecar"));
    let _requests = server.join();
}

#[test]
fn qbittorrent_maps_inventory_files_and_trackers() {
    let hash = "0123456789abcdef0123456789abcdef01234567";
    let server = http_server(vec![
        http_response("200 OK", "Ok."),
        http_response(
            "200 OK",
            &format!(
                r#"[{{"hash":"{hash}","name":"Example.Show.S01E01","save_path":"/downloads","category":"tv","tags":"tag, cross-seed","progress":1.0,"state":"uploading"}}]"#
            ),
        ),
        http_response(
            "200 OK",
            r#"[{"name":"Example.Show.S01E01.mkv","size":123}]"#,
        ),
        http_response("200 OK", r#"[{"url":"https://tracker.example/announce"}]"#),
    ]);
    let client = qb_client(&server.url);

    let torrents = client.get_all_torrents().expect("inventory");

    assert_eq!(torrents.len(), 1);
    assert_eq!(torrents[0].info_hash.as_str(), hash);
    assert_eq!(torrents[0].files[0].path, "Example.Show.S01E01.mkv");
    assert_eq!(
        torrents[0].category.as_ref().map(ClientLabel::as_str),
        Some("tv")
    );
    assert_eq!(torrents[0].tags.len(), 2);
    assert_eq!(torrents[0].trackers[0], "tracker.example");
    assert!(torrents[0].complete);
    let requests = server.join();
    assert!(requests.iter().any(|request| {
        request
            .contains("GET /api/v2/torrents/files?hash=0123456789abcdef0123456789abcdef01234567 ")
    }));
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.contains("POST /api/v2/auth/login "))
            .count(),
        1
    );
}

#[test]
fn qbittorrent_relogs_after_auth_failure() {
    let hash = "0123456789abcdef0123456789abcdef01234567";
    let server = http_server(vec![
        http_response("200 OK", "Ok."),
        http_response("403 Forbidden", "Forbidden"),
        http_response("200 OK", "Ok."),
        http_response(
            "200 OK",
            &format!(
                r#"[{{"hash":"{hash}","name":"Example","save_path":"/downloads","progress":1.0,"state":"uploading"}}]"#
            ),
        ),
    ]);
    let client = qb_client(&server.url);
    let metafile = Metafile::from_files(
        InfoHash::new(hash).expect("hash").into_owned(),
        "Example".to_owned(),
        "Example".to_owned(),
        42,
        vec![File::new("Example.mkv", 42)],
    );

    let remaining = client.remaining_bytes(&metafile).expect("remaining");

    assert_eq!(remaining, Some(0));
    let requests = server.join();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.contains("POST /api/v2/auth/login "))
            .count(),
        2
    );
}

#[test]
fn qbittorrent_retries_transient_info_status() {
    let hash = "0123456789abcdef0123456789abcdef01234567";
    let server = http_server(vec![
        http_response("200 OK", "Ok."),
        http_response("502 Bad Gateway", ""),
        http_response(
            "200 OK",
            &format!(
                r#"[{{"hash":"{hash}","name":"Example","save_path":"/downloads","progress":0.5,"amount_left":42,"state":"downloading"}}]"#
            ),
        ),
    ]);
    let client = qb_client(&server.url);
    let metafile = Metafile::from_files(
        InfoHash::new(hash).expect("hash").into_owned(),
        "Example".to_owned(),
        "Example".to_owned(),
        42,
        vec![File::new("Example.mkv", 42)],
    );

    let remaining = client.remaining_bytes(&metafile).expect("remaining");

    assert_eq!(remaining, Some(42));
    let requests = server.join();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.contains("GET /api/v2/torrents/info?hashes="))
            .count(),
        2
    );
}

#[test]
fn qbittorrent_visits_inventory_with_paged_file_backpressure() {
    let hash = "0123456789abcdef0123456789abcdef01234567";
    let server = http_server(vec![
        http_response("200 OK", "Ok."),
        http_response(
            "200 OK",
            &format!(
                r#"[{{"hash":"{hash}","name":"Example.Show.S01E01","save_path":"/downloads","progress":1.0,"state":"uploading"}}]"#
            ),
        ),
        http_response(
            "200 OK",
            r#"[{"name":"Example.Show.S01E01.mkv","size":123}]"#,
        ),
        http_response("200 OK", r#"[]"#),
    ]);
    let client = qb_client(&server.url);
    let mut seen = 0usize;

    client
        .for_each_torrent(&mut |torrent| {
            assert_eq!(torrent.info_hash.as_str(), hash);
            seen += 1;
            Ok(())
        })
        .expect("inventory");

    assert_eq!(seen, 1);
    let requests = server.join();
    assert!(
        requests
            .iter()
            .any(|request| { request.contains("GET /api/v2/torrents/info?offset=0&limit=1000 ") })
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.contains("GET /api/v2/torrents/files?hash="))
            .count(),
        1
    );
}

#[test]
fn qbittorrent_client_searchees_use_paged_inventory() {
    let hash = "0123456789abcdef0123456789abcdef01234567";
    let server = http_server(vec![
        http_response("200 OK", "Ok."),
        http_response(
            "200 OK",
            &format!(
                r#"[{{"hash":"{hash}","name":"Example.Show.S01E01","save_path":"/downloads","progress":1.0,"state":"uploading"}}]"#
            ),
        ),
        http_response(
            "200 OK",
            r#"[{"name":"Example.Show.S01E01.mkv","size":123}]"#,
        ),
        http_response("200 OK", r#"[]"#),
    ]);
    let client = qb_client(&server.url);

    let result = client.get_client_searchees().expect("searchees");

    assert_eq!(result.searchees.len(), 1);
    assert_eq!(result.skipped, 0);
    assert_eq!(
        result.searchees[0].info_hash.as_ref().map(InfoHash::as_str),
        Some(hash)
    );
    let requests = server.join();
    assert!(
        requests
            .iter()
            .any(|request| { request.contains("GET /api/v2/torrents/info?offset=0&limit=1000 ") })
    );
    assert!(
        !requests
            .iter()
            .any(|request| request.contains("GET /api/v2/torrents/info "))
    );
}

#[test]
fn qbittorrent_download_dir_lookup_stops_at_first_match() {
    let hash = "0123456789abcdef0123456789abcdef01234567";
    let server = http_server(vec![
        http_response("200 OK", "Ok."),
        http_response(
            "200 OK",
            &format!(
                r#"[{{"hash":"{hash}","name":"Example","save_path":"/downloads","progress":1.0,"state":"uploading"}}]"#
            ),
        ),
    ]);
    let client = qb_client(&server.url);
    let mut seen = Vec::new();

    let found = client
        .has_matching_download_dir(&mut |download_dir| {
            seen.push(download_dir.to_path_buf());
            Ok(download_dir == Path::new("/downloads"))
        })
        .expect("lookup");

    assert!(found);
    assert_eq!(seen, vec![PathBuf::from("/downloads")]);
    let requests = server.join();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.contains("GET /api/v2/torrents/info?offset="))
            .count(),
        1
    );
}

#[test]
fn qbittorrent_remaining_bytes_uses_single_info_lookup() {
    let hash = "0123456789abcdef0123456789abcdef01234567";
    let server = http_server(vec![
        http_response("200 OK", "Ok."),
        http_response(
            "200 OK",
            &format!(
                r#"[{{"hash":"{hash}","name":"Example","save_path":"/downloads","progress":0.5,"amount_left":42,"state":"downloading"}}]"#
            ),
        ),
    ]);
    let client = qb_client(&server.url);
    let metafile = Metafile::from_files(
        InfoHash::new(hash).expect("hash").into_owned(),
        "Example".to_owned(),
        "Example".to_owned(),
        42,
        vec![File::new("Example.mkv", 42)],
    );

    let remaining = client.remaining_bytes(&metafile).expect("remaining");

    assert_eq!(remaining, Some(42));
    let requests = server.join();
    assert!(requests.iter().any(|request| {
        request
            .contains("GET /api/v2/torrents/info?hashes=0123456789abcdef0123456789abcdef01234567 ")
    }));
    assert!(
        requests
            .iter()
            .all(|request| !request.contains("/api/v2/torrents/files"))
    );
}

#[test]
fn transmission_remaining_bytes_uses_single_info_lookup() {
    let hash = "0123456789abcdef0123456789abcdef01234567";
    let server = http_server(vec![
        http_response_with_headers("409 Conflict", &[("X-Transmission-Session-Id", "sid")], ""),
        http_response(
            "200 OK",
            &format!(
                r#"{{"result":"success","arguments":{{"torrents":[{{"hashString":"{hash}","name":"Example","downloadDir":"/downloads","files":[],"trackers":[],"labels":[],"percentDone":0.5,"leftUntilDone":7,"status":4}}]}}}}"#
            ),
        ),
    ]);
    let client = transmission_client(&server.url);
    let metafile = Metafile::from_files(
        InfoHash::new(hash).expect("hash").into_owned(),
        "Example".to_owned(),
        "Example".to_owned(),
        42,
        vec![File::new("Example.mkv", 42)],
    );

    let remaining = client.remaining_bytes(&metafile).expect("remaining");

    assert_eq!(remaining, Some(7));
    let requests = server.join();
    assert_eq!(requests.len(), 2);
    assert!(requests[1].contains(r#""ids":["0123456789abcdef0123456789abcdef01234567"]"#));
    assert!(requests[1].contains("leftUntilDone"));
}

#[test]
fn transmission_download_dir_lookup_stops_at_first_match() {
    let first = "0123456789abcdef0123456789abcdef01234567";
    let second = "89abcdef012345670123456789abcdef01234567";
    let server = http_server(vec![
        http_response_with_headers("409 Conflict", &[("X-Transmission-Session-Id", "sid")], ""),
        http_response(
            "200 OK",
            &format!(
                r#"{{"result":"success","arguments":{{"torrents":[{{"hashString":"{first}"}},{{"hashString":"{second}"}}]}}}}"#
            ),
        ),
        http_response(
            "200 OK",
            &format!(
                r#"{{"result":"success","arguments":{{"torrents":[{{"hashString":"{first}","downloadDir":"/match"}}]}}}}"#
            ),
        ),
    ]);
    let client = transmission_client(&server.url);

    let found = client
        .has_matching_download_dir(&mut |download_dir| Ok(download_dir == Path::new("/match")))
        .expect("lookup");

    assert!(found);
    let requests = server.join();
    assert_eq!(requests.len(), 3);
    assert!(requests[2].contains(r#""ids":["0123456789abcdef0123456789abcdef01234567"]"#));
    assert!(
        !requests
            .iter()
            .skip(2)
            .any(|request| request.contains(second))
    );
}

#[test]
fn deluge_remaining_bytes_uses_single_info_lookup() {
    let hash = "0123456789abcdef0123456789abcdef01234567";
    let server = http_server(vec![
        deluge_response("true"),
        deluge_response("true"),
        deluge_response(&format!(
            r#"{{"torrents":{{"{hash}":{{"name":"Example","save_path":"/downloads","files":[],"tracker_host":"","label":"","progress":50.0,"total_remaining":7,"state":"Downloading"}}}}}}"#
        )),
    ]);
    let client = deluge_client(&server.url);
    let metafile = Metafile::from_files(
        InfoHash::new(hash).expect("hash").into_owned(),
        "Example".to_owned(),
        "Example".to_owned(),
        42,
        vec![File::new("Example.mkv", 42)],
    );

    let remaining = client.remaining_bytes(&metafile).expect("remaining");

    assert_eq!(remaining, Some(7));
    let requests = server.join();
    assert_eq!(requests.len(), 3);
    assert!(requests[2].contains(r#""id":["0123456789abcdef0123456789abcdef01234567"]"#));
    assert!(requests[2].contains("total_remaining"));
}

#[test]
fn deluge_download_dir_lookup_stops_at_first_match() {
    let first = "0123456789abcdef0123456789abcdef01234567";
    let second = "89abcdef012345670123456789abcdef01234567";
    let server = http_server(vec![
        deluge_response("true"),
        deluge_response("true"),
        deluge_response(&format!(
            r#"{{"torrents":{{"{first}":{{"hash":"{first}"}},"{second}":{{"hash":"{second}"}}}}}}"#
        )),
        deluge_response("true"),
        deluge_response("true"),
        deluge_response(&format!(
            r#"{{"torrents":{{"{first}":{{"hash":"{first}","save_path":"/match"}}}}}}"#
        )),
    ]);
    let client = deluge_client(&server.url);

    let found = client
        .has_matching_download_dir(&mut |download_dir| Ok(download_dir == Path::new("/match")))
        .expect("lookup");

    assert!(found);
    let requests = server.join();
    assert_eq!(requests.len(), 6);
    assert!(requests[5].contains(r#""id":["0123456789abcdef0123456789abcdef01234567"]"#));
    assert!(
        !requests
            .iter()
            .skip(3)
            .any(|request| request.contains(second))
    );
}

#[test]
fn qbittorrent_contracts_presence_state_and_download_dir() {
    let hash = "0123456789abcdef0123456789abcdef01234567";
    let info = format!(
        r#"[{{"hash":"{hash}","name":"Example.Show.S01E01","save_path":"/downloads/show","progress":0.5,"state":"checkingUP"}}]"#
    );
    let server = http_server(vec![
        http_response("200 OK", "Ok."),
        http_response("200 OK", &info),
        http_response("200 OK", &info),
        http_response("200 OK", &info),
        http_response("200 OK", &info),
        http_response("200 OK", &info),
    ]);
    let client = qb_client(&server.url);
    let metafile = Metafile::from_files(
        InfoHash::from_validated(hash),
        "Example.Show.S01E01",
        "Example.Show.S01E01",
        16_384,
        vec![File::new("Example.Show.S01E01.mkv", 123)],
    );

    assert!(
        client
            .is_torrent_in_client(&metafile.info_hash)
            .expect("present")
    );
    assert!(
        !client
            .is_torrent_complete(&metafile.info_hash)
            .expect("complete")
    );
    assert!(
        client
            .is_torrent_checking(&metafile.info_hash)
            .expect("checking")
    );
    assert_eq!(
        client
            .get_download_dir(
                &metafile,
                DownloadDirOptions {
                    only_completed: true,
                },
            )
            .expect("download dir"),
        Err(ClientErrorCode::TorrentNotComplete)
    );
    assert_eq!(
        client
            .get_download_dir(
                &metafile,
                DownloadDirOptions {
                    only_completed: false,
                },
            )
            .expect("download dir")
            .expect("path"),
        PathBuf::from("/downloads/show")
    );

    let requests = server.join();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.contains("GET /api/v2/torrents/info?hashes="))
            .count(),
        5
    );
}

#[test]
fn qbittorrent_injects_with_multipart_add_and_starts() {
    let bytes = torrent_bytes("Inject.Release", 10);
    let metafile = crate::torrent::parse_metafile(&bytes).expect("metafile");
    let info = qb_info_body(&metafile.info_hash);
    let server = http_server(vec![
        http_response("200 OK", "Ok."),
        http_response("200 OK", ""),
        http_response("200 OK", &info),
        http_response("200 OK", ""),
    ]);
    let client = qb_client(&server.url);
    let new_torrent = NewTorrent {
        metafile,
        bytes: Cow::Owned(bytes),
    };
    let searchee = Searchee::from_files("Inject.Release", "Inject.Release", Vec::new());

    let result = client
        .inject(
            &new_torrent,
            &searchee,
            Decision::Match,
            &InjectionOptions {
                destination_dir: Some(PathBuf::from("/linked")),
                category: Some(ClientLabel::new("tv")),
                tags: vec![ClientLabel::new("cross-seed")],
                duplicate_categories: false,
                paused: true,
                skip_checking: true,
            },
        )
        .expect("inject");
    client
        .resume_injection(
            &new_torrent.metafile,
            Decision::Match,
            ResumeOptions::default(),
        )
        .expect("resume");

    assert_eq!(result, InjectionResult::Injected);
    let requests = server.join();
    assert!(
        requests
            .iter()
            .any(|request| request.contains("POST /api/v2/torrents/add "))
    );
    assert!(
        requests
            .iter()
            .any(|request| request.contains("POST /api/v2/torrents/start "))
    );
    assert!(
        requests
            .iter()
            .any(|request| request.contains("GET /api/v2/torrents/info?hashes="))
    );
}

#[test]
fn qbittorrent_injects_duplicate_source_category() {
    let bytes = torrent_bytes("Inject.Release", 10);
    let metafile = crate::torrent::parse_metafile(&bytes).expect("metafile");
    let info = qb_info_body(&metafile.info_hash);
    let server = http_server(vec![
        http_response("200 OK", "Ok."),
        http_response("200 OK", ""),
        http_response("200 OK", &info),
    ]);
    let client = qb_client(&server.url);
    let new_torrent = NewTorrent {
        metafile,
        bytes: Cow::Owned(bytes),
    };
    let mut searchee = Searchee::from_files("Inject.Release", "Inject.Release", Vec::new());
    searchee.client = Some(ClientTorrentMetadata::new(
        "client",
        "/downloads",
        Some(ClientLabel::new("movies")),
        Vec::new(),
        Vec::new(),
    ));

    client
        .inject(
            &new_torrent,
            &searchee,
            Decision::Match,
            &InjectionOptions {
                destination_dir: None,
                category: None,
                tags: Vec::new(),
                duplicate_categories: true,
                paused: false,
                skip_checking: true,
            },
        )
        .expect("inject");

    let requests = server.join();
    let add = requests
        .iter()
        .find(|request| request.contains("POST /api/v2/torrents/add "))
        .expect("add request");
    assert!(add.contains("movies.cross-seed"));
}

#[test]
fn qbittorrent_requires_injection_confirmation() {
    let bytes = torrent_bytes("Inject.Release", 10);
    let metafile = crate::torrent::parse_metafile(&bytes).expect("metafile");
    let server = http_server(vec![
        http_response("200 OK", "Ok."),
        http_response("200 OK", ""),
        http_response("200 OK", "[]"),
        http_response("200 OK", "[]"),
        http_response("200 OK", "[]"),
        http_response("200 OK", "[]"),
        http_response("200 OK", "[]"),
    ]);
    let client = qb_client(&server.url);
    let new_torrent = NewTorrent {
        metafile,
        bytes: Cow::Owned(bytes),
    };
    let searchee = Searchee::from_files("Inject.Release", "Inject.Release", Vec::new());

    let result = client
        .inject(
            &new_torrent,
            &searchee,
            Decision::Match,
            &InjectionOptions {
                destination_dir: None,
                category: None,
                tags: Vec::new(),
                duplicate_categories: false,
                paused: false,
                skip_checking: true,
            },
        )
        .expect("inject");

    assert_eq!(result, InjectionResult::Failure);
    let requests = server.join();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.contains("GET /api/v2/torrents/info?hashes="))
            .count(),
        super::INJECTION_CONFIRM_ATTEMPTS
    );
}

#[test]
fn transmission_negotiates_session_and_maps_inventory() {
    let hash = "0123456789abcdef0123456789abcdef01234567";
    let body = format!(
        r#"{{"result":"success","arguments":{{"torrents":[{{"hashString":"{hash}","name":"Example.Show.S01E01","downloadDir":"/downloads","files":[{{"name":"Example.Show.S01E01.mkv","length":123}}],"trackers":[{{"announce":"https://tracker.example/announce"}}],"labels":["tv","cross-seed"],"percentDone":1.0,"status":6}}]}}}}"#
    );
    let server = http_server(vec![
        http_response_with_headers("409 Conflict", &[("X-Transmission-Session-Id", "sid")], ""),
        http_response("200 OK", &body),
        http_response("200 OK", &body),
    ]);
    let client = transmission_client(&server.url);

    let torrents = client.get_all_torrents().expect("inventory");

    assert_eq!(torrents.len(), 1);
    assert_eq!(torrents[0].info_hash.as_str(), hash);
    assert_eq!(torrents[0].save_path, "/downloads");
    assert_eq!(torrents[0].files[0].path, "Example.Show.S01E01.mkv");
    assert_eq!(torrents[0].tags.len(), 2);
    assert_eq!(torrents[0].trackers[0], "tracker.example");
    assert!(torrents[0].complete);
    assert!(!torrents[0].checking);
    let requests = server.join();
    assert_eq!(requests.len(), 3);
    assert!(!requests[0].contains("X-Transmission-Session-Id"));
    assert!(
        requests[1]
            .to_ascii_lowercase()
            .contains("x-transmission-session-id: sid")
    );
    assert!(requests[1].contains(r#""method":"torrent-get""#));
}

#[test]
fn transmission_retries_transient_reads() {
    let hash = "0123456789abcdef0123456789abcdef01234567";
    let body = format!(
        r#"{{"result":"success","arguments":{{"torrents":[{{"hashString":"{hash}","name":"Example","downloadDir":"/downloads","files":[],"trackers":[],"labels":[],"percentDone":1.0,"status":6}}]}}}}"#
    );
    let server = http_server(vec![
        http_response("502 Bad Gateway", ""),
        http_response_with_headers("409 Conflict", &[("X-Transmission-Session-Id", "sid")], ""),
        http_response("200 OK", &body),
        http_response("200 OK", &body),
    ]);
    let client = transmission_client(&server.url);

    let torrents = client.get_all_torrents().expect("inventory");

    assert_eq!(torrents.len(), 1);
    let requests = server.join();
    assert_eq!(requests.len(), 4);
}

#[test]
fn transmission_injects_and_starts() {
    let bytes = torrent_bytes("Inject.Release", 10);
    let metafile = crate::torrent::parse_metafile(&bytes).expect("metafile");
    let info = transmission_info_body(&metafile.info_hash);
    let server = http_server(vec![
        http_response("200 OK", r#"{"result":"success","arguments":{}}"#),
        http_response("200 OK", &info),
        http_response("200 OK", r#"{"result":"success","arguments":{}}"#),
        http_response("200 OK", r#"{"result":"success","arguments":{}}"#),
        http_response("200 OK", r#"{"result":"success","arguments":{}}"#),
    ]);
    let client = transmission_client(&server.url);
    let new_torrent = NewTorrent {
        metafile,
        bytes: Cow::Owned(bytes),
    };
    let searchee = Searchee::from_files("Inject.Release", "Inject.Release", Vec::new());

    let result = client
        .inject(
            &new_torrent,
            &searchee,
            Decision::Match,
            &InjectionOptions {
                destination_dir: Some(PathBuf::from("/linked")),
                category: Some(ClientLabel::new("tv")),
                tags: vec![ClientLabel::new("cross-seed")],
                duplicate_categories: false,
                paused: true,
                skip_checking: true,
            },
        )
        .expect("inject");
    client
        .recheck_torrent(&new_torrent.metafile.info_hash)
        .expect("recheck");
    client
        .resume_injection(
            &new_torrent.metafile,
            Decision::Match,
            ResumeOptions::default(),
        )
        .expect("resume");

    assert_eq!(result, InjectionResult::Injected);
    let requests = server.join();
    assert_eq!(requests.len(), 5);
    assert!(requests[0].contains(r#""method":"torrent-add""#));
    assert!(requests[0].contains(r#""download-dir":"/linked""#));
    assert!(requests[0].contains(r#""labels":["tv","cross-seed"]"#));
    assert!(requests[0].contains(r#""paused":true"#));
    assert!(requests[1].contains(r#""method":"torrent-get""#));
    assert!(requests[2].contains(r#""method":"torrent-stop""#));
    assert!(requests[3].contains(r#""method":"torrent-verify""#));
    assert!(requests[4].contains(r#""method":"torrent-start""#));
}

#[test]
fn deluge_connects_and_maps_inventory() {
    let hash = "0123456789abcdef0123456789abcdef01234567";
    let body = format!(
        r#"{{"torrents":{{"{hash}":{{"name":"Example.Show.S01E01","save_path":"/downloads","files":[{{"path":"Example.Show.S01E01.mkv","size":123}}],"tracker_host":"tracker.example","label":"tv","progress":100.0,"state":"Seeding"}}}}}}"#
    );
    let server = http_server(vec![
        deluge_response("true"),
        deluge_response("true"),
        deluge_response(&body),
        deluge_response("true"),
        deluge_response("true"),
        deluge_response(&body),
    ]);
    let client = deluge_client(&server.url);

    let torrents = client.get_all_torrents().expect("inventory");

    assert_eq!(torrents.len(), 1);
    assert_eq!(torrents[0].info_hash.as_str(), hash);
    assert_eq!(torrents[0].save_path, "/downloads");
    assert_eq!(torrents[0].files[0].path, "Example.Show.S01E01.mkv");
    assert_eq!(
        torrents[0].category.as_ref().map(ClientLabel::as_str),
        Some("tv")
    );
    assert_eq!(torrents[0].trackers[0], "tracker.example");
    assert!(torrents[0].complete);
    let requests = server.join();
    assert_eq!(requests.len(), 6);
    assert!(requests[0].contains(r#""method":"auth.login""#));
    assert!(requests[1].contains(r#""method":"web.connected""#));
    assert!(requests[2].contains(r#""method":"web.update_ui""#));
}

#[test]
fn deluge_retries_transient_reads() {
    let hash = "0123456789abcdef0123456789abcdef01234567";
    let body = format!(
        r#"{{"torrents":{{"{hash}":{{"name":"Example","save_path":"/downloads","files":[],"tracker_host":"","label":"","progress":100.0,"state":"Seeding"}}}}}}"#
    );
    let server = http_server(vec![
        http_response("503 Service Unavailable", ""),
        deluge_response("true"),
        deluge_response("true"),
        deluge_response(&body),
        deluge_response("true"),
        deluge_response("true"),
        deluge_response(&body),
    ]);
    let client = deluge_client(&server.url);

    let torrents = client.get_all_torrents().expect("inventory");

    assert_eq!(torrents.len(), 1);
    let requests = server.join();
    assert_eq!(requests.len(), 7);
}

#[test]
fn deluge_injects_labels_rechecks_and_resumes() {
    let bytes = torrent_bytes("Inject.Release", 10);
    let metafile = crate::torrent::parse_metafile(&bytes).expect("metafile");
    let info = deluge_info_body(&metafile.info_hash);
    let server = http_server(vec![
        deluge_response("true"),
        deluge_response("true"),
        deluge_response("true"),
        deluge_response("true"),
        deluge_response("true"),
        deluge_response(&info),
        deluge_response("[]"),
        deluge_response("true"),
        deluge_response("true"),
        deluge_response("true"),
        deluge_response("true"),
        deluge_response("true"),
        deluge_response("true"),
        deluge_response("true"),
        deluge_response("true"),
        deluge_response("true"),
    ]);
    let client = deluge_client(&server.url);
    let new_torrent = NewTorrent {
        metafile,
        bytes: Cow::Owned(bytes),
    };
    let searchee = Searchee::from_files("Inject.Release", "Inject.Release", Vec::new());

    let result = client
        .inject(
            &new_torrent,
            &searchee,
            Decision::Match,
            &InjectionOptions {
                destination_dir: Some(PathBuf::from("/linked")),
                category: Some(ClientLabel::new("tv")),
                tags: vec![ClientLabel::new("cross-seed")],
                duplicate_categories: false,
                paused: true,
                skip_checking: true,
            },
        )
        .expect("inject");
    client
        .recheck_torrent(&new_torrent.metafile.info_hash)
        .expect("recheck");
    client
        .resume_injection(
            &new_torrent.metafile,
            Decision::Match,
            ResumeOptions::default(),
        )
        .expect("resume");

    assert_eq!(result, InjectionResult::Injected);
    let requests = server.join();
    assert_eq!(requests.len(), 16);
    assert!(requests[2].contains(r#""method":"core.add_torrent_file""#));
    assert!(requests[2].contains(r#""download_location":"/linked""#));
    assert!(requests[5].contains(r#""method":"web.update_ui""#));
    assert!(requests[6].contains(r#""method":"label.get_labels""#));
    assert!(requests[7].contains(r#""method":"label.add""#));
    assert!(requests[8].contains(r#""method":"label.set_torrent""#));
    assert!(requests[9].contains(r#""method":"core.pause_torrent""#));
    assert!(requests[12].contains(r#""method":"core.force_recheck""#));
    assert!(requests[15].contains(r#""method":"core.resume_torrent""#));
}

#[test]
fn deluge_injects_duplicate_source_category_label() {
    let bytes = torrent_bytes("Inject.Release", 10);
    let metafile = crate::torrent::parse_metafile(&bytes).expect("metafile");
    let info = deluge_info_body(&metafile.info_hash);
    let server = http_server(vec![
        deluge_response("true"),
        deluge_response("true"),
        deluge_response("true"),
        deluge_response("true"),
        deluge_response("true"),
        deluge_response(&info),
        deluge_response("[]"),
        deluge_response("true"),
        deluge_response("true"),
    ]);
    let client = deluge_client(&server.url);
    let new_torrent = NewTorrent {
        metafile,
        bytes: Cow::Owned(bytes),
    };
    let mut searchee = Searchee::from_files("Inject.Release", "Inject.Release", Vec::new());
    searchee.client = Some(ClientTorrentMetadata::new(
        "client",
        "/downloads",
        Some(ClientLabel::new("movies")),
        Vec::new(),
        Vec::new(),
    ));

    client
        .inject(
            &new_torrent,
            &searchee,
            Decision::Match,
            &InjectionOptions {
                destination_dir: None,
                category: None,
                tags: Vec::new(),
                duplicate_categories: true,
                paused: false,
                skip_checking: true,
            },
        )
        .expect("inject");

    let requests = server.join();
    assert!(requests[7].contains("movies.cross-seed"));
    assert!(requests[8].contains("movies.cross-seed"));
}

#[test]
fn rtorrent_maps_inventory_files_and_trackers() {
    let hash = "0123456789abcdef0123456789abcdef01234567";
    let server = http_server(vec![
        rt_response(&rt_array(&[rt_string(hash)])),
        rt_response(&rt_array(&[
            rt_array(&[rt_string("Example.Show.S01E01")]),
            rt_array(&[rt_string("/downloads")]),
            rt_array(&[rt_int(0)]),
            rt_array(&[rt_bool(false)]),
            rt_array(&[rt_bool(true)]),
            rt_array(&[rt_bool(false)]),
            rt_array(&[rt_bool(false)]),
            rt_array(&[rt_string("cross-seed")]),
            rt_array(&[rt_array(&[rt_array(&[
                rt_string("Example.Show.S01E01.mkv"),
                rt_int(123),
            ])])]),
            rt_array(&[rt_array(&[rt_array(&[
                rt_string("https://tracker.example/announce"),
                rt_string("tracker-group"),
            ])])]),
        ])),
    ]);
    let client = rtorrent_client(&server.url);

    let torrents = client.get_all_torrents().expect("inventory");

    assert_eq!(torrents.len(), 1);
    assert_eq!(torrents[0].info_hash.as_str(), hash);
    assert_eq!(torrents[0].save_path, "/downloads");
    assert_eq!(torrents[0].files[0].path, "Example.Show.S01E01.mkv");
    assert_eq!(torrents[0].tags[0].as_str(), "cross-seed");
    assert_eq!(torrents[0].trackers[0], "tracker.example");
    assert!(torrents[0].complete);
    let requests = server.join();
    assert_eq!(requests.len(), 2);
    assert!(requests[0].contains("<methodName>download_list</methodName>"));
    assert!(requests[1].contains("<methodName>system.multicall</methodName>"));
    assert!(requests[1].contains("f.multicall"));
    assert!(requests[1].contains("t.multicall"));
}

#[test]
fn rtorrent_retries_transient_reads() {
    let hash = "0123456789abcdef0123456789abcdef01234567";
    let server = http_server(vec![
        http_response("502 Bad Gateway", ""),
        rt_response(&rt_array(&[rt_string(hash)])),
        rt_response(&rt_array(&[
            rt_array(&[rt_string("Example")]),
            rt_array(&[rt_string("/downloads")]),
            rt_array(&[rt_int(0)]),
            rt_array(&[rt_bool(false)]),
            rt_array(&[rt_bool(true)]),
            rt_array(&[rt_bool(false)]),
            rt_array(&[rt_bool(false)]),
            rt_array(&[rt_string("cross-seed")]),
            rt_array(&[rt_array(&[])]),
            rt_array(&[rt_array(&[])]),
        ])),
    ]);
    let client = rtorrent_client(&server.url);

    let torrents = client.get_all_torrents().expect("inventory");

    assert_eq!(torrents.len(), 1);
    let requests = server.join();
    assert_eq!(requests.len(), 3);
}

#[test]
fn rtorrent_injects_labels_rechecks_and_resumes() {
    let bytes = torrent_bytes("Inject.Release", 10);
    let metafile = crate::torrent::parse_metafile(&bytes).expect("metafile");
    let server = http_server(vec![
        rt_response(&rt_string("")),
        rt_response(&rt_array(&[rt_string(metafile.info_hash.as_str())])),
        rt_response(&rt_bool(true)),
        rt_response(&rt_bool(true)),
        rt_response(&rt_bool(true)),
        rt_response(&rt_bool(true)),
    ]);
    let client = rtorrent_client(&server.url);
    let new_torrent = NewTorrent {
        metafile,
        bytes: Cow::Owned(bytes),
    };
    let searchee = Searchee::from_files("Inject.Release", "Inject.Release", Vec::new());

    let result = client
        .inject(
            &new_torrent,
            &searchee,
            Decision::Match,
            &InjectionOptions {
                destination_dir: Some(PathBuf::from("/linked")),
                category: None,
                tags: vec![ClientLabel::new("cross-seed")],
                duplicate_categories: false,
                paused: true,
                skip_checking: true,
            },
        )
        .expect("inject");
    client
        .recheck_torrent(&new_torrent.metafile.info_hash)
        .expect("recheck");
    client
        .resume_injection(
            &new_torrent.metafile,
            Decision::Match,
            ResumeOptions::default(),
        )
        .expect("resume");

    assert_eq!(result, InjectionResult::Injected);
    let requests = server.join();
    assert_eq!(requests.len(), 6);
    assert!(requests[0].contains("<methodName>load.raw</methodName>"));
    assert!(requests[0].contains("<base64>"));
    assert!(requests[0].contains("d.directory.set=/linked"));
    assert!(requests[1].contains("<methodName>download_list</methodName>"));
    assert!(requests[2].contains("<methodName>d.custom1.set</methodName>"));
    assert!(requests[3].contains("<methodName>d.pause</methodName>"));
    assert!(requests[4].contains("<methodName>d.check_hash</methodName>"));
    assert!(requests[5].contains("<methodName>d.resume</methodName>"));
}

struct FakeClient {
    metadata: TorrentClientMetadata<'static>,
}

impl FakeClient {
    fn new(host: &str, priority: u16, readonly: bool) -> Self {
        Self {
            metadata: TorrentClientMetadata::new(
                host.to_owned(),
                priority,
                TorrentClientKind::QBittorrent,
                readonly,
                "fake",
            ),
        }
    }
}

impl TorrentClient for FakeClient {
    fn metadata(&self) -> &TorrentClientMetadata<'_> {
        &self.metadata
    }

    fn is_torrent_in_client(&self, _info_hash: &InfoHash<'_>) -> crate::Result<bool> {
        Ok(false)
    }

    fn is_torrent_complete(&self, _info_hash: &InfoHash<'_>) -> crate::Result<bool> {
        Ok(false)
    }

    fn is_torrent_checking(&self, _info_hash: &InfoHash<'_>) -> crate::Result<bool> {
        Ok(false)
    }

    fn get_all_torrents(&self) -> crate::Result<Vec<ClientTorrent<'static>>> {
        Ok(Vec::new())
    }

    fn get_download_dir(
        &self,
        _metafile: &Metafile<'_>,
        _options: DownloadDirOptions,
    ) -> crate::Result<Result<PathBuf, super::ClientErrorCode>> {
        Ok(Err(super::ClientErrorCode::NotFound))
    }

    fn get_all_download_dirs(&self) -> crate::Result<BTreeMap<String, PathBuf>> {
        Ok(BTreeMap::new())
    }

    fn inject(
        &self,
        _new_torrent: &NewTorrent<'_>,
        _searchee: &Searchee<'_>,
        _decision: Decision,
        _options: &InjectionOptions,
    ) -> crate::Result<InjectionResult> {
        Ok(InjectionResult::Injected)
    }

    fn recheck_torrent(&self, _info_hash: &InfoHash<'_>) -> crate::Result<()> {
        Ok(())
    }

    fn resume_injection(
        &self,
        _metafile: &Metafile<'_>,
        _decision: Decision,
        _options: ResumeOptions,
    ) -> crate::Result<()> {
        Ok(())
    }

    fn validate_config(&self) -> crate::Result<()> {
        Ok(())
    }
}

fn qb_client(base_url: &str) -> QbittorrentClient {
    let identity =
        client_identities(&[
            TorrentClientConfig::parse(&format!("qbittorrent:{base_url}")).expect("config"),
        ])
        .expect("identity")
        .into_iter()
        .next()
        .expect("identity");
    QbittorrentClient::new(identity, Some(Duration::from_secs(1))).expect("client")
}

fn temp_path(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "sporos-clients-{name}-{}-{nanos}",
        std::process::id(),
    ))
}

fn transmission_client(base_url: &str) -> TransmissionClient {
    let identity =
        client_identities(&[
            TorrentClientConfig::parse(&format!("transmission:{base_url}")).expect("config"),
        ])
        .expect("identity")
        .into_iter()
        .next()
        .expect("identity");
    TransmissionClient::new(identity, Some(Duration::from_secs(1))).expect("client")
}

fn deluge_client(base_url: &str) -> DelugeClient {
    let identity = client_identities(&[
        TorrentClientConfig::parse(&format!("deluge:{base_url}")).expect("config")
    ])
    .expect("identity")
    .into_iter()
    .next()
    .expect("identity");
    DelugeClient::new(identity, Some(Duration::from_secs(1))).expect("client")
}

fn rtorrent_client(base_url: &str) -> RtorrentClient {
    let identity =
        client_identities(&[
            TorrentClientConfig::parse(&format!("rtorrent:{base_url}")).expect("config")
        ])
        .expect("identity")
        .into_iter()
        .next()
        .expect("identity");
    RtorrentClient::new(identity, Some(Duration::from_secs(1))).expect("client")
}

struct TestHttpServer {
    url: String,
    handle: thread::JoinHandle<Vec<String>>,
}

impl TestHttpServer {
    fn join(self) -> Vec<String> {
        self.handle.join().expect("server joins")
    }
}

fn http_server(responses: Vec<String>) -> TestHttpServer {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
    let url = format!("http://{}", listener.local_addr().expect("local addr"));
    let handle = thread::spawn(move || {
        let mut requests = Vec::new();
        for response in responses {
            let (mut stream, _) = listener.accept().expect("accept");
            let mut buf = [0_u8; 8192];
            let read = stream.read(&mut buf).expect("read");
            requests.push(String::from_utf8_lossy(&buf[..read]).into_owned());
            stream.write_all(response.as_bytes()).expect("write");
        }
        requests
    });
    TestHttpServer { url, handle }
}

fn http_response(status: &str, body: &str) -> String {
    http_response_with_headers(status, &[], body)
}

fn http_response_with_headers(status: &str, headers: &[(&str, &str)], body: &str) -> String {
    let extra_headers = headers
        .iter()
        .map(|(name, value)| format!("{name}: {value}\r\n"))
        .collect::<String>();
    format!(
        "HTTP/1.1 {status}\r\n{extra_headers}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

fn deluge_response(result: &str) -> String {
    http_response(
        "200 OK",
        &format!(r#"{{"result":{result},"error":null,"id":1}}"#),
    )
}

fn rt_response(value: &str) -> String {
    http_response(
        "200 OK",
        &format!(
            "<?xml version=\"1.0\"?><methodResponse><params><param><value>{value}</value></param></params></methodResponse>"
        ),
    )
}

fn rt_string(value: &str) -> String {
    format!("<string>{value}</string>")
}

fn rt_int(value: i64) -> String {
    format!("<i8>{value}</i8>")
}

fn rt_bool(value: bool) -> String {
    format!("<boolean>{}</boolean>", i64::from(value))
}

fn rt_array(values: &[String]) -> String {
    let values = values
        .iter()
        .map(|value| format!("<value>{value}</value>"))
        .collect::<String>();
    format!("<array><data>{values}</data></array>")
}

fn qb_info_body(info_hash: &InfoHash<'_>) -> String {
    format!(
        r#"[{{"hash":"{}","name":"Inject.Release","save_path":"/downloads","progress":1.0,"state":"uploading"}}]"#,
        info_hash.as_str()
    )
}

fn transmission_info_body(info_hash: &InfoHash<'_>) -> String {
    format!(
        r#"{{"result":"success","arguments":{{"torrents":[{{"hashString":"{}","name":"Inject.Release","downloadDir":"/downloads","files":[],"trackers":[],"labels":[],"percentDone":1.0,"status":6}}]}}}}"#,
        info_hash.as_str()
    )
}

fn deluge_info_body(info_hash: &InfoHash<'_>) -> String {
    format!(
        r#"{{"torrents":{{"{}":{{"name":"Inject.Release","save_path":"/downloads","files":[],"tracker_host":"","label":"","progress":100.0,"state":"Seeding"}}}}}}"#,
        info_hash.as_str()
    )
}

fn torrent_bytes(name: &str, length: u64) -> Vec<u8> {
    format!(
            "d4:infod6:lengthi{length}e4:name{}:{name}12:piece lengthi1e6:pieces20:aaaaaaaaaaaaaaaaaaaaee",
            name.len()
        )
        .into_bytes()
}
