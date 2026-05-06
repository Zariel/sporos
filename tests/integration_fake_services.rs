use std::{
    borrow::Cow,
    collections::BTreeMap,
    collections::VecDeque,
    fs,
    io::{Read, Write},
    net::TcpListener,
    path::PathBuf,
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use sporos::config::TorrentClientConfig;
use sporos::{
    actions::{InjectionActionOptions, SavedInjectionOptions, inject_saved_torrents},
    api::{
        AnnounceAccepted, AnnounceRequest, ApiHandlers, ApiMethod, ApiRequest, JobRequest,
        JobResponse, WebhookRequest, handle_api_request,
    },
    clients::{
        ClientErrorCode, ClientTorrent, DownloadDirOptions, InjectionOptions, NewTorrent,
        ResumeOptions, TorrentClient, TransmissionClient, client_identities,
    },
    config::{LinkType, MatchMode, RawConfig, RuntimeConfig, raw_config_from_source},
    domain::{
        ActionResult, Candidate, ClientLabel, Decision, File, InfoHash, InjectionResult, MediaType,
        Metafile, Searchee, TorrentClientKind, TorrentClientMetadata,
    },
    integrations::{
        ArrKind, CategoryCaps, LimitCaps, RssPagerOptions, SearchIndexer, TorznabCaps,
        cache_torrent_file, fetch_torznab_caps, lookup_arr_ids, rss_pager, validate_arr_instance,
        validate_arr_url, validate_torznab_url,
    },
    matching::AssessmentOptions,
    notifications::NotificationSender,
    operations::{
        run_announce_match, run_rss_workflow, run_search_workflow, run_update_indexer_caps,
    },
    persistence::{Database, DecisionRecord, SqlValue},
    scheduler::DaemonPlan,
    search::Blocklist,
    startup::Redactor,
    torrent::{parse_metafile, torrent_cache_dir},
};

#[test]
fn fake_torznab_rss_service_pages_and_persists_sqlite_state() {
    let server = FakeHttpServer::new(vec![
        HttpResponse::rss(
            r#"<rss><channel>
                <item><title>Example.Show.S01E02</title><guid>new-guid</guid><link>https://indexer.example/2.torrent</link><size>200</size><pubDate>Fri, 01 May 2026 00:00:00 GMT</pubDate></item>
                <item><title>Example.Show.S01E01</title><guid>old-guid</guid><link>https://indexer.example/1.torrent</link><size>100</size><pubDate>Thu, 30 Apr 2026 00:00:00 GMT</pubDate></item>
            </channel></rss>"#,
        ),
        HttpResponse::rss("<rss><channel></channel></rss>"),
    ]);
    let root = temp_path("rss-service");
    fs::create_dir_all(&root).expect("root");
    let database = Database::open_app_dir(&root).expect("database");
    database
        .execute_sql(
            "INSERT INTO indexer (id, url, apikey, active) VALUES (7, ?1, 'secret', 1)",
            &[SqlValue::Text(std::borrow::Cow::Borrowed(&server.url))],
        )
        .expect("indexer");
    let indexer = SearchIndexer {
        id: 7,
        url: server.url.clone(),
        apikey: "secret".to_owned(),
        caps: TorznabCaps {
            search: true,
            categories: CategoryCaps {
                tv: true,
                ..CategoryCaps::default()
            },
            limits: LimitCaps { default: 2, max: 2 },
            ..TorznabCaps::default()
        },
    };

    let candidates = rss_pager(
        &database,
        &indexer,
        RssPagerOptions {
            time_since_last_run: Duration::from_secs(86_400),
            timeout: Some(Duration::from_secs(1)),
            delay: Duration::ZERO,
            now_millis: 1_776_000_000_000,
        },
    )
    .expect("rss");

    assert_eq!(candidates.len(), 2);
    assert_eq!(candidates[0].guid, "new-guid");
    let cursor: String = database
        .read_rss_cursor(indexer.id)
        .expect("cursor")
        .expect("cursor");
    assert_eq!(cursor, "new-guid");
    let requests = server.join();
    assert!(
        requests
            .iter()
            .any(|request| request.contains("apikey=secret"))
    );
    assert!(requests.iter().any(|request| request.contains("offset=2")));
    if let Err(_error) = fs::remove_dir_all(root) {}
}

#[test]
fn fake_arr_and_notification_services_cover_external_http_contracts() {
    let sonarr = FakeHttpServer::new(vec![HttpResponse::json("{}")]);
    let radarr = FakeHttpServer::new(vec![HttpResponse::json(
        r#"{"movie":{"tmdbId":888,"imdbId":"tt888"}}"#,
    )]);
    let sonarr_config = validate_arr_url(&format!("{}?apikey=sonarr", sonarr.url), ArrKind::Sonarr)
        .expect("sonarr config");
    let radarr_config = validate_arr_url(&format!("{}?apikey=radarr", radarr.url), ArrKind::Radarr)
        .expect("radarr config");
    let mut searchee = Searchee::from_files(
        "Example Movie 2026",
        "Example Movie 2026",
        vec![File::new("Example.Movie.2026.mkv", 100)],
    );
    searchee.media_type = MediaType::Video;

    let lookup = lookup_arr_ids(
        &[sonarr_config, radarr_config],
        &searchee,
        Some(Duration::from_secs(1)),
    )
    .expect("arr lookup")
    .expect("ids");

    assert_eq!(lookup.ids.tmdbid.as_deref(), Some("888"));
    assert!(lookup.cache_key.contains("tmdbid=888"));
    assert!(
        sonarr
            .join()
            .iter()
            .any(|request| request.to_ascii_lowercase().contains("x-api-key: sonarr"))
    );
    assert!(
        radarr
            .join()
            .iter()
            .any(|request| request.to_ascii_lowercase().contains("x-api-key: radarr"))
    );

    let notifications = FakeHttpServer::new(vec![HttpResponse::json("{}")]);
    let sender = NotificationSender::new(vec![notifications.url.clone()], Redactor::default())
        .expect("sender");
    let report = sender.send_test();

    assert_eq!(report.attempted, 1);
    assert_eq!(report.succeeded, 1);
    let notification_requests = notifications.join();
    assert!(
        notification_requests
            .iter()
            .any(|request| request.contains(r#""event":"TEST""#))
    );
}

#[test]
fn fake_services_cover_retry_after_and_unsafe_client_retry() {
    let torznab = FakeHttpServer::new(vec![
        HttpResponse::new("429 Too Many Requests", &[("Retry-After", "0")], ""),
        HttpResponse::new(
            "200 OK",
            &[("Content-Type", "application/xml")],
            r#"<caps><searching searchAvailable="yes" /><limits default="25" max="100" /></caps>"#,
        ),
    ]);
    let torznab_config =
        validate_torznab_url(&format!("{}/api?apikey=torznab", torznab.url)).expect("torznab");
    let caps = fetch_torznab_caps(&torznab_config).expect("caps");
    assert!(caps.search);
    assert_eq!(torznab.join().len(), 2);

    let arr = FakeHttpServer::new(vec![
        HttpResponse::new("503 Service Unavailable", &[("Retry-After", "0")], ""),
        HttpResponse::json(r#"{"current":"4.0.0"}"#),
    ]);
    let arr_config =
        validate_arr_url(&format!("{}?apikey=arr", arr.url), ArrKind::Sonarr).expect("arr");
    validate_arr_instance(&arr_config, Some(Duration::from_secs(1))).expect("arr validate");
    assert_eq!(arr.join().len(), 2);

    let notifications = FakeHttpServer::new(vec![
        HttpResponse::new("503 Service Unavailable", &[("Retry-After", "0")], ""),
        HttpResponse::json("{}"),
    ]);
    let sender = NotificationSender::new(vec![notifications.url.clone()], Redactor::default())
        .expect("sender");
    let report = sender.send_test();
    assert_eq!(report.succeeded, 1);
    assert_eq!(report.retry_exhausted, 0);
    assert_eq!(notifications.join().len(), 2);

    let transmission = FakeHttpServer::new_until_idle(
        vec![
            HttpResponse::new("502 Bad Gateway", &[], ""),
            HttpResponse::json(r#"{"result":"success","arguments":{}}"#),
        ],
        Duration::from_millis(150),
    );
    let identity = client_identities(&[TorrentClientConfig::parse(&format!(
        "transmission:{}",
        transmission.url
    ))
    .expect("client config")])
    .expect("identity")
    .into_iter()
    .next()
    .expect("identity");
    let client = TransmissionClient::new(identity, Some(Duration::from_secs(1))).expect("client");
    let bytes = torrent_bytes("Unsafe.Inject", 10);
    let metafile = parse_metafile(&bytes).expect("metafile");
    let new_torrent = NewTorrent {
        metafile,
        bytes: Cow::Owned(bytes),
    };
    let searchee = Searchee::from_files("Unsafe.Inject", "Unsafe.Inject", Vec::new());

    let _error = client
        .inject(
            &new_torrent,
            &searchee,
            Decision::Match,
            &InjectionOptions::default(),
        )
        .expect_err("unsafe inject should fail without retrying");
    let transmission_requests = transmission.join();
    assert_eq!(transmission_requests.len(), 1);
    assert!(transmission_requests[0].contains(r#""method":"torrent-add""#));
}

#[test]
fn rss_workflow_rejects_client_owned_info_hash_before_save() {
    let root = temp_path("rss-client-hash");
    let app_dir = root.join("app");
    let data_dir = root.join("data");
    let release_dir = data_dir.join("Existing.Show.S01E01");
    let output_dir = root.join("output");
    fs::create_dir_all(&app_dir).expect("app dir");
    fs::create_dir_all(&release_dir).expect("release dir");
    fs::write(release_dir.join("Existing.Show.S01E01.mkv"), b"episode").expect("episode");

    let bytes = torrent_bytes("Existing.Show.S01E01.mkv", 7);
    let metafile = parse_metafile(&bytes).expect("metafile");
    let info_hash = metafile.info_hash.to_string();
    cache_torrent_file(&app_dir, &bytes).expect("cache torrent");
    let client = FakeHttpServer::new_until_idle(
        qbittorrent_inventory_responses(
            &info_hash,
            "Different.Client.Release",
            "Different.Client.Release.mkv",
            7,
        ),
        Duration::from_millis(250),
    );
    let torznab = FakeHttpServer::new_with_url_until_idle(
        |torznab_url| {
            vec![
                HttpResponse::rss(&rss_item(
                    "Existing.Show.S01E01",
                    "client-owned-rss",
                    &format!("{torznab_url}/download/existing.torrent"),
                )),
                HttpResponse::rss("<rss><channel></channel></rss>"),
            ]
        },
        Duration::from_millis(250),
    );
    let config = client_backed_config(&app_dir, &data_dir, &output_dir, &torznab.url, &client.url);
    let database = Database::open_app_dir(&app_dir).expect("database");
    let seed_searchee_id = database
        .get_or_insert_searchee("seed")
        .expect("seed searchee");
    database
        .upsert_decision(&DecisionRecord {
            searchee_id: seed_searchee_id,
            guid: "client-owned-rss",
            info_hash: Some(&info_hash),
            decision: Decision::Match,
            first_seen: 1,
            last_seen: 1,
            fuzzy_size_factor: 0.05,
        })
        .expect("seed decision");
    let notifier = NotificationSender::new(Vec::new(), Redactor::default()).expect("notifier");

    let result = run_rss_workflow(&database, &app_dir, &config, &notifier).expect("rss");

    assert_eq!(result.attempts, 1);
    let existing_decisions: i64 = database
        .query_scalar(
            "SELECT COUNT(*) FROM decision WHERE decision = ?1",
            &[SqlValue::Text(Cow::Borrowed(
                Decision::InfoHashAlreadyExists.as_str(),
            ))],
        )
        .expect("decision");
    assert_eq!(existing_decisions, 1);
    assert_eq!(torrent_file_count(&output_dir), 0);
    assert!(
        client
            .join()
            .iter()
            .any(|request| request.contains("/api/v2/torrents/info?offset=0&limit=1000"))
    );
    let _torznab_requests = torznab.join();
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn announce_workflow_rejects_client_owned_info_hash_before_save() {
    let root = temp_path("announce-client-hash");
    let app_dir = root.join("app");
    let data_dir = root.join("data");
    let release_dir = data_dir.join("Existing.Show.S01E01");
    let output_dir = root.join("output");
    fs::create_dir_all(&app_dir).expect("app dir");
    fs::create_dir_all(&release_dir).expect("release dir");
    fs::write(release_dir.join("Existing.Show.S01E01.mkv"), b"episode").expect("episode");

    let bytes = torrent_bytes("Existing.Show.S01E01.mkv", 7);
    let metafile = parse_metafile(&bytes).expect("metafile");
    let info_hash = metafile.info_hash.to_string();
    let torrent_body = String::from_utf8(bytes).expect("torrent bytes");
    let client = FakeHttpServer::new_until_idle(
        qbittorrent_inventory_responses(
            &info_hash,
            "Different.Client.Release",
            "Different.Client.Release.mkv",
            7,
        ),
        Duration::from_millis(250),
    );
    let download = FakeHttpServer::new_until_idle(
        vec![HttpResponse::new(
            "200 OK",
            &[("Content-Type", "application/x-bittorrent")],
            &torrent_body,
        )],
        Duration::from_millis(250),
    );
    let config = client_backed_config(&app_dir, &data_dir, &output_dir, &download.url, &client.url);
    let database = Database::open_app_dir(&app_dir).expect("database");
    let notifier = NotificationSender::new(Vec::new(), Redactor::default()).expect("notifier");
    let candidate = Candidate::new(
        "Existing.Show.S01E01",
        "client-owned-announce",
        Some(format!("{}/existing.torrent", download.url)),
        "tracker",
    );

    let outcome = run_announce_match(&database, &app_dir, &config, candidate, &notifier)
        .expect("announce")
        .expect("outcome");

    assert_eq!(outcome.decision, Decision::InfoHashAlreadyExists);
    assert_eq!(outcome.action_result, None);
    assert_eq!(torrent_file_count(&output_dir), 0);
    assert!(
        client
            .join()
            .iter()
            .any(|request| request.contains("/api/v2/torrents/info?offset=0&limit=1000"))
    );
    let _download_requests = download.join();
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn replacement_workflows_use_fresh_db_and_fake_services() {
    let root = temp_path("replacement-flow");
    let app_dir = root.join("app");
    let data_dir = root.join("data");
    let release_dir = data_dir.join("Replacement.Show.S01E01");
    let output_dir = root.join("output");
    fs::create_dir_all(&app_dir).expect("app dir");
    fs::create_dir_all(&release_dir).expect("release dir");
    fs::write(release_dir.join("Replacement.Show.S01E01.mkv"), b"episode").expect("episode");

    let bytes = torrent_bytes("Replacement.Show.S01E01.mkv", 7);
    let torrent_body = String::from_utf8(bytes.clone()).expect("torrent bytes");
    let download_path = "/download/replacement.torrent";
    let torznab = FakeHttpServer::new_with_url_until_idle(
        |torznab_url| {
            vec![
                HttpResponse::new(
                    "200 OK",
                    &[("Content-Type", "application/xml")],
                    r#"<caps><searching searchAvailable="yes" tv-searchAvailable="yes" /><limits default="100" max="100" /></caps>"#,
                ),
                HttpResponse::rss(&rss_item(
                    "Replacement.Show.S01E01",
                    "search-guid",
                    &format!("{torznab_url}{download_path}"),
                )),
                HttpResponse::new(
                    "200 OK",
                    &[("Content-Type", "application/x-bittorrent")],
                    &torrent_body,
                ),
                HttpResponse::rss(&rss_item(
                    "Replacement.Show.S01E01",
                    "search-guid",
                    &format!("{torznab_url}{download_path}"),
                )),
                HttpResponse::rss("<rss><channel></channel></rss>"),
            ]
        },
        Duration::from_millis(250),
    );
    let torznab_url = torznab.url.clone();
    let download_url = format!("{torznab_url}{download_path}");

    let raw = raw_config_from_source(&format!(
        r#"
        state_dir = "{}"
        database_path = "{}/sporos.db"
        data_dirs = ["{}"]
        output_dir = "{}"
        use_client_torrents = false
        include_single_episodes = true
        match_mode = "partial"
        action = "save"
        search_timeout = "1s"
        snatch_timeout = "1s"
        skip_recheck = true
        injection_category = "tv"
        injection_tags = ["managed"]

        [[torznab]]
        url = "{}/api"
        api_key = "key"
        "#,
        app_dir.display(),
        app_dir.display(),
        data_dir.display(),
        output_dir.display(),
        torznab_url,
    ))
    .expect("raw config");
    let mut config = RuntimeConfig::normalize(raw, &app_dir).expect("config");
    config.delay = 0;
    let database = Database::open_app_dir(&app_dir).expect("database");
    let notifier = NotificationSender::new(Vec::new(), Redactor::default()).expect("notifier");

    let caps = run_update_indexer_caps(&database, &config).expect("caps");
    assert_eq!(caps.indexers, 1);
    assert_eq!(caps.updated, 1);

    let search = run_search_workflow(&database, &app_dir, &config, &notifier).expect("search");
    assert_eq!(search.indexers, 1);
    assert_eq!(search.pipeline.attempts_total, 1);
    assert!(
        search
            .pipeline
            .attempts
            .iter()
            .any(|attempt| attempt.action_result.is_some_and(ActionResult::accepted))
    );
    assert_eq!(torrent_file_count(&output_dir), 1);

    let rss = run_rss_workflow(&database, &app_dir, &config, &notifier).expect("rss");
    assert_eq!(rss.candidates, 1);
    assert_eq!(rss.attempts, 1);

    let announce = run_announce_match(
        &database,
        &app_dir,
        &config,
        Candidate::new(
            "Replacement.Show.S01E01",
            "search-guid",
            Some(download_url.clone()),
            "ReplacementTracker",
        ),
        &notifier,
    )
    .expect("announce")
    .expect("announce match");
    assert_eq!(announce.decision, Decision::Match);
    assert!(matches!(
        announce.action_result,
        Some(ActionResult::Save(_))
    ));

    let decisions: i64 = database
        .query_scalar("SELECT COUNT(*) FROM decision", &[])
        .expect("decision count");
    assert!(decisions >= 1);
    let cached_torrents = fs::read_dir(torrent_cache_dir(&app_dir))
        .expect("cache dir")
        .filter_map(Result::ok)
        .count();
    assert_eq!(cached_torrents, 1);

    let client = RecordingClient::new();
    let clients: [&dyn TorrentClient; 1] = [&client];
    let blocklist = Blocklist::parse(&[]).expect("blocklist");
    let excluded = std::collections::BTreeSet::new();
    let assessment = AssessmentOptions {
        match_mode: MatchMode::Partial,
        fuzzy_size_threshold: config.fuzzy_size_threshold,
        season_from_episodes: 1.0,
        include_single_episodes: true,
        info_hashes_to_exclude: &excluded,
        blocklist: &blocklist,
    };
    let injection = InjectionActionOptions {
        clients: &clients,
        output_dir: Some(&output_dir),
        link_dirs: &[],
        link_type: LinkType::Symlink,
        flat_linking: false,
        unwrap_symlinks: false,
        skip_recheck: true,
        match_mode: MatchMode::Partial,
        auto_resume_max_download: 0,
        ignore_non_relevant_files_to_resume: false,
        category: Some(ClientLabel::new("tv")),
        tags: vec![ClientLabel::new("managed")],
        duplicate_categories: false,
    };
    let mut retry_searchee = Searchee::from_files(
        "Replacement.Show.S01E01",
        "Replacement.Show.S01E01",
        vec![File::new("Replacement.Show.S01E01.mkv", 7)],
    );
    retry_searchee.path = Some(Cow::Owned(release_dir.display().to_string()));
    retry_searchee.mtime_millis = Some(u64::MAX);
    retry_searchee.media_type = MediaType::Episode;
    let injected = inject_saved_torrents(
        &SavedInjectionOptions {
            input_dir: &output_dir,
            injection: &injection,
            assessment: &assessment,
            ignore_titles: true,
        },
        &[retry_searchee],
        |_| Ok(()),
    )
    .expect("inject");
    assert_eq!(injected.scanned, 1);
    assert_eq!(injected.injected, 1);
    assert_eq!(client.injected(), 1);
    assert_eq!(torrent_file_count(&output_dir), 0);

    let torznab_requests = torznab.join();
    assert!(
        torznab_requests
            .iter()
            .any(|request| request.contains("t=caps"))
    );
    assert!(
        torznab_requests
            .iter()
            .any(|request| request.contains("t=search"))
    );
    assert!(
        torznab_requests
            .iter()
            .any(|request| request.contains(download_path))
    );

    let _cleanup = fs::remove_dir_all(root);
}

#[tokio::test]
async fn daemon_api_and_scheduler_use_temp_sqlite_app_dir() {
    let root = temp_path("daemon-api");
    fs::create_dir_all(&root).expect("root");
    let data_dir = root.join("data");
    fs::create_dir_all(&data_dir).expect("data dir");
    let database = Database::open_app_dir(&root).expect("database");
    let config = RuntimeConfig::normalize(
        RawConfig {
            listen_port: Some(None),
            rss_cadence: Some(600_000),
            data_dirs: vec![data_dir],
            ..RawConfig::default()
        },
        &root,
    )
    .expect("config");
    let mut plan = DaemonPlan::from_config(&config);
    let run = plan
        .run_startup(&database, 2_000_000, || Ok(()))
        .expect("startup");
    assert!(run.startup_indexed);
    assert!(!run.serving);
    assert!(run.jobs.iter().any(|job| job.name.as_str() == "rss"));

    let mut handlers = TestHandlers::default();
    let ping = handle_api_request(
        ApiRequest::new(ApiMethod::Get, "/api/ping", BTreeMap::new(), ""),
        "secret",
        &mut handlers,
    )
    .await
    .expect("ping");
    assert_eq!(ping.status, 200);

    let unauthorized = handle_api_request(
        ApiRequest::new(ApiMethod::Get, "/api/status", BTreeMap::new(), ""),
        "secret",
        &mut handlers,
    )
    .await
    .expect("status");
    assert_eq!(unauthorized.status, 401);

    let webhook_path = root.join("webhook-source.mkv");
    fs::write(&webhook_path, b"data").expect("webhook path");
    let headers = BTreeMap::from([("X-Api-Key".to_owned(), "secret".to_owned())]);
    let webhook = handle_api_request(
        ApiRequest::new(
            ApiMethod::Post,
            "/api/webhook",
            headers.clone(),
            format!("path={}&includeNonVideos=true", webhook_path.display()),
        ),
        "secret",
        &mut handlers,
    )
    .await
    .expect("webhook");
    assert_eq!(webhook.status, 204);
    assert_eq!(handlers.webhooks, 1);

    let job = handle_api_request(
        ApiRequest::new(ApiMethod::Post, "/api/job", headers, r#"{"name":"rss"}"#),
        "secret",
        &mut handlers,
    )
    .await
    .expect("job");
    assert_eq!(job.status, 409);
    assert_eq!(handlers.jobs, 1);
    if let Err(_error) = fs::remove_dir_all(root) {}
}

#[derive(Debug, Clone)]
struct HttpResponse {
    status: &'static str,
    headers: Vec<(&'static str, &'static str)>,
    body: String,
}

impl HttpResponse {
    fn new(status: &'static str, headers: &[(&'static str, &'static str)], body: &str) -> Self {
        Self {
            status,
            headers: headers.to_vec(),
            body: body.to_owned(),
        }
    }

    fn json(body: &str) -> Self {
        Self {
            status: "200 OK",
            headers: vec![("Content-Type", "application/json")],
            body: body.to_owned(),
        }
    }

    fn rss(body: &str) -> Self {
        Self {
            status: "200 OK",
            headers: vec![("Content-Type", "application/rss+xml")],
            body: body.to_owned(),
        }
    }

    fn bytes(&self) -> Vec<u8> {
        let mut response = format!(
            "HTTP/1.1 {}\r\nContent-Length: {}\r\nConnection: close\r\n",
            self.status,
            self.body.len()
        );
        for (name, value) in &self.headers {
            response.push_str(&format!("{name}: {value}\r\n"));
        }
        response.push_str("\r\n");
        response.push_str(&self.body);
        response.into_bytes()
    }
}

struct FakeHttpServer {
    url: String,
    requests: Arc<Mutex<Vec<String>>>,
    handle: thread::JoinHandle<()>,
}

impl FakeHttpServer {
    fn new(responses: Vec<HttpResponse>) -> Self {
        Self::with_idle_timeout(responses, None)
    }

    fn new_until_idle(responses: Vec<HttpResponse>, idle_timeout: Duration) -> Self {
        Self::with_idle_timeout(responses, Some(idle_timeout))
    }

    fn new_with_url_until_idle<F>(responses: F, idle_timeout: Duration) -> Self
    where
        F: FnOnce(&str) -> Vec<HttpResponse>,
    {
        Self::with_url_idle_timeout(responses, Some(idle_timeout))
    }

    fn with_idle_timeout(responses: Vec<HttpResponse>, idle_timeout: Option<Duration>) -> Self {
        Self::with_url_idle_timeout(|_| responses, idle_timeout)
    }

    fn with_url_idle_timeout<F>(responses: F, idle_timeout: Option<Duration>) -> Self
    where
        F: FnOnce(&str) -> Vec<HttpResponse>,
    {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake server");
        if idle_timeout.is_some() {
            listener
                .set_nonblocking(true)
                .expect("nonblocking listener");
        }
        let url = format!("http://{}", listener.local_addr().expect("local addr"));
        let responses = responses(&url);
        let requests = Arc::new(Mutex::new(Vec::new()));
        let server_requests = Arc::clone(&requests);
        let handle = thread::spawn(move || {
            let mut responses = VecDeque::from(responses);
            let mut idle_since = Instant::now();
            while let Some(response) = responses.pop_front() {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        idle_since = Instant::now();
                        stream
                            .set_read_timeout(Some(Duration::from_secs(1)))
                            .expect("read timeout");
                        let request = read_http_request(&mut stream).expect("read request");
                        server_requests.lock().expect("requests lock").push(request);
                        stream.write_all(&response.bytes()).expect("write response");
                    }
                    Err(error)
                        if idle_timeout.is_some()
                            && error.kind() == std::io::ErrorKind::WouldBlock =>
                    {
                        responses.push_front(response);
                        if idle_since.elapsed() >= idle_timeout.expect("idle timeout") {
                            break;
                        }
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) => {
                        server_requests
                            .lock()
                            .expect("requests lock")
                            .push(format!("accept error: {error}"));
                        break;
                    }
                }
            }
        });
        Self {
            url,
            requests,
            handle,
        }
    }

    fn join(self) -> Vec<String> {
        self.handle.join().expect("server thread");
        Arc::try_unwrap(self.requests)
            .expect("requests references")
            .into_inner()
            .expect("requests mutex")
    }
}

fn read_http_request(stream: &mut std::net::TcpStream) -> std::io::Result<String> {
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 1024];
    loop {
        match stream.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => {
                if let Some(chunk) = buffer.get(..read) {
                    bytes.extend_from_slice(chunk);
                }
                if request_body_complete(&bytes) {
                    break;
                }
            }
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                break;
            }
            Err(error) => return Err(error),
        }
    }
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

fn request_body_complete(bytes: &[u8]) -> bool {
    let Some(header_end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") else {
        return false;
    };
    let Some(header_bytes) = bytes.get(..header_end) else {
        return false;
    };
    let headers = String::from_utf8_lossy(header_bytes);
    let content_length = headers
        .lines()
        .find_map(|line| line.split_once(':'))
        .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, value)| value.trim().parse::<usize>().ok())
        .unwrap_or(0);
    bytes.len() >= header_end.saturating_add(4).saturating_add(content_length)
}

#[derive(Default)]
struct TestHandlers {
    webhooks: usize,
    jobs: usize,
}

#[async_trait::async_trait]
impl ApiHandlers for TestHandlers {
    async fn announce(&mut self, _request: AnnounceRequest) -> sporos::Result<AnnounceAccepted> {
        Ok(AnnounceAccepted {
            work_id: "work-1".to_owned(),
            status: "queued".to_owned(),
        })
    }

    async fn webhook(&mut self, _request: WebhookRequest) -> sporos::Result<()> {
        self.webhooks += 1;
        Ok(())
    }

    async fn job(&mut self, _request: JobRequest) -> sporos::Result<JobResponse> {
        self.jobs += 1;
        Ok(JobResponse::AlreadyRunning(
            "rss: already running".to_owned(),
        ))
    }
}

#[derive(Clone)]
struct RecordingClient {
    metadata: TorrentClientMetadata<'static>,
    injected: Arc<Mutex<usize>>,
}

impl RecordingClient {
    fn new() -> Self {
        Self {
            metadata: TorrentClientMetadata::new(
                "fake-client",
                0,
                TorrentClientKind::QBittorrent,
                false,
                "fake",
            ),
            injected: Arc::new(Mutex::new(0)),
        }
    }

    fn injected(&self) -> usize {
        *self.injected.lock().expect("injected lock")
    }
}

impl TorrentClient for RecordingClient {
    fn metadata(&self) -> &TorrentClientMetadata<'_> {
        &self.metadata
    }

    fn is_torrent_in_client(&self, _info_hash: &InfoHash<'_>) -> sporos::Result<bool> {
        Ok(false)
    }

    fn is_torrent_complete(&self, _info_hash: &InfoHash<'_>) -> sporos::Result<bool> {
        Ok(true)
    }

    fn is_torrent_checking(&self, _info_hash: &InfoHash<'_>) -> sporos::Result<bool> {
        Ok(false)
    }

    fn get_all_torrents(&self) -> sporos::Result<Vec<ClientTorrent<'static>>> {
        Ok(Vec::new())
    }

    fn get_download_dir(
        &self,
        _metafile: &Metafile<'_>,
        _options: DownloadDirOptions,
    ) -> sporos::Result<Result<PathBuf, ClientErrorCode>> {
        Ok(Err(ClientErrorCode::NotFound))
    }

    fn get_all_download_dirs(&self) -> sporos::Result<BTreeMap<String, PathBuf>> {
        Ok(BTreeMap::new())
    }

    fn has_matching_download_dir(
        &self,
        _predicate: &mut dyn FnMut(&std::path::Path) -> sporos::Result<bool>,
    ) -> sporos::Result<bool> {
        Ok(false)
    }

    fn remaining_bytes(&self, _metafile: &Metafile<'_>) -> sporos::Result<Option<u64>> {
        Ok(Some(0))
    }

    fn inject(
        &self,
        _new_torrent: &NewTorrent<'_>,
        _searchee: &Searchee<'_>,
        _decision: Decision,
        _options: &InjectionOptions,
    ) -> sporos::Result<InjectionResult> {
        *self.injected.lock().expect("injected lock") += 1;
        Ok(InjectionResult::Injected)
    }

    fn recheck_torrent(&self, _info_hash: &InfoHash<'_>) -> sporos::Result<()> {
        Ok(())
    }

    fn resume_injection(
        &self,
        _metafile: &Metafile<'_>,
        _decision: Decision,
        _options: ResumeOptions,
    ) -> sporos::Result<()> {
        Ok(())
    }

    fn validate_config(&self) -> sporos::Result<()> {
        Ok(())
    }
}

fn temp_path(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!(
        "sporos-integration-{label}-{}-{nanos}",
        std::process::id()
    ))
}

fn rss_item(title: &str, guid: &str, link: &str) -> String {
    format!(
        r#"<rss><channel>
            <item>
                <title>{title}</title>
                <guid>{guid}</guid>
                <link>{link}</link>
                <size>7</size>
                <pubDate>Fri, 01 May 2099 00:00:00 GMT</pubDate>
                <indexer>ReplacementTracker</indexer>
            </item>
        </channel></rss>"#
    )
}

fn torrent_file_count(dir: &std::path::Path) -> usize {
    fs::read_dir(dir)
        .map(|entries| {
            entries
                .filter_map(Result::ok)
                .filter(|entry| {
                    entry.path().extension().and_then(|value| value.to_str()) == Some("torrent")
                })
                .count()
        })
        .unwrap_or(0)
}

fn qbittorrent_inventory_responses(
    info_hash: &str,
    torrent_name: &str,
    file_name: &str,
    file_size: u64,
) -> Vec<HttpResponse> {
    vec![
        HttpResponse::new("200 OK", &[], "Ok."),
        HttpResponse::json(&format!(
            r#"[{{"hash":"{info_hash}","name":"{torrent_name}","save_path":"/downloads","progress":1.0,"state":"uploading"}}]"#
        )),
        HttpResponse::json(&format!(r#"[{{"name":"{file_name}","size":{file_size}}}]"#)),
        HttpResponse::json("[]"),
    ]
}

fn client_backed_config(
    app_dir: &std::path::Path,
    data_dir: &std::path::Path,
    output_dir: &std::path::Path,
    torznab_url: &str,
    client_url: &str,
) -> RuntimeConfig {
    raw_config_from_source(&format!(
        r#"
        state_dir = "{}"
        database_path = "{}/sporos.db"
        data_dirs = ["{}"]
        output_dir = "{}"
        use_client_torrents = true
        include_single_episodes = true
        action = "save"
        search_timeout = "1s"
        snatch_timeout = "1s"

        [[torznab]]
        url = "{}/api"
        api_key = "key"

        [[torrent_clients]]
        kind = "qbittorrent"
        url = "{}"
        "#,
        app_dir.display(),
        app_dir.display(),
        data_dir.display(),
        output_dir.display(),
        torznab_url,
        client_url,
    ))
    .and_then(|raw| RuntimeConfig::normalize(raw, app_dir))
    .map(|mut config| {
        config.delay = 0;
        config
    })
    .expect("config")
}

fn torrent_bytes(name: &str, length: u64) -> Vec<u8> {
    format!(
        "d4:infod6:lengthi{length}e4:name{}:{name}12:piece lengthi1e6:pieces20:aaaaaaaaaaaaaaaaaaaaee",
        name.len()
    )
    .into_bytes()
}
