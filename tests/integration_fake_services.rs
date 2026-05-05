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
    api::{
        AnnounceAccepted, AnnounceRequest, ApiHandlers, ApiMethod, ApiRequest, JobRequest,
        JobResponse, WebhookRequest, handle_api_request,
    },
    clients::{InjectionOptions, NewTorrent, TorrentClient, TransmissionClient, client_identities},
    config::{RawConfig, RuntimeConfig},
    domain::{Decision, File, MediaType, Searchee},
    integrations::{
        ArrKind, CategoryCaps, LimitCaps, RssPagerOptions, SearchIndexer, TorznabCaps,
        fetch_torznab_caps, lookup_arr_ids, rss_pager, validate_arr_instance, validate_arr_url,
        validate_torznab_url,
    },
    notifications::NotificationSender,
    persistence::{Database, SqlValue},
    scheduler::DaemonPlan,
    startup::Redactor,
    torrent::parse_metafile,
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
    let webhook = handle_api_request(
        ApiRequest::new(
            ApiMethod::Post,
            "/api/webhook?apikey=secret",
            BTreeMap::new(),
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
        ApiRequest::new(
            ApiMethod::Post,
            "/api/job?apikey=secret",
            BTreeMap::new(),
            r#"{"name":"rss"}"#,
        ),
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

    fn with_idle_timeout(responses: Vec<HttpResponse>, idle_timeout: Option<Duration>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake server");
        if idle_timeout.is_some() {
            listener
                .set_nonblocking(true)
                .expect("nonblocking listener");
        }
        let url = format!("http://{}", listener.local_addr().expect("local addr"));
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

fn torrent_bytes(name: &str, length: u64) -> Vec<u8> {
    format!(
        "d4:infod6:lengthi{length}e4:name{}:{name}12:piece lengthi1e6:pieces20:aaaaaaaaaaaaaaaaaaaaee",
        name.len()
    )
    .into_bytes()
}
