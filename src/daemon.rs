//! Daemon runtime, HTTP serving, scheduler loop, and shutdown handling.

use std::{
    borrow::Cow,
    collections::BTreeMap,
    io::{BufRead, BufReader, Read, Write},
    net::{IpAddr, Ipv4Addr, Shutdown, SocketAddr, TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::{
    SporosError,
    api::{
        AnnounceRequest, ApiHandlers, ApiMethod, ApiOutcome, ApiRequest, JobRequest, JobResponse,
        WebhookRequest, handle_api_request,
    },
    clients::build_torrent_clients,
    config::RuntimeConfig,
    persistence::{DataRootRecord, Database},
    scheduler::{DaemonPlan, DaemonRun, JobName, Scheduler},
};

const JOB_LOOP_INTERVAL: Duration = Duration::from_secs(60);
const IDLE_SLEEP: Duration = Duration::from_millis(50);
const READ_TIMEOUT: Duration = Duration::from_secs(5);

/// Shared shutdown flag set by signal handlers or tests.
pub type ShutdownFlag = Arc<AtomicBool>;

/// Install process signal handling for daemon shutdown.
pub fn install_shutdown_handler() -> crate::Result<ShutdownFlag> {
    let shutdown = Arc::new(AtomicBool::new(false));
    let signal_shutdown = Arc::clone(&shutdown);
    ctrlc::set_handler(move || {
        signal_shutdown.store(true, Ordering::SeqCst);
    })
    .map_err(|error| daemon_error(format!("failed to install shutdown handler: {error}")))?;
    Ok(shutdown)
}

/// Run the daemon until the shutdown flag is set.
pub fn run_daemon(
    app_dir: &Path,
    config: &RuntimeConfig,
    database: &Database,
    shutdown: &ShutdownFlag,
) -> crate::Result<DaemonRun> {
    let mut plan = DaemonPlan::from_config(config);
    run_plan(app_dir, config, database, &mut plan, shutdown, None)
}

fn run_plan(
    app_dir: &Path,
    config: &RuntimeConfig,
    database: &Database,
    plan: &mut DaemonPlan,
    shutdown: &ShutdownFlag,
    max_iterations: Option<usize>,
) -> crate::Result<DaemonRun> {
    let mut run = plan.run_startup(database, now_millis(), || {
        index_torrents_and_data_dirs(config, database)
    })?;
    execute_ran_jobs(app_dir, config, database, &run.jobs)?;
    let listener = if let Some(address) = listen_address(config) {
        let listener = TcpListener::bind(address)
            .map_err(|error| daemon_error(format!("failed to bind {address}: {error}")))?;
        listener
            .set_nonblocking(true)
            .map_err(|error| daemon_error(format!("failed to configure listener: {error}")))?;
        let address = listener
            .local_addr()
            .map_err(|error| daemon_error(format!("failed to read listener address: {error}")))?;
        tracing::info!("daemon listening on {address}");
        run.listen_addr = Some(address);
        Some(listener)
    } else {
        tracing::info!("daemon HTTP serving disabled by --no-port or config");
        None
    };

    let mut next_job_check = now_millis().saturating_add(duration_millis(JOB_LOOP_INTERVAL));
    let mut iterations = 0usize;
    while !shutdown.load(Ordering::SeqCst) {
        if let Some(listener) = &listener {
            accept_ready(listener, app_dir, database, &mut plan.scheduler, config)?;
        }

        let now = now_millis();
        if now >= next_job_check {
            let results = plan.scheduler.check_jobs(database, now, false)?;
            execute_ran_jobs(app_dir, config, database, &results)?;
            next_job_check = now.saturating_add(duration_millis(JOB_LOOP_INTERVAL));
        }

        iterations = iterations.saturating_add(1);
        if max_iterations.is_some_and(|limit| iterations >= limit) {
            break;
        }
        thread::sleep(IDLE_SLEEP);
    }
    Ok(run)
}

fn execute_ran_jobs(
    app_dir: &Path,
    config: &RuntimeConfig,
    database: &Database,
    results: &[crate::scheduler::JobCheckResult],
) -> crate::Result<()> {
    for result in results {
        if !result.ran {
            continue;
        }
        match result.name {
            JobName::Rss => {
                let notifier = crate::notifications::NotificationSender::from_config(
                    config,
                    crate::startup::Redactor::from_config(config),
                )?;
                let rss =
                    crate::operations::run_rss_workflow(database, app_dir, config, &notifier)?;
                tracing::info!(
                    candidates = rss.candidates,
                    attempts = rss.attempts,
                    "rss job completed"
                );
            }
            JobName::Search => {
                let notifier = crate::notifications::NotificationSender::from_config(
                    config,
                    crate::startup::Redactor::from_config(config),
                )?;
                let search =
                    crate::operations::run_search_workflow(database, app_dir, config, &notifier)?;
                tracing::info!(
                    searchees = search.searchees,
                    indexers = search.indexers,
                    candidates = search.pipeline.candidates_assessed,
                    attempts = search.pipeline.attempts.len(),
                    "search job completed"
                );
            }
            JobName::UpdateIndexerCaps => {
                let caps = crate::operations::run_update_indexer_caps(database, config)?;
                tracing::info!(
                    indexers = caps.indexers,
                    updated = caps.updated,
                    "indexer caps job completed"
                );
            }
            JobName::Inject => {
                let inject = crate::operations::run_inject_workflow(database, app_dir, config)?;
                tracing::info!(
                    scanned = inject.scanned,
                    injected = inject.injected,
                    already_exists = inject.already_exists,
                    incomplete = inject.incomplete,
                    failed = inject.failed,
                    deleted = inject.deleted,
                    "inject job completed"
                );
            }
            JobName::Cleanup => {
                let client_timeout = config.search_timeout.map(Duration::from_millis);
                let client_adapters = if config.use_client_torrents {
                    build_torrent_clients(&config.torrent_clients, client_timeout)?
                } else {
                    Vec::new()
                };
                let client_refs = client_adapters
                    .iter()
                    .map(|client| client.as_ref())
                    .collect::<Vec<_>>();
                let cleanup = crate::operations::cleanup_db_with_clients(
                    database,
                    app_dir,
                    config,
                    now_millis(),
                    &client_refs,
                )?;
                tracing::info!(
                    client_searchees_refreshed = cleanup.client_searchees_refreshed,
                    client_searchees_pruned = cleanup.client_searchees_pruned,
                    client_ensemble_rows_rebuilt = cleanup.client_ensemble_rows_rebuilt,
                    data_rows_removed = cleanup.data_rows_removed,
                    ensemble_rows_removed = cleanup.ensemble_rows_removed,
                    torrent_cache_files_removed = cleanup.torrent_cache_files_removed,
                    null_decisions_removed = cleanup.null_decisions_removed,
                    missing_cache_decisions_removed = cleanup.missing_cache_decisions_removed,
                    catastrophic_decision_cleanup_skipped =
                        cleanup.catastrophic_decision_cleanup_skipped,
                    guid_info_hash_rows = cleanup.guid_info_hash_rows,
                    "cleanup job completed"
                );
            }
        }
    }
    Ok(())
}

fn accept_ready(
    listener: &TcpListener,
    app_dir: &Path,
    database: &Database,
    scheduler: &mut Scheduler,
    config: &RuntimeConfig,
) -> crate::Result<()> {
    loop {
        match listener.accept() {
            Ok((stream, remote_addr)) => {
                handle_stream(stream, remote_addr, app_dir, database, scheduler, config)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return Ok(()),
            Err(error) => return Err(daemon_error(format!("failed to accept request: {error}"))),
        }
    }
}

fn handle_stream(
    mut stream: TcpStream,
    remote_addr: SocketAddr,
    app_dir: &Path,
    database: &Database,
    scheduler: &mut Scheduler,
    config: &RuntimeConfig,
) -> crate::Result<()> {
    stream
        .set_read_timeout(Some(READ_TIMEOUT))
        .map_err(|error| daemon_error(format!("failed to set read timeout: {error}")))?;
    let mut request = read_request(&mut stream)?;
    request.remote_addr = Some(remote_addr.to_string());
    let api_key = crate::operations::api_key(database, config.api_key.as_deref())?;
    let mut handlers = RuntimeHandlers {
        app_dir,
        config,
        database,
        scheduler,
        now_millis: now_millis(),
    };
    let response = handle_api_request(request, &api_key, &mut handlers)?;
    write_response(&mut stream, response.status, &response.body)?;
    let _shutdown = stream.shutdown(Shutdown::Both);
    Ok(())
}

fn read_request(stream: &mut TcpStream) -> crate::Result<ApiRequest> {
    let mut reader = BufReader::new(stream);
    let mut request_line = String::new();
    let read = reader
        .read_line(&mut request_line)
        .map_err(|error| daemon_error(format!("failed to read request line: {error}")))?;
    if read == 0 {
        return Err(daemon_error("empty HTTP request"));
    }

    let mut parts = request_line.split_whitespace();
    let method = match parts.next() {
        Some("GET") => ApiMethod::Get,
        Some("POST") => ApiMethod::Post,
        Some(_) => ApiMethod::Other,
        None => return Err(daemon_error("missing HTTP method")),
    };
    let target = parts
        .next()
        .ok_or_else(|| daemon_error("missing HTTP request target"))?
        .to_owned();

    let mut headers = BTreeMap::new();
    let mut content_length = 0usize;
    loop {
        let mut line = String::new();
        let read = reader
            .read_line(&mut line)
            .map_err(|error| daemon_error(format!("failed to read HTTP header: {error}")))?;
        if read == 0 || line == "\r\n" || line == "\n" {
            break;
        }
        let Some((name, value)) = line.trim_end().split_once(':') else {
            continue;
        };
        let value = value.trim().to_owned();
        if name.eq_ignore_ascii_case("content-length") {
            content_length = value
                .parse()
                .map_err(|error| daemon_error(format!("invalid content-length: {error}")))?;
        }
        headers.insert(name.to_owned(), value);
    }

    let mut body = vec![0_u8; content_length];
    if content_length > 0 {
        reader
            .read_exact(&mut body)
            .map_err(|error| daemon_error(format!("failed to read request body: {error}")))?;
    }
    Ok(ApiRequest::new(
        method,
        &target,
        headers,
        String::from_utf8_lossy(&body),
    ))
}

fn write_response(stream: &mut TcpStream, status: u16, body: &str) -> crate::Result<()> {
    let reason = reason_phrase(status);
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream
        .write_all(response.as_bytes())
        .map_err(|error| daemon_error(format!("failed to write response: {error}")))
}

fn reason_phrase(status: u16) -> &'static str {
    match status {
        200 => "OK",
        202 => "Accepted",
        204 => "No Content",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        500 => "Internal Server Error",
        _ => "OK",
    }
}

fn listen_address(config: &RuntimeConfig) -> Option<SocketAddr> {
    config.port.map(|port| {
        let host = config.host.unwrap_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED));
        SocketAddr::new(host, port)
    })
}

fn index_torrents_and_data_dirs(config: &RuntimeConfig, database: &Database) -> crate::Result<()> {
    if let Some(torrent_dir) = &config.torrent_dir {
        let result = crate::search::index_torrent_dir(database, torrent_dir)?;
        tracing::info!(
            files_seen = result.files_seen,
            torrents_indexed = result.torrents_indexed,
            torrents_removed = result.torrents_removed,
            files_failed = result.files_failed,
            "indexed torrent_dir"
        );
    }

    if !config.data_dirs.is_empty() {
        database.begin_data_root_refresh()?;
        let roots_indexed = crate::search::for_each_data_dir_searchee(
            &config.data_dirs,
            config.max_data_depth,
            |searchee| {
                let Some(path) = searchee.path.as_deref() else {
                    return Ok(());
                };
                database.upsert_data_root(&DataRootRecord {
                    path,
                    title: searchee.title.as_ref(),
                })?;
                database.mark_refreshed_data_root(path)
            },
        )?;
        let removed = database.finish_data_root_refresh()?;
        tracing::info!(roots_indexed, roots_removed = removed, "indexed data_dirs");
    }
    Ok(())
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

fn duration_millis(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

fn daemon_error(message: impl Into<Cow<'static, str>>) -> SporosError {
    SporosError::Operation {
        message: message.into(),
    }
}

struct RuntimeHandlers<'a> {
    app_dir: &'a Path,
    config: &'a RuntimeConfig,
    database: &'a Database,
    scheduler: &'a mut Scheduler,
    now_millis: i64,
}

impl ApiHandlers for RuntimeHandlers<'_> {
    fn announce(&mut self, request: AnnounceRequest) -> crate::Result<Option<ApiOutcome>> {
        tracing::info!(
            tracker = request.tracker.as_str(),
            name = request.name.as_str(),
            "received announce request"
        );
        let notifier = crate::notifications::NotificationSender::from_config(
            self.config,
            crate::startup::Redactor::from_config(self.config),
        )?;
        crate::operations::run_announce_match(
            self.database,
            self.app_dir,
            self.config,
            request.into_candidate(),
            &notifier,
        )
    }

    fn webhook(&mut self, request: WebhookRequest) -> crate::Result<()> {
        tracing::info!(
            info_hash = request.info_hash.as_deref().unwrap_or_default(),
            path = request.path.as_deref().unwrap_or_default(),
            "received webhook request"
        );
        if self
            .scheduler
            .jobs()
            .iter()
            .any(|job| job.name == JobName::Inject && job.enabled)
        {
            let _response = self.scheduler.request_early_run(
                self.database,
                JobName::Inject,
                self.now_millis,
            )?;
            let _results = self
                .scheduler
                .check_jobs(self.database, self.now_millis, false)?;
        }
        let app_dir = PathBuf::from(self.app_dir);
        let config = self.config.clone();
        thread::spawn(move || {
            if let Err(error) = run_webhook_worker(&app_dir, &config, request) {
                tracing::error!("webhook targeted search failed: {error}");
            }
        });
        Ok(())
    }

    fn job(&mut self, request: JobRequest) -> crate::Result<JobResponse> {
        let Some(name) = JobName::parse(&request.name) else {
            return Ok(JobResponse::Disabled(format!(
                "{}: unable to run, disabled in config",
                request.name
            )));
        };
        let response = self
            .scheduler
            .request_early_run(self.database, name, self.now_millis)?;
        if matches!(response, JobResponse::Accepted(_)) {
            let _results = self
                .scheduler
                .check_jobs(self.database, self.now_millis, false)?;
        }
        Ok(response)
    }
}

fn run_webhook_worker(
    app_dir: &Path,
    config: &RuntimeConfig,
    request: WebhookRequest,
) -> crate::Result<()> {
    let database = Database::open_app_dir(app_dir)?;
    let notifier = crate::notifications::NotificationSender::from_config(
        config,
        crate::startup::Redactor::from_config(config),
    )?;
    let summary =
        crate::operations::run_webhook_search(&database, app_dir, config, request, &notifier)?;
    tracing::info!(
        searchees = summary.searchees_seen,
        indexer_searches = summary.indexer_searches,
        candidates = summary.candidates_assessed,
        attempts = summary.attempts.len(),
        "webhook targeted search completed"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{run_plan, write_response};
    use crate::{
        config::{RawConfig, RuntimeConfig},
        persistence::Database,
        scheduler::{DaemonPlan, JobName},
    };
    use std::{
        io::Read,
        net::{TcpListener, TcpStream},
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::{SystemTime, UNIX_EPOCH},
    };

    #[test]
    fn no_port_runs_startup_jobs_without_serving() {
        let root = temp_path("daemon-no-port");
        std::fs::create_dir_all(&root).expect("root");
        let database = Database::open_app_dir(&root).expect("database");
        let config = RuntimeConfig::normalize(
            RawConfig {
                port: Some(None),
                ..RawConfig::default()
            },
            &root,
        )
        .expect("config");
        let shutdown = Arc::new(AtomicBool::new(false));
        let mut plan = DaemonPlan::from_config(&config);

        let run =
            run_plan(&root, &config, &database, &mut plan, &shutdown, Some(1)).expect("run daemon");

        assert!(!run.serving);
        assert_eq!(run.listen_addr, None);
        assert!(
            run.jobs
                .iter()
                .any(|result| result.name == JobName::Cleanup && result.ran)
        );
        shutdown.store(true, Ordering::SeqCst);
        let _cleanup = std::fs::remove_dir_all(root);
    }

    #[test]
    fn http_response_includes_status_and_body_length() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let address = listener.local_addr().expect("address");
        let handle = std::thread::spawn(move || {
            let (mut stream, _remote) = listener.accept().expect("accept");
            write_response(&mut stream, 404, "Not Found").expect("response");
        });
        let mut client = TcpStream::connect(address).expect("connect");
        let mut response = String::new();
        client.read_to_string(&mut response).expect("read");
        handle.join().expect("join");

        assert!(response.starts_with("HTTP/1.1 404 Not Found"));
        assert!(response.contains("Content-Length: 9"));
        assert!(response.ends_with("Not Found"));
    }

    fn temp_path(name: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        std::env::temp_dir().join(format!("sporos-{name}-{}-{nanos}", std::process::id()))
    }
}
