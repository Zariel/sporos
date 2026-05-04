//! Daemon runtime, Axum HTTP serving, scheduler loop, and shutdown handling.

use std::{
    borrow::Cow,
    collections::BTreeMap,
    future::Future,
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use axum::{
    Router,
    body::Bytes,
    extract::{ConnectInfo, DefaultBodyLimit, State},
    http::{HeaderMap, Method, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::any,
};
use tokio::{net::TcpListener, sync::Mutex, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::{
    SporosError,
    api::{
        AnnounceRequest, ApiHandlers, ApiMethod, ApiOutcome, ApiRequest, JobRequest, JobResponse,
        WebhookRequest, handle_api_request,
    },
    config::RuntimeConfig,
    persistence::{AsyncDatabase, Database},
    runtime::RuntimeServices,
    scheduler::{DaemonPlan, DaemonRun, JobConfigOverride, JobName, Scheduler},
};

const JOB_LOOP_INTERVAL: Duration = Duration::from_secs(60);
const MAX_REQUEST_BODY_BYTES: usize = 64 * 1024;

/// Install process signal handling for daemon shutdown.
pub fn install_shutdown_handler() -> CancellationToken {
    let shutdown = CancellationToken::new();
    spawn_shutdown_signal_listener("ctrl-c", shutdown.clone(), tokio::signal::ctrl_c());
    spawn_sigterm_listener(shutdown.clone());
    shutdown
}

fn spawn_shutdown_signal_listener<F>(name: &'static str, shutdown: CancellationToken, signal: F)
where
    F: Future<Output = io::Result<()>> + Send + 'static,
{
    tokio::spawn(async move {
        match signal.await {
            Ok(()) => {
                tracing::info!(signal = name, "shutdown signal received");
                shutdown.cancel();
            }
            Err(error) => tracing::error!(
                signal = name,
                "failed to listen for shutdown signal: {error}"
            ),
        }
    });
}

#[cfg(unix)]
fn spawn_sigterm_listener(shutdown: CancellationToken) {
    tokio::spawn(async move {
        let mut signal =
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(signal) => signal,
                Err(error) => {
                    tracing::error!(
                        signal = "sigterm",
                        "failed to listen for shutdown signal: {error}"
                    );
                    return;
                }
            };

        match signal.recv().await {
            Some(()) => {
                tracing::info!(signal = "sigterm", "shutdown signal received");
                shutdown.cancel();
            }
            None => tracing::error!(
                signal = "sigterm",
                "failed to listen for shutdown signal: stream closed"
            ),
        }
    });
}

#[cfg(not(unix))]
fn spawn_sigterm_listener(_shutdown: CancellationToken) {
    tracing::debug!("SIGTERM shutdown listener is unavailable on this platform");
}

/// Run the daemon until cancellation is requested.
pub async fn run_daemon(
    app_dir: &Path,
    config: &RuntimeConfig,
    database: &Database,
    shutdown: CancellationToken,
) -> crate::Result<DaemonRun> {
    let mut plan = DaemonPlan::from_config(config);
    run_plan(app_dir, config, database, &mut plan, shutdown, None).await
}

async fn run_plan(
    app_dir: &Path,
    config: &RuntimeConfig,
    database: &Database,
    plan: &mut DaemonPlan,
    shutdown: CancellationToken,
    max_iterations: Option<usize>,
) -> crate::Result<DaemonRun> {
    let async_database = AsyncDatabase::open_app_dir(app_dir).await?;
    let runtime_services = RuntimeServices::start(shutdown.child_token());
    let mut run = plan
        .run_startup_async(&async_database, now_millis(), || {
            crate::operations::refresh_torrent_and_data_indexes(database, config).map(|_| ())
        })
        .await?;
    let startup_jobs = execute_ran_jobs(
        Arc::clone(&runtime_services),
        app_dir,
        config,
        &run.jobs,
        shutdown.child_token(),
    )
    .await;
    finish_executed_jobs(&mut plan.scheduler, &async_database, startup_jobs).await?;

    let mut server_state = None;
    let server = if let Some(address) = listen_address(config) {
        let listener = TcpListener::bind(address)
            .await
            .map_err(|error| daemon_error(format!("failed to bind {address}: {error}")))?;
        let address = listener
            .local_addr()
            .map_err(|error| daemon_error(format!("failed to read listener address: {error}")))?;
        tracing::info!("daemon listening on {address}");
        run.listen_addr = Some(address);
        let state = Arc::new(DaemonState {
            app_dir: app_dir.to_owned(),
            config: config.clone(),
            services: Arc::clone(&runtime_services),
            scheduler: Mutex::new(std::mem::replace(
                &mut plan.scheduler,
                Scheduler::new(Vec::new()),
            )),
        });
        server_state = Some(Arc::clone(&state));
        Some(serve_http(listener, state, shutdown.clone()))
    } else {
        tracing::info!("daemon HTTP serving disabled by --no-port or config");
        None
    };

    let mut interval = tokio::time::interval(JOB_LOOP_INTERVAL);
    let mut iterations = 0usize;
    loop {
        if max_iterations.is_some_and(|limit| iterations >= limit) {
            break;
        }
        tokio::select! {
            () = shutdown.cancelled() => break,
            _tick = interval.tick() => {
                let now = now_millis();
                let results = if let Some(state) = &server_state {
                    let mut scheduler = state.scheduler.lock().await;
                    scheduler.check_jobs_async(&async_database, now, false).await?
                } else {
                    plan.scheduler.check_jobs_async(&async_database, now, false).await?
                };
                let executed_jobs = execute_ran_jobs(
                    Arc::clone(&runtime_services),
                    app_dir,
                    config,
                    &results,
                    shutdown.child_token(),
                )
                .await;
                if let Some(state) = &server_state {
                    let mut scheduler = state.scheduler.lock().await;
                    finish_executed_jobs(&mut scheduler, &async_database, executed_jobs).await?;
                } else {
                    finish_executed_jobs(&mut plan.scheduler, &async_database, executed_jobs).await?;
                }
                iterations = iterations.saturating_add(1);
            }
        }
    }

    shutdown.cancel();
    if let Some(server) = server {
        server
            .await
            .map_err(|error| daemon_error(format!("HTTP server task failed: {error}")))??;
    }
    runtime_services.shutdown().await;
    async_database.close().await;
    Ok(run)
}

fn serve_http(
    listener: TcpListener,
    state: Arc<DaemonState>,
    shutdown: CancellationToken,
) -> JoinHandle<crate::Result<()>> {
    tokio::spawn(async move {
        let router = Router::new()
            .fallback(any(handle_axum_request))
            .layer(DefaultBodyLimit::max(MAX_REQUEST_BODY_BYTES))
            .with_state(state);
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(shutdown.cancelled_owned())
        .await
        .map_err(|error| daemon_error(format!("HTTP server failed: {error}")))
    })
}

async fn handle_axum_request(
    State(state): State<Arc<DaemonState>>,
    ConnectInfo(remote_addr): ConnectInfo<SocketAddr>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let request = ApiRequest {
        method: api_method(&method),
        path: uri.path().to_owned(),
        query: uri.query().map(parse_query).unwrap_or_default(),
        headers: headers
            .iter()
            .filter_map(|(key, value)| {
                value
                    .to_str()
                    .ok()
                    .map(|value| (key.as_str().to_ascii_lowercase(), value.to_owned()))
            })
            .collect(),
        body: String::from_utf8_lossy(&body).into_owned(),
        remote_addr: Some(remote_addr.to_string()),
    };

    let response = match handle_runtime_request(state, request).await {
        Ok(response) => response,
        Err(error) => {
            tracing::error!("API request failed: {error}");
            crate::api::ApiResponse {
                status: 500,
                body: error.to_string(),
            }
        }
    };
    let status = StatusCode::from_u16(response.status).unwrap_or(StatusCode::OK);
    (status, response.body).into_response()
}

async fn handle_runtime_request(
    state: Arc<DaemonState>,
    request: ApiRequest,
) -> crate::Result<crate::api::ApiResponse> {
    if let Some(response) = handle_health_request(Arc::clone(&state), &request).await {
        return Ok(response);
    }

    let async_database = AsyncDatabase::open_app_dir(&state.app_dir).await?;
    let api_key =
        crate::operations::api_key_async(&async_database, state.config.api_key.as_deref()).await?;
    let needs_scheduler = request.path == "/api/job";
    let mut scheduler = if needs_scheduler {
        Some(state.scheduler.lock().await)
    } else {
        None
    };
    let mut handlers = RuntimeHandlers {
        app_dir: &state.app_dir,
        config: &state.config,
        services: Arc::clone(&state.services),
        async_database: &async_database,
        scheduler: scheduler.as_deref_mut(),
        now_millis: now_millis(),
        webhook_requests: Vec::new(),
        job_dispatches: Vec::new(),
    };
    let response = handle_api_request(request, &api_key, &mut handlers).await?;
    let webhook_requests = std::mem::take(&mut handlers.webhook_requests);
    let job_dispatches = std::mem::take(&mut handlers.job_dispatches);
    drop(handlers);
    drop(scheduler);
    submit_webhook_workers(
        Arc::clone(&state.services),
        &state.app_dir,
        &state.config,
        webhook_requests,
    );
    let executed_jobs = execute_ran_jobs(
        Arc::clone(&state.services),
        &state.app_dir,
        &state.config,
        &job_dispatches,
        state.services.cancellation_token(),
    )
    .await;
    if !executed_jobs.is_empty() {
        let mut scheduler = state.scheduler.lock().await;
        finish_executed_jobs(&mut scheduler, &async_database, executed_jobs).await?;
    }
    async_database.close().await;
    Ok(response)
}

async fn handle_health_request(
    state: Arc<DaemonState>,
    request: &ApiRequest,
) -> Option<crate::api::ApiResponse> {
    match request.path.as_str() {
        "/_health/livez" => Some(health_livez_response(request.method)),
        "/_health/readyz" => Some(health_readyz_response(state, request.method).await),
        _ => None,
    }
}

fn health_livez_response(method: ApiMethod) -> crate::api::ApiResponse {
    if method != ApiMethod::Get {
        return crate::api::ApiResponse {
            status: 405,
            body: "Method Not Allowed".to_owned(),
        };
    }
    crate::api::ApiResponse {
        status: 200,
        body: r#"{"status":"live"}"#.to_owned(),
    }
}

async fn health_readyz_response(
    state: Arc<DaemonState>,
    method: ApiMethod,
) -> crate::api::ApiResponse {
    if method != ApiMethod::Get {
        return crate::api::ApiResponse {
            status: 405,
            body: "Method Not Allowed".to_owned(),
        };
    }

    let app_dir_ready = tokio::fs::metadata(&state.app_dir)
        .await
        .is_ok_and(|metadata| metadata.is_dir());
    let database_ready = match AsyncDatabase::open_app_dir(&state.app_dir).await {
        Ok(database) => {
            database.close().await;
            true
        }
        Err(error) => {
            tracing::warn!(error = %error, "readiness database check failed");
            false
        }
    };
    let runtime_ready = !state.services.cancellation_token().is_cancelled();
    let ready = app_dir_ready && database_ready && runtime_ready;
    let body = serde_json::json!({
        "status": if ready { "ready" } else { "not_ready" },
        "checks": {
            "appDir": app_dir_ready,
            "database": database_ready,
            "runtime": runtime_ready,
        },
    })
    .to_string();

    crate::api::ApiResponse {
        status: if ready { 200 } else { 503 },
        body,
    }
}

fn api_method(method: &Method) -> ApiMethod {
    match *method {
        Method::GET => ApiMethod::Get,
        Method::POST => ApiMethod::Post,
        _ => ApiMethod::Other,
    }
}

fn parse_query(query: &str) -> BTreeMap<String, String> {
    url::form_urlencoded::parse(query.as_bytes())
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect()
}

struct ExecutedJob {
    name: JobName,
    completion_last_run: Option<i64>,
    result: Option<crate::Result<()>>,
}

async fn execute_ran_jobs(
    services: Arc<RuntimeServices>,
    app_dir: &Path,
    config: &RuntimeConfig,
    results: &[crate::scheduler::JobCheckResult],
    shutdown: CancellationToken,
) -> Vec<ExecutedJob> {
    let mut executed = Vec::new();
    for result in results {
        if !result.ran {
            continue;
        }
        if shutdown.is_cancelled() {
            tracing::info!(job = result.name.as_str(), "job cancelled during shutdown");
            executed.push(ExecutedJob {
                name: result.name,
                completion_last_run: result.completion_last_run,
                result: None,
            });
            continue;
        }
        let job_result = tokio::select! {
            () = shutdown.cancelled() => {
                tracing::info!(job = result.name.as_str(), "job cancelled during shutdown");
                None
            }
            result = execute_ran_job(Arc::clone(&services), app_dir, config, result) => {
                Some(result)
            }
        };
        executed.push(ExecutedJob {
            name: result.name,
            completion_last_run: result.completion_last_run,
            result: job_result,
        });
    }
    executed
}

async fn finish_executed_jobs(
    scheduler: &mut Scheduler,
    database: &AsyncDatabase,
    executed_jobs: Vec<ExecutedJob>,
) -> crate::Result<()> {
    let mut first_error = None;
    for executed in executed_jobs {
        scheduler.finish_job(executed.name);
        if let Some(result) = executed.result {
            match result {
                Ok(()) => {
                    if let Some(last_run) = executed.completion_last_run
                        && let Err(error) = database
                            .write_last_run(executed.name.as_str(), last_run)
                            .await
                        && first_error.is_none()
                    {
                        first_error = Some(error);
                    }
                }
                Err(error) => {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }
    }
    if let Some(error) = first_error {
        return Err(error);
    }
    Ok(())
}

async fn execute_ran_job(
    services: Arc<RuntimeServices>,
    app_dir: &Path,
    config: &RuntimeConfig,
    job: &crate::scheduler::JobCheckResult,
) -> crate::Result<()> {
    match job.name {
        JobName::Rss => {
            let config = config_with_job_override(config, job.config_override);
            let notifier = crate::notifications::NotificationSender::from_config(
                &config,
                crate::startup::Redactor::from_config(&config),
            )?;
            let rss = crate::operations::run_rss_workflow_async(
                services.blocking().matching.clone(),
                app_dir.to_path_buf(),
                config,
                notifier,
            )
            .await?;
            tracing::info!(
                candidates = rss.candidates,
                attempts = rss.attempts,
                "rss job completed"
            );
        }
        JobName::Search => {
            let config = config_with_job_override(config, job.config_override);
            let notifier = crate::notifications::NotificationSender::from_config(
                &config,
                crate::startup::Redactor::from_config(&config),
            )?;
            let search = crate::operations::run_search_workflow_async(
                services.blocking().matching.clone(),
                app_dir.to_path_buf(),
                config,
                notifier,
            )
            .await?;
            tracing::info!(
                searchees = search.searchees,
                indexers = search.indexers,
                candidates = search.pipeline.candidates_assessed,
                attempts = search.pipeline.attempts_total,
                "search job completed"
            );
        }
        JobName::UpdateIndexerCaps => {
            let caps = crate::operations::run_update_indexer_caps_async(
                services.blocking().torrent_io.clone(),
                app_dir.to_path_buf(),
                config.clone(),
            )
            .await?;
            tracing::info!(
                indexers = caps.indexers,
                updated = caps.updated,
                "indexer caps job completed"
            );
        }
        JobName::Inject => {
            let inject = crate::operations::run_inject_workflow_async(
                services.blocking().linking.clone(),
                app_dir.to_path_buf(),
                config.clone(),
            )
            .await?;
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
            let cleanup = crate::operations::cleanup_db_async(
                services.blocking().filesystem.clone(),
                app_dir.to_path_buf(),
                config.clone(),
                now_millis(),
            )
            .await?;
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
    Ok(())
}

fn config_with_job_override(
    config: &RuntimeConfig,
    config_override: JobConfigOverride,
) -> RuntimeConfig {
    let mut config = config.clone();
    if config_override.ignore_exclude_recent_search {
        config.exclude_recent_search = Some(1);
    }
    if config_override.ignore_exclude_older {
        config.exclude_older = Some(u64::MAX);
    }
    config
}

fn listen_address(config: &RuntimeConfig) -> Option<SocketAddr> {
    config.port.map(|port| {
        let host = config.host.unwrap_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED));
        SocketAddr::new(host, port)
    })
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

fn daemon_error(message: impl Into<Cow<'static, str>>) -> SporosError {
    SporosError::Operation {
        message: message.into(),
    }
}

struct DaemonState {
    app_dir: PathBuf,
    config: RuntimeConfig,
    services: Arc<RuntimeServices>,
    scheduler: Mutex<Scheduler>,
}

struct RuntimeHandlers<'a> {
    app_dir: &'a Path,
    config: &'a RuntimeConfig,
    services: Arc<RuntimeServices>,
    async_database: &'a AsyncDatabase,
    scheduler: Option<&'a mut Scheduler>,
    now_millis: i64,
    webhook_requests: Vec<WebhookRequest>,
    job_dispatches: Vec<crate::scheduler::JobCheckResult>,
}

fn submit_webhook_workers(
    services: Arc<RuntimeServices>,
    app_dir: &Path,
    config: &RuntimeConfig,
    webhook_requests: Vec<WebhookRequest>,
) {
    for request in webhook_requests {
        let app_dir = PathBuf::from(app_dir);
        let config = config.clone();
        let worker_services = Arc::clone(&services);
        let result = services
            .queues()
            .webhooks
            .try_submit("webhook", move |shutdown| async move {
                if let Err(error) =
                    run_webhook_worker(worker_services, app_dir, config, request, shutdown).await
                {
                    tracing::error!("webhook background work failed: {error}");
                }
            });
        if let Err(error) = result {
            tracing::warn!("webhook background work was not queued: {error}");
        }
    }
}

#[async_trait]
impl ApiHandlers for RuntimeHandlers<'_> {
    async fn announce(&mut self, request: AnnounceRequest) -> crate::Result<Option<ApiOutcome>> {
        tracing::info!(
            tracker = request.tracker.as_str(),
            name = request.name.as_str(),
            "received announce request"
        );
        let notifier = crate::notifications::NotificationSender::from_config(
            self.config,
            crate::startup::Redactor::from_config(self.config),
        )?;
        crate::operations::run_announce_match_async(
            self.services.blocking().matching.clone(),
            self.app_dir.to_path_buf(),
            self.config.clone(),
            request.into_candidate(),
            notifier,
        )
        .await
    }

    async fn webhook(&mut self, request: WebhookRequest) -> crate::Result<()> {
        tracing::info!(
            info_hash = request.info_hash.as_deref().unwrap_or_default(),
            path = request.path.as_deref().unwrap_or_default(),
            "received webhook request"
        );
        self.webhook_requests.push(request);
        Ok(())
    }

    async fn job(&mut self, request: JobRequest) -> crate::Result<JobResponse> {
        let Some(scheduler) = self.scheduler.as_deref_mut() else {
            return Err(daemon_error("scheduler is unavailable for job request"));
        };
        let Some(name) = JobName::parse(&request.name) else {
            return Ok(JobResponse::Disabled(format!(
                "{}: unable to run, disabled in config",
                request.name
            )));
        };
        let config_override = JobConfigOverride {
            ignore_exclude_recent_search: request.ignore_exclude_recent_search,
            ignore_exclude_older: request.ignore_exclude_older,
        };
        let response = scheduler
            .request_early_run_async(self.async_database, name, self.now_millis, config_override)
            .await?;
        if matches!(response, JobResponse::Accepted(_)) {
            let results = scheduler
                .check_jobs_async(self.async_database, self.now_millis, false)
                .await?;
            self.job_dispatches.extend(results);
        }
        Ok(response)
    }
}

async fn run_webhook_worker(
    services: Arc<RuntimeServices>,
    app_dir: PathBuf,
    config: RuntimeConfig,
    request: WebhookRequest,
    shutdown: CancellationToken,
) -> crate::Result<()> {
    let async_database = AsyncDatabase::open_app_dir(&app_dir).await?;
    let mut plan = DaemonPlan::from_config(&config);
    let now = now_millis();
    if plan
        .scheduler
        .jobs()
        .iter()
        .any(|job| job.name == JobName::Inject && job.enabled)
    {
        let _response = plan
            .scheduler
            .request_early_run_async(
                &async_database,
                JobName::Inject,
                now,
                JobConfigOverride::default(),
            )
            .await?;
        let results = plan
            .scheduler
            .check_jobs_async(&async_database, now, false)
            .await?;
        let executed_jobs = execute_ran_jobs(
            Arc::clone(&services),
            &app_dir,
            &config,
            &results,
            shutdown.child_token(),
        )
        .await;
        finish_executed_jobs(&mut plan.scheduler, &async_database, executed_jobs).await?;
    }
    let notifier = crate::notifications::NotificationSender::from_config(
        &config,
        crate::startup::Redactor::from_config(&config),
    )?;
    let summary = tokio::select! {
        () = shutdown.cancelled() => {
            tracing::info!("webhook targeted search cancelled during shutdown");
            async_database.close().await;
            return Ok(());
        }
        summary = crate::operations::run_webhook_search_async(
            services.blocking().matching.clone(),
            app_dir.clone(),
            config,
            request,
            notifier,
        ) => summary?,
    };
    tracing::info!(
        searchees = summary.searchees_seen,
        indexer_searches = summary.indexer_searches,
        candidates = summary.candidates_assessed,
        attempts = summary.attempts_total,
        "webhook targeted search completed"
    );
    async_database.close().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{MAX_REQUEST_BODY_BYTES, handle_runtime_request, run_plan};
    use crate::{
        SporosError,
        api::{ApiMethod, ApiRequest, handle_api_request},
        config::{RawConfig, RuntimeConfig, TorrentClientConfig},
        persistence::{AsyncDatabase, Database},
        runtime::{RuntimeServices, RuntimeTaskQueue},
        scheduler::{
            DaemonPlan, JobCheckResult, JobConfigOverride, JobName, ScheduledJob, Scheduler,
        },
    };
    use std::{
        collections::BTreeMap,
        sync::Arc,
        time::{SystemTime, UNIX_EPOCH},
    };
    use tokio::sync::Mutex;
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn no_port_runs_startup_jobs_without_serving() {
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
        let shutdown = CancellationToken::new();
        let mut plan = DaemonPlan::from_config(&config);

        let run = run_plan(&root, &config, &database, &mut plan, shutdown, Some(1))
            .await
            .expect("run daemon");

        assert!(!run.serving);
        assert_eq!(run.listen_addr, None);
        assert!(
            run.jobs
                .iter()
                .any(|result| result.name == JobName::Cleanup && result.ran)
        );
        let _cleanup = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn runtime_webhook_queues_background_work_without_running_jobs_inline() {
        let root = temp_path("daemon-webhook-inline");
        std::fs::create_dir_all(&root).expect("root");
        let webhook_path = root.join("source.mkv");
        std::fs::write(&webhook_path, b"data").expect("webhook source");
        let database = Database::open_app_dir(&root).expect("database");
        let config = RuntimeConfig::normalize(
            RawConfig {
                action: Some("inject".to_owned()),
                torrent_clients: vec![
                    TorrentClientConfig::parse("qbittorrent:http://localhost:8080")
                        .expect("client"),
                ],
                port: Some(None),
                ..RawConfig::default()
            },
            &root,
        )
        .expect("config");
        let mut plan = DaemonPlan::from_config(&config);
        let async_database = AsyncDatabase::open_app_dir(&root).await.expect("database");
        let mut handlers = super::RuntimeHandlers {
            app_dir: &root,
            config: &config,
            services: RuntimeServices::start(CancellationToken::new()),
            async_database: &async_database,
            scheduler: Some(&mut plan.scheduler),
            now_millis: 1_000,
            webhook_requests: Vec::new(),
            job_dispatches: Vec::new(),
        };

        let response = handle_api_request(
            ApiRequest::new(
                ApiMethod::Post,
                "/api/webhook?apikey=secret",
                BTreeMap::new(),
                format!("path={}", webhook_path.display()),
            ),
            "secret",
            &mut handlers,
        )
        .await
        .expect("webhook");

        assert_eq!(response.status, 204);
        assert_eq!(handlers.webhook_requests.len(), 1);
        assert_eq!(
            handlers
                .scheduler
                .as_deref()
                .expect("scheduler")
                .jobs()
                .iter()
                .find(|job| job.name == JobName::Inject)
                .map(|job| job.runs),
            Some(0)
        );
        let inject_last_run = database
            .read_last_run(JobName::Inject.as_str())
            .expect("last run");
        assert_eq!(inject_last_run, None);
        async_database.close().await;
        let _cleanup = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn runtime_job_preserves_timestamp_overrides_for_dispatch() {
        let root = temp_path("daemon-job-override");
        let data_dir = temp_path("daemon-job-override-data");
        std::fs::create_dir_all(&root).expect("root");
        std::fs::create_dir_all(&data_dir).expect("data dir");
        let config = RuntimeConfig::normalize(
            RawConfig {
                port: Some(None),
                search_cadence: Some(86_400_000),
                exclude_recent_search: Some(259_200_000),
                exclude_older: Some(518_400_000),
                data_dirs: vec![data_dir.clone()],
                ..RawConfig::default()
            },
            &root,
        )
        .expect("config");
        let async_database = AsyncDatabase::open_app_dir(&root).await.expect("database");
        let mut plan = DaemonPlan::from_config(&config);
        let services = RuntimeServices::start(CancellationToken::new());
        let mut handlers = super::RuntimeHandlers {
            app_dir: &root,
            config: &config,
            services,
            async_database: &async_database,
            scheduler: Some(&mut plan.scheduler),
            now_millis: 1_000,
            webhook_requests: Vec::new(),
            job_dispatches: Vec::new(),
        };

        let response = handle_api_request(
            ApiRequest::new(
                ApiMethod::Post,
                "/api/job?apikey=secret",
                BTreeMap::new(),
                r#"{"name":"search","ignoreExcludeRecentSearch":true,"ignoreExcludeOlder":true}"#,
            ),
            "secret",
            &mut handlers,
        )
        .await
        .expect("job");

        assert_eq!(response.status, 200);
        let search = handlers
            .job_dispatches
            .iter()
            .find(|result| result.name == JobName::Search)
            .expect("search dispatch");
        assert!(search.ran);
        assert_eq!(
            search.config_override,
            JobConfigOverride {
                ignore_exclude_recent_search: true,
                ignore_exclude_older: true,
            }
        );
        handlers.services.shutdown().await;
        async_database.close().await;
        let _cleanup = std::fs::remove_dir_all(root);
        let _cleanup = std::fs::remove_dir_all(data_dir);
    }

    #[tokio::test]
    async fn axum_request_state_routes_ping_and_auth() {
        let root = temp_path("daemon-axum-state");
        std::fs::create_dir_all(&root).expect("root");
        let database = Database::open_app_dir(&root).expect("database");
        database.set_api_key("secret").expect("api key");
        let config = RuntimeConfig::normalize(RawConfig::default(), &root).expect("config");
        let scheduler = DaemonPlan::from_config(&config).scheduler;
        let state = Arc::new(super::DaemonState {
            app_dir: root.clone(),
            config,
            services: RuntimeServices::start(CancellationToken::new()),
            scheduler: Mutex::new(scheduler),
        });

        let ping = handle_runtime_request(
            Arc::clone(&state),
            ApiRequest::new(ApiMethod::Get, "/api/ping", BTreeMap::new(), ""),
        )
        .await
        .expect("ping");
        let unauthorized = handle_runtime_request(
            Arc::clone(&state),
            ApiRequest::new(ApiMethod::Get, "/api/status", BTreeMap::new(), ""),
        )
        .await
        .expect("status");

        assert_eq!(ping.status, 200);
        assert_eq!(unauthorized.status, 401);
        state.services.shutdown().await;
        let _cleanup = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn health_probes_skip_auth_and_report_readiness() {
        let root = temp_path("daemon-health-probes");
        std::fs::create_dir_all(&root).expect("root");
        let database = Database::open_app_dir(&root).expect("database");
        database.set_api_key("secret").expect("api key");
        let config = RuntimeConfig::normalize(RawConfig::default(), &root).expect("config");
        let state = Arc::new(super::DaemonState {
            app_dir: root.clone(),
            config,
            services: RuntimeServices::start(CancellationToken::new()),
            scheduler: Mutex::new(Scheduler::new(Vec::new())),
        });

        let livez = handle_runtime_request(
            Arc::clone(&state),
            ApiRequest::new(ApiMethod::Get, "/_health/livez", BTreeMap::new(), ""),
        )
        .await
        .expect("livez");
        let readyz = handle_runtime_request(
            Arc::clone(&state),
            ApiRequest::new(ApiMethod::Get, "/_health/readyz", BTreeMap::new(), ""),
        )
        .await
        .expect("readyz");
        let status = handle_runtime_request(
            Arc::clone(&state),
            ApiRequest::new(ApiMethod::Get, "/api/status", BTreeMap::new(), ""),
        )
        .await
        .expect("status");

        assert_eq!(livez.status, 200);
        assert_eq!(livez.body, r#"{"status":"live"}"#);
        assert_eq!(readyz.status, 200);
        assert!(readyz.body.contains(r#""status":"ready""#));
        assert_eq!(status.status, 401);
        state.services.shutdown().await;
        let _cleanup = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn status_request_does_not_wait_for_scheduler_lock() {
        let root = temp_path("daemon-status-no-scheduler-lock");
        std::fs::create_dir_all(&root).expect("root");
        let database = Database::open_app_dir(&root).expect("database");
        database.set_api_key("secret").expect("api key");
        let config = RuntimeConfig::normalize(RawConfig::default(), &root).expect("config");
        let scheduler = DaemonPlan::from_config(&config).scheduler;
        let state = Arc::new(super::DaemonState {
            app_dir: root.clone(),
            config,
            services: RuntimeServices::start(CancellationToken::new()),
            scheduler: Mutex::new(scheduler),
        });
        let guard = state.scheduler.lock().await;

        let response = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            handle_runtime_request(
                Arc::clone(&state),
                ApiRequest::new(
                    ApiMethod::Get,
                    "/api/status?apikey=secret",
                    BTreeMap::new(),
                    "",
                ),
            ),
        )
        .await
        .expect("status should not wait for scheduler")
        .expect("status");

        assert_eq!(response.status, 200);
        drop(guard);
        state.services.shutdown().await;
        let _cleanup = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn health_requests_ignore_running_and_saturated_work() {
        let root = temp_path("daemon-health-under-work");
        std::fs::create_dir_all(&root).expect("root");
        let database = Database::open_app_dir(&root).expect("database");
        database.set_api_key("secret").expect("api key");
        let config = RuntimeConfig::normalize(RawConfig::default(), &root).expect("config");
        let scheduler = DaemonPlan::from_config(&config).scheduler;
        let services = RuntimeServices::start(CancellationToken::new());
        submit_held(&services.queues().jobs, "search");
        submit_held(&services.queues().jobs, "rss");
        submit_held(&services.queues().jobs, "cleanup");
        saturate_queue(&services.queues().jobs, "job-overflow");
        saturate_queue(&services.queues().webhooks, "webhook");
        saturate_queue(&services.queues().injection, "injection");
        saturate_queue(&services.queues().blocking_local, "blocking-local");
        wait_for_started(&services.queues().jobs).await;
        wait_for_started(&services.queues().webhooks).await;
        wait_for_started(&services.queues().injection).await;
        wait_for_started(&services.queues().blocking_local).await;
        let state = Arc::new(super::DaemonState {
            app_dir: root.clone(),
            config,
            services,
            scheduler: Mutex::new(scheduler),
        });

        let ping = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            handle_runtime_request(
                Arc::clone(&state),
                ApiRequest::new(ApiMethod::Get, "/api/ping", BTreeMap::new(), ""),
            ),
        )
        .await
        .expect("ping should not wait for work queues")
        .expect("ping");
        let status = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            handle_runtime_request(
                Arc::clone(&state),
                ApiRequest::new(
                    ApiMethod::Get,
                    "/api/status?apikey=secret",
                    BTreeMap::new(),
                    "",
                ),
            ),
        )
        .await
        .expect("status should not wait for work queues")
        .expect("status");
        let readyz = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            handle_runtime_request(
                Arc::clone(&state),
                ApiRequest::new(ApiMethod::Get, "/_health/readyz", BTreeMap::new(), ""),
            ),
        )
        .await
        .expect("readyz should not wait for work queues")
        .expect("readyz");

        assert_eq!(ping.status, 200);
        assert_eq!(status.status, 200);
        assert_eq!(readyz.status, 200);
        state.services.shutdown().await;
        let _cleanup = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn job_execution_stops_waiting_when_shutdown_is_cancelled() {
        let root = temp_path("daemon-job-shutdown");
        let data_dir = temp_path("daemon-job-shutdown-data");
        std::fs::create_dir_all(&root).expect("root");
        std::fs::create_dir_all(&data_dir).expect("data dir");
        let config = RuntimeConfig::normalize(
            RawConfig {
                data_dirs: vec![data_dir.clone()],
                ..RawConfig::default()
            },
            &root,
        )
        .expect("config");
        let services = RuntimeServices::start(CancellationToken::new());
        let (release_sender, release_receiver) = std::sync::mpsc::channel();
        let held_matching = tokio::spawn({
            let matching = services.blocking().matching.clone();
            async move {
                matching
                    .submit("held matching", move || {
                        let _result = release_receiver.recv();
                    })
                    .await
            }
        });
        for _attempt in 0..20 {
            if services.blocking().matching.stats().started > 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(services.blocking().matching.stats().started, 1);
        let shutdown = CancellationToken::new();
        let cancel_shutdown = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            cancel_shutdown.cancel();
        });
        let results = vec![JobCheckResult {
            name: JobName::Search,
            config_override: JobConfigOverride::default(),
            completion_last_run: Some(1_000),
            ran: true,
            skipped: None,
        }];

        let executed = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            super::execute_ran_jobs(
                Arc::clone(&services),
                &root,
                &config,
                &results,
                shutdown.child_token(),
            ),
        )
        .await
        .expect("shutdown should interrupt queued job work");

        assert_eq!(executed.len(), 1);
        assert!(executed[0].result.is_none());
        services.shutdown().await;
        let _release = release_sender.send(());
        let _held = held_matching.await.expect("held matching joins");
        let _cleanup = std::fs::remove_dir_all(root);
        let _cleanup = std::fs::remove_dir_all(data_dir);
    }

    #[tokio::test]
    async fn finish_jobs_persists_last_run_only_after_success() {
        let root = temp_path("daemon-finish-last-run");
        std::fs::create_dir_all(&root).expect("root");
        let database = AsyncDatabase::open_app_dir(&root).await.expect("database");
        let mut search = ScheduledJob::new(JobName::Search, 60_000, true);
        search.is_active = true;
        let mut rss = ScheduledJob::new(JobName::Rss, 60_000, true);
        rss.is_active = true;
        let mut scheduler = Scheduler::new(vec![search, rss]);

        let result = super::finish_executed_jobs(
            &mut scheduler,
            &database,
            vec![
                super::ExecutedJob {
                    name: JobName::Search,
                    completion_last_run: Some(1_000),
                    result: Some(Ok(())),
                },
                super::ExecutedJob {
                    name: JobName::Rss,
                    completion_last_run: Some(2_000),
                    result: Some(Err(SporosError::configuration("rss failed"))),
                },
            ],
        )
        .await;

        assert!(result.is_err());
        assert_eq!(
            database
                .read_last_run(JobName::Search.as_str())
                .await
                .expect("search last run"),
            Some(1_000)
        );
        assert_eq!(
            database
                .read_last_run(JobName::Rss.as_str())
                .await
                .expect("rss last run"),
            None
        );
        assert!(!scheduler.jobs()[0].is_active);
        assert!(!scheduler.jobs()[1].is_active);
        database.close().await;
        let _cleanup = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn max_request_body_limit_matches_compatibility_limit() {
        assert_eq!(MAX_REQUEST_BODY_BYTES, 64 * 1024);
    }

    #[test]
    fn job_config_override_maps_timestamp_ignores() {
        let root = temp_path("daemon-job-config-override");
        let config = RuntimeConfig::normalize(
            RawConfig {
                exclude_recent_search: Some(180_000),
                exclude_older: Some(360_000),
                ..RawConfig::default()
            },
            &root,
        )
        .expect("config");

        let overridden = super::config_with_job_override(
            &config,
            JobConfigOverride {
                ignore_exclude_recent_search: true,
                ignore_exclude_older: true,
            },
        );

        assert_eq!(overridden.exclude_recent_search, Some(1));
        assert_eq!(overridden.exclude_older, Some(u64::MAX));
    }

    fn temp_path(name: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        std::env::temp_dir().join(format!("sporos-{name}-{}-{nanos}", std::process::id()))
    }

    fn submit_held(queue: &RuntimeTaskQueue, kind: &'static str) {
        queue
            .try_submit(kind, |shutdown| async move {
                shutdown.cancelled().await;
            })
            .expect("submit held work");
    }

    fn saturate_queue(queue: &RuntimeTaskQueue, kind: &'static str) {
        for _task in 0..queue.capacity().saturating_add(2) {
            let _result = queue.try_submit(kind, |shutdown| async move {
                shutdown.cancelled().await;
            });
        }
        assert!(
            queue.stats().rejected > 0,
            "{} queue should reject overflow",
            queue.name()
        );
    }

    async fn wait_for_started(queue: &RuntimeTaskQueue) {
        for _attempt in 0..20 {
            if queue.stats().started > 0 {
                return;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            queue.stats().started > 0,
            "{} queue should start held work",
            queue.name()
        );
    }
}
