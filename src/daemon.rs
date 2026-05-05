//! Daemon runtime, Axum HTTP serving, scheduler loop, and shutdown handling.

use std::{
    borrow::Cow,
    collections::BTreeMap,
    future::Future,
    io,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use axum::{
    Router,
    body::Bytes,
    extract::{ConnectInfo, DefaultBodyLimit, Query, Request, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header::CONTENT_TYPE},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use prometheus::{
    Encoder, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry, TextEncoder,
};
use sha1::{Digest, Sha1};
use tokio::{net::TcpListener, sync::Mutex, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::{
    SporosError,
    api::{
        AUTH_MESSAGE, AnnounceAccepted, AnnounceRequest, ApiHandlers, ApiMethod, ApiOutcome,
        ApiRequest, ApiResponse, JobRequest, JobResponse, WebhookRequest,
        handle_trusted_api_request,
    },
    config::RuntimeConfig,
    domain::{ActionResult, Candidate, Decision, InjectionResult, SaveResult},
    persistence::{
        AnnounceQueueStats, AnnounceWorkFinish, AnnounceWorkInsert, AnnounceWorkRecord,
        AnnounceWorkRetry, AnnounceWorkTerminalStatus, AsyncDatabase, Database,
    },
    runtime::{RuntimeBlockingExecutor, RuntimeServices, RuntimeTaskQueue},
    scheduler::{DaemonPlan, DaemonRun, JobConfigOverride, JobName, ScheduledJob, Scheduler},
};

const JOB_LOOP_INTERVAL: Duration = Duration::from_secs(60);
const MAX_REQUEST_BODY_BYTES: usize = 64 * 1024;
const ANNOUNCE_QUEUE_ATTEMPT_RESULTS: &[&str] = &["started", "retry_scheduled", "exhausted"];
const ANNOUNCE_QUEUE_OUTCOMES: &[&str] = &["succeeded", "terminal_failed", "expired"];

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
    let async_database = AsyncDatabase::open(&config.database_path).await?;
    let runtime_services = RuntimeServices::start(shutdown.child_token());
    let metrics = Arc::new(DaemonMetrics::default());
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
    finish_executed_jobs(&mut plan.scheduler, &async_database, startup_jobs, &metrics).await?;

    let mut server_state = None;
    let mut announce_workers = None;
    let server = if let Some(address) = listen_address(config) {
        announce_workers = Some(
            start_announce_workers(
                app_dir,
                config,
                Arc::clone(&runtime_services),
                shutdown.child_token(),
                now_millis(),
            )
            .await?,
        );
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
            metrics: Arc::clone(&metrics),
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
                    finish_executed_jobs(&mut scheduler, &async_database, executed_jobs, &metrics).await?;
                } else {
                    finish_executed_jobs(&mut plan.scheduler, &async_database, executed_jobs, &metrics).await?;
                }
                iterations = iterations.saturating_add(1);
            }
        }
    }

    let shutdown_started = Instant::now();
    tracing::info!("service shutdown starting");
    shutdown.cancel();
    if let Some(server) = server {
        server
            .await
            .map_err(|error| daemon_error(format!("HTTP server task failed: {error}")))??;
        tracing::info!("HTTP intake stopped");
    }
    if let Some(workers) = announce_workers {
        workers.shutdown().await?;
        tracing::info!("announce queue workers stopped");
    }
    runtime_services.shutdown().await;
    async_database.close().await;
    tracing::info!(
        elapsed_ms = shutdown_started.elapsed().as_millis(),
        "service shutdown complete"
    );
    Ok(run)
}

struct AnnounceWorkerGroup {
    shutdown: CancellationToken,
    handles: Vec<JoinHandle<crate::Result<()>>>,
}

impl AnnounceWorkerGroup {
    async fn shutdown(self) -> crate::Result<()> {
        self.shutdown.cancel();
        for handle in self.handles {
            handle
                .await
                .map_err(|error| daemon_error(format!("announce worker task failed: {error}")))??;
        }
        Ok(())
    }
}

async fn start_announce_workers(
    app_dir: &Path,
    config: &RuntimeConfig,
    services: Arc<RuntimeServices>,
    shutdown: CancellationToken,
    now: i64,
) -> crate::Result<AnnounceWorkerGroup> {
    let database = AsyncDatabase::open(&config.database_path).await?;
    let recovered = database
        .release_stale_announce_leases(now, now, config.announce_queue.max_accepted_backlog)
        .await?;
    if !recovered.is_empty() {
        tracing::warn!(
            recovered = recovered.len(),
            "recovered stale announce queue leases"
        );
    }
    database.close().await;

    let worker_shutdown = shutdown.child_token();
    let mut handles = Vec::new();
    for worker_index in 0..config.announce_queue.worker_concurrency {
        let worker_id = format!("announce-worker-{worker_index}");
        let worker = AnnounceWorker {
            worker_id,
            app_dir: app_dir.to_owned(),
            config: config.clone(),
            services: Arc::clone(&services),
            shutdown: worker_shutdown.child_token(),
        };
        handles.push(tokio::spawn(worker.run()));
    }
    tracing::info!(workers = handles.len(), "announce queue workers started");
    Ok(AnnounceWorkerGroup {
        shutdown: worker_shutdown,
        handles,
    })
}

struct AnnounceWorker {
    worker_id: String,
    app_dir: PathBuf,
    config: RuntimeConfig,
    services: Arc<RuntimeServices>,
    shutdown: CancellationToken,
}

impl AnnounceWorker {
    async fn run(self) -> crate::Result<()> {
        let database = AsyncDatabase::open(&self.config.database_path).await?;
        loop {
            tokio::select! {
                () = self.shutdown.cancelled() => break,
                result = self.poll_once(&database) => {
                    if !result? {
                        tokio::select! {
                            () = self.shutdown.cancelled() => break,
                            () = tokio::time::sleep(Duration::from_secs(1)) => {}
                        }
                    }
                }
            }
        }
        database.close().await;
        Ok(())
    }

    async fn poll_once(&self, database: &AsyncDatabase) -> crate::Result<bool> {
        let now = now_millis();
        let expired = database
            .expire_announce_work(now, self.config.announce_queue.claim_batch_size)
            .await?;
        for work in expired {
            tracing::warn!(
                work_id = work.work_id.as_str(),
                dedupe_key = work.dedupe_key.as_str(),
                attempt = work.attempts,
                status = work.status.as_str(),
                transition = "expired",
                "expired announce work before processing"
            );
        }

        let recovered = database
            .release_stale_announce_leases(now, now, self.config.announce_queue.claim_batch_size)
            .await?;
        for work in recovered {
            tracing::warn!(
                work_id = work.work_id.as_str(),
                dedupe_key = work.dedupe_key.as_str(),
                attempt = work.attempts,
                status = work.status.as_str(),
                transition = "retrying",
                error_class = "lease_timeout",
                "released stale announce work lease"
            );
        }

        let mut claimed = database
            .claim_announce_work(
                now,
                &self.worker_id,
                i64::try_from(self.config.announce_queue.lease_timeout).unwrap_or(i64::MAX),
                1,
            )
            .await?;
        let Some(work) = claimed.pop() else {
            return Ok(false);
        };
        self.process(database, work).await?;
        Ok(true)
    }

    async fn process(
        &self,
        database: &AsyncDatabase,
        work: AnnounceWorkRecord,
    ) -> crate::Result<()> {
        tracing::info!(
            work_id = work.work_id.as_str(),
            dedupe_key = work.dedupe_key.as_str(),
            attempt = work.attempts,
            "processing announce work"
        );
        let notifier = crate::notifications::NotificationSender::from_config(
            &self.config,
            crate::startup::Redactor::from_config(&self.config),
        )?;
        let result = crate::operations::run_announce_match_async(
            self.services.blocking().matching.clone(),
            self.app_dir.clone(),
            self.config.clone(),
            announce_candidate(&work),
            notifier,
        )
        .await;
        let now = now_millis();
        match classify_announce_work_result(result, &work, now, &self.config) {
            AnnounceWorkExecution::Succeeded { context } => {
                database
                    .finish_announce_work(&AnnounceWorkFinish {
                        work_id: &work.work_id,
                        now,
                        status: AnnounceWorkTerminalStatus::Succeeded,
                        error_class: None,
                        error_message: None,
                        outcome_context: Some(context),
                    })
                    .await?;
                tracing::info!(
                    work_id = work.work_id.as_str(),
                    dedupe_key = work.dedupe_key.as_str(),
                    attempt = work.attempts,
                    status = "succeeded",
                    transition = "succeeded",
                    outcome_context = context,
                    "announce work completed"
                );
            }
            AnnounceWorkExecution::Waiting {
                next_attempt_at,
                context,
            } => {
                database
                    .schedule_announce_retry(&AnnounceWorkRetry {
                        work_id: &work.work_id,
                        now,
                        next_attempt_at,
                        error_class: None,
                        error_message: None,
                        outcome_context: Some(context),
                    })
                    .await?;
                tracing::info!(
                    work_id = work.work_id.as_str(),
                    dedupe_key = work.dedupe_key.as_str(),
                    attempt = work.attempts,
                    next_attempt_at,
                    status = "retrying",
                    transition = "waiting",
                    outcome_context = context,
                    "announce work is waiting"
                );
            }
            AnnounceWorkExecution::Retryable {
                next_attempt_at,
                error_class,
                error_message,
                context,
            } => {
                database
                    .schedule_announce_retry(&AnnounceWorkRetry {
                        work_id: &work.work_id,
                        now,
                        next_attempt_at,
                        error_class: Some(error_class),
                        error_message: Some(&error_message),
                        outcome_context: Some(context),
                    })
                    .await?;
                tracing::warn!(
                    work_id = work.work_id.as_str(),
                    dedupe_key = work.dedupe_key.as_str(),
                    attempt = work.attempts,
                    next_attempt_at,
                    status = "retrying",
                    transition = "retrying",
                    error_class,
                    outcome_context = context,
                    "announce work scheduled for retry: {error_message}"
                );
            }
            AnnounceWorkExecution::TerminalFailed {
                error_class,
                error_message,
                context,
            } => {
                database
                    .finish_announce_work(&AnnounceWorkFinish {
                        work_id: &work.work_id,
                        now,
                        status: AnnounceWorkTerminalStatus::TerminalFailed,
                        error_class: Some(error_class),
                        error_message,
                        outcome_context: Some(context),
                    })
                    .await?;
                tracing::info!(
                    work_id = work.work_id.as_str(),
                    dedupe_key = work.dedupe_key.as_str(),
                    attempt = work.attempts,
                    status = "terminal_failed",
                    transition = "terminal_failed",
                    error_class,
                    context,
                    "announce work reached terminal state"
                );
            }
            AnnounceWorkExecution::Expired { context } => {
                database
                    .finish_announce_work(&AnnounceWorkFinish {
                        work_id: &work.work_id,
                        now,
                        status: AnnounceWorkTerminalStatus::Expired,
                        error_class: None,
                        error_message: None,
                        outcome_context: Some(context),
                    })
                    .await?;
                tracing::warn!(
                    work_id = work.work_id.as_str(),
                    dedupe_key = work.dedupe_key.as_str(),
                    attempt = work.attempts,
                    status = "expired",
                    transition = "expired",
                    outcome_context = context,
                    "announce work expired during processing"
                );
            }
        }
        Ok(())
    }
}

fn announce_candidate(work: &AnnounceWorkRecord) -> Candidate<'static> {
    let mut candidate = Candidate::new(
        work.name.clone(),
        work.guid.clone(),
        Some(work.link.clone()),
        work.tracker.clone(),
    );
    candidate.cookie = work.cookie.clone().map(Cow::Owned);
    candidate
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum AnnounceWorkExecution {
    Succeeded {
        context: &'static str,
    },
    Waiting {
        next_attempt_at: i64,
        context: &'static str,
    },
    Retryable {
        next_attempt_at: i64,
        error_class: &'static str,
        error_message: String,
        context: &'static str,
    },
    TerminalFailed {
        error_class: &'static str,
        error_message: Option<&'static str>,
        context: &'static str,
    },
    Expired {
        context: &'static str,
    },
}

fn classify_announce_work_result(
    result: crate::Result<Option<ApiOutcome>>,
    work: &AnnounceWorkRecord,
    now: i64,
    config: &RuntimeConfig,
) -> AnnounceWorkExecution {
    if now >= work.expires_at {
        return AnnounceWorkExecution::Expired {
            context: "expired_during_processing",
        };
    }

    match result {
        Ok(Some(outcome)) => classify_announce_outcome(outcome, work, now, config),
        Ok(None) => AnnounceWorkExecution::TerminalFailed {
            error_class: "no_match",
            error_message: None,
            context: "no_match",
        },
        Err(error) => AnnounceWorkExecution::Retryable {
            next_attempt_at: retry_next_attempt_at(now, work.attempts, config, None),
            error_class: "workflow_error",
            error_message: error.to_string(),
            context: "retryable_workflow_error",
        },
    }
}

fn classify_announce_outcome(
    outcome: ApiOutcome,
    work: &AnnounceWorkRecord,
    now: i64,
    config: &RuntimeConfig,
) -> AnnounceWorkExecution {
    match outcome.action_result {
        Some(ActionResult::Save(SaveResult::Saved))
        | Some(ActionResult::Injection(InjectionResult::Injected))
        | Some(ActionResult::Injection(InjectionResult::AlreadyExists))
        | Some(ActionResult::Injection(InjectionResult::Failure)) => {
            AnnounceWorkExecution::Succeeded {
                context: "action_completed",
            }
        }
        Some(ActionResult::Injection(InjectionResult::TorrentNotComplete)) => {
            AnnounceWorkExecution::Waiting {
                next_attempt_at: retry_next_attempt_at(now, work.attempts, config, None),
                context: "waiting_source_torrent_incomplete",
            }
        }
        None if outcome.decision == Decision::RateLimited => AnnounceWorkExecution::Retryable {
            next_attempt_at: retry_next_attempt_at(now, work.attempts, config, None),
            error_class: "rate_limited",
            error_message: "announce workflow hit a rate limit".to_owned(),
            context: "rate_limited_decision",
        },
        None if matches!(
            outcome.decision,
            Decision::InfoHashAlreadyExists | Decision::SameInfoHash
        ) =>
        {
            AnnounceWorkExecution::TerminalFailed {
                error_class: "already_present",
                error_message: None,
                context: "already_present",
            }
        }
        None => AnnounceWorkExecution::TerminalFailed {
            error_class: "terminal_decision",
            error_message: None,
            context: decision_context(outcome.decision),
        },
    }
}

fn retry_next_attempt_at(
    now: i64,
    attempts: i64,
    config: &RuntimeConfig,
    retry_after_at: Option<i64>,
) -> i64 {
    let min_delay = i64::try_from(config.announce_queue.retry_delay_min).unwrap_or(i64::MAX);
    let max_delay = i64::try_from(config.announce_queue.retry_delay_max).unwrap_or(i64::MAX);
    let growth = attempts
        .saturating_sub(1)
        .try_into()
        .ok()
        .and_then(|power| 1_i64.checked_shl(power))
        .unwrap_or(i64::MAX);
    let policy_delay = min_delay.saturating_mul(growth).min(max_delay);
    let policy_next = now.saturating_add(policy_delay);
    retry_after_at
        .filter(|retry_after| *retry_after > now)
        .filter(|retry_after| retry_after.saturating_sub(now) <= max_delay)
        .map_or(policy_next, |retry_after| retry_after.max(policy_next))
}

fn decision_context(decision: Decision) -> &'static str {
    match decision {
        Decision::BlockedRelease => "blocked_release",
        Decision::DownloadFailed => "download_failed",
        Decision::FileTreeMismatch => "file_tree_mismatch",
        Decision::FuzzySizeMismatch => "fuzzy_size_mismatch",
        Decision::InfoHashAlreadyExists => "already_present",
        Decision::MagnetLink => "magnet_link",
        Decision::NoDownloadLink => "no_download_link",
        Decision::PartialSizeMismatch => "partial_size_mismatch",
        Decision::ProperRepackMismatch => "proper_repack_mismatch",
        Decision::RateLimited => "rate_limited_decision",
        Decision::ReleaseGroupMismatch => "release_group_mismatch",
        Decision::ResolutionMismatch => "resolution_mismatch",
        Decision::SameInfoHash => "already_present",
        Decision::SizeMismatch => "size_mismatch",
        Decision::SourceMismatch => "source_mismatch",
        Decision::Match | Decision::MatchPartial | Decision::MatchSizeOnly => {
            "match_without_action"
        }
    }
}

fn serve_http(
    listener: TcpListener,
    state: Arc<DaemonState>,
    shutdown: CancellationToken,
) -> JoinHandle<crate::Result<()>> {
    tokio::spawn(async move {
        let router = http_router(state);
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(shutdown.cancelled_owned())
        .await
        .map_err(|error| daemon_error(format!("HTTP server failed: {error}")))
    })
}

fn http_router(state: Arc<DaemonState>) -> Router {
    let protected_api = Router::new()
        .route("/api/status", get(handle_status))
        .route("/api/announce", post(handle_announce))
        .route("/api/webhook", post(handle_webhook))
        .route("/api/job", post(handle_job))
        .route_layer(middleware::from_fn_with_state(
            Arc::clone(&state),
            require_api_auth,
        ));

    Router::new()
        .route("/_health/livez", get(handle_livez))
        .route("/_health/readyz", get(handle_readyz))
        .route("/metrics", get(handle_metrics))
        .route("/api/ping", get(handle_ping))
        .merge(protected_api)
        .fallback(handle_not_found)
        .method_not_allowed_fallback(handle_method_not_allowed)
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BODY_BYTES))
        .layer(middleware::from_fn_with_state(
            Arc::clone(&state),
            record_http_request,
        ))
        .with_state(state)
}

async fn require_api_auth(
    State(state): State<Arc<DaemonState>>,
    ConnectInfo(remote_addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Query(query): Query<BTreeMap<String, String>>,
    request: Request,
    next: Next,
) -> Response {
    match configured_api_key(&state).await {
        Ok(api_key) if request_authorized(&headers, &query, &api_key) => next.run(request).await,
        Ok(_) => {
            tracing::warn!(
                net.peer.addr = %client_addr(&headers, Some(remote_addr)),
                "unauthorized API request"
            );
            DaemonHttpResponse {
                status: StatusCode::UNAUTHORIZED,
                body: DaemonHttpBody::Text(AUTH_MESSAGE.to_owned()),
            }
            .into_response()
        }
        Err(error) => {
            tracing::error!("API auth failed: {error}");
            DaemonHttpResponse {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                body: DaemonHttpBody::Text(error.to_string()),
            }
            .into_response()
        }
    }
}

async fn configured_api_key(state: &DaemonState) -> crate::Result<String> {
    let async_database = AsyncDatabase::open(&state.config.database_path).await?;
    let api_key =
        crate::operations::api_key_async(&async_database, state.config.api_key.as_deref()).await;
    async_database.close().await;
    api_key
}

fn request_authorized(
    headers: &HeaderMap,
    query: &BTreeMap<String, String>,
    api_key: &str,
) -> bool {
    headers
        .get("x-api-key")
        .and_then(|value| value.to_str().ok())
        .or_else(|| query.get("apikey").map(String::as_str))
        .is_some_and(|value| value == api_key)
}

async fn record_http_request(
    State(state): State<Arc<DaemonState>>,
    ConnectInfo(remote_addr): ConnectInfo<SocketAddr>,
    request: Request,
    next: Next,
) -> Response {
    let started = Instant::now();
    let method_label = request.method().as_str().to_owned();
    let route = http_route(request.uri().path());
    let remote_addr = client_addr(request.headers(), Some(remote_addr));
    let response = next.run(request).await;
    let status = response.status().as_u16();
    let latency_ms = started.elapsed().as_millis();

    state
        .metrics
        .record_http_request(&method_label, route, status, latency_ms);
    tracing::info!(
        http.method = %method_label,
        http.route = route,
        http.status_code = status,
        http.latency_ms = latency_ms,
        net.peer.addr = %remote_addr,
        "http request completed"
    );
    response
}

fn client_addr(headers: &HeaderMap, remote_addr: Option<SocketAddr>) -> String {
    headers
        .get("x-forwarded-for")
        .and_then(|value| value.to_str().ok())
        .and_then(forwarded_client_addr)
        .unwrap_or_else(|| {
            remote_addr
                .map(|addr| addr.to_string())
                .unwrap_or_else(|| "unknown".to_owned())
        })
}

fn forwarded_client_addr(value: &str) -> Option<String> {
    value
        .split(',')
        .map(str::trim)
        .find(|addr| addr.parse::<std::net::IpAddr>().is_ok())
        .map(ToOwned::to_owned)
}

async fn handle_livez(
    State(state): State<Arc<DaemonState>>,
    method: Method,
    headers: HeaderMap,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    handle_api_parts(
        state,
        method,
        "/_health/livez",
        query,
        headers,
        String::new(),
    )
    .await
}

async fn handle_readyz(
    State(state): State<Arc<DaemonState>>,
    method: Method,
    headers: HeaderMap,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    handle_api_parts(
        state,
        method,
        "/_health/readyz",
        query,
        headers,
        String::new(),
    )
    .await
}

async fn handle_metrics(
    State(state): State<Arc<DaemonState>>,
    method: Method,
    headers: HeaderMap,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    handle_api_parts(state, method, "/metrics", query, headers, String::new()).await
}

async fn handle_ping(
    State(state): State<Arc<DaemonState>>,
    method: Method,
    headers: HeaderMap,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    handle_api_parts(state, method, "/api/ping", query, headers, String::new()).await
}

async fn handle_status(
    State(state): State<Arc<DaemonState>>,
    method: Method,
    headers: HeaderMap,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    handle_api_parts(state, method, "/api/status", query, headers, String::new()).await
}

async fn handle_announce(
    State(state): State<Arc<DaemonState>>,
    method: Method,
    headers: HeaderMap,
    Query(query): Query<BTreeMap<String, String>>,
    body: Bytes,
) -> Response {
    handle_api_parts(
        state,
        method,
        "/api/announce",
        query,
        headers,
        String::from_utf8_lossy(&body).into_owned(),
    )
    .await
}

async fn handle_webhook(
    State(state): State<Arc<DaemonState>>,
    method: Method,
    headers: HeaderMap,
    Query(query): Query<BTreeMap<String, String>>,
    body: Bytes,
) -> Response {
    handle_api_parts(
        state,
        method,
        "/api/webhook",
        query,
        headers,
        String::from_utf8_lossy(&body).into_owned(),
    )
    .await
}

async fn handle_job(
    State(state): State<Arc<DaemonState>>,
    method: Method,
    headers: HeaderMap,
    Query(query): Query<BTreeMap<String, String>>,
    body: Bytes,
) -> Response {
    handle_api_parts(
        state,
        method,
        "/api/job",
        query,
        headers,
        String::from_utf8_lossy(&body).into_owned(),
    )
    .await
}

async fn handle_api_parts(
    state: Arc<DaemonState>,
    method: Method,
    path: &'static str,
    query: BTreeMap<String, String>,
    headers: HeaderMap,
    body: String,
) -> Response {
    let request = ApiRequest {
        method: api_method(&method),
        path: path.to_owned(),
        query,
        headers: api_headers(&headers),
        body,
        remote_addr: None,
    };

    let response = match handle_runtime_request(Arc::clone(&state), request).await {
        Ok(response) => response,
        Err(error) => {
            tracing::error!("API request failed: {error}");
            crate::api::ApiResponse {
                status: 500,
                body: error.to_string(),
            }
        }
    };
    DaemonHttpResponse::from_api(path, response).into_response()
}

fn api_headers(headers: &HeaderMap) -> BTreeMap<String, String> {
    headers
        .iter()
        .filter_map(|(key, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (key.as_str().to_ascii_lowercase(), value.to_owned()))
        })
        .collect()
}

async fn handle_not_found(
    State(_state): State<Arc<DaemonState>>,
    _method: Method,
    uri: Uri,
) -> Response {
    let route = http_route(uri.path());
    DaemonHttpResponse::from_api(
        route,
        ApiResponse {
            status: 404,
            body: "Not Found".to_owned(),
        },
    )
    .into_response()
}

async fn handle_method_not_allowed(
    State(_state): State<Arc<DaemonState>>,
    _method: Method,
    uri: Uri,
) -> Response {
    let route = http_route(uri.path());
    DaemonHttpResponse::from_api(
        route,
        ApiResponse {
            status: 405,
            body: "Method Not Allowed".to_owned(),
        },
    )
    .into_response()
}

enum DaemonHttpBody {
    Empty,
    Json(String),
    Metrics(String),
    Text(String),
}

struct DaemonHttpResponse {
    status: StatusCode,
    body: DaemonHttpBody,
}

impl DaemonHttpResponse {
    fn from_api(route: &'static str, response: ApiResponse) -> Self {
        let status = StatusCode::from_u16(response.status).unwrap_or(StatusCode::OK);
        let body = if status == StatusCode::NO_CONTENT || response.body.is_empty() {
            DaemonHttpBody::Empty
        } else if route == "/metrics" && status == StatusCode::OK {
            DaemonHttpBody::Metrics(response.body)
        } else if route_returns_json(route) && looks_like_json_object(&response.body) {
            DaemonHttpBody::Json(response.body)
        } else {
            DaemonHttpBody::Text(response.body)
        };
        Self { status, body }
    }
}

impl IntoResponse for DaemonHttpResponse {
    fn into_response(self) -> Response {
        match self.body {
            DaemonHttpBody::Empty => self.status.into_response(),
            DaemonHttpBody::Json(body) => {
                let mut response = (self.status, body).into_response();
                response
                    .headers_mut()
                    .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
                response
            }
            DaemonHttpBody::Metrics(body) => {
                let mut response = (self.status, body).into_response();
                response.headers_mut().insert(
                    CONTENT_TYPE,
                    HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
                );
                response
            }
            DaemonHttpBody::Text(body) => (self.status, body).into_response(),
        }
    }
}

fn route_returns_json(route: &str) -> bool {
    matches!(
        route,
        "/_health/livez" | "/_health/readyz" | "/api/status" | "/api/announce"
    )
}

fn looks_like_json_object(body: &str) -> bool {
    body.trim_start().starts_with('{')
}

fn http_route(path: &str) -> &'static str {
    match path {
        "/_health/livez" => "/_health/livez",
        "/_health/readyz" => "/_health/readyz",
        "/api/announce" => "/api/announce",
        "/api/job" => "/api/job",
        "/api/ping" => "/api/ping",
        "/api/status" => "/api/status",
        "/api/webhook" => "/api/webhook",
        "/metrics" => "/metrics",
        _ => "unmatched",
    }
}

async fn handle_runtime_request(
    state: Arc<DaemonState>,
    request: ApiRequest,
) -> crate::Result<crate::api::ApiResponse> {
    if request.path == "/metrics" {
        return metrics_response(state, request.method).await;
    }
    if let Some(response) = handle_health_request(Arc::clone(&state), &request).await {
        return Ok(response);
    }

    let async_database = AsyncDatabase::open(&state.config.database_path).await?;
    let needs_scheduler = request.path == "/api/job";
    let scheduler_snapshot = if request.path == "/api/status" {
        state
            .scheduler
            .try_lock()
            .ok()
            .map(|scheduler| scheduler.jobs().to_vec())
    } else {
        None
    };
    let scheduler_available = request.path != "/api/status" || scheduler_snapshot.is_some();
    let mut scheduler = if needs_scheduler {
        Some(state.scheduler.lock().await)
    } else {
        None
    };
    let mut handlers = RuntimeHandlers {
        config: &state.config,
        services: Arc::clone(&state.services),
        metrics: Arc::clone(&state.metrics),
        async_database: &async_database,
        scheduler: scheduler.as_deref_mut(),
        scheduler_snapshot,
        scheduler_available,
        now_millis: now_millis(),
        webhook_requests: Vec::new(),
        job_dispatches: Vec::new(),
    };
    let response = handle_trusted_api_request(request, &mut handlers).await?;
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
        finish_executed_jobs(
            &mut scheduler,
            &async_database,
            executed_jobs,
            &state.metrics,
        )
        .await?;
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

async fn metrics_response(
    state: Arc<DaemonState>,
    method: ApiMethod,
) -> crate::Result<crate::api::ApiResponse> {
    if method != ApiMethod::Get {
        return Ok(crate::api::ApiResponse {
            status: 405,
            body: "Method Not Allowed".to_owned(),
        });
    }

    let registry = Registry::new();
    let service_info = IntGaugeVec::new(
        Opts::new("sporos_service_info", "Static service information."),
        &["version"],
    )
    .map_err(metrics_error)?;
    let service_uptime = IntGauge::with_opts(Opts::new(
        "sporos_service_uptime_seconds",
        "Seconds since this service runtime started.",
    ))
    .map_err(metrics_error)?;
    registry
        .register(Box::new(service_info.clone()))
        .map_err(metrics_error)?;
    registry
        .register(Box::new(service_uptime.clone()))
        .map_err(metrics_error)?;
    service_info.with_label_values(&[crate::VERSION]).set(1);
    service_uptime
        .set(i64::try_from(state.metrics.started_at.elapsed().as_secs()).unwrap_or(i64::MAX));

    let http_requests = IntCounter::with_opts(Opts::new(
        "sporos_http_requests_total",
        "Total HTTP requests received by the daemon.",
    ))
    .map_err(metrics_error)?;
    registry
        .register(Box::new(http_requests.clone()))
        .map_err(metrics_error)?;
    http_requests.inc_by(usize_to_u64(
        state.metrics.http_requests.load(Ordering::Relaxed),
    ));
    let http_requests_by_route = IntCounterVec::new(
        Opts::new(
            "sporos_http_requests_by_route_total",
            "Total HTTP requests by bounded method, route, and status labels.",
        ),
        &["method", "route", "status"],
    )
    .map_err(metrics_error)?;
    let http_latency_by_route = IntCounterVec::new(
        Opts::new(
            "sporos_http_request_latency_ms_total",
            "Total HTTP request latency in milliseconds by bounded labels.",
        ),
        &["method", "route", "status"],
    )
    .map_err(metrics_error)?;
    registry
        .register(Box::new(http_requests_by_route.clone()))
        .map_err(metrics_error)?;
    registry
        .register(Box::new(http_latency_by_route.clone()))
        .map_err(metrics_error)?;
    for (key, value) in state.metrics.http_metrics() {
        let status = key.status.to_string();
        http_requests_by_route
            .with_label_values(&[&key.method, &key.route, &status])
            .inc_by(usize_to_u64(value.count));
        http_latency_by_route
            .with_label_values(&[&key.method, &key.route, &status])
            .inc_by(usize_to_u64(value.latency_ms_total));
    }

    let queue_events = IntCounterVec::new(
        Opts::new(
            "sporos_runtime_queue_events_total",
            "Runtime queue lifecycle events.",
        ),
        &["queue", "event"],
    )
    .map_err(metrics_error)?;
    let queue_capacity = IntGaugeVec::new(
        Opts::new(
            "sporos_runtime_queue_capacity",
            "Configured runtime queue capacity.",
        ),
        &["queue"],
    )
    .map_err(metrics_error)?;
    let queue_depth = IntGaugeVec::new(
        Opts::new(
            "sporos_runtime_queue_depth",
            "Runtime commands queued but not started.",
        ),
        &["queue"],
    )
    .map_err(metrics_error)?;
    let queue_in_flight = IntGaugeVec::new(
        Opts::new(
            "sporos_runtime_queue_in_flight",
            "Runtime commands started and not yet finished or cancelled.",
        ),
        &["queue"],
    )
    .map_err(metrics_error)?;
    registry
        .register(Box::new(queue_events.clone()))
        .map_err(metrics_error)?;
    registry
        .register(Box::new(queue_capacity.clone()))
        .map_err(metrics_error)?;
    registry
        .register(Box::new(queue_depth.clone()))
        .map_err(metrics_error)?;
    registry
        .register(Box::new(queue_in_flight.clone()))
        .map_err(metrics_error)?;
    observe_queue_metrics(
        &queue_events,
        &queue_capacity,
        &queue_depth,
        &queue_in_flight,
        &state.services.queues().jobs,
    );
    observe_queue_metrics(
        &queue_events,
        &queue_capacity,
        &queue_depth,
        &queue_in_flight,
        &state.services.queues().webhooks,
    );
    observe_queue_metrics(
        &queue_events,
        &queue_capacity,
        &queue_depth,
        &queue_in_flight,
        &state.services.queues().reverse_lookup,
    );
    observe_queue_metrics(
        &queue_events,
        &queue_capacity,
        &queue_depth,
        &queue_in_flight,
        &state.services.queues().injection,
    );
    observe_queue_metrics(
        &queue_events,
        &queue_capacity,
        &queue_depth,
        &queue_in_flight,
        &state.services.queues().blocking_local,
    );
    observe_blocking_metrics(
        &queue_events,
        &queue_capacity,
        &queue_depth,
        &queue_in_flight,
        &state.services.blocking().filesystem,
    );
    observe_blocking_metrics(
        &queue_events,
        &queue_capacity,
        &queue_depth,
        &queue_in_flight,
        &state.services.blocking().torrent_io,
    );
    observe_blocking_metrics(
        &queue_events,
        &queue_capacity,
        &queue_depth,
        &queue_in_flight,
        &state.services.blocking().linking,
    );
    observe_blocking_metrics(
        &queue_events,
        &queue_capacity,
        &queue_depth,
        &queue_in_flight,
        &state.services.blocking().matching,
    );

    let database = AsyncDatabase::open(&state.config.database_path).await?;
    let announce_queue = announce_queue_status(&state.config, &database, now_millis()).await?;
    database.close().await;
    observe_announce_queue_metrics(&registry, &announce_queue)?;
    observe_job_metrics(&registry, &state).await?;
    observe_indexer_metrics(&registry, &state.config).await?;

    let encoder = TextEncoder::new();
    let mut output = Vec::new();
    encoder
        .encode(&registry.gather(), &mut output)
        .map_err(metrics_error)?;
    let body = String::from_utf8(output)
        .map_err(|error| daemon_error(format!("failed to encode metrics as UTF-8: {error}")))?;
    Ok(crate::api::ApiResponse { status: 200, body })
}

fn observe_queue_metrics(
    events: &IntCounterVec,
    capacity: &IntGaugeVec,
    depth: &IntGaugeVec,
    in_flight: &IntGaugeVec,
    queue: &RuntimeTaskQueue,
) {
    let stats = queue.stats();
    capacity
        .with_label_values(&[queue.name()])
        .set(usize_to_i64(queue.capacity()));
    depth
        .with_label_values(&[queue.name()])
        .set(usize_to_i64(stats.enqueued.saturating_sub(stats.started)));
    in_flight
        .with_label_values(&[queue.name()])
        .set(usize_to_i64(stats.started.saturating_sub(
            stats.finished.saturating_add(stats.cancelled),
        )));
    for (event, value) in [
        ("accepted", stats.enqueued),
        ("rejected", stats.rejected),
        ("started", stats.started),
        ("finished", stats.finished),
        ("cancelled", stats.cancelled),
    ] {
        events
            .with_label_values(&[queue.name(), event])
            .inc_by(usize_to_u64(value));
    }
}

fn observe_blocking_metrics(
    events: &IntCounterVec,
    capacity: &IntGaugeVec,
    depth: &IntGaugeVec,
    in_flight: &IntGaugeVec,
    executor: &RuntimeBlockingExecutor,
) {
    let stats = executor.stats();
    capacity
        .with_label_values(&[executor.name()])
        .set(usize_to_i64(executor.capacity()));
    depth
        .with_label_values(&[executor.name()])
        .set(usize_to_i64(stats.enqueued.saturating_sub(stats.started)));
    in_flight
        .with_label_values(&[executor.name()])
        .set(usize_to_i64(stats.started.saturating_sub(
            stats.finished.saturating_add(stats.cancelled),
        )));
    for (event, value) in [
        ("accepted", stats.enqueued),
        ("rejected", stats.rejected),
        ("started", stats.started),
        ("finished", stats.finished),
        ("cancelled", stats.cancelled),
    ] {
        events
            .with_label_values(&[executor.name(), event])
            .inc_by(usize_to_u64(value));
    }
}

fn observe_announce_queue_metrics(
    registry: &Registry,
    queue: &AnnounceQueueStatus,
) -> crate::Result<()> {
    let enabled = IntGauge::with_opts(Opts::new(
        "sporos_announce_queue_enabled",
        "Whether the durable announce queue is configured.",
    ))
    .map_err(metrics_error)?;
    let backlog = IntGauge::with_opts(Opts::new(
        "sporos_announce_queue_backlog",
        "Durable announce work items waiting to run.",
    ))
    .map_err(metrics_error)?;
    let running = IntGauge::with_opts(Opts::new(
        "sporos_announce_queue_running",
        "Durable announce work items currently running.",
    ))
    .map_err(metrics_error)?;
    let oldest_age = IntGauge::with_opts(Opts::new(
        "sporos_announce_queue_oldest_queued_age_seconds",
        "Age in seconds of the oldest queued durable announce work item.",
    ))
    .map_err(metrics_error)?;
    let retry_delay = IntGauge::with_opts(Opts::new(
        "sporos_announce_queue_retry_delay_seconds",
        "Current retry delay in seconds for durable announce work.",
    ))
    .map_err(metrics_error)?;
    let breaker_open = IntGauge::with_opts(Opts::new(
        "sporos_announce_queue_breaker_open",
        "Whether durable announce processing is blocked by an open breaker.",
    ))
    .map_err(metrics_error)?;
    let attempts = IntCounterVec::new(
        Opts::new(
            "sporos_announce_queue_attempts_total",
            "Durable announce processing attempts by bounded result.",
        ),
        &["result"],
    )
    .map_err(metrics_error)?;
    let outcomes = IntCounterVec::new(
        Opts::new(
            "sporos_announce_queue_outcomes_total",
            "Durable announce terminal outcomes by bounded result.",
        ),
        &["outcome"],
    )
    .map_err(metrics_error)?;
    let expired = IntCounter::with_opts(Opts::new(
        "sporos_announce_queue_expired_total",
        "Durable announce work items expired before completion.",
    ))
    .map_err(metrics_error)?;

    registry
        .register(Box::new(enabled.clone()))
        .map_err(metrics_error)?;
    registry
        .register(Box::new(backlog.clone()))
        .map_err(metrics_error)?;
    registry
        .register(Box::new(running.clone()))
        .map_err(metrics_error)?;
    registry
        .register(Box::new(oldest_age.clone()))
        .map_err(metrics_error)?;
    registry
        .register(Box::new(retry_delay.clone()))
        .map_err(metrics_error)?;
    registry
        .register(Box::new(breaker_open.clone()))
        .map_err(metrics_error)?;
    registry
        .register(Box::new(attempts.clone()))
        .map_err(metrics_error)?;
    registry
        .register(Box::new(outcomes.clone()))
        .map_err(metrics_error)?;
    registry
        .register(Box::new(expired.clone()))
        .map_err(metrics_error)?;

    enabled.set(bool_to_i64(queue.enabled));
    backlog.set(queue.backlog);
    running.set(queue.running);
    oldest_age.set(queue.oldest_queued_age_seconds.unwrap_or_default());
    retry_delay.set(queue.retry_delay_seconds.unwrap_or_default());
    breaker_open.set(bool_to_i64(queue.breaker_open));
    for result in ANNOUNCE_QUEUE_ATTEMPT_RESULTS {
        let value = match *result {
            "started" => queue.attempts_started,
            "retry_scheduled" => queue.attempts_retry_scheduled,
            "exhausted" => queue.attempts_exhausted,
            _ => 0,
        };
        attempts
            .with_label_values(&[result])
            .inc_by(i64_to_u64(value));
    }
    for outcome in ANNOUNCE_QUEUE_OUTCOMES {
        let value = match *outcome {
            "succeeded" => queue.outcomes_succeeded,
            "terminal_failed" => queue.outcomes_terminal_failed,
            "expired" => queue.outcomes_expired,
            _ => 0,
        };
        outcomes
            .with_label_values(&[outcome])
            .inc_by(i64_to_u64(value));
    }
    expired.inc_by(i64_to_u64(queue.expired));
    Ok(())
}

fn queue_status(queue: &RuntimeTaskQueue) -> serde_json::Value {
    let stats = queue.stats();
    serde_json::json!({
        "name": queue.name(),
        "capacity": queue.capacity(),
        "depth": stats.enqueued.saturating_sub(stats.started),
        "running": stats.started.saturating_sub(stats.finished.saturating_add(stats.cancelled)),
        "accepted": stats.enqueued,
        "rejected": stats.rejected,
        "started": stats.started,
        "finished": stats.finished,
        "cancelled": stats.cancelled,
    })
}

fn blocking_status(executor: &RuntimeBlockingExecutor) -> serde_json::Value {
    let stats = executor.stats();
    serde_json::json!({
        "name": executor.name(),
        "capacity": executor.capacity(),
        "depth": stats.enqueued.saturating_sub(stats.started),
        "running": stats.started.saturating_sub(stats.finished.saturating_add(stats.cancelled)),
        "accepted": stats.enqueued,
        "rejected": stats.rejected,
        "started": stats.started,
        "finished": stats.finished,
        "cancelled": stats.cancelled,
    })
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct AnnounceQueueStatus {
    enabled: bool,
    status: &'static str,
    backlog: i64,
    running: i64,
    oldest_queued_age_seconds: Option<i64>,
    retry_delay_seconds: Option<i64>,
    attempts_started: i64,
    attempts_retry_scheduled: i64,
    attempts_exhausted: i64,
    outcomes_succeeded: i64,
    outcomes_terminal_failed: i64,
    outcomes_expired: i64,
    expired: i64,
    breaker_open: bool,
    breaker_state: &'static str,
    last_error_class: Option<String>,
    last_error_message: Option<String>,
    last_outcome_context: Option<String>,
}

impl AnnounceQueueStatus {
    fn unavailable(status: &'static str) -> Self {
        Self {
            enabled: false,
            status,
            backlog: 0,
            running: 0,
            oldest_queued_age_seconds: None,
            retry_delay_seconds: None,
            attempts_started: 0,
            attempts_retry_scheduled: 0,
            attempts_exhausted: 0,
            outcomes_succeeded: 0,
            outcomes_terminal_failed: 0,
            outcomes_expired: 0,
            expired: 0,
            breaker_open: true,
            breaker_state: "blocked",
            last_error_class: Some(status.to_owned()),
            last_error_message: None,
            last_outcome_context: None,
        }
    }

    fn from_stats(config: &RuntimeConfig, stats: AnnounceQueueStats, now: i64) -> Self {
        let enabled = config.listen_port.is_some() && config.announce_queue.worker_concurrency > 0;
        Self {
            enabled,
            status: if enabled { "ready" } else { "disabled" },
            backlog: stats.backlog,
            running: stats.running,
            oldest_queued_age_seconds: stats
                .oldest_queued_at
                .map(|queued_at| now.saturating_sub(queued_at).max(0) / 1_000),
            retry_delay_seconds: stats
                .next_retry_at
                .map(|retry_at| retry_at.saturating_sub(now).max(0) / 1_000),
            attempts_started: stats.total_attempts,
            attempts_retry_scheduled: stats.retry_scheduled,
            attempts_exhausted: stats.terminal_failed,
            outcomes_succeeded: stats.succeeded,
            outcomes_terminal_failed: stats.terminal_failed,
            outcomes_expired: stats.expired,
            expired: stats.expired,
            breaker_open: false,
            breaker_state: "closed",
            last_error_class: stats.last_error_class,
            last_error_message: stats.last_error_message,
            last_outcome_context: stats.last_outcome_context,
        }
    }

    const fn ready(&self) -> bool {
        !self.breaker_open
    }

    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "enabled": self.enabled,
            "status": self.status,
            "backlog": self.backlog,
            "running": self.running,
            "oldestQueuedAgeSeconds": self.oldest_queued_age_seconds,
            "attempts": {
                "started": self.attempts_started,
                "retryScheduled": self.attempts_retry_scheduled,
                "exhausted": self.attempts_exhausted,
            },
            "outcomes": {
                "succeeded": self.outcomes_succeeded,
                "terminalFailed": self.outcomes_terminal_failed,
                "expired": self.outcomes_expired,
            },
            "retryDelaySeconds": self.retry_delay_seconds,
            "expiryCount": self.expired,
            "breaker": {
                "state": self.breaker_state,
                "open": self.breaker_open,
            },
            "lastError": {
                "class": self.last_error_class,
                "message": self.last_error_message,
                "context": self.last_outcome_context,
            },
        })
    }
}

async fn announce_queue_status(
    config: &RuntimeConfig,
    database: &AsyncDatabase,
    now: i64,
) -> crate::Result<AnnounceQueueStatus> {
    let stats = database.announce_queue_stats(now).await?;
    Ok(AnnounceQueueStatus::from_stats(config, stats, now))
}

async fn observe_job_metrics(registry: &Registry, state: &DaemonState) -> crate::Result<()> {
    let job_enabled = IntGaugeVec::new(
        Opts::new("sporos_job_enabled", "Whether a scheduled job is enabled."),
        &["job"],
    )
    .map_err(metrics_error)?;
    let job_active = IntGaugeVec::new(
        Opts::new("sporos_job_active", "Whether a scheduled job is active."),
        &["job"],
    )
    .map_err(metrics_error)?;
    let job_runs = IntCounterVec::new(
        Opts::new(
            "sporos_job_runs_total",
            "Scheduled job dispatches accepted by the daemon.",
        ),
        &["job"],
    )
    .map_err(metrics_error)?;
    let job_failures = IntCounterVec::new(
        Opts::new(
            "sporos_job_failures_total",
            "Scheduled job executions that returned an error.",
        ),
        &["job"],
    )
    .map_err(metrics_error)?;
    let job_duration = IntCounterVec::new(
        Opts::new(
            "sporos_job_duration_ms_total",
            "Total wall-clock duration of completed scheduled job executions.",
        ),
        &["job"],
    )
    .map_err(metrics_error)?;
    let job_last_run = IntGaugeVec::new(
        Opts::new(
            "sporos_job_last_run_timestamp_seconds",
            "Persisted successful scheduler job last-run timestamp.",
        ),
        &["job"],
    )
    .map_err(metrics_error)?;
    registry
        .register(Box::new(job_enabled.clone()))
        .map_err(metrics_error)?;
    registry
        .register(Box::new(job_active.clone()))
        .map_err(metrics_error)?;
    registry
        .register(Box::new(job_runs.clone()))
        .map_err(metrics_error)?;
    registry
        .register(Box::new(job_failures.clone()))
        .map_err(metrics_error)?;
    registry
        .register(Box::new(job_duration.clone()))
        .map_err(metrics_error)?;
    registry
        .register(Box::new(job_last_run.clone()))
        .map_err(metrics_error)?;

    let database = AsyncDatabase::open(&state.config.database_path).await?;
    let jobs = match state.scheduler.try_lock() {
        Ok(scheduler) => scheduler.jobs().to_vec(),
        Err(_error) => DaemonPlan::from_config(&state.config)
            .scheduler
            .jobs()
            .to_vec(),
    };
    for job in jobs {
        let name = job.name.as_str();
        job_enabled
            .with_label_values(&[name])
            .set(bool_to_i64(job.enabled));
        job_active
            .with_label_values(&[name])
            .set(bool_to_i64(job.is_active));
        job_runs.with_label_values(&[name]).inc_by(job.runs);
        job_failures
            .with_label_values(&[name])
            .inc_by(usize_to_u64(state.metrics.job_failures(job.name)));
        job_duration
            .with_label_values(&[name])
            .inc_by(usize_to_u64(state.metrics.job_duration_ms(job.name)));
        if let Some(last_run) = database.read_last_run(name).await? {
            job_last_run
                .with_label_values(&[name])
                .set(last_run / 1_000);
        }
    }
    database.close().await;
    Ok(())
}

async fn observe_indexer_metrics(registry: &Registry, config: &RuntimeConfig) -> crate::Result<()> {
    let indexer_active = IntGaugeVec::new(
        Opts::new("sporos_indexer_active", "Whether an indexer row is active."),
        &["indexer"],
    )
    .map_err(metrics_error)?;
    let indexer_rate_limited = IntGaugeVec::new(
        Opts::new(
            "sporos_indexer_rate_limited",
            "Whether an indexer is currently marked rate limited.",
        ),
        &["indexer"],
    )
    .map_err(metrics_error)?;
    let indexer_unknown_error = IntGaugeVec::new(
        Opts::new(
            "sporos_indexer_unknown_error",
            "Whether an indexer is currently marked with an unknown error.",
        ),
        &["indexer"],
    )
    .map_err(metrics_error)?;
    let indexer_retry_after = IntGaugeVec::new(
        Opts::new(
            "sporos_indexer_retry_after_timestamp_seconds",
            "Indexer retry-after timestamp when present.",
        ),
        &["indexer"],
    )
    .map_err(metrics_error)?;
    registry
        .register(Box::new(indexer_active.clone()))
        .map_err(metrics_error)?;
    registry
        .register(Box::new(indexer_rate_limited.clone()))
        .map_err(metrics_error)?;
    registry
        .register(Box::new(indexer_unknown_error.clone()))
        .map_err(metrics_error)?;
    registry
        .register(Box::new(indexer_retry_after.clone()))
        .map_err(metrics_error)?;

    let database = AsyncDatabase::open(&config.database_path).await?;
    for indexer in database.indexer_health_rows().await? {
        let label = indexer.url.as_str();
        indexer_active
            .with_label_values(&[label])
            .set(bool_to_i64(indexer.active));
        indexer_rate_limited
            .with_label_values(&[label])
            .set(bool_to_i64(
                indexer.status.as_deref() == Some("RATE_LIMITED"),
            ));
        indexer_unknown_error
            .with_label_values(&[label])
            .set(bool_to_i64(
                indexer.status.as_deref() == Some("UNKNOWN_ERROR"),
            ));
        if let Some(retry_after) = indexer.retry_after {
            indexer_retry_after
                .with_label_values(&[label])
                .set(retry_after / 1_000);
        }
    }
    database.close().await;
    Ok(())
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

    let state_dir_ready = tokio::fs::metadata(&state.app_dir)
        .await
        .is_ok_and(|metadata| metadata.is_dir());
    let database_result = AsyncDatabase::open(&state.config.database_path).await;
    let database_ready = database_result.is_ok();
    let runtime_ready = !state.services.cancellation_token().is_cancelled();
    let scheduler_ready = state.scheduler.try_lock().is_ok();
    let local_paths_ready = configured_local_paths_ready(&state.config).await;
    let intake_ready = state.config.listen_port.is_some() && runtime_ready;
    let announce_queue = match &database_result {
        Ok(database) => {
            let status = announce_queue_status(&state.config, database, now_millis()).await;
            match status {
                Ok(status) => status,
                Err(error) => {
                    tracing::warn!(error = %error, "readiness announce queue check failed");
                    AnnounceQueueStatus::unavailable("database_error")
                }
            }
        }
        Err(error) => {
            tracing::warn!(error = %error, "readiness database check failed");
            AnnounceQueueStatus::unavailable("database_error")
        }
    };
    if let Ok(database) = database_result {
        database.close().await;
    }
    let announce_queue_ready = announce_queue.ready();
    let ready = state_dir_ready
        && database_ready
        && runtime_ready
        && scheduler_ready
        && local_paths_ready
        && intake_ready
        && announce_queue_ready;
    let body = serde_json::json!({
        "status": if ready { "ready" } else { "not_ready" },
        "checks": {
            "stateDir": state_dir_ready,
            "database": database_ready,
            "runtime": runtime_ready,
            "scheduler": scheduler_ready,
            "localPaths": local_paths_ready,
            "intake": intake_ready,
            "durableAnnounceQueue": announce_queue_ready,
        },
        "durableAnnounceQueue": announce_queue.to_json(),
    })
    .to_string();

    crate::api::ApiResponse {
        status: if ready { 200 } else { 503 },
        body,
    }
}

async fn configured_local_paths_ready(config: &RuntimeConfig) -> bool {
    for path in config
        .data_dirs
        .iter()
        .chain(config.link_dirs.iter())
        .chain(config.torrent_dir.iter())
        .chain(config.inject_dir.iter())
    {
        if !tokio::fs::metadata(path)
            .await
            .is_ok_and(|metadata| metadata.is_dir() || metadata.is_file())
        {
            return false;
        }
    }
    true
}

fn api_method(method: &Method) -> ApiMethod {
    match *method {
        Method::GET => ApiMethod::Get,
        Method::POST => ApiMethod::Post,
        _ => ApiMethod::Other,
    }
}

struct ExecutedJob {
    name: JobName,
    completion_last_run: Option<i64>,
    duration_ms: Option<u128>,
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
                duration_ms: None,
                result: None,
            });
            continue;
        }
        let started_at = Instant::now();
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
            duration_ms: job_result
                .as_ref()
                .map(|_result| started_at.elapsed().as_millis()),
            result: job_result,
        });
    }
    executed
}

async fn finish_executed_jobs(
    scheduler: &mut Scheduler,
    database: &AsyncDatabase,
    executed_jobs: Vec<ExecutedJob>,
    metrics: &DaemonMetrics,
) -> crate::Result<()> {
    let mut first_error = None;
    for executed in executed_jobs {
        scheduler.finish_job(executed.name);
        if let Some(result) = executed.result {
            if let Some(duration_ms) = executed.duration_ms {
                metrics.record_job_duration(executed.name, duration_ms);
            }
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
                    metrics.record_job_failure(executed.name);
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
    let span = tracing::info_span!("service job", job = job.name.as_str());
    let _guard = span.enter();
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
    config
        .listen_port
        .map(|port| SocketAddr::new(config.listen_host, port))
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

fn metrics_error(error: prometheus::Error) -> SporosError {
    daemon_error(format!("failed to render Prometheus metrics: {error}"))
}

fn bool_to_i64(value: bool) -> i64 {
    if value { 1 } else { 0 }
}

fn usize_to_i64(value: usize) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn i64_to_u64(value: i64) -> u64 {
    u64::try_from(value.max(0)).unwrap_or(u64::MAX)
}

fn u128_to_usize(value: u128) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

fn display_path(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
struct HttpMetricKey {
    method: String,
    route: String,
    status: u16,
}

#[derive(Debug, Default, Clone, Eq, PartialEq)]
struct HttpMetricValue {
    count: usize,
    latency_ms_total: usize,
}

#[derive(Debug)]
struct DaemonMetrics {
    started_at: Instant,
    http_requests: AtomicUsize,
    http_by_route: StdMutex<BTreeMap<HttpMetricKey, HttpMetricValue>>,
    rss_failures: AtomicUsize,
    search_failures: AtomicUsize,
    update_indexer_caps_failures: AtomicUsize,
    inject_failures: AtomicUsize,
    cleanup_failures: AtomicUsize,
    rss_duration_ms: AtomicUsize,
    search_duration_ms: AtomicUsize,
    update_indexer_caps_duration_ms: AtomicUsize,
    inject_duration_ms: AtomicUsize,
    cleanup_duration_ms: AtomicUsize,
}

impl DaemonMetrics {
    fn record_http_request(&self, method: &str, route: &str, status: u16, latency_ms: u128) {
        self.http_requests.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut metrics) = self.http_by_route.lock() {
            let value = metrics
                .entry(HttpMetricKey {
                    method: method.to_owned(),
                    route: route.to_owned(),
                    status,
                })
                .or_default();
            value.count = value.count.saturating_add(1);
            value.latency_ms_total = value
                .latency_ms_total
                .saturating_add(u128_to_usize(latency_ms));
        }
    }

    fn http_metrics(&self) -> Vec<(HttpMetricKey, HttpMetricValue)> {
        self.http_by_route
            .lock()
            .map(|metrics| {
                metrics
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    fn record_job_failure(&self, name: JobName) {
        self.job_failure_counter(name)
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_job_duration(&self, name: JobName, duration_ms: u128) {
        self.job_duration_counter(name)
            .fetch_add(u128_to_usize(duration_ms), Ordering::Relaxed);
    }

    fn job_failures(&self, name: JobName) -> usize {
        self.job_failure_counter(name).load(Ordering::Relaxed)
    }

    fn job_duration_ms(&self, name: JobName) -> usize {
        self.job_duration_counter(name).load(Ordering::Relaxed)
    }

    fn job_failure_counter(&self, name: JobName) -> &AtomicUsize {
        match name {
            JobName::Rss => &self.rss_failures,
            JobName::Search => &self.search_failures,
            JobName::UpdateIndexerCaps => &self.update_indexer_caps_failures,
            JobName::Inject => &self.inject_failures,
            JobName::Cleanup => &self.cleanup_failures,
        }
    }

    fn job_duration_counter(&self, name: JobName) -> &AtomicUsize {
        match name {
            JobName::Rss => &self.rss_duration_ms,
            JobName::Search => &self.search_duration_ms,
            JobName::UpdateIndexerCaps => &self.update_indexer_caps_duration_ms,
            JobName::Inject => &self.inject_duration_ms,
            JobName::Cleanup => &self.cleanup_duration_ms,
        }
    }
}

impl Default for DaemonMetrics {
    fn default() -> Self {
        Self {
            started_at: Instant::now(),
            http_requests: AtomicUsize::new(0),
            http_by_route: StdMutex::new(BTreeMap::new()),
            rss_failures: AtomicUsize::new(0),
            search_failures: AtomicUsize::new(0),
            update_indexer_caps_failures: AtomicUsize::new(0),
            inject_failures: AtomicUsize::new(0),
            cleanup_failures: AtomicUsize::new(0),
            rss_duration_ms: AtomicUsize::new(0),
            search_duration_ms: AtomicUsize::new(0),
            update_indexer_caps_duration_ms: AtomicUsize::new(0),
            inject_duration_ms: AtomicUsize::new(0),
            cleanup_duration_ms: AtomicUsize::new(0),
        }
    }
}

struct DaemonState {
    app_dir: PathBuf,
    config: RuntimeConfig,
    services: Arc<RuntimeServices>,
    scheduler: Mutex<Scheduler>,
    metrics: Arc<DaemonMetrics>,
}

struct RuntimeHandlers<'a> {
    config: &'a RuntimeConfig,
    services: Arc<RuntimeServices>,
    metrics: Arc<DaemonMetrics>,
    async_database: &'a AsyncDatabase,
    scheduler: Option<&'a mut Scheduler>,
    scheduler_snapshot: Option<Vec<ScheduledJob>>,
    scheduler_available: bool,
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

fn announce_work_id(request: &AnnounceRequest) -> String {
    let mut hasher = Sha1::new();
    hasher.update(request.tracker.as_bytes());
    hasher.update([0]);
    hasher.update(request.guid.as_bytes());
    hasher.update([0]);
    hasher.update(request.link.as_bytes());
    hasher.update([0]);
    hasher.update(request.name.as_bytes());
    format!("{:x}", hasher.finalize())
}

#[async_trait]
impl ApiHandlers for RuntimeHandlers<'_> {
    async fn status(&mut self) -> crate::Result<ApiResponse> {
        let state_dir_ready = tokio::fs::metadata(&self.config.state_dir)
            .await
            .is_ok_and(|metadata| metadata.is_dir());
        let database_ready = tokio::fs::metadata(&self.config.database_path)
            .await
            .is_ok_and(|metadata| metadata.is_file());
        let runtime_ready = !self.services.cancellation_token().is_cancelled();
        let announce_queue =
            announce_queue_status(self.config, self.async_database, self.now_millis).await?;
        let announce_queue_ready = announce_queue.ready();
        let mut jobs = Vec::new();
        let mut recent_service_errors = Vec::new();
        if let Some(scheduler) = &self.scheduler_snapshot {
            for job in scheduler {
                let name = job.name.as_str();
                let last_success = self.async_database.read_last_run(name).await?;
                let failure_count = self.metrics.job_failures(job.name);
                if failure_count > 0 {
                    recent_service_errors.push(serde_json::json!({
                        "kind": "job",
                        "job": name,
                        "count": failure_count,
                    }));
                }
                jobs.push(serde_json::json!({
                    "name": name,
                    "enabled": job.enabled,
                    "running": job.is_active,
                    "cadenceMillis": job.cadence_millis,
                    "lastSuccess": last_success,
                    "lastFailure": null,
                    "failureCount": failure_count,
                    "nextDue": last_success.map(|last_run| last_run.saturating_add(job.cadence_millis as i64)),
                }));
            }
        }
        let indexers = self.async_database.indexer_health_rows().await?;
        let degraded_dependencies = indexers
            .iter()
            .filter(|indexer| {
                !indexer.active
                    || indexer
                        .status
                        .as_deref()
                        .is_some_and(|status| status != "OK")
            })
            .map(|indexer| {
                serde_json::json!({
                    "kind": "indexer",
                    "target": indexer.url,
                    "active": indexer.active,
                    "status": indexer.status,
                    "retryAfter": indexer.retry_after,
                })
            })
            .collect::<Vec<_>>();
        let ready = state_dir_ready && database_ready && runtime_ready && announce_queue_ready;
        let body = serde_json::json!({
            "version": crate::VERSION,
            "config": {
                "path": display_path(&self.config.config_path),
            },
            "listener": {
                "enabled": self.config.listen_port.is_some(),
                "host": self.config.listen_host.to_string(),
                "port": self.config.listen_port,
            },
            "state": {
                "stateDir": display_path(&self.config.state_dir),
                "databasePath": display_path(&self.config.database_path),
                "stateDirReady": state_dir_ready,
                "databaseReady": database_ready,
            },
            "ownership": {
                "mode": "single_writer",
                "singleWriter": true,
                "stateLockPath": display_path(&self.config.state_dir.join("sporos.lock")),
                "stateDir": display_path(&self.config.state_dir),
                "databasePath": display_path(&self.config.database_path),
            },
            "readiness": {
                "status": if ready { "ready" } else { "not_ready" },
                "checks": {
                    "stateDir": state_dir_ready,
                    "database": database_ready,
                    "runtime": runtime_ready,
                    "durableAnnounceQueue": announce_queue_ready,
                },
            },
            "scheduler": {
                "available": self.scheduler_available,
                "jobs": jobs,
            },
            "runtime": {
                "durableAnnounceQueue": announce_queue.to_json(),
                "queues": {
                    "jobs": queue_status(&self.services.queues().jobs),
                    "webhooks": queue_status(&self.services.queues().webhooks),
                    "reverseLookup": queue_status(&self.services.queues().reverse_lookup),
                    "injection": queue_status(&self.services.queues().injection),
                    "blockingLocal": queue_status(&self.services.queues().blocking_local),
                    "blockingFilesystem": blocking_status(&self.services.blocking().filesystem),
                    "blockingTorrentIo": blocking_status(&self.services.blocking().torrent_io),
                    "blockingLinking": blocking_status(&self.services.blocking().linking),
                    "blockingMatching": blocking_status(&self.services.blocking().matching),
                },
            },
            "degradedDependencies": degraded_dependencies,
            "recentServiceErrors": recent_service_errors,
        })
        .to_string();
        Ok(ApiResponse { status: 200, body })
    }

    async fn announce(&mut self, request: AnnounceRequest) -> crate::Result<AnnounceAccepted> {
        let work_id = announce_work_id(&request);
        let dedupe_key = work_id.as_str();
        tracing::info!(
            work_id = work_id.as_str(),
            dedupe_key,
            tracker = request.tracker.as_str(),
            name = request.name.as_str(),
            "received announce request"
        );
        if let Some(existing) = self
            .async_database
            .active_announce_work_by_dedupe_key(dedupe_key)
            .await?
        {
            tracing::info!(
                work_id = existing.work_id.as_str(),
                dedupe_key = existing.dedupe_key.as_str(),
                status = existing.status.as_str(),
                "deduped announce request to existing work"
            );
            return Ok(AnnounceAccepted {
                work_id: existing.work_id,
                status: "existing".to_owned(),
            });
        }

        let stats = self
            .async_database
            .announce_queue_stats(self.now_millis)
            .await?;
        if stats.backlog.saturating_add(stats.running)
            >= i64::from(self.config.announce_queue.max_accepted_backlog)
        {
            return Err(daemon_error("announce queue backlog limit reached"));
        }

        let ttl = i64::try_from(self.config.announce_queue.default_ttl).unwrap_or(i64::MAX);
        let accepted = self
            .async_database
            .insert_or_dedupe_announce_work(&AnnounceWorkInsert {
                work_id: &work_id,
                dedupe_key,
                name: &request.name,
                guid: &request.guid,
                link: &request.link,
                tracker: &request.tracker,
                cookie: request.cookie.as_deref(),
                now: self.now_millis,
                expires_at: self.now_millis.saturating_add(ttl),
            })
            .await?;
        let status = if accepted.inserted {
            "queued"
        } else {
            "existing"
        };
        tracing::info!(
            work_id = accepted.work.work_id.as_str(),
            dedupe_key = accepted.work.dedupe_key.as_str(),
            status,
            "accepted announce work"
        );
        Ok(AnnounceAccepted {
            work_id: accepted.work.work_id,
            status: status.to_owned(),
        })
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
    let async_database = AsyncDatabase::open(&config.database_path).await?;
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
        let metrics = DaemonMetrics::default();
        finish_executed_jobs(
            &mut plan.scheduler,
            &async_database,
            executed_jobs,
            &metrics,
        )
        .await?;
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
    use super::{MAX_REQUEST_BODY_BYTES, handle_runtime_request, http_route, run_plan};
    use crate::{
        SporosError,
        api::{ApiMethod, ApiOutcome, ApiRequest, handle_api_request},
        config::{RawAnnounceQueueConfig, RawConfig, RuntimeConfig, TorrentClientConfig},
        domain::{ActionResult, Decision, InjectionResult, SaveResult},
        persistence::{
            AnnounceWorkFinish, AnnounceWorkInsert, AnnounceWorkRecord, AnnounceWorkRetry,
            AnnounceWorkTerminalStatus, AsyncDatabase, Database,
        },
        runtime::{RuntimeServices, RuntimeTaskQueue},
        scheduler::{
            DaemonPlan, JobCheckResult, JobConfigOverride, JobName, ScheduledJob, Scheduler,
        },
    };
    use axum::http::header::CONTENT_TYPE;
    use std::{
        borrow::Cow,
        collections::BTreeMap,
        path::Path,
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
                listen_port: Some(None),
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
    async fn shutdown_stops_http_intake_and_runtime_workers() {
        let root = temp_path("daemon-shutdown-http");
        std::fs::create_dir_all(&root).expect("root");
        let database = Database::open_app_dir(&root).expect("database");
        let config = RuntimeConfig::normalize(
            RawConfig {
                listen_host: Some("127.0.0.1".parse().expect("listen host")),
                listen_port: Some(Some(0)),
                ..RawConfig::default()
            },
            &root,
        )
        .expect("config");
        let shutdown = CancellationToken::new();
        let mut plan = DaemonPlan::from_config(&config);

        let run = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            run_plan(&root, &config, &database, &mut plan, shutdown, Some(0)),
        )
        .await
        .expect("service shutdown should complete")
        .expect("run daemon");

        let address = run.listen_addr.expect("listener address");
        assert!(run.serving);
        let stopped = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            tokio::net::TcpStream::connect(address),
        )
        .await;
        assert!(matches!(stopped, Err(_) | Ok(Err(_))));
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
                listen_port: Some(None),
                ..RawConfig::default()
            },
            &root,
        )
        .expect("config");
        let mut plan = DaemonPlan::from_config(&config);
        let async_database = AsyncDatabase::open_app_dir(&root).await.expect("database");
        let mut handlers = super::RuntimeHandlers {
            config: &config,
            services: RuntimeServices::start(CancellationToken::new()),
            metrics: Arc::new(super::DaemonMetrics::default()),
            async_database: &async_database,
            scheduler: Some(&mut plan.scheduler),
            scheduler_snapshot: None,
            scheduler_available: true,
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
                listen_port: Some(None),
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
            config: &config,
            services,
            metrics: Arc::new(super::DaemonMetrics::default()),
            async_database: &async_database,
            scheduler: Some(&mut plan.scheduler),
            scheduler_snapshot: None,
            scheduler_available: true,
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
    async fn api_job_does_not_hold_scheduler_lock_while_workflow_waits() {
        let root = temp_path("daemon-api-job-lock");
        let data_dir = temp_path("daemon-api-job-lock-data");
        std::fs::create_dir_all(&root).expect("root");
        std::fs::create_dir_all(&data_dir).expect("data dir");
        let _database = Database::open_app_dir(&root).expect("database");
        let config = RuntimeConfig::normalize(
            RawConfig {
                api_key: Some("test-api-key-with-24-bytes".to_owned()),
                search_cadence: Some(86_400_000),
                exclude_recent_search: Some(259_200_000),
                exclude_older: Some(518_400_000),
                data_dirs: vec![data_dir.clone()],
                ..RawConfig::default()
            },
            &root,
        )
        .expect("config");
        let scheduler = DaemonPlan::from_config(&config).scheduler;
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
        let state = Arc::new(super::DaemonState {
            app_dir: root.clone(),
            config,
            services,
            scheduler: Mutex::new(scheduler),
            metrics: Arc::new(super::DaemonMetrics::default()),
        });
        let request = tokio::spawn(handle_runtime_request(
            Arc::clone(&state),
            ApiRequest::new(
                ApiMethod::Post,
                "/api/job?apikey=test-api-key-with-24-bytes",
                BTreeMap::new(),
                r#"{"name":"search"}"#,
            ),
        ));

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if state.services.blocking().matching.stats().enqueued > 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("search workflow should be queued behind held matching work");
        assert_eq!(state.services.blocking().matching.stats().enqueued, 2);
        let scheduler = state
            .scheduler
            .try_lock()
            .expect("scheduler lock should be free while job body waits");
        assert!(
            scheduler
                .jobs()
                .iter()
                .any(|job| job.name == JobName::Search && job.is_active)
        );
        drop(scheduler);

        state.services.shutdown().await;
        let response = request
            .await
            .expect("request task joins")
            .expect("job request");
        assert_eq!(response.status, 200);
        let _release = release_sender.send(());
        let _held = held_matching.await.expect("held matching joins");
        let _cleanup = std::fs::remove_dir_all(root);
        let _cleanup = std::fs::remove_dir_all(data_dir);
    }

    #[tokio::test]
    async fn axum_request_state_routes_ping() {
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
            metrics: Arc::new(super::DaemonMetrics::default()),
        });

        let ping = handle_runtime_request(
            Arc::clone(&state),
            ApiRequest::new(ApiMethod::Get, "/api/ping", BTreeMap::new(), ""),
        )
        .await
        .expect("ping");

        assert_eq!(ping.status, 200);
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
            metrics: Arc::new(super::DaemonMetrics::default()),
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

        assert_eq!(livez.status, 200);
        assert_eq!(livez.body, r#"{"status":"live"}"#);
        assert_eq!(readyz.status, 200);
        assert!(readyz.body.contains(r#""status":"ready""#));
        assert!(readyz.body.contains(r#""durableAnnounceQueue":true"#));
        state.services.shutdown().await;
        let _cleanup = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn metrics_endpoint_exports_runtime_jobs_and_indexers() {
        let root = temp_path("daemon-metrics");
        std::fs::create_dir_all(&root).expect("root");
        let database = Database::open_app_dir(&root).expect("database");
        database.set_api_key("secret").expect("api key");
        database
            .sync_indexers([("https://indexer.example/api", "indexer-secret")])
            .expect("indexer sync");
        let indexer_id = database
            .indexer_id("https://indexer.example/api")
            .expect("indexer id");
        database
            .set_indexer_status(indexer_id, Some("RATE_LIMITED"), Some(2_000))
            .expect("indexer status");
        let async_database = AsyncDatabase::open_app_dir(&root).await.expect("database");
        async_database
            .write_last_run(JobName::Search.as_str(), 1_000)
            .await
            .expect("last run");
        async_database.close().await;

        let config = RuntimeConfig::normalize(RawConfig::default(), &root).expect("config");
        let mut search = ScheduledJob::new(JobName::Search, 60_000, true);
        search.is_active = true;
        search.runs = 2;
        let services = RuntimeServices::start(CancellationToken::new());
        let _accepted = services
            .queues()
            .jobs
            .try_submit("search", |_shutdown| async {});
        wait_for_finished(&services.queues().jobs).await;
        let metrics = Arc::new(super::DaemonMetrics::default());
        metrics.record_http_request("GET", "/api/status", 200, 12);
        metrics.record_http_request("POST", "/api/job", 409, 3);
        metrics.record_job_failure(JobName::Search);
        metrics.record_job_duration(JobName::Search, 42);
        let state = Arc::new(super::DaemonState {
            app_dir: root.clone(),
            config,
            services,
            scheduler: Mutex::new(Scheduler::new(vec![search])),
            metrics,
        });

        let response = handle_runtime_request(
            Arc::clone(&state),
            ApiRequest::new(ApiMethod::Get, "/metrics", BTreeMap::new(), ""),
        )
        .await
        .expect("metrics");

        assert_eq!(response.status, 200);
        assert!(response.body.contains("sporos_service_info"));
        assert!(response.body.contains("sporos_service_uptime_seconds"));
        assert!(response.body.contains("sporos_http_requests_total 2"));
        assert!(response.body.contains(
            r#"sporos_http_requests_by_route_total{method="GET",route="/api/status",status="200"} 1"#
        ));
        assert!(response.body.contains(
            r#"sporos_http_request_latency_ms_total{method="GET",route="/api/status",status="200"} 12"#
        ));
        assert!(
            response
                .body
                .contains(r#"sporos_runtime_queue_events_total{event="accepted",queue="jobs"} 1"#)
        );
        assert!(
            response
                .body
                .contains(r#"sporos_runtime_queue_depth{queue="jobs"} 0"#)
        );
        assert!(
            response
                .body
                .contains(r#"sporos_runtime_queue_in_flight{queue="jobs"} 0"#)
        );
        assert!(response.body.contains("sporos_announce_queue_enabled 1"));
        assert!(response.body.contains("sporos_announce_queue_backlog 0"));
        assert!(
            response
                .body
                .contains(r#"sporos_announce_queue_attempts_total{result="started"} 0"#)
        );
        assert!(
            response
                .body
                .contains(r#"sporos_announce_queue_outcomes_total{outcome="expired"} 0"#)
        );
        assert!(
            response
                .body
                .contains("sporos_announce_queue_retry_delay_seconds 0")
        );
        assert!(
            response
                .body
                .contains("sporos_announce_queue_expired_total 0")
        );
        assert!(
            response
                .body
                .contains("sporos_announce_queue_breaker_open 0")
        );
        assert!(
            response
                .body
                .contains(r#"sporos_job_runs_total{job="search"} 2"#)
        );
        assert!(
            response
                .body
                .contains(r#"sporos_job_failures_total{job="search"} 1"#)
        );
        assert!(
            response
                .body
                .contains(r#"sporos_job_duration_ms_total{job="search"} 42"#)
        );
        assert!(
            response
                .body
                .contains(r#"sporos_job_last_run_timestamp_seconds{job="search"} 1"#)
        );
        assert!(
            response.body.contains(
                r#"sporos_indexer_rate_limited{indexer="https://indexer.example/api"} 1"#
            )
        );
        state.services.shutdown().await;
        let _cleanup = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn announce_queue_status_and_metrics_use_durable_state() {
        let root = temp_path("daemon-announce-queue-observability");
        std::fs::create_dir_all(&root).expect("root");
        let database = Database::open_app_dir(&root).expect("database");
        database.set_api_key("secret").expect("api key");
        let now = super::now_millis();

        insert_announce_work(
            &database,
            "running",
            now.saturating_sub(9_000),
            now + 60_000,
        );
        let running = database
            .claim_announce_work(now, "worker-a", 60_000, 1)
            .expect("claim running");
        assert_eq!(running.len(), 1);

        insert_announce_work(&database, "retry", now.saturating_sub(8_000), now + 60_000);
        let retrying = database
            .claim_announce_work(now + 1, "worker-a", 60_000, 1)
            .expect("claim retry");
        database
            .schedule_announce_retry(&AnnounceWorkRetry {
                work_id: &retrying[0].work_id,
                now: now + 2,
                next_attempt_at: now + 30_000,
                error_class: Some("workflow_error"),
                error_message: Some("remote dependency failed"),
                outcome_context: Some("retryable_workflow_error"),
            })
            .expect("schedule retry");

        insert_announce_work(
            &database,
            "succeeded",
            now.saturating_sub(7_000),
            now + 60_000,
        );
        let succeeded = database
            .claim_announce_work(now + 3, "worker-a", 60_000, 1)
            .expect("claim succeeded");
        database
            .finish_announce_work(&AnnounceWorkFinish {
                work_id: &succeeded[0].work_id,
                now: now + 4,
                status: AnnounceWorkTerminalStatus::Succeeded,
                error_class: None,
                error_message: None,
                outcome_context: Some("action_completed"),
            })
            .expect("finish succeeded");

        insert_announce_work(
            &database,
            "terminal",
            now.saturating_sub(6_000),
            now + 60_000,
        );
        let terminal = database
            .claim_announce_work(now + 5, "worker-a", 60_000, 1)
            .expect("claim terminal");
        database
            .finish_announce_work(&AnnounceWorkFinish {
                work_id: &terminal[0].work_id,
                now: now + 6,
                status: AnnounceWorkTerminalStatus::TerminalFailed,
                error_class: Some("terminal_decision"),
                error_message: Some("not matchable"),
                outcome_context: Some("file_tree_mismatch"),
            })
            .expect("finish terminal");

        insert_announce_work(&database, "expired", now.saturating_sub(5_000), now - 1);
        let expired = database
            .expire_announce_work(now + 7, 10)
            .expect("expire work");
        assert_eq!(expired.len(), 1);

        insert_announce_work(
            &database,
            "queued",
            now.saturating_sub(10_000),
            now + 60_000,
        );

        let config = RuntimeConfig::normalize(RawConfig::default(), &root).expect("config");
        let state = Arc::new(super::DaemonState {
            app_dir: root.clone(),
            config,
            services: RuntimeServices::start(CancellationToken::new()),
            scheduler: Mutex::new(Scheduler::new(Vec::new())),
            metrics: Arc::new(super::DaemonMetrics::default()),
        });

        let status = handle_runtime_request(
            Arc::clone(&state),
            ApiRequest::new(
                ApiMethod::Get,
                "/api/status?apikey=secret",
                BTreeMap::new(),
                "",
            ),
        )
        .await
        .expect("status");
        let body: serde_json::Value = serde_json::from_str(&status.body).expect("status json");
        let queue = &body["runtime"]["durableAnnounceQueue"];
        assert_eq!(queue["enabled"], true);
        assert_eq!(queue["status"], "ready");
        assert_eq!(queue["backlog"], 2);
        assert_eq!(queue["running"], 1);
        assert!(
            queue["oldestQueuedAgeSeconds"]
                .as_i64()
                .is_some_and(|age| age >= 9)
        );
        assert!(
            queue["retryDelaySeconds"]
                .as_i64()
                .is_some_and(|delay| delay > 0 && delay <= 30)
        );
        assert_eq!(queue["attempts"]["started"], 4);
        assert_eq!(queue["attempts"]["retryScheduled"], 1);
        assert_eq!(queue["attempts"]["exhausted"], 1);
        assert_eq!(queue["outcomes"]["succeeded"], 1);
        assert_eq!(queue["outcomes"]["terminalFailed"], 1);
        assert_eq!(queue["outcomes"]["expired"], 1);
        assert_eq!(queue["expiryCount"], 1);
        assert_eq!(queue["lastError"]["class"], "terminal_decision");
        assert_eq!(queue["lastError"]["context"], "file_tree_mismatch");

        let metrics = handle_runtime_request(
            Arc::clone(&state),
            ApiRequest::new(ApiMethod::Get, "/metrics", BTreeMap::new(), ""),
        )
        .await
        .expect("metrics");
        assert!(metrics.body.contains("sporos_announce_queue_enabled 1"));
        assert!(metrics.body.contains("sporos_announce_queue_backlog 2"));
        assert!(metrics.body.contains("sporos_announce_queue_running 1"));
        assert!(
            metrics
                .body
                .contains(r#"sporos_announce_queue_attempts_total{result="started"} 4"#)
        );
        assert!(
            metrics
                .body
                .contains(r#"sporos_announce_queue_attempts_total{result="retry_scheduled"} 1"#)
        );
        assert!(
            metrics
                .body
                .contains(r#"sporos_announce_queue_attempts_total{result="exhausted"} 1"#)
        );
        assert!(
            metrics
                .body
                .contains(r#"sporos_announce_queue_outcomes_total{outcome="succeeded"} 1"#)
        );
        assert!(
            metrics
                .body
                .contains(r#"sporos_announce_queue_outcomes_total{outcome="terminal_failed"} 1"#)
        );
        assert!(
            metrics
                .body
                .contains(r#"sporos_announce_queue_outcomes_total{outcome="expired"} 1"#)
        );
        assert!(
            metrics
                .body
                .contains("sporos_announce_queue_expired_total 1")
        );

        state.services.shutdown().await;
        let _cleanup = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn explicit_axum_routes_cover_public_endpoints() {
        let root = temp_path("daemon-routes");
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
            metrics: Arc::new(super::DaemonMetrics::default()),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind route test listener");
        let address = listener.local_addr().expect("listener address");
        let shutdown = CancellationToken::new();
        let server = super::serve_http(listener, Arc::clone(&state), shutdown.clone());
        let client = reqwest::Client::new();
        let base_url = format!("http://{address}");

        let livez = client
            .get(format!("{base_url}/_health/livez"))
            .send()
            .await
            .expect("livez response");
        assert_eq!(livez.status(), reqwest::StatusCode::OK);
        assert_eq!(content_type(&livez), Some("application/json"));
        assert_eq!(
            livez.text().await.expect("livez body"),
            r#"{"status":"live"}"#
        );

        let readyz = client
            .get(format!("{base_url}/_health/readyz"))
            .send()
            .await
            .expect("readyz response");
        assert_eq!(readyz.status(), reqwest::StatusCode::OK);
        assert_eq!(content_type(&readyz), Some("application/json"));

        let metrics = client
            .get(format!("{base_url}/metrics"))
            .send()
            .await
            .expect("metrics response");
        assert_eq!(metrics.status(), reqwest::StatusCode::OK);
        assert_eq!(
            metrics
                .headers()
                .get(CONTENT_TYPE)
                .and_then(|value| value.to_str().ok()),
            Some("text/plain; version=0.0.4; charset=utf-8")
        );

        let ping = client
            .get(format!("{base_url}/api/ping"))
            .send()
            .await
            .expect("ping response");
        assert_eq!(ping.status(), reqwest::StatusCode::OK);
        assert_eq!(ping.text().await.expect("ping body"), "OK");

        let unauthorized_status = client
            .get(format!("{base_url}/api/status"))
            .send()
            .await
            .expect("unauthorized status response");
        assert_eq!(
            unauthorized_status.status(),
            reqwest::StatusCode::UNAUTHORIZED
        );
        assert!(
            content_type(&unauthorized_status).is_some_and(|value| value.starts_with("text/plain"))
        );
        assert_eq!(
            unauthorized_status.text().await.expect("unauthorized body"),
            crate::api::AUTH_MESSAGE
        );

        let status = client
            .get(format!("{base_url}/api/status?apikey=secret"))
            .send()
            .await
            .expect("status response");
        assert_eq!(status.status(), reqwest::StatusCode::OK);
        assert_eq!(content_type(&status), Some("application/json"));
        let status_body: serde_json::Value =
            serde_json::from_str(&status.text().await.expect("status body")).expect("status json");
        assert_eq!(status_body["version"], crate::VERSION);

        let header_status = client
            .get(format!("{base_url}/api/status"))
            .header("X-Api-Key", "secret")
            .send()
            .await
            .expect("header status response");
        assert_eq!(header_status.status(), reqwest::StatusCode::OK);
        assert_eq!(content_type(&header_status), Some("application/json"));

        let webhook = client
            .post(format!("{base_url}/api/webhook?apikey=secret"))
            .body("infoHash=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .send()
            .await
            .expect("webhook response");
        assert_eq!(webhook.status(), reqwest::StatusCode::NO_CONTENT);
        assert_eq!(content_type(&webhook), None);
        assert!(webhook.bytes().await.expect("webhook body").is_empty());

        let announce_body = r#"{"name":"Release","guid":"https://idx/t","link":"https://idx/t","tracker":"Tracker"}"#;
        let announce = client
            .post(format!("{base_url}/api/announce?apikey=secret"))
            .body(announce_body)
            .send()
            .await
            .expect("announce response");
        assert_eq!(announce.status(), reqwest::StatusCode::ACCEPTED);
        assert_eq!(content_type(&announce), Some("application/json"));
        let announce_json: serde_json::Value =
            serde_json::from_str(&announce.text().await.expect("announce body"))
                .expect("announce json");
        assert_eq!(announce_json["status"], "queued");
        let work_id = announce_json["workId"].as_str().expect("work id");
        assert_eq!(work_id.len(), 40);
        assert_eq!(
            database
                .announce_queue_stats(2_000)
                .expect("queue stats")
                .backlog,
            1
        );

        let deduped_announce = client
            .post(format!("{base_url}/api/announce?apikey=secret"))
            .body(announce_body)
            .send()
            .await
            .expect("deduped announce response");
        assert_eq!(deduped_announce.status(), reqwest::StatusCode::ACCEPTED);
        let deduped_json: serde_json::Value =
            serde_json::from_str(&deduped_announce.text().await.expect("dedupe body"))
                .expect("dedupe json");
        assert_eq!(deduped_json["workId"], work_id);
        assert_eq!(deduped_json["status"], "existing");
        assert_eq!(
            database
                .announce_queue_stats(2_000)
                .expect("queue stats")
                .backlog,
            1
        );

        for path in ["/api/announce", "/api/webhook", "/api/job"] {
            let response = client
                .post(format!("{base_url}{path}?apikey=secret"))
                .body("{")
                .send()
                .await
                .unwrap_or_else(|error| panic!("{path} response: {error}"));

            assert_eq!(
                response.status(),
                reqwest::StatusCode::BAD_REQUEST,
                "{path} should route to API validation"
            );
            assert!(
                content_type(&response).is_some_and(|value| value.starts_with("text/plain")),
                "{path} should return text errors"
            );
        }

        let unsupported_method = client
            .get(format!("{base_url}/api/announce?apikey=secret"))
            .send()
            .await
            .expect("method response");
        assert_eq!(
            unsupported_method.status(),
            reqwest::StatusCode::METHOD_NOT_ALLOWED
        );
        assert!(
            content_type(&unsupported_method).is_some_and(|value| value.starts_with("text/plain"))
        );
        assert_eq!(
            unsupported_method.text().await.expect("method body"),
            "Method Not Allowed"
        );

        let not_found = client
            .get(format!("{base_url}/missing"))
            .send()
            .await
            .expect("not found response");
        assert_eq!(not_found.status(), reqwest::StatusCode::NOT_FOUND);
        assert!(content_type(&not_found).is_some_and(|value| value.starts_with("text/plain")));
        assert_eq!(not_found.text().await.expect("not found body"), "Not Found");

        let http_metrics = state.metrics.http_metrics();
        assert!(http_metrics.iter().any(|(key, value)| {
            key.method == "GET"
                && key.route == "/api/status"
                && key.status == 401
                && value.count == 1
        }));
        assert!(http_metrics.iter().any(|(key, value)| {
            key.method == "GET" && key.route == "/metrics" && key.status == 200 && value.count == 1
        }));

        shutdown.cancel();
        server
            .await
            .expect("server task joins")
            .expect("server stops");
        state.services.shutdown().await;
        let _cleanup = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn announce_enqueue_rejects_new_work_over_capacity() {
        let root = temp_path("daemon-announce-capacity");
        std::fs::create_dir_all(&root).expect("root");
        let database = Database::open_app_dir(&root).expect("database");
        database.set_api_key("secret").expect("api key");
        let config = RuntimeConfig::normalize(
            RawConfig {
                announce_queue: RawAnnounceQueueConfig {
                    max_accepted_backlog: Some(1),
                    ..Default::default()
                },
                ..RawConfig::default()
            },
            &root,
        )
        .expect("config");
        let state = Arc::new(super::DaemonState {
            app_dir: root.clone(),
            config,
            services: RuntimeServices::start(CancellationToken::new()),
            scheduler: Mutex::new(Scheduler::new(Vec::new())),
            metrics: Arc::new(super::DaemonMetrics::default()),
        });

        let first = handle_runtime_request(
            Arc::clone(&state),
            ApiRequest::new(
                ApiMethod::Post,
                "/api/announce?apikey=secret",
                BTreeMap::new(),
                r#"{"name":"Release One","guid":"https://idx/1","link":"https://idx/1","tracker":"Tracker"}"#,
            ),
        )
        .await
        .expect("first announce");
        assert_eq!(first.status, 202);

        let second = handle_runtime_request(
            Arc::clone(&state),
            ApiRequest::new(
                ApiMethod::Post,
                "/api/announce?apikey=secret",
                BTreeMap::new(),
                r#"{"name":"Release Two","guid":"https://idx/2","link":"https://idx/2","tracker":"Tracker"}"#,
            ),
        )
        .await
        .expect_err("second announce");
        assert!(second.to_string().contains("backlog limit"));
        assert_eq!(
            database
                .announce_queue_stats(2_000)
                .expect("queue stats")
                .backlog,
            1
        );
        state.services.shutdown().await;
        let _cleanup = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn announce_workers_claim_and_retry_work() {
        let root = temp_path("daemon-announce-workers");
        std::fs::create_dir_all(&root).expect("root");
        let database = Database::open_app_dir(&root).expect("database");
        let now = super::now_millis();
        database
            .insert_or_dedupe_announce_work(&AnnounceWorkInsert {
                work_id: "work-1",
                dedupe_key: "dedupe-1",
                name: "Release",
                guid: "https://idx/t",
                link: "https://idx/t",
                tracker: "Tracker",
                cookie: None,
                now,
                expires_at: now.saturating_add(60_000),
            })
            .expect("enqueue");
        let config = RuntimeConfig::normalize(
            RawConfig {
                announce_queue: RawAnnounceQueueConfig {
                    worker_concurrency: Some(1),
                    retry_delay_min: Some(60_000),
                    ..Default::default()
                },
                ..RawConfig::default()
            },
            &root,
        )
        .expect("config");
        let services = RuntimeServices::start(CancellationToken::new());
        let workers = super::start_announce_workers(
            &root,
            &config,
            Arc::clone(&services),
            CancellationToken::new(),
            now,
        )
        .await
        .expect("workers");
        assert_eq!(workers.handles.len(), 1);

        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let stats = database.announce_queue_stats(10_000).expect("stats");
                if stats.total_attempts == 1 {
                    assert_eq!(stats.backlog, 1);
                    assert_eq!(stats.running, 0);
                    assert_eq!(stats.last_error_class.as_deref(), Some("workflow_error"));
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("worker retry observed");

        workers.shutdown().await.expect("workers stop");
        services.shutdown().await;
        let _cleanup = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn durable_announce_queue_survives_restart_and_reports_retry() {
        let root = temp_path("daemon-announce-queue-e2e");
        std::fs::create_dir_all(&root).expect("root");
        let database = Database::open_app_dir(&root).expect("database");
        database.set_api_key("secret").expect("api key");
        let config = RuntimeConfig::normalize(
            RawConfig {
                announce_queue: RawAnnounceQueueConfig {
                    worker_concurrency: Some(1),
                    retry_delay_min: Some(60_000),
                    ..Default::default()
                },
                ..RawConfig::default()
            },
            &root,
        )
        .expect("config");
        let state = Arc::new(super::DaemonState {
            app_dir: root.clone(),
            config: config.clone(),
            services: RuntimeServices::start(CancellationToken::new()),
            scheduler: Mutex::new(Scheduler::new(Vec::new())),
            metrics: Arc::new(super::DaemonMetrics::default()),
        });

        let accepted = handle_runtime_request(
            Arc::clone(&state),
            ApiRequest::new(
                ApiMethod::Post,
                "/api/announce?apikey=secret",
                BTreeMap::new(),
                r#"{"name":"Restarted Release","guid":"https://idx/restart","link":"https://idx/restart","tracker":"Tracker"}"#,
            ),
        )
        .await
        .expect("announce accepted");
        assert_eq!(accepted.status, 202);
        let accepted_body: serde_json::Value =
            serde_json::from_str(&accepted.body).expect("accepted json");
        assert_eq!(accepted_body["status"], "queued");
        assert_eq!(
            database
                .announce_queue_stats(super::now_millis())
                .expect("queued stats")
                .backlog,
            1
        );
        state.services.shutdown().await;

        let restart_database = Database::open_app_dir(&root).expect("reopened database");
        assert_eq!(
            restart_database
                .announce_queue_stats(super::now_millis())
                .expect("persisted stats")
                .backlog,
            1
        );
        let now = super::now_millis();
        insert_announce_work(
            &restart_database,
            "expired-before-restart",
            now.saturating_sub(120_000),
            now - 1,
        );

        let services = RuntimeServices::start(CancellationToken::new());
        let workers = super::start_announce_workers(
            &root,
            &config,
            Arc::clone(&services),
            CancellationToken::new(),
            now,
        )
        .await
        .expect("workers");

        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let stats = restart_database
                    .announce_queue_stats(super::now_millis())
                    .expect("retry stats");
                if stats.total_attempts == 1 && stats.retry_scheduled == 1 && stats.expired == 1 {
                    assert_eq!(stats.backlog, 1);
                    assert_eq!(stats.running, 0);
                    assert_eq!(stats.last_error_class.as_deref(), Some("workflow_error"));
                    assert_eq!(
                        stats.last_outcome_context.as_deref(),
                        Some("retryable_workflow_error")
                    );
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("worker retry and expiry observed");

        let observed_state = Arc::new(super::DaemonState {
            app_dir: root.clone(),
            config,
            services: Arc::clone(&services),
            scheduler: Mutex::new(Scheduler::new(Vec::new())),
            metrics: Arc::new(super::DaemonMetrics::default()),
        });
        let status = handle_runtime_request(
            Arc::clone(&observed_state),
            ApiRequest::new(
                ApiMethod::Get,
                "/api/status?apikey=secret",
                BTreeMap::new(),
                "",
            ),
        )
        .await
        .expect("status");
        let status_body: serde_json::Value =
            serde_json::from_str(&status.body).expect("status json");
        let queue = &status_body["runtime"]["durableAnnounceQueue"];
        assert_eq!(queue["attempts"]["started"], 1);
        assert_eq!(queue["attempts"]["retryScheduled"], 1);
        assert_eq!(queue["outcomes"]["expired"], 1);
        assert_eq!(queue["lastError"]["class"], "workflow_error");
        assert_eq!(queue["lastError"]["context"], "retryable_workflow_error");

        let metrics = handle_runtime_request(
            Arc::clone(&observed_state),
            ApiRequest::new(ApiMethod::Get, "/metrics", BTreeMap::new(), ""),
        )
        .await
        .expect("metrics");
        assert!(
            metrics
                .body
                .contains(r#"sporos_announce_queue_attempts_total{result="started"} 1"#)
        );
        assert!(
            metrics
                .body
                .contains(r#"sporos_announce_queue_attempts_total{result="retry_scheduled"} 1"#)
        );
        assert!(
            metrics
                .body
                .contains(r#"sporos_announce_queue_outcomes_total{outcome="expired"} 1"#)
        );

        workers.shutdown().await.expect("workers stop");
        services.shutdown().await;
        let _cleanup = std::fs::remove_dir_all(root);
    }

    #[test]
    fn announce_work_outcome_classification_covers_queue_states() {
        let config = RuntimeConfig::normalize(
            RawConfig {
                announce_queue: RawAnnounceQueueConfig {
                    retry_delay_min: Some(5_000),
                    ..Default::default()
                },
                ..RawConfig::default()
            },
            Path::new("/state"),
        )
        .expect("config");
        let work = announce_work_record(20_000);

        assert_eq!(
            super::classify_announce_work_result(
                Ok(Some(ApiOutcome {
                    decision: Decision::Match,
                    action_result: Some(ActionResult::Save(SaveResult::Saved)),
                })),
                &work,
                10_000,
                &config,
            ),
            super::AnnounceWorkExecution::Succeeded {
                context: "action_completed"
            }
        );
        assert_eq!(
            super::classify_announce_work_result(
                Ok(Some(ApiOutcome {
                    decision: Decision::Match,
                    action_result: Some(ActionResult::Injection(
                        InjectionResult::TorrentNotComplete
                    )),
                })),
                &work,
                10_000,
                &config,
            ),
            super::AnnounceWorkExecution::Waiting {
                next_attempt_at: 15_000,
                context: "waiting_source_torrent_incomplete"
            }
        );
        assert_eq!(
            super::classify_announce_work_result(
                Err(SporosError::Operation {
                    message: Cow::Borrowed("temporary tracker failure"),
                }),
                &work,
                10_000,
                &config,
            ),
            super::AnnounceWorkExecution::Retryable {
                next_attempt_at: 15_000,
                error_class: "workflow_error",
                error_message: "operation error: temporary tracker failure".to_owned(),
                context: "retryable_workflow_error",
            }
        );
        assert_eq!(
            super::classify_announce_work_result(
                Ok(Some(ApiOutcome {
                    decision: Decision::SameInfoHash,
                    action_result: None,
                })),
                &work,
                10_000,
                &config,
            ),
            super::AnnounceWorkExecution::TerminalFailed {
                error_class: "already_present",
                error_message: None,
                context: "already_present",
            }
        );
        assert_eq!(
            super::classify_announce_work_result(
                Ok(Some(ApiOutcome {
                    decision: Decision::BlockedRelease,
                    action_result: None,
                })),
                &work,
                10_000,
                &config,
            ),
            super::AnnounceWorkExecution::TerminalFailed {
                error_class: "terminal_decision",
                error_message: None,
                context: "blocked_release",
            }
        );
        assert_eq!(
            super::classify_announce_work_result(
                Ok(Some(ApiOutcome {
                    decision: Decision::RateLimited,
                    action_result: None,
                })),
                &work,
                10_000,
                &config,
            ),
            super::AnnounceWorkExecution::Retryable {
                next_attempt_at: 15_000,
                error_class: "rate_limited",
                error_message: "announce workflow hit a rate limit".to_owned(),
                context: "rate_limited_decision",
            }
        );
        assert_eq!(
            super::classify_announce_work_result(Ok(None), &work, 10_000, &config),
            super::AnnounceWorkExecution::TerminalFailed {
                error_class: "no_match",
                error_message: None,
                context: "no_match",
            }
        );
        assert_eq!(
            super::classify_announce_work_result(Ok(None), &work, 20_000, &config),
            super::AnnounceWorkExecution::Expired {
                context: "expired_during_processing"
            }
        );
    }

    #[test]
    fn announce_retry_policy_grows_bounds_and_preserves_retry_after() {
        let config = RuntimeConfig::normalize(
            RawConfig {
                announce_queue: RawAnnounceQueueConfig {
                    retry_delay_min: Some(5_000),
                    retry_delay_max: Some(20_000),
                    ..Default::default()
                },
                ..RawConfig::default()
            },
            Path::new("/state"),
        )
        .expect("config");

        assert_eq!(
            super::retry_next_attempt_at(100_000, 1, &config, None),
            105_000
        );
        assert_eq!(
            super::retry_next_attempt_at(100_000, 2, &config, None),
            110_000
        );
        assert_eq!(
            super::retry_next_attempt_at(100_000, 4, &config, None),
            120_000
        );
        assert_eq!(
            super::retry_next_attempt_at(100_000, 2, &config, Some(115_000)),
            115_000
        );
        assert_eq!(
            super::retry_next_attempt_at(100_000, 2, &config, Some(200_000)),
            110_000
        );
    }

    #[tokio::test]
    async fn status_reports_runtime_diagnostics() {
        let root = temp_path("daemon-status-diagnostics");
        std::fs::create_dir_all(&root).expect("root");
        let database = Database::open_app_dir(&root).expect("database");
        database.set_api_key("secret").expect("api key");
        database
            .sync_indexers([("https://indexer.example/api", "indexer-secret")])
            .expect("indexer sync");
        let indexer_id = database
            .indexer_id("https://indexer.example/api")
            .expect("indexer id");
        database
            .set_indexer_status(indexer_id, Some("RATE_LIMITED"), Some(2_000))
            .expect("indexer status");
        let async_database = AsyncDatabase::open_app_dir(&root).await.expect("database");
        async_database
            .write_last_run(JobName::Search.as_str(), 1_000)
            .await
            .expect("last run");
        async_database.close().await;

        let config = RuntimeConfig::normalize(RawConfig::default(), &root).expect("config");
        let mut search = ScheduledJob::new(JobName::Search, 60_000, true);
        search.is_active = true;
        let services = RuntimeServices::start(CancellationToken::new());
        services
            .queues()
            .jobs
            .try_submit("observed", |_shutdown| async {})
            .expect("submit observed job");
        wait_for_finished(&services.queues().jobs).await;
        let metrics = Arc::new(super::DaemonMetrics::default());
        metrics.record_job_failure(JobName::Search);
        let state = Arc::new(super::DaemonState {
            app_dir: root.clone(),
            config,
            services,
            scheduler: Mutex::new(Scheduler::new(vec![search])),
            metrics,
        });

        let response = handle_runtime_request(
            Arc::clone(&state),
            ApiRequest::new(
                ApiMethod::Get,
                "/api/status?apikey=secret",
                BTreeMap::new(),
                "",
            ),
        )
        .await
        .expect("status");

        assert_eq!(response.status, 200);
        let body: serde_json::Value = serde_json::from_str(&response.body).expect("status json");
        assert_eq!(body["version"], crate::VERSION);
        assert_eq!(
            body["config"]["path"].as_str(),
            Some(root.join("config.toml").to_string_lossy().as_ref())
        );
        assert_eq!(
            body["state"]["databasePath"].as_str(),
            Some(root.join("sporos.db").to_string_lossy().as_ref())
        );
        assert_eq!(body["ownership"]["mode"], "single_writer");
        assert_eq!(body["ownership"]["singleWriter"], true);
        assert_eq!(
            body["ownership"]["stateLockPath"].as_str(),
            Some(root.join("sporos.lock").to_string_lossy().as_ref())
        );
        assert_eq!(body["readiness"]["status"], "ready");
        assert_eq!(body["readiness"]["checks"]["durableAnnounceQueue"], true);
        assert_eq!(body["scheduler"]["available"], true);
        assert_eq!(body["scheduler"]["jobs"][0]["name"], "search");
        assert_eq!(body["scheduler"]["jobs"][0]["running"], true);
        assert_eq!(body["scheduler"]["jobs"][0]["lastSuccess"], 1_000);
        assert_eq!(body["scheduler"]["jobs"][0]["failureCount"], 1);
        assert_eq!(body["runtime"]["queues"]["jobs"]["accepted"], 1);
        assert_eq!(body["runtime"]["durableAnnounceQueue"]["status"], "ready");
        assert_eq!(
            body["runtime"]["durableAnnounceQueue"]["oldestQueuedAgeSeconds"],
            serde_json::Value::Null
        );
        assert_eq!(body["runtime"]["durableAnnounceQueue"]["backlog"], 0);
        assert_eq!(
            body["runtime"]["durableAnnounceQueue"]["attempts"]["started"],
            0
        );
        assert_eq!(
            body["runtime"]["durableAnnounceQueue"]["outcomes"]["expired"],
            0
        );
        assert_eq!(
            body["runtime"]["durableAnnounceQueue"]["breaker"]["open"],
            false
        );
        assert_eq!(body["degradedDependencies"][0]["status"], "RATE_LIMITED");
        assert_eq!(body["recentServiceErrors"][0]["job"], "search");
        state.services.shutdown().await;
        let _cleanup = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn metrics_reports_configured_jobs_when_scheduler_is_locked() {
        let root = temp_path("daemon-metrics-locked-scheduler");
        std::fs::create_dir_all(&root).expect("root");
        let database = Database::open_app_dir(&root).expect("database");
        database.set_api_key("secret").expect("api key");
        let config = RuntimeConfig::normalize(RawConfig::default(), &root).expect("config");
        let state = Arc::new(super::DaemonState {
            app_dir: root.clone(),
            config,
            services: RuntimeServices::start(CancellationToken::new()),
            scheduler: Mutex::new(Scheduler::new(Vec::new())),
            metrics: Arc::new(super::DaemonMetrics::default()),
        });
        let guard = state.scheduler.lock().await;

        let response = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            handle_runtime_request(
                Arc::clone(&state),
                ApiRequest::new(ApiMethod::Get, "/metrics", BTreeMap::new(), ""),
            ),
        )
        .await
        .expect("metrics should not wait for scheduler")
        .expect("metrics");

        assert_eq!(response.status, 200);
        assert!(
            response
                .body
                .contains(r#"sporos_job_enabled{job="cleanup"} 1"#)
        );
        drop(guard);
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
            metrics: Arc::new(super::DaemonMetrics::default()),
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
        let body: serde_json::Value = serde_json::from_str(&response.body).expect("status json");
        assert_eq!(body["scheduler"]["available"], false);
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
            metrics: Arc::new(super::DaemonMetrics::default()),
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
                    duration_ms: Some(5),
                    result: Some(Ok(())),
                },
                super::ExecutedJob {
                    name: JobName::Rss,
                    completion_last_run: Some(2_000),
                    duration_ms: Some(7),
                    result: Some(Err(SporosError::configuration("rss failed"))),
                },
            ],
            &super::DaemonMetrics::default(),
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
    fn http_route_labels_are_bounded() {
        assert_eq!(http_route("/api/status"), "/api/status");
        assert_eq!(http_route("/_health/readyz"), "/_health/readyz");
        assert_eq!(http_route("/api/status/extra"), "unmatched");
        assert_eq!(http_route("/secret-token-in-path"), "unmatched");
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

    fn announce_work_record(expires_at: i64) -> AnnounceWorkRecord {
        AnnounceWorkRecord {
            work_id: "work-1".to_owned(),
            dedupe_key: "dedupe-1".to_owned(),
            name: "Release".to_owned(),
            guid: "https://idx/t".to_owned(),
            link: "https://idx/t".to_owned(),
            tracker: "Tracker".to_owned(),
            cookie: None,
            status: "running".to_owned(),
            attempts: 1,
            created_at: 1_000,
            updated_at: 10_000,
            next_attempt_at: 10_000,
            expires_at,
            lease_owner: Some("worker".to_owned()),
            lease_expires_at: Some(30_000),
            last_error_class: None,
            last_error_message: None,
            last_outcome_context: None,
        }
    }

    fn insert_announce_work(database: &Database, suffix: &str, now: i64, expires_at: i64) {
        database
            .insert_or_dedupe_announce_work(&AnnounceWorkInsert {
                work_id: &format!("work-{suffix}"),
                dedupe_key: &format!("dedupe-{suffix}"),
                name: &format!("Release {suffix}"),
                guid: &format!("https://idx/{suffix}"),
                link: &format!("https://idx/{suffix}"),
                tracker: "Tracker",
                cookie: None,
                now,
                expires_at,
            })
            .unwrap_or_else(|error| panic!("insert announce work {suffix}: {error}"));
    }

    fn content_type(response: &reqwest::Response) -> Option<&str> {
        response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
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

    async fn wait_for_finished(queue: &RuntimeTaskQueue) {
        for _attempt in 0..20 {
            if queue.stats().finished > 0 {
                return;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            queue.stats().finished > 0,
            "{} queue should finish work",
            queue.name()
        );
    }
}
