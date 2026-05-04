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
    extract::{ConnectInfo, DefaultBodyLimit, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header::CONTENT_TYPE},
    response::{IntoResponse, Response},
    routing::any,
};
use prometheus::{
    Encoder, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry, TextEncoder,
};
use tokio::{net::TcpListener, sync::Mutex, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::{
    SporosError,
    api::{
        AnnounceRequest, ApiHandlers, ApiMethod, ApiOutcome, ApiRequest, ApiResponse, JobRequest,
        JobResponse, WebhookRequest, handle_api_request,
    },
    config::RuntimeConfig,
    persistence::{AsyncDatabase, Database},
    runtime::{RuntimeBlockingExecutor, RuntimeServices, RuntimeTaskQueue},
    scheduler::{DaemonPlan, DaemonRun, JobConfigOverride, JobName, ScheduledJob, Scheduler},
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
    runtime_services.shutdown().await;
    async_database.close().await;
    tracing::info!(
        elapsed_ms = shutdown_started.elapsed().as_millis(),
        "service shutdown complete"
    );
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
    let started = Instant::now();
    let path = uri.path().to_owned();
    let route = http_route(&path);
    let method_label = method.as_str().to_owned();
    let remote_addr = remote_addr.to_string();
    let request = ApiRequest {
        method: api_method(&method),
        path,
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
        remote_addr: Some(remote_addr.clone()),
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
    let latency_ms = started.elapsed().as_millis();
    state
        .metrics
        .record_http_request(&method_label, route, response.status, latency_ms);
    tracing::info!(
        http.method = %method_label,
        http.route = route,
        http.status_code = response.status,
        http.latency_ms = latency_ms,
        net.peer.addr = %remote_addr,
        "http request completed"
    );
    let status = StatusCode::from_u16(response.status).unwrap_or(StatusCode::OK);
    let mut response = (status, response.body).into_response();
    if route == "/metrics" && status == StatusCode::OK {
        response.headers_mut().insert(
            CONTENT_TYPE,
            HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
        );
    }
    response
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
    let api_key =
        crate::operations::api_key_async(&async_database, state.config.api_key.as_deref()).await?;
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
        app_dir: &state.app_dir,
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
    let database_ready = match AsyncDatabase::open(&state.config.database_path).await {
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
    let scheduler_ready = state.scheduler.try_lock().is_ok();
    let local_paths_ready = configured_local_paths_ready(&state.config).await;
    let intake_ready = state.config.listen_port.is_some() && runtime_ready;
    let ready = state_dir_ready
        && database_ready
        && runtime_ready
        && scheduler_ready
        && local_paths_ready
        && intake_ready;
    let body = serde_json::json!({
        "status": if ready { "ready" } else { "not_ready" },
        "checks": {
            "stateDir": state_dir_ready,
            "database": database_ready,
            "runtime": runtime_ready,
            "scheduler": scheduler_ready,
            "localPaths": local_paths_ready,
            "intake": intake_ready,
        },
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

fn parse_query(query: &str) -> BTreeMap<String, String> {
    url::form_urlencoded::parse(query.as_bytes())
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect()
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
    app_dir: &'a Path,
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
        let ready = state_dir_ready && database_ready && runtime_ready;
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
            "readiness": {
                "status": if ready { "ready" } else { "not_ready" },
                "checks": {
                    "stateDir": state_dir_ready,
                    "database": database_ready,
                    "runtime": runtime_ready,
                },
            },
            "scheduler": {
                "available": self.scheduler_available,
                "jobs": jobs,
            },
            "runtime": {
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
    use super::{
        MAX_REQUEST_BODY_BYTES, handle_axum_request, handle_runtime_request, http_route, run_plan,
    };
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
    use axum::body::Bytes;
    use axum::extract::{ConnectInfo, State};
    use axum::http::{HeaderMap, HeaderValue, Method, Uri, header::CONTENT_TYPE};
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
            metrics: Arc::new(super::DaemonMetrics::default()),
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
        let axum_response = handle_axum_request(
            State(Arc::clone(&state)),
            ConnectInfo("127.0.0.1:12345".parse().expect("remote addr")),
            Method::GET,
            Uri::from_static("/metrics"),
            HeaderMap::new(),
            Bytes::new(),
        )
        .await;
        assert_eq!(
            axum_response.headers().get(CONTENT_TYPE),
            Some(&HeaderValue::from_static(
                "text/plain; version=0.0.4; charset=utf-8"
            ))
        );
        state.services.shutdown().await;
        let _cleanup = std::fs::remove_dir_all(root);
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
        assert_eq!(body["readiness"]["status"], "ready");
        assert_eq!(body["scheduler"]["available"], true);
        assert_eq!(body["scheduler"]["jobs"][0]["name"], "search");
        assert_eq!(body["scheduler"]["jobs"][0]["running"], true);
        assert_eq!(body["scheduler"]["jobs"][0]["lastSuccess"], 1_000);
        assert_eq!(body["scheduler"]["jobs"][0]["failureCount"], 1);
        assert_eq!(body["runtime"]["queues"]["jobs"]["accepted"], 1);
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
