use std::future::Future;
use std::os::unix::fs::DirBuilderExt;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::http::StatusCode;
use duroxide::Client;
use duroxide::runtime::{Runtime, RuntimeOptions};
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::{oneshot, watch};
use tower::limit::ConcurrencyLimitLayer;
use tower_http::timeout::TimeoutLayer;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::Subscriber;
use tracing_subscriber::util::SubscriberInitExt;

use crate::arr::ArrEnricher;
use crate::candidate_workflow::SOURCE_COMPLETED_EVENT;
use crate::config::{Config, LogFormat, Logging};
use crate::data_scan::DataScanExecutor;
use crate::engine::registries_with_limits;
use crate::execution::ExecutionLimits;
use crate::http::{HttpState, router};
use crate::outbox::OutboxDispatcher;
use crate::prowlarr::ProwlarrClient;
use crate::qbit_sync::InventorySynchronizer;
use crate::qbittorrent::{ApiKey, QbittorrentClient, QbittorrentConfigError};
use crate::search::{SearchExecutor, SearchPolicy};
use crate::storage::Storage;

const OUTBOX_INTERVAL: Duration = Duration::from_millis(100);
const PROJECTION_REPAIR_INTERVAL: Duration = Duration::from_secs(5);
const PROJECTION_REPAIR_BATCH_SIZE: usize = 32;

pub fn init_logging(config: &Logging) -> Result<(), AppError> {
    let filter = EnvFilter::try_new(&config.level).map_err(AppError::LogFilter)?;
    match config.format {
        LogFormat::Json => Subscriber::builder()
            .with_env_filter(filter)
            .json()
            .flatten_event(true)
            .with_current_span(false)
            .with_span_list(false)
            .finish()
            .try_init()
            .map_err(AppError::LogSubscriber),
        LogFormat::Pretty => Subscriber::builder()
            .with_env_filter(filter)
            .pretty()
            .finish()
            .try_init()
            .map_err(AppError::LogSubscriber),
    }
}

pub async fn run(config: Config, shutdown: impl Future<Output = ()>) -> Result<(), AppError> {
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(&config.runtime.data_dir)
        .map_err(AppError::CreateDataDirectory)?;
    let storage = Arc::new(
        Storage::open(&config.runtime.lock_path, &config.runtime.database_path)
            .await
            .map_err(AppError::OpenStorage)?,
    );
    let execution = ExecutionLimits::new(&config.limits);
    let qbit_api = config
        .qbittorrent
        .as_ref()
        .map(|settings| {
            let api_key = settings
                .api_key
                .as_ref()
                .map(|api_key| ApiKey::new(api_key.expose()))
                .transpose()?;
            QbittorrentClient::with_timeout(settings.url.clone(), api_key, settings.request_timeout)
        })
        .transpose()
        .map_err(AppError::QbittorrentConfig)?;
    let synchronizer =
        config
            .qbittorrent
            .as_ref()
            .zip(qbit_api.clone())
            .map(|(settings, client)| {
                InventorySynchronizer::new(
                    Arc::clone(&storage),
                    client,
                    settings.inventory_batch_size,
                    settings.database_batch_size,
                    config.limits.internal_channel_capacity,
                )
            });
    let prowlarr_api = config
        .prowlarr
        .as_ref()
        .map(|settings| {
            ProwlarrClient::new(settings, config.matching.max_torrent_bytes)
                .map(|client| client.with_limiter(execution.indexer()))
        })
        .transpose()
        .map_err(|error| AppError::ProwlarrConfig(error.to_string()))?;
    let arr = (!config.arr.is_empty())
        .then(|| ArrEnricher::new(Arc::clone(&storage), &config.arr))
        .transpose()
        .map_err(|error| AppError::ArrConfig(error.to_string()))?;
    let provider = storage.duroxide_provider();
    let client = Client::new(provider.clone());
    OutboxDispatcher::new(&storage, client.clone(), config.limits.outbox_batch_size)
        .run_once(now_ms())
        .await
        .map_err(AppError::InitialOutboxDispatch)?;
    let listener = TcpListener::bind(config.server.bind)
        .await
        .map_err(AppError::Bind)?;
    let address = listener.local_addr().map_err(AppError::LocalAddress)?;
    let search = prowlarr_api.clone().map(|client| {
        let executor = SearchExecutor::new(
            Arc::clone(&storage),
            client,
            SearchPolicy::new(
                config.matching.clone(),
                config.sources.clone(),
                config.injection.clone(),
                config.paths.clone(),
            ),
        )
        .with_limiter(execution.search());
        if let Some(arr) = arr.clone() {
            executor.with_arr(arr)
        } else {
            executor
        }
    });
    let data_scan = (!config.data_roots.is_empty()).then(|| {
        DataScanExecutor::new(Arc::clone(&storage), config.data_roots.clone())
            .with_limiter(execution.filesystem())
    });
    let (activities, orchestrations) = registries_with_limits(
        Arc::clone(&storage),
        qbit_api,
        search,
        data_scan,
        Some(execution),
    );
    let runtime_options = runtime_options(&config.limits);
    let runtime = Runtime::start_with_options(
        provider,
        activities,
        orchestrations,
        runtime_options.clone(),
    )
    .await;
    let state = HttpState::new(Arc::clone(&storage), &config, prowlarr_api.clone());
    let app: Router = router(state.clone(), config.server.admin_body_limit_bytes)
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            config.server.request_timeout,
        ))
        .layer(ConcurrencyLimitLayer::new(config.limits.max_http_requests));

    let (http_stop_tx, http_stop_rx) = oneshot::channel();
    let mut server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = http_stop_rx.await;
            })
            .await
    });
    let (outbox_stop_tx, outbox_stop_rx) = watch::channel(false);
    let (projection_stop_tx, projection_stop_rx) = watch::channel(false);
    let qbit_client = client.clone();
    let projection_client = client.clone();
    let outbox_state = state.clone();
    let outbox_storage = Arc::clone(&storage);
    let outbox_batch_size = config.limits.outbox_batch_size;
    let outbox = tokio::spawn(async move {
        dispatch_loop(
            outbox_storage,
            client,
            outbox_batch_size,
            outbox_state,
            outbox_stop_rx,
        )
        .await;
    });
    let projection_storage = Arc::clone(&storage);
    let projection_repair = tokio::spawn(async move {
        projection_repair_loop(projection_storage, projection_client, projection_stop_rx).await;
    });
    let (qbit_stop_tx, qbit_stop_rx) = watch::channel(false);
    let qbit = synchronizer.map(|synchronizer| {
        let qbit_storage = Arc::clone(&storage);
        let settings = config
            .qbittorrent
            .as_ref()
            .expect("synchronizer has qBittorrent settings");
        let sync_interval = settings.sync_interval;
        let full_reconcile_interval = settings.full_reconcile_interval;
        tokio::spawn(async move {
            qbit_loop(
                synchronizer,
                qbit_storage,
                qbit_client,
                sync_interval,
                full_reconcile_interval,
                qbit_stop_rx,
            )
            .await;
        })
    });
    let (prowlarr_stop_tx, prowlarr_stop_rx) = watch::channel(false);
    let prowlarr = config
        .prowlarr
        .as_ref()
        .zip(prowlarr_api)
        .map(|(settings, client)| {
            let storage = Arc::clone(&storage);
            let refresh_interval = settings.refresh_interval;
            tokio::spawn(async move {
                prowlarr_loop(client, storage, refresh_interval, prowlarr_stop_rx).await;
            })
        });

    state.set_ready(true);
    info!(
        service = "sporos",
        version = env!("CARGO_PKG_VERSION"),
        bind = %address,
        orchestration_concurrency = runtime_options.orchestration_concurrency,
        activity_concurrency = runtime_options.worker_concurrency,
        max_candidate_workflows = config.limits.max_candidate_workflows,
        max_search_workflows = config.limits.max_search_workflows,
        max_indexer_requests = config.limits.max_indexer_requests,
        max_filesystem_operations = config.limits.max_filesystem_operations,
        internal_channel_capacity = config.limits.internal_channel_capacity,
        "service ready"
    );

    tokio::pin!(shutdown);
    let server_result = tokio::select! {
        () = &mut shutdown => None,
        result = &mut server => Some(result),
    };

    state.set_ready(false);
    let _ = outbox_stop_tx.send(true);
    let _ = projection_stop_tx.send(true);
    let _ = qbit_stop_tx.send(true);
    let _ = prowlarr_stop_tx.send(true);
    let _ = http_stop_tx.send(());
    info!(service = "sporos", "shutdown started");

    let grace = config.server.shutdown_grace;
    let runtime_timeout_ms = u64::try_from(grace.as_millis()).unwrap_or(u64::MAX);
    let runtime_shutdown = runtime.shutdown(Some(runtime_timeout_ms));
    let outbox_shutdown = async {
        if tokio::time::timeout(grace, outbox).await.is_err() {
            warn!(
                service = "sporos",
                "outbox dispatcher exceeded shutdown grace"
            );
        }
    };
    let projection_shutdown = async {
        if tokio::time::timeout(grace, projection_repair)
            .await
            .is_err()
        {
            warn!(
                service = "sporos",
                "task projection repair exceeded shutdown grace"
            );
        }
    };
    let qbit_shutdown = async {
        if let Some(mut qbit) = qbit
            && tokio::time::timeout(grace, &mut qbit).await.is_err()
        {
            qbit.abort();
            warn!(
                service = "sporos",
                "qBittorrent observer exceeded shutdown grace"
            );
        }
    };
    let prowlarr_shutdown = async {
        if let Some(mut prowlarr) = prowlarr
            && tokio::time::timeout(grace, &mut prowlarr).await.is_err()
        {
            prowlarr.abort();
            warn!(
                service = "sporos",
                "Prowlarr observer exceeded shutdown grace"
            );
        }
    };
    let server_shutdown = async {
        if server_result.is_none() && tokio::time::timeout(grace, &mut server).await.is_err() {
            server.abort();
            warn!(service = "sporos", "HTTP server exceeded shutdown grace");
        }
    };
    tokio::join!(
        runtime_shutdown,
        outbox_shutdown,
        projection_shutdown,
        qbit_shutdown,
        prowlarr_shutdown,
        server_shutdown
    );

    storage.checkpoint().await.map_err(AppError::Checkpoint)?;
    info!(service = "sporos", "shutdown complete");

    if let Some(result) = server_result {
        result.map_err(AppError::ServerJoin)??;
        return Err(AppError::ServerStopped);
    }
    Ok(())
}

fn runtime_options(limits: &crate::config::Limits) -> RuntimeOptions {
    RuntimeOptions {
        orchestration_concurrency: limits
            .max_candidate_workflows
            .saturating_add(limits.max_search_workflows)
            .saturating_add(2),
        worker_concurrency: limits
            .max_candidate_workflows
            .saturating_add(limits.max_search_workflows)
            .saturating_add(limits.max_filesystem_operations)
            .saturating_add(2),
        ..RuntimeOptions::default()
    }
}

async fn projection_repair_loop(
    storage: Arc<Storage>,
    client: Client,
    mut stop: watch::Receiver<bool>,
) {
    let mut interval = tokio::time::interval(PROJECTION_REPAIR_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() {
                    return;
                }
            }
            _ = interval.tick() => {
                match storage
                    .repair_terminal_task_projections(
                        &client,
                        PROJECTION_REPAIR_BATCH_SIZE,
                        now_ms(),
                    )
                    .await
                {
                    Ok(report) if report.repaired > 0 => info!(
                        service = "sporos",
                        inspected = report.inspected,
                        repaired = report.repaired,
                        "terminal task projections repaired"
                    ),
                    Ok(_) => {}
                    Err(error) => warn!(
                        service = "sporos",
                        error = %error,
                        "task projection repair failed"
                    ),
                }
            }
        }
    }
}

async fn prowlarr_loop(
    client: ProwlarrClient,
    storage: Arc<Storage>,
    refresh_period: Duration,
    mut stop: watch::Receiver<bool>,
) {
    let mut interval = tokio::time::interval(refresh_period);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() {
                    return;
                }
            }
            _ = interval.tick() => {
                match client.indexers().await {
                    Ok(indexers) => match storage.project_indexers(&indexers, now_ms()).await {
                        Ok(()) => info!(service = "sporos", count = indexers.len(), "Prowlarr indexers refreshed"),
                        Err(error) => warn!(service = "sporos", error = %error, "Prowlarr indexer projection failed"),
                    },
                    Err(error) => warn!(service = "sporos", error = %error, "Prowlarr indexer refresh failed"),
                }
            }
        }
    }
}

async fn qbit_loop(
    synchronizer: InventorySynchronizer,
    storage: Arc<Storage>,
    client: Client,
    sync_period: Duration,
    full_period: Duration,
    mut stop: watch::Receiver<bool>,
) {
    let mut contract_validated = false;
    let mut sync_interval = tokio::time::interval(sync_period);
    sync_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut full_interval =
        tokio::time::interval_at(tokio::time::Instant::now() + full_period, full_period);
    full_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() {
                    return;
                }
            }
            _ = sync_interval.tick() => {
                if !contract_validated {
                    match synchronizer.negotiate().await {
                        Ok(versions) => {
                            contract_validated = true;
                            info!(
                                service = "sporos",
                                qbit_application_version = %versions.application,
                                qbit_web_api_version = %versions.web_api,
                                "qBittorrent contract enabled"
                            );
                        }
                        Err(error) => {
                            warn!(service = "sporos", error = %error, "qBittorrent contract unavailable");
                            continue;
                        }
                    }
                }
                match synchronizer.reconcile_requested().await {
                    Ok(true) => {
                        match synchronizer.reconcile(now_ms()).await {
                            Ok(report) => {
                                info!(
                                    service = "sporos",
                                    changed = report.changed,
                                    completions = report.completions.len(),
                                    "requested qBittorrent inventory reconciliation completed"
                                );
                                finish_qbit_report(&synchronizer, &storage, &client, &report).await;
                            }
                            Err(error) => {
                                warn!(service = "sporos", error = %error, "requested qBittorrent inventory reconciliation failed");
                                continue;
                            }
                        }
                    }
                    Ok(false) => {}
                    Err(error) => {
                        warn!(service = "sporos", error = %error, "qBittorrent inventory state unavailable");
                        continue;
                    }
                }
                match synchronizer.sync_once(now_ms()).await {
                    Ok(report) => {
                        if report.changed > 0 || !report.completions.is_empty() {
                            info!(
                                service = "sporos",
                                changed = report.changed,
                                completions = report.completions.len(),
                                full_update = report.full_update,
                                "qBittorrent inventory synchronized"
                            );
                        }
                        finish_qbit_report(&synchronizer, &storage, &client, &report).await;
                    }
                    Err(error) => warn!(service = "sporos", error = %error, "qBittorrent inventory sync failed"),
                }
            }
            _ = full_interval.tick(), if contract_validated => {
                match synchronizer.reconcile(now_ms()).await {
                    Ok(report) => {
                        info!(
                            service = "sporos",
                            changed = report.changed,
                            completions = report.completions.len(),
                            "qBittorrent inventory reconciled"
                        );
                        finish_qbit_report(&synchronizer, &storage, &client, &report).await;
                    }
                    Err(error) => warn!(service = "sporos", error = %error, "qBittorrent inventory reconciliation failed"),
                }
            }
        }
    }
}

async fn finish_qbit_report(
    synchronizer: &InventorySynchronizer,
    storage: &Storage,
    client: &Client,
    report: &crate::qbit_sync::SyncReport,
) {
    if let Err(error) = synchronizer.refresh_manifests(8, now_ms()).await {
        warn!(service = "sporos", error = %error, "qBittorrent manifest refresh failed");
        return;
    }
    for completion in &report.completions {
        let instances = match storage
            .waiting_candidate_instances(completion.source_id)
            .await
        {
            Ok(instances) => instances,
            Err(error) => {
                warn!(service = "sporos", error = %error, "candidate completion waiters unavailable");
                continue;
            }
        };
        for instance in instances {
            if let Err(error) = client
                .enqueue_event(&instance, SOURCE_COMPLETED_EVENT, "{}")
                .await
            {
                // The durable reconciliation timer remains authoritative if advisory
                // completion delivery is temporarily unavailable.
                warn!(service = "sporos", instance, error = %error, "candidate completion signal failed");
            }
        }
    }
}

async fn dispatch_loop(
    storage: Arc<Storage>,
    client: Client,
    batch_size: usize,
    readiness: HttpState,
    mut stop: watch::Receiver<bool>,
) {
    let mut interval = tokio::time::interval(OUTBOX_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() {
                    return;
                }
            }
            _ = interval.tick() => {
                match OutboxDispatcher::new(&storage, client.clone(), batch_size)
                    .run_once(now_ms())
                    .await
                {
                    Ok(report) if report.claimed > 0 => info!(
                        service = "sporos",
                        claimed = report.claimed,
                        dispatched = report.dispatched,
                        retrying = report.retrying,
                        permanently_failed = report.permanently_failed,
                        "outbox batch processed"
                    ),
                    Ok(_) => {}
                    Err(error) => {
                        readiness.set_ready(false);
                        error!(service = "sporos", error = %error, "outbox dispatcher unavailable");
                    }
                }
            }
        }
    }
}

fn now_ms() -> i64 {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO);
    i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX)
}

#[derive(Debug, Error)]
pub enum AppError {
    #[error("invalid logging filter")]
    LogFilter(#[source] tracing_subscriber::filter::ParseError),
    #[error("failed to install logging subscriber")]
    LogSubscriber(#[source] tracing_subscriber::util::TryInitError),
    #[error("failed to create the data directory")]
    CreateDataDirectory(#[source] std::io::Error),
    #[error("failed to open service storage")]
    OpenStorage(#[source] crate::storage::StorageOpenError),
    #[error("initial outbox delivery check failed")]
    InitialOutboxDispatch(#[source] crate::outbox::DispatchError),
    #[error("invalid qBittorrent client configuration")]
    QbittorrentConfig(#[source] QbittorrentConfigError),
    #[error("invalid Prowlarr client configuration")]
    ProwlarrConfig(String),
    #[error("invalid Arr client configuration: {0}")]
    ArrConfig(String),
    #[error("failed to bind the HTTP listener")]
    Bind(#[source] std::io::Error),
    #[error("failed to inspect the HTTP listener")]
    LocalAddress(#[source] std::io::Error),
    #[error("HTTP server task failed")]
    ServerJoin(#[source] tokio::task::JoinError),
    #[error("HTTP server failed")]
    Server(#[from] std::io::Error),
    #[error("HTTP server stopped before shutdown was requested")]
    ServerStopped,
    #[error("failed to checkpoint SQLite during shutdown")]
    Checkpoint(#[source] sqlx::Error),
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::config::{Auth, Limits, Metrics, Runtime as RuntimeConfig, Server};

    #[tokio::test]
    async fn releases_storage_after_graceful_shutdown() {
        let directory = TempDir::new().expect("create temporary directory");
        let config = Config {
            server: Server {
                bind: "127.0.0.1:0".parse().expect("parse test bind"),
                shutdown_grace: Duration::from_millis(50),
                ..Server::default()
            },
            auth: Auth { api_key: None },
            runtime: RuntimeConfig {
                data_dir: directory.path().to_owned(),
                database_path: directory.path().join("sporos.db"),
                lock_path: directory.path().join("sporos.lock"),
                lock_wait: Duration::ZERO,
            },
            limits: Limits::default(),
            logging: Logging::default(),
            metrics: Metrics::default(),
            qbittorrent: None,
            prowlarr: None,
            arr: Vec::new(),
            sources: crate::config::SourceFilters::default(),
            matching: crate::config::Matching::default(),
            injection: crate::config::Injection::default(),
            paths: crate::config::Paths {
                link_root: directory.path().join("links"),
                rewrite: Vec::new(),
            },
            data_roots: Default::default(),
        };
        let lock_path = config.runtime.lock_path.clone();
        let database_path = config.runtime.database_path.clone();

        run(config, tokio::time::sleep(Duration::from_millis(20)))
            .await
            .expect("run service lifecycle");

        Storage::open(lock_path, database_path)
            .await
            .expect("reacquire storage after shutdown");
    }
}
