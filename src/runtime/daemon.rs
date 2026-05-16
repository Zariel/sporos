use std::fmt;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::{error, warn};

use crate::config::{SporosConfig, validate_server_auth};
use crate::errors::{ConfigError, DatabaseError};
use crate::http::router;
use crate::inventory_refresh::run_inventory_refresh_worker;
use crate::notifications::{NotificationWorker, run_notification_worker};
use crate::runtime::announce_worker::{AnnounceWorkerError, unix_time_ms};
use crate::runtime::app::{AppRuntime, RuntimeReceivers};
use crate::runtime::injection_worker::{InjectionWorker, SavedTorrentRetryConfig};
use crate::runtime::shutdown::{ShutdownController, ShutdownSignal};

const BACKGROUND_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug)]
pub enum DaemonError {
    Config { source: ConfigError },
    BuildRuntime { source: DatabaseError },
    Bind { message: String },
    Serve { message: String },
    AnnounceStartup { source: AnnounceWorkerError },
}

pub async fn serve(config: SporosConfig) -> Result<(), DaemonError> {
    validate_server_auth(&config).map_err(|source| DaemonError::Config { source })?;
    let bind = config.server.bind;
    let runtime = AppRuntime::build(config)
        .await
        .map_err(|source| DaemonError::BuildRuntime { source })?;
    let listener = TcpListener::bind(bind)
        .await
        .map_err(|error| DaemonError::Bind {
            message: format!("cannot bind {bind}: {error}"),
        })?;

    serve_with_listener(runtime, listener).await
}

pub async fn serve_with_listener(
    runtime: AppRuntime,
    listener: TcpListener,
) -> Result<(), DaemonError> {
    let shutdown = runtime.state.shutdown.clone();
    let shutdown_signal = runtime.state.shutdown_signal.clone();
    let http = runtime.state.http.clone();
    let app = router(http.clone());
    let background = start_background_tasks(runtime).await?;
    let mut readiness = http.readiness();
    readiness.workers_running = true;
    http.set_readiness(readiness);
    let signal_task = tokio::spawn(process_shutdown_signal(shutdown.clone()));

    let serve_result = axum::serve(listener, app)
        .with_graceful_shutdown(wait_for_shutdown(shutdown_signal))
        .await
        .map_err(|error| DaemonError::Serve {
            message: error.to_string(),
        });
    signal_task.abort();
    let _ = shutdown.cancel_now("server stopping");
    let mut readiness = http.readiness();
    readiness.workers_running = false;
    http.set_readiness(readiness);
    stop_background_tasks(background).await;
    serve_result
}

#[derive(Debug)]
struct BackgroundTask {
    name: &'static str,
    handle: JoinHandle<()>,
    abort_on_timeout: bool,
}

impl BackgroundTask {
    fn new(name: &'static str, handle: JoinHandle<()>, abort_on_timeout: bool) -> Self {
        Self {
            name,
            handle,
            abort_on_timeout,
        }
    }
}

async fn start_background_tasks(runtime: AppRuntime) -> Result<Vec<BackgroundTask>, DaemonError> {
    runtime
        .state
        .announce_worker
        .recover_startup(unix_time_ms())
        .await
        .map_err(|source| DaemonError::AnnounceStartup { source })?;

    let RuntimeReceivers {
        announcements,
        searches,
        jobs,
        scheduler,
        inventory_refresh,
        notifications,
    } = runtime.receivers;

    let mut handles = Vec::new();
    handles.push(BackgroundTask::new(
        "inventory-refresh",
        tokio::spawn(run_inventory_refresh_worker(
            runtime.state.inventory_refresh.clone(),
            inventory_refresh,
            runtime.state.shutdown_signal.clone(),
        )),
        true,
    ));
    handles.push(BackgroundTask::new(
        "notifications",
        tokio::spawn(run_notification_worker(
            NotificationWorker::new(runtime.state.health.clone(), runtime.state.metrics.clone()),
            notifications,
        )),
        true,
    ));
    handles.push(BackgroundTask::new(
        "saved-torrent-retry",
        tokio::spawn(run_saved_retry_loop(
            runtime.state.injection_worker.clone(),
            SavedTorrentRetryConfig {
                directories: vec![runtime.state.config.paths.output_dir.clone()],
                ..SavedTorrentRetryConfig::default()
            },
            runtime.state.saved_retry_interval,
            runtime.state.shutdown_signal.clone(),
        )),
        false,
    ));
    handles.push(BackgroundTask::new(
        "announcements-receiver",
        tokio::spawn(hold_receiver_open(
            announcements,
            runtime.state.shutdown_signal.clone(),
        )),
        true,
    ));
    handles.push(BackgroundTask::new(
        "searches-receiver",
        tokio::spawn(hold_receiver_open(
            searches,
            runtime.state.shutdown_signal.clone(),
        )),
        true,
    ));
    handles.push(BackgroundTask::new(
        "jobs-receiver",
        tokio::spawn(hold_receiver_open(
            jobs,
            runtime.state.shutdown_signal.clone(),
        )),
        true,
    ));
    handles.push(BackgroundTask::new(
        "scheduler-receiver",
        tokio::spawn(hold_receiver_open(
            scheduler,
            runtime.state.shutdown_signal.clone(),
        )),
        true,
    ));

    Ok(handles)
}

async fn run_saved_retry_loop(
    worker: InjectionWorker,
    config: SavedTorrentRetryConfig,
    interval: Duration,
    mut shutdown: ShutdownSignal,
) {
    loop {
        if shutdown.state().phase != crate::runtime::shutdown::ShutdownPhase::Running {
            break;
        }

        let mut run_config = config.clone();
        run_config.assessed_at_ms = unix_time_ms();
        match worker
            .retry_saved_torrents_until_shutdown(run_config, &mut shutdown)
            .await
        {
            Ok(summary) => {
                tracing::info!(
                    scanned = summary.scanned,
                    attempted = summary.attempted,
                    injected = summary.injected,
                    failed = summary.failed,
                    kept = summary.kept,
                    deleted = summary.deleted,
                    "saved torrent retry completed"
                );
            }
            Err(error) => warn!(error = ?error, "saved torrent retry failed"),
        }
        if shutdown.state().phase != crate::runtime::shutdown::ShutdownPhase::Running {
            break;
        }

        tokio::select! {
            _state = shutdown.cancelled() => {
                break;
            }
            () = tokio::time::sleep(interval) => {}
        }
    }
}

async fn hold_receiver_open<T>(
    mut receiver: crate::runtime::queue::WorkReceiver<T>,
    mut shutdown: ShutdownSignal,
) where
    T: Send + 'static,
{
    shutdown.cancelled().await;
    receiver.close();
}

async fn wait_for_shutdown(mut shutdown: ShutdownSignal) {
    let _ = shutdown.cancelled().await;
}

async fn process_shutdown_signal(shutdown: ShutdownController) {
    let reason = process_shutdown_reason().await;
    if let Err(error) = shutdown.cancel_now(reason) {
        warn!(error = %error, "failed to publish shutdown signal");
    }
}

async fn process_shutdown_reason() -> &'static str {
    #[cfg(unix)]
    {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut terminate) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => "ctrl-c",
                    _ = terminate.recv() => "sigterm",
                }
            }
            Err(error) => {
                warn!(error = %error, "failed to install sigterm handler");
                let _ = tokio::signal::ctrl_c().await;
                "ctrl-c"
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        "ctrl-c"
    }
}

async fn stop_background_tasks(handles: Vec<BackgroundTask>) {
    stop_background_tasks_with_timeout(handles, BACKGROUND_SHUTDOWN_TIMEOUT).await;
}

async fn stop_background_tasks_with_timeout(mut handles: Vec<BackgroundTask>, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while !handles.is_empty() && Instant::now() < deadline {
        let mut index = 0;
        while index < handles.len() {
            if handles[index].handle.is_finished() {
                let task = handles.swap_remove(index);
                match task.handle.await {
                    Ok(()) => {}
                    Err(error) if error.is_cancelled() => {}
                    Err(error) => {
                        error!(
                            task = task.name,
                            error = %error,
                            "background task failed during shutdown"
                        );
                    }
                }
            } else {
                index += 1;
            }
        }
        if !handles.is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    for task in handles {
        if task.abort_on_timeout {
            task.handle.abort();
            warn!(
                task = task.name,
                "background task did not stop before shutdown timeout; aborted"
            );
        } else {
            warn!(
                task = task.name,
                "background task did not stop before shutdown timeout; waiting for in-flight work"
            );
            match task.handle.await {
                Ok(()) => {}
                Err(error) if error.is_cancelled() => {}
                Err(error) => {
                    error!(
                        task = task.name,
                        error = %error,
                        "background task failed during shutdown"
                    );
                }
            }
        }
    }
}

impl fmt::Display for DaemonError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config { source } => write!(formatter, "{source}"),
            Self::BuildRuntime { source } => write!(formatter, "{source}"),
            Self::Bind { message } | Self::Serve { message } => formatter.write_str(message),
            Self::AnnounceStartup { source } => write!(formatter, "{source}"),
        }
    }
}

impl std::error::Error for DaemonError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Config { source } => Some(source),
            Self::BuildRuntime { source } => Some(source),
            Self::AnnounceStartup { source } => Some(source),
            Self::Bind { .. } | Self::Serve { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::config::SporosConfig;
    use crate::persistence::repository::Repository;

    #[tokio::test]
    async fn serve_runtime_listens_until_aborted() {
        let config = SporosConfig::default();
        let repository = Repository::connect_in_memory().await.unwrap();
        let runtime = AppRuntime::from_repository(config, repository)
            .await
            .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();

        let handle = tokio::spawn(async move { serve_with_listener(runtime, listener).await });
        let response = wait_for_livez(address).await;

        handle.abort();
        assert_eq!(200, response);
    }

    #[tokio::test]
    async fn serve_runtime_stops_on_shutdown_signal() {
        let config = SporosConfig::default();
        let repository = Repository::connect_in_memory().await.unwrap();
        let runtime = AppRuntime::from_repository(config, repository)
            .await
            .unwrap();
        let shutdown = runtime.state.shutdown.clone();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();

        let handle = tokio::spawn(async move { serve_with_listener(runtime, listener).await });
        let response = wait_for_livez(address).await;
        let ready = wait_for_readyz(address).await;
        shutdown.cancel_now("test shutdown").unwrap();
        let result = tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(200, response);
        assert_eq!(200, ready);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn background_shutdown_timeout_is_global() {
        let handles = vec![
            BackgroundTask::new(
                "stuck-a",
                tokio::spawn(async {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                }),
                true,
            ),
            BackgroundTask::new(
                "stuck-b",
                tokio::spawn(async {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                }),
                true,
            ),
        ];
        let started = tokio::time::Instant::now();

        stop_background_tasks_with_timeout(handles, Duration::from_millis(50)).await;

        assert!(started.elapsed() < Duration::from_millis(250));
    }

    #[tokio::test]
    async fn non_abort_background_task_is_awaited_after_timeout() {
        let handles = vec![BackgroundTask::new(
            "finishes-late",
            tokio::spawn(async {
                tokio::time::sleep(Duration::from_millis(75)).await;
            }),
            false,
        )];
        let started = tokio::time::Instant::now();

        stop_background_tasks_with_timeout(handles, Duration::from_millis(10)).await;

        assert!(started.elapsed() >= Duration::from_millis(60));
    }

    #[tokio::test]
    async fn serve_rejects_external_bind_without_api_token() {
        let mut config = SporosConfig::default();
        config.server.bind = "0.0.0.0:0".parse().unwrap();

        let error = serve(config).await.unwrap_err();

        assert!(matches!(error, DaemonError::Config { .. }));
        assert!(error.to_string().contains("server.api_token"));
    }

    async fn wait_for_livez(address: std::net::SocketAddr) -> u16 {
        let url = format!("http://{address}/livez");
        wait_for_status(&url).await
    }

    async fn wait_for_readyz(address: std::net::SocketAddr) -> u16 {
        let url = format!("http://{address}/readyz");
        wait_for_status(&url).await
    }

    async fn wait_for_status(url: &str) -> u16 {
        for _attempt in 0..20 {
            if let Ok(response) = reqwest::get(url).await {
                return response.status().as_u16();
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        0
    }
}
