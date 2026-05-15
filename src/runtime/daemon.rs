use std::fmt;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tracing::warn;

use crate::config::{SporosConfig, validate_server_auth};
use crate::errors::{ConfigError, DatabaseError};
use crate::http::router;
use crate::inventory_refresh::run_inventory_refresh_worker;
use crate::metrics::MetricsRegistry;
use crate::notifications::{NotificationWorker, run_notification_worker};
use crate::runtime::announce_worker::{AnnounceWorkerError, unix_time_ms};
use crate::runtime::app::{AppRuntime, RuntimeReceivers};
use crate::runtime::injection_worker::{InjectionWorker, SavedTorrentRetryConfig};
use crate::runtime::scheduler::PersistedScheduler;
use crate::runtime::shutdown::ShutdownSignal;

const SCHEDULER_TICK_INTERVAL: Duration = Duration::from_secs(1);

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
    let app = router(runtime.state.http.clone());
    let _background = start_background_tasks(runtime).await?;

    axum::serve(listener, app)
        .await
        .map_err(|error| DaemonError::Serve {
            message: error.to_string(),
        })
}

async fn start_background_tasks(runtime: AppRuntime) -> Result<Vec<JoinHandle<()>>, DaemonError> {
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
    handles.push(tokio::spawn(run_scheduler_loop(
        runtime.state.scheduler.clone(),
        runtime.state.shutdown_signal.clone(),
    )));
    handles.push(tokio::spawn(run_inventory_refresh_worker(
        runtime.state.inventory_refresh.clone(),
        inventory_refresh,
    )));
    handles.push(tokio::spawn(run_notification_worker(
        NotificationWorker::new(runtime.state.health.clone(), MetricsRegistry::new()),
        notifications,
    )));
    handles.push(tokio::spawn(run_saved_retry_loop(
        runtime.state.injection_worker.clone(),
        SavedTorrentRetryConfig {
            directories: vec![runtime.state.config.paths.output_dir.clone()],
            ..SavedTorrentRetryConfig::default()
        },
        runtime.state.saved_retry_interval,
        runtime.state.shutdown_signal.clone(),
    )));
    handles.push(tokio::spawn(hold_receiver_open(
        announcements,
        runtime.state.shutdown_signal.clone(),
    )));
    handles.push(tokio::spawn(hold_receiver_open(
        searches,
        runtime.state.shutdown_signal.clone(),
    )));
    handles.push(tokio::spawn(hold_receiver_open(
        jobs,
        runtime.state.shutdown_signal.clone(),
    )));
    handles.push(tokio::spawn(hold_receiver_open(
        scheduler,
        runtime.state.shutdown_signal.clone(),
    )));

    Ok(handles)
}

async fn run_scheduler_loop(scheduler: PersistedScheduler, mut shutdown: ShutdownSignal) {
    loop {
        if shutdown.state().phase != crate::runtime::shutdown::ShutdownPhase::Running {
            break;
        }
        if let Err(error) = scheduler.tick(unix_time_ms()).await {
            warn!(error = %error, "scheduler tick failed");
        }

        tokio::select! {
            _state = shutdown.cancelled() => {
                break;
            }
            () = tokio::time::sleep(SCHEDULER_TICK_INTERVAL) => {}
        }
    }
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
        tokio::select! {
            result = worker.retry_saved_torrents(run_config) => {
                match result {
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
            }
            _state = shutdown.cancelled() => {
                break;
            }
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
    async fn serve_rejects_external_bind_without_api_token() {
        let mut config = SporosConfig::default();
        config.server.bind = "0.0.0.0:0".parse().unwrap();

        let error = serve(config).await.unwrap_err();

        assert!(matches!(error, DaemonError::Config { .. }));
        assert!(error.to_string().contains("server.api_token"));
    }

    async fn wait_for_livez(address: std::net::SocketAddr) -> u16 {
        let url = format!("http://{address}/livez");
        for _attempt in 0..20 {
            if let Ok(response) = reqwest::get(&url).await {
                return response.status().as_u16();
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        0
    }
}
