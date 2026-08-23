use std::future::Future;
use std::os::unix::fs::DirBuilderExt;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::http::StatusCode;
use duroxide::Client;
use duroxide::runtime::Runtime;
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::{oneshot, watch};
use tower::limit::ConcurrencyLimitLayer;
use tower_http::timeout::TimeoutLayer;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::Subscriber;
use tracing_subscriber::util::SubscriberInitExt;

use crate::config::{Config, LogFormat, Logging};
use crate::engine::registries;
use crate::http::{HttpState, router};
use crate::outbox::OutboxDispatcher;
use crate::storage::Storage;

const OUTBOX_INTERVAL: Duration = Duration::from_millis(100);

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
    let (activities, orchestrations) = registries(Arc::clone(&storage));
    let runtime = Runtime::start_with_store(provider, activities, orchestrations).await;
    let state = HttpState::new(Arc::clone(&storage), &config);
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

    state.set_ready(true);
    info!(
        service = "sporos",
        version = env!("CARGO_PKG_VERSION"),
        bind = %address,
        "service ready"
    );

    tokio::pin!(shutdown);
    let server_result = tokio::select! {
        () = &mut shutdown => None,
        result = &mut server => Some(result),
    };

    state.set_ready(false);
    let _ = outbox_stop_tx.send(true);
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
    let server_shutdown = async {
        if server_result.is_none() && tokio::time::timeout(grace, &mut server).await.is_err() {
            server.abort();
            warn!(service = "sporos", "HTTP server exceeded shutdown grace");
        }
    };
    tokio::join!(runtime_shutdown, outbox_shutdown, server_shutdown);

    storage.checkpoint().await.map_err(AppError::Checkpoint)?;
    info!(service = "sporos", "shutdown complete");

    if let Some(result) = server_result {
        result.map_err(AppError::ServerJoin)??;
        return Err(AppError::ServerStopped);
    }
    Ok(())
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
            auth: Auth {
                webhook_token: crate::config::Secret::new("webhook"),
                admin_token: crate::config::Secret::new("admin"),
            },
            runtime: RuntimeConfig {
                data_dir: directory.path().to_owned(),
                database_path: directory.path().join("sporos.db"),
                lock_path: directory.path().join("sporos.lock"),
                lock_wait: Duration::ZERO,
            },
            limits: Limits::default(),
            logging: Logging::default(),
            metrics: Metrics::default(),
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
