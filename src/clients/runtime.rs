use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::stream::{self, StreamExt};
use tokio::sync::{Mutex as AsyncMutex, mpsc};

use crate::clients::qbittorrent::{
    QbitAddTorrent, QbitContentLayout, QbitTorrent, QbittorrentClient,
};
use crate::clients::rtorrent::{RtorrentClient, RtorrentDownload};
use crate::clients::{TorrentClientDescriptor, TorrentClientRegistry};
use crate::config::{ConfigTorrentClientKind, TorrentClientConfig};
use crate::domain::{ByteSize, DisplayName, InfoHash, MediaType, TorrentFile};
use crate::errors::{DatabaseError, TorrentClientError};
use crate::inventory_refresh::{
    ClientInventoryItem, ClientInventoryMessage, InventoryRefreshError, InventoryRefreshSummary,
    InventoryRefreshWorker,
};
use crate::metrics::{ExternalOperation, ExternalOutcome, MetricsRegistry};
use crate::runtime::injection_worker::{
    ClientInjectionRequest, ClientInventoryRefreshFuture, ClientResultFuture, InjectionClient,
};
use crate::runtime::shutdown::{ShutdownPhase, ShutdownSignal};

const RUNTIME_CLIENT_INVENTORY_BUFFER: usize = 64;
pub(crate) const CLIENT_INVENTORY_FILE_FETCH_CONCURRENCY: usize = 8;

fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

fn external_result_metric_outcome<T>(result: &Result<T, TorrentClientError>) -> ExternalOutcome {
    match result {
        Ok(_) => ExternalOutcome::Succeeded,
        Err(TorrentClientError::UnsupportedCapability { .. }) => ExternalOutcome::Unsupported,
        Err(_) => ExternalOutcome::Failed,
    }
}

fn inventory_refresh_metric_outcome<T>(
    result: &Result<T, InventoryRefreshError>,
) -> ExternalOutcome {
    match result {
        Ok(_) => ExternalOutcome::Succeeded,
        Err(InventoryRefreshError::Client {
            source: TorrentClientError::UnsupportedCapability { .. },
        }) => ExternalOutcome::Unsupported,
        Err(_) => ExternalOutcome::Failed,
    }
}

enum RuntimeInjectionClientInner {
    Qbittorrent(QbittorrentClient),
    Rtorrent(RtorrentClient),
}

pub(crate) struct RuntimeInjectionClient {
    descriptor: TorrentClientDescriptor,
    inner: RuntimeInjectionClientInner,
    qbit_validated: AsyncMutex<bool>,
    qbit_default_category: Option<String>,
    qbit_default_tags: Vec<String>,
    default_label: String,
    metrics: MetricsRegistry,
}

impl RuntimeInjectionClient {
    pub(crate) fn new(
        name: &str,
        config: &TorrentClientConfig,
        descriptor: TorrentClientDescriptor,
        metrics: MetricsRegistry,
    ) -> Self {
        let timeout = Duration::from_secs(30);
        let inner = match config.kind {
            ConfigTorrentClientKind::Qbittorrent => {
                RuntimeInjectionClientInner::Qbittorrent(QbittorrentClient::new(
                    name,
                    config.url.clone(),
                    config.username.clone(),
                    config
                        .password
                        .as_ref()
                        .map(|password| password.expose_secret().to_owned()),
                    timeout,
                ))
            }
            ConfigTorrentClientKind::Rtorrent => RuntimeInjectionClientInner::Rtorrent(
                RtorrentClient::new(name, config.url.clone(), timeout),
            ),
        };

        Self {
            descriptor,
            inner,
            qbit_validated: AsyncMutex::new(false),
            qbit_default_category: config.default_category.clone(),
            qbit_default_tags: config.default_tags.clone(),
            default_label: config.default_label.clone(),
            metrics,
        }
    }

    async fn ensure_qbittorrent_ready(
        &self,
        client: &QbittorrentClient,
    ) -> Result<(), TorrentClientError> {
        let mut validated = self.qbit_validated.lock().await;
        if !*validated {
            client.validate().await?;
            for tag in &self.qbit_default_tags {
                client.create_tag(tag).await?;
            }
            if let Some(category) = &self.qbit_default_category {
                client
                    .create_category(category, Some(&self.descriptor.default_save_path))
                    .await?;
            }
            *validated = true;
        }
        Ok(())
    }

    async fn refresh_inventory_stream(
        &self,
        worker: &InventoryRefreshWorker,
        shutdown: ShutdownSignal,
    ) -> Result<InventoryRefreshSummary, InventoryRefreshError> {
        if shutdown.state().phase != ShutdownPhase::Running {
            return Err(InventoryRefreshError::Client {
                source: cancelled_client_inventory(&self.descriptor),
            });
        }
        let (sender, receiver) = mpsc::channel(RUNTIME_CLIENT_INVENTORY_BUFFER);
        let refresh =
            worker.refresh_client_inventory_receiver(self.descriptor.host.clone(), receiver);
        let stream = async move {
            match &self.inner {
                RuntimeInjectionClientInner::Qbittorrent(client) => {
                    client
                        .list_inventory_pages_until_shutdown(
                            || wait_for_inventory_shutdown(shutdown.clone()),
                            |page| {
                                let sender = sender.clone();
                                let shutdown = shutdown.clone();
                                async move {
                                    self.stream_qbit_inventory_page(client, page, &sender, shutdown)
                                        .await
                                }
                            },
                        )
                        .await?;
                }
                RuntimeInjectionClientInner::Rtorrent(client) => {
                    client
                        .list_inventory_chunks_until_shutdown(
                            || wait_for_inventory_shutdown(shutdown.clone()),
                            |chunk| {
                                let sender = sender.clone();
                                let shutdown = shutdown.clone();
                                async move {
                                    self.stream_rtorrent_inventory_chunk(
                                        client, chunk, &sender, shutdown,
                                    )
                                    .await
                                }
                            },
                        )
                        .await?;
                }
            }
            sender
                .send(ClientInventoryMessage::Finished)
                .await
                .map_err(|send_error| InventoryRefreshError::InvalidClientInventory {
                    message: format!(
                        "client inventory receiver closed before completion: {send_error}"
                    ),
                })
        };
        let (refresh_result, stream_result) = tokio::join!(refresh, stream);
        match (refresh_result, stream_result) {
            (_, Err(error)) => Err(error),
            (Err(error), Ok(())) => Err(error),
            (Ok(summary), Ok(())) => Ok(summary),
        }
    }

    async fn stream_qbit_inventory_page(
        &self,
        client: &QbittorrentClient,
        page: Vec<QbitTorrent>,
        sender: &mpsc::Sender<ClientInventoryMessage>,
        shutdown: ShutdownSignal,
    ) -> Result<(), TorrentClientError> {
        let descriptor = &self.descriptor;
        let mut items = stream::iter(page.into_iter().map(|torrent| {
            let shutdown = shutdown.clone();
            async move {
                let info_hash = torrent.info_hash(descriptor.name.as_str())?;
                let torrent_name = torrent.name.clone();
                let files = client
                    .fetch_files_until_shutdown(&info_hash, || {
                        wait_for_inventory_shutdown(shutdown.clone())
                    })
                    .await
                    .map_err(|error| {
                        client_inventory_file_error(descriptor, &info_hash, &torrent_name, error)
                    })?;
                qbit_client_inventory_item(descriptor, torrent, files)
            }
        }))
        .buffered(CLIENT_INVENTORY_FILE_FETCH_CONCURRENCY);

        while let Some(item) = items.next().await {
            send_client_inventory_item(sender, item?)
                .await
                .map_err(|error| unavailable_client_inventory(descriptor, error.to_string()))?;
        }
        Ok(())
    }

    async fn stream_rtorrent_inventory_chunk(
        &self,
        client: &RtorrentClient,
        chunk: Vec<RtorrentDownload>,
        sender: &mpsc::Sender<ClientInventoryMessage>,
        shutdown: ShutdownSignal,
    ) -> Result<(), TorrentClientError> {
        let descriptor = &self.descriptor;
        let mut items = stream::iter(chunk.into_iter().map(|download| {
            let shutdown = shutdown.clone();
            async move {
                let info_hash = download.info_hash.clone();
                let torrent_name = download.name.to_string();
                let files = client
                    .fetch_files_until_shutdown(&info_hash, || {
                        wait_for_inventory_shutdown(shutdown.clone())
                    })
                    .await
                    .map_err(|error| {
                        client_inventory_file_error(descriptor, &info_hash, &torrent_name, error)
                    })?;
                Ok(rtorrent_client_inventory_item(descriptor, download, files))
            }
        }))
        .buffered(CLIENT_INVENTORY_FILE_FETCH_CONCURRENCY);

        while let Some(item) = items.next().await {
            send_client_inventory_item(sender, item?)
                .await
                .map_err(|error| unavailable_client_inventory(descriptor, error.to_string()))?;
        }
        Ok(())
    }
}

pub(crate) fn client_inventory_file_error(
    descriptor: &TorrentClientDescriptor,
    info_hash: &InfoHash,
    torrent_name: &str,
    error: TorrentClientError,
) -> TorrentClientError {
    let context = format!(
        "fetch files for torrent `{torrent_name}` ({}) failed",
        info_hash.as_str()
    );
    match error {
        TorrentClientError::ApiChanged { message, .. } => TorrentClientError::ApiChanged {
            client: descriptor.name.to_string(),
            message: format!("{context}: {message}"),
        },
        TorrentClientError::BadResponse { message, .. } => TorrentClientError::BadResponse {
            client: descriptor.name.to_string(),
            message: format!("{context}: {message}"),
        },
        TorrentClientError::Cancelled { .. } => error,
        TorrentClientError::Unauthorized { .. } => TorrentClientError::Unauthorized {
            client: descriptor.name.to_string(),
        },
        TorrentClientError::Unavailable {
            retry_after_ms,
            message,
            ..
        } => TorrentClientError::Unavailable {
            client: descriptor.name.to_string(),
            retry_after_ms,
            message: format!("{context}: {message}"),
        },
        TorrentClientError::UnsupportedCapability { capability, .. } => {
            TorrentClientError::UnsupportedCapability {
                client: descriptor.name.to_string(),
                capability: format!("{context}: {capability}"),
            }
        }
    }
}

impl InjectionClient for RuntimeInjectionClient {
    fn descriptor(&self) -> &TorrentClientDescriptor {
        &self.descriptor
    }

    fn has_torrent<'a>(&'a self, info_hash: &'a InfoHash) -> ClientResultFuture<'a, bool> {
        Box::pin(async move {
            let started = Instant::now();
            let result = match &self.inner {
                RuntimeInjectionClientInner::Qbittorrent(client) => {
                    match client.torrent_info(info_hash).await {
                        Ok(torrent) => Ok(torrent.is_some()),
                        Err(error) => Err(error),
                    }
                }
                RuntimeInjectionClientInner::Rtorrent(client) => {
                    match client.download_info(info_hash).await {
                        Ok(download) => Ok(download.is_some()),
                        Err(error) => Err(error),
                    }
                }
            };
            self.metrics.record_client_request(
                ExternalOperation::Inventory,
                external_result_metric_outcome(&result),
                elapsed_ms(started),
            );
            result
        })
    }

    fn inject<'a>(&'a self, request: ClientInjectionRequest<'a>) -> ClientResultFuture<'a, ()> {
        Box::pin(async move {
            let started = Instant::now();
            let result = match &self.inner {
                RuntimeInjectionClientInner::Qbittorrent(client) => {
                    match self.ensure_qbittorrent_ready(client).await {
                        Ok(()) => {
                            let save_path = request.save_path.map(PathBuf::from);
                            client
                                .inject(QbitAddTorrent {
                                    torrent_bytes: request.torrent_bytes,
                                    save_path: save_path.as_ref(),
                                    category: self.qbit_default_category.as_deref(),
                                    tags: &self.qbit_default_tags,
                                    pause_for_recheck: request.pause_for_recheck,
                                    content_layout: QbitContentLayout::Original,
                                })
                                .await
                        }
                        Err(error) => Err(error),
                    }
                }
                RuntimeInjectionClientInner::Rtorrent(client) => {
                    match client
                        .inject(
                            request.torrent_bytes,
                            request.save_path,
                            &self.default_label,
                            !request.pause_for_recheck,
                        )
                        .await
                    {
                        Ok(()) => Ok(()),
                        Err(error) => Err(error),
                    }
                }
            };
            self.metrics.record_client_request(
                ExternalOperation::Inject,
                external_result_metric_outcome(&result),
                elapsed_ms(started),
            );
            result
        })
    }

    fn recheck<'a>(&'a self, info_hash: &'a InfoHash) -> ClientResultFuture<'a, ()> {
        Box::pin(async move {
            let started = Instant::now();
            let result = match &self.inner {
                RuntimeInjectionClientInner::Qbittorrent(client) => client.recheck(info_hash).await,
                RuntimeInjectionClientInner::Rtorrent(client) => client.recheck(info_hash).await,
            };
            self.metrics.record_client_request(
                ExternalOperation::Recheck,
                external_result_metric_outcome(&result),
                elapsed_ms(started),
            );
            result
        })
    }

    fn is_checking<'a>(&'a self, info_hash: &'a InfoHash) -> ClientResultFuture<'a, bool> {
        Box::pin(async move {
            let started = Instant::now();
            let result = match &self.inner {
                RuntimeInjectionClientInner::Qbittorrent(client) => {
                    match client.torrent_info(info_hash).await {
                        Ok(torrent) => Ok(torrent
                            .and_then(|torrent| torrent.state)
                            .is_some_and(|state| state.to_ascii_lowercase().contains("check"))),
                        Err(error) => Err(error),
                    }
                }
                RuntimeInjectionClientInner::Rtorrent(client) => {
                    match client.download_info(info_hash).await {
                        Ok(download) => Ok(download.is_some_and(|download| download.hashing)),
                        Err(error) => Err(error),
                    }
                }
            };
            self.metrics.record_client_request(
                ExternalOperation::Inventory,
                external_result_metric_outcome(&result),
                elapsed_ms(started),
            );
            result
        })
    }

    fn remaining_bytes<'a>(&'a self, info_hash: &'a InfoHash) -> ClientResultFuture<'a, ByteSize> {
        Box::pin(async move {
            let started = Instant::now();
            let result = match &self.inner {
                RuntimeInjectionClientInner::Qbittorrent(client) => {
                    match client.torrent_info(info_hash).await {
                        Ok(Some(torrent)) => match torrent.amount_left {
                            Some(remaining) => Ok(ByteSize::new(remaining)),
                            None => Err(TorrentClientError::BadResponse {
                                client: self.descriptor.name.as_str().to_owned(),
                                message: format!(
                                    "torrent {} is missing amount_left",
                                    info_hash.as_str()
                                ),
                            }),
                        },
                        Ok(None) => Err(missing_torrent(&self.descriptor, info_hash)),
                        Err(error) => Err(error),
                    }
                }
                RuntimeInjectionClientInner::Rtorrent(client) => {
                    match client.download_info(info_hash).await {
                        Ok(Some(download)) => Ok(download.left_bytes),
                        Ok(None) => Err(missing_torrent(&self.descriptor, info_hash)),
                        Err(error) => Err(error),
                    }
                }
            };
            self.metrics.record_client_request(
                ExternalOperation::Inventory,
                external_result_metric_outcome(&result),
                elapsed_ms(started),
            );
            result
        })
    }

    fn resume<'a>(&'a self, info_hash: &'a InfoHash) -> ClientResultFuture<'a, ()> {
        Box::pin(async move {
            let started = Instant::now();
            let result = match &self.inner {
                RuntimeInjectionClientInner::Qbittorrent(client) => client.resume(info_hash).await,
                RuntimeInjectionClientInner::Rtorrent(client) => client.resume(info_hash).await,
            };
            self.metrics.record_client_request(
                ExternalOperation::Resume,
                external_result_metric_outcome(&result),
                elapsed_ms(started),
            );
            result
        })
    }

    fn refresh_inventory<'a>(
        &'a self,
        worker: &'a InventoryRefreshWorker,
        shutdown: ShutdownSignal,
    ) -> ClientInventoryRefreshFuture<'a> {
        Box::pin(async move {
            let started = Instant::now();
            let result = self.refresh_inventory_stream(worker, shutdown).await;
            self.metrics.record_client_request(
                ExternalOperation::Inventory,
                inventory_refresh_metric_outcome(&result),
                elapsed_ms(started),
            );
            result
        })
    }
}

pub(crate) fn build_injection_clients(
    config: &BTreeMap<String, TorrentClientConfig>,
    registry: &TorrentClientRegistry,
    metrics: &MetricsRegistry,
) -> Result<Vec<Arc<dyn InjectionClient>>, DatabaseError> {
    let mut clients = Vec::<Arc<dyn InjectionClient>>::new();
    for (name, client_config) in config {
        let display_name = DisplayName::new(name).map_err(|error| DatabaseError::Unavailable {
            operation: "build injection client".to_owned(),
            message: error.to_string(),
        })?;
        let descriptor = registry
            .get(&display_name)
            .ok_or_else(|| DatabaseError::Unavailable {
                operation: "build injection client".to_owned(),
                message: format!("missing descriptor for torrent client {name}"),
            })?;
        if descriptor.kind == crate::domain::TorrentClientKind::Rtorrent
            && client_config
                .label_field
                .as_deref()
                .is_some_and(|field| field != "custom1")
        {
            return Err(DatabaseError::Unavailable {
                operation: "build injection client".to_owned(),
                message: format!("rtorrent client {name} only supports label_field custom1"),
            });
        }
        if descriptor.kind == crate::domain::TorrentClientKind::Rtorrent
            && (client_config.username.is_some()
                || client_config.password.is_some()
                || client_config.password_file.is_some()
                || client_config.password_env.is_some())
        {
            return Err(DatabaseError::Unavailable {
                operation: "build injection client".to_owned(),
                message: format!("rtorrent client {name} does not support configured auth fields"),
            });
        }
        clients.push(Arc::new(RuntimeInjectionClient::new(
            name,
            client_config,
            descriptor.clone(),
            metrics.clone(),
        )));
    }

    Ok(clients)
}

fn missing_torrent(
    descriptor: &TorrentClientDescriptor,
    info_hash: &InfoHash,
) -> TorrentClientError {
    TorrentClientError::BadResponse {
        client: descriptor.name.as_str().to_owned(),
        message: format!("torrent {} was not found", info_hash.as_str()),
    }
}

fn unavailable_client_inventory(
    descriptor: &TorrentClientDescriptor,
    message: String,
) -> TorrentClientError {
    TorrentClientError::Unavailable {
        client: descriptor.name.as_str().to_owned(),
        retry_after_ms: None,
        message,
    }
}

fn cancelled_client_inventory(descriptor: &TorrentClientDescriptor) -> TorrentClientError {
    TorrentClientError::Cancelled {
        client: descriptor.name.as_str().to_owned(),
        message: "shutdown requested".to_owned(),
    }
}

async fn wait_for_inventory_shutdown(mut shutdown: ShutdownSignal) {
    shutdown.cancelled().await;
}

async fn send_client_inventory_item(
    sender: &mpsc::Sender<ClientInventoryMessage>,
    item: ClientInventoryItem,
) -> Result<(), InventoryRefreshError> {
    sender
        .send(ClientInventoryMessage::Item(item))
        .await
        .map_err(|send_error| InventoryRefreshError::InvalidClientInventory {
            message: format!(
                "client inventory receiver closed before item was persisted: {send_error}"
            ),
        })
}

fn qbit_client_inventory_item(
    descriptor: &TorrentClientDescriptor,
    torrent: QbitTorrent,
    files: Vec<TorrentFile>,
) -> Result<ClientInventoryItem, TorrentClientError> {
    Ok(ClientInventoryItem {
        client_host: descriptor.host.clone(),
        info_hash: torrent.info_hash(descriptor.name.as_str())?,
        display_name: torrent.display_name(descriptor.name.as_str())?,
        media_type: MediaType::Video,
        save_path: torrent
            .save_path
            .unwrap_or_else(|| descriptor.default_save_path.clone()),
        files,
    })
}

fn rtorrent_client_inventory_item(
    descriptor: &TorrentClientDescriptor,
    download: RtorrentDownload,
    files: Vec<TorrentFile>,
) -> ClientInventoryItem {
    ClientInventoryItem {
        client_host: descriptor.host.clone(),
        info_hash: download.info_hash,
        display_name: download.name,
        media_type: MediaType::Video,
        save_path: download.directory,
        files,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use axum::response::Response;
    use axum::routing::{get, post};
    use tokio::net::TcpListener;

    use super::*;
    use crate::clients::TorrentClientCapabilities;
    use crate::domain::{ClientHost, TorrentClientKind};

    const TEST_INFO_HASH: &str = "0123456789abcdef0123456789abcdef01234567";

    mod injection_contract {
        use super::*;

        #[tokio::test]
        async fn qbittorrent_validates_capabilities_before_mutation() {
            let add_calls = Arc::new(AtomicUsize::new(0));
            let add_call_counter = add_calls.clone();
            let app = axum::Router::new()
                .route(
                    "/api/v2/auth/login",
                    post(|| async { ([(axum::http::header::SET_COOKIE, "SID=ok")], "Ok") }),
                )
                .route("/api/v2/app/version", get(|| async { "4.2.0" }))
                .route(
                    "/api/v2/torrents/add",
                    post(move || {
                        let add_call_counter = add_call_counter.clone();
                        async move {
                            add_call_counter.fetch_add(1, Ordering::SeqCst);
                            StatusCode::OK
                        }
                    }),
                );
            let (base_url, handle) = spawn_contract_server(app).await;
            let descriptor =
                torrent_client_descriptor("qbit", TorrentClientKind::Qbittorrent, base_url.clone());
            let config = torrent_client_config(ConfigTorrentClientKind::Qbittorrent, base_url);
            let metrics = MetricsRegistry::new();
            let client = RuntimeInjectionClient::new("qbit", &config, descriptor, metrics.clone());
            let info_hash = test_info_hash();

            let error = client
                .inject(ClientInjectionRequest {
                    info_hash: &info_hash,
                    torrent_bytes: b"torrent bytes",
                    save_path: None,
                    pause_for_recheck: false,
                })
                .await
                .unwrap_err();

            handle.abort();
            assert!(matches!(
                error,
                TorrentClientError::UnsupportedCapability { .. }
            ));
            assert_eq!(0, add_calls.load(Ordering::SeqCst));
            let output = metrics.render_prometheus(&crate::metrics::MetricsSnapshot::default());
            assert!(output.contains("sporos_client_requests_total"));
            assert!(output.contains("operation=\"inject\""));
            assert!(output.contains("outcome=\"unsupported\""));
        }

        #[tokio::test]
        async fn qbittorrent_maps_metadata_pause_and_save_path() {
            let add_body = Arc::new(Mutex::new(None::<String>));
            let seen_add_body = add_body.clone();
            let create_tag_bodies = Arc::new(Mutex::new(Vec::<String>::new()));
            let seen_create_tag_bodies = create_tag_bodies.clone();
            let create_category_body = Arc::new(Mutex::new(None::<String>));
            let seen_create_category_body = create_category_body.clone();
            let app = axum::Router::new()
                .route(
                    "/api/v2/auth/login",
                    post(|| async { ([(axum::http::header::SET_COOKIE, "SID=ok")], "Ok") }),
                )
                .route("/api/v2/app/version", get(|| async { "4.6.0" }))
                .route(
                    "/api/v2/torrents/createTags",
                    post(move |request: Request<Body>| {
                        let seen_create_tag_bodies = seen_create_tag_bodies.clone();
                        async move {
                            let body = to_bytes(request.into_body(), 1_000_000).await.unwrap();
                            let body = String::from_utf8(body.to_vec()).unwrap();
                            seen_create_tag_bodies.lock().unwrap().push(body);
                            StatusCode::OK
                        }
                    }),
                )
                .route(
                    "/api/v2/torrents/createCategory",
                    post(move |request: Request<Body>| {
                        let seen_create_category_body = seen_create_category_body.clone();
                        async move {
                            let body = to_bytes(request.into_body(), 1_000_000).await.unwrap();
                            let body = String::from_utf8(body.to_vec()).unwrap();
                            *seen_create_category_body.lock().unwrap() = Some(body);
                            StatusCode::OK
                        }
                    }),
                )
                .route(
                    "/api/v2/torrents/add",
                    post(move |request: Request<Body>| {
                        let seen_add_body = seen_add_body.clone();
                        async move {
                            let body = to_bytes(request.into_body(), 1_000_000).await.unwrap();
                            let body = String::from_utf8(body.to_vec()).unwrap();
                            *seen_add_body.lock().unwrap() = Some(body);
                            StatusCode::OK
                        }
                    }),
                );
            let (base_url, handle) = spawn_contract_server(app).await;
            let descriptor =
                torrent_client_descriptor("qbit", TorrentClientKind::Qbittorrent, base_url.clone());
            let mut config = torrent_client_config(ConfigTorrentClientKind::Qbittorrent, base_url);
            config.default_category = Some("movies".to_owned());
            config.default_tags = vec!["cross-seed".to_owned(), "sporos".to_owned()];
            let client =
                RuntimeInjectionClient::new("qbit", &config, descriptor, MetricsRegistry::new());
            let info_hash = test_info_hash();

            client
                .inject(ClientInjectionRequest {
                    info_hash: &info_hash,
                    torrent_bytes: b"torrent bytes",
                    save_path: None,
                    pause_for_recheck: true,
                })
                .await
                .unwrap();

            handle.abort();
            let body = add_body.lock().unwrap().clone().unwrap();
            let tag_bodies = create_tag_bodies.lock().unwrap().join("\n");
            let category_body = create_category_body.lock().unwrap().clone().unwrap();
            assert!(tag_bodies.contains("tags=cross-seed"));
            assert!(tag_bodies.contains("tags=sporos"));
            assert!(category_body.contains("category=movies"));
            assert!(category_body.contains("savePath=%2Fdownloads"));
            assert!(body.contains("name=\"tags\"\r\n\r\ncross-seed,sporos"));
            assert!(body.contains("name=\"category\"\r\n\r\nmovies"));
            assert!(body.contains("name=\"paused\"\r\n\r\ntrue"));
            assert!(body.contains("name=\"skip_checking\"\r\n\r\nfalse"));
        }

        #[tokio::test]
        async fn rtorrent_maps_paused_injection_and_label() {
            let inject_body = Arc::new(Mutex::new(None::<String>));
            let seen_inject_body = inject_body.clone();
            let app = axum::Router::new().route(
                "/RPC2",
                post(move |request: Request<Body>| {
                    let seen_inject_body = seen_inject_body.clone();
                    async move {
                        let body = to_bytes(request.into_body(), 65_536).await.unwrap();
                        let body = String::from_utf8(body.to_vec()).unwrap();
                        *seen_inject_body.lock().unwrap() = Some(body);
                        (StatusCode::OK, xml_response("<i8>0</i8>"))
                    }
                }),
            );
            let (base_url, handle) = spawn_contract_server(app).await;
            let endpoint = format!("{base_url}/RPC2");
            let descriptor = torrent_client_descriptor(
                "rtorrent",
                TorrentClientKind::Rtorrent,
                endpoint.clone(),
            );
            let mut config = torrent_client_config(ConfigTorrentClientKind::Rtorrent, endpoint);
            config.default_label = "cross-seed".to_owned();
            let client = RuntimeInjectionClient::new(
                "rtorrent",
                &config,
                descriptor,
                MetricsRegistry::new(),
            );
            let info_hash = test_info_hash();

            client
                .inject(ClientInjectionRequest {
                    info_hash: &info_hash,
                    torrent_bytes: b"torrent bytes",
                    save_path: None,
                    pause_for_recheck: true,
                })
                .await
                .unwrap();

            handle.abort();
            let body = inject_body.lock().unwrap().clone().unwrap();
            assert!(body.contains("<methodName>load.raw</methodName>"));
            assert!(!body.contains("<methodName>load.raw_start</methodName>"));
            assert!(body.contains("<string>d.custom1.set=cross-seed</string>"));
        }
    }

    mod inventory_contract {
        use super::*;

        #[tokio::test]
        async fn qbittorrent_inventory_failures_record_metrics() {
            let app = axum::Router::new()
                .route(
                    "/api/v2/auth/login",
                    post(|| async { ([(axum::http::header::SET_COOKIE, "SID=ok")], "Ok") }),
                )
                .route(
                    "/api/v2/torrents/info",
                    get(|| async { oversized_response(65 * 1024 * 1024) }),
                );
            let (base_url, handle) = spawn_contract_server(app).await;
            let descriptor =
                torrent_client_descriptor("qbit", TorrentClientKind::Qbittorrent, base_url.clone());
            let config = torrent_client_config(ConfigTorrentClientKind::Qbittorrent, base_url);
            let metrics = MetricsRegistry::new();
            let client = RuntimeInjectionClient::new("qbit", &config, descriptor, metrics.clone());
            let info_hash = test_info_hash();

            let error = client.has_torrent(&info_hash).await.unwrap_err();

            handle.abort();
            assert!(matches!(error, TorrentClientError::BadResponse { .. }));
            let output = metrics.render_prometheus(&crate::metrics::MetricsSnapshot::default());
            assert!(output.contains("sporos_client_requests_total"));
            assert!(output.contains("operation=\"inventory\""));
            assert!(output.contains("outcome=\"failed\""));
        }

        #[test]
        fn file_fetch_errors_preserve_failure_class() {
            let descriptor = torrent_client_descriptor(
                "qbit",
                TorrentClientKind::Qbittorrent,
                "http://qbit.local".to_owned(),
            );
            let error = client_inventory_file_error(
                &descriptor,
                &test_info_hash(),
                "Torrent 1",
                TorrentClientError::Unauthorized {
                    client: "qbit".to_owned(),
                },
            );

            assert!(matches!(error, TorrentClientError::Unauthorized { .. }));
        }
    }

    fn test_info_hash() -> InfoHash {
        InfoHash::new(TEST_INFO_HASH).unwrap()
    }

    fn torrent_client_descriptor(
        name: &str,
        kind: TorrentClientKind,
        url: String,
    ) -> TorrentClientDescriptor {
        TorrentClientDescriptor {
            name: DisplayName::new(name).unwrap(),
            kind,
            host: ClientHost::new(format!("{name}.local")).unwrap(),
            url,
            default_save_path: "/downloads".into(),
            readonly: false,
            capabilities: TorrentClientCapabilities::for_kind(kind),
        }
    }

    fn torrent_client_config(kind: ConfigTorrentClientKind, url: String) -> TorrentClientConfig {
        TorrentClientConfig {
            kind,
            url,
            username: None,
            password: None,
            password_file: None,
            password_env: None,
            default_save_path: "/downloads".into(),
            default_category: None,
            default_tags: vec![crate::config::DEFAULT_INJECTION_METADATA.to_owned()],
            default_label: crate::config::DEFAULT_INJECTION_METADATA.to_owned(),
            label_field: None,
        }
    }

    async fn spawn_contract_server(app: axum::Router) -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{address}"), handle)
    }

    fn xml_response(inner: &str) -> String {
        format!(
            r#"<?xml version="1.0"?><methodResponse><params><param><value>{inner}</value></param></params></methodResponse>"#
        )
    }

    fn oversized_response(length: u64) -> Response {
        Response::builder()
            .status(StatusCode::OK)
            .header(axum::http::header::CONTENT_LENGTH, length.to_string())
            .body(Body::empty())
            .unwrap()
    }

    #[tokio::test]
    async fn contract_records_failed_metrics_for_oversized_qbittorrent_responses() {
        let app = axum::Router::new()
            .route(
                "/api/v2/auth/login",
                post(|| async { ([(axum::http::header::SET_COOKIE, "SID=ok")], "Ok") }),
            )
            .route(
                "/api/v2/app/version",
                get(|| async { oversized_response(65 * 1024 * 1024) }),
            );
        let (base_url, handle) = spawn_contract_server(app).await;
        let descriptor =
            torrent_client_descriptor("qbit", TorrentClientKind::Qbittorrent, base_url.clone());
        let config = torrent_client_config(ConfigTorrentClientKind::Qbittorrent, base_url);
        let metrics = MetricsRegistry::new();
        let client = RuntimeInjectionClient::new("qbit", &config, descriptor, metrics.clone());
        let info_hash = test_info_hash();

        let error = client
            .inject(ClientInjectionRequest {
                info_hash: &info_hash,
                torrent_bytes: b"torrent bytes",
                save_path: None,
                pause_for_recheck: false,
            })
            .await
            .unwrap_err();

        handle.abort();
        assert!(matches!(error, TorrentClientError::BadResponse { .. }));
        let output = metrics.render_prometheus(&crate::metrics::MetricsSnapshot::default());
        assert!(output.contains("sporos_client_requests_total"));
        assert!(output.contains("operation=\"inject\""));
        assert!(output.contains("outcome=\"failed\""));
    }

    #[tokio::test]
    async fn contract_records_failed_metrics_for_oversized_rtorrent_responses() {
        let app = axum::Router::new().route(
            "/RPC2",
            post(|_request: Request<Body>| async { oversized_response(65 * 1024 * 1024) }),
        );
        let (base_url, handle) = spawn_contract_server(app).await;
        let endpoint = format!("{base_url}/RPC2");
        let descriptor =
            torrent_client_descriptor("rtorrent", TorrentClientKind::Rtorrent, endpoint.clone());
        let config = torrent_client_config(ConfigTorrentClientKind::Rtorrent, endpoint);
        let metrics = MetricsRegistry::new();
        let client = RuntimeInjectionClient::new("rtorrent", &config, descriptor, metrics.clone());
        let info_hash = test_info_hash();

        let error = client.has_torrent(&info_hash).await.unwrap_err();

        handle.abort();
        assert!(matches!(error, TorrentClientError::BadResponse { .. }));
        let output = metrics.render_prometheus(&crate::metrics::MetricsSnapshot::default());
        assert!(output.contains("sporos_client_requests_total"));
        assert!(output.contains("operation=\"inventory\""));
        assert!(output.contains("outcome=\"failed\""));
    }
}
