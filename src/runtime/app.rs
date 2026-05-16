use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Mutex as AsyncMutex, mpsc};

use crate::arr::{ArrEndpoint, ArrRegistry};
use crate::clients::qbittorrent::QbitTorrent;
use crate::clients::qbittorrent::{QbitAddTorrent, QbitContentLayout, QbittorrentClient};
use crate::clients::rtorrent::{RtorrentClient, RtorrentDownload};
use crate::clients::{TorrentClientDescriptor, TorrentClientRegistry};
use crate::config::{ConfigTorrentClientKind, SporosConfig, TorrentClientConfig};
use crate::domain::{
    ByteSize, DependencyName, DisplayName, IndexerId, InfoHash, ItemTitle, LocalItem,
    LocalItemSource, MediaType, ReasonText, SourceKey, TorrentFile,
};
use crate::errors::{DatabaseError, TorrentClientError};
use crate::http::{
    AnnouncementWorkflowRequest, HttpState, JobRunWorkflowRequest, ReadinessState,
    SearchWorkflowRequest, WorkflowQueues,
};
use crate::indexers::{
    ConfiguredTorznabIndexer, IndexerBackoffPolicy, TorznabEndpoint, TorznabHttpClient,
    TorznabRegistry, TorznabRequestError,
};
use crate::inventory::InventoryScanOptions;
use crate::inventory_refresh::{
    ClientInventoryItem, ClientInventoryMessage, InventoryRefreshError, InventoryRefreshRequest,
    InventoryRefreshSummary, InventoryRefreshWorker, inventory_refresh_queue,
};
use crate::metrics::MetricsRegistry;
use crate::notifications::{NotificationJob, notification_queue};
use crate::persistence::repository::Repository;
use crate::runtime::announce_worker::AnnounceWorker;
use crate::runtime::health::HealthRegistry;
use crate::runtime::injection_worker::{
    ClientInjectionRequest, ClientInventoryRefreshFuture, ClientResultFuture, InjectionClient,
    InjectionWorker,
};
use crate::runtime::queue::{QueueKind, RuntimeQueueConfig, WorkReceiver, bounded_work_queue};
use crate::runtime::scheduler::{
    PersistedScheduler, ScheduledJobRun, SchedulerConfig, parse_interval_ms, scheduler_queue,
};
use crate::runtime::search::{
    RuntimeSearchPlanner, RuntimeTorznabSearchPlan, plan_runtime_torznab_search,
    seed_arr_endpoint_backoff,
};
use crate::runtime::shutdown::{
    ShutdownController, ShutdownPhase, ShutdownSignal, shutdown_channel,
};

const RUNTIME_CLIENT_INVENTORY_BUFFER: usize = 64;

#[derive(Debug)]
pub struct AppRuntime {
    pub state: AppState,
    pub receivers: RuntimeReceivers,
}

#[derive(Debug, Clone)]
pub struct AppState {
    pub config: SporosConfig,
    pub repository: Repository,
    pub clients: TorrentClientRegistry,
    pub health: HealthRegistry,
    pub metrics: MetricsRegistry,
    pub http: HttpState,
    pub queues: RuntimeQueues,
    pub announce_worker: AnnounceWorker,
    pub scheduler: PersistedScheduler,
    pub inventory_refresh: InventoryRefreshWorker,
    pub injection_worker: InjectionWorker,
    pub search_planner: RuntimeSearchPlanner,
    pub torznab_indexers: BTreeMap<DependencyName, ConfiguredTorznabIndexer>,
    pub torznab_client: TorznabHttpClient,
    pub saved_retry_interval: Duration,
    pub shutdown: ShutdownController,
    pub shutdown_signal: ShutdownSignal,
}

impl AppState {
    pub async fn refresh_torrent_client_inventories(
        &self,
    ) -> Result<Vec<InventoryRefreshSummary>, InventoryRefreshError> {
        self.injection_worker
            .refresh_client_inventories_until_shutdown(
                &self.inventory_refresh,
                self.shutdown_signal.clone(),
            )
            .await
    }

    pub async fn plan_search_workflow(
        &self,
        request: SearchWorkflowRequest,
        now_ms: i64,
    ) -> Result<SearchWorkflowPlanSummary, DatabaseError> {
        let item = search_workflow_item(request.query)?;
        let ids = self
            .search_planner
            .lookup_ids_for_item(&item, now_ms)
            .await?;
        let indexers = self.repository.indexer_search_caps_snapshot(1_000).await?;
        let mut plans = Vec::new();
        let mut candidate_count = 0_usize;

        for indexer in indexers
            .into_iter()
            .filter(|indexer| indexer.enabled)
            .filter(|indexer| {
                indexer
                    .retry_after_ms
                    .is_none_or(|retry_after| retry_after <= now_ms)
            })
        {
            let Some(plan) = plan_runtime_torznab_search(&item, &ids, &indexer.caps) else {
                continue;
            };
            plans.push(IndexerSearchPlan {
                indexer_id: indexer.indexer_id,
                indexer_name: indexer.name.clone(),
                plan: plan.clone(),
            });
            let Some(configured) = self.torznab_indexers.get(&indexer.name) else {
                continue;
            };
            let endpoint = TorznabEndpoint {
                indexer_id: indexer.indexer_id,
                name: indexer.name,
                url: configured.url.clone(),
                api_key: configured
                    .api_key
                    .as_ref()
                    .map(|api_key| api_key.expose_secret().to_owned()),
                caps: indexer.caps,
                retry_after_ms: indexer.retry_after_ms,
            };
            let candidates = self
                .torznab_client
                .search(&endpoint, item.media_type, &plan.plan, now_ms)
                .await
                .map_err(|error| DatabaseError::Unavailable {
                    operation: "execute Torznab search workflow".to_owned(),
                    message: error.to_string(),
                })?;
            candidate_count = candidate_count.saturating_add(candidates.len());
        }

        Ok(SearchWorkflowPlanSummary {
            plans,
            candidate_count,
        })
    }

    pub async fn refresh_indexer_capabilities(
        &self,
        now_ms: i64,
    ) -> Result<IndexerCapsRefreshSummary, DatabaseError> {
        let mut summary = IndexerCapsRefreshSummary::default();
        let mut last_error = None;
        let registry = self.repository.indexer_registry_snapshot(1_000).await?;

        for indexer in self
            .torznab_indexers
            .values()
            .filter(|indexer| indexer.enabled)
        {
            let Some(row) = registry.iter().find(|row| row.name == indexer.name) else {
                continue;
            };
            if row
                .retry_after_ms
                .is_some_and(|retry_after| retry_after > now_ms)
            {
                summary.skipped_backoff += 1;
                summary.next_backoff_deadline_ms = Some(
                    summary
                        .next_backoff_deadline_ms
                        .map_or(row.retry_after_ms.unwrap_or(now_ms), |current| {
                            current.min(row.retry_after_ms.unwrap_or(now_ms))
                        }),
                );
                continue;
            }
            match self.torznab_client.caps(indexer).await {
                Ok(caps) => {
                    self.repository
                        .record_indexer_caps_success(&indexer.name, &caps, now_ms)
                        .await?;
                    summary.refreshed += 1;
                }
                Err(error) => {
                    let message = error.to_string();
                    let reason = health_reason(Some(&message), "caps failed")
                        .unwrap_or_else(|| ReasonText::new("caps failed").unwrap());
                    let retry_after_ms = indexer_error_retry_after(&error, now_ms);
                    self.repository
                        .record_indexer_caps_failure(
                            &indexer.name,
                            &reason,
                            Some(retry_after_ms),
                            now_ms,
                        )
                        .await?;
                    summary.failed += 1;
                    last_error = Some(message);
                }
            }
        }

        summary.last_error = last_error;
        Ok(summary)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SearchWorkflowPlanSummary {
    pub plans: Vec<IndexerSearchPlan>,
    pub candidate_count: usize,
}

#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct IndexerCapsRefreshSummary {
    pub refreshed: usize,
    pub failed: usize,
    pub skipped_backoff: usize,
    pub next_backoff_deadline_ms: Option<i64>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct IndexerSearchPlan {
    pub indexer_id: IndexerId,
    pub indexer_name: DependencyName,
    pub plan: RuntimeTorznabSearchPlan,
}

#[derive(Debug, Clone)]
pub struct RuntimeQueues {
    pub workflow: WorkflowQueues,
    pub scheduler: crate::runtime::queue::BoundedWorkQueue<ScheduledJobRun>,
    pub inventory_refresh: crate::runtime::queue::BoundedWorkQueue<InventoryRefreshRequest>,
    pub notifications: crate::runtime::queue::BoundedWorkQueue<NotificationJob>,
}

#[derive(Debug)]
pub struct RuntimeReceivers {
    pub announcements: WorkReceiver<AnnouncementWorkflowRequest>,
    pub searches: WorkReceiver<SearchWorkflowRequest>,
    pub jobs: WorkReceiver<JobRunWorkflowRequest>,
    pub scheduler: WorkReceiver<ScheduledJobRun>,
    pub inventory_refresh: WorkReceiver<InventoryRefreshRequest>,
    pub notifications: WorkReceiver<NotificationJob>,
}

impl AppRuntime {
    pub async fn build(config: SporosConfig) -> Result<Self, DatabaseError> {
        let repository = Repository::connect(&config.paths.database).await?;
        Self::from_repository(config, repository).await
    }

    pub async fn from_repository(
        config: SporosConfig,
        repository: Repository,
    ) -> Result<Self, DatabaseError> {
        let health = HealthRegistry::new();
        let metrics = MetricsRegistry::new();
        let indexers = TorznabRegistry::from_config(&config.indexers).map_err(|error| {
            DatabaseError::Unavailable {
                operation: "build Torznab indexer registry".to_owned(),
                message: error.to_string(),
            }
        })?;
        let torznab_indexers = indexers
            .indexers()
            .iter()
            .cloned()
            .map(|indexer| (indexer.name.clone(), indexer))
            .collect::<BTreeMap<_, _>>();
        let now_ms = crate::runtime::announce_worker::unix_time_ms();
        repository
            .sync_torznab_indexers(indexers.indexers(), now_ms)
            .await?;
        let arr = ArrRegistry::from_config(&config.indexers.arr).map_err(|error| {
            DatabaseError::Unavailable {
                operation: "build Arr registry".to_owned(),
                message: error.to_string(),
            }
        })?;
        let clients =
            TorrentClientRegistry::from_config(&config.torrent_clients).map_err(|error| {
                DatabaseError::Unavailable {
                    operation: "build torrent client registry".to_owned(),
                    message: error.to_string(),
                }
            })?;
        let injection_clients = build_injection_clients(&config.torrent_clients, &clients)?;
        let queue_config = RuntimeQueueConfig::default();
        let (workflow, workflow_receivers) = workflow_queues(queue_config);
        let (scheduler_queue, scheduler_receiver) = scheduler_queue(queue_config.indexing_limit);
        let (inventory_queue, inventory_receiver) =
            inventory_refresh_queue(queue_config.indexing_limit);
        let (notification_queue, notification_receiver) =
            notification_queue(queue_config.notification_limit);
        let scheduler_config = SchedulerConfig::from_scheduling_config(&config.scheduling)
            .map_err(|error| DatabaseError::Unavailable {
                operation: "build scheduler config".to_owned(),
                message: error.to_string(),
            })?;
        let http_jobs = http_supported_jobs(&scheduler_config);
        let saved_retry_interval_ms = parse_interval_ms(&config.scheduling.saved_retry_interval)
            .map_err(|error| DatabaseError::Unavailable {
                operation: "build saved retry interval".to_owned(),
                message: error.to_string(),
            })?;
        let saved_retry_interval =
            Duration::from_millis(u64::try_from(saved_retry_interval_ms).map_err(|error| {
                DatabaseError::Unavailable {
                    operation: "build saved retry interval".to_owned(),
                    message: error.to_string(),
                }
            })?);
        let announce_worker = AnnounceWorker::new(
            repository.clone(),
            "sporos-announce-worker",
            &config.announce,
        )
        .map_err(|error| DatabaseError::Unavailable {
            operation: "build announce worker".to_owned(),
            message: error.to_string(),
        })?;
        let scheduler = PersistedScheduler::new(
            repository.clone(),
            scheduler_queue.clone(),
            scheduler_config,
        );
        let inventory_refresh = InventoryRefreshWorker::new(
            repository.clone(),
            InventoryScanOptions {
                max_depth: config.inventory.media_scan_max_depth,
            },
        );
        let injection_worker = InjectionWorker::new(repository.clone(), injection_clients);
        let mut arr_endpoints = arr
            .instances()
            .iter()
            .map(ArrEndpoint::from_configured)
            .collect::<Vec<_>>();
        let persisted_health = repository.dependency_health_snapshot(1_000).await?;
        seed_runtime_health(&health, &persisted_health);
        seed_arr_endpoint_backoff(&mut arr_endpoints, &persisted_health, now_ms);
        let search_planner = RuntimeSearchPlanner::new(
            repository.clone(),
            health.clone(),
            arr_endpoints,
            Duration::from_secs(30),
        );
        let (shutdown, shutdown_signal) = shutdown_channel();
        let queues = RuntimeQueues {
            workflow: workflow.clone(),
            scheduler: scheduler_queue,
            inventory_refresh: inventory_queue,
            notifications: notification_queue,
        };
        let mut readiness = ReadinessState::ready();
        readiness.workers_running = false;
        let mut http = HttpState::new(readiness, health.clone())
            .with_metrics(metrics.clone())
            .with_job_queue(workflow.jobs.clone())
            .with_allowed_jobs(http_jobs)
            .with_announce_acceptor(repository.clone(), config.announce.clone());
        if let Some(api_token) = config.server.api_token.as_ref() {
            http = http.with_api_token(api_token.expose_secret());
        }

        Ok(Self {
            state: AppState {
                config,
                repository,
                clients,
                health,
                metrics,
                http,
                queues,
                announce_worker,
                scheduler,
                inventory_refresh,
                injection_worker,
                search_planner,
                torznab_indexers,
                torznab_client: TorznabHttpClient::new(Duration::from_secs(120)),
                saved_retry_interval,
                shutdown,
                shutdown_signal,
            },
            receivers: RuntimeReceivers {
                announcements: workflow_receivers.announcements,
                searches: workflow_receivers.searches,
                jobs: workflow_receivers.jobs,
                scheduler: scheduler_receiver,
                inventory_refresh: inventory_receiver,
                notifications: notification_receiver,
            },
        })
    }
}

fn search_workflow_item(title: ItemTitle) -> Result<LocalItem, DatabaseError> {
    let source_key = SourceKey::new(format!("search:{}", title.as_str())).map_err(|error| {
        DatabaseError::QueryFailed {
            operation: "build search workflow source key".to_owned(),
            message: error.to_string(),
        }
    })?;
    let display_name =
        DisplayName::new(title.as_str()).map_err(|error| DatabaseError::QueryFailed {
            operation: "build search workflow display name".to_owned(),
            message: error.to_string(),
        })?;

    let media_type = infer_search_media_type(title.as_str());

    Ok(LocalItem {
        id: None,
        source: LocalItemSource::Virtual { source_key },
        title,
        display_name,
        media_type,
        info_hash: None,
        path: None,
        save_path: None,
        total_size: ByteSize::new(1),
        mtime_ms: None,
    })
}

fn infer_search_media_type(title: &str) -> MediaType {
    let mut has_season = false;
    for token in title.split(['.', '_', ' ', '-']) {
        let lower = token.to_ascii_lowercase();
        if looks_like_episode_token(&lower) {
            return MediaType::Episode;
        }
        if looks_like_season_token(&lower) {
            has_season = true;
        }
    }
    if has_season {
        MediaType::SeasonPack
    } else {
        MediaType::Movie
    }
}

fn looks_like_episode_token(token: &str) -> bool {
    let mut chars = token.chars();
    if chars.next() != Some('s') {
        return false;
    }
    let mut saw_season_digit = false;
    for character in chars.by_ref() {
        if character == 'e' {
            return saw_season_digit
                && chars.next().is_some_and(|next| next.is_ascii_digit())
                && chars.all(|remaining| remaining.is_ascii_digit());
        }
        if !character.is_ascii_digit() {
            return false;
        }
        saw_season_digit = true;
    }
    false
}

fn looks_like_season_token(token: &str) -> bool {
    let mut chars = token.chars();
    chars.next() == Some('s')
        && chars
            .next()
            .is_some_and(|character| character.is_ascii_digit())
        && chars.all(|character| character.is_ascii_digit())
}

fn seed_runtime_health(
    health: &HealthRegistry,
    rows: &[crate::persistence::repository::DependencyHealthSnapshot],
) {
    for row in rows {
        let kind = match row.dependency_type.as_str() {
            "arr" => crate::runtime::health::DependencyKind::Arr,
            "indexer" => crate::runtime::health::DependencyKind::Indexer,
            _ => continue,
        };
        match row.state.as_str() {
            "healthy" => health.set_healthy(kind, row.dependency_name.clone(), row.checked_at_ms),
            "degraded" => {
                if let Some(reason) = health_reason(row.reason.as_deref(), "dependency degraded") {
                    health.set_degraded(
                        kind,
                        row.dependency_name.clone(),
                        reason,
                        row.retry_after_ms,
                    );
                }
            }
            "unavailable" => {
                if let Some(reason) = health_reason(row.reason.as_deref(), "dependency unavailable")
                {
                    health.set_unavailable(
                        kind,
                        row.dependency_name.clone(),
                        reason,
                        row.retry_after_ms,
                    );
                }
            }
            _ => health.set_unknown(kind, row.dependency_name.clone()),
        }
    }
}

fn health_reason(value: Option<&str>, fallback: &'static str) -> Option<ReasonText> {
    value
        .and_then(|reason| ReasonText::new(reason).ok())
        .or_else(|| ReasonText::new(fallback).ok())
}

fn http_supported_jobs(config: &SchedulerConfig) -> BTreeSet<crate::domain::JobName> {
    config
        .jobs
        .iter()
        .filter(|job| job.name.as_str() == "indexer_caps")
        .map(|job| job.name.clone())
        .collect()
}

fn indexer_error_retry_after(error: &TorznabRequestError, now_ms: i64) -> i64 {
    let policy = IndexerBackoffPolicy::default();
    match error {
        TorznabRequestError::Backoff { retry_after_ms } => retry_after_ms
            .filter(|retry_after| *retry_after > now_ms)
            .unwrap_or_else(|| policy.retry_after_deadline(now_ms, 0, None)),
        TorznabRequestError::RateLimited { retry_after }
        | TorznabRequestError::HttpStatus { retry_after, .. } => {
            policy.retry_after_deadline(now_ms, 0, *retry_after)
        }
        TorznabRequestError::Timeout
        | TorznabRequestError::Request { .. }
        | TorznabRequestError::InvalidXml { .. }
        | TorznabRequestError::InvalidCandidate { .. }
        | TorznabRequestError::ResponseTooLarge { .. } => {
            policy.retry_after_deadline(now_ms, 0, None)
        }
    }
}

enum RuntimeInjectionClientInner {
    Qbittorrent(QbittorrentClient),
    Rtorrent(RtorrentClient),
}

struct RuntimeInjectionClient {
    descriptor: TorrentClientDescriptor,
    inner: RuntimeInjectionClientInner,
    qbit_validated: AsyncMutex<bool>,
}

impl RuntimeInjectionClient {
    fn new(name: &str, config: &TorrentClientConfig, descriptor: TorrentClientDescriptor) -> Self {
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
        }
    }

    async fn ensure_qbittorrent_ready(
        &self,
        client: &QbittorrentClient,
    ) -> Result<(), TorrentClientError> {
        let mut validated = self.qbit_validated.lock().await;
        if !*validated {
            client.validate().await?;
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
                                    for torrent in page {
                                        let info_hash =
                                            torrent.info_hash(self.descriptor.name.as_str())?;
                                        let files = client
                                            .fetch_files_until_shutdown(&info_hash, || {
                                                wait_for_inventory_shutdown(shutdown.clone())
                                            })
                                            .await?;
                                        send_client_inventory_item(
                                            &sender,
                                            qbit_client_inventory_item(
                                                &self.descriptor,
                                                torrent,
                                                files,
                                            )?,
                                        )
                                        .await
                                        .map_err(
                                            |error| {
                                                unavailable_client_inventory(
                                                    &self.descriptor,
                                                    error.to_string(),
                                                )
                                            },
                                        )?;
                                    }
                                    Ok(())
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
                                    for download in chunk {
                                        let files = client
                                            .fetch_files_until_shutdown(&download.info_hash, || {
                                                wait_for_inventory_shutdown(shutdown.clone())
                                            })
                                            .await?;
                                        send_client_inventory_item(
                                            &sender,
                                            rtorrent_client_inventory_item(
                                                &self.descriptor,
                                                download,
                                                files,
                                            ),
                                        )
                                        .await
                                        .map_err(
                                            |error| {
                                                unavailable_client_inventory(
                                                    &self.descriptor,
                                                    error.to_string(),
                                                )
                                            },
                                        )?;
                                    }
                                    Ok(())
                                }
                            },
                        )
                        .await?;
                }
            }
            sender
                .send(ClientInventoryMessage::Finished)
                .await
                .map_err(|_| InventoryRefreshError::InvalidClientInventory {
                    message: "client inventory receiver closed before completion".to_owned(),
                })
        };
        let (refresh_result, stream_result) = tokio::join!(refresh, stream);
        match (refresh_result, stream_result) {
            (_, Err(error)) => Err(error),
            (Err(error), Ok(())) => Err(error),
            (Ok(summary), Ok(())) => Ok(summary),
        }
    }
}

impl InjectionClient for RuntimeInjectionClient {
    fn descriptor(&self) -> &TorrentClientDescriptor {
        &self.descriptor
    }

    fn has_torrent<'a>(&'a self, info_hash: &'a InfoHash) -> ClientResultFuture<'a, bool> {
        Box::pin(async move {
            match &self.inner {
                RuntimeInjectionClientInner::Qbittorrent(client) => {
                    Ok(client.torrent_info(info_hash).await?.is_some())
                }
                RuntimeInjectionClientInner::Rtorrent(client) => {
                    Ok(client.download_info(info_hash).await?.is_some())
                }
            }
        })
    }

    fn inject<'a>(&'a self, request: ClientInjectionRequest<'a>) -> ClientResultFuture<'a, ()> {
        Box::pin(async move {
            match &self.inner {
                RuntimeInjectionClientInner::Qbittorrent(client) => {
                    self.ensure_qbittorrent_ready(client).await?;
                    let save_path = request.save_path.map(PathBuf::from);
                    client
                        .inject(QbitAddTorrent {
                            torrent_bytes: request.torrent_bytes,
                            save_path: save_path.as_ref(),
                            category: None,
                            pause_for_recheck: request.pause_for_recheck,
                            content_layout: QbitContentLayout::Original,
                        })
                        .await
                }
                RuntimeInjectionClientInner::Rtorrent(client) => {
                    client
                        .inject(
                            request.torrent_bytes,
                            request.save_path,
                            !request.pause_for_recheck,
                        )
                        .await?;
                    Ok(())
                }
            }
        })
    }

    fn recheck<'a>(&'a self, info_hash: &'a InfoHash) -> ClientResultFuture<'a, ()> {
        Box::pin(async move {
            match &self.inner {
                RuntimeInjectionClientInner::Qbittorrent(client) => client.recheck(info_hash).await,
                RuntimeInjectionClientInner::Rtorrent(client) => client.recheck(info_hash).await,
            }
        })
    }

    fn is_checking<'a>(&'a self, info_hash: &'a InfoHash) -> ClientResultFuture<'a, bool> {
        Box::pin(async move {
            match &self.inner {
                RuntimeInjectionClientInner::Qbittorrent(client) => Ok(client
                    .torrent_info(info_hash)
                    .await?
                    .and_then(|torrent| torrent.state)
                    .is_some_and(|state| state.to_ascii_lowercase().contains("check"))),
                RuntimeInjectionClientInner::Rtorrent(client) => Ok(client
                    .download_info(info_hash)
                    .await?
                    .is_some_and(|download| download.hashing)),
            }
        })
    }

    fn remaining_bytes<'a>(&'a self, info_hash: &'a InfoHash) -> ClientResultFuture<'a, ByteSize> {
        Box::pin(async move {
            match &self.inner {
                RuntimeInjectionClientInner::Qbittorrent(client) => {
                    let torrent = client
                        .torrent_info(info_hash)
                        .await?
                        .ok_or_else(|| missing_torrent(&self.descriptor, info_hash))?;
                    let remaining =
                        torrent
                            .amount_left
                            .ok_or_else(|| TorrentClientError::BadResponse {
                                client: self.descriptor.name.as_str().to_owned(),
                                message: format!(
                                    "torrent {} is missing amount_left",
                                    info_hash.as_str()
                                ),
                            })?;
                    Ok(ByteSize::new(remaining))
                }
                RuntimeInjectionClientInner::Rtorrent(client) => client
                    .download_info(info_hash)
                    .await?
                    .map(|download| download.left_bytes)
                    .ok_or_else(|| missing_torrent(&self.descriptor, info_hash)),
            }
        })
    }

    fn resume<'a>(&'a self, info_hash: &'a InfoHash) -> ClientResultFuture<'a, ()> {
        Box::pin(async move {
            match &self.inner {
                RuntimeInjectionClientInner::Qbittorrent(client) => client.resume(info_hash).await,
                RuntimeInjectionClientInner::Rtorrent(client) => client.resume(info_hash).await,
            }
        })
    }

    fn refresh_inventory<'a>(
        &'a self,
        worker: &'a InventoryRefreshWorker,
        shutdown: ShutdownSignal,
    ) -> ClientInventoryRefreshFuture<'a> {
        Box::pin(async move { self.refresh_inventory_stream(worker, shutdown).await })
    }
}

fn build_injection_clients(
    config: &BTreeMap<String, TorrentClientConfig>,
    registry: &TorrentClientRegistry,
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
    let _ = shutdown.cancelled().await;
}

async fn send_client_inventory_item(
    sender: &mpsc::Sender<ClientInventoryMessage>,
    item: ClientInventoryItem,
) -> Result<(), InventoryRefreshError> {
    sender
        .send(ClientInventoryMessage::Item(item))
        .await
        .map_err(|_| InventoryRefreshError::InvalidClientInventory {
            message: "client inventory receiver closed before item was persisted".to_owned(),
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

#[derive(Debug)]
struct WorkflowReceivers {
    announcements: WorkReceiver<AnnouncementWorkflowRequest>,
    searches: WorkReceiver<SearchWorkflowRequest>,
    jobs: WorkReceiver<JobRunWorkflowRequest>,
}

fn workflow_queues(queue_config: RuntimeQueueConfig) -> (WorkflowQueues, WorkflowReceivers) {
    let (announcements, announcement_receiver) =
        bounded_work_queue(QueueKind::Announcement, queue_config.announcement_limit);
    let (searches, search_receiver) =
        bounded_work_queue(QueueKind::Search, queue_config.search_limit);
    let (jobs, job_receiver) = bounded_work_queue(QueueKind::Indexing, queue_config.indexing_limit);

    (
        WorkflowQueues {
            announcements,
            searches,
            jobs,
        },
        WorkflowReceivers {
            announcements: announcement_receiver,
            searches: search_receiver,
            jobs: job_receiver,
        },
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::future::{Future, pending};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::clients::TorrentClientCapabilities;
    use crate::config::{ArrInstanceConfig, TorznabIndexerConfig};
    use crate::domain::{
        ClientHost, DependencyName, DependencyState, ItemTitle, ReasonText, TorrentClientKind,
    };
    use crate::http::router;
    use crate::indexers::{CategoryCaps, SearchCaps, TorznabCaps, TorznabLimits};
    use crate::metrics::ExternalOutcome;
    use crate::secrets::{ApiKey, ApiToken};
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode, header::SET_COOKIE};
    use axum::response::{IntoResponse, Response};
    use axum::routing::{get, post};
    use tokio::net::TcpListener;
    use tower::ServiceExt;

    #[tokio::test]
    async fn runtime_composes_services_from_config_and_repository() {
        let mut config = SporosConfig::default();
        config.scheduling.saved_retry_interval = "5m".to_owned();
        config.torrent_clients.insert(
            "qbit".to_owned(),
            TorrentClientConfig {
                kind: ConfigTorrentClientKind::Qbittorrent,
                url: "http://qbittorrent:8080".to_owned(),
                username: None,
                password: None,
                password_file: None,
                password_env: None,
                default_save_path: "/downloads".into(),
                label_field: None,
            },
        );
        config.indexers.torznab.insert(
            "main".to_owned(),
            TorznabIndexerConfig {
                url: "https://indexer.example/api".to_owned(),
                api_key: Some(ApiKey::new("secret").unwrap()),
                api_key_file: None,
                api_key_env: None,
            },
        );
        let repository = Repository::connect_in_memory().await.unwrap();

        let runtime = AppRuntime::from_repository(config, repository.clone())
            .await
            .unwrap();
        let indexers = repository.indexer_registry_snapshot(10).await.unwrap();

        assert!(!runtime.state.http.clone().readiness().is_ready());
        assert!(!runtime.state.http.clone().readiness().workers_running);
        assert_eq!(1, indexers.len());
        assert_eq!("main", indexers[0].name.as_str());
        assert_eq!("https://indexer.example/api", indexers[0].url);
        assert_eq!("direct", indexers[0].api_key_source);
        assert_eq!(0, runtime.state.queues.workflow.announcements.stats().depth);
        assert_eq!(0, runtime.state.queues.scheduler.stats().depth);
        assert_eq!(0, runtime.state.queues.inventory_refresh.stats().depth);
        assert_eq!(0, runtime.state.queues.notifications.stats().depth);
        assert_eq!(1, runtime.state.injection_worker.client_count());
        assert_eq!(Duration::from_secs(300), runtime.state.saved_retry_interval);
        assert_eq!(
            crate::runtime::shutdown::ShutdownPhase::Running,
            runtime.state.shutdown.state().phase
        );
    }

    #[tokio::test]
    async fn search_workflow_planning_uses_arr_ids() {
        let torznab_queries = Arc::new(Mutex::new(Vec::new()));
        let torznab_url = spawn_runtime_torznab_search_server(Arc::clone(&torznab_queries)).await;
        let arr_url = spawn_runtime_arr_parse_server(
            Arc::new(AtomicUsize::new(0)),
            StatusCode::OK,
            r#"{"movie":{"tmdbId":99}}"#,
        )
        .await;
        let mut config = SporosConfig::default();
        config.indexers.torznab.insert(
            "main".to_owned(),
            TorznabIndexerConfig {
                url: torznab_url,
                api_key: Some(ApiKey::new("indexer-secret").unwrap()),
                api_key_file: None,
                api_key_env: None,
            },
        );
        config.indexers.arr.radarr.insert(
            "main".to_owned(),
            ArrInstanceConfig {
                url: arr_url,
                api_key: Some(ApiKey::new("arr-secret").unwrap()),
                api_key_file: None,
                api_key_env: None,
            },
        );
        let repository = Repository::connect_in_memory().await.unwrap();
        let runtime = AppRuntime::from_repository(config, repository.clone())
            .await
            .unwrap();
        repository
            .record_indexer_caps_success(&DependencyName::new("main").unwrap(), &movie_caps(), 100)
            .await
            .unwrap();

        let summary = runtime
            .state
            .plan_search_workflow(
                SearchWorkflowRequest {
                    query: ItemTitle::new("Example.Movie.1080p").unwrap(),
                },
                1_000,
            )
            .await
            .unwrap();

        assert_eq!(1, summary.plans.len());
        assert_eq!(
            Some("99"),
            summary.plans[0].plan.plan.query.ids.tmdb_id.as_deref()
        );
        assert_eq!(None, summary.plans[0].plan.plan.query.q);
        assert!(summary.plans[0].plan.cache_key.as_str().contains("tmdb:99"));
        assert_eq!(1, summary.candidate_count);
        let queries = torznab_queries.lock().unwrap();
        assert_eq!(1, queries.len());
        assert!(queries[0].contains("tmdbid=99"));
        assert!(!queries[0].contains("q=Example"));
    }

    #[tokio::test]
    async fn indexer_caps_refresh_honors_persisted_backoff() {
        let caps_requests = Arc::new(Mutex::new(Vec::new()));
        let torznab_url = spawn_runtime_torznab_search_server(Arc::clone(&caps_requests)).await;
        let mut config = SporosConfig::default();
        config.indexers.torznab.insert(
            "main".to_owned(),
            TorznabIndexerConfig {
                url: torznab_url,
                api_key: Some(ApiKey::new("indexer-secret").unwrap()),
                api_key_file: None,
                api_key_env: None,
            },
        );
        let repository = Repository::connect_in_memory().await.unwrap();
        let runtime = AppRuntime::from_repository(config, repository.clone())
            .await
            .unwrap();
        repository
            .record_indexer_caps_failure(
                &DependencyName::new("main").unwrap(),
                &ReasonText::new("rate limited").unwrap(),
                Some(2_000),
                1_000,
            )
            .await
            .unwrap();

        let skipped = runtime
            .state
            .refresh_indexer_capabilities(1_500)
            .await
            .unwrap();
        let refreshed = runtime
            .state
            .refresh_indexer_capabilities(2_000)
            .await
            .unwrap();
        let queries = caps_requests.lock().unwrap();

        assert_eq!(1, skipped.skipped_backoff);
        assert_eq!(0, skipped.refreshed);
        assert_eq!(1, refreshed.refreshed);
        assert_eq!(1, queries.len());
        assert!(queries[0].contains("t=caps"));
    }

    #[tokio::test]
    async fn runtime_restores_arr_backoff_before_search_planning() {
        let torznab_queries = Arc::new(Mutex::new(Vec::new()));
        let torznab_url = spawn_runtime_torznab_search_server(Arc::clone(&torznab_queries)).await;
        let requests = Arc::new(AtomicUsize::new(0));
        let arr_url = spawn_runtime_arr_parse_server(
            Arc::clone(&requests),
            StatusCode::OK,
            r#"{"movie":{"tmdbId":99}}"#,
        )
        .await;
        let mut config = SporosConfig::default();
        config.indexers.torznab.insert(
            "main".to_owned(),
            TorznabIndexerConfig {
                url: torznab_url,
                api_key: Some(ApiKey::new("indexer-secret").unwrap()),
                api_key_file: None,
                api_key_env: None,
            },
        );
        config.indexers.arr.radarr.insert(
            "main".to_owned(),
            ArrInstanceConfig {
                url: arr_url,
                api_key: Some(ApiKey::new("arr-secret").unwrap()),
                api_key_file: None,
                api_key_env: None,
            },
        );
        let repository = Repository::connect_in_memory().await.unwrap();
        repository
            .record_dependency_health(
                "arr",
                &DependencyName::new("radarr-main").unwrap(),
                &DependencyState::Degraded {
                    reason: ReasonText::new("rate limited").unwrap(),
                    retry_after_ms: Some(i64::MAX / 2),
                },
                100,
            )
            .await
            .unwrap();
        let runtime = AppRuntime::from_repository(config, repository.clone())
            .await
            .unwrap();
        repository
            .record_indexer_caps_success(&DependencyName::new("main").unwrap(), &movie_caps(), 100)
            .await
            .unwrap();

        let summary = runtime
            .state
            .plan_search_workflow(
                SearchWorkflowRequest {
                    query: ItemTitle::new("Example.Movie.1080p").unwrap(),
                },
                1_000,
            )
            .await
            .unwrap();

        assert_eq!(0, requests.load(Ordering::SeqCst));
        assert_eq!(1, summary.plans.len());
        assert_eq!(
            Some("Example.Movie.1080p"),
            summary.plans[0].plan.plan.query.q.as_deref()
        );
        assert!(summary.plans[0].plan.plan.query.ids.is_empty());
        assert_eq!(1, summary.candidate_count);
        let health = runtime.state.health.snapshot();
        assert_eq!(
            Some(&crate::runtime::health::DependencySummary::Degraded),
            health
                .summaries
                .get(&crate::runtime::health::DependencyKind::Arr)
        );
        let queries = torznab_queries.lock().unwrap();
        assert_eq!(1, queries.len());
        assert!(queries[0].contains("q=Example"));
    }

    #[tokio::test]
    async fn search_workflow_planning_uses_sonarr_episode_ids() {
        let torznab_queries = Arc::new(Mutex::new(Vec::new()));
        let torznab_url = spawn_runtime_torznab_search_server(Arc::clone(&torznab_queries)).await;
        let arr_url = spawn_runtime_arr_parse_server(
            Arc::new(AtomicUsize::new(0)),
            StatusCode::OK,
            r#"{"series":{"tvdbId":42}}"#,
        )
        .await;
        let mut config = SporosConfig::default();
        config.indexers.torznab.insert(
            "main".to_owned(),
            TorznabIndexerConfig {
                url: torznab_url,
                api_key: Some(ApiKey::new("indexer-secret").unwrap()),
                api_key_file: None,
                api_key_env: None,
            },
        );
        config.indexers.arr.sonarr.insert(
            "main".to_owned(),
            ArrInstanceConfig {
                url: arr_url,
                api_key: Some(ApiKey::new("arr-secret").unwrap()),
                api_key_file: None,
                api_key_env: None,
            },
        );
        let repository = Repository::connect_in_memory().await.unwrap();
        let runtime = AppRuntime::from_repository(config, repository.clone())
            .await
            .unwrap();
        repository
            .record_indexer_caps_success(&DependencyName::new("main").unwrap(), &tv_caps(), 100)
            .await
            .unwrap();

        let summary = runtime
            .state
            .plan_search_workflow(
                SearchWorkflowRequest {
                    query: ItemTitle::new("Example.Show.S01E02.1080p").unwrap(),
                },
                1_000,
            )
            .await
            .unwrap();

        assert_eq!(1, summary.plans.len());
        assert_eq!(
            Some("42"),
            summary.plans[0].plan.plan.query.ids.tvdb_id.as_deref()
        );
        assert_eq!(Some(1), summary.plans[0].plan.plan.query.season);
        assert_eq!(Some(2), summary.plans[0].plan.plan.query.episode);
        let queries = torznab_queries.lock().unwrap();
        assert_eq!(1, queries.len());
        assert!(queries[0].contains("tvdbid=42"));
        assert!(queries[0].contains("t=tvsearch"));
    }

    #[tokio::test]
    async fn runtime_exposes_receivers_for_owned_workers() {
        let config = SporosConfig::default();
        let repository = Repository::connect_in_memory().await.unwrap();
        let mut runtime = AppRuntime::from_repository(config, repository)
            .await
            .unwrap();

        runtime
            .state
            .queues
            .inventory_refresh
            .try_enqueue(InventoryRefreshRequest {
                media_dirs: Vec::new(),
            })
            .unwrap();
        let received = runtime.receivers.inventory_refresh.recv().await.unwrap();

        assert!(received.media_dirs.is_empty());
    }

    #[tokio::test]
    async fn runtime_streams_qbit_inventory_into_refresh_persistence() {
        let info_requests = Arc::new(AtomicUsize::new(0));
        let endpoint = spawn_runtime_qbit_inventory_server(info_requests.clone()).await;
        let mut config = SporosConfig::default();
        config.torrent_clients.insert(
            "qbit".to_owned(),
            TorrentClientConfig {
                kind: ConfigTorrentClientKind::Qbittorrent,
                url: endpoint,
                username: None,
                password: None,
                password_file: None,
                password_env: None,
                default_save_path: "/downloads/default".into(),
                label_field: None,
            },
        );
        let repository = Repository::connect_in_memory().await.unwrap();
        let runtime = AppRuntime::from_repository(config, repository.clone())
            .await
            .unwrap();

        let summaries = runtime
            .state
            .refresh_torrent_client_inventories()
            .await
            .unwrap();

        let item_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM local_items")
            .fetch_one(repository.pool())
            .await
            .unwrap();
        let file_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM local_files")
            .fetch_one(repository.pool())
            .await
            .unwrap();

        assert_eq!(1, summaries.len());
        assert_eq!(2, summaries[0].persisted_items);
        assert_eq!(2, item_count);
        assert_eq!(2, file_count);
        assert_eq!(1, info_requests.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn runtime_cancels_qbit_inventory_during_file_fetch() {
        let file_requests = Arc::new(AtomicUsize::new(0));
        let endpoint = spawn_runtime_qbit_blocked_files_server(file_requests.clone()).await;
        let mut config = SporosConfig::default();
        config.torrent_clients.insert(
            "qbit".to_owned(),
            TorrentClientConfig {
                kind: ConfigTorrentClientKind::Qbittorrent,
                url: endpoint,
                username: None,
                password: None,
                password_file: None,
                password_env: None,
                default_save_path: "/downloads/default".into(),
                label_field: None,
            },
        );
        let repository = Repository::connect_in_memory().await.unwrap();
        let runtime = AppRuntime::from_repository(config, repository.clone())
            .await
            .unwrap();
        let state = runtime.state.clone();
        let refresh = tokio::spawn(async move { state.refresh_torrent_client_inventories().await });

        tokio::time::timeout(Duration::from_secs(1), async {
            while file_requests.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        runtime.state.shutdown.cancel_now("test shutdown").unwrap();
        let error = tokio::time::timeout(Duration::from_secs(1), refresh)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        let item_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM local_items")
            .fetch_one(repository.pool())
            .await
            .unwrap();

        assert!(matches!(
            error,
            InventoryRefreshError::Client {
                source: TorrentClientError::Cancelled { .. }
            }
        ));
        assert_eq!(0, item_count);
    }

    #[tokio::test]
    async fn runtime_streams_rtorrent_inventory_into_refresh_persistence() {
        let inventory_requests = Arc::new(AtomicUsize::new(0));
        let endpoint = spawn_runtime_rtorrent_inventory_server(inventory_requests.clone()).await;
        let mut config = SporosConfig::default();
        config.torrent_clients.insert(
            "rtorrent".to_owned(),
            TorrentClientConfig {
                kind: ConfigTorrentClientKind::Rtorrent,
                url: endpoint,
                username: None,
                password: None,
                password_file: None,
                password_env: None,
                default_save_path: "/downloads/default".into(),
                label_field: Some("custom1".to_owned()),
            },
        );
        let repository = Repository::connect_in_memory().await.unwrap();
        let runtime = AppRuntime::from_repository(config, repository.clone())
            .await
            .unwrap();

        let summaries = runtime
            .state
            .refresh_torrent_client_inventories()
            .await
            .unwrap();

        let item_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM local_items")
            .fetch_one(repository.pool())
            .await
            .unwrap();
        let file_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM local_files")
            .fetch_one(repository.pool())
            .await
            .unwrap();

        assert_eq!(1, summaries.len());
        assert_eq!(1, summaries[0].persisted_items);
        assert_eq!(1, item_count);
        assert_eq!(1, file_count);
        assert_eq!(1, inventory_requests.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn runtime_cancels_rtorrent_inventory_during_file_fetch() {
        let file_requests = Arc::new(AtomicUsize::new(0));
        let endpoint = spawn_runtime_rtorrent_blocked_files_server(file_requests.clone()).await;
        let mut config = SporosConfig::default();
        config.torrent_clients.insert(
            "rtorrent".to_owned(),
            TorrentClientConfig {
                kind: ConfigTorrentClientKind::Rtorrent,
                url: endpoint,
                username: None,
                password: None,
                password_file: None,
                password_env: None,
                default_save_path: "/downloads/default".into(),
                label_field: Some("custom1".to_owned()),
            },
        );
        let repository = Repository::connect_in_memory().await.unwrap();
        let runtime = AppRuntime::from_repository(config, repository.clone())
            .await
            .unwrap();
        let state = runtime.state.clone();
        let refresh = tokio::spawn(async move { state.refresh_torrent_client_inventories().await });

        tokio::time::timeout(Duration::from_secs(1), async {
            while file_requests.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        runtime.state.shutdown.cancel_now("test shutdown").unwrap();
        let error = tokio::time::timeout(Duration::from_secs(1), refresh)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        let item_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM local_items")
            .fetch_one(repository.pool())
            .await
            .unwrap();

        assert!(matches!(
            error,
            InventoryRefreshError::Client {
                source: TorrentClientError::Cancelled { .. }
            }
        ));
        assert_eq!(0, item_count);
    }

    #[tokio::test]
    async fn runtime_rejects_unsupported_rtorrent_label_field() {
        let mut config = SporosConfig::default();
        config.torrent_clients.insert(
            "rtorrent".to_owned(),
            TorrentClientConfig {
                kind: ConfigTorrentClientKind::Rtorrent,
                url: "http://rtorrent:5000/RPC2".to_owned(),
                username: None,
                password: None,
                password_file: None,
                password_env: None,
                default_save_path: "/downloads".into(),
                label_field: Some("custom2".to_owned()),
            },
        );
        let repository = Repository::connect_in_memory().await.unwrap();

        let error = AppRuntime::from_repository(config, repository)
            .await
            .unwrap_err();

        assert!(error.to_string().contains("label_field custom1"));
    }

    #[tokio::test]
    async fn runtime_rejects_unsupported_rtorrent_auth_fields() {
        let mut config = SporosConfig::default();
        config.torrent_clients.insert(
            "rtorrent".to_owned(),
            TorrentClientConfig {
                kind: ConfigTorrentClientKind::Rtorrent,
                url: "http://rtorrent:5000/RPC2".to_owned(),
                username: Some("sporos".to_owned()),
                password: None,
                password_file: None,
                password_env: None,
                default_save_path: "/downloads".into(),
                label_field: Some("custom1".to_owned()),
            },
        );
        let repository = Repository::connect_in_memory().await.unwrap();

        let error = AppRuntime::from_repository(config, repository)
            .await
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("does not support configured auth")
        );
    }

    #[tokio::test]
    async fn runtime_accepts_durable_work_without_enabling_searches() {
        let config = SporosConfig::default();
        let repository = Repository::connect_in_memory().await.unwrap();
        let runtime = AppRuntime::from_repository(config, repository)
            .await
            .unwrap();
        let app = router(runtime.state.http.clone());

        let search = app.clone().oneshot(search_request(None)).await.unwrap();
        let job_run = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/jobs/indexer_caps/runs")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let unavailable_job = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/jobs/rss/runs")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let announcement = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/announcements")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"name":"Example","guid":"guid-1","download_url":"https://indexer.example/download","tracker":"tracker"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        let readyz = router(runtime.state.http.clone())
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/readyz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = router(runtime.state.http.clone())
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status_body = axum::body::to_bytes(status.into_body(), 65_536)
            .await
            .unwrap();
        let status_json: serde_json::Value = serde_json::from_slice(&status_body).unwrap();

        assert_eq!(StatusCode::SERVICE_UNAVAILABLE, search.status());
        assert_eq!(StatusCode::ACCEPTED, job_run.status());
        assert_eq!(StatusCode::NOT_FOUND, unavailable_job.status());
        assert_eq!(StatusCode::ACCEPTED, announcement.status());
        assert_eq!(StatusCode::SERVICE_UNAVAILABLE, readyz.status());
        assert_eq!(true, status_json["readiness"]["accepting_work"]);
        assert_eq!(false, status_json["readiness"]["processing_ready"]);
    }

    #[tokio::test]
    async fn runtime_http_exposes_shared_metrics_registry() {
        let config = SporosConfig::default();
        let repository = Repository::connect_in_memory().await.unwrap();
        let runtime = AppRuntime::from_repository(config, repository)
            .await
            .unwrap();
        runtime
            .state
            .metrics
            .record_notification_request(ExternalOutcome::Succeeded, 25);
        let app = router(runtime.state.http.clone());

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();

        assert!(text.contains("sporos_notification_requests_total"));
        assert!(text.contains("outcome=\"succeeded\""));
    }

    #[tokio::test]
    async fn runtime_qbittorrent_adapter_validates_before_inject() {
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
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move { axum::serve(listener, app).await });
        let descriptor = TorrentClientDescriptor {
            name: DisplayName::new("qbit").unwrap(),
            kind: TorrentClientKind::Qbittorrent,
            host: ClientHost::new(address.to_string()).unwrap(),
            url: format!("http://{address}"),
            default_save_path: "/downloads".into(),
            readonly: false,
            capabilities: TorrentClientCapabilities::for_kind(TorrentClientKind::Qbittorrent),
        };
        let config = TorrentClientConfig {
            kind: ConfigTorrentClientKind::Qbittorrent,
            url: format!("http://{address}"),
            username: None,
            password: None,
            password_file: None,
            password_env: None,
            default_save_path: "/downloads".into(),
            label_field: None,
        };
        let client = RuntimeInjectionClient::new("qbit", &config, descriptor);
        let info_hash = InfoHash::new("0123456789abcdef0123456789abcdef01234567").unwrap();

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
    }

    #[tokio::test]
    async fn runtime_composes_configured_api_auth() {
        let mut config = SporosConfig::default();
        config.server.api_token = Some(ApiToken::new("secret-token").unwrap());
        let repository = Repository::connect_in_memory().await.unwrap();
        let runtime = AppRuntime::from_repository(config, repository)
            .await
            .unwrap();
        assert!(!format!("{:?}", runtime.state.http).contains("secret-token"));
        let app = router(runtime.state.http.clone());

        let unauthorized = app.clone().oneshot(search_request(None)).await.unwrap();
        let unavailable = app
            .oneshot(search_request(Some("Bearer secret-token")))
            .await
            .unwrap();

        assert_eq!(StatusCode::UNAUTHORIZED, unauthorized.status());
        assert_eq!(StatusCode::SERVICE_UNAVAILABLE, unavailable.status());
    }

    async fn spawn_runtime_qbit_inventory_server(info_requests: Arc<AtomicUsize>) -> String {
        spawn_runtime_test_server(move |request| {
            let info_requests = info_requests.clone();
            async move {
                let path = request.uri().path().to_owned();
                match path.as_str() {
                    "/api/v2/auth/login" => response_with_cookie(StatusCode::OK, "Ok.", "SID=ok"),
                    "/api/v2/torrents/info" => {
                        info_requests.fetch_add(1, Ordering::SeqCst);
                        (
                            StatusCode::OK,
                            r#"[
                              {"hash":"0123456789abcdef0123456789abcdef01234567","name":"First","save_path":"/downloads/first","amount_left":0,"progress":1.0},
                              {"hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","name":"Second","save_path":"/downloads/second","amount_left":0,"progress":1.0}
                            ]"#,
                        )
                            .into_response()
                    }
                    "/api/v2/torrents/files" => (
                        StatusCode::OK,
                        r#"[{"name":"Example/file.mkv","size":42,"progress":1.0,"priority":1}]"#,
                    )
                        .into_response(),
                    _ => (StatusCode::NOT_FOUND, path).into_response(),
                }
            }
        })
        .await
    }

    async fn spawn_runtime_qbit_blocked_files_server(file_requests: Arc<AtomicUsize>) -> String {
        spawn_runtime_test_server(move |request| {
            let file_requests = file_requests.clone();
            async move {
                let path = request.uri().path().to_owned();
                match path.as_str() {
                    "/api/v2/auth/login" => response_with_cookie(StatusCode::OK, "Ok.", "SID=ok"),
                    "/api/v2/torrents/info" => (
                        StatusCode::OK,
                        r#"[
                          {"hash":"0123456789abcdef0123456789abcdef01234567","name":"First","save_path":"/downloads/first","amount_left":0,"progress":1.0}
                        ]"#,
                    )
                        .into_response(),
                    "/api/v2/torrents/files" => {
                        file_requests.fetch_add(1, Ordering::SeqCst);
                        pending::<Response>().await
                    }
                    _ => (StatusCode::NOT_FOUND, path).into_response(),
                }
            }
        })
        .await
    }

    async fn spawn_runtime_rtorrent_inventory_server(
        inventory_requests: Arc<AtomicUsize>,
    ) -> String {
        let app = axum::Router::new().route(
            "/RPC2",
            post(move |request: Request<Body>| {
                let inventory_requests = inventory_requests.clone();
                async move {
                    let body = to_bytes(request.into_body(), 65_536).await.unwrap();
                    let body = String::from_utf8(body.to_vec()).unwrap();
                    if body.contains("<methodName>download_list</methodName>") {
                        return (
                            StatusCode::OK,
                            xml_response(
                                r#"<array><data><value><string>0123456789abcdef0123456789abcdef01234567</string></value></data></array>"#,
                            ),
                        )
                            .into_response();
                    }
                    if body.contains("<methodName>system.multicall</methodName>")
                        && body.contains("d.custom1")
                    {
                        inventory_requests.fetch_add(1, Ordering::SeqCst);
                        return (
                            StatusCode::OK,
                            xml_response(
                                r#"<array><data>
                                  <value><array><data><value><string>Example</string></value></data></array></value>
                                  <value><array><data><value><string>/downloads/example</string></value></data></array></value>
                                  <value><array><data><value><i8>0</i8></value></data></array></value>
                                  <value><array><data><value><boolean>0</boolean></value></data></array></value>
                                  <value><array><data><value><boolean>1</boolean></value></data></array></value>
                                  <value><array><data><value><boolean>1</boolean></value></data></array></value>
                                  <value><array><data><value><boolean>0</boolean></value></data></array></value>
                                  <value><array><data><value><string>sporos</string></value></data></array></value>
                                </data></array>"#,
                            ),
                        )
                            .into_response();
                    }
                    if body.contains("<methodName>f.multicall</methodName>") {
                        return (
                            StatusCode::OK,
                            xml_response(
                                r#"<array><data><value><array><data>
                                  <value><string>Example/file.mkv</string></value>
                                  <value><i8>42</i8></value>
                                </data></array></value></data></array>"#,
                            ),
                        )
                            .into_response();
                    }
                    (StatusCode::BAD_REQUEST, body).into_response()
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{address}/RPC2")
    }

    async fn spawn_runtime_rtorrent_blocked_files_server(
        file_requests: Arc<AtomicUsize>,
    ) -> String {
        let app = axum::Router::new().route(
            "/RPC2",
            post(move |request: Request<Body>| {
                let file_requests = file_requests.clone();
                async move {
                    let body = to_bytes(request.into_body(), 65_536).await.unwrap();
                    let body = String::from_utf8(body.to_vec()).unwrap();
                    if body.contains("<methodName>download_list</methodName>") {
                        return (
                            StatusCode::OK,
                            xml_response(
                                r#"<array><data><value><string>0123456789abcdef0123456789abcdef01234567</string></value></data></array>"#,
                            ),
                        )
                            .into_response();
                    }
                    if body.contains("<methodName>system.multicall</methodName>")
                        && body.contains("d.custom1")
                    {
                        return (
                            StatusCode::OK,
                            xml_response(
                                r#"<array><data>
                                  <value><array><data><value><string>Example</string></value></data></array></value>
                                  <value><array><data><value><string>/downloads/example</string></value></data></array></value>
                                  <value><array><data><value><i8>0</i8></value></data></array></value>
                                  <value><array><data><value><boolean>0</boolean></value></data></array></value>
                                  <value><array><data><value><boolean>1</boolean></value></data></array></value>
                                  <value><array><data><value><boolean>1</boolean></value></data></array></value>
                                  <value><array><data><value><boolean>0</boolean></value></data></array></value>
                                  <value><array><data><value><string>sporos</string></value></data></array></value>
                                </data></array>"#,
                            ),
                        )
                            .into_response();
                    }
                    if body.contains("<methodName>f.multicall</methodName>") {
                        file_requests.fetch_add(1, Ordering::SeqCst);
                        return pending::<Response>().await;
                    }
                    (StatusCode::BAD_REQUEST, body).into_response()
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{address}/RPC2")
    }

    async fn spawn_runtime_test_server<F, Fut, R>(handler: F) -> String
    where
        F: Fn(Request<Body>) -> Fut + Clone + Send + Sync + 'static,
        Fut: Future<Output = R> + Send + 'static,
        R: IntoResponse + Send + 'static,
    {
        let app = axum::Router::new()
            .route("/api/v2/auth/login", post(handler.clone()))
            .route("/api/v2/torrents/info", get(handler.clone()))
            .route("/api/v2/torrents/files", get(handler));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{address}")
    }

    async fn spawn_runtime_arr_parse_server(
        requests: Arc<AtomicUsize>,
        status: StatusCode,
        body: &'static str,
    ) -> String {
        let app = axum::Router::new().route(
            "/api/v3/parse",
            get(move |_request: Request<Body>| {
                let requests = Arc::clone(&requests);
                async move {
                    requests.fetch_add(1, Ordering::SeqCst);
                    (status, body)
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{address}")
    }

    async fn spawn_runtime_torznab_search_server(queries: Arc<Mutex<Vec<String>>>) -> String {
        let app = axum::Router::new().route(
            "/api",
            get(move |request: Request<Body>| {
                let queries = Arc::clone(&queries);
                async move {
                    let query = request.uri().query().unwrap_or_default().to_owned();
                    queries.lock().unwrap().push(query.clone());
                    let body = if query.contains("t=caps") {
                        torznab_caps_xml().to_owned()
                    } else {
                        search_rss("candidate-1", "Example")
                    };
                    (StatusCode::OK, body)
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{address}/api")
    }

    fn movie_caps() -> TorznabCaps {
        TorznabCaps {
            search: SearchCaps {
                movie_search: true,
                supported_id_params: BTreeSet::from(["tmdbid".to_owned()]),
                ..SearchCaps::default()
            },
            categories: CategoryCaps {
                movie: true,
                ..CategoryCaps::default()
            },
            limits: TorznabLimits::default(),
        }
    }

    fn tv_caps() -> TorznabCaps {
        TorznabCaps {
            search: SearchCaps {
                tv_search: true,
                supported_id_params: BTreeSet::from(["tvdbid".to_owned()]),
                ..SearchCaps::default()
            },
            categories: CategoryCaps {
                tv: true,
                ..CategoryCaps::default()
            },
            limits: TorznabLimits::default(),
        }
    }

    fn search_rss(guid: &str, title: &str) -> String {
        format!(
            r#"
            <rss>
              <channel>
                <item>
                  <title>{title}</title>
                  <guid>{guid}</guid>
                  <link>https://indexer.example/download/{guid}</link>
                  <torznab:attr name="size" value="1234"/>
                  <torznab:attr name="infohash" value="0123456789abcdef0123456789abcdef01234567"/>
                </item>
              </channel>
            </rss>
            "#
        )
    }

    fn torznab_caps_xml() -> &'static str {
        r#"
        <caps>
          <limits default="50" max="200"/>
          <searching>
            <search available="yes" supportedParams="q"/>
            <movie-search available="yes" supportedParams="q,imdbid"/>
          </searching>
          <categories>
            <category id="2000" name="Movies"/>
          </categories>
        </caps>
        "#
    }

    fn response_with_cookie(
        status: StatusCode,
        body: &'static str,
        cookie: &'static str,
    ) -> Response {
        let mut response = (status, body).into_response();
        response
            .headers_mut()
            .insert(SET_COOKIE, cookie.parse().unwrap());
        response
    }

    fn xml_response(inner: &str) -> String {
        format!(
            r#"<?xml version="1.0"?><methodResponse><params><param><value>{inner}</value></param></params></methodResponse>"#
        )
    }

    fn search_request(auth: Option<&str>) -> Request<Body> {
        let mut builder = Request::builder()
            .method("POST")
            .uri("/v1/searches")
            .header("content-type", "application/json");
        if let Some(auth) = auth {
            builder = builder.header("authorization", auth);
        }
        builder.body(Body::from(r#"{"query":"Example"}"#)).unwrap()
    }
}
