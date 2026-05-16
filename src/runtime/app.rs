use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{Mutex as AsyncMutex, Semaphore, mpsc};
use tokio::task::JoinSet;

use crate::arr::{ArrEndpoint, ArrRegistry};
use crate::clients::qbittorrent::QbitTorrent;
use crate::clients::qbittorrent::{QbitAddTorrent, QbitContentLayout, QbittorrentClient};
use crate::clients::rtorrent::{RtorrentClient, RtorrentDownload};
use crate::clients::{TorrentClientDescriptor, TorrentClientRegistry};
use crate::config::{
    ConfigTorrentClientKind, ProwlarrRemovePolicy, SporosConfig, TorrentClientConfig,
};
use crate::domain::{
    ByteSize, DependencyName, DependencyState, DisplayName, IndexerId, InfoHash, ItemTitle,
    LocalItem, LocalItemSource, MediaType, ReasonText, RemoteCandidate, SourceKey, TorrentFile,
};
use crate::errors::{DatabaseError, TorrentClientError};
use crate::http::{
    AnnouncementWorkflowRequest, HttpState, JobRunWorkflowRequest, ReadinessState,
    SearchWorkflowRequest, WorkflowQueues,
};
use crate::indexers::{
    ConfiguredTorznabIndexer, IndexerBackoffPolicy, ProwlarrConfigError, ProwlarrHttpClient,
    ProwlarrRequestError, ProwlarrSource, SanitizedTorznabUrl, TorznabCaps, TorznabEndpoint,
    TorznabHttpClient, TorznabRegistry, TorznabRequestError,
};
use crate::inventory::InventoryScanOptions;
use crate::inventory_refresh::{
    ClientInventoryItem, ClientInventoryMessage, InventoryRefreshError, InventoryRefreshRequest,
    InventoryRefreshSummary, InventoryRefreshWorker, inventory_refresh_queue,
};
use crate::metrics::{MetricsRegistry, ProwlarrRefreshOutcome};
use crate::notifications::{NotificationJob, notification_queue};
use crate::persistence::repository::{IndexerRegistryRow, IndexerSearchCapsRow, Repository};
use crate::runtime::announce_worker::AnnounceWorker;
use crate::runtime::health::{DependencyKind, HealthRegistry};
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
const PROWLARR_REFRESH_CONCURRENCY: usize = 4;
const INDEXER_CAPS_REFRESH_PAGE_SIZE: u16 = 1_000;
const INDEXER_SEARCH_CAPS_PAGE_SIZE: u16 = 1_000;

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
    pub prowlarr_sources: BTreeMap<DependencyName, RuntimeProwlarrSource>,
    pub torznab_client: TorznabHttpClient,
    pub prowlarr_client: ProwlarrHttpClient,
    pub saved_retry_interval: Duration,
    pub shutdown: ShutdownController,
    pub shutdown_signal: ShutdownSignal,
}

#[derive(Debug, Clone)]
pub struct RuntimeProwlarrSource {
    pub source: ProwlarrSource,
    pub update_interval_ms: i64,
    pub initial_refresh_after_ms: i64,
    pub refresh_on_startup: bool,
    pub required: bool,
    pub remove_policy: ProwlarrRemovePolicy,
}

#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct ProwlarrRefreshSummary {
    pub refreshed: usize,
    pub failed: usize,
    pub skipped_backoff: usize,
    pub skipped_interval: usize,
    pub skipped_shutdown: usize,
    pub imported: usize,
    pub deactivated: u64,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
struct ProwlarrRefreshResult {
    imported: usize,
    deactivated: u64,
}

#[derive(Debug)]
enum ProwlarrRefreshError {
    Local(DatabaseError),
    Remote(DatabaseError),
}

impl ProwlarrRefreshError {
    fn as_database_error(&self) -> &DatabaseError {
        match self {
            Self::Local(error) | Self::Remote(error) => error,
        }
    }

    fn into_database_error(self) -> DatabaseError {
        match self {
            Self::Local(error) | Self::Remote(error) => error,
        }
    }

    fn optional_suppressible(&self) -> bool {
        matches!(self, Self::Remote(_))
    }
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

    pub async fn refresh_startup_prowlarr_sources(
        &self,
        now_ms: i64,
    ) -> Result<ProwlarrRefreshSummary, DatabaseError> {
        let sources = self
            .prowlarr_sources
            .values()
            .filter(|source| source.refresh_on_startup)
            .collect::<Vec<_>>();
        self.refresh_selected_prowlarr_sources(sources, now_ms, false)
            .await
    }

    pub async fn refresh_due_prowlarr_sources(
        &self,
        now_ms: i64,
    ) -> Result<ProwlarrRefreshSummary, DatabaseError> {
        let persisted = self.repository.dependency_health_snapshot(1_000).await?;
        let mut sources = Vec::new();
        let mut summary = ProwlarrRefreshSummary::default();
        for source in self.prowlarr_sources.values() {
            let Some(row) = persisted.iter().find(|row| {
                row.dependency_type == "prowlarr" && row.dependency_name == source.source.name
            }) else {
                if source.initial_refresh_after_ms <= now_ms {
                    sources.push(source);
                } else {
                    summary.skipped_interval += 1;
                }
                continue;
            };
            if let Some(retry_after) = row.retry_after_ms {
                if retry_after > now_ms {
                    summary.skipped_backoff += 1;
                } else {
                    sources.push(source);
                }
                continue;
            }
            let due_at = row
                .checked_at_ms
                .saturating_add(source.update_interval_ms)
                .saturating_add(prowlarr_refresh_jitter_ms(
                    &source.source.name,
                    source.update_interval_ms,
                ));
            if due_at > now_ms {
                summary.skipped_interval += 1;
                continue;
            }
            sources.push(source);
        }
        let refreshed = self
            .refresh_selected_prowlarr_sources(sources, now_ms, true)
            .await?;
        summary.refreshed += refreshed.refreshed;
        summary.failed += refreshed.failed;
        summary.skipped_backoff += refreshed.skipped_backoff;
        summary.skipped_interval += refreshed.skipped_interval;
        summary.skipped_shutdown += refreshed.skipped_shutdown;
        summary.imported += refreshed.imported;
        summary.deactivated += refreshed.deactivated;
        summary.last_error = refreshed.last_error;
        Ok(summary)
    }

    async fn refresh_selected_prowlarr_sources(
        &self,
        sources: Vec<&RuntimeProwlarrSource>,
        now_ms: i64,
        optional_failures_only: bool,
    ) -> Result<ProwlarrRefreshSummary, DatabaseError> {
        let mut summary = ProwlarrRefreshSummary::default();
        let semaphore = Arc::new(Semaphore::new(PROWLARR_REFRESH_CONCURRENCY));
        let mut tasks = JoinSet::new();
        for source in sources.into_iter().cloned() {
            if self.shutdown_signal.state().phase != ShutdownPhase::Running {
                summary.skipped_shutdown += 1;
                break;
            }
            let state = self.clone();
            let semaphore = Arc::clone(&semaphore);
            tasks.spawn(async move {
                let _permit = semaphore.acquire_owned().await.map_err(|error| {
                    ProwlarrRefreshError::Local(DatabaseError::Unavailable {
                        operation: "refresh Prowlarr source".to_owned(),
                        message: error.to_string(),
                    })
                })?;
                let required = source.required;
                let result = state
                    .refresh_prowlarr_source_until_shutdown(&source, now_ms)
                    .await;
                Ok::<_, ProwlarrRefreshError>((required, result))
            });
        }

        while let Some(result) = tasks.join_next().await {
            let task_result = result.map_err(|error| DatabaseError::Unavailable {
                operation: "refresh Prowlarr source".to_owned(),
                message: error.to_string(),
            })?;
            let (required, result) =
                task_result.map_err(ProwlarrRefreshError::into_database_error)?;
            match result {
                Ok(result) => {
                    summary.refreshed += 1;
                    summary.imported += result.imported;
                    summary.deactivated += result.deactivated;
                }
                Err(error) => {
                    summary.failed += 1;
                    summary.last_error = Some(error.as_database_error().to_string());
                    if !error.optional_suppressible() || (required && !optional_failures_only) {
                        tasks.abort_all();
                        return Err(error.into_database_error());
                    }
                }
            }
        }
        Ok(summary)
    }

    async fn refresh_prowlarr_source_until_shutdown(
        &self,
        source: &RuntimeProwlarrSource,
        now_ms: i64,
    ) -> Result<ProwlarrRefreshResult, ProwlarrRefreshError> {
        let mut shutdown = self.shutdown_signal.clone();
        let refresh = self.refresh_prowlarr_source(source, now_ms);
        tokio::select! {
            result = refresh => result,
            _state = shutdown.cancelled() => {
                Err(ProwlarrRefreshError::Remote(DatabaseError::Unavailable {
                    operation: "refresh Prowlarr source".to_owned(),
                    message: "shutdown requested".to_owned(),
                }))
            }
        }
    }

    async fn refresh_prowlarr_source(
        &self,
        source: &RuntimeProwlarrSource,
        now_ms: i64,
    ) -> Result<ProwlarrRefreshResult, ProwlarrRefreshError> {
        let started = Instant::now();
        match self.prowlarr_client.indexers(&source.source).await {
            Ok(indexers) => {
                match self
                    .repository
                    .sync_prowlarr_indexers_with_summary(
                        &source.source.name,
                        &indexers,
                        source.remove_policy,
                        now_ms,
                    )
                    .await
                {
                    Ok(sync) => {
                        self.metrics.record_prowlarr_refresh(
                            source.source.name.as_str(),
                            ProwlarrRefreshOutcome::Succeeded,
                            elapsed_ms(started),
                            sync.imported as u64,
                            sync.deactivated,
                        );
                        self.record_prowlarr_health(
                            &source.source.name,
                            DependencyState::Healthy {
                                checked_at_ms: now_ms,
                            },
                            now_ms,
                        )
                        .await
                        .map_err(ProwlarrRefreshError::Local)?;
                        Ok(ProwlarrRefreshResult {
                            imported: sync.imported,
                            deactivated: sync.deactivated,
                        })
                    }
                    Err(error) => {
                        self.metrics.record_prowlarr_refresh(
                            source.source.name.as_str(),
                            ProwlarrRefreshOutcome::Failed,
                            elapsed_ms(started),
                            0,
                            0,
                        );
                        Err(ProwlarrRefreshError::Local(error))
                    }
                }
            }
            Err(error) => {
                self.metrics.record_prowlarr_refresh(
                    source.source.name.as_str(),
                    prowlarr_refresh_outcome(&error),
                    elapsed_ms(started),
                    0,
                    0,
                );
                let state = prowlarr_error_dependency_state(&error, now_ms);
                self.record_prowlarr_health(&source.source.name, state.clone(), now_ms)
                    .await
                    .map_err(ProwlarrRefreshError::Local)?;
                Err(ProwlarrRefreshError::Remote(DatabaseError::Unavailable {
                    operation: format!("refresh Prowlarr source {}", source.source.name.as_str()),
                    message: error.to_string(),
                }))
            }
        }
    }

    async fn record_prowlarr_health(
        &self,
        name: &DependencyName,
        state: DependencyState,
        now_ms: i64,
    ) -> Result<(), DatabaseError> {
        self.repository
            .record_dependency_health("prowlarr", name, &state, now_ms)
            .await?;
        match state {
            DependencyState::Healthy { checked_at_ms } => {
                self.health
                    .set_healthy(DependencyKind::Prowlarr, name.clone(), checked_at_ms);
            }
            DependencyState::Degraded {
                reason,
                retry_after_ms,
            } => self.health.set_degraded(
                DependencyKind::Prowlarr,
                name.clone(),
                reason,
                retry_after_ms,
            ),
            DependencyState::Unavailable {
                reason,
                retry_after_ms,
            } => self.health.set_unavailable(
                DependencyKind::Prowlarr,
                name.clone(),
                reason,
                retry_after_ms,
            ),
            DependencyState::Unknown => self
                .health
                .set_unknown(DependencyKind::Prowlarr, name.clone()),
        }
        Ok(())
    }

    fn search_caps_endpoint(
        &self,
        row: &IndexerSearchCapsRow,
    ) -> Result<Option<TorznabEndpoint>, DatabaseError> {
        self.indexer_endpoint(
            row.indexer_id,
            &row.name,
            &row.url,
            &row.source_kind,
            &row.source_name,
            row.caps.clone(),
            row.retry_after_ms,
        )
    }

    fn registry_endpoint(
        &self,
        row: &IndexerRegistryRow,
        caps: TorznabCaps,
    ) -> Result<Option<TorznabEndpoint>, DatabaseError> {
        let indexer_id = IndexerId::new(row.id).map_err(|error| DatabaseError::QueryFailed {
            operation: "build runtime indexer id".to_owned(),
            message: error.to_string(),
        })?;
        self.indexer_endpoint(
            indexer_id,
            &row.name,
            &row.url,
            &row.source_kind,
            &row.source_name,
            caps,
            row.retry_after_ms,
        )
    }

    fn indexer_endpoint(
        &self,
        indexer_id: IndexerId,
        name: &DependencyName,
        url: &str,
        source_kind: &str,
        source_name: &str,
        caps: TorznabCaps,
        retry_after_ms: Option<i64>,
    ) -> Result<Option<TorznabEndpoint>, DatabaseError> {
        if source_kind == "static" {
            let Some(configured) = self.torznab_indexers.get(name) else {
                return Ok(None);
            };
            return Ok(Some(TorznabEndpoint {
                indexer_id,
                name: name.clone(),
                url: configured.url.clone(),
                api_key: configured
                    .api_key
                    .as_ref()
                    .map(|api_key| api_key.expose_secret().to_owned()),
                caps,
                retry_after_ms,
            }));
        }
        if source_kind == "prowlarr" {
            let source_name = DependencyName::new(source_name.to_owned()).map_err(|error| {
                DatabaseError::QueryFailed {
                    operation: "build Prowlarr source name".to_owned(),
                    message: error.to_string(),
                }
            })?;
            let Some(source) = self.prowlarr_sources.get(&source_name) else {
                return Ok(None);
            };
            return Ok(Some(TorznabEndpoint {
                indexer_id,
                name: name.clone(),
                url: SanitizedTorznabUrl::new(url.to_owned()).map_err(|error| {
                    DatabaseError::QueryFailed {
                        operation: "build Prowlarr Torznab endpoint".to_owned(),
                        message: error.to_string(),
                    }
                })?,
                api_key: Some(source.source.api_key.expose_secret().to_owned()),
                caps,
                retry_after_ms,
            }));
        }
        Ok(None)
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
        let mut plans = Vec::new();
        let mut candidate_count = 0_usize;
        let mut all_candidates = Vec::new();
        let mut failed_indexers = 0_usize;
        let mut after_name = None;

        loop {
            let indexers = self
                .repository
                .ready_indexer_search_caps_page(
                    now_ms,
                    after_name.as_ref(),
                    INDEXER_SEARCH_CAPS_PAGE_SIZE,
                )
                .await;
            let indexers = indexers?;
            let is_last_page = indexers.len() < usize::from(INDEXER_SEARCH_CAPS_PAGE_SIZE);
            for indexer in indexers {
                after_name = Some(indexer.name.clone());
                let Some(plan) = plan_runtime_torznab_search(&item, &ids, &indexer.caps) else {
                    continue;
                };
                let Some(endpoint) = self.search_caps_endpoint(&indexer)? else {
                    continue;
                };
                plans.push(IndexerSearchPlan {
                    indexer_id: indexer.indexer_id,
                    indexer_name: indexer.name.clone(),
                    plan: plan.clone(),
                });
                let candidates = self
                    .torznab_client
                    .search(&endpoint, item.media_type, &plan.plan, now_ms)
                    .await;
                match candidates {
                    Ok(candidates) => {
                        candidate_count = candidate_count.saturating_add(candidates.len());
                        all_candidates.extend(candidates);
                    }
                    Err(error) => {
                        tracing::warn!(
                            indexer_name = %endpoint.name,
                            error = %error,
                            "Torznab search failed"
                        );
                        let message = error.to_string();
                        let reason = health_reason(Some(&message), "search failed")
                            .unwrap_or_else(|| ReasonText::new("search failed").unwrap());
                        let retry_after_ms = indexer_error_retry_after(&error, now_ms);
                        self.repository
                            .record_indexer_request_backoff(
                                &endpoint.name,
                                &reason,
                                retry_after_ms,
                                now_ms,
                                indexer_error_is_unavailable(&error),
                            )
                            .await?;
                        failed_indexers += 1;
                    }
                }
            }
            if is_last_page {
                break;
            }
        }

        Ok(SearchWorkflowPlanSummary {
            plans,
            candidate_count,
            candidates: all_candidates,
            failed_indexers,
        })
    }

    pub async fn refresh_indexer_capabilities(
        &self,
        now_ms: i64,
    ) -> Result<IndexerCapsRefreshSummary, DatabaseError> {
        let mut summary = IndexerCapsRefreshSummary::default();
        let mut last_error = None;
        let (skipped_backoff, next_backoff_deadline_ms) =
            self.repository.indexer_caps_backoff_summary(now_ms).await?;
        summary.skipped_backoff = skipped_backoff;
        summary.next_backoff_deadline_ms = next_backoff_deadline_ms;
        let mut after_name = None;

        loop {
            let registry = self
                .repository
                .due_indexer_registry_page(
                    now_ms,
                    after_name.as_ref(),
                    INDEXER_CAPS_REFRESH_PAGE_SIZE,
                )
                .await?;
            let is_last_page = registry.len() < usize::from(INDEXER_CAPS_REFRESH_PAGE_SIZE);
            for row in registry {
                after_name = Some(row.name.clone());
                let Some(endpoint) = self.registry_endpoint(&row, TorznabCaps::default())? else {
                    continue;
                };
                match self.torznab_client.caps_endpoint(&endpoint).await {
                    Ok(caps) => {
                        self.repository
                            .record_indexer_caps_success(&row.name, &caps, now_ms)
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
                                &row.name,
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
            if is_last_page {
                break;
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
    pub candidates: Vec<RemoteCandidate>,
    pub failed_indexers: usize,
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
        let prowlarr_sources = runtime_prowlarr_sources(&config, now_ms)?;
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
        let scheduler_config = daemon_scheduler_config(scheduler_config);
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
        )
        .with_season_from_episodes(config.matching.season_from_episodes);
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
            .with_search_queue(workflow.searches.clone())
            .with_job_queue(workflow.jobs.clone())
            .with_allowed_jobs(http_jobs)
            .with_announce_acceptor(repository.clone(), config.announce.clone());
        if let Some(api_token) = config.server.api_token.as_ref() {
            http = http.with_api_token(api_token.expose_secret());
        }

        let state = AppState {
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
            prowlarr_sources,
            torznab_client: TorznabHttpClient::new(Duration::from_secs(120)),
            prowlarr_client: ProwlarrHttpClient::new(Duration::from_secs(30)),
            saved_retry_interval,
            shutdown,
            shutdown_signal,
        };
        state.refresh_startup_prowlarr_sources(now_ms).await?;

        Ok(Self {
            state,
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

fn runtime_prowlarr_sources(
    config: &SporosConfig,
    now_ms: i64,
) -> Result<BTreeMap<DependencyName, RuntimeProwlarrSource>, DatabaseError> {
    let mut sources = BTreeMap::new();
    for (name, source_config) in &config.indexers.prowlarr {
        let Some(source) = ProwlarrSource::from_config(name, source_config)
            .map_err(|error| prowlarr_config_database_error("build Prowlarr source", error))?
        else {
            continue;
        };
        let update_interval_ms =
            parse_interval_ms(&source_config.update_interval).map_err(|error| {
                DatabaseError::Unavailable {
                    operation: "build Prowlarr source interval".to_owned(),
                    message: error.to_string(),
                }
            })?;
        sources.insert(
            source.name.clone(),
            RuntimeProwlarrSource {
                initial_refresh_after_ms: if source_config.refresh_on_startup {
                    0
                } else {
                    now_ms.saturating_add(update_interval_ms).saturating_add(
                        prowlarr_refresh_jitter_ms(&source.name, update_interval_ms),
                    )
                },
                source,
                update_interval_ms,
                refresh_on_startup: source_config.refresh_on_startup,
                required: source_config.required,
                remove_policy: source_config.remove_policy,
            },
        );
    }
    Ok(sources)
}

fn prowlarr_config_database_error(
    operation: &'static str,
    error: ProwlarrConfigError,
) -> DatabaseError {
    DatabaseError::Unavailable {
        operation: operation.to_owned(),
        message: error.to_string(),
    }
}

fn prowlarr_refresh_jitter_ms(name: &DependencyName, interval_ms: i64) -> i64 {
    let max_jitter = (interval_ms / 10).clamp(0, 60_000);
    if max_jitter == 0 {
        return 0;
    }
    let hash = name.as_str().bytes().fold(0_u64, |accumulator, byte| {
        accumulator.wrapping_mul(31).wrapping_add(u64::from(byte))
    });
    i64::try_from(hash % u64::try_from(max_jitter).unwrap_or(1)).unwrap_or_default()
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
            "arr" => DependencyKind::Arr,
            "indexer" => DependencyKind::Indexer,
            "prowlarr" => DependencyKind::Prowlarr,
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

fn daemon_scheduler_config(mut config: SchedulerConfig) -> SchedulerConfig {
    config
        .jobs
        .retain(|job| matches!(job.name.as_str(), "indexer_caps"));
    config
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

fn prowlarr_error_dependency_state(error: &ProwlarrRequestError, now_ms: i64) -> DependencyState {
    let reason = health_reason(Some(&error.to_string()), "Prowlarr refresh failed")
        .unwrap_or_else(|| ReasonText::new("Prowlarr refresh failed").unwrap());
    let retry_after_ms = Some(prowlarr_error_retry_after(error, now_ms));
    match error {
        ProwlarrRequestError::HttpStatus { status, .. } if *status == 401 || *status == 403 => {
            DependencyState::Unavailable {
                reason,
                retry_after_ms,
            }
        }
        ProwlarrRequestError::InvalidResponse { .. }
        | ProwlarrRequestError::InvalidIndexer { .. } => DependencyState::Degraded {
            reason,
            retry_after_ms,
        },
        ProwlarrRequestError::HttpStatus { .. }
        | ProwlarrRequestError::Timeout
        | ProwlarrRequestError::Request { .. }
        | ProwlarrRequestError::ResponseTooLarge { .. } => DependencyState::Unavailable {
            reason,
            retry_after_ms,
        },
    }
}

fn prowlarr_refresh_outcome(error: &ProwlarrRequestError) -> ProwlarrRefreshOutcome {
    match error {
        ProwlarrRequestError::HttpStatus { status, .. } if *status == 429 => {
            ProwlarrRefreshOutcome::RateLimited
        }
        _ => ProwlarrRefreshOutcome::Failed,
    }
}

fn prowlarr_error_retry_after(error: &ProwlarrRequestError, now_ms: i64) -> i64 {
    let policy = IndexerBackoffPolicy::default();
    match error {
        ProwlarrRequestError::HttpStatus { retry_after, .. } => {
            policy.retry_after_deadline(now_ms, 0, *retry_after)
        }
        ProwlarrRequestError::Timeout
        | ProwlarrRequestError::Request { .. }
        | ProwlarrRequestError::InvalidResponse { .. }
        | ProwlarrRequestError::InvalidIndexer { .. }
        | ProwlarrRequestError::ResponseTooLarge { .. } => {
            policy.retry_after_deadline(now_ms, 0, None)
        }
    }
}

fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

fn indexer_error_is_unavailable(error: &TorznabRequestError) -> bool {
    match error {
        TorznabRequestError::Backoff { .. } | TorznabRequestError::RateLimited { .. } => false,
        TorznabRequestError::HttpStatus { status, .. } => *status >= 500,
        TorznabRequestError::Timeout
        | TorznabRequestError::Request { .. }
        | TorznabRequestError::InvalidXml { .. }
        | TorznabRequestError::InvalidCandidate { .. }
        | TorznabRequestError::ResponseTooLarge { .. } => true,
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
    use crate::config::{ArrInstanceConfig, ProwlarrSourceConfig, TorznabIndexerConfig};
    use crate::domain::{
        ClientHost, DependencyName, DependencyState, ItemTitle, ReasonText, TorrentClientKind,
    };
    use crate::http::router;
    use crate::indexers::{
        ApiKeySource, CategoryCaps, ProwlarrIndexer, SanitizedTorznabUrl, SearchCaps, TorznabCaps,
        TorznabLimits,
    };
    use crate::metrics::ExternalOutcome;
    use crate::secrets::{ApiKey, ApiToken};
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode, header::RETRY_AFTER, header::SET_COOKIE};
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
    async fn runtime_startup_refreshes_optional_prowlarr_sources() {
        let requests = Arc::new(AtomicUsize::new(0));
        let prowlarr_url = spawn_runtime_prowlarr_server(
            Arc::clone(&requests),
            StatusCode::OK,
            prowlarr_catalog(),
        )
        .await;
        let mut config = SporosConfig::default();
        config.indexers.prowlarr.insert(
            "main".to_owned(),
            test_prowlarr_config(prowlarr_url, true, false, "10m"),
        );
        let repository = Repository::connect_in_memory().await.unwrap();

        let runtime = AppRuntime::from_repository(config, repository.clone())
            .await
            .unwrap();
        let indexers = repository.indexer_registry_snapshot(10).await.unwrap();
        let health = repository.dependency_health_snapshot(10).await.unwrap();

        assert_eq!(1, requests.load(Ordering::SeqCst));
        assert_eq!(1, runtime.state.prowlarr_sources.len());
        assert!(
            indexers
                .iter()
                .any(|indexer| indexer.source_kind == "prowlarr"
                    && indexer.source_indexer_id == "101"
                    && indexer.enabled)
        );
        assert_eq!("prowlarr", health[0].dependency_type);
        assert_eq!("healthy", health[0].state);
    }

    #[tokio::test]
    async fn runtime_startup_degrades_optional_prowlarr_failure() {
        let prowlarr_url = spawn_runtime_prowlarr_server(
            Arc::new(AtomicUsize::new(0)),
            StatusCode::INTERNAL_SERVER_ERROR,
            "down",
        )
        .await;
        let mut config = SporosConfig::default();
        config.indexers.prowlarr.insert(
            "main".to_owned(),
            test_prowlarr_config(prowlarr_url, true, false, "10m"),
        );
        let repository = Repository::connect_in_memory().await.unwrap();

        let runtime = AppRuntime::from_repository(config, repository.clone())
            .await
            .unwrap();
        let health = repository.dependency_health_snapshot(10).await.unwrap();

        assert!(
            runtime
                .state
                .prowlarr_sources
                .contains_key(&DependencyName::new("main").unwrap())
        );
        assert_eq!("prowlarr", health[0].dependency_type);
        assert_eq!("unavailable", health[0].state);
        assert!(health[0].retry_after_ms.is_some());
    }

    #[tokio::test]
    async fn runtime_optional_prowlarr_refresh_surfaces_database_failure() {
        let requests = Arc::new(AtomicUsize::new(0));
        let prowlarr_url = spawn_runtime_prowlarr_server(
            Arc::clone(&requests),
            StatusCode::OK,
            prowlarr_catalog(),
        )
        .await;
        let mut config = SporosConfig::default();
        config.indexers.prowlarr.insert(
            "main".to_owned(),
            test_prowlarr_config(prowlarr_url, false, false, "24h"),
        );
        let repository = Repository::connect_in_memory().await.unwrap();
        let runtime = AppRuntime::from_repository(config, repository.clone())
            .await
            .unwrap();
        sqlx::query(
            r#"
            CREATE TRIGGER fail_prowlarr_sync
            BEFORE INSERT ON indexers
            BEGIN
                SELECT RAISE(FAIL, 'forced prowlarr sync failure');
            END;
            "#,
        )
        .execute(repository.pool())
        .await
        .unwrap();
        let due_at = runtime
            .state
            .prowlarr_sources
            .values()
            .next()
            .unwrap()
            .initial_refresh_after_ms;

        let error = runtime
            .state
            .refresh_due_prowlarr_sources(due_at)
            .await
            .unwrap_err();

        assert_eq!(1, requests.load(Ordering::SeqCst));
        assert!(error.to_string().contains("forced prowlarr sync failure"));
    }

    #[tokio::test]
    async fn runtime_startup_fails_required_prowlarr_failure() {
        let prowlarr_url = spawn_runtime_prowlarr_server(
            Arc::new(AtomicUsize::new(0)),
            StatusCode::UNAUTHORIZED,
            "bad key",
        )
        .await;
        let mut config = SporosConfig::default();
        config.indexers.prowlarr.insert(
            "main".to_owned(),
            test_prowlarr_config(prowlarr_url, true, true, "10m"),
        );
        let repository = Repository::connect_in_memory().await.unwrap();

        let error = AppRuntime::from_repository(config, repository)
            .await
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("Prowlarr returned HTTP status 401")
        );
    }

    #[tokio::test]
    async fn runtime_periodic_prowlarr_refresh_respects_interval_and_shutdown() {
        let requests = Arc::new(AtomicUsize::new(0));
        let prowlarr_url = spawn_runtime_prowlarr_server(
            Arc::clone(&requests),
            StatusCode::OK,
            prowlarr_catalog(),
        )
        .await;
        let mut config = SporosConfig::default();
        config.indexers.prowlarr.insert(
            "main".to_owned(),
            test_prowlarr_config(prowlarr_url, false, false, "10m"),
        );
        let repository = Repository::connect_in_memory().await.unwrap();
        let runtime = AppRuntime::from_repository(config, repository)
            .await
            .unwrap();
        let due_at = runtime
            .state
            .prowlarr_sources
            .values()
            .next()
            .unwrap()
            .initial_refresh_after_ms;

        let skipped_before_due = runtime
            .state
            .refresh_due_prowlarr_sources(1_000)
            .await
            .unwrap();
        let first = runtime
            .state
            .refresh_due_prowlarr_sources(due_at)
            .await
            .unwrap();
        runtime.state.shutdown.cancel_now("test shutdown").unwrap();
        let shutdown = runtime
            .state
            .refresh_due_prowlarr_sources(i64::MAX)
            .await
            .unwrap();

        assert_eq!(1, skipped_before_due.skipped_interval);
        assert_eq!(1, first.refreshed);
        assert_eq!(1, first.imported);
        assert_eq!(1, shutdown.skipped_shutdown);
        assert_eq!(1, requests.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn runtime_periodic_prowlarr_refresh_retries_after_backoff() {
        let requests = Arc::new(AtomicUsize::new(0));
        let prowlarr_url = spawn_runtime_prowlarr_server(
            Arc::clone(&requests),
            StatusCode::OK,
            prowlarr_catalog(),
        )
        .await;
        let mut config = SporosConfig::default();
        config.indexers.prowlarr.insert(
            "main".to_owned(),
            test_prowlarr_config(prowlarr_url, false, false, "24h"),
        );
        let repository = Repository::connect_in_memory().await.unwrap();
        let source = DependencyName::new("main").unwrap();
        repository
            .record_dependency_health(
                "prowlarr",
                &source,
                &DependencyState::Unavailable {
                    reason: ReasonText::new("down").unwrap(),
                    retry_after_ms: Some(1_000),
                },
                100,
            )
            .await
            .unwrap();
        let runtime = AppRuntime::from_repository(config, repository)
            .await
            .unwrap();

        let backed_off = runtime
            .state
            .refresh_due_prowlarr_sources(999)
            .await
            .unwrap();
        let retried = runtime
            .state
            .refresh_due_prowlarr_sources(1_000)
            .await
            .unwrap();

        assert_eq!(1, backed_off.skipped_backoff);
        assert_eq!(1, retried.refreshed);
        assert_eq!(1, requests.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn runtime_prowlarr_refresh_honors_retry_after() {
        let requests = Arc::new(AtomicUsize::new(0));
        let prowlarr_url =
            spawn_runtime_prowlarr_server_with_retry_after(Arc::clone(&requests), "3600").await;
        let mut config = SporosConfig::default();
        config.indexers.prowlarr.insert(
            "main".to_owned(),
            test_prowlarr_config(prowlarr_url, false, false, "24h"),
        );
        let repository = Repository::connect_in_memory().await.unwrap();
        let runtime = AppRuntime::from_repository(config, repository.clone())
            .await
            .unwrap();

        let now_ms = runtime
            .state
            .prowlarr_sources
            .values()
            .next()
            .unwrap()
            .initial_refresh_after_ms;
        let summary = runtime
            .state
            .refresh_due_prowlarr_sources(now_ms)
            .await
            .unwrap();
        let health = repository.dependency_health_snapshot(10).await.unwrap();

        assert_eq!(1, summary.failed);
        assert_eq!(1, requests.load(Ordering::SeqCst));
        assert_eq!("prowlarr", health[0].dependency_type);
        assert_eq!("unavailable", health[0].state);
        assert_eq!(
            Some(now_ms.saturating_add(3_600_000)),
            health[0].retry_after_ms
        );
    }

    #[tokio::test]
    async fn runtime_prowlarr_refresh_exports_metrics() {
        let success_url = spawn_runtime_prowlarr_server(
            Arc::new(AtomicUsize::new(0)),
            StatusCode::OK,
            prowlarr_catalog(),
        )
        .await;
        let failed_url = spawn_runtime_prowlarr_server(
            Arc::new(AtomicUsize::new(0)),
            StatusCode::INTERNAL_SERVER_ERROR,
            "down",
        )
        .await;
        let limited_url =
            spawn_runtime_prowlarr_server_with_retry_after(Arc::new(AtomicUsize::new(0)), "60")
                .await;
        let mut config = SporosConfig::default();
        config.indexers.prowlarr.insert(
            "main".to_owned(),
            test_prowlarr_config(success_url.clone(), true, false, "24h"),
        );
        config.indexers.prowlarr.insert(
            "failed".to_owned(),
            test_prowlarr_config(failed_url.clone(), true, false, "24h"),
        );
        config.indexers.prowlarr.insert(
            "limited".to_owned(),
            test_prowlarr_config(limited_url.clone(), true, false, "24h"),
        );
        let repository = Repository::connect_in_memory().await.unwrap();
        let source = DependencyName::new("main").unwrap();
        repository
            .sync_prowlarr_indexers(
                &source,
                &[ProwlarrIndexer {
                    source: source.clone(),
                    prowlarr_id: 202,
                    name: DependencyName::new("OldMovies").unwrap(),
                    url: SanitizedTorznabUrl::new("https://old.example/202/api").unwrap(),
                    api_key: Some(ApiKey::new("old-secret").unwrap()),
                    api_key_source: ApiKeySource::Direct,
                    tags: Vec::new(),
                }],
                ProwlarrRemovePolicy::Deactivate,
                100,
            )
            .await
            .unwrap();

        let runtime = AppRuntime::from_repository(config, repository)
            .await
            .unwrap();
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
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();

        assert!(
            text.contains("sporos_prowlarr_refresh_total{source=\"main\",outcome=\"succeeded\"} 1")
        );
        assert!(
            text.contains("sporos_prowlarr_refresh_total{source=\"failed\",outcome=\"failed\"} 1")
        );
        assert!(text.contains(
            "sporos_prowlarr_refresh_total{source=\"limited\",outcome=\"rate_limited\"} 1"
        ));
        assert!(text.contains("sporos_prowlarr_refresh_imported_total{source=\"main\"} 1"));
        assert!(text.contains("sporos_prowlarr_refresh_deactivated_total{source=\"main\"} 1"));
        assert!(!text.contains("prowlarr-secret"));
        assert!(!text.contains(&success_url));
        assert!(!text.contains(&failed_url));
        assert!(!text.contains(&limited_url));
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
    async fn search_workflow_skips_unresolved_prowlarr_sources_before_planning() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let source = DependencyName::new("main").unwrap();
        let name = DependencyName::new("main:Movies").unwrap();
        repository
            .sync_prowlarr_indexers(
                &source,
                &[ProwlarrIndexer {
                    source: source.clone(),
                    prowlarr_id: 101,
                    name: DependencyName::new("Movies").unwrap(),
                    url: SanitizedTorznabUrl::new("https://prowlarr.example/101/api").unwrap(),
                    api_key: Some(ApiKey::new("prowlarr-secret").unwrap()),
                    api_key_source: ApiKeySource::Direct,
                    tags: Vec::new(),
                }],
                ProwlarrRemovePolicy::Deactivate,
                100,
            )
            .await
            .unwrap();
        repository
            .record_indexer_caps_success(&name, &movie_caps(), 200)
            .await
            .unwrap();
        let runtime = AppRuntime::from_repository(SporosConfig::default(), repository)
            .await
            .unwrap();

        let summary = runtime
            .state
            .plan_search_workflow(
                SearchWorkflowRequest {
                    query: ItemTitle::new("Example.Movie.1080p").unwrap(),
                },
                300,
            )
            .await
            .unwrap();

        assert!(summary.plans.is_empty());
        assert_eq!(0, summary.candidate_count);
    }

    #[tokio::test]
    async fn search_workflow_skips_unrefreshed_indexer_caps() {
        let torznab_queries = Arc::new(Mutex::new(Vec::new()));
        let torznab_url = spawn_runtime_torznab_search_server(Arc::clone(&torznab_queries)).await;
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
        let runtime = AppRuntime::from_repository(config, repository)
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

        assert_eq!(0, summary.plans.len());
        assert_eq!(0, summary.failed_indexers);
        assert_eq!(0, summary.candidate_count);
        assert!(torznab_queries.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn search_workflow_skips_unrefreshed_indexers_before_limit() {
        let torznab_queries = Arc::new(Mutex::new(Vec::new()));
        let torznab_url = spawn_runtime_torznab_search_server(Arc::clone(&torznab_queries)).await;
        let mut config = SporosConfig::default();
        for index in 0..1_001 {
            config.indexers.torznab.insert(
                format!("aaa-unrefreshed-{index:04}"),
                TorznabIndexerConfig {
                    url: format!("https://unrefreshed-{index:04}.example/api"),
                    api_key: None,
                    api_key_file: None,
                    api_key_env: None,
                },
            );
        }
        config.indexers.torznab.insert(
            "zzzz-searchable".to_owned(),
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
            .record_indexer_caps_success(
                &DependencyName::new("zzzz-searchable").unwrap(),
                &movie_caps(),
                100,
            )
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
        assert_eq!(1, summary.candidate_count);
        let queries = torznab_queries.lock().unwrap();
        assert_eq!(1, queries.len());
        assert!(queries[0].contains("q=Example"));
    }

    #[tokio::test]
    async fn search_workflow_pages_past_incompatible_indexers_before_limit() {
        let torznab_queries = Arc::new(Mutex::new(Vec::new()));
        let torznab_url = spawn_runtime_torznab_search_server(Arc::clone(&torznab_queries)).await;
        let mut config = SporosConfig::default();
        config.indexers.torznab.insert(
            "zzzz-searchable".to_owned(),
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
        insert_indexer_rows(
            &repository,
            "aaa-tv-only",
            1_001,
            true,
            None,
            Some(100),
            &serde_json::to_string(&tv_caps()).unwrap(),
        )
        .await;
        repository
            .record_indexer_caps_success(
                &DependencyName::new("zzzz-searchable").unwrap(),
                &movie_caps(),
                100,
            )
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
        assert_eq!(1, summary.candidate_count);
        let queries = torznab_queries.lock().unwrap();
        assert_eq!(1, queries.len());
        assert!(queries[0].contains("q=Example"));
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
    async fn indexer_caps_refresh_skips_backoff_indexers_before_limit() {
        let caps_requests = Arc::new(Mutex::new(Vec::new()));
        let torznab_url = spawn_runtime_torznab_search_server(Arc::clone(&caps_requests)).await;
        let mut config = SporosConfig::default();
        config.indexers.torznab.insert(
            "zzzz-refreshable".to_owned(),
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
        insert_indexer_rows(
            &repository,
            "aaa-backoff",
            1_001,
            true,
            Some(10_000),
            None,
            "{}",
        )
        .await;

        let summary = runtime
            .state
            .refresh_indexer_capabilities(1_000)
            .await
            .unwrap();

        assert_eq!(1, summary.refreshed);
        assert_eq!(0, summary.failed);
        assert_eq!(1_001, summary.skipped_backoff);
        assert_eq!(Some(10_000), summary.next_backoff_deadline_ms);
        let queries = caps_requests.lock().unwrap();
        assert_eq!(1, queries.len());
        assert!(queries[0].contains("t=caps"));
    }

    #[tokio::test]
    async fn indexer_caps_refresh_pages_due_indexers_before_limit() {
        let caps_requests = Arc::new(Mutex::new(Vec::new()));
        let torznab_url = spawn_runtime_torznab_search_server(Arc::clone(&caps_requests)).await;
        let mut config = SporosConfig::default();
        config.indexers.torznab.insert(
            "zzzz-refreshable".to_owned(),
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
        insert_indexer_rows(&repository, "aaa-due", 1_001, true, None, None, "{}").await;

        let summary = runtime
            .state
            .refresh_indexer_capabilities(1_000)
            .await
            .unwrap();

        assert_eq!(1, summary.refreshed);
        assert_eq!(0, summary.failed);
        let queries = caps_requests.lock().unwrap();
        assert_eq!(1, queries.len());
        assert!(queries[0].contains("t=caps"));
    }

    #[tokio::test]
    async fn indexer_caps_refresh_skips_disabled_indexers_before_limit() {
        let caps_requests = Arc::new(Mutex::new(Vec::new()));
        let torznab_url = spawn_runtime_torznab_search_server(Arc::clone(&caps_requests)).await;
        let mut config = SporosConfig::default();
        config.indexers.torznab.insert(
            "zzzz-refreshable".to_owned(),
            TorznabIndexerConfig {
                url: torznab_url,
                api_key: Some(ApiKey::new("indexer-secret").unwrap()),
                api_key_file: None,
                api_key_env: None,
            },
        );
        let repository = Repository::connect_in_memory().await.unwrap();
        insert_disabled_indexers(&repository, 1_001).await;
        let runtime = AppRuntime::from_repository(config, repository)
            .await
            .unwrap();

        let summary = runtime
            .state
            .refresh_indexer_capabilities(1_000)
            .await
            .unwrap();

        assert_eq!(1, summary.refreshed);
        assert_eq!(0, summary.failed);
        let queries = caps_requests.lock().unwrap();
        assert_eq!(1, queries.len());
        assert!(queries[0].contains("t=caps"));
    }

    #[tokio::test]
    async fn search_workflow_planning_keeps_candidates_after_one_indexer_fails() {
        let ok_queries = Arc::new(Mutex::new(Vec::new()));
        let failing_queries = Arc::new(Mutex::new(Vec::new()));
        let ok_url = spawn_runtime_torznab_search_server(Arc::clone(&ok_queries)).await;
        let failing_url = spawn_runtime_torznab_status_server(
            Arc::clone(&failing_queries),
            StatusCode::TOO_MANY_REQUESTS,
        )
        .await;
        let mut config = SporosConfig::default();
        config.indexers.torznab.insert(
            "ok".to_owned(),
            TorznabIndexerConfig {
                url: ok_url,
                api_key: Some(ApiKey::new("ok-secret").unwrap()),
                api_key_file: None,
                api_key_env: None,
            },
        );
        config.indexers.torznab.insert(
            "failing".to_owned(),
            TorznabIndexerConfig {
                url: failing_url,
                api_key: Some(ApiKey::new("failing-secret").unwrap()),
                api_key_file: None,
                api_key_env: None,
            },
        );
        let repository = Repository::connect_in_memory().await.unwrap();
        let runtime = AppRuntime::from_repository(config, repository.clone())
            .await
            .unwrap();
        repository
            .record_indexer_caps_success(&DependencyName::new("ok").unwrap(), &movie_caps(), 100)
            .await
            .unwrap();
        repository
            .record_indexer_caps_success(
                &DependencyName::new("failing").unwrap(),
                &movie_caps(),
                100,
            )
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

        assert_eq!(2, summary.plans.len());
        assert_eq!(1, summary.failed_indexers);
        assert_eq!(1, summary.candidate_count);
        assert_eq!(1, summary.candidates.len());
        assert_eq!(1, ok_queries.lock().unwrap().len());
        assert_eq!(1, failing_queries.lock().unwrap().len());
        let retry_after = repository
            .indexer_registry_snapshot(10)
            .await
            .unwrap()
            .into_iter()
            .find(|row| row.name.as_str() == "failing")
            .unwrap()
            .retry_after_ms;
        assert!(retry_after.is_some_and(|deadline| deadline > 1_000));

        let skipped = runtime
            .state
            .plan_search_workflow(
                SearchWorkflowRequest {
                    query: ItemTitle::new("Example.Movie.1080p").unwrap(),
                },
                1_500,
            )
            .await
            .unwrap();

        assert_eq!(1, skipped.plans.len());
        assert_eq!(0, skipped.failed_indexers);
        assert_eq!(2, ok_queries.lock().unwrap().len());
        assert_eq!(1, failing_queries.lock().unwrap().len());
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
    async fn runtime_accepts_durable_workflows() {
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

        assert_eq!(StatusCode::ACCEPTED, search.status());
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
        let accepted = app
            .oneshot(search_request(Some("Bearer secret-token")))
            .await
            .unwrap();

        assert_eq!(StatusCode::UNAUTHORIZED, unauthorized.status());
        assert_eq!(StatusCode::ACCEPTED, accepted.status());
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

    async fn spawn_runtime_prowlarr_server(
        requests: Arc<AtomicUsize>,
        status: StatusCode,
        body: &'static str,
    ) -> String {
        let handler = move |_request: Request<Body>| {
            let requests = Arc::clone(&requests);
            async move {
                requests.fetch_add(1, Ordering::SeqCst);
                (status, body).into_response()
            }
        };
        let app = axum::Router::new()
            .route("/api/v1/indexer", get(handler.clone()))
            .route("/api/v1/tag", get(handler));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{address}")
    }

    async fn spawn_runtime_prowlarr_server_with_retry_after(
        requests: Arc<AtomicUsize>,
        retry_after: &'static str,
    ) -> String {
        let handler = move |_request: Request<Body>| {
            let requests = Arc::clone(&requests);
            async move {
                requests.fetch_add(1, Ordering::SeqCst);
                let mut response = (StatusCode::TOO_MANY_REQUESTS, "limited").into_response();
                response
                    .headers_mut()
                    .insert(RETRY_AFTER, retry_after.parse().unwrap());
                response
            }
        };
        let app = axum::Router::new()
            .route("/api/v1/indexer", get(handler.clone()))
            .route("/api/v1/tag", get(handler));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{address}")
    }

    fn test_prowlarr_config(
        url: String,
        refresh_on_startup: bool,
        required: bool,
        update_interval: &str,
    ) -> ProwlarrSourceConfig {
        ProwlarrSourceConfig {
            url,
            api_key: Some(ApiKey::new("prowlarr-secret").unwrap()),
            refresh_on_startup,
            required,
            update_interval: update_interval.to_owned(),
            ..ProwlarrSourceConfig::default()
        }
    }

    fn prowlarr_catalog() -> &'static str {
        r#"
        [
          {
            "id": 101,
            "name": "Movies",
            "enable": true,
            "protocol": "torrent",
            "implementation": "Cardigann",
            "supportsRss": true,
            "supportsSearch": true,
            "tags": []
          }
        ]
        "#
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

    async fn insert_disabled_indexers(repository: &Repository, count: usize) {
        insert_indexer_rows(repository, "aaa-disabled", count, false, None, None, "{}").await;
    }

    async fn insert_indexer_rows(
        repository: &Repository,
        prefix: &str,
        count: usize,
        enabled: bool,
        retry_after_ms: Option<i64>,
        last_caps_refresh_at_ms: Option<i64>,
        caps_json: &str,
    ) {
        for index in 0..count {
            let name = format!("{prefix}-{index:04}");
            sqlx::query(
                r#"
                INSERT INTO indexers (
                    name,
                    url,
                    source_kind,
                    source_name,
                    source_indexer_id,
                    api_key_source,
                    enabled,
                    capabilities_json,
                    state,
                    retry_after,
                    last_caps_refresh_at,
                    created_at,
                    updated_at
                )
                VALUES (?, ?, 'static', '', ?, 'direct', ?, ?, 'unknown', ?, ?, 1, 1)
                "#,
            )
            .bind(&name)
            .bind(format!("https://{prefix}-{index:04}.example/api"))
            .bind(&name)
            .bind(if enabled { 1_i64 } else { 0_i64 })
            .bind(caps_json)
            .bind(retry_after_ms)
            .bind(last_caps_refresh_at_ms)
            .execute(repository.pool())
            .await
            .unwrap();
        }
    }

    async fn spawn_runtime_torznab_status_server(
        queries: Arc<Mutex<Vec<String>>>,
        status: StatusCode,
    ) -> String {
        let app = axum::Router::new().route(
            "/api",
            get(move |request: Request<Body>| {
                let queries = Arc::clone(&queries);
                async move {
                    queries
                        .lock()
                        .unwrap()
                        .push(request.uri().query().unwrap_or_default().to_owned());
                    let mut response = (status, "unavailable").into_response();
                    if status == StatusCode::TOO_MANY_REQUESTS {
                        response
                            .headers_mut()
                            .insert("retry-after", "5".parse().unwrap());
                    }
                    response
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
