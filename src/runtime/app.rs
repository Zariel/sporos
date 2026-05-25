use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::stream::{FuturesUnordered, StreamExt};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::arr::{ArrEndpoint, ArrRegistry};
use crate::clients::TorrentClientRegistry;
use crate::clients::runtime::build_injection_clients;
use crate::config::{ProwlarrRemovePolicy, SporosConfig};
use crate::domain::{
    ByteSize, DependencyName, DependencyState, DisplayName, IndexerId, ItemTitle, LocalItem,
    LocalItemSource, MediaType, ReasonText, RemoteCandidate, SourceKey,
};
use crate::errors::DatabaseError;
use crate::http::{
    AnnouncementWorkflowRequest, HttpState, JobRunWorkflowRequest, ReadinessPaths, ReadinessState,
    SearchWorkflowRequest, WorkflowQueues,
};
use crate::indexers::{
    ConfiguredTorznabIndexer, IndexerBackoffPolicy, ProwlarrConfigError, ProwlarrHttpClient,
    ProwlarrRequestError, ProwlarrSource, SanitizedTorznabUrl, TorznabCaps, TorznabEndpoint,
    TorznabHttpClient, TorznabRegistry, TorznabRequestError,
};
use crate::inventory::InventoryScanOptions;
use crate::inventory_refresh::{
    InventoryRefreshError, InventoryRefreshRequest, InventoryRefreshSummary,
    InventoryRefreshWorker, inventory_refresh_queue,
};
use crate::metrics::{ExternalOperation, ExternalOutcome, MetricsRegistry, ProwlarrRefreshOutcome};
use crate::notifications::{NotificationJob, notification_queue};
use crate::persistence::repository::{
    DependencyHealthSnapshot, IndexerRegistryRow, IndexerSearchCapsRow, Repository,
};
use crate::runtime::announce_worker::AnnounceWorker;
use crate::runtime::backoff::stable_jitter_seed;
use crate::runtime::health::{DependencyKind, HealthRegistry};
use crate::runtime::injection_worker::{InjectionClient, InjectionWorker};
use crate::runtime::queue::{QueueKind, RuntimeQueueConfig, WorkReceiver, bounded_work_queue};
use crate::runtime::scheduler::{
    PersistedScheduler, ScheduledJobRun, SchedulerConfig, parse_interval_ms,
    scheduled_job_has_executor, scheduler_queue,
};
use crate::runtime::search::{
    RuntimeSearchPlanner, RuntimeTorznabSearchPlan, plan_runtime_torznab_search,
    seed_arr_endpoint_backoff,
};
use crate::runtime::shutdown::{
    ShutdownController, ShutdownPhase, ShutdownSignal, shutdown_channel,
};

const PROWLARR_REFRESH_CONCURRENCY: usize = 4;
const INDEXER_CAPS_REFRESH_CONCURRENCY: usize = 4;
const INDEXER_SEARCH_CONCURRENCY: usize = 4;
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

struct IndexerEndpointInput<'a> {
    indexer_id: IndexerId,
    name: &'a DependencyName,
    url: &'a str,
    source_kind: &'a str,
    source_name: &'a str,
    caps: TorznabCaps,
    retry_after_ms: Option<i64>,
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
        let source_names = self
            .prowlarr_sources
            .values()
            .map(|source| source.source.name.clone())
            .collect::<Vec<_>>();
        let persisted = self
            .repository
            .dependency_health_for_type_names("prowlarr", &source_names)
            .await?;
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
        let mut shutdown = self.shutdown_signal.clone();
        for source in sources.into_iter().cloned() {
            if self.shutdown_signal.state().phase != ShutdownPhase::Running {
                summary.skipped_shutdown += 1;
                break;
            }
            let permit = tokio::select! {
                biased;
                _ = shutdown.cancelled() => {
                    summary.skipped_shutdown += 1;
                    break;
                }
                permit = semaphore.clone().acquire_owned() => {
                    permit.map_err(|error| DatabaseError::Unavailable {
                        operation: "refresh Prowlarr source".to_owned(),
                        message: error.to_string(),
                    })?
                }
            };
            if self.shutdown_signal.state().phase != ShutdownPhase::Running {
                summary.skipped_shutdown += 1;
                break;
            }
            let state = self.clone();
            tasks.spawn(async move {
                let _permit = permit;
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
                let failure_count = self
                    .repository
                    .dependency_failure_count("prowlarr", &source.source.name)
                    .await
                    .map_err(ProwlarrRefreshError::Local)?;
                let state = prowlarr_error_dependency_state(
                    &error,
                    now_ms,
                    failure_count,
                    source.source.name.as_str(),
                )
                .map_err(ProwlarrRefreshError::Local)?;
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
        self.indexer_endpoint(IndexerEndpointInput {
            indexer_id: row.indexer_id,
            name: &row.name,
            url: &row.url,
            source_kind: &row.source_kind,
            source_name: &row.source_name,
            caps: row.caps.clone(),
            retry_after_ms: row.retry_after_ms,
        })
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
        self.indexer_endpoint(IndexerEndpointInput {
            indexer_id,
            name: &row.name,
            url: &row.url,
            source_kind: &row.source_kind,
            source_name: &row.source_name,
            caps,
            retry_after_ms: row.retry_after_ms,
        })
    }

    fn indexer_endpoint(
        &self,
        input: IndexerEndpointInput<'_>,
    ) -> Result<Option<TorznabEndpoint>, DatabaseError> {
        let IndexerEndpointInput {
            indexer_id,
            name,
            url,
            source_kind,
            source_name,
            caps,
            retry_after_ms,
        } = input;
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
        let mut failed_indexers = 0_usize;
        let mut search_results = Vec::new();
        let mut after_name = None;
        let mut search_ordinal = 0_usize;

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
            let mut indexers = indexers.into_iter();
            let mut active = FuturesUnordered::new();
            let mut endpoint_error = None;
            let mut search_error = None;

            loop {
                while active.len() < INDEXER_SEARCH_CONCURRENCY
                    && endpoint_error.is_none()
                    && search_error.is_none()
                {
                    let Some(indexer) = indexers.next() else {
                        break;
                    };
                    let ordinal = search_ordinal;
                    search_ordinal = search_ordinal.saturating_add(1);
                    after_name = Some(indexer.name.clone());
                    let Some(plan) = plan_runtime_torznab_search(&item, &ids, &indexer.caps) else {
                        continue;
                    };
                    let endpoint = match self.search_caps_endpoint(&indexer) {
                        Ok(Some(endpoint)) => endpoint,
                        Ok(None) => continue,
                        Err(error) => {
                            endpoint_error = Some(error);
                            break;
                        }
                    };
                    plans.push(IndexerSearchPlan {
                        indexer_id: indexer.indexer_id,
                        indexer_name: indexer.name,
                        plan: plan.clone(),
                    });
                    let state = self.clone();
                    let media_type = item.media_type;
                    active.push(async move {
                        (
                            ordinal,
                            state
                                .search_indexer_endpoint(endpoint, media_type, plan, now_ms)
                                .await,
                        )
                    });
                }

                let Some((ordinal, result)) = active.next().await else {
                    break;
                };
                match result {
                    Ok(result) => search_results.push((ordinal, result)),
                    Err(error) => {
                        search_error = Some(error);
                    }
                }
            }

            if let Some(error) = endpoint_error {
                return Err(error);
            }
            if let Some(error) = search_error {
                return Err(error);
            }
            if is_last_page {
                break;
            }
        }

        search_results.sort_by_key(|(ordinal, _)| *ordinal);
        let mut candidate_count = 0_usize;
        let mut all_candidates = Vec::new();
        for (_, result) in search_results {
            match result {
                IndexerSearchResult::Succeeded(candidates) => {
                    candidate_count = candidate_count.saturating_add(candidates.len());
                    all_candidates.extend(candidates);
                }
                IndexerSearchResult::Failed => {
                    failed_indexers += 1;
                }
            }
        }

        Ok(SearchWorkflowPlanSummary {
            plans,
            candidate_count,
            candidates: all_candidates,
            failed_indexers,
        })
    }

    async fn search_indexer_endpoint(
        &self,
        endpoint: TorznabEndpoint,
        media_type: MediaType,
        plan: RuntimeTorznabSearchPlan,
        now_ms: i64,
    ) -> Result<IndexerSearchResult, DatabaseError> {
        let started = Instant::now();
        let candidates = self
            .torznab_client
            .search(&endpoint, media_type, &plan.plan, now_ms)
            .await;
        match candidates {
            Ok(candidates) => {
                self.metrics.record_indexer_request(
                    ExternalOperation::Search,
                    ExternalOutcome::Succeeded,
                    elapsed_ms(started),
                );
                self.repository
                    .record_indexer_request_success(&endpoint.name, now_ms)
                    .await?;
                Ok(IndexerSearchResult::Succeeded(candidates))
            }
            Err(error) => {
                self.metrics.record_indexer_request(
                    ExternalOperation::Search,
                    indexer_request_metric_outcome(&error),
                    elapsed_ms(started),
                );
                tracing::warn!(
                    indexer_name = %endpoint.name,
                    error = %error,
                    "Torznab search failed"
                );
                let message = error.to_string();
                let reason = required_health_reason(
                    Some(&message),
                    "search failed",
                    "build indexer search health reason",
                )?;
                let failure_count = self
                    .repository
                    .dependency_failure_count("indexer", &endpoint.name)
                    .await?;
                let retry_after_ms = indexer_error_retry_after(
                    &error,
                    now_ms,
                    failure_count,
                    endpoint.name.as_str(),
                );
                self.repository
                    .record_indexer_request_backoff(
                        &endpoint.name,
                        &reason,
                        retry_after_ms,
                        now_ms,
                        indexer_error_is_unavailable(&error),
                    )
                    .await?;
                Ok(IndexerSearchResult::Failed)
            }
        }
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
            let mut rows = registry.into_iter().enumerate();
            let mut results = Vec::new();
            let mut active = FuturesUnordered::new();
            let mut endpoint_error = None;
            let mut refresh_error = None;

            loop {
                while active.len() < INDEXER_CAPS_REFRESH_CONCURRENCY
                    && endpoint_error.is_none()
                    && refresh_error.is_none()
                {
                    let Some((ordinal, row)) = rows.next() else {
                        break;
                    };
                    after_name = Some(row.name.clone());
                    let endpoint = match self.registry_endpoint(&row, TorznabCaps::default()) {
                        Ok(Some(endpoint)) => endpoint,
                        Ok(None) => continue,
                        Err(error) => {
                            endpoint_error = Some(error);
                            break;
                        }
                    };
                    let state = self.clone();
                    active.push(async move {
                        (
                            ordinal,
                            state
                                .refresh_indexer_capability_endpoint(row.name, endpoint, now_ms)
                                .await,
                        )
                    });
                }

                let Some((ordinal, result)) = active.next().await else {
                    break;
                };
                match result {
                    Ok(result) => results.push((ordinal, result)),
                    Err(error) => {
                        refresh_error = Some(error);
                    }
                }
            }

            results.sort_by_key(|(ordinal, _)| *ordinal);
            for (_, result) in results {
                if result.refreshed {
                    summary.refreshed += 1;
                } else {
                    summary.failed += 1;
                }
                if let Some(error) = result.last_error {
                    last_error = Some(error);
                }
            }
            if let Some(error) = endpoint_error {
                return Err(error);
            }
            if let Some(error) = refresh_error {
                return Err(error);
            }
            if is_last_page {
                break;
            }
        }

        summary.last_error = last_error;
        Ok(summary)
    }

    async fn refresh_indexer_capability_endpoint(
        &self,
        name: DependencyName,
        endpoint: TorznabEndpoint,
        now_ms: i64,
    ) -> Result<IndexerCapsRefreshResult, DatabaseError> {
        let started = Instant::now();
        match self.torznab_client.caps_endpoint(&endpoint).await {
            Ok(caps) => {
                self.metrics.record_indexer_request(
                    ExternalOperation::Capabilities,
                    ExternalOutcome::Succeeded,
                    elapsed_ms(started),
                );
                self.repository
                    .record_indexer_caps_success(&name, &caps, now_ms)
                    .await?;
                Ok(IndexerCapsRefreshResult {
                    refreshed: true,
                    last_error: None,
                })
            }
            Err(error) => {
                self.metrics.record_indexer_request(
                    ExternalOperation::Capabilities,
                    indexer_request_metric_outcome(&error),
                    elapsed_ms(started),
                );
                let message = error.to_string();
                let reason = required_health_reason(
                    Some(&message),
                    "caps failed",
                    "build indexer caps health reason",
                )?;
                let failure_count = self
                    .repository
                    .dependency_failure_count("indexer", &name)
                    .await?;
                let retry_after_ms =
                    indexer_error_retry_after(&error, now_ms, failure_count, name.as_str());
                self.repository
                    .record_indexer_caps_failure(&name, &reason, Some(retry_after_ms), now_ms)
                    .await?;
                Ok(IndexerCapsRefreshResult {
                    refreshed: false,
                    last_error: Some(message),
                })
            }
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SearchWorkflowPlanSummary {
    pub plans: Vec<IndexerSearchPlan>,
    pub candidate_count: usize,
    pub candidates: Vec<RemoteCandidate>,
    pub failed_indexers: usize,
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum IndexerSearchResult {
    Succeeded(Vec<RemoteCandidate>),
    Failed,
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
struct IndexerCapsRefreshResult {
    refreshed: bool,
    last_error: Option<String>,
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

struct RuntimeConfigParts {
    indexers: TorznabRegistry,
    torznab_indexers: BTreeMap<DependencyName, ConfiguredTorznabIndexer>,
    prowlarr_sources: BTreeMap<DependencyName, RuntimeProwlarrSource>,
    arr: ArrRegistry,
    clients: TorrentClientRegistry,
    injection_clients: Vec<Arc<dyn InjectionClient>>,
    scheduler_config: SchedulerConfig,
    http_jobs: BTreeSet<crate::domain::JobName>,
    saved_retry_interval: Duration,
}

pub fn validate_runtime_config(config: &SporosConfig) -> Result<(), DatabaseError> {
    let metrics = MetricsRegistry::new();
    let now_ms = crate::runtime::announce_worker::unix_time_ms();
    build_runtime_config(config, &metrics, now_ms).map(|_| ())
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
        let now_ms = crate::runtime::announce_worker::unix_time_ms();
        let runtime_config = build_runtime_config(&config, &metrics, now_ms)?;
        repository
            .sync_torznab_indexers(runtime_config.indexers.indexers(), now_ms)
            .await?;
        let queue_config = RuntimeQueueConfig::default();
        let (workflow, workflow_receivers) = workflow_queues(queue_config);
        let (scheduler_queue, scheduler_receiver) = scheduler_queue(queue_config.indexing_limit);
        let (inventory_queue, inventory_receiver) =
            inventory_refresh_queue(queue_config.indexing_limit);
        let (notification_queue, notification_receiver) =
            notification_queue(queue_config.notification_limit);
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
            runtime_config.scheduler_config,
        );
        let inventory_refresh = InventoryRefreshWorker::new(
            repository.clone(),
            InventoryScanOptions {
                max_depth: config.inventory.media_scan_max_depth,
            },
        )
        .with_season_from_episodes(config.matching.season_from_episodes);
        let injection_worker =
            InjectionWorker::new(repository.clone(), runtime_config.injection_clients);
        let mut arr_endpoints = runtime_config
            .arr
            .instances()
            .iter()
            .map(ArrEndpoint::from_configured)
            .collect::<Vec<_>>();
        let arr_names = arr_endpoints
            .iter()
            .map(|endpoint| endpoint.name.clone())
            .collect::<Vec<_>>();
        let prowlarr_names = runtime_config
            .prowlarr_sources
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        let mut persisted_health = repository
            .dependency_health_for_type_names("arr", &arr_names)
            .await?;
        persisted_health.extend(
            repository
                .dependency_health_for_type_names("prowlarr", &prowlarr_names)
                .await?,
        );
        persisted_health.extend(repository.dependency_health_for_indexer_registry().await?);
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
            .with_live_readiness(
                repository.clone(),
                ReadinessPaths::new(
                    &config.paths.database,
                    &config.paths.torrent_cache_dir,
                    &config.paths.output_dir,
                ),
            )
            .with_metrics(metrics.clone())
            .with_search_queue(workflow.searches.clone())
            .with_job_queue(workflow.jobs.clone())
            .with_allowed_jobs(runtime_config.http_jobs)
            .with_announce_acceptor(repository.clone(), config.announce.clone());
        if let Some(api_token) = config.server.api_token.as_ref() {
            http = http.with_api_token(api_token.expose_secret());
        }

        let state = AppState {
            config,
            repository,
            clients: runtime_config.clients,
            health,
            metrics,
            http,
            queues,
            announce_worker,
            scheduler,
            inventory_refresh,
            injection_worker,
            search_planner,
            torznab_indexers: runtime_config.torznab_indexers,
            prowlarr_sources: runtime_config.prowlarr_sources,
            torznab_client: TorznabHttpClient::new(Duration::from_secs(120)),
            prowlarr_client: ProwlarrHttpClient::new(Duration::from_secs(30)),
            saved_retry_interval: runtime_config.saved_retry_interval,
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

fn build_runtime_config(
    config: &SporosConfig,
    metrics: &MetricsRegistry,
    now_ms: i64,
) -> Result<RuntimeConfigParts, DatabaseError> {
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
    let prowlarr_sources = runtime_prowlarr_sources(config, now_ms)?;
    let arr = ArrRegistry::from_config(&config.indexers.arr).map_err(|error| {
        DatabaseError::Unavailable {
            operation: "build Arr registry".to_owned(),
            message: error.to_string(),
        }
    })?;
    let clients = TorrentClientRegistry::from_config(&config.torrent_clients).map_err(|error| {
        DatabaseError::Unavailable {
            operation: "build torrent client registry".to_owned(),
            message: error.to_string(),
        }
    })?;
    let injection_clients = build_injection_clients(&config.torrent_clients, &clients, metrics)?;
    let scheduler_config =
        SchedulerConfig::from_scheduling_config(&config.scheduling).map_err(|error| {
            DatabaseError::Unavailable {
                operation: "build scheduler config".to_owned(),
                message: error.to_string(),
            }
        })?;
    let scheduler_config = daemon_scheduler_config(scheduler_config);
    let http_jobs = http_supported_jobs(&scheduler_config);
    parse_interval_ms(&config.scheduling.client_inventory_interval).map_err(|error| {
        DatabaseError::Unavailable {
            operation: "build client inventory interval".to_owned(),
            message: error.to_string(),
        }
    })?;
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

    Ok(RuntimeConfigParts {
        indexers,
        torznab_indexers,
        prowlarr_sources,
        arr,
        clients,
        injection_clients,
        scheduler_config,
        http_jobs,
        saved_retry_interval,
    })
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
    let hash = stable_jitter_seed(name.as_str());
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

fn seed_runtime_health(health: &HealthRegistry, rows: &[DependencyHealthSnapshot]) {
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

fn required_health_reason(
    value: Option<&str>,
    fallback: &'static str,
    operation: &'static str,
) -> Result<ReasonText, DatabaseError> {
    health_reason(value, fallback).ok_or_else(|| DatabaseError::QueryFailed {
        operation: operation.to_owned(),
        message: format!("fallback dependency health reason `{fallback}` is invalid"),
    })
}

fn http_supported_jobs(config: &SchedulerConfig) -> BTreeSet<crate::domain::JobName> {
    config
        .jobs
        .iter()
        .filter(|job| scheduled_job_has_executor(&job.name))
        .map(|job| job.name.clone())
        .collect()
}

fn daemon_scheduler_config(mut config: SchedulerConfig) -> SchedulerConfig {
    config
        .jobs
        .retain(|job| scheduled_job_has_executor(&job.name));
    config
}

fn indexer_error_retry_after(
    error: &TorznabRequestError,
    now_ms: i64,
    consecutive_failures: u16,
    jitter_key: &str,
) -> i64 {
    let policy = IndexerBackoffPolicy::default();
    match error {
        TorznabRequestError::Backoff { retry_after_ms } => retry_after_ms
            .filter(|retry_after| *retry_after > now_ms)
            .unwrap_or_else(|| {
                policy.retry_after_deadline(now_ms, consecutive_failures, None, jitter_key)
            }),
        TorznabRequestError::RateLimited { retry_after }
        | TorznabRequestError::HttpStatus { retry_after, .. } => {
            policy.retry_after_deadline(now_ms, consecutive_failures, *retry_after, jitter_key)
        }
        TorznabRequestError::Timeout
        | TorznabRequestError::Request { .. }
        | TorznabRequestError::InvalidXml { .. }
        | TorznabRequestError::InvalidCandidate { .. }
        | TorznabRequestError::ResponseTooLarge { .. } => {
            policy.retry_after_deadline(now_ms, consecutive_failures, None, jitter_key)
        }
    }
}

fn prowlarr_error_dependency_state(
    error: &ProwlarrRequestError,
    now_ms: i64,
    consecutive_failures: u16,
    jitter_key: &str,
) -> Result<DependencyState, DatabaseError> {
    let message = error.to_string();
    let reason = required_health_reason(
        Some(&message),
        "Prowlarr refresh failed",
        "build Prowlarr health reason",
    )?;
    let retry_after_ms = Some(prowlarr_error_retry_after(
        error,
        now_ms,
        consecutive_failures,
        jitter_key,
    ));
    let state = match error {
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
    };
    Ok(state)
}

fn prowlarr_refresh_outcome(error: &ProwlarrRequestError) -> ProwlarrRefreshOutcome {
    match error {
        ProwlarrRequestError::HttpStatus { status, .. } if *status == 429 => {
            ProwlarrRefreshOutcome::RateLimited
        }
        _ => ProwlarrRefreshOutcome::Failed,
    }
}

fn prowlarr_error_retry_after(
    error: &ProwlarrRequestError,
    now_ms: i64,
    consecutive_failures: u16,
    jitter_key: &str,
) -> i64 {
    let policy = IndexerBackoffPolicy::default();
    match error {
        ProwlarrRequestError::HttpStatus { retry_after, .. } => {
            policy.retry_after_deadline(now_ms, consecutive_failures, *retry_after, jitter_key)
        }
        ProwlarrRequestError::Timeout
        | ProwlarrRequestError::Request { .. }
        | ProwlarrRequestError::InvalidResponse { .. }
        | ProwlarrRequestError::InvalidIndexer { .. }
        | ProwlarrRequestError::ResponseTooLarge { .. } => {
            policy.retry_after_deadline(now_ms, consecutive_failures, None, jitter_key)
        }
    }
}

fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

fn indexer_request_metric_outcome(error: &TorznabRequestError) -> ExternalOutcome {
    match error {
        TorznabRequestError::RateLimited { .. } => ExternalOutcome::RateLimited,
        TorznabRequestError::Backoff { .. } => ExternalOutcome::Failed,
        TorznabRequestError::HttpStatus { status, .. } if *status == 429 => {
            ExternalOutcome::RateLimited
        }
        TorznabRequestError::HttpStatus { .. }
        | TorznabRequestError::Timeout
        | TorznabRequestError::Request { .. }
        | TorznabRequestError::InvalidXml { .. }
        | TorznabRequestError::InvalidCandidate { .. }
        | TorznabRequestError::ResponseTooLarge { .. } => ExternalOutcome::Failed,
    }
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
    use std::fs;
    use std::future::{Future, pending};
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::clients::runtime::CLIENT_INVENTORY_FILE_FETCH_CONCURRENCY;
    use crate::config::{
        ArrInstanceConfig, ConfigTorrentClientKind, ProwlarrSourceConfig, TorrentClientConfig,
        TorznabIndexerConfig,
    };
    use crate::domain::{DependencyName, DependencyState, InfoHash, ItemTitle, ReasonText};
    use crate::errors::TorrentClientError;
    use crate::http::router;
    use crate::indexers::{
        ApiKeySource, CategoryCaps, ConfiguredTorznabIndexer, ProwlarrIndexer, SanitizedTorznabUrl,
        SearchCaps, TorznabCaps, TorznabLimits,
    };
    use crate::metrics::ExternalOutcome;
    use crate::secrets::{ApiKey, ApiToken};
    use axum::body::{Body, to_bytes};
    use axum::http::{
        Request, StatusCode,
        header::{RETRY_AFTER, SET_COOKIE},
    };
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
                default_category: None,
                default_tags: vec![crate::config::DEFAULT_INJECTION_METADATA.to_owned()],
                default_label: crate::config::DEFAULT_INJECTION_METADATA.to_owned(),
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
    async fn runtime_periodic_prowlarr_refresh_targets_health_after_many_rows() {
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
        for index in 0..1_001 {
            let name = DependencyName::new(format!("arr{index:04}")).unwrap();
            repository
                .record_dependency_health(
                    "arr",
                    &name,
                    &DependencyState::Degraded {
                        reason: ReasonText::new("down").unwrap(),
                        retry_after_ms: Some(60_000),
                    },
                    100,
                )
                .await
                .unwrap();
        }
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

        assert_eq!(1, backed_off.skipped_backoff);
        assert_eq!(0, requests.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn runtime_startup_seeds_only_configured_dependency_health() {
        let mut config = SporosConfig::default();
        config.indexers.torznab.insert(
            "main".to_owned(),
            TorznabIndexerConfig {
                url: "https://indexer.example/api".to_owned(),
                api_key: Some(ApiKey::new("indexer-secret").unwrap()),
                api_key_file: None,
                api_key_env: None,
            },
        );
        config.indexers.arr.radarr.insert(
            "main".to_owned(),
            ArrInstanceConfig {
                url: "https://radarr.example".to_owned(),
                api_key: Some(ApiKey::new("arr-secret").unwrap()),
                api_key_file: None,
                api_key_env: None,
            },
        );
        let repository = Repository::connect_in_memory().await.unwrap();
        let mut disabled_indexers = Vec::new();
        for index in 0..1_001 {
            let name = DependencyName::new(format!("indexer{index:04}")).unwrap();
            disabled_indexers.push(ConfiguredTorznabIndexer {
                name: name.clone(),
                url: SanitizedTorznabUrl::new(format!("https://indexer{index:04}.example/api"))
                    .unwrap(),
                api_key: None,
                api_key_source: ApiKeySource::Missing,
                enabled: false,
            });
            repository
                .record_dependency_health(
                    "indexer",
                    &name,
                    &DependencyState::Unavailable {
                        reason: ReasonText::new("stale").unwrap(),
                        retry_after_ms: Some(60_000),
                    },
                    100,
                )
                .await
                .unwrap();
        }
        repository
            .sync_torznab_indexers(&disabled_indexers, 100)
            .await
            .unwrap();
        repository
            .record_dependency_health(
                "arr",
                &DependencyName::new("radarr-main").unwrap(),
                &DependencyState::Degraded {
                    reason: ReasonText::new("rate limited").unwrap(),
                    retry_after_ms: Some(1_000),
                },
                100,
            )
            .await
            .unwrap();
        repository
            .record_dependency_health(
                "indexer",
                &DependencyName::new("main").unwrap(),
                &DependencyState::Degraded {
                    reason: ReasonText::new("indexer backoff").unwrap(),
                    retry_after_ms: Some(2_000),
                },
                100,
            )
            .await
            .unwrap();

        let runtime = AppRuntime::from_repository(config, repository)
            .await
            .unwrap();
        let health = runtime.state.health.snapshot();

        assert_eq!(2, health.entries.len());
        assert!(health.entries.iter().any(|entry| {
            entry.key.kind == DependencyKind::Arr && entry.key.name.as_str() == "radarr-main"
        }));
        assert!(health.entries.iter().any(|entry| {
            entry.key.kind == DependencyKind::Indexer && entry.key.name.as_str() == "main"
        }));
        assert!(!health.entries.iter().any(|entry| {
            entry.key.kind == DependencyKind::Indexer && entry.key.name.as_str() == "indexer0000"
        }));
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
    async fn runtime_repeated_prowlarr_failures_back_off_exponentially() {
        let requests = Arc::new(AtomicUsize::new(0));
        let prowlarr_url = spawn_runtime_prowlarr_server(
            Arc::clone(&requests),
            StatusCode::INTERNAL_SERVER_ERROR,
            "down",
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
        let now_ms = runtime
            .state
            .prowlarr_sources
            .values()
            .next()
            .unwrap()
            .initial_refresh_after_ms;

        runtime
            .state
            .refresh_due_prowlarr_sources(now_ms)
            .await
            .unwrap();
        let first = repository.dependency_health_snapshot(10).await.unwrap();
        let first_retry_after = first[0].retry_after_ms.unwrap();
        runtime
            .state
            .refresh_due_prowlarr_sources(first_retry_after)
            .await
            .unwrap();
        let second = repository.dependency_health_snapshot(10).await.unwrap();
        let second_retry_after = second[0].retry_after_ms.unwrap();

        assert_eq!(2, requests.load(Ordering::SeqCst));
        assert!(second_retry_after > first_retry_after.saturating_add(600_000));
        assert_eq!(2, second[0].failure_count);
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
            text.contains("sporos_prowlarr_refresh_total{outcome=\"succeeded\",source=\"main\"} 1")
        );
        assert!(
            text.contains("sporos_prowlarr_refresh_total{outcome=\"failed\",source=\"failed\"} 1")
        );
        assert!(text.contains(
            "sporos_prowlarr_refresh_total{outcome=\"rate_limited\",source=\"limited\"} 1"
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
        let metrics = runtime
            .state
            .metrics
            .render_prometheus(&crate::metrics::MetricsSnapshot::default());
        assert!(metrics.contains(
            "sporos_indexer_requests_total{operation=\"capabilities\",outcome=\"succeeded\"} 1"
        ));
    }

    #[tokio::test]
    async fn indexer_caps_refresh_records_rate_limited_metrics() {
        let caps_requests = Arc::new(Mutex::new(Vec::new()));
        let torznab_url = spawn_runtime_torznab_status_server(
            Arc::clone(&caps_requests),
            StatusCode::TOO_MANY_REQUESTS,
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
        let repository = Repository::connect_in_memory().await.unwrap();
        let runtime = AppRuntime::from_repository(config, repository.clone())
            .await
            .unwrap();

        let summary = runtime
            .state
            .refresh_indexer_capabilities(1_000)
            .await
            .unwrap();

        assert_eq!(0, summary.refreshed);
        assert_eq!(1, summary.failed);
        assert_eq!(1, caps_requests.lock().unwrap().len());
        let metrics = runtime
            .state
            .metrics
            .render_prometheus(&crate::metrics::MetricsSnapshot::default());
        assert!(metrics.contains(
            "sporos_indexer_requests_total{operation=\"capabilities\",outcome=\"rate_limited\"} 1"
        ));
    }

    #[tokio::test]
    async fn indexer_caps_refresh_runs_requests_concurrently() {
        let caps_requests = Arc::new(Mutex::new(Vec::new()));
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_in_flight = Arc::new(AtomicUsize::new(0));
        let mut config = SporosConfig::default();
        let repository = Repository::connect_in_memory().await.unwrap();
        for name in [
            "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel",
        ] {
            let torznab_url = spawn_runtime_torznab_delayed_caps_server(
                Arc::clone(&caps_requests),
                Arc::clone(&in_flight),
                Arc::clone(&max_in_flight),
                Duration::from_millis(100),
            )
            .await;
            config.indexers.torznab.insert(
                name.to_owned(),
                TorznabIndexerConfig {
                    url: torznab_url,
                    api_key: Some(ApiKey::new(name).unwrap()),
                    api_key_file: None,
                    api_key_env: None,
                },
            );
            insert_indexer_row(&repository, name, true, None, None, "{}").await;
        }
        let runtime = AppRuntime::from_repository(config, repository)
            .await
            .unwrap();

        let summary = runtime
            .state
            .refresh_indexer_capabilities(1_000)
            .await
            .unwrap();

        assert_eq!(8, summary.refreshed);
        assert_eq!(0, summary.failed);
        assert_eq!(8, caps_requests.lock().unwrap().len());
        assert!(
            max_in_flight.load(Ordering::SeqCst) > 1,
            "expected overlapping caps requests"
        );
        assert!(
            max_in_flight.load(Ordering::SeqCst) <= INDEXER_CAPS_REFRESH_CONCURRENCY,
            "expected caps requests to stay within the concurrency limit"
        );
    }

    #[tokio::test]
    async fn indexer_caps_refresh_refills_slots_when_earlier_result_is_slow() {
        let caps_requests = Arc::new(Mutex::new(Vec::new()));
        let slow_in_flight = Arc::new(AtomicBool::new(false));
        let echo_started_while_slow = Arc::new(AtomicBool::new(false));
        let mut config = SporosConfig::default();
        let repository = Repository::connect_in_memory().await.unwrap();
        for (name, delay, mark_slow, observe_slow) in [
            ("alpha", Duration::from_millis(250), true, false),
            ("bravo", Duration::from_millis(20), false, false),
            ("charlie", Duration::from_millis(20), false, false),
            ("delta", Duration::from_millis(20), false, false),
            ("echo", Duration::from_millis(20), false, true),
        ] {
            let torznab_url = spawn_runtime_torznab_observed_caps_server(
                Arc::clone(&caps_requests),
                delay,
                mark_slow.then(|| Arc::clone(&slow_in_flight)),
                observe_slow.then(|| Arc::clone(&slow_in_flight)),
                observe_slow.then(|| Arc::clone(&echo_started_while_slow)),
            )
            .await;
            config.indexers.torznab.insert(
                name.to_owned(),
                TorznabIndexerConfig {
                    url: torznab_url,
                    api_key: Some(ApiKey::new(name).unwrap()),
                    api_key_file: None,
                    api_key_env: None,
                },
            );
            insert_indexer_row(&repository, name, true, None, None, "{}").await;
        }
        let runtime = AppRuntime::from_repository(config, repository)
            .await
            .unwrap();

        let summary = runtime
            .state
            .refresh_indexer_capabilities(1_000)
            .await
            .unwrap();

        assert_eq!(5, summary.refreshed);
        assert_eq!(0, summary.failed);
        assert_eq!(5, caps_requests.lock().unwrap().len());
        assert!(
            echo_started_while_slow.load(Ordering::SeqCst),
            "expected a freed caps slot to start the fifth request before the first completed"
        );
    }

    #[tokio::test]
    async fn indexer_caps_refresh_records_mixed_success_and_failure_deterministically() {
        let ok_requests = Arc::new(Mutex::new(Vec::new()));
        let rate_limited_requests = Arc::new(Mutex::new(Vec::new()));
        let unavailable_requests = Arc::new(Mutex::new(Vec::new()));
        let ok_url = spawn_runtime_torznab_search_server(Arc::clone(&ok_requests)).await;
        let rate_limited_url = spawn_runtime_torznab_status_server(
            Arc::clone(&rate_limited_requests),
            StatusCode::TOO_MANY_REQUESTS,
        )
        .await;
        let unavailable_url = spawn_runtime_torznab_status_server(
            Arc::clone(&unavailable_requests),
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await;
        let mut config = SporosConfig::default();
        for (name, url) in [
            ("aaa-rate-limited", rate_limited_url),
            ("bbb-ok", ok_url),
            ("ccc-unavailable", unavailable_url),
        ] {
            config.indexers.torznab.insert(
                name.to_owned(),
                TorznabIndexerConfig {
                    url,
                    api_key: Some(ApiKey::new(name).unwrap()),
                    api_key_file: None,
                    api_key_env: None,
                },
            );
        }
        let repository = Repository::connect_in_memory().await.unwrap();
        for name in ["aaa-rate-limited", "bbb-ok", "ccc-unavailable"] {
            insert_indexer_row(&repository, name, true, None, None, "{}").await;
        }
        let runtime = AppRuntime::from_repository(config, repository.clone())
            .await
            .unwrap();

        let summary = runtime
            .state
            .refresh_indexer_capabilities(1_000)
            .await
            .unwrap();

        assert_eq!(1, summary.refreshed);
        assert_eq!(2, summary.failed);
        assert_eq!(1, ok_requests.lock().unwrap().len());
        assert_eq!(1, rate_limited_requests.lock().unwrap().len());
        assert_eq!(1, unavailable_requests.lock().unwrap().len());
        assert_eq!(
            Some("indexer returned HTTP status 500".to_owned()),
            summary.last_error
        );
        let health = repository.dependency_health_snapshot(10).await.unwrap();
        let rate_limited_health = health
            .iter()
            .find(|snapshot| snapshot.dependency_name.as_str() == "aaa-rate-limited")
            .unwrap();
        let ok_health = health
            .iter()
            .find(|snapshot| snapshot.dependency_name.as_str() == "bbb-ok")
            .unwrap();
        let unavailable_health = health
            .iter()
            .find(|snapshot| snapshot.dependency_name.as_str() == "ccc-unavailable")
            .unwrap();
        assert_eq!("healthy", ok_health.state);
        assert_eq!(1_000, ok_health.checked_at_ms);
        assert_eq!("degraded", rate_limited_health.state);
        assert!(rate_limited_health.retry_after_ms.is_some());
        assert_eq!("degraded", unavailable_health.state);
        assert!(unavailable_health.retry_after_ms.is_some());
        let metrics = runtime
            .state
            .metrics
            .render_prometheus(&crate::metrics::MetricsSnapshot::default());
        assert!(metrics.contains(
            "sporos_indexer_requests_total{operation=\"capabilities\",outcome=\"succeeded\"} 1"
        ));
        assert!(metrics.contains(
            "sporos_indexer_requests_total{operation=\"capabilities\",outcome=\"rate_limited\"} 1"
        ));
        assert!(metrics.contains(
            "sporos_indexer_requests_total{operation=\"capabilities\",outcome=\"failed\"} 1"
        ));
    }

    #[tokio::test]
    async fn indexer_caps_refresh_finishes_prior_requests_before_endpoint_error() {
        let ok_requests = Arc::new(Mutex::new(Vec::new()));
        let ok_url = spawn_runtime_torznab_search_server(Arc::clone(&ok_requests)).await;
        let mut config = SporosConfig::default();
        config.indexers.torznab.insert(
            "aaa-ok".to_owned(),
            TorznabIndexerConfig {
                url: ok_url,
                api_key: Some(ApiKey::new("ok-secret").unwrap()),
                api_key_file: None,
                api_key_env: None,
            },
        );
        config.indexers.prowlarr.insert(
            "main".to_owned(),
            test_prowlarr_config("https://prowlarr.example".to_owned(), false, false, "24h"),
        );
        let repository = Repository::connect_in_memory().await.unwrap();
        let runtime = AppRuntime::from_repository(config, repository.clone())
            .await
            .unwrap();
        insert_prowlarr_indexer_row(&repository, "zzz-invalid", "not a url", "main").await;

        let error = runtime
            .state
            .refresh_indexer_capabilities(1_000)
            .await
            .unwrap_err();

        assert_eq!(1, ok_requests.lock().unwrap().len());
        assert!(
            error
                .to_string()
                .contains("build Prowlarr Torznab endpoint")
        );
        let health = repository.dependency_health_snapshot(10).await.unwrap();
        let ok_health = health
            .iter()
            .find(|snapshot| snapshot.dependency_name.as_str() == "aaa-ok")
            .unwrap();
        assert_eq!("healthy", ok_health.state);
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
    async fn search_workflow_runs_indexer_searches_concurrently_with_limit() {
        let queries = Arc::new(Mutex::new(Vec::new()));
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_in_flight = Arc::new(AtomicUsize::new(0));
        let mut config = SporosConfig::default();
        let repository = Repository::connect_in_memory().await.unwrap();
        for name in [
            "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel",
        ] {
            let torznab_url = spawn_runtime_torznab_delayed_search_server(
                Arc::clone(&queries),
                Arc::clone(&in_flight),
                Arc::clone(&max_in_flight),
                Duration::from_millis(100),
                name,
            )
            .await;
            config.indexers.torznab.insert(
                name.to_owned(),
                TorznabIndexerConfig {
                    url: torznab_url,
                    api_key: Some(ApiKey::new(name).unwrap()),
                    api_key_file: None,
                    api_key_env: None,
                },
            );
        }
        let runtime = AppRuntime::from_repository(config, repository.clone())
            .await
            .unwrap();
        for name in [
            "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel",
        ] {
            repository
                .record_indexer_caps_success(
                    &DependencyName::new(name).unwrap(),
                    &movie_caps(),
                    100,
                )
                .await
                .unwrap();
        }

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

        assert_eq!(8, summary.plans.len());
        assert_eq!(8, summary.candidate_count);
        assert_eq!(8, queries.lock().unwrap().len());
        assert!(
            max_in_flight.load(Ordering::SeqCst) > 1,
            "expected overlapping search requests"
        );
        assert!(
            max_in_flight.load(Ordering::SeqCst) <= INDEXER_SEARCH_CONCURRENCY,
            "expected search requests to stay within the concurrency limit"
        );
        assert_eq!(
            vec![
                "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel"
            ],
            summary
                .candidates
                .iter()
                .map(|candidate| candidate.guid.as_str())
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn search_workflow_refills_slots_when_earlier_result_is_slow() {
        let queries = Arc::new(Mutex::new(Vec::new()));
        let slow_in_flight = Arc::new(AtomicBool::new(false));
        let echo_started_while_slow = Arc::new(AtomicBool::new(false));
        let mut config = SporosConfig::default();
        let repository = Repository::connect_in_memory().await.unwrap();
        for (name, delay, mark_slow, observe_slow) in [
            ("alpha", Duration::from_millis(250), true, false),
            ("bravo", Duration::from_millis(20), false, false),
            ("charlie", Duration::from_millis(20), false, false),
            ("delta", Duration::from_millis(20), false, false),
            ("echo", Duration::from_millis(20), false, true),
        ] {
            let torznab_url = spawn_runtime_torznab_observed_search_server(
                Arc::clone(&queries),
                delay,
                name,
                mark_slow.then(|| Arc::clone(&slow_in_flight)),
                observe_slow.then(|| Arc::clone(&slow_in_flight)),
                observe_slow.then(|| Arc::clone(&echo_started_while_slow)),
            )
            .await;
            config.indexers.torznab.insert(
                name.to_owned(),
                TorznabIndexerConfig {
                    url: torznab_url,
                    api_key: Some(ApiKey::new(name).unwrap()),
                    api_key_file: None,
                    api_key_env: None,
                },
            );
        }
        let runtime = AppRuntime::from_repository(config, repository.clone())
            .await
            .unwrap();
        for name in ["alpha", "bravo", "charlie", "delta", "echo"] {
            repository
                .record_indexer_caps_success(
                    &DependencyName::new(name).unwrap(),
                    &movie_caps(),
                    100,
                )
                .await
                .unwrap();
        }

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

        assert_eq!(5, summary.candidate_count);
        assert_eq!(5, queries.lock().unwrap().len());
        assert!(
            echo_started_while_slow.load(Ordering::SeqCst),
            "expected a freed search slot to start the fifth request before the first completed"
        );
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
        let metrics = runtime
            .state
            .metrics
            .render_prometheus(&crate::metrics::MetricsSnapshot::default());
        assert!(metrics.contains(
            "sporos_indexer_requests_total{operation=\"search\",outcome=\"succeeded\"} 1"
        ));
        assert!(metrics.contains(
            "sporos_indexer_requests_total{operation=\"search\",outcome=\"rate_limited\"} 1"
        ));
        let retry_after = repository
            .indexer_registry_snapshot(10)
            .await
            .unwrap()
            .into_iter()
            .find(|row| row.name.as_str() == "failing")
            .unwrap()
            .retry_after_ms;
        assert_eq!(Some(6_000), retry_after);

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
    async fn search_workflow_keeps_candidate_order_with_mixed_concurrent_results() {
        let alpha_queries = Arc::new(Mutex::new(Vec::new()));
        let bravo_queries = Arc::new(Mutex::new(Vec::new()));
        let charlie_queries = Arc::new(Mutex::new(Vec::new()));
        let alpha_url = spawn_runtime_torznab_observed_search_server(
            Arc::clone(&alpha_queries),
            Duration::from_millis(150),
            "alpha",
            None,
            None,
            None,
        )
        .await;
        let bravo_url = spawn_runtime_torznab_status_server(
            Arc::clone(&bravo_queries),
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await;
        let charlie_url = spawn_runtime_torznab_observed_search_server(
            Arc::clone(&charlie_queries),
            Duration::from_millis(10),
            "charlie",
            None,
            None,
            None,
        )
        .await;
        let mut config = SporosConfig::default();
        for (name, url) in [
            ("alpha", alpha_url),
            ("bravo", bravo_url),
            ("charlie", charlie_url),
        ] {
            config.indexers.torznab.insert(
                name.to_owned(),
                TorznabIndexerConfig {
                    url,
                    api_key: Some(ApiKey::new(name).unwrap()),
                    api_key_file: None,
                    api_key_env: None,
                },
            );
        }
        let repository = Repository::connect_in_memory().await.unwrap();
        let runtime = AppRuntime::from_repository(config, repository.clone())
            .await
            .unwrap();
        for name in ["alpha", "bravo", "charlie"] {
            repository
                .record_indexer_caps_success(
                    &DependencyName::new(name).unwrap(),
                    &movie_caps(),
                    100,
                )
                .await
                .unwrap();
        }

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

        assert_eq!(1, summary.failed_indexers);
        assert_eq!(
            vec!["alpha", "bravo", "charlie"],
            summary
                .plans
                .iter()
                .map(|plan| plan.indexer_name.as_str())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            vec!["alpha", "charlie"],
            summary
                .candidates
                .iter()
                .map(|candidate| candidate.guid.as_str())
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn search_workflow_repeated_indexer_failures_back_off_exponentially() {
        let failing_queries = Arc::new(Mutex::new(Vec::new()));
        let failing_url = spawn_runtime_torznab_status_server(
            Arc::clone(&failing_queries),
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await;
        let mut config = SporosConfig::default();
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
            .record_indexer_caps_success(
                &DependencyName::new("failing").unwrap(),
                &movie_caps(),
                100,
            )
            .await
            .unwrap();

        runtime
            .state
            .plan_search_workflow(
                SearchWorkflowRequest {
                    query: ItemTitle::new("Example.Movie.1080p").unwrap(),
                },
                1_000,
            )
            .await
            .unwrap();
        let first_retry_after = repository
            .indexer_registry_snapshot(10)
            .await
            .unwrap()
            .into_iter()
            .find(|row| row.name.as_str() == "failing")
            .unwrap()
            .retry_after_ms
            .unwrap();

        runtime
            .state
            .plan_search_workflow(
                SearchWorkflowRequest {
                    query: ItemTitle::new("Example.Movie.1080p").unwrap(),
                },
                first_retry_after,
            )
            .await
            .unwrap();
        let second_retry_after = repository
            .indexer_registry_snapshot(10)
            .await
            .unwrap()
            .into_iter()
            .find(|row| row.name.as_str() == "failing")
            .unwrap()
            .retry_after_ms
            .unwrap();
        let health = repository.dependency_health_snapshot(10).await.unwrap();

        assert_eq!(2, failing_queries.lock().unwrap().len());
        assert!(second_retry_after > first_retry_after.saturating_add(600_000));
        assert_eq!(2, health[0].failure_count);
    }

    #[tokio::test]
    async fn search_workflow_jitters_simultaneous_indexer_failures_by_name() {
        let first_queries = Arc::new(Mutex::new(Vec::new()));
        let second_queries = Arc::new(Mutex::new(Vec::new()));
        let first_url = spawn_runtime_torznab_status_server(
            Arc::clone(&first_queries),
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await;
        let second_url = spawn_runtime_torznab_status_server(
            Arc::clone(&second_queries),
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await;
        let mut config = SporosConfig::default();
        config.indexers.torznab.insert(
            "alpha".to_owned(),
            TorznabIndexerConfig {
                url: first_url,
                api_key: Some(ApiKey::new("alpha-secret").unwrap()),
                api_key_file: None,
                api_key_env: None,
            },
        );
        config.indexers.torznab.insert(
            "bravo".to_owned(),
            TorznabIndexerConfig {
                url: second_url,
                api_key: Some(ApiKey::new("bravo-secret").unwrap()),
                api_key_file: None,
                api_key_env: None,
            },
        );
        let repository = Repository::connect_in_memory().await.unwrap();
        let runtime = AppRuntime::from_repository(config, repository.clone())
            .await
            .unwrap();
        for name in ["alpha", "bravo"] {
            repository
                .record_indexer_caps_success(
                    &DependencyName::new(name).unwrap(),
                    &movie_caps(),
                    100,
                )
                .await
                .unwrap();
        }

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
        let registry = repository.indexer_registry_snapshot(10).await.unwrap();
        let alpha_retry = registry
            .iter()
            .find(|row| row.name.as_str() == "alpha")
            .unwrap()
            .retry_after_ms
            .unwrap();
        let bravo_retry = registry
            .iter()
            .find(|row| row.name.as_str() == "bravo")
            .unwrap()
            .retry_after_ms
            .unwrap();

        assert_eq!(2, summary.failed_indexers);
        assert_eq!(1, first_queries.lock().unwrap().len());
        assert_eq!(1, second_queries.lock().unwrap().len());
        assert_ne!(alpha_retry, bravo_retry);
        assert!((601_000..631_000).contains(&alpha_retry));
        assert!((601_000..631_000).contains(&bravo_retry));
    }

    #[tokio::test]
    async fn search_workflow_retry_after_overrides_indexer_jitter() {
        let first_queries = Arc::new(Mutex::new(Vec::new()));
        let second_queries = Arc::new(Mutex::new(Vec::new()));
        let first_url = spawn_runtime_torznab_status_server(
            Arc::clone(&first_queries),
            StatusCode::TOO_MANY_REQUESTS,
        )
        .await;
        let second_url = spawn_runtime_torznab_status_server(
            Arc::clone(&second_queries),
            StatusCode::TOO_MANY_REQUESTS,
        )
        .await;
        let mut config = SporosConfig::default();
        config.indexers.torznab.insert(
            "alpha".to_owned(),
            TorznabIndexerConfig {
                url: first_url,
                api_key: Some(ApiKey::new("alpha-secret").unwrap()),
                api_key_file: None,
                api_key_env: None,
            },
        );
        config.indexers.torznab.insert(
            "bravo".to_owned(),
            TorznabIndexerConfig {
                url: second_url,
                api_key: Some(ApiKey::new("bravo-secret").unwrap()),
                api_key_file: None,
                api_key_env: None,
            },
        );
        let repository = Repository::connect_in_memory().await.unwrap();
        let runtime = AppRuntime::from_repository(config, repository.clone())
            .await
            .unwrap();
        for name in ["alpha", "bravo"] {
            repository
                .record_indexer_caps_success(
                    &DependencyName::new(name).unwrap(),
                    &movie_caps(),
                    100,
                )
                .await
                .unwrap();
        }

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
        let registry = repository.indexer_registry_snapshot(10).await.unwrap();

        assert_eq!(2, summary.failed_indexers);
        for name in ["alpha", "bravo"] {
            let retry_after = registry
                .iter()
                .find(|row| row.name.as_str() == name)
                .unwrap()
                .retry_after_ms;
            assert_eq!(Some(6_000), retry_after);
        }
    }

    #[tokio::test]
    async fn search_workflow_success_resets_indexer_failure_count() {
        let queries = Arc::new(Mutex::new(Vec::new()));
        let torznab_url =
            spawn_runtime_torznab_dynamic_server(Arc::clone(&queries), |query, call| {
                if query.contains("t=caps") || call == 2 {
                    (StatusCode::OK, search_rss("candidate-1", "Example"))
                } else {
                    (StatusCode::INTERNAL_SERVER_ERROR, "unavailable".to_owned())
                }
            })
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
        let repository = Repository::connect_in_memory().await.unwrap();
        let runtime = AppRuntime::from_repository(config, repository.clone())
            .await
            .unwrap();
        repository
            .record_indexer_caps_success(&DependencyName::new("main").unwrap(), &movie_caps(), 100)
            .await
            .unwrap();

        runtime
            .state
            .plan_search_workflow(
                SearchWorkflowRequest {
                    query: ItemTitle::new("Example.Movie.1080p").unwrap(),
                },
                1_000,
            )
            .await
            .unwrap();
        let first_retry_after = repository.indexer_registry_snapshot(10).await.unwrap()[0]
            .retry_after_ms
            .unwrap();
        runtime
            .state
            .plan_search_workflow(
                SearchWorkflowRequest {
                    query: ItemTitle::new("Example.Movie.1080p").unwrap(),
                },
                first_retry_after,
            )
            .await
            .unwrap();
        let after_success = repository.dependency_health_snapshot(10).await.unwrap();
        runtime
            .state
            .plan_search_workflow(
                SearchWorkflowRequest {
                    query: ItemTitle::new("Example.Movie.1080p").unwrap(),
                },
                first_retry_after + 1,
            )
            .await
            .unwrap();
        let final_health = repository.dependency_health_snapshot(10).await.unwrap();
        let final_retry_after = repository.indexer_registry_snapshot(10).await.unwrap()[0]
            .retry_after_ms
            .unwrap();

        assert_eq!(3, queries.lock().unwrap().len());
        assert_eq!("healthy", after_success[0].state);
        assert_eq!(0, after_success[0].failure_count);
        assert_eq!(1, final_health[0].failure_count);
        assert!(final_retry_after < first_retry_after.saturating_add(1_200_000));
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
                default_category: None,
                default_tags: vec![crate::config::DEFAULT_INJECTION_METADATA.to_owned()],
                default_label: crate::config::DEFAULT_INJECTION_METADATA.to_owned(),
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
        let metrics = runtime
            .state
            .metrics
            .render_prometheus(&crate::metrics::MetricsSnapshot::default());
        assert!(metrics.contains(
            "sporos_client_requests_total{operation=\"inventory\",outcome=\"succeeded\"} 1"
        ));
    }

    #[tokio::test]
    async fn runtime_fetches_qbit_inventory_files_with_bounded_concurrency() {
        let file_requests = Arc::new(AtomicUsize::new(0));
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_in_flight = Arc::new(AtomicUsize::new(0));
        let endpoint = spawn_runtime_qbit_delayed_files_server(
            file_requests.clone(),
            in_flight,
            max_in_flight.clone(),
        )
        .await;
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
                default_category: None,
                default_tags: vec![crate::config::DEFAULT_INJECTION_METADATA.to_owned()],
                default_label: crate::config::DEFAULT_INJECTION_METADATA.to_owned(),
                label_field: None,
            },
        );
        let repository = Repository::connect_in_memory().await.unwrap();
        let runtime = AppRuntime::from_repository(config, repository.clone())
            .await
            .unwrap();

        let expected_torrents = CLIENT_INVENTORY_FILE_FETCH_CONCURRENCY + 2;
        let started_at = Instant::now();
        let summaries = runtime
            .state
            .refresh_torrent_client_inventories()
            .await
            .unwrap();

        let item_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM local_items")
            .fetch_one(repository.pool())
            .await
            .unwrap();

        assert_eq!(1, summaries.len());
        assert_eq!(expected_torrents, summaries[0].persisted_items);
        assert_eq!(i64::try_from(expected_torrents).unwrap(), item_count);
        assert_eq!(expected_torrents, file_requests.load(Ordering::SeqCst));
        assert!(max_in_flight.load(Ordering::SeqCst) > 1);
        assert!(max_in_flight.load(Ordering::SeqCst) <= CLIENT_INVENTORY_FILE_FETCH_CONCURRENCY);
        assert!(started_at.elapsed() < Duration::from_millis(300));
    }

    #[tokio::test]
    async fn runtime_reports_qbit_inventory_file_fetch_failures_with_hash() {
        let failed_hash = inventory_test_hash(2);
        let endpoint = spawn_runtime_qbit_failed_files_server(failed_hash.clone()).await;
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
                default_category: None,
                default_tags: vec![crate::config::DEFAULT_INJECTION_METADATA.to_owned()],
                default_label: crate::config::DEFAULT_INJECTION_METADATA.to_owned(),
                label_field: None,
            },
        );
        let repository = Repository::connect_in_memory().await.unwrap();
        let runtime = AppRuntime::from_repository(config, repository.clone())
            .await
            .unwrap();

        let error = runtime
            .state
            .refresh_torrent_client_inventories()
            .await
            .unwrap_err();
        let item_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM local_items")
            .fetch_one(repository.pool())
            .await
            .unwrap();

        assert!(error.to_string().contains(failed_hash.as_str()));
        assert!(error.to_string().contains("fetch files for torrent"));
        assert_eq!(0, item_count);
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
                default_category: None,
                default_tags: vec![crate::config::DEFAULT_INJECTION_METADATA.to_owned()],
                default_label: crate::config::DEFAULT_INJECTION_METADATA.to_owned(),
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
            while file_requests.load(Ordering::SeqCst) < CLIENT_INVENTORY_FILE_FETCH_CONCURRENCY {
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
        assert_eq!(
            CLIENT_INVENTORY_FILE_FETCH_CONCURRENCY,
            file_requests.load(Ordering::SeqCst)
        );
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
                default_category: None,
                default_tags: vec![crate::config::DEFAULT_INJECTION_METADATA.to_owned()],
                default_label: crate::config::DEFAULT_INJECTION_METADATA.to_owned(),
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
    async fn runtime_fetches_rtorrent_inventory_files_with_bounded_concurrency() {
        let file_requests = Arc::new(AtomicUsize::new(0));
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_in_flight = Arc::new(AtomicUsize::new(0));
        let endpoint = spawn_runtime_rtorrent_delayed_files_server(
            file_requests.clone(),
            in_flight,
            max_in_flight.clone(),
        )
        .await;
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
                default_category: None,
                default_tags: vec![crate::config::DEFAULT_INJECTION_METADATA.to_owned()],
                default_label: crate::config::DEFAULT_INJECTION_METADATA.to_owned(),
                label_field: Some("custom1".to_owned()),
            },
        );
        let repository = Repository::connect_in_memory().await.unwrap();
        let runtime = AppRuntime::from_repository(config, repository.clone())
            .await
            .unwrap();

        let expected_torrents = CLIENT_INVENTORY_FILE_FETCH_CONCURRENCY + 2;
        let started_at = Instant::now();
        let summaries = runtime
            .state
            .refresh_torrent_client_inventories()
            .await
            .unwrap();

        let item_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM local_items")
            .fetch_one(repository.pool())
            .await
            .unwrap();

        assert_eq!(1, summaries.len());
        assert_eq!(expected_torrents, summaries[0].persisted_items);
        assert_eq!(i64::try_from(expected_torrents).unwrap(), item_count);
        assert_eq!(expected_torrents, file_requests.load(Ordering::SeqCst));
        assert!(max_in_flight.load(Ordering::SeqCst) > 1);
        assert!(max_in_flight.load(Ordering::SeqCst) <= CLIENT_INVENTORY_FILE_FETCH_CONCURRENCY);
        assert!(started_at.elapsed() < Duration::from_millis(300));
    }

    #[tokio::test]
    async fn runtime_reports_rtorrent_inventory_file_fetch_failures_with_hash() {
        let failed_hash = inventory_test_hash(2);
        let endpoint = spawn_runtime_rtorrent_failed_files_server(failed_hash.clone()).await;
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
                default_category: None,
                default_tags: vec![crate::config::DEFAULT_INJECTION_METADATA.to_owned()],
                default_label: crate::config::DEFAULT_INJECTION_METADATA.to_owned(),
                label_field: Some("custom1".to_owned()),
            },
        );
        let repository = Repository::connect_in_memory().await.unwrap();
        let runtime = AppRuntime::from_repository(config, repository.clone())
            .await
            .unwrap();

        let error = runtime
            .state
            .refresh_torrent_client_inventories()
            .await
            .unwrap_err();
        let item_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM local_items")
            .fetch_one(repository.pool())
            .await
            .unwrap();

        assert!(error.to_string().contains(failed_hash.as_str()));
        assert!(error.to_string().contains("fetch files for torrent"));
        assert_eq!(0, item_count);
    }

    #[tokio::test]
    async fn runtime_preserves_rtorrent_inventory_file_unauthorized_failure_class() {
        let endpoint = spawn_runtime_rtorrent_unauthorized_files_server().await;
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
                default_category: None,
                default_tags: vec![crate::config::DEFAULT_INJECTION_METADATA.to_owned()],
                default_label: crate::config::DEFAULT_INJECTION_METADATA.to_owned(),
                label_field: Some("custom1".to_owned()),
            },
        );
        let repository = Repository::connect_in_memory().await.unwrap();
        let runtime = AppRuntime::from_repository(config, repository)
            .await
            .unwrap();

        let error = runtime
            .state
            .refresh_torrent_client_inventories()
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            InventoryRefreshError::Client {
                source: TorrentClientError::Unauthorized { .. }
            }
        ));
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
                default_category: None,
                default_tags: vec![crate::config::DEFAULT_INJECTION_METADATA.to_owned()],
                default_label: crate::config::DEFAULT_INJECTION_METADATA.to_owned(),
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
        assert_eq!(1, file_requests.load(Ordering::SeqCst));
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
                default_category: None,
                default_tags: vec![crate::config::DEFAULT_INJECTION_METADATA.to_owned()],
                default_label: crate::config::DEFAULT_INJECTION_METADATA.to_owned(),
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
                default_category: None,
                default_tags: vec![crate::config::DEFAULT_INJECTION_METADATA.to_owned()],
                default_label: crate::config::DEFAULT_INJECTION_METADATA.to_owned(),
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
    async fn runtime_rejects_invalid_client_inventory_interval() {
        let mut config = SporosConfig::default();
        config.scheduling.client_inventory_interval = "0s".to_owned();
        let repository = Repository::connect_in_memory().await.unwrap();

        let error = AppRuntime::from_repository(config, repository)
            .await
            .unwrap_err();

        assert!(error.to_string().contains("client inventory interval"));
    }

    #[tokio::test]
    async fn runtime_accepts_durable_workflows() {
        let root = unique_temp_dir("app-durable-workflows");
        let config = runtime_test_config(&root);
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
        let unsupported_rss_job = app
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
        let cleanup_job_run = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/jobs/cleanup/runs")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let unsupported_search_job = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/jobs/search/runs")
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
                        r#"{"name":"Example","guid":"guid-1","download_url":"https://93.184.216.34/download","tracker":"tracker"}"#,
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
        assert_eq!(StatusCode::NOT_FOUND, unsupported_rss_job.status());
        assert_eq!(StatusCode::ACCEPTED, cleanup_job_run.status());
        assert_eq!(StatusCode::NOT_FOUND, unsupported_search_job.status());
        assert_eq!(StatusCode::ACCEPTED, announcement.status());
        assert_eq!(StatusCode::SERVICE_UNAVAILABLE, readyz.status());
        assert_eq!(true, status_json["readiness"]["accepting_work"]);
        assert_eq!(false, status_json["readiness"]["processing_ready"]);
        fs::remove_dir_all(root).unwrap();
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

    async fn spawn_runtime_qbit_delayed_files_server(
        file_requests: Arc<AtomicUsize>,
        in_flight: Arc<AtomicUsize>,
        max_in_flight: Arc<AtomicUsize>,
    ) -> String {
        spawn_runtime_test_server(move |request| {
            let file_requests = file_requests.clone();
            let in_flight = in_flight.clone();
            let max_in_flight = max_in_flight.clone();
            async move {
                let path = request.uri().path().to_owned();
                match path.as_str() {
                    "/api/v2/auth/login" => response_with_cookie(StatusCode::OK, "Ok.", "SID=ok"),
                    "/api/v2/torrents/info" => (
                        StatusCode::OK,
                        qbit_inventory_json(CLIENT_INVENTORY_FILE_FETCH_CONCURRENCY + 2),
                    )
                        .into_response(),
                    "/api/v2/torrents/files" => {
                        file_requests.fetch_add(1, Ordering::SeqCst);
                        let active = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                        update_max_atomic(&max_in_flight, active);
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        in_flight.fetch_sub(1, Ordering::SeqCst);
                        (
                            StatusCode::OK,
                            r#"[{"name":"Example/file.mkv","size":42,"progress":1.0,"priority":1}]"#,
                        )
                            .into_response()
                    }
                    _ => (StatusCode::NOT_FOUND, path).into_response(),
                }
            }
        })
        .await
    }

    async fn spawn_runtime_qbit_failed_files_server(failed_hash: InfoHash) -> String {
        spawn_runtime_test_server(move |request| {
            let failed_hash = failed_hash.clone();
            async move {
                let path = request.uri().path().to_owned();
                match path.as_str() {
                    "/api/v2/auth/login" => response_with_cookie(StatusCode::OK, "Ok.", "SID=ok"),
                    "/api/v2/torrents/info" => {
                        (StatusCode::OK, qbit_inventory_json(4)).into_response()
                    }
                    "/api/v2/torrents/files" => {
                        if request
                            .uri()
                            .query()
                            .is_some_and(|query| query.contains(failed_hash.as_str()))
                        {
                            return (StatusCode::INTERNAL_SERVER_ERROR, "file list failed")
                                .into_response();
                        }
                        (
                            StatusCode::OK,
                            r#"[{"name":"Example/file.mkv","size":42,"progress":1.0,"priority":1}]"#,
                        )
                            .into_response()
                    }
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
                        qbit_inventory_json(CLIENT_INVENTORY_FILE_FETCH_CONCURRENCY + 2),
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

    async fn spawn_runtime_rtorrent_delayed_files_server(
        file_requests: Arc<AtomicUsize>,
        in_flight: Arc<AtomicUsize>,
        max_in_flight: Arc<AtomicUsize>,
    ) -> String {
        let app = axum::Router::new().route(
            "/RPC2",
            post(move |request: Request<Body>| {
                let file_requests = file_requests.clone();
                let in_flight = in_flight.clone();
                let max_in_flight = max_in_flight.clone();
                async move {
                    let body = to_bytes(request.into_body(), 65_536).await.unwrap();
                    let body = String::from_utf8(body.to_vec()).unwrap();
                    if body.contains("<methodName>download_list</methodName>") {
                        return (
                            StatusCode::OK,
                            xml_response(&rtorrent_download_list_xml(
                                CLIENT_INVENTORY_FILE_FETCH_CONCURRENCY + 2,
                            )),
                        )
                            .into_response();
                    }
                    if body.contains("<methodName>system.multicall</methodName>")
                        && body.contains("d.custom1")
                    {
                        return (
                            StatusCode::OK,
                            xml_response(&rtorrent_inventory_rows_xml(
                                CLIENT_INVENTORY_FILE_FETCH_CONCURRENCY + 2,
                            )),
                        )
                            .into_response();
                    }
                    if body.contains("<methodName>f.multicall</methodName>") {
                        file_requests.fetch_add(1, Ordering::SeqCst);
                        let active = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                        update_max_atomic(&max_in_flight, active);
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        in_flight.fetch_sub(1, Ordering::SeqCst);
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

    async fn spawn_runtime_rtorrent_failed_files_server(failed_hash: InfoHash) -> String {
        let app = axum::Router::new().route(
            "/RPC2",
            post(move |request: Request<Body>| {
                let failed_hash = failed_hash.clone();
                async move {
                    let body = to_bytes(request.into_body(), 65_536).await.unwrap();
                    let body = String::from_utf8(body.to_vec()).unwrap();
                    if body.contains("<methodName>download_list</methodName>") {
                        return (StatusCode::OK, xml_response(&rtorrent_download_list_xml(4)))
                            .into_response();
                    }
                    if body.contains("<methodName>system.multicall</methodName>")
                        && body.contains("d.custom1")
                    {
                        return (
                            StatusCode::OK,
                            xml_response(&rtorrent_inventory_rows_xml(4)),
                        )
                            .into_response();
                    }
                    if body.contains("<methodName>f.multicall</methodName>") {
                        if body.contains(failed_hash.as_str()) {
                            return (StatusCode::INTERNAL_SERVER_ERROR, "file list failed")
                                .into_response();
                        }
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

    async fn spawn_runtime_rtorrent_unauthorized_files_server() -> String {
        let app = axum::Router::new().route(
            "/RPC2",
            post(move |request: Request<Body>| async move {
                let body = to_bytes(request.into_body(), 65_536).await.unwrap();
                let body = String::from_utf8(body.to_vec()).unwrap();
                if body.contains("<methodName>download_list</methodName>") {
                    return (StatusCode::OK, xml_response(&rtorrent_download_list_xml(1)))
                        .into_response();
                }
                if body.contains("<methodName>system.multicall</methodName>")
                    && body.contains("d.custom1")
                {
                    return (
                        StatusCode::OK,
                        xml_response(&rtorrent_inventory_rows_xml(1)),
                    )
                        .into_response();
                }
                if body.contains("<methodName>f.multicall</methodName>") {
                    return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
                }
                (StatusCode::BAD_REQUEST, body).into_response()
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

    fn update_max_atomic(max: &AtomicUsize, candidate: usize) {
        let mut observed = max.load(Ordering::SeqCst);
        while candidate > observed {
            match max.compare_exchange(observed, candidate, Ordering::SeqCst, Ordering::SeqCst) {
                Ok(_) => break,
                Err(current) => observed = current,
            }
        }
    }

    fn qbit_inventory_json(count: usize) -> String {
        let torrents = (1..=count)
            .map(|index| {
                let hash = inventory_test_hash(index);
                format!(
                    r#"{{"hash":"{}","name":"Torrent {index}","save_path":"/downloads/torrent-{index}","amount_left":0,"progress":1.0}}"#,
                    hash.as_str()
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        format!("[{torrents}]")
    }

    fn rtorrent_download_list_xml(count: usize) -> String {
        let mut rows = String::from("<array><data>");
        for index in 1..=count {
            rows.push_str(&format!(
                "<value><string>{}</string></value>",
                inventory_test_hash(index).as_str()
            ));
        }
        rows.push_str("</data></array>");
        rows
    }

    fn rtorrent_inventory_rows_xml(count: usize) -> String {
        let mut rows = String::from("<array><data>");
        for index in 1..=count {
            rows.push_str(&format!(
                r#"
                  <value><array><data><value><string>Example {index}</string></value></data></array></value>
                  <value><array><data><value><string>/downloads/example-{index}</string></value></data></array></value>
                  <value><array><data><value><i8>0</i8></value></data></array></value>
                  <value><array><data><value><boolean>0</boolean></value></data></array></value>
                  <value><array><data><value><boolean>1</boolean></value></data></array></value>
                  <value><array><data><value><boolean>1</boolean></value></data></array></value>
                  <value><array><data><value><boolean>0</boolean></value></data></array></value>
                  <value><array><data><value><string>sporos</string></value></data></array></value>
                "#
            ));
        }
        rows.push_str("</data></array>");
        rows
    }

    fn inventory_test_hash(index: usize) -> InfoHash {
        InfoHash::new(format!("{index:040x}")).unwrap()
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
        spawn_runtime_torznab_dynamic_server(queries, |query, _call| {
            if query.contains("t=caps") {
                (StatusCode::OK, torznab_caps_xml().to_owned())
            } else {
                (StatusCode::OK, search_rss("candidate-1", "Example"))
            }
        })
        .await
    }

    async fn spawn_runtime_torznab_dynamic_server<F>(
        queries: Arc<Mutex<Vec<String>>>,
        handler: F,
    ) -> String
    where
        F: Fn(&str, usize) -> (StatusCode, String) + Clone + Send + Sync + 'static,
    {
        let handler = Arc::new(handler);
        let app = axum::Router::new().route(
            "/api",
            get(move |request: Request<Body>| {
                let queries = Arc::clone(&queries);
                let handler = Arc::clone(&handler);
                async move {
                    let query = request.uri().query().unwrap_or_default().to_owned();
                    let call = {
                        let mut queries = queries.lock().unwrap();
                        queries.push(query.clone());
                        queries.len()
                    };
                    let (status, body) = handler(&query, call);
                    (status, body)
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

    async fn spawn_runtime_torznab_delayed_caps_server(
        queries: Arc<Mutex<Vec<String>>>,
        in_flight: Arc<AtomicUsize>,
        max_in_flight: Arc<AtomicUsize>,
        delay: Duration,
    ) -> String {
        let app = axum::Router::new().route(
            "/api",
            get(move |request: Request<Body>| {
                let queries = Arc::clone(&queries);
                let in_flight = Arc::clone(&in_flight);
                let max_in_flight = Arc::clone(&max_in_flight);
                async move {
                    queries
                        .lock()
                        .unwrap()
                        .push(request.uri().query().unwrap_or_default().to_owned());
                    let active = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                    update_max_atomic(&max_in_flight, active);
                    tokio::time::sleep(delay).await;
                    in_flight.fetch_sub(1, Ordering::SeqCst);
                    (StatusCode::OK, torznab_caps_xml().to_owned())
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

    async fn spawn_runtime_torznab_observed_caps_server(
        queries: Arc<Mutex<Vec<String>>>,
        delay: Duration,
        mark_slow: Option<Arc<AtomicBool>>,
        observe_slow: Option<Arc<AtomicBool>>,
        started_while_slow: Option<Arc<AtomicBool>>,
    ) -> String {
        let app = axum::Router::new().route(
            "/api",
            get(move |request: Request<Body>| {
                let queries = Arc::clone(&queries);
                let mark_slow = mark_slow.clone();
                let observe_slow = observe_slow.clone();
                let started_while_slow = started_while_slow.clone();
                async move {
                    queries
                        .lock()
                        .unwrap()
                        .push(request.uri().query().unwrap_or_default().to_owned());
                    if let Some(observe_slow) = observe_slow
                        && observe_slow.load(Ordering::SeqCst)
                        && let Some(started_while_slow) = started_while_slow
                    {
                        started_while_slow.store(true, Ordering::SeqCst);
                    }
                    if let Some(mark_slow) = &mark_slow {
                        mark_slow.store(true, Ordering::SeqCst);
                    }
                    tokio::time::sleep(delay).await;
                    if let Some(mark_slow) = &mark_slow {
                        mark_slow.store(false, Ordering::SeqCst);
                    }
                    (StatusCode::OK, torznab_caps_xml().to_owned())
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

    async fn spawn_runtime_torznab_delayed_search_server(
        queries: Arc<Mutex<Vec<String>>>,
        in_flight: Arc<AtomicUsize>,
        max_in_flight: Arc<AtomicUsize>,
        delay: Duration,
        guid: &str,
    ) -> String {
        let guid = guid.to_owned();
        let app = axum::Router::new().route(
            "/api",
            get(move |request: Request<Body>| {
                let queries = Arc::clone(&queries);
                let in_flight = Arc::clone(&in_flight);
                let max_in_flight = Arc::clone(&max_in_flight);
                let guid = guid.clone();
                async move {
                    queries
                        .lock()
                        .unwrap()
                        .push(request.uri().query().unwrap_or_default().to_owned());
                    let active = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                    update_max_atomic(&max_in_flight, active);
                    tokio::time::sleep(delay).await;
                    in_flight.fetch_sub(1, Ordering::SeqCst);
                    (
                        StatusCode::OK,
                        search_rss(&guid, &format!("Example {guid}")),
                    )
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

    async fn spawn_runtime_torznab_observed_search_server(
        queries: Arc<Mutex<Vec<String>>>,
        delay: Duration,
        guid: &str,
        mark_slow: Option<Arc<AtomicBool>>,
        observe_slow: Option<Arc<AtomicBool>>,
        started_while_slow: Option<Arc<AtomicBool>>,
    ) -> String {
        let guid = guid.to_owned();
        let app = axum::Router::new().route(
            "/api",
            get(move |request: Request<Body>| {
                let queries = Arc::clone(&queries);
                let mark_slow = mark_slow.clone();
                let observe_slow = observe_slow.clone();
                let started_while_slow = started_while_slow.clone();
                let guid = guid.clone();
                async move {
                    queries
                        .lock()
                        .unwrap()
                        .push(request.uri().query().unwrap_or_default().to_owned());
                    if let Some(observe_slow) = observe_slow
                        && observe_slow.load(Ordering::SeqCst)
                        && let Some(started_while_slow) = started_while_slow
                    {
                        started_while_slow.store(true, Ordering::SeqCst);
                    }
                    if let Some(mark_slow) = &mark_slow {
                        mark_slow.store(true, Ordering::SeqCst);
                    }
                    tokio::time::sleep(delay).await;
                    if let Some(mark_slow) = &mark_slow {
                        mark_slow.store(false, Ordering::SeqCst);
                    }
                    (
                        StatusCode::OK,
                        search_rss(&guid, &format!("Example {guid}")),
                    )
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

    async fn insert_indexer_row(
        repository: &Repository,
        name: &str,
        enabled: bool,
        retry_after_ms: Option<i64>,
        last_caps_refresh_at_ms: Option<i64>,
        caps_json: &str,
    ) {
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
        .bind(name)
        .bind(format!("https://{name}.example/api"))
        .bind(name)
        .bind(if enabled { 1_i64 } else { 0_i64 })
        .bind(caps_json)
        .bind(retry_after_ms)
        .bind(last_caps_refresh_at_ms)
        .execute(repository.pool())
        .await
        .unwrap();
    }

    async fn insert_prowlarr_indexer_row(
        repository: &Repository,
        name: &str,
        url: &str,
        source_name: &str,
    ) {
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
            VALUES (?, ?, 'prowlarr', ?, ?, 'prowlarr', 1, '{}', 'unknown', NULL, NULL, 1, 1)
            "#,
        )
        .bind(name)
        .bind(url)
        .bind(source_name)
        .bind(name)
        .execute(repository.pool())
        .await
        .unwrap();
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
            insert_indexer_row(
                repository,
                &name,
                enabled,
                retry_after_ms,
                last_caps_refresh_at_ms,
                caps_json,
            )
            .await;
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

    fn unique_temp_dir(label: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("sporos-{label}-{}-{unique}", std::process::id()));
        fs::create_dir_all(&path).unwrap();
        path
    }

    fn runtime_test_config(root: &Path) -> SporosConfig {
        let mut config = SporosConfig::default();
        config.paths.database = root.join("state/sporos.db");
        config.paths.torrent_cache_dir = root.join("cache/torrents");
        config.paths.output_dir = root.join("output");
        fs::create_dir_all(config.paths.database.parent().unwrap()).unwrap();
        fs::create_dir_all(&config.paths.torrent_cache_dir).unwrap();
        fs::create_dir_all(&config.paths.output_dir).unwrap();
        config
    }
}
