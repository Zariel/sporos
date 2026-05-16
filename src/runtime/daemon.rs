use std::fmt;
use std::path::Path;
use std::time::Duration;

use sqlx::Row;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::{error, warn};

use crate::actions::{candidate_output_metadata, save_candidate_torrent};
use crate::announce::{AnnounceReason, AnnounceWorkId};
use crate::config::{SporosConfig, validate_server_auth};
use crate::content_filter::ContentFilterContext;
use crate::domain::{
    CandidateAssessment, CandidateGuid, DependencyName, DownloadUrl, IndexerId, InfoHash,
    InjectionOutcome, ItemTitle, JobName, MatchDecision, ReasonText, RemoteCandidate,
    RemoteCandidateId, TrackerName,
};
use crate::errors::{ConfigError, DatabaseError};
use crate::http::{JobRunWorkflowRequest, SearchWorkflowRequest, router};
use crate::indexers::{
    CachedCandidateTorrent, CandidateDownloadClient, CandidateDownloadError, IndexerBackoffPolicy,
};
use crate::inventory_refresh::run_inventory_refresh_worker;
use crate::matching::{
    PersistedCandidateAssessment, ReverseLookupConfig, ReverseLookupError, ReverseLookupOutcome,
    assess_and_persist_candidate, reverse_lookup_and_assess_candidate, reverse_lookup_candidates,
};
use crate::metrics::{
    ActionOutcome, DecisionOutcome, ExternalOperation, ExternalOutcome, SearchOutcome,
};
use crate::notifications::{NotificationWorker, run_notification_worker};
use crate::persistence::repository::Repository;
use crate::runtime::announce_worker::{
    AnnounceOutcomeConfig, AnnounceWorkOutcome, AnnounceWorker, AnnounceWorkerError,
    classify_injection_result, classify_reverse_lookup_outcome, unix_time_ms,
};
use crate::runtime::app::{AppRuntime, AppState, RuntimeReceivers};
use crate::runtime::health::DependencyKind;
use crate::runtime::injection_worker::{
    InjectionRequest, InjectionWorker, RecheckResumeConfig, SavedTorrentRetryConfig,
};
use crate::runtime::scheduler::{ImmediateRunOutcome, ScheduledJobRun, parse_interval_ms};
use crate::runtime::shutdown::{ShutdownController, ShutdownPhase, ShutdownSignal};
use crate::torrent::parse_metafile;

const BACKGROUND_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
const ANNOUNCE_IDLE_SLEEP: Duration = Duration::from_millis(500);
const SCHEDULER_TICK_INTERVAL: Duration = Duration::from_millis(500);
const ANNOUNCE_CANDIDATE_INDEXER_ID: u64 = i64::MAX as u64;
const SCHEDULER_SHUTDOWN_ERROR: &str = "scheduler is shutting down";

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
    shutdown_policy: BackgroundShutdownPolicy,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum BackgroundShutdownPolicy {
    AbortOnTimeout,
    // Use for workers that may own external side effects and must record a
    // durable outcome instead of being dropped mid-operation.
    AwaitInFlight,
}

impl BackgroundTask {
    fn new(
        name: &'static str,
        handle: JoinHandle<()>,
        shutdown_policy: BackgroundShutdownPolicy,
    ) -> Self {
        Self {
            name,
            handle,
            shutdown_policy,
        }
    }

    const fn should_abort_on_timeout(&self) -> bool {
        matches!(
            self.shutdown_policy,
            BackgroundShutdownPolicy::AbortOnTimeout
        )
    }
}

async fn start_background_tasks(runtime: AppRuntime) -> Result<Vec<BackgroundTask>, DaemonError> {
    runtime
        .state
        .announce_worker
        .recover_startup(unix_time_ms())
        .await
        .map_err(|source| DaemonError::AnnounceStartup { source })?;
    let announce_owner_prefix = announce_worker_owner_prefix();

    let RuntimeReceivers {
        announcements,
        searches,
        jobs,
        scheduler,
        inventory_refresh,
        notifications,
    } = runtime.receivers;

    let mut handles = Vec::new();
    for worker_index in 0..runtime.state.config.announce.worker_concurrency {
        let announce_worker = AnnounceWorker::new(
            runtime.state.repository.clone(),
            &format!("{announce_owner_prefix}-{worker_index}"),
            &runtime.state.config.announce,
        )
        .map_err(|source| DaemonError::AnnounceStartup { source })?;
        handles.push(BackgroundTask::new(
            "announce-worker",
            tokio::spawn(run_announce_worker_loop(
                runtime.state.clone(),
                announce_worker,
                runtime.state.shutdown_signal.clone(),
            )),
            BackgroundShutdownPolicy::AwaitInFlight,
        ));
    }
    handles.push(BackgroundTask::new(
        "inventory-refresh",
        tokio::spawn(run_inventory_refresh_worker(
            runtime.state.inventory_refresh.clone(),
            inventory_refresh,
            runtime.state.shutdown_signal.clone(),
        )),
        BackgroundShutdownPolicy::AbortOnTimeout,
    ));
    handles.push(BackgroundTask::new(
        "notifications",
        tokio::spawn(run_notification_worker(
            NotificationWorker::new(runtime.state.health.clone(), runtime.state.metrics.clone()),
            notifications,
        )),
        BackgroundShutdownPolicy::AbortOnTimeout,
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
        BackgroundShutdownPolicy::AwaitInFlight,
    ));
    let client_inventory_interval = runtime_client_inventory_interval(&runtime.state);
    handles.push(BackgroundTask::new(
        "client-inventory-refresh",
        tokio::spawn(run_client_inventory_refresh_loop(
            runtime.state.clone(),
            client_inventory_interval,
            runtime.state.shutdown_signal.clone(),
        )),
        BackgroundShutdownPolicy::AbortOnTimeout,
    ));
    if let Some(interval) = runtime_prowlarr_refresh_interval(&runtime.state) {
        handles.push(BackgroundTask::new(
            "prowlarr-refresh",
            tokio::spawn(run_prowlarr_refresh_loop(
                runtime.state.clone(),
                interval,
                runtime.state.shutdown_signal.clone(),
            )),
            BackgroundShutdownPolicy::AbortOnTimeout,
        ));
    }
    handles.push(BackgroundTask::new(
        "announcements-receiver",
        tokio::spawn(hold_receiver_open(
            announcements,
            runtime.state.shutdown_signal.clone(),
        )),
        BackgroundShutdownPolicy::AbortOnTimeout,
    ));
    handles.push(BackgroundTask::new(
        "searches-receiver",
        tokio::spawn(run_search_receiver(
            runtime.state.clone(),
            searches,
            runtime.state.shutdown_signal.clone(),
        )),
        BackgroundShutdownPolicy::AwaitInFlight,
    ));
    handles.push(BackgroundTask::new(
        "jobs-receiver",
        tokio::spawn(run_job_receiver(
            runtime.state.clone(),
            jobs,
            runtime.state.shutdown_signal.clone(),
        )),
        BackgroundShutdownPolicy::AwaitInFlight,
    ));
    handles.push(BackgroundTask::new(
        "scheduler-tick",
        tokio::spawn(run_scheduler_tick_loop(
            runtime.state.clone(),
            SCHEDULER_TICK_INTERVAL,
            runtime.state.shutdown_signal.clone(),
        )),
        BackgroundShutdownPolicy::AbortOnTimeout,
    ));
    handles.push(BackgroundTask::new(
        "scheduler-receiver",
        tokio::spawn(run_scheduler_receiver(
            runtime.state.clone(),
            scheduler,
            runtime.state.shutdown_signal.clone(),
        )),
        BackgroundShutdownPolicy::AwaitInFlight,
    ));

    Ok(handles)
}

async fn run_scheduler_tick_loop(
    state: AppState,
    interval: Duration,
    mut shutdown: ShutdownSignal,
) {
    loop {
        tokio::select! {
            _state = shutdown.cancelled() => break,
            () = tokio::time::sleep(interval) => {}
        }

        if shutdown.state().phase != ShutdownPhase::Running {
            break;
        }

        match state.scheduler.tick(unix_time_ms()).await {
            Ok(summary) => {
                if summary.seeded > 0 || summary.enqueued > 0 || summary.deferred > 0 {
                    tracing::debug!(
                        seeded = summary.seeded,
                        enqueued = summary.enqueued,
                        deferred = summary.deferred,
                        "scheduler tick completed"
                    );
                }
            }
            Err(error) => warn!(error = %error, "scheduler tick failed"),
        }
    }
}

fn announce_worker_owner_prefix() -> String {
    format!(
        "sporos-announce-worker-{}-{}",
        std::process::id(),
        unix_time_ms()
    )
}

async fn run_search_receiver(
    state: AppState,
    mut receiver: crate::runtime::queue::WorkReceiver<SearchWorkflowRequest>,
    mut shutdown: ShutdownSignal,
) {
    loop {
        tokio::select! {
            biased;
            _state = shutdown.cancelled() => {
                receiver.close();
                release_queued_search_requests(&mut receiver).await;
                break;
            }
            request = receiver.recv() => {
                let Some(request) = request else {
                    break;
                };
                if shutdown.state().phase != crate::runtime::shutdown::ShutdownPhase::Running {
                    receiver.mark_cancelled();
                    receiver.close();
                    release_queued_search_requests(&mut receiver).await;
                    break;
                }
                match process_search_workflow(state.clone(), request, shutdown.clone()).await {
                    Ok(summary) => {
                        state
                            .metrics
                            .record_search_attempt(search_metric_outcome(&summary));
                        tracing::info!(
                            planned_indexers = summary.planned_indexers,
                            failed_indexers = summary.failed_indexers,
                            candidates = summary.candidates,
                            persisted = summary.persisted,
                            downloaded = summary.downloaded,
                            saved = summary.saved,
                            injected = summary.injected,
                            already_present = summary.already_present,
                            rejected = summary.rejected,
                            failed = summary.failed,
                            "search workflow completed"
                        );
                    }
                    Err(error) => {
                        state.metrics.record_search_attempt(SearchOutcome::Failed);
                        warn!(error = %error, "search workflow query planning failed");
                    }
                }
                receiver.mark_completed();
            }
        }
    }
}

async fn release_queued_search_requests(
    receiver: &mut crate::runtime::queue::WorkReceiver<SearchWorkflowRequest>,
) {
    while receiver.recv().await.is_some() {
        receiver.mark_cancelled();
    }
}

#[derive(Debug, Default, Clone, Eq, PartialEq)]
struct SearchWorkflowExecutionSummary {
    planned_indexers: usize,
    failed_indexers: usize,
    candidates: usize,
    persisted: usize,
    downloaded: usize,
    saved: usize,
    injected: usize,
    already_present: usize,
    rejected: usize,
    failed: usize,
}

async fn process_search_workflow(
    state: AppState,
    request: SearchWorkflowRequest,
    shutdown: ShutdownSignal,
) -> Result<SearchWorkflowExecutionSummary, String> {
    if shutdown.state().phase != ShutdownPhase::Running {
        return Err("search workflow is shutting down".to_owned());
    }

    let now_ms = unix_time_ms();
    let mut planning_shutdown = shutdown.clone();
    let planned = tokio::select! {
        _state = planning_shutdown.cancelled() => {
            return Err("search workflow is shutting down".to_owned());
        }
        result = state.plan_search_workflow(request, now_ms) => {
            result.map_err(|error| error.to_string())?
        }
    };
    let mut summary = SearchWorkflowExecutionSummary {
        planned_indexers: planned.plans.len(),
        failed_indexers: planned.failed_indexers,
        candidates: planned.candidate_count,
        ..SearchWorkflowExecutionSummary::default()
    };

    for candidate in planned.candidates {
        if shutdown.state().phase != ShutdownPhase::Running {
            return Err("search workflow is shutting down".to_owned());
        }
        match process_search_candidate(state.clone(), candidate, now_ms, shutdown.clone()).await {
            Ok(outcome) => summary.record(outcome),
            Err(error) => {
                if shutdown.state().phase != ShutdownPhase::Running {
                    return Err("search workflow is shutting down".to_owned());
                }
                summary.failed += 1;
                warn!(error = %error, "search candidate processing failed");
            }
        }
    }

    Ok(summary)
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum SearchCandidateOutcome {
    Persisted,
    Saved,
    Injected,
    AlreadyPresent,
    Rejected,
}

impl SearchWorkflowExecutionSummary {
    fn record(&mut self, outcome: SearchCandidateOutcome) {
        match outcome {
            SearchCandidateOutcome::Persisted => self.persisted += 1,
            SearchCandidateOutcome::Saved => {
                self.downloaded += 1;
                self.saved += 1;
            }
            SearchCandidateOutcome::Injected => {
                self.downloaded += 1;
                self.injected += 1;
            }
            SearchCandidateOutcome::AlreadyPresent => self.already_present += 1,
            SearchCandidateOutcome::Rejected => self.rejected += 1,
        }
    }
}

fn search_metric_outcome(summary: &SearchWorkflowExecutionSummary) -> SearchOutcome {
    if summary.failed > 0 || summary.failed_indexers > 0 {
        SearchOutcome::Failed
    } else if summary.saved > 0 || summary.injected > 0 || summary.already_present > 0 {
        SearchOutcome::Succeeded
    } else {
        SearchOutcome::NoMatch
    }
}

async fn process_search_candidate(
    state: AppState,
    candidate: RemoteCandidate,
    now_ms: i64,
    shutdown: ShutdownSignal,
) -> Result<SearchCandidateOutcome, String> {
    let candidate = hydrate_search_candidate(&state.repository, candidate)
        .await
        .map_err(|error| error.to_string())?;
    state
        .repository
        .upsert_remote_candidate(&candidate)
        .await
        .map_err(|error| error.to_string())?;
    let config = ReverseLookupConfig::default();
    let initial = reverse_lookup_and_assess_candidate(
        &state.repository,
        &candidate,
        &[],
        now_ms,
        ContentFilterContext::Search,
        &config,
    )
    .await
    .map_err(|error| format!("{error:?}"))?;

    match initial {
        ReverseLookupOutcome::NeedsTorrentDownload { .. } => {
            if shutdown.state().phase != ShutdownPhase::Running {
                return Err("search workflow is shutting down".to_owned());
            }
            let downloader = CandidateDownloadClient::new(Duration::from_secs(120));
            let mut download_shutdown = shutdown.clone();
            let started = Instant::now();
            let cached = tokio::select! {
                _state = download_shutdown.cancelled() => {
                    return Err("search workflow is shutting down".to_owned());
                }
                result = downloader.download_and_cache(
                    &candidate,
                    &state.config.paths.torrent_cache_dir,
                    None,
                ) => {
                    match result {
                        Ok(cached) => {
                            state.metrics.record_indexer_request(
                                ExternalOperation::Download,
                                ExternalOutcome::Succeeded,
                                elapsed_ms(started),
                            );
                            cached
                        }
                        Err(error) => {
                            state.metrics.record_indexer_request(
                                ExternalOperation::Download,
                                candidate_download_metric_outcome(&error),
                                elapsed_ms(started),
                            );
                            record_candidate_download_failure(
                                &state,
                                &candidate,
                                &error,
                                now_ms,
                            )
                            .await
                            .map_err(|error| error.to_string())?;
                            return Err(error.to_string());
                        }
                    }
                }
            };
            let torrent_bytes = read_cached_torrent(&cached.cache_path).await?;
            process_downloaded_search_candidate(state, cached, torrent_bytes, now_ms, shutdown)
                .await
        }
        ReverseLookupOutcome::Matched { assessment, .. } => {
            let Some((cached, torrent_bytes)) = cached_search_candidate(&candidate).await? else {
                record_persisted_decision(&state, &assessment);
                return Ok(SearchCandidateOutcome::Persisted);
            };
            process_downloaded_search_candidate(state, cached, torrent_bytes, now_ms, shutdown)
                .await
        }
        ReverseLookupOutcome::AlreadyPresent { assessment, .. } => {
            record_persisted_decision(&state, &assessment);
            Ok(SearchCandidateOutcome::AlreadyPresent)
        }
        ReverseLookupOutcome::BestFailure { assessment, .. } => {
            record_persisted_decision(&state, &assessment);
            Ok(SearchCandidateOutcome::Rejected)
        }
        ReverseLookupOutcome::NoCandidates => Ok(SearchCandidateOutcome::Persisted),
    }
}

async fn hydrate_search_candidate(
    repository: &Repository,
    mut candidate: RemoteCandidate,
) -> Result<RemoteCandidate, DatabaseError> {
    let indexer_id =
        i64::try_from(candidate.indexer_id.get()).map_err(|error| DatabaseError::QueryFailed {
            operation: "hydrate search candidate".to_owned(),
            message: error.to_string(),
        })?;
    let row = sqlx::query(
        r#"
        SELECT info_hash, torrent_cache_path
        FROM remote_candidates
        WHERE indexer_id = ? AND guid = ?
        "#,
    )
    .bind(indexer_id)
    .bind(candidate.guid.as_str())
    .fetch_optional(repository.pool())
    .await
    .map_err(|error| DatabaseError::QueryFailed {
        operation: "hydrate search candidate".to_owned(),
        message: error.to_string(),
    })?;

    if let Some(row) = row {
        if candidate.info_hash.is_none() {
            candidate.info_hash = row
                .get::<Option<String>, _>("info_hash")
                .map(InfoHash::new)
                .transpose()
                .map_err(domain_database_error)?;
        }
        if candidate.torrent_cache_path.is_none() {
            candidate.torrent_cache_path = row
                .get::<Option<String>, _>("torrent_cache_path")
                .map(std::path::PathBuf::from);
        }
    }

    Ok(candidate)
}

async fn cached_search_candidate(
    candidate: &RemoteCandidate,
) -> Result<Option<(CachedCandidateTorrent, Vec<u8>)>, String> {
    let Some(cache_path) = candidate.torrent_cache_path.as_ref() else {
        return Ok(None);
    };
    let torrent_bytes = read_cached_torrent(cache_path).await?;
    let parsed = parse_metafile(&torrent_bytes).map_err(|error| error.to_string())?;
    let mut cached_candidate = candidate.clone();
    cached_candidate.info_hash = Some(parsed.metafile.info_hash().clone());
    cached_candidate.torrent_cache_path = Some(cache_path.clone());
    Ok(Some((
        CachedCandidateTorrent {
            candidate: cached_candidate,
            metafile: parsed.metafile,
            tracker_hosts: parsed.tracker_hosts,
            cache_path: cache_path.clone(),
        },
        torrent_bytes,
    )))
}

async fn process_downloaded_search_candidate(
    state: AppState,
    cached: CachedCandidateTorrent,
    torrent_bytes: Vec<u8>,
    now_ms: i64,
    shutdown: ShutdownSignal,
) -> Result<SearchCandidateOutcome, String> {
    let config = ReverseLookupConfig::default();
    let lookups = reverse_lookup_candidates(
        &state.repository,
        &cached.candidate,
        ContentFilterContext::Search,
        &config,
    )
    .await
    .map_err(|error| format!("{error:?}"))?;
    let mut best_failure = None;

    for lookup in lookups {
        if shutdown.state().phase != ShutdownPhase::Running {
            return Err("search workflow is shutting down".to_owned());
        }
        let assessment = assess_and_persist_candidate(
            &state.repository,
            &lookup.local_item,
            &lookup.local_files,
            lookup.local_files_truncated,
            &cached.candidate,
            &[],
            now_ms,
            &config.assessment,
        )
        .await
        .map_err(|error| format!("{error:?}"))?;
        record_persisted_decision(&state, &assessment);
        if let Some((candidate_id, assessment)) = actionable_assessment(&assessment) {
            if shutdown.state().phase != ShutdownPhase::Running {
                return Err("search workflow is shutting down".to_owned());
            }
            if state.injection_worker.client_count() == 0 {
                let save = save_candidate_torrent(
                    &state.config.paths.output_dir,
                    &candidate_output_metadata(
                        lookup.local_item.media_type,
                        &cached.candidate,
                        &cached.metafile,
                    ),
                    &torrent_bytes,
                );
                match save {
                    Ok(outcome) => state.metrics.record_action(outcome.action_outcome()),
                    Err(error) => {
                        state.metrics.record_action(ActionOutcome::Failed);
                        return Err(error.to_string());
                    }
                }
                return Ok(SearchCandidateOutcome::Saved);
            }
            let result = state
                .injection_worker
                .process_until_shutdown(
                    InjectionRequest {
                        local_item: lookup.local_item,
                        local_files: lookup.local_files,
                        candidate: cached.candidate.clone(),
                        candidate_id,
                        metafile: cached.metafile,
                        torrent_bytes,
                        assessment,
                        assessed_at_ms: now_ms,
                        output_dir: state.config.paths.output_dir,
                        link_dirs: Vec::new(),
                        link_type: None,
                        flat_linking: false,
                        recheck: RecheckResumeConfig::default(),
                    },
                    shutdown.clone(),
                )
                .await;
            let result = match result {
                Ok(result) => result,
                Err(error) => {
                    state.metrics.record_action(ActionOutcome::Failed);
                    return Err(format!("{error:?}"));
                }
            };
            state
                .metrics
                .record_action(injection_metric_outcome(result.outcome));
            return Ok(match result.outcome {
                InjectionOutcome::Injected => SearchCandidateOutcome::Injected,
                InjectionOutcome::Saved => SearchCandidateOutcome::Saved,
                InjectionOutcome::AlreadyExists => SearchCandidateOutcome::AlreadyPresent,
                InjectionOutcome::SourceIncomplete | InjectionOutcome::Failed => {
                    SearchCandidateOutcome::Rejected
                }
            });
        }
        best_failure = Some(assessment);
    }

    Ok(best_failure.map_or(SearchCandidateOutcome::Persisted, |_| {
        SearchCandidateOutcome::Rejected
    }))
}

fn record_persisted_decision(state: &AppState, assessment: &PersistedCandidateAssessment) {
    let Some(decision) = persisted_decision(assessment) else {
        return;
    };
    state
        .metrics
        .record_decision(decision_metric_outcome(decision));
}

fn persisted_decision(assessment: &PersistedCandidateAssessment) -> Option<MatchDecision> {
    match assessment {
        PersistedCandidateAssessment::Assessed { assessment, .. }
        | PersistedCandidateAssessment::Rejected { assessment, .. } => Some(assessment.decision),
        PersistedCandidateAssessment::NeedsTorrentDownload { .. } => None,
    }
}

fn decision_metric_outcome(decision: MatchDecision) -> DecisionOutcome {
    match decision {
        MatchDecision::Exact => DecisionOutcome::ExactMatch,
        MatchDecision::SizeOnly => DecisionOutcome::SizeOnlyMatch,
        MatchDecision::Partial => DecisionOutcome::PartialMatch,
        MatchDecision::NoMatch | MatchDecision::Rejected => DecisionOutcome::Rejected,
    }
}

fn injection_metric_outcome(outcome: InjectionOutcome) -> ActionOutcome {
    match outcome {
        InjectionOutcome::Injected => ActionOutcome::Injected,
        InjectionOutcome::Saved => ActionOutcome::Saved,
        InjectionOutcome::AlreadyExists => ActionOutcome::AlreadyExisting,
        InjectionOutcome::SourceIncomplete | InjectionOutcome::Failed => ActionOutcome::Failed,
    }
}

fn candidate_download_metric_outcome(error: &CandidateDownloadError) -> ExternalOutcome {
    match error {
        CandidateDownloadError::RateLimited { .. } => ExternalOutcome::RateLimited,
        CandidateDownloadError::HttpStatus { status, .. } if *status == 429 => {
            ExternalOutcome::RateLimited
        }
        CandidateDownloadError::HttpStatus { .. }
        | CandidateDownloadError::MagnetLink
        | CandidateDownloadError::Timeout
        | CandidateDownloadError::Request { .. }
        | CandidateDownloadError::InvalidContents { .. }
        | CandidateDownloadError::ResponseTooLarge { .. }
        | CandidateDownloadError::CacheWrite { .. } => ExternalOutcome::Failed,
    }
}

async fn record_candidate_download_failure(
    state: &AppState,
    candidate: &RemoteCandidate,
    error: &CandidateDownloadError,
    now_ms: i64,
) -> Result<(), DatabaseError> {
    if !candidate_download_is_dependency_failure(error) {
        return Ok(());
    }
    let name = DependencyName::new(candidate.tracker.as_str()).map_err(|error| {
        DatabaseError::QueryFailed {
            operation: "build candidate download dependency name".to_owned(),
            message: error.to_string(),
        }
    })?;
    let reason =
        ReasonText::new(error.to_string()).map_err(|error| DatabaseError::QueryFailed {
            operation: "build candidate download health reason".to_owned(),
            message: error.to_string(),
        })?;
    let failure_count = state
        .repository
        .dependency_failure_count("indexer", &name)
        .await?;
    let retry_after_ms =
        candidate_download_retry_after(error, now_ms, failure_count, name.as_str());
    if candidate_download_error_is_unavailable(error) {
        state.health.set_unavailable(
            DependencyKind::Indexer,
            name.clone(),
            reason.clone(),
            Some(retry_after_ms),
        );
    } else {
        state.health.set_degraded(
            DependencyKind::Indexer,
            name.clone(),
            reason.clone(),
            Some(retry_after_ms),
        );
    }
    state
        .repository
        .record_indexer_request_backoff(
            &name,
            &reason,
            retry_after_ms,
            now_ms,
            candidate_download_error_is_unavailable(error),
        )
        .await
}

fn candidate_download_is_dependency_failure(error: &CandidateDownloadError) -> bool {
    match error {
        CandidateDownloadError::RateLimited { .. }
        | CandidateDownloadError::Timeout
        | CandidateDownloadError::Request { .. }
        | CandidateDownloadError::ResponseTooLarge { .. } => true,
        CandidateDownloadError::HttpStatus { status, .. } => *status == 429 || *status >= 500,
        CandidateDownloadError::MagnetLink
        | CandidateDownloadError::InvalidContents { .. }
        | CandidateDownloadError::CacheWrite { .. } => false,
    }
}

fn candidate_download_retry_after(
    error: &CandidateDownloadError,
    now_ms: i64,
    consecutive_failures: u16,
    jitter_key: &str,
) -> i64 {
    let policy = IndexerBackoffPolicy::default();
    match error {
        CandidateDownloadError::RateLimited { retry_after }
        | CandidateDownloadError::HttpStatus { retry_after, .. } => {
            policy.retry_after_deadline(now_ms, consecutive_failures, *retry_after, jitter_key)
        }
        CandidateDownloadError::MagnetLink
        | CandidateDownloadError::Timeout
        | CandidateDownloadError::Request { .. }
        | CandidateDownloadError::InvalidContents { .. }
        | CandidateDownloadError::ResponseTooLarge { .. }
        | CandidateDownloadError::CacheWrite { .. } => {
            policy.retry_after_deadline(now_ms, consecutive_failures, None, jitter_key)
        }
    }
}

fn candidate_download_error_is_unavailable(error: &CandidateDownloadError) -> bool {
    match error {
        CandidateDownloadError::RateLimited { .. } => false,
        CandidateDownloadError::HttpStatus { status, .. } => *status >= 500,
        CandidateDownloadError::Timeout
        | CandidateDownloadError::Request { .. }
        | CandidateDownloadError::ResponseTooLarge { .. } => true,
        CandidateDownloadError::MagnetLink
        | CandidateDownloadError::InvalidContents { .. }
        | CandidateDownloadError::CacheWrite { .. } => false,
    }
}

fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

async fn run_job_receiver(
    state: AppState,
    mut receiver: crate::runtime::queue::WorkReceiver<JobRunWorkflowRequest>,
    mut shutdown: ShutdownSignal,
) {
    loop {
        tokio::select! {
            _state = shutdown.cancelled() => {
                receiver.close();
                release_queued_job_requests(&state, &mut receiver).await;
                break;
            }
            request = receiver.recv() => {
                let Some(request) = request else {
                    break;
                };
                let job_name = request.job_name.clone();
                match state
                    .scheduler
                    .enqueue_immediate_run(&request.job_name, unix_time_ms())
                    .await
                {
                    Ok(ImmediateRunOutcome::Queued) => {
                        tracing::info!(job_name = %job_name, "scheduled job run queued");
                    }
                    Ok(ImmediateRunOutcome::Coalesced) => {
                        tracing::info!(job_name = %job_name, "scheduled job run already active");
                    }
                    Ok(ImmediateRunOutcome::Deferred) => {
                        warn!(job_name = %job_name, "scheduled job run deferred");
                    }
                    Err(error) => warn!(job_name = %job_name, error = %error, "scheduled job trigger failed"),
                }
                receiver.mark_completed();
            }
        }
    }
}

async fn release_queued_job_requests(
    state: &AppState,
    receiver: &mut crate::runtime::queue::WorkReceiver<JobRunWorkflowRequest>,
) {
    while let Some(request) = receiver.recv().await {
        let now_ms = unix_time_ms();
        if let Err(error) = state
            .scheduler
            .complete_failure(&request.job_name, now_ms, "scheduler shutting down")
            .await
        {
            warn!(
                job_name = %request.job_name,
                error = %error,
                "queued scheduled job request shutdown release failed"
            );
        }
        receiver.mark_completed();
    }
}

async fn run_scheduler_receiver(
    state: AppState,
    mut receiver: crate::runtime::queue::WorkReceiver<ScheduledJobRun>,
    mut shutdown: ShutdownSignal,
) {
    loop {
        tokio::select! {
            _state = shutdown.cancelled() => {
                receiver.close();
                release_queued_scheduler_runs(&state, &mut receiver).await;
                break;
            }
            run = receiver.recv() => {
                let Some(run) = run else {
                    break;
                };
                process_scheduled_job_run(&state, run, shutdown.clone()).await;
                receiver.mark_completed();
            }
        }
    }
}

async fn release_queued_scheduler_runs(
    state: &AppState,
    receiver: &mut crate::runtime::queue::WorkReceiver<ScheduledJobRun>,
) {
    while let Some(run) = receiver.recv().await {
        let now_ms = unix_time_ms();
        if let Err(error) = state
            .scheduler
            .complete_shutdown(&run.job_name, now_ms)
            .await
        {
            warn!(
                job_name = %run.job_name,
                error = %error,
                "scheduled job shutdown release failed"
            );
        }
        receiver.mark_completed();
    }
}

async fn process_scheduled_job_run(
    state: &AppState,
    run: ScheduledJobRun,
    shutdown: ShutdownSignal,
) {
    let started = Instant::now();
    let job_name = run.job_name.clone();
    let result = execute_scheduled_job(state, &job_name, shutdown.clone()).await;
    let finished_at_ms = unix_time_ms();
    let duration_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;

    match result {
        Ok(()) => {
            if let Err(error) = state
                .scheduler
                .complete_success(&job_name, finished_at_ms)
                .await
            {
                warn!(job_name = %job_name, error = %error, "scheduled job success status update failed");
            }
            state.metrics.record_job_duration(
                job_name.as_str(),
                ExternalOutcome::Succeeded,
                duration_ms,
            );
            tracing::info!(
                job_name = %job_name,
                scheduled_at_ms = run.scheduled_at_ms,
                "scheduled job completed"
            );
        }
        Err(error) => {
            if error == SCHEDULER_SHUTDOWN_ERROR && shutdown.state().phase != ShutdownPhase::Running
            {
                if let Err(status_error) = state
                    .scheduler
                    .complete_shutdown(&job_name, finished_at_ms)
                    .await
                {
                    warn!(
                        job_name = %job_name,
                        error = %status_error,
                        "scheduled job shutdown status update failed"
                    );
                }
                state.metrics.record_job_duration(
                    job_name.as_str(),
                    ExternalOutcome::Failed,
                    duration_ms,
                );
                warn!(job_name = %job_name, error = %error, "scheduled job stopped for shutdown");
                return;
            }
            if let Err(status_error) = state
                .scheduler
                .complete_failure(&job_name, finished_at_ms, &error)
                .await
            {
                warn!(
                    job_name = %job_name,
                    error = %status_error,
                    "scheduled job failure status update failed"
                );
            }
            state.metrics.record_job_duration(
                job_name.as_str(),
                ExternalOutcome::Failed,
                duration_ms,
            );
            warn!(job_name = %job_name, error = %error, "scheduled job failed");
        }
    }
}

async fn execute_scheduled_job(
    state: &AppState,
    job_name: &JobName,
    mut shutdown: ShutdownSignal,
) -> Result<(), String> {
    if shutdown.state().phase != ShutdownPhase::Running {
        return Err(SCHEDULER_SHUTDOWN_ERROR.to_owned());
    }

    match job_name.as_str() {
        "indexer_caps" => {
            let summary = tokio::select! {
                _state = shutdown.cancelled() => {
                    return Err(SCHEDULER_SHUTDOWN_ERROR.to_owned());
                }
                result = state.refresh_indexer_capabilities(unix_time_ms()) => {
                    result.map_err(|error| error.to_string())?
                }
            };
            if summary.refreshed > 0 && summary.failed == 0 {
                Ok(())
            } else if summary.failed == 0 && summary.skipped_backoff > 0 {
                Err(match summary.next_backoff_deadline_ms {
                    Some(deadline) => {
                        format!("indexer caps refresh is in backoff until {deadline}")
                    }
                    None => "indexer caps refresh is in backoff".to_owned(),
                })
            } else {
                Err(summary
                    .last_error
                    .unwrap_or_else(|| "indexer caps refresh failed".to_owned()))
            }
        }
        "rss" | "search" | "cleanup" => Err(format!(
            "scheduled job {} does not have an executor yet",
            job_name.as_str()
        )),
        other => Err(format!("unknown scheduled job {other}")),
    }
}

async fn run_announce_worker_loop(
    state: AppState,
    worker: AnnounceWorker,
    mut shutdown: ShutdownSignal,
) {
    loop {
        if shutdown.state().phase != ShutdownPhase::Running {
            break;
        }

        let batch = worker
            .run_batch(unix_time_ms(), shutdown.clone(), |id, shutdown| {
                process_announce_work(state.clone(), id, shutdown)
            })
            .await;
        match batch {
            Ok(summary) => {
                if summary.claimed > 0 {
                    tracing::info!(
                        claimed = summary.claimed,
                        completed = summary.completed,
                        released = summary.released,
                        cancelled = summary.cancelled,
                        "announce worker batch completed"
                    );
                }
            }
            Err(error) => warn!(error = %error, "announce worker batch failed"),
        }

        tokio::select! {
            _state = shutdown.cancelled() => break,
            () = tokio::time::sleep(ANNOUNCE_IDLE_SLEEP) => {}
        }
    }
}

async fn process_announce_work(
    state: AppState,
    id: AnnounceWorkId,
    shutdown: ShutdownSignal,
) -> AnnounceWorkOutcome {
    let now_ms = unix_time_ms();
    if shutdown.state().phase != ShutdownPhase::Running {
        return AnnounceWorkOutcome::Release {
            reason: AnnounceReason::DependencyBackoff,
            next_attempt_at_ms: now_ms,
        };
    }

    let candidate = match load_announce_candidate(&state.repository, &id).await {
        Ok(Some(candidate)) => candidate,
        Ok(None) => {
            return AnnounceWorkOutcome::TerminalFailed {
                reason: AnnounceReason::InvalidRequest,
                redacted_message: "announce work was not found".to_owned(),
            };
        }
        Err(error) => return retryable_database_outcome(error, now_ms),
    };
    let config = ReverseLookupConfig::default();
    let initial = match reverse_lookup_and_assess_candidate(
        &state.repository,
        &candidate.candidate,
        &[],
        now_ms,
        ContentFilterContext::Announcement,
        &config,
    )
    .await
    {
        Ok(outcome) => outcome,
        Err(error) => return classify_reverse_lookup_error(error, now_ms),
    };

    match initial {
        ReverseLookupOutcome::NeedsTorrentDownload { .. } => {}
        outcome => return classify_reverse_lookup_outcome(&outcome, now_ms, outcome_config()),
    }

    if shutdown.state().phase != ShutdownPhase::Running {
        return AnnounceWorkOutcome::Release {
            reason: AnnounceReason::DependencyBackoff,
            next_attempt_at_ms: now_ms,
        };
    }

    let Some(fetch) = candidate.cookie_or_fetch.as_ref() else {
        return AnnounceWorkOutcome::Waiting {
            reason: AnnounceReason::CandidateDownloading,
            next_attempt_at_ms: now_ms.saturating_add(outcome_config().candidate_download_wait_ms),
            dependency: None,
        };
    };
    let downloader = CandidateDownloadClient::new(Duration::from_secs(120));
    let mut download_shutdown = shutdown.clone();
    let started = Instant::now();
    let cached = tokio::select! {
        _state = download_shutdown.cancelled() => {
            return AnnounceWorkOutcome::Release {
                reason: AnnounceReason::DependencyBackoff,
                next_attempt_at_ms: now_ms,
            };
        }
        result = downloader.download_and_cache(
            &candidate.candidate,
            &state.config.paths.torrent_cache_dir,
            fetch.cookie.as_deref(),
        ) => {
            match result {
                Ok(cached) => {
                    state.metrics.record_indexer_request(
                        ExternalOperation::Download,
                        ExternalOutcome::Succeeded,
                        elapsed_ms(started),
                    );
                    cached
                }
                Err(error) => {
                    state.metrics.record_indexer_request(
                        ExternalOperation::Download,
                        candidate_download_metric_outcome(&error),
                        elapsed_ms(started),
                    );
                    if let Err(error) =
                        record_candidate_download_failure(&state, &candidate.candidate, &error, now_ms)
                            .await
                    {
                        return retryable_database_outcome(error, now_ms);
                    }
                    return classify_candidate_download_error(error, now_ms);
                }
            }
        }
    };
    let torrent_bytes = match read_cached_torrent(&cached.cache_path).await {
        Ok(bytes) => bytes,
        Err(error) => return retryable_worker_outcome("torrent-cache", error, now_ms),
    };

    match process_downloaded_announce_candidate(state, cached, torrent_bytes, now_ms, shutdown)
        .await
    {
        Ok(outcome) => outcome,
        Err(error) => retryable_worker_outcome("announce", error, now_ms),
    }
}

async fn process_downloaded_announce_candidate(
    state: AppState,
    cached: CachedCandidateTorrent,
    torrent_bytes: Vec<u8>,
    now_ms: i64,
    shutdown: ShutdownSignal,
) -> Result<AnnounceWorkOutcome, String> {
    let config = ReverseLookupConfig::default();
    let lookups = reverse_lookup_candidates(
        &state.repository,
        &cached.candidate,
        ContentFilterContext::Announcement,
        &config,
    )
    .await
    .map_err(|error| format!("{error:?}"))?;
    let mut best_failure = None;

    for lookup in lookups {
        if shutdown.state().phase != ShutdownPhase::Running {
            return Ok(AnnounceWorkOutcome::Release {
                reason: AnnounceReason::DependencyBackoff,
                next_attempt_at_ms: now_ms,
            });
        }
        let assessment = assess_and_persist_candidate(
            &state.repository,
            &lookup.local_item,
            &lookup.local_files,
            lookup.local_files_truncated,
            &cached.candidate,
            &[],
            now_ms,
            &config.assessment,
        )
        .await
        .map_err(|error| format!("{error:?}"))?;
        if let Some((candidate_id, assessment)) = actionable_assessment(&assessment) {
            if shutdown.state().phase != ShutdownPhase::Running {
                return Ok(AnnounceWorkOutcome::Release {
                    reason: AnnounceReason::DependencyBackoff,
                    next_attempt_at_ms: now_ms,
                });
            }
            if state.injection_worker.client_count() == 0 {
                let save = save_candidate_torrent(
                    &state.config.paths.output_dir,
                    &candidate_output_metadata(
                        lookup.local_item.media_type,
                        &cached.candidate,
                        &cached.metafile,
                    ),
                    &torrent_bytes,
                );
                match save {
                    Ok(outcome) => state.metrics.record_action(outcome.action_outcome()),
                    Err(error) => {
                        state.metrics.record_action(ActionOutcome::Failed);
                        return Err(error.to_string());
                    }
                }
                return Ok(AnnounceWorkOutcome::Succeeded {
                    reason: AnnounceReason::Saved,
                    outcome: "saved".to_owned(),
                });
            }
            if shutdown.state().phase != ShutdownPhase::Running {
                return Ok(AnnounceWorkOutcome::Release {
                    reason: AnnounceReason::DependencyBackoff,
                    next_attempt_at_ms: now_ms,
                });
            }
            let result = state
                .injection_worker
                .process_until_shutdown(
                    InjectionRequest {
                        local_item: lookup.local_item,
                        local_files: lookup.local_files,
                        candidate: cached.candidate.clone(),
                        candidate_id,
                        metafile: cached.metafile,
                        torrent_bytes,
                        assessment,
                        assessed_at_ms: now_ms,
                        output_dir: state.config.paths.output_dir,
                        link_dirs: Vec::new(),
                        link_type: None,
                        flat_linking: false,
                        recheck: RecheckResumeConfig::default(),
                    },
                    shutdown.clone(),
                )
                .await;
            let result = match result {
                Ok(result) => result,
                Err(error) => {
                    state.metrics.record_action(ActionOutcome::Failed);
                    return Err(format!("{error:?}"));
                }
            };
            state
                .metrics
                .record_action(injection_metric_outcome(result.outcome));
            return Ok(classify_injection_result(&result, now_ms, outcome_config()));
        }
        best_failure = Some(ReverseLookupOutcome::BestFailure {
            local_item: lookup.local_item,
            assessment,
        });
    }

    Ok(best_failure.map_or_else(
        || {
            classify_reverse_lookup_outcome(
                &ReverseLookupOutcome::NoCandidates,
                now_ms,
                outcome_config(),
            )
        },
        |outcome| classify_reverse_lookup_outcome(&outcome, now_ms, outcome_config()),
    ))
}

#[derive(Debug, Clone)]
struct RuntimeAnnounceCandidate {
    candidate: RemoteCandidate,
    cookie_or_fetch: Option<RuntimeAnnounceFetch>,
}

#[derive(Debug, Clone)]
struct RuntimeAnnounceFetch {
    cookie: Option<String>,
}

async fn load_announce_candidate(
    repository: &Repository,
    id: &AnnounceWorkId,
) -> Result<Option<RuntimeAnnounceCandidate>, DatabaseError> {
    let row = sqlx::query(
        r#"
        SELECT title, tracker, guid, info_hash, size, download_url, cookie
        FROM announce_work
        WHERE id = ?
        "#,
    )
    .bind(id.as_str())
    .fetch_optional(repository.pool())
    .await
    .map_err(|error| DatabaseError::QueryFailed {
        operation: "load announce work candidate".to_owned(),
        message: error.to_string(),
    })?;
    let Some(row) = row else {
        return Ok(None);
    };
    let download_url = row
        .get::<Option<String>, _>("download_url")
        .unwrap_or_else(|| format!("announce:{}", id.as_str()));
    let candidate = RemoteCandidate {
        id: None,
        indexer_id: IndexerId::new(ANNOUNCE_CANDIDATE_INDEXER_ID).map_err(domain_database_error)?,
        guid: CandidateGuid::new(format!(
            "announce:{}:{}",
            row.get::<String, _>("tracker"),
            row.get::<Option<String>, _>("guid")
                .unwrap_or_else(|| id.as_str().to_owned())
        ))
        .map_err(domain_database_error)?,
        download_url: DownloadUrl::new(download_url).map_err(domain_database_error)?,
        title: ItemTitle::new(row.get::<String, _>("title")).map_err(domain_database_error)?,
        tracker: TrackerName::new(row.get::<String, _>("tracker"))
            .map_err(domain_database_error)?,
        size: row
            .get::<Option<i64>, _>("size")
            .and_then(|size| u64::try_from(size).ok())
            .map(crate::domain::ByteSize::new),
        published_at_ms: None,
        info_hash: row
            .get::<Option<String>, _>("info_hash")
            .map(crate::domain::InfoHash::new)
            .transpose()
            .map_err(domain_database_error)?,
        torrent_cache_path: None,
    };
    let cookie = row.get::<Option<String>, _>("cookie");
    let cookie_or_fetch = row
        .get::<Option<String>, _>("download_url")
        .map(|_download_url| RuntimeAnnounceFetch { cookie });

    Ok(Some(RuntimeAnnounceCandidate {
        candidate,
        cookie_or_fetch,
    }))
}

async fn read_cached_torrent(path: &Path) -> Result<Vec<u8>, String> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || std::fs::read(&path).map_err(|error| error.to_string()))
        .await
        .map_err(|error| error.to_string())?
}

fn actionable_assessment(
    assessment: &PersistedCandidateAssessment,
) -> Option<(RemoteCandidateId, CandidateAssessment)> {
    match assessment {
        PersistedCandidateAssessment::Assessed {
            candidate_id,
            assessment,
            ..
        } if matches!(
            assessment.decision,
            MatchDecision::Exact | MatchDecision::SizeOnly | MatchDecision::Partial
        ) =>
        {
            Some((*candidate_id, assessment.clone()))
        }
        PersistedCandidateAssessment::Assessed { .. }
        | PersistedCandidateAssessment::Rejected { .. }
        | PersistedCandidateAssessment::NeedsTorrentDownload { .. } => None,
    }
}

fn classify_reverse_lookup_error(error: ReverseLookupError, now_ms: i64) -> AnnounceWorkOutcome {
    match error {
        ReverseLookupError::Database { source } => retryable_database_outcome(source, now_ms),
        ReverseLookupError::Assessment { source } => AnnounceWorkOutcome::TerminalFailed {
            reason: AnnounceReason::InvalidTorrentMetadata,
            redacted_message: format!("{source:?}"),
        },
    }
}

fn classify_candidate_download_error(
    error: CandidateDownloadError,
    now_ms: i64,
) -> AnnounceWorkOutcome {
    match error {
        CandidateDownloadError::RateLimited { retry_after } => AnnounceWorkOutcome::Retryable {
            reason: AnnounceReason::RetryAfter,
            next_attempt_at_ms: retry_after
                .map(|retry_after| retry_after.deadline_ms(now_ms))
                .unwrap_or_else(|| now_ms.saturating_add(outcome_config().retry_delay_ms)),
            error_class: "candidate_download".to_owned(),
            redacted_message: "candidate download is rate limited".to_owned(),
        },
        CandidateDownloadError::HttpStatus {
            retry_after: Some(retry_after),
            ..
        } => AnnounceWorkOutcome::Retryable {
            reason: AnnounceReason::RetryAfter,
            next_attempt_at_ms: retry_after.deadline_ms(now_ms),
            error_class: "candidate_download".to_owned(),
            redacted_message: "candidate download returned Retry-After".to_owned(),
        },
        CandidateDownloadError::HttpStatus { status, .. } if status >= 500 => {
            AnnounceWorkOutcome::Retryable {
                reason: AnnounceReason::TransientDependencyFailure,
                next_attempt_at_ms: now_ms.saturating_add(outcome_config().retry_delay_ms),
                error_class: "candidate_download".to_owned(),
                redacted_message: format!("candidate download returned HTTP status {status}"),
            }
        }
        CandidateDownloadError::Timeout | CandidateDownloadError::Request { .. } => {
            AnnounceWorkOutcome::Retryable {
                reason: AnnounceReason::TransientDependencyFailure,
                next_attempt_at_ms: now_ms.saturating_add(outcome_config().retry_delay_ms),
                error_class: "candidate_download".to_owned(),
                redacted_message: error.to_string(),
            }
        }
        CandidateDownloadError::HttpStatus { status, .. } => AnnounceWorkOutcome::TerminalFailed {
            reason: AnnounceReason::InvalidRequest,
            redacted_message: format!("candidate download returned HTTP status {status}"),
        },
        CandidateDownloadError::MagnetLink => AnnounceWorkOutcome::TerminalFailed {
            reason: AnnounceReason::UnsupportedShape,
            redacted_message: "candidate download is a magnet link".to_owned(),
        },
        CandidateDownloadError::InvalidContents { .. }
        | CandidateDownloadError::ResponseTooLarge { .. } => AnnounceWorkOutcome::TerminalFailed {
            reason: AnnounceReason::InvalidTorrentMetadata,
            redacted_message: error.to_string(),
        },
        CandidateDownloadError::CacheWrite { .. } => AnnounceWorkOutcome::Retryable {
            reason: AnnounceReason::TransientDependencyFailure,
            next_attempt_at_ms: now_ms.saturating_add(outcome_config().retry_delay_ms),
            error_class: "candidate_cache".to_owned(),
            redacted_message: error.to_string(),
        },
    }
}

fn retryable_database_outcome(error: DatabaseError, now_ms: i64) -> AnnounceWorkOutcome {
    AnnounceWorkOutcome::Retryable {
        reason: AnnounceReason::TransientDependencyFailure,
        next_attempt_at_ms: error
            .retry_after_ms()
            .filter(|retry_after| *retry_after > now_ms)
            .unwrap_or_else(|| now_ms.saturating_add(outcome_config().retry_delay_ms)),
        error_class: "database".to_owned(),
        redacted_message: error.to_string(),
    }
}

fn retryable_worker_outcome(dependency: &str, message: String, now_ms: i64) -> AnnounceWorkOutcome {
    AnnounceWorkOutcome::Retryable {
        reason: AnnounceReason::TransientDependencyFailure,
        next_attempt_at_ms: now_ms.saturating_add(outcome_config().retry_delay_ms),
        error_class: dependency.to_owned(),
        redacted_message: message,
    }
}

fn outcome_config() -> AnnounceOutcomeConfig {
    AnnounceOutcomeConfig::default()
}

fn domain_database_error(error: crate::domain::DomainError) -> DatabaseError {
    DatabaseError::QueryFailed {
        operation: "load announce candidate".to_owned(),
        message: error.to_string(),
    }
}

fn runtime_client_inventory_interval(state: &AppState) -> Duration {
    let interval_ms =
        parse_interval_ms(&state.config.scheduling.search_interval).unwrap_or(86_400_000);
    Duration::from_millis(u64::try_from(interval_ms).unwrap_or(u64::MAX))
}

fn runtime_prowlarr_refresh_interval(state: &AppState) -> Option<Duration> {
    state
        .prowlarr_sources
        .values()
        .map(|source| source.update_interval_ms)
        .min()
        .and_then(|interval_ms| u64::try_from(interval_ms).ok())
        .map(|interval_ms| interval_ms.min(60_000))
        .filter(|interval_ms| *interval_ms > 0)
        .map(Duration::from_millis)
}

async fn run_prowlarr_refresh_loop(
    state: AppState,
    interval: Duration,
    mut shutdown: ShutdownSignal,
) {
    loop {
        if shutdown.state().phase != crate::runtime::shutdown::ShutdownPhase::Running {
            break;
        }

        match state.refresh_due_prowlarr_sources(unix_time_ms()).await {
            Ok(summary) => {
                if summary.refreshed > 0 || summary.failed > 0 {
                    tracing::info!(
                        refreshed = summary.refreshed,
                        failed = summary.failed,
                        imported = summary.imported,
                        skipped_backoff = summary.skipped_backoff,
                        skipped_interval = summary.skipped_interval,
                        "Prowlarr refresh completed"
                    );
                }
            }
            Err(error) => warn!(error = %error, "Prowlarr refresh failed"),
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

async fn run_client_inventory_refresh_loop(
    state: AppState,
    interval: Duration,
    mut shutdown: ShutdownSignal,
) {
    loop {
        if shutdown.state().phase != crate::runtime::shutdown::ShutdownPhase::Running {
            break;
        }

        match state.refresh_torrent_client_inventories().await {
            Ok(summaries) => {
                let scanned: usize = summaries.iter().map(|summary| summary.scanned_items).sum();
                let persisted: usize = summaries
                    .iter()
                    .map(|summary| summary.persisted_items)
                    .sum();
                let pruned: u64 = summaries.iter().map(|summary| summary.pruned_items).sum();
                tracing::info!(
                    clients = summaries.len(),
                    scanned,
                    persisted,
                    pruned,
                    "client inventory refresh completed"
                );
            }
            Err(error) => warn!(error = %error, "client inventory refresh failed"),
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

    let mut await_in_flight = Vec::new();
    for task in handles {
        if task.should_abort_on_timeout() {
            task.handle.abort();
            warn!(
                task = task.name,
                "background task did not stop before shutdown timeout; aborted"
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
        } else {
            await_in_flight.push(task);
        }
    }

    for task in await_in_flight {
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
    use std::fs;
    use std::future::{Future, pending};
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::config::{
        ConfigTorrentClientKind, ProwlarrSourceConfig, SporosConfig, TorrentClientConfig,
        TorznabIndexerConfig,
    };
    use crate::domain::{
        ByteSize, CandidateGuid, DependencyName, DisplayName, DownloadUrl, FileIndex, IndexerId,
        InfoHash, ItemTitle, LocalFile, LocalItem, LocalItemSource, MediaType, ReasonText,
        RemoteCandidate, TrackerName,
    };
    use crate::indexers::{CategoryCaps, SearchCaps, TorznabCaps, TorznabLimits};
    use crate::persistence::repository::Repository;
    use crate::secrets::ApiKey;
    use axum::body::Body;
    use axum::http::{
        Request, StatusCode,
        header::{CONTENT_LENGTH, SET_COOKIE},
    };
    use axum::response::{IntoResponse, Response};
    use axum::routing::{get, post};
    use serde_json::Value;
    use tower::ServiceExt;

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
    async fn background_tasks_refresh_client_inventory() {
        let info_requests = Arc::new(AtomicUsize::new(0));
        let endpoint = spawn_daemon_qbit_inventory_server(info_requests.clone()).await;
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
        let shutdown = runtime.state.shutdown.clone();

        let handles = start_background_tasks(runtime).await.unwrap();
        wait_for_local_item_count(&repository, 1).await;
        shutdown.cancel_now("test shutdown").unwrap();
        stop_background_tasks(handles).await;

        let file_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM local_files")
            .fetch_one(repository.pool())
            .await
            .unwrap();
        assert_eq!(1, file_count);
        assert_eq!(1, info_requests.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn background_tasks_process_durable_announcements() {
        let root = unique_temp_dir("daemon-announce");
        let output_dir = root.join("output");
        let cache_dir = root.join("cache");
        fs::create_dir_all(&output_dir).unwrap();
        fs::create_dir_all(&cache_dir).unwrap();
        fs::write(root.join("movie.mkv"), b"0123456789").unwrap();
        let download_url = spawn_daemon_torrent_download_server().await;
        let mut config = SporosConfig::default();
        config.paths.output_dir = output_dir.clone();
        config.paths.torrent_cache_dir = cache_dir;
        config.announce.worker_concurrency = 1;
        config.announce.claim_batch_size = 1;
        let repository = Repository::connect_in_memory().await.unwrap();
        repository
            .upsert_local_item_with_files(&local_item(&root), &[local_file()])
            .await
            .unwrap();
        repository
            .upsert_remote_candidate(&preexisting_indexer_candidate())
            .await
            .unwrap();
        let runtime = AppRuntime::from_repository(config, repository.clone())
            .await
            .unwrap();
        let shutdown = runtime.state.shutdown.clone();
        let metrics = runtime.state.metrics.clone();
        let app = router(runtime.state.http.clone());
        let handles = start_background_tasks(runtime).await.unwrap();

        let accepted = app
            .oneshot(json_post(
                "/v1/announcements",
                serde_json::json!({
                    "name": "movie.mkv",
                    "guid": "guid-announce",
                    "download_url": download_url,
                    "tracker": "tracker.example",
                    "size": 10
                }),
            ))
            .await
            .unwrap();
        let body = axum::body::to_bytes(accepted.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        let id = json["id"].as_str().unwrap();

        wait_for_announce_status(&repository, id, "succeeded").await;
        shutdown.cancel_now("test shutdown").unwrap();
        stop_background_tasks(handles).await;

        let reason: String = sqlx::query_scalar("SELECT reason FROM announce_work WHERE id = ?")
            .bind(id)
            .fetch_one(repository.pool())
            .await
            .unwrap();
        assert_eq!("saved", reason);
        assert_eq!(1, saved_torrent_count(&output_dir));
        let candidates: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM remote_candidates WHERE guid IN (?, ?)")
                .bind("guid-announce")
                .bind("announce:tracker.example:guid-announce")
                .fetch_one(repository.pool())
                .await
                .unwrap();
        assert_eq!(2, candidates);
        let metrics = metrics.render_prometheus(&crate::metrics::MetricsSnapshot::default());
        assert!(metrics.contains(
            "sporos_indexer_requests_total{operation=\"download\",outcome=\"succeeded\"} 1"
        ));
        assert!(metrics.contains("sporos_actions_total{outcome=\"saved\"} 1"));
    }

    #[tokio::test]
    async fn background_tasks_process_search_workflows() {
        let root = unique_temp_dir("daemon-search");
        let output_dir = root.join("output");
        let cache_dir = root.join("cache");
        fs::create_dir_all(&output_dir).unwrap();
        fs::create_dir_all(&cache_dir).unwrap();
        fs::write(root.join("movie.mkv"), b"0123456789").unwrap();
        let indexer_url = spawn_daemon_torznab_search_download_server().await;
        let mut config = SporosConfig::default();
        config.paths.output_dir = output_dir.clone();
        config.paths.torrent_cache_dir = cache_dir;
        config.indexers.torznab.insert(
            "main".to_owned(),
            TorznabIndexerConfig {
                url: indexer_url,
                api_key: Some(ApiKey::new("secret").unwrap()),
                api_key_file: None,
                api_key_env: None,
            },
        );
        let repository = Repository::connect_in_memory().await.unwrap();
        let mut item = local_item(&root);
        item.info_hash = None;
        repository
            .upsert_local_item_with_files(&item, &[local_file()])
            .await
            .unwrap();
        let runtime = AppRuntime::from_repository(config, repository.clone())
            .await
            .unwrap();
        repository
            .record_indexer_caps_success(
                &DependencyName::new("main").unwrap(),
                &daemon_movie_caps(),
                unix_time_ms(),
            )
            .await
            .unwrap();
        let shutdown = runtime.state.shutdown.clone();
        let search_queue = runtime.state.queues.workflow.searches.clone();
        let metrics = runtime.state.metrics.clone();
        let app = router(runtime.state.http.clone());
        let handles = start_background_tasks(runtime).await.unwrap();

        let accepted = app
            .oneshot(json_post(
                "/v1/searches",
                serde_json::json!({ "query": "movie.mkv" }),
            ))
            .await
            .unwrap();

        assert_eq!(StatusCode::ACCEPTED, accepted.status());
        wait_for_saved_torrent_count(&output_dir, 1).await;
        wait_for_queue_completed(&search_queue, 1).await;
        shutdown.cancel_now("test shutdown").unwrap();
        stop_background_tasks(handles).await;

        let candidates: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM remote_candidates")
            .fetch_one(repository.pool())
            .await
            .unwrap();
        let decisions: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM match_decisions")
            .fetch_one(repository.pool())
            .await
            .unwrap();
        assert_eq!(1, candidates);
        assert_eq!(1, decisions);
        assert_eq!(0, search_queue.stats().depth);
        let metrics = metrics.render_prometheus(&crate::metrics::MetricsSnapshot::default());
        assert!(metrics.contains("sporos_search_attempts_total{outcome=\"succeeded\"} 1"));
        assert!(metrics.contains(
            "sporos_indexer_requests_total{operation=\"search\",outcome=\"succeeded\"} 1"
        ));
        assert!(metrics.contains(
            "sporos_indexer_requests_total{operation=\"download\",outcome=\"succeeded\"} 1"
        ));
        assert!(metrics.contains("sporos_decisions_total{outcome=\"exact_match\"} 1"));
        assert!(metrics.contains("sporos_actions_total{outcome=\"saved\"} 1"));
    }

    #[tokio::test]
    async fn background_tasks_record_failed_search_attempt_when_indexer_fails() {
        let root = unique_temp_dir("daemon-search-failed-indexer");
        fs::write(root.join("movie.mkv"), b"0123456789").unwrap();
        let indexer_url = spawn_daemon_torznab_status_server(StatusCode::TOO_MANY_REQUESTS).await;
        let mut config = SporosConfig::default();
        config.indexers.torznab.insert(
            "main".to_owned(),
            TorznabIndexerConfig {
                url: indexer_url,
                api_key: Some(ApiKey::new("secret").unwrap()),
                api_key_file: None,
                api_key_env: None,
            },
        );
        let repository = Repository::connect_in_memory().await.unwrap();
        let mut item = local_item(&root);
        item.info_hash = None;
        repository
            .upsert_local_item_with_files(&item, &[local_file()])
            .await
            .unwrap();
        let runtime = AppRuntime::from_repository(config, repository.clone())
            .await
            .unwrap();
        repository
            .record_indexer_caps_success(
                &DependencyName::new("main").unwrap(),
                &daemon_movie_caps(),
                unix_time_ms(),
            )
            .await
            .unwrap();
        let shutdown = runtime.state.shutdown.clone();
        let search_queue = runtime.state.queues.workflow.searches.clone();
        let metrics = runtime.state.metrics.clone();
        let app = router(runtime.state.http.clone());
        let handles = start_background_tasks(runtime).await.unwrap();

        let accepted = app
            .oneshot(json_post(
                "/v1/searches",
                serde_json::json!({ "query": "movie.mkv" }),
            ))
            .await
            .unwrap();

        assert_eq!(StatusCode::ACCEPTED, accepted.status());
        wait_for_queue_completed(&search_queue, 1).await;
        shutdown.cancel_now("test shutdown").unwrap();
        stop_background_tasks(handles).await;

        let metrics = metrics.render_prometheus(&crate::metrics::MetricsSnapshot::default());
        assert!(metrics.contains("sporos_search_attempts_total{outcome=\"failed\"} 1"));
        assert!(metrics.contains(
            "sporos_indexer_requests_total{operation=\"search\",outcome=\"rate_limited\"} 1"
        ));
    }

    #[tokio::test]
    async fn background_tasks_record_oversized_indexer_health_and_metrics() {
        let root = unique_temp_dir("daemon-search-oversized-indexer");
        fs::write(root.join("movie.mkv"), b"0123456789").unwrap();
        let indexer_url = spawn_daemon_torznab_oversized_server().await;
        let mut config = SporosConfig::default();
        config.indexers.torznab.insert(
            "main".to_owned(),
            TorznabIndexerConfig {
                url: indexer_url,
                api_key: Some(ApiKey::new("secret").unwrap()),
                api_key_file: None,
                api_key_env: None,
            },
        );
        let repository = Repository::connect_in_memory().await.unwrap();
        let mut item = local_item(&root);
        item.info_hash = None;
        repository
            .upsert_local_item_with_files(&item, &[local_file()])
            .await
            .unwrap();
        let runtime = AppRuntime::from_repository(config, repository.clone())
            .await
            .unwrap();
        repository
            .record_indexer_caps_success(
                &DependencyName::new("main").unwrap(),
                &daemon_movie_caps(),
                unix_time_ms(),
            )
            .await
            .unwrap();
        let shutdown = runtime.state.shutdown.clone();
        let search_queue = runtime.state.queues.workflow.searches.clone();
        let metrics = runtime.state.metrics.clone();
        let app = router(runtime.state.http.clone());
        let handles = start_background_tasks(runtime).await.unwrap();

        let accepted = app
            .oneshot(json_post(
                "/v1/searches",
                serde_json::json!({ "query": "movie.mkv" }),
            ))
            .await
            .unwrap();

        assert_eq!(StatusCode::ACCEPTED, accepted.status());
        wait_for_queue_completed(&search_queue, 1).await;
        shutdown.cancel_now("test shutdown").unwrap();
        stop_background_tasks(handles).await;

        let metrics = metrics.render_prometheus(&crate::metrics::MetricsSnapshot::default());
        assert!(metrics.contains("sporos_search_attempts_total{outcome=\"failed\"} 1"));
        assert!(
            metrics.contains(
                "sporos_indexer_requests_total{operation=\"search\",outcome=\"failed\"} 1"
            )
        );
        let health = repository.dependency_health_snapshot(10).await.unwrap();
        let entry = health
            .iter()
            .find(|entry| entry.dependency_name.as_str() == "main")
            .unwrap();
        assert_eq!("unavailable", entry.state);
        assert!(entry.reason.as_deref().unwrap().contains("exceeded"));
    }

    #[tokio::test]
    async fn search_planning_stops_on_shutdown() {
        let search_requests = Arc::new(AtomicUsize::new(0));
        let indexer_url =
            spawn_daemon_stalled_torznab_search_server(Arc::clone(&search_requests)).await;
        let mut config = SporosConfig::default();
        config.indexers.torznab.insert(
            "main".to_owned(),
            TorznabIndexerConfig {
                url: indexer_url,
                api_key: Some(ApiKey::new("secret").unwrap()),
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
                &DependencyName::new("main").unwrap(),
                &daemon_movie_caps(),
                unix_time_ms(),
            )
            .await
            .unwrap();
        let state = runtime.state.clone();
        let shutdown = state.shutdown.clone();
        let signal = state.shutdown_signal.clone();

        let handle = tokio::spawn(process_search_workflow(
            state,
            SearchWorkflowRequest {
                query: ItemTitle::new("movie.mkv").unwrap(),
            },
            signal,
        ));
        wait_for_atomic_count(&search_requests, 1).await;
        shutdown.cancel_now("test shutdown").unwrap();
        let result = tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(Err("search workflow is shutting down".to_owned()), result);
    }

    #[tokio::test]
    async fn search_candidate_download_stops_on_shutdown() {
        let root = unique_temp_dir("daemon-search-shutdown");
        let cache_dir = root.join("cache");
        fs::create_dir_all(&cache_dir).unwrap();
        fs::write(root.join("movie.mkv"), b"0123456789").unwrap();
        let download_requests = Arc::new(AtomicUsize::new(0));
        let indexer_url = spawn_daemon_torznab_search_server_with_stalled_download(Arc::clone(
            &download_requests,
        ))
        .await;
        let mut config = SporosConfig::default();
        config.paths.torrent_cache_dir = cache_dir;
        config.indexers.torznab.insert(
            "main".to_owned(),
            TorznabIndexerConfig {
                url: indexer_url,
                api_key: Some(ApiKey::new("secret").unwrap()),
                api_key_file: None,
                api_key_env: None,
            },
        );
        let repository = Repository::connect_in_memory().await.unwrap();
        let mut item = local_item(&root);
        item.info_hash = None;
        repository
            .upsert_local_item_with_files(&item, &[local_file()])
            .await
            .unwrap();
        let runtime = AppRuntime::from_repository(config, repository.clone())
            .await
            .unwrap();
        repository
            .record_indexer_caps_success(
                &DependencyName::new("main").unwrap(),
                &daemon_movie_caps(),
                unix_time_ms(),
            )
            .await
            .unwrap();
        let state = runtime.state.clone();
        let shutdown = state.shutdown.clone();
        let signal = state.shutdown_signal.clone();

        let handle = tokio::spawn(process_search_workflow(
            state,
            SearchWorkflowRequest {
                query: ItemTitle::new("movie.mkv").unwrap(),
            },
            signal,
        ));
        wait_for_atomic_count(&download_requests, 1).await;
        shutdown.cancel_now("test shutdown").unwrap();
        let result = tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(Err("search workflow is shutting down".to_owned()), result);
    }

    #[tokio::test]
    async fn announce_candidate_download_stops_on_shutdown() {
        let root = unique_temp_dir("daemon-announce-shutdown");
        let cache_dir = root.join("cache");
        fs::create_dir_all(&cache_dir).unwrap();
        fs::write(root.join("movie.mkv"), b"0123456789").unwrap();
        let download_requests = Arc::new(AtomicUsize::new(0));
        let download_url =
            spawn_daemon_stalled_torrent_download_server(Arc::clone(&download_requests)).await;
        let mut config = SporosConfig::default();
        config.paths.torrent_cache_dir = cache_dir;
        let repository = Repository::connect_in_memory().await.unwrap();
        repository
            .upsert_local_item_with_files(&local_item(&root), &[local_file()])
            .await
            .unwrap();
        let id = AnnounceWorkId::new("ann_stalled").unwrap();
        insert_announce_row(
            &repository,
            &id,
            "guid-announce-stalled",
            "tracker.example",
            &download_url,
        )
        .await;
        let runtime = AppRuntime::from_repository(config, repository)
            .await
            .unwrap();
        let state = runtime.state.clone();
        let shutdown = state.shutdown.clone();
        let signal = state.shutdown_signal.clone();

        let handle = tokio::spawn(process_announce_work(state, id, signal));
        wait_for_atomic_count(&download_requests, 1).await;
        shutdown.cancel_now("test shutdown").unwrap();
        let result = tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .unwrap()
            .unwrap();

        assert!(matches!(result, AnnounceWorkOutcome::Release { .. }));
    }

    #[tokio::test]
    async fn announce_candidate_download_records_rate_limited_metric() {
        let root = unique_temp_dir("daemon-announce-rate-limited");
        let cache_dir = root.join("cache");
        fs::create_dir_all(&cache_dir).unwrap();
        fs::write(root.join("movie.mkv"), b"0123456789").unwrap();
        let download_url = spawn_daemon_torrent_status_server(StatusCode::TOO_MANY_REQUESTS).await;
        let mut config = SporosConfig::default();
        config.paths.torrent_cache_dir = cache_dir;
        let repository = Repository::connect_in_memory().await.unwrap();
        repository
            .upsert_local_item_with_files(&local_item(&root), &[local_file()])
            .await
            .unwrap();
        let id = AnnounceWorkId::new("ann_rate_limited").unwrap();
        insert_announce_row(
            &repository,
            &id,
            "guid-announce-limited",
            "tracker.example",
            &download_url,
        )
        .await;
        let runtime = AppRuntime::from_repository(config, repository)
            .await
            .unwrap();
        let state = runtime.state.clone();
        let metrics = state.metrics.clone();

        let result = process_announce_work(state.clone(), id, state.shutdown_signal.clone()).await;

        assert!(matches!(result, AnnounceWorkOutcome::Retryable { .. }));
        let metrics = metrics.render_prometheus(&crate::metrics::MetricsSnapshot::default());
        assert!(metrics.contains(
            "sporos_indexer_requests_total{operation=\"download\",outcome=\"rate_limited\"} 1"
        ));
    }

    #[tokio::test]
    async fn announce_candidate_download_records_oversized_health_and_metrics() {
        let root = unique_temp_dir("daemon-announce-oversized");
        let cache_dir = root.join("cache");
        fs::create_dir_all(&cache_dir).unwrap();
        fs::write(root.join("movie.mkv"), b"0123456789").unwrap();
        let download_url = spawn_daemon_torrent_oversized_server().await;
        let mut config = SporosConfig::default();
        config.paths.torrent_cache_dir = cache_dir;
        let repository = Repository::connect_in_memory().await.unwrap();
        repository
            .upsert_local_item_with_files(&local_item(&root), &[local_file()])
            .await
            .unwrap();
        let id = AnnounceWorkId::new("ann_oversized").unwrap();
        insert_announce_row(
            &repository,
            &id,
            "guid-announce-oversized",
            "tracker.example",
            &download_url,
        )
        .await;
        let runtime = AppRuntime::from_repository(config, repository.clone())
            .await
            .unwrap();
        let state = runtime.state.clone();
        let metrics = state.metrics.clone();

        let result = process_announce_work(state.clone(), id, state.shutdown_signal.clone()).await;

        assert!(matches!(result, AnnounceWorkOutcome::TerminalFailed { .. }));
        let metrics = metrics.render_prometheus(&crate::metrics::MetricsSnapshot::default());
        assert!(metrics.contains(
            "sporos_indexer_requests_total{operation=\"download\",outcome=\"failed\"} 1"
        ));
        let runtime_health = state.health.snapshot();
        assert!(
            runtime_health
                .summaries
                .get(&DependencyKind::Indexer)
                .is_some_and(|summary| {
                    *summary == crate::runtime::health::DependencySummary::Unavailable
                })
        );
        let health = repository.dependency_health_snapshot(10).await.unwrap();
        let entry = health
            .iter()
            .find(|entry| entry.dependency_name.as_str() == "tracker.example")
            .unwrap();
        assert_eq!("unavailable", entry.state);
        assert!(entry.reason.as_deref().unwrap().contains("exceeded"));
    }

    #[tokio::test]
    async fn background_search_workflow_uses_cached_candidate_without_redownloading() {
        let root = unique_temp_dir("daemon-search-cached");
        let output_dir = root.join("output");
        let cache_dir = root.join("cache");
        fs::create_dir_all(&output_dir).unwrap();
        fs::create_dir_all(&cache_dir).unwrap();
        fs::write(root.join("movie.mkv"), b"0123456789").unwrap();
        let cache_path = cache_dir.join("cached.torrent");
        fs::write(&cache_path, test_torrent_bytes()).unwrap();
        let download_requests = Arc::new(AtomicUsize::new(0));
        let indexer_url = spawn_daemon_torznab_search_server_with_download(
            StatusCode::INTERNAL_SERVER_ERROR,
            download_requests.clone(),
        )
        .await;
        let mut config = SporosConfig::default();
        config.paths.output_dir = output_dir.clone();
        config.paths.torrent_cache_dir = cache_dir;
        config.indexers.torznab.insert(
            "main".to_owned(),
            TorznabIndexerConfig {
                url: indexer_url,
                api_key: Some(ApiKey::new("secret").unwrap()),
                api_key_file: None,
                api_key_env: None,
            },
        );
        let repository = Repository::connect_in_memory().await.unwrap();
        let mut item = local_item(&root);
        item.info_hash = None;
        repository
            .upsert_local_item_with_files(&item, &[local_file()])
            .await
            .unwrap();
        let runtime = AppRuntime::from_repository(config, repository.clone())
            .await
            .unwrap();
        let metrics = runtime.state.metrics.clone();
        let indexer = repository
            .indexer_registry_snapshot(10)
            .await
            .unwrap()
            .into_iter()
            .find(|row| row.name.as_str() == "main")
            .unwrap();
        repository
            .record_indexer_caps_success(
                &DependencyName::new("main").unwrap(),
                &daemon_movie_caps(),
                unix_time_ms(),
            )
            .await
            .unwrap();
        repository
            .upsert_remote_candidate(&RemoteCandidate {
                id: None,
                indexer_id: IndexerId::new(indexer.id).unwrap(),
                guid: CandidateGuid::new("candidate-search").unwrap(),
                download_url: DownloadUrl::new("http://127.0.0.1/download-would-fail").unwrap(),
                title: ItemTitle::new("movie.mkv").unwrap(),
                tracker: TrackerName::new("indexer.example").unwrap(),
                size: Some(ByteSize::new(10)),
                published_at_ms: None,
                info_hash: None,
                torrent_cache_path: Some(cache_path),
            })
            .await
            .unwrap();
        let shutdown = runtime.state.shutdown.clone();
        let app = router(runtime.state.http.clone());
        let handles = start_background_tasks(runtime).await.unwrap();

        let accepted = app
            .oneshot(json_post(
                "/v1/searches",
                serde_json::json!({ "query": "movie.mkv" }),
            ))
            .await
            .unwrap();

        assert_eq!(StatusCode::ACCEPTED, accepted.status());
        wait_for_saved_torrent_count(&output_dir, 1).await;
        shutdown.cancel_now("test shutdown").unwrap();
        stop_background_tasks(handles).await;

        assert_eq!(0, download_requests.load(Ordering::SeqCst));
        let metrics = metrics.render_prometheus(&crate::metrics::MetricsSnapshot::default());
        assert!(metrics.contains("sporos_decisions_total{outcome=\"exact_match\"} 1"));
    }

    #[tokio::test]
    async fn background_tasks_process_posted_job_runs() {
        let caps_requests = Arc::new(AtomicUsize::new(0));
        let indexer_url = spawn_daemon_torznab_caps_server(caps_requests.clone()).await;
        let mut config = SporosConfig::default();
        config.indexers.torznab.insert(
            "main".to_owned(),
            TorznabIndexerConfig {
                url: indexer_url,
                api_key: Some(ApiKey::new("secret").unwrap()),
                api_key_file: None,
                api_key_env: None,
            },
        );
        let repository = Repository::connect_in_memory().await.unwrap();
        let runtime = AppRuntime::from_repository(config, repository.clone())
            .await
            .unwrap();
        let shutdown = runtime.state.shutdown.clone();
        let job_queue = runtime.state.queues.workflow.jobs.clone();
        let scheduler_queue = runtime.state.queues.scheduler.clone();
        let app = router(runtime.state.http.clone());
        let handles = start_background_tasks(runtime).await.unwrap();

        let accepted = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/jobs/indexer_caps/runs")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(StatusCode::ACCEPTED, accepted.status());
        wait_for_job_state(&repository, "indexer_caps", "succeeded").await;
        wait_for_queue_completed(&job_queue, 1).await;
        wait_for_queue_completed(&scheduler_queue, 1).await;
        shutdown.cancel_now("test shutdown").unwrap();
        stop_background_tasks(handles).await;

        let jobs = repository.job_status_snapshot(10).await.unwrap();
        let indexer_caps = jobs
            .iter()
            .find(|job| job.name.as_str() == "indexer_caps")
            .unwrap();
        let stored_caps: String =
            sqlx::query_scalar("SELECT capabilities_json FROM indexers WHERE name = 'main'")
                .fetch_one(repository.pool())
                .await
                .unwrap();
        let stored_caps: Value = serde_json::from_str(&stored_caps).unwrap();
        assert_eq!("succeeded", indexer_caps.state);
        assert!(indexer_caps.last_started_at_ms.is_some());
        assert!(indexer_caps.last_finished_at_ms.is_some());
        assert!(indexer_caps.next_run_at_ms.is_some());
        assert_eq!(None, indexer_caps.last_error);
        assert_eq!(true, stored_caps["search"]["movie_search"]);
        assert_eq!(true, stored_caps["categories"]["movie"]);
        assert_eq!(1, caps_requests.load(Ordering::SeqCst));
        assert_eq!(0, job_queue.stats().depth);
        assert_eq!(0, scheduler_queue.stats().depth);
    }

    #[tokio::test]
    async fn scheduled_indexer_caps_stops_on_shutdown() {
        let caps_requests = Arc::new(AtomicUsize::new(0));
        let indexer_url =
            spawn_daemon_stalled_torznab_caps_server(Arc::clone(&caps_requests)).await;
        let mut config = SporosConfig::default();
        config.indexers.torznab.insert(
            "main".to_owned(),
            TorznabIndexerConfig {
                url: indexer_url,
                api_key: Some(ApiKey::new("secret").unwrap()),
                api_key_file: None,
                api_key_env: None,
            },
        );
        let repository = Repository::connect_in_memory().await.unwrap();
        let runtime = AppRuntime::from_repository(config, repository)
            .await
            .unwrap();
        let state = runtime.state.clone();
        let shutdown = state.shutdown.clone();
        let signal = state.shutdown_signal.clone();

        let handle = tokio::spawn(async move {
            let job_name = JobName::new("indexer_caps").unwrap();
            execute_scheduled_job(&state, &job_name, signal).await
        });
        wait_for_atomic_count(&caps_requests, 1).await;
        shutdown.cancel_now("test shutdown").unwrap();
        let result = tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(Err("scheduler is shutting down".to_owned()), result);
    }

    #[tokio::test]
    async fn scheduled_indexer_caps_shutdown_persists_waiting() {
        let caps_requests = Arc::new(AtomicUsize::new(0));
        let indexer_url =
            spawn_daemon_stalled_torznab_caps_server(Arc::clone(&caps_requests)).await;
        let mut config = SporosConfig::default();
        config.indexers.torznab.insert(
            "main".to_owned(),
            TorznabIndexerConfig {
                url: indexer_url,
                api_key: Some(ApiKey::new("secret").unwrap()),
                api_key_file: None,
                api_key_env: None,
            },
        );
        let repository = Repository::connect_in_memory().await.unwrap();
        let runtime = AppRuntime::from_repository(config, repository.clone())
            .await
            .unwrap();
        let state = runtime.state.clone();
        let shutdown = state.shutdown.clone();
        let signal = state.shutdown_signal.clone();
        let job_name = JobName::new("indexer_caps").unwrap();
        repository
            .claim_immediate_job_run(&job_name, unix_time_ms())
            .await
            .unwrap();

        let handle = tokio::spawn(async move {
            process_scheduled_job_run(
                &state,
                ScheduledJobRun {
                    job_name,
                    scheduled_at_ms: unix_time_ms(),
                },
                signal,
            )
            .await;
        });
        wait_for_atomic_count(&caps_requests, 1).await;
        shutdown.cancel_now("test shutdown").unwrap();
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .unwrap()
            .unwrap();
        let jobs = repository.job_status_snapshot(10).await.unwrap();
        let indexer_caps = jobs
            .iter()
            .find(|job| job.name.as_str() == "indexer_caps")
            .unwrap();

        assert_eq!("waiting", indexer_caps.state);
        assert_eq!(
            Some("scheduler shutting down".to_owned()),
            indexer_caps.last_error
        );
        assert!(indexer_caps.next_run_at_ms.is_some());
    }

    #[tokio::test]
    async fn background_scheduler_tick_refreshes_prowlarr_import_caps() {
        let catalog_requests = Arc::new(AtomicUsize::new(0));
        let caps_requests = Arc::new(AtomicUsize::new(0));
        let prowlarr_url = spawn_daemon_prowlarr_with_caps_server(
            Arc::clone(&catalog_requests),
            Arc::clone(&caps_requests),
        )
        .await;
        let mut config = SporosConfig::default();
        config.indexers.prowlarr.insert(
            "main".to_owned(),
            ProwlarrSourceConfig {
                url: prowlarr_url,
                api_key: Some(ApiKey::new("prowlarr-secret").unwrap()),
                refresh_on_startup: true,
                ..ProwlarrSourceConfig::default()
            },
        );
        let repository = Repository::connect_in_memory().await.unwrap();
        let runtime = AppRuntime::from_repository(config, repository.clone())
            .await
            .unwrap();
        let shutdown = runtime.state.shutdown.clone();
        let scheduler_queue = runtime.state.queues.scheduler.clone();
        let handles = start_background_tasks(runtime).await.unwrap();

        wait_for_job_state(&repository, "indexer_caps", "succeeded").await;
        wait_for_queue_completed(&scheduler_queue, 1).await;
        shutdown.cancel_now("test shutdown").unwrap();
        stop_background_tasks(handles).await;

        let stored_caps: String =
            sqlx::query_scalar("SELECT capabilities_json FROM indexers WHERE name = 'main:Movies'")
                .fetch_one(repository.pool())
                .await
                .unwrap();
        let stored_caps: Value = serde_json::from_str(&stored_caps).unwrap();
        assert_eq!(true, stored_caps["search"]["movie_search"]);
        assert_eq!(true, stored_caps["categories"]["movie"]);
        assert_eq!(1, catalog_requests.load(Ordering::SeqCst));
        assert_eq!(1, caps_requests.load(Ordering::SeqCst));
        assert_eq!(0, scheduler_queue.stats().depth);
    }

    #[tokio::test]
    async fn posted_indexer_caps_job_does_not_succeed_when_every_indexer_is_backed_off() {
        let caps_requests = Arc::new(AtomicUsize::new(0));
        let indexer_url = spawn_daemon_torznab_caps_server(caps_requests.clone()).await;
        let mut config = SporosConfig::default();
        config.indexers.torznab.insert(
            "main".to_owned(),
            TorznabIndexerConfig {
                url: indexer_url,
                api_key: Some(ApiKey::new("secret").unwrap()),
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
                Some(unix_time_ms() + 60_000),
                unix_time_ms(),
            )
            .await
            .unwrap();
        let shutdown = runtime.state.shutdown.clone();
        let app = router(runtime.state.http.clone());
        let handles = start_background_tasks(runtime).await.unwrap();

        let accepted = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/jobs/indexer_caps/runs")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(StatusCode::ACCEPTED, accepted.status());
        wait_for_job_state(&repository, "indexer_caps", "failed").await;
        let jobs = repository.job_status_snapshot(10).await.unwrap();
        let indexer_caps = jobs
            .iter()
            .find(|job| job.name.as_str() == "indexer_caps")
            .unwrap();
        assert_eq!("failed", indexer_caps.state);
        assert!(
            indexer_caps
                .last_error
                .as_deref()
                .is_some_and(|error| error.contains("backoff")),
            "unexpected last_error: {:?}",
            indexer_caps.last_error
        );
        shutdown.cancel_now("test shutdown").unwrap();
        stop_background_tasks(handles).await;
        assert_eq!(0, caps_requests.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn scheduler_receiver_releases_queued_runs_on_shutdown() {
        let config = SporosConfig::default();
        let repository = Repository::connect_in_memory().await.unwrap();
        let runtime = AppRuntime::from_repository(config, repository.clone())
            .await
            .unwrap();
        let state = runtime.state.clone();
        let scheduler_queue = state.queues.scheduler.clone();
        let job_name = JobName::new("indexer_caps").unwrap();

        state
            .scheduler
            .enqueue_immediate_run(&job_name, 100)
            .await
            .unwrap();
        state.shutdown.cancel_now("test shutdown").unwrap();
        run_scheduler_receiver(
            state.clone(),
            runtime.receivers.scheduler,
            state.shutdown_signal.clone(),
        )
        .await;

        let jobs = repository.job_status_snapshot(10).await.unwrap();
        let indexer_caps = jobs
            .iter()
            .find(|job| job.name.as_str() == "indexer_caps")
            .unwrap();
        assert_eq!("waiting", indexer_caps.state);
        assert!(
            matches!(
                indexer_caps.last_error.as_deref(),
                Some("scheduler shutting down") | Some("scheduler is shutting down")
            ),
            "unexpected last_error: {:?}",
            indexer_caps.last_error
        );
        assert_eq!(0, scheduler_queue.stats().depth);
        assert_eq!(1, scheduler_queue.stats().completed);
    }

    #[tokio::test]
    async fn job_receiver_releases_queued_requests_on_shutdown() {
        let config = SporosConfig::default();
        let repository = Repository::connect_in_memory().await.unwrap();
        let mut runtime = AppRuntime::from_repository(config, repository.clone())
            .await
            .unwrap();
        let state = runtime.state.clone();
        let job_queue = state.queues.workflow.jobs.clone();
        let job_name = JobName::new("indexer_caps").unwrap();

        job_queue
            .try_enqueue(JobRunWorkflowRequest {
                job_name: job_name.clone(),
            })
            .unwrap();
        runtime.receivers.jobs.close();
        release_queued_job_requests(&state, &mut runtime.receivers.jobs).await;

        let jobs = repository.job_status_snapshot(10).await.unwrap();
        let indexer_caps = jobs
            .iter()
            .find(|job| job.name.as_str() == "indexer_caps")
            .unwrap();
        assert_eq!("failed", indexer_caps.state);
        assert_eq!(
            Some("scheduler shutting down".to_owned()),
            indexer_caps.last_error
        );
        assert_eq!(0, job_queue.stats().depth);
        assert_eq!(1, job_queue.stats().completed);
    }

    #[tokio::test]
    async fn search_receiver_releases_queued_requests_on_shutdown() {
        let config = SporosConfig::default();
        let repository = Repository::connect_in_memory().await.unwrap();
        let mut runtime = AppRuntime::from_repository(config, repository.clone())
            .await
            .unwrap();
        let search_queue = runtime.state.queues.workflow.searches.clone();

        search_queue
            .try_enqueue(SearchWorkflowRequest {
                query: ItemTitle::new("movie.mkv").unwrap(),
            })
            .unwrap();
        runtime.receivers.searches.close();
        release_queued_search_requests(&mut runtime.receivers.searches).await;

        assert_eq!(0, search_queue.stats().depth);
        assert_eq!(0, search_queue.stats().completed);
        assert_eq!(1, search_queue.stats().cancelled);
        let metrics = runtime
            .state
            .metrics
            .render_prometheus(&crate::metrics::MetricsSnapshot {
                queues: vec![search_queue.stats()],
                ..crate::metrics::MetricsSnapshot::default()
            });
        assert!(metrics.contains("sporos_queue_cancelled_total{queue=\"search\"} 1"));
    }

    #[tokio::test]
    async fn search_receiver_prioritizes_shutdown_over_ready_request() {
        let config = SporosConfig::default();
        let repository = Repository::connect_in_memory().await.unwrap();
        let runtime = AppRuntime::from_repository(config, repository.clone())
            .await
            .unwrap();
        let state = runtime.state.clone();
        let search_queue = state.queues.workflow.searches.clone();

        search_queue
            .try_enqueue(SearchWorkflowRequest {
                query: ItemTitle::new("movie.mkv").unwrap(),
            })
            .unwrap();
        state.shutdown.cancel_now("test shutdown").unwrap();
        run_search_receiver(
            state.clone(),
            runtime.receivers.searches,
            state.shutdown_signal.clone(),
        )
        .await;

        assert_eq!(0, search_queue.stats().depth);
        assert_eq!(0, search_queue.stats().completed);
        assert_eq!(1, search_queue.stats().cancelled);
    }

    #[tokio::test]
    async fn announce_candidate_guid_is_tracker_scoped() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let first = AnnounceWorkId::new("ann_first").unwrap();
        let second = AnnounceWorkId::new("ann_second").unwrap();
        insert_announce_row(
            &repository,
            &first,
            "same-guid",
            "tracker-one.example",
            "https://tracker-one.example/download",
        )
        .await;
        insert_announce_row(
            &repository,
            &second,
            "same-guid",
            "tracker-two.example",
            "https://tracker-two.example/download",
        )
        .await;

        let first_candidate = load_announce_candidate(&repository, &first)
            .await
            .unwrap()
            .unwrap();
        let second_candidate = load_announce_candidate(&repository, &second)
            .await
            .unwrap()
            .unwrap();
        repository
            .upsert_remote_candidate(&first_candidate.candidate)
            .await
            .unwrap();
        repository
            .upsert_remote_candidate(&second_candidate.candidate)
            .await
            .unwrap();

        let candidate_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM remote_candidates WHERE indexer_id = ?")
                .bind(i64::MAX)
                .fetch_one(repository.pool())
                .await
                .unwrap();
        assert_eq!(2, candidate_count);
    }

    #[tokio::test]
    async fn background_shutdown_timeout_is_global() {
        let handles = vec![
            BackgroundTask::new(
                "stuck-a",
                tokio::spawn(async {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                }),
                BackgroundShutdownPolicy::AbortOnTimeout,
            ),
            BackgroundTask::new(
                "stuck-b",
                tokio::spawn(async {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                }),
                BackgroundShutdownPolicy::AbortOnTimeout,
            ),
        ];
        let started = tokio::time::Instant::now();

        stop_background_tasks_with_timeout(handles, Duration::from_millis(50)).await;

        assert!(started.elapsed() < Duration::from_millis(250));
    }

    #[tokio::test]
    async fn aborted_background_task_is_awaited_for_cleanup() {
        let cleaned_up = Arc::new(AtomicUsize::new(0));
        struct CleanupCounter(Arc<AtomicUsize>);
        impl Drop for CleanupCounter {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let cleanup = CleanupCounter(cleaned_up.clone());
        let handles = vec![BackgroundTask::new(
            "stuck-cleanup",
            tokio::spawn(async move {
                let _cleanup = cleanup;
                tokio::time::sleep(Duration::from_secs(60)).await;
            }),
            BackgroundShutdownPolicy::AbortOnTimeout,
        )];

        stop_background_tasks_with_timeout(handles, Duration::from_millis(10)).await;

        assert_eq!(1, cleaned_up.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn non_abort_background_task_is_awaited_after_timeout() {
        let handles = vec![BackgroundTask::new(
            "finishes-late",
            tokio::spawn(async {
                tokio::time::sleep(Duration::from_millis(75)).await;
            }),
            BackgroundShutdownPolicy::AwaitInFlight,
        )];
        let started = tokio::time::Instant::now();

        stop_background_tasks_with_timeout(handles, Duration::from_millis(10)).await;

        assert!(started.elapsed() >= Duration::from_millis(60));
    }

    #[tokio::test]
    async fn timeout_aborts_abortable_tasks_before_waiting_in_flight() {
        let cleaned_up = Arc::new(AtomicUsize::new(0));
        struct CleanupCounter(Arc<AtomicUsize>, Option<tokio::sync::oneshot::Sender<()>>);
        impl Drop for CleanupCounter {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
                if let Some(sender) = self.1.take() {
                    let _ = sender.send(());
                }
            }
        }
        let (release_in_flight, wait_in_flight) = tokio::sync::oneshot::channel::<()>();
        let (abort_seen, abort_seen_receiver) = tokio::sync::oneshot::channel::<()>();
        let abort_cleanup = CleanupCounter(cleaned_up.clone(), Some(abort_seen));
        let handles = vec![
            BackgroundTask::new(
                "await-first",
                tokio::spawn(async {
                    let _ = wait_in_flight.await;
                }),
                BackgroundShutdownPolicy::AwaitInFlight,
            ),
            BackgroundTask::new(
                "abort-second",
                tokio::spawn(async move {
                    let _abort_cleanup = abort_cleanup;
                    tokio::time::sleep(Duration::from_secs(60)).await;
                }),
                BackgroundShutdownPolicy::AbortOnTimeout,
            ),
        ];
        let shutdown = tokio::spawn(stop_background_tasks_with_timeout(
            handles,
            Duration::from_millis(10),
        ));
        abort_seen_receiver.await.unwrap();
        assert_eq!(1, cleaned_up.load(Ordering::SeqCst));

        release_in_flight.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), shutdown)
            .await
            .unwrap()
            .unwrap();
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

    async fn wait_for_local_item_count(repository: &Repository, expected: i64) {
        for _attempt in 0..50 {
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM local_items")
                .fetch_one(repository.pool())
                .await
                .unwrap();
            if count == expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM local_items")
            .fetch_one(repository.pool())
            .await
            .unwrap();
        assert_eq!(expected, count);
    }

    async fn wait_for_announce_status(repository: &Repository, id: &str, expected: &str) {
        for _attempt in 0..50 {
            let status: String =
                sqlx::query_scalar("SELECT status FROM announce_work WHERE id = ?")
                    .bind(id)
                    .fetch_one(repository.pool())
                    .await
                    .unwrap();
            if status == expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        let status: String = sqlx::query_scalar("SELECT status FROM announce_work WHERE id = ?")
            .bind(id)
            .fetch_one(repository.pool())
            .await
            .unwrap();
        assert_eq!(expected, status);
    }

    async fn wait_for_job_state(repository: &Repository, name: &str, expected: &str) {
        for _attempt in 0..50 {
            let status = sqlx::query_scalar::<_, String>("SELECT state FROM jobs WHERE name = ?")
                .bind(name)
                .fetch_optional(repository.pool())
                .await
                .unwrap();
            if status.as_deref() == Some(expected) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        let status = sqlx::query_scalar::<_, String>("SELECT state FROM jobs WHERE name = ?")
            .bind(name)
            .fetch_optional(repository.pool())
            .await
            .unwrap();
        assert_eq!(Some(expected), status.as_deref());
    }

    async fn wait_for_atomic_count(counter: &AtomicUsize, expected: usize) {
        for _attempt in 0..50 {
            if counter.load(Ordering::SeqCst) >= expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert_eq!(expected, counter.load(Ordering::SeqCst));
    }

    async fn wait_for_queue_completed<T>(
        queue: &crate::runtime::queue::BoundedWorkQueue<T>,
        expected: u64,
    ) {
        for _attempt in 0..50 {
            if queue.stats().completed >= expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert_eq!(expected, queue.stats().completed);
    }

    async fn wait_for_saved_torrent_count(path: &Path, expected: usize) {
        for _attempt in 0..50 {
            let count = saved_torrent_count(path);
            if count == expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert_eq!(expected, saved_torrent_count(path));
    }

    async fn spawn_daemon_qbit_inventory_server(info_requests: Arc<AtomicUsize>) -> String {
        spawn_daemon_test_server(move |request| {
            let info_requests = info_requests.clone();
            async move {
                let path = request.uri().path().to_owned();
                match path.as_str() {
                    "/api/v2/auth/login" => response_with_cookie(StatusCode::OK, "Ok.", "SID=ok"),
                    "/api/v2/torrents/info" => {
                        info_requests.fetch_add(1, Ordering::SeqCst);
                        (
                            StatusCode::OK,
                            r#"[{"hash":"0123456789abcdef0123456789abcdef01234567","name":"Example","save_path":"/downloads/example","amount_left":0,"progress":1.0}]"#,
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

    async fn spawn_daemon_torrent_download_server() -> String {
        let app = axum::Router::new().route(
            "/download",
            get(|| async { (StatusCode::OK, test_torrent_bytes()) }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{address}/download")
    }

    async fn spawn_daemon_torrent_status_server(status: StatusCode) -> String {
        let app =
            axum::Router::new().route("/download", get(move || async move { (status, "limited") }));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{address}/download")
    }

    async fn spawn_daemon_torrent_oversized_server() -> String {
        let app = axum::Router::new().route(
            "/download",
            get(|| async { oversized_response(33 * 1024 * 1024) }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{address}/download")
    }

    async fn spawn_daemon_stalled_torrent_download_server(requests: Arc<AtomicUsize>) -> String {
        let app = axum::Router::new().route(
            "/download",
            get(move || {
                let requests = Arc::clone(&requests);
                async move {
                    requests.fetch_add(1, Ordering::SeqCst);
                    pending::<Response>().await
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{address}/download")
    }

    async fn spawn_daemon_torznab_search_download_server() -> String {
        spawn_daemon_torznab_search_server_with_download(
            StatusCode::OK,
            Arc::new(AtomicUsize::new(0)),
        )
        .await
    }

    async fn spawn_daemon_torznab_search_server_with_download(
        download_status: StatusCode,
        download_requests: Arc<AtomicUsize>,
    ) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let download_url = format!("http://{address}/download");
        let app = axum::Router::new()
            .route(
                "/api",
                get(move || {
                    let download_url = download_url.clone();
                    async move {
                        (
                            StatusCode::OK,
                            search_rss_with_download(
                                "candidate-search",
                                "movie.mkv",
                                &download_url,
                            ),
                        )
                    }
                }),
            )
            .route(
                "/download",
                get(move || {
                    let download_requests = download_requests.clone();
                    async move {
                        download_requests.fetch_add(1, Ordering::SeqCst);
                        (download_status, test_torrent_bytes())
                    }
                }),
            );
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{address}/api")
    }

    async fn spawn_daemon_stalled_torznab_search_server(requests: Arc<AtomicUsize>) -> String {
        let app = axum::Router::new().route(
            "/api",
            get(move || {
                let requests = Arc::clone(&requests);
                async move {
                    requests.fetch_add(1, Ordering::SeqCst);
                    pending::<Response>().await
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

    async fn spawn_daemon_torznab_status_server(status: StatusCode) -> String {
        let app =
            axum::Router::new().route("/api", get(move || async move { (status, "limited") }));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{address}/api")
    }

    async fn spawn_daemon_torznab_oversized_server() -> String {
        let app = axum::Router::new().route(
            "/api",
            get(|| async { oversized_response(9 * 1024 * 1024) }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{address}/api")
    }

    fn oversized_response(length: u64) -> Response {
        Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_LENGTH, length.to_string())
            .body(Body::from(vec![b'a'; length as usize]))
            .unwrap()
    }

    async fn spawn_daemon_torznab_search_server_with_stalled_download(
        download_requests: Arc<AtomicUsize>,
    ) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let download_url = format!("http://{address}/download");
        let app = axum::Router::new()
            .route(
                "/api",
                get(move || {
                    let download_url = download_url.clone();
                    async move {
                        (
                            StatusCode::OK,
                            search_rss_with_download(
                                "candidate-search",
                                "movie.mkv",
                                &download_url,
                            ),
                        )
                    }
                }),
            )
            .route(
                "/download",
                get(move || {
                    let download_requests = Arc::clone(&download_requests);
                    async move {
                        download_requests.fetch_add(1, Ordering::SeqCst);
                        pending::<Response>().await
                    }
                }),
            );
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{address}/api")
    }

    async fn spawn_daemon_torznab_caps_server(requests: Arc<AtomicUsize>) -> String {
        let app = axum::Router::new().route(
            "/api",
            get(move || {
                let requests = requests.clone();
                async move {
                    requests.fetch_add(1, Ordering::SeqCst);
                    (StatusCode::OK, torznab_caps_xml())
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

    async fn spawn_daemon_stalled_torznab_caps_server(requests: Arc<AtomicUsize>) -> String {
        let app = axum::Router::new().route(
            "/api",
            get(move || {
                let requests = Arc::clone(&requests);
                async move {
                    requests.fetch_add(1, Ordering::SeqCst);
                    pending::<Response>().await
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

    async fn spawn_daemon_prowlarr_with_caps_server(
        catalog_requests: Arc<AtomicUsize>,
        caps_requests: Arc<AtomicUsize>,
    ) -> String {
        let catalog = move || {
            let catalog_requests = Arc::clone(&catalog_requests);
            async move {
                catalog_requests.fetch_add(1, Ordering::SeqCst);
                (StatusCode::OK, prowlarr_catalog())
            }
        };
        let caps = move || {
            let caps_requests = Arc::clone(&caps_requests);
            async move {
                caps_requests.fetch_add(1, Ordering::SeqCst);
                (StatusCode::OK, torznab_caps_xml())
            }
        };
        let app = axum::Router::new()
            .route("/api/v1/indexer", get(catalog))
            .route("/101/api", get(caps));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{address}")
    }

    async fn spawn_daemon_test_server<F, Fut, R>(handler: F) -> String
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

    fn json_post(path: &str, body: Value) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri(path)
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
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

    fn local_item(root: &Path) -> LocalItem {
        LocalItem {
            id: None,
            source: LocalItemSource::DataRoot {
                path: root.to_path_buf(),
            },
            title: ItemTitle::new("movie.mkv").unwrap(),
            display_name: DisplayName::new("movie.mkv").unwrap(),
            media_type: MediaType::Movie,
            info_hash: Some(InfoHash::new("0123456789abcdef0123456789abcdef01234567").unwrap()),
            path: Some(root.to_path_buf()),
            save_path: Some(root.to_path_buf()),
            total_size: ByteSize::new(10),
            mtime_ms: None,
        }
    }

    fn local_file() -> LocalFile {
        LocalFile::new(
            None,
            PathBuf::from("movie.mkv"),
            ByteSize::new(10),
            FileIndex::new(0),
        )
        .unwrap()
    }

    fn preexisting_indexer_candidate() -> RemoteCandidate {
        RemoteCandidate {
            id: None,
            indexer_id: IndexerId::new(1).unwrap(),
            guid: CandidateGuid::new("guid-announce").unwrap(),
            download_url: DownloadUrl::new("https://indexer.example/download/guid-announce")
                .unwrap(),
            title: ItemTitle::new("other.mkv").unwrap(),
            tracker: TrackerName::new("indexer.example").unwrap(),
            size: Some(ByteSize::new(10)),
            published_at_ms: None,
            info_hash: None,
            torrent_cache_path: None,
        }
    }

    async fn insert_announce_row(
        repository: &Repository,
        id: &AnnounceWorkId,
        guid: &str,
        tracker: &str,
        download_url: &str,
    ) {
        sqlx::query(
            r#"
            INSERT INTO announce_work (
                id, dedupe_hash, received_at, updated_at, tracker, guid,
                title, download_url, redacted_download_url, status, reason,
                attempt_count, next_attempt_at, expires_at
            )
            VALUES (?, ?, 1, 1, ?, ?, 'movie.mkv', ?, ?, 'queued', 'accepted', 0, 1, 100000)
            "#,
        )
        .bind(id.as_str())
        .bind(format!("dedupe-{}", id.as_str()))
        .bind(tracker)
        .bind(guid)
        .bind(download_url)
        .bind(download_url)
        .execute(repository.pool())
        .await
        .unwrap();
    }

    fn test_torrent_bytes() -> &'static [u8] {
        b"d8:announce14:http://tracker4:infod6:lengthi10e4:name9:movie.mkv12:piece lengthi10e6:pieces20:aaaaaaaaaaaaaaaaaaaaee"
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

    fn search_rss_with_download(guid: &str, title: &str, download_url: &str) -> String {
        format!(
            r#"
            <rss>
              <channel>
                <item>
                  <title>{title}</title>
                  <guid>{guid}</guid>
                  <link>https://indexer.example/details/{guid}</link>
                  <enclosure url="{download_url}" length="10" type="application/x-bittorrent"/>
                  <torznab:attr name="size" value="10"/>
                </item>
              </channel>
            </rss>
            "#
        )
    }

    fn daemon_movie_caps() -> TorznabCaps {
        TorznabCaps {
            search: SearchCaps {
                movie_search: true,
                supported_id_params: std::collections::BTreeSet::from(["q".to_owned()]),
                ..SearchCaps::default()
            },
            categories: CategoryCaps {
                movie: true,
                ..CategoryCaps::default()
            },
            limits: TorznabLimits::default(),
        }
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

    fn saved_torrent_count(path: &Path) -> usize {
        fs::read_dir(path)
            .map(|entries| entries.count())
            .unwrap_or(0)
    }
}
