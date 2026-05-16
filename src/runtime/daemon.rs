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
    CandidateAssessment, CandidateGuid, DownloadUrl, IndexerId, ItemTitle, MatchDecision,
    RemoteCandidate, RemoteCandidateId, TrackerName,
};
use crate::errors::{ConfigError, DatabaseError};
use crate::http::{SearchWorkflowRequest, router};
use crate::indexers::{CachedCandidateTorrent, CandidateDownloadClient, CandidateDownloadError};
use crate::inventory_refresh::run_inventory_refresh_worker;
use crate::matching::{
    PersistedCandidateAssessment, ReverseLookupConfig, ReverseLookupError, ReverseLookupOutcome,
    assess_and_persist_candidate, reverse_lookup_and_assess_candidate, reverse_lookup_candidates,
};
use crate::notifications::{NotificationWorker, run_notification_worker};
use crate::persistence::repository::Repository;
use crate::runtime::announce_worker::{
    AnnounceOutcomeConfig, AnnounceWorkOutcome, AnnounceWorker, AnnounceWorkerError,
    classify_injection_result, classify_reverse_lookup_outcome, unix_time_ms,
};
use crate::runtime::app::{AppRuntime, AppState, RuntimeReceivers};
use crate::runtime::injection_worker::{
    InjectionRequest, InjectionWorker, RecheckResumeConfig, SavedTorrentRetryConfig,
};
use crate::runtime::scheduler::parse_interval_ms;
use crate::runtime::shutdown::{ShutdownController, ShutdownPhase, ShutdownSignal};

const BACKGROUND_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
const ANNOUNCE_IDLE_SLEEP: Duration = Duration::from_millis(500);
const ANNOUNCE_CANDIDATE_INDEXER_ID: u64 = i64::MAX as u64;

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
        BackgroundShutdownPolicy::AbortOnTimeout,
    ));
    handles.push(BackgroundTask::new(
        "jobs-receiver",
        tokio::spawn(hold_receiver_open(
            jobs,
            runtime.state.shutdown_signal.clone(),
        )),
        BackgroundShutdownPolicy::AbortOnTimeout,
    ));
    handles.push(BackgroundTask::new(
        "scheduler-receiver",
        tokio::spawn(hold_receiver_open(
            scheduler,
            runtime.state.shutdown_signal.clone(),
        )),
        BackgroundShutdownPolicy::AbortOnTimeout,
    ));

    Ok(handles)
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
            _state = shutdown.cancelled() => {
                receiver.close();
                break;
            }
            request = receiver.recv() => {
                let Some(request) = request else {
                    break;
                };
                match state.plan_search_workflow(request, unix_time_ms()).await {
                    Ok(summary) => {
                        receiver.mark_completed();
                        tracing::info!(
                            planned_indexers = summary.plans.len(),
                            "search workflow query planning completed"
                        );
                    }
                    Err(error) => warn!(error = %error, "search workflow query planning failed"),
                }
            }
        }
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
    let cached = match downloader
        .download_and_cache(
            &candidate.candidate,
            &state.config.paths.torrent_cache_dir,
            fetch.cookie.as_deref(),
        )
        .await
    {
        Ok(cached) => cached,
        Err(error) => return classify_candidate_download_error(error, now_ms),
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
                save_candidate_torrent(
                    &state.config.paths.output_dir,
                    &candidate_output_metadata(
                        lookup.local_item.media_type,
                        &cached.candidate,
                        &cached.metafile,
                    ),
                    &torrent_bytes,
                )
                .map_err(|error| error.to_string())?;
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
                .process(InjectionRequest {
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
                })
                .await
                .map_err(|error| format!("{error:?}"))?;
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
    use std::future::Future;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::config::{ConfigTorrentClientKind, SporosConfig, TorrentClientConfig};
    use crate::domain::{
        ByteSize, CandidateGuid, DisplayName, DownloadUrl, FileIndex, IndexerId, InfoHash,
        ItemTitle, LocalFile, LocalItem, LocalItemSource, MediaType, RemoteCandidate, TrackerName,
    };
    use crate::persistence::repository::Repository;
    use axum::body::Body;
    use axum::http::{Request, StatusCode, header::SET_COOKIE};
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
