use std::collections::VecDeque;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::future::Future;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(test)]
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::time::{Duration, Instant};

use futures_util::stream::{FuturesUnordered, StreamExt};
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
#[cfg(windows)]
use std::os::windows::fs::MetadataExt;

use tokio::sync::{Mutex, MutexGuard, mpsc};
use tokio::task::JoinSet;
use tracing::warn;

use crate::actions::{
    CreatedLink, CreatedRoot, DryRunLinkOptions, LinkActionError, LinkDirOptions, LinkFilesOptions,
    LinkType, PreparedLink, SaveTorrentError, candidate_output_metadata,
    cleanup_created_links_and_roots, link_destination_dir, link_metafile_files,
    plan_metafile_link_dry_run, save_candidate_torrent, select_link_dir_pinned,
    validate_prepared_links,
};
use crate::clients::TorrentClientDescriptor;
use crate::domain::{
    ByteSize, CandidateAssessment, CandidateGuid, ClientHost, DependencyKind, DependencyName,
    DependencyState, DownloadUrl, IndexerId, InfoHash, InjectionOutcome, ItemTitle, LocalFile,
    LocalItem, MatchDecision, ReasonText, RemoteCandidate, RemoteCandidateId, TorrentMetafile,
    TrackerName, checked_file_total,
};
use crate::errors::{
    ClassifyFailure, DatabaseError, FailureClass, TorrentClientError, TorrentParseError,
};
use crate::inventory_refresh::{
    InventoryRefreshError, InventoryRefreshSummary, InventoryRefreshWorker,
};
use crate::matching::{
    CandidateAssessmentConfig, CandidateAssessmentInput, FileTreeMatchConfig,
    PersistedCandidateAssessment, ReverseLookupConfig, assess_and_persist_candidate,
    reverse_lookup_candidates_for_media_types,
};
use crate::persistence::repository::Repository;
use crate::persistence::torrent_cache::{TorrentOutputMetadata, parse_torrent_output_filename};
use crate::runtime::queue::{BoundedWorkQueue, QueueKind, WorkReceiver, bounded_work_queue};
use crate::runtime::shutdown::{ShutdownPhase, ShutdownSignal, shutdown_channel};
use crate::torrent::parse_metafile;

const MAX_SAVED_TORRENT_BYTES: u64 = 32 * 1024 * 1024;
const SAVED_TORRENT_SCAN_BATCH: usize = 32;
const SAVED_TORRENT_RETRY_PREFLIGHT_CONCURRENCY: usize = 4;
const CLIENT_INVENTORY_REFRESH_CONCURRENCY: usize = 4;

#[cfg(test)]
static SAVED_TORRENT_READ_DELAY_MS: AtomicU64 = AtomicU64::new(0);
#[cfg(test)]
static SAVED_TORRENT_READ_IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);
#[cfg(test)]
static SAVED_TORRENT_READ_MAX_IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);

pub type ClientResultFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, TorrentClientError>> + Send + 'a>>;
pub type ClientInventoryRefreshFuture<'a> = Pin<
    Box<dyn Future<Output = Result<InventoryRefreshSummary, InventoryRefreshError>> + Send + 'a>,
>;

pub trait InjectionClient: Send + Sync {
    fn descriptor(&self) -> &TorrentClientDescriptor;
    fn has_torrent<'a>(&'a self, info_hash: &'a InfoHash) -> ClientResultFuture<'a, bool>;
    fn inject<'a>(&'a self, request: ClientInjectionRequest<'a>) -> ClientResultFuture<'a, ()>;
    fn recheck<'a>(&'a self, info_hash: &'a InfoHash) -> ClientResultFuture<'a, ()>;
    fn is_checking<'a>(&'a self, info_hash: &'a InfoHash) -> ClientResultFuture<'a, bool>;
    fn remaining_bytes<'a>(&'a self, info_hash: &'a InfoHash) -> ClientResultFuture<'a, ByteSize>;
    fn resume<'a>(&'a self, info_hash: &'a InfoHash) -> ClientResultFuture<'a, ()>;
    fn refresh_inventory<'a>(
        &'a self,
        _worker: &'a InventoryRefreshWorker,
        _shutdown: ShutdownSignal,
    ) -> ClientInventoryRefreshFuture<'a> {
        Box::pin(async move {
            let descriptor = self.descriptor();
            Err(TorrentClientError::UnsupportedCapability {
                client: descriptor.name.as_str().to_owned(),
                capability: "refresh inventory".to_owned(),
            }
            .into())
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ClientInjectionRequest<'a> {
    pub info_hash: &'a InfoHash,
    pub torrent_bytes: &'a [u8],
    pub save_path: Option<&'a Path>,
    pub pause_for_recheck: bool,
}

#[derive(Debug, Clone)]
pub struct InjectionRequest {
    pub local_item: LocalItem,
    pub local_files: Vec<LocalFile>,
    pub candidate: RemoteCandidate,
    pub candidate_id: RemoteCandidateId,
    pub metafile: TorrentMetafile,
    pub torrent_bytes: Vec<u8>,
    pub assessment: CandidateAssessment,
    pub assessed_at_ms: i64,
    pub output_dir: PathBuf,
    pub link_dirs: Vec<PathBuf>,
    pub link_type: Option<LinkType>,
    pub flat_linking: bool,
    pub recheck: RecheckResumeConfig,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct InjectionWorkResult {
    pub outcome: InjectionOutcome,
    pub target_client: Option<DependencyName>,
    pub dry_run_action: Option<DryRunAction>,
    pub saved_for_retry: bool,
    pub linked_files: usize,
    pub prepared_link_cleanup_incomplete: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DryRunAction {
    SaveCandidateTorrent {
        output_dir: PathBuf,
    },
    InjectTorrent {
        save_path: Option<PathBuf>,
        planned_link_destination: Option<PathBuf>,
        link_type: Option<LinkType>,
        link_dir_count: usize,
        flat_linking: bool,
        pause_for_recheck: bool,
        would_recheck: bool,
    },
}

#[derive(Debug, Clone)]
pub struct SavedTorrentRetryConfig {
    pub directories: Vec<PathBuf>,
    pub max_saved_torrents: usize,
    pub link_dirs: Vec<PathBuf>,
    pub link_type: Option<LinkType>,
    pub flat_linking: bool,
    pub recheck: RecheckResumeConfig,
    pub reverse_lookup: ReverseLookupConfig,
    pub assessed_at_ms: i64,
}

impl Default for SavedTorrentRetryConfig {
    fn default() -> Self {
        Self {
            directories: Vec::new(),
            max_saved_torrents: 1_000,
            link_dirs: Vec::new(),
            link_type: None,
            flat_linking: false,
            recheck: RecheckResumeConfig::default(),
            reverse_lookup: ReverseLookupConfig {
                assessment: CandidateAssessmentConfig {
                    file_tree: FileTreeMatchConfig {
                        mode: crate::matching::FileTreeMatchMode::Flexible,
                        ..FileTreeMatchConfig::default()
                    },
                    ..CandidateAssessmentConfig::default()
                },
                ..ReverseLookupConfig::default()
            },
            assessed_at_ms: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub struct SavedTorrentRetrySummary {
    pub scanned: usize,
    pub attempted: usize,
    pub injected: usize,
    pub dry_run: usize,
    pub already_exists: usize,
    pub source_incomplete: usize,
    pub failed: usize,
    pub no_match: usize,
    pub skipped: usize,
    pub deleted: usize,
    pub kept: usize,
}

#[derive(Debug)]
pub enum InjectionWorkerError {
    NoWritableClient,
    MissingLocalItemId,
    Database(DatabaseError),
    Save(SaveTorrentError),
    Link(LinkActionError),
    Client(TorrentClientError),
    ClientWithPreparedLinkCleanup {
        source: TorrentClientError,
        prepared_link_cleanup_incomplete: bool,
    },
    TorrentParse(TorrentParseError),
    Io {
        operation: &'static str,
        path: PathBuf,
        source: std::io::Error,
    },
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RecheckResumeConfig {
    pub skip_recheck: bool,
    pub auto_resume_max_download: ByteSize,
    pub min_completion_percent: Option<f64>,
    pub max_remaining_percent: Option<f64>,
    pub ignore_non_relevant_files_to_resume: bool,
    pub non_relevant_max_remaining: ByteSize,
    pub piece_slack_multiplier: u64,
    pub poll_interval_ms: u64,
    pub max_resume_wait_ms: u64,
    pub below_threshold_action: BelowThresholdAction,
}

impl Default for RecheckResumeConfig {
    fn default() -> Self {
        Self {
            skip_recheck: false,
            auto_resume_max_download: ByteSize::new(0),
            min_completion_percent: None,
            max_remaining_percent: None,
            ignore_non_relevant_files_to_resume: false,
            non_relevant_max_remaining: ByteSize::new(200 * 1024 * 1024),
            piece_slack_multiplier: 2,
            poll_interval_ms: 5_000,
            max_resume_wait_ms: 60 * 60 * 1_000,
            below_threshold_action: BelowThresholdAction::InjectPaused,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub enum BelowThresholdAction {
    InjectAndStart,
    #[default]
    InjectPaused,
    RejectWithoutInjecting,
}

impl From<&crate::config::AutoResumePolicyConfig> for RecheckResumeConfig {
    fn from(config: &crate::config::AutoResumePolicyConfig) -> Self {
        Self {
            skip_recheck: config.skip_recheck,
            auto_resume_max_download: ByteSize::new(config.max_remaining_bytes),
            min_completion_percent: config.min_completion_percent,
            max_remaining_percent: config.max_remaining_percent,
            ignore_non_relevant_files_to_resume: config.ignore_non_relevant_files_to_resume,
            non_relevant_max_remaining: ByteSize::new(config.non_relevant_max_remaining_bytes),
            piece_slack_multiplier: config.piece_slack_multiplier,
            poll_interval_ms: config.poll_interval_ms,
            max_resume_wait_ms: config.max_resume_wait_ms,
            below_threshold_action: BelowThresholdAction::from(config.below_threshold_action),
        }
    }
}

impl From<crate::config::BelowThresholdActionConfig> for BelowThresholdAction {
    fn from(config: crate::config::BelowThresholdActionConfig) -> Self {
        match config {
            crate::config::BelowThresholdActionConfig::InjectAndStart => Self::InjectAndStart,
            crate::config::BelowThresholdActionConfig::InjectPaused => Self::InjectPaused,
            crate::config::BelowThresholdActionConfig::RejectWithoutInjecting => {
                Self::RejectWithoutInjecting
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RecheckResumePlan {
    pub should_recheck: bool,
    pub max_remaining_bytes: ByteSize,
    pub min_completion_percent: Option<f64>,
    pub max_remaining_percent: Option<f64>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ResumeLoopOutcome {
    NotRequired,
    Resumed,
    WaitingForCompletion,
    StillChecking,
}

impl From<DatabaseError> for InjectionWorkerError {
    fn from(error: DatabaseError) -> Self {
        Self::Database(error)
    }
}

impl From<SaveTorrentError> for InjectionWorkerError {
    fn from(error: SaveTorrentError) -> Self {
        Self::Save(error)
    }
}

impl From<LinkActionError> for InjectionWorkerError {
    fn from(error: LinkActionError) -> Self {
        Self::Link(error)
    }
}

impl From<TorrentClientError> for InjectionWorkerError {
    fn from(error: TorrentClientError) -> Self {
        Self::Client(error)
    }
}

impl From<TorrentParseError> for InjectionWorkerError {
    fn from(error: TorrentParseError) -> Self {
        Self::TorrentParse(error)
    }
}

#[derive(Clone)]
pub struct InjectionWorker {
    repository: Repository,
    clients: Vec<Arc<dyn InjectionClient>>,
    mutation_lock: Arc<Mutex<()>>,
    dry_run: bool,
}

impl fmt::Debug for InjectionWorker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InjectionWorker")
            .field("client_count", &self.clients.len())
            .finish_non_exhaustive()
    }
}

async fn refresh_client_inventory_task(
    index: usize,
    client: Arc<dyn InjectionClient>,
    worker: InventoryRefreshWorker,
    shutdown: ShutdownSignal,
) -> (
    usize,
    String,
    ClientHost,
    Result<InventoryRefreshSummary, InventoryRefreshError>,
) {
    let name = client.descriptor().name.as_str().to_owned();
    let host = client.descriptor().host.clone();
    let result = client.refresh_inventory(&worker, shutdown).await;
    (index, name, host, result)
}

fn spawn_client_inventory_refreshes(
    refreshes: &mut JoinSet<(
        usize,
        String,
        ClientHost,
        Result<InventoryRefreshSummary, InventoryRefreshError>,
    )>,
    pending_clients: &mut VecDeque<(usize, Arc<dyn InjectionClient>)>,
    worker: &InventoryRefreshWorker,
    shutdown: &ShutdownSignal,
) {
    while refreshes.len() < CLIENT_INVENTORY_REFRESH_CONCURRENCY {
        let Some((index, client)) = pending_clients.pop_front() else {
            break;
        };
        refreshes.spawn(refresh_client_inventory_task(
            index,
            client,
            worker.clone(),
            shutdown.clone(),
        ));
    }
}

impl InjectionWorker {
    pub fn new(repository: Repository, clients: Vec<Arc<dyn InjectionClient>>) -> Self {
        Self {
            repository,
            clients,
            mutation_lock: Arc::new(Mutex::new(())),
            dry_run: false,
        }
    }

    pub const fn with_dry_run(mut self, dry_run: bool) -> Self {
        self.dry_run = dry_run;
        self
    }

    pub fn client_count(&self) -> usize {
        self.clients.len()
    }

    pub async fn refresh_client_inventories(
        &self,
        worker: &InventoryRefreshWorker,
    ) -> Result<Vec<InventoryRefreshSummary>, InventoryRefreshError> {
        let (_controller, shutdown) = shutdown_channel();
        self.refresh_client_inventories_until_shutdown(worker, shutdown)
            .await
    }

    pub async fn refresh_client_inventories_until_shutdown(
        &self,
        worker: &InventoryRefreshWorker,
        shutdown: ShutdownSignal,
    ) -> Result<Vec<InventoryRefreshSummary>, InventoryRefreshError> {
        let mut summaries_by_index = Vec::with_capacity(self.clients.len());
        summaries_by_index.resize_with(self.clients.len(), || None);
        let mut refreshed_client_hosts = Vec::with_capacity(self.clients.len());
        let mut last_error = None;
        let mut cancellation_error = None;
        let client_worker = worker.without_client_post_refresh_work();
        if self.clients.is_empty() {
            return Ok(Vec::new());
        }
        if shutdown.state().phase != ShutdownPhase::Running {
            let Some(client) = self.clients.first() else {
                return Ok(Vec::new());
            };
            return Err(InventoryRefreshError::Client {
                source: TorrentClientError::Cancelled {
                    client: client.descriptor().name.as_str().to_owned(),
                    message: "shutdown requested".to_owned(),
                },
            });
        }

        let mut pending_clients = self
            .clients
            .iter()
            .cloned()
            .enumerate()
            .collect::<VecDeque<_>>();
        let mut refreshes = JoinSet::new();
        spawn_client_inventory_refreshes(
            &mut refreshes,
            &mut pending_clients,
            &client_worker,
            &shutdown,
        );

        while let Some(result) = refreshes.join_next().await {
            let (index, client_name, client_host, result) =
                result.map_err(|error| InventoryRefreshError::ScanWorkerFailed {
                    message: error.to_string(),
                })?;
            match result {
                Ok(summary) => {
                    let summary_slot = summaries_by_index.get_mut(index).ok_or_else(|| {
                        InventoryRefreshError::ScanWorkerFailed {
                            message: format!(
                                "client inventory refresh task returned out-of-range index {index}"
                            ),
                        }
                    })?;
                    *summary_slot = Some(summary);
                    refreshed_client_hosts.push(client_host);
                }
                Err(
                    error @ InventoryRefreshError::Client {
                        source: TorrentClientError::Cancelled { .. },
                    },
                ) => {
                    cancellation_error = Some(error);
                    pending_clients.clear();
                }
                Err(error) => {
                    warn!(
                        client = %client_name,
                        error = %error,
                        "client inventory refresh failed"
                    );
                    last_error = Some(error);
                }
            }
            if cancellation_error.is_none() {
                if shutdown.state().phase == ShutdownPhase::Running {
                    spawn_client_inventory_refreshes(
                        &mut refreshes,
                        &mut pending_clients,
                        &client_worker,
                        &shutdown,
                    );
                } else if !pending_clients.is_empty() {
                    let skipped_client = pending_clients
                        .front()
                        .map(|(_, client)| client.descriptor().name.as_str().to_owned())
                        .unwrap_or(client_name);
                    pending_clients.clear();
                    cancellation_error = Some(InventoryRefreshError::Client {
                        source: TorrentClientError::Cancelled {
                            client: skipped_client,
                            message: "shutdown requested".to_owned(),
                        },
                    });
                }
            }
        }

        let summaries = summaries_by_index.into_iter().flatten().collect::<Vec<_>>();
        if !refreshed_client_hosts.is_empty() {
            worker
                .refresh_virtual_seasons_after_client_batch(&refreshed_client_hosts)
                .await?;
        }
        if let Some(error) = cancellation_error {
            return Err(error);
        }
        if summaries.is_empty()
            && let Some(error) = last_error
        {
            return Err(error);
        }
        Ok(summaries)
    }

    pub async fn process(
        &self,
        request: InjectionRequest,
    ) -> Result<InjectionWorkResult, InjectionWorkerError> {
        self.process_inner(request, &mut || false, None, false)
            .await
    }

    pub async fn process_until_shutdown(
        &self,
        request: InjectionRequest,
        shutdown: ShutdownSignal,
    ) -> Result<InjectionWorkResult, InjectionWorkerError> {
        let stop_signal = shutdown.clone();
        self.process_inner(
            request,
            &mut || stop_signal.state().phase != ShutdownPhase::Running,
            Some(&shutdown),
            false,
        )
        .await
    }

    async fn process_inner<F>(
        &self,
        request: InjectionRequest,
        should_stop: &mut F,
        shutdown: Option<&ShutdownSignal>,
        from_saved_retry: bool,
    ) -> Result<InjectionWorkResult, InjectionWorkerError>
    where
        F: FnMut() -> bool,
    {
        let local_item_id = request
            .local_item
            .id
            .ok_or(InjectionWorkerError::MissingLocalItemId)?;
        self.repository
            .record_match_decision(
                local_item_id,
                request.candidate_id,
                request.assessment,
                request.assessed_at_ms,
            )
            .await?;

        let target = self
            .select_target(&request.local_item)
            .ok_or(InjectionWorkerError::NoWritableClient)?;
        let target_name = dependency_name(target.descriptor())?;
        if should_stop() {
            if self.dry_run {
                return Ok(InjectionWorkResult {
                    outcome: InjectionOutcome::DryRun,
                    target_client: Some(target_name),
                    dry_run_action: Some(dry_run_inject_action(&request, None)),
                    saved_for_retry: false,
                    linked_files: 0,
                    prepared_link_cleanup_incomplete: false,
                });
            }
            self.save_for_retry_phase(&request).await?;
            return Ok(InjectionWorkResult {
                outcome: InjectionOutcome::Saved,
                target_client: Some(target_name),
                dry_run_action: None,
                saved_for_retry: true,
                linked_files: 0,
                prepared_link_cleanup_incomplete: false,
            });
        }
        let existing = self
            .find_existing_client(
                request.metafile.info_hash(),
                target.descriptor(),
                request.assessed_at_ms,
                shutdown,
            )
            .await?;
        match existing {
            ExistingClientLookup::Found(existing_client) => {
                self.record_client_health(
                    existing_client.descriptor(),
                    true,
                    None,
                    request.assessed_at_ms,
                )
                .await?;
                return Ok(InjectionWorkResult {
                    outcome: InjectionOutcome::AlreadyExists,
                    target_client: Some(dependency_name(existing_client.descriptor())?),
                    dry_run_action: None,
                    saved_for_retry: false,
                    linked_files: 0,
                    prepared_link_cleanup_incomplete: false,
                });
            }
            ExistingClientLookup::NotFound => {}
            ExistingClientLookup::Shutdown => {
                if self.dry_run {
                    return Ok(InjectionWorkResult {
                        outcome: InjectionOutcome::DryRun,
                        target_client: Some(target_name),
                        dry_run_action: Some(dry_run_inject_action(&request, None)),
                        saved_for_retry: false,
                        linked_files: 0,
                        prepared_link_cleanup_incomplete: false,
                    });
                }
                self.save_for_retry_phase(&request).await?;
                return Ok(InjectionWorkResult {
                    outcome: InjectionOutcome::Saved,
                    target_client: Some(target_name),
                    dry_run_action: None,
                    saved_for_retry: true,
                    linked_files: 0,
                    prepared_link_cleanup_incomplete: false,
                });
            }
        }

        if should_stop() {
            if self.dry_run {
                return Ok(InjectionWorkResult {
                    outcome: InjectionOutcome::DryRun,
                    target_client: Some(target_name),
                    dry_run_action: Some(dry_run_inject_action(&request, None)),
                    saved_for_retry: false,
                    linked_files: 0,
                    prepared_link_cleanup_incomplete: false,
                });
            }
            self.save_for_retry_phase(&request).await?;
            return Ok(InjectionWorkResult {
                outcome: InjectionOutcome::Saved,
                target_client: Some(target_name),
                dry_run_action: None,
                saved_for_retry: true,
                linked_files: 0,
                prepared_link_cleanup_incomplete: false,
            });
        }

        let preparation_phase = if self.dry_run {
            self.dry_run_injection_phase(&request, &target_name)?
        } else {
            self.prepare_injection_phase(&request, &target_name, from_saved_retry)
                .await?
        };
        let preparation = match preparation_phase {
            InjectionPreparationPhase::Ready(preparation) => preparation,
            InjectionPreparationPhase::Rejected(result) => return Ok(result),
            InjectionPreparationPhase::SourceIncomplete => {
                if self.dry_run {
                    return Ok(InjectionWorkResult {
                        outcome: InjectionOutcome::SourceIncomplete,
                        target_client: Some(target_name),
                        dry_run_action: None,
                        saved_for_retry: false,
                        linked_files: 0,
                        prepared_link_cleanup_incomplete: false,
                    });
                }
                self.save_for_retry_phase(&request).await?;
                return Ok(InjectionWorkResult {
                    outcome: InjectionOutcome::SourceIncomplete,
                    target_client: Some(target_name),
                    dry_run_action: None,
                    saved_for_retry: true,
                    linked_files: 0,
                    prepared_link_cleanup_incomplete: false,
                });
            }
        };

        if should_stop() {
            if self.dry_run {
                return Ok(InjectionWorkResult {
                    outcome: InjectionOutcome::DryRun,
                    target_client: Some(target_name),
                    dry_run_action: Some(dry_run_inject_action(&request, Some(&preparation))),
                    saved_for_retry: false,
                    linked_files: preparation.linked_files,
                    prepared_link_cleanup_incomplete: false,
                });
            }
            let save_result = self.save_for_retry_phase(&request).await;
            let cleanup = preparation.cleanup_prepared_links_phase();
            save_result?;
            return Ok(InjectionWorkResult {
                outcome: InjectionOutcome::Saved,
                target_client: Some(target_name),
                dry_run_action: None,
                saved_for_retry: true,
                linked_files: preparation.linked_files,
                prepared_link_cleanup_incomplete: cleanup.prepared_link_cleanup_incomplete,
            });
        }
        let mutation_result = self
            .run_mutation_phase(
                &request,
                target.as_ref(),
                &preparation,
                should_stop,
                shutdown,
            )
            .await;

        match mutation_result {
            InjectionMutationResult::SavedForShutdown => {
                if self.dry_run {
                    return Ok(InjectionWorkResult {
                        outcome: InjectionOutcome::DryRun,
                        target_client: Some(target_name),
                        dry_run_action: Some(dry_run_inject_action(&request, Some(&preparation))),
                        saved_for_retry: false,
                        linked_files: preparation.linked_files,
                        prepared_link_cleanup_incomplete: false,
                    });
                }
                let save_result = self.save_for_retry_phase(&request).await;
                let cleanup = preparation.cleanup_prepared_links_phase();
                save_result?;
                Ok(InjectionWorkResult {
                    outcome: InjectionOutcome::Saved,
                    target_client: Some(target_name),
                    dry_run_action: None,
                    saved_for_retry: true,
                    linked_files: preparation.linked_files,
                    prepared_link_cleanup_incomplete: cleanup.prepared_link_cleanup_incomplete,
                })
            }
            InjectionMutationResult::AlreadyExistsOnTarget => {
                self.record_client_health(target.descriptor(), true, None, request.assessed_at_ms)
                    .await?;
                if self.dry_run {
                    return Ok(InjectionWorkResult {
                        outcome: InjectionOutcome::AlreadyExists,
                        target_client: Some(target_name),
                        dry_run_action: None,
                        saved_for_retry: false,
                        linked_files: 0,
                        prepared_link_cleanup_incomplete: false,
                    });
                }
                let mut saved_for_retry = false;
                if preparation.has_prepared_links
                    || (from_saved_retry && preparation.recheck_plan.should_recheck)
                {
                    let resume_outcome = self
                        .run_recheck_resume(
                            target.as_ref(),
                            &request,
                            if preparation.has_prepared_links {
                                preparation.recheck_after_linking
                            } else {
                                preparation.recheck_plan
                            },
                            shutdown,
                        )
                        .await?;
                    if resume_outcome == ResumeLoopOutcome::StillChecking {
                        self.save_for_retry_phase(&request).await?;
                        saved_for_retry = true;
                    }
                }
                Ok(InjectionWorkResult {
                    outcome: InjectionOutcome::AlreadyExists,
                    target_client: Some(target_name),
                    dry_run_action: None,
                    saved_for_retry,
                    linked_files: preparation.linked_files,
                    prepared_link_cleanup_incomplete: false,
                })
            }
            InjectionMutationResult::AlreadyExistsOnOtherClient(existing_client) => {
                let cleanup = preparation.cleanup_prepared_links_phase();
                self.record_client_health(
                    existing_client.descriptor(),
                    true,
                    None,
                    request.assessed_at_ms,
                )
                .await?;
                Ok(InjectionWorkResult {
                    outcome: InjectionOutcome::AlreadyExists,
                    target_client: Some(dependency_name(existing_client.descriptor())?),
                    dry_run_action: None,
                    saved_for_retry: false,
                    linked_files: 0,
                    prepared_link_cleanup_incomplete: cleanup.prepared_link_cleanup_incomplete,
                })
            }
            InjectionMutationResult::Injected(Ok(())) => {
                self.record_client_health(target.descriptor(), true, None, request.assessed_at_ms)
                    .await?;
                if preparation.run_resume_after_inject {
                    let save_result = self.save_for_retry_phase(&request).await;
                    let resume_result = self
                        .run_recheck_resume(
                            target.as_ref(),
                            &request,
                            if preparation.has_prepared_links {
                                preparation.recheck_after_linking
                            } else {
                                preparation.recheck_plan
                            },
                            shutdown,
                        )
                        .await;
                    save_result?;
                    resume_result?;
                }
                Ok(InjectionWorkResult {
                    outcome: InjectionOutcome::Injected,
                    target_client: Some(target_name),
                    dry_run_action: None,
                    saved_for_retry: preparation.run_resume_after_inject,
                    linked_files: preparation.linked_files,
                    prepared_link_cleanup_incomplete: false,
                })
            }
            InjectionMutationResult::DryRun => {
                self.record_client_health(target.descriptor(), true, None, request.assessed_at_ms)
                    .await?;
                let cleanup = preparation.cleanup_prepared_links_phase();
                Ok(InjectionWorkResult {
                    outcome: InjectionOutcome::DryRun,
                    target_client: Some(target_name),
                    dry_run_action: Some(dry_run_inject_action(&request, Some(&preparation))),
                    saved_for_retry: false,
                    linked_files: preparation.linked_files,
                    prepared_link_cleanup_incomplete: cleanup.prepared_link_cleanup_incomplete,
                })
            }
            InjectionMutationResult::Injected(Err(error)) => {
                let save_result = self.save_for_retry_phase(&request).await;
                let cleanup = preparation.cleanup_prepared_links_phase();
                self.record_client_health(
                    target.descriptor(),
                    false,
                    Some(&error),
                    request.assessed_at_ms,
                )
                .await?;
                save_result?;
                Ok(InjectionWorkResult {
                    outcome: InjectionOutcome::Failed,
                    target_client: Some(target_name),
                    dry_run_action: None,
                    saved_for_retry: true,
                    linked_files: preparation.linked_files,
                    prepared_link_cleanup_incomplete: cleanup.prepared_link_cleanup_incomplete,
                })
            }
            InjectionMutationResult::PreparedLinksInvalid(error) => {
                let save_result = self.save_for_retry_phase(&request).await;
                let cleanup = preparation.cleanup_prepared_links_phase();
                if cleanup.prepared_link_cleanup_incomplete {
                    Err(InjectionWorkerError::Link(
                        LinkActionError::CleanupIncomplete {
                            primary: Box::new(error),
                            cleanup: Box::new(LinkActionError::Io {
                                operation: "clean prepared links after revalidation failure",
                                path: preparation.save_path.clone().unwrap_or_default(),
                                source: std::io::Error::other("prepared link cleanup incomplete"),
                            }),
                        },
                    ))
                } else {
                    save_result?;
                    Err(error.into())
                }
            }
            InjectionMutationResult::ExistingLookupFailed(error) => {
                let cleanup = preparation.cleanup_prepared_links_phase();
                if cleanup.prepared_link_cleanup_incomplete {
                    match error {
                        InjectionWorkerError::Client(source) => {
                            Err(InjectionWorkerError::ClientWithPreparedLinkCleanup {
                                source,
                                prepared_link_cleanup_incomplete: cleanup
                                    .prepared_link_cleanup_incomplete,
                            })
                        }
                        error => Err(error),
                    }
                } else {
                    Err(error)
                }
            }
            InjectionMutationResult::TargetPrecheckFailed(error) => {
                let cleanup = preparation.cleanup_prepared_links_phase();
                self.record_client_health(
                    target.descriptor(),
                    false,
                    Some(&error),
                    request.assessed_at_ms,
                )
                .await?;
                if cleanup.prepared_link_cleanup_incomplete {
                    Err(InjectionWorkerError::ClientWithPreparedLinkCleanup {
                        source: error,
                        prepared_link_cleanup_incomplete: cleanup.prepared_link_cleanup_incomplete,
                    })
                } else {
                    Err(error.into())
                }
            }
        }
    }

    pub async fn retry_saved_torrents(
        &self,
        config: SavedTorrentRetryConfig,
    ) -> Result<SavedTorrentRetrySummary, InjectionWorkerError> {
        self.retry_saved_torrents_inner(config, || false, None)
            .await
    }

    pub async fn retry_saved_torrents_until_shutdown(
        &self,
        config: SavedTorrentRetryConfig,
        shutdown: &mut ShutdownSignal,
    ) -> Result<SavedTorrentRetrySummary, InjectionWorkerError> {
        let wait_signal = shutdown.clone();
        self.retry_saved_torrents_inner(
            config,
            || shutdown.state().phase != ShutdownPhase::Running,
            Some(&wait_signal),
        )
        .await
    }

    async fn retry_saved_torrents_inner<F>(
        &self,
        config: SavedTorrentRetryConfig,
        mut should_stop: F,
        shutdown: Option<&ShutdownSignal>,
    ) -> Result<SavedTorrentRetrySummary, InjectionWorkerError>
    where
        F: FnMut() -> bool,
    {
        let mut summary = SavedTorrentRetrySummary::default();
        if config.directories.is_empty() || config.max_saved_torrents == 0 {
            return Ok(summary);
        }

        for directory in &config.directories {
            if should_stop() {
                return Ok(summary);
            }
            let mut scan =
                saved_torrent_path_scan(directory, config.max_saved_torrents - summary.scanned);
            let mut preflight = FuturesUnordered::new();
            while let Some(path) = scan.next_path_until_stop(&mut should_stop).await? {
                if summary.scanned >= config.max_saved_torrents || should_stop() {
                    scan.cancel();
                    scan.finish().await?;
                    return Ok(summary);
                }
                summary.scanned += 1;
                preflight.push(preflight_saved_torrent_retry(
                    directory.clone(),
                    path,
                    config.clone(),
                ));
                if preflight.len() >= SAVED_TORRENT_RETRY_PREFLIGHT_CONCURRENCY
                    && let Err(error) = self
                        .drain_one_saved_torrent_preflight(
                            &mut preflight,
                            &mut summary,
                            &mut should_stop,
                            shutdown,
                        )
                        .await
                {
                    scan.cancel();
                    scan.finish().await?;
                    return Err(error);
                }
            }
            while !preflight.is_empty() {
                if should_stop() {
                    summary.kept += preflight.len();
                    preflight.clear();
                    scan.cancel();
                    scan.finish().await?;
                    return Ok(summary);
                }
                if let Err(error) = self
                    .drain_one_saved_torrent_preflight(
                        &mut preflight,
                        &mut summary,
                        &mut should_stop,
                        shutdown,
                    )
                    .await
                {
                    scan.cancel();
                    scan.finish().await?;
                    return Err(error);
                }
            }
            if should_stop() {
                scan.cancel();
                scan.finish().await?;
                return Ok(summary);
            }
            scan.finish().await?;
        }

        Ok(summary)
    }

    async fn drain_one_saved_torrent_preflight<F>(
        &self,
        preflight: &mut FuturesUnordered<
            impl Future<Output = Result<SavedTorrentPreflight, InjectionWorkerError>>,
        >,
        summary: &mut SavedTorrentRetrySummary,
        should_stop: &mut F,
        shutdown: Option<&ShutdownSignal>,
    ) -> Result<(), InjectionWorkerError>
    where
        F: FnMut() -> bool,
    {
        let result = match shutdown {
            Some(signal) => {
                if shutdown_requested(Some(signal)) {
                    summary.kept += preflight.len();
                    preflight.clear();
                    return Ok(());
                }
                let mut signal = signal.clone();
                tokio::select! {
                    biased;
                    _ = signal.cancelled() => {
                        summary.kept += preflight.len();
                        preflight.clear();
                        return Ok(());
                    }
                    result = preflight.next() => result,
                }
            }
            None => preflight.next().await,
        };
        let Some(result) = result else {
            return Ok(());
        };
        match result? {
            SavedTorrentPreflight::Ready(prepared) => {
                if should_stop() {
                    summary.kept += 1;
                    return Ok(());
                }
                self.retry_preflighted_saved_torrent(*prepared, summary, should_stop, shutdown)
                    .await
            }
            SavedTorrentPreflight::Skipped => {
                summary.skipped += 1;
                summary.kept += 1;
                Ok(())
            }
            SavedTorrentPreflight::Failed => {
                summary.failed += 1;
                summary.kept += 1;
                Ok(())
            }
        }
    }

    async fn retry_preflighted_saved_torrent<F>(
        &self,
        prepared: LoadedSavedTorrentRetry,
        summary: &mut SavedTorrentRetrySummary,
        should_stop: &mut F,
        shutdown: Option<&ShutdownSignal>,
    ) -> Result<(), InjectionWorkerError>
    where
        F: FnMut() -> bool,
    {
        let LoadedSavedTorrentRetry {
            directory,
            path,
            file_name,
            metadata,
            saved,
            candidate,
            config,
        } = prepared;
        if should_stop() {
            summary.kept += 1;
            return Ok(());
        }
        let saved_media_types = if metadata.media_type == crate::domain::MediaType::Unknown {
            Vec::new()
        } else {
            vec![metadata.media_type]
        };
        let lookups = reverse_lookup_candidates_for_media_types(
            &self.repository,
            &candidate,
            crate::content_filter::ContentFilterContext::ReverseLookup,
            &config.reverse_lookup,
            &saved_media_types,
        )
        .await
        .map_err(saved_retry_database_error)?;

        let mut attempted_match = false;
        for lookup in lookups {
            if should_stop() {
                summary.kept += 1;
                return Ok(());
            }
            let assessment = assess_and_persist_candidate(
                &self.repository,
                CandidateAssessmentInput {
                    local_item: &lookup.local_item,
                    local_files: &lookup.local_files,
                    local_files_truncated: lookup.local_files_truncated,
                    candidate: &candidate,
                    owned_info_hashes: &[],
                    assessed_at_ms: config.assessed_at_ms,
                    config: &config.reverse_lookup.assessment,
                },
            )
            .await
            .map_err(saved_retry_assessment_error)?;
            let Some((candidate_id, assessment)) = actionable_saved_assessment(assessment) else {
                continue;
            };
            if should_stop() {
                summary.kept += 1;
                return Ok(());
            }
            attempted_match = true;
            summary.attempted += 1;
            let request = InjectionRequest {
                local_item: lookup.local_item,
                local_files: lookup.local_files,
                candidate: candidate.clone(),
                candidate_id,
                metafile: saved.parsed.metafile.clone(),
                torrent_bytes: saved.bytes.clone(),
                assessment,
                assessed_at_ms: config.assessed_at_ms,
                output_dir: directory.clone(),
                link_dirs: config.link_dirs.clone(),
                link_type: config.link_type,
                flat_linking: config.flat_linking,
                recheck: config.recheck,
            };
            let result = match self
                .process_inner(request, should_stop, shutdown, true)
                .await
            {
                Ok(result) => result,
                Err(error) if saved_retry_can_continue_after_error(&error) => {
                    summary.failed += 1;
                    summary.kept += 1;
                    continue;
                }
                Err(error) => return Err(error),
            };
            record_saved_retry_result(result.outcome, summary);
            if self.dry_run {
                summary.kept += 1;
                if result.outcome == InjectionOutcome::SourceIncomplete {
                    continue;
                }
                return Ok(());
            }
            match result.outcome {
                InjectionOutcome::SourceIncomplete => {
                    summary.kept += 1;
                    continue;
                }
                InjectionOutcome::Injected | InjectionOutcome::AlreadyExists => {
                    match self
                        .delete_saved_torrent_if_complete(
                            &path,
                            &file_name,
                            &metadata.info_hash,
                            saved.identity,
                            &result,
                            shutdown,
                        )
                        .await
                    {
                        Ok(true) => summary.deleted += 1,
                        Ok(false) => summary.kept += 1,
                        Err(error) if saved_retry_can_continue_after_error(&error) => {
                            summary.failed += 1;
                            summary.kept += 1;
                        }
                        Err(error) => return Err(error),
                    }
                    return Ok(());
                }
                InjectionOutcome::DryRun => {
                    summary.kept += 1;
                    return Ok(());
                }
                InjectionOutcome::Rejected => {
                    match delete_saved_torrent(&path, saved.identity).await {
                        Ok(true) => summary.deleted += 1,
                        Ok(false) => summary.kept += 1,
                        Err(error) if saved_retry_can_continue_after_error(&error) => {
                            summary.failed += 1;
                            summary.kept += 1;
                        }
                        Err(error) => return Err(error),
                    }
                    return Ok(());
                }
                InjectionOutcome::Failed | InjectionOutcome::Saved => {
                    summary.kept += 1;
                    return Ok(());
                }
            }
        }

        if !attempted_match {
            summary.no_match += 1;
            summary.kept += 1;
        }
        Ok(())
    }

    fn select_target(&self, item: &LocalItem) -> Option<Arc<dyn InjectionClient>> {
        let preferred = match &item.source {
            crate::domain::LocalItemSource::Client { client_host, .. } => Some(client_host),
            _ => None,
        };
        if let Some(client_host) = preferred
            && let Some(client) = self.clients.iter().find(|client| {
                &client.descriptor().host == client_host && client.descriptor().can_inject()
            })
        {
            return Some(Arc::clone(client));
        }

        self.clients
            .iter()
            .find(|client| client.descriptor().can_inject())
            .cloned()
    }

    async fn find_existing_client(
        &self,
        info_hash: &InfoHash,
        target: &TorrentClientDescriptor,
        checked_at_ms: i64,
        shutdown: Option<&ShutdownSignal>,
    ) -> Result<ExistingClientLookup, InjectionWorkerError> {
        for client in &self.clients {
            if client.descriptor().host == target.host {
                continue;
            }
            match client_call_until_shutdown(shutdown, || client.has_torrent(info_hash)).await {
                ClientCall::Shutdown => return Ok(ExistingClientLookup::Shutdown),
                ClientCall::Completed(Ok(true)) => {
                    return Ok(ExistingClientLookup::Found(Arc::clone(client)));
                }
                ClientCall::Completed(Ok(false)) => {}
                ClientCall::Completed(Err(error)) => {
                    self.record_client_health(
                        client.descriptor(),
                        false,
                        Some(&error),
                        checked_at_ms,
                    )
                    .await?;
                    return Err(error.into());
                }
            }
        }
        Ok(ExistingClientLookup::NotFound)
    }

    async fn prepare_links(
        &self,
        request: &InjectionRequest,
    ) -> Result<LinkPreparation, InjectionWorkerError> {
        let Some(link_type) = request.link_type else {
            return Ok(LinkPreparation::Ready {
                save_path: source_root(&request.local_item).map(Path::to_path_buf),
                created_links: Vec::new(),
                prepared_links: Vec::new(),
                created_roots: Vec::new(),
                linked_files: 0,
            });
        };
        if request.link_dirs.is_empty() {
            return Ok(LinkPreparation::Ready {
                save_path: source_root(&request.local_item).map(Path::to_path_buf),
                created_links: Vec::new(),
                prepared_links: Vec::new(),
                created_roots: Vec::new(),
                linked_files: 0,
            });
        }
        let Some(source_root) = source_root(&request.local_item) else {
            return Ok(LinkPreparation::SourceIncomplete);
        };
        let source_root = source_root.to_path_buf();
        let link_dirs = request.link_dirs.clone();
        let tracker = request.candidate.tracker.as_str().to_owned();
        let flat_linking = request.flat_linking;
        let local_files = request.local_files.clone();
        let metafile_files = request.metafile.files().to_vec();
        let decision = request.assessment.decision;
        let join_error_path = source_root.clone();
        tokio::task::spawn_blocking(move || {
            let link_dir =
                select_link_dir_pinned(&source_root, &link_dirs, LinkDirOptions::new(link_type))?;
            let destination_dir = link_destination_dir(link_dir.path(), &tracker, flat_linking)?;
            let outcome = match link_metafile_files(
                &source_root,
                &local_files,
                &metafile_files,
                decision,
                &destination_dir,
                LinkFilesOptions::new(link_type).with_link_root(link_dir),
            ) {
                Ok(outcome) => outcome,
                Err(LinkActionError::MissingSource { .. })
                | Err(LinkActionError::NoSourceMatch { .. }) => {
                    return Ok(LinkPreparation::SourceIncomplete);
                }
                Err(error) => return Err(error),
            };

            Ok(LinkPreparation::Ready {
                save_path: Some(destination_dir),
                linked_files: outcome.created_links.len(),
                created_links: outcome.created_links,
                prepared_links: outcome.prepared_links,
                created_roots: outcome.created_roots,
            })
        })
        .await
        .map_err(|error| InjectionWorkerError::Io {
            operation: "join link preparation task",
            path: join_error_path,
            source: std::io::Error::other(error.to_string()),
        })?
        .map_err(InjectionWorkerError::Link)
    }

    async fn prepare_injection_phase(
        &self,
        request: &InjectionRequest,
        target_name: &DependencyName,
        from_saved_retry: bool,
    ) -> Result<InjectionPreparationPhase, InjectionWorkerError> {
        let recheck_plan =
            recheck_resume_plan(&request.metafile, &request.assessment, request.recheck);
        let below_threshold = is_below_resume_threshold(
            &request.metafile,
            &request.assessment,
            request.recheck,
            recheck_plan,
        );
        if below_threshold
            && request.recheck.below_threshold_action
                == BelowThresholdAction::RejectWithoutInjecting
        {
            return Ok(InjectionPreparationPhase::Rejected(InjectionWorkResult {
                outcome: InjectionOutcome::Rejected,
                target_client: Some(target_name.clone()),
                dry_run_action: None,
                saved_for_retry: false,
                linked_files: 0,
                prepared_link_cleanup_incomplete: false,
            }));
        }

        if !from_saved_retry && request.link_type.is_some() && !request.link_dirs.is_empty() {
            self.save_for_retry_phase(request).await?;
        }

        let link_result = self.prepare_links(request).await?;
        let LinkPreparation::Ready {
            save_path,
            created_links,
            prepared_links,
            created_roots,
            linked_files,
        } = link_result
        else {
            return Ok(InjectionPreparationPhase::SourceIncomplete);
        };

        let has_prepared_links = !prepared_links.is_empty();
        let recheck_after_linking = RecheckResumePlan {
            should_recheck: true,
            ..recheck_plan
        };
        let pause_for_recheck = has_prepared_links
            || (recheck_plan.should_recheck
                && !(below_threshold
                    && request.recheck.below_threshold_action
                        == BelowThresholdAction::InjectAndStart));
        let run_resume_after_inject = pause_for_recheck
            && !(below_threshold
                && request.recheck.below_threshold_action == BelowThresholdAction::InjectPaused);

        Ok(InjectionPreparationPhase::Ready(PreparedInjectionPhase {
            save_path,
            dry_run_link_destination: None,
            created_links,
            prepared_links,
            created_roots,
            linked_files,
            has_prepared_links,
            recheck_plan,
            recheck_after_linking,
            pause_for_recheck,
            run_resume_after_inject,
        }))
    }

    fn dry_run_injection_phase(
        &self,
        request: &InjectionRequest,
        target_name: &DependencyName,
    ) -> Result<InjectionPreparationPhase, InjectionWorkerError> {
        let recheck_plan =
            recheck_resume_plan(&request.metafile, &request.assessment, request.recheck);
        let below_threshold = is_below_resume_threshold(
            &request.metafile,
            &request.assessment,
            request.recheck,
            recheck_plan,
        );
        if below_threshold
            && request.recheck.below_threshold_action
                == BelowThresholdAction::RejectWithoutInjecting
        {
            return Ok(InjectionPreparationPhase::Rejected(InjectionWorkResult {
                outcome: InjectionOutcome::Rejected,
                target_client: Some(target_name.clone()),
                dry_run_action: None,
                saved_for_retry: false,
                linked_files: 0,
                prepared_link_cleanup_incomplete: false,
            }));
        }
        let has_configured_links = request.link_type.is_some() && !request.link_dirs.is_empty();
        let dry_run_link_plan = if has_configured_links {
            let Some(source_root) = source_root(&request.local_item) else {
                return Ok(InjectionPreparationPhase::SourceIncomplete);
            };
            let link_type = match request.link_type {
                Some(link_type) => link_type,
                None => return Ok(InjectionPreparationPhase::SourceIncomplete),
            };
            match plan_metafile_link_dry_run(
                source_root,
                &request.local_files,
                request.metafile.files(),
                request.assessment.decision,
                DryRunLinkOptions {
                    link_dirs: &request.link_dirs,
                    tracker: request.candidate.tracker.as_str(),
                    flat_linking: request.flat_linking,
                    link_type,
                },
            ) {
                Ok(plan) => Some(plan),
                Err(LinkActionError::MissingSource { .. })
                | Err(LinkActionError::NoSourceMatch { .. }) => {
                    return Ok(InjectionPreparationPhase::SourceIncomplete);
                }
                Err(error) => return Err(error.into()),
            }
        } else {
            None
        };
        let pause_for_recheck = has_configured_links
            || (recheck_plan.should_recheck
                && !(below_threshold
                    && request.recheck.below_threshold_action
                        == BelowThresholdAction::InjectAndStart));
        let run_resume_after_inject = pause_for_recheck
            && !(below_threshold
                && request.recheck.below_threshold_action == BelowThresholdAction::InjectPaused);

        Ok(InjectionPreparationPhase::Ready(PreparedInjectionPhase {
            save_path: dry_run_save_path(request),
            dry_run_link_destination: dry_run_link_plan
                .as_ref()
                .map(|plan| plan.destination_dir.clone()),
            created_links: Vec::new(),
            prepared_links: Vec::new(),
            created_roots: Vec::new(),
            linked_files: dry_run_link_plan
                .as_ref()
                .map(|plan| plan.linked_files)
                .unwrap_or(0),
            has_prepared_links: has_configured_links,
            recheck_plan,
            recheck_after_linking: recheck_plan,
            pause_for_recheck,
            run_resume_after_inject,
        }))
    }

    async fn run_mutation_phase<F>(
        &self,
        request: &InjectionRequest,
        target: &dyn InjectionClient,
        preparation: &PreparedInjectionPhase,
        should_stop: &mut F,
        shutdown: Option<&ShutdownSignal>,
    ) -> InjectionMutationResult
    where
        F: FnMut() -> bool,
    {
        let Some(_guard) = lock_until_shutdown(&self.mutation_lock, shutdown).await else {
            return InjectionMutationResult::SavedForShutdown;
        };
        if should_stop() {
            return InjectionMutationResult::SavedForShutdown;
        }

        match self
            .find_existing_client(
                request.metafile.info_hash(),
                target.descriptor(),
                request.assessed_at_ms,
                shutdown,
            )
            .await
        {
            Ok(ExistingClientLookup::Found(existing_client)) => {
                InjectionMutationResult::AlreadyExistsOnOtherClient(existing_client)
            }
            Ok(ExistingClientLookup::Shutdown) => InjectionMutationResult::SavedForShutdown,
            Ok(ExistingClientLookup::NotFound) => {
                match client_call_until_shutdown(shutdown, || {
                    target.has_torrent(request.metafile.info_hash())
                })
                .await
                {
                    ClientCall::Shutdown => InjectionMutationResult::SavedForShutdown,
                    ClientCall::Completed(Ok(true)) => {
                        match validate_prepared_links_for_inject(&preparation.prepared_links).await
                        {
                            Ok(()) => InjectionMutationResult::AlreadyExistsOnTarget,
                            Err(error) => InjectionMutationResult::PreparedLinksInvalid(error),
                        }
                    }
                    ClientCall::Completed(Ok(false)) => {
                        match validate_prepared_links_for_inject(&preparation.prepared_links).await
                        {
                            Ok(()) => {
                                if self.dry_run {
                                    InjectionMutationResult::DryRun
                                } else {
                                    match client_call_until_shutdown(shutdown, || {
                                        target.inject(ClientInjectionRequest {
                                            info_hash: request.metafile.info_hash(),
                                            torrent_bytes: &request.torrent_bytes,
                                            save_path: preparation.save_path.as_deref(),
                                            pause_for_recheck: preparation.pause_for_recheck,
                                        })
                                    })
                                    .await
                                    {
                                        ClientCall::Shutdown => {
                                            InjectionMutationResult::SavedForShutdown
                                        }
                                        ClientCall::Completed(result) => {
                                            InjectionMutationResult::Injected(result)
                                        }
                                    }
                                }
                            }
                            Err(error) => InjectionMutationResult::PreparedLinksInvalid(error),
                        }
                    }
                    ClientCall::Completed(Err(error)) => {
                        InjectionMutationResult::TargetPrecheckFailed(error)
                    }
                }
            }
            Err(error) => InjectionMutationResult::ExistingLookupFailed(error),
        }
    }

    async fn save_for_retry(&self, request: &InjectionRequest) -> Result<(), InjectionWorkerError> {
        let metadata = candidate_output_metadata(
            request.local_item.media_type,
            &request.candidate,
            &request.metafile,
        );
        let output_dir = request.output_dir.clone();
        let torrent_bytes = request.torrent_bytes.clone();
        tokio::task::spawn_blocking(move || {
            save_candidate_torrent(&output_dir, &metadata, &torrent_bytes)
        })
        .await
        .map_err(|error| InjectionWorkerError::Io {
            operation: "join saved torrent write task",
            path: request.output_dir.clone(),
            source: std::io::Error::other(error.to_string()),
        })?
        .map(|_| ())
        .map_err(InjectionWorkerError::Save)
    }

    async fn save_for_retry_phase(
        &self,
        request: &InjectionRequest,
    ) -> Result<RetrySavePhaseResult, InjectionWorkerError> {
        self.save_for_retry(request).await?;
        Ok(RetrySavePhaseResult::Saved)
    }

    async fn record_client_health(
        &self,
        descriptor: &TorrentClientDescriptor,
        healthy: bool,
        error: Option<&TorrentClientError>,
        checked_at_ms: i64,
    ) -> Result<(), DatabaseError> {
        let name = dependency_name(descriptor).map_err(|error| DatabaseError::QueryFailed {
            operation: "build client dependency name".to_owned(),
            message: format!("{error:?}"),
        })?;
        let state = if healthy {
            DependencyState::Healthy { checked_at_ms }
        } else {
            let reason = ReasonText::new(
                error
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "torrent client injection failed".to_owned()),
            )
            .map_err(|error| DatabaseError::QueryFailed {
                operation: "build client health reason".to_owned(),
                message: error.to_string(),
            })?;
            match error.map(ClassifyFailure::failure_class) {
                Some(FailureClass::UserActionRequired | FailureClass::FatalLocal) => {
                    DependencyState::Unavailable {
                        reason,
                        retry_after_ms: None,
                    }
                }
                _ => DependencyState::Degraded {
                    reason,
                    retry_after_ms: error.and_then(TorrentClientError::retry_after_ms),
                },
            }
        };
        self.repository
            .record_dependency_health(DependencyKind::TorrentClient, &name, &state, checked_at_ms)
            .await
    }

    async fn delete_saved_torrent_if_complete(
        &self,
        path: &Path,
        file_name: &str,
        info_hash: &InfoHash,
        identity: SavedTorrentIdentity,
        result: &InjectionWorkResult,
        shutdown: Option<&ShutdownSignal>,
    ) -> Result<bool, InjectionWorkerError> {
        if !matches!(
            result.outcome,
            InjectionOutcome::Injected | InjectionOutcome::AlreadyExists
        ) || result.saved_for_retry
            || parse_torrent_output_filename(file_name)
                .map(|metadata| metadata.cached)
                .unwrap_or(true)
        {
            return Ok(false);
        }
        let Some(client_name) = result.target_client.as_ref() else {
            return Ok(false);
        };
        let Some(client) = self.client_by_dependency_name(client_name) else {
            return Ok(false);
        };
        let checking =
            match client_call_until_shutdown(shutdown, || client.is_checking(info_hash)).await {
                ClientCall::Shutdown => return Ok(false),
                ClientCall::Completed(result) => result?,
            };
        let remaining = match client_call_until_shutdown(shutdown, || {
            client.remaining_bytes(info_hash)
        })
        .await
        {
            ClientCall::Shutdown => return Ok(false),
            ClientCall::Completed(result) => result?,
        };
        if checking || remaining.get() > 0 {
            return Ok(false);
        }
        delete_saved_torrent(path, identity).await
    }

    fn client_by_dependency_name(
        &self,
        name: &DependencyName,
    ) -> Option<&Arc<dyn InjectionClient>> {
        self.clients
            .iter()
            .find(|client| client.descriptor().host.as_str() == name.as_str())
    }

    async fn run_recheck_resume(
        &self,
        client: &dyn InjectionClient,
        request: &InjectionRequest,
        plan: RecheckResumePlan,
        shutdown: Option<&ShutdownSignal>,
    ) -> Result<ResumeLoopOutcome, InjectionWorkerError> {
        if !plan.should_recheck {
            return Ok(ResumeLoopOutcome::NotRequired);
        }
        {
            let Some(_guard) = lock_until_shutdown(&self.mutation_lock, shutdown).await else {
                return Ok(ResumeLoopOutcome::StillChecking);
            };
            match client_call_until_shutdown(shutdown, || {
                client.recheck(request.metafile.info_hash())
            })
            .await
            {
                ClientCall::Shutdown => return Ok(ResumeLoopOutcome::StillChecking),
                ClientCall::Completed(result) => result?,
            }
        }
        let max_polls = max_resume_polls(request.recheck);
        let resume_deadline = Instant::now()
            .checked_add(Duration::from_millis(request.recheck.max_resume_wait_ms))
            .unwrap_or_else(Instant::now);
        for _ in 0..max_polls {
            let checking = match client_call_until_shutdown(shutdown, || {
                client.is_checking(request.metafile.info_hash())
            })
            .await
            {
                ClientCall::Shutdown => return Ok(ResumeLoopOutcome::StillChecking),
                ClientCall::Completed(result) => result?,
            };
            if checking {
                if Instant::now() >= resume_deadline {
                    return Ok(ResumeLoopOutcome::StillChecking);
                }
                if sleep_between_resume_polls(request.recheck, resume_deadline, shutdown).await {
                    return Ok(ResumeLoopOutcome::StillChecking);
                }
                continue;
            }
            let remaining = match client_call_until_shutdown(shutdown, || {
                client.remaining_bytes(request.metafile.info_hash())
            })
            .await
            {
                ClientCall::Shutdown => return Ok(ResumeLoopOutcome::StillChecking),
                ClientCall::Completed(result) => result?,
            };
            if can_resume_with_remaining(
                &request.metafile,
                &request.assessment,
                request.recheck,
                plan,
                remaining,
            ) {
                let Some(_guard) = lock_until_shutdown(&self.mutation_lock, shutdown).await else {
                    return Ok(ResumeLoopOutcome::StillChecking);
                };
                match client_call_until_shutdown(shutdown, || {
                    client.resume(request.metafile.info_hash())
                })
                .await
                {
                    ClientCall::Shutdown => return Ok(ResumeLoopOutcome::StillChecking),
                    ClientCall::Completed(result) => result?,
                }
                return Ok(ResumeLoopOutcome::Resumed);
            }
            return Ok(ResumeLoopOutcome::WaitingForCompletion);
        }
        Ok(ResumeLoopOutcome::StillChecking)
    }
}

async fn client_call_until_shutdown<T, MakeFuture, Fut>(
    shutdown: Option<&ShutdownSignal>,
    make_future: MakeFuture,
) -> ClientCall<T>
where
    MakeFuture: FnOnce() -> Fut,
    Fut: Future<Output = Result<T, TorrentClientError>>,
{
    let Some(shutdown) = shutdown else {
        return ClientCall::Completed(make_future().await);
    };
    if shutdown_requested(Some(shutdown)) {
        return ClientCall::Shutdown;
    }
    let mut shutdown = shutdown.clone();
    tokio::select! {
        biased;
        _ = shutdown.cancelled() => ClientCall::Shutdown,
        result = make_future() => ClientCall::Completed(result),
    }
}

async fn lock_until_shutdown<'a>(
    mutex: &'a Mutex<()>,
    shutdown: Option<&ShutdownSignal>,
) -> Option<MutexGuard<'a, ()>> {
    let Some(shutdown) = shutdown else {
        return Some(mutex.lock().await);
    };
    if shutdown_requested(Some(shutdown)) {
        return None;
    }
    let mut shutdown = shutdown.clone();
    tokio::select! {
        biased;
        _ = shutdown.cancelled() => None,
        guard = mutex.lock() => Some(guard),
    }
}

fn shutdown_requested(shutdown: Option<&ShutdownSignal>) -> bool {
    shutdown.is_some_and(|signal| signal.state().phase != ShutdownPhase::Running)
}

fn cleanup_prepared_links(links: &[CreatedLink], roots: &[CreatedRoot]) -> bool {
    if let Err(error) = cleanup_created_links_and_roots(links, roots) {
        warn!(
            link_count = links.len(),
            root_count = roots.len(),
            error = %error,
            "left prepared injection links in place after cleanup was unsafe"
        );
        true
    } else {
        false
    }
}

async fn validate_prepared_links_for_inject(links: &[PreparedLink]) -> Result<(), LinkActionError> {
    if links.is_empty() {
        return Ok(());
    }
    let links = links.to_vec();
    tokio::task::spawn_blocking(move || validate_prepared_links(&links))
        .await
        .map_err(|error| LinkActionError::Io {
            operation: "join prepared link revalidation task",
            path: PathBuf::new(),
            source: std::io::Error::other(error.to_string()),
        })?
}

enum ClientCall<T> {
    Completed(Result<T, TorrentClientError>),
    Shutdown,
}

enum ExistingClientLookup {
    Found(Arc<dyn InjectionClient>),
    NotFound,
    Shutdown,
}

enum SavedTorrentPreflight {
    Ready(Box<LoadedSavedTorrentRetry>),
    Skipped,
    Failed,
}

struct LoadedSavedTorrentRetry {
    directory: PathBuf,
    path: PathBuf,
    file_name: String,
    metadata: TorrentOutputMetadata,
    saved: SavedTorrentFile,
    candidate: RemoteCandidate,
    config: SavedTorrentRetryConfig,
}

async fn preflight_saved_torrent_retry(
    directory: PathBuf,
    path: PathBuf,
    config: SavedTorrentRetryConfig,
) -> Result<SavedTorrentPreflight, InjectionWorkerError> {
    let Some(file_name) = path
        .file_name()
        .and_then(|value| value.to_str())
        .map(str::to_owned)
    else {
        return Ok(SavedTorrentPreflight::Skipped);
    };
    let metadata = match parse_torrent_output_filename(&file_name) {
        Ok(metadata) if !metadata.cached => metadata,
        Ok(_) | Err(_) => return Ok(SavedTorrentPreflight::Skipped),
    };
    let saved = match read_saved_torrent(&path).await {
        Ok(saved) if saved.parsed.metafile.info_hash() == &metadata.info_hash => saved,
        Ok(_) => return Ok(SavedTorrentPreflight::Failed),
        Err(InjectionWorkerError::Io { .. } | InjectionWorkerError::TorrentParse(_)) => {
            return Ok(SavedTorrentPreflight::Failed);
        }
        Err(error) => return Err(error),
    };
    let candidate = saved_remote_candidate(&metadata, &path)?;
    Ok(SavedTorrentPreflight::Ready(Box::new(
        LoadedSavedTorrentRetry {
            directory,
            path,
            file_name,
            metadata,
            saved,
            candidate,
            config,
        },
    )))
}

struct SavedTorrentPathScan {
    directory: PathBuf,
    receiver: mpsc::Receiver<Result<PathBuf, InjectionWorkerError>>,
    join: Option<tokio::task::JoinHandle<()>>,
    cancelled: Arc<AtomicBool>,
}

#[derive(Clone, Default)]
struct SavedTorrentScanProgress {
    #[cfg(test)]
    send_attempts: Arc<AtomicUsize>,
    #[cfg(test)]
    send_attempted: Arc<tokio::sync::Notify>,
}

impl SavedTorrentScanProgress {
    #[cfg(test)]
    fn new() -> Self {
        Self {
            send_attempts: Arc::new(AtomicUsize::new(0)),
            send_attempted: Arc::new(tokio::sync::Notify::new()),
        }
    }

    #[cfg(test)]
    async fn wait_for_send_attempts(&self, expected: usize) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if self.send_attempts.load(Ordering::SeqCst) >= expected {
                    return;
                }
                self.send_attempted.notified().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {expected} saved torrent scan sends"));
    }

    fn before_send(&self) {
        #[cfg(test)]
        {
            self.send_attempts.fetch_add(1, Ordering::SeqCst);
            self.send_attempted.notify_waiters();
        }
    }
}

impl SavedTorrentPathScan {
    #[cfg(test)]
    async fn next_path(&mut self) -> Result<Option<PathBuf>, InjectionWorkerError> {
        let message = self.receiver.recv().await;
        self.handle_scan_message(message).await
    }

    async fn next_path_until_stop<F>(
        &mut self,
        should_stop: &mut F,
    ) -> Result<Option<PathBuf>, InjectionWorkerError>
    where
        F: FnMut() -> bool,
    {
        loop {
            if should_stop() {
                self.cancel();
                self.finish().await?;
                return Ok(None);
            }
            tokio::select! {
                message = self.receiver.recv() => {
                    return self.handle_scan_message(message).await;
                }
                () = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
        }
    }

    async fn handle_scan_message(
        &mut self,
        message: Option<Result<PathBuf, InjectionWorkerError>>,
    ) -> Result<Option<PathBuf>, InjectionWorkerError> {
        match message {
            Some(Ok(path)) => Ok(Some(path)),
            Some(Err(error)) => {
                self.finish().await?;
                Err(error)
            }
            None => {
                self.finish().await?;
                Ok(None)
            }
        }
    }

    fn cancel(&mut self) {
        self.cancelled.store(true, Ordering::Relaxed);
        self.receiver.close();
    }

    async fn finish(&mut self) -> Result<(), InjectionWorkerError> {
        if let Some(join) = self.join.take() {
            join.await.map_err(|source| InjectionWorkerError::Io {
                operation: "join saved torrent scan",
                path: self.directory.clone(),
                source: std::io::Error::other(source),
            })?;
        }
        Ok(())
    }
}

fn saved_torrent_path_scan(directory: &Path, limit: usize) -> SavedTorrentPathScan {
    saved_torrent_path_scan_with_progress(directory, limit, None)
}

fn saved_torrent_path_scan_with_progress(
    directory: &Path,
    limit: usize,
    progress: Option<SavedTorrentScanProgress>,
) -> SavedTorrentPathScan {
    let directory = directory.to_path_buf();
    let blocking_directory = directory.clone();
    let (sender, receiver) = mpsc::channel(SAVED_TORRENT_SCAN_BATCH);
    let cancelled = Arc::new(AtomicBool::new(false));
    let blocking_cancelled = Arc::clone(&cancelled);
    let join = tokio::task::spawn_blocking(move || {
        if limit == 0 {
            return;
        }
        let entries = match std::fs::read_dir(&blocking_directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
            Err(source) => {
                drop(sender.blocking_send(Err(InjectionWorkerError::Io {
                    operation: "read saved torrent directory",
                    path: blocking_directory,
                    source,
                })));
                return;
            }
        };
        let mut batch = Vec::with_capacity(SAVED_TORRENT_SCAN_BATCH);
        let mut sent = 0_usize;
        for entry in entries {
            if blocking_cancelled.load(Ordering::Relaxed) {
                return;
            }
            let entry = match entry {
                Ok(entry) => entry,
                Err(source) => {
                    drop(sender.blocking_send(Err(InjectionWorkerError::Io {
                        operation: "read saved torrent directory entry",
                        path: blocking_directory.clone(),
                        source,
                    })));
                    return;
                }
            };
            let path = entry.path();
            if is_direct_saved_torrent_file(&blocking_directory, &path) {
                batch.push(path);
                if batch.len() >= SAVED_TORRENT_SCAN_BATCH {
                    if !send_saved_torrent_scan_batch(
                        &sender,
                        &mut batch,
                        limit,
                        &mut sent,
                        progress.as_ref(),
                    ) {
                        return;
                    }
                    if blocking_cancelled.load(Ordering::Relaxed) {
                        return;
                    }
                }
                if sent + batch.len() >= limit {
                    break;
                }
            }
        }
        send_saved_torrent_scan_batch(&sender, &mut batch, limit, &mut sent, progress.as_ref());
    });

    SavedTorrentPathScan {
        directory,
        receiver,
        join: Some(join),
        cancelled,
    }
}

fn send_saved_torrent_scan_batch(
    sender: &mpsc::Sender<Result<PathBuf, InjectionWorkerError>>,
    batch: &mut Vec<PathBuf>,
    limit: usize,
    sent: &mut usize,
    progress: Option<&SavedTorrentScanProgress>,
) -> bool {
    batch.sort();
    for path in batch.drain(..) {
        if *sent >= limit {
            return true;
        }
        if let Some(progress) = progress {
            progress.before_send();
        }
        if sender.blocking_send(Ok(path)).is_err() {
            return false;
        }
        *sent += 1;
    }
    true
}

async fn read_saved_torrent(path: &Path) -> Result<SavedTorrentFile, InjectionWorkerError> {
    let path = path.to_path_buf();
    let blocking_path = path.clone();
    tokio::task::spawn_blocking(move || {
        #[cfg(test)]
        let _read_tracker = SavedTorrentReadTracker::enter(&blocking_path);
        let file =
            open_saved_torrent_file(&blocking_path).map_err(|source| InjectionWorkerError::Io {
                operation: "open saved torrent",
                path: blocking_path.clone(),
                source,
            })?;
        let identity = saved_torrent_identity(&file.metadata().map_err(|source| {
            InjectionWorkerError::Io {
                operation: "read saved torrent metadata",
                path: blocking_path.clone(),
                source,
            }
        })?);
        let mut reader = file.take(MAX_SAVED_TORRENT_BYTES + 1);
        let mut bytes = Vec::new();
        reader
            .read_to_end(&mut bytes)
            .map_err(|source| InjectionWorkerError::Io {
                operation: "read saved torrent",
                path: blocking_path.clone(),
                source,
            })?;
        if bytes.len() > usize::try_from(MAX_SAVED_TORRENT_BYTES).unwrap_or(usize::MAX) {
            return Err(InjectionWorkerError::Io {
                operation: "read saved torrent",
                path: blocking_path.clone(),
                source: std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "saved torrent exceeds maximum size",
                ),
            });
        }
        let parsed = parse_metafile(&bytes).map_err(InjectionWorkerError::TorrentParse)?;
        Ok(SavedTorrentFile {
            bytes,
            parsed,
            identity,
        })
    })
    .await
    .map_err(|source| InjectionWorkerError::Io {
        operation: "join saved torrent read",
        path: path.to_path_buf(),
        source: std::io::Error::other(source),
    })?
}

#[cfg(test)]
struct SavedTorrentReadTracker;

#[cfg(test)]
impl SavedTorrentReadTracker {
    fn enter(path: &Path) -> Option<Self> {
        let delay = SAVED_TORRENT_READ_DELAY_MS.load(Ordering::SeqCst);
        if delay == 0 || !path.to_string_lossy().contains("saved-retry-prefetch") {
            return None;
        }
        let active = SAVED_TORRENT_READ_IN_FLIGHT.fetch_add(1, Ordering::SeqCst) + 1;
        update_saved_torrent_read_max(active);
        std::thread::sleep(Duration::from_millis(delay));
        Some(Self)
    }
}

#[cfg(test)]
impl Drop for SavedTorrentReadTracker {
    fn drop(&mut self) {
        SAVED_TORRENT_READ_IN_FLIGHT.fetch_sub(1, Ordering::SeqCst);
    }
}

#[cfg(test)]
fn update_saved_torrent_read_max(active: usize) {
    let mut current = SAVED_TORRENT_READ_MAX_IN_FLIGHT.load(Ordering::SeqCst);
    while active > current {
        match SAVED_TORRENT_READ_MAX_IN_FLIGHT.compare_exchange(
            current,
            active,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            Ok(_) => return,
            Err(next) => current = next,
        }
    }
}

#[derive(Debug)]
struct SavedTorrentFile {
    bytes: Vec<u8>,
    parsed: crate::torrent::ParsedMetafile,
    identity: SavedTorrentIdentity,
}

#[derive(Debug, Clone, Copy)]
struct SavedTorrentIdentity {
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(unix)]
    size: u64,
    #[cfg(unix)]
    ctime: i64,
    #[cfg(unix)]
    ctime_nsec: i64,
    #[cfg(windows)]
    volume_serial_number: Option<u32>,
    #[cfg(windows)]
    file_index: Option<u64>,
    #[cfg(windows)]
    file_size: u64,
    #[cfg(windows)]
    last_write_time: u64,
}

fn saved_torrent_identity(metadata: &std::fs::Metadata) -> SavedTorrentIdentity {
    SavedTorrentIdentity {
        #[cfg(unix)]
        device: metadata.dev(),
        #[cfg(unix)]
        inode: metadata.ino(),
        #[cfg(unix)]
        size: metadata.len(),
        #[cfg(unix)]
        ctime: metadata.ctime(),
        #[cfg(unix)]
        ctime_nsec: metadata.ctime_nsec(),
        #[cfg(windows)]
        volume_serial_number: metadata.volume_serial_number(),
        #[cfg(windows)]
        file_index: metadata.file_index(),
        #[cfg(windows)]
        file_size: metadata.file_size(),
        #[cfg(windows)]
        last_write_time: metadata.last_write_time(),
    }
}

impl SavedTorrentIdentity {
    fn matches(self, metadata: &std::fs::Metadata) -> bool {
        #[cfg(unix)]
        {
            self.device == metadata.dev()
                && self.inode == metadata.ino()
                && self.size == metadata.len()
                && self.ctime == metadata.ctime()
                && self.ctime_nsec == metadata.ctime_nsec()
        }
        #[cfg(windows)]
        {
            self.volume_serial_number == metadata.volume_serial_number()
                && self.file_index == metadata.file_index()
                && self.file_size == metadata.file_size()
                && self.last_write_time == metadata.last_write_time()
        }
        #[cfg(not(any(unix, windows)))]
        {
            let _metadata = metadata;
            true
        }
    }
}

fn open_saved_torrent_file(path: &Path) -> std::io::Result<File> {
    #[cfg(unix)]
    {
        let mut options = OpenOptions::new();
        options.read(true);
        options.custom_flags(libc::O_NOFOLLOW);
        let file = options.open(path)?;
        validate_regular_file(&file.metadata()?)?;
        Ok(file)
    }
    #[cfg(not(unix))]
    {
        validate_regular_file(&std::fs::symlink_metadata(path)?)?;
        let file = File::open(path)?;
        validate_regular_file(&file.metadata()?)?;
        Ok(file)
    }
}

fn validate_regular_file(metadata: &std::fs::Metadata) -> std::io::Result<()> {
    if metadata.file_type().is_file() {
        Ok(())
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "saved torrent path is not a regular file",
        ))
    }
}

async fn delete_saved_torrent(
    path: &Path,
    identity: SavedTorrentIdentity,
) -> Result<bool, InjectionWorkerError> {
    let path = path.to_path_buf();
    let blocking_path = path.clone();
    tokio::task::spawn_blocking(move || {
        let metadata = match std::fs::symlink_metadata(&blocking_path) {
            Ok(metadata) => metadata,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(source) => {
                return Err(InjectionWorkerError::Io {
                    operation: "read completed saved torrent metadata",
                    path: blocking_path.clone(),
                    source,
                });
            }
        };
        if !metadata.file_type().is_file() || !identity.matches(&metadata) {
            return Ok(false);
        }
        match std::fs::remove_file(&blocking_path) {
            Ok(()) => {}
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(source) => {
                return Err(InjectionWorkerError::Io {
                    operation: "delete completed saved torrent",
                    path: blocking_path.clone(),
                    source,
                });
            }
        }
        Ok(true)
    })
    .await
    .map_err(|source| InjectionWorkerError::Io {
        operation: "join saved torrent delete",
        path: path.to_path_buf(),
        source: std::io::Error::other(source),
    })?
}

fn is_direct_saved_torrent_file(directory: &Path, path: &Path) -> bool {
    path.parent() == Some(directory)
        && path
            .file_name()
            .and_then(|value| value.to_str())
            .is_some_and(|name| {
                parse_torrent_output_filename(name)
                    .map(|metadata| !metadata.cached)
                    .unwrap_or(false)
            })
        && std::fs::symlink_metadata(path)
            .map(|metadata| metadata.file_type().is_file())
            .unwrap_or(false)
}

fn saved_remote_candidate(
    metadata: &TorrentOutputMetadata,
    path: &Path,
) -> Result<RemoteCandidate, InjectionWorkerError> {
    let indexer_id = IndexerId::new(1).map_err(saved_retry_domain_error)?;
    let guid = CandidateGuid::new(format!("saved-{}", metadata.info_hash.as_str()))
        .map_err(saved_retry_domain_error)?;
    let download_url =
        DownloadUrl::new(format!("file://{}", path.display())).map_err(saved_retry_domain_error)?;
    let title = ItemTitle::new(metadata.name.clone()).map_err(saved_retry_domain_error)?;
    let tracker = TrackerName::new(metadata.tracker.clone()).map_err(saved_retry_domain_error)?;
    Ok(RemoteCandidate {
        id: None,
        indexer_id,
        guid,
        download_url,
        title,
        tracker,
        size: None,
        published_at_ms: None,
        info_hash: Some(metadata.info_hash.clone()),
        torrent_cache_path: Some(path.to_path_buf()),
    })
}

fn actionable_saved_assessment(
    assessment: PersistedCandidateAssessment,
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
            Some((candidate_id, assessment))
        }
        _ => None,
    }
}

fn record_saved_retry_result(outcome: InjectionOutcome, summary: &mut SavedTorrentRetrySummary) {
    match outcome {
        InjectionOutcome::Injected => summary.injected += 1,
        InjectionOutcome::DryRun => summary.dry_run += 1,
        InjectionOutcome::AlreadyExists => summary.already_exists += 1,
        InjectionOutcome::SourceIncomplete => summary.source_incomplete += 1,
        InjectionOutcome::Rejected => summary.no_match += 1,
        InjectionOutcome::Failed => summary.failed += 1,
        InjectionOutcome::Saved => summary.kept += 1,
    }
}

fn saved_retry_can_continue_after_error(error: &InjectionWorkerError) -> bool {
    matches!(
        error,
        InjectionWorkerError::Client(_)
            | InjectionWorkerError::ClientWithPreparedLinkCleanup { .. }
            | InjectionWorkerError::TorrentParse(_)
            | InjectionWorkerError::Save(_)
            | InjectionWorkerError::Link(_)
            | InjectionWorkerError::NoWritableClient
    )
}

fn saved_retry_database_error(error: crate::matching::ReverseLookupError) -> InjectionWorkerError {
    match error {
        crate::matching::ReverseLookupError::Database { source } => {
            InjectionWorkerError::Database(source)
        }
        crate::matching::ReverseLookupError::Assessment { source } => {
            saved_retry_assessment_error(source)
        }
    }
}

fn saved_retry_assessment_error(
    error: crate::matching::CandidateAssessmentError,
) -> InjectionWorkerError {
    match error {
        crate::matching::CandidateAssessmentError::Database { source } => {
            InjectionWorkerError::Database(source)
        }
        crate::matching::CandidateAssessmentError::MissingLocalItemId => {
            InjectionWorkerError::MissingLocalItemId
        }
    }
}

fn saved_retry_domain_error(error: crate::domain::DomainError) -> InjectionWorkerError {
    InjectionWorkerError::Database(DatabaseError::QueryFailed {
        operation: "build saved torrent candidate".to_owned(),
        message: error.to_string(),
    })
}

enum LinkPreparation {
    Ready {
        save_path: Option<PathBuf>,
        created_links: Vec<CreatedLink>,
        prepared_links: Vec<PreparedLink>,
        created_roots: Vec<CreatedRoot>,
        linked_files: usize,
    },
    SourceIncomplete,
}

enum InjectionPreparationPhase {
    Ready(PreparedInjectionPhase),
    Rejected(InjectionWorkResult),
    SourceIncomplete,
}

struct PreparedInjectionPhase {
    save_path: Option<PathBuf>,
    dry_run_link_destination: Option<PathBuf>,
    created_links: Vec<CreatedLink>,
    prepared_links: Vec<PreparedLink>,
    created_roots: Vec<CreatedRoot>,
    linked_files: usize,
    has_prepared_links: bool,
    recheck_plan: RecheckResumePlan,
    recheck_after_linking: RecheckResumePlan,
    pause_for_recheck: bool,
    run_resume_after_inject: bool,
}

impl PreparedInjectionPhase {
    fn cleanup_prepared_links_phase(&self) -> CleanupPhaseResult {
        CleanupPhaseResult {
            prepared_link_cleanup_incomplete: cleanup_prepared_links(
                &self.created_links,
                &self.created_roots,
            ),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum RetrySavePhaseResult {
    Saved,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct CleanupPhaseResult {
    prepared_link_cleanup_incomplete: bool,
}

enum InjectionMutationResult {
    SavedForShutdown,
    AlreadyExistsOnTarget,
    AlreadyExistsOnOtherClient(Arc<dyn InjectionClient>),
    Injected(Result<(), TorrentClientError>),
    DryRun,
    PreparedLinksInvalid(LinkActionError),
    ExistingLookupFailed(InjectionWorkerError),
    TargetPrecheckFailed(TorrentClientError),
}

pub fn injection_queue(
    capacity: std::num::NonZeroUsize,
) -> (
    BoundedWorkQueue<InjectionRequest>,
    WorkReceiver<InjectionRequest>,
) {
    bounded_work_queue(QueueKind::Injection, capacity)
}

fn source_root(item: &LocalItem) -> Option<&Path> {
    item.save_path.as_deref().or(item.path.as_deref())
}

pub fn recheck_resume_plan(
    metafile: &TorrentMetafile,
    assessment: &CandidateAssessment,
    config: RecheckResumeConfig,
) -> RecheckResumePlan {
    let partial = assessment.decision == MatchDecision::Partial;
    let video_disc = has_video_disc_files(metafile);
    let should_recheck = !config.skip_recheck || partial || video_disc;
    let max_remaining_bytes = if partial && !video_disc {
        config.auto_resume_max_download
    } else {
        ByteSize::new(0)
    };

    RecheckResumePlan {
        should_recheck,
        max_remaining_bytes,
        min_completion_percent: (partial && !video_disc)
            .then_some(config.min_completion_percent)
            .flatten(),
        max_remaining_percent: (partial && !video_disc)
            .then_some(config.max_remaining_percent)
            .flatten(),
    }
}

pub fn can_resume_with_remaining(
    metafile: &TorrentMetafile,
    assessment: &CandidateAssessment,
    config: RecheckResumeConfig,
    plan: RecheckResumePlan,
    remaining: ByteSize,
) -> bool {
    if remaining.get() <= plan.max_remaining_bytes.get() {
        return true;
    }
    if can_resume_with_percentage(metafile.total_size(), remaining, plan) {
        return true;
    }
    if !config.ignore_non_relevant_files_to_resume
        || assessment.decision != MatchDecision::Partial
        || has_video_disc_files(metafile)
        || remaining.get() > config.non_relevant_max_remaining.get()
    {
        return false;
    }

    let Some(piece_slack) = metafile
        .piece_length()
        .unwrap_or(ByteSize::new(0))
        .get()
        .checked_mul(config.piece_slack_multiplier)
    else {
        return false;
    };
    let Some(irrelevant_file_bytes) = irrelevant_file_bytes(metafile) else {
        return false;
    };
    let Some(allowed_slack) = irrelevant_file_bytes.get().checked_add(piece_slack) else {
        return false;
    };
    remaining.get() <= allowed_slack
}

fn is_below_resume_threshold(
    metafile: &TorrentMetafile,
    assessment: &CandidateAssessment,
    config: RecheckResumeConfig,
    plan: RecheckResumePlan,
) -> bool {
    if assessment.decision != MatchDecision::Partial || has_video_disc_files(metafile) {
        return false;
    }

    let Some(matched_size) = assessment.matched_size else {
        return true;
    };
    let remaining = ByteSize::new(
        metafile
            .total_size()
            .get()
            .saturating_sub(matched_size.get()),
    );
    !can_resume_with_remaining(metafile, assessment, config, plan, remaining)
}

fn can_resume_with_percentage(
    total_size: ByteSize,
    remaining: ByteSize,
    plan: RecheckResumePlan,
) -> bool {
    if plan.min_completion_percent.is_none() && plan.max_remaining_percent.is_none() {
        return false;
    }
    if total_size.get() == 0 {
        return remaining.get() == 0;
    }

    let remaining_percent = remaining.get() as f64 * 100.0 / total_size.get() as f64;
    let completion_percent = 100.0 - remaining_percent;

    plan.min_completion_percent
        .is_some_and(|minimum| completion_percent >= minimum)
        || plan
            .max_remaining_percent
            .is_some_and(|maximum| remaining_percent <= maximum)
}

fn has_video_disc_files(metafile: &TorrentMetafile) -> bool {
    metafile.files().iter().any(|file| {
        file.relative_path
            .extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| {
                matches!(
                    extension.to_ascii_lowercase().as_str(),
                    "m2ts" | "ifo" | "vob" | "bup"
                )
            })
    })
}

fn irrelevant_file_bytes(metafile: &TorrentMetafile) -> Option<ByteSize> {
    checked_file_total(
        metafile
            .files()
            .iter()
            .filter(|file| is_irrelevant_file(&file.relative_path))
            .map(|file| file.size),
        "irrelevant file total",
    )
    .ok()
}

fn is_irrelevant_file(path: &Path) -> bool {
    let normalized = path.to_string_lossy().to_ascii_lowercase();
    let has_keyword = ["sample", "trailer", "extras", "bonus"]
        .iter()
        .any(|keyword| normalized.contains(keyword));
    let has_extension = path
        .extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| {
            matches!(
                extension.to_ascii_lowercase().as_str(),
                "nfo" | "srr" | "srt" | "txt" | "ass"
            )
        });
    has_keyword || has_extension
}

fn max_resume_polls(config: RecheckResumeConfig) -> u64 {
    let interval = config.poll_interval_ms.max(1);
    config.max_resume_wait_ms.div_ceil(interval).max(1)
}

async fn sleep_between_resume_polls(
    config: RecheckResumeConfig,
    deadline: Instant,
    shutdown: Option<&ShutdownSignal>,
) -> bool {
    let Some(sleep_duration) = resume_poll_sleep_duration(config, Instant::now(), deadline) else {
        return false;
    };
    let sleep = tokio::time::sleep(sleep_duration);
    let Some(shutdown) = shutdown else {
        sleep.await;
        return false;
    };
    let mut shutdown = shutdown.clone();
    tokio::select! {
        () = sleep => false,
        _ = shutdown.cancelled() => true,
    }
}

fn resume_poll_sleep_duration(
    config: RecheckResumeConfig,
    now: Instant,
    deadline: Instant,
) -> Option<Duration> {
    if config.poll_interval_ms == 0 {
        return None;
    }
    let remaining = deadline.saturating_duration_since(now);
    if remaining.is_zero() {
        return None;
    }
    Some(Duration::from_millis(config.poll_interval_ms).min(remaining))
}

fn dependency_name(
    descriptor: &TorrentClientDescriptor,
) -> Result<DependencyName, InjectionWorkerError> {
    descriptor.dependency_name().map_err(|error| {
        InjectionWorkerError::Database(DatabaseError::QueryFailed {
            operation: "build client dependency name".to_owned(),
            message: error.to_string(),
        })
    })
}

fn dry_run_save_path(request: &InjectionRequest) -> Option<PathBuf> {
    if request.link_type.is_some() && !request.link_dirs.is_empty() {
        None
    } else {
        source_root(&request.local_item).map(Path::to_path_buf)
    }
}

fn dry_run_inject_action(
    request: &InjectionRequest,
    preparation: Option<&PreparedInjectionPhase>,
) -> DryRunAction {
    DryRunAction::InjectTorrent {
        save_path: preparation
            .and_then(|preparation| preparation.save_path.clone())
            .or_else(|| dry_run_save_path(request)),
        planned_link_destination: preparation
            .and_then(|preparation| preparation.dry_run_link_destination.clone()),
        link_type: request.link_type,
        link_dir_count: request.link_dirs.len(),
        flat_linking: request.flat_linking,
        pause_for_recheck: preparation
            .map(|preparation| preparation.pause_for_recheck)
            .unwrap_or(false),
        would_recheck: preparation
            .map(|preparation| preparation.run_resume_after_inject)
            .unwrap_or(false),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::future::pending;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::clients::TorrentClientCapabilities;
    use crate::domain::{
        ByteSize, CandidateGuid, ClientHost, DisplayName, DownloadUrl, FileIndex, ItemTitle,
        LocalItemSource, MatchRatio, MediaType, SourceKey, TrackerName,
    };
    use crate::inventory::{InventoryScanOptions, ScannedLocalItem};
    use crate::inventory_refresh::{ClientInventoryItem, ClientInventoryMessage};
    use crate::persistence::repository::Repository;
    use crate::runtime::shutdown::ShutdownController;
    use tokio::sync::Barrier;

    static SAVED_TORRENT_READ_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    #[tokio::test]
    async fn worker_checks_other_clients_before_mutating_target() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("injection-existing");
        let target = Arc::new(FakeClient::new(descriptor("target", "target")));
        let existing = Arc::new(FakeClient::new(descriptor("other", "other")).with_existing(true));
        let (local, candidate, candidate_id) = persisted_inputs(&repository, &root).await;
        let worker = InjectionWorker::new(
            repository.clone(),
            vec![
                target.clone() as Arc<dyn InjectionClient>,
                existing.clone() as Arc<dyn InjectionClient>,
            ],
        );

        let result = worker
            .process(request(local, candidate, candidate_id, &root))
            .await
            .unwrap();
        let health = repository.dependency_health_snapshot(10).await.unwrap();

        assert_eq!(InjectionOutcome::AlreadyExists, result.outcome);
        assert_eq!(0, target.inject_calls.load(Ordering::SeqCst));
        assert_eq!(1, existing.has_calls.load(Ordering::SeqCst));
        assert_eq!("healthy", health[0].state);
    }

    #[tokio::test]
    async fn worker_rechecks_other_clients_while_mutation_is_locked() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("injection-cross-client-race");
        let first = Arc::new(
            FakeClient::new(descriptor("first", "first"))
                .with_existing_after_inject()
                .with_first_has_barrier(Arc::new(Barrier::new(2))),
        );
        let second = Arc::new(
            FakeClient::new(descriptor("second", "second"))
                .with_existing_after_inject()
                .with_first_has_barrier(first.first_has_barrier.as_ref().unwrap().clone()),
        );
        let (local, candidate, candidate_id) = persisted_inputs(&repository, &root).await;
        let worker = Arc::new(InjectionWorker::new(
            repository,
            vec![
                first.clone() as Arc<dyn InjectionClient>,
                second.clone() as Arc<dyn InjectionClient>,
            ],
        ));
        let mut first_request = request(local.clone(), candidate.clone(), candidate_id, &root);
        first_request.local_item.source = LocalItemSource::Client {
            client_host: first.descriptor.host.clone(),
            source_key: SourceKey::new("first-source").unwrap(),
        };
        first_request.recheck = skip_recheck_config();
        let mut second_request = request(local, candidate, candidate_id, &root);
        second_request.local_item.source = LocalItemSource::Client {
            client_host: second.descriptor.host.clone(),
            source_key: SourceKey::new("second-source").unwrap(),
        };
        second_request.recheck = skip_recheck_config();

        let first_worker = worker.clone();
        let first_handle = tokio::spawn(async move { first_worker.process(first_request).await });
        let second_worker = worker.clone();
        let second_handle =
            tokio::spawn(async move { second_worker.process(second_request).await });
        let (first_result, second_result) = tokio::join!(first_handle, second_handle);
        let outcomes = [
            first_result.unwrap().unwrap().outcome,
            second_result.unwrap().unwrap().outcome,
        ];

        assert_eq!(
            1,
            first.inject_calls.load(Ordering::SeqCst) + second.inject_calls.load(Ordering::SeqCst)
        );
        assert!(outcomes.contains(&InjectionOutcome::Injected));
        assert!(outcomes.contains(&InjectionOutcome::AlreadyExists));
    }

    #[tokio::test]
    async fn worker_records_client_health_when_has_torrent_fails() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("injection-has-error");
        let target =
            Arc::new(FakeClient::new(descriptor("target", "qbit.local")).with_has_errors(1));
        let (local, candidate, candidate_id) = persisted_inputs(&repository, &root).await;
        let worker = InjectionWorker::new(
            repository.clone(),
            vec![target.clone() as Arc<dyn InjectionClient>],
        );

        let error = worker
            .process(request(local, candidate, candidate_id, &root))
            .await
            .unwrap_err();
        let health = repository.dependency_health_snapshot(10).await.unwrap();

        assert!(matches!(error, InjectionWorkerError::Client(_)));
        assert_eq!(1, target.has_calls.load(Ordering::SeqCst));
        assert_eq!(0, target.inject_calls.load(Ordering::SeqCst));
        assert_eq!("degraded", health[0].state);
        assert_eq!("qbit.local", health[0].dependency_name.as_str());
        assert_eq!(Some(1_000), health[0].retry_after_ms);
    }

    #[tokio::test]
    async fn worker_dry_run_validates_target_without_injecting() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("injection-dry-run");
        let target = Arc::new(FakeClient::new(descriptor("target", "target")));
        let (local, candidate, candidate_id) = persisted_inputs(&repository, &root).await;
        let worker = InjectionWorker::new(
            repository.clone(),
            vec![target.clone() as Arc<dyn InjectionClient>],
        )
        .with_dry_run(true);

        let result = worker
            .process(request(local, candidate, candidate_id, &root))
            .await
            .unwrap();
        let health = repository.dependency_health_snapshot(10).await.unwrap();

        assert_eq!(InjectionOutcome::DryRun, result.outcome);
        assert_eq!(
            Some(DependencyName::new("target").unwrap()),
            result.target_client
        );
        assert_eq!(
            Some(DryRunAction::InjectTorrent {
                save_path: Some(root.clone()),
                planned_link_destination: None,
                link_type: None,
                link_dir_count: 0,
                flat_linking: false,
                pause_for_recheck: true,
                would_recheck: true,
            }),
            result.dry_run_action
        );
        assert_eq!(1, target.has_calls.load(Ordering::SeqCst));
        assert_eq!(0, target.inject_calls.load(Ordering::SeqCst));
        assert_eq!(0, target.recheck_calls.load(Ordering::SeqCst));
        assert_eq!(0, target.resume_calls.load(Ordering::SeqCst));
        assert_eq!("healthy", health[0].state);
    }

    #[tokio::test]
    async fn worker_dry_run_skips_link_preparation_and_retry_save() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("injection-dry-run-link");
        fs::write(root.join("movie.mkv"), b"0123456789").unwrap();
        let link_dir = root.join("links");
        fs::create_dir_all(&link_dir).unwrap();
        let target = Arc::new(FakeClient::new(descriptor("target", "target")));
        let (local, candidate, candidate_id) = persisted_inputs(&repository, &root).await;
        let mut request = request(local, candidate, candidate_id, &root);
        request.link_type = Some(LinkType::Hardlink);
        request.link_dirs = vec![link_dir.clone()];
        let worker =
            InjectionWorker::new(repository, vec![target.clone() as Arc<dyn InjectionClient>])
                .with_dry_run(true);

        let result = worker.process(request).await.unwrap();

        assert_eq!(InjectionOutcome::DryRun, result.outcome);
        assert_eq!(
            Some(DryRunAction::InjectTorrent {
                save_path: None,
                planned_link_destination: Some(link_dir.join("tracker.example")),
                link_type: Some(LinkType::Hardlink),
                link_dir_count: 1,
                flat_linking: false,
                pause_for_recheck: true,
                would_recheck: true,
            }),
            result.dry_run_action
        );
        assert_eq!(1, result.linked_files);
        assert!(!result.prepared_link_cleanup_incomplete);
        assert_eq!(0, target.inject_calls.load(Ordering::SeqCst));
        assert_eq!(
            0,
            target
                .save_path_file_exists_at_inject
                .load(Ordering::SeqCst)
        );
        assert!(!link_dir.join("tracker.example/movie.mkv").exists());
        assert!(!root.join("output").exists());
    }

    #[tokio::test]
    async fn worker_dry_run_reports_source_incomplete_without_retry_save() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("injection-dry-run-missing-source");
        let link_dir = root.join("links");
        fs::create_dir_all(&link_dir).unwrap();
        let target = Arc::new(FakeClient::new(descriptor("target", "target")));
        let (local, candidate, candidate_id) = persisted_inputs(&repository, &root).await;
        let mut request = request(local, candidate, candidate_id, &root);
        request.link_type = Some(LinkType::Hardlink);
        request.link_dirs = vec![link_dir.clone()];
        let worker =
            InjectionWorker::new(repository, vec![target.clone() as Arc<dyn InjectionClient>])
                .with_dry_run(true);

        let result = worker.process(request).await.unwrap();

        assert_eq!(InjectionOutcome::SourceIncomplete, result.outcome);
        assert!(!result.saved_for_retry);
        assert_eq!(0, target.inject_calls.load(Ordering::SeqCst));
        assert_eq!(0, target.recheck_calls.load(Ordering::SeqCst));
        assert_eq!(0, target.resume_calls.load(Ordering::SeqCst));
        assert!(!root.join("output").exists());
        assert_eq!(0, saved_torrent_count(&root.join("output")));
    }

    #[tokio::test]
    async fn worker_dry_run_rejects_missing_link_dir_without_retry_save() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("injection-dry-run-missing-link-dir");
        fs::write(root.join("movie.mkv"), b"0123456789").unwrap();
        let target = Arc::new(FakeClient::new(descriptor("target", "target")));
        let (local, candidate, candidate_id) = persisted_inputs(&repository, &root).await;
        let mut request = request(local, candidate, candidate_id, &root);
        request.link_type = Some(LinkType::Hardlink);
        request.link_dirs = vec![root.join("missing-links")];
        let worker =
            InjectionWorker::new(repository, vec![target.clone() as Arc<dyn InjectionClient>])
                .with_dry_run(true);

        let error = worker.process(request).await.unwrap_err();

        assert!(matches!(error, InjectionWorkerError::Link(_)));
        assert_eq!(0, target.inject_calls.load(Ordering::SeqCst));
        assert_eq!(0, target.recheck_calls.load(Ordering::SeqCst));
        assert_eq!(0, target.resume_calls.load(Ordering::SeqCst));
        assert!(!root.join("output").exists());
    }

    #[tokio::test]
    async fn worker_dry_run_rejects_mismatched_existing_link_destination() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("injection-dry-run-destination-mismatch");
        fs::write(root.join("movie.mkv"), b"0123456789").unwrap();
        let link_dir = root.join("links");
        fs::create_dir_all(link_dir.join("tracker.example")).unwrap();
        fs::write(link_dir.join("tracker.example/movie.mkv"), b"abcdefghij").unwrap();
        let target = Arc::new(FakeClient::new(descriptor("target", "target")));
        let (local, candidate, candidate_id) = persisted_inputs(&repository, &root).await;
        let mut request = request(local, candidate, candidate_id, &root);
        request.link_type = Some(LinkType::Hardlink);
        request.link_dirs = vec![link_dir];
        let worker =
            InjectionWorker::new(repository, vec![target.clone() as Arc<dyn InjectionClient>])
                .with_dry_run(true);

        let error = worker.process(request).await.unwrap_err();

        assert!(matches!(error, InjectionWorkerError::Link(_)));
        assert_eq!(0, target.inject_calls.load(Ordering::SeqCst));
        assert_eq!(0, target.recheck_calls.load(Ordering::SeqCst));
        assert_eq!(0, target.resume_calls.load(Ordering::SeqCst));
        assert!(!root.join("output").exists());
    }

    #[tokio::test]
    async fn worker_dry_run_existing_target_does_not_recheck_or_resume() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("injection-dry-run-existing-target");
        fs::write(root.join("movie.mkv"), b"0123456789").unwrap();
        let link_dir = root.join("links");
        fs::create_dir_all(&link_dir).unwrap();
        let target = Arc::new(FakeClient::new(descriptor("target", "target")).with_existing(true));
        let (local, candidate, candidate_id) = persisted_inputs(&repository, &root).await;
        let mut request = request(local, candidate, candidate_id, &root);
        request.link_type = Some(LinkType::Hardlink);
        request.link_dirs = vec![link_dir];
        let worker =
            InjectionWorker::new(repository, vec![target.clone() as Arc<dyn InjectionClient>])
                .with_dry_run(true);

        let result = worker.process(request).await.unwrap();

        assert_eq!(InjectionOutcome::AlreadyExists, result.outcome);
        assert_eq!(1, target.has_calls.load(Ordering::SeqCst));
        assert_eq!(0, target.inject_calls.load(Ordering::SeqCst));
        assert_eq!(0, target.recheck_calls.load(Ordering::SeqCst));
        assert_eq!(0, target.resume_calls.load(Ordering::SeqCst));
        assert_eq!(0, saved_torrent_count(&root.join("output")));
    }

    #[tokio::test]
    async fn worker_dry_run_reports_below_threshold_start_policy() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("injection-dry-run-start-policy");
        let target = Arc::new(FakeClient::new(descriptor("target", "target")));
        let (local, candidate, candidate_id) = persisted_inputs(&repository, &root).await;
        let mut request = request(local, candidate, candidate_id, &root);
        request.metafile = metafile_with_files(&[("movie.mkv", 20)]);
        request.assessment = CandidateAssessment {
            decision: MatchDecision::Partial,
            reason: crate::domain::DecisionReason::PartialOverlap,
            matched_size: Some(ByteSize::new(10)),
            matched_ratio: Some(MatchRatio::new(0.5).unwrap()),
        };
        request.recheck = RecheckResumeConfig {
            below_threshold_action: BelowThresholdAction::InjectAndStart,
            ..RecheckResumeConfig::default()
        };
        let worker =
            InjectionWorker::new(repository, vec![target.clone() as Arc<dyn InjectionClient>])
                .with_dry_run(true);

        let result = worker.process(request).await.unwrap();

        assert_eq!(
            Some(DryRunAction::InjectTorrent {
                save_path: Some(root.clone()),
                planned_link_destination: None,
                link_type: None,
                link_dir_count: 0,
                flat_linking: false,
                pause_for_recheck: false,
                would_recheck: false,
            }),
            result.dry_run_action
        );
        assert_eq!(0, target.inject_calls.load(Ordering::SeqCst));
        assert_eq!(0, target.recheck_calls.load(Ordering::SeqCst));
        assert_eq!(0, target.resume_calls.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn client_inventory_refresh_continues_after_one_client_fails() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let refresh_worker =
            InventoryRefreshWorker::new(repository.clone(), InventoryScanOptions::default());
        let failing = Arc::new(FakeRefreshClient::failing(descriptor("failing", "failing")));
        let successful = Arc::new(FakeRefreshClient::successful(
            descriptor("successful", "successful"),
            2,
        ));
        let worker = InjectionWorker::new(
            repository,
            vec![
                failing.clone() as Arc<dyn InjectionClient>,
                successful.clone() as Arc<dyn InjectionClient>,
            ],
        );

        let summaries = worker
            .refresh_client_inventories(&refresh_worker)
            .await
            .unwrap();

        assert_eq!(1, summaries.len());
        assert_eq!(2, summaries[0].persisted_items);
        assert_eq!(1, failing.calls.load(Ordering::SeqCst));
        assert_eq!(1, successful.calls.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn client_inventory_refreshes_multiple_clients_with_bounded_concurrency() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let refresh_worker =
            InventoryRefreshWorker::new(repository.clone(), InventoryScanOptions::default());
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_in_flight = Arc::new(AtomicUsize::new(0));
        let first = Arc::new(
            FakeRefreshClient::delayed_successful(
                descriptor("first", "first"),
                1,
                Duration::from_millis(100),
            )
            .with_in_flight_tracking(in_flight.clone(), max_in_flight.clone()),
        );
        let second = Arc::new(
            FakeRefreshClient::delayed_successful(
                descriptor("second", "second"),
                2,
                Duration::from_millis(100),
            )
            .with_in_flight_tracking(in_flight.clone(), max_in_flight.clone()),
        );
        let worker = InjectionWorker::new(
            repository,
            vec![
                first.clone() as Arc<dyn InjectionClient>,
                second.clone() as Arc<dyn InjectionClient>,
            ],
        );

        let summaries = worker
            .refresh_client_inventories(&refresh_worker)
            .await
            .unwrap();

        assert_eq!(2, summaries.len());
        assert_eq!(1, summaries[0].persisted_items);
        assert_eq!(2, summaries[1].persisted_items);
        assert_eq!(1, first.calls.load(Ordering::SeqCst));
        assert_eq!(1, second.calls.load(Ordering::SeqCst));
        assert_eq!(2, max_in_flight.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn client_inventory_refresh_streams_multiple_clients_through_staging_pool() {
        let root = unique_temp_dir("client-inventory-concurrent-staging");
        let database = root.join("sporos.db");
        let repository = Repository::connect(&database).await.unwrap();
        let virtual_refreshes = Arc::new(AtomicUsize::new(0));
        let refresh_worker =
            InventoryRefreshWorker::new(repository.clone(), InventoryScanOptions::default())
                .with_virtual_refresh_attempts(virtual_refreshes.clone());
        let first_host = ClientHost::new("first").unwrap();
        let second_host = ClientHost::new("second").unwrap();
        let first = Arc::new(FakeRefreshClient::streaming(
            descriptor("first", first_host.as_str()),
            client_inventory_items(first_host.clone(), "1", 80),
        ));
        let second = Arc::new(FakeRefreshClient::streaming(
            descriptor("second", second_host.as_str()),
            client_inventory_items(second_host.clone(), "2", 80),
        ));
        let worker = InjectionWorker::new(
            repository.clone(),
            vec![
                first.clone() as Arc<dyn InjectionClient>,
                second.clone() as Arc<dyn InjectionClient>,
            ],
        );

        let summaries = tokio::time::timeout(
            Duration::from_secs(2),
            worker.refresh_client_inventories(&refresh_worker),
        )
        .await
        .unwrap()
        .unwrap();

        let first_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM local_items WHERE source_key LIKE ?")
                .bind("%:first:%")
                .fetch_one(repository.pool())
                .await
                .unwrap();
        let second_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM local_items WHERE source_key LIKE ?")
                .bind("%:second:%")
                .fetch_one(repository.pool())
                .await
                .unwrap();

        assert_eq!(2, summaries.len());
        assert_eq!(80, summaries[0].persisted_items);
        assert_eq!(80, summaries[1].persisted_items);
        assert_eq!(1, virtual_refreshes.load(Ordering::SeqCst));
        assert_eq!(80, first_count);
        assert_eq!(80, second_count);
        repository.pool().close().await;
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn client_inventory_refresh_does_not_start_more_clients_after_shutdown() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let refresh_worker =
            InventoryRefreshWorker::new(repository.clone(), InventoryScanOptions::default());
        let clients = (0..(CLIENT_INVENTORY_REFRESH_CONCURRENCY + 1))
            .map(|index| {
                Arc::new(FakeRefreshClient::delayed_successful(
                    descriptor(&format!("client-{index}"), &format!("client-{index}")),
                    1,
                    Duration::from_millis(100),
                ))
            })
            .collect::<Vec<_>>();
        let worker = InjectionWorker::new(
            repository,
            clients
                .iter()
                .cloned()
                .map(|client| client as Arc<dyn InjectionClient>)
                .collect(),
        );
        let (controller, shutdown) = shutdown_channel();
        let refresh = tokio::spawn(async move {
            worker
                .refresh_client_inventories_until_shutdown(&refresh_worker, shutdown)
                .await
        });

        wait_for_calls(&clients[CLIENT_INVENTORY_REFRESH_CONCURRENCY - 1].calls, 1).await;
        controller.cancel_now("test shutdown").unwrap();
        let error = refresh.await.unwrap().unwrap_err();

        let InventoryRefreshError::Client {
            source: TorrentClientError::Cancelled { client, .. },
        } = error
        else {
            panic!("expected client cancellation error");
        };
        assert_eq!(
            format!("client-{CLIENT_INVENTORY_REFRESH_CONCURRENCY}"),
            client
        );
        assert_eq!(
            0,
            clients[CLIENT_INVENTORY_REFRESH_CONCURRENCY]
                .calls
                .load(Ordering::SeqCst)
        );
    }

    #[tokio::test]
    async fn client_inventory_refresh_completed_batch_stays_successful_after_shutdown() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let refresh_worker =
            InventoryRefreshWorker::new(repository.clone(), InventoryScanOptions::default());
        let (controller, shutdown) = shutdown_channel();
        let client = Arc::new(
            FakeRefreshClient::successful(descriptor("client", "client"), 1)
                .with_completion_shutdown(controller),
        );
        let worker =
            InjectionWorker::new(repository, vec![client.clone() as Arc<dyn InjectionClient>]);

        let summaries = worker
            .refresh_client_inventories_until_shutdown(&refresh_worker, shutdown)
            .await
            .unwrap();

        assert_eq!(1, summaries.len());
        assert_eq!(1, summaries[0].persisted_items);
        assert_eq!(1, client.calls.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn client_inventory_refresh_rebuilds_virtual_seasons_once_per_batch() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let virtual_refreshes = Arc::new(AtomicUsize::new(0));
        let refresh_worker =
            InventoryRefreshWorker::new(repository.clone(), InventoryScanOptions::default())
                .with_virtual_refresh_attempts(virtual_refreshes.clone());
        let first_host = ClientHost::new("first").unwrap();
        let second_host = ClientHost::new("second").unwrap();
        let first = Arc::new(FakeRefreshClient::persisting(
            descriptor("first", first_host.as_str()),
            vec![client_episode(
                first_host,
                "0123456789abcdef0123456789abcdef01234561",
                "Client Show S01E01",
                "client-e01.mkv",
            )],
        ));
        let second = Arc::new(FakeRefreshClient::persisting(
            descriptor("second", second_host.as_str()),
            vec![
                client_episode(
                    second_host.clone(),
                    "0123456789abcdef0123456789abcdef01234562",
                    "Client Show S01E02",
                    "client-e02.mkv",
                ),
                client_episode(
                    second_host,
                    "0123456789abcdef0123456789abcdef01234563",
                    "Client Show S01E03",
                    "client-e03.mkv",
                ),
            ],
        ));
        let worker = InjectionWorker::new(
            repository.clone(),
            vec![
                first.clone() as Arc<dyn InjectionClient>,
                second.clone() as Arc<dyn InjectionClient>,
            ],
        );

        let summaries = worker
            .refresh_client_inventories(&refresh_worker)
            .await
            .unwrap();

        assert_eq!(2, summaries.len());
        assert_eq!(1, virtual_refreshes.load(Ordering::SeqCst));
        assert_virtual_season(&repository, "Client Show S01", 3).await;
        assert_eq!(1, first.calls.load(Ordering::SeqCst));
        assert_eq!(1, second.calls.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn client_inventory_refresh_finalizes_partial_batch_before_cancel() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let virtual_refreshes = Arc::new(AtomicUsize::new(0));
        let refresh_worker =
            InventoryRefreshWorker::new(repository.clone(), InventoryScanOptions::default())
                .with_virtual_refresh_attempts(virtual_refreshes.clone());
        let first_host = ClientHost::new("first").unwrap();
        let first = Arc::new(FakeRefreshClient::persisting(
            descriptor("first", first_host.as_str()),
            vec![
                client_episode(
                    first_host.clone(),
                    "0123456789abcdef0123456789abcdef01234561",
                    "Old Show S01E01",
                    "old-e01.mkv",
                ),
                client_episode(
                    first_host.clone(),
                    "0123456789abcdef0123456789abcdef01234562",
                    "Old Show S01E02",
                    "old-e02.mkv",
                ),
                client_episode(
                    first_host,
                    "0123456789abcdef0123456789abcdef01234563",
                    "Old Show S01E03",
                    "old-e03.mkv",
                ),
            ],
        ));
        let second = Arc::new(FakeRefreshClient::cancelled(descriptor("second", "second")));
        let worker = InjectionWorker::new(
            repository.clone(),
            vec![
                first.clone() as Arc<dyn InjectionClient>,
                second.clone() as Arc<dyn InjectionClient>,
            ],
        );

        let error = worker
            .refresh_client_inventories(&refresh_worker)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            InventoryRefreshError::Client {
                source: TorrentClientError::Cancelled { .. }
            }
        ));
        assert_eq!(1, virtual_refreshes.load(Ordering::SeqCst));
        assert_virtual_season(&repository, "Old Show S01", 3).await;
    }

    #[tokio::test]
    async fn worker_saves_for_retry_when_link_source_is_incomplete() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("injection-incomplete");
        let target = Arc::new(FakeClient::new(descriptor("target", "target")));
        let (mut local, candidate, candidate_id) = persisted_inputs(&repository, &root).await;
        local.path = None;
        local.save_path = None;
        let mut request = request(local, candidate, candidate_id, &root);
        request.link_type = Some(LinkType::Hardlink);
        request.link_dirs = vec![root.join("links")];
        fs::create_dir_all(&request.link_dirs[0]).unwrap();
        let worker =
            InjectionWorker::new(repository, vec![target.clone() as Arc<dyn InjectionClient>]);

        let result = worker.process(request).await.unwrap();

        assert_eq!(InjectionOutcome::SourceIncomplete, result.outcome);
        assert!(result.saved_for_retry);
        assert_eq!(0, target.inject_calls.load(Ordering::SeqCst));
        assert_eq!(1, saved_torrent_count(&root.join("output")));
    }

    #[tokio::test]
    async fn worker_links_before_inject_and_cleans_up_after_failure() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("injection-failure");
        fs::write(root.join("movie.mkv"), b"0123456789").unwrap();
        let target = Arc::new(FakeClient::new(descriptor("target", "target")).with_inject_error());
        let (local, candidate, candidate_id) = persisted_inputs(&repository, &root).await;
        let mut request = request(local, candidate, candidate_id, &root);
        request.link_type = Some(LinkType::Hardlink);
        request.link_dirs = vec![root.join("links")];
        fs::create_dir_all(&request.link_dirs[0]).unwrap();
        let worker = InjectionWorker::new(
            repository.clone(),
            vec![target.clone() as Arc<dyn InjectionClient>],
        );

        let result = worker.process(request).await.unwrap();
        let health = repository.dependency_health_snapshot(10).await.unwrap();

        assert_eq!(InjectionOutcome::Failed, result.outcome);
        assert!(result.saved_for_retry);
        assert!(!result.prepared_link_cleanup_incomplete);
        assert_eq!(1, target.inject_calls.load(Ordering::SeqCst));
        assert!(!root.join("links/tracker.example/movie.mkv").exists());
        assert_eq!(1, saved_torrent_count(&root.join("output")));
        assert_eq!("degraded", health[0].state);
    }

    #[tokio::test]
    async fn worker_cleans_links_when_retry_save_fails_after_client_failure() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("injection-failure-save");
        let output_dir = root.join("output");
        fs::write(root.join("movie.mkv"), b"0123456789").unwrap();
        let target = Arc::new(
            FakeClient::new(descriptor("target", "target"))
                .with_inject_error()
                .with_replace_path_with_file_on_inject(output_dir.clone(), b"not a dir".to_vec()),
        );
        let (local, candidate, candidate_id) = persisted_inputs(&repository, &root).await;
        let mut request = request(local, candidate, candidate_id, &root);
        request.link_type = Some(LinkType::Hardlink);
        request.link_dirs = vec![root.join("links")];
        fs::create_dir_all(&request.link_dirs[0]).unwrap();
        let worker =
            InjectionWorker::new(repository, vec![target.clone() as Arc<dyn InjectionClient>]);

        let error = worker.process(request).await.unwrap_err();

        assert!(matches!(error, InjectionWorkerError::Save(_)));
        assert_eq!(1, target.inject_calls.load(Ordering::SeqCst));
        assert!(output_dir.is_file());
        assert!(!root.join("links/tracker.example/movie.mkv").exists());
    }

    #[tokio::test]
    async fn worker_cleans_links_when_locked_other_client_recheck_fails() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("injection-other-recheck-failure");
        fs::write(root.join("movie.mkv"), b"0123456789").unwrap();
        let target = Arc::new(FakeClient::new(descriptor("target", "target")));
        let other = Arc::new(
            FakeClient::new(descriptor("other", "other"))
                .with_has_errors(1)
                .with_has_successes_before_error(1),
        );
        let (local, candidate, candidate_id) = persisted_inputs(&repository, &root).await;
        let mut request = request(local, candidate, candidate_id, &root);
        request.link_type = Some(LinkType::Hardlink);
        request.link_dirs = vec![root.join("links")];
        fs::create_dir_all(&request.link_dirs[0]).unwrap();
        let worker = InjectionWorker::new(
            repository.clone(),
            vec![
                target.clone() as Arc<dyn InjectionClient>,
                other.clone() as Arc<dyn InjectionClient>,
            ],
        );

        let error = worker.process(request).await.unwrap_err();
        let health = repository.dependency_health_snapshot(10).await.unwrap();

        assert!(matches!(error, InjectionWorkerError::Client(_)));
        assert_eq!(2, other.has_calls.load(Ordering::SeqCst));
        assert_eq!(0, target.inject_calls.load(Ordering::SeqCst));
        assert!(!root.join("links/tracker.example/movie.mkv").exists());
        assert_eq!(1, health.len());
        assert_eq!("other", health[0].dependency_name.as_str());
        assert_eq!("degraded", health[0].state);
    }

    #[tokio::test]
    async fn worker_revalidates_prepared_links_before_injecting() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("injection-link-replaced-before-inject");
        fs::write(root.join("movie.mkv"), b"0123456789").unwrap();
        let stale_destination = root.join("links/tracker.example/movie.mkv");
        let target = Arc::new(
            FakeClient::new(descriptor("target", "target"))
                .with_replace_save_path_file_on_has(stale_destination.clone(), b"stale".to_vec()),
        );
        let (local, candidate, candidate_id) = persisted_inputs(&repository, &root).await;
        let mut request = request(local, candidate, candidate_id, &root);
        request.link_type = Some(LinkType::Hardlink);
        request.link_dirs = vec![root.join("links")];
        fs::create_dir_all(&request.link_dirs[0]).unwrap();
        let worker =
            InjectionWorker::new(repository, vec![target.clone() as Arc<dyn InjectionClient>]);

        let error = worker.process(request).await.unwrap_err();

        assert!(matches!(
            error,
            InjectionWorkerError::Link(
                LinkActionError::ExistingDestinationMismatch { .. }
                    | LinkActionError::CleanupIncomplete { .. }
            )
        ));
        assert_eq!(1, target.has_calls.load(Ordering::SeqCst));
        assert_eq!(0, target.inject_calls.load(Ordering::SeqCst));
        assert_eq!(1, saved_torrent_count(&root.join("output")));
        assert_eq!(b"stale", fs::read(stale_destination).unwrap().as_slice());
    }

    #[tokio::test]
    async fn worker_revalidates_prepared_links_before_existing_recheck() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("injection-existing-link-replaced");
        fs::write(root.join("movie.mkv"), b"0123456789").unwrap();
        let stale_destination = root.join("links/tracker.example/movie.mkv");
        let target = Arc::new(
            FakeClient::new(descriptor("target", "target"))
                .with_existing(true)
                .with_replace_save_path_file_on_has(stale_destination.clone(), b"stale".to_vec()),
        );
        let (local, candidate, candidate_id) = persisted_inputs(&repository, &root).await;
        let mut request = request(local, candidate, candidate_id, &root);
        request.link_type = Some(LinkType::Hardlink);
        request.link_dirs = vec![root.join("links")];
        fs::create_dir_all(&request.link_dirs[0]).unwrap();
        let worker =
            InjectionWorker::new(repository, vec![target.clone() as Arc<dyn InjectionClient>]);

        let error = worker.process(request).await.unwrap_err();

        assert!(matches!(
            error,
            InjectionWorkerError::Link(
                LinkActionError::ExistingDestinationMismatch { .. }
                    | LinkActionError::CleanupIncomplete { .. }
            )
        ));
        assert_eq!(1, target.has_calls.load(Ordering::SeqCst));
        assert_eq!(0, target.inject_calls.load(Ordering::SeqCst));
        assert_eq!(0, target.recheck_calls.load(Ordering::SeqCst));
        assert_eq!(0, target.resume_calls.load(Ordering::SeqCst));
        assert_eq!(1, saved_torrent_count(&root.join("output")));
        assert_eq!(b"stale", fs::read(stale_destination).unwrap().as_slice());
    }

    #[tokio::test]
    async fn worker_rechecks_target_after_linking_before_injecting() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("injection-target-exists");
        fs::write(root.join("movie.mkv"), b"0123456789").unwrap();
        let target = Arc::new(FakeClient::new(descriptor("target", "target")).with_existing(true));
        let (local, candidate, candidate_id) = persisted_inputs(&repository, &root).await;
        let mut request = request(local, candidate, candidate_id, &root);
        request.link_type = Some(LinkType::Hardlink);
        request.link_dirs = vec![root.join("links")];
        request.recheck = RecheckResumeConfig {
            poll_interval_ms: 0,
            ..RecheckResumeConfig::default()
        };
        fs::create_dir_all(&request.link_dirs[0]).unwrap();
        let worker =
            InjectionWorker::new(repository, vec![target.clone() as Arc<dyn InjectionClient>]);

        let result = worker.process(request).await.unwrap();

        assert_eq!(InjectionOutcome::AlreadyExists, result.outcome);
        assert_eq!(1, result.linked_files);
        assert_eq!(0, target.inject_calls.load(Ordering::SeqCst));
        assert_eq!(1, target.has_calls.load(Ordering::SeqCst));
        assert_eq!(1, target.recheck_calls.load(Ordering::SeqCst));
        assert_eq!(1, target.resume_calls.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn worker_saves_partial_success_for_recheck_without_retrying_inject() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("injection-partial");
        let target = Arc::new(FakeClient::new(descriptor("target", "target")));
        let (local, candidate, candidate_id) = persisted_inputs(&repository, &root).await;
        let mut request = request(local, candidate, candidate_id, &root);
        request.assessment = CandidateAssessment {
            decision: MatchDecision::Partial,
            reason: crate::domain::DecisionReason::PartialOverlap,
            matched_size: Some(ByteSize::new(5)),
            matched_ratio: Some(MatchRatio::new(0.5).unwrap()),
        };
        request.recheck = RecheckResumeConfig {
            auto_resume_max_download: ByteSize::new(10),
            poll_interval_ms: 0,
            ..RecheckResumeConfig::default()
        };
        let worker =
            InjectionWorker::new(repository, vec![target.clone() as Arc<dyn InjectionClient>]);

        let result = worker.process(request).await.unwrap();

        assert_eq!(InjectionOutcome::Injected, result.outcome);
        assert!(result.saved_for_retry);
        assert_eq!(1, target.inject_calls.load(Ordering::SeqCst));
        assert_eq!(1, target.recheck_calls.load(Ordering::SeqCst));
        assert_eq!(1, target.resume_calls.load(Ordering::SeqCst));
        assert_eq!(1, saved_torrent_count(&root.join("output")));
        assert_eq!(Some(true), target.last_pause_for_recheck());
    }

    #[tokio::test]
    async fn worker_rejects_below_threshold_without_client_mutation() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("injection-reject-below-threshold");
        let target = Arc::new(FakeClient::new(descriptor("target", "target")));
        let (local, candidate, candidate_id) = persisted_inputs(&repository, &root).await;
        let mut request = request(local, candidate, candidate_id, &root);
        request.metafile = metafile_with_files(&[("movie.mkv", 20)]);
        request.assessment = CandidateAssessment {
            decision: MatchDecision::Partial,
            reason: crate::domain::DecisionReason::PartialOverlap,
            matched_size: Some(ByteSize::new(10)),
            matched_ratio: Some(MatchRatio::new(0.5).unwrap()),
        };
        request.recheck = RecheckResumeConfig {
            below_threshold_action: BelowThresholdAction::RejectWithoutInjecting,
            ..RecheckResumeConfig::default()
        };
        let worker =
            InjectionWorker::new(repository, vec![target.clone() as Arc<dyn InjectionClient>]);

        let result = worker.process(request).await.unwrap();

        assert_eq!(InjectionOutcome::Rejected, result.outcome);
        assert!(!result.saved_for_retry);
        assert_eq!(0, target.has_calls.load(Ordering::SeqCst));
        assert_eq!(0, target.inject_calls.load(Ordering::SeqCst));
        assert_eq!(0, target.recheck_calls.load(Ordering::SeqCst));
        assert_eq!(0, target.resume_calls.load(Ordering::SeqCst));
        assert_eq!(0, saved_torrent_count(&root.join("output")));
    }

    #[tokio::test]
    async fn worker_rejects_linked_below_threshold_without_retry_checkpoint() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("injection-link-reject-below-threshold");
        fs::write(root.join("movie.mkv"), b"0123456789").unwrap();
        let target = Arc::new(FakeClient::new(descriptor("target", "target")));
        let (local, candidate, candidate_id) = persisted_inputs(&repository, &root).await;
        let mut request = request(local, candidate, candidate_id, &root);
        request.metafile = metafile_with_files(&[("movie.mkv", 20)]);
        request.assessment = CandidateAssessment {
            decision: MatchDecision::Partial,
            reason: crate::domain::DecisionReason::PartialOverlap,
            matched_size: Some(ByteSize::new(10)),
            matched_ratio: Some(MatchRatio::new(0.5).unwrap()),
        };
        request.link_type = Some(LinkType::Hardlink);
        request.link_dirs = vec![root.join("links")];
        fs::create_dir_all(&request.link_dirs[0]).unwrap();
        request.recheck = RecheckResumeConfig {
            below_threshold_action: BelowThresholdAction::RejectWithoutInjecting,
            ..RecheckResumeConfig::default()
        };
        let worker =
            InjectionWorker::new(repository, vec![target.clone() as Arc<dyn InjectionClient>]);

        let result = worker.process(request).await.unwrap();

        assert_eq!(InjectionOutcome::Rejected, result.outcome);
        assert!(!result.saved_for_retry);
        assert_eq!(0, result.linked_files);
        assert_eq!(0, target.has_calls.load(Ordering::SeqCst));
        assert_eq!(0, target.inject_calls.load(Ordering::SeqCst));
        assert_eq!(0, saved_torrent_count(&root.join("output")));
        assert!(!root.join("links/tracker.example/movie.mkv").exists());
    }

    #[tokio::test]
    async fn worker_injects_and_starts_below_threshold_when_configured() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("injection-start-below-threshold");
        let target = Arc::new(FakeClient::new(descriptor("target", "target")));
        let (local, candidate, candidate_id) = persisted_inputs(&repository, &root).await;
        let mut request = request(local, candidate, candidate_id, &root);
        request.metafile = metafile_with_files(&[("movie.mkv", 20)]);
        request.assessment = CandidateAssessment {
            decision: MatchDecision::Partial,
            reason: crate::domain::DecisionReason::PartialOverlap,
            matched_size: Some(ByteSize::new(10)),
            matched_ratio: Some(MatchRatio::new(0.5).unwrap()),
        };
        request.recheck = RecheckResumeConfig {
            below_threshold_action: BelowThresholdAction::InjectAndStart,
            ..RecheckResumeConfig::default()
        };
        let worker =
            InjectionWorker::new(repository, vec![target.clone() as Arc<dyn InjectionClient>]);

        let result = worker.process(request).await.unwrap();

        assert_eq!(InjectionOutcome::Injected, result.outcome);
        assert!(!result.saved_for_retry);
        assert_eq!(1, target.inject_calls.load(Ordering::SeqCst));
        assert_eq!(0, target.recheck_calls.load(Ordering::SeqCst));
        assert_eq!(0, target.resume_calls.load(Ordering::SeqCst));
        assert_eq!(Some(false), target.last_pause_for_recheck());
        assert_eq!(0, saved_torrent_count(&root.join("output")));
    }

    #[tokio::test]
    async fn worker_injects_paused_below_threshold_without_auto_resume() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("injection-paused-below-threshold");
        let target = Arc::new(FakeClient::new(descriptor("target", "target")));
        let (local, candidate, candidate_id) = persisted_inputs(&repository, &root).await;
        let mut request = request(local, candidate, candidate_id, &root);
        request.metafile = metafile_with_files(&[("movie.mkv", 20)]);
        request.assessment = CandidateAssessment {
            decision: MatchDecision::Partial,
            reason: crate::domain::DecisionReason::PartialOverlap,
            matched_size: Some(ByteSize::new(10)),
            matched_ratio: Some(MatchRatio::new(0.5).unwrap()),
        };
        request.recheck = RecheckResumeConfig {
            below_threshold_action: BelowThresholdAction::InjectPaused,
            ..RecheckResumeConfig::default()
        };
        let worker =
            InjectionWorker::new(repository, vec![target.clone() as Arc<dyn InjectionClient>]);

        let result = worker.process(request).await.unwrap();

        assert_eq!(InjectionOutcome::Injected, result.outcome);
        assert!(!result.saved_for_retry);
        assert_eq!(1, target.inject_calls.load(Ordering::SeqCst));
        assert_eq!(0, target.recheck_calls.load(Ordering::SeqCst));
        assert_eq!(0, target.resume_calls.load(Ordering::SeqCst));
        assert_eq!(0, target.remaining_calls.load(Ordering::SeqCst));
        assert_eq!(Some(true), target.last_pause_for_recheck());
        assert_eq!(0, saved_torrent_count(&root.join("output")));
    }

    #[tokio::test]
    async fn worker_configured_byte_threshold_resumes_partial_match_default_leaves_paused() {
        let default_repository = Repository::connect_in_memory().await.unwrap();
        let default_root = unique_temp_dir("injection-default-threshold");
        let default_target =
            Arc::new(FakeClient::new(descriptor("target", "target")).with_remaining_bytes(10));
        let (local, candidate, candidate_id) =
            persisted_inputs(&default_repository, &default_root).await;
        let mut default_request = request(local, candidate, candidate_id, &default_root);
        default_request.metafile = metafile_with_files(&[("movie.mkv", 20)]);
        default_request.assessment = CandidateAssessment {
            decision: MatchDecision::Partial,
            reason: crate::domain::DecisionReason::PartialOverlap,
            matched_size: Some(ByteSize::new(10)),
            matched_ratio: Some(MatchRatio::new(0.5).unwrap()),
        };
        let default_worker = InjectionWorker::new(
            default_repository,
            vec![default_target.clone() as Arc<dyn InjectionClient>],
        );

        let default_result = default_worker.process(default_request).await.unwrap();

        assert_eq!(InjectionOutcome::Injected, default_result.outcome);
        assert!(!default_result.saved_for_retry);
        assert_eq!(1, default_target.inject_calls.load(Ordering::SeqCst));
        assert_eq!(0, default_target.recheck_calls.load(Ordering::SeqCst));
        assert_eq!(0, default_target.resume_calls.load(Ordering::SeqCst));
        assert_eq!(Some(true), default_target.last_pause_for_recheck());

        let configured_repository = Repository::connect_in_memory().await.unwrap();
        let configured_root = unique_temp_dir("injection-configured-threshold");
        let configured_target =
            Arc::new(FakeClient::new(descriptor("target", "target")).with_remaining_bytes(10));
        let (local, candidate, candidate_id) =
            persisted_inputs(&configured_repository, &configured_root).await;
        let mut configured_request = request(local, candidate, candidate_id, &configured_root);
        configured_request.metafile = metafile_with_files(&[("movie.mkv", 20)]);
        configured_request.assessment = CandidateAssessment {
            decision: MatchDecision::Partial,
            reason: crate::domain::DecisionReason::PartialOverlap,
            matched_size: Some(ByteSize::new(10)),
            matched_ratio: Some(MatchRatio::new(0.5).unwrap()),
        };
        configured_request.recheck = RecheckResumeConfig {
            auto_resume_max_download: ByteSize::new(10),
            poll_interval_ms: 0,
            ..RecheckResumeConfig::default()
        };
        let configured_worker = InjectionWorker::new(
            configured_repository,
            vec![configured_target.clone() as Arc<dyn InjectionClient>],
        );

        let configured_result = configured_worker.process(configured_request).await.unwrap();

        assert_eq!(InjectionOutcome::Injected, configured_result.outcome);
        assert!(configured_result.saved_for_retry);
        assert_eq!(1, configured_target.inject_calls.load(Ordering::SeqCst));
        assert_eq!(1, configured_target.recheck_calls.load(Ordering::SeqCst));
        assert_eq!(1, configured_target.remaining_calls.load(Ordering::SeqCst));
        assert_eq!(1, configured_target.resume_calls.load(Ordering::SeqCst));
        assert_eq!(Some(true), configured_target.last_pause_for_recheck());
    }

    #[tokio::test]
    async fn worker_configured_thresholds_do_not_auto_resume_exact_size_only_or_video_disc() {
        for (name, decision, reason, metafile) in [
            (
                "exact",
                MatchDecision::Exact,
                crate::domain::DecisionReason::FileTreeMatched,
                metafile_with_files(&[("movie.mkv", 20)]),
            ),
            (
                "size-only",
                MatchDecision::SizeOnly,
                crate::domain::DecisionReason::SizeMatched,
                metafile_with_files(&[("movie.mkv", 20)]),
            ),
            (
                "video-disc",
                MatchDecision::Partial,
                crate::domain::DecisionReason::PartialOverlap,
                metafile_with_files(&[("BDMV/STREAM/00001.m2ts", 20)]),
            ),
        ] {
            let repository = Repository::connect_in_memory().await.unwrap();
            let root = unique_temp_dir(&format!("injection-{name}-threshold"));
            let target =
                Arc::new(FakeClient::new(descriptor("target", "target")).with_remaining_bytes(10));
            let (local, candidate, candidate_id) = persisted_inputs(&repository, &root).await;
            let mut request = request(local, candidate, candidate_id, &root);
            request.metafile = metafile;
            request.assessment = CandidateAssessment {
                decision,
                reason,
                matched_size: Some(ByteSize::new(10)),
                matched_ratio: Some(MatchRatio::new(0.5).unwrap()),
            };
            request.recheck = RecheckResumeConfig {
                auto_resume_max_download: ByteSize::new(10),
                min_completion_percent: Some(50.0),
                max_remaining_percent: Some(50.0),
                poll_interval_ms: 0,
                ..RecheckResumeConfig::default()
            };
            let worker =
                InjectionWorker::new(repository, vec![target.clone() as Arc<dyn InjectionClient>]);

            let result = worker.process(request).await.unwrap();

            assert_eq!(InjectionOutcome::Injected, result.outcome);
            assert!(result.saved_for_retry, "{name} should stay saved for retry");
            assert_eq!(1, target.inject_calls.load(Ordering::SeqCst));
            assert_eq!(1, target.recheck_calls.load(Ordering::SeqCst));
            assert_eq!(1, target.remaining_calls.load(Ordering::SeqCst));
            assert_eq!(
                0,
                target.resume_calls.load(Ordering::SeqCst),
                "{name} should not resume"
            );
            assert_eq!(Some(true), target.last_pause_for_recheck());
        }
    }

    #[tokio::test]
    async fn worker_configured_non_relevant_slack_resumes_partial_match() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("injection-non-relevant-threshold");
        let target =
            Arc::new(FakeClient::new(descriptor("target", "target")).with_remaining_bytes(30));
        let (local, candidate, candidate_id) = persisted_inputs(&repository, &root).await;
        let mut request = request(local, candidate, candidate_id, &root);
        request.metafile = TorrentMetafile::new_with_piece_length(
            InfoHash::new("0123456789abcdef0123456789abcdef01234567").unwrap(),
            DisplayName::new("movie").unwrap(),
            vec![
                crate::domain::TorrentFile::new(
                    PathBuf::from("movie.mkv"),
                    ByteSize::new(100),
                    FileIndex::new(0),
                )
                .unwrap(),
                crate::domain::TorrentFile::new(
                    PathBuf::from("extras/sample.nfo"),
                    ByteSize::new(20),
                    FileIndex::new(1),
                )
                .unwrap(),
            ],
            Some(ByteSize::new(5)),
        )
        .unwrap();
        request.assessment = CandidateAssessment {
            decision: MatchDecision::Partial,
            reason: crate::domain::DecisionReason::PartialOverlap,
            matched_size: Some(ByteSize::new(100)),
            matched_ratio: Some(MatchRatio::new(0.83).unwrap()),
        };
        request.recheck = RecheckResumeConfig {
            ignore_non_relevant_files_to_resume: true,
            poll_interval_ms: 0,
            ..RecheckResumeConfig::default()
        };
        let worker =
            InjectionWorker::new(repository, vec![target.clone() as Arc<dyn InjectionClient>]);

        let result = worker.process(request).await.unwrap();

        assert_eq!(InjectionOutcome::Injected, result.outcome);
        assert!(result.saved_for_retry);
        assert_eq!(1, target.inject_calls.load(Ordering::SeqCst));
        assert_eq!(1, target.recheck_calls.load(Ordering::SeqCst));
        assert_eq!(1, target.remaining_calls.load(Ordering::SeqCst));
        assert_eq!(1, target.resume_calls.load(Ordering::SeqCst));
        assert_eq!(Some(true), target.last_pause_for_recheck());
    }

    #[tokio::test]
    async fn worker_rechecks_exact_match_by_default_before_resume() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("injection-default-recheck");
        let target = Arc::new(FakeClient::new(descriptor("target", "target")));
        let (local, candidate, candidate_id) = persisted_inputs(&repository, &root).await;
        let request = request(local, candidate, candidate_id, &root);
        let worker =
            InjectionWorker::new(repository, vec![target.clone() as Arc<dyn InjectionClient>]);

        let result = worker.process(request).await.unwrap();

        assert_eq!(InjectionOutcome::Injected, result.outcome);
        assert!(result.saved_for_retry);
        assert_eq!(1, target.inject_calls.load(Ordering::SeqCst));
        assert_eq!(1, target.recheck_calls.load(Ordering::SeqCst));
        assert_eq!(1, target.resume_calls.load(Ordering::SeqCst));
        assert_eq!(Some(true), target.last_pause_for_recheck());
        assert_eq!(1, saved_torrent_count(&root.join("output")));
    }

    #[tokio::test]
    async fn recheck_resume_clamps_final_poll_sleep_to_deadline() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("injection-resume-deadline");
        let target =
            Arc::new(FakeClient::new(descriptor("target", "target")).with_checking_true(10));
        let (local, candidate, candidate_id) = persisted_inputs(&repository, &root).await;
        let mut request = request(local, candidate, candidate_id, &root);
        request.recheck = RecheckResumeConfig {
            poll_interval_ms: 100,
            max_resume_wait_ms: 15,
            ..RecheckResumeConfig::default()
        };
        let worker =
            InjectionWorker::new(repository, vec![target.clone() as Arc<dyn InjectionClient>]);
        let plan = recheck_resume_plan(&request.metafile, &request.assessment, request.recheck);

        let outcome = worker
            .run_recheck_resume(target.as_ref(), &request, plan, None)
            .await
            .unwrap();

        assert_eq!(ResumeLoopOutcome::StillChecking, outcome);
        assert_eq!(1, target.recheck_calls.load(Ordering::SeqCst));
        assert_eq!(1, target.checking_calls.load(Ordering::SeqCst));
        assert_eq!(0, target.resume_calls.load(Ordering::SeqCst));
    }

    #[test]
    fn resume_poll_sleep_duration_clamps_to_deadline() {
        let now = Instant::now();
        let config = RecheckResumeConfig {
            poll_interval_ms: 100,
            max_resume_wait_ms: 1_000,
            ..RecheckResumeConfig::default()
        };

        assert_eq!(
            Some(Duration::from_millis(100)),
            resume_poll_sleep_duration(config, now, now + Duration::from_millis(250))
        );
        assert_eq!(
            Some(Duration::from_millis(15)),
            resume_poll_sleep_duration(config, now, now + Duration::from_millis(15))
        );
        assert_eq!(None, resume_poll_sleep_duration(config, now, now));
        assert_eq!(
            None,
            resume_poll_sleep_duration(
                RecheckResumeConfig {
                    poll_interval_ms: 0,
                    ..config
                },
                now,
                now + Duration::from_millis(15),
            )
        );
    }

    #[tokio::test]
    async fn worker_saves_paused_inject_when_recheck_fails() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("injection-recheck-fails");
        let target =
            Arc::new(FakeClient::new(descriptor("target", "target")).with_recheck_errors(1));
        let (local, candidate, candidate_id) = persisted_inputs(&repository, &root).await;
        let request = request(local, candidate, candidate_id, &root);
        let worker =
            InjectionWorker::new(repository, vec![target.clone() as Arc<dyn InjectionClient>]);

        let error = worker.process(request).await.unwrap_err();

        assert!(matches!(error, InjectionWorkerError::Client(_)));
        assert_eq!(1, target.inject_calls.load(Ordering::SeqCst));
        assert_eq!(1, target.recheck_calls.load(Ordering::SeqCst));
        assert_eq!(Some(true), target.last_pause_for_recheck());
        assert_eq!(1, saved_torrent_count(&root.join("output")));
    }

    #[tokio::test]
    async fn worker_rechecks_paused_inject_when_retry_save_fails() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("injection-save-fails");
        fs::write(root.join("output"), b"not a directory").unwrap();
        let target = Arc::new(FakeClient::new(descriptor("target", "target")));
        let (local, candidate, candidate_id) = persisted_inputs(&repository, &root).await;
        let request = request(local, candidate, candidate_id, &root);
        let worker =
            InjectionWorker::new(repository, vec![target.clone() as Arc<dyn InjectionClient>]);

        let error = worker.process(request).await.unwrap_err();

        assert!(matches!(error, InjectionWorkerError::Save(_)));
        assert_eq!(1, target.inject_calls.load(Ordering::SeqCst));
        assert_eq!(1, target.recheck_calls.load(Ordering::SeqCst));
        assert_eq!(1, target.resume_calls.load(Ordering::SeqCst));
        assert_eq!(Some(true), target.last_pause_for_recheck());
    }

    #[tokio::test]
    async fn worker_does_not_recheck_existing_target_without_saved_retry() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("injection-existing-target");
        let target = Arc::new(FakeClient::new(descriptor("target", "target")).with_existing(true));
        let (local, candidate, candidate_id) = persisted_inputs(&repository, &root).await;
        let request = request(local, candidate, candidate_id, &root);
        let worker =
            InjectionWorker::new(repository, vec![target.clone() as Arc<dyn InjectionClient>]);

        let result = worker.process(request).await.unwrap();

        assert_eq!(InjectionOutcome::AlreadyExists, result.outcome);
        assert!(!result.saved_for_retry);
        assert_eq!(1, target.has_calls.load(Ordering::SeqCst));
        assert_eq!(0, target.inject_calls.load(Ordering::SeqCst));
        assert_eq!(0, target.recheck_calls.load(Ordering::SeqCst));
        assert_eq!(0, target.resume_calls.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn saved_retry_resumes_prior_paused_inject_without_links() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("injection-retry-paused");
        let initial_target =
            Arc::new(FakeClient::new(descriptor("target", "target")).with_checking_true(1));
        let (local, mut candidate, candidate_id) = persisted_inputs(&repository, &root).await;
        repository
            .upsert_local_item_with_files(&local, &[local_file()])
            .await
            .unwrap();
        let parsed = parse_metafile(test_torrent_bytes()).unwrap();
        candidate.info_hash = Some(parsed.metafile.info_hash().clone());
        let mut request = request(local, candidate, candidate_id, &root);
        request.metafile = parsed.metafile;
        request.torrent_bytes = test_torrent_bytes().to_vec();
        request.recheck = RecheckResumeConfig {
            poll_interval_ms: 0,
            max_resume_wait_ms: 0,
            ..RecheckResumeConfig::default()
        };
        let worker = InjectionWorker::new(
            repository.clone(),
            vec![initial_target.clone() as Arc<dyn InjectionClient>],
        );

        let result = worker.process(request).await.unwrap();

        assert_eq!(InjectionOutcome::Injected, result.outcome);
        assert!(result.saved_for_retry);
        assert_eq!(1, initial_target.inject_calls.load(Ordering::SeqCst));
        assert_eq!(0, initial_target.resume_calls.load(Ordering::SeqCst));
        assert_eq!(1, saved_torrent_count(&root.join("output")));

        let retry_target =
            Arc::new(FakeClient::new(descriptor("target", "target")).with_existing(true));
        let retry_worker = InjectionWorker::new(
            repository,
            vec![retry_target.clone() as Arc<dyn InjectionClient>],
        );
        let summary = retry_worker
            .retry_saved_torrents(SavedTorrentRetryConfig {
                directories: vec![root.join("output")],
                recheck: RecheckResumeConfig {
                    poll_interval_ms: 0,
                    max_resume_wait_ms: 0,
                    ..RecheckResumeConfig::default()
                },
                assessed_at_ms: 1_700_000_000_000,
                ..SavedTorrentRetryConfig::default()
            })
            .await
            .unwrap();

        assert_eq!(1, summary.attempted);
        assert_eq!(1, summary.already_exists);
        assert_eq!(1, summary.deleted);
        assert_eq!(1, retry_target.has_calls.load(Ordering::SeqCst));
        assert_eq!(0, retry_target.inject_calls.load(Ordering::SeqCst));
        assert_eq!(1, retry_target.recheck_calls.load(Ordering::SeqCst));
        assert_eq!(1, retry_target.resume_calls.load(Ordering::SeqCst));
        assert_eq!(0, saved_torrent_count(&root.join("output")));
    }

    #[tokio::test]
    async fn saved_retry_dry_run_keeps_existing_retry_file_without_recheck() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("saved-retry-dry-run-existing");
        let output_dir = root.join("output");
        repository
            .upsert_local_item_with_files(&local_item(&root), &[local_file()])
            .await
            .unwrap();
        save_test_torrent(
            &output_dir,
            "movie.mkv",
            test_torrent_bytes(),
            MediaType::Movie,
        );
        let target = Arc::new(FakeClient::new(descriptor("target", "target")).with_existing(true));
        let worker =
            InjectionWorker::new(repository, vec![target.clone() as Arc<dyn InjectionClient>])
                .with_dry_run(true);

        let summary = worker
            .retry_saved_torrents(SavedTorrentRetryConfig {
                directories: vec![output_dir.clone()],
                assessed_at_ms: 1_700_000_000_000,
                ..SavedTorrentRetryConfig::default()
            })
            .await
            .unwrap();

        assert_eq!(1, summary.scanned);
        assert_eq!(1, summary.attempted);
        assert_eq!(1, summary.already_exists);
        assert_eq!(1, summary.kept);
        assert_eq!(0, summary.deleted);
        assert_eq!(1, saved_torrent_count(&output_dir));
        assert_eq!(1, target.has_calls.load(Ordering::SeqCst));
        assert_eq!(0, target.inject_calls.load(Ordering::SeqCst));
        assert_eq!(0, target.recheck_calls.load(Ordering::SeqCst));
        assert_eq!(0, target.resume_calls.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn worker_process_until_shutdown_stops_pending_has_torrent() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("injection-shutdown-has");
        let target = Arc::new(FakeClient::new(descriptor("target", "target")).with_pending_has());
        let (local, candidate, candidate_id) = persisted_inputs(&repository, &root).await;
        let output_dir = root.join("output");
        let request = request(local, candidate, candidate_id, &root);
        let worker =
            InjectionWorker::new(repository, vec![target.clone() as Arc<dyn InjectionClient>]);
        let (shutdown, signal) = shutdown_channel();
        let handle =
            tokio::spawn(async move { worker.process_until_shutdown(request, signal).await });

        wait_for_calls(&target.has_calls, 1).await;
        shutdown.cancel_now("test shutdown").unwrap();
        let result = tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        assert_eq!(InjectionOutcome::Saved, result.outcome);
        assert!(result.saved_for_retry);
        assert_eq!(0, target.inject_calls.load(Ordering::SeqCst));
        assert_eq!(1, saved_torrent_count(&output_dir));
    }

    #[tokio::test]
    async fn worker_process_until_shutdown_stops_pending_inject() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("injection-shutdown-inject");
        let target =
            Arc::new(FakeClient::new(descriptor("target", "target")).with_pending_inject());
        let (local, candidate, candidate_id) = persisted_inputs(&repository, &root).await;
        let output_dir = root.join("output");
        let request = request(local, candidate, candidate_id, &root);
        let worker =
            InjectionWorker::new(repository, vec![target.clone() as Arc<dyn InjectionClient>]);
        let (shutdown, signal) = shutdown_channel();
        let handle =
            tokio::spawn(async move { worker.process_until_shutdown(request, signal).await });

        wait_for_calls(&target.inject_calls, 1).await;
        assert_eq!(0, saved_torrent_count(&output_dir));
        shutdown.cancel_now("test shutdown").unwrap();
        let result = tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        assert_eq!(InjectionOutcome::Saved, result.outcome);
        assert!(result.saved_for_retry);
        assert_eq!(1, target.has_calls.load(Ordering::SeqCst));
        assert_eq!(1, saved_torrent_count(&output_dir));
    }

    #[tokio::test]
    async fn worker_process_until_shutdown_cleans_prepared_links() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("injection-shutdown-link-cleanup");
        fs::write(root.join("movie.mkv"), b"0123456789").unwrap();
        let target =
            Arc::new(FakeClient::new(descriptor("target", "target")).with_pending_inject());
        let (local, candidate, candidate_id) = persisted_inputs(&repository, &root).await;
        let output_dir = root.join("output");
        let mut request = request(local, candidate, candidate_id, &root);
        request.link_type = Some(LinkType::Hardlink);
        request.link_dirs = vec![root.join("links")];
        fs::create_dir_all(&request.link_dirs[0]).unwrap();
        let worker =
            InjectionWorker::new(repository, vec![target.clone() as Arc<dyn InjectionClient>]);
        let (shutdown, signal) = shutdown_channel();
        let handle =
            tokio::spawn(async move { worker.process_until_shutdown(request, signal).await });

        wait_for_calls(&target.inject_calls, 1).await;
        shutdown.cancel_now("test shutdown").unwrap();
        let result = tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        assert_eq!(InjectionOutcome::Saved, result.outcome);
        assert!(result.saved_for_retry);
        assert_eq!(1, result.linked_files);
        assert!(!result.prepared_link_cleanup_incomplete);
        assert_eq!(1, saved_torrent_count(&output_dir));
        assert!(!root.join("links/tracker.example/movie.mkv").exists());
    }

    #[tokio::test]
    async fn worker_process_until_shutdown_stops_pending_resume() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("injection-shutdown-resume");
        let target =
            Arc::new(FakeClient::new(descriptor("target", "target")).with_pending_resume());
        let (local, candidate, candidate_id) = persisted_inputs(&repository, &root).await;
        let mut request = request(local, candidate, candidate_id, &root);
        request.recheck = RecheckResumeConfig {
            skip_recheck: false,
            poll_interval_ms: 0,
            ..RecheckResumeConfig::default()
        };
        let worker =
            InjectionWorker::new(repository, vec![target.clone() as Arc<dyn InjectionClient>]);
        let (shutdown, signal) = shutdown_channel();
        let handle =
            tokio::spawn(async move { worker.process_until_shutdown(request, signal).await });

        wait_for_calls(&target.resume_calls, 1).await;
        shutdown.cancel_now("test shutdown").unwrap();
        let result = tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        assert_eq!(InjectionOutcome::Injected, result.outcome);
        assert!(result.saved_for_retry);
        assert_eq!(1, target.recheck_calls.load(Ordering::SeqCst));
        assert_eq!(1, saved_torrent_count(&root.join("output")));
    }

    #[tokio::test]
    async fn worker_process_until_shutdown_saves_existing_linked_recheck() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("injection-shutdown-existing");
        fs::write(root.join("movie.mkv"), b"0123456789").unwrap();
        let target = Arc::new(
            FakeClient::new(descriptor("target", "target"))
                .with_existing(true)
                .with_pending_resume(),
        );
        let (local, candidate, candidate_id) = persisted_inputs(&repository, &root).await;
        let output_dir = root.join("output");
        let mut request = request(local, candidate, candidate_id, &root);
        request.link_type = Some(LinkType::Hardlink);
        request.link_dirs = vec![root.join("links")];
        fs::create_dir_all(&request.link_dirs[0]).unwrap();
        let worker =
            InjectionWorker::new(repository, vec![target.clone() as Arc<dyn InjectionClient>]);
        let (shutdown, signal) = shutdown_channel();
        let handle =
            tokio::spawn(async move { worker.process_until_shutdown(request, signal).await });

        wait_for_calls(&target.resume_calls, 1).await;
        shutdown.cancel_now("test shutdown").unwrap();
        let result = tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        assert_eq!(InjectionOutcome::AlreadyExists, result.outcome);
        assert!(result.saved_for_retry);
        assert_eq!(1, result.linked_files);
        assert_eq!(1, saved_torrent_count(&output_dir));
    }

    #[tokio::test]
    async fn recheck_resume_does_not_call_ready_client_after_shutdown() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("injection-shutdown-ready-recheck");
        let target = Arc::new(FakeClient::new(descriptor("target", "target")));
        let (local, candidate, candidate_id) = persisted_inputs(&repository, &root).await;
        let mut request = request(local, candidate, candidate_id, &root);
        request.recheck = RecheckResumeConfig {
            skip_recheck: false,
            poll_interval_ms: 0,
            ..RecheckResumeConfig::default()
        };
        let worker =
            InjectionWorker::new(repository, vec![target.clone() as Arc<dyn InjectionClient>]);
        let (shutdown, signal) = shutdown_channel();
        shutdown.cancel_now("test shutdown").unwrap();
        let plan = recheck_resume_plan(&request.metafile, &request.assessment, request.recheck);

        let outcome = worker
            .run_recheck_resume(target.as_ref(), &request, plan, Some(&signal))
            .await
            .unwrap();

        assert_eq!(ResumeLoopOutcome::StillChecking, outcome);
        assert_eq!(0, target.recheck_calls.load(Ordering::SeqCst));
        assert_eq!(0, target.resume_calls.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn saved_torrent_cleanup_does_not_probe_client_after_shutdown() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("injection-shutdown-cleanup");
        let output_dir = root.join("output");
        save_test_torrent(
            &output_dir,
            "movie.mkv",
            test_torrent_bytes(),
            MediaType::Movie,
        );
        let path = fs::read_dir(&output_dir)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let file_name = path.file_name().and_then(|name| name.to_str()).unwrap();
        let saved = read_saved_torrent(&path).await.unwrap();
        let target = Arc::new(FakeClient::new(descriptor("target", "target")));
        let worker =
            InjectionWorker::new(repository, vec![target.clone() as Arc<dyn InjectionClient>]);
        let result = InjectionWorkResult {
            outcome: InjectionOutcome::Injected,
            target_client: Some(DependencyName::new("target").unwrap()),
            dry_run_action: None,
            saved_for_retry: false,
            linked_files: 0,
            prepared_link_cleanup_incomplete: false,
        };
        let (shutdown, signal) = shutdown_channel();
        shutdown.cancel_now("test shutdown").unwrap();

        let deleted = worker
            .delete_saved_torrent_if_complete(
                &path,
                file_name,
                saved.parsed.metafile.info_hash(),
                saved.identity,
                &result,
                Some(&signal),
            )
            .await
            .unwrap();

        assert!(!deleted);
        assert!(path.exists());
        assert_eq!(0, target.checking_calls.load(Ordering::SeqCst));
        assert_eq!(0, target.remaining_calls.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn saved_torrent_retry_injects_match_and_deletes_completed_file() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("saved-retry-success");
        let output_dir = root.join("output");
        let local = local_item(&root);
        repository
            .upsert_local_item_with_files(&local, &[local_file()])
            .await
            .unwrap();
        let parsed = parse_metafile(test_torrent_bytes()).unwrap();
        let mut candidate = remote_candidate();
        candidate.info_hash = Some(parsed.metafile.info_hash().clone());
        save_candidate_torrent(
            &output_dir,
            &candidate_output_metadata(MediaType::Movie, &candidate, &parsed.metafile),
            test_torrent_bytes(),
        )
        .unwrap();
        let target = Arc::new(FakeClient::new(descriptor("target", "target")));
        let worker =
            InjectionWorker::new(repository, vec![target.clone() as Arc<dyn InjectionClient>]);

        let summary = worker
            .retry_saved_torrents(SavedTorrentRetryConfig {
                directories: vec![output_dir.clone()],
                recheck: skip_recheck_config(),
                assessed_at_ms: 1_700_000_000_000,
                ..SavedTorrentRetryConfig::default()
            })
            .await
            .unwrap();

        assert_eq!(1, summary.scanned);
        assert_eq!(1, summary.attempted);
        assert_eq!(1, summary.injected);
        assert_eq!(1, summary.deleted);
        assert_eq!(0, saved_torrent_count(&output_dir));
        assert_eq!(1, target.inject_calls.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn saved_retry_rejects_below_threshold_without_client_mutation() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("saved-retry-rejected");
        let target = Arc::new(FakeClient::new(descriptor("target", "target")));
        let (local, candidate, candidate_id) = persisted_inputs(&repository, &root).await;
        let mut request = request(local, candidate, candidate_id, &root);
        request.metafile = metafile_with_files(&[("movie.mkv", 20)]);
        request.assessment = CandidateAssessment {
            decision: MatchDecision::Partial,
            reason: crate::domain::DecisionReason::PartialOverlap,
            matched_size: Some(ByteSize::new(10)),
            matched_ratio: Some(MatchRatio::new(0.5).unwrap()),
        };
        request.recheck = RecheckResumeConfig {
            below_threshold_action: BelowThresholdAction::RejectWithoutInjecting,
            ..RecheckResumeConfig::default()
        };
        let worker =
            InjectionWorker::new(repository, vec![target.clone() as Arc<dyn InjectionClient>]);
        let mut should_stop = || false;

        let result = worker
            .process_inner(request, &mut should_stop, None, true)
            .await
            .unwrap();

        assert_eq!(InjectionOutcome::Rejected, result.outcome);
        assert!(!result.saved_for_retry);
        assert_eq!(0, target.has_calls.load(Ordering::SeqCst));
        assert_eq!(0, target.inject_calls.load(Ordering::SeqCst));
        assert_eq!(0, target.recheck_calls.load(Ordering::SeqCst));
        assert_eq!(0, target.resume_calls.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn saved_torrent_retry_links_data_dir_files_before_injecting() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("saved-retry-linking");
        let output_dir = root.join("output");
        let link_dir = root.join("links");
        fs::create_dir_all(&link_dir).unwrap();
        fs::write(root.join("movie.mkv"), b"0123456789").unwrap();
        repository
            .upsert_local_item_with_files(&local_item(&root), &[local_file()])
            .await
            .unwrap();
        let parsed = parse_metafile(test_torrent_bytes()).unwrap();
        let mut candidate = remote_candidate();
        candidate.info_hash = Some(parsed.metafile.info_hash().clone());
        save_candidate_torrent(
            &output_dir,
            &candidate_output_metadata(MediaType::Movie, &candidate, &parsed.metafile),
            test_torrent_bytes(),
        )
        .unwrap();
        let target = Arc::new(FakeClient::new(descriptor("target", "target")));
        let worker =
            InjectionWorker::new(repository, vec![target.clone() as Arc<dyn InjectionClient>]);

        let summary = worker
            .retry_saved_torrents(SavedTorrentRetryConfig {
                directories: vec![output_dir.clone()],
                link_dirs: vec![link_dir],
                link_type: Some(LinkType::Hardlink),
                recheck: skip_recheck_config(),
                assessed_at_ms: 1_700_000_000_000,
                ..SavedTorrentRetryConfig::default()
            })
            .await
            .unwrap();

        assert_eq!(1, summary.scanned);
        assert_eq!(1, summary.attempted);
        assert_eq!(1, summary.injected);
        assert_eq!(0, summary.deleted);
        assert_eq!(1, saved_torrent_count(&output_dir));
        assert!(root.join("links/tracker.example/movie.mkv").exists());
        assert_eq!(1, target.inject_calls.load(Ordering::SeqCst));
        assert_eq!(Some(true), target.last_pause_for_recheck());
        assert_eq!(
            Some(root.join("links/tracker.example")),
            target.last_save_path()
        );
        assert_eq!(
            1,
            target
                .save_path_file_exists_at_inject
                .load(Ordering::SeqCst)
        );
    }

    #[tokio::test]
    async fn saved_torrent_retry_reconciles_existing_prepared_links_after_restart() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("saved-retry-link-restart");
        let output_dir = root.join("output");
        let link_dir = root.join("links");
        let prepared_dir = link_dir.join("tracker.example");
        fs::create_dir_all(&prepared_dir).unwrap();
        fs::write(root.join("movie.mkv"), b"0123456789").unwrap();
        fs::hard_link(root.join("movie.mkv"), prepared_dir.join("movie.mkv")).unwrap();
        repository
            .upsert_local_item_with_files(&local_item(&root), &[local_file()])
            .await
            .unwrap();
        let parsed = parse_metafile(test_torrent_bytes()).unwrap();
        let mut candidate = remote_candidate();
        candidate.info_hash = Some(parsed.metafile.info_hash().clone());
        save_candidate_torrent(
            &output_dir,
            &candidate_output_metadata(MediaType::Movie, &candidate, &parsed.metafile),
            test_torrent_bytes(),
        )
        .unwrap();

        let target = Arc::new(FakeClient::new(descriptor("target", "target")));
        let worker = InjectionWorker::new(
            repository.clone(),
            vec![target.clone() as Arc<dyn InjectionClient>],
        );
        let summary = worker
            .retry_saved_torrents(SavedTorrentRetryConfig {
                directories: vec![output_dir.clone()],
                link_dirs: vec![link_dir.clone()],
                link_type: Some(LinkType::Hardlink),
                recheck: skip_recheck_config(),
                assessed_at_ms: 1_700_000_000_000,
                ..SavedTorrentRetryConfig::default()
            })
            .await
            .unwrap();

        assert_eq!(1, summary.scanned);
        assert_eq!(1, summary.attempted);
        assert_eq!(1, summary.injected);
        assert_eq!(0, summary.deleted);
        assert_eq!(1, summary.kept);
        assert_eq!(1, target.inject_calls.load(Ordering::SeqCst));
        assert_eq!(Some(prepared_dir.clone()), target.last_save_path());
        assert_eq!(
            1,
            target
                .save_path_file_exists_at_inject
                .load(Ordering::SeqCst)
        );
        assert_eq!(1, saved_torrent_count(&output_dir));
        assert!(prepared_dir.join("movie.mkv").exists());

        let restarted_target =
            Arc::new(FakeClient::new(descriptor("target", "target")).with_existing(true));
        let restarted_worker = InjectionWorker::new(
            repository,
            vec![restarted_target.clone() as Arc<dyn InjectionClient>],
        );
        let summary = restarted_worker
            .retry_saved_torrents(SavedTorrentRetryConfig {
                directories: vec![output_dir.clone()],
                link_dirs: vec![link_dir],
                link_type: Some(LinkType::Hardlink),
                recheck: skip_recheck_config(),
                assessed_at_ms: 1_700_000_000_000,
                ..SavedTorrentRetryConfig::default()
            })
            .await
            .unwrap();

        assert_eq!(1, summary.scanned);
        assert_eq!(1, summary.attempted);
        assert_eq!(1, summary.already_exists);
        assert_eq!(1, summary.deleted);
        assert_eq!(0, restarted_target.inject_calls.load(Ordering::SeqCst));
        assert_eq!(1, restarted_target.has_calls.load(Ordering::SeqCst));
        assert_eq!(0, saved_torrent_count(&output_dir));
        assert!(prepared_dir.join("movie.mkv").exists());
    }

    #[tokio::test]
    async fn saved_torrent_retry_observes_shutdown_before_mutation() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("saved-retry-shutdown");
        let output_dir = root.join("output");
        let local = local_item(&root);
        repository
            .upsert_local_item_with_files(&local, &[local_file()])
            .await
            .unwrap();
        let parsed = parse_metafile(test_torrent_bytes()).unwrap();
        let mut candidate = remote_candidate();
        candidate.info_hash = Some(parsed.metafile.info_hash().clone());
        save_candidate_torrent(
            &output_dir,
            &candidate_output_metadata(MediaType::Movie, &candidate, &parsed.metafile),
            test_torrent_bytes(),
        )
        .unwrap();
        let target = Arc::new(FakeClient::new(descriptor("target", "target")));
        let worker =
            InjectionWorker::new(repository, vec![target.clone() as Arc<dyn InjectionClient>]);
        let (shutdown, mut signal) = crate::runtime::shutdown::shutdown_channel();
        shutdown.cancel_now("test shutdown").unwrap();

        let summary = worker
            .retry_saved_torrents_until_shutdown(
                SavedTorrentRetryConfig {
                    directories: vec![output_dir.clone()],
                    assessed_at_ms: 1_700_000_000_000,
                    ..SavedTorrentRetryConfig::default()
                },
                &mut signal,
            )
            .await
            .unwrap();

        assert_eq!(0, summary.scanned);
        assert_eq!(1, saved_torrent_count(&output_dir));
        assert_eq!(0, target.inject_calls.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn saved_torrent_scan_streams_limited_sorted_paths() {
        let root = unique_temp_dir("saved-retry-stream-scan");
        let output_dir = root.join("output");
        fs::create_dir_all(&output_dir).unwrap();
        let limit = SAVED_TORRENT_SCAN_BATCH + 5;
        for index in 0..limit {
            fs::write(
                output_dir.join(format!("manual-{index}.torrent")),
                test_torrent_bytes(),
            )
            .unwrap();
            save_test_torrent(
                &output_dir,
                &format!("movie-{index}.mkv"),
                test_torrent_bytes(),
                MediaType::Movie,
            );
        }

        let mut scan = saved_torrent_path_scan(&output_dir, limit);
        let mut paths = Vec::new();
        while let Some(path) = scan.next_path().await.unwrap() {
            paths.push(path);
        }
        scan.finish().await.unwrap();

        assert_eq!(limit, paths.len());
        for chunk in paths.chunks(SAVED_TORRENT_SCAN_BATCH) {
            let mut sorted = chunk.to_vec();
            sorted.sort();
            assert_eq!(sorted, chunk);
        }
        assert!(
            paths
                .iter()
                .all(|path| is_direct_saved_torrent_file(&output_dir, path))
        );
    }

    #[tokio::test]
    async fn saved_torrent_scan_can_cancel_before_full_directory_walk() {
        let root = unique_temp_dir("saved-retry-stream-cancel");
        let output_dir = root.join("output");
        fs::create_dir_all(&output_dir).unwrap();
        for index in 0..2_000 {
            fs::write(
                output_dir.join(format!("manual-{index}.torrent")),
                test_torrent_bytes(),
            )
            .unwrap();
        }
        let mut scan = saved_torrent_path_scan(&output_dir, 1);
        let mut checks = 0;

        let result = tokio::time::timeout(Duration::from_secs(1), async {
            scan.next_path_until_stop(&mut || {
                checks += 1;
                checks >= 2
            })
            .await
        })
        .await
        .unwrap()
        .unwrap();

        assert_eq!(None, result);
    }

    #[tokio::test]
    async fn saved_torrent_scan_cancel_unblocks_full_channel_sender() {
        let root = unique_temp_dir("saved-retry-stream-cancel-full");
        let output_dir = root.join("output");
        fs::create_dir_all(&output_dir).unwrap();
        for index in 0..(SAVED_TORRENT_SCAN_BATCH * 4) {
            save_test_torrent(
                &output_dir,
                &format!("movie-{index}.mkv"),
                test_torrent_bytes(),
                MediaType::Movie,
            );
        }
        let progress = SavedTorrentScanProgress::new();
        let mut scan = saved_torrent_path_scan_with_progress(
            &output_dir,
            SAVED_TORRENT_SCAN_BATCH * 4,
            Some(progress.clone()),
        );

        assert!(scan.next_path().await.unwrap().is_some());
        progress
            .wait_for_send_attempts(SAVED_TORRENT_SCAN_BATCH + 2)
            .await;
        scan.cancel();

        tokio::time::timeout(Duration::from_secs(1), scan.finish())
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn saved_torrent_retry_stops_before_actionable_mutation() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("saved-retry-mid-shutdown");
        let output_dir = root.join("output");
        let local = local_item(&root);
        repository
            .upsert_local_item_with_files(&local, &[local_file()])
            .await
            .unwrap();
        let parsed = parse_metafile(test_torrent_bytes()).unwrap();
        let mut candidate = remote_candidate();
        candidate.info_hash = Some(parsed.metafile.info_hash().clone());
        save_candidate_torrent(
            &output_dir,
            &candidate_output_metadata(MediaType::Movie, &candidate, &parsed.metafile),
            test_torrent_bytes(),
        )
        .unwrap();
        let target = Arc::new(FakeClient::new(descriptor("target", "target")));
        let worker =
            InjectionWorker::new(repository, vec![target.clone() as Arc<dyn InjectionClient>]);
        let mut checks = 0;

        let summary = worker
            .retry_saved_torrents_inner(
                SavedTorrentRetryConfig {
                    directories: vec![output_dir.clone()],
                    assessed_at_ms: 1_700_000_000_000,
                    ..SavedTorrentRetryConfig::default()
                },
                || {
                    checks += 1;
                    checks >= 4
                },
                None,
            )
            .await
            .unwrap();

        assert_eq!(1, summary.scanned);
        assert_eq!(0, summary.attempted);
        assert_eq!(1, summary.kept);
        assert_eq!(1, saved_torrent_count(&output_dir));
        assert_eq!(0, target.inject_calls.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn saved_torrent_retry_keeps_retryable_failures_and_skips_unsafe_names() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("saved-retry-failure");
        let output_dir = root.join("output");
        let local = local_item(&root);
        repository
            .upsert_local_item_with_files(&local, &[local_file()])
            .await
            .unwrap();
        let parsed = parse_metafile(test_torrent_bytes()).unwrap();
        let mut candidate = remote_candidate();
        candidate.info_hash = Some(parsed.metafile.info_hash().clone());
        save_candidate_torrent(
            &output_dir,
            &candidate_output_metadata(MediaType::Movie, &candidate, &parsed.metafile),
            test_torrent_bytes(),
        )
        .unwrap();
        fs::write(output_dir.join("manual.torrent"), test_torrent_bytes()).unwrap();
        let target = Arc::new(FakeClient::new(descriptor("target", "target")).with_inject_error());
        let worker =
            InjectionWorker::new(repository, vec![target.clone() as Arc<dyn InjectionClient>]);

        let summary = worker
            .retry_saved_torrents(SavedTorrentRetryConfig {
                directories: vec![output_dir.clone()],
                recheck: skip_recheck_config(),
                assessed_at_ms: 1_700_000_000_000,
                ..SavedTorrentRetryConfig::default()
            })
            .await
            .unwrap();

        assert_eq!(1, summary.scanned);
        assert_eq!(1, summary.attempted);
        assert_eq!(1, summary.failed);
        assert_eq!(0, summary.skipped);
        assert_eq!(1, summary.kept);
        assert_eq!(2, saved_torrent_count(&output_dir));
        assert_eq!(1, target.inject_calls.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn saved_torrent_retry_continues_after_transient_preinject_error() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("saved-retry-transient");
        let output_dir = root.join("output");
        repository
            .upsert_local_item_with_files(&local_item(&root), &[local_file()])
            .await
            .unwrap();
        save_test_torrent(
            &output_dir,
            "movie.mkv",
            test_torrent_bytes(),
            MediaType::Movie,
        );
        save_test_torrent(
            &output_dir,
            "movie.mkv",
            alternate_torrent_bytes(),
            MediaType::Movie,
        );
        let target = Arc::new(FakeClient::new(descriptor("target", "target")).with_has_errors(1));
        let worker =
            InjectionWorker::new(repository, vec![target.clone() as Arc<dyn InjectionClient>]);

        let summary = worker
            .retry_saved_torrents(SavedTorrentRetryConfig {
                directories: vec![output_dir.clone()],
                recheck: skip_recheck_config(),
                assessed_at_ms: 1_700_000_000_000,
                ..SavedTorrentRetryConfig::default()
            })
            .await
            .unwrap();

        assert_eq!(2, summary.scanned);
        assert_eq!(2, summary.attempted);
        assert_eq!(1, summary.failed);
        assert_eq!(1, summary.injected);
        assert_eq!(1, summary.deleted);
        assert_eq!(1, summary.kept);
        assert_eq!(1, saved_torrent_count(&output_dir));
        assert_eq!(1, target.inject_calls.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn saved_torrent_retry_keeps_oversized_saved_files() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("saved-retry-oversized");
        let output_dir = root.join("output");
        save_test_torrent(
            &output_dir,
            "movie.mkv",
            test_torrent_bytes(),
            MediaType::Movie,
        );
        let path = fs::read_dir(&output_dir)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(MAX_SAVED_TORRENT_BYTES + 1)
            .unwrap();
        let target = Arc::new(FakeClient::new(descriptor("target", "target")));
        let worker =
            InjectionWorker::new(repository, vec![target.clone() as Arc<dyn InjectionClient>]);

        let summary = worker
            .retry_saved_torrents(SavedTorrentRetryConfig {
                directories: vec![output_dir.clone()],
                ..SavedTorrentRetryConfig::default()
            })
            .await
            .unwrap();

        assert_eq!(1, summary.scanned);
        assert_eq!(0, summary.attempted);
        assert_eq!(1, summary.failed);
        assert_eq!(1, summary.kept);
        assert_eq!(0, target.inject_calls.load(Ordering::SeqCst));
        assert_eq!(1, saved_torrent_count(&output_dir));
    }

    #[tokio::test]
    async fn saved_torrent_retry_uses_saved_media_type_for_lookup() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("saved-retry-audio");
        let output_dir = root.join("output");
        let audio = LocalItem {
            media_type: MediaType::Audio,
            title: ItemTitle::new("album").unwrap(),
            display_name: DisplayName::new("album").unwrap(),
            ..local_item(&root)
        };
        repository
            .upsert_local_item_with_files(&audio, &[local_file()])
            .await
            .unwrap();
        save_test_torrent(&output_dir, "album", test_torrent_bytes(), MediaType::Audio);
        let target = Arc::new(FakeClient::new(descriptor("target", "target")));
        let worker =
            InjectionWorker::new(repository, vec![target.clone() as Arc<dyn InjectionClient>]);

        let summary = worker
            .retry_saved_torrents(SavedTorrentRetryConfig {
                directories: vec![output_dir],
                assessed_at_ms: 1_700_000_000_000,
                ..SavedTorrentRetryConfig::default()
            })
            .await
            .unwrap();

        assert_eq!(1, summary.attempted);
        assert_eq!(1, summary.injected);
        assert_eq!(1, target.inject_calls.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn saved_torrent_retry_rejects_info_hash_media_type_mismatch() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("saved-retry-media-mismatch");
        let output_dir = root.join("output");
        let mut movie = local_item(&root);
        movie.info_hash = Some(
            parse_metafile(test_torrent_bytes())
                .unwrap()
                .metafile
                .info_hash()
                .clone(),
        );
        repository
            .upsert_local_item_with_files(&movie, &[local_file()])
            .await
            .unwrap();
        save_test_torrent(
            &output_dir,
            "movie.mkv",
            test_torrent_bytes(),
            MediaType::Audio,
        );
        let target = Arc::new(FakeClient::new(descriptor("target", "target")));
        let worker =
            InjectionWorker::new(repository, vec![target.clone() as Arc<dyn InjectionClient>]);

        let summary = worker
            .retry_saved_torrents(SavedTorrentRetryConfig {
                directories: vec![output_dir],
                assessed_at_ms: 1_700_000_000_000,
                ..SavedTorrentRetryConfig::default()
            })
            .await
            .unwrap();

        assert_eq!(1, summary.scanned);
        assert_eq!(0, summary.attempted);
        assert_eq!(1, summary.no_match);
        assert_eq!(0, target.inject_calls.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn saved_torrent_retry_budget_ignores_non_sporos_torrents() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("saved-retry-noise-budget");
        let output_dir = root.join("output");
        fs::create_dir_all(&output_dir).unwrap();
        repository
            .upsert_local_item_with_files(&local_item(&root), &[local_file()])
            .await
            .unwrap();
        for index in 0..8 {
            fs::write(
                output_dir.join(format!("manual-{index}.torrent")),
                test_torrent_bytes(),
            )
            .unwrap();
        }
        save_test_torrent(
            &output_dir,
            "movie.mkv",
            test_torrent_bytes(),
            MediaType::Movie,
        );
        let target = Arc::new(FakeClient::new(descriptor("target", "target")));
        let worker =
            InjectionWorker::new(repository, vec![target.clone() as Arc<dyn InjectionClient>]);

        let summary = worker
            .retry_saved_torrents(SavedTorrentRetryConfig {
                directories: vec![output_dir],
                max_saved_torrents: 1,
                assessed_at_ms: 1_700_000_000_000,
                ..SavedTorrentRetryConfig::default()
            })
            .await
            .unwrap();

        assert_eq!(1, summary.scanned);
        assert_eq!(1, summary.attempted);
        assert_eq!(1, summary.injected);
        assert_eq!(1, target.inject_calls.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn saved_torrent_retry_continues_after_corrupt_saved_file() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("saved-retry-corrupt");
        let output_dir = root.join("output");
        repository
            .upsert_local_item_with_files(&local_item(&root), &[local_file()])
            .await
            .unwrap();
        save_test_torrent(
            &output_dir,
            "movie.mkv",
            test_torrent_bytes(),
            MediaType::Movie,
        );
        let corrupt_path = fs::read_dir(&output_dir)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        fs::write(&corrupt_path, b"not a torrent").unwrap();
        save_test_torrent(
            &output_dir,
            "movie.mkv",
            alternate_torrent_bytes(),
            MediaType::Movie,
        );
        let target = Arc::new(FakeClient::new(descriptor("target", "target")));
        let worker =
            InjectionWorker::new(repository, vec![target.clone() as Arc<dyn InjectionClient>]);

        let summary = worker
            .retry_saved_torrents(SavedTorrentRetryConfig {
                directories: vec![output_dir.clone()],
                recheck: skip_recheck_config(),
                assessed_at_ms: 1_700_000_000_000,
                ..SavedTorrentRetryConfig::default()
            })
            .await
            .unwrap();

        assert_eq!(2, summary.scanned);
        assert_eq!(1, summary.attempted);
        assert_eq!(1, summary.injected);
        assert_eq!(1, summary.failed);
        assert_eq!(1, summary.deleted);
        assert_eq!(1, summary.kept);
        assert_eq!(1, saved_torrent_count(&output_dir));
    }

    #[tokio::test]
    async fn saved_torrent_retry_prefetches_saved_files_with_bounded_concurrency() {
        let _read_lock = SAVED_TORRENT_READ_TEST_LOCK.lock().await;
        let _read_delay = SavedTorrentReadDelay::new(Duration::from_millis(75));
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("saved-retry-prefetch");
        let output_dir = root.join("output");
        repository
            .upsert_local_item_with_files(&local_item(&root), &[local_file()])
            .await
            .unwrap();
        for index in 0..=SAVED_TORRENT_RETRY_PREFLIGHT_CONCURRENCY {
            let bytes = test_torrent_bytes_with_source(&format!("prefetch-{index}"));
            save_test_torrent(&output_dir, "movie.mkv", &bytes, MediaType::Movie);
        }
        let target = Arc::new(FakeClient::new(descriptor("target", "target")));
        let worker =
            InjectionWorker::new(repository, vec![target.clone() as Arc<dyn InjectionClient>]);

        let summary = worker
            .retry_saved_torrents(SavedTorrentRetryConfig {
                directories: vec![output_dir.clone()],
                recheck: skip_recheck_config(),
                assessed_at_ms: 1_700_000_000_000,
                ..SavedTorrentRetryConfig::default()
            })
            .await
            .unwrap();

        let max_in_flight = SAVED_TORRENT_READ_MAX_IN_FLIGHT.load(Ordering::SeqCst);
        assert_eq!(
            SAVED_TORRENT_RETRY_PREFLIGHT_CONCURRENCY + 1,
            summary.scanned
        );
        assert_eq!(
            SAVED_TORRENT_RETRY_PREFLIGHT_CONCURRENCY + 1,
            summary.attempted
        );
        assert_eq!(
            SAVED_TORRENT_RETRY_PREFLIGHT_CONCURRENCY + 1,
            summary.injected
        );
        assert_eq!(
            SAVED_TORRENT_RETRY_PREFLIGHT_CONCURRENCY + 1,
            summary.deleted
        );
        assert_eq!(
            SAVED_TORRENT_RETRY_PREFLIGHT_CONCURRENCY + 1,
            target.inject_calls.load(Ordering::SeqCst)
        );
        assert!(max_in_flight > 1);
        assert!(max_in_flight <= SAVED_TORRENT_RETRY_PREFLIGHT_CONCURRENCY);
    }

    #[tokio::test]
    async fn saved_torrent_retry_shutdown_keeps_pending_preflight_files() {
        let _read_lock = SAVED_TORRENT_READ_TEST_LOCK.lock().await;
        let _read_delay = SavedTorrentReadDelay::new(Duration::from_millis(200));
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("saved-retry-prefetch-shutdown");
        let output_dir = root.join("output");
        repository
            .upsert_local_item_with_files(&local_item(&root), &[local_file()])
            .await
            .unwrap();
        for index in 0..SAVED_TORRENT_RETRY_PREFLIGHT_CONCURRENCY {
            let bytes = test_torrent_bytes_with_source(&format!("shutdown-{index}"));
            save_test_torrent(&output_dir, "movie.mkv", &bytes, MediaType::Movie);
        }
        let target = Arc::new(FakeClient::new(descriptor("target", "target")));
        let worker = InjectionWorker::new(repository, vec![target as Arc<dyn InjectionClient>]);
        let (shutdown, mut signal) = shutdown_channel();

        let retry = tokio::spawn(async move {
            worker
                .retry_saved_torrents_until_shutdown(
                    SavedTorrentRetryConfig {
                        directories: vec![output_dir],
                        recheck: skip_recheck_config(),
                        assessed_at_ms: 1_700_000_000_000,
                        ..SavedTorrentRetryConfig::default()
                    },
                    &mut signal,
                )
                .await
        });
        wait_for_saved_torrent_reads(1).await;
        shutdown.cancel_now("test shutdown").unwrap();
        let summary = retry.await.unwrap().unwrap();

        assert_eq!(SAVED_TORRENT_RETRY_PREFLIGHT_CONCURRENCY, summary.scanned);
        assert_eq!(0, summary.attempted);
        assert_eq!(SAVED_TORRENT_RETRY_PREFLIGHT_CONCURRENCY, summary.kept);
    }

    #[tokio::test]
    async fn saved_torrent_retry_serializes_duplicate_info_hash_mutation() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("saved-retry-duplicate-info-hash");
        let first_output_dir = root.join("first-output");
        let second_output_dir = root.join("second-output");
        repository
            .upsert_local_item_with_files(&local_item(&root), &[local_file()])
            .await
            .unwrap();
        save_test_torrent(
            &first_output_dir,
            "movie.mkv",
            test_torrent_bytes(),
            MediaType::Movie,
        );
        save_test_torrent(
            &second_output_dir,
            "movie.mkv",
            test_torrent_bytes(),
            MediaType::Movie,
        );
        let target =
            Arc::new(FakeClient::new(descriptor("target", "target")).with_existing_after_inject());
        let worker =
            InjectionWorker::new(repository, vec![target.clone() as Arc<dyn InjectionClient>]);

        let summary = worker
            .retry_saved_torrents(SavedTorrentRetryConfig {
                directories: vec![first_output_dir.clone(), second_output_dir.clone()],
                recheck: skip_recheck_config(),
                assessed_at_ms: 1_700_000_000_000,
                ..SavedTorrentRetryConfig::default()
            })
            .await
            .unwrap();

        assert_eq!(2, summary.scanned);
        assert_eq!(2, summary.attempted);
        assert_eq!(1, summary.injected);
        assert_eq!(1, summary.already_exists);
        assert_eq!(2, summary.deleted);
        assert_eq!(1, target.inject_calls.load(Ordering::SeqCst));
        assert_eq!(0, saved_torrent_count(&first_output_dir));
        assert_eq!(0, saved_torrent_count(&second_output_dir));
    }

    #[tokio::test]
    async fn saved_torrent_retry_filters_info_hash_media_type_before_limit() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("saved-retry-info-hash-limit");
        let output_dir = root.join("output");
        let info_hash = parse_metafile(test_torrent_bytes())
            .unwrap()
            .metafile
            .info_hash()
            .clone();
        let mut movie = local_item(&root.join("a-movie"));
        movie.info_hash = Some(info_hash.clone());
        let mut audio = LocalItem {
            source: LocalItemSource::DataRoot {
                path: root.join("z-audio"),
            },
            media_type: MediaType::Audio,
            title: ItemTitle::new("movie.mkv").unwrap(),
            display_name: DisplayName::new("movie.mkv").unwrap(),
            info_hash: Some(info_hash),
            ..local_item(&root.join("z-audio"))
        };
        audio.path = Some(root.join("z-audio"));
        audio.save_path = Some(root.join("z-audio"));
        repository
            .upsert_local_item_with_files(&movie, &[local_file()])
            .await
            .unwrap();
        repository
            .upsert_local_item_with_files(&audio, &[local_file()])
            .await
            .unwrap();
        save_test_torrent(
            &output_dir,
            "movie.mkv",
            test_torrent_bytes(),
            MediaType::Audio,
        );
        let saved_path = fs::read_dir(&output_dir)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let metadata = parse_torrent_output_filename(
            saved_path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap(),
        )
        .unwrap();
        let candidate = saved_remote_candidate(&metadata, &saved_path).unwrap();
        let lookups = reverse_lookup_candidates_for_media_types(
            &repository,
            &candidate,
            crate::content_filter::ContentFilterContext::ReverseLookup,
            &ReverseLookupConfig {
                max_local_candidates: 1,
                ..ReverseLookupConfig::default()
            },
            &[MediaType::Audio],
        )
        .await
        .unwrap();

        assert_eq!(1, lookups.len());
        assert_eq!(MediaType::Audio, lookups[0].local_item.media_type);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn saved_torrent_reader_rejects_symlink_swaps() {
        let root = unique_temp_dir("saved-retry-symlink");
        let output_dir = root.join("output");
        save_test_torrent(
            &output_dir,
            "movie.mkv",
            test_torrent_bytes(),
            MediaType::Movie,
        );
        let path = fs::read_dir(&output_dir)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let target = root.join("target.torrent");
        fs::write(&target, test_torrent_bytes()).unwrap();
        fs::remove_file(&path).unwrap();
        std::os::unix::fs::symlink(&target, &path).unwrap();

        let result = read_saved_torrent(&path).await;

        assert!(matches!(result, Err(InjectionWorkerError::Io { .. })));
    }

    #[cfg(any(unix, windows))]
    #[tokio::test]
    async fn saved_torrent_delete_keeps_replaced_file() {
        let root = unique_temp_dir("saved-retry-replaced-delete");
        let output_dir = root.join("output");
        save_test_torrent(
            &output_dir,
            "movie.mkv",
            test_torrent_bytes(),
            MediaType::Movie,
        );
        let path = fs::read_dir(&output_dir)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let saved = read_saved_torrent(&path).await.unwrap();
        fs::remove_file(&path).unwrap();
        fs::write(&path, alternate_torrent_bytes()).unwrap();

        let deleted = delete_saved_torrent(&path, saved.identity).await.unwrap();

        assert!(!deleted);
        assert!(path.exists());
    }

    #[tokio::test]
    async fn saved_torrent_delete_tolerates_missing_file() {
        let root = unique_temp_dir("saved-retry-missing-delete");
        let output_dir = root.join("output");
        save_test_torrent(
            &output_dir,
            "movie.mkv",
            test_torrent_bytes(),
            MediaType::Movie,
        );
        let path = fs::read_dir(&output_dir)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let saved = read_saved_torrent(&path).await.unwrap();
        fs::remove_file(&path).unwrap();

        let deleted = delete_saved_torrent(&path, saved.identity).await.unwrap();

        assert!(!deleted);
    }

    #[tokio::test]
    async fn saved_torrent_retry_keeps_file_when_completion_probe_fails() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("saved-retry-probe");
        let output_dir = root.join("output");
        repository
            .upsert_local_item_with_files(&local_item(&root), &[local_file()])
            .await
            .unwrap();
        save_test_torrent(
            &output_dir,
            "movie.mkv",
            test_torrent_bytes(),
            MediaType::Movie,
        );
        save_test_torrent(
            &output_dir,
            "movie.mkv",
            alternate_torrent_bytes(),
            MediaType::Movie,
        );
        let target =
            Arc::new(FakeClient::new(descriptor("target", "target")).with_completion_errors(1));
        let worker =
            InjectionWorker::new(repository, vec![target.clone() as Arc<dyn InjectionClient>]);

        let summary = worker
            .retry_saved_torrents(SavedTorrentRetryConfig {
                directories: vec![output_dir.clone()],
                recheck: skip_recheck_config(),
                assessed_at_ms: 1_700_000_000_000,
                ..SavedTorrentRetryConfig::default()
            })
            .await
            .unwrap();

        assert_eq!(2, summary.scanned);
        assert_eq!(2, summary.attempted);
        assert_eq!(1, summary.failed);
        assert_eq!(2, summary.injected);
        assert_eq!(1, summary.deleted);
        assert_eq!(1, summary.kept);
        assert_eq!(1, saved_torrent_count(&output_dir));
    }

    #[test]
    fn recheck_policy_covers_skip_partial_and_video_disc_rules() {
        let exact = CandidateAssessment {
            decision: MatchDecision::Exact,
            reason: crate::domain::DecisionReason::FileTreeMatched,
            matched_size: Some(ByteSize::new(10)),
            matched_ratio: Some(MatchRatio::new(1.0).unwrap()),
        };
        let partial = CandidateAssessment {
            decision: MatchDecision::Partial,
            reason: crate::domain::DecisionReason::PartialOverlap,
            matched_size: Some(ByteSize::new(5)),
            matched_ratio: Some(MatchRatio::new(0.5).unwrap()),
        };
        let normal = metafile();
        let disc = metafile_with_files(&[("BDMV/STREAM/00001.m2ts", 10)]);

        assert!(
            recheck_resume_plan(&normal, &exact, RecheckResumeConfig::default()).should_recheck
        );
        assert!(
            !recheck_resume_plan(
                &normal,
                &exact,
                RecheckResumeConfig {
                    skip_recheck: true,
                    ..RecheckResumeConfig::default()
                }
            )
            .should_recheck
        );
        assert!(
            recheck_resume_plan(
                &normal,
                &exact,
                RecheckResumeConfig {
                    skip_recheck: false,
                    ..RecheckResumeConfig::default()
                }
            )
            .should_recheck
        );
        assert!(
            recheck_resume_plan(&normal, &partial, RecheckResumeConfig::default()).should_recheck
        );
        assert!(recheck_resume_plan(&disc, &exact, RecheckResumeConfig::default()).should_recheck);
        assert_eq!(
            ByteSize::new(0),
            recheck_resume_plan(
                &disc,
                &partial,
                RecheckResumeConfig {
                    auto_resume_max_download: ByteSize::new(10),
                    ..RecheckResumeConfig::default()
                },
            )
            .max_remaining_bytes
        );
    }

    #[test]
    fn resume_policy_does_not_apply_partial_thresholds_to_exact_or_video_disc_matches() {
        let exact = CandidateAssessment {
            decision: MatchDecision::Exact,
            reason: crate::domain::DecisionReason::FileTreeMatched,
            matched_size: Some(ByteSize::new(1_000)),
            matched_ratio: Some(MatchRatio::new(1.0).unwrap()),
        };
        let partial = CandidateAssessment {
            decision: MatchDecision::Partial,
            reason: crate::domain::DecisionReason::PartialOverlap,
            matched_size: Some(ByteSize::new(850)),
            matched_ratio: Some(MatchRatio::new(0.85).unwrap()),
        };
        let normal = metafile_with_files(&[("movie.mkv", 1_000)]);
        let disc = metafile_with_files(&[("BDMV/STREAM/00001.m2ts", 1_000)]);
        let config = RecheckResumeConfig {
            auto_resume_max_download: ByteSize::new(100),
            min_completion_percent: Some(85.0),
            max_remaining_percent: Some(15.0),
            ..RecheckResumeConfig::default()
        };

        let exact_plan = recheck_resume_plan(&normal, &exact, config);
        assert_eq!(ByteSize::new(0), exact_plan.max_remaining_bytes);
        assert_eq!(None, exact_plan.min_completion_percent);
        assert_eq!(None, exact_plan.max_remaining_percent);
        assert!(!can_resume_with_remaining(
            &normal,
            &exact,
            config,
            exact_plan,
            ByteSize::new(1)
        ));

        let disc_plan = recheck_resume_plan(&disc, &partial, config);
        assert_eq!(ByteSize::new(0), disc_plan.max_remaining_bytes);
        assert_eq!(None, disc_plan.min_completion_percent);
        assert_eq!(None, disc_plan.max_remaining_percent);
        assert!(!can_resume_with_remaining(
            &disc,
            &partial,
            config,
            disc_plan,
            ByteSize::new(1)
        ));
    }

    #[test]
    fn resume_policy_uses_configured_percentage_thresholds() {
        let metafile = metafile_with_files(&[("movie.mkv", 1_000)]);
        let assessment = CandidateAssessment {
            decision: MatchDecision::Partial,
            reason: crate::domain::DecisionReason::PartialOverlap,
            matched_size: Some(ByteSize::new(850)),
            matched_ratio: Some(MatchRatio::new(0.85).unwrap()),
        };
        let config = RecheckResumeConfig {
            auto_resume_max_download: ByteSize::new(100),
            min_completion_percent: Some(85.5),
            max_remaining_percent: Some(20.0),
            ..RecheckResumeConfig::default()
        };
        let plan = recheck_resume_plan(&metafile, &assessment, config);

        assert_eq!(ByteSize::new(100), plan.max_remaining_bytes);
        assert_eq!(Some(85.5), plan.min_completion_percent);
        assert_eq!(Some(20.0), plan.max_remaining_percent);
        assert!(can_resume_with_remaining(
            &metafile,
            &assessment,
            config,
            plan,
            ByteSize::new(200)
        ));
        assert!(!can_resume_with_remaining(
            &metafile,
            &assessment,
            config,
            plan,
            ByteSize::new(201)
        ));
    }

    #[test]
    fn resume_policy_uses_minimum_completion_threshold() {
        let metafile = metafile_with_files(&[("movie.mkv", 1_000)]);
        let assessment = CandidateAssessment {
            decision: MatchDecision::Partial,
            reason: crate::domain::DecisionReason::PartialOverlap,
            matched_size: Some(ByteSize::new(850)),
            matched_ratio: Some(MatchRatio::new(0.85).unwrap()),
        };
        let config = RecheckResumeConfig {
            min_completion_percent: Some(85.5),
            ..RecheckResumeConfig::default()
        };
        let plan = recheck_resume_plan(&metafile, &assessment, config);

        assert!(can_resume_with_remaining(
            &metafile,
            &assessment,
            config,
            plan,
            ByteSize::new(145)
        ));
        assert!(!can_resume_with_remaining(
            &metafile,
            &assessment,
            config,
            plan,
            ByteSize::new(146)
        ));
    }

    #[test]
    fn resume_policy_treats_zero_size_percentage_thresholds_as_complete_only() {
        let metafile = TorrentMetafile::new_unchecked_for_test(
            InfoHash::new("0123456789abcdef0123456789abcdef01234567").unwrap(),
            DisplayName::new("empty").unwrap(),
            vec![
                crate::domain::TorrentFile::new(
                    PathBuf::from("empty.mkv"),
                    ByteSize::new(0),
                    FileIndex::new(0),
                )
                .unwrap(),
            ],
            ByteSize::new(0),
            None,
        );
        let assessment = CandidateAssessment {
            decision: MatchDecision::Partial,
            reason: crate::domain::DecisionReason::PartialOverlap,
            matched_size: Some(ByteSize::new(0)),
            matched_ratio: Some(MatchRatio::new(0.0).unwrap()),
        };
        let config = RecheckResumeConfig {
            min_completion_percent: Some(85.0),
            max_remaining_percent: Some(15.0),
            ..RecheckResumeConfig::default()
        };
        let plan = recheck_resume_plan(&metafile, &assessment, config);

        assert_eq!(ByteSize::new(0), plan.max_remaining_bytes);
        assert!(can_resume_with_remaining(
            &metafile,
            &assessment,
            config,
            plan,
            ByteSize::new(0)
        ));
        assert!(!can_resume_with_remaining(
            &metafile,
            &assessment,
            config,
            plan,
            ByteSize::new(1)
        ));
    }

    #[test]
    fn recheck_config_converts_from_typed_auto_resume_config() {
        let config = crate::config::AutoResumePolicyConfig {
            skip_recheck: true,
            max_remaining_bytes: 123,
            min_completion_percent: Some(85.0),
            max_remaining_percent: Some(15.0),
            ignore_non_relevant_files_to_resume: true,
            non_relevant_max_remaining_bytes: 456,
            piece_slack_multiplier: 3,
            poll_interval_ms: 250,
            max_resume_wait_ms: 500,
            below_threshold_action:
                crate::config::BelowThresholdActionConfig::RejectWithoutInjecting,
        };

        assert_eq!(
            RecheckResumeConfig {
                skip_recheck: true,
                auto_resume_max_download: ByteSize::new(123),
                min_completion_percent: Some(85.0),
                max_remaining_percent: Some(15.0),
                ignore_non_relevant_files_to_resume: true,
                non_relevant_max_remaining: ByteSize::new(456),
                piece_slack_multiplier: 3,
                poll_interval_ms: 250,
                max_resume_wait_ms: 500,
                below_threshold_action: BelowThresholdAction::RejectWithoutInjecting,
            },
            RecheckResumeConfig::from(&config)
        );
    }

    #[test]
    fn resume_policy_allows_irrelevant_file_slack_for_partial_matches() {
        let metafile = TorrentMetafile::new_with_piece_length(
            InfoHash::new("0123456789abcdef0123456789abcdef01234567").unwrap(),
            DisplayName::new("movie").unwrap(),
            vec![
                crate::domain::TorrentFile::new(
                    PathBuf::from("movie.mkv"),
                    ByteSize::new(100),
                    FileIndex::new(0),
                )
                .unwrap(),
                crate::domain::TorrentFile::new(
                    PathBuf::from("extras/sample.nfo"),
                    ByteSize::new(20),
                    FileIndex::new(1),
                )
                .unwrap(),
            ],
            Some(ByteSize::new(5)),
        )
        .unwrap();
        let assessment = CandidateAssessment {
            decision: MatchDecision::Partial,
            reason: crate::domain::DecisionReason::PartialOverlap,
            matched_size: Some(ByteSize::new(100)),
            matched_ratio: Some(MatchRatio::new(0.8).unwrap()),
        };
        let config = RecheckResumeConfig {
            auto_resume_max_download: ByteSize::new(0),
            ignore_non_relevant_files_to_resume: true,
            ..RecheckResumeConfig::default()
        };
        let plan = recheck_resume_plan(&metafile, &assessment, config);

        assert!(can_resume_with_remaining(
            &metafile,
            &assessment,
            config,
            plan,
            ByteSize::new(30)
        ));
        assert!(!can_resume_with_remaining(
            &metafile,
            &assessment,
            config,
            plan,
            ByteSize::new(31)
        ));
    }

    #[test]
    fn resume_policy_uses_configured_irrelevant_file_cap_and_piece_slack() {
        let metafile = TorrentMetafile::new_with_piece_length(
            InfoHash::new("0123456789abcdef0123456789abcdef01234567").unwrap(),
            DisplayName::new("movie").unwrap(),
            vec![
                crate::domain::TorrentFile::new(
                    PathBuf::from("movie.mkv"),
                    ByteSize::new(1_000),
                    FileIndex::new(0),
                )
                .unwrap(),
                crate::domain::TorrentFile::new(
                    PathBuf::from("extras/sample.nfo"),
                    ByteSize::new(20),
                    FileIndex::new(1),
                )
                .unwrap(),
            ],
            Some(ByteSize::new(5)),
        )
        .unwrap();
        let assessment = CandidateAssessment {
            decision: MatchDecision::Partial,
            reason: crate::domain::DecisionReason::PartialOverlap,
            matched_size: Some(ByteSize::new(1_000)),
            matched_ratio: Some(MatchRatio::new(0.98).unwrap()),
        };
        let config = RecheckResumeConfig {
            ignore_non_relevant_files_to_resume: true,
            non_relevant_max_remaining: ByteSize::new(35),
            piece_slack_multiplier: 3,
            ..RecheckResumeConfig::default()
        };
        let plan = recheck_resume_plan(&metafile, &assessment, config);

        assert!(can_resume_with_remaining(
            &metafile,
            &assessment,
            config,
            plan,
            ByteSize::new(35)
        ));
        assert!(!can_resume_with_remaining(
            &metafile,
            &assessment,
            config,
            plan,
            ByteSize::new(36)
        ));
    }

    #[test]
    fn resume_policy_handles_irrelevant_file_overflow_as_not_resumable() {
        let metafile = TorrentMetafile::new_unchecked_for_test(
            InfoHash::new("0123456789abcdef0123456789abcdef01234567").unwrap(),
            DisplayName::new("movie").unwrap(),
            vec![
                crate::domain::TorrentFile::new(
                    PathBuf::from("movie.mkv"),
                    ByteSize::new(10),
                    FileIndex::new(0),
                )
                .unwrap(),
                crate::domain::TorrentFile::new(
                    PathBuf::from("extras/first.nfo"),
                    ByteSize::new(u64::MAX),
                    FileIndex::new(1),
                )
                .unwrap(),
                crate::domain::TorrentFile::new(
                    PathBuf::from("extras/second.nfo"),
                    ByteSize::new(1),
                    FileIndex::new(2),
                )
                .unwrap(),
            ],
            ByteSize::new(10),
            Some(ByteSize::new(5)),
        );
        let assessment = CandidateAssessment {
            decision: MatchDecision::Partial,
            reason: crate::domain::DecisionReason::PartialOverlap,
            matched_size: Some(ByteSize::new(10)),
            matched_ratio: Some(MatchRatio::new(0.5).unwrap()),
        };
        let config = RecheckResumeConfig {
            ignore_non_relevant_files_to_resume: true,
            ..RecheckResumeConfig::default()
        };
        let plan = recheck_resume_plan(&metafile, &assessment, config);

        assert!(!can_resume_with_remaining(
            &metafile,
            &assessment,
            config,
            plan,
            ByteSize::new(30)
        ));
    }

    #[test]
    fn resume_policy_handles_piece_slack_overflow_as_not_resumable() {
        let metafile = TorrentMetafile::new_unchecked_for_test(
            InfoHash::new("0123456789abcdef0123456789abcdef01234567").unwrap(),
            DisplayName::new("movie").unwrap(),
            vec![
                crate::domain::TorrentFile::new(
                    PathBuf::from("movie.mkv"),
                    ByteSize::new(10),
                    FileIndex::new(0),
                )
                .unwrap(),
                crate::domain::TorrentFile::new(
                    PathBuf::from("extras/sample.nfo"),
                    ByteSize::new(1),
                    FileIndex::new(1),
                )
                .unwrap(),
            ],
            ByteSize::new(11),
            Some(ByteSize::new(u64::MAX)),
        );
        let assessment = CandidateAssessment {
            decision: MatchDecision::Partial,
            reason: crate::domain::DecisionReason::PartialOverlap,
            matched_size: Some(ByteSize::new(10)),
            matched_ratio: Some(MatchRatio::new(0.5).unwrap()),
        };
        let config = RecheckResumeConfig {
            ignore_non_relevant_files_to_resume: true,
            ..RecheckResumeConfig::default()
        };
        let plan = recheck_resume_plan(&metafile, &assessment, config);

        assert!(!can_resume_with_remaining(
            &metafile,
            &assessment,
            config,
            plan,
            ByteSize::new(30)
        ));
    }

    struct FakeClient {
        descriptor: TorrentClientDescriptor,
        existing: AtomicBool,
        inject_error: bool,
        has_pending: bool,
        inject_pending: bool,
        resume_pending: bool,
        mark_existing_on_inject: bool,
        first_has_barrier: Option<Arc<Barrier>>,
        has_successes_before_error: AtomicUsize,
        has_errors_remaining: AtomicUsize,
        recheck_errors_remaining: AtomicUsize,
        checking_true_remaining: AtomicUsize,
        completion_errors_remaining: AtomicUsize,
        inject_calls: AtomicUsize,
        has_calls: AtomicUsize,
        recheck_calls: AtomicUsize,
        checking_calls: AtomicUsize,
        remaining_calls: AtomicUsize,
        remaining_bytes: AtomicUsize,
        resume_calls: AtomicUsize,
        save_path_file_exists_at_inject: AtomicUsize,
        replace_save_path_file_on_has: StdMutex<Option<(PathBuf, Vec<u8>)>>,
        replace_path_with_file_on_inject: StdMutex<Option<(PathBuf, Vec<u8>)>>,
        last_pause_for_recheck: StdMutex<Option<bool>>,
        last_save_path: StdMutex<Option<PathBuf>>,
    }

    struct SavedTorrentReadDelay;

    impl SavedTorrentReadDelay {
        fn new(delay: Duration) -> Self {
            SAVED_TORRENT_READ_DELAY_MS.store(
                u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
                Ordering::SeqCst,
            );
            SAVED_TORRENT_READ_IN_FLIGHT.store(0, Ordering::SeqCst);
            SAVED_TORRENT_READ_MAX_IN_FLIGHT.store(0, Ordering::SeqCst);
            Self
        }
    }

    impl Drop for SavedTorrentReadDelay {
        fn drop(&mut self) {
            SAVED_TORRENT_READ_DELAY_MS.store(0, Ordering::SeqCst);
            SAVED_TORRENT_READ_IN_FLIGHT.store(0, Ordering::SeqCst);
            SAVED_TORRENT_READ_MAX_IN_FLIGHT.store(0, Ordering::SeqCst);
        }
    }

    struct FakeRefreshClient {
        descriptor: TorrentClientDescriptor,
        calls: AtomicUsize,
        summary: Option<InventoryRefreshSummary>,
        items: Vec<ScannedLocalItem>,
        stream_items: Vec<ClientInventoryItem>,
        cancel: bool,
        delay: Duration,
        completion_shutdown: Option<ShutdownController>,
        in_flight: Option<Arc<AtomicUsize>>,
        max_in_flight: Option<Arc<AtomicUsize>>,
    }

    impl FakeRefreshClient {
        fn successful(descriptor: TorrentClientDescriptor, persisted_items: usize) -> Self {
            Self {
                descriptor,
                calls: AtomicUsize::new(0),
                summary: Some(InventoryRefreshSummary {
                    scanned_items: persisted_items,
                    persisted_items,
                    pruned_items: 0,
                    scan_failures: Vec::new(),
                }),
                items: Vec::new(),
                stream_items: Vec::new(),
                cancel: false,
                delay: Duration::ZERO,
                completion_shutdown: None,
                in_flight: None,
                max_in_flight: None,
            }
        }

        fn delayed_successful(
            descriptor: TorrentClientDescriptor,
            persisted_items: usize,
            delay: Duration,
        ) -> Self {
            Self {
                delay,
                ..Self::successful(descriptor, persisted_items)
            }
        }

        fn with_completion_shutdown(mut self, controller: ShutdownController) -> Self {
            self.completion_shutdown = Some(controller);
            self
        }

        fn with_in_flight_tracking(
            mut self,
            in_flight: Arc<AtomicUsize>,
            max_in_flight: Arc<AtomicUsize>,
        ) -> Self {
            self.in_flight = Some(in_flight);
            self.max_in_flight = Some(max_in_flight);
            self
        }

        fn failing(descriptor: TorrentClientDescriptor) -> Self {
            Self {
                descriptor,
                calls: AtomicUsize::new(0),
                summary: None,
                items: Vec::new(),
                stream_items: Vec::new(),
                cancel: false,
                delay: Duration::ZERO,
                completion_shutdown: None,
                in_flight: None,
                max_in_flight: None,
            }
        }

        fn streaming(
            descriptor: TorrentClientDescriptor,
            stream_items: Vec<ClientInventoryItem>,
        ) -> Self {
            Self {
                descriptor,
                calls: AtomicUsize::new(0),
                summary: None,
                items: Vec::new(),
                stream_items,
                cancel: false,
                delay: Duration::ZERO,
                completion_shutdown: None,
                in_flight: None,
                max_in_flight: None,
            }
        }

        fn cancelled(descriptor: TorrentClientDescriptor) -> Self {
            Self {
                descriptor,
                calls: AtomicUsize::new(0),
                summary: None,
                items: Vec::new(),
                stream_items: Vec::new(),
                cancel: true,
                delay: Duration::ZERO,
                completion_shutdown: None,
                in_flight: None,
                max_in_flight: None,
            }
        }

        fn persisting(descriptor: TorrentClientDescriptor, items: Vec<ScannedLocalItem>) -> Self {
            Self {
                descriptor,
                calls: AtomicUsize::new(0),
                summary: None,
                items,
                stream_items: Vec::new(),
                cancel: false,
                delay: Duration::ZERO,
                completion_shutdown: None,
                in_flight: None,
                max_in_flight: None,
            }
        }
    }

    impl InjectionClient for FakeRefreshClient {
        fn descriptor(&self) -> &TorrentClientDescriptor {
            &self.descriptor
        }

        fn has_torrent<'a>(&'a self, _info_hash: &'a InfoHash) -> ClientResultFuture<'a, bool> {
            Box::pin(async move { Ok(false) })
        }

        fn inject<'a>(
            &'a self,
            _request: ClientInjectionRequest<'a>,
        ) -> ClientResultFuture<'a, ()> {
            Box::pin(async move { Ok(()) })
        }

        fn recheck<'a>(&'a self, _info_hash: &'a InfoHash) -> ClientResultFuture<'a, ()> {
            Box::pin(async move { Ok(()) })
        }

        fn is_checking<'a>(&'a self, _info_hash: &'a InfoHash) -> ClientResultFuture<'a, bool> {
            Box::pin(async move { Ok(false) })
        }

        fn remaining_bytes<'a>(
            &'a self,
            _info_hash: &'a InfoHash,
        ) -> ClientResultFuture<'a, ByteSize> {
            Box::pin(async move { Ok(ByteSize::new(0)) })
        }

        fn resume<'a>(&'a self, _info_hash: &'a InfoHash) -> ClientResultFuture<'a, ()> {
            Box::pin(async move { Ok(()) })
        }

        fn refresh_inventory<'a>(
            &'a self,
            worker: &'a InventoryRefreshWorker,
            _shutdown: ShutdownSignal,
        ) -> ClientInventoryRefreshFuture<'a> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let summary = self.summary.clone();
            let client = self.descriptor.name.as_str().to_owned();
            let host = self.descriptor.host.clone();
            let items = self.items.clone();
            let stream_items = self.stream_items.clone();
            let cancel = self.cancel;
            let delay = self.delay;
            let completion_shutdown = self.completion_shutdown.clone();
            let in_flight = self.in_flight.clone();
            let max_in_flight = self.max_in_flight.clone();
            Box::pin(async move {
                let active = in_flight.as_ref().map(|in_flight| {
                    let active = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                    if let Some(max_in_flight) = &max_in_flight {
                        update_max_atomic(max_in_flight, active);
                    }
                    active
                });
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
                if active.is_some()
                    && let Some(in_flight) = &in_flight
                {
                    in_flight.fetch_sub(1, Ordering::SeqCst);
                }
                if cancel {
                    return Err(InventoryRefreshError::Client {
                        source: TorrentClientError::Cancelled {
                            client: client.clone(),
                            message: "shutdown requested".to_owned(),
                        },
                    });
                }
                if !items.is_empty() {
                    return worker.refresh_client_items(host, &items).await;
                }
                if !stream_items.is_empty() {
                    let (sender, receiver) = mpsc::channel(1);
                    let send = async move {
                        for item in stream_items {
                            if sender
                                .send(ClientInventoryMessage::Item(item))
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }
                        drop(sender.send(ClientInventoryMessage::Finished).await);
                    };
                    let refresh = worker.refresh_client_inventory_receiver(host, receiver);
                    let ((), result) = tokio::join!(send, refresh);
                    return result;
                }
                if let Some(controller) = completion_shutdown {
                    controller.cancel_now("test shutdown").unwrap();
                }
                summary.ok_or_else(|| {
                    TorrentClientError::Unavailable {
                        client,
                        retry_after_ms: None,
                        message: "offline".to_owned(),
                    }
                    .into()
                })
            })
        }
    }

    impl FakeClient {
        fn new(descriptor: TorrentClientDescriptor) -> Self {
            Self {
                descriptor,
                existing: AtomicBool::new(false),
                inject_error: false,
                has_pending: false,
                inject_pending: false,
                resume_pending: false,
                mark_existing_on_inject: false,
                first_has_barrier: None,
                has_successes_before_error: AtomicUsize::new(0),
                has_errors_remaining: AtomicUsize::new(0),
                recheck_errors_remaining: AtomicUsize::new(0),
                checking_true_remaining: AtomicUsize::new(0),
                completion_errors_remaining: AtomicUsize::new(0),
                inject_calls: AtomicUsize::new(0),
                has_calls: AtomicUsize::new(0),
                recheck_calls: AtomicUsize::new(0),
                checking_calls: AtomicUsize::new(0),
                remaining_calls: AtomicUsize::new(0),
                remaining_bytes: AtomicUsize::new(0),
                resume_calls: AtomicUsize::new(0),
                save_path_file_exists_at_inject: AtomicUsize::new(0),
                replace_save_path_file_on_has: StdMutex::new(None),
                replace_path_with_file_on_inject: StdMutex::new(None),
                last_pause_for_recheck: StdMutex::new(None),
                last_save_path: StdMutex::new(None),
            }
        }

        fn with_existing(self, existing: bool) -> Self {
            self.existing.store(existing, Ordering::SeqCst);
            self
        }

        fn with_first_has_barrier(mut self, barrier: Arc<Barrier>) -> Self {
            self.first_has_barrier = Some(barrier);
            self
        }

        const fn with_existing_after_inject(mut self) -> Self {
            self.mark_existing_on_inject = true;
            self
        }

        const fn with_inject_error(mut self) -> Self {
            self.inject_error = true;
            self
        }

        const fn with_pending_has(mut self) -> Self {
            self.has_pending = true;
            self
        }

        const fn with_pending_inject(mut self) -> Self {
            self.inject_pending = true;
            self
        }

        const fn with_pending_resume(mut self) -> Self {
            self.resume_pending = true;
            self
        }

        fn with_has_errors(self, count: usize) -> Self {
            self.has_errors_remaining.store(count, Ordering::SeqCst);
            self
        }

        fn with_has_successes_before_error(self, count: usize) -> Self {
            self.has_successes_before_error
                .store(count, Ordering::SeqCst);
            self
        }

        fn with_recheck_errors(self, count: usize) -> Self {
            self.recheck_errors_remaining.store(count, Ordering::SeqCst);
            self
        }

        fn with_checking_true(self, count: usize) -> Self {
            self.checking_true_remaining.store(count, Ordering::SeqCst);
            self
        }

        fn with_completion_errors(self, count: usize) -> Self {
            self.completion_errors_remaining
                .store(count, Ordering::SeqCst);
            self
        }

        fn with_remaining_bytes(self, bytes: usize) -> Self {
            self.remaining_bytes.store(bytes, Ordering::SeqCst);
            self
        }

        fn with_replace_save_path_file_on_has(self, path: PathBuf, contents: Vec<u8>) -> Self {
            *self.replace_save_path_file_on_has.lock().unwrap() = Some((path, contents));
            self
        }

        fn with_replace_path_with_file_on_inject(self, path: PathBuf, contents: Vec<u8>) -> Self {
            *self.replace_path_with_file_on_inject.lock().unwrap() = Some((path, contents));
            self
        }

        fn last_pause_for_recheck(&self) -> Option<bool> {
            *self.last_pause_for_recheck.lock().unwrap()
        }

        fn last_save_path(&self) -> Option<PathBuf> {
            self.last_save_path.lock().unwrap().clone()
        }
    }

    impl InjectionClient for FakeClient {
        fn descriptor(&self) -> &TorrentClientDescriptor {
            &self.descriptor
        }

        fn has_torrent<'a>(&'a self, _info_hash: &'a InfoHash) -> ClientResultFuture<'a, bool> {
            let has_call = self.has_calls.fetch_add(1, Ordering::SeqCst);
            let first_has_barrier = (has_call == 0)
                .then(|| self.first_has_barrier.clone())
                .flatten();
            if let Some((path, contents)) =
                self.replace_save_path_file_on_has.lock().unwrap().take()
            {
                match std::fs::remove_file(&path) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => panic!("remove prepared link before test replacement: {error}"),
                }
                std::fs::write(path, contents).unwrap();
            }
            if self.has_pending {
                return Box::pin(
                    async move { pending::<Result<bool, TorrentClientError>>().await },
                );
            }
            let succeed_before_error = self
                .has_successes_before_error
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                    current.checked_sub(1)
                })
                .is_ok();
            let error = (!succeed_before_error)
                .then(|| {
                    self.has_errors_remaining
                        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                            current.checked_sub(1)
                        })
                        .is_ok()
                        .then(|| TorrentClientError::Unavailable {
                            client: self.descriptor.name.as_str().to_owned(),
                            retry_after_ms: Some(1_000),
                            message: "offline".to_owned(),
                        })
                })
                .flatten();
            let existing = &self.existing;
            Box::pin(async move {
                if let Some(barrier) = first_has_barrier {
                    barrier.wait().await;
                }
                if let Some(error) = error {
                    Err(error)
                } else {
                    Ok(existing.load(Ordering::SeqCst))
                }
            })
        }

        fn inject<'a>(&'a self, request: ClientInjectionRequest<'a>) -> ClientResultFuture<'a, ()> {
            self.inject_calls.fetch_add(1, Ordering::SeqCst);
            *self.last_pause_for_recheck.lock().unwrap() = Some(request.pause_for_recheck);
            *self.last_save_path.lock().unwrap() = request.save_path.map(Path::to_path_buf);
            if let Some((path, contents)) =
                self.replace_path_with_file_on_inject.lock().unwrap().take()
            {
                match std::fs::remove_dir_all(&path) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => panic!("remove directory before test replacement: {error}"),
                }
                std::fs::write(path, contents).unwrap();
            }
            if request
                .save_path
                .is_some_and(|save_path| save_path.join("movie.mkv").exists())
            {
                self.save_path_file_exists_at_inject
                    .fetch_add(1, Ordering::SeqCst);
            }
            if self.inject_pending {
                return Box::pin(async move { pending::<Result<(), TorrentClientError>>().await });
            }
            let error = self.inject_error.then(|| TorrentClientError::Unavailable {
                client: self.descriptor.name.as_str().to_owned(),
                retry_after_ms: Some(1_000),
                message: "offline".to_owned(),
            });
            let existing = self.mark_existing_on_inject.then_some(&self.existing);
            Box::pin(async move {
                if let Some(error) = error {
                    Err(error)
                } else {
                    if let Some(existing) = existing {
                        existing.store(true, Ordering::SeqCst);
                    }
                    Ok(())
                }
            })
        }

        fn recheck<'a>(&'a self, _info_hash: &'a InfoHash) -> ClientResultFuture<'a, ()> {
            self.recheck_calls.fetch_add(1, Ordering::SeqCst);
            let error = self
                .recheck_errors_remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                    current.checked_sub(1)
                })
                .is_ok()
                .then(|| TorrentClientError::Unavailable {
                    client: self.descriptor.name.as_str().to_owned(),
                    retry_after_ms: Some(1_000),
                    message: "recheck unavailable".to_owned(),
                });
            Box::pin(async move {
                if let Some(error) = error {
                    Err(error)
                } else {
                    Ok(())
                }
            })
        }

        fn is_checking<'a>(&'a self, _info_hash: &'a InfoHash) -> ClientResultFuture<'a, bool> {
            self.checking_calls.fetch_add(1, Ordering::SeqCst);
            let checking = self
                .checking_true_remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                    current.checked_sub(1)
                })
                .is_ok();
            Box::pin(async move { Ok(checking) })
        }

        fn remaining_bytes<'a>(
            &'a self,
            _info_hash: &'a InfoHash,
        ) -> ClientResultFuture<'a, ByteSize> {
            self.remaining_calls.fetch_add(1, Ordering::SeqCst);
            let error = self
                .completion_errors_remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                    current.checked_sub(1)
                })
                .is_ok()
                .then(|| TorrentClientError::Unavailable {
                    client: self.descriptor.name.as_str().to_owned(),
                    retry_after_ms: Some(1_000),
                    message: "offline".to_owned(),
                });
            Box::pin(async move {
                if let Some(error) = error {
                    Err(error)
                } else {
                    Ok(ByteSize::new(
                        u64::try_from(self.remaining_bytes.load(Ordering::SeqCst)).unwrap(),
                    ))
                }
            })
        }

        fn resume<'a>(&'a self, _info_hash: &'a InfoHash) -> ClientResultFuture<'a, ()> {
            self.resume_calls.fetch_add(1, Ordering::SeqCst);
            if self.resume_pending {
                return Box::pin(async move { pending::<Result<(), TorrentClientError>>().await });
            }
            Box::pin(async move { Ok(()) })
        }
    }

    async fn wait_for_calls(counter: &AtomicUsize, expected: usize) {
        for _ in 0..100 {
            if counter.load(Ordering::SeqCst) >= expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!(
            "timed out waiting for at least {expected} calls; saw {}",
            counter.load(Ordering::SeqCst)
        );
    }

    fn client_episode(
        client_host: ClientHost,
        hash: &str,
        title: &str,
        relative_path: &str,
    ) -> ScannedLocalItem {
        crate::inventory_refresh::ClientInventoryItem {
            client_host,
            info_hash: InfoHash::new(hash).unwrap(),
            display_name: DisplayName::new(title).unwrap(),
            media_type: MediaType::Movie,
            save_path: PathBuf::from("/downloads"),
            files: vec![
                crate::domain::TorrentFile::new(
                    PathBuf::from(relative_path),
                    ByteSize::new(10),
                    FileIndex::new(0),
                )
                .unwrap(),
            ],
        }
        .into_scanned()
        .unwrap()
    }

    fn client_inventory_items(
        client_host: ClientHost,
        hash_prefix: &str,
        count: usize,
    ) -> Vec<ClientInventoryItem> {
        (1..=count)
            .map(|index| {
                client_inventory_item(
                    client_host.clone(),
                    &format!("{hash_prefix}{index:039x}"),
                    &format!("Client Movie {hash_prefix}-{index}"),
                    &format!("client-{hash_prefix}-{index}.mkv"),
                )
            })
            .collect()
    }

    fn client_inventory_item(
        client_host: ClientHost,
        hash: &str,
        title: &str,
        relative_path: &str,
    ) -> ClientInventoryItem {
        ClientInventoryItem {
            client_host,
            info_hash: InfoHash::new(hash).unwrap(),
            display_name: DisplayName::new(title).unwrap(),
            media_type: MediaType::Movie,
            save_path: PathBuf::from("/downloads"),
            files: vec![
                crate::domain::TorrentFile::new(
                    PathBuf::from(relative_path),
                    ByteSize::new(10),
                    FileIndex::new(0),
                )
                .unwrap(),
            ],
        }
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

    async fn assert_virtual_season(repository: &Repository, title: &str, files: usize) {
        let seasons = repository
            .local_items_by_media_type(MediaType::SeasonPack, 10)
            .await
            .unwrap();
        let episodes = repository
            .local_items_by_media_type(MediaType::Episode, 10)
            .await
            .unwrap();
        let season_titles = seasons
            .iter()
            .map(|item| item.title.as_str().to_owned())
            .collect::<Vec<_>>();
        let episode_titles = episodes
            .iter()
            .map(|item| item.title.as_str().to_owned())
            .collect::<Vec<_>>();
        let item = seasons
            .into_iter()
            .find(|item| {
                item.title.as_str() == title
                    && matches!(item.source, LocalItemSource::Virtual { .. })
            })
            .unwrap_or_else(|| {
                panic!(
                    "missing virtual season {title}; saw seasons {season_titles:?}; episodes {episode_titles:?}"
                )
            });
        let item_files = repository
            .local_files_for_item(item.id.unwrap(), 10)
            .await
            .unwrap();

        assert_eq!(files, item_files.len());
    }

    async fn persisted_inputs(
        repository: &Repository,
        root: &Path,
    ) -> (LocalItem, RemoteCandidate, RemoteCandidateId) {
        let mut local = local_item(root);
        let item_id = repository
            .upsert_local_item_with_files(&local, &[])
            .await
            .unwrap();
        local.id = Some(item_id);
        let candidate = remote_candidate();
        let candidate_id = repository
            .upsert_remote_candidate(&candidate)
            .await
            .unwrap();
        (local, candidate, candidate_id)
    }

    fn request(
        local_item: LocalItem,
        candidate: RemoteCandidate,
        candidate_id: RemoteCandidateId,
        root: &Path,
    ) -> InjectionRequest {
        InjectionRequest {
            local_item,
            local_files: vec![local_file()],
            candidate,
            candidate_id,
            metafile: metafile(),
            torrent_bytes: b"torrent bytes".to_vec(),
            assessment: CandidateAssessment {
                decision: MatchDecision::Exact,
                reason: crate::domain::DecisionReason::FileTreeMatched,
                matched_size: Some(ByteSize::new(10)),
                matched_ratio: Some(MatchRatio::new(1.0).unwrap()),
            },
            assessed_at_ms: 1_700_000_000_000,
            output_dir: root.join("output"),
            link_dirs: Vec::new(),
            link_type: None,
            flat_linking: false,
            recheck: RecheckResumeConfig::default(),
        }
    }

    fn skip_recheck_config() -> RecheckResumeConfig {
        RecheckResumeConfig {
            skip_recheck: true,
            ..RecheckResumeConfig::default()
        }
    }

    fn descriptor(name: &str, host: &str) -> TorrentClientDescriptor {
        TorrentClientDescriptor {
            name: DisplayName::new(name).unwrap(),
            kind: crate::domain::TorrentClientKind::Qbittorrent,
            host: ClientHost::new(host).unwrap(),
            url: format!("http://{host}:8080"),
            default_save_path: PathBuf::from("/downloads"),
            readonly: false,
            capabilities: TorrentClientCapabilities::for_kind(
                crate::domain::TorrentClientKind::Qbittorrent,
            ),
        }
    }

    fn local_item(root: &Path) -> LocalItem {
        LocalItem {
            id: None,
            source: LocalItemSource::DataRoot {
                path: root.to_path_buf(),
            },
            title: ItemTitle::new("movie.mkv").unwrap(),
            display_name: DisplayName::new("movie.mkv").unwrap(),
            media_type: crate::domain::MediaType::Movie,
            info_hash: None,
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

    fn metafile() -> TorrentMetafile {
        metafile_with_files(&[("movie.mkv", 10)])
    }

    fn metafile_with_files(files: &[(&str, u64)]) -> TorrentMetafile {
        TorrentMetafile::new(
            InfoHash::new("0123456789abcdef0123456789abcdef01234567").unwrap(),
            DisplayName::new("movie.mkv").unwrap(),
            files
                .iter()
                .enumerate()
                .map(|(index, (path, size))| {
                    crate::domain::TorrentFile::new(
                        PathBuf::from(path),
                        ByteSize::new(*size),
                        FileIndex::new(u32::try_from(index).unwrap()),
                    )
                    .unwrap()
                })
                .collect(),
        )
        .unwrap()
    }

    fn remote_candidate() -> RemoteCandidate {
        RemoteCandidate {
            id: None,
            indexer_id: crate::domain::IndexerId::new(1).unwrap(),
            guid: CandidateGuid::new("guid-1").unwrap(),
            download_url: DownloadUrl::new("https://indexer.example/download/1").unwrap(),
            title: ItemTitle::new("movie.mkv").unwrap(),
            tracker: TrackerName::new("tracker.example").unwrap(),
            size: Some(ByteSize::new(10)),
            published_at_ms: None,
            info_hash: Some(InfoHash::new("0123456789abcdef0123456789abcdef01234567").unwrap()),
            torrent_cache_path: None,
        }
    }

    fn test_torrent_bytes() -> &'static [u8] {
        b"d8:announce14:http://tracker4:infod6:lengthi10e4:name9:movie.mkv12:piece lengthi10e6:pieces20:aaaaaaaaaaaaaaaaaaaaee"
    }

    fn alternate_torrent_bytes() -> &'static [u8] {
        b"d8:announce14:http://tracker4:infod6:lengthi10e4:name9:movie.mkv12:piece lengthi10e6:pieces20:bbbbbbbbbbbbbbbbbbbbee"
    }

    fn test_torrent_bytes_with_source(source: &str) -> Vec<u8> {
        format!(
            "d8:announce14:http://tracker4:infod6:lengthi10e4:name9:movie.mkv12:piece lengthi10e6:pieces20:aaaaaaaaaaaaaaaaaaaa6:source{}:{source}ee",
            source.len()
        )
        .into_bytes()
    }

    fn save_test_torrent(output_dir: &Path, title: &str, bytes: &[u8], media_type: MediaType) {
        let parsed = parse_metafile(bytes).unwrap();
        let mut candidate = remote_candidate();
        candidate.title = ItemTitle::new(title).unwrap();
        candidate.info_hash = Some(parsed.metafile.info_hash().clone());
        save_candidate_torrent(
            output_dir,
            &candidate_output_metadata(media_type, &candidate, &parsed.metafile),
            bytes,
        )
        .unwrap();
    }

    async fn wait_for_saved_torrent_reads(minimum: usize) {
        let deadline = Instant::now() + Duration::from_secs(1);
        while SAVED_TORRENT_READ_IN_FLIGHT.load(Ordering::SeqCst) < minimum {
            assert!(
                Instant::now() < deadline,
                "saved torrent reads did not start before timeout"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    fn unique_temp_dir(label: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir()
            .canonicalize()
            .unwrap_or_else(|_| std::env::temp_dir())
            .join(format!("sporos-{label}-{}-{unique}", std::process::id()));
        fs::create_dir_all(&path).unwrap();
        path
    }

    fn saved_torrent_count(path: &Path) -> usize {
        fs::read_dir(path)
            .map(|entries| entries.count())
            .unwrap_or(0)
    }
}
