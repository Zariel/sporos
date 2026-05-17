#![expect(
    clippy::unreachable,
    reason = "mechanical clippy gate enablement leaves state-machine assertion cleanup to a linked lint-class bead"
)]

use std::fmt;
use std::fs::{File, OpenOptions};
use std::future::Future;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
#[cfg(windows)]
use std::os::windows::fs::MetadataExt;

use tokio::sync::{Mutex, MutexGuard};
use tracing::warn;

use crate::actions::{
    LinkActionError, LinkDirOptions, LinkFilesOptions, LinkType, SaveTorrentError,
    candidate_output_metadata, cleanup_created_roots, link_destination_dir, link_metafile_files,
    save_candidate_torrent, select_link_dir,
};
use crate::clients::TorrentClientDescriptor;
use crate::domain::{
    ByteSize, CandidateAssessment, CandidateGuid, DependencyName, DependencyState, DownloadUrl,
    IndexerId, InfoHash, InjectionOutcome, ItemTitle, LocalFile, LocalItem, MatchDecision,
    ReasonText, RemoteCandidate, RemoteCandidateId, TorrentMetafile, TrackerName,
    checked_file_total,
};
use crate::errors::{
    ClassifyFailure, DatabaseError, FailureClass, TorrentClientError, TorrentParseError,
};
use crate::inventory_refresh::{
    InventoryRefreshError, InventoryRefreshSummary, InventoryRefreshWorker,
};
use crate::matching::{
    CandidateAssessmentConfig, FileTreeMatchConfig, PersistedCandidateAssessment,
    ReverseLookupConfig, assess_and_persist_candidate, reverse_lookup_candidates_for_media_types,
};
use crate::persistence::repository::Repository;
use crate::persistence::torrent_cache::{TorrentOutputMetadata, parse_torrent_output_filename};
use crate::runtime::queue::{BoundedWorkQueue, QueueKind, WorkReceiver, bounded_work_queue};
use crate::runtime::shutdown::{ShutdownPhase, ShutdownSignal, shutdown_channel};
use crate::torrent::parse_metafile;

const MAX_SAVED_TORRENT_BYTES: u64 = 32 * 1024 * 1024;

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
        worker: &'a InventoryRefreshWorker,
        shutdown: ShutdownSignal,
    ) -> ClientInventoryRefreshFuture<'a> {
        Box::pin(async move {
            let descriptor = self.descriptor();
            let _ = (worker, shutdown);
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
    pub saved_for_retry: bool,
    pub linked_files: usize,
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
    TorrentParse(TorrentParseError),
    Io {
        operation: &'static str,
        path: PathBuf,
        source: std::io::Error,
    },
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct RecheckResumeConfig {
    pub skip_recheck: bool,
    pub auto_resume_max_download: ByteSize,
    pub ignore_non_relevant_files_to_resume: bool,
    pub poll_interval_ms: u64,
    pub max_resume_wait_ms: u64,
}

impl Default for RecheckResumeConfig {
    fn default() -> Self {
        Self {
            skip_recheck: false,
            auto_resume_max_download: ByteSize::new(0),
            ignore_non_relevant_files_to_resume: false,
            poll_interval_ms: 5_000,
            max_resume_wait_ms: 60 * 60 * 1_000,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct RecheckResumePlan {
    pub should_recheck: bool,
    pub max_remaining_bytes: ByteSize,
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
}

impl fmt::Debug for InjectionWorker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InjectionWorker")
            .field("client_count", &self.clients.len())
            .finish_non_exhaustive()
    }
}

impl InjectionWorker {
    pub fn new(repository: Repository, clients: Vec<Arc<dyn InjectionClient>>) -> Self {
        Self {
            repository,
            clients,
            mutation_lock: Arc::new(Mutex::new(())),
        }
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
        let mut summaries = Vec::with_capacity(self.clients.len());
        let mut refreshed_client_hosts = Vec::with_capacity(self.clients.len());
        let mut last_error = None;
        let client_worker = worker.without_client_post_refresh_work();
        for client in &self.clients {
            if shutdown.state().phase != ShutdownPhase::Running {
                let error = InventoryRefreshError::Client {
                    source: TorrentClientError::Cancelled {
                        client: client.descriptor().name.as_str().to_owned(),
                        message: "shutdown requested".to_owned(),
                    },
                };
                if !refreshed_client_hosts.is_empty() {
                    worker
                        .refresh_virtual_seasons_after_client_batch(&refreshed_client_hosts)
                        .await?;
                }
                return Err(error);
            }
            match client
                .refresh_inventory(&client_worker, shutdown.clone())
                .await
            {
                Ok(summary) => {
                    summaries.push(summary);
                    refreshed_client_hosts.push(client.descriptor().host.clone());
                }
                Err(
                    error @ InventoryRefreshError::Client {
                        source: TorrentClientError::Cancelled { .. },
                    },
                ) => {
                    if !refreshed_client_hosts.is_empty() {
                        worker
                            .refresh_virtual_seasons_after_client_batch(&refreshed_client_hosts)
                            .await?;
                    }
                    return Err(error);
                }
                Err(error) => {
                    warn!(
                        client = %client.descriptor().name,
                        error = %error,
                        "client inventory refresh failed"
                    );
                    last_error = Some(error);
                }
            }
        }
        if summaries.is_empty()
            && let Some(error) = last_error
        {
            return Err(error);
        }
        if !summaries.is_empty() {
            worker
                .refresh_virtual_seasons_after_client_batch(&refreshed_client_hosts)
                .await?;
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
            self.save_for_retry(&request).await?;
            return Ok(InjectionWorkResult {
                outcome: InjectionOutcome::Saved,
                target_client: Some(target_name),
                saved_for_retry: true,
                linked_files: 0,
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
                    saved_for_retry: false,
                    linked_files: 0,
                });
            }
            ExistingClientLookup::NotFound => {}
            ExistingClientLookup::Shutdown => {
                self.save_for_retry(&request).await?;
                return Ok(InjectionWorkResult {
                    outcome: InjectionOutcome::Saved,
                    target_client: Some(target_name),
                    saved_for_retry: true,
                    linked_files: 0,
                });
            }
        }

        if should_stop() {
            self.save_for_retry(&request).await?;
            return Ok(InjectionWorkResult {
                outcome: InjectionOutcome::Saved,
                target_client: Some(target_name),
                saved_for_retry: true,
                linked_files: 0,
            });
        }

        let link_result = self.prepare_links(&request).await?;
        if matches!(link_result, LinkPreparation::SourceIncomplete) {
            self.save_for_retry(&request).await?;
            return Ok(InjectionWorkResult {
                outcome: InjectionOutcome::SourceIncomplete,
                target_client: Some(target_name),
                saved_for_retry: true,
                linked_files: 0,
            });
        }
        let LinkPreparation::Ready {
            save_path,
            created_roots,
            linked_files,
        } = link_result
        else {
            unreachable!("source incomplete handled above");
        };

        let recheck_plan =
            recheck_resume_plan(&request.metafile, &request.assessment, request.recheck);
        let pause_for_recheck = recheck_plan.should_recheck;
        if should_stop() {
            self.save_for_retry(&request).await?;
            cleanup_prepared_roots(&created_roots)?;
            return Ok(InjectionWorkResult {
                outcome: InjectionOutcome::Saved,
                target_client: Some(target_name),
                saved_for_retry: true,
                linked_files,
            });
        }
        let mutation_result = {
            let Some(_guard) = lock_until_shutdown(&self.mutation_lock, shutdown).await else {
                self.save_for_retry(&request).await?;
                cleanup_prepared_roots(&created_roots)?;
                return Ok(InjectionWorkResult {
                    outcome: InjectionOutcome::Saved,
                    target_client: Some(target_name),
                    saved_for_retry: true,
                    linked_files,
                });
            };
            if should_stop() {
                InjectionMutationResult::SavedForShutdown
            } else {
                match client_call_until_shutdown(shutdown, || {
                    target.has_torrent(request.metafile.info_hash())
                })
                .await
                {
                    ClientCall::Shutdown => InjectionMutationResult::SavedForShutdown,
                    ClientCall::Completed(Ok(true)) => InjectionMutationResult::AlreadyExists,
                    ClientCall::Completed(Ok(false)) => {
                        match client_call_until_shutdown(shutdown, || {
                            target.inject(ClientInjectionRequest {
                                info_hash: request.metafile.info_hash(),
                                torrent_bytes: &request.torrent_bytes,
                                save_path: save_path.as_deref(),
                                pause_for_recheck,
                            })
                        })
                        .await
                        {
                            ClientCall::Shutdown => InjectionMutationResult::SavedForShutdown,
                            ClientCall::Completed(result) => {
                                InjectionMutationResult::Injected(result)
                            }
                        }
                    }
                    ClientCall::Completed(Err(error)) => {
                        InjectionMutationResult::PrecheckFailed(error)
                    }
                }
            }
        };

        match mutation_result {
            InjectionMutationResult::SavedForShutdown => {
                self.save_for_retry(&request).await?;
                cleanup_prepared_roots(&created_roots)?;
                Ok(InjectionWorkResult {
                    outcome: InjectionOutcome::Saved,
                    target_client: Some(target_name),
                    saved_for_retry: true,
                    linked_files,
                })
            }
            InjectionMutationResult::AlreadyExists => {
                self.record_client_health(target.descriptor(), true, None, request.assessed_at_ms)
                    .await?;
                let mut saved_for_retry = false;
                if linked_files > 0 || (from_saved_retry && recheck_plan.should_recheck) {
                    let recheck_plan = RecheckResumePlan {
                        should_recheck: true,
                        ..recheck_plan
                    };
                    let resume_outcome = self
                        .run_recheck_resume(target.as_ref(), &request, recheck_plan, shutdown)
                        .await?;
                    if resume_outcome == ResumeLoopOutcome::StillChecking {
                        self.save_for_retry(&request).await?;
                        saved_for_retry = true;
                    }
                }
                Ok(InjectionWorkResult {
                    outcome: InjectionOutcome::AlreadyExists,
                    target_client: Some(target_name),
                    saved_for_retry,
                    linked_files,
                })
            }
            InjectionMutationResult::Injected(Ok(())) => {
                self.record_client_health(target.descriptor(), true, None, request.assessed_at_ms)
                    .await?;
                if recheck_plan.should_recheck {
                    let save_result = self.save_for_retry(&request).await;
                    let resume_result = self
                        .run_recheck_resume(target.as_ref(), &request, recheck_plan, shutdown)
                        .await;
                    save_result?;
                    resume_result?;
                }
                Ok(InjectionWorkResult {
                    outcome: InjectionOutcome::Injected,
                    target_client: Some(target_name),
                    saved_for_retry: pause_for_recheck,
                    linked_files,
                })
            }
            InjectionMutationResult::Injected(Err(error)) => {
                self.save_for_retry(&request).await?;
                let cleanup_result = cleanup_prepared_roots(&created_roots);
                self.record_client_health(
                    target.descriptor(),
                    false,
                    Some(&error),
                    request.assessed_at_ms,
                )
                .await?;
                cleanup_result?;
                Ok(InjectionWorkResult {
                    outcome: InjectionOutcome::Failed,
                    target_client: Some(target_name),
                    saved_for_retry: true,
                    linked_files,
                })
            }
            InjectionMutationResult::PrecheckFailed(error) => {
                let cleanup_result = cleanup_prepared_roots(&created_roots);
                self.record_client_health(
                    target.descriptor(),
                    false,
                    Some(&error),
                    request.assessed_at_ms,
                )
                .await?;
                cleanup_result?;
                Err(error.into())
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
            let paths =
                saved_torrent_paths(directory, config.max_saved_torrents - summary.scanned).await?;
            for path in paths {
                if summary.scanned >= config.max_saved_torrents || should_stop() {
                    return Ok(summary);
                }
                summary.scanned += 1;
                self.retry_saved_torrent(
                    directory,
                    &path,
                    &config,
                    &mut summary,
                    &mut should_stop,
                    shutdown,
                )
                .await?;
            }
        }

        Ok(summary)
    }

    async fn retry_saved_torrent<F>(
        &self,
        directory: &Path,
        path: &Path,
        config: &SavedTorrentRetryConfig,
        summary: &mut SavedTorrentRetrySummary,
        should_stop: &mut F,
        shutdown: Option<&ShutdownSignal>,
    ) -> Result<(), InjectionWorkerError>
    where
        F: FnMut() -> bool,
    {
        if should_stop() {
            summary.kept += 1;
            return Ok(());
        }
        let Some(file_name) = path.file_name().and_then(|value| value.to_str()) else {
            summary.skipped += 1;
            summary.kept += 1;
            return Ok(());
        };
        let metadata = match parse_torrent_output_filename(file_name) {
            Ok(metadata) if !metadata.cached => metadata,
            Ok(_) | Err(_) => {
                summary.skipped += 1;
                summary.kept += 1;
                return Ok(());
            }
        };
        let saved = match read_saved_torrent(path).await {
            Ok(saved) if saved.parsed.metafile.info_hash() == &metadata.info_hash => saved,
            Ok(_) => {
                summary.failed += 1;
                summary.kept += 1;
                return Ok(());
            }
            Err(InjectionWorkerError::Io { .. } | InjectionWorkerError::TorrentParse(_)) => {
                summary.failed += 1;
                summary.kept += 1;
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        let candidate = saved_remote_candidate(&metadata, path)?;
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
                &lookup.local_item,
                &lookup.local_files,
                lookup.local_files_truncated,
                &candidate,
                &[],
                config.assessed_at_ms,
                &config.reverse_lookup.assessment,
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
                output_dir: directory.to_path_buf(),
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
            match result.outcome {
                InjectionOutcome::SourceIncomplete => {
                    summary.kept += 1;
                    continue;
                }
                InjectionOutcome::Injected | InjectionOutcome::AlreadyExists => {
                    match self
                        .delete_saved_torrent_if_complete(
                            path,
                            file_name,
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
                created_roots: Vec::new(),
                linked_files: 0,
            });
        };
        if request.link_dirs.is_empty() {
            return Ok(LinkPreparation::Ready {
                save_path: source_root(&request.local_item).map(Path::to_path_buf),
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
                select_link_dir(&source_root, &link_dirs, LinkDirOptions::new(link_type))?;
            let destination_dir = link_destination_dir(&link_dir, &tracker, flat_linking)?;
            let outcome = match link_metafile_files(
                &source_root,
                &local_files,
                &metafile_files,
                decision,
                &destination_dir,
                LinkFilesOptions::new(link_type),
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
            .record_dependency_health("client", &name, &state, checked_at_ms)
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
            .find(|client| client.descriptor().name.as_str() == name.as_str())
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
                if sleep_between_resume_polls(request.recheck, shutdown).await {
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

fn cleanup_prepared_roots(roots: &[PathBuf]) -> Result<(), InjectionWorkerError> {
    cleanup_created_roots(roots).map_err(|error| {
        warn!(
            root_count = roots.len(),
            error = %error,
            "failed to clean prepared injection links"
        );
        InjectionWorkerError::Link(error)
    })
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

async fn saved_torrent_paths(
    directory: &Path,
    limit: usize,
) -> Result<Vec<PathBuf>, InjectionWorkerError> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let directory = directory.to_path_buf();
    let blocking_directory = directory.clone();
    tokio::task::spawn_blocking(move || {
        let entries = match std::fs::read_dir(&blocking_directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(source) => {
                return Err(InjectionWorkerError::Io {
                    operation: "read saved torrent directory",
                    path: blocking_directory,
                    source,
                });
            }
        };
        let mut paths = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|source| InjectionWorkerError::Io {
                operation: "read saved torrent directory entry",
                path: blocking_directory.clone(),
                source,
            })?;
            let path = entry.path();
            if is_direct_saved_torrent_file(&blocking_directory, &path) {
                paths.push(path);
                if paths.len() >= limit {
                    break;
                }
            }
        }
        paths.sort();
        Ok(paths)
    })
    .await
    .map_err(|source| InjectionWorkerError::Io {
        operation: "join saved torrent scan",
        path: directory.to_path_buf(),
        source: std::io::Error::other(source),
    })?
}

async fn read_saved_torrent(path: &Path) -> Result<SavedTorrentFile, InjectionWorkerError> {
    let path = path.to_path_buf();
    let blocking_path = path.clone();
    tokio::task::spawn_blocking(move || {
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
    #[cfg(windows)]
    volume_serial_number: Option<u32>,
    #[cfg(windows)]
    file_index: Option<u64>,
}

fn saved_torrent_identity(metadata: &std::fs::Metadata) -> SavedTorrentIdentity {
    SavedTorrentIdentity {
        #[cfg(unix)]
        device: metadata.dev(),
        #[cfg(unix)]
        inode: metadata.ino(),
        #[cfg(windows)]
        volume_serial_number: metadata.volume_serial_number(),
        #[cfg(windows)]
        file_index: metadata.file_index(),
    }
}

impl SavedTorrentIdentity {
    fn matches(self, metadata: &std::fs::Metadata) -> bool {
        #[cfg(unix)]
        {
            self.device == metadata.dev() && self.inode == metadata.ino()
        }
        #[cfg(windows)]
        {
            self.volume_serial_number == metadata.volume_serial_number()
                && self.file_index == metadata.file_index()
        }
        #[cfg(not(any(unix, windows)))]
        {
            let _ = metadata;
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
        InjectionOutcome::AlreadyExists => summary.already_exists += 1,
        InjectionOutcome::SourceIncomplete => summary.source_incomplete += 1,
        InjectionOutcome::Failed => summary.failed += 1,
        InjectionOutcome::Saved => summary.kept += 1,
    }
}

fn saved_retry_can_continue_after_error(error: &InjectionWorkerError) -> bool {
    matches!(
        error,
        InjectionWorkerError::Client(_)
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
        created_roots: Vec<PathBuf>,
        linked_files: usize,
    },
    SourceIncomplete,
}

enum InjectionMutationResult {
    SavedForShutdown,
    AlreadyExists,
    Injected(Result<(), TorrentClientError>),
    PrecheckFailed(TorrentClientError),
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
    if !config.ignore_non_relevant_files_to_resume
        || assessment.decision != MatchDecision::Partial
        || has_video_disc_files(metafile)
        || remaining.get() > 200 * 1024 * 1024
    {
        return false;
    }

    let Some(piece_slack) = metafile
        .piece_length()
        .unwrap_or(ByteSize::new(0))
        .get()
        .checked_mul(2)
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
    shutdown: Option<&ShutdownSignal>,
) -> bool {
    if config.poll_interval_ms == 0 {
        return false;
    }
    let sleep = tokio::time::sleep(Duration::from_millis(config.poll_interval_ms));
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

fn dependency_name(
    descriptor: &TorrentClientDescriptor,
) -> Result<DependencyName, InjectionWorkerError> {
    DependencyName::new(descriptor.name.as_str()).map_err(|error| {
        InjectionWorkerError::Database(DatabaseError::QueryFailed {
            operation: "build client dependency name".to_owned(),
            message: error.to_string(),
        })
    })
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::future::pending;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::clients::TorrentClientCapabilities;
    use crate::domain::{
        ByteSize, CandidateGuid, ClientHost, DisplayName, DownloadUrl, FileIndex, ItemTitle,
        LocalItemSource, MatchRatio, MediaType, TrackerName,
    };
    use crate::inventory::{InventoryScanOptions, ScannedLocalItem};
    use crate::persistence::repository::Repository;

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
    async fn worker_records_client_health_when_has_torrent_fails() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let root = unique_temp_dir("injection-has-error");
        let target = Arc::new(FakeClient::new(descriptor("target", "target")).with_has_errors(1));
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
        assert_eq!(Some(1_000), health[0].retry_after_ms);
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
        assert_eq!(1, target.inject_calls.load(Ordering::SeqCst));
        assert!(!root.join("links/tracker.example/movie.mkv").exists());
        assert_eq!(1, saved_torrent_count(&root.join("output")));
        assert_eq!("degraded", health[0].state);
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
            saved_for_retry: false,
            linked_files: 0,
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
        assert_eq!(1, summary.deleted);
        assert_eq!(0, saved_torrent_count(&output_dir));
        assert!(root.join("links/tracker.example/movie.mkv").exists());
        assert_eq!(1, target.inject_calls.load(Ordering::SeqCst));
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
        existing: bool,
        inject_error: bool,
        has_pending: bool,
        inject_pending: bool,
        resume_pending: bool,
        has_errors_remaining: AtomicUsize,
        recheck_errors_remaining: AtomicUsize,
        checking_true_remaining: AtomicUsize,
        completion_errors_remaining: AtomicUsize,
        inject_calls: AtomicUsize,
        has_calls: AtomicUsize,
        recheck_calls: AtomicUsize,
        checking_calls: AtomicUsize,
        remaining_calls: AtomicUsize,
        resume_calls: AtomicUsize,
        save_path_file_exists_at_inject: AtomicUsize,
        last_pause_for_recheck: StdMutex<Option<bool>>,
        last_save_path: StdMutex<Option<PathBuf>>,
    }

    struct FakeRefreshClient {
        descriptor: TorrentClientDescriptor,
        calls: AtomicUsize,
        summary: Option<InventoryRefreshSummary>,
        items: Vec<ScannedLocalItem>,
        cancel: bool,
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
                cancel: false,
            }
        }

        fn failing(descriptor: TorrentClientDescriptor) -> Self {
            Self {
                descriptor,
                calls: AtomicUsize::new(0),
                summary: None,
                items: Vec::new(),
                cancel: false,
            }
        }

        fn cancelled(descriptor: TorrentClientDescriptor) -> Self {
            Self {
                descriptor,
                calls: AtomicUsize::new(0),
                summary: None,
                items: Vec::new(),
                cancel: true,
            }
        }

        fn persisting(descriptor: TorrentClientDescriptor, items: Vec<ScannedLocalItem>) -> Self {
            Self {
                descriptor,
                calls: AtomicUsize::new(0),
                summary: None,
                items,
                cancel: false,
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
            let cancel = self.cancel;
            Box::pin(async move {
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
                existing: false,
                inject_error: false,
                has_pending: false,
                inject_pending: false,
                resume_pending: false,
                has_errors_remaining: AtomicUsize::new(0),
                recheck_errors_remaining: AtomicUsize::new(0),
                checking_true_remaining: AtomicUsize::new(0),
                completion_errors_remaining: AtomicUsize::new(0),
                inject_calls: AtomicUsize::new(0),
                has_calls: AtomicUsize::new(0),
                recheck_calls: AtomicUsize::new(0),
                checking_calls: AtomicUsize::new(0),
                remaining_calls: AtomicUsize::new(0),
                resume_calls: AtomicUsize::new(0),
                save_path_file_exists_at_inject: AtomicUsize::new(0),
                last_pause_for_recheck: StdMutex::new(None),
                last_save_path: StdMutex::new(None),
            }
        }

        const fn with_existing(mut self, existing: bool) -> Self {
            self.existing = existing;
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
            self.has_calls.fetch_add(1, Ordering::SeqCst);
            if self.has_pending {
                return Box::pin(
                    async move { pending::<Result<bool, TorrentClientError>>().await },
                );
            }
            let error = self
                .has_errors_remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                    current.checked_sub(1)
                })
                .is_ok()
                .then(|| TorrentClientError::Unavailable {
                    client: self.descriptor.name.as_str().to_owned(),
                    retry_after_ms: Some(1_000),
                    message: "offline".to_owned(),
                });
            let existing = self.existing;
            Box::pin(async move {
                if let Some(error) = error {
                    Err(error)
                } else {
                    Ok(existing)
                }
            })
        }

        fn inject<'a>(&'a self, request: ClientInjectionRequest<'a>) -> ClientResultFuture<'a, ()> {
            self.inject_calls.fetch_add(1, Ordering::SeqCst);
            *self.last_pause_for_recheck.lock().unwrap() = Some(request.pause_for_recheck);
            *self.last_save_path.lock().unwrap() = request.save_path.map(Path::to_path_buf);
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
            Box::pin(async move {
                if let Some(error) = error {
                    Err(error)
                } else {
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
                    Ok(ByteSize::new(0))
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
