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

use tokio::sync::Mutex;

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
use crate::matching::{
    CandidateAssessmentConfig, FileTreeMatchConfig, PersistedCandidateAssessment,
    ReverseLookupConfig, assess_and_persist_candidate, reverse_lookup_candidates_for_media_types,
};
use crate::persistence::repository::Repository;
use crate::persistence::torrent_cache::{TorrentOutputMetadata, parse_torrent_output_filename};
use crate::runtime::queue::{BoundedWorkQueue, QueueKind, WorkReceiver, bounded_work_queue};
use crate::torrent::parse_metafile;

const MAX_SAVED_TORRENT_BYTES: u64 = 32 * 1024 * 1024;

pub type ClientResultFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, TorrentClientError>> + Send + 'a>>;

pub trait InjectionClient: Send + Sync {
    fn descriptor(&self) -> &TorrentClientDescriptor;
    fn has_torrent<'a>(&'a self, info_hash: &'a InfoHash) -> ClientResultFuture<'a, bool>;
    fn inject<'a>(&'a self, request: ClientInjectionRequest<'a>) -> ClientResultFuture<'a, ()>;
    fn recheck<'a>(&'a self, info_hash: &'a InfoHash) -> ClientResultFuture<'a, ()>;
    fn is_checking<'a>(&'a self, info_hash: &'a InfoHash) -> ClientResultFuture<'a, bool>;
    fn remaining_bytes<'a>(&'a self, info_hash: &'a InfoHash) -> ClientResultFuture<'a, ByteSize>;
    fn resume<'a>(&'a self, info_hash: &'a InfoHash) -> ClientResultFuture<'a, ()>;
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
            skip_recheck: true,
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

impl InjectionWorker {
    pub fn new(repository: Repository, clients: Vec<Arc<dyn InjectionClient>>) -> Self {
        Self {
            repository,
            clients,
            mutation_lock: Arc::new(Mutex::new(())),
        }
    }

    pub async fn process(
        &self,
        request: InjectionRequest,
    ) -> Result<InjectionWorkResult, InjectionWorkerError> {
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
        let existing = self
            .find_existing_client(request.metafile.info_hash(), target.descriptor())
            .await?;
        if let Some(existing_client) = existing {
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

        let link_result = self.prepare_links(&request).await?;
        if matches!(link_result, LinkPreparation::SourceIncomplete) {
            self.save_for_retry(&request)?;
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
        let mutation_result = {
            let _guard = self.mutation_lock.lock().await;
            if target.has_torrent(request.metafile.info_hash()).await? {
                InjectionMutationResult::AlreadyExists
            } else {
                InjectionMutationResult::Injected(
                    target
                        .inject(ClientInjectionRequest {
                            info_hash: request.metafile.info_hash(),
                            torrent_bytes: &request.torrent_bytes,
                            save_path: save_path.as_deref(),
                            pause_for_recheck,
                        })
                        .await,
                )
            }
        };

        match mutation_result {
            InjectionMutationResult::AlreadyExists => {
                self.record_client_health(target.descriptor(), true, None, request.assessed_at_ms)
                    .await?;
                if linked_files > 0 {
                    let recheck_plan = RecheckResumePlan {
                        should_recheck: true,
                        ..recheck_plan
                    };
                    let _ = self
                        .run_recheck_resume(target.as_ref(), &request, recheck_plan)
                        .await?;
                }
                Ok(InjectionWorkResult {
                    outcome: InjectionOutcome::AlreadyExists,
                    target_client: Some(target_name),
                    saved_for_retry: false,
                    linked_files,
                })
            }
            InjectionMutationResult::Injected(Ok(())) => {
                self.record_client_health(target.descriptor(), true, None, request.assessed_at_ms)
                    .await?;
                if recheck_plan.should_recheck {
                    let _ = self
                        .run_recheck_resume(target.as_ref(), &request, recheck_plan)
                        .await?;
                    self.save_for_retry(&request)?;
                }
                Ok(InjectionWorkResult {
                    outcome: InjectionOutcome::Injected,
                    target_client: Some(target_name),
                    saved_for_retry: pause_for_recheck,
                    linked_files,
                })
            }
            InjectionMutationResult::Injected(Err(error)) => {
                let _ = cleanup_created_roots(&created_roots);
                self.save_for_retry(&request)?;
                self.record_client_health(
                    target.descriptor(),
                    false,
                    Some(&error),
                    request.assessed_at_ms,
                )
                .await?;
                Ok(InjectionWorkResult {
                    outcome: InjectionOutcome::Failed,
                    target_client: Some(target_name),
                    saved_for_retry: true,
                    linked_files,
                })
            }
        }
    }

    pub async fn retry_saved_torrents(
        &self,
        config: SavedTorrentRetryConfig,
    ) -> Result<SavedTorrentRetrySummary, InjectionWorkerError> {
        let mut summary = SavedTorrentRetrySummary::default();
        if config.directories.is_empty() || config.max_saved_torrents == 0 {
            return Ok(summary);
        }

        for directory in &config.directories {
            let paths =
                saved_torrent_paths(directory, config.max_saved_torrents - summary.scanned).await?;
            for path in paths {
                if summary.scanned >= config.max_saved_torrents {
                    return Ok(summary);
                }
                summary.scanned += 1;
                self.retry_saved_torrent(directory, &path, &config, &mut summary)
                    .await?;
            }
        }

        Ok(summary)
    }

    async fn retry_saved_torrent(
        &self,
        directory: &Path,
        path: &Path,
        config: &SavedTorrentRetryConfig,
        summary: &mut SavedTorrentRetrySummary,
    ) -> Result<(), InjectionWorkerError> {
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
            attempted_match = true;
            summary.attempted += 1;
            let result = match self
                .process(InjectionRequest {
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
                })
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
    ) -> Result<Option<Arc<dyn InjectionClient>>, InjectionWorkerError> {
        for client in &self.clients {
            if client.descriptor().host == target.host {
                continue;
            }
            if client.has_torrent(info_hash).await? {
                return Ok(Some(Arc::clone(client)));
            }
        }
        Ok(None)
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
        let link_dir = select_link_dir(
            source_root,
            &request.link_dirs,
            LinkDirOptions::new(link_type),
        )?;
        let destination_dir = link_destination_dir(
            &link_dir,
            request.candidate.tracker.as_str(),
            request.flat_linking,
        )?;
        let outcome = match link_metafile_files(
            source_root,
            &request.local_files,
            request.metafile.files(),
            request.assessment.decision,
            &destination_dir,
            LinkFilesOptions::new(link_type),
        ) {
            Ok(outcome) => outcome,
            Err(LinkActionError::MissingSource { .. })
            | Err(LinkActionError::NoSourceMatch { .. }) => {
                return Ok(LinkPreparation::SourceIncomplete);
            }
            Err(error) => return Err(error.into()),
        };

        Ok(LinkPreparation::Ready {
            save_path: Some(destination_dir),
            linked_files: outcome.created_links.len(),
            created_roots: outcome.created_roots,
        })
    }

    fn save_for_retry(&self, request: &InjectionRequest) -> Result<(), InjectionWorkerError> {
        let metadata = candidate_output_metadata(
            request.local_item.media_type,
            &request.candidate,
            &request.metafile,
        );
        save_candidate_torrent(&request.output_dir, &metadata, &request.torrent_bytes)?;
        Ok(())
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
        if client.is_checking(info_hash).await?
            || client.remaining_bytes(info_hash).await?.get() > 0
        {
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
    ) -> Result<ResumeLoopOutcome, InjectionWorkerError> {
        if !plan.should_recheck {
            return Ok(ResumeLoopOutcome::NotRequired);
        }
        {
            let _guard = self.mutation_lock.lock().await;
            client.recheck(request.metafile.info_hash()).await?;
        }
        let max_polls = max_resume_polls(request.recheck);
        for _ in 0..max_polls {
            if client.is_checking(request.metafile.info_hash()).await? {
                sleep_between_resume_polls(request.recheck).await;
                continue;
            }
            let remaining = client.remaining_bytes(request.metafile.info_hash()).await?;
            if can_resume_with_remaining(
                &request.metafile,
                &request.assessment,
                request.recheck,
                plan,
                remaining,
            ) {
                let _guard = self.mutation_lock.lock().await;
                client.resume(request.metafile.info_hash()).await?;
                return Ok(ResumeLoopOutcome::Resumed);
            }
            return Ok(ResumeLoopOutcome::WaitingForCompletion);
        }
        Ok(ResumeLoopOutcome::StillChecking)
    }
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
    AlreadyExists,
    Injected(Result<(), TorrentClientError>),
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

async fn sleep_between_resume_polls(config: RecheckResumeConfig) {
    if config.poll_interval_ms > 0 {
        tokio::time::sleep(Duration::from_millis(config.poll_interval_ms)).await;
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
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::clients::TorrentClientCapabilities;
    use crate::domain::{
        ByteSize, CandidateGuid, ClientHost, DisplayName, DownloadUrl, FileIndex, ItemTitle,
        LocalItemSource, MatchRatio, MediaType, TrackerName,
    };
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
            !recheck_resume_plan(&normal, &exact, RecheckResumeConfig::default()).should_recheck
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
        has_errors_remaining: AtomicUsize,
        completion_errors_remaining: AtomicUsize,
        inject_calls: AtomicUsize,
        has_calls: AtomicUsize,
        recheck_calls: AtomicUsize,
        resume_calls: AtomicUsize,
        last_pause_for_recheck: StdMutex<Option<bool>>,
    }

    impl FakeClient {
        fn new(descriptor: TorrentClientDescriptor) -> Self {
            Self {
                descriptor,
                existing: false,
                inject_error: false,
                has_errors_remaining: AtomicUsize::new(0),
                completion_errors_remaining: AtomicUsize::new(0),
                inject_calls: AtomicUsize::new(0),
                has_calls: AtomicUsize::new(0),
                recheck_calls: AtomicUsize::new(0),
                resume_calls: AtomicUsize::new(0),
                last_pause_for_recheck: StdMutex::new(None),
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

        fn with_has_errors(self, count: usize) -> Self {
            self.has_errors_remaining.store(count, Ordering::SeqCst);
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
    }

    impl InjectionClient for FakeClient {
        fn descriptor(&self) -> &TorrentClientDescriptor {
            &self.descriptor
        }

        fn has_torrent<'a>(&'a self, _info_hash: &'a InfoHash) -> ClientResultFuture<'a, bool> {
            self.has_calls.fetch_add(1, Ordering::SeqCst);
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
            Box::pin(async move { Ok(()) })
        }

        fn is_checking<'a>(&'a self, _info_hash: &'a InfoHash) -> ClientResultFuture<'a, bool> {
            Box::pin(async move { Ok(false) })
        }

        fn remaining_bytes<'a>(
            &'a self,
            _info_hash: &'a InfoHash,
        ) -> ClientResultFuture<'a, ByteSize> {
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
            Box::pin(async move { Ok(()) })
        }
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
