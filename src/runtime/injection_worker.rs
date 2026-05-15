use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;

use crate::actions::{
    LinkActionError, LinkDirOptions, LinkFilesOptions, LinkType, SaveTorrentError,
    candidate_output_metadata, cleanup_created_roots, link_destination_dir, link_metafile_files,
    save_candidate_torrent, select_link_dir,
};
use crate::clients::TorrentClientDescriptor;
use crate::domain::{
    ByteSize, CandidateAssessment, DependencyName, DependencyState, InfoHash, InjectionOutcome,
    LocalFile, LocalItem, MatchDecision, ReasonText, RemoteCandidate, RemoteCandidateId,
    TorrentMetafile,
};
use crate::errors::{ClassifyFailure, DatabaseError, FailureClass, TorrentClientError};
use crate::persistence::repository::Repository;
use crate::runtime::queue::{BoundedWorkQueue, QueueKind, WorkReceiver, bounded_work_queue};

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

#[derive(Debug)]
pub enum InjectionWorkerError {
    NoWritableClient,
    MissingLocalItemId,
    Database(DatabaseError),
    Save(SaveTorrentError),
    Link(LinkActionError),
    Client(TorrentClientError),
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
            .find_existing_client(&request.metafile.info_hash, target.descriptor())
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
        let inject_result = {
            let _guard = self.mutation_lock.lock().await;
            target
                .inject(ClientInjectionRequest {
                    info_hash: &request.metafile.info_hash,
                    torrent_bytes: &request.torrent_bytes,
                    save_path: save_path.as_deref(),
                    pause_for_recheck,
                })
                .await
        };

        match inject_result {
            Ok(()) => {
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
            Err(error) => {
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
            &request.metafile.files,
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

    async fn run_recheck_resume(
        &self,
        client: &dyn InjectionClient,
        request: &InjectionRequest,
        plan: RecheckResumePlan,
    ) -> Result<ResumeLoopOutcome, InjectionWorkerError> {
        if !plan.should_recheck {
            return Ok(ResumeLoopOutcome::NotRequired);
        }
        client.recheck(&request.metafile.info_hash).await?;
        let max_polls = max_resume_polls(request.recheck);
        for _ in 0..max_polls {
            if client.is_checking(&request.metafile.info_hash).await? {
                sleep_between_resume_polls(request.recheck).await;
                continue;
            }
            let remaining = client.remaining_bytes(&request.metafile.info_hash).await?;
            if can_resume_with_remaining(
                &request.metafile,
                &request.assessment,
                request.recheck,
                plan,
                remaining,
            ) {
                client.resume(&request.metafile.info_hash).await?;
                return Ok(ResumeLoopOutcome::Resumed);
            }
            return Ok(ResumeLoopOutcome::WaitingForCompletion);
        }
        Ok(ResumeLoopOutcome::StillChecking)
    }
}

enum LinkPreparation {
    Ready {
        save_path: Option<PathBuf>,
        created_roots: Vec<PathBuf>,
        linked_files: usize,
    },
    SourceIncomplete,
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

    let piece_slack = metafile
        .piece_length
        .unwrap_or(ByteSize::new(0))
        .get()
        .saturating_mul(2);
    remaining.get()
        <= irrelevant_file_bytes(metafile)
            .get()
            .saturating_add(piece_slack)
}

fn has_video_disc_files(metafile: &TorrentMetafile) -> bool {
    metafile.files.iter().any(|file| {
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

fn irrelevant_file_bytes(metafile: &TorrentMetafile) -> ByteSize {
    ByteSize::new(
        metafile
            .files
            .iter()
            .filter(|file| is_irrelevant_file(&file.relative_path))
            .map(|file| file.size.get())
            .sum(),
    )
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
        LocalItemSource, MatchRatio, TrackerName,
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

    struct FakeClient {
        descriptor: TorrentClientDescriptor,
        existing: bool,
        inject_error: bool,
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
            let existing = self.existing;
            Box::pin(async move { Ok(existing) })
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
            Box::pin(async move { Ok(ByteSize::new(0)) })
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
