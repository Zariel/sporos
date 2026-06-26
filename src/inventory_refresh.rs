use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::fs;
use std::io::{ErrorKind, Write};
use std::num::NonZeroUsize;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc as std_mpsc;
use std::time::{Duration, Instant};

use notify::{
    Config as NotifyConfig, Event, EventKind, PollWatcher, RecursiveMode, Watcher,
    recommended_watcher,
};
use tokio::sync::mpsc;
use tokio::task;
use tracing::{info, info_span, warn};

use crate::domain::{
    ByteSize, ClientHost, DependencyName, DependencyState, DisplayName, InfoHash, ItemTitle,
    LocalFile, LocalItem, LocalItemSource, MediaType, ReasonText, SourceKey, TorrentFile,
    checked_file_total,
};
use crate::errors::{DatabaseError, TorrentClientError};
use crate::inventory::{
    InventoryScanFailure, InventoryScanOptions, InventoryScanner, ScannedLocalItem,
    parse_media_title,
};
use crate::persistence::repository::{
    LocalFileSnapshot, LocalInventoryReplaceSummary, LocalInventoryReplaceTransaction,
    LocalInventoryScope, LocalItemFileBatch, LocalItemPageCursor, LocalItemWithFile,
    OwnedLocalInventoryMessage, OwnedLocalItemFileBatch, Repository, StagedVirtualEpisodeCandidate,
    StagedVirtualSeason, StagedVirtualSeasonCursor,
};
use crate::runtime::announce_worker::unix_time_ms;
use crate::runtime::health::{DependencyKey, DependencyKind, HealthRegistry};
use crate::runtime::queue::{BoundedWorkQueue, QueueKind, WorkReceiver, bounded_work_queue};
use crate::runtime::shutdown::{ShutdownPhase, ShutdownSignal};

pub(crate) const INVENTORY_REFRESH_DEPENDENCY: &str = "inventory-refresh";
const INVENTORY_REFRESH_RETRY_INITIAL: Duration = Duration::from_millis(25);
const INVENTORY_REFRESH_RETRY_MAX: Duration = Duration::from_secs(5);
const DATA_ROOT_SCAN_BUFFER: usize = 64;
const CLIENT_INVENTORY_BUFFER: usize = 64;
const VIRTUAL_SEASON_PAGE_SIZE: u16 = 512;
const VIRTUAL_SEASON_MIN_EPISODES: usize = 3;
const VIRTUAL_SEASON_INCOMPLETE_MIN_AGE_MS: i64 = 8 * 24 * 60 * 60 * 1_000;
const MEDIA_WATCH_STOP_POLL: Duration = Duration::from_millis(500);
const MEDIA_WATCH_DEBOUNCE: Duration = Duration::from_millis(250);
const MEDIA_WATCH_MAX_BATCH_AGE: Duration = Duration::from_secs(2);
const MEDIA_WATCH_NATIVE_PROBE_TIMEOUT: Duration = Duration::from_millis(750);
const MEDIA_WATCH_POLL_INTERVAL: Duration = Duration::from_secs(30);
const MEDIA_WATCH_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
const MEDIA_WATCH_MAX_PENDING_PATHS: usize = 1_024;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct InventoryRefreshRequest {
    pub media_dirs: Vec<PathBuf>,
    pub changed_paths: Vec<PathBuf>,
}

impl InventoryRefreshRequest {
    pub fn full(media_dirs: Vec<PathBuf>) -> Self {
        Self {
            media_dirs,
            changed_paths: Vec::new(),
        }
    }

    pub fn changed_paths(media_dirs: Vec<PathBuf>, changed_paths: Vec<PathBuf>) -> Self {
        Self {
            media_dirs,
            changed_paths,
        }
    }
}

fn extend_unique_paths(paths: &mut Vec<PathBuf>, additional: Vec<PathBuf>) {
    let mut seen = paths.iter().cloned().collect::<BTreeSet<_>>();
    for path in additional {
        if seen.insert(path.clone()) {
            paths.push(path);
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct ChangedMediaRoot {
    media_dir: PathBuf,
    root: PathBuf,
}

#[derive(Debug, Clone, Default, Eq, PartialEq)]
struct DataDirRefreshPlan {
    scan_media_dirs: Vec<PathBuf>,
    scan_item_roots: Vec<PathBuf>,
    prune_roots: Vec<PathBuf>,
}

fn data_dir_refresh_plan(request: &InventoryRefreshRequest) -> DataDirRefreshPlan {
    if request.changed_paths.is_empty() {
        return DataDirRefreshPlan {
            scan_media_dirs: request.media_dirs.clone(),
            scan_item_roots: Vec::new(),
            prune_roots: Vec::new(),
        };
    }

    let mut plan = DataDirRefreshPlan::default();
    for changed in changed_media_roots(&request.media_dirs, &request.changed_paths) {
        match std::fs::symlink_metadata(&changed.root) {
            Ok(_) => plan_scan_existing_changed_root(&mut plan, changed),
            Err(error) if error.kind() == ErrorKind::NotFound => {
                plan_missing_changed_root(&mut plan, changed);
            }
            Err(_) => plan_scan_existing_changed_root(&mut plan, changed),
        }
    }
    plan
}

fn plan_scan_existing_changed_root(plan: &mut DataDirRefreshPlan, changed: ChangedMediaRoot) {
    if changed.root == changed.media_dir {
        plan.scan_media_dirs.push(changed.root);
    } else {
        plan.scan_item_roots.push(changed.root);
    }
}

fn plan_missing_changed_root(plan: &mut DataDirRefreshPlan, changed: ChangedMediaRoot) {
    if changed.root == changed.media_dir {
        plan.scan_media_dirs.push(changed.root);
        return;
    }
    match std::fs::symlink_metadata(&changed.media_dir) {
        Ok(_) => plan.prune_roots.push(changed.root),
        Err(error) if error.kind() == ErrorKind::NotFound => {
            plan.scan_media_dirs.push(changed.root)
        }
        Err(_) => plan.scan_item_roots.push(changed.root),
    }
}

fn changed_media_roots(media_dirs: &[PathBuf], changed_paths: &[PathBuf]) -> Vec<ChangedMediaRoot> {
    let mut media_dirs = media_dirs.iter().collect::<Vec<_>>();
    media_dirs.sort_by_key(|media_dir| std::cmp::Reverse(media_dir.components().count()));
    let mut roots = Vec::new();
    for changed_path in changed_paths {
        for media_dir in &media_dirs {
            if let Some(root) = changed_media_root(media_dir, changed_path) {
                roots.push(ChangedMediaRoot {
                    media_dir: media_dir.to_path_buf(),
                    root,
                });
                break;
            }
        }
    }
    collapse_changed_media_roots(roots)
}

fn changed_media_root(media_dir: &Path, changed_path: &Path) -> Option<PathBuf> {
    if changed_path == media_dir {
        return Some(media_dir.to_path_buf());
    }
    let relative = changed_path.strip_prefix(media_dir).ok()?;
    let mut components = relative.components();
    let component = components.next()?;
    match component {
        Component::Normal(name) => Some(media_dir.join(name)),
        _ => None,
    }
}

fn collapse_changed_media_roots(mut roots: Vec<ChangedMediaRoot>) -> Vec<ChangedMediaRoot> {
    roots.sort_by(|left, right| {
        left.root
            .components()
            .count()
            .cmp(&right.root.components().count())
            .then_with(|| left.root.cmp(&right.root))
    });
    let mut collapsed = Vec::<ChangedMediaRoot>::new();
    for root in roots {
        if collapsed
            .iter()
            .any(|existing| path_contains_or_equals(&existing.root, &root.root))
        {
            continue;
        }
        collapsed.push(root);
    }
    collapsed
}

fn path_contains_or_equals(parent: &Path, child: &Path) -> bool {
    child == parent || child.strip_prefix(parent).is_ok()
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct InventoryRefreshSummary {
    pub scanned_items: usize,
    pub persisted_items: usize,
    pub pruned_items: u64,
    pub scan_failures: Vec<InventoryScanFailure>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ClientInventoryItem {
    pub client_host: ClientHost,
    pub info_hash: InfoHash,
    pub display_name: DisplayName,
    pub media_type: MediaType,
    pub save_path: PathBuf,
    pub files: Vec<TorrentFile>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ClientInventoryMessage {
    Item(ClientInventoryItem),
    Finished,
}

impl ClientInventoryItem {
    pub fn into_scanned(self) -> Result<ScannedLocalItem, InventoryRefreshError> {
        let file_paths = self
            .files
            .iter()
            .map(|file| file.relative_path.clone())
            .collect::<Vec<_>>();
        let parsed = parse_media_title(self.display_name.as_str(), &file_paths);
        let total_size = checked_file_total(
            self.files.iter().map(|file| file.size),
            "client inventory total",
        )
        .map_err(|error| InventoryRefreshError::InvalidClientInventory {
            message: error.to_string(),
        })?;
        let files = self
            .files
            .into_iter()
            .map(|file| LocalFile::new(None, file.relative_path, file.size, file.file_index))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| InventoryRefreshError::InvalidClientInventory {
                message: error.to_string(),
            })?;
        Ok(ScannedLocalItem {
            item: LocalItem {
                id: None,
                source: LocalItemSource::Client {
                    client_host: self.client_host,
                    source_key: SourceKey::new(self.info_hash.as_str()).map_err(|error| {
                        InventoryRefreshError::InvalidClientInventory {
                            message: error.to_string(),
                        }
                    })?,
                },
                title: ItemTitle::new(self.display_name.as_str()).map_err(|error| {
                    InventoryRefreshError::InvalidClientInventory {
                        message: error.to_string(),
                    }
                })?,
                display_name: self.display_name,
                media_type: parsed.media_type,
                info_hash: Some(self.info_hash),
                path: None,
                save_path: Some(self.save_path),
                total_size,
                mtime_ms: None,
            },
            files,
        })
    }
}

#[derive(Debug)]
pub enum InventoryRefreshError {
    InvalidClientInventory { message: String },
    ScanWorkerFailed { message: String },
    Cancelled { message: String },
    Client { source: TorrentClientError },
    Database { source: DatabaseError },
}

#[derive(Debug, Clone)]
pub struct InventoryRefreshWorker {
    repository: Repository,
    health: Option<HealthRegistry>,
    scan_options: InventoryScanOptions,
    season_from_episodes: f64,
    run_client_post_refresh_work: bool,
    #[cfg(test)]
    data_root_scan_send_attempts: Option<Arc<std::sync::atomic::AtomicUsize>>,
    #[cfg(test)]
    virtual_refresh_attempts: Option<Arc<std::sync::atomic::AtomicUsize>>,
}

impl InventoryRefreshWorker {
    pub fn new(repository: Repository, scan_options: InventoryScanOptions) -> Self {
        Self {
            repository,
            health: None,
            scan_options,
            season_from_episodes: 1.0,
            run_client_post_refresh_work: true,
            #[cfg(test)]
            data_root_scan_send_attempts: None,
            #[cfg(test)]
            virtual_refresh_attempts: None,
        }
    }

    pub const fn with_season_from_episodes(mut self, season_from_episodes: f64) -> Self {
        self.season_from_episodes = season_from_episodes;
        self
    }

    pub fn with_health_registry(mut self, health: HealthRegistry) -> Self {
        self.health = Some(health);
        self
    }

    pub(crate) fn without_client_post_refresh_work(&self) -> Self {
        Self {
            repository: self.repository.clone(),
            health: self.health.clone(),
            scan_options: self.scan_options,
            season_from_episodes: self.season_from_episodes,
            run_client_post_refresh_work: false,
            #[cfg(test)]
            data_root_scan_send_attempts: self.data_root_scan_send_attempts.clone(),
            #[cfg(test)]
            virtual_refresh_attempts: self.virtual_refresh_attempts.clone(),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_data_root_scan_send_attempts(
        mut self,
        attempts: Arc<std::sync::atomic::AtomicUsize>,
    ) -> Self {
        self.data_root_scan_send_attempts = Some(attempts);
        self
    }

    #[cfg(test)]
    pub(crate) fn with_virtual_refresh_attempts(
        mut self,
        attempts: Arc<std::sync::atomic::AtomicUsize>,
    ) -> Self {
        self.virtual_refresh_attempts = Some(attempts);
        self
    }

    pub async fn refresh_data_dirs(
        &self,
        request: InventoryRefreshRequest,
    ) -> Result<InventoryRefreshSummary, InventoryRefreshError> {
        self.refresh_data_dirs_inner(request, None).await
    }

    pub async fn refresh_data_dirs_until_shutdown(
        &self,
        request: InventoryRefreshRequest,
        shutdown: ShutdownSignal,
    ) -> Result<InventoryRefreshSummary, InventoryRefreshError> {
        self.refresh_data_dirs_inner(request, Some(shutdown)).await
    }

    async fn refresh_data_dirs_inner(
        &self,
        request: InventoryRefreshRequest,
        mut shutdown: Option<ShutdownSignal>,
    ) -> Result<InventoryRefreshSummary, InventoryRefreshError> {
        let _span = info_span!(
            "inventory.refresh_data_dirs",
            media_dir_count = request.media_dirs.len(),
            changed_path_count = request.changed_paths.len()
        );
        let scanner = InventoryScanner::new(self.scan_options);
        let plan = data_dir_refresh_plan(&request);
        let mut pruned = if plan.prune_roots.is_empty() {
            0
        } else {
            self.prune_missing_data_roots(plan.prune_roots.clone())
                .await?
                .pruned
        };
        if plan.scan_media_dirs.is_empty() && plan.scan_item_roots.is_empty() {
            let now_ms = unix_time_ms();
            self.refresh_virtual_seasons(now_ms).await?;
            self.repository
                .wake_announce_inventory_refresh(now_ms, 1_000)
                .await?;

            return Ok(InventoryRefreshSummary {
                scanned_items: 0,
                persisted_items: 0,
                pruned_items: pruned,
                scan_failures: Vec::new(),
            });
        }

        let (sender, receiver) = mpsc::channel(DATA_ROOT_SCAN_BUFFER);
        let media_dirs = plan.scan_media_dirs;
        let item_roots = plan.scan_item_roots;
        let mut scope_roots = media_dirs.clone();
        scope_roots.extend(item_roots.clone());
        let cancelled = Arc::new(AtomicBool::new(false));
        let scanner_cancelled = cancelled.clone();
        #[cfg(test)]
        let send_attempts = self.data_root_scan_send_attempts.clone();
        let scan_task = task::spawn_blocking(move || {
            let mut report = scanner.scan_media_dirs_until(
                &media_dirs,
                || !scanner_cancelled.load(Ordering::Relaxed),
                |scanned| {
                    if scanner_cancelled.load(Ordering::Relaxed) {
                        return false;
                    }
                    #[cfg(test)]
                    if let Some(send_attempts) = &send_attempts {
                        send_attempts.fetch_add(1, Ordering::SeqCst);
                    }
                    sender
                        .blocking_send(OwnedLocalInventoryMessage::item(OwnedLocalItemFileBatch {
                            item: scanned.item,
                            files: scanned.files,
                        }))
                        .is_ok()
                },
            );
            if !scanner_cancelled.load(Ordering::Relaxed) && report.failures.is_empty() {
                let item_report = scanner.scan_item_roots_until(
                    &item_roots,
                    || !scanner_cancelled.load(Ordering::Relaxed),
                    |scanned| {
                        if scanner_cancelled.load(Ordering::Relaxed) {
                            return false;
                        }
                        #[cfg(test)]
                        if let Some(send_attempts) = &send_attempts {
                            send_attempts.fetch_add(1, Ordering::SeqCst);
                        }
                        sender
                            .blocking_send(OwnedLocalInventoryMessage::item(
                                OwnedLocalItemFileBatch {
                                    item: scanned.item,
                                    files: scanned.files,
                                },
                            ))
                            .is_ok()
                    },
                );
                report.scanned_items = report
                    .scanned_items
                    .saturating_add(item_report.scanned_items);
                report.failures.extend(item_report.failures);
            }
            if !scanner_cancelled.load(Ordering::Relaxed) && report.failures.is_empty() {
                drop(sender.blocking_send(OwnedLocalInventoryMessage::Finished));
            }
            report
        });
        let staging_started = Arc::new(AtomicBool::new(false));
        let mut replace = Box::pin(
            self.repository
                .replace_local_inventory_owned_receiver_with_staging_signal(
                    LocalInventoryScope::DataRoots { roots: scope_roots },
                    receiver,
                    Some(staging_started.clone()),
                ),
        );
        let replace_result = if let Some(shutdown) = shutdown.as_mut() {
            let mut cancelled_by_shutdown = false;
            let selected = tokio::select! {
                result = replace.as_mut() => Some(result),
                _state = shutdown.cancelled() => {
                    cancelled_by_shutdown = true;
                    None
                }
            };
            if cancelled_by_shutdown {
                cancelled.store(true, Ordering::Relaxed);
                if !staging_started.load(Ordering::Acquire) {
                    drop(replace);
                    scan_task
                        .await
                        .map_err(|error| InventoryRefreshError::ScanWorkerFailed {
                            message: error.to_string(),
                        })?;
                    return Err(InventoryRefreshError::Cancelled {
                        message: "shutdown requested".to_owned(),
                    });
                }
                let replace_result = replace.await;
                scan_task
                    .await
                    .map_err(|error| InventoryRefreshError::ScanWorkerFailed {
                        message: error.to_string(),
                    })?;
                match replace_result {
                    Ok(_) | Err(DatabaseError::IncompleteStream { .. }) => {
                        return Err(InventoryRefreshError::Cancelled {
                            message: "shutdown requested".to_owned(),
                        });
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            selected.ok_or_else(|| DatabaseError::Unavailable {
                operation: "refresh inventory".to_owned(),
                message: "inventory refresh ended without repository result".to_owned(),
            })?
        } else {
            replace.await
        };
        let LocalInventoryReplaceSummary {
            upserted,
            pruned: scan_pruned,
        } = match replace_result {
            Ok(summary) => summary,
            Err(error) => {
                cancelled.store(true, Ordering::Relaxed);
                let scan_report =
                    scan_task
                        .await
                        .map_err(|error| InventoryRefreshError::ScanWorkerFailed {
                            message: error.to_string(),
                        })?;
                if !scan_report.failures.is_empty()
                    && matches!(error, DatabaseError::IncompleteStream { .. })
                {
                    return Ok(InventoryRefreshSummary {
                        scanned_items: scan_report.scanned_items,
                        persisted_items: 0,
                        pruned_items: 0,
                        scan_failures: scan_report.failures,
                    });
                }
                return Err(error.into());
            }
        };
        pruned = pruned.saturating_add(scan_pruned);
        let scan_report =
            scan_task
                .await
                .map_err(|error| InventoryRefreshError::ScanWorkerFailed {
                    message: error.to_string(),
                })?;
        let now_ms = unix_time_ms();
        self.refresh_virtual_seasons(now_ms).await?;
        self.repository
            .wake_announce_inventory_refresh(now_ms, 1_000)
            .await?;

        Ok(InventoryRefreshSummary {
            scanned_items: scan_report.scanned_items,
            persisted_items: upserted,
            pruned_items: pruned,
            scan_failures: scan_report.failures,
        })
    }

    async fn prune_missing_data_roots(
        &self,
        roots: Vec<PathBuf>,
    ) -> Result<LocalInventoryReplaceSummary, InventoryRefreshError> {
        self.repository
            .replace_local_inventory_stream(
                LocalInventoryScope::DataRoots { roots },
                std::iter::empty(),
            )
            .await
            .map_err(InventoryRefreshError::from)
    }

    pub async fn refresh_client_items(
        &self,
        client_host: ClientHost,
        items: &[ScannedLocalItem],
    ) -> Result<InventoryRefreshSummary, InventoryRefreshError> {
        let _span = info_span!(
            "inventory.refresh_client_items",
            client_host = %client_host,
            item_count = items.len()
        );
        let LocalInventoryReplaceSummary { upserted, pruned } = self
            .repository
            .replace_local_inventory_stream(
                LocalInventoryScope::Client {
                    client_host: client_host.clone(),
                },
                items.iter().map(local_item_file_batch),
            )
            .await?;
        let now_ms = unix_time_ms();
        self.finish_client_refresh(&client_host, now_ms).await?;

        Ok(InventoryRefreshSummary {
            scanned_items: items.len(),
            persisted_items: upserted,
            pruned_items: pruned,
            scan_failures: Vec::new(),
        })
    }

    pub async fn refresh_client_inventory_receiver(
        &self,
        client_host: ClientHost,
        mut items: mpsc::Receiver<ClientInventoryMessage>,
    ) -> Result<InventoryRefreshSummary, InventoryRefreshError> {
        let _span = info_span!(
            "inventory.refresh_client_items",
            client_host = %client_host
        );
        let (sender, receiver) = mpsc::channel(CLIENT_INVENTORY_BUFFER);
        let transform_host = client_host.clone();
        let transform_task = tokio::spawn(async move {
            let mut scanned_items = 0usize;
            while let Some(message) = items.recv().await {
                let ClientInventoryMessage::Item(item) = message else {
                    drop(sender.send(OwnedLocalInventoryMessage::Finished).await);
                    return Ok(scanned_items);
                };
                if item.client_host != transform_host {
                    return Err(InventoryRefreshError::InvalidClientInventory {
                        message: format!(
                            "client inventory item for {} is outside {} refresh",
                            item.client_host.as_str(),
                            transform_host.as_str()
                        ),
                    });
                }
                let scanned = item.into_scanned()?;
                let message = OwnedLocalInventoryMessage::item(OwnedLocalItemFileBatch {
                    item: scanned.item,
                    files: scanned.files,
                });
                if sender.send(message).await.is_err() {
                    return Ok(scanned_items);
                }
                scanned_items = scanned_items.saturating_add(1);
            }

            Err(InventoryRefreshError::InvalidClientInventory {
                message: "client inventory stream ended before completion marker".to_owned(),
            })
        });

        let replace_result = self
            .repository
            .replace_local_inventory_owned_receiver(
                LocalInventoryScope::Client {
                    client_host: client_host.clone(),
                },
                receiver,
            )
            .await;
        let scanned_items =
            transform_task
                .await
                .map_err(|error| InventoryRefreshError::ScanWorkerFailed {
                    message: error.to_string(),
                })??;
        let LocalInventoryReplaceSummary { upserted, pruned } = replace_result?;
        let now_ms = unix_time_ms();
        self.finish_client_refresh(&client_host, now_ms).await?;

        Ok(InventoryRefreshSummary {
            scanned_items,
            persisted_items: upserted,
            pruned_items: pruned,
            scan_failures: Vec::new(),
        })
    }

    pub(crate) async fn refresh_virtual_seasons_after_client_batch(
        &self,
        client_hosts: &[ClientHost],
    ) -> Result<(), InventoryRefreshError> {
        let now_ms = unix_time_ms();
        self.refresh_virtual_seasons(now_ms).await?;
        self.repository
            .wake_announce_inventory_refresh(now_ms, 1_000)
            .await?;
        for client_host in client_hosts {
            self.repository
                .wake_announce_client_source_completion(client_host, now_ms, 1_000)
                .await?;
        }
        Ok(())
    }

    async fn finish_client_refresh(
        &self,
        client_host: &ClientHost,
        now_ms: i64,
    ) -> Result<(), InventoryRefreshError> {
        if self.run_client_post_refresh_work {
            self.refresh_virtual_seasons(now_ms).await?;
            self.repository
                .wake_announce_client_source_completion(client_host, now_ms, 1_000)
                .await?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
struct VirtualSeasonKey {
    title: String,
    season: u16,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct VirtualEpisodeFile {
    episode: u16,
    source_file: PathBuf,
    size: ByteSize,
    mtime_ms: Option<i64>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct VirtualSeasonAccumulator {
    key: VirtualSeasonKey,
    episodes: BTreeMap<u16, VirtualEpisodeFile>,
    newest_mtime_ms: Option<i64>,
}

impl InventoryRefreshWorker {
    async fn refresh_virtual_seasons(&self, now_ms: i64) -> Result<(), InventoryRefreshError> {
        #[cfg(test)]
        if let Some(attempts) = &self.virtual_refresh_attempts {
            attempts.fetch_add(1, Ordering::SeqCst);
        }
        if !self.season_from_episodes.is_finite() || self.season_from_episodes <= 0.0 {
            self.replace_virtual_inventory(std::iter::empty(), now_ms)
                .await?;
            return Ok(());
        }

        let mut replacement = self
            .repository
            .begin_local_inventory_replace_transaction(LocalInventoryScope::Virtual)
            .await?;
        replacement
            .initialize_virtual_season_candidate_stage()
            .await?;

        self.stage_existing_real_season_keys(&mut replacement)
            .await?;
        self.stage_virtual_episode_candidates(&mut replacement)
            .await?;
        self.replace_staged_virtual_seasons(&mut replacement, now_ms)
            .await?;
        replacement.commit().await?;

        Ok(())
    }

    async fn stage_existing_real_season_keys(
        &self,
        replacement: &mut LocalInventoryReplaceTransaction<'_>,
    ) -> Result<(), InventoryRefreshError> {
        let mut cursor = None::<LocalItemPageCursor>;
        loop {
            let page = replacement
                .local_items_by_media_type_keyset_page(
                    MediaType::SeasonPack,
                    VIRTUAL_SEASON_PAGE_SIZE,
                    cursor.as_ref(),
                )
                .await?;
            if page.is_empty() {
                break;
            }
            for item in page {
                cursor = Some(LocalItemPageCursor::from_item(&item));
                if matches!(item.source, LocalItemSource::Virtual { .. }) {
                    continue;
                }
                if let Some(key) = real_season_key(&item) {
                    replacement
                        .stage_virtual_real_season_key(&key.title, key.season)
                        .await?;
                }
            }
        }

        Ok(())
    }

    async fn stage_virtual_episode_candidates(
        &self,
        replacement: &mut LocalInventoryReplaceTransaction<'_>,
    ) -> Result<(), InventoryRefreshError> {
        let mut cursor = None::<LocalItemPageCursor>;
        loop {
            let page = replacement
                .local_items_with_largest_file_by_media_type_keyset_page(
                    MediaType::Episode,
                    VIRTUAL_SEASON_PAGE_SIZE,
                    cursor.as_ref(),
                )
                .await?;
            if page.is_empty() {
                break;
            }
            for row in page {
                let LocalItemWithFile { item, file } = row;
                cursor = Some(LocalItemPageCursor::from_item(&item));
                if !is_virtual_episode_source(&item.source) {
                    continue;
                }
                let Some((key, episode)) = episode_season_key(&item) else {
                    continue;
                };
                let Some(episode_file) = virtual_episode_file(&item, episode, file) else {
                    continue;
                };
                replacement
                    .stage_virtual_episode_candidate(&StagedVirtualEpisodeCandidate {
                        title: key.title,
                        season: key.season,
                        episode,
                        newest_mtime_ms: newest_mtime(item.mtime_ms, episode_file.mtime_ms),
                        source_file: episode_file.source_file,
                        size: episode_file.size,
                        mtime_ms: episode_file.mtime_ms,
                    })
                    .await?;
            }
        }

        Ok(())
    }

    async fn replace_staged_virtual_seasons(
        &self,
        replacement: &mut LocalInventoryReplaceTransaction<'_>,
        now_ms: i64,
    ) -> Result<(), InventoryRefreshError> {
        let mut cursor = None::<StagedVirtualSeasonCursor>;
        loop {
            let page = replacement
                .staged_virtual_seasons_page(VIRTUAL_SEASON_PAGE_SIZE, cursor.as_ref())
                .await?;
            if page.is_empty() {
                break;
            }
            for staged in page {
                cursor = Some(StagedVirtualSeasonCursor {
                    title: staged.title.clone(),
                    season: staged.season,
                });
                let Some(item) = self.materialize_virtual_season(
                    staged_virtual_season_accumulator(staged),
                    now_ms,
                )?
                else {
                    continue;
                };
                replacement.retain_item(&item).await?;
            }
        }

        Ok(())
    }

    async fn replace_virtual_inventory<I>(
        &self,
        seasons: I,
        now_ms: i64,
    ) -> Result<LocalInventoryReplaceSummary, InventoryRefreshError>
    where
        I: IntoIterator<Item = VirtualSeasonAccumulator>,
    {
        let (sender, receiver) = mpsc::channel(CLIENT_INVENTORY_BUFFER);
        let repository = self.repository.clone();
        let replace = tokio::spawn(async move {
            repository
                .replace_local_inventory_owned_receiver(LocalInventoryScope::Virtual, receiver)
                .await
        });
        for season in seasons {
            let Some(item) = self.materialize_virtual_season(season, now_ms)? else {
                continue;
            };
            sender
                .send(OwnedLocalInventoryMessage::item(item))
                .await
                .map_err(|error| InventoryRefreshError::InvalidClientInventory {
                    message: format!("stage virtual season inventory: {error}"),
                })?;
        }
        sender
            .send(OwnedLocalInventoryMessage::Finished)
            .await
            .map_err(|error| InventoryRefreshError::InvalidClientInventory {
                message: format!("finish virtual season inventory: {error}"),
            })?;
        drop(sender);

        replace
            .await
            .map_err(|error| InventoryRefreshError::ScanWorkerFailed {
                message: error.to_string(),
            })?
            .map_err(InventoryRefreshError::from)
    }

    fn materialize_virtual_season(
        &self,
        season: VirtualSeasonAccumulator,
        now_ms: i64,
    ) -> Result<Option<OwnedLocalItemFileBatch>, InventoryRefreshError> {
        if season.episodes.len() < VIRTUAL_SEASON_MIN_EPISODES {
            return Ok(None);
        }
        let Some(highest_episode) = season.episodes.keys().next_back().copied() else {
            return Ok(None);
        };
        let coverage = season.episodes.len() as f64 / f64::from(highest_episode);
        if coverage < self.season_from_episodes {
            return Ok(None);
        }
        if season.newest_mtime_ms.is_some_and(|mtime| {
            now_ms.saturating_sub(mtime) < VIRTUAL_SEASON_INCOMPLETE_MIN_AGE_MS
        }) {
            return Ok(None);
        }

        let mut files = Vec::with_capacity(season.episodes.len());
        for (index, episode) in season.episodes.into_values().enumerate() {
            let index = u32::try_from(index).map_err(|error| {
                InventoryRefreshError::InvalidClientInventory {
                    message: format!("virtual season has too many files: {error}"),
                }
            })?;
            files.push(
                LocalFile::new(
                    None,
                    virtual_relative_path(&episode.source_file).ok_or_else(|| {
                        InventoryRefreshError::InvalidClientInventory {
                            message: format!(
                                "virtual season source path is not absolute: {}",
                                episode.source_file.display()
                            ),
                        }
                    })?,
                    episode.size,
                    crate::domain::FileIndex::new(index),
                )
                .map_err(|error| InventoryRefreshError::InvalidClientInventory {
                    message: error.to_string(),
                })?
                .with_mtime_ms(episode.mtime_ms),
            );
        }
        let total_size = estimated_virtual_total_size(&files, highest_episode)?;
        let title = format!("{} S{:02}", season.key.title, season.key.season);
        let source_key = SourceKey::new(format!(
            "season:{}:s{:02}:{}",
            source_key_title(&season.key.title),
            season.key.season,
            stable_hash_hex(&season.key.title)
        ))
        .map_err(|error| InventoryRefreshError::InvalidClientInventory {
            message: error.to_string(),
        })?;
        let item = LocalItem {
            id: None,
            source: LocalItemSource::Virtual { source_key },
            title: ItemTitle::new(&title).map_err(|error| {
                InventoryRefreshError::InvalidClientInventory {
                    message: error.to_string(),
                }
            })?,
            display_name: DisplayName::new(&title).map_err(|error| {
                InventoryRefreshError::InvalidClientInventory {
                    message: error.to_string(),
                }
            })?,
            media_type: MediaType::SeasonPack,
            info_hash: None,
            path: Some(PathBuf::from("/")),
            save_path: None,
            total_size,
            mtime_ms: season.newest_mtime_ms,
        };

        Ok(Some(OwnedLocalItemFileBatch { item, files }))
    }
}

fn local_item_file_batch(scanned: &ScannedLocalItem) -> LocalItemFileBatch<'_> {
    LocalItemFileBatch {
        item: &scanned.item,
        files: &scanned.files,
    }
}

fn is_virtual_episode_source(source: &LocalItemSource) -> bool {
    matches!(
        source,
        LocalItemSource::Client { .. } | LocalItemSource::DataRoot { .. }
    )
}

fn episode_season_key(item: &LocalItem) -> Option<(VirtualSeasonKey, u16)> {
    let parsed = parse_media_title(item.title.as_str(), &[]);
    if parsed.media_type != MediaType::Episode {
        return None;
    }
    let season = parsed.season?;
    let episode = parsed.episode?;
    let suffix = format!(" S{season:02}E{episode:02}");
    let title = parsed.search_title.strip_suffix(&suffix)?.to_owned();
    Some((VirtualSeasonKey { title, season }, episode))
}

fn real_season_key(item: &LocalItem) -> Option<VirtualSeasonKey> {
    let parsed = parse_media_title(item.title.as_str(), &[]);
    if parsed.media_type != MediaType::SeasonPack {
        return None;
    }
    let season = parsed.season?;
    let suffix = format!(" S{season:02}");
    let title = parsed.search_title.strip_suffix(&suffix)?.to_owned();
    Some(VirtualSeasonKey { title, season })
}

fn staged_virtual_season_accumulator(staged: StagedVirtualSeason) -> VirtualSeasonAccumulator {
    let key = VirtualSeasonKey {
        title: staged.title,
        season: staged.season,
    };
    let episodes = staged
        .episodes
        .into_iter()
        .map(|episode| {
            (
                episode.episode,
                VirtualEpisodeFile {
                    episode: episode.episode,
                    source_file: episode.source_file,
                    size: episode.size,
                    mtime_ms: episode.mtime_ms,
                },
            )
        })
        .collect();
    VirtualSeasonAccumulator {
        key,
        episodes,
        newest_mtime_ms: staged.newest_mtime_ms,
    }
}

fn virtual_episode_file(
    item: &LocalItem,
    episode: u16,
    file: LocalFileSnapshot,
) -> Option<VirtualEpisodeFile> {
    let source_root = item.save_path.as_deref().or(item.path.as_deref())?;
    let source_file = if item.path.as_deref().is_some_and(|path| {
        path.file_name()
            .is_some_and(|name| name == file.relative_path.as_os_str())
    }) {
        source_root.to_path_buf()
    } else {
        source_root.join(&file.relative_path)
    };
    VirtualEpisodeFile {
        episode,
        source_file,
        size: file.size,
        mtime_ms: file.mtime_ms,
    }
    .into()
}

fn newest_mtime(current: Option<i64>, candidate: Option<i64>) -> Option<i64> {
    match (current, candidate) {
        (Some(current), Some(candidate)) => Some(current.max(candidate)),
        (Some(current), None) => Some(current),
        (None, Some(candidate)) => Some(candidate),
        (None, None) => None,
    }
}

fn estimated_virtual_total_size(
    files: &[LocalFile],
    highest_episode: u16,
) -> Result<ByteSize, InventoryRefreshError> {
    let total = files.iter().try_fold(0_u128, |total, file| {
        total.checked_add(u128::from(file.size.get())).ok_or(
            InventoryRefreshError::InvalidClientInventory {
                message: "virtual season total size overflowed".to_owned(),
            },
        )
    })?;
    let count = u128::try_from(files.len()).map_err(|error| {
        InventoryRefreshError::InvalidClientInventory {
            message: format!("virtual season file count overflowed: {error}"),
        }
    })?;
    let estimated = total
        .checked_mul(u128::from(highest_episode))
        .and_then(|value| value.checked_add(count.saturating_sub(1)))
        .map(|value| value / count)
        .ok_or(InventoryRefreshError::InvalidClientInventory {
            message: "virtual season estimated size overflowed".to_owned(),
        })?;
    let estimated = u64::try_from(estimated).map_err(|error| {
        InventoryRefreshError::InvalidClientInventory {
            message: format!("virtual season estimated size overflowed: {error}"),
        }
    })?;
    Ok(ByteSize::new(estimated))
}

fn virtual_relative_path(source_file: &Path) -> Option<PathBuf> {
    source_file
        .strip_prefix("/")
        .ok()
        .filter(|path| !path.as_os_str().is_empty())
        .map(PathBuf::from)
}

fn source_key_title(title: &str) -> String {
    let mut normalized = String::new();
    let mut last_separator = false;
    for character in title.chars().flat_map(char::to_lowercase) {
        if character.is_ascii_alphanumeric() {
            normalized.push(character);
            last_separator = false;
        } else if !last_separator && !normalized.is_empty() {
            normalized.push('-');
            last_separator = true;
        }
    }
    while normalized.ends_with('-') {
        normalized.pop();
    }
    if normalized.is_empty() {
        "unknown".to_owned()
    } else {
        normalized
    }
}

fn stable_hash_hex(value: &str) -> String {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

pub fn inventory_refresh_queue(
    capacity: NonZeroUsize,
) -> (
    BoundedWorkQueue<InventoryRefreshRequest>,
    WorkReceiver<InventoryRefreshRequest>,
) {
    bounded_work_queue(QueueKind::Indexing, capacity)
}

pub async fn run_inventory_refresh_worker(
    worker: InventoryRefreshWorker,
    mut receiver: WorkReceiver<InventoryRefreshRequest>,
    mut shutdown: ShutdownSignal,
) {
    loop {
        let request = tokio::select! {
            request = receiver.recv() => request,
            _state = shutdown.cancelled() => break,
        };
        let Some(request) = request else {
            break;
        };
        let completed = run_inventory_refresh_with_retry(&worker, request, &mut shutdown).await;
        if completed {
            receiver.mark_completed();
        }
        if shutdown.state().phase != ShutdownPhase::Running {
            break;
        }
    }
}

pub async fn run_media_inventory_watcher(
    media_dirs: Vec<PathBuf>,
    queue: BoundedWorkQueue<InventoryRefreshRequest>,
    mut shutdown: ShutdownSignal,
) {
    if media_dirs.is_empty() {
        return;
    }
    let (stop_sender, stop_receiver) = std_mpsc::channel();
    let watch_task = task::spawn_blocking(move || {
        run_media_inventory_watcher_blocking(media_dirs, queue, stop_receiver);
    });
    tokio::pin!(watch_task);

    tokio::select! {
        result = &mut watch_task => {
            if let Err(error) = result {
                warn!(error = %error, "media inventory watcher task failed");
            }
        }
        _state = shutdown.cancelled() => {
            if stop_sender.send(()).is_err() {
                warn!("media inventory watcher stop signal receiver was closed");
            }
            match tokio::time::timeout(MEDIA_WATCH_SHUTDOWN_TIMEOUT, &mut watch_task).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    warn!(error = %error, "media inventory watcher task failed during shutdown");
                }
                Err(_) => {
                    warn!(
                        timeout_ms = u64::try_from(MEDIA_WATCH_SHUTDOWN_TIMEOUT.as_millis())
                            .unwrap_or(u64::MAX),
                        "media inventory watcher did not stop before timeout"
                    );
                }
            }
        }
    }
}

fn run_media_inventory_watcher_blocking(
    media_dirs: Vec<PathBuf>,
    queue: BoundedWorkQueue<InventoryRefreshRequest>,
    stop_receiver: std_mpsc::Receiver<()>,
) {
    let (event_sender, event_receiver) = std_mpsc::channel::<notify::Result<Event>>();
    let (native_dirs, mut polling_dirs) = classify_media_watch_dirs(&media_dirs);

    let mut native_watcher = if native_dirs.is_empty() {
        None
    } else {
        match recommended_watcher({
            let event_sender = event_sender.clone();
            move |event| {
                drop(event_sender.send(event));
            }
        }) {
            Ok(watcher) => Some(watcher),
            Err(error) => {
                warn!(error = %error, "failed to start native media inventory watcher");
                polling_dirs.extend(native_dirs.iter().cloned());
                None
            }
        }
    };

    let mut native_watched_dirs = 0usize;
    if let Some(watcher) = native_watcher.as_mut() {
        for media_dir in &native_dirs {
            match watcher.watch(media_dir, RecursiveMode::Recursive) {
                Ok(()) => native_watched_dirs = native_watched_dirs.saturating_add(1),
                Err(error) => {
                    warn!(path = %media_dir.display(), error = %error, "failed to watch media dir with native watcher");
                    polling_dirs.push(media_dir.clone());
                }
            }
        }
    }

    let mut poll_watcher = if polling_dirs.is_empty() {
        None
    } else {
        match PollWatcher::new(
            {
                let event_sender = event_sender.clone();
                move |event| {
                    drop(event_sender.send(event));
                }
            },
            NotifyConfig::default().with_poll_interval(MEDIA_WATCH_POLL_INTERVAL),
        ) {
            Ok(watcher) => Some(watcher),
            Err(error) => {
                warn!(error = %error, "failed to start polling media inventory watcher");
                None
            }
        }
    };

    let mut polling_watched_dirs = 0usize;
    if let Some(watcher) = poll_watcher.as_mut() {
        for media_dir in &polling_dirs {
            match watcher.watch(media_dir, RecursiveMode::Recursive) {
                Ok(()) => polling_watched_dirs = polling_watched_dirs.saturating_add(1),
                Err(error) => {
                    warn!(path = %media_dir.display(), error = %error, "failed to watch media dir with polling watcher");
                }
            }
        }
    }
    if native_watcher.is_none() && poll_watcher.is_none() {
        warn!("media inventory watcher unavailable");
        return;
    }
    if native_watched_dirs == 0 && polling_watched_dirs == 0 {
        warn!(
            media_dir_count = media_dirs.len(),
            "media inventory watcher has no watched media dirs"
        );
        return;
    }
    info!(
        media_dir_count = media_dirs.len(),
        native_watched_dirs,
        polling_watched_dirs,
        poll_interval_ms = u64::try_from(MEDIA_WATCH_POLL_INTERVAL.as_millis()).unwrap_or(u64::MAX),
        "media inventory watcher started"
    );

    let mut pending_paths = Vec::<PathBuf>::new();
    let mut pending_since = None::<Instant>;
    loop {
        if stop_receiver.try_recv().is_ok() {
            break;
        }
        let timeout = if pending_paths.is_empty() {
            MEDIA_WATCH_STOP_POLL
        } else {
            MEDIA_WATCH_DEBOUNCE
        };
        match event_receiver.recv_timeout(timeout) {
            Ok(Ok(event)) => {
                if collect_media_watch_event(&mut pending_paths, event) && pending_since.is_none() {
                    pending_since = Some(Instant::now());
                }
                if pending_paths.len() >= MEDIA_WATCH_MAX_PENDING_PATHS
                    || pending_since
                        .is_some_and(|started| started.elapsed() >= MEDIA_WATCH_MAX_BATCH_AGE)
                {
                    pending_since = flush_media_changed_paths(
                        &media_dirs,
                        &queue,
                        &mut pending_paths,
                        pending_since,
                    );
                }
            }
            Ok(Err(error)) => {
                warn!(error = %error, "media inventory watcher reported an error");
            }
            Err(std_mpsc::RecvTimeoutError::Timeout) => {
                pending_since = flush_media_changed_paths(
                    &media_dirs,
                    &queue,
                    &mut pending_paths,
                    pending_since,
                );
            }
            Err(std_mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
    let _ = try_enqueue_media_changed_paths(&media_dirs, &queue, pending_paths);
}

fn classify_media_watch_dirs(media_dirs: &[PathBuf]) -> (Vec<PathBuf>, Vec<PathBuf>) {
    classify_media_watch_dirs_with(
        media_dirs,
        media_dir_polling_reason,
        probe_native_media_watch,
    )
}

fn classify_media_watch_dirs_with<R, P>(
    media_dirs: &[PathBuf],
    mut polling_reason: R,
    mut probe_native: P,
) -> (Vec<PathBuf>, Vec<PathBuf>)
where
    R: FnMut(&Path) -> Option<String>,
    P: FnMut(&Path) -> Result<(), String>,
{
    let mut native_dirs = Vec::new();
    let mut polling_dirs = Vec::new();
    for media_dir in media_dirs {
        if let Some(reason) = polling_reason(media_dir) {
            info!(
                path = %media_dir.display(),
                reason = %reason,
                "media inventory watcher using polling watcher"
            );
            polling_dirs.push(media_dir.clone());
            continue;
        }
        match probe_native(media_dir) {
            Ok(()) => native_dirs.push(media_dir.clone()),
            Err(reason) => {
                info!(
                    path = %media_dir.display(),
                    reason = %reason,
                    "native media inventory watcher probe did not confirm events; using polling watcher"
                );
                polling_dirs.push(media_dir.clone());
            }
        }
    }
    (native_dirs, polling_dirs)
}

#[cfg(target_os = "linux")]
fn media_dir_polling_reason(media_dir: &Path) -> Option<String> {
    match linux_fs_type(media_dir) {
        Ok(fs_type) => linux_fs_type_polling_reason(fs_type).map(str::to_owned),
        Err(error) => Some(format!("filesystem type probe failed: {error}")),
    }
}

#[cfg(not(target_os = "linux"))]
fn media_dir_polling_reason(_media_dir: &Path) -> Option<String> {
    None
}

#[cfg(target_os = "linux")]
fn linux_fs_type(media_dir: &Path) -> Result<i64, String> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let path = CString::new(media_dir.as_os_str().as_bytes())
        .map_err(|_| "path contains an interior nul byte".to_owned())?;
    let mut stat = std::mem::MaybeUninit::<libc::statfs>::uninit();
    // SAFETY: `path` is a valid nul-terminated path and `stat` points to
    // writable memory for libc to initialize.
    let result = unsafe { libc::statfs(path.as_ptr(), stat.as_mut_ptr()) };
    if result == 0 {
        // SAFETY: statfs returned success and initialized the struct.
        Ok(unsafe { stat.assume_init() }.f_type as i64)
    } else {
        Err(std::io::Error::last_os_error().to_string())
    }
}

#[cfg(target_os = "linux")]
fn linux_fs_type_polling_reason(fs_type: i64) -> Option<&'static str> {
    match fs_type {
        0x6969 => Some("nfs filesystem"),
        0x517B => Some("smb filesystem"),
        0xFF53_4D42 => Some("cifs filesystem"),
        0x6573_5546 => Some("fuse filesystem"),
        0x00C3_6400 => Some("ceph filesystem"),
        0x0102_1997 => Some("9p filesystem"),
        _ => None,
    }
}

fn probe_native_media_watch(media_dir: &Path) -> Result<(), String> {
    let (probe_sender, probe_receiver) = std_mpsc::channel::<notify::Result<Event>>();
    let mut watcher = recommended_watcher(move |event| {
        drop(probe_sender.send(event));
    })
    .map_err(|error| format!("native watcher unavailable: {error}"))?;
    watcher
        .watch(media_dir, RecursiveMode::Recursive)
        .map_err(|error| format!("native watch registration failed: {error}"))?;
    let probe = create_native_watch_probe(media_dir)?;
    let deadline = Instant::now() + MEDIA_WATCH_NATIVE_PROBE_TIMEOUT;
    let mut observed = false;
    while Instant::now() < deadline {
        let timeout = deadline.saturating_duration_since(Instant::now());
        match probe_receiver.recv_timeout(timeout.min(Duration::from_millis(100))) {
            Ok(Ok(event)) if event_contains_path(&event, &probe.file_path) => {
                observed = true;
                break;
            }
            Ok(Ok(_)) => {}
            Ok(Err(error)) => {
                probe.cleanup()?;
                return Err(format!("native watcher probe error: {error}"));
            }
            Err(std_mpsc::RecvTimeoutError::Timeout) => {}
            Err(std_mpsc::RecvTimeoutError::Disconnected) => {
                probe.cleanup()?;
                return Err("native watcher probe channel closed".to_owned());
            }
        }
    }
    probe.cleanup()?;
    if observed {
        Ok(())
    } else {
        Err(format!(
            "probe event not observed within {}ms",
            MEDIA_WATCH_NATIVE_PROBE_TIMEOUT.as_millis()
        ))
    }
}

#[derive(Debug)]
struct NativeWatchProbe {
    dir_path: PathBuf,
    file_path: PathBuf,
}

impl NativeWatchProbe {
    fn cleanup(self) -> Result<(), String> {
        let mut failures = Vec::new();
        match fs::remove_file(&self.file_path) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => failures.push(format!("remove file failed: {error}")),
        }
        match fs::remove_dir(&self.dir_path) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => failures.push(format!("remove directory failed: {error}")),
        }
        if failures.is_empty() {
            Ok(())
        } else {
            let reason = failures.join("; ");
            warn!(
                path = %self.dir_path.display(),
                reason = %reason,
                "native media inventory watcher probe cleanup failed"
            );
            Err(format!("probe cleanup failed: {reason}"))
        }
    }
}

fn create_native_watch_probe(media_dir: &Path) -> Result<NativeWatchProbe, String> {
    for attempt in 0..8 {
        let dir_path = media_dir.join(format!(
            ".sporos-notify-probe-{}-{}-{attempt}",
            std::process::id(),
            unix_time_ms()
        ));
        match fs::create_dir(&dir_path) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(format!("probe directory creation failed: {error}"));
            }
        }
        let file_path = dir_path.join("probe.tmp");
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&file_path)
        {
            Ok(mut file) => {
                if let Err(error) = file.write_all(b"sporos notify probe\n") {
                    let probe = NativeWatchProbe {
                        dir_path,
                        file_path,
                    };
                    drop(probe.cleanup());
                    return Err(format!("probe file write failed: {error}"));
                }
                return Ok(NativeWatchProbe {
                    dir_path,
                    file_path,
                });
            }
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                drop(fs::remove_dir(&dir_path));
            }
            Err(error) => {
                drop(fs::remove_dir(&dir_path));
                return Err(format!("probe file creation failed: {error}"));
            }
        }
    }
    Err("probe file path collision".to_owned())
}

fn event_contains_path(event: &Event, path: &Path) -> bool {
    event.paths.iter().any(|event_path| event_path == path)
}

fn collect_media_watch_event(pending_paths: &mut Vec<PathBuf>, event: Event) -> bool {
    if matches!(&event.kind, EventKind::Access(_) | EventKind::Other) || event.paths.is_empty() {
        return false;
    }
    extend_unique_paths(pending_paths, event.paths);
    true
}

fn flush_media_changed_paths(
    media_dirs: &[PathBuf],
    queue: &BoundedWorkQueue<InventoryRefreshRequest>,
    pending_paths: &mut Vec<PathBuf>,
    pending_since: Option<Instant>,
) -> Option<Instant> {
    match try_enqueue_media_changed_paths(media_dirs, queue, std::mem::take(pending_paths)) {
        ChangedPathEnqueue::Enqueued | ChangedPathEnqueue::Empty | ChangedPathEnqueue::Closed => {
            None
        }
        ChangedPathEnqueue::Full { changed_paths } => {
            extend_unique_paths(pending_paths, changed_paths);
            pending_since.or_else(|| Some(Instant::now()))
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
enum ChangedPathEnqueue {
    Empty,
    Enqueued,
    Full { changed_paths: Vec<PathBuf> },
    Closed,
}

fn try_enqueue_media_changed_paths(
    media_dirs: &[PathBuf],
    queue: &BoundedWorkQueue<InventoryRefreshRequest>,
    changed_paths: Vec<PathBuf>,
) -> ChangedPathEnqueue {
    if changed_paths.is_empty() {
        return ChangedPathEnqueue::Empty;
    }
    let request = InventoryRefreshRequest::changed_paths(media_dirs.to_vec(), changed_paths);
    if request.changed_paths.is_empty() {
        return ChangedPathEnqueue::Empty;
    }
    if let Err(error) = queue.try_enqueue(request) {
        match error {
            crate::runtime::queue::EnqueueError::Full { item } => {
                warn!("media inventory changed-path refresh queue is full");
                ChangedPathEnqueue::Full {
                    changed_paths: item.changed_paths,
                }
            }
            crate::runtime::queue::EnqueueError::Closed { .. } => {
                warn!("media inventory changed-path refresh queue is closed");
                ChangedPathEnqueue::Closed
            }
        }
    } else {
        ChangedPathEnqueue::Enqueued
    }
}

async fn run_inventory_refresh_with_retry(
    worker: &InventoryRefreshWorker,
    request: InventoryRefreshRequest,
    shutdown: &mut ShutdownSignal,
) -> bool {
    let mut delay = INVENTORY_REFRESH_RETRY_INITIAL;
    loop {
        match worker
            .refresh_data_dirs_until_shutdown(request.clone(), shutdown.clone())
            .await
        {
            Ok(summary) if summary.scan_failures.is_empty() => {
                record_inventory_refresh_health(worker, None, None).await;
                return true;
            }
            Ok(summary) => {
                let reason = scan_failure_reason(&summary.scan_failures);
                warn!(reason, "inventory refresh reported scan failures");
                record_inventory_refresh_health(worker, Some(reason), None).await;
                return true;
            }
            Err(InventoryRefreshError::Cancelled { .. }) => return false,
            Err(error) => {
                let reason = error.to_string();
                warn!(error = %reason, "inventory refresh failed; retrying");
                record_inventory_refresh_health(worker, Some(reason), Some(delay)).await;
            }
        }

        tokio::select! {
            _state = shutdown.cancelled() => return false,
            () = tokio::time::sleep(delay) => {}
        }
        delay = delay.saturating_mul(2).min(INVENTORY_REFRESH_RETRY_MAX);
    }
}

pub(crate) async fn record_inventory_refresh_health(
    worker: &InventoryRefreshWorker,
    reason: Option<String>,
    retry_after: Option<Duration>,
) {
    let Ok(name) = DependencyName::new(INVENTORY_REFRESH_DEPENDENCY) else {
        return;
    };
    let checked_at_ms = unix_time_ms();
    let state = if let Some(reason) = reason {
        let Ok(reason) = ReasonText::new(reason) else {
            return;
        };
        DependencyState::Degraded {
            reason,
            retry_after_ms: retry_after
                .map(duration_ms)
                .map(|delay| checked_at_ms.saturating_add(delay)),
        }
    } else {
        DependencyState::Healthy { checked_at_ms }
    };
    match worker
        .repository
        .record_dependency_health(DependencyKind::LocalState, &name, &state, checked_at_ms)
        .await
    {
        Ok(()) => {
            if let Some(health) = &worker.health {
                health.set_state(
                    DependencyKey::new(DependencyKind::LocalState, name),
                    state.clone(),
                );
            }
        }
        Err(error) => {
            warn!(error = ?error, "failed to record local inventory dependency health");
        }
    }
}

pub(crate) fn scan_failure_reason(failures: &[InventoryScanFailure]) -> String {
    match failures {
        [] => "inventory refresh failed".to_owned(),
        [failure] => format!(
            "scan {} failed for {}: {}",
            failure.kind,
            failure.path.display(),
            failure.message
        ),
        [first, ..] => format!(
            "{} scan failures; first {} failed for {}: {}",
            failures.len(),
            first.kind,
            first.path.display(),
            first.message
        ),
    }
}

fn duration_ms(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

impl From<DatabaseError> for InventoryRefreshError {
    fn from(source: DatabaseError) -> Self {
        Self::Database { source }
    }
}

impl From<TorrentClientError> for InventoryRefreshError {
    fn from(source: TorrentClientError) -> Self {
        Self::Client { source }
    }
}

impl fmt::Display for InventoryRefreshError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidClientInventory { message } => {
                write!(formatter, "invalid client inventory: {message}")
            }
            Self::ScanWorkerFailed { message } => {
                write!(formatter, "inventory scan worker failed: {message}")
            }
            Self::Cancelled { message } => {
                write!(formatter, "inventory refresh cancelled: {message}")
            }
            Self::Client { source } => write!(formatter, "{source}"),
            Self::Database { source } => write!(formatter, "{source}"),
        }
    }
}

impl Error for InventoryRefreshError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidClientInventory { .. } => None,
            Self::ScanWorkerFailed { .. } => None,
            Self::Cancelled { .. } => None,
            Self::Client { source } => Some(source),
            Self::Database { source } => Some(source),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::num::NonZeroUsize;
    use std::time::{SystemTime, UNIX_EPOCH};

    use sqlx::Row;

    use super::*;
    use crate::announce::{
        AnnounceDedupeIdentity, AnnounceReason, AnnounceStatus, AnnounceWorkId, AnnounceWorkItem,
    };
    use crate::domain::{
        ByteSize, CandidateGuid, ClientHost, DisplayName, FileIndex, InfoHash, ItemTitle,
        LocalFile, LocalItem, LocalItemSource, MediaType, SourceKey, TrackerName,
    };
    use crate::persistence::repository::AnnounceInsertResult;

    #[tokio::test]
    async fn refresh_data_dirs_persists_startup_scan() {
        let root = unique_temp_dir("startup");
        let release = root.join("Movie.2026.1080p");
        fs::create_dir_all(&release).unwrap();
        write_file(&release.join("movie.mkv"), 10);
        let repository = Repository::connect_in_memory().await.unwrap();
        let worker =
            InventoryRefreshWorker::new(repository.clone(), InventoryScanOptions::default());

        let summary = worker
            .refresh_data_dirs(InventoryRefreshRequest::full(vec![root.clone()]))
            .await
            .unwrap();
        let local_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM local_items")
            .fetch_one(repository.pool())
            .await
            .unwrap();
        let file = repository
            .local_files_by_relative_path(&PathBuf::from("movie.mkv"), 10)
            .await
            .unwrap();

        assert_eq!(1, summary.scanned_items);
        assert_eq!(1, summary.persisted_items);
        assert_eq!(0, summary.pruned_items);
        assert!(summary.scan_failures.is_empty());
        assert_eq!(1, local_count);
        assert_eq!(1, file.len());
        assert_eq!(ByteSize::new(10), file[0].size);
        assert!(file[0].mtime_ms.is_some());

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn refresh_virtual_seasons_materializes_complete_episode_sets() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let worker =
            InventoryRefreshWorker::new(repository.clone(), InventoryScanOptions::default());
        let items = [
            data_root_item(
                "Example Show S01E01",
                MediaType::Episode,
                "e01.mkv",
                10,
                100,
            ),
            data_root_item(
                "Example Show S01E02",
                MediaType::Episode,
                "e02a.mkv",
                20,
                200,
            ),
            data_root_item(
                "Example Show S01E02",
                MediaType::Episode,
                "e02b.mkv",
                25,
                150,
            ),
            data_root_item(
                "Example Show S01E03",
                MediaType::Episode,
                "e03.mkv",
                30,
                300,
            ),
        ];
        repository
            .replace_local_inventory_stream(
                LocalInventoryScope::DataRoot,
                items.iter().map(local_item_file_batch),
            )
            .await
            .unwrap();

        worker
            .refresh_virtual_seasons(VIRTUAL_SEASON_INCOMPLETE_MIN_AGE_MS + 1_000)
            .await
            .unwrap();

        let virtual_seasons = repository
            .local_items_by_media_type(MediaType::SeasonPack, 10)
            .await
            .unwrap()
            .into_iter()
            .filter(|item| matches!(item.source, LocalItemSource::Virtual { .. }))
            .collect::<Vec<_>>();

        assert_eq!(1, virtual_seasons.len());
        assert_eq!("Example Show S01", virtual_seasons[0].title.as_str());
        assert_eq!(ByteSize::new(65), virtual_seasons[0].total_size);
        assert_eq!(Some(300), virtual_seasons[0].mtime_ms);
        assert_eq!(Some(PathBuf::from("/")), virtual_seasons[0].path);
        let files = repository
            .local_files_for_item(virtual_seasons[0].id.unwrap(), 10)
            .await
            .unwrap();
        assert_eq!(vec![10, 25, 30], file_sizes(&files));
        assert_eq!(PathBuf::from("media/e02b.mkv"), files[1].relative_path);
    }

    #[tokio::test]
    async fn refresh_virtual_seasons_pages_large_episode_inventory() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let worker =
            InventoryRefreshWorker::new(repository.clone(), InventoryScanOptions::default());
        let season_count = usize::from(VIRTUAL_SEASON_PAGE_SIZE) + 8;
        let mut items = Vec::with_capacity(season_count * 3);
        for season_index in 0..season_count {
            for episode in 1..=3 {
                items.push(data_root_item(
                    &format!("Paged Show {season_index:04} S01E{episode:02}"),
                    MediaType::Episode,
                    &format!("paged-{season_index:04}-e{episode:02}.mkv"),
                    u64::try_from(episode).unwrap(),
                    100 + i64::from(episode),
                ));
            }
            items.push(data_root_item(
                &format!("Existing Show {season_index:04} S01"),
                MediaType::SeasonPack,
                &format!("existing-{season_index:04}.mkv"),
                100,
                100,
            ));
        }
        let last_title = format!("Paged Show {:04} S01", season_count - 1);
        items.push(data_root_item(
            &last_title,
            MediaType::SeasonPack,
            "paged-last-real-pack.mkv",
            100,
            100,
        ));
        repository
            .replace_local_inventory_stream(
                LocalInventoryScope::DataRoot,
                items.iter().map(local_item_file_batch),
            )
            .await
            .unwrap();

        worker
            .refresh_virtual_seasons(VIRTUAL_SEASON_INCOMPLETE_MIN_AGE_MS + 1_000)
            .await
            .unwrap();

        let virtual_seasons = repository
            .local_items_by_media_type(MediaType::SeasonPack, 2_000)
            .await
            .unwrap()
            .into_iter()
            .filter(|item| matches!(item.source, LocalItemSource::Virtual { .. }))
            .collect::<Vec<_>>();

        assert_eq!(season_count - 1, virtual_seasons.len());
        assert!(
            virtual_seasons
                .iter()
                .any(|item| item.title.as_str() == "Paged Show 0000 S01")
        );
        assert!(
            virtual_seasons
                .iter()
                .all(|item| item.title.as_str() != last_title)
        );
    }

    #[tokio::test]
    async fn refresh_virtual_seasons_groups_normalized_non_contiguous_titles() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let worker =
            InventoryRefreshWorker::new(repository.clone(), InventoryScanOptions::default());
        let items = [
            data_root_item(
                "Example Show S01E01",
                MediaType::Episode,
                "example-e01.mkv",
                10,
                100,
            ),
            data_root_item(
                "Example Zebra S01E01",
                MediaType::Episode,
                "zebra-e01.mkv",
                10,
                100,
            ),
            data_root_item(
                "Example.Show S01E02",
                MediaType::Episode,
                "example-e02.mkv",
                10,
                100,
            ),
            data_root_item(
                "Example.Show S01E03",
                MediaType::Episode,
                "example-e03.mkv",
                10,
                100,
            ),
        ];
        repository
            .replace_local_inventory_stream(
                LocalInventoryScope::DataRoot,
                items.iter().map(local_item_file_batch),
            )
            .await
            .unwrap();

        worker
            .refresh_virtual_seasons(VIRTUAL_SEASON_INCOMPLETE_MIN_AGE_MS + 1_000)
            .await
            .unwrap();

        let virtual_seasons = repository
            .local_items_by_media_type(MediaType::SeasonPack, 10)
            .await
            .unwrap()
            .into_iter()
            .filter(|item| matches!(item.source, LocalItemSource::Virtual { .. }))
            .collect::<Vec<_>>();

        assert_eq!(1, virtual_seasons.len());
        assert_eq!("Example Show S01", virtual_seasons[0].title.as_str());
    }

    #[tokio::test]
    async fn refresh_virtual_seasons_suppresses_real_and_young_incomplete_packs() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let worker =
            InventoryRefreshWorker::new(repository.clone(), InventoryScanOptions::default())
                .with_season_from_episodes(0.75);
        let items = vec![
            data_root_item("Real Pack S01", MediaType::SeasonPack, "pack.mkv", 99, 900),
            data_root_item(
                "Real Pack S01E01",
                MediaType::Episode,
                "real-e01.mkv",
                10,
                100,
            ),
            data_root_item(
                "Real Pack S01E02",
                MediaType::Episode,
                "real-e02.mkv",
                10,
                100,
            ),
            data_root_item(
                "Real Pack S01E03",
                MediaType::Episode,
                "real-e03.mkv",
                10,
                100,
            ),
            data_root_item(
                "Young Show S01E01",
                MediaType::Episode,
                "young-e01.mkv",
                10,
                900,
            ),
            data_root_item(
                "Young Show S01E02",
                MediaType::Episode,
                "young-e02.mkv",
                10,
                900,
            ),
            data_root_item(
                "Young Show S01E04",
                MediaType::Episode,
                "young-e04.mkv",
                10,
                900,
            ),
            data_root_item(
                "Old Show S01E01",
                MediaType::Episode,
                "old-e01.mkv",
                10,
                100,
            ),
            data_root_item(
                "Old Show S01E02",
                MediaType::Episode,
                "old-e02.mkv",
                10,
                100,
            ),
            data_root_item(
                "Old Show S01E04",
                MediaType::Episode,
                "old-e04.mkv",
                10,
                100,
            ),
        ];
        repository
            .replace_local_inventory_stream(
                LocalInventoryScope::DataRoot,
                items.iter().map(local_item_file_batch),
            )
            .await
            .unwrap();

        worker
            .refresh_virtual_seasons(VIRTUAL_SEASON_INCOMPLETE_MIN_AGE_MS + 500)
            .await
            .unwrap();

        let virtual_seasons = repository
            .local_items_by_media_type(MediaType::SeasonPack, 10)
            .await
            .unwrap()
            .into_iter()
            .filter(|item| matches!(item.source, LocalItemSource::Virtual { .. }))
            .collect::<Vec<_>>();

        let virtual_titles = virtual_seasons
            .iter()
            .map(|item| item.title.as_str().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(vec!["Old Show S01"], virtual_titles);
        assert_eq!(ByteSize::new(40), virtual_seasons[0].total_size);
    }

    #[tokio::test]
    async fn refresh_virtual_seasons_classifies_client_items_and_real_packs() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let worker =
            InventoryRefreshWorker::new(repository.clone(), InventoryScanOptions::default());
        let host = ClientHost::new("qbit.local").unwrap();
        let episodes = vec![
            client_inventory_item(
                host.clone(),
                "0123456789abcdef0123456789abcdef01234561",
                "Client Show S01E01",
                "client-e01.mkv",
                10,
            )
            .into_scanned()
            .unwrap(),
            client_inventory_item(
                host.clone(),
                "0123456789abcdef0123456789abcdef01234562",
                "Client Show S01E02",
                "client-e02.mkv",
                10,
            )
            .into_scanned()
            .unwrap(),
            client_inventory_item(
                host.clone(),
                "0123456789abcdef0123456789abcdef01234563",
                "Client Show S01E03",
                "client-e03.mkv",
                10,
            )
            .into_scanned()
            .unwrap(),
        ];
        worker
            .refresh_client_items(host.clone(), &episodes)
            .await
            .unwrap();
        let created = repository
            .local_items_by_media_type(MediaType::SeasonPack, 10)
            .await
            .unwrap()
            .into_iter()
            .any(|item| {
                item.title.as_str() == "Client Show S01"
                    && matches!(item.source, LocalItemSource::Virtual { .. })
            });
        assert!(created);

        let real_pack = vec![
            client_inventory_item(
                host.clone(),
                "0123456789abcdef0123456789abcdef01234571",
                "Client Show S01",
                "client-pack.mkv",
                30,
            )
            .into_scanned()
            .unwrap(),
        ];
        worker.refresh_client_items(host, &real_pack).await.unwrap();

        let created_after_pack = repository
            .local_items_by_media_type(MediaType::SeasonPack, 10)
            .await
            .unwrap()
            .into_iter()
            .filter(|item| {
                item.title.as_str() == "Client Show S01"
                    && matches!(item.source, LocalItemSource::Virtual { .. })
            })
            .count();
        assert_eq!(0, created_after_pack);
    }

    #[tokio::test]
    async fn refresh_virtual_season_source_keys_include_stable_hash() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let worker =
            InventoryRefreshWorker::new(repository.clone(), InventoryScanOptions::default());
        let items = [
            data_root_item(
                "Æther Show S01E01",
                MediaType::Episode,
                "ae-e01.mkv",
                10,
                100,
            ),
            data_root_item(
                "Æther Show S01E02",
                MediaType::Episode,
                "ae-e02.mkv",
                10,
                100,
            ),
            data_root_item(
                "Æther Show S01E03",
                MediaType::Episode,
                "ae-e03.mkv",
                10,
                100,
            ),
            data_root_item(
                "東京 Show S01E01",
                MediaType::Episode,
                "tokyo-e01.mkv",
                10,
                100,
            ),
            data_root_item(
                "東京 Show S01E02",
                MediaType::Episode,
                "tokyo-e02.mkv",
                10,
                100,
            ),
            data_root_item(
                "東京 Show S01E03",
                MediaType::Episode,
                "tokyo-e03.mkv",
                10,
                100,
            ),
        ];
        repository
            .replace_local_inventory_stream(
                LocalInventoryScope::DataRoot,
                items.iter().map(local_item_file_batch),
            )
            .await
            .unwrap();

        worker
            .refresh_virtual_seasons(VIRTUAL_SEASON_INCOMPLETE_MIN_AGE_MS + 1_000)
            .await
            .unwrap();

        let source_keys = repository
            .local_items_by_media_type(MediaType::SeasonPack, 10)
            .await
            .unwrap()
            .into_iter()
            .filter_map(|item| match item.source {
                LocalItemSource::Virtual { source_key } => Some(source_key.to_string()),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert_eq!(2, source_keys.len());
        assert_ne!(source_keys[0], source_keys[1]);
        assert!(
            source_keys
                .iter()
                .all(|key| key.len() > "season:show:s01:".len())
        );
    }

    #[tokio::test]
    async fn refresh_data_dirs_until_shutdown_cancels_before_scan() {
        let root = unique_temp_dir("shutdown-before-scan");
        let release = root.join("Movie.2026.1080p");
        fs::create_dir_all(&release).unwrap();
        write_file(&release.join("movie.mkv"), 10);
        let repository = Repository::connect_in_memory().await.unwrap();
        let worker =
            InventoryRefreshWorker::new(repository.clone(), InventoryScanOptions::default());
        let (shutdown, signal) = crate::runtime::shutdown::shutdown_channel();
        shutdown.cancel_now("test shutdown").unwrap();

        let error = worker
            .refresh_data_dirs_until_shutdown(
                InventoryRefreshRequest::full(vec![root.clone()]),
                signal,
            )
            .await
            .unwrap_err();
        let local_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM local_items")
            .fetch_one(repository.pool())
            .await
            .unwrap();

        assert!(matches!(error, InventoryRefreshError::Cancelled { .. }));
        assert_eq!(0, local_count);

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn refresh_data_dirs_until_shutdown_cancels_mid_scan() {
        let root = unique_temp_dir("shutdown-mid-scan");
        for index in 0..128 {
            let release = root.join(format!("Movie.{index:03}.2026.1080p"));
            fs::create_dir_all(&release).unwrap();
            write_file(&release.join("movie.mkv"), 10);
        }
        let repository = Repository::connect_in_memory().await.unwrap();
        let send_attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let worker =
            InventoryRefreshWorker::new(repository.clone(), InventoryScanOptions::default())
                .with_data_root_scan_send_attempts(send_attempts.clone());
        let (shutdown, signal) = crate::runtime::shutdown::shutdown_channel();
        let scan_root = root.clone();
        let refresh = tokio::spawn(async move {
            worker
                .refresh_data_dirs_until_shutdown(
                    InventoryRefreshRequest::full(vec![scan_root]),
                    signal,
                )
                .await
        });

        tokio::time::timeout(Duration::from_secs(2), async {
            while send_attempts.load(Ordering::SeqCst) <= DATA_ROOT_SCAN_BUFFER {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        shutdown.cancel_now("test shutdown").unwrap();
        let error = tokio::time::timeout(Duration::from_secs(2), refresh)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();

        assert!(matches!(error, InventoryRefreshError::Cancelled { .. }));
        let staged_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM temp.staged_local_inventory_items")
                .fetch_one(repository.pool())
                .await
                .unwrap();
        let staged_file_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM temp.staged_local_inventory_files")
                .fetch_one(repository.pool())
                .await
                .unwrap();

        assert_eq!(0, staged_count);
        assert_eq!(0, staged_file_count);

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn refresh_data_dirs_until_shutdown_cancels_before_staging_connection() {
        let root = unique_temp_dir("shutdown-before-staging");
        for index in 0..128 {
            let release = root.join(format!("Movie.{index:03}.2026.1080p"));
            fs::create_dir_all(&release).unwrap();
            write_file(&release.join("movie.mkv"), 10);
        }
        let repository = Repository::connect(root.join("sporos.sqlite"))
            .await
            .unwrap();
        let mut held_connections = Vec::new();
        for _ in 0..5 {
            held_connections.push(repository.pool().acquire().await.unwrap());
        }
        let send_attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let worker =
            InventoryRefreshWorker::new(repository.clone(), InventoryScanOptions::default())
                .with_data_root_scan_send_attempts(send_attempts.clone());
        let (shutdown, signal) = crate::runtime::shutdown::shutdown_channel();
        let scan_root = root.clone();
        let refresh = tokio::spawn(async move {
            worker
                .refresh_data_dirs_until_shutdown(
                    InventoryRefreshRequest::full(vec![scan_root]),
                    signal,
                )
                .await
        });

        tokio::time::timeout(Duration::from_secs(2), async {
            while send_attempts.load(Ordering::SeqCst) <= DATA_ROOT_SCAN_BUFFER {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        shutdown.cancel_now("test shutdown").unwrap();
        let error = tokio::time::timeout(Duration::from_secs(2), refresh)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();

        assert!(matches!(error, InventoryRefreshError::Cancelled { .. }));

        drop(held_connections);
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn refresh_data_dirs_prunes_deleted_items_incrementally() {
        let root = unique_temp_dir("incremental");
        let first = root.join("First.2026.1080p");
        let second = root.join("Second.2026.1080p");
        fs::create_dir_all(&first).unwrap();
        fs::create_dir_all(&second).unwrap();
        write_file(&first.join("first.mkv"), 10);
        write_file(&second.join("second.mkv"), 20);
        let repository = Repository::connect_in_memory().await.unwrap();
        let worker =
            InventoryRefreshWorker::new(repository.clone(), InventoryScanOptions::default());
        let request = InventoryRefreshRequest::full(vec![root.clone()]);

        let first_summary = worker.refresh_data_dirs(request.clone()).await.unwrap();
        fs::remove_dir_all(&first).unwrap();
        let second_summary = worker.refresh_data_dirs(request).await.unwrap();

        let local_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM local_items")
            .fetch_one(repository.pool())
            .await
            .unwrap();
        let row = sqlx::query("SELECT display_name, total_size FROM local_items")
            .fetch_one(repository.pool())
            .await
            .unwrap();
        let display_name: String = row.get("display_name");
        let total_size: i64 = row.get("total_size");

        assert_eq!(2, first_summary.persisted_items);
        assert_eq!(1, second_summary.persisted_items);
        assert_eq!(1, second_summary.pruned_items);
        assert_eq!(1, local_count);
        assert_eq!("Second.2026.1080p", display_name);
        assert_eq!(20, total_size);

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn refresh_data_dirs_changed_path_refreshes_only_changed_item_root() {
        let root = unique_temp_dir("changed-path");
        let first = root.join("First.2026.1080p");
        let second = root.join("Second.2026.1080p");
        fs::create_dir_all(&first).unwrap();
        fs::create_dir_all(&second).unwrap();
        let first_file = first.join("first.mkv");
        write_file(&first_file, 10);
        write_file(&second.join("second.mkv"), 20);
        let repository = Repository::connect_in_memory().await.unwrap();
        let worker =
            InventoryRefreshWorker::new(repository.clone(), InventoryScanOptions::default());

        worker
            .refresh_data_dirs(InventoryRefreshRequest::full(vec![root.clone()]))
            .await
            .unwrap();
        write_file(&first_file, 30);
        let summary = worker
            .refresh_data_dirs(InventoryRefreshRequest::changed_paths(
                vec![root.clone()],
                vec![first_file],
            ))
            .await
            .unwrap();

        let rows =
            sqlx::query("SELECT display_name, total_size FROM local_items ORDER BY display_name")
                .fetch_all(repository.pool())
                .await
                .unwrap();
        let values = rows
            .into_iter()
            .map(|row| {
                (
                    row.get::<String, _>("display_name"),
                    row.get::<i64, _>("total_size"),
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(1, summary.scanned_items);
        assert_eq!(1, summary.persisted_items);
        assert_eq!(0, summary.pruned_items);
        assert_eq!(
            vec![
                ("First.2026.1080p".to_owned(), 30),
                ("Second.2026.1080p".to_owned(), 20),
            ],
            values
        );

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn refresh_data_dirs_changed_path_prefers_nested_media_root() {
        let root = unique_temp_dir("changed-nested-root");
        let child_root = root.join("movies");
        let first = child_root.join("First.2026.1080p");
        let second = child_root.join("Second.2026.1080p");
        fs::create_dir_all(&first).unwrap();
        fs::create_dir_all(&second).unwrap();
        let first_file = first.join("first.mkv");
        write_file(&first_file, 10);
        write_file(&second.join("second.mkv"), 20);
        let repository = Repository::connect_in_memory().await.unwrap();
        let worker =
            InventoryRefreshWorker::new(repository.clone(), InventoryScanOptions::default());

        worker
            .refresh_data_dirs(InventoryRefreshRequest::full(vec![child_root.clone()]))
            .await
            .unwrap();
        write_file(&first_file, 30);
        let summary = worker
            .refresh_data_dirs(InventoryRefreshRequest::changed_paths(
                vec![root.clone(), child_root],
                vec![first_file],
            ))
            .await
            .unwrap();

        let rows =
            sqlx::query("SELECT display_name, total_size FROM local_items ORDER BY display_name")
                .fetch_all(repository.pool())
                .await
                .unwrap();
        let values = rows
            .into_iter()
            .map(|row| {
                (
                    row.get::<String, _>("display_name"),
                    row.get::<i64, _>("total_size"),
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(1, summary.scanned_items);
        assert_eq!(1, summary.persisted_items);
        assert_eq!(0, summary.pruned_items);
        assert_eq!(
            vec![
                ("First.2026.1080p".to_owned(), 30),
                ("Second.2026.1080p".to_owned(), 20),
            ],
            values
        );

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn refresh_data_dirs_changed_deleted_root_prunes_without_full_scan() {
        let root = unique_temp_dir("changed-delete-root");
        let first = root.join("First.2026.1080p");
        let second = root.join("Second.2026.1080p");
        fs::create_dir_all(&first).unwrap();
        fs::create_dir_all(&second).unwrap();
        write_file(&first.join("first.mkv"), 10);
        write_file(&second.join("second.mkv"), 20);
        let repository = Repository::connect_in_memory().await.unwrap();
        let worker =
            InventoryRefreshWorker::new(repository.clone(), InventoryScanOptions::default());

        worker
            .refresh_data_dirs(InventoryRefreshRequest::full(vec![root.clone()]))
            .await
            .unwrap();
        fs::remove_dir_all(&first).unwrap();
        let summary = worker
            .refresh_data_dirs(InventoryRefreshRequest::changed_paths(
                vec![root.clone()],
                vec![first],
            ))
            .await
            .unwrap();

        let names = sqlx::query_scalar::<_, String>(
            "SELECT display_name FROM local_items ORDER BY display_name",
        )
        .fetch_all(repository.pool())
        .await
        .unwrap();

        assert_eq!(0, summary.scanned_items);
        assert_eq!(0, summary.persisted_items);
        assert_eq!(1, summary.pruned_items);
        assert_eq!(vec!["Second.2026.1080p"], names);

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn refresh_data_dirs_changed_deleted_direct_file_prunes_exact_root() {
        let root = unique_temp_dir("changed-delete-file");
        let movie = root.join("Standalone.2026.mkv");
        write_file(&movie, 10);
        let repository = Repository::connect_in_memory().await.unwrap();
        let worker =
            InventoryRefreshWorker::new(repository.clone(), InventoryScanOptions::default());

        worker
            .refresh_data_dirs(InventoryRefreshRequest::full(vec![root.clone()]))
            .await
            .unwrap();
        fs::remove_file(&movie).unwrap();
        let summary = worker
            .refresh_data_dirs(InventoryRefreshRequest::changed_paths(
                vec![root.clone()],
                vec![movie],
            ))
            .await
            .unwrap();

        let local_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM local_items")
            .fetch_one(repository.pool())
            .await
            .unwrap();

        assert_eq!(0, summary.scanned_items);
        assert_eq!(0, summary.persisted_items);
        assert_eq!(1, summary.pruned_items);
        assert_eq!(0, local_count);

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn refresh_data_dirs_prunes_deleted_items_after_full_two_root_scan() {
        let root = unique_temp_dir("two-root-clean-prune");
        let first_root = root.join("mounted-a");
        let second_root = root.join("mounted-b");
        let first = first_root.join("First.2026.1080p");
        let second = second_root.join("Second.2026.1080p");
        fs::create_dir_all(&first).unwrap();
        fs::create_dir_all(&second).unwrap();
        write_file(&first.join("first.mkv"), 10);
        write_file(&second.join("second.mkv"), 20);
        let repository = Repository::connect_in_memory().await.unwrap();
        let worker =
            InventoryRefreshWorker::new(repository.clone(), InventoryScanOptions::default());
        let request = InventoryRefreshRequest::full(vec![first_root.clone(), second_root.clone()]);

        let first_summary = worker.refresh_data_dirs(request.clone()).await.unwrap();
        fs::remove_dir_all(&first).unwrap();
        let second_summary = worker.refresh_data_dirs(request).await.unwrap();

        let names = sqlx::query_scalar::<_, String>(
            "SELECT display_name FROM local_items ORDER BY display_name",
        )
        .fetch_all(repository.pool())
        .await
        .unwrap();

        assert_eq!(2, first_summary.persisted_items);
        assert_eq!(0, first_summary.pruned_items);
        assert!(first_summary.scan_failures.is_empty());
        assert_eq!(1, second_summary.scanned_items);
        assert_eq!(1, second_summary.persisted_items);
        assert_eq!(1, second_summary.pruned_items);
        assert!(second_summary.scan_failures.is_empty());
        assert_eq!(vec!["Second.2026.1080p"], names);

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn refresh_data_dirs_retains_all_rows_after_two_root_scan_failure() {
        let root = unique_temp_dir("two-root-partial-retain");
        let first_root = root.join("mounted-a");
        let second_root = root.join("mounted-b");
        let first = first_root.join("First.2026.1080p");
        let second = second_root.join("Second.2026.1080p");
        fs::create_dir_all(&first).unwrap();
        fs::create_dir_all(&second).unwrap();
        write_file(&first.join("first.mkv"), 10);
        write_file(&second.join("second.mkv"), 20);
        let repository = Repository::connect_in_memory().await.unwrap();
        let worker =
            InventoryRefreshWorker::new(repository.clone(), InventoryScanOptions::default());
        let request = InventoryRefreshRequest::full(vec![first_root.clone(), second_root.clone()]);

        let first_summary = worker.refresh_data_dirs(request.clone()).await.unwrap();
        fs::remove_dir_all(&first_root).unwrap();
        let second_summary = worker.refresh_data_dirs(request).await.unwrap();

        let names = sqlx::query_scalar::<_, String>(
            "SELECT display_name FROM local_items ORDER BY display_name",
        )
        .fetch_all(repository.pool())
        .await
        .unwrap();

        assert_eq!(2, first_summary.persisted_items);
        assert_eq!(0, first_summary.pruned_items);
        assert!(first_summary.scan_failures.is_empty());
        assert_eq!(1, second_summary.scanned_items);
        assert_eq!(0, second_summary.persisted_items);
        assert_eq!(0, second_summary.pruned_items);
        assert_eq!(1, second_summary.scan_failures.len());
        assert_eq!(vec!["First.2026.1080p", "Second.2026.1080p"], names);

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn refresh_data_dirs_partial_clean_root_cannot_prune_other_roots() {
        let root = unique_temp_dir("two-root-partial-clean");
        let first_root = root.join("mounted-a");
        let second_root = root.join("mounted-b");
        let first = first_root.join("First.2026.1080p");
        let second = second_root.join("Second.2026.1080p");
        fs::create_dir_all(&first).unwrap();
        fs::create_dir_all(&second).unwrap();
        write_file(&first.join("first.mkv"), 10);
        write_file(&second.join("second.mkv"), 20);
        let repository = Repository::connect_in_memory().await.unwrap();
        let worker =
            InventoryRefreshWorker::new(repository.clone(), InventoryScanOptions::default());

        let first_summary = worker
            .refresh_data_dirs(InventoryRefreshRequest::full(vec![
                first_root.clone(),
                second_root.clone(),
            ]))
            .await
            .unwrap();
        let second_summary = worker
            .refresh_data_dirs(InventoryRefreshRequest::full(vec![second_root]))
            .await
            .unwrap();

        let names = sqlx::query_scalar::<_, String>(
            "SELECT display_name FROM local_items ORDER BY display_name",
        )
        .fetch_all(repository.pool())
        .await
        .unwrap();

        assert_eq!(2, first_summary.persisted_items);
        assert_eq!(0, first_summary.pruned_items);
        assert!(first_summary.scan_failures.is_empty());
        assert_eq!(1, second_summary.scanned_items);
        assert_eq!(1, second_summary.persisted_items);
        assert_eq!(0, second_summary.pruned_items);
        assert!(second_summary.scan_failures.is_empty());
        assert_eq!(vec!["First.2026.1080p", "Second.2026.1080p"], names);

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn refresh_data_dirs_retains_existing_rows_after_scan_failure() {
        let root = unique_temp_dir("partial-failure");
        let first_root = root.join("mounted-a");
        let second_root = root.join("mounted-b");
        let second = second_root.join("Second.2026.1080p");
        fs::create_dir_all(&second).unwrap();
        write_file(&second.join("second.mkv"), 20);
        let repository = Repository::connect_in_memory().await.unwrap();
        let worker =
            InventoryRefreshWorker::new(repository.clone(), InventoryScanOptions::default());
        let existing = [
            data_root_item("First.2026.1080p", MediaType::Movie, "first.mkv", 10, 100),
            data_root_item(
                "Example Show S01E01",
                MediaType::Episode,
                "episode-a.mkv",
                10,
                100,
            ),
            data_root_item(
                "Example Show S01E02",
                MediaType::Episode,
                "episode-b.mkv",
                20,
                200,
            ),
            data_root_item(
                "Example Show S01E03",
                MediaType::Episode,
                "episode-c.mkv",
                30,
                300,
            ),
            data_root_item("Second.2026.1080p", MediaType::Movie, "second.mkv", 20, 100),
        ];
        repository
            .replace_local_inventory_stream(
                LocalInventoryScope::DataRoot,
                existing.iter().map(local_item_file_batch),
            )
            .await
            .unwrap();
        let request = InventoryRefreshRequest::full(vec![first_root.clone(), second_root.clone()]);

        worker
            .refresh_virtual_seasons(VIRTUAL_SEASON_INCOMPLETE_MIN_AGE_MS + 1_000)
            .await
            .unwrap();
        let second_summary = worker.refresh_data_dirs(request).await.unwrap();

        let local_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM local_items")
            .fetch_one(repository.pool())
            .await
            .unwrap();
        let names = sqlx::query_scalar::<_, String>(
            "SELECT display_name FROM local_items ORDER BY display_name",
        )
        .fetch_all(repository.pool())
        .await
        .unwrap();
        let virtual_seasons = repository
            .local_items_by_media_type(MediaType::SeasonPack, 10)
            .await
            .unwrap()
            .into_iter()
            .filter(|item| matches!(item.source, LocalItemSource::Virtual { .. }))
            .collect::<Vec<_>>();

        assert_eq!(1, second_summary.scanned_items);
        assert_eq!(0, second_summary.persisted_items);
        assert_eq!(0, second_summary.pruned_items);
        assert_eq!(1, second_summary.scan_failures.len());
        assert_eq!(6, local_count);
        assert_eq!(
            vec![
                "Example Show S01",
                "Example Show S01E01",
                "Example Show S01E02",
                "Example Show S01E03",
                "First.2026.1080p",
                "Second.2026.1080p",
            ],
            names
        );
        assert_eq!(1, virtual_seasons.len());
        assert_eq!("Example Show S01", virtual_seasons[0].title.as_str());

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn refresh_data_dirs_wakes_source_incomplete_announcements() {
        let root = unique_temp_dir("announce-wake");
        let repository = Repository::connect_in_memory().await.unwrap();
        let worker =
            InventoryRefreshWorker::new(repository.clone(), InventoryScanOptions::default());
        insert_waiting_announce(
            &repository,
            "ann_inventory",
            "guid-inventory",
            AnnounceReason::SourceIncomplete,
            None,
        )
        .await;

        worker
            .refresh_data_dirs(InventoryRefreshRequest::full(vec![root.clone()]))
            .await
            .unwrap();

        let status: String =
            sqlx::query_scalar("SELECT status FROM announce_work WHERE id = 'ann_inventory'")
                .fetch_one(repository.pool())
                .await
                .unwrap();
        assert_eq!("queued", status);

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn inventory_refresh_worker_consumes_bounded_queue() {
        let root = unique_temp_dir("queue");
        let release = root.join("Queued.2026.1080p");
        fs::create_dir_all(&release).unwrap();
        write_file(&release.join("queued.mkv"), 10);
        let repository = Repository::connect_in_memory().await.unwrap();
        let worker =
            InventoryRefreshWorker::new(repository.clone(), InventoryScanOptions::default());
        let (queue, receiver) = inventory_refresh_queue(NonZeroUsize::new(1).unwrap());

        queue
            .try_enqueue(InventoryRefreshRequest::full(vec![root.clone()]))
            .unwrap();
        drop(queue);
        let (_shutdown, signal) = crate::runtime::shutdown::shutdown_channel();
        run_inventory_refresh_worker(worker, receiver, signal).await;

        let local_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM local_items")
            .fetch_one(repository.pool())
            .await
            .unwrap();

        assert_eq!(1, local_count);

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn media_watch_event_collection_deduplicates_changed_paths() {
        let first = PathBuf::from("/media/First.2026.1080p/first.mkv");
        let second = PathBuf::from("/media/Second.2026.1080p/second.mkv");
        let mut pending = Vec::new();

        assert!(collect_media_watch_event(
            &mut pending,
            Event {
                kind: EventKind::Modify(notify::event::ModifyKind::Any),
                paths: vec![first.clone(), second.clone(), first.clone()],
                attrs: notify::event::EventAttributes::new(),
            },
        ));

        assert_eq!(vec![first, second], pending);
    }

    #[test]
    fn media_watch_probe_matches_exact_event_path() {
        let probe = PathBuf::from("/media/.sporos-notify-probe.tmp");
        let other = PathBuf::from("/media/Movie.2026/movie.mkv");
        let event = Event {
            kind: EventKind::Create(notify::event::CreateKind::File),
            paths: vec![other, probe.clone()],
            attrs: notify::event::EventAttributes::new(),
        };

        assert!(event_contains_path(&event, &probe));
        assert!(!event_contains_path(&event, Path::new("/media/other.tmp")));
    }

    #[test]
    fn media_watch_classification_uses_polling_for_network_roots_before_probe() {
        let local = PathBuf::from("/media/local");
        let nfs = PathBuf::from("/media/nfs");
        let (native_dirs, polling_dirs) = classify_media_watch_dirs_with(
            &[local.clone(), nfs.clone()],
            |path| {
                if path == nfs {
                    Some("nfs filesystem".to_owned())
                } else {
                    None
                }
            },
            |path| {
                assert_ne!(path, nfs.as_path());
                Ok(())
            },
        );

        assert_eq!(vec![local], native_dirs);
        assert_eq!(vec![nfs], polling_dirs);
    }

    #[test]
    fn media_watch_classification_falls_back_to_polling_when_probe_fails() {
        let local = PathBuf::from("/media/local");
        let (native_dirs, polling_dirs) = classify_media_watch_dirs_with(
            std::slice::from_ref(&local),
            |_| None,
            |_| Err("probe file creation failed: permission denied".to_owned()),
        );

        assert!(native_dirs.is_empty());
        assert_eq!(vec![local], polling_dirs);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_media_watch_classification_prefers_polling_for_network_filesystems() {
        assert_eq!(Some("nfs filesystem"), linux_fs_type_polling_reason(0x6969));
        assert_eq!(
            Some("fuse filesystem"),
            linux_fs_type_polling_reason(0x6573_5546)
        );
        assert_eq!(None, linux_fs_type_polling_reason(0xEF53));
    }

    #[tokio::test]
    async fn media_watch_changed_paths_are_retained_when_queue_is_full() {
        let media_dir = PathBuf::from("/media");
        let changed = PathBuf::from("/media/First.2026.1080p/first.mkv");
        let (queue, mut receiver) = inventory_refresh_queue(NonZeroUsize::new(1).unwrap());
        queue
            .try_enqueue(InventoryRefreshRequest::full(vec![media_dir.clone()]))
            .unwrap();
        let mut pending = Vec::new();

        let outcome = try_enqueue_media_changed_paths(
            std::slice::from_ref(&media_dir),
            &queue,
            vec![changed.clone()],
        );
        match outcome {
            ChangedPathEnqueue::Full { changed_paths } => {
                extend_unique_paths(&mut pending, changed_paths);
            }
            other => panic!("expected full queue, got {other:?}"),
        }
        assert_eq!(vec![changed.clone()], pending);

        let _queued = receiver.recv().await.unwrap();
        let pending_since =
            flush_media_changed_paths(&[media_dir], &queue, &mut pending, Some(Instant::now()));
        assert!(pending_since.is_none());
        assert!(pending.is_empty());
        let request = receiver.recv().await.unwrap();
        assert_eq!(vec![changed], request.changed_paths);
    }

    #[tokio::test]
    async fn inventory_refresh_worker_records_partial_failures_and_continues() {
        let missing = unique_temp_dir("queue-partial-missing");
        fs::remove_dir_all(&missing).unwrap();
        let root = unique_temp_dir("queue-partial-next");
        let release = root.join("Queued.2026.1080p");
        fs::create_dir_all(&release).unwrap();
        write_file(&release.join("queued.mkv"), 10);
        let repository = Repository::connect_in_memory().await.unwrap();
        let worker =
            InventoryRefreshWorker::new(repository.clone(), InventoryScanOptions::default());
        let (queue, receiver) = inventory_refresh_queue(NonZeroUsize::new(2).unwrap());

        queue
            .try_enqueue(InventoryRefreshRequest::full(vec![missing]))
            .unwrap();
        queue
            .try_enqueue(InventoryRefreshRequest::full(vec![root.clone()]))
            .unwrap();
        drop(queue);
        let (_shutdown, signal) = crate::runtime::shutdown::shutdown_channel();
        run_inventory_refresh_worker(worker, receiver, signal).await;

        let local_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM local_items")
            .fetch_one(repository.pool())
            .await
            .unwrap();
        let health = inventory_refresh_health(&repository).await.unwrap();

        assert_eq!(1, local_count);
        assert_eq!("healthy", health.state);

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn inventory_refresh_worker_retries_database_failures_before_completing() {
        let root = unique_temp_dir("queue-retry-db");
        let release = root.join("Recovered.2026.1080p");
        fs::create_dir_all(&release).unwrap();
        write_file(&release.join("recovered.mkv"), 10);
        let repository = Repository::connect_in_memory().await.unwrap();
        repository.pool().close().await;
        let worker =
            InventoryRefreshWorker::new(repository.clone(), InventoryScanOptions::default());
        let (queue, receiver) = inventory_refresh_queue(NonZeroUsize::new(1).unwrap());

        queue
            .try_enqueue(InventoryRefreshRequest::full(vec![root.clone()]))
            .unwrap();
        let (_shutdown, signal) = crate::runtime::shutdown::shutdown_channel();
        let handle = tokio::spawn(run_inventory_refresh_worker(worker, receiver, signal));

        tokio::time::sleep(Duration::from_millis(75)).await;

        assert_eq!(0, queue.stats().completed);
        handle.abort();
        drop(handle.await);

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn inventory_refresh_worker_stops_retry_sleep_on_shutdown() {
        let root = unique_temp_dir("queue-retry-shutdown");
        let release = root.join("Stopped.2026.1080p");
        fs::create_dir_all(&release).unwrap();
        write_file(&release.join("stopped.mkv"), 10);
        let repository = Repository::connect_in_memory().await.unwrap();
        repository.pool().close().await;
        let worker =
            InventoryRefreshWorker::new(repository.clone(), InventoryScanOptions::default());
        let (queue, receiver) = inventory_refresh_queue(NonZeroUsize::new(1).unwrap());

        queue
            .try_enqueue(InventoryRefreshRequest::full(vec![root.clone()]))
            .unwrap();
        let (shutdown, signal) = crate::runtime::shutdown::shutdown_channel();
        let handle = tokio::spawn(run_inventory_refresh_worker(worker, receiver, signal));

        tokio::time::sleep(Duration::from_millis(75)).await;
        shutdown.cancel_now("test shutdown").unwrap();

        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(0, queue.stats().completed);

        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn inventory_refresh_error_health_uses_current_backoff() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let worker =
            InventoryRefreshWorker::new(repository.clone(), InventoryScanOptions::default());

        record_inventory_refresh_health(
            &worker,
            Some("database unavailable".to_owned()),
            Some(Duration::from_secs(2)),
        )
        .await;

        let health = inventory_refresh_health(&repository).await.unwrap();
        let delay = health.retry_after_ms.unwrap() - health.checked_at_ms;

        assert_eq!("degraded", health.state);
        assert!((2_000..=2_100).contains(&delay), "delay was {delay}");
    }

    #[tokio::test]
    async fn refresh_client_items_prunes_only_one_client_scope() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let worker =
            InventoryRefreshWorker::new(repository.clone(), InventoryScanOptions::default());
        let host_a = ClientHost::new("qbit-a.local").unwrap();
        let host_b = ClientHost::new("qbit-b.local").unwrap();
        let item_a1 = client_item(
            host_a.clone(),
            "0123456789abcdef0123456789abcdef01234567",
            "First",
            "First/file-a.mkv",
            10,
        );
        let item_a2 = client_item(
            host_a.clone(),
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "Second",
            "Second/file-b.mkv",
            20,
        );
        let item_b1 = client_item(
            host_b.clone(),
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "Third",
            "Third/file-c.mkv",
            30,
        );

        worker
            .refresh_client_items(host_a.clone(), &[item_a1, item_a2.clone()])
            .await
            .unwrap();
        worker
            .refresh_client_items(host_b, &[item_b1])
            .await
            .unwrap();
        let summary = worker
            .refresh_client_items(host_a, &[item_a2])
            .await
            .unwrap();

        let client_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM local_items WHERE source_type = 'client'")
                .fetch_one(repository.pool())
                .await
                .unwrap();
        let qbit_a_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM local_items WHERE source_type = 'client' AND source_key LIKE '12:qbit-a.local:%'",
        )
        .fetch_one(repository.pool())
        .await
        .unwrap();

        assert_eq!(1, summary.pruned_items);
        assert_eq!(2, client_count);
        assert_eq!(1, qbit_a_count);
    }

    #[tokio::test]
    async fn refresh_client_items_wakes_matching_client_waits() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let worker =
            InventoryRefreshWorker::new(repository.clone(), InventoryScanOptions::default());
        let client_host = ClientHost::new("qbit.local").unwrap();
        insert_waiting_announce(
            &repository,
            "ann_client",
            "guid-client",
            AnnounceReason::ClientChecking,
            Some(("torrent_client", "qbit.local")),
        )
        .await;
        insert_waiting_announce(
            &repository,
            "ann_other",
            "guid-other",
            AnnounceReason::ClientChecking,
            Some(("torrent_client", "other.local")),
        )
        .await;

        worker
            .refresh_client_items(
                client_host.clone(),
                &[client_item(
                    client_host,
                    "0123456789abcdef0123456789abcdef01234567",
                    "First",
                    "First/file-a.mkv",
                    10,
                )],
            )
            .await
            .unwrap();

        let rows = sqlx::query("SELECT id, status FROM announce_work ORDER BY id")
            .fetch_all(repository.pool())
            .await
            .unwrap()
            .into_iter()
            .map(|row| (row.get::<String, _>("id"), row.get::<String, _>("status")))
            .collect::<Vec<_>>();

        assert_eq!(
            vec![
                ("ann_client".to_owned(), "queued".to_owned()),
                ("ann_other".to_owned(), "waiting".to_owned())
            ],
            rows
        );
    }

    #[tokio::test]
    async fn batch_client_refresh_wakes_matching_waits_after_virtual_refresh() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let worker =
            InventoryRefreshWorker::new(repository.clone(), InventoryScanOptions::default());
        let batch_worker = worker.without_client_post_refresh_work();
        let client_host = ClientHost::new("qbit.local").unwrap();
        insert_waiting_announce(
            &repository,
            "ann_client",
            "guid-client",
            AnnounceReason::ClientChecking,
            Some(("torrent_client", "qbit.local")),
        )
        .await;

        batch_worker
            .refresh_client_items(
                client_host.clone(),
                &[client_item(
                    client_host.clone(),
                    "0123456789abcdef0123456789abcdef01234567",
                    "First",
                    "First/file-a.mkv",
                    10,
                )],
            )
            .await
            .unwrap();
        let status_before = announce_status(&repository, "ann_client").await;

        worker
            .refresh_virtual_seasons_after_client_batch(&[client_host])
            .await
            .unwrap();
        let status_after = announce_status(&repository, "ann_client").await;

        assert_eq!("waiting", status_before);
        assert_eq!("queued", status_after);
    }

    #[tokio::test]
    async fn client_inventory_items_materialize_scanned_batches() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let worker =
            InventoryRefreshWorker::new(repository.clone(), InventoryScanOptions::default());
        let client_host = ClientHost::new("qbit.local").unwrap();
        let inventory = ClientInventoryItem {
            client_host: client_host.clone(),
            info_hash: InfoHash::new("0123456789abcdef0123456789abcdef01234567").unwrap(),
            display_name: DisplayName::new("Example").unwrap(),
            media_type: MediaType::Movie,
            save_path: PathBuf::from("/downloads"),
            files: vec![
                crate::domain::TorrentFile::new(
                    PathBuf::from("Example/file.mkv"),
                    ByteSize::new(42),
                    FileIndex::new(0),
                )
                .unwrap(),
            ],
        };
        let scanned = inventory.into_scanned().unwrap();

        let summary = worker
            .refresh_client_items(client_host, &[scanned])
            .await
            .unwrap();

        let row =
            sqlx::query("SELECT source_type, source_key, save_path, total_size FROM local_items")
                .fetch_one(repository.pool())
                .await
                .unwrap();
        let source_type: String = row.get("source_type");
        let source_key: String = row.get("source_key");
        let save_path: String = row.get("save_path");
        let total_size: i64 = row.get("total_size");

        assert_eq!(1, summary.persisted_items);
        assert_eq!("client", source_type);
        assert_eq!(
            "10:qbit.local:0123456789abcdef0123456789abcdef01234567",
            source_key
        );
        assert_eq!("/downloads", save_path);
        assert_eq!(42, total_size);
    }

    #[tokio::test]
    async fn refresh_client_inventory_receiver_persists_streamed_items() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let worker =
            InventoryRefreshWorker::new(repository.clone(), InventoryScanOptions::default());
        let client_host = ClientHost::new("qbit.local").unwrap();
        let (sender, receiver) = mpsc::channel(2);
        sender
            .send(ClientInventoryMessage::Item(client_inventory_item(
                client_host.clone(),
                "0123456789abcdef0123456789abcdef01234567",
                "Example",
                "Example/file.mkv",
                42,
            )))
            .await
            .unwrap();
        sender.send(ClientInventoryMessage::Finished).await.unwrap();
        drop(sender);

        let summary = worker
            .refresh_client_inventory_receiver(client_host, receiver)
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

        assert_eq!(1, summary.scanned_items);
        assert_eq!(1, summary.persisted_items);
        assert_eq!(1, item_count);
        assert_eq!(1, file_count);
    }

    #[tokio::test]
    async fn unfinished_client_inventory_receiver_rolls_back_partial_refresh() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let worker =
            InventoryRefreshWorker::new(repository.clone(), InventoryScanOptions::default());
        let client_host = ClientHost::new("qbit.local").unwrap();
        let existing = client_item(
            client_host.clone(),
            "0123456789abcdef0123456789abcdef01234567",
            "Existing",
            "Existing/file.mkv",
            10,
        );
        worker
            .refresh_client_items(client_host.clone(), &[existing])
            .await
            .unwrap();
        let (sender, receiver) = mpsc::channel(1);
        sender
            .send(ClientInventoryMessage::Item(client_inventory_item(
                client_host.clone(),
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "Partial",
                "Partial/file.mkv",
                20,
            )))
            .await
            .unwrap();
        drop(sender);

        let result = worker
            .refresh_client_inventory_receiver(client_host, receiver)
            .await;
        let existing_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM local_items WHERE source_key LIKE ?")
                .bind("%0123456789abcdef0123456789abcdef01234567")
                .fetch_one(repository.pool())
                .await
                .unwrap();
        let partial_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM local_items WHERE source_key LIKE ?")
                .bind("%aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
                .fetch_one(repository.pool())
                .await
                .unwrap();

        assert!(matches!(
            result,
            Err(InventoryRefreshError::InvalidClientInventory { .. })
        ));
        assert_eq!(1, existing_count);
        assert_eq!(0, partial_count);
    }

    #[test]
    fn client_inventory_rejects_total_size_overflow() {
        let inventory = ClientInventoryItem {
            client_host: ClientHost::new("qbit.local").unwrap(),
            info_hash: InfoHash::new("0123456789abcdef0123456789abcdef01234567").unwrap(),
            display_name: DisplayName::new("Example").unwrap(),
            media_type: MediaType::Movie,
            save_path: PathBuf::from("/downloads"),
            files: vec![
                crate::domain::TorrentFile::new(
                    PathBuf::from("first.bin"),
                    ByteSize::new(u64::MAX),
                    FileIndex::new(0),
                )
                .unwrap(),
                crate::domain::TorrentFile::new(
                    PathBuf::from("second.bin"),
                    ByteSize::new(1),
                    FileIndex::new(1),
                )
                .unwrap(),
            ],
        };

        let error = inventory.into_scanned().unwrap_err();

        assert!(error.to_string().contains("client inventory total"));
        assert!(error.to_string().contains("overflow"));
    }

    #[tokio::test]
    async fn refresh_client_items_keeps_colon_host_scopes_distinct() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let worker =
            InventoryRefreshWorker::new(repository.clone(), InventoryScanOptions::default());
        let host_base = ClientHost::new("rtorrent").unwrap();
        let host_port = ClientHost::new("rtorrent:5000").unwrap();
        let base_item = client_item(
            host_base.clone(),
            "0123456789abcdef0123456789abcdef01234567",
            "Base",
            "Base/file-a.mkv",
            10,
        );
        let port_item = client_item(
            host_port.clone(),
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "Port",
            "Port/file-b.mkv",
            20,
        );

        worker
            .refresh_client_items(host_base.clone(), std::slice::from_ref(&base_item))
            .await
            .unwrap();
        worker
            .refresh_client_items(host_port, &[port_item])
            .await
            .unwrap();
        let summary = worker.refresh_client_items(host_base, &[]).await.unwrap();

        let rows = repository
            .local_items_by_media_type(MediaType::Movie, 10)
            .await
            .unwrap();

        assert_eq!(1, summary.pruned_items);
        assert_eq!(1, rows.len());
        match &rows[0].source {
            LocalItemSource::Client { client_host, .. } => {
                assert_eq!("rtorrent:5000", client_host.as_str());
            }
            source => panic!("expected client source, got {source:?}"),
        }
    }

    #[tokio::test]
    async fn refresh_client_items_normalizes_and_prunes_legacy_client_keys() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let worker =
            InventoryRefreshWorker::new(repository.clone(), InventoryScanOptions::default());
        insert_legacy_client_item(
            &repository,
            "qbit.local:0123456789abcdef0123456789abcdef01234567",
            "Legacy Qbit",
        )
        .await;
        insert_legacy_client_item(
            &repository,
            "rtorrent:5000:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "Legacy Port",
        )
        .await;

        let summary = worker
            .refresh_client_items(ClientHost::new("qbit.local").unwrap(), &[])
            .await
            .unwrap();
        let rows = repository
            .local_items_by_media_type(MediaType::Movie, 10)
            .await
            .unwrap();

        assert_eq!(1, summary.pruned_items);
        assert_eq!(1, rows.len());
        match &rows[0].source {
            LocalItemSource::Client { client_host, .. } => {
                assert_eq!("rtorrent:5000", client_host.as_str());
            }
            source => panic!("expected client source, got {source:?}"),
        }
    }

    #[tokio::test]
    async fn refresh_client_items_pages_legacy_client_key_normalization() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let worker =
            InventoryRefreshWorker::new(repository.clone(), InventoryScanOptions::default());
        let retained_hash = format!("{:040x}", 0);
        insert_legacy_client_item(
            &repository,
            &format!("10:qbit.local:{retained_hash}"),
            "Existing Normalized Qbit",
        )
        .await;
        for index in 0..130 {
            let hash = format!("{index:040x}");
            insert_legacy_client_item(
                &repository,
                &format!("qbit.local:{hash}"),
                &format!("Legacy Qbit {index}"),
            )
            .await;
        }
        insert_legacy_client_item(
            &repository,
            "rtorrent:5000:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "Legacy Port",
        )
        .await;
        insert_legacy_client_item(&repository, "qbit.local:tracker:123", "Legacy Qbit Tracker")
            .await;
        let qbit = ClientHost::new("qbit.local").unwrap();
        let retained = client_item(
            qbit.clone(),
            &retained_hash,
            "Current Qbit",
            "current-qbit.mkv",
            10,
        );

        let summary = worker
            .refresh_client_items(qbit, &[retained])
            .await
            .unwrap();
        let legacy_qbit_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM local_items WHERE source_type = 'client' AND source_key LIKE 'qbit.local:%'",
        )
        .fetch_one(repository.pool())
        .await
        .unwrap();
        let normalized_qbit_rows = sqlx::query_as::<_, (String, String)>(
            "SELECT source_key, title FROM local_items WHERE source_type = 'client' AND source_key LIKE '10:qbit.local:%'",
        )
        .fetch_all(repository.pool())
        .await
        .unwrap();
        let all_rows = repository
            .local_items_by_media_type(MediaType::Movie, 10)
            .await
            .unwrap();

        assert_eq!(1, summary.persisted_items);
        assert_eq!(130, summary.pruned_items);
        assert_eq!(0, legacy_qbit_count);
        assert_eq!(
            vec![(
                format!("10:qbit.local:{retained_hash}"),
                "Current Qbit".to_owned()
            )],
            normalized_qbit_rows
        );
        assert_eq!(2, all_rows.len());
        assert!(all_rows.iter().any(|row| matches!(
            &row.source,
            LocalItemSource::Client { client_host, .. } if client_host.as_str() == "rtorrent:5000"
        )));
    }

    #[tokio::test]
    async fn refresh_client_items_persists_large_inventory_with_pruning() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let worker =
            InventoryRefreshWorker::new(repository.clone(), InventoryScanOptions::default());
        let client_host = ClientHost::new("qbit-large.local").unwrap();
        let items = (0..1_500)
            .map(|index| {
                client_item(
                    client_host.clone(),
                    &format!("{:040x}", index + 1),
                    &format!("Large {index}"),
                    &format!("Large/file-{index:04}.mkv"),
                    u64::try_from(index).unwrap() + 1,
                )
            })
            .collect::<Vec<_>>();

        let first_summary = worker
            .refresh_client_items(client_host.clone(), &items)
            .await
            .unwrap();
        let second_summary = worker
            .refresh_client_items(client_host, &items[..1_024])
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

        assert_eq!(1_500, first_summary.persisted_items);
        assert_eq!(0, first_summary.pruned_items);
        assert_eq!(1_024, second_summary.persisted_items);
        assert_eq!(476, second_summary.pruned_items);
        assert_eq!(1_024, item_count);
        assert_eq!(1_024, file_count);
    }

    fn write_file(path: &std::path::Path, bytes: usize) {
        fs::write(path, vec![b'x'; bytes]).unwrap();
    }

    async fn inventory_refresh_health(
        repository: &Repository,
    ) -> Option<crate::persistence::repository::DependencyHealthSnapshot> {
        repository
            .dependency_health_snapshot(10)
            .await
            .unwrap()
            .into_iter()
            .find(|health| {
                health.dependency_type == DependencyKind::LocalState.as_str()
                    && health.dependency_name.as_str() == INVENTORY_REFRESH_DEPENDENCY
            })
    }

    fn data_root_item(
        title: &str,
        media_type: MediaType,
        relative_path: &str,
        size: u64,
        mtime_ms: i64,
    ) -> ScannedLocalItem {
        ScannedLocalItem {
            item: LocalItem {
                id: None,
                source: LocalItemSource::DataRoot {
                    path: PathBuf::from(format!("/media/{relative_path}")),
                },
                title: ItemTitle::new(title).unwrap(),
                display_name: DisplayName::new(title).unwrap(),
                media_type,
                info_hash: None,
                path: Some(PathBuf::from(format!("/media/{relative_path}"))),
                save_path: None,
                total_size: ByteSize::new(size),
                mtime_ms: Some(mtime_ms),
            },
            files: vec![
                LocalFile::new(
                    None,
                    PathBuf::from(relative_path),
                    ByteSize::new(size),
                    FileIndex::new(0),
                )
                .unwrap()
                .with_mtime_ms(Some(mtime_ms)),
            ],
        }
    }

    fn file_sizes(files: &[LocalFileSnapshot]) -> Vec<u64> {
        files.iter().map(|file| file.size.get()).collect()
    }

    fn client_item(
        client_host: ClientHost,
        hash: &str,
        title: &str,
        relative_path: &str,
        size: u64,
    ) -> ScannedLocalItem {
        ScannedLocalItem {
            item: LocalItem {
                id: None,
                source: LocalItemSource::Client {
                    client_host,
                    source_key: SourceKey::new(hash).unwrap(),
                },
                title: ItemTitle::new(title).unwrap(),
                display_name: DisplayName::new(title).unwrap(),
                media_type: MediaType::Movie,
                info_hash: Some(InfoHash::new(hash).unwrap()),
                path: None,
                save_path: Some(PathBuf::from("/downloads")),
                total_size: ByteSize::new(size),
                mtime_ms: Some(1_700_000_000_000),
            },
            files: vec![
                LocalFile::new(
                    None,
                    PathBuf::from(relative_path),
                    ByteSize::new(size),
                    FileIndex::new(0),
                )
                .unwrap(),
            ],
        }
    }

    fn client_inventory_item(
        client_host: ClientHost,
        hash: &str,
        title: &str,
        relative_path: &str,
        size: u64,
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
                    ByteSize::new(size),
                    FileIndex::new(0),
                )
                .unwrap(),
            ],
        }
    }

    async fn insert_waiting_announce(
        repository: &Repository,
        id: &str,
        guid: &str,
        reason: AnnounceReason,
        dependency: Option<(&str, &str)>,
    ) {
        let now_ms = unix_time_ms();
        let tracker = TrackerName::new("tracker.example").unwrap();
        let guid = CandidateGuid::new(guid).unwrap();
        let work = AnnounceWorkItem {
            id: AnnounceWorkId::new(id).unwrap(),
            status: AnnounceStatus::Queued,
            reason: AnnounceReason::Accepted,
            dedupe_hash: AnnounceDedupeIdentity::Guid {
                tracker: tracker.clone(),
                guid: guid.clone(),
            }
            .hash(),
            title: ItemTitle::new("Example").unwrap(),
            tracker,
            guid: Some(guid),
            info_hash: None,
            size: Some(ByteSize::new(42)),
            fetch: None,
            received_at_ms: now_ms,
            updated_at_ms: now_ms,
            first_attempt_at_ms: None,
            finished_at_ms: None,
            attempt_count: 0,
            next_attempt_at_ms: now_ms,
            expires_at_ms: now_ms.saturating_add(120_000),
            lease: None,
            last_dependency_kind: None,
            last_dependency_name: None,
            last_error_class: None,
            last_redacted_message: None,
        };
        let result = repository
            .insert_or_dedupe_announce_work(&work, 10)
            .await
            .unwrap();
        assert_eq!(AnnounceInsertResult::Inserted { id: work.id }, result);
        let (dependency_kind, dependency_name) = dependency.unwrap_or(("", ""));
        sqlx::query(
            r#"
            UPDATE announce_work
            SET status = 'waiting',
                reason = ?,
                next_attempt_at = ?,
                last_dependency_kind = NULLIF(?, ''),
                last_dependency_name = NULLIF(?, '')
            WHERE id = ?
            "#,
        )
        .bind(announce_reason_key(reason))
        .bind(now_ms.saturating_add(60_000))
        .bind(dependency_kind)
        .bind(dependency_name)
        .bind(id)
        .execute(repository.pool())
        .await
        .unwrap();
    }

    async fn announce_status(repository: &Repository, id: &str) -> String {
        sqlx::query_scalar("SELECT status FROM announce_work WHERE id = ?")
            .bind(id)
            .fetch_one(repository.pool())
            .await
            .unwrap()
    }

    async fn insert_legacy_client_item(repository: &Repository, source_key: &str, title: &str) {
        sqlx::query(
            r#"
            INSERT INTO local_items (
                source_type,
                source_key,
                title,
                display_name,
                media_type,
                total_size,
                created_at,
                updated_at
            )
            VALUES ('client', ?, ?, ?, 'movie', 1, 1, 1)
            "#,
        )
        .bind(source_key)
        .bind(title)
        .bind(title)
        .execute(repository.pool())
        .await
        .unwrap();
    }

    fn announce_reason_key(reason: AnnounceReason) -> &'static str {
        match reason {
            AnnounceReason::Accepted => "accepted",
            AnnounceReason::Deduplicated => "deduplicated",
            AnnounceReason::SourceIncomplete => "source_incomplete",
            AnnounceReason::InventoryRefreshing => "inventory_refreshing",
            AnnounceReason::DependencyBackoff => "dependency_backoff",
            AnnounceReason::CandidateDownloading => "candidate_downloading",
            AnnounceReason::ClientChecking => "client_checking",
            AnnounceReason::RetryAfter => "retry_after",
            AnnounceReason::TransientDependencyFailure => "transient_dependency_failure",
            AnnounceReason::Saved => "saved",
            AnnounceReason::Injected => "injected",
            AnnounceReason::DryRun => "dry_run",
            AnnounceReason::AlreadyExists => "already_exists",
            AnnounceReason::NoMatchTerminal => "no_match_terminal",
            AnnounceReason::InvalidRequest => "invalid_request",
            AnnounceReason::UnsupportedShape => "unsupported_shape",
            AnnounceReason::UnsafePath => "unsafe_path",
            AnnounceReason::InvalidTorrentMetadata => "invalid_torrent_metadata",
            AnnounceReason::Expired => "expired",
        }
    }

    fn unique_temp_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("sporos-refresh-test-{label}-{nanos}"));
        fs::create_dir_all(&path).unwrap();
        path
    }
}
