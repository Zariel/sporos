use std::error::Error;
use std::fmt;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::time::Duration;

use tokio::task;
use tracing::{info_span, warn};

use crate::domain::{
    ClientHost, DependencyName, DependencyState, DisplayName, InfoHash, ItemTitle, LocalFile,
    LocalItem, LocalItemSource, MediaType, ReasonText, SourceKey, TorrentFile, checked_file_total,
};
use crate::errors::DatabaseError;
use crate::inventory::{
    InventoryScanFailure, InventoryScanOptions, InventoryScanner, ScannedLocalItem,
};
use crate::persistence::repository::{
    LocalInventoryReplaceSummary, LocalInventoryScope, LocalItemFileBatch, Repository,
};
use crate::runtime::announce_worker::unix_time_ms;
use crate::runtime::health::DependencyKind;
use crate::runtime::queue::{BoundedWorkQueue, QueueKind, WorkReceiver, bounded_work_queue};

const INVENTORY_REFRESH_DEPENDENCY: &str = "inventory-refresh";
const INVENTORY_REFRESH_RETRY_INITIAL: Duration = Duration::from_millis(25);
const INVENTORY_REFRESH_RETRY_MAX: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct InventoryRefreshRequest {
    pub media_dirs: Vec<PathBuf>,
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

impl ClientInventoryItem {
    pub fn into_scanned(self) -> Result<ScannedLocalItem, InventoryRefreshError> {
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
                media_type: self.media_type,
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
    Database { source: DatabaseError },
}

#[derive(Debug, Clone)]
pub struct InventoryRefreshWorker {
    repository: Repository,
    scan_options: InventoryScanOptions,
}

impl InventoryRefreshWorker {
    pub const fn new(repository: Repository, scan_options: InventoryScanOptions) -> Self {
        Self {
            repository,
            scan_options,
        }
    }

    pub async fn refresh_data_dirs(
        &self,
        request: InventoryRefreshRequest,
    ) -> Result<InventoryRefreshSummary, InventoryRefreshError> {
        let _span = info_span!(
            "inventory.refresh_data_dirs",
            media_dir_count = request.media_dirs.len()
        );
        let scanner = InventoryScanner::new(self.scan_options);
        let scan_report =
            task::spawn_blocking(move || scanner.scan_media_dirs(&request.media_dirs))
                .await
                .map_err(|error| InventoryRefreshError::ScanWorkerFailed {
                    message: error.to_string(),
                })?;
        let scanned_items = scan_report.items.len();
        let LocalInventoryReplaceSummary { upserted, pruned } = self
            .repository
            .replace_local_inventory_stream(
                LocalInventoryScope::DataRoot,
                scan_report.items.iter().map(local_item_file_batch),
            )
            .await?;
        self.repository
            .wake_announce_inventory_refresh(unix_time_ms(), 1_000)
            .await?;

        Ok(InventoryRefreshSummary {
            scanned_items,
            persisted_items: upserted,
            pruned_items: pruned,
            scan_failures: scan_report.failures,
        })
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
        self.repository
            .wake_announce_client_source_completion(&client_host, unix_time_ms(), 1_000)
            .await?;

        Ok(InventoryRefreshSummary {
            scanned_items: items.len(),
            persisted_items: upserted,
            pruned_items: pruned,
            scan_failures: Vec::new(),
        })
    }
}

fn local_item_file_batch(scanned: &ScannedLocalItem) -> LocalItemFileBatch<'_> {
    LocalItemFileBatch {
        item: &scanned.item,
        files: &scanned.files,
    }
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
) {
    while let Some(request) = receiver.recv().await {
        run_inventory_refresh_with_retry(&worker, request).await;
        receiver.mark_completed();
    }
}

async fn run_inventory_refresh_with_retry(
    worker: &InventoryRefreshWorker,
    request: InventoryRefreshRequest,
) {
    let mut delay = INVENTORY_REFRESH_RETRY_INITIAL;
    loop {
        match worker.refresh_data_dirs(request.clone()).await {
            Ok(summary) if summary.scan_failures.is_empty() => {
                record_inventory_refresh_health(worker, None, None).await;
                return;
            }
            Ok(summary) => {
                let reason = scan_failure_reason(&summary.scan_failures);
                warn!(reason, "inventory refresh reported scan failures");
                record_inventory_refresh_health(worker, Some(reason), None).await;
                return;
            }
            Err(error) => {
                let reason = error.to_string();
                warn!(error = %reason, "inventory refresh failed; retrying");
                record_inventory_refresh_health(worker, Some(reason), Some(delay)).await;
            }
        }

        tokio::time::sleep(delay).await;
        delay = delay.saturating_mul(2).min(INVENTORY_REFRESH_RETRY_MAX);
    }
}

async fn record_inventory_refresh_health(
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
    let _ = worker
        .repository
        .record_dependency_health(
            DependencyKind::LocalState.as_str(),
            &name,
            &state,
            checked_at_ms,
        )
        .await;
}

fn scan_failure_reason(failures: &[InventoryScanFailure]) -> String {
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

impl fmt::Display for InventoryRefreshError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidClientInventory { message } => {
                write!(formatter, "invalid client inventory: {message}")
            }
            Self::ScanWorkerFailed { message } => {
                write!(formatter, "inventory scan worker failed: {message}")
            }
            Self::Database { source } => write!(formatter, "{source}"),
        }
    }
}

impl Error for InventoryRefreshError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidClientInventory { .. } => None,
            Self::ScanWorkerFailed { .. } => None,
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
            .refresh_data_dirs(InventoryRefreshRequest {
                media_dirs: vec![root.clone()],
            })
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
        let request = InventoryRefreshRequest {
            media_dirs: vec![root.clone()],
        };

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
            .refresh_data_dirs(InventoryRefreshRequest {
                media_dirs: vec![root.clone()],
            })
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
            .try_enqueue(InventoryRefreshRequest {
                media_dirs: vec![root.clone()],
            })
            .unwrap();
        drop(queue);
        run_inventory_refresh_worker(worker, receiver).await;

        let local_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM local_items")
            .fetch_one(repository.pool())
            .await
            .unwrap();

        assert_eq!(1, local_count);

        fs::remove_dir_all(root).unwrap();
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
            .try_enqueue(InventoryRefreshRequest {
                media_dirs: vec![missing],
            })
            .unwrap();
        queue
            .try_enqueue(InventoryRefreshRequest {
                media_dirs: vec![root.clone()],
            })
            .unwrap();
        drop(queue);
        run_inventory_refresh_worker(worker, receiver).await;

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
            .try_enqueue(InventoryRefreshRequest {
                media_dirs: vec![root.clone()],
            })
            .unwrap();
        let handle = tokio::spawn(run_inventory_refresh_worker(worker, receiver));

        tokio::time::sleep(Duration::from_millis(75)).await;

        assert_eq!(0, queue.stats().completed);
        handle.abort();
        let _ = handle.await;

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
            Some(("client", "qbit.local")),
        )
        .await;
        insert_waiting_announce(
            &repository,
            "ann_other",
            "guid-other",
            AnnounceReason::ClientChecking,
            Some(("client", "other.local")),
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
            .refresh_client_items(host_base.clone(), &[base_item.clone()])
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
                    index as u64 + 1,
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
            AnnounceReason::SourceIncomplete => "source_incomplete",
            AnnounceReason::ClientChecking => "client_checking",
            _ => unreachable!("unsupported inventory refresh wake test reason"),
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
