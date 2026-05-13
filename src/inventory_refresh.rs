use std::error::Error;
use std::fmt;
use std::num::NonZeroUsize;
use std::path::PathBuf;

use tokio::task;

use crate::domain::ClientHost;
use crate::errors::DatabaseError;
use crate::inventory::{
    InventoryScanFailure, InventoryScanOptions, InventoryScanner, ScannedLocalItem,
};
use crate::persistence::repository::{
    LocalInventoryReplaceSummary, LocalInventoryScope, LocalItemFileBatch, Repository,
};
use crate::runtime::queue::{BoundedWorkQueue, QueueKind, WorkReceiver, bounded_work_queue};

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

#[derive(Debug)]
pub enum InventoryRefreshError {
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
        let scanner = InventoryScanner::new(self.scan_options);
        let scan_report =
            task::spawn_blocking(move || scanner.scan_media_dirs(&request.media_dirs))
                .await
                .map_err(|error| InventoryRefreshError::ScanWorkerFailed {
                    message: error.to_string(),
                })?;
        let batches = scan_report
            .items
            .iter()
            .map(|scanned| LocalItemFileBatch {
                item: &scanned.item,
                files: &scanned.files,
            })
            .collect::<Vec<_>>();
        let LocalInventoryReplaceSummary { upserted, pruned } = self
            .repository
            .replace_local_inventory(LocalInventoryScope::DataRoot, &batches)
            .await?;

        Ok(InventoryRefreshSummary {
            scanned_items: scan_report.items.len(),
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
        let batches = items
            .iter()
            .map(|scanned| LocalItemFileBatch {
                item: &scanned.item,
                files: &scanned.files,
            })
            .collect::<Vec<_>>();
        let LocalInventoryReplaceSummary { upserted, pruned } = self
            .repository
            .replace_local_inventory(LocalInventoryScope::Client { client_host }, &batches)
            .await?;

        Ok(InventoryRefreshSummary {
            scanned_items: items.len(),
            persisted_items: upserted,
            pruned_items: pruned,
            scan_failures: Vec::new(),
        })
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
        if let Err(error) = worker.refresh_data_dirs(request).await {
            drop(error);
        }
        receiver.mark_completed();
    }
}

impl From<DatabaseError> for InventoryRefreshError {
    fn from(source: DatabaseError) -> Self {
        Self::Database { source }
    }
}

impl fmt::Display for InventoryRefreshError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
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
    use crate::domain::{
        ByteSize, ClientHost, DisplayName, FileIndex, InfoHash, ItemTitle, LocalFile, LocalItem,
        LocalItemSource, MediaType, SourceKey,
    };

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
            "SELECT COUNT(*) FROM local_items WHERE source_type = 'client' AND source_key LIKE 'qbit-a.local:%'",
        )
        .fetch_one(repository.pool())
        .await
        .unwrap();

        assert_eq!(1, summary.pruned_items);
        assert_eq!(2, client_count);
        assert_eq!(1, qbit_a_count);
    }

    fn write_file(path: &std::path::Path, bytes: usize) {
        fs::write(path, vec![b'x'; bytes]).unwrap();
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
