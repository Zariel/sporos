use std::io::Cursor;
use std::sync::Arc;

use serde::Deserialize;
use thiserror::Error;
use tokio::sync::mpsc;

use crate::inventory::{InventoryChange, InventoryParseError, parse_inventory, parse_main_data};
use crate::qbit_projection::{CompletionTransition, ProjectionError};
use crate::qbittorrent::{QbittorrentClient, QbittorrentError, SupportedVersions};
use crate::storage::Storage;

const PARSER_CHANNEL_CAPACITY: usize = 32;
const MAX_FILES_PER_TORRENT: usize = 100_000;

#[derive(Debug, Default, PartialEq, Eq)]
pub struct SyncReport {
    pub changed: usize,
    pub completions: Vec<CompletionTransition>,
    pub manifests_needed: Vec<[u8; 16]>,
    pub full_update: bool,
}

#[derive(Clone)]
pub struct InventorySynchronizer {
    storage: Arc<Storage>,
    client: QbittorrentClient,
    inventory_batch_size: usize,
    database_batch_size: usize,
}

impl InventorySynchronizer {
    pub fn new(
        storage: Arc<Storage>,
        client: QbittorrentClient,
        inventory_batch_size: usize,
        database_batch_size: usize,
    ) -> Self {
        Self {
            storage,
            client,
            inventory_batch_size,
            database_batch_size,
        }
    }

    pub async fn negotiate(&self) -> Result<SupportedVersions, SyncError> {
        let versions = self.client.validate_contract().await?;
        self.storage
            .record_qbit_versions(
                &versions.application.to_string(),
                &versions.web_api.to_string(),
            )
            .await?;
        Ok(versions)
    }

    pub async fn sync_once(&self, now: i64) -> Result<SyncReport, SyncError> {
        let state = self.storage.qbit_inventory_state().await?;
        let requested_id = state.response_id.unwrap_or(0);
        let body = self.client.sync_main_data(requested_id).await?;
        apply_main_data(
            &self.storage,
            body,
            self.database_batch_size,
            state.has_baseline,
            state.generation,
            now,
        )
        .await
    }

    pub async fn reconcile(&self, now: i64) -> Result<SyncReport, SyncError> {
        let state = self.storage.qbit_inventory_state().await?;
        let generation = state
            .generation
            .checked_add(1)
            .ok_or(SyncError::GenerationOverflow)?;
        let mut offset = 0_usize;
        let mut report = SyncReport {
            full_update: true,
            ..SyncReport::default()
        };
        loop {
            let body = self
                .client
                .inventory_page(offset, self.inventory_batch_size)
                .await?;
            let mut page = Vec::with_capacity(self.inventory_batch_size);
            let count = parse_inventory(Cursor::new(body), u64::MAX, |torrent| {
                page.push(torrent.into());
                Ok(())
            })?;
            for batch in page.chunks(self.database_batch_size) {
                let projected = self
                    .storage
                    .project_qbit_batch(batch, generation, state.has_baseline, now)
                    .await?;
                report.changed += projected.changed;
                report.completions.extend(projected.completions);
                report.manifests_needed.extend(projected.manifests_needed);
            }
            if count < self.inventory_batch_size {
                break;
            }
            offset = offset.checked_add(count).ok_or(SyncError::OffsetOverflow)?;
        }
        self.storage.finish_qbit_reconcile(generation, now).await?;
        Ok(report)
    }

    pub async fn refresh_manifests(&self, limit: usize, now: i64) -> Result<usize, SyncError> {
        let sources = self.storage.stale_qbit_manifests(limit).await?;
        for source_id in &sources {
            self.load_manifest(*source_id, now).await?;
        }
        Ok(sources.len())
    }

    pub async fn load_manifest(&self, source_id: [u8; 16], now: i64) -> Result<usize, SyncError> {
        let result = self.load_manifest_inner(source_id, now).await;
        if result.is_err() {
            self.storage.fail_qbit_manifest(source_id).await?;
        }
        result
    }

    async fn load_manifest_inner(&self, source_id: [u8; 16], now: i64) -> Result<usize, SyncError> {
        let target = self.storage.prepare_qbit_manifest(source_id).await?;
        let body = self.client.torrent_files(&target.qbit_id).await?;
        let (sender, mut receiver) = mpsc::channel(PARSER_CHANNEL_CAPACITY);
        let parser = tokio::task::spawn_blocking(move || {
            crate::inventory::parse_files(
                Cursor::new(body),
                u64::MAX,
                MAX_FILES_PER_TORRENT,
                |file| {
                    sender
                        .blocking_send(file)
                        .map_err(|_| "manifest projection stopped".to_owned())
                },
            )
        });
        let mut batch = Vec::with_capacity(self.database_batch_size);
        let mut count = 0_usize;
        while let Some(file) = receiver.recv().await {
            if file.index != count {
                return Err(SyncError::NonContiguousFileIndex {
                    expected: count,
                    actual: file.index,
                });
            }
            count += 1;
            batch.push(file);
            if batch.len() == self.database_batch_size {
                self.storage.project_qbit_files(&target, &batch).await?;
                batch.clear();
            }
        }
        if !batch.is_empty() {
            self.storage.project_qbit_files(&target, &batch).await?;
        }
        let parsed = parser.await.map_err(SyncError::ParserTask)??;
        if parsed != count {
            return Err(SyncError::FileCountChanged);
        }
        self.storage
            .finish_qbit_manifest(&target, count, now)
            .await?;
        Ok(count)
    }
}

async fn apply_main_data(
    storage: &Storage,
    body: Vec<u8>,
    database_batch_size: usize,
    has_baseline: bool,
    current_generation: u64,
    now: i64,
) -> Result<SyncReport, SyncError> {
    if database_batch_size == 0 {
        return Err(SyncError::ZeroBatchSize);
    }
    let header: MainDataHeader = serde_json::from_slice(&body).map_err(SyncError::Header)?;
    if !has_baseline && !header.full_update {
        return Err(SyncError::InitialUpdateNotFull);
    }
    let generation = if header.full_update {
        current_generation
            .checked_add(1)
            .ok_or(SyncError::GenerationOverflow)?
    } else {
        current_generation
    };
    let (sender, mut receiver) = mpsc::channel(PARSER_CHANNEL_CAPACITY);
    let parser = tokio::task::spawn_blocking(move || {
        parse_main_data(Cursor::new(body), u64::MAX, |change| {
            sender
                .blocking_send(change)
                .map_err(|_| "inventory projection stopped".to_owned())
        })
    });

    let mut report = SyncReport {
        full_update: header.full_update,
        ..SyncReport::default()
    };
    let mut batch = Vec::with_capacity(database_batch_size);
    while let Some(change) = receiver.recv().await {
        batch.push(change);
        if batch.len() == database_batch_size {
            project_batch(storage, &mut report, &batch, generation, has_baseline, now).await?;
            batch.clear();
        }
    }
    if !batch.is_empty() {
        project_batch(storage, &mut report, &batch, generation, has_baseline, now).await?;
    }
    let parsed = parser.await.map_err(SyncError::ParserTask)??;
    if parsed.response_id != header.response_id || parsed.full_update != header.full_update {
        return Err(SyncError::MetadataChanged);
    }
    storage
        .finish_qbit_sync(
            parsed.response_id,
            parsed.full_update.then_some(generation),
            now,
        )
        .await?;
    Ok(report)
}

async fn project_batch(
    storage: &Storage,
    report: &mut SyncReport,
    batch: &[InventoryChange],
    generation: u64,
    detect_completions: bool,
    now: i64,
) -> Result<(), SyncError> {
    let projected = storage
        .project_qbit_batch(batch, generation, detect_completions, now)
        .await?;
    report.changed += projected.changed;
    report.completions.extend(projected.completions);
    report.manifests_needed.extend(projected.manifests_needed);
    Ok(())
}

#[derive(Debug, Deserialize)]
struct MainDataHeader {
    #[serde(rename = "rid")]
    response_id: u64,
    #[serde(default)]
    full_update: bool,
}

#[derive(Debug, Error)]
pub enum SyncError {
    #[error("qBittorrent inventory request failed")]
    Qbittorrent(#[from] QbittorrentError),
    #[error("qBittorrent inventory projection failed")]
    Projection(#[from] ProjectionError),
    #[error("qBittorrent inventory response is invalid")]
    Parse(#[from] InventoryParseError),
    #[error("qBittorrent main-data header is invalid")]
    Header(#[source] serde_json::Error),
    #[error("qBittorrent did not provide a full initial inventory update")]
    InitialUpdateNotFull,
    #[error("qBittorrent changed main-data metadata while it was decoded")]
    MetadataChanged,
    #[error("qBittorrent inventory generation overflowed")]
    GenerationOverflow,
    #[error("qBittorrent inventory offset overflowed")]
    OffsetOverflow,
    #[error("qBittorrent database batch size must be greater than zero")]
    ZeroBatchSize,
    #[error("qBittorrent parser task failed")]
    ParserTask(#[source] tokio::task::JoinError),
    #[error("qBittorrent file indexes are not contiguous: expected {expected}, got {actual}")]
    NonContiguousFileIndex { expected: usize, actual: usize },
    #[error("qBittorrent file count changed while it was decoded")]
    FileCountChanged,
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[tokio::test]
    async fn bootstraps_in_small_batches_without_completion_events() {
        let directory = TempDir::new().expect("temporary directory");
        let storage = open(&directory).await;
        let body = full_update(1, 5, true);

        let report = apply_main_data(&storage, body, 2, false, 0, 10)
            .await
            .expect("apply initial inventory");

        assert_eq!(report.changed, 5);
        assert!(report.completions.is_empty());
        let state = storage.qbit_inventory_state().await.unwrap();
        assert_eq!(state.response_id, Some(1));
        assert_eq!(state.generation, 1);
        assert!(state.has_baseline);
    }

    #[tokio::test]
    async fn detects_downtime_completion_after_the_baseline() {
        let directory = TempDir::new().expect("temporary directory");
        let storage = open(&directory).await;
        apply_main_data(&storage, full_update(1, 1, false), 2, false, 0, 10)
            .await
            .unwrap();

        let report = apply_main_data(&storage, full_update(2, 1, true), 2, true, 1, 20)
            .await
            .expect("apply restart inventory");

        assert_eq!(report.completions.len(), 1);
        assert_eq!(
            storage.qbit_inventory_state().await.unwrap().response_id,
            Some(2)
        );
    }

    #[tokio::test]
    async fn rejects_a_partial_first_response() {
        let directory = TempDir::new().expect("temporary directory");
        let storage = open(&directory).await;
        let error = apply_main_data(
            &storage,
            br#"{"rid":1,"torrents":{}}"#.to_vec(),
            2,
            false,
            0,
            10,
        )
        .await
        .expect_err("require a full initial response");
        assert!(matches!(error, SyncError::InitialUpdateNotFull));
    }

    fn full_update(response_id: u64, count: usize, complete: bool) -> Vec<u8> {
        let mut torrents = serde_json::Map::new();
        for number in 0..count {
            let id = format!("{number:040x}");
            torrents.insert(
                id.clone(),
                serde_json::json!({
                    "infohash_v1": id,
                    "infohash_v2": "",
                    "name": format!("release-{number}"),
                    "total_size": 4,
                    "amount_left": if complete { 0 } else { 4 },
                    "progress": if complete { 1.0 } else { 0.0 },
                    "state": if complete { "stoppedUP" } else { "stoppedDL" },
                    "save_path": "/data",
                    "content_path": format!("/data/release-{number}"),
                    "category": "",
                    "tags": "",
                    "added_on": 1,
                    "completion_on": if complete { 2 } else { 0 }
                }),
            );
        }
        serde_json::to_vec(&serde_json::json!({
            "rid": response_id,
            "full_update": true,
            "torrents": torrents
        }))
        .unwrap()
    }

    async fn open(directory: &TempDir) -> Storage {
        Storage::open(
            directory.path().join("sporos.lock"),
            directory.path().join("sporos.db"),
        )
        .await
        .expect("open storage")
    }
}
