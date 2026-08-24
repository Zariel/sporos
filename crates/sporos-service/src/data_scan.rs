use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use duroxide::runtime::registry::ActivityRegistryBuilder;
use duroxide::{ActivityContext, OrchestrationContext};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sporos_matcher::parse_release;
use sporos_model::{PolicySnapshotId, TaskId, TaskKey};
use sqlx::Row;
use thiserror::Error;

use crate::config::DataRoot;
use crate::durable_ingress::{NewTask, PolicySnapshot, accept_task_in};
use crate::search::{SearchError, SearchPolicy, accept_in};
use crate::storage::Storage;

pub(crate) const ORCHESTRATION_NAME: &str = "ScanDataRoot";
pub(crate) const ORCHESTRATION_VERSION: &str = "1.0.0";
const ACTIVITY: &str = "ScanDataRootPage";
const PAGE_SIZE: usize = 100;
const MAX_FILE_DEPTH: usize = 16;

#[derive(Clone)]
pub(crate) struct DataScanExecutor {
    storage: Arc<Storage>,
    roots: Arc<BTreeMap<String, DataRoot>>,
    filesystem: Option<Arc<tokio::sync::Semaphore>>,
}

impl DataScanExecutor {
    pub(crate) fn new(storage: Arc<Storage>, roots: BTreeMap<String, DataRoot>) -> Self {
        Self {
            storage,
            roots: Arc::new(roots),
            filesystem: None,
        }
    }

    pub(crate) fn with_limiter(mut self, limiter: Arc<tokio::sync::Semaphore>) -> Self {
        self.filesystem = Some(limiter);
        self
    }

    pub(crate) fn register(self, activities: ActivityRegistryBuilder) -> ActivityRegistryBuilder {
        activities.register(ACTIVITY, move |_context: ActivityContext, input: String| {
            let executor = self.clone();
            async move {
                let input: ScanInput = serde_json::from_str(&input)
                    .map_err(|error| format!("invalid data scan input: {error}"))?;
                let step = executor
                    .scan_page(&input, now_ms())
                    .await
                    .map_err(|error| format!("scan data root: {error}"))?;
                serde_json::to_string(&step)
                    .map_err(|error| format!("encode data scan step: {error}"))
            }
        })
    }

    async fn scan_page(&self, input: &ScanInput, now: i64) -> Result<ScanStep, DataScanError> {
        let Some(root) = self.roots.get(&input.root_name).cloned() else {
            return self.finish(input, now, "root_not_configured", true).await;
        };
        match safe_root(&root.path) {
            Ok(()) => {}
            Err(ScanFsError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                sqlx::query(
                    "UPDATE sporos_data_source SET available = 0, updated_at = ?
                     WHERE root_name = ?",
                )
                .bind(now)
                .bind(&input.root_name)
                .execute(self.storage.pool())
                .await?;
                return self.finish(input, now, "root_unavailable", false).await;
            }
            Err(_) => return self.finish(input, now, "unsafe_root", true).await,
        };
        let directory = sqlx::query(
            "SELECT relative_path, depth, cursor_name
             FROM sporos_data_scan_directory
             WHERE operation_id = ? AND state IN ('queued', 'running')
             ORDER BY relative_path LIMIT 1",
        )
        .bind(input.operation_id.as_slice())
        .fetch_optional(self.storage.pool())
        .await?;
        let Some(directory) = directory else {
            sqlx::query(
                "UPDATE sporos_data_source SET available = 0, updated_at = ?
                 WHERE root_name = ? AND last_seen_generation != ?",
            )
            .bind(now)
            .bind(&input.root_name)
            .bind(input.generation)
            .execute(self.storage.pool())
            .await?;
            return self.finish(input, now, "completed", false).await;
        };
        let relative: Vec<u8> = directory.try_get("relative_path")?;
        let depth = usize::try_from(directory.try_get::<i64, _>("depth")?)
            .map_err(|_| DataScanError::StoredRange)?;
        let cursor: Option<Vec<u8>> = directory.try_get("cursor_name")?;
        let policy_json = sqlx::query_scalar::<_, String>(
            "SELECT payload_json FROM sporos_policy_snapshot WHERE id = ?",
        )
        .bind(input.policy_snapshot_id.as_slice())
        .fetch_one(self.storage.pool())
        .await?;
        let policy: SearchPolicy = serde_json::from_str(&policy_json)?;
        let primary_extensions = policy.matching.policy.primary_video_extensions.clone();
        let root_path = root.path.clone();
        let relative_for_scan = relative.clone();
        let root_for_scan = root.clone();
        let _permit = match &self.filesystem {
            Some(limiter) => Some(crate::execution::permit(limiter).await),
            None => None,
        };
        let page = tokio::task::spawn_blocking(move || {
            scan_directory(
                &root_path,
                &relative_for_scan,
                depth,
                cursor.as_deref(),
                &root_for_scan,
                &primary_extensions,
            )
        })
        .await
        .map_err(DataScanError::Join)?;
        let page = match page {
            Ok(page) => page,
            Err(error) => {
                return self.finish(input, now, error.reason_code(), true).await;
            }
        };
        let indexers = eligible_indexers(&self.storage, &input.indexer_ids).await?;
        let trigger = format!("data_scan:{}", hex(&input.operation_id));
        let mut transaction = self.storage.pool().begin().await?;
        let mut observed = sqlx::query_scalar::<_, i64>(
            "SELECT observed_releases FROM sporos_data_scan_state WHERE operation_id = ?",
        )
        .bind(input.operation_id.as_slice())
        .fetch_one(&mut *transaction)
        .await?;
        let mut produced = 0_i64;
        for release in &page.releases {
            observed = observed.saturating_add(1);
            if observed > i64::try_from(root.max_releases).unwrap_or(i64::MAX) {
                transaction.rollback().await?;
                return self.finish(input, now, "release_limit", true).await;
            }
            let source_id = persist_release(
                &mut transaction,
                &input.root_name,
                input.generation,
                release,
                now,
            )
            .await?;
            for indexer_id in &indexers {
                if accept_in(
                    &mut transaction,
                    source_id,
                    *indexer_id,
                    &policy,
                    &trigger,
                    Some(input.operation_id),
                    now,
                )
                .await?
                {
                    produced += 1;
                }
            }
        }
        for child in &page.children {
            sqlx::query(
                "INSERT INTO sporos_data_scan_directory
                 (operation_id, relative_path, depth, state)
                 VALUES (?, ?, ?, 'queued') ON CONFLICT DO NOTHING",
            )
            .bind(input.operation_id.as_slice())
            .bind(child)
            .bind(i64::try_from(depth.saturating_add(1)).unwrap_or(i64::MAX))
            .execute(&mut *transaction)
            .await?;
        }
        if page.has_more {
            sqlx::query(
                "UPDATE sporos_data_scan_directory SET state = 'running', cursor_name = ?
                 WHERE operation_id = ? AND relative_path = ?",
            )
            .bind(page.last_name)
            .bind(input.operation_id.as_slice())
            .bind(&relative)
            .execute(&mut *transaction)
            .await?;
        } else {
            sqlx::query(
                "UPDATE sporos_data_scan_directory SET state = 'completed', cursor_name = NULL
                 WHERE operation_id = ? AND relative_path = ?",
            )
            .bind(input.operation_id.as_slice())
            .bind(&relative)
            .execute(&mut *transaction)
            .await?;
        }
        sqlx::query(
            "UPDATE sporos_data_scan_state SET observed_releases = ?, state = 'running',
             updated_at = ? WHERE operation_id = ?",
        )
        .bind(observed)
        .bind(now)
        .bind(input.operation_id.as_slice())
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE sporos_operation SET state = 'running',
             produced_tasks = produced_tasks + ?, updated_at = ? WHERE id = ?",
        )
        .bind(produced)
        .bind(now)
        .bind(input.operation_id.as_slice())
        .execute(&mut *transaction)
        .await?;
        sqlx::query("UPDATE sporos_task SET state = 'running', updated_at = ? WHERE id = ?")
            .bind(now)
            .bind(input.task_id.as_slice())
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        let remaining = sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM sporos_data_scan_directory
             WHERE operation_id = ? AND state IN ('queued', 'running')",
        )
        .bind(input.operation_id.as_slice())
        .fetch_one(self.storage.pool())
        .await?;
        if remaining == 0 {
            sqlx::query(
                "UPDATE sporos_data_source SET available = 0, updated_at = ?
                 WHERE root_name = ? AND last_seen_generation != ?",
            )
            .bind(now)
            .bind(&input.root_name)
            .bind(input.generation)
            .execute(self.storage.pool())
            .await?;
            self.finish(input, now, "completed", false).await
        } else {
            Ok(ScanStep {
                done: false,
                reason: None,
            })
        }
    }

    async fn finish(
        &self,
        input: &ScanInput,
        now: i64,
        reason: &str,
        failed: bool,
    ) -> Result<ScanStep, DataScanError> {
        let state = if failed { "failed" } else { "completed" };
        let mut transaction = self.storage.pool().begin().await?;
        sqlx::query(
            "UPDATE sporos_data_scan_state SET state = ?, reason_code = ?, updated_at = ?
             WHERE operation_id = ?",
        )
        .bind(state)
        .bind(reason)
        .bind(now)
        .bind(input.operation_id.as_slice())
        .execute(&mut *transaction)
        .await?;
        sqlx::query("UPDATE sporos_operation SET state = ?, updated_at = ? WHERE id = ?")
            .bind(state)
            .bind(now)
            .bind(input.operation_id.as_slice())
            .execute(&mut *transaction)
            .await?;
        sqlx::query(
            "UPDATE sporos_task SET state = ?, reason_code = ?, updated_at = ?, terminal_at = ?
             WHERE id = ?",
        )
        .bind(state)
        .bind(reason)
        .bind(now)
        .bind(now)
        .bind(input.task_id.as_slice())
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(ScanStep {
            done: true,
            reason: Some(reason.to_owned()),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScanInput {
    operation_id: [u8; 16],
    task_id: [u8; 16],
    root_name: String,
    generation: i64,
    policy_snapshot_id: [u8; 16],
    indexer_ids: Vec<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScanStep {
    done: bool,
    reason: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct AcceptedScan {
    pub operation_id: [u8; 16],
    pub task_id: [u8; 16],
    pub duplicate: bool,
}

pub(crate) async fn accept(
    storage: &Storage,
    root_name: &str,
    policy: SearchPolicy,
    indexer_ids: Vec<i64>,
    force_nonce: Option<&str>,
    now: i64,
) -> Result<AcceptedScan, DataScanError> {
    let policy_json = serde_json::to_string(&policy)?;
    let policy_hash: [u8; 32] = Sha256::digest(policy_json.as_bytes()).into();
    let policy_id = first16(&policy_hash);
    let request_json = serde_json::json!({
        "root": root_name,
        "indexerIds": &indexer_ids,
        "force": force_nonce.is_some(),
    })
    .to_string();
    let mut hash = Sha256::new();
    hash.update(b"data-scan-operation-v1");
    hash.update(request_json.as_bytes());
    hash.update(policy_id);
    if let Some(nonce) = force_nonce {
        hash.update(nonce.as_bytes());
    }
    let operation_digest: [u8; 32] = hash.finalize().into();
    let operation_id = first16(&operation_digest);
    let task_digest: [u8; 32] =
        Sha256::digest([b"data-scan-task-v1".as_slice(), operation_id.as_slice()].concat()).into();
    let task_id = first16(&task_digest);
    let input = ScanInput {
        operation_id,
        task_id,
        root_name: root_name.to_owned(),
        generation: generation(&operation_id),
        policy_snapshot_id: policy_id,
        indexer_ids,
    };
    let instance_id = format!("data-scan-v1:{}:{root_name}", hex(&operation_id));
    let task = NewTask {
        id: TaskId::from_bytes(task_id),
        key: TaskKey::from_bytes(task_digest),
        kind: "data_scan".to_owned(),
        policy: PolicySnapshot {
            id: PolicySnapshotId::from_bytes(policy_id),
            config_hash: policy_hash,
            matcher_version: "sporos-matcher/1".to_owned(),
            payload_json: policy_json,
            created_at: now,
        },
        orchestration_name: ORCHESTRATION_NAME.to_owned(),
        orchestration_version: ORCHESTRATION_VERSION.to_owned(),
        instance_id: instance_id.clone(),
        input_json: serde_json::to_string(&input)?,
        created_at: now,
    };
    let mut transaction = storage.pool().begin().await?;
    let inserted = accept_task_in(&mut transaction, &task).await?;
    sqlx::query(
        "INSERT INTO sporos_operation (id, kind, state, duroxide_instance_id,
         request_json, produced_tasks, completed_tasks, failed_tasks, created_at, updated_at)
         VALUES (?, 'data_scan', 'queued', ?, ?, 0, 0, 0, ?, ?)
         ON CONFLICT(id) DO NOTHING",
    )
    .bind(operation_id.as_slice())
    .bind(instance_id)
    .bind(request_json)
    .bind(now)
    .bind(now)
    .execute(&mut *transaction)
    .await?;
    sqlx::query("UPDATE sporos_task SET operation_id = ? WHERE id = ?")
        .bind(operation_id.as_slice())
        .bind(task_id.as_slice())
        .execute(&mut *transaction)
        .await?;
    sqlx::query(
        "INSERT INTO sporos_data_scan_state
         (operation_id, root_name, generation, state, updated_at)
         VALUES (?, ?, ?, 'queued', ?) ON CONFLICT(operation_id) DO NOTHING",
    )
    .bind(operation_id.as_slice())
    .bind(root_name)
    .bind(input.generation)
    .bind(now)
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        "INSERT INTO sporos_data_scan_directory
         (operation_id, relative_path, depth, state) VALUES (?, X'', 0, 'queued')
         ON CONFLICT DO NOTHING",
    )
    .bind(operation_id.as_slice())
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(AcceptedScan {
        operation_id,
        task_id,
        duplicate: !inserted,
    })
}

pub(crate) async fn workflow(
    context: OrchestrationContext,
    input: String,
) -> Result<String, String> {
    let _: ScanInput = serde_json::from_str(&input)
        .map_err(|error| format!("invalid data scan input: {error}"))?;
    let output = context
        .schedule_activity_with_retry(
            ACTIVITY,
            input.clone(),
            crate::engine::activity_retry_policy(),
        )
        .await?;
    let step: ScanStep = serde_json::from_str(&output)
        .map_err(|error| format!("invalid data scan step: {error}"))?;
    if step.done {
        Ok(output)
    } else {
        context.continue_as_new(input).await
    }
}

struct DirectoryPage {
    releases: Vec<ScannedRelease>,
    children: Vec<Vec<u8>>,
    last_name: Option<Vec<u8>>,
    has_more: bool,
}

struct ScannedRelease {
    relative_path: Vec<u8>,
    kind: &'static str,
    name: String,
    total_size: u64,
    device: u64,
    inode: u64,
    modified_at: i64,
    files: Vec<ScannedFile>,
}

struct ScannedFile {
    relative_path: Vec<u8>,
    display_path: String,
    local_path: String,
    size: u64,
    kind: &'static str,
    device: u64,
    inode: u64,
    modified_at: i64,
}

fn scan_directory(
    root: &Path,
    relative: &[u8],
    depth: usize,
    cursor: Option<&[u8]>,
    settings: &DataRoot,
    primary_extensions: &[String],
) -> Result<DirectoryPage, ScanFsError> {
    let directory = join_bytes(root, relative)?;
    let metadata = std::fs::symlink_metadata(&directory)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(ScanFsError::UnsafePath);
    }
    let mut selected = Vec::<(Vec<u8>, PathBuf)>::new();
    let mut has_more = false;
    for entry in std::fs::read_dir(&directory)? {
        let entry = entry?;
        let name = entry.file_name().as_bytes().to_vec();
        if cursor.is_some_and(|cursor| name.as_slice() <= cursor) {
            continue;
        }
        selected.push((name, entry.path()));
        selected.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        if selected.len() > PAGE_SIZE {
            selected.pop();
            has_more = true;
        }
    }
    let last_name = selected.last().map(|entry| entry.0.clone());
    let mut releases = Vec::new();
    let mut children = Vec::new();
    for (name, path) in selected {
        let metadata = std::fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink() {
            continue;
        }
        let child_relative = append_component(relative, &name);
        if metadata.is_file() && primary_video(&path, primary_extensions) {
            if let Some(release) = standalone_release(root, child_relative, path, metadata)? {
                releases.push(release);
            }
        } else if metadata.is_dir() {
            if looks_like_release(&path, primary_extensions)? {
                if let Some(release) = directory_release(
                    root,
                    child_relative,
                    path,
                    metadata,
                    settings.max_files_per_release,
                    primary_extensions,
                )? {
                    releases.push(release);
                }
            } else if depth < settings.max_depth {
                children.push(child_relative);
            }
        }
    }
    Ok(DirectoryPage {
        releases,
        children,
        last_name,
        has_more,
    })
}

fn looks_like_release(path: &Path, primary_extensions: &[String]) -> Result<bool, ScanFsError> {
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let metadata = std::fs::symlink_metadata(entry.path())?;
        if metadata.file_type().is_symlink() {
            continue;
        }
        if metadata.is_file() && primary_video(&entry.path(), primary_extensions) {
            return Ok(true);
        }
        if metadata.is_dir()
            && entry.file_name().to_str().is_some_and(|name| {
                matches!(name.to_ascii_uppercase().as_str(), "BDMV" | "VIDEO_TS")
            })
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn standalone_release(
    root: &Path,
    relative_path: Vec<u8>,
    path: PathBuf,
    metadata: std::fs::Metadata,
) -> Result<Option<ScannedRelease>, ScanFsError> {
    let Some(name) = path.file_name().and_then(OsStr::to_str).map(str::to_owned) else {
        return Ok(None);
    };
    let local_path = path.to_str().ok_or(ScanFsError::NonUtf8)?.to_owned();
    let display_path = name.clone();
    let relative_file = path
        .strip_prefix(root)
        .map_err(|_| ScanFsError::UnsafePath)?
        .as_os_str()
        .as_bytes()
        .to_vec();
    Ok(Some(ScannedRelease {
        relative_path,
        kind: "file",
        name,
        total_size: metadata.len(),
        device: metadata.dev(),
        inode: metadata.ino(),
        modified_at: metadata.mtime(),
        files: vec![ScannedFile {
            relative_path: relative_file,
            display_path,
            local_path,
            size: metadata.len(),
            kind: "video",
            device: metadata.dev(),
            inode: metadata.ino(),
            modified_at: metadata.mtime(),
        }],
    }))
}

fn directory_release(
    root: &Path,
    relative_path: Vec<u8>,
    path: PathBuf,
    metadata: std::fs::Metadata,
    max_files: usize,
    primary_extensions: &[String],
) -> Result<Option<ScannedRelease>, ScanFsError> {
    let Some(name) = path.file_name().and_then(OsStr::to_str).map(str::to_owned) else {
        return Ok(None);
    };
    let mut files = Vec::new();
    let mut pending = vec![(path.clone(), 0_usize)];
    while let Some((directory, depth)) = pending.pop() {
        if depth > MAX_FILE_DEPTH {
            return Err(ScanFsError::FileDepth);
        }
        for entry in std::fs::read_dir(&directory)? {
            let entry = entry?;
            let metadata = std::fs::symlink_metadata(entry.path())?;
            if metadata.file_type().is_symlink() {
                continue;
            }
            if metadata.is_dir() {
                pending.push((entry.path(), depth.saturating_add(1)));
                continue;
            }
            if !metadata.is_file() {
                continue;
            }
            if files.len() == max_files {
                return Err(ScanFsError::FileLimit);
            }
            let local_path = entry.path();
            let display = local_path
                .strip_prefix(&path)
                .map_err(|_| ScanFsError::UnsafePath)?;
            let Some(display_path) = display.to_str().map(str::to_owned) else {
                continue;
            };
            let Some(local_path_text) = local_path.to_str().map(str::to_owned) else {
                continue;
            };
            let relative_file = local_path
                .strip_prefix(root)
                .map_err(|_| ScanFsError::UnsafePath)?
                .as_os_str()
                .as_bytes()
                .to_vec();
            let primary = primary_video(&local_path, primary_extensions) || disc_video(display);
            files.push(ScannedFile {
                relative_path: relative_file,
                display_path,
                local_path: local_path_text,
                size: metadata.len(),
                kind: if primary { "video" } else { "other" },
                device: metadata.dev(),
                inode: metadata.ino(),
                modified_at: metadata.mtime(),
            });
        }
    }
    files.sort_unstable_by(|left, right| left.relative_path.cmp(&right.relative_path));
    if !files.iter().any(|file| file.kind == "video") {
        return Ok(None);
    }
    let total_size = files.iter().try_fold(0_u64, |total, file| {
        total
            .checked_add(file.size)
            .ok_or(ScanFsError::SizeOverflow)
    })?;
    Ok(Some(ScannedRelease {
        relative_path,
        kind: "directory",
        name,
        total_size,
        device: metadata.dev(),
        inode: metadata.ino(),
        modified_at: metadata.mtime(),
        files,
    }))
}

async fn persist_release(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    root_name: &str,
    generation: i64,
    release: &ScannedRelease,
    now: i64,
) -> Result<[u8; 16], DataScanError> {
    let source_id = data_source_id(root_name, &release.relative_path);
    let descriptor = parse_release(&release.name);
    sqlx::query(
        "INSERT INTO sporos_data_source (id, root_name, relative_path, kind, name,
         total_size, release_json, device, inode, modified_at, available,
         last_seen_generation, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
         ON CONFLICT(root_name, relative_path) DO UPDATE SET kind = excluded.kind,
         name = excluded.name, total_size = excluded.total_size,
         release_json = excluded.release_json, device = excluded.device, inode = excluded.inode,
         modified_at = excluded.modified_at,
         available = 1, last_seen_generation = excluded.last_seen_generation,
         updated_at = excluded.updated_at",
    )
    .bind(source_id.as_slice())
    .bind(root_name)
    .bind(&release.relative_path)
    .bind(release.kind)
    .bind(&release.name)
    .bind(to_i64(release.total_size)?)
    .bind(serde_json::to_string(&descriptor)?)
    .bind(to_i64(release.device)?)
    .bind(to_i64(release.inode)?)
    .bind(release.modified_at)
    .bind(generation)
    .bind(now)
    .execute(&mut **transaction)
    .await?;
    sqlx::query("UPDATE sporos_data_source SET normalized_title = ? WHERE id = ?")
        .bind(descriptor.primary_title.as_str())
        .bind(source_id.as_slice())
        .execute(&mut **transaction)
        .await?;
    crate::source_facts::replace(transaction, &source_id, "data", &descriptor).await?;
    sqlx::query("UPDATE sporos_source_file SET available = 0 WHERE source_id = ?")
        .bind(source_id.as_slice())
        .execute(&mut **transaction)
        .await?;
    for (ordinal, file) in release.files.iter().enumerate() {
        let episode = parse_release(&file.display_path);
        let episode_key = episode
            .season
            .zip(episode.episode)
            .map(|(season, episode)| {
                format!(
                    "{}:s{season:04}:e{episode:04}",
                    descriptor.primary_title.as_str()
                )
            });
        sqlx::query(
            "INSERT INTO sporos_source_file (source_id, manifest_version, relative_path,
             display_path, size, file_kind, episode_key, local_path, device, inode,
             modified_at, available, ordinal) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
             ON CONFLICT(source_id, manifest_version, ordinal) DO UPDATE SET
             relative_path = excluded.relative_path, display_path = excluded.display_path,
             size = excluded.size, file_kind = excluded.file_kind,
             episode_key = excluded.episode_key, local_path = excluded.local_path,
             device = excluded.device, inode = excluded.inode,
             modified_at = excluded.modified_at, available = 1",
        )
        .bind(source_id.as_slice())
        .bind(generation)
        .bind(&file.relative_path)
        .bind(&file.display_path)
        .bind(to_i64(file.size)?)
        .bind(file.kind)
        .bind(episode_key)
        .bind(&file.local_path)
        .bind(to_i64(file.device)?)
        .bind(to_i64(file.inode)?)
        .bind(file.modified_at)
        .bind(i64::try_from(ordinal).map_err(|_| DataScanError::Range)?)
        .execute(&mut **transaction)
        .await?;
    }
    Ok(source_id)
}

async fn eligible_indexers(storage: &Storage, selected: &[i64]) -> Result<Vec<i64>, DataScanError> {
    if selected.is_empty() {
        return Ok(sqlx::query_scalar(
            "SELECT prowlarr_id FROM sporos_indexer WHERE eligible = 1
             ORDER BY priority, prowlarr_id",
        )
        .fetch_all(storage.pool())
        .await?);
    }
    let mut result = Vec::new();
    for id in selected {
        if sqlx::query_scalar::<_, i64>(
            "SELECT 1 FROM sporos_indexer WHERE prowlarr_id = ? AND eligible = 1",
        )
        .bind(id)
        .fetch_optional(storage.pool())
        .await?
        .is_some()
        {
            result.push(*id);
        }
    }
    Ok(result)
}

fn primary_video(path: &Path, extensions: &[String]) -> bool {
    path.extension()
        .and_then(OsStr::to_str)
        .is_some_and(|extension| {
            extensions
                .iter()
                .any(|allowed| allowed.eq_ignore_ascii_case(extension))
        })
}

fn disc_video(relative: &Path) -> bool {
    relative.components().any(|component| {
        component
            .as_os_str()
            .to_str()
            .is_some_and(|name| name.eq_ignore_ascii_case("VIDEO_TS"))
    }) && relative
        .extension()
        .and_then(OsStr::to_str)
        .is_some_and(|extension| extension.eq_ignore_ascii_case("vob"))
}

fn join_bytes(root: &Path, relative: &[u8]) -> Result<PathBuf, ScanFsError> {
    if relative.is_empty() {
        Ok(root.to_owned())
    } else {
        let relative = PathBuf::from(OsString::from_vec(relative.to_vec()));
        if relative.is_absolute()
            || relative
                .components()
                .any(|component| !matches!(component, std::path::Component::Normal(_)))
        {
            return Err(ScanFsError::UnsafePath);
        }
        Ok(root.join(relative))
    }
}

fn safe_root(root: &Path) -> Result<(), ScanFsError> {
    for ancestor in root.ancestors() {
        let metadata = std::fs::symlink_metadata(ancestor)?;
        if metadata.file_type().is_symlink() {
            return Err(ScanFsError::UnsafePath);
        }
    }
    if std::fs::symlink_metadata(root)?.is_dir() {
        Ok(())
    } else {
        Err(ScanFsError::UnsafePath)
    }
}

fn append_component(parent: &[u8], name: &[u8]) -> Vec<u8> {
    let mut result =
        Vec::with_capacity(parent.len() + usize::from(!parent.is_empty()) + name.len());
    result.extend_from_slice(parent);
    if !parent.is_empty() {
        result.push(b'/');
    }
    result.extend_from_slice(name);
    result
}

fn data_source_id(root_name: &str, relative: &[u8]) -> [u8; 16] {
    let mut hash = Sha256::new();
    hash.update(b"data-source-v1");
    hash.update(root_name.as_bytes());
    hash.update([0]);
    hash.update(relative);
    first16(&hash.finalize().into())
}

fn first16(value: &[u8; 32]) -> [u8; 16] {
    let mut id = [0_u8; 16];
    id.copy_from_slice(&value[..16]);
    id
}

fn generation(operation_id: &[u8; 16]) -> i64 {
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&operation_id[..8]);
    i64::from_be_bytes(bytes) & i64::MAX
}

fn to_i64(value: u64) -> Result<i64, DataScanError> {
    i64::try_from(value).map_err(|_| DataScanError::Range)
}

fn hex(value: &[u8]) -> String {
    value.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn now_ms() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(i64::MAX)
}

#[derive(Debug, Error)]
pub(crate) enum ScanFsError {
    #[error("filesystem access failed")]
    Io(#[from] std::io::Error),
    #[error("scan encountered an unsafe path")]
    UnsafePath,
    #[error("release path is not valid UTF-8")]
    NonUtf8,
    #[error("release exceeded its file limit")]
    FileLimit,
    #[error("release exceeded its directory depth limit")]
    FileDepth,
    #[error("release size overflowed")]
    SizeOverflow,
}

impl ScanFsError {
    fn reason_code(&self) -> &'static str {
        match self {
            Self::Io(_) => "filesystem_error",
            Self::UnsafePath => "unsafe_path",
            Self::NonUtf8 => "non_utf8_path",
            Self::FileLimit => "file_limit",
            Self::FileDepth => "file_depth_limit",
            Self::SizeOverflow => "size_overflow",
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum DataScanError {
    #[error("data scan database operation failed")]
    Database(#[from] sqlx::Error),
    #[error("data scan payload is invalid")]
    Json(#[from] serde_json::Error),
    #[error("data scan durable ingress failed")]
    DurableIngress(#[from] crate::durable_ingress::DurableIngressError),
    #[error("data scan search ingress failed")]
    Search(#[from] SearchError),
    #[error("data scan blocking activity failed")]
    Join(#[source] tokio::task::JoinError),
    #[error("data scan filesystem operation failed")]
    Filesystem(#[from] ScanFsError),
    #[error("data scan value is out of range")]
    Range,
    #[error("database contains an out-of-range scan value")]
    StoredRange,
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::symlink;

    use sporos_matcher::parse_release;
    use tempfile::TempDir;

    use super::*;
    use crate::config::{Injection, Matching, Paths, SourceFilters};
    use crate::preflight::SourceState;

    #[tokio::test]
    async fn scans_files_and_discs_without_following_symlinks() {
        let directory = TempDir::new().unwrap();
        let root = directory.path().join("media");
        std::fs::create_dir(&root).unwrap();
        std::fs::write(root.join("Example.Show.S01E01.mkv"), b"episode").unwrap();
        let video_ts = root.join("Example.Movie.2024/VIDEO_TS");
        std::fs::create_dir_all(&video_ts).unwrap();
        std::fs::write(video_ts.join("VTS_01_1.VOB"), b"disc-video").unwrap();
        let outside = directory.path().join("Outside.Movie.2024.mkv");
        std::fs::write(&outside, b"outside").unwrap();
        symlink(&outside, root.join("Escaped.Movie.2024.mkv")).unwrap();

        let storage = Arc::new(open(&directory).await);
        let executor = executor(Arc::clone(&storage), &root);
        let accepted = accept(&storage, "media", policy(), Vec::new(), None, 10)
            .await
            .unwrap();
        let input = load_input(&storage, accepted.task_id).await;
        let step = executor.scan_page(&input, 11).await.unwrap();
        assert!(step.done);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM sporos_data_source")
                .fetch_one(storage.pool())
                .await
                .unwrap(),
            2
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM sporos_source_file
                 WHERE available = 1 AND file_kind = 'video' AND modified_at IS NOT NULL",
            )
            .fetch_one(storage.pool())
            .await
            .unwrap(),
            2
        );
        assert_eq!(
            storage
                .preflight_source(
                    &parse_release("Example.Show.S01"),
                    None,
                    0.02,
                    true,
                    &SourceFilters::default(),
                )
                .await
                .unwrap(),
            Some(SourceState::Complete)
        );

        let moved = directory.path().join("media-away");
        std::fs::rename(&root, &moved).unwrap();
        let accepted = accept(&storage, "media", policy(), Vec::new(), Some("again"), 20)
            .await
            .unwrap();
        let input = load_input(&storage, accepted.task_id).await;
        let step = executor.scan_page(&input, 21).await.unwrap();
        assert_eq!(step.reason.as_deref(), Some("root_unavailable"));
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM sporos_data_source WHERE available = 0",
            )
            .fetch_one(storage.pool())
            .await
            .unwrap(),
            2
        );
    }

    #[tokio::test]
    async fn rejects_a_symlinked_configured_root() {
        let directory = TempDir::new().unwrap();
        let actual = directory.path().join("actual");
        std::fs::create_dir(&actual).unwrap();
        std::fs::write(actual.join("Movie.2024.mkv"), b"movie").unwrap();
        let root = directory.path().join("media");
        symlink(&actual, &root).unwrap();

        let storage = Arc::new(open(&directory).await);
        let executor = executor(Arc::clone(&storage), &root);
        let accepted = accept(&storage, "media", policy(), Vec::new(), None, 10)
            .await
            .unwrap();
        let input = load_input(&storage, accepted.task_id).await;
        let step = executor.scan_page(&input, 11).await.unwrap();

        assert_eq!(step.reason.as_deref(), Some("unsafe_root"));
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM sporos_data_source")
                .fetch_one(storage.pool())
                .await
                .unwrap(),
            0
        );
    }

    fn executor(storage: Arc<Storage>, root: &Path) -> DataScanExecutor {
        DataScanExecutor::new(
            storage,
            BTreeMap::from([(
                "media".to_owned(),
                DataRoot {
                    path: root.to_owned(),
                    max_depth: 4,
                    max_releases: 100,
                    max_files_per_release: 100,
                },
            )]),
        )
    }

    fn policy() -> SearchPolicy {
        SearchPolicy::new(
            Matching::default(),
            SourceFilters::default(),
            Injection::default(),
            Paths::default(),
        )
    }

    async fn load_input(storage: &Storage, task_id: [u8; 16]) -> ScanInput {
        let json = sqlx::query_scalar::<_, String>(
            "SELECT input_json FROM sporos_outbox WHERE task_id = ?",
        )
        .bind(task_id.as_slice())
        .fetch_one(storage.pool())
        .await
        .unwrap();
        serde_json::from_str(&json).unwrap()
    }

    async fn open(directory: &TempDir) -> Storage {
        Storage::open(
            directory.path().join("sporos.lock"),
            directory.path().join("sporos.db"),
        )
        .await
        .unwrap()
    }
}
