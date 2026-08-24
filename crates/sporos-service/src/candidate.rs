use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sporos_matcher::parse_release;
use sporos_model::{
    CandidateId, InfoHashes, MatchingPolicy, PolicySnapshotId, TaskId, TaskKey, TorrentFile,
    TorrentManifest,
};
use sqlx::Row;
use thiserror::Error;

use crate::config::{Injection, Matching, Paths, SourceFilters};
use crate::durable_ingress::{DurableIngressError, NewTask, PolicySnapshot, accept_task_in};
use crate::storage::Storage;
use crate::torrent::{TorrentParseError, TorrentParser};

pub const ORCHESTRATION_NAME: &str = "ProcessCandidate";
pub const ORCHESTRATION_VERSION: &str = "1.0.0";
pub const MATCHER_VERSION: &str = "sporos-matcher/1";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateWorkflowInput {
    pub task_id: [u8; 16],
    pub candidate_id: [u8; 16],
    pub policy_snapshot_id: [u8; 16],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidatePolicy {
    pub matching: MatchingPolicy,
    pub source_filters: SourceFilters,
    pub pending_source_timeout_ms: u64,
    pub injection: Injection,
    pub namespace_local_root: String,
    pub save_path_remote_root: String,
    pub path_rewrites: Vec<crate::config::PathRewrite>,
}

#[derive(Debug)]
pub struct CandidateSubmission {
    pub bytes: Vec<u8>,
    pub announcement_name: Option<String>,
    pub indexer: Option<String>,
    pub indexer_id: Option<i64>,
    pub trigger: String,
    pub release_hint: Option<sporos_model::ReleaseDescriptor>,
    pub category: Option<String>,
    pub tags: Vec<String>,
    pub request_id: String,
    pub dry_run: bool,
    pub received_at: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AcceptedCandidate {
    pub candidate_id: CandidateId,
    pub task_id: TaskId,
    pub duplicate: bool,
}

pub struct CandidateIngress {
    matching: Matching,
    source_filters: SourceFilters,
    injection: Injection,
    paths: Paths,
}

impl CandidateIngress {
    pub fn new(
        matching: Matching,
        source_filters: SourceFilters,
        injection: Injection,
        paths: Paths,
    ) -> Self {
        Self {
            matching,
            source_filters,
            injection,
            paths,
        }
    }

    pub async fn accept(
        &self,
        storage: &Storage,
        submission: CandidateSubmission,
    ) -> Result<AcceptedCandidate, CandidateError> {
        if submission.bytes.len() > self.matching.max_torrent_bytes {
            return Err(CandidateError::TorrentTooLarge);
        }
        let parsed = TorrentParser.parse(&submission.bytes)?;
        if parsed.files().len() > self.matching.max_files_per_torrent {
            return Err(CandidateError::TooManyFiles);
        }
        let mut files = Vec::with_capacity(parsed.files().len());
        for (ordinal, file) in parsed.files().iter().enumerate() {
            let components = file
                .path()
                .iter()
                .map(|component| {
                    std::str::from_utf8(component).map_err(|_| CandidateError::InvalidUtf8Path)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let path = components.join("/");
            if path.len() > self.matching.max_path_bytes {
                return Err(CandidateError::PathTooLong);
            }
            files.push(TorrentFile {
                ordinal: u32::try_from(ordinal).map_err(|_| CandidateError::TooManyFiles)?,
                path,
                size: file.length(),
                padding: components.contains(&".pad"),
            });
        }
        let manifest = TorrentManifest {
            hashes: InfoHashes {
                v1: parsed.v1_hash(),
                v2: parsed.v2_hash(),
            },
            files,
            piece_length: Some(parsed.piece_length()),
        };
        let torrent_name =
            std::str::from_utf8(parsed.name()).map_err(|_| CandidateError::InvalidUtf8Name)?;
        let display_name = submission
            .announcement_name
            .as_deref()
            .filter(|name| !name.is_empty())
            .unwrap_or(torrent_name);
        let mut release = parse_release(display_name);
        if let Some(hint) = submission.release_hint
            && (hint.primary_title == release.primary_title
                || hint.alternate_titles.contains(&release.primary_title))
        {
            release.arr_identity = hint.arr_identity;
            for title in hint
                .alternate_titles
                .into_iter()
                .chain(std::iter::once(hint.primary_title))
            {
                if title != release.primary_title && !release.alternate_titles.contains(&title) {
                    release.alternate_titles.push(title);
                }
            }
        }
        let policy = CandidatePolicy {
            matching: self.matching.policy.clone(),
            source_filters: self.source_filters.clone(),
            pending_source_timeout_ms: u64::try_from(
                self.matching.pending_source_timeout.as_millis(),
            )
            .unwrap_or(u64::MAX),
            injection: Injection {
                dry_run: self.injection.dry_run || submission.dry_run,
                ..self.injection.clone()
            },
            namespace_local_root: self.paths.link_root.to_string_lossy().into_owned(),
            save_path_remote_root: self.paths.qbit_link_root().to_string_lossy().into_owned(),
            path_rewrites: self.paths.rewrite.clone(),
        };
        let manifest_json = serde_json::to_string(&manifest)?;
        let release_json = serde_json::to_string(&release)?;
        let policy_json = serde_json::to_string(&policy)?;
        let blob_hash: [u8; 32] = Sha256::digest(&submission.bytes).into();
        let candidate_id = CandidateId::from_bytes(first_16(&blob_hash));
        let manifest_digest: [u8; 32] = Sha256::digest(manifest_json.as_bytes()).into();
        let config_hash: [u8; 32] = Sha256::digest(policy_json.as_bytes()).into();
        let policy_id = PolicySnapshotId::from_bytes(first_16(&config_hash));
        let task_digest: [u8; 32] = Sha256::digest(
            [
                b"candidate-task-v1:".as_slice(),
                candidate_id.as_bytes(),
                policy_id.as_bytes(),
            ]
            .concat(),
        )
        .into();
        let task_id = TaskId::from_bytes(first_16(&task_digest));
        let task = NewTask {
            id: task_id,
            key: TaskKey::from_bytes(task_digest),
            kind: "process_candidate".to_owned(),
            policy: PolicySnapshot {
                id: policy_id,
                config_hash,
                matcher_version: MATCHER_VERSION.to_owned(),
                payload_json: policy_json,
                created_at: submission.received_at,
            },
            orchestration_name: ORCHESTRATION_NAME.to_owned(),
            orchestration_version: ORCHESTRATION_VERSION.to_owned(),
            instance_id: format!(
                "candidate-v1:{}:{}",
                encode_hex(candidate_id.as_bytes()),
                encode_hex(policy_id.as_bytes())
            ),
            input_json: serde_json::to_string(&CandidateWorkflowInput {
                task_id: *task_id.as_bytes(),
                candidate_id: *candidate_id.as_bytes(),
                policy_snapshot_id: *policy_id.as_bytes(),
            })?,
            created_at: submission.received_at,
        };

        let mut transaction = storage.pool().begin().await?;
        sqlx::query(
            "INSERT INTO sporos_blob (sha256, media_type, size, data, created_at)
             VALUES (?, 'application/x-bittorrent', ?, ?, ?)
             ON CONFLICT(sha256) DO NOTHING",
        )
        .bind(blob_hash.as_slice())
        .bind(i64::try_from(submission.bytes.len()).map_err(|_| CandidateError::TorrentTooLarge)?)
        .bind(&submission.bytes)
        .bind(submission.received_at)
        .execute(&mut *transaction)
        .await?;
        verify_blob(&mut transaction, &blob_hash, &submission.bytes).await?;

        sqlx::query(
            "INSERT INTO sporos_candidate (
                id, blob_sha256, v1_hash, v2_hash, manifest_digest, display_name,
                release_json, state, created_at, updated_at, manifest_json
             ) VALUES (?, ?, ?, ?, ?, ?, ?, 'received', ?, ?, ?)
             ON CONFLICT(id) DO NOTHING",
        )
        .bind(candidate_id.as_bytes().as_slice())
        .bind(blob_hash.as_slice())
        .bind(manifest.hashes.v1.map(|hash| hash.to_vec()))
        .bind(manifest.hashes.v2.map(|hash| hash.to_vec()))
        .bind(manifest_digest.as_slice())
        .bind(display_name)
        .bind(release_json)
        .bind(submission.received_at)
        .bind(submission.received_at)
        .bind(manifest_json)
        .execute(&mut *transaction)
        .await?;
        verify_candidate(&mut transaction, candidate_id, &blob_hash, &manifest_digest).await?;

        let inserted = accept_task_in(&mut transaction, &task).await?;
        sqlx::query(
            "INSERT INTO sporos_candidate_task (candidate_id, policy_snapshot_id, task_id)
             VALUES (?, ?, ?) ON CONFLICT(candidate_id, policy_snapshot_id) DO NOTHING",
        )
        .bind(candidate_id.as_bytes().as_slice())
        .bind(policy_id.as_bytes().as_slice())
        .bind(task_id.as_bytes().as_slice())
        .execute(&mut *transaction)
        .await?;
        let stored_task = sqlx::query_scalar::<_, Vec<u8>>(
            "SELECT task_id FROM sporos_candidate_task
             WHERE candidate_id = ? AND policy_snapshot_id = ?",
        )
        .bind(candidate_id.as_bytes().as_slice())
        .bind(policy_id.as_bytes().as_slice())
        .fetch_one(&mut *transaction)
        .await?;
        if stored_task.as_slice() != task_id.as_bytes() {
            return Err(CandidateError::CandidateTaskCollision);
        }

        let detail_json = serde_json::to_string(&serde_json::json!({
            "category": submission.category,
            "tags": submission.tags,
        }))?;
        sqlx::query(
            "INSERT INTO sporos_candidate_provenance (
                candidate_id, trigger, indexer_id, indexer_name, announcement_name,
                request_id, received_at, detail_json
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(candidate_id.as_bytes().as_slice())
        .bind(submission.trigger)
        .bind(submission.indexer_id)
        .bind(submission.indexer)
        .bind(submission.announcement_name)
        .bind(submission.request_id)
        .bind(submission.received_at)
        .bind(detail_json)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;

        Ok(AcceptedCandidate {
            candidate_id,
            task_id,
            duplicate: !inserted,
        })
    }
}

async fn verify_blob(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    hash: &[u8; 32],
    expected: &[u8],
) -> Result<(), CandidateError> {
    let row = sqlx::query("SELECT size, data FROM sporos_blob WHERE sha256 = ?")
        .bind(hash.as_slice())
        .fetch_one(&mut **transaction)
        .await?;
    if row.try_get::<i64, _>("size")? == i64::try_from(expected.len()).unwrap_or(i64::MAX)
        && row.try_get::<Vec<u8>, _>("data")? == expected
    {
        Ok(())
    } else {
        Err(CandidateError::BlobCollision)
    }
}

async fn verify_candidate(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    id: CandidateId,
    blob_hash: &[u8; 32],
    manifest_digest: &[u8; 32],
) -> Result<(), CandidateError> {
    let row = sqlx::query("SELECT blob_sha256, manifest_digest FROM sporos_candidate WHERE id = ?")
        .bind(id.as_bytes().as_slice())
        .fetch_one(&mut **transaction)
        .await?;
    if row.try_get::<Vec<u8>, _>("blob_sha256")?.as_slice() == blob_hash
        && row.try_get::<Vec<u8>, _>("manifest_digest")?.as_slice() == manifest_digest
    {
        Ok(())
    } else {
        Err(CandidateError::CandidateCollision)
    }
}

fn first_16(hash: &[u8; 32]) -> [u8; 16] {
    let mut id = [0_u8; 16];
    id.copy_from_slice(&hash[..16]);
    id
}

fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[derive(Debug, Error)]
pub enum CandidateError {
    #[error("torrent exceeds the configured byte limit")]
    TorrentTooLarge,
    #[error("torrent exceeds the configured file-count limit")]
    TooManyFiles,
    #[error("torrent contains a path exceeding the configured limit")]
    PathTooLong,
    #[error("torrent name is not valid UTF-8")]
    InvalidUtf8Name,
    #[error("torrent path is not valid UTF-8")]
    InvalidUtf8Path,
    #[error("torrent is structurally invalid")]
    Torrent(#[from] TorrentParseError),
    #[error("candidate data could not be encoded")]
    Json(#[from] serde_json::Error),
    #[error("candidate database operation failed")]
    Database(#[from] sqlx::Error),
    #[error("candidate durable ingress failed")]
    DurableIngress(#[from] DurableIngressError),
    #[error("blob hash refers to different content")]
    BlobCollision,
    #[error("candidate ID refers to different content")]
    CandidateCollision,
    #[error("candidate and policy refer to a different task")]
    CandidateTaskCollision,
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[tokio::test]
    async fn commits_candidate_and_work_once_with_repeatable_provenance() {
        let directory = TempDir::new().expect("temporary directory");
        let storage = open(&directory).await;
        let ingress = CandidateIngress::new(
            Matching::default(),
            SourceFilters::default(),
            Injection::default(),
            Paths {
                link_root: directory.path().join("links"),
                rewrite: Vec::new(),
            },
        );

        let first = ingress
            .accept(&storage, submission(10, false))
            .await
            .expect("accept candidate");
        let second = ingress
            .accept(&storage, submission(20, false))
            .await
            .expect("accept duplicate");

        assert!(!first.duplicate);
        assert!(second.duplicate);
        assert_eq!(first.candidate_id, second.candidate_id);
        assert_eq!(first.task_id, second.task_id);
        assert_counts(&storage, [1, 1, 1, 1, 1, 2]).await;
    }

    #[tokio::test]
    async fn a_safer_request_creates_a_distinct_policy_task() {
        let directory = TempDir::new().expect("temporary directory");
        let storage = open(&directory).await;
        let ingress = CandidateIngress::new(
            Matching::default(),
            SourceFilters::default(),
            Injection::default(),
            Paths::default(),
        );

        let normal = ingress
            .accept(&storage, submission(10, false))
            .await
            .unwrap();
        let dry_run = ingress
            .accept(&storage, submission(20, true))
            .await
            .unwrap();

        assert_eq!(normal.candidate_id, dry_run.candidate_id);
        assert_ne!(normal.task_id, dry_run.task_id);
        assert!(!dry_run.duplicate);
        assert_counts(&storage, [1, 1, 2, 2, 2, 2]).await;
    }

    #[tokio::test]
    async fn malformed_input_commits_nothing() {
        let directory = TempDir::new().expect("temporary directory");
        let storage = open(&directory).await;
        let ingress = CandidateIngress::new(
            Matching::default(),
            SourceFilters::default(),
            Injection::default(),
            Paths::default(),
        );
        let mut invalid = submission(10, false);
        invalid.bytes = b"not a torrent".to_vec();

        assert!(ingress.accept(&storage, invalid).await.is_err());
        assert_counts(&storage, [0, 0, 0, 0, 0, 0]).await;
    }

    fn submission(received_at: i64, dry_run: bool) -> CandidateSubmission {
        let bytes = format!(
            "d4:infod6:lengthi13e4:name18:Example.Movie.202412:piece lengthi16384e6:pieces20:{}ee",
            "a".repeat(20)
        )
        .into_bytes();
        CandidateSubmission {
            bytes,
            announcement_name: Some("Example.Movie.2024.1080p".to_owned()),
            indexer: Some("fixture".to_owned()),
            indexer_id: None,
            trigger: "test".to_owned(),
            release_hint: None,
            category: None,
            tags: Vec::new(),
            request_id: format!("req-{received_at}"),
            dry_run,
            received_at,
        }
    }

    async fn open(directory: &TempDir) -> Storage {
        Storage::open(
            directory.path().join("sporos.lock"),
            directory.path().join("sporos.db"),
        )
        .await
        .expect("open storage")
    }

    async fn assert_counts(storage: &Storage, expected: [i64; 6]) {
        for (index, (table, query)) in [
            ("sporos_blob", "SELECT count(*) FROM sporos_blob"),
            ("sporos_candidate", "SELECT count(*) FROM sporos_candidate"),
            (
                "sporos_policy_snapshot",
                "SELECT count(*) FROM sporos_policy_snapshot",
            ),
            ("sporos_task", "SELECT count(*) FROM sporos_task"),
            ("sporos_outbox", "SELECT count(*) FROM sporos_outbox"),
            (
                "sporos_candidate_provenance",
                "SELECT count(*) FROM sporos_candidate_provenance",
            ),
        ]
        .into_iter()
        .enumerate()
        {
            let count = sqlx::query_scalar::<_, i64>(query)
                .fetch_one(storage.pool())
                .await
                .unwrap();
            assert_eq!(count, expected[index], "{table}");
        }
    }
}
