use std::sync::Arc;
use std::time::Duration;

use duroxide::runtime::registry::ActivityRegistryBuilder;
use duroxide::{ActivityContext, OrchestrationContext};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sporos_matcher::parse_release;
use sporos_model::{PolicySnapshotId, ReleaseDescriptor, TaskId, TaskKey};
use sqlx::{Row, Sqlite, Transaction};
use thiserror::Error;

use crate::candidate::{CandidateError, CandidateIngress, CandidateSubmission};
use crate::completion::CompletionInput;
use crate::config::{Injection, Matching, Paths, SourceFilters};
use crate::durable_ingress::{NewTask, PolicySnapshot, accept_task_in};
use crate::prowlarr::{ProwlarrClient, ProwlarrError};
use crate::storage::Storage;

pub(crate) const ORCHESTRATION_NAME: &str = "SearchSourceIndexer";
pub(crate) const ORCHESTRATION_VERSION: &str = "1.0.0";
pub(crate) const EXECUTE_ACTIVITY: &str = "ExecuteProwlarrSearch";
pub(crate) const BACKFILL_ORCHESTRATION_NAME: &str = "ProduceInventorySearch";
pub(crate) const BACKFILL_ORCHESTRATION_VERSION: &str = "1.0.0";
const BACKFILL_ACTIVITY: &str = "ProduceInventorySearchPage";
const BACKFILL_PAGE_SIZE: i64 = 100;
const MATCHER_VERSION: &str = "sporos-matcher/1";
const MIN_REQUEST_GAP_MS: i64 = 1_000;
const DEFAULT_RETRY_MS: i64 = 60_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SearchPolicy {
    pub(crate) matching: Matching,
    source_filters: SourceFilters,
    injection: Injection,
    paths: Paths,
}

impl SearchPolicy {
    pub(crate) fn new(
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

    pub(crate) fn with_dry_run(mut self, dry_run: bool) -> Self {
        self.injection.dry_run |= dry_run;
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SearchInput {
    pub task_id: [u8; 16],
    pub attempt_id: [u8; 16],
    pub source_id: [u8; 16],
    pub indexer_id: i64,
    pub policy_snapshot_id: [u8; 16],
    pub trigger: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
enum SearchOutcome {
    Complete,
    Waiting {
        next_eligible_at: i64,
        delay_ms: u64,
    },
    Failed {
        reason: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct BackfillSelection {
    #[serde(default)]
    pub hashes: Vec<String>,
    #[serde(default)]
    pub include_categories: Vec<String>,
    #[serde(default)]
    pub exclude_categories: Vec<String>,
    #[serde(default)]
    pub include_tags: Vec<String>,
    #[serde(default)]
    pub exclude_tags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BackfillInput {
    operation_id: [u8; 16],
    task_id: [u8; 16],
    policy_snapshot_id: [u8; 16],
    selection: BackfillSelection,
    indexer_ids: Vec<i64>,
    cursor: Option<[u8; 16]>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BackfillPage {
    next_cursor: Option<[u8; 16]>,
    done: bool,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct AcceptedBackfill {
    pub operation_id: [u8; 16],
    pub task_id: [u8; 16],
    pub duplicate: bool,
}

#[derive(Clone)]
pub(crate) struct SearchExecutor {
    storage: Arc<Storage>,
    client: ProwlarrClient,
    policy: SearchPolicy,
    arr: Option<crate::arr::ArrEnricher>,
    limiter: Option<Arc<tokio::sync::Semaphore>>,
}

impl SearchExecutor {
    pub(crate) fn new(storage: Arc<Storage>, client: ProwlarrClient, policy: SearchPolicy) -> Self {
        Self {
            storage,
            client,
            policy,
            arr: None,
            limiter: None,
        }
    }

    pub(crate) fn with_arr(mut self, arr: crate::arr::ArrEnricher) -> Self {
        self.arr = Some(arr);
        self
    }

    pub(crate) fn with_limiter(mut self, limiter: Arc<tokio::sync::Semaphore>) -> Self {
        self.limiter = Some(limiter);
        self
    }

    pub(crate) fn register(self, activities: ActivityRegistryBuilder) -> ActivityRegistryBuilder {
        let executor = self.clone();
        let activities = activities.register(
            EXECUTE_ACTIVITY,
            move |_context: ActivityContext, input: String| {
                let executor = executor.clone();
                async move {
                    let input: SearchInput = serde_json::from_str(&input).map_err(|error| {
                        crate::activity_failure::permanent("invalid_search_input", error)
                    })?;
                    let outcome = executor
                        .execute(&input, now_ms())
                        .await
                        .map_err(|error| error.activity_failure())?;
                    serde_json::to_string(&outcome).map_err(|error| {
                        crate::activity_failure::permanent("encode_search_result", error)
                    })
                }
            },
        );
        let storage = Arc::clone(&self.storage);
        let limiter = self.limiter.clone();
        activities.register(
            BACKFILL_ACTIVITY,
            move |_context: ActivityContext, input: String| {
                let storage = Arc::clone(&storage);
                let limiter = limiter.clone();
                async move {
                    let _permit = match &limiter {
                        Some(limiter) => Some(crate::execution::permit(limiter).await),
                        None => None,
                    };
                    let input: BackfillInput = serde_json::from_str(&input).map_err(|error| {
                        crate::activity_failure::permanent("invalid_backfill_input", error)
                    })?;
                    let page = produce_page(&storage, &input, now_ms())
                        .await
                        .map_err(|error| error.activity_failure())?;
                    serde_json::to_string(&page).map_err(|error| {
                        crate::activity_failure::permanent("encode_backfill_result", error)
                    })
                }
            },
        )
    }

    pub(crate) async fn project_completion(
        &self,
        input: &CompletionInput,
    ) -> Result<(), SearchError> {
        let mut transaction = self.storage.pool().begin().await?;
        let indexers = sqlx::query_scalar::<_, i64>(
            "SELECT prowlarr_id FROM sporos_indexer
             WHERE eligible = 1 ORDER BY priority, prowlarr_id",
        )
        .fetch_all(&mut *transaction)
        .await?;
        let mut produced = 0_i64;
        for indexer_id in indexers {
            if accept_in(
                &mut transaction,
                input.source_id,
                indexer_id,
                &self.policy,
                "completion",
                Some(input.operation_id),
                input.observed_at,
            )
            .await?
            {
                produced += 1;
            }
        }
        project_completion_in(&mut transaction, input, produced).await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn execute(&self, input: &SearchInput, now: i64) -> Result<SearchOutcome, SearchError> {
        let _permit = match &self.limiter {
            Some(limiter) => Some(crate::execution::permit(limiter).await),
            None => None,
        };
        if let Some(arr) = &self.arr {
            // Enrichment is advisory: filename and file-tree matching remains available
            // while an Arr instance is down.
            let _ = arr.enrich_source(input.source_id, now).await;
        }
        let Some(source) = self.load_source(input).await? else {
            self.finish(input, now, "source_unavailable", true, 0, 0)
                .await?;
            return Ok(SearchOutcome::Failed {
                reason: "source_unavailable".to_owned(),
            });
        };
        let Some(query) = self
            .storage
            .indexer_query(input.indexer_id, &source.release)
            .await?
        else {
            self.finish(input, now, "indexer_ineligible", true, 0, 0)
                .await?;
            return Ok(SearchOutcome::Failed {
                reason: "indexer_ineligible".to_owned(),
            });
        };
        if let Some(next) = self.claim_indexer(input.indexer_id, now).await? {
            self.wait(input, next, "indexer_rate_limit").await?;
            return Ok(SearchOutcome::Waiting {
                next_eligible_at: next,
                delay_ms: u64::try_from(next.saturating_sub(now)).unwrap_or(u64::MAX),
            });
        }
        self.mark_searching(input, now).await?;
        let results = match self.client.search(input.indexer_id, &query).await {
            Ok(results) => results,
            Err(error) => return self.dependency_error(input, now, error).await,
        };
        let mut seen = 0_i64;
        let mut downloaded = 0_i64;
        for (ordinal, result) in results.into_iter().enumerate() {
            seen += 1;
            let eligible = plausible(&source.release, source.total_size, &result);
            self.summarize(
                input,
                ordinal,
                SummaryUpdate {
                    result: &result,
                    state: if eligible { "eligible" } else { "filtered" },
                    candidate_id: None,
                },
            )
            .await?;
            if !eligible {
                continue;
            }
            let bytes = match self
                .client
                .download(input.indexer_id, &result.download_url)
                .await
            {
                Ok(bytes) => bytes,
                Err(error @ ProwlarrError::RateLimited { .. }) => {
                    return self.dependency_error(input, now, error).await;
                }
                Err(ProwlarrError::UnsafeDownloadUrl | ProwlarrError::RedirectRejected) => {
                    self.summarize(
                        input,
                        ordinal,
                        SummaryUpdate {
                            result: &result,
                            state: "unsafe_download",
                            candidate_id: None,
                        },
                    )
                    .await?;
                    continue;
                }
                Err(_) => {
                    self.summarize(
                        input,
                        ordinal,
                        SummaryUpdate {
                            result: &result,
                            state: "download_failed",
                            candidate_id: None,
                        },
                    )
                    .await?;
                    continue;
                }
            };
            let policy = self.load_policy(input.policy_snapshot_id).await?;
            let ingress = CandidateIngress::new(
                policy.matching,
                policy.source_filters,
                policy.injection,
                policy.paths,
            );
            match ingress
                .accept(
                    &self.storage,
                    CandidateSubmission {
                        bytes,
                        announcement_name: Some(result.title.clone()),
                        indexer: Some(source.indexer_name.clone()),
                        indexer_id: Some(input.indexer_id),
                        trigger: input.trigger.clone(),
                        release_hint: Some(source.release.clone()),
                        category: None,
                        tags: Vec::new(),
                        request_id: format!("search:{}:{ordinal}", hex(&input.attempt_id)),
                        dry_run: false,
                        received_at: now,
                    },
                )
                .await
            {
                Ok(accepted) => {
                    downloaded += 1;
                    self.summarize(
                        input,
                        ordinal,
                        SummaryUpdate {
                            result: &result,
                            state: "candidate_accepted",
                            candidate_id: Some(*accepted.candidate_id.as_bytes()),
                        },
                    )
                    .await?;
                }
                Err(error) if candidate_rejected(&error) => {
                    self.summarize(
                        input,
                        ordinal,
                        SummaryUpdate {
                            result: &result,
                            state: "invalid_torrent",
                            candidate_id: None,
                        },
                    )
                    .await?;
                }
                Err(error) => return Err(error.into()),
            }
        }
        self.finish(input, now, "completed", false, seen, downloaded)
            .await?;
        Ok(SearchOutcome::Complete)
    }

    async fn load_source(&self, input: &SearchInput) -> Result<Option<SearchSource>, SearchError> {
        let indexer_name = sqlx::query_scalar::<_, String>(
            "SELECT name FROM sporos_indexer WHERE prowlarr_id = ? AND eligible = 1",
        )
        .bind(input.indexer_id)
        .fetch_optional(self.storage.pool())
        .await?;
        let Some(indexer_name) = indexer_name else {
            return Ok(None);
        };
        let policy = self.load_policy(input.policy_snapshot_id).await?;
        let row = sqlx::query(
            "SELECT release_json, total_size, category, tags_json
             FROM sporos_qbit_torrent
             WHERE id = ? AND available = 1 AND is_complete = 1",
        )
        .bind(input.source_id.as_slice())
        .fetch_optional(self.storage.pool())
        .await?;
        if let Some(row) = row {
            let category = row.try_get::<String, _>("category")?;
            let tags: Vec<String> = serde_json::from_str(&row.try_get::<String, _>("tags_json")?)?;
            if !source_allowed(&category, &tags, &policy.source_filters) {
                return Ok(None);
            }
            let Some(release_json) = row.try_get::<Option<String>, _>("release_json")? else {
                return Ok(None);
            };
            return Ok(Some(SearchSource {
                release: serde_json::from_str(&release_json)?,
                total_size: u64::try_from(row.try_get::<i64, _>("total_size")?).unwrap_or(u64::MAX),
                indexer_name,
            }));
        }
        let row = sqlx::query(
            "SELECT release_json, total_size FROM sporos_data_source
             WHERE id = ? AND available = 1",
        )
        .bind(input.source_id.as_slice())
        .fetch_optional(self.storage.pool())
        .await?;
        let Some(row) = row else { return Ok(None) };
        let Some(release_json) = row.try_get::<Option<String>, _>("release_json")? else {
            return Ok(None);
        };
        Ok(Some(SearchSource {
            release: serde_json::from_str(&release_json)?,
            total_size: u64::try_from(row.try_get::<i64, _>("total_size")?).unwrap_or(u64::MAX),
            indexer_name,
        }))
    }

    async fn load_policy(&self, id: [u8; 16]) -> Result<SearchPolicy, SearchError> {
        let json = sqlx::query_scalar::<_, String>(
            "SELECT payload_json FROM sporos_policy_snapshot WHERE id = ?",
        )
        .bind(id.as_slice())
        .fetch_one(self.storage.pool())
        .await?;
        Ok(serde_json::from_str(&json)?)
    }

    async fn claim_indexer(&self, indexer_id: i64, now: i64) -> Result<Option<i64>, SearchError> {
        let mut transaction = self.storage.pool().begin().await?;
        let next = sqlx::query_scalar::<_, i64>(
            "SELECT next_eligible_at FROM sporos_indexer_rate_limit WHERE indexer_id = ?",
        )
        .bind(indexer_id)
        .fetch_optional(&mut *transaction)
        .await?;
        if next.is_some_and(|next| next > now) {
            transaction.rollback().await?;
            return Ok(next);
        }
        sqlx::query(
            "INSERT INTO sporos_indexer_rate_limit (indexer_id, next_eligible_at, updated_at)
             VALUES (?, ?, ?) ON CONFLICT(indexer_id) DO UPDATE SET
             next_eligible_at = excluded.next_eligible_at, updated_at = excluded.updated_at",
        )
        .bind(indexer_id)
        .bind(now.saturating_add(MIN_REQUEST_GAP_MS))
        .bind(now)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(None)
    }

    async fn dependency_error(
        &self,
        input: &SearchInput,
        now: i64,
        error: ProwlarrError,
    ) -> Result<SearchOutcome, SearchError> {
        let delay = match error {
            ProwlarrError::RateLimited { retry_after } => retry_after
                .and_then(|value| i64::try_from(value.as_millis()).ok())
                .unwrap_or(DEFAULT_RETRY_MS),
            ProwlarrError::Request(_) | ProwlarrError::HttpStatus(_) => DEFAULT_RETRY_MS,
            other => {
                self.finish(input, now, "invalid_search_response", true, 0, 0)
                    .await?;
                return Ok(SearchOutcome::Failed {
                    reason: other.to_string(),
                });
            }
        };
        let next = now.saturating_add(delay.max(MIN_REQUEST_GAP_MS));
        sqlx::query(
            "INSERT INTO sporos_indexer_rate_limit (indexer_id, next_eligible_at, updated_at)
             VALUES (?, ?, ?) ON CONFLICT(indexer_id) DO UPDATE SET
             next_eligible_at = max(next_eligible_at, excluded.next_eligible_at),
             updated_at = excluded.updated_at",
        )
        .bind(input.indexer_id)
        .bind(next)
        .bind(now)
        .execute(self.storage.pool())
        .await?;
        self.wait(input, next, "dependency_unavailable").await?;
        Ok(SearchOutcome::Waiting {
            next_eligible_at: next,
            delay_ms: u64::try_from(next.saturating_sub(now)).unwrap_or(u64::MAX),
        })
    }

    async fn mark_searching(&self, input: &SearchInput, now: i64) -> Result<(), SearchError> {
        sqlx::query("UPDATE sporos_task SET state = 'searching', updated_at = ? WHERE id = ? AND terminal_at IS NULL")
            .bind(now).bind(input.task_id.as_slice()).execute(self.storage.pool()).await?;
        sqlx::query("UPDATE sporos_search_attempt SET state = 'searching' WHERE id = ?")
            .bind(input.attempt_id.as_slice())
            .execute(self.storage.pool())
            .await?;
        Ok(())
    }

    async fn wait(&self, input: &SearchInput, next: i64, reason: &str) -> Result<(), SearchError> {
        sqlx::query("UPDATE sporos_search_attempt SET state = 'waiting', next_eligible_at = ?, reason_code = ?, dependency_attempts = dependency_attempts + 1 WHERE id = ?")
            .bind(next).bind(reason).bind(input.attempt_id.as_slice()).execute(self.storage.pool()).await?;
        sqlx::query("UPDATE sporos_task SET state = 'waiting_dependency', reason_code = ?, observed_retry_count = observed_retry_count + 1, updated_at = ? WHERE id = ? AND terminal_at IS NULL")
            .bind(reason).bind(now_ms()).bind(input.task_id.as_slice()).execute(self.storage.pool()).await?;
        Ok(())
    }

    async fn finish(
        &self,
        input: &SearchInput,
        now: i64,
        reason: &str,
        failed: bool,
        seen: i64,
        downloaded: i64,
    ) -> Result<(), SearchError> {
        let task_state = if failed { "failed" } else { "completed" };
        sqlx::query("UPDATE sporos_search_attempt SET state = ?, results_seen = max(results_seen, ?), results_downloaded = max(results_downloaded, ?), next_eligible_at = NULL, reason_code = ?, completed_at = ? WHERE id = ?")
            .bind(task_state).bind(seen).bind(downloaded).bind(reason).bind(now).bind(input.attempt_id.as_slice()).execute(self.storage.pool()).await?;
        sqlx::query("UPDATE sporos_task SET state = ?, reason_code = ?, updated_at = ?, terminal_at = ? WHERE id = ?")
            .bind(task_state).bind(reason).bind(now).bind(now).bind(input.task_id.as_slice()).execute(self.storage.pool()).await?;
        Ok(())
    }

    async fn summarize(
        &self,
        input: &SearchInput,
        ordinal: usize,
        update: SummaryUpdate<'_>,
    ) -> Result<(), SearchError> {
        let fingerprint = result_fingerprint(update.result);
        sqlx::query("INSERT INTO sporos_search_result_summary (search_attempt_id, ordinal, fingerprint, title, size, state, candidate_id) VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT(search_attempt_id, ordinal) DO UPDATE SET state = excluded.state, candidate_id = coalesce(excluded.candidate_id, candidate_id)")
            .bind(input.attempt_id.as_slice()).bind(i64::try_from(ordinal).unwrap_or(i64::MAX)).bind(fingerprint.as_slice()).bind(&update.result.title).bind(update.result.size.and_then(|value| i64::try_from(value).ok())).bind(update.state).bind(update.candidate_id.map(|id| id.to_vec())).execute(self.storage.pool()).await?;
        Ok(())
    }
}

pub(crate) async fn workflow(
    context: OrchestrationContext,
    input: String,
) -> Result<String, String> {
    let _: SearchInput =
        serde_json::from_str(&input).map_err(|error| format!("invalid search input: {error}"))?;
    loop {
        let output = context
            .schedule_activity_with_retry(
                EXECUTE_ACTIVITY,
                input.clone(),
                crate::engine::activity_retry_policy(),
            )
            .await?;
        match serde_json::from_str::<SearchOutcome>(&output)
            .map_err(|error| format!("invalid search outcome: {error}"))?
        {
            SearchOutcome::Waiting { delay_ms, .. } => {
                context
                    .schedule_timer(Duration::from_millis(delay_ms))
                    .await;
            }
            SearchOutcome::Complete | SearchOutcome::Failed { .. } => return Ok(output),
        }
    }
}

pub(crate) async fn backfill_workflow(
    context: OrchestrationContext,
    input: String,
) -> Result<String, String> {
    let mut input: BackfillInput =
        serde_json::from_str(&input).map_err(|error| format!("invalid backfill input: {error}"))?;
    let output = context
        .schedule_activity_with_retry(
            BACKFILL_ACTIVITY,
            serde_json::to_string(&input)
                .map_err(|error| format!("encode backfill input: {error}"))?,
            crate::engine::activity_retry_policy(),
        )
        .await?;
    let page: BackfillPage =
        serde_json::from_str(&output).map_err(|error| format!("invalid backfill page: {error}"))?;
    if page.done {
        return Ok(output);
    }
    input.cursor = page.next_cursor;
    context
        .continue_as_new(
            serde_json::to_string(&input)
                .map_err(|error| format!("encode continued backfill: {error}"))?,
        )
        .await
}

pub(crate) async fn accept_backfill(
    storage: &Storage,
    policy: SearchPolicy,
    selection: BackfillSelection,
    indexer_ids: Vec<i64>,
    force_nonce: Option<&str>,
    now: i64,
) -> Result<AcceptedBackfill, SearchError> {
    let request_json = serde_json::to_string(&serde_json::json!({
        "source": &selection,
        "indexerIds": &indexer_ids,
        "force": force_nonce.is_some(),
    }))?;
    let policy_json = serde_json::to_string(&policy)?;
    let policy_hash: [u8; 32] = Sha256::digest(policy_json.as_bytes()).into();
    let policy_id = first16(&policy_hash);
    let mut operation_hash = Sha256::new();
    operation_hash.update(b"inventory-search-operation-v1");
    operation_hash.update(request_json.as_bytes());
    operation_hash.update(policy_id);
    if let Some(nonce) = force_nonce {
        operation_hash.update(nonce.as_bytes());
    }
    let operation_digest: [u8; 32] = operation_hash.finalize().into();
    let operation_id = first16(&operation_digest);
    let task_digest: [u8; 32] = Sha256::digest(
        [
            b"inventory-search-task-v1".as_slice(),
            operation_id.as_slice(),
        ]
        .concat(),
    )
    .into();
    let task_id = first16(&task_digest);
    let instance_id = format!("inventory-search-v1:{}", hex(&operation_id));
    let input = BackfillInput {
        operation_id,
        task_id,
        policy_snapshot_id: policy_id,
        selection,
        indexer_ids,
        cursor: None,
    };
    let task = NewTask {
        id: TaskId::from_bytes(task_id),
        key: TaskKey::from_bytes(task_digest),
        kind: "inventory_search".to_owned(),
        policy: PolicySnapshot {
            id: PolicySnapshotId::from_bytes(policy_id),
            config_hash: policy_hash,
            matcher_version: MATCHER_VERSION.to_owned(),
            payload_json: policy_json,
            created_at: now,
        },
        orchestration_name: BACKFILL_ORCHESTRATION_NAME.to_owned(),
        orchestration_version: BACKFILL_ORCHESTRATION_VERSION.to_owned(),
        instance_id: instance_id.clone(),
        input_json: serde_json::to_string(&input)?,
        created_at: now,
    };
    let mut transaction = storage.pool().begin().await?;
    let inserted = accept_task_in(&mut transaction, &task).await?;
    sqlx::query(
        "INSERT INTO sporos_operation (id, kind, state, duroxide_instance_id,
         request_json, produced_tasks, completed_tasks, failed_tasks, created_at, updated_at)
         VALUES (?, 'inventory_search', 'queued', ?, ?, 0, 0, 0, ?, ?)
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
    transaction.commit().await?;
    Ok(AcceptedBackfill {
        operation_id,
        task_id,
        duplicate: !inserted,
    })
}

async fn produce_page(
    storage: &Storage,
    input: &BackfillInput,
    now: i64,
) -> Result<BackfillPage, SearchError> {
    let rows = sqlx::query(
        "SELECT id, v1_hash, v2_hash, category, tags_json
         FROM sporos_qbit_torrent
         WHERE available = 1 AND is_complete = 1 AND (? IS NULL OR id > ?)
         ORDER BY id LIMIT ?",
    )
    .bind(input.cursor.as_ref().map(|value| value.as_slice()))
    .bind(input.cursor.as_ref().map(|value| value.as_slice()))
    .bind(BACKFILL_PAGE_SIZE)
    .fetch_all(storage.pool())
    .await?;
    let done = rows.len() < usize::try_from(BACKFILL_PAGE_SIZE).expect("positive page size");
    let next_cursor = rows
        .last()
        .map(|row| id16_from_vec(row.try_get::<Vec<u8>, _>("id")?))
        .transpose()?;
    let policy_json = sqlx::query_scalar::<_, String>(
        "SELECT payload_json FROM sporos_policy_snapshot WHERE id = ?",
    )
    .bind(input.policy_snapshot_id.as_slice())
    .fetch_one(storage.pool())
    .await?;
    let policy: SearchPolicy = serde_json::from_str(&policy_json)?;
    let indexer_ids = eligible_indexers(storage, &input.indexer_ids).await?;
    let trigger = format!("manual:{}", hex(&input.operation_id));
    let mut produced = 0_i64;
    let mut transaction = storage.pool().begin().await?;
    for row in rows {
        let source_id = id16_from_vec(row.try_get("id")?)?;
        if !backfill_selected(&row, &input.selection)? {
            continue;
        }
        for indexer_id in &indexer_ids {
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
    sqlx::query(
        "UPDATE sporos_operation SET state = ?, produced_tasks = produced_tasks + ?,
         last_reported_cursor = ?, updated_at = ? WHERE id = ?",
    )
    .bind(if done { "completed" } else { "running" })
    .bind(produced)
    .bind(next_cursor.map(|value| value.to_vec()))
    .bind(now)
    .bind(input.operation_id.as_slice())
    .execute(&mut *transaction)
    .await?;
    sqlx::query("UPDATE sporos_task SET state = ?, updated_at = ?, terminal_at = ? WHERE id = ?")
        .bind(if done { "completed" } else { "running" })
        .bind(now)
        .bind(done.then_some(now))
        .bind(input.task_id.as_slice())
        .execute(&mut *transaction)
        .await?;
    transaction.commit().await?;
    Ok(BackfillPage { next_cursor, done })
}

async fn eligible_indexers(storage: &Storage, selected: &[i64]) -> Result<Vec<i64>, SearchError> {
    if selected.is_empty() {
        return Ok(sqlx::query_scalar::<_, i64>(
            "SELECT prowlarr_id FROM sporos_indexer
             WHERE eligible = 1 ORDER BY priority, prowlarr_id",
        )
        .fetch_all(storage.pool())
        .await?);
    }
    let mut eligible = Vec::new();
    for indexer_id in selected {
        if sqlx::query_scalar::<_, i64>(
            "SELECT 1 FROM sporos_indexer WHERE prowlarr_id = ? AND eligible = 1",
        )
        .bind(indexer_id)
        .fetch_optional(storage.pool())
        .await?
        .is_some()
        {
            eligible.push(*indexer_id);
        }
    }
    Ok(eligible)
}

fn backfill_selected(
    row: &sqlx::sqlite::SqliteRow,
    selection: &BackfillSelection,
) -> Result<bool, SearchError> {
    let category: String = row.try_get("category")?;
    let tags: Vec<String> = serde_json::from_str(&row.try_get::<String, _>("tags_json")?)?;
    let hashes = [
        row.try_get::<Option<Vec<u8>>, _>("v1_hash")?,
        row.try_get::<Option<Vec<u8>>, _>("v2_hash")?,
    ];
    Ok((selection.hashes.is_empty()
        || hashes
            .iter()
            .flatten()
            .any(|hash| selection.hashes.iter().any(|value| value == &hex(hash))))
        && (selection.include_categories.is_empty()
            || selection.include_categories.contains(&category))
        && !selection.exclude_categories.contains(&category)
        && (selection.include_tags.is_empty()
            || selection.include_tags.iter().any(|tag| tags.contains(tag)))
        && !selection.exclude_tags.iter().any(|tag| tags.contains(tag))
        && !tags.iter().any(|tag| tag.starts_with("sporos")))
}

pub(crate) async fn accept_in(
    transaction: &mut Transaction<'_, Sqlite>,
    source_id: [u8; 16],
    indexer_id: i64,
    policy: &SearchPolicy,
    trigger: &str,
    operation_id: Option<[u8; 16]>,
    now: i64,
) -> Result<bool, SearchError> {
    let mut release_json = sqlx::query_scalar::<_, Option<String>>(
        "SELECT release_json FROM sporos_qbit_torrent WHERE id = ? AND available = 1 AND is_complete = 1",
    )
    .bind(source_id.as_slice())
    .fetch_optional(&mut **transaction)
    .await?
    .flatten();
    if release_json.is_none() {
        release_json = sqlx::query_scalar::<_, Option<String>>(
            "SELECT release_json FROM sporos_data_source WHERE id = ? AND available = 1",
        )
        .bind(source_id.as_slice())
        .fetch_optional(&mut **transaction)
        .await?
        .flatten();
    }
    let release_json = release_json.ok_or(SearchError::SourceUnavailable)?;
    let policy_json = serde_json::to_string(policy)?;
    let policy_hash: [u8; 32] = Sha256::digest(policy_json.as_bytes()).into();
    let policy_id = first16(&policy_hash);
    let query_fingerprint: [u8; 32] = Sha256::digest(release_json.as_bytes()).into();
    let digest = identity(&source_id, indexer_id, &policy_id, trigger);
    let attempt_id = first16(&digest);
    let task_id =
        first16(&Sha256::digest([b"search-task-v1".as_slice(), digest.as_slice()].concat()).into());
    let instance_id = format!("search-v1:{}", hex(&attempt_id));
    let input = SearchInput {
        task_id,
        attempt_id,
        source_id,
        indexer_id,
        policy_snapshot_id: policy_id,
        trigger: trigger.to_owned(),
    };
    let task = NewTask {
        id: TaskId::from_bytes(task_id),
        key: TaskKey::from_bytes(digest),
        kind: "search_source_indexer".to_owned(),
        policy: PolicySnapshot {
            id: PolicySnapshotId::from_bytes(policy_id),
            config_hash: policy_hash,
            matcher_version: MATCHER_VERSION.to_owned(),
            payload_json: policy_json,
            created_at: now,
        },
        orchestration_name: ORCHESTRATION_NAME.to_owned(),
        orchestration_version: ORCHESTRATION_VERSION.to_owned(),
        instance_id,
        input_json: serde_json::to_string(&input)?,
        created_at: now,
    };
    let inserted = accept_task_in(transaction, &task).await?;
    sqlx::query("UPDATE sporos_task SET operation_id = ? WHERE id = ? AND operation_id IS NULL")
        .bind(operation_id.map(|id| id.to_vec()))
        .bind(task_id.as_slice())
        .execute(&mut **transaction)
        .await?;
    sqlx::query("INSERT INTO sporos_search_attempt (id, source_id, indexer_id, query_fingerprint, policy_snapshot_id, trigger, state, results_seen, results_downloaded, created_at, task_id) VALUES (?, ?, ?, ?, ?, ?, 'queued', 0, 0, ?, ?) ON CONFLICT(id) DO NOTHING")
        .bind(attempt_id.as_slice()).bind(source_id.as_slice()).bind(indexer_id).bind(query_fingerprint.as_slice()).bind(policy_id.as_slice()).bind(trigger).bind(now).bind(task_id.as_slice()).execute(&mut **transaction).await?;
    Ok(inserted)
}

async fn project_completion_in(
    transaction: &mut Transaction<'_, Sqlite>,
    input: &CompletionInput,
    produced: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE sporos_task SET state = 'completed', projection_generation = 1, updated_at = ?, terminal_at = ? WHERE id = ? AND projection_generation = 0")
        .bind(input.observed_at).bind(input.observed_at).bind(input.task_id.as_slice()).execute(&mut **transaction).await?;
    sqlx::query("INSERT INTO sporos_task_event (task_id, sequence, state, detail_json, created_at) VALUES (?, 1, 'completed', ?, ?) ON CONFLICT(task_id, sequence) DO NOTHING")
        .bind(input.task_id.as_slice()).bind(serde_json::json!({"sourceId": hex(&input.source_id), "completedAt": input.completed_at, "searchTasks": produced}).to_string()).bind(input.observed_at).execute(&mut **transaction).await?;
    sqlx::query("UPDATE sporos_operation SET state = 'completed', produced_tasks = max(produced_tasks, ?), updated_at = ? WHERE id = ?")
        .bind(produced).bind(input.observed_at).bind(input.operation_id.as_slice()).execute(&mut **transaction).await?;
    Ok(())
}

struct SearchSource {
    release: ReleaseDescriptor,
    total_size: u64,
    indexer_name: String,
}

struct SummaryUpdate<'a> {
    result: &'a crate::torznab::TorznabResult,
    state: &'a str,
    candidate_id: Option<[u8; 16]>,
}

fn plausible(
    source: &ReleaseDescriptor,
    source_size: u64,
    result: &crate::torznab::TorznabResult,
) -> bool {
    if result.size == Some(0) || result.size.is_some_and(|size| size < source_size / 20) {
        return false;
    }
    let candidate = parse_release(&result.title);
    candidate.primary_title == source.primary_title
        && (candidate.year.is_none() || source.year.is_none() || candidate.year == source.year)
}

fn source_allowed(category: &str, tags: &[String], filters: &SourceFilters) -> bool {
    (filters.include_categories.is_empty()
        || filters
            .include_categories
            .iter()
            .any(|value| value == category))
        && !filters
            .exclude_categories
            .iter()
            .any(|value| value == category)
        && (filters.include_tags.is_empty()
            || filters
                .include_tags
                .iter()
                .any(|value| tags.contains(value)))
        && !filters
            .exclude_tags
            .iter()
            .any(|value| tags.contains(value))
        && (!filters.exclude_sporos_managed || !tags.iter().any(|tag| tag.starts_with("sporos")))
}

fn result_fingerprint(result: &crate::torznab::TorznabResult) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(result.title.as_bytes());
    hash.update([0]);
    hash.update(result.guid.as_deref().unwrap_or_default().as_bytes());
    hash.update(result.size.unwrap_or_default().to_be_bytes());
    hash.finalize().into()
}

fn candidate_rejected(error: &CandidateError) -> bool {
    matches!(
        error,
        CandidateError::TorrentTooLarge
            | CandidateError::TooManyFiles
            | CandidateError::PathTooLong
            | CandidateError::InvalidUtf8Name
            | CandidateError::InvalidUtf8Path
            | CandidateError::Torrent(_)
    )
}

fn identity(
    source_id: &[u8; 16],
    indexer_id: i64,
    policy_id: &[u8; 16],
    trigger: &str,
) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"search-attempt-v1");
    hash.update(source_id);
    hash.update(indexer_id.to_be_bytes());
    hash.update(policy_id);
    hash.update(trigger.as_bytes());
    hash.finalize().into()
}

fn first16(hash: &[u8; 32]) -> [u8; 16] {
    let mut id = [0; 16];
    id.copy_from_slice(&hash[..16]);
    id
}

fn id16_from_vec(value: Vec<u8>) -> Result<[u8; 16], SearchError> {
    value.try_into().map_err(|_| SearchError::InvalidStoredId)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn now_ms() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis(),
    )
    .unwrap_or(i64::MAX)
}

#[derive(Debug, Error)]
pub(crate) enum SearchError {
    #[error("search database operation failed")]
    Database(#[from] sqlx::Error),
    #[error("search data is invalid")]
    Json(#[from] serde_json::Error),
    #[error("Prowlarr search failed")]
    Prowlarr(#[from] ProwlarrError),
    #[error("candidate acceptance failed")]
    Candidate(#[from] CandidateError),
    #[error("durable search ingress failed")]
    DurableIngress(#[from] crate::durable_ingress::DurableIngressError),
    #[error("source is not available for search")]
    SourceUnavailable,
    #[error("database contains an invalid identifier")]
    InvalidStoredId,
}

impl SearchError {
    pub(crate) fn activity_failure(&self) -> String {
        match self {
            Self::Database(_) | Self::DurableIngress(_) | Self::Candidate(_) => {
                crate::activity_failure::transient("search_storage_unavailable", self)
            }
            Self::Prowlarr(error) if retryable_prowlarr(error) => {
                crate::activity_failure::transient("prowlarr_unavailable", self)
            }
            Self::Prowlarr(_) | Self::Json(_) | Self::SourceUnavailable | Self::InvalidStoredId => {
                crate::activity_failure::permanent("invalid_search_state", self)
            }
        }
    }
}

fn retryable_prowlarr(error: &ProwlarrError) -> bool {
    match error {
        ProwlarrError::Request(_) | ProwlarrError::RateLimited { .. } => true,
        ProwlarrError::HttpStatus(status) => {
            status.is_server_error()
                || *status == reqwest::StatusCode::REQUEST_TIMEOUT
                || *status == reqwest::StatusCode::TOO_MANY_REQUESTS
        }
        ProwlarrError::Database(_) => true,
        ProwlarrError::InvalidApiKey
        | ProwlarrError::Client(_)
        | ProwlarrError::ResponseTooLarge(_)
        | ProwlarrError::Malformed(_, _)
        | ProwlarrError::MalformedField(_)
        | ProwlarrError::UnsafeDownloadUrl
        | ProwlarrError::RedirectRejected
        | ProwlarrError::Torznab(_)
        | ProwlarrError::Json(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use axum::Router;
    use axum::http::{HeaderValue, StatusCode};
    use axum::response::IntoResponse;
    use axum::routing::get;
    use reqwest::Url;
    use tempfile::TempDir;
    use tokio::net::TcpListener;

    use super::*;
    use crate::config::{Prowlarr, Secret};

    #[tokio::test]
    async fn completion_produces_one_task_per_eligible_indexer() {
        let (_directory, storage) = open().await;
        let source_id = source_id(1);
        insert_source(&storage, source_id, 1).await;
        insert_indexer(&storage, 7).await;
        let mut transaction = storage.pool().begin().await.unwrap();
        let completion = crate::completion::accept(&mut transaction, source_id, 10, 11)
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        let input_json = sqlx::query_scalar::<_, String>(
            "SELECT input_json FROM sporos_outbox WHERE task_id = ?",
        )
        .bind(completion.task_id.as_slice())
        .fetch_one(storage.pool())
        .await
        .unwrap();
        let input: CompletionInput = serde_json::from_str(&input_json).unwrap();
        executor(Arc::clone(&storage), "http://127.0.0.1:1/")
            .project_completion(&input)
            .await
            .unwrap();

        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM sporos_search_attempt")
                .fetch_one(storage.pool())
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT produced_tasks FROM sporos_operation WHERE id = ?",
            )
            .bind(completion.operation_id.as_slice())
            .fetch_one(storage.pool())
            .await
            .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn backfill_pages_by_source_id_and_bounds_each_activity() {
        let (_directory, storage) = open().await;
        insert_indexer(&storage, 7).await;
        for marker in 0..205_u128 {
            insert_source(&storage, marker.to_be_bytes(), marker).await;
        }
        let accepted = accept_backfill(
            &storage,
            policy(),
            BackfillSelection::default(),
            vec![7],
            None,
            10,
        )
        .await
        .unwrap();
        let input_json = sqlx::query_scalar::<_, String>(
            "SELECT input_json FROM sporos_outbox WHERE task_id = ?",
        )
        .bind(accepted.task_id.as_slice())
        .fetch_one(storage.pool())
        .await
        .unwrap();
        let mut input: BackfillInput = serde_json::from_str(&input_json).unwrap();

        let first = produce_page(&storage, &input, 11).await.unwrap();
        assert!(!first.done);
        input.cursor = first.next_cursor;
        let second = produce_page(&storage, &input, 12).await.unwrap();
        assert!(!second.done);
        input.cursor = second.next_cursor;
        let third = produce_page(&storage, &input, 13).await.unwrap();
        assert!(third.done);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM sporos_search_attempt")
                .fetch_one(storage.pool())
                .await
                .unwrap(),
            205
        );
    }

    #[tokio::test]
    async fn rate_limit_is_persisted_for_a_durable_timer() {
        let app = Router::new().route(
            "/api/v1/indexer/7/newznab",
            get(|| async {
                (
                    StatusCode::TOO_MANY_REQUESTS,
                    [("retry-after", HeaderValue::from_static("3"))],
                )
                    .into_response()
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}/", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let (_directory, storage) = open().await;
        let source_id = source_id(1);
        insert_source(&storage, source_id, 1).await;
        insert_indexer(&storage, 7).await;
        let input = accept_search(&storage, source_id, 7).await;

        let outcome = executor(Arc::clone(&storage), &base)
            .execute(&input, 100)
            .await
            .unwrap();

        assert!(matches!(
            outcome,
            SearchOutcome::Waiting {
                next_eligible_at: 3_100,
                delay_ms: 3_000
            }
        ));
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT next_eligible_at FROM sporos_search_attempt WHERE id = ?",
            )
            .bind(input.attempt_id.as_slice())
            .fetch_one(storage.pool())
            .await
            .unwrap(),
            3_100
        );
        server.abort();
    }

    #[tokio::test]
    async fn only_downloaded_proxy_results_become_candidates() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}/", listener.local_addr().unwrap());
        let download_url = format!("{base}api/v1/indexer/7/download?id=one");
        let xml = format!(
            "<rss><channel><item><title>Example.Movie.2024.1080p</title><guid>one</guid><enclosure url=\"{download_url}\"/><size>1000</size></item></channel></rss>"
        );
        let torrent = format!(
            "d4:infod6:lengthi13e4:name18:Example.Movie.202412:piece lengthi16384e6:pieces20:{}ee",
            "a".repeat(20)
        )
        .into_bytes();
        let app = Router::new()
            .route(
                "/api/v1/indexer/7/newznab",
                get(move || {
                    let xml = xml.clone();
                    async move { xml }
                }),
            )
            .route(
                "/api/v1/indexer/7/download",
                get(move || {
                    let torrent = torrent.clone();
                    async move { torrent }
                }),
            );
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let (_directory, storage) = open().await;
        let source_id = source_id(1);
        insert_source(&storage, source_id, 1).await;
        insert_indexer(&storage, 7).await;
        let input = accept_search(&storage, source_id, 7).await;

        assert!(matches!(
            executor(Arc::clone(&storage), &base)
                .execute(&input, 100)
                .await
                .unwrap(),
            SearchOutcome::Complete
        ));
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM sporos_candidate")
                .fetch_one(storage.pool())
                .await
                .unwrap(),
            1
        );
        let provenance = sqlx::query_as::<_, (String, Option<i64>)>(
            "SELECT trigger, indexer_id FROM sporos_candidate_provenance",
        )
        .fetch_one(storage.pool())
        .await
        .unwrap();
        assert_eq!(provenance, ("test".to_owned(), Some(7)));
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT state FROM sporos_search_result_summary WHERE search_attempt_id = ?",
            )
            .bind(input.attempt_id.as_slice())
            .fetch_one(storage.pool())
            .await
            .unwrap(),
            "candidate_accepted"
        );
        server.abort();
    }

    async fn accept_search(storage: &Storage, source_id: [u8; 16], indexer: i64) -> SearchInput {
        let mut transaction = storage.pool().begin().await.unwrap();
        accept_in(
            &mut transaction,
            source_id,
            indexer,
            &policy(),
            "test",
            None,
            1,
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();
        let json = sqlx::query_scalar::<_, String>(
            "SELECT input_json FROM sporos_outbox ORDER BY rowid DESC LIMIT 1",
        )
        .fetch_one(storage.pool())
        .await
        .unwrap();
        serde_json::from_str(&json).unwrap()
    }

    fn executor(storage: Arc<Storage>, base: &str) -> SearchExecutor {
        let settings = Prowlarr {
            url: Url::parse(base).unwrap(),
            api_key: Secret::new("secret"),
            request_timeout: Duration::from_secs(2),
            refresh_interval: Duration::from_secs(300),
            include_tags: Vec::new(),
            exclude_tags: Vec::new(),
            require_proxy_downloads: true,
            max_results_per_query: 100,
        };
        SearchExecutor::new(
            storage,
            ProwlarrClient::new(&settings, 8 * 1024 * 1024).unwrap(),
            policy(),
        )
    }

    fn policy() -> SearchPolicy {
        let link_root = PathBuf::from("/data/links");
        SearchPolicy::new(
            Matching::default(),
            SourceFilters::default(),
            Injection::default(),
            Paths {
                link_root: link_root.clone(),
                rewrite: vec![crate::config::PathRewrite {
                    name: "test-identity".to_owned(),
                    remote: link_root.clone(),
                    local: link_root,
                    services: vec!["qbittorrent".to_owned()],
                }],
            },
        )
    }

    async fn open() -> (TempDir, Arc<Storage>) {
        let directory = TempDir::new().unwrap();
        let storage = Arc::new(
            Storage::open(
                directory.path().join("sporos.lock"),
                directory.path().join("sporos.db"),
            )
            .await
            .unwrap(),
        );
        (directory, storage)
    }

    async fn insert_indexer(storage: &Storage, id: i64) {
        sqlx::query(
            "INSERT INTO sporos_indexer (prowlarr_id, name, protocol, enabled,
             supports_search, redirect, priority, tags_json, capabilities_json,
             eligible, refreshed_at) VALUES (?, 'fixture', 'torrent', 1, 1, 0, 1,
             '[]', '{\"searchParams\":[\"q\"],\"movieSearchParams\":[\"q\",\"year\"],\"tvSearchParams\":[\"q\",\"season\",\"ep\"]}', 1, 1)",
        )
        .bind(id)
        .execute(storage.pool())
        .await
        .unwrap();
    }

    async fn insert_source(storage: &Storage, id: [u8; 16], marker: u128) {
        let mut v1 = [0_u8; 20];
        v1[4..].copy_from_slice(&marker.to_be_bytes());
        let release = parse_release("Example.Movie.2024.1080p");
        sqlx::query(
            "INSERT INTO sporos_qbit_torrent (id, v1_hash, name, total_size,
             amount_left, progress_ppm, state, save_path, content_path, category,
             tags_json, is_complete, available, file_manifest_version, release_json,
             last_seen_generation, updated_at) VALUES (?, ?, 'Example.Movie.2024.1080p',
             1000, 0, 1000000, 'uploading', '/data', '/data/example', '', '[]',
             1, 1, 0, ?, 1, 1)",
        )
        .bind(id.as_slice())
        .bind(v1.as_slice())
        .bind(serde_json::to_string(&release).unwrap())
        .execute(storage.pool())
        .await
        .unwrap();
    }

    fn source_id(marker: u128) -> [u8; 16] {
        marker.to_be_bytes()
    }
}
