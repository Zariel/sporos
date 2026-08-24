use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use duroxide::OrchestrationContext;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sporos_matcher::{MatchRequest, Matcher, PureMatcher};
use sporos_model::{
    CandidateId, InfoHashes, LocalSourceFile, LocalSourceManifest, MatchDecision, MatchOutcome,
    ReleaseDescriptor, SourceId, SourceKind, TaskId, TorrentManifest,
};
use sqlx::{QueryBuilder, Row, Sqlite};
use thiserror::Error;

use crate::candidate::{CandidatePolicy, CandidateWorkflowInput, MATCHER_VERSION};
use crate::storage::Storage;
use crate::task_projection::{ProjectionOutcome, ProjectionUpdate};

pub const EVALUATE_ACTIVITY: &str = "EvaluateCandidate";
pub const SOURCE_COMPLETED_EVENT: &str = "SourceCompleted";
const MAX_PLAUSIBLE_SOURCES: i64 = 64;
const MAX_WAITERS_PER_COMPLETION: i64 = 1_024;
const RECONCILE_INTERVAL: Duration = Duration::from_secs(5 * 60);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum EvaluationResult {
    Waiting {
        deadline_ms: i64,
        source_ids: Vec<[u8; 16]>,
    },
    Terminal {
        state: String,
        reason_code: String,
        match_id: [u8; 16],
        plan_id: Option<[u8; 16]>,
    },
}

pub async fn workflow(context: OrchestrationContext, input: String) -> Result<String, String> {
    let _: CandidateWorkflowInput = serde_json::from_str(&input)
        .map_err(|error| format!("invalid candidate workflow input: {error}"))?;
    loop {
        let output = context
            .schedule_activity(EVALUATE_ACTIVITY, input.clone())
            .await?;
        let result: EvaluationResult = serde_json::from_str(&output)
            .map_err(|error| format!("invalid candidate evaluation result: {error}"))?;
        match result {
            EvaluationResult::Terminal {
                ref state,
                plan_id: Some(plan_id),
                ..
            } if state == "planned" => {
                return crate::injection::run(&context, &input, plan_id).await;
            }
            EvaluationResult::Terminal { .. } => return Ok(output),
            EvaluationResult::Waiting { deadline_ms, .. } => {
                let remaining = deadline_ms.saturating_sub(now_ms());
                if remaining <= 0 {
                    continue;
                }
                let timer = Duration::from_millis(
                    u64::try_from(remaining)
                        .unwrap_or(u64::MAX)
                        .min(RECONCILE_INTERVAL.as_millis() as u64),
                );
                let _ = context
                    .select2(
                        context.dequeue_event(SOURCE_COMPLETED_EVENT),
                        context.schedule_timer(timer),
                    )
                    .await;
            }
        }
    }
}

impl Storage {
    pub async fn waiting_candidate_instances(
        &self,
        source_id: [u8; 16],
    ) -> Result<Vec<String>, CandidateWorkflowError> {
        Ok(sqlx::query_scalar(
            "SELECT DISTINCT t.duroxide_instance_id
             FROM sporos_waiting_source w
             JOIN sporos_task t ON t.id = w.candidate_task_id
             WHERE w.source_id = ? AND t.terminal_at IS NULL
             ORDER BY t.duroxide_instance_id
             LIMIT ?",
        )
        .bind(source_id.as_slice())
        .bind(MAX_WAITERS_PER_COMPLETION)
        .fetch_all(self.pool())
        .await?)
    }

    pub async fn evaluate_candidate(
        &self,
        input: &CandidateWorkflowInput,
        now: i64,
    ) -> Result<EvaluationResult, CandidateWorkflowError> {
        let loaded = self.load_candidate(input).await?;
        let sources = self
            .candidate_sources(&loaded.release, &loaded.policy)
            .await?;
        let mut waiting = Vec::new();
        let mut available = Vec::new();
        for source in sources {
            if !source.complete {
                waiting.push(*source.manifest.id.as_bytes());
                continue;
            }
            if source.manifest_loaded || same_identity(&loaded.manifest, &source.manifest) {
                available.push(source.manifest);
            } else {
                self.request_manifest(source.manifest.id).await?;
                waiting.push(*source.manifest.id.as_bytes());
            }
        }

        let decision = PureMatcher.evaluate(&MatchRequest {
            candidate: &loaded.manifest,
            candidate_release: &loaded.release,
            sources: &available,
            policy: &loaded.policy.matching,
        });
        if decision.outcome == MatchOutcome::NoMatch && !waiting.is_empty() {
            let deadline = loaded.deadline()?;
            if now < deadline {
                waiting.sort_unstable();
                waiting.dedup();
                self.persist_waiting(input, &waiting, now).await?;
                self.project_candidate_task(
                    TaskId::from_bytes(input.task_id),
                    "waiting_for_source",
                    None,
                    false,
                    serde_json::json!({ "sourceCount": waiting.len(), "deadlineMs": deadline }),
                    now,
                )
                .await?;
                return Ok(EvaluationResult::Waiting {
                    deadline_ms: deadline,
                    source_ids: waiting,
                });
            }
            return self
                .persist_terminal(input, &loaded, rejected_source_timeout(&decision), now)
                .await;
        }

        self.persist_terminal(input, &loaded, decision, now).await
    }

    async fn load_candidate(
        &self,
        input: &CandidateWorkflowInput,
    ) -> Result<LoadedCandidate, CandidateWorkflowError> {
        let row = sqlx::query(
            "SELECT c.manifest_json, c.release_json, c.created_at, p.payload_json,
                    cp.trigger, cp.indexer_id, cp.indexer_name
             FROM sporos_candidate c
             JOIN sporos_candidate_task ct ON ct.candidate_id = c.id
             JOIN sporos_policy_snapshot p ON p.id = ct.policy_snapshot_id
             JOIN sporos_candidate_provenance cp ON cp.id = (
                 SELECT MIN(first_cp.id) FROM sporos_candidate_provenance first_cp
                 WHERE first_cp.candidate_id = c.id
             )
             WHERE c.id = ? AND ct.task_id = ? AND ct.policy_snapshot_id = ?",
        )
        .bind(input.candidate_id.as_slice())
        .bind(input.task_id.as_slice())
        .bind(input.policy_snapshot_id.as_slice())
        .fetch_optional(self.pool())
        .await?
        .ok_or(CandidateWorkflowError::MissingCandidate)?;
        let manifest_json = row
            .try_get::<Option<String>, _>("manifest_json")?
            .ok_or(CandidateWorkflowError::MissingManifest)?;
        Ok(LoadedCandidate {
            id: CandidateId::from_bytes(input.candidate_id),
            manifest: serde_json::from_str(&manifest_json)?,
            release: serde_json::from_str(&row.try_get::<String, _>("release_json")?)?,
            policy: serde_json::from_str(&row.try_get::<String, _>("payload_json")?)?,
            created_at: row.try_get("created_at")?,
            trigger: row.try_get("trigger")?,
            indexer_id: row.try_get("indexer_id")?,
            indexer_name: row.try_get("indexer_name")?,
        })
    }

    async fn candidate_sources(
        &self,
        release: &ReleaseDescriptor,
        policy: &CandidatePolicy,
    ) -> Result<Vec<LoadedSource>, CandidateWorkflowError> {
        let include_categories = serde_json::to_string(&policy.source_filters.include_categories)?;
        let exclude_categories = serde_json::to_string(&policy.source_filters.exclude_categories)?;
        let include_tags = serde_json::to_string(&policy.source_filters.include_tags)?;
        let exclude_tags = serde_json::to_string(&policy.source_filters.exclude_tags)?;
        let mut query = QueryBuilder::<Sqlite>::new(
            "SELECT id, v1_hash, v2_hash, release_json, is_complete,
                    file_manifest_version, file_manifest_state
             FROM sporos_qbit_torrent
             WHERE available = 1 AND normalized_title = ",
        );
        query.push_bind(release.primary_title.as_str());
        query.push(" AND (");
        query.push_bind(&include_categories);
        query.push(" = '[]' OR category IN (SELECT value FROM json_each(");
        query.push_bind(&include_categories);
        query.push("))) AND (");
        query.push_bind(&exclude_categories);
        query.push(" = '[]' OR category NOT IN (SELECT value FROM json_each(");
        query.push_bind(&exclude_categories);
        query.push("))) AND (");
        query.push_bind(&include_tags);
        query.push(
            " = '[]' OR EXISTS (
                 SELECT 1 FROM json_each(tags_json)
                 WHERE value IN (SELECT value FROM json_each(",
        );
        query.push_bind(&include_tags);
        query.push(")))) AND (");
        query.push_bind(&exclude_tags);
        query.push(
            " = '[]' OR NOT EXISTS (
                 SELECT 1 FROM json_each(tags_json)
                 WHERE value IN (SELECT value FROM json_each(",
        );
        query.push_bind(&exclude_tags);
        query.push("))))");
        if policy.source_filters.exclude_sporos_managed {
            query.push(
                " AND NOT EXISTS (
                    SELECT 1 FROM json_each(tags_json)
                    WHERE value = 'sporos' OR value LIKE 'sporos:%'
                 )",
            );
        }
        query.push(" ORDER BY is_complete DESC, id LIMIT ");
        query.push_bind(MAX_PLAUSIBLE_SOURCES);

        let rows = query.build().fetch_all(self.pool()).await?;
        let mut sources = Vec::with_capacity(rows.len());
        for row in rows {
            let id = SourceId::from_bytes(bytes_16(row.try_get("id")?, "source ID")?);
            let manifest_version = row.try_get::<i64, _>("file_manifest_version")?;
            let state = row.try_get::<String, _>("file_manifest_state")?;
            let manifest_loaded = manifest_version > 0 && state == "loaded";
            let files = if manifest_loaded {
                self.source_files(id, manifest_version).await?
            } else {
                Vec::new()
            };
            sources.push(LoadedSource {
                complete: row.try_get::<i64, _>("is_complete")? == 1,
                manifest_loaded,
                manifest: LocalSourceManifest {
                    id,
                    kind: SourceKind::QbittorrentTorrent,
                    release: serde_json::from_str(&row.try_get::<String, _>("release_json")?)?,
                    hashes: InfoHashes {
                        v1: optional_bytes(row.try_get("v1_hash")?, "v1 hash")?,
                        v2: optional_bytes(row.try_get("v2_hash")?, "v2 hash")?,
                    },
                    files,
                    available: true,
                },
            });
        }
        Ok(sources)
    }

    async fn source_files(
        &self,
        source_id: SourceId,
        manifest_version: i64,
    ) -> Result<Vec<LocalSourceFile>, CandidateWorkflowError> {
        let rows = sqlx::query(
            "SELECT id, display_path, size, device, inode
             FROM sporos_source_file
             WHERE source_id = ? AND manifest_version = ? AND available = 1
             ORDER BY ordinal LIMIT 100000",
        )
        .bind(source_id.as_bytes().as_slice())
        .bind(manifest_version)
        .fetch_all(self.pool())
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(LocalSourceFile {
                    id: to_u64(row.try_get("id")?, "source file ID")?,
                    path: row.try_get("display_path")?,
                    size: to_u64(row.try_get("size")?, "source file size")?,
                    device_id: optional_u64(row.try_get("device")?, "source device")?,
                    inode: optional_u64(row.try_get("inode")?, "source inode")?,
                })
            })
            .collect()
    }

    async fn request_manifest(&self, source_id: SourceId) -> Result<(), CandidateWorkflowError> {
        sqlx::query(
            "UPDATE sporos_qbit_torrent SET file_manifest_state = 'stale'
             WHERE id = ? AND file_manifest_state = 'unloaded'",
        )
        .bind(source_id.as_bytes().as_slice())
        .execute(self.pool())
        .await?;
        Ok(())
    }

    async fn persist_waiting(
        &self,
        input: &CandidateWorkflowInput,
        source_ids: &[[u8; 16]],
        now: i64,
    ) -> Result<(), CandidateWorkflowError> {
        let mut transaction = self.pool().begin().await?;
        sqlx::query("DELETE FROM sporos_waiting_source WHERE candidate_task_id = ?")
            .bind(input.task_id.as_slice())
            .execute(&mut *transaction)
            .await?;
        for source_id in source_ids {
            sqlx::query(
                "INSERT INTO sporos_waiting_source (candidate_task_id, source_id, created_at)
                 VALUES (?, ?, ?)",
            )
            .bind(input.task_id.as_slice())
            .bind(source_id.as_slice())
            .bind(now)
            .execute(&mut *transaction)
            .await?;
        }
        sqlx::query(
            "UPDATE sporos_candidate SET state = 'waiting_for_source', updated_at = ? WHERE id = ?",
        )
        .bind(now)
        .bind(input.candidate_id.as_slice())
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn persist_terminal(
        &self,
        input: &CandidateWorkflowInput,
        loaded: &LoadedCandidate,
        decision: MatchDecision,
        now: i64,
    ) -> Result<EvaluationResult, CandidateWorkflowError> {
        let decision_json = serde_json::to_string(&decision)?;
        let decision_digest: [u8; 32] = Sha256::digest(decision_json.as_bytes()).into();
        let match_digest: [u8; 32] = Sha256::digest(
            [
                b"candidate-match-v1:".as_slice(),
                loaded.id.as_bytes(),
                input.policy_snapshot_id.as_slice(),
                decision_digest.as_slice(),
            ]
            .concat(),
        )
        .into();
        let match_id = first_16(&match_digest);
        let state = terminal_state(&decision, loaded.policy.injection.dry_run);
        let reason_code = enum_text(&decision.reason)?;
        let mut transaction = self.pool().begin().await?;
        sqlx::query("DELETE FROM sporos_waiting_source WHERE candidate_task_id = ?")
            .bind(input.task_id.as_slice())
            .execute(&mut *transaction)
            .await?;
        sqlx::query(
            "INSERT INTO sporos_match (
                id, candidate_id, matcher_version, mode, outcome, reason_code,
                mapped_bytes, missing_bytes, present_ratio_ppm, evidence_json,
                decision_digest, created_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(id) DO NOTHING",
        )
        .bind(match_id.as_slice())
        .bind(input.candidate_id.as_slice())
        .bind(MATCHER_VERSION)
        .bind(decision.mode.map(|mode| enum_text(&mode)).transpose()?)
        .bind(enum_text(&decision.outcome)?)
        .bind(&reason_code)
        .bind(to_i64(decision.mapped_bytes, "mapped bytes")?)
        .bind(to_i64(decision.missing_bytes, "missing bytes")?)
        .bind(i64::from(decision.present_ratio.as_ppm()))
        .bind(serde_json::to_string(&decision.evidence)?)
        .bind(decision_digest.as_slice())
        .bind(now)
        .execute(&mut *transaction)
        .await?;
        let stored_digest = sqlx::query_scalar::<_, Vec<u8>>(
            "SELECT decision_digest FROM sporos_match WHERE id = ?",
        )
        .bind(match_id.as_slice())
        .fetch_one(&mut *transaction)
        .await?;
        if stored_digest.as_slice() != decision_digest {
            return Err(CandidateWorkflowError::MatchCollision);
        }
        for mapping in &decision.mappings {
            sqlx::query(
                "INSERT INTO sporos_file_mapping (
                    match_id, candidate_ordinal, source_id, source_file_id,
                    candidate_path, size, score
                 ) VALUES (?, ?, ?, ?, ?, ?, ?)
                 ON CONFLICT(match_id, candidate_ordinal) DO NOTHING",
            )
            .bind(match_id.as_slice())
            .bind(i64::from(mapping.candidate_ordinal))
            .bind(mapping.source_id.as_bytes().as_slice())
            .bind(to_i64(mapping.source_file_id, "source file ID")?)
            .bind(mapping.candidate_path.as_bytes())
            .bind(to_i64(mapping.size, "mapping size")?)
            .bind(i64::from(mapping.score))
            .execute(&mut *transaction)
            .await?;
        }
        let plan_id = if decision.outcome == MatchOutcome::Match {
            Some(persist_plan(&mut transaction, input, loaded, &decision, match_id, now).await?)
        } else {
            None
        };
        sqlx::query("UPDATE sporos_candidate SET state = ?, updated_at = ? WHERE id = ?")
            .bind(state)
            .bind(now)
            .bind(input.candidate_id.as_slice())
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;

        self.project_candidate_task(
            TaskId::from_bytes(input.task_id),
            state,
            Some(reason_code.clone()),
            state != "planned",
            serde_json::json!({
                "matchId": encode_hex(&match_id),
                "planId": plan_id.map(|id| encode_hex(&id)),
            }),
            now,
        )
        .await?;
        Ok(EvaluationResult::Terminal {
            state: state.to_owned(),
            reason_code,
            match_id,
            plan_id,
        })
    }

    pub(crate) async fn project_candidate_task(
        &self,
        task_id: TaskId,
        state: &str,
        reason_code: Option<String>,
        terminal: bool,
        detail: serde_json::Value,
        now: i64,
    ) -> Result<(), CandidateWorkflowError> {
        let row = sqlx::query(
            "SELECT state, projection_generation, terminal_at FROM sporos_task WHERE id = ?",
        )
        .bind(task_id.as_bytes().as_slice())
        .fetch_optional(self.pool())
        .await?;
        let Some(row) = row else {
            return Ok(());
        };
        if row.try_get::<String, _>("state")? == state
            || row.try_get::<Option<i64>, _>("terminal_at")?.is_some()
        {
            return Ok(());
        }
        let generation = to_u64(row.try_get("projection_generation")?, "task generation")?;
        let outcome = self
            .project_task(&ProjectionUpdate {
                task_id,
                expected_generation: generation,
                state: state.to_owned(),
                reason_code,
                execution_id: None,
                observed_retry_count: 0,
                detail_json: Some(serde_json::to_string(&detail)?),
                occurred_at: now,
                terminal,
            })
            .await?;
        if matches!(outcome, ProjectionOutcome::Missing) {
            return Err(CandidateWorkflowError::MissingTask);
        }
        Ok(())
    }
}

async fn persist_plan(
    transaction: &mut sqlx::Transaction<'_, Sqlite>,
    input: &CandidateWorkflowInput,
    loaded: &LoadedCandidate,
    decision: &MatchDecision,
    match_id: [u8; 16],
    now: i64,
) -> Result<[u8; 16], CandidateWorkflowError> {
    let candidate_key = format!("cand_{}", encode_hex(loaded.id.as_bytes()));
    let shard = encode_hex(&loaded.id.as_bytes()[..2]);
    let namespace_local = Path::new(&loaded.policy.namespace_local_root)
        .join(&shard)
        .join(&candidate_key)
        .to_string_lossy()
        .into_owned();
    let save_path_remote = Path::new(&loaded.policy.save_path_remote_root)
        .join(shard)
        .join(candidate_key)
        .to_string_lossy()
        .into_owned();
    let source = source_template_data(transaction, &decision.source_ids).await?;
    let mut context = crate::template::TemplateContext::default();
    context.insert("trigger", &loaded.trigger);
    context.insert(
        "indexer_id",
        loaded
            .indexer_id
            .map(|id| id.to_string())
            .unwrap_or_default(),
    );
    context.insert(
        "indexer_name",
        loaded.indexer_name.as_deref().unwrap_or_default(),
    );
    context.insert(
        "indexer_slug",
        crate::template::slug(loaded.indexer_name.as_deref().unwrap_or_default()),
    );
    context.insert(
        "match_mode",
        decision
            .mode
            .map(|mode| enum_text(&mode))
            .transpose()?
            .unwrap_or_default(),
    );
    context.insert("source_category", &source.category);
    context.insert("source_kind", &source.kind);
    context.insert("video_kind", enum_text(&loaded.release.kind)?);
    context.insert(
        "year",
        loaded
            .release
            .year
            .map(|value| value.to_string())
            .unwrap_or_default(),
    );
    context.insert(
        "season",
        loaded
            .release
            .season
            .map(|value| value.to_string())
            .unwrap_or_default(),
    );
    context.insert(
        "episode",
        loaded
            .release
            .episode
            .map(|value| value.to_string())
            .unwrap_or_default(),
    );
    let category = if loaded.policy.injection.inherit_source_category && !source.category.is_empty()
    {
        source.category
    } else {
        context.render_category(&loaded.policy.injection.category_template)?
    };
    let inherited_tags = loaded
        .policy
        .injection
        .inherit_source_tags
        .then_some(source.tags)
        .into_iter()
        .flatten();
    let tags = context.render_tags(&loaded.policy.injection.tag_templates, inherited_tags)?;
    let tags_json = serde_json::to_string(&tags)?;
    let resume_policy_json = serde_json::to_string(&loaded.policy.injection.resume)?;
    let material = serde_json::to_vec(&serde_json::json!({
        "matchId": encode_hex(&match_id),
        "candidateId": encode_hex(&input.candidate_id),
        "namespaceLocal": namespace_local,
        "savePathRemote": save_path_remote,
        "category": category,
        "tags": tags,
        "resumePolicy": &resume_policy_json,
        "mappings": decision.mappings,
    }))?;
    let plan_digest: [u8; 32] = Sha256::digest(material).into();
    let plan_id = first_16(&plan_digest);
    sqlx::query(
        "INSERT INTO sporos_injection_plan (
            id, match_id, candidate_id, namespace_local, save_path_remote,
            category, tags_json, resume_policy_json, plan_digest, state, created_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(id) DO NOTHING",
    )
    .bind(plan_id.as_slice())
    .bind(match_id.as_slice())
    .bind(input.candidate_id.as_slice())
    .bind(namespace_local)
    .bind(save_path_remote)
    .bind(category)
    .bind(tags_json)
    .bind(resume_policy_json)
    .bind(plan_digest.as_slice())
    .bind(if loaded.policy.injection.dry_run {
        "dry_run_complete"
    } else {
        "planned"
    })
    .bind(now)
    .execute(&mut **transaction)
    .await?;
    let stored_digest = sqlx::query_scalar::<_, Vec<u8>>(
        "SELECT plan_digest FROM sporos_injection_plan WHERE id = ?",
    )
    .bind(plan_id.as_slice())
    .fetch_one(&mut **transaction)
    .await?;
    if stored_digest.as_slice() != plan_digest {
        return Err(CandidateWorkflowError::PlanCollision);
    }
    Ok(plan_id)
}

async fn source_template_data(
    transaction: &mut sqlx::Transaction<'_, Sqlite>,
    source_ids: &[SourceId],
) -> Result<SourceTemplateData, CandidateWorkflowError> {
    let mut ids = source_ids.to_vec();
    ids.sort_unstable_by_key(|id| *id.as_bytes());
    let mut category = String::new();
    let mut tags = Vec::new();
    for source_id in ids {
        let row = sqlx::query("SELECT category, tags_json FROM sporos_qbit_torrent WHERE id = ?")
            .bind(source_id.as_bytes().as_slice())
            .fetch_optional(&mut **transaction)
            .await?;
        let Some(row) = row else {
            continue;
        };
        if category.is_empty() {
            category = row.try_get("category")?;
        }
        tags.extend(serde_json::from_str::<Vec<String>>(
            &row.try_get::<String, _>("tags_json")?,
        )?);
    }
    Ok(SourceTemplateData {
        category,
        tags,
        kind: "qbittorrent_torrent".to_owned(),
    })
}

struct SourceTemplateData {
    category: String,
    tags: Vec<String>,
    kind: String,
}

struct LoadedCandidate {
    id: CandidateId,
    manifest: TorrentManifest,
    release: ReleaseDescriptor,
    policy: CandidatePolicy,
    created_at: i64,
    trigger: String,
    indexer_id: Option<i64>,
    indexer_name: Option<String>,
}

impl LoadedCandidate {
    fn deadline(&self) -> Result<i64, CandidateWorkflowError> {
        Ok(self.created_at.saturating_add(to_i64(
            self.policy.pending_source_timeout_ms,
            "pending source timeout",
        )?))
    }
}

struct LoadedSource {
    complete: bool,
    manifest_loaded: bool,
    manifest: LocalSourceManifest,
}

fn same_identity(candidate: &TorrentManifest, source: &LocalSourceManifest) -> bool {
    (candidate.hashes.v1.is_some() && candidate.hashes.v1 == source.hashes.v1)
        || (candidate.hashes.v2.is_some() && candidate.hashes.v2 == source.hashes.v2)
}

fn rejected_source_timeout(decision: &MatchDecision) -> MatchDecision {
    let mut decision = decision.clone();
    decision.outcome = MatchOutcome::Rejected;
    decision.reason = sporos_model::MatchReason::NoPlausibleSource;
    decision
}

fn terminal_state(decision: &MatchDecision, dry_run: bool) -> &'static str {
    match decision.outcome {
        MatchOutcome::Match if dry_run => "dry_run_complete",
        MatchOutcome::Match => "planned",
        MatchOutcome::AlreadyPresent => "already_present",
        MatchOutcome::NoMatch | MatchOutcome::Rejected => "rejected",
    }
}

fn enum_text(value: &impl Serialize) -> Result<String, CandidateWorkflowError> {
    let encoded = serde_json::to_string(value)?;
    Ok(encoded.trim_matches('"').to_owned())
}

fn optional_bytes<const N: usize>(
    value: Option<Vec<u8>>,
    field: &'static str,
) -> Result<Option<[u8; N]>, CandidateWorkflowError> {
    value.map(|value| bytes(value, field)).transpose()
}

fn bytes_16(value: Vec<u8>, field: &'static str) -> Result<[u8; 16], CandidateWorkflowError> {
    bytes(value, field)
}

fn bytes<const N: usize>(
    value: Vec<u8>,
    field: &'static str,
) -> Result<[u8; N], CandidateWorkflowError> {
    value
        .try_into()
        .map_err(|_| CandidateWorkflowError::StoredRange(field))
}

fn optional_u64(
    value: Option<i64>,
    field: &'static str,
) -> Result<Option<u64>, CandidateWorkflowError> {
    value.map(|value| to_u64(value, field)).transpose()
}

fn to_u64(value: i64, field: &'static str) -> Result<u64, CandidateWorkflowError> {
    value
        .try_into()
        .map_err(|_| CandidateWorkflowError::StoredRange(field))
}

fn to_i64(value: u64, field: &'static str) -> Result<i64, CandidateWorkflowError> {
    value
        .try_into()
        .map_err(|_| CandidateWorkflowError::Range(field))
}

fn first_16(hash: &[u8; 32]) -> [u8; 16] {
    let mut id = [0; 16];
    id.copy_from_slice(&hash[..16]);
    id
}

fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn now_ms() -> i64 {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO);
    i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX)
}

#[derive(Debug, Error)]
pub enum CandidateWorkflowError {
    #[error("candidate workflow database operation failed")]
    Database(#[from] sqlx::Error),
    #[error("candidate workflow data is invalid")]
    Json(#[from] serde_json::Error),
    #[error("candidate task projection failed")]
    Projection(#[from] crate::task_projection::ProjectionError),
    #[error("candidate workflow input does not refer to accepted work")]
    MissingCandidate,
    #[error("candidate manifest is missing")]
    MissingManifest,
    #[error("candidate task projection is missing")]
    MissingTask,
    #[error("{0} is outside the supported range")]
    Range(&'static str),
    #[error("stored {0} is outside the supported range")]
    StoredRange(&'static str),
    #[error("match identity refers to a different decision")]
    MatchCollision,
    #[error("plan identity refers to different content")]
    PlanCollision,
    #[error("candidate template is invalid")]
    Template,
}

impl From<crate::template::TemplateError> for CandidateWorkflowError {
    fn from(_: crate::template::TemplateError) -> Self {
        Self::Template
    }
}

#[cfg(test)]
mod tests {
    use std::process::Command;
    use std::sync::Arc;

    use duroxide::runtime::Runtime;
    use duroxide::{Client, OrchestrationStatus};
    use tempfile::TempDir;

    use super::*;
    use crate::candidate::{CandidateIngress, CandidateSubmission};
    use crate::config::{Injection, Matching, Paths, SourceFilters};
    use crate::inventory::{InventoryChange, InventoryDelta, InventoryFile};
    use crate::outbox::OutboxDispatcher;

    const CRASH_PROBE_DIRECTORY: &str = "SPOROS_CANDIDATE_CRASH_PROBE_DIRECTORY";

    #[tokio::test]
    async fn dry_run_persists_a_complete_plan_without_external_mutation() {
        let directory = TempDir::new().expect("temporary directory");
        let storage = open(&directory).await;
        project_source(&storage, true, 13, 1).await;
        let input = accept(&storage, directory.path(), 10).await;

        let result = storage
            .evaluate_candidate(&input, 20)
            .await
            .expect("evaluate candidate");

        assert!(matches!(
            result,
            EvaluationResult::Terminal { ref state, plan_id: Some(_), .. }
                if state == "dry_run_complete"
        ));
        assert_eq!(count(&storage, "sporos_match").await, 1);
        assert_eq!(count(&storage, "sporos_file_mapping").await, 1);
        assert_eq!(count(&storage, "sporos_injection_plan").await, 1);
        assert!(!directory.path().join("links").exists());
        let plan = sqlx::query(
            "SELECT namespace_local, save_path_remote, state FROM sporos_injection_plan",
        )
        .fetch_one(storage.pool())
        .await
        .unwrap();
        assert!(
            plan.get::<String, _>("namespace_local")
                .starts_with(&format!("{}/links/", directory.path().display()))
        );
        assert!(
            plan.get::<String, _>("save_path_remote")
                .starts_with("/qbit-links/")
        );
        assert_eq!(plan.get::<String, _>("state"), "dry_run_complete");
    }

    #[tokio::test]
    async fn plan_renders_restricted_category_and_tag_templates() {
        let directory = TempDir::new().expect("temporary directory");
        let storage = open(&directory).await;
        project_source(&storage, true, 13, 1).await;
        let input = accept_with(
            &storage,
            directory.path(),
            10,
            Injection {
                dry_run: true,
                category_template: "sporos/{{ indexer_slug }}".to_owned(),
                tag_templates: vec![
                    "sporos:{{ trigger }}".to_owned(),
                    "sporos:{{ match_mode }}".to_owned(),
                ],
                ..Injection::default()
            },
        )
        .await;

        storage.evaluate_candidate(&input, 20).await.unwrap();

        let row = sqlx::query("SELECT category, tags_json FROM sporos_injection_plan")
            .fetch_one(storage.pool())
            .await
            .unwrap();
        assert_eq!(row.get::<String, _>("category"), "sporos/fixture");
        assert_eq!(
            serde_json::from_str::<Vec<String>>(&row.get::<String, _>("tags_json")).unwrap(),
            ["sporos:autobrr", "sporos:strict"]
        );
    }

    #[tokio::test]
    async fn full_metadata_can_reject_after_a_plausible_title() {
        let directory = TempDir::new().expect("temporary directory");
        let storage = open(&directory).await;
        project_source(&storage, true, 12, 1).await;
        let input = accept(&storage, directory.path(), 10).await;

        let result = storage.evaluate_candidate(&input, 20).await.unwrap();

        assert!(matches!(
            result,
            EvaluationResult::Terminal { ref state, plan_id: None, .. } if state == "rejected"
        ));
        assert_eq!(count(&storage, "sporos_injection_plan").await, 0);
    }

    #[tokio::test]
    async fn waits_for_an_incomplete_source_then_re_evaluates_it() {
        let directory = TempDir::new().expect("temporary directory");
        let storage = open(&directory).await;
        project_source(&storage, false, 13, 1).await;
        let input = accept(&storage, directory.path(), 10).await;

        let waiting = storage.evaluate_candidate(&input, 20).await.unwrap();
        assert!(matches!(waiting, EvaluationResult::Waiting { .. }));
        assert_eq!(count(&storage, "sporos_waiting_source").await, 1);

        project_source(&storage, true, 13, 2).await;
        let terminal = storage.evaluate_candidate(&input, 30).await.unwrap();
        assert!(matches!(
            terminal,
            EvaluationResult::Terminal { ref state, plan_id: Some(_), .. }
                if state == "dry_run_complete"
        ));
        assert_eq!(count(&storage, "sporos_waiting_source").await, 0);
    }

    #[tokio::test]
    async fn completion_event_wakes_the_durable_workflow() {
        let directory = TempDir::new().expect("temporary directory");
        let storage = Arc::new(open(&directory).await);
        project_source(&storage, false, 13, 1).await;
        let input = accept(&storage, directory.path(), now_ms()).await;
        let instance = sqlx::query_scalar::<_, String>(
            "SELECT duroxide_instance_id FROM sporos_task WHERE id = ?",
        )
        .bind(input.task_id.as_slice())
        .fetch_one(storage.pool())
        .await
        .unwrap();
        let provider = storage.duroxide_provider();
        let client = Client::new(provider.clone());
        let (activities, orchestrations) = crate::engine::registries(Arc::clone(&storage), None);
        let runtime = Runtime::start_with_store(provider, activities, orchestrations).await;
        OutboxDispatcher::new(&storage, client.clone(), 1)
            .run_once(now_ms())
            .await
            .expect("dispatch candidate workflow");
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let subscription = sqlx::query_scalar::<_, i64>(
                    "SELECT count(*) FROM history
                     WHERE instance_id = ? AND event_type = 'ExternalSubscribedPersistent'",
                )
                .bind(&instance)
                .fetch_one(storage.pool())
                .await
                .unwrap();
                if count(&storage, "sporos_waiting_source").await == 1 && subscription == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("candidate did not begin waiting");

        project_source(&storage, true, 13, 2).await;
        let source_id = sqlx::query_scalar::<_, Vec<u8>>(
            "SELECT id FROM sporos_qbit_torrent WHERE qbit_id = ?",
        )
        .bind("b".repeat(40))
        .fetch_one(storage.pool())
        .await
        .unwrap();
        let instances = storage
            .waiting_candidate_instances(source_id.try_into().unwrap())
            .await
            .unwrap();
        assert_eq!(instances.len(), 1);
        assert_eq!(instances[0], instance);
        client
            .enqueue_event(&instance, SOURCE_COMPLETED_EVENT, "{}")
            .await
            .expect("signal source completion");

        let status = client
            .wait_for_orchestration(&instance, Duration::from_secs(5))
            .await
            .expect("wait for candidate workflow");
        assert!(matches!(status, OrchestrationStatus::Completed { .. }));
        let task_state =
            sqlx::query_scalar::<_, String>("SELECT state FROM sporos_task WHERE id = ?")
                .bind(input.task_id.as_slice())
                .fetch_one(storage.pool())
                .await
                .unwrap();
        assert_eq!(task_state, "dry_run_complete");
        runtime.shutdown(Some(100)).await;
    }

    #[tokio::test]
    async fn acknowledged_candidate_survives_process_kill() {
        let directory = TempDir::new().expect("temporary directory");
        let marker = directory.path().join("candidate-acknowledged");
        let mut child = Command::new(std::env::current_exe().expect("locate test executable"))
            .args([
                "--exact",
                "candidate_workflow::tests::candidate_crash_probe",
                "--nocapture",
            ])
            .env(CRASH_PROBE_DIRECTORY, directory.path())
            .spawn()
            .expect("start candidate crash probe");
        tokio::time::timeout(Duration::from_secs(5), async {
            while !marker.exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("candidate was not acknowledged");
        child.kill().expect("kill candidate process");
        assert!(!child.wait().expect("wait for candidate process").success());

        let storage = Arc::new(open(&directory).await);
        let row = sqlx::query(
            "SELECT t.id, t.duroxide_instance_id
             FROM sporos_task t JOIN sporos_outbox o ON o.task_id = t.id
             WHERE t.kind = 'process_candidate'",
        )
        .fetch_one(storage.pool())
        .await
        .expect("load acknowledged candidate task");
        let task_id: Vec<u8> = row.get("id");
        let instance: String = row.get("duroxide_instance_id");
        let provider = storage.duroxide_provider();
        let client = Client::new(provider.clone());
        let (activities, orchestrations) = crate::engine::registries(Arc::clone(&storage), None);
        let runtime = Runtime::start_with_store(provider, activities, orchestrations).await;
        OutboxDispatcher::new(&storage, client.clone(), 1)
            .run_once(now_ms())
            .await
            .expect("dispatch recovered candidate");

        let status = client
            .wait_for_orchestration(&instance, Duration::from_secs(5))
            .await
            .expect("wait for recovered candidate");
        assert!(matches!(status, OrchestrationStatus::Completed { .. }));
        let state = sqlx::query_scalar::<_, String>("SELECT state FROM sporos_task WHERE id = ?")
            .bind(task_id)
            .fetch_one(storage.pool())
            .await
            .unwrap();
        assert_eq!(state, "dry_run_complete");
        runtime.shutdown(Some(100)).await;
    }

    #[tokio::test]
    async fn candidate_crash_probe() {
        let Some(directory) = std::env::var_os(CRASH_PROBE_DIRECTORY) else {
            return;
        };
        let root = Path::new(&directory);
        let storage = open_path(root).await;
        project_source(&storage, true, 13, 1).await;
        let _ = accept(&storage, root, now_ms()).await;
        std::fs::write(root.join("candidate-acknowledged"), b"acknowledged")
            .expect("write acknowledgement marker");
        std::future::pending::<()>().await;
    }

    async fn accept(
        storage: &Storage,
        directory: &Path,
        received_at: i64,
    ) -> CandidateWorkflowInput {
        accept_with(
            storage,
            directory,
            received_at,
            Injection {
                dry_run: true,
                ..Injection::default()
            },
        )
        .await
    }

    async fn accept_with(
        storage: &Storage,
        directory: &Path,
        received_at: i64,
        injection: Injection,
    ) -> CandidateWorkflowInput {
        let ingress = CandidateIngress::new(
            Matching::default(),
            SourceFilters::default(),
            injection,
            Paths {
                link_root: directory.join("links"),
                rewrite: vec![crate::config::PathRewrite {
                    name: "qbit".to_owned(),
                    remote: "/qbit-links".into(),
                    local: directory.join("links"),
                    services: vec!["qbittorrent".to_owned()],
                }],
            },
        );
        let bytes = format!(
            "d4:infod6:lengthi13e4:name18:Example.Movie.202412:piece lengthi16384e6:pieces20:{}ee",
            "a".repeat(20)
        )
        .into_bytes();
        let accepted = ingress
            .accept(
                storage,
                CandidateSubmission {
                    bytes,
                    announcement_name: Some("Example.Movie.2024.1080p".to_owned()),
                    indexer: Some("fixture".to_owned()),
                    category: None,
                    tags: Vec::new(),
                    request_id: "request".to_owned(),
                    dry_run: false,
                    received_at,
                },
            )
            .await
            .expect("accept candidate");
        let input = sqlx::query_scalar::<_, String>(
            "SELECT input_json FROM sporos_outbox WHERE task_id = ?",
        )
        .bind(accepted.task_id.as_bytes().as_slice())
        .fetch_one(storage.pool())
        .await
        .unwrap();
        serde_json::from_str(&input).unwrap()
    }

    async fn project_source(storage: &Storage, complete: bool, size: u64, generation: u64) {
        let qbit_id = "b".repeat(40);
        storage
            .project_qbit_batch(
                &[InventoryChange::Upsert {
                    qbit_id,
                    delta: Box::new(InventoryDelta {
                        infohash_v1: Some("b".repeat(40)),
                        name: Some("Example.Movie.2024.1080p".to_owned()),
                        total_size: Some(size),
                        amount_left: Some(if complete { 0 } else { size }),
                        progress: Some(if complete { 1.0 } else { 0.0 }),
                        state: Some(if complete { "stoppedUP" } else { "stoppedDL" }.to_owned()),
                        save_path: Some("/downloads".to_owned()),
                        content_path: Some("/downloads/Example.Movie.2024".to_owned()),
                        category: Some(String::new()),
                        tags: Some(String::new()),
                        added_on: Some(1),
                        completion_on: complete.then_some(2),
                        ..InventoryDelta::default()
                    }),
                }],
                generation,
                false,
                i64::try_from(generation).unwrap(),
            )
            .await
            .unwrap();
        if complete {
            let source_id = sqlx::query_scalar::<_, Vec<u8>>(
                "SELECT id FROM sporos_qbit_torrent WHERE qbit_id = ?",
            )
            .bind("b".repeat(40))
            .fetch_one(storage.pool())
            .await
            .unwrap();
            let target = storage
                .prepare_qbit_manifest(source_id.try_into().unwrap())
                .await
                .unwrap();
            storage
                .project_qbit_files(
                    &target,
                    &[InventoryFile {
                        index: 0,
                        name: "Example.Movie.2024".to_owned(),
                        size,
                        progress: 1.0,
                    }],
                )
                .await
                .unwrap();
            storage.finish_qbit_manifest(&target, 1, 3).await.unwrap();
        }
    }

    async fn count(storage: &Storage, table: &str) -> i64 {
        let query = match table {
            "sporos_match" => "SELECT count(*) FROM sporos_match",
            "sporos_file_mapping" => "SELECT count(*) FROM sporos_file_mapping",
            "sporos_injection_plan" => "SELECT count(*) FROM sporos_injection_plan",
            "sporos_waiting_source" => "SELECT count(*) FROM sporos_waiting_source",
            _ => panic!("unknown test table"),
        };
        sqlx::query_scalar(query)
            .fetch_one(storage.pool())
            .await
            .unwrap()
    }

    async fn open(directory: &TempDir) -> Storage {
        open_path(directory.path()).await
    }

    async fn open_path(directory: &Path) -> Storage {
        Storage::open(directory.join("sporos.lock"), directory.join("sporos.db"))
            .await
            .expect("open storage")
    }
}
