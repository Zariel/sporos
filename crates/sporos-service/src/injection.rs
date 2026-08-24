use std::collections::BTreeSet;
use std::io::Cursor;
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use duroxide::{ActivityContext, OrchestrationContext};
use serde::{Deserialize, Serialize};
use sporos_model::{TaskId, TorrentManifest};
use sqlx::Row;
use thiserror::Error;

use crate::candidate::{CandidatePolicy, CandidateWorkflowInput};
use crate::config::{Paths, ResumePolicy, ThresholdCombination};
use crate::hardlink::{HardlinkMaterializer, MaterializeError, PlannedLink};
use crate::inventory::{InventoryParseError, parse_piece_states};
use crate::qbittorrent::{AddTorrentRequest, QbittorrentClient, TorrentState};
use crate::storage::Storage;

const PREPARE_ACTIVITY: &str = "PrepareInjection";
const MATERIALIZE_ACTIVITY: &str = "MaterializeInjectionLinks";
const RECHECK_ACTIVITY: &str = "RequestInjectionRecheck";
const OBSERVE_ACTIVITY: &str = "ObserveInjectionRecheck";
const POLL_DELAY: Duration = Duration::from_secs(2);
const MAX_FINAL_VERIFY_ATTEMPTS: i64 = 30;
const MAX_PIECES: usize = 20_000_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InjectionInput {
    task_id: [u8; 16],
    candidate_id: [u8; 16],
    policy_snapshot_id: [u8; 16],
    plan_id: [u8; 16],
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case")]
enum StepResult {
    Ready,
    Waiting { delay_ms: u64 },
    Terminal { state: String, reason_code: String },
}

pub(crate) async fn run(
    context: &OrchestrationContext,
    candidate: &str,
    plan_id: [u8; 16],
) -> Result<String, String> {
    let candidate: CandidateWorkflowInput = serde_json::from_str(candidate)
        .map_err(|error| format!("invalid injection candidate input: {error}"))?;
    let input = serde_json::to_string(&InjectionInput {
        task_id: candidate.task_id,
        candidate_id: candidate.candidate_id,
        policy_snapshot_id: candidate.policy_snapshot_id,
        plan_id,
    })
    .map_err(|error| format!("encode injection input: {error}"))?;

    loop {
        match step(context, PREPARE_ACTIVITY, &input).await? {
            StepResult::Ready => break,
            StepResult::Waiting { delay_ms } => {
                context
                    .schedule_timer(Duration::from_millis(delay_ms))
                    .await;
            }
            terminal @ StepResult::Terminal { .. } => return encode(terminal),
        }
    }
    match step(context, MATERIALIZE_ACTIVITY, &input).await? {
        StepResult::Ready => {}
        terminal @ StepResult::Terminal { .. } => return encode(terminal),
        StepResult::Waiting { .. } => return Err("materializer returned a wait state".to_owned()),
    }
    match step(context, RECHECK_ACTIVITY, &input).await? {
        StepResult::Ready | StepResult::Waiting { .. } => {}
        terminal @ StepResult::Terminal { .. } => return encode(terminal),
    }
    loop {
        match step(context, OBSERVE_ACTIVITY, &input).await? {
            StepResult::Ready => {
                return Err("observer returned a non-terminal ready state".to_owned());
            }
            StepResult::Waiting { delay_ms } => {
                context
                    .schedule_timer(Duration::from_millis(delay_ms))
                    .await;
            }
            terminal @ StepResult::Terminal { .. } => return encode(terminal),
        }
    }
}

async fn step(
    context: &OrchestrationContext,
    activity: &str,
    input: &str,
) -> Result<StepResult, String> {
    let output = context
        .schedule_activity_with_retry(
            activity,
            input.to_owned(),
            crate::engine::activity_retry_policy(),
        )
        .await?;
    serde_json::from_str(&output).map_err(|error| format!("invalid {activity} result: {error}"))
}

fn encode(result: StepResult) -> Result<String, String> {
    serde_json::to_string(&result).map_err(|error| format!("encode injection result: {error}"))
}

#[derive(Clone)]
pub(crate) struct InjectionExecutor {
    storage: Arc<Storage>,
    qbit: Option<QbittorrentClient>,
    candidate: Option<Arc<tokio::sync::Semaphore>>,
    filesystem: Option<Arc<tokio::sync::Semaphore>>,
}

impl InjectionExecutor {
    pub(crate) fn new(storage: Arc<Storage>, qbit: Option<QbittorrentClient>) -> Self {
        Self {
            storage,
            qbit,
            candidate: None,
            filesystem: None,
        }
    }

    pub(crate) fn with_limiters(
        mut self,
        candidate: Arc<tokio::sync::Semaphore>,
        filesystem: Arc<tokio::sync::Semaphore>,
    ) -> Self {
        self.candidate = Some(candidate);
        self.filesystem = Some(filesystem);
        self
    }

    pub(crate) fn register(
        self,
        builder: duroxide::runtime::registry::ActivityRegistryBuilder,
    ) -> duroxide::runtime::registry::ActivityRegistryBuilder {
        let prepare = self.clone();
        let materialize = self.clone();
        let recheck = self.clone();
        builder
            .register(PREPARE_ACTIVITY, move |_context: ActivityContext, input| {
                let executor = prepare.clone();
                async move { executor.activity(&input, Stage::Prepare).await }
            })
            .register(
                MATERIALIZE_ACTIVITY,
                move |_context: ActivityContext, input| {
                    let executor = materialize.clone();
                    async move { executor.activity(&input, Stage::Materialize).await }
                },
            )
            .register(RECHECK_ACTIVITY, move |_context: ActivityContext, input| {
                let executor = recheck.clone();
                async move { executor.activity(&input, Stage::Recheck).await }
            })
            .register(OBSERVE_ACTIVITY, move |_context: ActivityContext, input| {
                let executor = self.clone();
                async move { executor.activity(&input, Stage::Observe).await }
            })
    }

    async fn activity(&self, raw: &str, stage: Stage) -> Result<String, String> {
        let _permit = match &self.candidate {
            Some(limiter) => Some(crate::execution::permit(limiter).await),
            None => None,
        };
        let input: InjectionInput = serde_json::from_str(raw)
            .map_err(|error| format!("invalid injection activity input: {error}"))?;
        let result = match stage {
            Stage::Prepare => self.prepare(&input).await,
            Stage::Materialize => self.materialize(&input).await,
            Stage::Recheck => self.recheck(&input).await,
            Stage::Observe => self.observe(&input).await,
        }
        .map_err(|error| error.to_string())?;
        serde_json::to_string(&result).map_err(|error| format!("encode injection result: {error}"))
    }

    async fn prepare(&self, input: &InjectionInput) -> Result<StepResult, InjectionError> {
        let plan = self.storage.load_injection(input).await?;
        let Some(qbit) = &self.qbit else {
            return self.fail(input, "qbittorrent_unconfigured").await;
        };
        qbit.validate_contract().await?;
        ensure_namespace(&plan)?;
        let hashes = plan.hashes()?;
        let mut found = None;
        for hash in &hashes {
            if let Some(state) = qbit.torrent_state(hash).await? {
                if found
                    .as_ref()
                    .is_some_and(|existing: &TorrentState| existing.hash != state.hash)
                {
                    return self.fail(input, "qbittorrent_identity_conflict").await;
                }
                found = Some(state);
            }
        }
        if let Some(state) = found {
            if !same_save_path(&state.save_path, &plan.save_path_remote) || state.auto_tmm {
                return self.fail(input, "qbittorrent_identity_conflict").await;
            }
            if !state.is_stopped() {
                qbit.stop(&state.hash).await?;
                return Ok(waiting());
            }
            self.storage
                .record_injection(input, &plan, &state, "added_stopped", None, None)
                .await?;
            return Ok(StepResult::Ready);
        }
        qbit.add_stopped(AddTorrentRequest {
            torrent: plan.blob.clone(),
            filename: format!("cand_{}.torrent", encode_hex(&input.candidate_id)),
            save_path: plan.save_path_remote.clone(),
            category: (!plan.category.is_empty()).then_some(plan.category.clone()),
            tags: plan.tags.clone(),
        })
        .await?;
        Ok(waiting())
    }

    async fn materialize(&self, input: &InjectionInput) -> Result<StepResult, InjectionError> {
        let plan = self.storage.load_injection(input).await?;
        let Some(qbit) = &self.qbit else {
            return self.fail(input, "qbittorrent_unconfigured").await;
        };
        let Some(state) = qbit.torrent_state(&plan.hash()?).await? else {
            return self.fail(input, "injected_torrent_missing").await;
        };
        if !state.is_stopped()
            || state.auto_tmm
            || !same_save_path(&state.save_path, &plan.save_path_remote)
        {
            return self.fail(input, "qbittorrent_not_stopped").await;
        }
        let namespace = plan.namespace_relative()?;
        let links = match plan.planned_links() {
            Ok(links) => links,
            Err(InjectionError::UnmappedSourceRoot) => {
                return self.fail(input, "unmapped_source_root").await;
            }
            Err(error) => return Err(error),
        };
        let root = PathBuf::from(&plan.policy.namespace_local_root);
        let _permit = match &self.filesystem {
            Some(limiter) => Some(crate::execution::permit(limiter).await),
            None => None,
        };
        let results = tokio::task::spawn_blocking(move || {
            let materializer = HardlinkMaterializer::open(&root)?;
            let mut materialized = Vec::with_capacity(links.len());
            for link in links {
                match materializer.materialize(&namespace, &[link]) {
                    Ok(mut result) => materialized.append(&mut result),
                    Err(error) => return Ok((materialized, Err(error))),
                }
            }
            Ok::<_, MaterializeError>((materialized, Ok(())))
        })
        .await
        .map_err(InjectionError::MaterializerTask)?;
        let (results, outcome) = match results {
            Ok(results) => results,
            Err(error) => return self.fail(input, materialize_reason(&error)).await,
        };
        self.storage
            .persist_links(input, &plan, &results, outcome.is_ok())
            .await?;
        if let Err(error) = outcome {
            return self.fail(input, materialize_reason(&error)).await;
        }
        Ok(StepResult::Ready)
    }

    async fn recheck(&self, input: &InjectionInput) -> Result<StepResult, InjectionError> {
        let plan = self.storage.load_injection(input).await?;
        let Some(qbit) = &self.qbit else {
            return self.fail(input, "qbittorrent_unconfigured").await;
        };
        qbit.force_recheck(&plan.hash()?).await?;
        self.storage
            .set_injection_state(input, "recheck_requested")
            .await?;
        Ok(waiting())
    }

    async fn observe(&self, input: &InjectionInput) -> Result<StepResult, InjectionError> {
        let plan = self.storage.load_injection(input).await?;
        let Some(qbit) = &self.qbit else {
            return self.fail(input, "qbittorrent_unconfigured").await;
        };
        let hash = plan.hash()?;
        let Some(state) = qbit.torrent_state(&hash).await? else {
            return self.fail(input, "injected_torrent_missing").await;
        };
        if plan.injection_state.as_deref() == Some("finalizing") {
            return self.verify_final(input, &plan, qbit, &state).await;
        }
        match recheck_observation(plan.injection_state.as_deref(), &state) {
            RecheckObservation::Checking => {
                self.storage
                    .update_qbit_observation(input, &state, "checking")
                    .await?;
                return Ok(waiting());
            }
            RecheckObservation::Settling => {
                self.storage
                    .update_qbit_observation(input, &state, "recheck_settling")
                    .await?;
                return Ok(waiting());
            }
            RecheckObservation::Waiting => {
                let attempts = self.storage.bump_verification(input, &state).await?;
                if attempts >= MAX_FINAL_VERIFY_ATTEMPTS {
                    return self.fail(input, "ambiguous_qbittorrent_state").await;
                }
                return Ok(waiting());
            }
            RecheckObservation::Inspect => {}
        }
        let pieces = qbit.piece_states(&hash).await?;
        let integrity = inspect_pieces(&plan.manifest, &plan.mapped_ordinals, &pieces)?;
        let resume =
            integrity.integrity_safe && resume_allowed(&plan.resume, state.amount_left, &integrity);
        self.storage
            .record_resume_decision(input, &state, &integrity, resume)
            .await?;
        if resume {
            qbit.start(&hash).await?;
        } else {
            qbit.stop(&hash).await?;
        }
        Ok(waiting())
    }

    async fn verify_final(
        &self,
        input: &InjectionInput,
        plan: &InjectionPlan,
        qbit: &QbittorrentClient,
        state: &TorrentState,
    ) -> Result<StepResult, InjectionError> {
        let resume = plan.resume_decision.as_deref() == Some("resume");
        if (resume && state.is_started()) || (!resume && state.is_stopped()) {
            self.storage.finish_injection(input, state, resume).await?;
            return Ok(StepResult::Terminal {
                state: "completed".to_owned(),
                reason_code: if resume { "resumed" } else { "left_stopped" }.to_owned(),
            });
        }
        let attempts = self.storage.bump_verification(input, state).await?;
        if attempts >= MAX_FINAL_VERIFY_ATTEMPTS {
            return self.fail(input, "ambiguous_qbittorrent_state").await;
        }
        if resume {
            qbit.start(&plan.hash()?).await?;
        } else {
            qbit.stop(&plan.hash()?).await?;
        }
        Ok(waiting())
    }

    async fn fail(
        &self,
        input: &InjectionInput,
        reason: &str,
    ) -> Result<StepResult, InjectionError> {
        self.storage.fail_injection(input, reason, now_ms()).await?;
        Ok(StepResult::Terminal {
            state: "failed".to_owned(),
            reason_code: reason.to_owned(),
        })
    }
}

#[derive(Clone, Copy)]
enum Stage {
    Prepare,
    Materialize,
    Recheck,
    Observe,
}

fn waiting() -> StepResult {
    StepResult::Waiting {
        delay_ms: POLL_DELAY.as_millis() as u64,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecheckObservation {
    Checking,
    Settling,
    Waiting,
    Inspect,
}

fn recheck_observation(
    injection_state: Option<&str>,
    torrent: &TorrentState,
) -> RecheckObservation {
    if torrent.is_checking() {
        return RecheckObservation::Checking;
    }
    if matches!(torrent.state.as_str(), "queuedDL" | "queuedUP") {
        return RecheckObservation::Waiting;
    }
    match injection_state {
        Some("recheck_requested") => RecheckObservation::Settling,
        Some("recheck_settling" | "checking") => RecheckObservation::Inspect,
        _ => RecheckObservation::Waiting,
    }
}

struct InjectionPlan {
    blob: Vec<u8>,
    manifest: TorrentManifest,
    policy: CandidatePolicy,
    namespace_local: String,
    save_path_remote: String,
    category: String,
    tags: Vec<String>,
    resume: ResumePolicy,
    mappings: Vec<LinkRow>,
    mapped_ordinals: BTreeSet<u32>,
    injection_state: Option<String>,
    resume_decision: Option<String>,
}

struct LinkRow {
    candidate_ordinal: u32,
    candidate_path: String,
    source_root_remote: String,
    source_path: String,
    source_service: String,
    size: u64,
    device: Option<u64>,
    inode: Option<u64>,
    source_file_id: u64,
}

impl InjectionPlan {
    fn hashes(&self) -> Result<Vec<String>, InjectionError> {
        let mut hashes = Vec::with_capacity(2);
        if let Some(hash) = &self.manifest.hashes.v1 {
            hashes.push(encode_hex(hash));
        }
        if let Some(hash) = &self.manifest.hashes.v2 {
            hashes.push(encode_hex(hash));
        }
        if hashes.is_empty() {
            return Err(InjectionError::MissingHash);
        }
        Ok(hashes)
    }

    fn hash(&self) -> Result<String, InjectionError> {
        self.hashes()?
            .into_iter()
            .next()
            .ok_or(InjectionError::MissingHash)
    }

    fn namespace_relative(&self) -> Result<PathBuf, InjectionError> {
        Path::new(&self.namespace_local)
            .strip_prefix(&self.policy.namespace_local_root)
            .map(Path::to_owned)
            .map_err(|_| InjectionError::NamespaceOutsideRoot)
    }

    fn planned_links(&self) -> Result<Vec<PlannedLink>, InjectionError> {
        let paths = Paths {
            link_root: self.policy.namespace_local_root.clone().into(),
            rewrite: self.policy.path_rewrites.clone(),
        };
        self.mappings
            .iter()
            .map(|mapping| {
                let source_root = if mapping.source_service == "data" {
                    PathBuf::from(&mapping.source_root_remote)
                } else {
                    paths
                        .remote_to_local(
                            &mapping.source_service,
                            Path::new(&mapping.source_root_remote),
                        )
                        .ok_or(InjectionError::UnmappedSourceRoot)?
                };
                Ok(PlannedLink {
                    source_root,
                    source_relative: PathBuf::from(&mapping.source_path),
                    destination_relative: PathBuf::from(&mapping.candidate_path),
                    expected_size: mapping.size,
                    expected_device: mapping.device,
                    expected_inode: mapping.inode,
                })
            })
            .collect()
    }
}

impl Storage {
    async fn load_injection(
        &self,
        input: &InjectionInput,
    ) -> Result<InjectionPlan, InjectionError> {
        let row = sqlx::query(
            "SELECT b.data, c.manifest_json, p.payload_json, ip.namespace_local,
                    ip.save_path_remote, ip.category, ip.tags_json, ip.resume_policy_json,
                    i.state AS injection_state, i.resume_decision
             FROM sporos_injection_plan ip
             JOIN sporos_candidate c ON c.id = ip.candidate_id
             JOIN sporos_blob b ON b.sha256 = c.blob_sha256
             JOIN sporos_candidate_task ct ON ct.candidate_id = c.id
             JOIN sporos_policy_snapshot p ON p.id = ct.policy_snapshot_id
             LEFT JOIN sporos_injection i ON i.plan_id = ip.id
             WHERE ip.id = ? AND c.id = ? AND ct.task_id = ? AND p.id = ?",
        )
        .bind(input.plan_id.as_slice())
        .bind(input.candidate_id.as_slice())
        .bind(input.task_id.as_slice())
        .bind(input.policy_snapshot_id.as_slice())
        .fetch_optional(self.pool())
        .await?
        .ok_or(InjectionError::MissingPlan)?;
        let manifest: TorrentManifest = serde_json::from_str(
            &row.try_get::<Option<String>, _>("manifest_json")?
                .ok_or(InjectionError::MissingManifest)?,
        )?;
        let policy: CandidatePolicy =
            serde_json::from_str(&row.try_get::<String, _>("payload_json")?)?;
        let resume = serde_json::from_str(&row.try_get::<String, _>("resume_policy_json")?)?;
        let mappings = self.load_link_rows(input.plan_id).await?;
        let mapped_ordinals = mappings
            .iter()
            .map(|mapping| mapping.candidate_ordinal)
            .collect();
        Ok(InjectionPlan {
            blob: row.try_get("data")?,
            manifest,
            policy,
            namespace_local: row.try_get("namespace_local")?,
            save_path_remote: row.try_get("save_path_remote")?,
            category: row.try_get("category")?,
            tags: serde_json::from_str(&row.try_get::<String, _>("tags_json")?)?,
            resume,
            mappings,
            mapped_ordinals,
            injection_state: row.try_get("injection_state")?,
            resume_decision: row.try_get("resume_decision")?,
        })
    }

    async fn load_link_rows(&self, plan_id: [u8; 16]) -> Result<Vec<LinkRow>, InjectionError> {
        let rows = sqlx::query(
            "SELECT fm.candidate_ordinal, CAST(fm.candidate_path AS TEXT) AS candidate_path,
                    qt.save_path, sf.display_path, sf.local_path, sf.size, sf.device,
                    sf.inode, sf.id, qt.id AS qbit_id
             FROM sporos_injection_plan ip
             JOIN sporos_file_mapping fm ON fm.match_id = ip.match_id
             JOIN sporos_source_file sf ON sf.id = fm.source_file_id
             LEFT JOIN sporos_qbit_torrent qt ON qt.id = sf.source_id
             LEFT JOIN sporos_data_source ds ON ds.id = sf.source_id
             WHERE ip.id = ? AND (qt.id IS NOT NULL OR ds.id IS NOT NULL)
             ORDER BY fm.candidate_ordinal LIMIT 100000",
        )
        .bind(plan_id.as_slice())
        .fetch_all(self.pool())
        .await?;
        rows.into_iter()
            .map(|row| {
                let qbit_id: Option<Vec<u8>> = row.try_get("qbit_id")?;
                let (source_root_remote, source_path, source_service) = if qbit_id.is_some() {
                    (
                        row.try_get("save_path")?,
                        row.try_get("display_path")?,
                        "qbittorrent".to_owned(),
                    )
                } else {
                    let local_path: String = row
                        .try_get::<Option<String>, _>("local_path")?
                        .ok_or(InjectionError::InvalidSourcePath)?;
                    let path = Path::new(&local_path);
                    let parent = path
                        .parent()
                        .and_then(Path::to_str)
                        .ok_or(InjectionError::InvalidSourcePath)?;
                    let name = path
                        .file_name()
                        .and_then(|name| name.to_str())
                        .ok_or(InjectionError::InvalidSourcePath)?;
                    (parent.to_owned(), name.to_owned(), "data".to_owned())
                };
                Ok(LinkRow {
                    candidate_ordinal: to_u32(row.try_get("candidate_ordinal")?, "ordinal")?,
                    candidate_path: row.try_get("candidate_path")?,
                    source_root_remote,
                    source_path,
                    source_service,
                    size: to_u64(row.try_get("size")?, "size")?,
                    device: optional_u64(row.try_get("device")?, "device")?,
                    inode: optional_u64(row.try_get("inode")?, "inode")?,
                    source_file_id: to_u64(row.try_get("id")?, "source file ID")?,
                })
            })
            .collect()
    }

    async fn record_injection(
        &self,
        input: &InjectionInput,
        plan: &InjectionPlan,
        state: &TorrentState,
        injection_state: &str,
        integrity_safe: Option<bool>,
        resume_decision: Option<&str>,
    ) -> Result<(), InjectionError> {
        sqlx::query(
            "INSERT INTO sporos_injection (
                id, plan_id, v1_hash, v2_hash, qbit_state, amount_left, progress_ppm,
                integrity_safe, resume_decision, state, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(plan_id) DO UPDATE SET qbit_state = excluded.qbit_state,
                amount_left = excluded.amount_left, progress_ppm = excluded.progress_ppm,
                integrity_safe = excluded.integrity_safe,
                resume_decision = excluded.resume_decision, state = excluded.state,
                updated_at = excluded.updated_at",
        )
        .bind(input.plan_id.as_slice())
        .bind(input.plan_id.as_slice())
        .bind(plan.manifest.hashes.v1.map(|hash| hash.to_vec()))
        .bind(plan.manifest.hashes.v2.map(|hash| hash.to_vec()))
        .bind(&state.state)
        .bind(to_i64(state.amount_left, "amount left")?)
        .bind(progress_ppm(state.progress)?)
        .bind(integrity_safe.map(i64::from))
        .bind(resume_decision)
        .bind(injection_state)
        .bind(now_ms())
        .bind(now_ms())
        .execute(self.pool())
        .await?;
        sqlx::query("UPDATE sporos_injection_plan SET state = ? WHERE id = ?")
            .bind(injection_state)
            .bind(input.plan_id.as_slice())
            .execute(self.pool())
            .await?;
        Ok(())
    }

    async fn persist_links(
        &self,
        input: &InjectionInput,
        plan: &InjectionPlan,
        links: &[crate::hardlink::MaterializedLink],
        complete: bool,
    ) -> Result<(), InjectionError> {
        let mut transaction = self.pool().begin().await?;
        for (mapping, link) in plan.mappings.iter().zip(links) {
            sqlx::query(
                "INSERT INTO sporos_link (
                    plan_id, candidate_ordinal, source_file_id, destination_relative,
                    device, inode, size, state, created_at, verified_at
                 ) VALUES (?, ?, ?, ?, ?, ?, ?, 'verified', ?, ?)
                 ON CONFLICT(plan_id, candidate_ordinal) DO UPDATE SET
                    device = excluded.device, inode = excluded.inode, size = excluded.size,
                    state = 'verified', verified_at = excluded.verified_at",
            )
            .bind(input.plan_id.as_slice())
            .bind(i64::from(mapping.candidate_ordinal))
            .bind(to_i64(mapping.source_file_id, "source file ID")?)
            .bind(link.destination_relative.as_os_str().as_encoded_bytes())
            .bind(to_i64(link.device, "link device")?)
            .bind(to_i64(link.inode, "link inode")?)
            .bind(to_i64(link.size, "link size")?)
            .bind(now_ms())
            .bind(now_ms())
            .execute(&mut *transaction)
            .await?;
        }
        let state = if complete {
            "links_complete"
        } else {
            "links_partial"
        };
        sqlx::query("UPDATE sporos_injection SET state = ?, updated_at = ? WHERE plan_id = ?")
            .bind(state)
            .bind(now_ms())
            .bind(input.plan_id.as_slice())
            .execute(&mut *transaction)
            .await?;
        sqlx::query("UPDATE sporos_injection_plan SET state = ? WHERE id = ?")
            .bind(state)
            .bind(input.plan_id.as_slice())
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn set_injection_state(
        &self,
        input: &InjectionInput,
        state: &str,
    ) -> Result<(), InjectionError> {
        sqlx::query("UPDATE sporos_injection SET state = ?, updated_at = ? WHERE plan_id = ?")
            .bind(state)
            .bind(now_ms())
            .bind(input.plan_id.as_slice())
            .execute(self.pool())
            .await?;
        if state == "recheck_requested" {
            sqlx::query("UPDATE sporos_injection SET verification_attempts = 0 WHERE plan_id = ?")
                .bind(input.plan_id.as_slice())
                .execute(self.pool())
                .await?;
        }
        Ok(())
    }

    async fn update_qbit_observation(
        &self,
        input: &InjectionInput,
        state: &TorrentState,
        injection_state: &str,
    ) -> Result<(), InjectionError> {
        sqlx::query("UPDATE sporos_injection SET qbit_state = ?, amount_left = ?, progress_ppm = ?, state = ?, updated_at = ? WHERE plan_id = ?")
            .bind(&state.state)
            .bind(to_i64(state.amount_left, "amount left")?)
            .bind(progress_ppm(state.progress)?)
            .bind(injection_state)
            .bind(now_ms())
            .bind(input.plan_id.as_slice())
            .execute(self.pool())
            .await?;
        Ok(())
    }

    async fn record_resume_decision(
        &self,
        input: &InjectionInput,
        state: &TorrentState,
        integrity: &PieceInspection,
        resume: bool,
    ) -> Result<(), InjectionError> {
        sqlx::query(
            "UPDATE sporos_injection SET qbit_state = ?, amount_left = ?, progress_ppm = ?,
                    integrity_safe = ?, resume_decision = ?, state = 'finalizing',
                    verification_attempts = 0, updated_at = ? WHERE plan_id = ?",
        )
        .bind(&state.state)
        .bind(to_i64(state.amount_left, "amount left")?)
        .bind(progress_ppm(state.progress)?)
        .bind(i64::from(integrity.integrity_safe))
        .bind(if resume { "resume" } else { "stop" })
        .bind(now_ms())
        .bind(input.plan_id.as_slice())
        .execute(self.pool())
        .await?;
        Ok(())
    }

    async fn bump_verification(
        &self,
        input: &InjectionInput,
        state: &TorrentState,
    ) -> Result<i64, InjectionError> {
        sqlx::query("UPDATE sporos_injection SET qbit_state = ?, amount_left = ?, progress_ppm = ?, verification_attempts = verification_attempts + 1, updated_at = ? WHERE plan_id = ?")
            .bind(&state.state)
            .bind(to_i64(state.amount_left, "amount left")?)
            .bind(progress_ppm(state.progress)?)
            .bind(now_ms())
            .bind(input.plan_id.as_slice())
            .execute(self.pool())
            .await?;
        Ok(sqlx::query_scalar(
            "SELECT verification_attempts FROM sporos_injection WHERE plan_id = ?",
        )
        .bind(input.plan_id.as_slice())
        .fetch_one(self.pool())
        .await?)
    }

    async fn finish_injection(
        &self,
        input: &InjectionInput,
        state: &TorrentState,
        resumed: bool,
    ) -> Result<(), InjectionError> {
        let now = now_ms();
        sqlx::query(
            "UPDATE sporos_injection SET qbit_state = ?, amount_left = ?, progress_ppm = ?, state = 'completed',
                    updated_at = ? WHERE plan_id = ?",
        )
        .bind(&state.state)
        .bind(to_i64(state.amount_left, "amount left")?)
        .bind(progress_ppm(state.progress)?)
        .bind(now)
        .bind(input.plan_id.as_slice())
        .execute(self.pool())
        .await?;
        sqlx::query("UPDATE sporos_injection_plan SET state = 'completed' WHERE id = ?")
            .bind(input.plan_id.as_slice())
            .execute(self.pool())
            .await?;
        sqlx::query("UPDATE sporos_candidate SET state = 'completed', updated_at = ? WHERE id = ?")
            .bind(now)
            .bind(input.candidate_id.as_slice())
            .execute(self.pool())
            .await?;
        self.project_candidate_state(
            input,
            "completed",
            Some(if resumed { "resumed" } else { "left_stopped" }),
            true,
            now,
        )
        .await
    }

    async fn fail_injection(
        &self,
        input: &InjectionInput,
        reason: &str,
        now: i64,
    ) -> Result<(), InjectionError> {
        sqlx::query("UPDATE sporos_injection_plan SET state = 'failed' WHERE id = ?")
            .bind(input.plan_id.as_slice())
            .execute(self.pool())
            .await?;
        sqlx::query(
            "UPDATE sporos_injection SET state = 'failed', updated_at = ? WHERE plan_id = ?",
        )
        .bind(now)
        .bind(input.plan_id.as_slice())
        .execute(self.pool())
        .await?;
        sqlx::query("UPDATE sporos_candidate SET state = 'failed', updated_at = ? WHERE id = ?")
            .bind(now)
            .bind(input.candidate_id.as_slice())
            .execute(self.pool())
            .await?;
        self.project_candidate_state(input, "failed", Some(reason), true, now)
            .await
    }

    async fn project_candidate_state(
        &self,
        input: &InjectionInput,
        state: &str,
        reason: Option<&str>,
        terminal: bool,
        now: i64,
    ) -> Result<(), InjectionError> {
        self.project_candidate_task(
            TaskId::from_bytes(input.task_id),
            state,
            reason.map(str::to_owned),
            terminal,
            serde_json::json!({ "planId": encode_hex(&input.plan_id) }),
            now,
        )
        .await?;
        Ok(())
    }
}

fn ensure_namespace(plan: &InjectionPlan) -> Result<(), InjectionError> {
    let root = Path::new(&plan.policy.namespace_local_root);
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o750)
        .create(root)?;
    HardlinkMaterializer::open(root)?.materialize(&plan.namespace_relative()?, &[])?;
    Ok(())
}

fn same_save_path(actual: &str, expected: &str) -> bool {
    actual.trim_end_matches('/') == expected.trim_end_matches('/')
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PieceInspection {
    integrity_safe: bool,
    missing_bytes: u64,
    present_ratio_ppm: u32,
}

fn inspect_pieces(
    manifest: &TorrentManifest,
    mapped_ordinals: &BTreeSet<u32>,
    bytes: &[u8],
) -> Result<PieceInspection, InjectionError> {
    let piece_length = manifest
        .piece_length
        .ok_or(InjectionError::MissingPieceLength)?;
    if piece_length == 0 {
        return Err(InjectionError::MissingPieceLength);
    }
    let mut intervals = Vec::new();
    let total = if manifest.piece_files.is_empty() {
        // Compatibility with manifests accepted before protocol layout was stored.
        let v2_only = manifest.hashes.v1.is_none() && manifest.hashes.v2.is_some();
        let mut offset = 0_u64;
        for file in &manifest.files {
            if v2_only {
                offset = offset.div_ceil(piece_length).saturating_mul(piece_length);
            }
            let end = offset
                .checked_add(file.size)
                .ok_or(InjectionError::TorrentSizeOverflow)?;
            if mapped_ordinals.contains(&file.ordinal) && file.size > 0 {
                intervals.push((offset, end));
            }
            offset = end;
        }
        offset
    } else {
        let mut total = 0_u64;
        for file in &manifest.piece_files {
            let end = file
                .offset
                .checked_add(file.size)
                .ok_or(InjectionError::TorrentSizeOverflow)?;
            if file
                .file_ordinal
                .is_some_and(|ordinal| mapped_ordinals.contains(&ordinal))
                && file.size > 0
            {
                intervals.push((file.offset, end));
            }
            total = total.max(end);
        }
        total
    };
    let expected =
        usize::try_from(total.div_ceil(piece_length)).map_err(|_| InjectionError::PieceCount)?;
    let mut index = 0_usize;
    let mut interval = 0_usize;
    let mut integrity_safe = true;
    let mut present = 0_u64;
    let count = parse_piece_states(
        Cursor::new(bytes),
        bytes.len() as u64,
        MAX_PIECES,
        |state| {
            let start = u64::try_from(index)
                .unwrap_or(u64::MAX)
                .saturating_mul(piece_length);
            let end = start.saturating_add(piece_length).min(total);
            while intervals
                .get(interval)
                .is_some_and(|(_, end)| *end <= start)
            {
                interval += 1;
            }
            if state == 2 {
                present = present.saturating_add(end.saturating_sub(start));
            } else if intervals
                .get(interval)
                .is_some_and(|(mapped_start, mapped_end)| {
                    *mapped_start < end && *mapped_end > start
                })
            {
                integrity_safe = false;
            }
            index += 1;
            Ok(())
        },
    )?;
    if count != expected {
        return Err(InjectionError::PieceCount);
    }
    let missing = total.saturating_sub(present);
    let ratio = if total == 0 {
        0
    } else {
        u32::try_from(u128::from(present) * 1_000_000 / u128::from(total)).unwrap_or(1_000_000)
    };
    Ok(PieceInspection {
        integrity_safe,
        missing_bytes: missing,
        present_ratio_ppm: ratio,
    })
}

fn resume_allowed(policy: &ResumePolicy, amount_left: u64, pieces: &PieceInspection) -> bool {
    match policy {
        ResumePolicy::Never => false,
        ResumePolicy::CompleteOnly => amount_left == 0 && pieces.missing_bytes == 0,
        ResumePolicy::Always => true,
        ResumePolicy::Threshold {
            max_missing_bytes,
            min_present_ratio_ppm,
            combine,
        } => {
            let mut checks = Vec::with_capacity(2);
            if let Some(maximum) = max_missing_bytes {
                checks.push(pieces.missing_bytes <= *maximum);
            }
            if let Some(minimum) = min_present_ratio_ppm {
                checks.push(pieces.present_ratio_ppm >= *minimum);
            }
            match combine {
                ThresholdCombination::And => checks.into_iter().all(|check| check),
                ThresholdCombination::Or => checks.into_iter().any(|check| check),
            }
        }
    }
}

fn materialize_reason(error: &MaterializeError) -> &'static str {
    match error {
        MaterializeError::DeviceMismatch => "hardlink_device_mismatch",
        MaterializeError::LinkConflict => "link_conflict",
        MaterializeError::SourceIdentityChanged | MaterializeError::SourceSizeChanged => {
            "source_changed"
        }
        _ => "hardlink_materialization_failed",
    }
}

fn to_i64(value: u64, field: &'static str) -> Result<i64, InjectionError> {
    value.try_into().map_err(|_| InjectionError::Range(field))
}

fn progress_ppm(value: f64) -> Result<i64, InjectionError> {
    if !value.is_finite() || !(0.0..=1.0).contains(&value) {
        return Err(InjectionError::InvalidProgress);
    }
    Ok((value * 1_000_000.0).round() as i64)
}

fn to_u64(value: i64, field: &'static str) -> Result<u64, InjectionError> {
    value
        .try_into()
        .map_err(|_| InjectionError::StoredRange(field))
}

fn to_u32(value: i64, field: &'static str) -> Result<u32, InjectionError> {
    value
        .try_into()
        .map_err(|_| InjectionError::StoredRange(field))
}

fn optional_u64(value: Option<i64>, field: &'static str) -> Result<Option<u64>, InjectionError> {
    value.map(|value| to_u64(value, field)).transpose()
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
enum InjectionError {
    #[error("injection database operation failed")]
    Database(#[from] sqlx::Error),
    #[error("injection data is invalid")]
    Json(#[from] serde_json::Error),
    #[error("qBittorrent injection request failed")]
    Qbittorrent(#[from] crate::qbittorrent::QbittorrentError),
    #[error("hardlink materialization failed")]
    Materialize(#[from] MaterializeError),
    #[error("hardlink materializer task failed")]
    MaterializerTask(#[source] tokio::task::JoinError),
    #[error("candidate projection failed")]
    Projection(#[from] crate::candidate_workflow::CandidateWorkflowError),
    #[error("filesystem operation failed")]
    Filesystem(#[from] std::io::Error),
    #[error("piece-state response is invalid")]
    Pieces(#[from] InventoryParseError),
    #[error("injection plan is missing")]
    MissingPlan,
    #[error("candidate manifest is missing")]
    MissingManifest,
    #[error("candidate infohash is missing")]
    MissingHash,
    #[error("candidate piece length is missing")]
    MissingPieceLength,
    #[error("candidate namespace is outside its managed root")]
    NamespaceOutsideRoot,
    #[error("external source root has no approved local path mapping")]
    UnmappedSourceRoot,
    #[error("source file path is invalid")]
    InvalidSourcePath,
    #[error("candidate torrent size overflowed")]
    TorrentSizeOverflow,
    #[error("qBittorrent piece count does not match the torrent")]
    PieceCount,
    #[error("qBittorrent progress is outside zero through one")]
    InvalidProgress,
    #[error("{0} is outside the supported SQLite range")]
    Range(&'static str),
    #[error("stored {0} is outside the supported range")]
    StoredRange(&'static str),
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::os::unix::fs::MetadataExt;
    use std::thread;

    use reqwest::Url;
    use sporos_matcher::parse_release;
    use sporos_model::{InfoHashes, TorrentFile, TorrentPieceFile};
    use tempfile::TempDir;

    use super::*;
    use crate::candidate::{CandidateIngress, CandidateSubmission};
    use crate::candidate_workflow::EvaluationResult;
    use crate::config::{Injection, Matching, PathRewrite, SourceFilters};
    use crate::inventory::{InventoryChange, InventoryDelta, InventoryFile};
    use crate::qbittorrent::ApiKey;

    const API_KEY: &str = "qbt_0123456789abcdefghijklmnopqr";

    #[tokio::test]
    async fn injection_materializes_rechecks_and_verifies_final_state() {
        let directory = TempDir::new().unwrap();
        let source_root = directory.path().join("source");
        std::fs::create_dir(&source_root).unwrap();
        let source_file = source_root.join("Example.Movie.2024");
        std::fs::write(&source_file, b"source-bytes!").unwrap();
        let storage = Arc::new(
            Storage::open(
                directory.path().join("sporos.lock"),
                directory.path().join("sporos.db"),
            )
            .await
            .unwrap(),
        );
        project_source(&storage).await;
        let accepted = accept_candidate(&storage, directory.path(), &source_root).await;
        let workflow_input: CandidateWorkflowInput = serde_json::from_str(
            &sqlx::query_scalar::<_, String>(
                "SELECT input_json FROM sporos_outbox WHERE task_id = ?",
            )
            .bind(accepted.task_id.as_bytes().as_slice())
            .fetch_one(storage.pool())
            .await
            .unwrap(),
        )
        .unwrap();
        let evaluation = storage
            .evaluate_candidate(&workflow_input, 20)
            .await
            .unwrap();
        let EvaluationResult::Terminal {
            plan_id: Some(plan_id),
            ..
        } = evaluation
        else {
            panic!("candidate did not produce an injection plan");
        };
        let row = sqlx::query(
            "SELECT ip.save_path_remote, c.v1_hash FROM sporos_injection_plan ip
             JOIN sporos_candidate c ON c.id = ip.candidate_id WHERE ip.id = ?",
        )
        .bind(plan_id.as_slice())
        .fetch_one(storage.pool())
        .await
        .unwrap();
        let save_path: String = row.get("save_path_remote");
        let hash = encode_hex(&row.get::<Vec<u8>, _>("v1_hash"));
        let stopped = state(&hash, &save_path, "stoppedUP", 0, 1.0);
        let started = state(&hash, &save_path, "uploading", 0, 1.0);
        let (url, server) = qbit_server(vec![
            b"v5.2.5".to_vec(),
            b"2.14.3".to_vec(),
            b"[]".to_vec(),
            format!(
                r#"{{"success_count":1,"failure_count":0,"pending_count":0,"added_torrent_ids":["{hash}"]}}"#
            )
            .into_bytes(),
            b"v5.2.5".to_vec(),
            b"2.14.3".to_vec(),
            format!("[{stopped}]").into_bytes(),
            format!("[{stopped}]").into_bytes(),
            Vec::new(),
            format!("[{stopped}]").into_bytes(),
            format!("[{stopped}]").into_bytes(),
            b"[2]".to_vec(),
            Vec::new(),
            format!("[{started}]").into_bytes(),
        ]);
        let executor = InjectionExecutor::new(
            Arc::clone(&storage),
            Some(
                QbittorrentClient::new(url, ApiKey::new(API_KEY).unwrap())
                    .expect("qBittorrent client"),
            ),
        );
        let input = InjectionInput {
            task_id: workflow_input.task_id,
            candidate_id: workflow_input.candidate_id,
            policy_snapshot_id: workflow_input.policy_snapshot_id,
            plan_id,
        };

        assert_eq!(executor.prepare(&input).await.unwrap(), waiting());
        let mut plan = storage.load_injection(&input).await.unwrap();
        let rewrites = std::mem::take(&mut plan.policy.path_rewrites);
        assert!(matches!(
            plan.planned_links(),
            Err(InjectionError::UnmappedSourceRoot)
        ));
        plan.policy.path_rewrites = rewrites;
        let destination = Path::new(&plan.namespace_local).join(&plan.mappings[0].candidate_path);
        std::fs::create_dir_all(destination.parent().unwrap()).unwrap();
        std::fs::hard_link(&source_file, &destination).unwrap();
        assert_eq!(executor.prepare(&input).await.unwrap(), StepResult::Ready);
        assert_eq!(
            executor.materialize(&input).await.unwrap(),
            StepResult::Ready
        );
        assert_eq!(executor.recheck(&input).await.unwrap(), waiting());
        assert_eq!(executor.observe(&input).await.unwrap(), waiting());
        assert_eq!(executor.observe(&input).await.unwrap(), waiting());
        assert!(matches!(
            executor.observe(&input).await.unwrap(),
            StepResult::Terminal { ref state, ref reason_code }
                if state == "completed" && reason_code == "resumed"
        ));
        server.join().unwrap();

        let destination = sqlx::query_scalar::<_, Vec<u8>>(
            "SELECT destination_relative FROM sporos_link WHERE plan_id = ?",
        )
        .bind(plan_id.as_slice())
        .fetch_one(storage.pool())
        .await
        .unwrap();
        let namespace = sqlx::query_scalar::<_, String>(
            "SELECT namespace_local FROM sporos_injection_plan WHERE id = ?",
        )
        .bind(plan_id.as_slice())
        .fetch_one(storage.pool())
        .await
        .unwrap();
        let destination = Path::new(&namespace).join(String::from_utf8(destination).unwrap());
        assert_eq!(
            std::fs::metadata(source_file).unwrap().ino(),
            std::fs::metadata(destination).unwrap().ino()
        );
        let injection = sqlx::query(
            "SELECT integrity_safe, resume_decision, progress_ppm, state
             FROM sporos_injection WHERE plan_id = ?",
        )
        .bind(plan_id.as_slice())
        .fetch_one(storage.pool())
        .await
        .unwrap();
        assert_eq!(injection.get::<i64, _>("integrity_safe"), 1);
        assert_eq!(injection.get::<String, _>("resume_decision"), "resume");
        assert_eq!(injection.get::<i64, _>("progress_ppm"), 1_000_000);
        assert_eq!(injection.get::<String, _>("state"), "completed");
    }

    #[test]
    fn recheck_observation_does_not_require_a_transient_checking_state() {
        let stopped: TorrentState =
            serde_json::from_str(&state("hash", "/data", "stoppedUP", 0, 1.0)).unwrap();
        let checking: TorrentState =
            serde_json::from_str(&state("hash", "/data", "checkingUP", 0, 1.0)).unwrap();
        let queued: TorrentState =
            serde_json::from_str(&state("hash", "/data", "queuedUP", 0, 1.0)).unwrap();

        assert_eq!(
            recheck_observation(Some("recheck_requested"), &checking),
            RecheckObservation::Checking
        );
        assert_eq!(
            recheck_observation(Some("recheck_requested"), &stopped),
            RecheckObservation::Settling
        );
        assert_eq!(
            recheck_observation(Some("recheck_settling"), &stopped),
            RecheckObservation::Inspect
        );
        assert_eq!(
            recheck_observation(Some("recheck_requested"), &queued),
            RecheckObservation::Waiting
        );
        assert_eq!(
            recheck_observation(Some("checking"), &stopped),
            RecheckObservation::Inspect
        );
    }

    #[tokio::test]
    async fn data_source_plan_uses_its_catalogued_local_path() {
        let directory = TempDir::new().unwrap();
        let source_root = directory.path().join("source");
        std::fs::create_dir(&source_root).unwrap();
        let source_file = source_root.join("Example.Movie.2024");
        std::fs::write(&source_file, b"source-bytes!").unwrap();
        let storage = Storage::open(
            directory.path().join("sporos.lock"),
            directory.path().join("sporos.db"),
        )
        .await
        .unwrap();
        project_data_source(&storage, &source_file).await;
        let accepted = accept_candidate(&storage, directory.path(), &source_root).await;
        let workflow_input: CandidateWorkflowInput = serde_json::from_str(
            &sqlx::query_scalar::<_, String>(
                "SELECT input_json FROM sporos_outbox WHERE task_id = ?",
            )
            .bind(accepted.task_id.as_bytes().as_slice())
            .fetch_one(storage.pool())
            .await
            .unwrap(),
        )
        .unwrap();
        let EvaluationResult::Terminal {
            plan_id: Some(plan_id),
            ..
        } = storage
            .evaluate_candidate(&workflow_input, 20)
            .await
            .unwrap()
        else {
            panic!("data source did not produce an injection plan");
        };
        let plan = storage
            .load_injection(&InjectionInput {
                task_id: workflow_input.task_id,
                candidate_id: workflow_input.candidate_id,
                policy_snapshot_id: workflow_input.policy_snapshot_id,
                plan_id,
            })
            .await
            .unwrap();
        let links = plan.planned_links().unwrap();

        assert_eq!(links.len(), 1);
        assert_eq!(links[0].source_root, source_root);
        assert_eq!(links[0].source_relative, Path::new("Example.Movie.2024"));
    }

    #[test]
    fn missing_piece_intersecting_a_link_is_never_safe() {
        let manifest = manifest();
        let inspection = inspect_pieces(&manifest, &[0].into(), b"[2,0,2]").unwrap();
        assert!(!inspection.integrity_safe);
    }

    #[test]
    fn missing_piece_is_safe_when_isolated_to_an_unlinked_file() {
        let manifest = manifest();
        let inspection = inspect_pieces(&manifest, &[0].into(), b"[2,2,0]").unwrap();
        assert!(inspection.integrity_safe);
        assert_eq!(inspection.present_ratio_ppm, 800_000);
        assert!(resume_allowed(
            &ResumePolicy::Threshold {
                max_missing_bytes: None,
                min_present_ratio_ppm: Some(100_000),
                combine: ThresholdCombination::And,
            },
            2,
            &inspection,
        ));
    }

    #[test]
    fn hybrid_padding_keeps_piece_intersections_aligned() {
        let manifest = TorrentManifest {
            hashes: InfoHashes {
                v1: Some([1; 20]),
                v2: Some([2; 32]),
            },
            files: vec![
                TorrentFile {
                    ordinal: 0,
                    path: "root/a.bin".into(),
                    size: 3,
                    padding: false,
                },
                TorrentFile {
                    ordinal: 1,
                    path: "root/b.bin".into(),
                    size: 3,
                    padding: false,
                },
            ],
            piece_length: Some(4),
            piece_files: vec![
                TorrentPieceFile {
                    file_ordinal: Some(0),
                    offset: 0,
                    size: 3,
                },
                TorrentPieceFile {
                    file_ordinal: None,
                    offset: 3,
                    size: 1,
                },
                TorrentPieceFile {
                    file_ordinal: Some(1),
                    offset: 4,
                    size: 3,
                },
            ],
        };

        let inspection = inspect_pieces(&manifest, &BTreeSet::from([1]), b"[0,2]").unwrap();
        assert!(inspection.integrity_safe);
        let inspection = inspect_pieces(&manifest, &BTreeSet::from([1]), b"[2,0]").unwrap();
        assert!(!inspection.integrity_safe);
    }

    async fn project_source(storage: &Storage) {
        storage
            .project_qbit_batch(
                &[InventoryChange::Upsert {
                    qbit_id: "b".repeat(40),
                    delta: Box::new(InventoryDelta {
                        infohash_v1: Some("b".repeat(40)),
                        name: Some("Example.Movie.2024.1080p".to_owned()),
                        total_size: Some(13),
                        amount_left: Some(0),
                        progress: Some(1.0),
                        state: Some("stoppedUP".to_owned()),
                        save_path: Some("/downloads".to_owned()),
                        content_path: Some("/downloads/Example.Movie.2024".to_owned()),
                        category: Some("source".to_owned()),
                        tags: Some("source-tag".to_owned()),
                        added_on: Some(1),
                        completion_on: Some(2),
                        ..InventoryDelta::default()
                    }),
                }],
                1,
                false,
                1,
            )
            .await
            .unwrap();
        let source_id = sqlx::query_scalar::<_, Vec<u8>>("SELECT id FROM sporos_qbit_torrent")
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
                    size: 13,
                    progress: 1.0,
                }],
            )
            .await
            .unwrap();
        storage.finish_qbit_manifest(&target, 1, 2).await.unwrap();
    }

    async fn project_data_source(storage: &Storage, source_file: &Path) {
        let source_id = [9_u8; 16];
        let metadata = std::fs::metadata(source_file).unwrap();
        let release = parse_release("Example.Movie.2024");
        sqlx::query(
            "INSERT INTO sporos_data_source
             (id, root_name, relative_path, kind, name, total_size, release_json,
              normalized_title, device, inode, modified_at, available,
              last_seen_generation, updated_at)
             VALUES (?, 'media', ?, 'file', 'Example.Movie.2024', 13, ?, ?, ?, ?, ?, 1, 1, 1)",
        )
        .bind(source_id.as_slice())
        .bind(b"Example.Movie.2024".as_slice())
        .bind(serde_json::to_string(&release).unwrap())
        .bind(release.primary_title.as_str())
        .bind(i64::try_from(metadata.dev()).unwrap())
        .bind(i64::try_from(metadata.ino()).unwrap())
        .bind(metadata.mtime())
        .execute(storage.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO sporos_source_file
             (source_id, manifest_version, relative_path, display_path, size,
              file_kind, local_path, device, inode, modified_at, available, ordinal)
             VALUES (?, 1, ?, 'Example.Movie.2024', 13, 'video', ?, ?, ?, ?, 1, 0)",
        )
        .bind(source_id.as_slice())
        .bind(b"Example.Movie.2024".as_slice())
        .bind(source_file.to_str().unwrap())
        .bind(i64::try_from(metadata.dev()).unwrap())
        .bind(i64::try_from(metadata.ino()).unwrap())
        .bind(metadata.mtime())
        .execute(storage.pool())
        .await
        .unwrap();
        let mut transaction = storage.pool().begin().await.unwrap();
        crate::source_facts::replace(&mut transaction, &source_id, "data", &release)
            .await
            .unwrap();
        transaction.commit().await.unwrap();
    }

    async fn accept_candidate(
        storage: &Storage,
        directory: &Path,
        source_root: &Path,
    ) -> crate::candidate::AcceptedCandidate {
        let ingress = CandidateIngress::new(
            Matching::default(),
            SourceFilters::default(),
            Injection::default(),
            Paths {
                link_root: directory.join("links"),
                rewrite: vec![
                    PathRewrite {
                        name: "source".to_owned(),
                        remote: "/downloads".into(),
                        local: source_root.to_owned(),
                        services: vec!["qbittorrent".to_owned()],
                    },
                    PathRewrite {
                        name: "links".to_owned(),
                        remote: "/qbit-links".into(),
                        local: directory.join("links"),
                        services: vec!["qbittorrent".to_owned()],
                    },
                ],
            },
        );
        let bytes = format!(
            "d4:infod6:lengthi13e4:name18:Example.Movie.202412:piece lengthi16384e6:pieces20:{}ee",
            "a".repeat(20)
        )
        .into_bytes();
        ingress
            .accept(
                storage,
                CandidateSubmission {
                    bytes,
                    announcement_name: Some("Example.Movie.2024.1080p".to_owned()),
                    indexer: Some("fixture".to_owned()),
                    indexer_id: None,
                    trigger: "test".to_owned(),
                    release_hint: None,
                    category: None,
                    tags: Vec::new(),
                    request_id: "injection-test".to_owned(),
                    dry_run: false,
                    received_at: 10,
                },
            )
            .await
            .unwrap()
    }

    fn state(
        hash: &str,
        save_path: &str,
        torrent_state: &str,
        amount_left: u64,
        progress: f64,
    ) -> String {
        serde_json::json!({
            "hash": hash,
            "name": "Example.Movie.2024",
            "state": torrent_state,
            "amount_left": amount_left,
            "progress": progress,
            "save_path": save_path,
            "content_path": format!("{save_path}/Example.Movie.2024"),
            "category": "sporos",
            "tags": "",
            "auto_tmm": false,
        })
        .to_string()
    }

    fn qbit_server(bodies: Vec<Vec<u8>>) -> (Url, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let handle = thread::spawn(move || {
            for body in bodies {
                let (mut stream, _) = listener.accept().unwrap();
                read_request(&mut stream);
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                )
                .unwrap();
                stream.write_all(&body).unwrap();
            }
        });
        (Url::parse(&format!("http://{address}")).unwrap(), handle)
    }

    fn read_request(stream: &mut TcpStream) {
        let mut bytes = Vec::new();
        let mut chunk = [0_u8; 4096];
        let header_end = loop {
            let count = stream.read(&mut chunk).unwrap();
            bytes.extend_from_slice(&chunk[..count]);
            if let Some(position) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                break position + 4;
            }
        };
        let head = String::from_utf8_lossy(&bytes[..header_end]);
        let length = head
            .lines()
            .find_map(|line| {
                line.to_ascii_lowercase()
                    .strip_prefix("content-length:")
                    .and_then(|value| value.trim().parse::<usize>().ok())
            })
            .unwrap_or(0);
        while bytes.len().saturating_sub(header_end) < length {
            let count = stream.read(&mut chunk).unwrap();
            bytes.extend_from_slice(&chunk[..count]);
        }
    }

    fn manifest() -> TorrentManifest {
        TorrentManifest {
            hashes: InfoHashes {
                v1: Some([1; 20]),
                v2: None,
            },
            files: vec![
                TorrentFile {
                    ordinal: 0,
                    path: "linked.mkv".into(),
                    size: 8,
                    padding: false,
                },
                TorrentFile {
                    ordinal: 1,
                    path: "missing.mkv".into(),
                    size: 2,
                    padding: false,
                },
            ],
            piece_length: Some(4),
            piece_files: Vec::new(),
        }
    }
}
