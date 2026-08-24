use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::extract::{MatchedPath, Path, Query, Request, State};
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE, RETRY_AFTER};
use axum::http::{HeaderValue, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::Row;
use subtle::ConstantTimeEq;
use tokio::sync::Semaphore;
use tower::ServiceBuilder;
use tower_http::limit::RequestBodyLimitLayer;

use crate::candidate::{CandidateError, CandidateIngress, CandidateSubmission};
use crate::config::{Config, Matching, Secret, SourceFilters};
use crate::data_scan::accept as accept_data_scan;
use crate::preflight::SourceState;
use crate::prowlarr::ProwlarrClient;
use crate::search::{BackfillSelection, SearchPolicy, accept_backfill};
use crate::storage::Storage;
use sporos_matcher::parse_release;

static REQUEST_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
pub struct HttpState {
    storage: Arc<Storage>,
    webhook_token: Secret,
    admin_token: Secret,
    readiness: Arc<AtomicBool>,
    metrics: Arc<Metrics>,
    inventory_stale_after: Option<Duration>,
    source_filters: SourceFilters,
    matching: Matching,
    candidate_ingress: Arc<CandidateIngress>,
    search_policy: SearchPolicy,
    prowlarr_configured: bool,
    prowlarr_client: Option<ProwlarrClient>,
    data_roots: std::collections::BTreeSet<String>,
    upload_permits: Arc<Semaphore>,
    autobrr_body_limit_bytes: usize,
}

impl HttpState {
    pub(crate) fn new(
        storage: Arc<Storage>,
        config: &Config,
        prowlarr_client: Option<ProwlarrClient>,
    ) -> Self {
        Self {
            storage,
            webhook_token: config.auth.webhook_token.clone(),
            admin_token: config.auth.admin_token.clone(),
            readiness: Arc::new(AtomicBool::new(false)),
            metrics: Arc::new(Metrics::new()),
            inventory_stale_after: config
                .qbittorrent
                .as_ref()
                .map(|settings| settings.inventory_stale_after),
            source_filters: config.sources.clone(),
            matching: config.matching.clone(),
            candidate_ingress: Arc::new(CandidateIngress::new(
                config.matching.clone(),
                config.sources.clone(),
                config.injection.clone(),
                config.paths.clone(),
            )),
            search_policy: SearchPolicy::new(
                config.matching.clone(),
                config.sources.clone(),
                config.injection.clone(),
                config.paths.clone(),
            ),
            prowlarr_configured: config.prowlarr.is_some(),
            prowlarr_client,
            data_roots: config.data_roots.keys().cloned().collect(),
            upload_permits: Arc::new(Semaphore::new(config.limits.max_uploads)),
            autobrr_body_limit_bytes: config.server.autobrr_body_limit_bytes,
        }
    }

    pub fn set_ready(&self, ready: bool) {
        self.readiness.store(ready, Ordering::Release);
    }
}

pub fn router(state: HttpState, admin_body_limit: usize) -> Router {
    let check = Router::new()
        .route("/api/v1/autobrr/check", post(autobrr_check))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_webhook,
        ))
        .layer(RequestBodyLimitLayer::new(64 * 1024));
    let uploads = Router::new()
        .route("/api/v1/autobrr/torrents", post(autobrr_torrent))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_webhook,
        ))
        .layer(RequestBodyLimitLayer::new(state.autobrr_body_limit_bytes));
    let admin = Router::new()
        .route("/api/v1/admin/tasks", get(list_tasks))
        .route("/api/v1/admin/operations", get(list_operations))
        .route(
            "/api/v1/admin/operations/{operation_id}",
            get(get_operation),
        )
        .route("/api/v1/admin/searches", post(start_inventory_search))
        .route("/api/v1/admin/data-scans", post(start_data_scan))
        .route("/api/v1/admin/tasks/{task_id}", get(get_task))
        .route("/api/v1/admin/tasks/{task_id}/events", get(get_task_events))
        .route("/api/v1/admin/inventory", get(get_inventory))
        .route(
            "/api/v1/admin/inventory/torrents",
            get(list_inventory_torrents),
        )
        .route(
            "/api/v1/admin/inventory/reconcile",
            post(request_inventory_reconcile),
        )
        .route_layer(middleware::from_fn_with_state(state.clone(), require_admin))
        .layer(RequestBodyLimitLayer::new(admin_body_limit));

    Router::new()
        .route("/livez", get(livez))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        .merge(check)
        .merge(uploads)
        .merge(admin)
        .fallback(not_found)
        .layer(
            ServiceBuilder::new()
                .layer(middleware::from_fn_with_state(
                    state.clone(),
                    observe_request,
                ))
                .layer(middleware::from_fn(request_id)),
        )
        .with_state(state)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AutobrrCheckRequest {
    torrent_name: String,
    size: Option<u64>,
    indexer: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AutobrrCheckResponse {
    decision: &'static str,
    provisional: bool,
    source_state: SourceState,
    reason: &'static str,
    request_id: String,
}

async fn autobrr_check(
    State(state): State<HttpState>,
    Extension(request_id): Extension<RequestId>,
    Json(request): Json<AutobrrCheckRequest>,
) -> Result<Json<AutobrrCheckResponse>, Problem> {
    if request.torrent_name.is_empty() || request.torrent_name.len() > 1024 {
        return Err(Problem::new(
            StatusCode::BAD_REQUEST,
            "invalid_torrent_name",
            "Invalid torrent name",
            "torrentName must contain between 1 and 1024 UTF-8 bytes",
            request_id,
        ));
    }
    if request
        .indexer
        .as_ref()
        .is_some_and(|value| value.len() > 128)
    {
        return Err(Problem::new(
            StatusCode::BAD_REQUEST,
            "invalid_indexer",
            "Invalid indexer",
            "indexer must not exceed 128 UTF-8 bytes",
            request_id,
        ));
    }
    let inventory = state
        .storage
        .qbit_inventory_state()
        .await
        .map_err(|_| Problem::database(request_id.clone()))?;
    let usable = state.inventory_stale_after.is_some()
        && inventory.has_baseline
        && inventory.last_success_at.is_some_and(|success| {
            now_ms().saturating_sub(success)
                <= i64::try_from(
                    state
                        .inventory_stale_after
                        .expect("checked inventory staleness")
                        .as_millis(),
                )
                .unwrap_or(i64::MAX)
        });
    if !usable {
        return Err(Problem::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "inventory_unavailable",
            "Inventory unavailable",
            "the local qBittorrent inventory is not reconciled or is stale",
            request_id,
        ));
    }

    let release = parse_release(&request.torrent_name);
    let source = state
        .storage
        .preflight_source(
            &release,
            request.size.filter(|size| *size > 0),
            state.matching.preflight_size_tolerance,
            state.matching.policy.allow_season_from_episodes,
            &state.source_filters,
        )
        .await
        .map_err(|error| match error {
            crate::preflight::PreflightError::Database(_)
            | crate::preflight::PreflightError::Json(_) => Problem::database(request_id.clone()),
            crate::preflight::PreflightError::SizeRange => Problem::new(
                StatusCode::BAD_REQUEST,
                "invalid_size",
                "Invalid size",
                "size exceeds the supported SQLite integer range",
                request_id.clone(),
            ),
        })?;
    let Some(source_state) = source else {
        return Err(Problem::new(
            StatusCode::NOT_FOUND,
            "no_match",
            "No plausible source",
            "no plausible local source matches the announced release",
            request_id,
        ));
    };
    Ok(Json(AutobrrCheckResponse {
        decision: "accept",
        provisional: true,
        source_state,
        reason: "plausible_source",
        request_id: request_id.0,
    }))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AutobrrTorrentRequest {
    torrent_data: String,
    torrent_name: Option<String>,
    indexer: Option<String>,
    category: Option<String>,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    dry_run: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AutobrrTorrentResponse {
    candidate_id: String,
    task_id: String,
    duplicate: bool,
    status: &'static str,
    request_id: String,
}

async fn autobrr_torrent(
    State(state): State<HttpState>,
    Extension(request_id): Extension<RequestId>,
    Json(request): Json<AutobrrTorrentRequest>,
) -> Result<(StatusCode, Json<AutobrrTorrentResponse>), Problem> {
    validate_upload_metadata(&request, request_id.clone())?;
    let _permit = Arc::clone(&state.upload_permits)
        .try_acquire_owned()
        .map_err(|_| {
            Problem::new(
                StatusCode::TOO_MANY_REQUESTS,
                "upload_capacity_reached",
                "Upload capacity reached",
                "the bounded upload admission pool is full",
                request_id.clone(),
            )
            .with_retry_after("1")
        })?;
    let bytes = STANDARD.decode(&request.torrent_data).map_err(|_| {
        Problem::new(
            StatusCode::BAD_REQUEST,
            "invalid_torrent_data",
            "Invalid torrent data",
            "torrentData must be canonical base64",
            request_id.clone(),
        )
    })?;
    if STANDARD.encode(&bytes) != request.torrent_data {
        return Err(Problem::new(
            StatusCode::BAD_REQUEST,
            "invalid_torrent_data",
            "Invalid torrent data",
            "torrentData must be canonical base64",
            request_id,
        ));
    }
    let accepted = state
        .candidate_ingress
        .accept(
            &state.storage,
            CandidateSubmission {
                bytes,
                announcement_name: request.torrent_name,
                indexer: request.indexer,
                indexer_id: None,
                trigger: "autobrr".to_owned(),
                release_hint: None,
                category: request.category,
                tags: request.tags,
                request_id: request_id.0.clone(),
                dry_run: request.dry_run,
                received_at: now_ms(),
            },
        )
        .await
        .map_err(|error| candidate_problem(error, request_id.clone()))?;
    Ok((
        if accepted.duplicate {
            StatusCode::OK
        } else {
            StatusCode::ACCEPTED
        },
        Json(AutobrrTorrentResponse {
            candidate_id: format!("cand_{}", encode_hex(accepted.candidate_id.as_bytes())),
            task_id: format!("task_{}", encode_hex(accepted.task_id.as_bytes())),
            duplicate: accepted.duplicate,
            status: "queued",
            request_id: request_id.0,
        }),
    ))
}

fn validate_upload_metadata(
    request: &AutobrrTorrentRequest,
    request_id: RequestId,
) -> Result<(), Problem> {
    let valid_optional = |value: &Option<String>, maximum: usize| {
        value
            .as_ref()
            .is_none_or(|value| !value.is_empty() && value.len() <= maximum)
    };
    if request.torrent_data.is_empty() {
        return Err(Problem::new(
            StatusCode::BAD_REQUEST,
            "invalid_torrent_data",
            "Invalid torrent data",
            "torrentData cannot be empty",
            request_id,
        ));
    }
    if !valid_optional(&request.torrent_name, 1024)
        || !valid_optional(&request.indexer, 128)
        || !valid_optional(&request.category, 128)
        || request.tags.len() > 64
        || request
            .tags
            .iter()
            .any(|tag| tag.is_empty() || tag.len() > 128)
    {
        return Err(Problem::new(
            StatusCode::BAD_REQUEST,
            "invalid_candidate_metadata",
            "Invalid candidate metadata",
            "candidate metadata exceeds its configured field limits",
            request_id,
        ));
    }
    Ok(())
}

fn candidate_problem(error: CandidateError, request_id: RequestId) -> Problem {
    match error {
        CandidateError::TorrentTooLarge => Problem::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "torrent_too_large",
            "Torrent too large",
            "decoded torrent data exceeds the configured byte limit",
            request_id,
        ),
        CandidateError::TooManyFiles
        | CandidateError::PathTooLong
        | CandidateError::InvalidUtf8Name
        | CandidateError::InvalidUtf8Path
        | CandidateError::Torrent(_) => Problem::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_torrent",
            "Invalid torrent",
            "the torrent cannot pass structural validation",
            request_id,
        ),
        CandidateError::Database(_) | CandidateError::DurableIngress(_) => Problem::new(
            StatusCode::INSUFFICIENT_STORAGE,
            "storage_unavailable",
            "Storage unavailable",
            "the candidate could not be committed durably",
            request_id,
        ),
        CandidateError::Json(_)
        | CandidateError::BlobCollision
        | CandidateError::CandidateCollision
        | CandidateError::CandidateTaskCollision => Problem::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "candidate_ingress_failed",
            "Candidate ingress failed",
            "the candidate identity could not be established safely",
            request_id,
        ),
    }
}

async fn livez() -> StatusCode {
    StatusCode::OK
}

async fn readyz(State(state): State<HttpState>) -> StatusCode {
    if !state.readiness.load(Ordering::Acquire) {
        return StatusCode::SERVICE_UNAVAILABLE;
    }
    match sqlx::query_scalar::<_, i64>("SELECT 1")
        .fetch_one(state.storage.pool())
        .await
    {
        Ok(1) => StatusCode::OK,
        _ => StatusCode::SERVICE_UNAVAILABLE,
    }
}

async fn metrics(State(state): State<HttpState>) -> Response {
    let outbox_depth = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM sporos_outbox
         WHERE dispatched_at IS NULL AND permanent_failure_at IS NULL",
    )
    .fetch_one(state.storage.pool())
    .await
    .unwrap_or(-1);
    let task_rows =
        sqlx::query("SELECT kind, state, count(*) AS count FROM sporos_task GROUP BY kind, state")
            .fetch_all(state.storage.pool())
            .await
            .unwrap_or_default();
    let mut output = state.metrics.render();
    output.push_str("# TYPE sporos_outbox_depth gauge\n");
    output.push_str(&format!("sporos_outbox_depth {outbox_depth}\n"));
    output.push_str("# TYPE sporos_tasks gauge\n");
    for row in task_rows {
        let kind = row.try_get::<String, _>("kind").unwrap_or_default();
        let task_state = row.try_get::<String, _>("state").unwrap_or_default();
        let count = row.try_get::<i64, _>("count").unwrap_or_default();
        output.push_str(&format!(
            "sporos_tasks{{kind=\"{}\",state=\"{}\"}} {count}\n",
            metric_escape(&kind),
            metric_escape(&task_state)
        ));
    }
    let inventory = sqlx::query_as::<_, (i64, i64, Option<i64>)>(
        "SELECT
            (SELECT count(*) FROM sporos_qbit_torrent),
            (SELECT count(*) FROM sporos_source_file WHERE available = 1),
            (SELECT last_success_at FROM sporos_qbit_inventory_state WHERE singleton = 1)",
    )
    .fetch_optional(state.storage.pool())
    .await
    .ok()
    .flatten()
    .unwrap_or((-1, -1, None));
    output.push_str("# TYPE sporos_qbit_inventory_torrents gauge\n");
    output.push_str(&format!("sporos_qbit_inventory_torrents {}\n", inventory.0));
    output.push_str("# TYPE sporos_qbit_inventory_files gauge\n");
    output.push_str(&format!("sporos_qbit_inventory_files {}\n", inventory.1));
    output.push_str("# TYPE sporos_qbit_inventory_last_success_timestamp_seconds gauge\n");
    output.push_str(&format!(
        "sporos_qbit_inventory_last_success_timestamp_seconds {}\n",
        inventory.2.unwrap_or(0) / 1000
    ));
    (
        [(
            CONTENT_TYPE,
            "application/openmetrics-text; version=1.0.0; charset=utf-8",
        )],
        output,
    )
        .into_response()
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct InventoryStatus {
    configured: bool,
    baseline_complete: bool,
    stale: bool,
    response_id: Option<u64>,
    generation: u64,
    torrents: i64,
    available_torrents: i64,
    complete_torrents: i64,
    files: i64,
    last_success_at: Option<i64>,
    last_full_reconcile_at: Option<i64>,
    reconcile_requested_at: Option<i64>,
}

async fn get_inventory(
    State(state): State<HttpState>,
    Extension(request_id): Extension<RequestId>,
) -> Result<Json<InventoryStatus>, Problem> {
    let inventory = state
        .storage
        .qbit_inventory_state()
        .await
        .map_err(|_| Problem::database(request_id.clone()))?;
    let counts = sqlx::query_as::<_, (i64, i64, i64, i64)>(
        "SELECT
            count(*),
            coalesce(sum(available), 0),
            coalesce(sum(CASE WHEN available = 1 AND is_complete = 1 THEN 1 ELSE 0 END), 0),
            (SELECT count(*) FROM sporos_source_file WHERE available = 1)
         FROM sporos_qbit_torrent",
    )
    .fetch_one(state.storage.pool())
    .await
    .map_err(|_| Problem::database(request_id))?;
    let stale = match (inventory.last_success_at, state.inventory_stale_after) {
        (Some(success), Some(limit)) => {
            now_ms().saturating_sub(success) > i64::try_from(limit.as_millis()).unwrap_or(i64::MAX)
        }
        _ => true,
    };
    Ok(Json(InventoryStatus {
        configured: state.inventory_stale_after.is_some(),
        baseline_complete: inventory.has_baseline,
        stale,
        response_id: inventory.response_id,
        generation: inventory.generation,
        torrents: counts.0,
        available_torrents: counts.1,
        complete_torrents: counts.2,
        files: counts.3,
        last_success_at: inventory.last_success_at,
        last_full_reconcile_at: inventory.last_full_reconcile_at,
        reconcile_requested_at: inventory.reconcile_requested_at,
    }))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReconcileRequest {
    full: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ReconcileResponse {
    queued: bool,
    duplicate: bool,
}

async fn request_inventory_reconcile(
    State(state): State<HttpState>,
    Extension(request_id): Extension<RequestId>,
    Json(request): Json<ReconcileRequest>,
) -> Result<(StatusCode, Json<ReconcileResponse>), Problem> {
    if !request.full {
        return Err(Problem::new(
            StatusCode::BAD_REQUEST,
            "full_reconcile_required",
            "Full reconciliation required",
            "the Phase 2 administrative reconciliation must set full to true",
            request_id,
        ));
    }
    if state.inventory_stale_after.is_none() {
        return Err(Problem::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "qbittorrent_not_configured",
            "qBittorrent not configured",
            "the qBittorrent integration is not configured",
            request_id,
        ));
    }
    let queued = state
        .storage
        .request_qbit_reconcile(now_ms())
        .await
        .map_err(|_| Problem::database(request_id))?;
    Ok((
        StatusCode::ACCEPTED,
        Json(ReconcileResponse {
            queued,
            duplicate: !queued,
        }),
    ))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct InventorySearchRequest {
    source: InventorySearchSource,
    #[serde(default)]
    indexer_ids: Vec<i64>,
    #[serde(default)]
    force: bool,
    #[serde(default)]
    dry_run: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct InventorySearchSource {
    kind: String,
    #[serde(default)]
    hashes: Vec<String>,
    #[serde(default)]
    include_categories: Vec<String>,
    #[serde(default)]
    exclude_categories: Vec<String>,
    #[serde(default)]
    include_tags: Vec<String>,
    #[serde(default)]
    exclude_tags: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct InventorySearchResponse {
    operation_id: String,
    task_id: String,
    duplicate: bool,
    status: &'static str,
}

async fn start_inventory_search(
    State(state): State<HttpState>,
    Extension(request_id): Extension<RequestId>,
    Json(request): Json<InventorySearchRequest>,
) -> Result<(StatusCode, Json<InventorySearchResponse>), Problem> {
    if !state.prowlarr_configured {
        return Err(Problem::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "prowlarr_not_configured",
            "Prowlarr not configured",
            "the Prowlarr integration is not configured",
            request_id,
        ));
    }
    if !request.indexer_ids.is_empty()
        && let Some(client) = state.prowlarr_client.as_ref()
    {
        let indexers = client.indexers().await.map_err(|_| {
            Problem::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "prowlarr_unavailable",
                "Prowlarr unavailable",
                "the Prowlarr indexer projection could not be refreshed",
                request_id.clone(),
            )
        })?;
        state
            .storage
            .project_indexers(&indexers, now_ms())
            .await
            .map_err(|_| Problem::database(request_id.clone()))?;
    }
    if request.source.kind != "qbittorrent"
        || request.indexer_ids.len() > 100
        || request.source.hashes.len() > 10_000
        || request
            .source
            .hashes
            .iter()
            .any(|hash| !valid_info_hash(hash))
    {
        return Err(Problem::new(
            StatusCode::BAD_REQUEST,
            "invalid_inventory_search",
            "Invalid inventory search",
            "source selection, hashes, or indexer IDs are invalid",
            request_id,
        ));
    }
    for indexer_id in &request.indexer_ids {
        let eligible = sqlx::query_scalar::<_, i64>(
            "SELECT 1 FROM sporos_indexer WHERE prowlarr_id = ? AND eligible = 1",
        )
        .bind(indexer_id)
        .fetch_optional(state.storage.pool())
        .await
        .map_err(|_| Problem::database(request_id.clone()))?
        .is_some();
        if !eligible {
            return Err(Problem::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "indexer_ineligible",
                "Indexer is not eligible",
                "an explicitly selected Prowlarr indexer is absent or unsafe",
                request_id,
            ));
        }
    }
    let selection = BackfillSelection {
        hashes: request.source.hashes,
        include_categories: request.source.include_categories,
        exclude_categories: request.source.exclude_categories,
        include_tags: request.source.include_tags,
        exclude_tags: request.source.exclude_tags,
    };
    let accepted = accept_backfill(
        &state.storage,
        state.search_policy.clone().with_dry_run(request.dry_run),
        selection,
        request.indexer_ids,
        request.force.then_some(request_id.0.as_str()),
        now_ms(),
    )
    .await
    .map_err(|_| Problem::database(request_id))?;
    Ok((
        if accepted.duplicate {
            StatusCode::OK
        } else {
            StatusCode::ACCEPTED
        },
        Json(InventorySearchResponse {
            operation_id: encode_hex(&accepted.operation_id),
            task_id: encode_hex(&accepted.task_id),
            duplicate: accepted.duplicate,
            status: "queued",
        }),
    ))
}

fn valid_info_hash(value: &str) -> bool {
    matches!(value.len(), 40 | 64)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DataScanRequest {
    root: String,
    #[serde(default)]
    indexer_ids: Vec<i64>,
    #[serde(default)]
    force: bool,
    #[serde(default)]
    dry_run: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DataScanResponse {
    operation_id: String,
    task_id: String,
    duplicate: bool,
    status: &'static str,
}

async fn start_data_scan(
    State(state): State<HttpState>,
    Extension(request_id): Extension<RequestId>,
    Json(request): Json<DataScanRequest>,
) -> Result<(StatusCode, Json<DataScanResponse>), Problem> {
    if !state.data_roots.contains(&request.root) {
        return Err(Problem::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "data_root_not_configured",
            "Data root not configured",
            "root must name a configured data scan root",
            request_id,
        ));
    }
    if request.indexer_ids.len() > 100 {
        return Err(Problem::new(
            StatusCode::BAD_REQUEST,
            "invalid_data_scan",
            "Invalid data scan",
            "at most 100 Prowlarr indexers may be selected",
            request_id,
        ));
    }
    for indexer_id in &request.indexer_ids {
        if sqlx::query_scalar::<_, i64>(
            "SELECT 1 FROM sporos_indexer WHERE prowlarr_id = ? AND eligible = 1",
        )
        .bind(indexer_id)
        .fetch_optional(state.storage.pool())
        .await
        .map_err(|_| Problem::database(request_id.clone()))?
        .is_none()
        {
            return Err(Problem::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "indexer_ineligible",
                "Indexer is not eligible",
                "an explicitly selected Prowlarr indexer is absent or unsafe",
                request_id,
            ));
        }
    }
    let accepted = accept_data_scan(
        &state.storage,
        &request.root,
        state.search_policy.clone().with_dry_run(request.dry_run),
        request.indexer_ids,
        request.force.then_some(request_id.0.as_str()),
        now_ms(),
    )
    .await
    .map_err(|_| Problem::database(request_id))?;
    Ok((
        if accepted.duplicate {
            StatusCode::OK
        } else {
            StatusCode::ACCEPTED
        },
        Json(DataScanResponse {
            operation_id: encode_hex(&accepted.operation_id),
            task_id: encode_hex(&accepted.task_id),
            duplicate: accepted.duplicate,
            status: "queued",
        }),
    ))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OperationQuery {
    #[serde(default = "default_page_size")]
    limit: usize,
    cursor: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct OperationPage {
    items: Vec<OperationView>,
    next_cursor: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct OperationView {
    id: String,
    kind: String,
    state: String,
    produced_tasks: i64,
    completed_tasks: i64,
    failed_tasks: i64,
    created_at: i64,
    updated_at: i64,
}

async fn list_operations(
    State(state): State<HttpState>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<OperationQuery>,
) -> Result<Json<OperationPage>, Problem> {
    if !(1..=200).contains(&query.limit) {
        return Err(Problem::new(
            StatusCode::BAD_REQUEST,
            "invalid_page_size",
            "Invalid page size",
            "limit must be between 1 and 200",
            request_id,
        ));
    }
    let cursor = query
        .cursor
        .as_deref()
        .map(decode_cursor)
        .transpose()
        .map_err(|_| {
            Problem::new(
                StatusCode::BAD_REQUEST,
                "invalid_cursor",
                "Invalid cursor",
                "the pagination cursor is invalid",
                request_id.clone(),
            )
        })?;
    let fetch = i64::try_from(query.limit + 1).expect("page limit fits SQLite");
    let rows = if let Some((created_at, id)) = cursor {
        sqlx::query(
            "SELECT * FROM sporos_operation
             WHERE created_at < ? OR (created_at = ? AND id < ?)
             ORDER BY created_at DESC, id DESC LIMIT ?",
        )
        .bind(created_at)
        .bind(created_at)
        .bind(id)
        .bind(fetch)
        .fetch_all(state.storage.pool())
        .await
    } else {
        sqlx::query("SELECT * FROM sporos_operation ORDER BY created_at DESC, id DESC LIMIT ?")
            .bind(fetch)
            .fetch_all(state.storage.pool())
            .await
    }
    .map_err(|_| Problem::database(request_id.clone()))?;
    let has_more = rows.len() > query.limit;
    let rows = rows.into_iter().take(query.limit).collect::<Vec<_>>();
    let next_cursor = if has_more {
        rows.last()
            .map(encode_cursor)
            .transpose()
            .map_err(|_| Problem::database(request_id.clone()))?
    } else {
        None
    };
    let items = rows
        .into_iter()
        .map(operation_view)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| Problem::database(request_id))?;
    Ok(Json(OperationPage { items, next_cursor }))
}

async fn get_operation(
    State(state): State<HttpState>,
    Extension(request_id): Extension<RequestId>,
    Path(operation_id): Path<String>,
) -> Result<Json<OperationView>, Problem> {
    let id = parse_id(&operation_id).ok_or_else(|| {
        Problem::new(
            StatusCode::BAD_REQUEST,
            "invalid_operation_id",
            "Invalid operation ID",
            "operation ID must be 32 lowercase hexadecimal characters",
            request_id.clone(),
        )
    })?;
    let row = sqlx::query("SELECT * FROM sporos_operation WHERE id = ?")
        .bind(id.as_slice())
        .fetch_optional(state.storage.pool())
        .await
        .map_err(|_| Problem::database(request_id.clone()))?
        .ok_or_else(|| {
            Problem::new(
                StatusCode::NOT_FOUND,
                "operation_not_found",
                "Operation not found",
                "no operation has that ID",
                request_id.clone(),
            )
        })?;
    Ok(Json(
        operation_view(row).map_err(|_| Problem::database(request_id))?,
    ))
}

fn operation_view(row: sqlx::sqlite::SqliteRow) -> Result<OperationView, sqlx::Error> {
    Ok(OperationView {
        id: encode_hex(&row.try_get::<Vec<u8>, _>("id")?),
        kind: row.try_get("kind")?,
        state: row.try_get("state")?,
        produced_tasks: row.try_get("produced_tasks")?,
        completed_tasks: row.try_get("completed_tasks")?,
        failed_tasks: row.try_get("failed_tasks")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct InventoryQuery {
    #[serde(default = "default_page_size")]
    limit: usize,
    cursor: Option<String>,
    available: Option<bool>,
    complete: Option<bool>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct InventoryPage {
    items: Vec<InventoryView>,
    next_cursor: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct InventoryView {
    id: String,
    v1_hash: Option<String>,
    v2_hash: Option<String>,
    name: String,
    total_size: i64,
    amount_left: i64,
    progress_ppm: i64,
    state: String,
    category: String,
    tags: serde_json::Value,
    complete: bool,
    available: bool,
    file_manifest_version: i64,
    file_manifest_state: String,
    added_at: Option<i64>,
    completed_at: Option<i64>,
    updated_at: i64,
}

async fn list_inventory_torrents(
    State(state): State<HttpState>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<InventoryQuery>,
) -> Result<Json<InventoryPage>, Problem> {
    if !(1..=200).contains(&query.limit) {
        return Err(Problem::new(
            StatusCode::BAD_REQUEST,
            "invalid_page_size",
            "Invalid page size",
            "limit must be between 1 and 200",
            request_id,
        ));
    }
    let cursor = query
        .cursor
        .as_deref()
        .map(|value| {
            parse_id(value).ok_or_else(|| {
                Problem::new(
                    StatusCode::BAD_REQUEST,
                    "invalid_cursor",
                    "Invalid cursor",
                    "the inventory cursor must be a source ID",
                    request_id.clone(),
                )
            })
        })
        .transpose()?;
    let fetch = i64::try_from(query.limit + 1).expect("page limit fits SQLite");
    let rows = sqlx::query(
        "SELECT * FROM sporos_qbit_torrent
         WHERE (? IS NULL OR id > ?)
           AND (? IS NULL OR available = ?)
           AND (? IS NULL OR is_complete = ?)
         ORDER BY id LIMIT ?",
    )
    .bind(cursor.as_ref().map(|id| id.as_slice()))
    .bind(cursor.as_ref().map(|id| id.as_slice()))
    .bind(query.available.map(i64::from))
    .bind(query.available.map(i64::from))
    .bind(query.complete.map(i64::from))
    .bind(query.complete.map(i64::from))
    .bind(fetch)
    .fetch_all(state.storage.pool())
    .await
    .map_err(|_| Problem::database(request_id.clone()))?;
    let has_more = rows.len() > query.limit;
    let items = rows
        .into_iter()
        .take(query.limit)
        .map(inventory_view)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| Problem::database(request_id))?;
    let next_cursor = has_more
        .then(|| items.last().map(|item| item.id.clone()))
        .flatten();
    Ok(Json(InventoryPage { items, next_cursor }))
}

fn inventory_view(row: sqlx::sqlite::SqliteRow) -> Result<InventoryView, sqlx::Error> {
    let tags = serde_json::from_str(&row.try_get::<String, _>("tags_json")?)
        .map_err(|error| sqlx::Error::Decode(Box::new(error)))?;
    Ok(InventoryView {
        id: encode_hex(&row.try_get::<Vec<u8>, _>("id")?),
        v1_hash: row
            .try_get::<Option<Vec<u8>>, _>("v1_hash")?
            .map(|value| encode_hex(&value)),
        v2_hash: row
            .try_get::<Option<Vec<u8>>, _>("v2_hash")?
            .map(|value| encode_hex(&value)),
        name: row.try_get("name")?,
        total_size: row.try_get("total_size")?,
        amount_left: row.try_get("amount_left")?,
        progress_ppm: row.try_get("progress_ppm")?,
        state: row.try_get("state")?,
        category: row.try_get("category")?,
        tags,
        complete: row.try_get::<i64, _>("is_complete")? == 1,
        available: row.try_get::<i64, _>("available")? == 1,
        file_manifest_version: row.try_get("file_manifest_version")?,
        file_manifest_state: row.try_get("file_manifest_state")?,
        added_at: row.try_get("added_at")?,
        completed_at: row.try_get("completed_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn now_ms() -> i64 {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO);
    i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskQuery {
    #[serde(default = "default_page_size")]
    limit: usize,
    cursor: Option<String>,
    state: Option<String>,
}

const fn default_page_size() -> usize {
    50
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TaskPage {
    items: Vec<TaskView>,
    next_cursor: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TaskView {
    id: String,
    kind: String,
    state: String,
    projection_generation: i64,
    duroxide_instance_id: String,
    duroxide_execution_id: Option<String>,
    reason_code: Option<String>,
    created_at: i64,
    updated_at: i64,
    terminal_at: Option<i64>,
}

async fn list_tasks(
    State(state): State<HttpState>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<TaskQuery>,
) -> Result<Json<TaskPage>, Problem> {
    if !(1..=200).contains(&query.limit) {
        return Err(Problem::new(
            StatusCode::BAD_REQUEST,
            "invalid_page_size",
            "Invalid page size",
            "limit must be between 1 and 200",
            request_id,
        ));
    }
    let cursor = query
        .cursor
        .as_deref()
        .map(decode_cursor)
        .transpose()
        .map_err(|_| {
            Problem::new(
                StatusCode::BAD_REQUEST,
                "invalid_cursor",
                "Invalid cursor",
                "the pagination cursor is invalid",
                request_id.clone(),
            )
        })?;
    let fetch = i64::try_from(query.limit + 1).expect("page limit fits SQLite");
    let rows = match (cursor, query.state.as_deref()) {
        (Some((created_at, id)), Some(filter)) => {
            sqlx::query(
                "SELECT * FROM sporos_task WHERE state = ?
                 AND (created_at < ? OR (created_at = ? AND id < ?))
                 ORDER BY created_at DESC, id DESC LIMIT ?",
            )
            .bind(filter)
            .bind(created_at)
            .bind(created_at)
            .bind(id)
            .bind(fetch)
            .fetch_all(state.storage.pool())
            .await
        }
        (Some((created_at, id)), None) => {
            sqlx::query(
                "SELECT * FROM sporos_task
                 WHERE created_at < ? OR (created_at = ? AND id < ?)
                 ORDER BY created_at DESC, id DESC LIMIT ?",
            )
            .bind(created_at)
            .bind(created_at)
            .bind(id)
            .bind(fetch)
            .fetch_all(state.storage.pool())
            .await
        }
        (None, Some(filter)) => {
            sqlx::query(
                "SELECT * FROM sporos_task WHERE state = ?
                 ORDER BY created_at DESC, id DESC LIMIT ?",
            )
            .bind(filter)
            .bind(fetch)
            .fetch_all(state.storage.pool())
            .await
        }
        (None, None) => {
            sqlx::query("SELECT * FROM sporos_task ORDER BY created_at DESC, id DESC LIMIT ?")
                .bind(fetch)
                .fetch_all(state.storage.pool())
                .await
        }
    }
    .map_err(|_| Problem::database(request_id.clone()))?;
    let has_more = rows.len() > query.limit;
    let rows = rows.into_iter().take(query.limit).collect::<Vec<_>>();
    let next_cursor = if has_more {
        rows.last()
            .map(encode_cursor)
            .transpose()
            .map_err(|_| Problem::database(request_id.clone()))?
    } else {
        None
    };
    let items = rows
        .into_iter()
        .map(task_view)
        .collect::<Result<_, _>>()
        .map_err(|_| Problem::database(request_id))?;
    Ok(Json(TaskPage { items, next_cursor }))
}

async fn get_task(
    State(state): State<HttpState>,
    Extension(request_id): Extension<RequestId>,
    Path(task_id): Path<String>,
) -> Result<Json<TaskView>, Problem> {
    let id = parse_id(&task_id).ok_or_else(|| {
        Problem::new(
            StatusCode::BAD_REQUEST,
            "invalid_task_id",
            "Invalid task ID",
            "task ID must be 32 lowercase hexadecimal characters",
            request_id.clone(),
        )
    })?;
    let row = sqlx::query("SELECT * FROM sporos_task WHERE id = ?")
        .bind(id.as_slice())
        .fetch_optional(state.storage.pool())
        .await
        .map_err(|_| Problem::database(request_id.clone()))?
        .ok_or_else(|| {
            Problem::new(
                StatusCode::NOT_FOUND,
                "task_not_found",
                "Task not found",
                "no task has that ID",
                request_id.clone(),
            )
        })?;
    Ok(Json(
        task_view(row).map_err(|_| Problem::database(request_id))?,
    ))
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TaskEventView {
    sequence: i64,
    state: String,
    reason_code: Option<String>,
    detail: Option<serde_json::Value>,
    created_at: i64,
}

async fn get_task_events(
    State(state): State<HttpState>,
    Extension(request_id): Extension<RequestId>,
    Path(task_id): Path<String>,
) -> Result<Json<Vec<TaskEventView>>, Problem> {
    let id = parse_id(&task_id).ok_or_else(|| {
        Problem::new(
            StatusCode::BAD_REQUEST,
            "invalid_task_id",
            "Invalid task ID",
            "task ID must be 32 lowercase hexadecimal characters",
            request_id.clone(),
        )
    })?;
    let rows = sqlx::query(
        "SELECT sequence, state, reason_code, detail_json, created_at
         FROM sporos_task_event WHERE task_id = ? ORDER BY sequence",
    )
    .bind(id.as_slice())
    .fetch_all(state.storage.pool())
    .await
    .map_err(|_| Problem::database(request_id.clone()))?;
    if rows.is_empty() {
        return Err(Problem::new(
            StatusCode::NOT_FOUND,
            "task_not_found",
            "Task not found",
            "no task has that ID",
            request_id,
        ));
    }
    let events = rows
        .into_iter()
        .map(|row| {
            let detail = row
                .try_get::<Option<String>, _>("detail_json")?
                .map(|value| serde_json::from_str(&value))
                .transpose()
                .map_err(|error| sqlx::Error::Decode(Box::new(error)))?;
            Ok(TaskEventView {
                sequence: row.try_get("sequence")?,
                state: row.try_get("state")?,
                reason_code: row.try_get("reason_code")?,
                detail,
                created_at: row.try_get("created_at")?,
            })
        })
        .collect::<Result<Vec<_>, sqlx::Error>>()
        .map_err(|_| Problem::database(request_id))?;
    Ok(Json(events))
}

fn task_view(row: sqlx::sqlite::SqliteRow) -> Result<TaskView, sqlx::Error> {
    let id = row.try_get::<Vec<u8>, _>("id")?;
    Ok(TaskView {
        id: encode_hex(&id),
        kind: row.try_get("kind")?,
        state: row.try_get("state")?,
        projection_generation: row.try_get("projection_generation")?,
        duroxide_instance_id: row.try_get("duroxide_instance_id")?,
        duroxide_execution_id: row.try_get("duroxide_execution_id")?,
        reason_code: row.try_get("reason_code")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        terminal_at: row.try_get("terminal_at")?,
    })
}

fn encode_cursor(row: &sqlx::sqlite::SqliteRow) -> Result<String, sqlx::Error> {
    let cursor = (
        row.try_get::<i64, _>("created_at")?,
        row.try_get::<Vec<u8>, _>("id")?,
    );
    let bytes =
        serde_json::to_vec(&cursor).map_err(|error| sqlx::Error::Encode(Box::new(error)))?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

fn decode_cursor(cursor: &str) -> Result<(i64, Vec<u8>), ()> {
    let bytes = URL_SAFE_NO_PAD.decode(cursor).map_err(|_| ())?;
    let cursor: (i64, Vec<u8>) = serde_json::from_slice(&bytes).map_err(|_| ())?;
    if cursor.1.len() == 16 {
        Ok(cursor)
    } else {
        Err(())
    }
}

async fn require_admin(State(state): State<HttpState>, request: Request, next: Next) -> Response {
    authorize(&state.admin_token, request, next).await
}

pub async fn require_webhook(
    State(state): State<HttpState>,
    request: Request,
    next: Next,
) -> Response {
    authorize(&state.webhook_token, request, next).await
}

async fn authorize(expected: &Secret, request: Request, next: Next) -> Response {
    let request_id = request
        .extensions()
        .get::<RequestId>()
        .cloned()
        .unwrap_or_else(RequestId::next);
    let supplied = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    if !supplied.is_some_and(|value| token_eq(expected.expose(), value)) {
        return Problem::new(
            StatusCode::UNAUTHORIZED,
            "authentication_required",
            "Authentication required",
            "a valid bearer token is required",
            request_id,
        )
        .into_response();
    }
    next.run(request).await
}

fn token_eq(expected: &str, supplied: &str) -> bool {
    let expected = Sha256::digest(expected.as_bytes());
    let supplied = Sha256::digest(supplied.as_bytes());
    bool::from(expected.ct_eq(&supplied))
}

async fn request_id(mut request: Request, next: Next) -> Response {
    let request_id = RequestId::next();
    request.extensions_mut().insert(request_id.clone());
    let mut response = next.run(request).await;
    if let Ok(value) = HeaderValue::from_str(&request_id.0) {
        response.headers_mut().insert("x-request-id", value);
    }
    response
}

async fn observe_request(State(state): State<HttpState>, request: Request, next: Next) -> Response {
    let method = request.method().as_str().to_owned();
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map(MatchedPath::as_str)
        .unwrap_or("unmatched")
        .to_owned();
    let started = Instant::now();
    let response = next.run(request).await;
    state.metrics.observe(
        route,
        method,
        response.status().as_u16(),
        started.elapsed().as_micros() as u64,
    );
    response
}

async fn not_found(Extension(request_id): Extension<RequestId>) -> Problem {
    Problem::new(
        StatusCode::NOT_FOUND,
        "route_not_found",
        "Route not found",
        "no endpoint matches this request",
        request_id,
    )
}

#[derive(Debug, Clone)]
struct RequestId(String);

impl RequestId {
    fn next() -> Self {
        let sequence = REQUEST_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        Self(format!("req_{sequence:016x}"))
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Problem {
    #[serde(skip)]
    status_code: StatusCode,
    r#type: String,
    title: &'static str,
    status: u16,
    code: &'static str,
    detail: &'static str,
    request_id: String,
    #[serde(skip)]
    retry_after: Option<&'static str>,
}

impl Problem {
    fn new(
        status: StatusCode,
        code: &'static str,
        title: &'static str,
        detail: &'static str,
        request_id: RequestId,
    ) -> Self {
        Self {
            status_code: status,
            r#type: format!("urn:sporos:error:{code}"),
            title,
            status: status.as_u16(),
            code,
            detail,
            request_id: request_id.0,
            retry_after: None,
        }
    }

    fn with_retry_after(mut self, value: &'static str) -> Self {
        self.retry_after = Some(value);
        self
    }

    fn database(request_id: RequestId) -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "database_unavailable",
            "Database unavailable",
            "the task projection could not be read",
            request_id,
        )
    }
}

impl IntoResponse for Problem {
    fn into_response(self) -> Response {
        let status = self.status_code;
        let retry_after = self.retry_after;
        let mut response = (status, Json(self)).into_response();
        if let Some(value) = retry_after {
            response
                .headers_mut()
                .insert(RETRY_AFTER, HeaderValue::from_static(value));
        }
        response
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct MetricKey {
    route: String,
    method: String,
    status: u16,
}

#[derive(Debug)]
struct MetricValue {
    count: u64,
    duration_micros: u64,
}

#[derive(Debug)]
struct Metrics {
    started: Instant,
    http: Mutex<BTreeMap<MetricKey, MetricValue>>,
}

impl Metrics {
    fn new() -> Self {
        Self {
            started: Instant::now(),
            http: Mutex::new(BTreeMap::new()),
        }
    }

    fn observe(&self, route: String, method: String, status: u16, duration_micros: u64) {
        let mut http = self
            .http
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let value = http
            .entry(MetricKey {
                route,
                method,
                status,
            })
            .or_insert(MetricValue {
                count: 0,
                duration_micros: 0,
            });
        value.count = value.count.saturating_add(1);
        value.duration_micros = value.duration_micros.saturating_add(duration_micros);
    }

    fn render(&self) -> String {
        let http = self
            .http
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut output = String::from(
            "# TYPE sporos_build_info gauge\nsporos_build_info{version=\"0.0.0\"} 1\n\
             # TYPE sporos_process_uptime_seconds gauge\n",
        );
        output.push_str(&format!(
            "sporos_process_uptime_seconds {}\n",
            self.started.elapsed().as_secs()
        ));
        output.push_str("# TYPE sporos_http_requests_total counter\n");
        output.push_str("# TYPE sporos_http_request_duration_seconds summary\n");
        for (key, value) in http.iter() {
            let labels = format!(
                "route=\"{}\",method=\"{}\",status=\"{}\"",
                metric_escape(&key.route),
                metric_escape(&key.method),
                key.status
            );
            output.push_str(&format!(
                "sporos_http_requests_total{{{labels}}} {}\n",
                value.count
            ));
            output.push_str(&format!(
                "sporos_http_request_duration_seconds_count{{{labels}}} {}\n",
                value.count
            ));
            output.push_str(&format!(
                "sporos_http_request_duration_seconds_sum{{{labels}}} {:.6}\n",
                value.duration_micros as f64 / 1_000_000.0
            ));
        }
        output
    }
}

fn metric_escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('"', "\\\"")
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn parse_id(value: &str) -> Option<[u8; 16]> {
    if value.len() != 32
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return None;
    }
    let mut id = [0; 16];
    let (pairs, remainder) = value.as_bytes().as_chunks::<2>();
    debug_assert!(remainder.is_empty());
    for (index, pair) in pairs.iter().enumerate() {
        id[index] = (hex_value(pair[0])? << 4) | hex_value(pair[1])?;
    }
    Some(id)
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use axum::body::{Body, to_bytes};
    use axum::http::Request as HttpRequest;
    use axum::routing::post;
    use serde_json::Value;
    use tempfile::TempDir;
    use tower::ServiceExt;

    use super::*;
    use crate::durable_ingress::{NewTask, PolicySnapshot};
    use crate::engine::{FAKE_TASK_NAME, FAKE_TASK_VERSION, FakeTaskInput};
    use crate::inventory::{InventoryChange, InventoryDelta};
    use sporos_model::{PolicySnapshotId, TaskId, TaskKey};

    #[tokio::test]
    async fn separates_admin_and_webhook_authentication() {
        let (_directory, state) = test_state().await;
        let app = test_router(state);

        assert_eq!(
            request(&app, "/api/v1/admin/tasks", Some("webhook"), "")
                .await
                .0,
            401
        );
        assert_eq!(
            request(&app, "/api/v1/admin/tasks", Some("admin"), "")
                .await
                .0,
            200
        );
        assert_eq!(
            request(&app, "/_test/webhook", Some("admin"), r#"{"marker":1}"#)
                .await
                .0,
            401
        );
        assert_eq!(
            request(&app, "/_test/webhook", Some("webhook"), r#"{"marker":1}"#)
                .await
                .0,
            202
        );
    }

    #[tokio::test]
    async fn rejects_authentication_before_decoding_the_body() {
        let (_directory, state) = test_state().await;
        let app = test_router(state);
        let (status, body) = request(&app, "/_test/webhook", None, "not-json").await;

        assert_eq!(status, 401);
        assert_eq!(body["code"], "authentication_required");
    }

    #[tokio::test]
    async fn duplicate_webhook_requests_return_the_existing_task() {
        let (_directory, state) = test_state().await;
        let app = test_router(state);
        let first = request(&app, "/_test/webhook", Some("webhook"), r#"{"marker":7}"#).await;
        let second = request(&app, "/_test/webhook", Some("webhook"), r#"{"marker":7}"#).await;

        assert_eq!(first.0, 202);
        assert_eq!(second.0, 200);
        assert_eq!(first.1["taskId"], second.1["taskId"]);
        assert_eq!(second.1["duplicate"], true);
    }

    #[tokio::test]
    async fn autobrr_preflight_is_provisional_and_inventory_bounded() {
        let (_directory, mut state) = test_state().await;
        state.inventory_stale_after = Some(Duration::from_secs(300));
        state
            .storage
            .project_qbit_batch(
                &[InventoryChange::Upsert {
                    qbit_id: "a".repeat(40),
                    delta: Box::new(InventoryDelta {
                        infohash_v1: Some("a".repeat(40)),
                        name: Some("Example.Show.S01E02.1080p".to_owned()),
                        total_size: Some(1_000),
                        amount_left: Some(100),
                        progress: Some(0.9),
                        state: Some("downloading".to_owned()),
                        save_path: Some("/downloads".to_owned()),
                        content_path: Some("/downloads/example".to_owned()),
                        category: Some(String::new()),
                        tags: Some(String::new()),
                        added_on: Some(1),
                        completion_on: Some(0),
                        ..InventoryDelta::default()
                    }),
                }],
                1,
                false,
                now_ms(),
            )
            .await
            .expect("project preflight source");
        state
            .storage
            .finish_qbit_sync(1, Some(1), now_ms())
            .await
            .expect("finish inventory baseline");
        let app = test_router(state);

        let accepted = request(
            &app,
            "/api/v1/autobrr/check",
            Some("webhook"),
            r#"{"torrentName":"Example.Show.S01E02.2160p","size":1010,"indexer":"tracker"}"#,
        )
        .await;
        assert_eq!(accepted.0, 200);
        assert_eq!(accepted.1["decision"], "accept");
        assert_eq!(accepted.1["provisional"], true);
        assert_eq!(accepted.1["sourceState"], "downloading");

        let rejected = request(
            &app,
            "/api/v1/autobrr/check",
            Some("webhook"),
            r#"{"torrentName":"Other.Show.S01E02"}"#,
        )
        .await;
        assert_eq!(rejected.0, 404);
        assert_eq!(rejected.1["code"], "no_match");
    }

    #[tokio::test]
    async fn autobrr_upload_commits_before_acknowledgement_and_deduplicates() {
        let (_directory, state) = test_state().await;
        let app = test_router(state.clone());
        let torrent = format!(
            "d4:infod6:lengthi13e4:name18:Example.Movie.202412:piece lengthi16384e6:pieces20:{}ee",
            "a".repeat(20)
        );
        let body = serde_json::json!({
            "torrentData": STANDARD.encode(torrent.as_bytes()),
            "torrentName": "Example.Movie.2024.1080p",
            "indexer": "tracker",
            "dryRun": true
        })
        .to_string();

        let first = request(&app, "/api/v1/autobrr/torrents", Some("webhook"), &body).await;
        let second = request(&app, "/api/v1/autobrr/torrents", Some("webhook"), &body).await;

        assert_eq!(first.0, 202);
        assert_eq!(second.0, 200);
        assert_eq!(first.1["candidateId"], second.1["candidateId"]);
        assert_eq!(first.1["taskId"], second.1["taskId"]);
        assert_eq!(second.1["duplicate"], true);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM sporos_blob")
                .fetch_one(state.storage.pool())
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM sporos_candidate_provenance")
                .fetch_one(state.storage.pool())
                .await
                .unwrap(),
            2
        );
    }

    #[tokio::test]
    async fn autobrr_upload_rejects_noncanonical_and_malformed_torrents() {
        let (_directory, state) = test_state().await;
        let app = test_router(state);
        let invalid_base64 = request(
            &app,
            "/api/v1/autobrr/torrents",
            Some("webhook"),
            r#"{"torrentData":"not base64"}"#,
        )
        .await;
        assert_eq!(invalid_base64.0, 400);
        assert_eq!(invalid_base64.1["code"], "invalid_torrent_data");

        let malformed = serde_json::json!({
            "torrentData": STANDARD.encode(b"not a torrent")
        })
        .to_string();
        let malformed = request(
            &app,
            "/api/v1/autobrr/torrents",
            Some("webhook"),
            &malformed,
        )
        .await;
        assert_eq!(malformed.0, 422);
        assert_eq!(malformed.1["code"], "invalid_torrent");
    }

    #[tokio::test]
    async fn autobrr_upload_authenticates_before_body_admission() {
        let (_directory, state) = test_state().await;
        let app = test_router(state);
        let response = app
            .oneshot(
                HttpRequest::post("/api/v1/autobrr/torrents")
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(vec![b'x'; 12 * 1024 * 1024 + 1]))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn exposes_stable_health_and_metric_names() {
        let (_directory, state) = test_state().await;
        let app = test_router(state.clone());
        assert_eq!(request(&app, "/livez", None, "").await.0, 200);
        assert_eq!(request(&app, "/readyz", None, "").await.0, 503);
        state.set_ready(true);
        assert_eq!(request(&app, "/readyz", None, "").await.0, 200);

        let response = app
            .clone()
            .oneshot(HttpRequest::get("/metrics").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let body = String::from_utf8(
            to_bytes(response.into_body(), 1024 * 1024)
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap();
        for name in [
            "sporos_build_info",
            "sporos_http_requests_total",
            "sporos_outbox_depth",
            "sporos_tasks",
            "sporos_qbit_inventory_torrents",
            "sporos_qbit_inventory_files",
            "sporos_qbit_inventory_last_success_timestamp_seconds",
        ] {
            assert!(body.contains(name), "missing metric {name}");
        }
    }

    #[tokio::test]
    async fn exposes_inventory_status_and_coalesces_reconcile_requests() {
        let (_directory, mut state) = test_state().await;
        state.inventory_stale_after = Some(Duration::from_secs(300));
        let app = test_router(state);

        let status = request(&app, "/api/v1/admin/inventory", Some("admin"), "").await;
        assert_eq!(status.0, 200);
        assert_eq!(status.1["configured"], true);
        assert_eq!(status.1["baselineComplete"], false);
        let first = request(
            &app,
            "/api/v1/admin/inventory/reconcile",
            Some("admin"),
            r#"{"full":true}"#,
        )
        .await;
        let duplicate = request(
            &app,
            "/api/v1/admin/inventory/reconcile",
            Some("admin"),
            r#"{"full":true}"#,
        )
        .await;
        assert_eq!(first.0, 202);
        assert_eq!(first.1["queued"], true);
        assert_eq!(duplicate.0, 202);
        assert_eq!(duplicate.1["duplicate"], true);

        let page = request(
            &app,
            "/api/v1/admin/inventory/torrents?limit=25",
            Some("admin"),
            "",
        )
        .await;
        assert_eq!(page.0, 200);
        assert_eq!(page.1["items"], serde_json::json!([]));
    }

    #[derive(Debug, Deserialize)]
    struct FakeRequest {
        marker: u8,
    }

    #[derive(Debug, Serialize)]
    #[serde(rename_all = "camelCase")]
    struct FakeResponse {
        task_id: String,
        duplicate: bool,
    }

    async fn fake_webhook(
        State(state): State<HttpState>,
        Json(request): Json<FakeRequest>,
    ) -> impl IntoResponse {
        let task = fake_task(request.marker);
        match state.storage.accept_task(&task).await {
            Ok(accepted) => (
                if accepted.duplicate {
                    StatusCode::OK
                } else {
                    StatusCode::ACCEPTED
                },
                Json(FakeResponse {
                    task_id: encode_hex(accepted.id.as_bytes()),
                    duplicate: accepted.duplicate,
                }),
            )
                .into_response(),
            Err(_) => StatusCode::INSUFFICIENT_STORAGE.into_response(),
        }
    }

    fn test_router(state: HttpState) -> Router {
        router(state.clone(), 1024 * 1024).merge(
            Router::new()
                .route("/_test/webhook", post(fake_webhook))
                .route_layer(middleware::from_fn_with_state(
                    state.clone(),
                    require_webhook,
                ))
                .with_state(state),
        )
    }

    async fn test_state() -> (TempDir, HttpState) {
        let directory = TempDir::new().expect("create temporary directory");
        let storage = Arc::new(
            Storage::open(
                directory.path().join("sporos.lock"),
                directory.path().join("sporos.db"),
            )
            .await
            .expect("open storage"),
        );
        let state = HttpState {
            storage,
            webhook_token: Secret::new("webhook"),
            admin_token: Secret::new("admin"),
            readiness: Arc::new(AtomicBool::new(false)),
            metrics: Arc::new(Metrics::new()),
            inventory_stale_after: None,
            source_filters: SourceFilters::default(),
            matching: Matching::default(),
            candidate_ingress: Arc::new(CandidateIngress::new(
                Matching::default(),
                SourceFilters::default(),
                crate::config::Injection::default(),
                crate::config::Paths::default(),
            )),
            search_policy: SearchPolicy::new(
                Matching::default(),
                SourceFilters::default(),
                crate::config::Injection::default(),
                crate::config::Paths::default(),
            ),
            prowlarr_configured: false,
            prowlarr_client: None,
            data_roots: Default::default(),
            upload_permits: Arc::new(Semaphore::new(4)),
            autobrr_body_limit_bytes: 12 * 1024 * 1024,
        };
        (directory, state)
    }

    async fn request(app: &Router, path: &str, token: Option<&str>, body: &str) -> (u16, Value) {
        let mut request = if body.is_empty() {
            HttpRequest::get(path)
        } else {
            HttpRequest::post(path).header(CONTENT_TYPE, "application/json")
        };
        if let Some(token) = token {
            request = request.header(AUTHORIZATION, format!("Bearer {token}"));
        }
        let response = app
            .clone()
            .oneshot(request.body(Body::from(body.to_owned())).unwrap())
            .await
            .unwrap();
        let status = response.status().as_u16();
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let body = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap()
        };
        (status, body)
    }

    fn fake_task(marker: u8) -> NewTask {
        NewTask {
            id: TaskId::from_bytes([marker; 16]),
            key: TaskKey::from_bytes([marker; 32]),
            kind: "fake".to_owned(),
            policy: PolicySnapshot {
                id: PolicySnapshotId::from_bytes([marker; 16]),
                config_hash: [marker; 32],
                matcher_version: "phase1".to_owned(),
                payload_json: "{}".to_owned(),
                created_at: 1,
            },
            orchestration_name: FAKE_TASK_NAME.to_owned(),
            orchestration_version: FAKE_TASK_VERSION.to_owned(),
            instance_id: format!("fake-v1:{marker}"),
            input_json: serde_json::to_string(&FakeTaskInput {
                task_id: [marker; 16],
                accepted_at_ms: 1,
                delay_ms: 1,
            })
            .expect("encode fake task"),
            created_at: 1,
        }
    }
}
