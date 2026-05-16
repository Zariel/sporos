use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::{HeaderMap, Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use tracing::{Instrument, debug_span, info_span};

use crate::announce::{
    AnnounceDedupeIdentity, AnnounceFetchMaterial, AnnounceQueueConfig, AnnounceReason,
    AnnounceStatus, AnnounceWorkId, AnnounceWorkItem,
};
use crate::domain::{ByteSize, CandidateGuid, DownloadUrl, ItemTitle, JobName, TrackerName};
use crate::errors::DatabaseError;
use crate::metrics::{
    HttpMethod, HttpRoute, MetricsRegistry, MetricsSnapshot, WorkflowMetric, WorkflowOutcome,
};
use crate::persistence::repository::{AnnounceInsertResult, AnnounceQueueSnapshot, Repository};
use crate::runtime::announce_worker::unix_time_ms;
use crate::runtime::health::{DependencyHealthSnapshot, HealthRegistry};
use crate::runtime::queue::{BoundedWorkQueue, EnqueueError};
use crate::secrets::CookieSecret;

const WORKFLOW_BODY_LIMIT_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone)]
pub struct HttpState {
    readiness: Arc<RwLock<ReadinessState>>,
    health: HealthRegistry,
    workflow_queues: Option<WorkflowQueues>,
    job_queue: Option<BoundedWorkQueue<JobRunWorkflowRequest>>,
    allowed_jobs: Option<Arc<BTreeSet<JobName>>>,
    announce_acceptor: Option<AnnounceAcceptor>,
    api_auth: Option<ApiAuth>,
    request_timeout: Duration,
    metrics: MetricsRegistry,
}

impl HttpState {
    pub fn new(readiness: ReadinessState, health: HealthRegistry) -> Self {
        Self {
            readiness: Arc::new(RwLock::new(readiness)),
            health,
            workflow_queues: None,
            job_queue: None,
            allowed_jobs: None,
            announce_acceptor: None,
            api_auth: None,
            request_timeout: Duration::from_secs(5),
            metrics: MetricsRegistry::new(),
        }
    }

    pub fn with_workflow_queues(mut self, workflow_queues: WorkflowQueues) -> Self {
        self.workflow_queues = Some(workflow_queues);
        self
    }

    pub fn with_job_queue(mut self, job_queue: BoundedWorkQueue<JobRunWorkflowRequest>) -> Self {
        self.job_queue = Some(job_queue);
        self
    }

    pub fn with_allowed_jobs(mut self, allowed_jobs: BTreeSet<JobName>) -> Self {
        self.allowed_jobs = Some(Arc::new(allowed_jobs));
        self
    }

    pub fn with_announce_acceptor(
        mut self,
        repository: Repository,
        config: AnnounceQueueConfig,
    ) -> Self {
        self.announce_acceptor = Some(AnnounceAcceptor { repository, config });
        self
    }

    pub fn with_api_token(mut self, token: impl Into<String>) -> Self {
        self.api_auth = Some(ApiAuth {
            bearer_token: Arc::from(token.into()),
        });
        self
    }

    pub fn with_request_timeout(mut self, request_timeout: Duration) -> Self {
        self.request_timeout = request_timeout;
        self
    }

    pub fn with_metrics(mut self, metrics: MetricsRegistry) -> Self {
        self.metrics = metrics;
        self
    }

    pub fn set_readiness(&self, readiness: ReadinessState) {
        let mut current = self
            .readiness
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *current = readiness;
    }

    pub(crate) fn readiness(&self) -> ReadinessState {
        self.readiness
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn dependency_health(&self) -> DependencyHealthSnapshot {
        self.health.snapshot()
    }

    fn workflow_queues(&self) -> Result<&WorkflowQueues, ApiErrorResponse> {
        self.workflow_queues
            .as_ref()
            .ok_or(ApiErrorResponse::service_unavailable(
                "workflow queues are not running",
            ))
    }

    fn job_queue(&self) -> Result<&BoundedWorkQueue<JobRunWorkflowRequest>, ApiErrorResponse> {
        self.workflow_queues
            .as_ref()
            .map(|queues| &queues.jobs)
            .or(self.job_queue.as_ref())
            .ok_or(ApiErrorResponse::service_unavailable(
                "job workflow queue is not running",
            ))
    }

    fn accepts_job(&self, job_name: &JobName) -> bool {
        self.allowed_jobs
            .as_ref()
            .is_none_or(|allowed| allowed.contains(job_name))
    }

    fn queue_stats(&self) -> Vec<crate::runtime::queue::QueueStats> {
        if let Some(workflow_queues) = self.workflow_queues.as_ref() {
            return workflow_queues.stats();
        }
        self.job_queue
            .as_ref()
            .map(|queue| vec![queue.stats()])
            .unwrap_or_default()
    }
}

#[derive(Debug, Clone)]
struct AnnounceAcceptor {
    repository: Repository,
    config: AnnounceQueueConfig,
}

#[derive(Debug, Clone)]
pub struct WorkflowQueues {
    pub announcements: BoundedWorkQueue<AnnouncementWorkflowRequest>,
    pub searches: BoundedWorkQueue<SearchWorkflowRequest>,
    pub jobs: BoundedWorkQueue<JobRunWorkflowRequest>,
}

impl WorkflowQueues {
    fn stats(&self) -> Vec<crate::runtime::queue::QueueStats> {
        vec![
            self.announcements.stats(),
            self.searches.stats(),
            self.jobs.stats(),
        ]
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AnnouncementWorkflowRequest {
    pub title: ItemTitle,
    pub guid: CandidateGuid,
    pub download_url: DownloadUrl,
    pub tracker: TrackerName,
    pub cookie: Option<String>,
    pub size: Option<ByteSize>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SearchWorkflowRequest {
    pub query: ItemTitle,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct JobRunWorkflowRequest {
    pub job_name: JobName,
}

#[derive(Clone)]
struct ApiAuth {
    bearer_token: Arc<str>,
}

impl fmt::Debug for ApiAuth {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ApiAuth")
            .field("bearer_token", &"<redacted>")
            .finish()
    }
}

impl ApiAuth {
    fn authorizes(&self, headers: &HeaderMap) -> bool {
        let expected = format!("Bearer {}", self.bearer_token);
        headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            == Some(expected.as_str())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ReadinessState {
    pub config_loaded: bool,
    pub database_available: bool,
    pub schema_initialized: bool,
    pub state_paths_writable: bool,
    pub workers_running: bool,
}

impl ReadinessState {
    pub const fn ready() -> Self {
        Self {
            config_loaded: true,
            database_available: true,
            schema_initialized: true,
            state_paths_writable: true,
            workers_running: true,
        }
    }

    pub const fn is_ready(&self) -> bool {
        self.config_loaded
            && self.database_available
            && self.schema_initialized
            && self.state_paths_writable
            && self.workers_running
    }
}

#[derive(Debug, Serialize)]
struct LivenessResponse {
    status: &'static str,
}

#[derive(Debug, Serialize)]
struct ReadinessResponse {
    status: &'static str,
    accepting_work: bool,
    processing_ready: bool,
    checks: ReadinessChecks,
    dependencies: BTreeMap<String, String>,
}

#[derive(Debug, Serialize)]
struct StatusResponse {
    status: &'static str,
    readiness: ReadinessResponse,
    announce_queue: Option<AnnounceQueueStatusResponse>,
    announce_queue_error: Option<&'static str>,
}

#[derive(Debug, Serialize)]
struct AnnounceQueueStatusResponse {
    active_count: i64,
    max_pending: u32,
    worker_capacity: u16,
    worker_busy: i64,
    worker_idle: i64,
    oldest_active_age_ms: Option<i64>,
    next_retry_delay_ms: Option<i64>,
    running_leases: i64,
    statuses: Vec<AnnounceStatusCountResponse>,
    attempts: Vec<AnnounceAttemptCountResponse>,
    dependency_waits: Vec<AnnounceDependencyWaitResponse>,
}

#[derive(Debug, Serialize)]
struct AnnounceStatusCountResponse {
    status: String,
    reason: String,
    count: i64,
}

#[derive(Debug, Serialize)]
struct AnnounceAttemptCountResponse {
    outcome_class: String,
    attempts: i64,
}

#[derive(Debug, Serialize)]
struct AnnounceDependencyWaitResponse {
    dependency_kind: String,
    dependency_name: String,
    count: i64,
}

#[derive(Debug, Serialize)]
struct ReadinessChecks {
    config_loaded: bool,
    database_available: bool,
    schema_initialized: bool,
    state_paths_writable: bool,
    workers_running: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AnnouncementRequestDto {
    name: String,
    guid: String,
    download_url: String,
    tracker: String,
    cookie: Option<String>,
    size: Option<u64>,
}

impl AnnouncementRequestDto {
    fn try_into_workflow(self) -> Result<AnnouncementWorkflowRequest, ApiErrorResponse> {
        Ok(AnnouncementWorkflowRequest {
            title: ItemTitle::new(self.name).map_err(|error| {
                ApiErrorResponse::unprocessable(format!("invalid name: {error}"))
            })?,
            guid: CandidateGuid::new(self.guid).map_err(|error| {
                ApiErrorResponse::unprocessable(format!("invalid guid: {error}"))
            })?,
            download_url: DownloadUrl::new(self.download_url).map_err(|error| {
                ApiErrorResponse::unprocessable(format!("invalid download_url: {error}"))
            })?,
            tracker: TrackerName::new(self.tracker).map_err(|error| {
                ApiErrorResponse::unprocessable(format!("invalid tracker: {error}"))
            })?,
            cookie: self.cookie,
            size: self.size.map(ByteSize::new),
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchRequestDto {
    query: String,
}

impl SearchRequestDto {
    fn try_into_workflow(self) -> Result<SearchWorkflowRequest, ApiErrorResponse> {
        Ok(SearchWorkflowRequest {
            query: ItemTitle::new(self.query).map_err(|error| {
                ApiErrorResponse::unprocessable(format!("invalid query: {error}"))
            })?,
        })
    }
}

#[derive(Debug, Serialize)]
struct WorkflowAcceptedResponse {
    status: &'static str,
    workflow: &'static str,
}

#[derive(Debug, Serialize)]
struct AnnouncementAcceptedResponse {
    id: String,
    status: &'static str,
    deduplicated: bool,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum WorkflowKind {
    Search,
    JobRun,
}

impl WorkflowKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Search => "search",
            Self::JobRun => "job_run",
        }
    }

    const fn metric(self) -> WorkflowMetric {
        match self {
            Self::Search => WorkflowMetric::Search,
            Self::JobRun => WorkflowMetric::JobRun,
        }
    }
}

#[derive(Debug, Serialize)]
struct ApiErrorBody {
    error: ApiErrorDetail,
}

#[derive(Debug, Serialize)]
struct ApiErrorDetail {
    code: &'static str,
    message: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct ApiErrorResponse {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl ApiErrorResponse {
    fn unauthorized(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            code: "unauthorized",
            message: message.into(),
        }
    }

    fn unprocessable(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            code: "invalid_request",
            message: message.into(),
        }
    }

    fn service_unavailable(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "service_unavailable",
            message: message.into(),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: "not_found",
            message: message.into(),
        }
    }

    fn timeout(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::REQUEST_TIMEOUT,
            code: "request_timeout",
            message: message.into(),
        }
    }
}

impl IntoResponse for ApiErrorResponse {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ApiErrorBody {
                error: ApiErrorDetail {
                    code: self.code,
                    message: self.message,
                },
            }),
        )
            .into_response()
    }
}

pub fn router(state: HttpState) -> Router {
    let workflow_routes = Router::new()
        .route("/v1/announcements", post(post_announcement))
        .route("/v1/searches", post(post_search))
        .route("/v1/jobs/{job_name}/runs", post(post_job_run))
        .layer(DefaultBodyLimit::max(WORKFLOW_BODY_LIMIT_BYTES))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            timeout_middleware,
        ));

    Router::new()
        .route("/livez", get(livez))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        .route("/v1/status", get(status))
        .merge(workflow_routes)
        .with_state(state)
}

async fn livez(State(state): State<HttpState>) -> impl IntoResponse {
    state
        .metrics
        .record_http_request(HttpMethod::Get, HttpRoute::Livez, StatusCode::OK.as_u16());
    (StatusCode::OK, Json(LivenessResponse { status: "live" }))
}

async fn readyz(State(state): State<HttpState>) -> impl IntoResponse {
    let readiness = readiness_response(&state);
    let status = if readiness.status == "ready" {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    state
        .metrics
        .record_http_request(HttpMethod::Get, HttpRoute::Readyz, status.as_u16());
    (status, Json(readiness))
}

async fn metrics(State(state): State<HttpState>) -> impl IntoResponse {
    let snapshot = metrics_snapshot(&state).await;
    let body = state.metrics.render_prometheus(&snapshot);
    state
        .metrics
        .record_http_request(HttpMethod::Get, HttpRoute::Metrics, StatusCode::OK.as_u16());
    (
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        body,
    )
}

async fn status(State(state): State<HttpState>) -> impl IntoResponse {
    let readiness = readiness_response(&state);
    let (announce_queue, announce_queue_error) = announce_queue_status(&state).await;
    state
        .metrics
        .record_http_request(HttpMethod::Get, HttpRoute::Status, StatusCode::OK.as_u16());
    (
        StatusCode::OK,
        Json(StatusResponse {
            status: "ok",
            readiness,
            announce_queue,
            announce_queue_error,
        }),
    )
}

async fn post_announcement(
    State(state): State<HttpState>,
    Json(request): Json<AnnouncementRequestDto>,
) -> Response {
    let request = match request.try_into_workflow() {
        Ok(request) => request,
        Err(error) => {
            state
                .metrics
                .record_workflow_enqueue(WorkflowMetric::Announcement, WorkflowOutcome::Invalid);
            state.metrics.record_http_request(
                HttpMethod::Post,
                HttpRoute::Announcements,
                error.status.as_u16(),
            );
            return error.into_response();
        }
    };
    let span = info_span!(
        "http.announcement",
        tracker = %request.tracker,
        candidate_guid = %request.guid
    );
    if let Some(acceptor) = state.announce_acceptor.as_ref() {
        let response = accept_announcement(&state.metrics, acceptor, request)
            .instrument(span)
            .await;
        state.metrics.record_http_request(
            HttpMethod::Post,
            HttpRoute::Announcements,
            response.status().as_u16(),
        );
        return response;
    }

    let _entered = span.enter();
    state.metrics.record_workflow_enqueue(
        WorkflowMetric::Announcement,
        WorkflowOutcome::RejectedClosed,
    );
    let error = ApiErrorResponse::service_unavailable("durable announce queue is not running");
    state.metrics.record_http_request(
        HttpMethod::Post,
        HttpRoute::Announcements,
        error.status.as_u16(),
    );
    error.into_response()
}

async fn accept_announcement(
    metrics: &MetricsRegistry,
    acceptor: &AnnounceAcceptor,
    request: AnnouncementWorkflowRequest,
) -> Response {
    let work = match announcement_work_item(request, acceptor.config.default_ttl_secs) {
        Ok(work) => work,
        Err(error) => {
            metrics.record_workflow_enqueue(WorkflowMetric::Announcement, WorkflowOutcome::Invalid);
            return error.into_response();
        }
    };

    match acceptor
        .repository
        .insert_or_dedupe_announce_work(&work, acceptor.config.max_pending)
        .await
    {
        Ok(AnnounceInsertResult::Inserted { id }) => {
            metrics.record_workflow_enqueue(
                WorkflowMetric::Announcement,
                WorkflowOutcome::DurableAccepted,
            );
            announcement_accepted(id, false)
        }
        Ok(AnnounceInsertResult::Deduplicated { id }) => {
            metrics.record_workflow_enqueue(
                WorkflowMetric::Announcement,
                WorkflowOutcome::DurableDeduplicated,
            );
            announcement_accepted(id, true)
        }
        Err(DatabaseError::Busy { .. }) => {
            metrics.record_workflow_enqueue(
                WorkflowMetric::Announcement,
                WorkflowOutcome::DurableCapacity,
            );
            ApiErrorResponse::service_unavailable("announce queue is at durable capacity")
                .into_response()
        }
        Err(error) => {
            metrics.record_workflow_enqueue(
                WorkflowMetric::Announcement,
                WorkflowOutcome::RejectedClosed,
            );
            ApiErrorResponse::service_unavailable(format!(
                "cannot durably accept announcement: {error}"
            ))
            .into_response()
        }
    }
}

fn announcement_accepted(id: AnnounceWorkId, deduplicated: bool) -> Response {
    (
        StatusCode::ACCEPTED,
        Json(AnnouncementAcceptedResponse {
            id: id.to_string(),
            status: "queued",
            deduplicated,
        }),
    )
        .into_response()
}

fn announcement_work_item(
    request: AnnouncementWorkflowRequest,
    ttl_secs: u64,
) -> Result<AnnounceWorkItem, ApiErrorResponse> {
    let cookie = request
        .cookie
        .map(CookieSecret::new)
        .transpose()
        .map_err(|error| ApiErrorResponse::unprocessable(format!("invalid cookie: {error}")))?;
    let fetch = AnnounceFetchMaterial::new(&request.download_url, cookie).map_err(|error| {
        ApiErrorResponse::unprocessable(format!("invalid fetch material: {error}"))
    })?;
    let now_ms = unix_time_ms();
    let ttl_ms = i64::try_from(ttl_secs.saturating_mul(1_000)).unwrap_or(i64::MAX);
    let expires_at_ms = now_ms.saturating_add(ttl_ms);
    let dedupe_hash = AnnounceDedupeIdentity::Guid {
        tracker: request.tracker.clone(),
        guid: request.guid.clone(),
    }
    .hash();
    let id_suffix = dedupe_hash.as_str().chars().take(12).collect::<String>();
    let id = AnnounceWorkId::new(format!("ann_{now_ms}_{id_suffix}")).map_err(|error| {
        ApiErrorResponse::service_unavailable(format!("cannot create announce work id: {error}"))
    })?;

    Ok(AnnounceWorkItem {
        id,
        status: AnnounceStatus::Queued,
        reason: AnnounceReason::Accepted,
        dedupe_hash,
        title: request.title,
        tracker: request.tracker,
        guid: Some(request.guid),
        info_hash: None,
        size: request.size,
        fetch: Some(fetch),
        received_at_ms: now_ms,
        updated_at_ms: now_ms,
        first_attempt_at_ms: None,
        finished_at_ms: None,
        attempt_count: 0,
        next_attempt_at_ms: now_ms,
        expires_at_ms,
        lease: None,
        last_dependency_kind: None,
        last_dependency_name: None,
        last_error_class: None,
        last_redacted_message: None,
    })
}

async fn post_search(
    State(state): State<HttpState>,
    Json(request): Json<SearchRequestDto>,
) -> Response {
    let request = match request.try_into_workflow() {
        Ok(request) => request,
        Err(error) => {
            state
                .metrics
                .record_workflow_enqueue(WorkflowMetric::Search, WorkflowOutcome::Invalid);
            state.metrics.record_http_request(
                HttpMethod::Post,
                HttpRoute::Searches,
                error.status.as_u16(),
            );
            return error.into_response();
        }
    };
    let _span = debug_span!("http.search", query = %request.query);
    let queues = match state.workflow_queues() {
        Ok(queues) => queues,
        Err(error) => {
            state
                .metrics
                .record_workflow_enqueue(WorkflowMetric::Search, WorkflowOutcome::RejectedClosed);
            state.metrics.record_http_request(
                HttpMethod::Post,
                HttpRoute::Searches,
                error.status.as_u16(),
            );
            return error.into_response();
        }
    };

    let response = enqueue_work(
        &state.metrics,
        queues.searches.try_enqueue(request),
        WorkflowKind::Search,
    );
    state.metrics.record_http_request(
        HttpMethod::Post,
        HttpRoute::Searches,
        response.status().as_u16(),
    );
    response
}

async fn post_job_run(State(state): State<HttpState>, Path(job_name): Path<String>) -> Response {
    let request = match JobName::new(job_name) {
        Ok(job_name) => JobRunWorkflowRequest { job_name },
        Err(error) => {
            state
                .metrics
                .record_workflow_enqueue(WorkflowMetric::JobRun, WorkflowOutcome::Invalid);
            let error = ApiErrorResponse::unprocessable(format!("invalid job name: {error}"));
            state.metrics.record_http_request(
                HttpMethod::Post,
                HttpRoute::JobRuns,
                error.status.as_u16(),
            );
            return error.into_response();
        }
    };
    if !state.accepts_job(&request.job_name) {
        state
            .metrics
            .record_workflow_enqueue(WorkflowMetric::JobRun, WorkflowOutcome::Invalid);
        let error = ApiErrorResponse::not_found("unknown scheduled job");
        state.metrics.record_http_request(
            HttpMethod::Post,
            HttpRoute::JobRuns,
            error.status.as_u16(),
        );
        return error.into_response();
    }
    let _span = info_span!("http.job_run", job_name = %request.job_name);
    let queue = match state.job_queue() {
        Ok(queue) => queue,
        Err(error) => {
            state
                .metrics
                .record_workflow_enqueue(WorkflowMetric::JobRun, WorkflowOutcome::RejectedClosed);
            state.metrics.record_http_request(
                HttpMethod::Post,
                HttpRoute::JobRuns,
                error.status.as_u16(),
            );
            return error.into_response();
        }
    };

    let response = enqueue_work(
        &state.metrics,
        queue.try_enqueue(request),
        WorkflowKind::JobRun,
    );
    state.metrics.record_http_request(
        HttpMethod::Post,
        HttpRoute::JobRuns,
        response.status().as_u16(),
    );
    response
}

async fn auth_middleware(
    State(state): State<HttpState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    if state
        .api_auth
        .as_ref()
        .is_some_and(|auth| !auth.authorizes(request.headers()))
    {
        return ApiErrorResponse::unauthorized("missing or invalid bearer token").into_response();
    }

    next.run(request).await
}

async fn timeout_middleware(
    State(state): State<HttpState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    if state.request_timeout.is_zero() {
        return ApiErrorResponse::timeout("request timed out").into_response();
    }
    match tokio::time::timeout(state.request_timeout, next.run(request)).await {
        Ok(response) => response,
        Err(_elapsed) => ApiErrorResponse::timeout("request timed out").into_response(),
    }
}

fn enqueue_work<T>(
    metrics: &MetricsRegistry,
    result: Result<(), EnqueueError<T>>,
    kind: WorkflowKind,
) -> Response {
    match result {
        Ok(()) => {
            metrics.record_workflow_enqueue(kind.metric(), WorkflowOutcome::Accepted);
            (
                StatusCode::ACCEPTED,
                Json(WorkflowAcceptedResponse {
                    status: "queued",
                    workflow: kind.as_str(),
                }),
            )
                .into_response()
        }
        Err(EnqueueError::Full { .. }) => {
            metrics.record_workflow_enqueue(kind.metric(), WorkflowOutcome::RejectedFull);
            ApiErrorResponse::service_unavailable("workflow queue is full").into_response()
        }
        Err(EnqueueError::Closed { .. }) => {
            metrics.record_workflow_enqueue(kind.metric(), WorkflowOutcome::RejectedClosed);
            ApiErrorResponse::service_unavailable("workflow queue is closed").into_response()
        }
    }
}

async fn metrics_snapshot(state: &HttpState) -> MetricsSnapshot {
    let queues = state.queue_stats();
    let dependency_health = state.dependency_health();
    let mut snapshot = MetricsSnapshot {
        queues,
        dependency_health,
        ..MetricsSnapshot::default()
    };

    if let Some(acceptor) = state.announce_acceptor.as_ref() {
        snapshot.announce_worker_capacity = Some(acceptor.config.worker_concurrency);
        match acceptor
            .repository
            .announce_queue_snapshot(100, unix_time_ms())
            .await
        {
            Ok(queue) => snapshot.announce_queue = Some(queue),
            Err(_error) => snapshot.snapshot_errors.push("announce_work"),
        }
        match acceptor.repository.job_status_snapshot(1_000).await {
            Ok(jobs) => snapshot.jobs = jobs,
            Err(_error) => snapshot.snapshot_errors.push("jobs"),
        }
        match acceptor.repository.dependency_health_snapshot(1_000).await {
            Ok(health) => snapshot.stored_dependency_health = health,
            Err(_error) => snapshot.snapshot_errors.push("dependency_health"),
        }
    }

    snapshot
}

async fn announce_queue_status(
    state: &HttpState,
) -> (Option<AnnounceQueueStatusResponse>, Option<&'static str>) {
    let Some(acceptor) = state.announce_acceptor.as_ref() else {
        return (None, None);
    };
    match acceptor
        .repository
        .announce_queue_snapshot(100, unix_time_ms())
        .await
    {
        Ok(snapshot) => (
            Some(announce_queue_status_response(snapshot, &acceptor.config)),
            None,
        ),
        Err(_error) => (None, Some("unavailable")),
    }
}

fn announce_queue_status_response(
    snapshot: AnnounceQueueSnapshot,
    config: &AnnounceQueueConfig,
) -> AnnounceQueueStatusResponse {
    let worker_capacity = i64::from(config.worker_concurrency);
    let worker_busy = snapshot.running_leases.min(worker_capacity).max(0);
    let worker_idle = worker_capacity.saturating_sub(worker_busy);
    AnnounceQueueStatusResponse {
        active_count: snapshot.active_count,
        max_pending: config.max_pending,
        worker_capacity: config.worker_concurrency,
        worker_busy,
        worker_idle,
        oldest_active_age_ms: snapshot.oldest_active_age_ms,
        next_retry_delay_ms: snapshot.next_retry_delay_ms,
        running_leases: snapshot.running_leases,
        statuses: snapshot
            .status_counts
            .into_iter()
            .map(|count| AnnounceStatusCountResponse {
                status: count.status,
                reason: count.reason,
                count: count.count,
            })
            .collect(),
        attempts: snapshot
            .attempt_counts
            .into_iter()
            .map(|count| AnnounceAttemptCountResponse {
                outcome_class: count.outcome_class,
                attempts: count.attempts,
            })
            .collect(),
        dependency_waits: snapshot
            .dependency_wait_counts
            .into_iter()
            .map(|count| AnnounceDependencyWaitResponse {
                dependency_kind: count.dependency_kind,
                dependency_name: count.dependency_name,
                count: count.count,
            })
            .collect(),
    }
}

fn readiness_response(state: &HttpState) -> ReadinessResponse {
    let readiness = state.readiness();
    let local_ready = readiness.config_loaded
        && readiness.database_available
        && readiness.schema_initialized
        && readiness.state_paths_writable;
    let work_acceptors_configured =
        state.workflow_queues.is_some() || state.announce_acceptor.is_some();
    let accepting_work = local_ready && work_acceptors_configured;
    let processing_ready = accepting_work && readiness.workers_running;
    ReadinessResponse {
        status: if readiness.is_ready() {
            "ready"
        } else {
            "not_ready"
        },
        accepting_work,
        processing_ready,
        checks: ReadinessChecks {
            config_loaded: readiness.config_loaded,
            database_available: readiness.database_available,
            schema_initialized: readiness.schema_initialized,
            state_paths_writable: readiness.state_paths_writable,
            workers_running: readiness.workers_running,
        },
        dependencies: state
            .dependency_health()
            .summaries
            .into_iter()
            .map(|(kind, summary)| (kind.as_str().to_owned(), summary.as_str().to_owned()))
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use super::*;
    use axum::body::Body;
    use axum::http::{Request, header};
    use serde_json::Value;
    use tower::ServiceExt;

    use crate::domain::{DependencyName, ReasonText};
    use crate::persistence::repository::Repository;
    use crate::runtime::health::DependencyKind;
    use crate::runtime::queue::{QueueKind, WorkReceiver, bounded_work_queue};

    #[tokio::test]
    async fn livez_does_not_depend_on_external_health() {
        let health = HealthRegistry::new();
        health.set_unavailable(
            DependencyKind::Indexer,
            DependencyName::new("torznab").unwrap(),
            ReasonText::new("rate limited").unwrap(),
            None,
        );
        let app = router(HttpState::new(ReadinessState::ready(), health));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/livez")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(StatusCode::OK, response.status());
    }

    #[tokio::test]
    async fn readyz_reflects_local_readiness_and_includes_dependencies() {
        let health = HealthRegistry::new();
        health.set_degraded(
            DependencyKind::TorrentClient,
            DependencyName::new("qbit").unwrap(),
            ReasonText::new("auth failed").unwrap(),
            None,
        );
        let state = HttpState::new(
            ReadinessState {
                config_loaded: true,
                database_available: false,
                schema_initialized: true,
                state_paths_writable: true,
                workers_running: true,
            },
            health,
        );
        let app = router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/readyz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(StatusCode::SERVICE_UNAVAILABLE, status);
        assert_eq!("not_ready", json["status"]);
        assert_eq!(false, json["checks"]["database_available"]);
        assert_eq!("degraded", json["dependencies"]["torrent_client"]);
    }

    #[tokio::test]
    async fn status_route_returns_typed_status_body() {
        let app = router(HttpState::new(
            ReadinessState::ready(),
            HealthRegistry::new(),
        ));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(StatusCode::OK, status);
        assert_eq!("ok", json["status"]);
        assert_eq!("ready", json["readiness"]["status"]);
    }

    #[tokio::test]
    async fn status_distinguishes_accepting_work_from_processing() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let (announcements, _announcement_receiver) =
            bounded_work_queue(QueueKind::Announcement, nonzero(4));
        let (searches, _search_receiver) = bounded_work_queue(QueueKind::Search, nonzero(4));
        let (jobs, _job_receiver) = bounded_work_queue(QueueKind::Indexing, nonzero(4));
        let state = HttpState::new(
            ReadinessState {
                config_loaded: true,
                database_available: true,
                schema_initialized: true,
                state_paths_writable: true,
                workers_running: false,
            },
            HealthRegistry::new(),
        )
        .with_workflow_queues(WorkflowQueues {
            announcements,
            searches,
            jobs,
        })
        .with_announce_acceptor(repository, AnnounceQueueConfig::default());
        let app = router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(true, json["readiness"]["accepting_work"]);
        assert_eq!(false, json["readiness"]["processing_ready"]);
        assert_eq!(false, json["readiness"]["checks"]["workers_running"]);
    }

    #[tokio::test]
    async fn metrics_route_exports_prometheus_text() {
        let (app, _announcements, _searches, _jobs) = workflow_app(None);
        let search = app
            .clone()
            .oneshot(json_post(
                "/v1/searches",
                serde_json::json!({ "query": "Example Movie 2026" }),
                None,
            ))
            .await
            .unwrap();
        assert_eq!(StatusCode::ACCEPTED, search.status());

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let text = std::str::from_utf8(&body).unwrap();

        assert_eq!(StatusCode::OK, status);
        assert!(text.contains("# TYPE sporos_http_requests_total counter"));
        assert!(text.contains(
            "sporos_http_requests_total{method=\"POST\",route=\"/v1/searches\",status=\"202\"} 1"
        ));
        assert!(
            text.contains(
                "sporos_workflow_enqueue_total{workflow=\"search\",outcome=\"accepted\"} 1"
            )
        );
        assert!(text.contains("sporos_queue_depth{queue=\"search\"} 1"));
    }

    #[tokio::test]
    async fn announcement_endpoint_validates_auth_and_persists_work() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let app = announce_app(
            repository.clone(),
            Some("secret"),
            AnnounceQueueConfig::default(),
        );

        let unauthorized = app
            .clone()
            .oneshot(json_post(
                "/v1/announcements",
                serde_json::json!({
                    "name": "Example",
                    "guid": "guid-1",
                    "download_url": "https://tracker.example/download?id=1",
                    "tracker": "tracker.example"
                }),
                None,
            ))
            .await
            .unwrap();
        let accepted = app
            .oneshot(json_post(
                "/v1/announcements",
                serde_json::json!({
                    "name": "Example",
                    "guid": "guid-1",
                    "download_url": "https://tracker.example/download?id=1&passkey=secret",
                    "tracker": "tracker.example",
                    "cookie": "sid=secret-cookie",
                    "size": 42
                }),
                Some("Bearer secret"),
            ))
            .await
            .unwrap();
        let status = accepted.status();
        let body = axum::body::to_bytes(accepted.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        let stored_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM announce_work")
            .fetch_one(repository.pool())
            .await
            .unwrap();
        let id = AnnounceWorkId::new(json["id"].as_str().unwrap_or_default()).unwrap();
        let fetch = repository
            .announce_fetch_material(&id)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(StatusCode::UNAUTHORIZED, unauthorized.status());
        assert_eq!(StatusCode::ACCEPTED, status);
        assert_eq!("queued", json["status"]);
        assert_eq!(false, json["deduplicated"]);
        assert!(json["id"].as_str().is_some_and(|id| id.starts_with("ann_")));
        assert_eq!(1, stored_count);
        assert_eq!(
            "https://tracker.example/download?id=1&passkey=secret",
            fetch.expose_download_url()
        );
        assert_eq!("sid=secret-cookie", fetch.cookie().unwrap().expose_secret());
        assert!(!fetch.redacted_download_url().as_str().contains("secret"));
    }

    #[tokio::test]
    async fn announcement_endpoint_deduplicates_active_work() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let app = announce_app(repository.clone(), None, AnnounceQueueConfig::default());
        let body = serde_json::json!({
            "name": "Example",
            "guid": "guid-1",
            "download_url": "https://tracker.example/download?id=1",
            "tracker": "tracker.example",
            "size": 42
        });

        let first = app
            .clone()
            .oneshot(json_post("/v1/announcements", body.clone(), None))
            .await
            .unwrap();
        let second = app
            .oneshot(json_post("/v1/announcements", body, None))
            .await
            .unwrap();
        let first_body = axum::body::to_bytes(first.into_body(), usize::MAX)
            .await
            .unwrap();
        let second_body = axum::body::to_bytes(second.into_body(), usize::MAX)
            .await
            .unwrap();
        let first_json: Value = serde_json::from_slice(&first_body).unwrap();
        let second_json: Value = serde_json::from_slice(&second_body).unwrap();
        let stored_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM announce_work")
            .fetch_one(repository.pool())
            .await
            .unwrap();

        assert_eq!(first_json["id"], second_json["id"]);
        assert_eq!(false, first_json["deduplicated"]);
        assert_eq!(true, second_json["deduplicated"]);
        assert_eq!(1, stored_count);
    }

    #[tokio::test]
    async fn announcement_endpoint_reports_durable_capacity() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let app = announce_app(
            repository.clone(),
            None,
            AnnounceQueueConfig {
                max_pending: 1,
                ..AnnounceQueueConfig::default()
            },
        );

        let first = app
            .clone()
            .oneshot(json_post(
                "/v1/announcements",
                serde_json::json!({
                    "name": "Example",
                    "guid": "guid-1",
                    "download_url": "https://tracker.example/download?id=1",
                    "tracker": "tracker.example"
                }),
                None,
            ))
            .await
            .unwrap();
        let rejected = app
            .oneshot(json_post(
                "/v1/announcements",
                serde_json::json!({
                    "name": "Other",
                    "guid": "guid-2",
                    "download_url": "https://tracker.example/download?id=2",
                    "tracker": "tracker.example"
                }),
                None,
            ))
            .await
            .unwrap();
        let stored_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM announce_work")
            .fetch_one(repository.pool())
            .await
            .unwrap();

        assert_eq!(StatusCode::ACCEPTED, first.status());
        assert_eq!(StatusCode::SERVICE_UNAVAILABLE, rejected.status());
        assert_eq!(1, stored_count);
    }

    #[tokio::test]
    async fn announcement_endpoint_requires_durable_acceptor() {
        let (announcements, _announcement_receiver) =
            bounded_work_queue(QueueKind::Announcement, nonzero(4));
        let (searches, _search_receiver) = bounded_work_queue(QueueKind::Search, nonzero(4));
        let (jobs, _job_receiver) = bounded_work_queue(QueueKind::Indexing, nonzero(4));
        let app = router(
            HttpState::new(ReadinessState::ready(), HealthRegistry::new()).with_workflow_queues(
                WorkflowQueues {
                    announcements: announcements.clone(),
                    searches,
                    jobs,
                },
            ),
        );

        let response = app
            .oneshot(json_post(
                "/v1/announcements",
                serde_json::json!({
                    "name": "Example",
                    "guid": "guid-1",
                    "download_url": "https://tracker.example/download?id=1",
                    "tracker": "tracker.example"
                }),
                None,
            ))
            .await
            .unwrap();
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(StatusCode::SERVICE_UNAVAILABLE, status);
        assert_eq!("service_unavailable", json["error"]["code"]);
        assert_eq!(0, announcements.stats().depth);
    }

    #[tokio::test]
    async fn status_and_metrics_expose_announce_queue_snapshots() {
        let repository = Repository::connect_in_memory().await.unwrap();
        let app = announce_app(
            repository,
            None,
            AnnounceQueueConfig {
                worker_concurrency: 3,
                ..AnnounceQueueConfig::default()
            },
        );
        let accepted = app
            .clone()
            .oneshot(json_post(
                "/v1/announcements",
                serde_json::json!({
                    "name": "Example",
                    "guid": "guid-1",
                    "download_url": "https://tracker.example/download?id=1&passkey=secret",
                    "tracker": "tracker.example"
                }),
                None,
            ))
            .await
            .unwrap();
        assert_eq!(StatusCode::ACCEPTED, accepted.status());

        let status_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status_body = axum::body::to_bytes(status_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let status_json: Value = serde_json::from_slice(&status_body).unwrap();

        assert_eq!(1, status_json["announce_queue"]["active_count"]);
        assert_eq!(3, status_json["announce_queue"]["worker_capacity"]);
        assert_eq!(
            "queued",
            status_json["announce_queue"]["statuses"][0]["status"]
        );
        assert_eq!(
            "accepted",
            status_json["announce_queue"]["statuses"][0]["reason"]
        );
        assert_eq!(true, status_json["readiness"]["accepting_work"]);
        assert_eq!(true, status_json["readiness"]["processing_ready"]);

        let metrics_response = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let metrics_body = axum::body::to_bytes(metrics_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let metrics_text = std::str::from_utf8(&metrics_body).unwrap();

        assert!(metrics_text.contains("sporos_announce_active_work 1"));
        assert!(metrics_text.contains("sporos_announce_worker_capacity 3"));
        assert!(
            metrics_text
                .contains("sporos_announce_work_total{status=\"queued\",reason=\"accepted\"} 1")
        );
        assert!(!metrics_text.contains("secret"));
    }

    #[tokio::test]
    async fn workflow_endpoints_validate_dtos_before_enqueueing() {
        let (app, _announcements, _searches, _jobs) = workflow_app(None);

        let response = app
            .oneshot(json_post(
                "/v1/announcements",
                serde_json::json!({
                    "name": "",
                    "guid": "guid-1",
                    "download_url": "https://tracker.example/download",
                    "tracker": "tracker.example"
                }),
                None,
            ))
            .await
            .unwrap();

        assert_eq!(StatusCode::UNPROCESSABLE_ENTITY, response.status());
    }

    #[tokio::test]
    async fn search_and_job_run_endpoints_use_bounded_queues() {
        let (app, _announcements, mut searches, mut jobs) = workflow_app(None);

        let search = app
            .clone()
            .oneshot(json_post(
                "/v1/searches",
                serde_json::json!({ "query": "Example Movie 2026" }),
                None,
            ))
            .await
            .unwrap();
        let job = app
            .oneshot(json_post("/v1/jobs/rss/runs", serde_json::json!({}), None))
            .await
            .unwrap();
        let search_work = searches.recv().await.unwrap();
        let job_work = jobs.recv().await.unwrap();

        assert_eq!(StatusCode::ACCEPTED, search.status());
        assert_eq!("Example Movie 2026", search_work.query.as_str());
        assert_eq!(StatusCode::ACCEPTED, job.status());
        assert_eq!("rss", job_work.job_name.as_str());
    }

    #[tokio::test]
    async fn workflow_endpoints_report_backpressure() {
        let (announcements, _announcement_receiver) =
            bounded_work_queue(QueueKind::Announcement, nonzero(1));
        let (searches, _search_receiver) = bounded_work_queue(QueueKind::Search, nonzero(1));
        let (jobs, _job_receiver) = bounded_work_queue(QueueKind::Indexing, nonzero(1));
        searches
            .try_enqueue(SearchWorkflowRequest {
                query: ItemTitle::new("Already Queued").unwrap(),
            })
            .unwrap();
        let app = router(
            HttpState::new(ReadinessState::ready(), HealthRegistry::new()).with_workflow_queues(
                WorkflowQueues {
                    announcements,
                    searches,
                    jobs,
                },
            ),
        );

        let response = app
            .oneshot(json_post(
                "/v1/searches",
                serde_json::json!({ "query": "Example" }),
                None,
            ))
            .await
            .unwrap();

        assert_eq!(StatusCode::SERVICE_UNAVAILABLE, response.status());
    }

    #[tokio::test]
    async fn workflow_routes_enforce_bounded_bodies() {
        let (app, _announcements, _searches, _jobs) = workflow_app(None);
        let oversized_query = "x".repeat(WORKFLOW_BODY_LIMIT_BYTES + 1);

        let response = app
            .oneshot(json_post(
                "/v1/searches",
                serde_json::json!({ "query": oversized_query }),
                None,
            ))
            .await
            .unwrap();

        assert_eq!(StatusCode::PAYLOAD_TOO_LARGE, response.status());
    }

    #[tokio::test]
    async fn workflow_routes_enforce_request_timeout() {
        let (announcements, _announcement_receiver) =
            bounded_work_queue(QueueKind::Announcement, nonzero(4));
        let (searches, _search_receiver) = bounded_work_queue(QueueKind::Search, nonzero(4));
        let (jobs, _job_receiver) = bounded_work_queue(QueueKind::Indexing, nonzero(4));
        let app = router(
            HttpState::new(ReadinessState::ready(), HealthRegistry::new())
                .with_workflow_queues(WorkflowQueues {
                    announcements,
                    searches,
                    jobs,
                })
                .with_request_timeout(Duration::ZERO),
        );

        let response = app
            .oneshot(json_post(
                "/v1/searches",
                serde_json::json!({ "query": "Example" }),
                None,
            ))
            .await
            .unwrap();

        assert_eq!(StatusCode::REQUEST_TIMEOUT, response.status());
    }

    fn workflow_app(
        token: Option<&str>,
    ) -> (
        Router,
        WorkReceiver<AnnouncementWorkflowRequest>,
        WorkReceiver<SearchWorkflowRequest>,
        WorkReceiver<JobRunWorkflowRequest>,
    ) {
        let (announcements, announcement_receiver) =
            bounded_work_queue(QueueKind::Announcement, nonzero(4));
        let (searches, search_receiver) = bounded_work_queue(QueueKind::Search, nonzero(4));
        let (jobs, job_receiver) = bounded_work_queue(QueueKind::Indexing, nonzero(4));
        let mut state = HttpState::new(ReadinessState::ready(), HealthRegistry::new())
            .with_workflow_queues(WorkflowQueues {
                announcements,
                searches,
                jobs,
            });
        if let Some(token) = token {
            state = state.with_api_token(token);
        }

        (
            router(state),
            announcement_receiver,
            search_receiver,
            job_receiver,
        )
    }

    fn announce_app(
        repository: Repository,
        token: Option<&str>,
        config: AnnounceQueueConfig,
    ) -> Router {
        let (announcements, _announcement_receiver) =
            bounded_work_queue(QueueKind::Announcement, nonzero(4));
        let (searches, _search_receiver) = bounded_work_queue(QueueKind::Search, nonzero(4));
        let (jobs, _job_receiver) = bounded_work_queue(QueueKind::Indexing, nonzero(4));
        let mut state = HttpState::new(ReadinessState::ready(), HealthRegistry::new())
            .with_workflow_queues(WorkflowQueues {
                announcements,
                searches,
                jobs,
            })
            .with_announce_acceptor(repository, config);
        if let Some(token) = token {
            state = state.with_api_token(token);
        }

        router(state)
    }

    fn json_post(path: &str, body: Value, authorization: Option<&str>) -> Request<Body> {
        let mut builder = Request::builder()
            .method("POST")
            .uri(path)
            .header(header::CONTENT_TYPE, "application/json");
        if let Some(authorization) = authorization {
            builder = builder.header(header::AUTHORIZATION, authorization);
        }
        builder.body(Body::from(body.to_string())).unwrap()
    }

    fn nonzero(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).unwrap_or(NonZeroUsize::MIN)
    }
}
