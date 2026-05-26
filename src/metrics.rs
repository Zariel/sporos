use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use prometheus::{
    Encoder, GaugeVec, HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts, Registry,
    TextEncoder,
};

use crate::persistence::repository::{
    AnnounceQueueSnapshot, DependencyHealthSnapshot as StoredDependencyHealthSnapshot,
    JobStatusSnapshot,
};
use crate::runtime::health::DependencyHealthSnapshot;
use crate::runtime::queue::QueueStats;
use crate::secrets::sanitize_url_for_logging;

#[derive(Clone)]
pub struct MetricsRegistry {
    inner: Arc<MetricsInner>,
}

struct MetricsInner {
    registry: Registry,
    http_requests: IntCounterVec,
    workflow_enqueue: IntCounterVec,
    search_attempts: IntCounterVec,
    decisions: IntCounterVec,
    indexer_requests: IntCounterVec,
    indexer_request_duration: HistogramVec,
    client_requests: IntCounterVec,
    client_request_duration: HistogramVec,
    notification_requests: IntCounterVec,
    notification_request_duration: HistogramVec,
    actions: IntCounterVec,
    job_duration: HistogramVec,
    prowlarr_refresh: IntCounterVec,
    prowlarr_refresh_duration: HistogramVec,
    prowlarr_refresh_imported: IntCounterVec,
    prowlarr_refresh_deactivated: IntCounterVec,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum HttpMethod {
    Get,
    Post,
}

impl HttpMethod {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Post => "POST",
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum HttpRoute {
    Livez,
    Readyz,
    Metrics,
    Status,
    Announcements,
    Searches,
    JobRuns,
    NotificationTest,
}

impl HttpRoute {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Livez => "/livez",
            Self::Readyz => "/readyz",
            Self::Metrics => "/metrics",
            Self::Status => "/v1/status",
            Self::Announcements => "/v1/announcements",
            Self::Searches => "/v1/searches",
            Self::JobRuns => "/v1/jobs/{job_name}/runs",
            Self::NotificationTest => "/v1/notifications/test",
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum WorkflowMetric {
    Announcement,
    Search,
    JobRun,
}

impl WorkflowMetric {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Announcement => "announcement",
            Self::Search => "search",
            Self::JobRun => "job_run",
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum WorkflowOutcome {
    Accepted,
    RejectedFull,
    RejectedClosed,
    DurableAccepted,
    DurableDeduplicated,
    DurableCapacity,
    Invalid,
}

impl WorkflowOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::RejectedFull => "rejected_full",
            Self::RejectedClosed => "rejected_closed",
            Self::DurableAccepted => "durable_accepted",
            Self::DurableDeduplicated => "durable_deduplicated",
            Self::DurableCapacity => "durable_capacity",
            Self::Invalid => "invalid",
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum SearchOutcome {
    Succeeded,
    Failed,
    NoMatch,
}

impl SearchOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::NoMatch => "no_match",
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum DecisionOutcome {
    ExactMatch,
    SizeOnlyMatch,
    PartialMatch,
    Rejected,
}

impl DecisionOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::ExactMatch => "exact_match",
            Self::SizeOnlyMatch => "size_only_match",
            Self::PartialMatch => "partial_match",
            Self::Rejected => "rejected",
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ExternalOperation {
    Capabilities,
    Search,
    Rss,
    Download,
    Inventory,
    Inject,
    Recheck,
    Resume,
    Notify,
}

impl ExternalOperation {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Capabilities => "capabilities",
            Self::Search => "search",
            Self::Rss => "rss",
            Self::Download => "download",
            Self::Inventory => "inventory",
            Self::Inject => "inject",
            Self::Recheck => "recheck",
            Self::Resume => "resume",
            Self::Notify => "notify",
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ExternalOutcome {
    Succeeded,
    Failed,
    RateLimited,
    Unsupported,
}

impl ExternalOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::RateLimited => "rate_limited",
            Self::Unsupported => "unsupported",
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ProwlarrRefreshOutcome {
    Succeeded,
    Failed,
    RateLimited,
}

impl ProwlarrRefreshOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::RateLimited => "rate_limited",
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ActionOutcome {
    Saved,
    Injected,
    AlreadyExisting,
    Rejected,
    Failed,
}

impl ActionOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Saved => "saved",
            Self::Injected => "injected",
            Self::AlreadyExisting => "already_existing",
            Self::Rejected => "rejected",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MetricsSnapshot {
    pub queues: Vec<QueueStats>,
    pub dependency_health: DependencyHealthSnapshot,
    pub announce_queue: Option<AnnounceQueueSnapshot>,
    pub announce_worker_capacity: Option<u16>,
    pub jobs: Vec<JobStatusSnapshot>,
    pub stored_dependency_health: Vec<StoredDependencyHealthSnapshot>,
    pub snapshot_errors: Vec<&'static str>,
}

impl Default for MetricsSnapshot {
    fn default() -> Self {
        Self {
            queues: Vec::new(),
            dependency_health: DependencyHealthSnapshot {
                entries: Vec::new(),
                summaries: BTreeMap::new(),
            },
            announce_queue: None,
            announce_worker_capacity: None,
            jobs: Vec::new(),
            stored_dependency_health: Vec::new(),
            snapshot_errors: Vec::new(),
        }
    }
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for MetricsRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MetricsRegistry")
            .finish_non_exhaustive()
    }
}

impl MetricsRegistry {
    pub fn new() -> Self {
        let registry = Registry::new();
        let http_requests = counter_vec(
            "sporos_http_requests_total",
            "HTTP requests with bounded route labels.",
            &["method", "route", "status"],
        );
        let workflow_enqueue = counter_vec(
            "sporos_workflow_enqueue_total",
            "Workflow enqueue outcomes.",
            &["workflow", "outcome"],
        );
        let search_attempts = counter_vec(
            "sporos_search_attempts_total",
            "Search attempt outcomes.",
            &["outcome"],
        );
        let decisions = counter_vec(
            "sporos_decisions_total",
            "Candidate decision outcomes.",
            &["outcome"],
        );
        let indexer_requests = counter_vec(
            "sporos_indexer_requests_total",
            "Indexer request outcomes.",
            &["operation", "outcome"],
        );
        let indexer_request_duration = histogram_vec(
            "sporos_indexer_request_duration_seconds",
            "Indexer request latency.",
            &["operation", "outcome"],
        );
        let client_requests = counter_vec(
            "sporos_client_requests_total",
            "Torrent client request outcomes.",
            &["operation", "outcome"],
        );
        let client_request_duration = histogram_vec(
            "sporos_client_request_duration_seconds",
            "Torrent client request latency.",
            &["operation", "outcome"],
        );
        let notification_requests = counter_vec(
            "sporos_notification_requests_total",
            "Notification request outcomes.",
            &["operation", "outcome"],
        );
        let notification_request_duration = histogram_vec(
            "sporos_notification_request_duration_seconds",
            "Notification request latency.",
            &["operation", "outcome"],
        );
        let actions = counter_vec(
            "sporos_actions_total",
            "Saved, injected, already-existing, and failed action outcomes.",
            &["outcome"],
        );
        let job_duration = histogram_vec(
            "sporos_job_duration_seconds",
            "Recorded job duration observations.",
            &["job", "outcome"],
        );
        let prowlarr_refresh = counter_vec(
            "sporos_prowlarr_refresh_total",
            "Prowlarr refresh attempts by configured source and safe outcome.",
            &["source", "outcome"],
        );
        let prowlarr_refresh_duration = histogram_vec(
            "sporos_prowlarr_refresh_duration_seconds",
            "Prowlarr refresh duration by configured source and safe outcome.",
            &["source", "outcome"],
        );
        let prowlarr_refresh_imported = counter_vec(
            "sporos_prowlarr_refresh_imported_total",
            "Prowlarr indexers accepted during successful refreshes.",
            &["source"],
        );
        let prowlarr_refresh_deactivated = counter_vec(
            "sporos_prowlarr_refresh_deactivated_total",
            "Prowlarr indexers deactivated during successful refreshes.",
            &["source"],
        );

        for metric in [
            Box::new(http_requests.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(workflow_enqueue.clone()),
            Box::new(search_attempts.clone()),
            Box::new(decisions.clone()),
            Box::new(indexer_requests.clone()),
            Box::new(indexer_request_duration.clone()),
            Box::new(client_requests.clone()),
            Box::new(client_request_duration.clone()),
            Box::new(notification_requests.clone()),
            Box::new(notification_request_duration.clone()),
            Box::new(actions.clone()),
            Box::new(job_duration.clone()),
            Box::new(prowlarr_refresh.clone()),
            Box::new(prowlarr_refresh_duration.clone()),
            Box::new(prowlarr_refresh_imported.clone()),
            Box::new(prowlarr_refresh_deactivated.clone()),
        ] {
            register(&registry, metric);
        }

        Self {
            inner: Arc::new(MetricsInner {
                registry,
                http_requests,
                workflow_enqueue,
                search_attempts,
                decisions,
                indexer_requests,
                indexer_request_duration,
                client_requests,
                client_request_duration,
                notification_requests,
                notification_request_duration,
                actions,
                job_duration,
                prowlarr_refresh,
                prowlarr_refresh_duration,
                prowlarr_refresh_imported,
                prowlarr_refresh_deactivated,
            }),
        }
    }

    pub fn record_http_request(&self, method: HttpMethod, route: HttpRoute, status: u16) {
        self.inner
            .http_requests
            .with_label_values(&[method.as_str(), route.as_str(), &status.to_string()])
            .inc();
    }

    pub fn record_workflow_enqueue(&self, workflow: WorkflowMetric, outcome: WorkflowOutcome) {
        self.inner
            .workflow_enqueue
            .with_label_values(&[workflow.as_str(), outcome.as_str()])
            .inc();
    }

    pub fn record_search_attempt(&self, outcome: SearchOutcome) {
        self.inner
            .search_attempts
            .with_label_values(&[outcome.as_str()])
            .inc();
    }

    pub fn record_decision(&self, outcome: DecisionOutcome) {
        self.inner
            .decisions
            .with_label_values(&[outcome.as_str()])
            .inc();
    }

    pub fn record_indexer_request(
        &self,
        operation: ExternalOperation,
        outcome: ExternalOutcome,
        latency_ms: u64,
    ) {
        self.inner
            .indexer_requests
            .with_label_values(&[operation.as_str(), outcome.as_str()])
            .inc();
        self.inner
            .indexer_request_duration
            .with_label_values(&[operation.as_str(), outcome.as_str()])
            .observe(ms_to_seconds(latency_ms));
    }

    pub fn record_client_request(
        &self,
        operation: ExternalOperation,
        outcome: ExternalOutcome,
        latency_ms: u64,
    ) {
        self.inner
            .client_requests
            .with_label_values(&[operation.as_str(), outcome.as_str()])
            .inc();
        self.inner
            .client_request_duration
            .with_label_values(&[operation.as_str(), outcome.as_str()])
            .observe(ms_to_seconds(latency_ms));
    }

    pub fn record_notification_request(&self, outcome: ExternalOutcome, latency_ms: u64) {
        self.inner
            .notification_requests
            .with_label_values(&[ExternalOperation::Notify.as_str(), outcome.as_str()])
            .inc();
        self.inner
            .notification_request_duration
            .with_label_values(&[ExternalOperation::Notify.as_str(), outcome.as_str()])
            .observe(ms_to_seconds(latency_ms));
    }

    pub fn record_action(&self, outcome: ActionOutcome) {
        self.inner
            .actions
            .with_label_values(&[outcome.as_str()])
            .inc();
    }

    pub fn record_job_duration(&self, job: &str, outcome: ExternalOutcome, duration_ms: u64) {
        self.inner
            .job_duration
            .with_label_values(&[job, outcome.as_str()])
            .observe(ms_to_seconds(duration_ms));
    }

    pub fn record_prowlarr_refresh(
        &self,
        source: &str,
        outcome: ProwlarrRefreshOutcome,
        duration_ms: u64,
        imported: u64,
        deactivated: u64,
    ) {
        self.inner
            .prowlarr_refresh
            .with_label_values(&[source, outcome.as_str()])
            .inc();
        self.inner
            .prowlarr_refresh_duration
            .with_label_values(&[source, outcome.as_str()])
            .observe(ms_to_seconds(duration_ms));
        self.inner
            .prowlarr_refresh_imported
            .with_label_values(&[source])
            .inc_by(imported);
        self.inner
            .prowlarr_refresh_deactivated
            .with_label_values(&[source])
            .inc_by(deactivated);
    }

    pub fn render_prometheus(&self, snapshot: &MetricsSnapshot) -> String {
        let mut families = self.inner.registry.gather();
        families.extend(snapshot_metric_families(snapshot));

        let encoder = TextEncoder::new();
        let mut bytes = Vec::new();
        if encoder.encode(&families, &mut bytes).is_err() {
            return String::new();
        }
        String::from_utf8(bytes).unwrap_or_default()
    }
}

fn snapshot_metric_families(snapshot: &MetricsSnapshot) -> Vec<prometheus::proto::MetricFamily> {
    let registry = Registry::new();
    let queue_depth = int_gauge_vec(
        "sporos_queue_depth",
        "Current workflow queue depth.",
        &["queue"],
    );
    let queue_capacity = int_gauge_vec(
        "sporos_queue_capacity",
        "Configured workflow queue capacity.",
        &["queue"],
    );
    let queue_enqueued = counter_vec(
        "sporos_queue_enqueued_total",
        "Accepted workflow queue items.",
        &["queue"],
    );
    let queue_rejected = counter_vec(
        "sporos_queue_rejected_total",
        "Rejected workflow queue items.",
        &["queue"],
    );
    let queue_completed = counter_vec(
        "sporos_queue_completed_total",
        "Completed workflow queue items.",
        &["queue"],
    );
    let queue_cancelled = counter_vec(
        "sporos_queue_cancelled_total",
        "Cancelled workflow queue items.",
        &["queue"],
    );
    for stats in &snapshot.queues {
        let queue = stats.kind.as_str();
        queue_depth
            .with_label_values(&[queue])
            .set(i64_from_usize(stats.depth));
        queue_capacity
            .with_label_values(&[queue])
            .set(i64_from_usize(stats.capacity));
        queue_enqueued
            .with_label_values(&[queue])
            .inc_by(stats.accepted);
        queue_rejected
            .with_label_values(&[queue])
            .inc_by(stats.rejected);
        queue_completed
            .with_label_values(&[queue])
            .inc_by(stats.completed);
        queue_cancelled
            .with_label_values(&[queue])
            .inc_by(stats.cancelled);
    }

    let dependency_health = int_gauge_vec(
        "sporos_dependency_health_state",
        "Dependency health summaries.",
        &["dependency", "state"],
    );
    for (kind, summary) in &snapshot.dependency_health.summaries {
        dependency_health
            .with_label_values(&[kind.as_str(), summary.as_str()])
            .set(1);
    }
    let mut stored_dependency_counts = BTreeMap::<(String, String), i64>::new();
    for entry in &snapshot.stored_dependency_health {
        let count = stored_dependency_counts
            .entry((entry.dependency_type.clone(), entry.state.clone()))
            .or_insert(0);
        *count = count.saturating_add(1);
    }
    for ((dependency, state), count) in stored_dependency_counts {
        dependency_health
            .with_label_values(&[&dependency, &state])
            .set(count);
    }

    let announce_work = int_gauge_vec(
        "sporos_announce_work_total",
        "Durable announce work by status and reason.",
        &["status", "reason"],
    );
    let announce_dependency_wait = int_gauge_vec(
        "sporos_announce_dependency_wait_count",
        "Announce work waiting on dependency state.",
        &["dependency_kind", "dependency_name"],
    );
    let announce_attempts = counter_vec(
        "sporos_announce_attempts_total",
        "Announce attempts by safe outcome class.",
        &["outcome_class"],
    );
    if let Some(queue) = &snapshot.announce_queue {
        let announce_active = int_gauge(
            "sporos_announce_active_work",
            "Active durable announce work.",
        );
        let announce_oldest_age = gauge(
            "sporos_announce_oldest_active_age_seconds",
            "Oldest active announce work age.",
        );
        let announce_next_retry = gauge(
            "sporos_announce_next_retry_delay_seconds",
            "Next announce retry delay.",
        );
        let announce_running_leases =
            int_gauge("sporos_announce_running_leases", "Running announce leases.");
        announce_active.set(queue.active_count);
        if let Some(age_ms) = queue.oldest_active_age_ms {
            announce_oldest_age.set(ms_to_seconds(i64_to_u64_floor(age_ms)));
        }
        if let Some(delay_ms) = queue.next_retry_delay_ms {
            announce_next_retry.set(ms_to_seconds(i64_to_u64_floor(delay_ms)));
        }
        announce_running_leases.set(queue.running_leases);
        register(&registry, Box::new(announce_active.clone()));
        register(&registry, Box::new(announce_running_leases.clone()));
        if queue.oldest_active_age_ms.is_some() {
            register(&registry, Box::new(announce_oldest_age.clone()));
        }
        if queue.next_retry_delay_ms.is_some() {
            register(&registry, Box::new(announce_next_retry.clone()));
        }
        if let Some(capacity) = snapshot.announce_worker_capacity {
            let announce_worker_capacity = int_gauge(
                "sporos_announce_worker_capacity",
                "Configured announce worker capacity.",
            );
            let announce_worker_busy = int_gauge(
                "sporos_announce_worker_busy",
                "Busy announce workers inferred from running leases.",
            );
            let announce_worker_idle = int_gauge(
                "sporos_announce_worker_idle",
                "Idle announce worker capacity.",
            );
            let capacity = i64::from(capacity);
            let busy = queue.running_leases.min(capacity).max(0);
            announce_worker_capacity.set(capacity);
            announce_worker_busy.set(busy);
            announce_worker_idle.set(capacity.saturating_sub(busy));
            register(&registry, Box::new(announce_worker_capacity.clone()));
            register(&registry, Box::new(announce_worker_busy.clone()));
            register(&registry, Box::new(announce_worker_idle.clone()));
        }
        let mut announce_work_counts = BTreeMap::<(String, String), i64>::new();
        for count in &queue.status_counts {
            let status = sanitized_label(&count.status);
            let reason = sanitized_label(&count.reason);
            let total = announce_work_counts.entry((status, reason)).or_insert(0);
            *total = total.saturating_add(count.count);
        }
        for ((status, reason), count) in announce_work_counts {
            announce_work
                .with_label_values(&[status.as_str(), reason.as_str()])
                .set(count);
        }
        for count in &queue.attempt_counts {
            let outcome_class = sanitized_label(&count.outcome_class);
            announce_attempts
                .with_label_values(&[outcome_class.as_str()])
                .inc_by(i64_to_u64_floor(count.attempts));
        }
        let mut dependency_wait_counts = BTreeMap::<(String, String), i64>::new();
        for count in &queue.dependency_wait_counts {
            let dependency_kind = sanitized_label(&count.dependency_kind);
            let dependency_name = sanitized_label(&count.dependency_name);
            let total = dependency_wait_counts
                .entry((dependency_kind, dependency_name))
                .or_insert(0);
            *total = total.saturating_add(count.count);
        }
        for ((dependency_kind, dependency_name), count) in dependency_wait_counts {
            announce_dependency_wait
                .with_label_values(&[dependency_kind.as_str(), dependency_name.as_str()])
                .set(count);
        }
    }

    let job_state = int_gauge_vec(
        "sporos_job_state",
        "Persisted job state snapshots.",
        &["job", "state"],
    );
    let job_last_duration = gauge_vec(
        "sporos_job_last_duration_seconds",
        "Last persisted job duration.",
        &["job", "state"],
    );
    for job in &snapshot.jobs {
        job_state
            .with_label_values(&[job.name.as_str(), &job.state])
            .set(1);
        if let (Some(started_at), Some(finished_at)) =
            (job.last_started_at_ms, job.last_finished_at_ms)
        {
            let duration_ms = i64_to_u64_floor(finished_at.saturating_sub(started_at).max(0));
            job_last_duration
                .with_label_values(&[job.name.as_str(), &job.state])
                .set(ms_to_seconds(duration_ms));
        }
    }

    let snapshot_errors = int_gauge_vec(
        "sporos_metrics_snapshot_error",
        "Metrics snapshot source errors.",
        &["source"],
    );
    for source in &snapshot.snapshot_errors {
        snapshot_errors.with_label_values(&[source]).set(1);
    }

    for metric in [
        Box::new(queue_depth.clone()) as Box<dyn prometheus::core::Collector>,
        Box::new(queue_capacity.clone()),
        Box::new(queue_enqueued.clone()),
        Box::new(queue_rejected.clone()),
        Box::new(queue_completed.clone()),
        Box::new(queue_cancelled.clone()),
        Box::new(dependency_health.clone()),
        Box::new(announce_work.clone()),
        Box::new(announce_attempts.clone()),
        Box::new(announce_dependency_wait.clone()),
        Box::new(job_state.clone()),
        Box::new(job_last_duration.clone()),
        Box::new(snapshot_errors.clone()),
    ] {
        register(&registry, metric);
    }

    registry.gather()
}

fn counter_vec(name: &'static str, help: &'static str, labels: &[&'static str]) -> IntCounterVec {
    IntCounterVec::new(Opts::new(name, help), labels)
        .expect("static counter metric definition is valid")
}

fn histogram_vec(name: &'static str, help: &'static str, labels: &[&'static str]) -> HistogramVec {
    HistogramVec::new(
        HistogramOpts::new(name, help).buckets(vec![
            0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.000, 2.500, 5.000, 10.000, 30.000,
            60.000,
        ]),
        labels,
    )
    .expect("static histogram metric definition is valid")
}

fn int_gauge_vec(name: &'static str, help: &'static str, labels: &[&'static str]) -> IntGaugeVec {
    IntGaugeVec::new(Opts::new(name, help), labels)
        .expect("static integer gauge metric definition is valid")
}

fn gauge_vec(name: &'static str, help: &'static str, labels: &[&'static str]) -> GaugeVec {
    GaugeVec::new(Opts::new(name, help), labels).expect("static gauge metric definition is valid")
}

fn int_gauge(name: &'static str, help: &'static str) -> prometheus::IntGauge {
    prometheus::IntGauge::new(name, help).expect("static integer gauge metric definition is valid")
}

fn gauge(name: &'static str, help: &'static str) -> prometheus::Gauge {
    prometheus::Gauge::new(name, help).expect("static gauge metric definition is valid")
}

fn register(registry: &Registry, metric: Box<dyn prometheus::core::Collector>) {
    registry
        .register(metric)
        .expect("static metric registration is unique and valid");
}

fn ms_to_seconds(value_ms: u64) -> f64 {
    value_ms as f64 / 1_000.0
}

fn sanitized_label(value: &str) -> String {
    sanitize_url_for_logging(value).to_string()
}

fn i64_from_usize(value: usize) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn i64_to_u64_floor(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{DependencyName, JobName};
    use crate::persistence::repository::{
        AnnounceAttemptCount, AnnounceDependencyWaitCount, AnnounceStatusCount,
    };
    use crate::runtime::health::{DependencyKind, DependencySummary};
    use crate::runtime::queue::QueueKind;

    #[test]
    fn renders_prometheus_metrics_with_bounded_labels() {
        let registry = MetricsRegistry::new();
        registry.record_http_request(HttpMethod::Post, HttpRoute::Searches, 202);
        registry.record_workflow_enqueue(WorkflowMetric::Search, WorkflowOutcome::Accepted);
        registry.record_search_attempt(SearchOutcome::NoMatch);
        registry.record_decision(DecisionOutcome::Rejected);
        registry.record_indexer_request(
            ExternalOperation::Search,
            ExternalOutcome::RateLimited,
            1_250,
        );
        registry.record_client_request(ExternalOperation::Inject, ExternalOutcome::Succeeded, 250);
        registry.record_action(ActionOutcome::AlreadyExisting);
        registry.record_job_duration("rss", ExternalOutcome::Succeeded, 3_000);
        registry.record_prowlarr_refresh("main", ProwlarrRefreshOutcome::Succeeded, 500, 2, 1);

        let mut summaries = BTreeMap::new();
        summaries.insert(DependencyKind::Indexer, DependencySummary::Degraded);
        let snapshot = MetricsSnapshot {
            queues: vec![QueueStats {
                kind: QueueKind::Search,
                capacity: 4,
                depth: 1,
                accepted: 2,
                rejected: 1,
                completed: 1,
                cancelled: 1,
            }],
            dependency_health: DependencyHealthSnapshot {
                entries: Vec::new(),
                summaries,
            },
            announce_queue: Some(AnnounceQueueSnapshot {
                active_count: 3,
                oldest_active_age_ms: Some(4_000),
                next_retry_delay_ms: Some(2_000),
                running_leases: 1,
                status_counts: vec![AnnounceStatusCount {
                    status: "queued".to_owned(),
                    reason: "accepted".to_owned(),
                    count: 3,
                }],
                attempt_counts: vec![AnnounceAttemptCount {
                    outcome_class: "retryable_dependency".to_owned(),
                    attempts: 2,
                }],
                dependency_wait_counts: vec![AnnounceDependencyWaitCount {
                    dependency_kind: "indexer".to_owned(),
                    dependency_name: "torznab".to_owned(),
                    count: 1,
                }],
            }),
            announce_worker_capacity: Some(2),
            jobs: vec![JobStatusSnapshot {
                name: JobName::new("rss").unwrap(),
                state: "succeeded".to_owned(),
                last_started_at_ms: Some(1_000),
                last_finished_at_ms: Some(2_500),
                next_run_at_ms: Some(10_000),
                last_error: None,
            }],
            stored_dependency_health: vec![StoredDependencyHealthSnapshot {
                dependency_type: "torrent_client".to_owned(),
                dependency_name: DependencyName::new("qbit").unwrap(),
                state: "healthy".to_owned(),
                reason: None,
                retry_after_ms: None,
                failure_count: 0,
                checked_at_ms: 2_000,
            }],
            snapshot_errors: vec!["announce_work"],
        };

        let output = registry.render_prometheus(&snapshot);

        assert!(output.contains(
            "sporos_http_requests_total{method=\"POST\",route=\"/v1/searches\",status=\"202\"} 1"
        ));
        assert!(
            output.contains(
                "sporos_workflow_enqueue_total{outcome=\"accepted\",workflow=\"search\"} 1"
            )
        );
        assert!(output.contains("sporos_search_attempts_total{outcome=\"no_match\"} 1"));
        assert!(output.contains("sporos_decisions_total{outcome=\"rejected\"} 1"));
        assert!(output.contains(
            "sporos_indexer_requests_total{operation=\"search\",outcome=\"rate_limited\"} 1"
        ));
        assert!(output.contains("sporos_indexer_request_duration_seconds_sum{operation=\"search\",outcome=\"rate_limited\"} 1.25"));
        assert!(output.contains("sporos_client_request_duration_seconds_sum{operation=\"inject\",outcome=\"succeeded\"} 0.25"));
        assert!(output.contains("sporos_actions_total{outcome=\"already_existing\"} 1"));
        assert!(
            output.contains("sporos_job_duration_seconds_sum{job=\"rss\",outcome=\"succeeded\"} 3")
        );
        assert!(
            output
                .contains("sporos_prowlarr_refresh_total{outcome=\"succeeded\",source=\"main\"} 1")
        );
        assert!(output.contains(
            "sporos_prowlarr_refresh_duration_seconds_sum{outcome=\"succeeded\",source=\"main\"} 0.5"
        ));
        assert!(output.contains("sporos_prowlarr_refresh_imported_total{source=\"main\"} 2"));
        assert!(output.contains("sporos_prowlarr_refresh_deactivated_total{source=\"main\"} 1"));
        assert!(output.contains("sporos_announce_active_work 3"));
        assert!(output.contains("sporos_announce_oldest_active_age_seconds 4"));
        assert!(output.contains("sporos_announce_worker_busy 1"));
        assert!(output.contains("sporos_announce_worker_idle 1"));
        assert!(output.contains("sporos_queue_depth{queue=\"search\"} 1"));
        assert!(output.contains("sporos_queue_cancelled_total{queue=\"search\"} 1"));
        assert!(output.contains(
            "sporos_dependency_health_state{dependency=\"indexer\",state=\"degraded\"} 1"
        ));
        assert!(
            output.contains("sporos_announce_work_total{reason=\"accepted\",status=\"queued\"} 3")
        );
        assert!(
            output.contains(
                "sporos_announce_attempts_total{outcome_class=\"retryable_dependency\"} 2"
            )
        );
        assert!(output.contains("sporos_announce_dependency_wait_count{dependency_kind=\"indexer\",dependency_name=\"torznab\"} 1"));
        assert!(
            output
                .contains("sporos_job_last_duration_seconds{job=\"rss\",state=\"succeeded\"} 1.5")
        );
        assert!(output.contains("sporos_metrics_snapshot_error{source=\"announce_work\"} 1"));
    }

    #[test]
    fn announce_metrics_redact_secret_bearing_snapshot_labels() {
        let registry = MetricsRegistry::new();
        let snapshot = MetricsSnapshot {
            announce_queue: Some(AnnounceQueueSnapshot {
                active_count: 1,
                oldest_active_age_ms: None,
                next_retry_delay_ms: None,
                running_leases: 0,
                status_counts: vec![
                    AnnounceStatusCount {
                        status: "queued".to_owned(),
                        reason: "https://tracker.example/status?passkey=status-secret".to_owned(),
                        count: 1,
                    },
                    AnnounceStatusCount {
                        status: "queued".to_owned(),
                        reason: "https://tracker.example/status?passkey=other-status-secret"
                            .to_owned(),
                        count: 2,
                    },
                ],
                attempt_counts: vec![AnnounceAttemptCount {
                    outcome_class: "https://tracker.example/error?token=attempt-secret".to_owned(),
                    attempts: 1,
                }],
                dependency_wait_counts: vec![
                    AnnounceDependencyWaitCount {
                        dependency_kind: "indexer".to_owned(),
                        dependency_name: "https://tracker.example/wait?cookie=dependency-secret"
                            .to_owned(),
                        count: 1,
                    },
                    AnnounceDependencyWaitCount {
                        dependency_kind: "indexer".to_owned(),
                        dependency_name:
                            "https://tracker.example/wait?cookie=other-dependency-secret".to_owned(),
                        count: 2,
                    },
                ],
            }),
            ..MetricsSnapshot::default()
        };

        let output = registry.render_prometheus(&snapshot);

        assert!(output.contains("[REDACTED]"));
        assert!(output.contains("sporos_announce_work_total{reason=\"https://tracker.example/status?passkey=[REDACTED]\",status=\"queued\"} 3"));
        assert!(output.contains("sporos_announce_dependency_wait_count{dependency_kind=\"indexer\",dependency_name=\"https://tracker.example/wait?cookie=[REDACTED]\"} 3"));
        assert!(!output.contains("status-secret"));
        assert!(!output.contains("other-status-secret"));
        assert!(!output.contains("attempt-secret"));
        assert!(!output.contains("dependency-secret"));
        assert!(!output.contains("other-dependency-secret"));
    }

    #[test]
    fn prometheus_encoder_escapes_label_values() {
        let registry = MetricsRegistry::new();
        registry.record_job_duration("quote\"slash\\newline\n", ExternalOutcome::Succeeded, 1_000);

        let output = registry.render_prometheus(&MetricsSnapshot::default());

        assert!(output.contains("job=\"quote\\\"slash\\\\newline\\n\""));
    }

    #[test]
    fn scrape_omits_absent_optional_announce_scalars() {
        let registry = MetricsRegistry::new();

        let output = registry.render_prometheus(&MetricsSnapshot::default());

        assert!(!output.contains("sporos_announce_active_work"));
        assert!(!output.contains("sporos_announce_oldest_active_age_seconds"));
        assert!(!output.contains("sporos_announce_next_retry_delay_seconds"));
        assert!(!output.contains("sporos_announce_running_leases"));
        assert!(!output.contains("sporos_announce_worker_capacity"));
        assert!(!output.contains("sporos_announce_worker_busy"));
        assert!(!output.contains("sporos_announce_worker_idle"));
    }
}
