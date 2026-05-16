use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use crate::persistence::repository::{
    AnnounceQueueSnapshot, DependencyHealthSnapshot as StoredDependencyHealthSnapshot,
    JobStatusSnapshot,
};
use crate::runtime::health::DependencyHealthSnapshot;
use crate::runtime::queue::QueueStats;

#[derive(Debug, Clone, Default)]
pub struct MetricsRegistry {
    state: Arc<RwLock<MetricsState>>,
}

#[derive(Debug, Default)]
struct MetricsState {
    counters: BTreeMap<MetricKey, u64>,
    durations: BTreeMap<MetricKey, DurationStats>,
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
struct MetricKey {
    name: &'static str,
    labels: Vec<MetricLabel>,
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
struct MetricLabel {
    name: &'static str,
    value: String,
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
struct DurationStats {
    count: u64,
    sum_ms: u64,
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
    Failed,
}

impl ActionOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Saved => "saved",
            Self::Injected => "injected",
            Self::AlreadyExisting => "already_existing",
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

impl MetricsRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_http_request(&self, method: HttpMethod, route: HttpRoute, status: u16) {
        self.increment(
            "sporos_http_requests_total",
            vec![
                label("method", method.as_str()),
                label("route", route.as_str()),
                label("status", status.to_string()),
            ],
        );
    }

    pub fn record_workflow_enqueue(&self, workflow: WorkflowMetric, outcome: WorkflowOutcome) {
        self.increment(
            "sporos_workflow_enqueue_total",
            vec![
                label("workflow", workflow.as_str()),
                label("outcome", outcome.as_str()),
            ],
        );
    }

    pub fn record_search_attempt(&self, outcome: SearchOutcome) {
        self.increment(
            "sporos_search_attempts_total",
            vec![label("outcome", outcome.as_str())],
        );
    }

    pub fn record_decision(&self, outcome: DecisionOutcome) {
        self.increment(
            "sporos_decisions_total",
            vec![label("outcome", outcome.as_str())],
        );
    }

    pub fn record_indexer_request(
        &self,
        operation: ExternalOperation,
        outcome: ExternalOutcome,
        latency_ms: u64,
    ) {
        self.record_external_request(
            "sporos_indexer_requests_total",
            "sporos_indexer_request_duration_seconds",
            operation,
            outcome,
            latency_ms,
        );
    }

    pub fn record_client_request(
        &self,
        operation: ExternalOperation,
        outcome: ExternalOutcome,
        latency_ms: u64,
    ) {
        self.record_external_request(
            "sporos_client_requests_total",
            "sporos_client_request_duration_seconds",
            operation,
            outcome,
            latency_ms,
        );
    }

    pub fn record_notification_request(&self, outcome: ExternalOutcome, latency_ms: u64) {
        self.record_external_request(
            "sporos_notification_requests_total",
            "sporos_notification_request_duration_seconds",
            ExternalOperation::Notify,
            outcome,
            latency_ms,
        );
    }

    pub fn record_action(&self, outcome: ActionOutcome) {
        self.increment(
            "sporos_actions_total",
            vec![label("outcome", outcome.as_str())],
        );
    }

    pub fn record_job_duration(&self, job: &str, outcome: ExternalOutcome, duration_ms: u64) {
        self.observe_duration(
            "sporos_job_duration_seconds",
            vec![label("job", job), label("outcome", outcome.as_str())],
            duration_ms,
        );
    }

    pub fn record_prowlarr_refresh(
        &self,
        source: &str,
        outcome: ProwlarrRefreshOutcome,
        duration_ms: u64,
        imported: u64,
        deactivated: u64,
    ) {
        let labels = vec![label("source", source), label("outcome", outcome.as_str())];
        self.increment("sporos_prowlarr_refresh_total", labels.clone());
        self.observe_duration(
            "sporos_prowlarr_refresh_duration_seconds",
            labels,
            duration_ms,
        );
        let source_labels = vec![label("source", source)];
        self.add_counter(
            "sporos_prowlarr_refresh_imported_total",
            source_labels.clone(),
            imported,
        );
        self.add_counter(
            "sporos_prowlarr_refresh_deactivated_total",
            source_labels,
            deactivated,
        );
    }

    pub fn render_prometheus(&self, snapshot: &MetricsSnapshot) -> String {
        let state = self
            .state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut output = String::new();
        write_descriptors(&mut output);

        for (key, value) in &state.counters {
            write_metric(&mut output, key.name, &key.labels, *value);
        }
        for (key, stats) in &state.durations {
            write_duration(&mut output, key, *stats);
        }

        write_queue_metrics(&mut output, &snapshot.queues);
        write_dependency_metrics(&mut output, &snapshot.dependency_health);
        write_announce_metrics(
            &mut output,
            snapshot.announce_queue.as_ref(),
            snapshot.announce_worker_capacity,
        );
        write_job_snapshot_metrics(&mut output, &snapshot.jobs);
        write_stored_dependency_metrics(&mut output, &snapshot.stored_dependency_health);
        write_snapshot_errors(&mut output, &snapshot.snapshot_errors);

        output
    }

    fn record_external_request(
        &self,
        counter_name: &'static str,
        duration_name: &'static str,
        operation: ExternalOperation,
        outcome: ExternalOutcome,
        latency_ms: u64,
    ) {
        let labels = vec![
            label("operation", operation.as_str()),
            label("outcome", outcome.as_str()),
        ];
        self.increment(counter_name, labels.clone());
        self.observe_duration(duration_name, labels, latency_ms);
    }

    fn increment(&self, name: &'static str, labels: Vec<MetricLabel>) {
        self.add_counter(name, labels, 1);
    }

    fn add_counter(&self, name: &'static str, labels: Vec<MetricLabel>, amount: u64) {
        let mut state = self
            .state
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let counter = state
            .counters
            .entry(MetricKey { name, labels })
            .or_insert(0);
        *counter = counter.saturating_add(amount);
    }

    fn observe_duration(&self, name: &'static str, labels: Vec<MetricLabel>, duration_ms: u64) {
        let mut state = self
            .state
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let stats = state
            .durations
            .entry(MetricKey { name, labels })
            .or_default();
        stats.count = stats.count.saturating_add(1);
        stats.sum_ms = stats.sum_ms.saturating_add(duration_ms);
    }
}

fn write_descriptors(output: &mut String) {
    let descriptors = [
        (
            "sporos_http_requests_total",
            "counter",
            "HTTP requests with bounded route labels.",
        ),
        (
            "sporos_workflow_enqueue_total",
            "counter",
            "Workflow enqueue outcomes.",
        ),
        (
            "sporos_search_attempts_total",
            "counter",
            "Search attempt outcomes.",
        ),
        (
            "sporos_decisions_total",
            "counter",
            "Candidate decision outcomes.",
        ),
        (
            "sporos_indexer_requests_total",
            "counter",
            "Indexer request outcomes.",
        ),
        (
            "sporos_indexer_request_duration_seconds",
            "summary",
            "Indexer request latency.",
        ),
        (
            "sporos_client_requests_total",
            "counter",
            "Torrent client request outcomes.",
        ),
        (
            "sporos_client_request_duration_seconds",
            "summary",
            "Torrent client request latency.",
        ),
        (
            "sporos_notification_requests_total",
            "counter",
            "Notification request outcomes.",
        ),
        (
            "sporos_notification_request_duration_seconds",
            "summary",
            "Notification request latency.",
        ),
        (
            "sporos_actions_total",
            "counter",
            "Saved, injected, already-existing, and failed action outcomes.",
        ),
        (
            "sporos_job_duration_seconds",
            "summary",
            "Recorded job duration observations.",
        ),
        (
            "sporos_prowlarr_refresh_total",
            "counter",
            "Prowlarr refresh attempts by configured source and safe outcome.",
        ),
        (
            "sporos_prowlarr_refresh_duration_seconds",
            "summary",
            "Prowlarr refresh duration by configured source and safe outcome.",
        ),
        (
            "sporos_prowlarr_refresh_imported_total",
            "counter",
            "Prowlarr indexers accepted during successful refreshes.",
        ),
        (
            "sporos_prowlarr_refresh_deactivated_total",
            "counter",
            "Prowlarr indexers deactivated during successful refreshes.",
        ),
        (
            "sporos_queue_depth",
            "gauge",
            "Current workflow queue depth.",
        ),
        (
            "sporos_queue_capacity",
            "gauge",
            "Configured workflow queue capacity.",
        ),
        (
            "sporos_queue_enqueued_total",
            "counter",
            "Accepted workflow queue items.",
        ),
        (
            "sporos_queue_rejected_total",
            "counter",
            "Rejected workflow queue items.",
        ),
        (
            "sporos_queue_completed_total",
            "counter",
            "Completed workflow queue items.",
        ),
        (
            "sporos_queue_cancelled_total",
            "counter",
            "Cancelled workflow queue items.",
        ),
        (
            "sporos_dependency_health_state",
            "gauge",
            "Dependency health summaries.",
        ),
        (
            "sporos_announce_work_total",
            "gauge",
            "Durable announce work by status and reason.",
        ),
        (
            "sporos_announce_active_work",
            "gauge",
            "Active durable announce work.",
        ),
        (
            "sporos_announce_oldest_active_age_seconds",
            "gauge",
            "Oldest active announce work age.",
        ),
        (
            "sporos_announce_next_retry_delay_seconds",
            "gauge",
            "Next announce retry delay.",
        ),
        (
            "sporos_announce_running_leases",
            "gauge",
            "Running announce leases.",
        ),
        (
            "sporos_announce_worker_capacity",
            "gauge",
            "Configured announce worker capacity.",
        ),
        (
            "sporos_announce_worker_busy",
            "gauge",
            "Busy announce workers inferred from running leases.",
        ),
        (
            "sporos_announce_worker_idle",
            "gauge",
            "Idle announce worker capacity.",
        ),
        (
            "sporos_announce_attempts_total",
            "counter",
            "Announce attempts by safe outcome class.",
        ),
        (
            "sporos_announce_dependency_wait_count",
            "gauge",
            "Announce work waiting on dependency state.",
        ),
        (
            "sporos_job_state",
            "gauge",
            "Persisted job state snapshots.",
        ),
        (
            "sporos_job_last_duration_seconds",
            "gauge",
            "Last persisted job duration.",
        ),
        (
            "sporos_metrics_snapshot_error",
            "gauge",
            "Metrics snapshot source errors.",
        ),
    ];

    for (name, metric_type, help) in descriptors {
        output.push_str("# HELP ");
        output.push_str(name);
        output.push(' ');
        output.push_str(help);
        output.push('\n');
        output.push_str("# TYPE ");
        output.push_str(name);
        output.push(' ');
        output.push_str(metric_type);
        output.push('\n');
    }
}

fn write_queue_metrics(output: &mut String, queues: &[QueueStats]) {
    for stats in queues {
        let labels = vec![label("queue", stats.kind.as_str())];
        write_metric(output, "sporos_queue_depth", &labels, stats.depth);
        write_metric(output, "sporos_queue_capacity", &labels, stats.capacity);
        write_metric(
            output,
            "sporos_queue_enqueued_total",
            &labels,
            stats.accepted,
        );
        write_metric(
            output,
            "sporos_queue_rejected_total",
            &labels,
            stats.rejected,
        );
        write_metric(
            output,
            "sporos_queue_completed_total",
            &labels,
            stats.completed,
        );
        write_metric(
            output,
            "sporos_queue_cancelled_total",
            &labels,
            stats.cancelled,
        );
    }
}

fn write_dependency_metrics(output: &mut String, health: &DependencyHealthSnapshot) {
    for (kind, summary) in &health.summaries {
        let labels = vec![
            label("dependency", kind.as_str()),
            label("state", summary.as_str()),
        ];
        write_metric(output, "sporos_dependency_health_state", &labels, 1_u8);
    }
}

fn write_announce_metrics(
    output: &mut String,
    queue: Option<&AnnounceQueueSnapshot>,
    worker_capacity: Option<u16>,
) {
    let Some(queue) = queue else {
        return;
    };

    write_metric(
        output,
        "sporos_announce_active_work",
        &[],
        queue.active_count,
    );
    if let Some(age_ms) = queue.oldest_active_age_ms {
        let value_ms = u64::try_from(age_ms).unwrap_or(0);
        write_metric_seconds(
            output,
            "sporos_announce_oldest_active_age_seconds",
            &[],
            value_ms,
        );
    }
    if let Some(delay_ms) = queue.next_retry_delay_ms {
        let value_ms = u64::try_from(delay_ms).unwrap_or(0);
        write_metric_seconds(
            output,
            "sporos_announce_next_retry_delay_seconds",
            &[],
            value_ms,
        );
    }
    write_metric(
        output,
        "sporos_announce_running_leases",
        &[],
        queue.running_leases,
    );
    if let Some(worker_capacity) = worker_capacity {
        let capacity = i64::from(worker_capacity);
        let busy = queue.running_leases.min(capacity).max(0);
        let idle = capacity.saturating_sub(busy);
        write_metric(output, "sporos_announce_worker_capacity", &[], capacity);
        write_metric(output, "sporos_announce_worker_busy", &[], busy);
        write_metric(output, "sporos_announce_worker_idle", &[], idle);
    }

    for count in &queue.status_counts {
        let labels = vec![
            label("status", &count.status),
            label("reason", &count.reason),
        ];
        write_metric(output, "sporos_announce_work_total", &labels, count.count);
    }
    for count in &queue.attempt_counts {
        let labels = vec![label("outcome_class", &count.outcome_class)];
        write_metric(
            output,
            "sporos_announce_attempts_total",
            &labels,
            count.attempts,
        );
    }
    for count in &queue.dependency_wait_counts {
        let labels = vec![
            label("dependency_kind", &count.dependency_kind),
            label("dependency_name", &count.dependency_name),
        ];
        write_metric(
            output,
            "sporos_announce_dependency_wait_count",
            &labels,
            count.count,
        );
    }
}

fn write_job_snapshot_metrics(output: &mut String, jobs: &[JobStatusSnapshot]) {
    for job in jobs {
        let labels = vec![label("job", job.name.as_str()), label("state", &job.state)];
        write_metric(output, "sporos_job_state", &labels, 1_u8);
        if let (Some(started_at), Some(finished_at)) =
            (job.last_started_at_ms, job.last_finished_at_ms)
        {
            let duration_ms = finished_at.saturating_sub(started_at).max(0);
            let value = u64::try_from(duration_ms).unwrap_or(0);
            write_metric_seconds(output, "sporos_job_last_duration_seconds", &labels, value);
        }
    }
}

fn write_stored_dependency_metrics(output: &mut String, health: &[StoredDependencyHealthSnapshot]) {
    let mut counts: BTreeMap<(String, String), u64> = BTreeMap::new();
    for entry in health {
        let key = (entry.dependency_type.clone(), entry.state.clone());
        let count = counts.entry(key).or_insert(0);
        *count = count.saturating_add(1);
    }
    for ((dependency, state), count) in counts {
        let labels = vec![label("dependency", dependency), label("state", state)];
        write_metric(output, "sporos_dependency_health_state", &labels, count);
    }
}

fn write_snapshot_errors(output: &mut String, errors: &[&'static str]) {
    for source in errors {
        let labels = vec![label("source", *source)];
        write_metric(output, "sporos_metrics_snapshot_error", &labels, 1_u8);
    }
}

fn write_duration(output: &mut String, key: &MetricKey, stats: DurationStats) {
    let count_name = format!("{}_count", key.name);
    write_metric(output, &count_name, &key.labels, stats.count);

    let sum_name = format!("{}_sum", key.name);
    write_metric_seconds(output, &sum_name, &key.labels, stats.sum_ms);
}

fn write_metric(
    output: &mut String,
    name: &str,
    labels: &[MetricLabel],
    value: impl PrometheusValue,
) {
    output.push_str(name);
    write_labels(output, labels);
    output.push(' ');
    output.push_str(&value.prometheus_value());
    output.push('\n');
}

fn write_metric_seconds(output: &mut String, name: &str, labels: &[MetricLabel], value_ms: u64) {
    output.push_str(name);
    write_labels(output, labels);
    output.push(' ');
    output.push_str(&format_millis_as_seconds(value_ms));
    output.push('\n');
}

fn write_labels(output: &mut String, labels: &[MetricLabel]) {
    if labels.is_empty() {
        return;
    }
    output.push('{');
    for (index, label) in labels.iter().enumerate() {
        if index > 0 {
            output.push(',');
        }
        output.push_str(label.name);
        output.push_str("=\"");
        write_escaped_label_value(output, &label.value);
        output.push('"');
    }
    output.push('}');
}

fn write_escaped_label_value(output: &mut String, value: &str) {
    for character in value.chars() {
        match character {
            '\\' => {
                output.push_str("\\\\");
            }
            '"' => {
                output.push_str("\\\"");
            }
            '\n' => {
                output.push_str("\\n");
            }
            _ => {
                output.push(character);
            }
        }
    }
}

fn format_millis_as_seconds(value_ms: u64) -> String {
    let seconds = value_ms / 1_000;
    let millis = value_ms % 1_000;
    format!("{seconds}.{millis:03}")
}

fn label(name: &'static str, value: impl Into<String>) -> MetricLabel {
    MetricLabel {
        name,
        value: value.into(),
    }
}

trait PrometheusValue {
    fn prometheus_value(self) -> String;
}

impl PrometheusValue for u8 {
    fn prometheus_value(self) -> String {
        self.to_string()
    }
}

impl PrometheusValue for u64 {
    fn prometheus_value(self) -> String {
        self.to_string()
    }
}

impl PrometheusValue for usize {
    fn prometheus_value(self) -> String {
        self.to_string()
    }
}

impl PrometheusValue for i64 {
    fn prometheus_value(self) -> String {
        self.to_string()
    }
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
                "sporos_workflow_enqueue_total{workflow=\"search\",outcome=\"accepted\"} 1"
            )
        );
        assert!(output.contains("sporos_search_attempts_total{outcome=\"no_match\"} 1"));
        assert!(output.contains("sporos_decisions_total{outcome=\"rejected\"} 1"));
        assert!(output.contains(
            "sporos_indexer_requests_total{operation=\"search\",outcome=\"rate_limited\"} 1"
        ));
        assert!(output.contains("sporos_client_request_duration_seconds_sum{operation=\"inject\",outcome=\"succeeded\"} 0.250"));
        assert!(output.contains("sporos_actions_total{outcome=\"already_existing\"} 1"));
        assert!(
            output.contains(
                "sporos_job_duration_seconds_sum{job=\"rss\",outcome=\"succeeded\"} 3.000"
            )
        );
        assert!(
            output
                .contains("sporos_prowlarr_refresh_total{source=\"main\",outcome=\"succeeded\"} 1")
        );
        assert!(output.contains(
            "sporos_prowlarr_refresh_duration_seconds_sum{source=\"main\",outcome=\"succeeded\"} 0.500"
        ));
        assert!(output.contains("sporos_prowlarr_refresh_imported_total{source=\"main\"} 2"));
        assert!(output.contains("sporos_prowlarr_refresh_deactivated_total{source=\"main\"} 1"));
        assert!(output.contains("sporos_announce_active_work 3"));
        assert!(output.contains("sporos_announce_oldest_active_age_seconds 4.000"));
        assert!(output.contains("sporos_announce_worker_busy 1"));
        assert!(output.contains("sporos_announce_worker_idle 1"));
        assert!(output.contains("sporos_queue_depth{queue=\"search\"} 1"));
        assert!(output.contains("sporos_queue_cancelled_total{queue=\"search\"} 1"));
        assert!(output.contains(
            "sporos_dependency_health_state{dependency=\"indexer\",state=\"degraded\"} 1"
        ));
        assert!(
            output.contains("sporos_announce_work_total{status=\"queued\",reason=\"accepted\"} 3")
        );
        assert!(
            output.contains(
                "sporos_announce_attempts_total{outcome_class=\"retryable_dependency\"} 2"
            )
        );
        assert!(output.contains("sporos_announce_dependency_wait_count{dependency_kind=\"indexer\",dependency_name=\"torznab\"} 1"));
        assert!(
            output.contains(
                "sporos_job_last_duration_seconds{job=\"rss\",state=\"succeeded\"} 1.500"
            )
        );
        assert!(output.contains("sporos_metrics_snapshot_error{source=\"announce_work\"} 1"));
    }

    #[test]
    fn escapes_prometheus_label_values() {
        let mut output = String::new();
        write_labels(&mut output, &[label("value", "quote\"slash\\newline\n")]);

        assert_eq!("{value=\"quote\\\"slash\\\\newline\\n\"}", output);
    }
}
