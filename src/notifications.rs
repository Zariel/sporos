use std::collections::BTreeMap;
use std::fmt;
use std::num::NonZeroU8;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use reqwest::StatusCode;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, USER_AGENT};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{debug, warn};

use crate::domain::{DependencyName, ReasonText};
use crate::metrics::{ExternalOutcome, MetricsRegistry};
use crate::runtime::backoff::{
    JitteredBackoffPolicy, RetryOutcome, fixed_retry_deadline_ms, retry_with_backoff,
};
use crate::runtime::health::{DependencyKey, DependencyKind, HealthRegistry};
use crate::runtime::queue::{BoundedWorkQueue, QueueKind, WorkReceiver, bounded_work_queue};
use crate::runtime::shutdown::ShutdownSignal;
use crate::secrets::{NotificationToken, SanitizedUrl, sanitize_url_for_logging};

const USER_AGENT_VALUE: &str = concat!("Sporos/", env!("CARGO_PKG_VERSION"));
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(300);
const DEFAULT_INITIAL_RETRY_DELAY: Duration = Duration::from_millis(250);
const DEFAULT_MAX_RETRY_DELAY: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum NotificationEventKind {
    Test,
    Results,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct NotificationEvent {
    pub event: NotificationEventKind,
    pub title: String,
    pub body: String,
    pub extra: BTreeMap<String, Value>,
}

impl NotificationEvent {
    pub fn test() -> Self {
        Self {
            event: NotificationEventKind::Test,
            title: "sporos".to_owned(),
            body: "test notification".to_owned(),
            extra: BTreeMap::from([("event".to_owned(), Value::String("TEST".to_owned()))]),
        }
    }

    pub fn results(body: impl Into<String>, extra: BTreeMap<String, Value>) -> Self {
        Self {
            event: NotificationEventKind::Results,
            title: "sporos".to_owned(),
            body: body.into(),
            extra,
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct NotificationEndpoint {
    pub name: DependencyName,
    url: String,
    token: Option<NotificationToken>,
}

impl NotificationEndpoint {
    pub fn new(name: DependencyName, url: impl Into<String>) -> Self {
        Self {
            name,
            url: url.into(),
            token: None,
        }
    }

    pub fn with_token(mut self, token: NotificationToken) -> Self {
        self.token = Some(token);
        self
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn sanitized_url(&self) -> SanitizedUrl {
        sanitize_url_for_logging(&self.url)
    }

    fn token(&self) -> Option<&NotificationToken> {
        self.token.as_ref()
    }
}

impl fmt::Debug for NotificationEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NotificationEndpoint")
            .field("name", &self.name)
            .field("url", &self.sanitized_url())
            .field("token", &self.token)
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct NotificationJob {
    pub endpoint: NotificationEndpoint,
    pub event: NotificationEvent,
}

impl NotificationJob {
    pub fn new(endpoint: NotificationEndpoint, event: NotificationEvent) -> Self {
        Self { endpoint, event }
    }
}

impl fmt::Debug for NotificationJob {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NotificationJob")
            .field("endpoint", &self.endpoint)
            .field("event", &self.event)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct NotificationRetryPolicy {
    pub max_attempts: NonZeroU8,
    pub initial_delay: Duration,
    pub max_delay: Duration,
}

impl NotificationRetryPolicy {
    pub const fn single_attempt() -> Self {
        Self {
            max_attempts: NonZeroU8::MIN,
            initial_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
        }
    }
}

impl Default for NotificationRetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: NonZeroU8::new(3).unwrap_or(NonZeroU8::MIN),
            initial_delay: DEFAULT_INITIAL_RETRY_DELAY,
            max_delay: DEFAULT_MAX_RETRY_DELAY,
        }
    }
}

#[derive(Debug, Clone)]
pub struct NotificationWorker {
    client: reqwest::Client,
    health: HealthRegistry,
    metrics: MetricsRegistry,
    retry: NotificationRetryPolicy,
}

impl NotificationWorker {
    pub fn new(health: HealthRegistry, metrics: MetricsRegistry) -> Self {
        Self::with_config(
            health,
            metrics,
            DEFAULT_TIMEOUT,
            NotificationRetryPolicy::default(),
        )
    }

    pub fn with_config(
        health: HealthRegistry,
        metrics: MetricsRegistry,
        timeout: Duration,
        retry: NotificationRetryPolicy,
    ) -> Self {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .unwrap_or_else(|_error| reqwest::Client::new());
        Self {
            client,
            health,
            metrics,
            retry,
        }
    }

    pub async fn deliver(&self, job: &NotificationJob) -> NotificationDeliveryReport {
        self.deliver_until_shutdown(job, None).await
    }

    pub async fn deliver_until_shutdown(
        &self,
        job: &NotificationJob,
        shutdown: Option<&ShutdownSignal>,
    ) -> NotificationDeliveryReport {
        let attempts = Arc::new(Mutex::new(Vec::new()));

        let retry_attempts = Arc::clone(&attempts);
        let outcome = retry_with_backoff(
            self.retry.max_attempts.get(),
            self.retry_backoff_policy(),
            job.endpoint.name.as_str(),
            shutdown,
            move |_attempt| {
                let retry_attempts = Arc::clone(&retry_attempts);
                async move {
                    let report = self.send_once(job).await;
                    let retryable = report.retryable;
                    let succeeded = report.outcome == NotificationDeliveryOutcome::Succeeded;
                    retry_attempts
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .push(report);
                    if succeeded || !retryable {
                        Ok(())
                    } else {
                        Err(())
                    }
                }
            },
            |_| true,
        )
        .await;
        let mut attempts = attempts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        if matches!(outcome, RetryOutcome::Shutdown) && attempts.is_empty() {
            attempts.push(NotificationAttemptReport {
                outcome: NotificationDeliveryOutcome::Failed,
                latency_ms: 0,
                retryable: true,
                error: Some("notification delivery stopped during shutdown".to_owned()),
            });
        }

        let final_attempt = attempts
            .last()
            .cloned()
            .unwrap_or_else(|| NotificationAttemptReport {
                outcome: NotificationDeliveryOutcome::Failed,
                latency_ms: 0,
                retryable: false,
                error: Some("notification delivery was not attempted".to_owned()),
            });
        self.record_health(&job.endpoint, &final_attempt);

        NotificationDeliveryReport {
            attempts,
            final_outcome: final_attempt.outcome,
        }
    }

    fn retry_backoff_policy(&self) -> JitteredBackoffPolicy {
        JitteredBackoffPolicy {
            base_delay_ms: i64::try_from(self.retry.initial_delay.as_millis()).unwrap_or(i64::MAX),
            max_delay_ms: i64::try_from(self.retry.max_delay.as_millis()).unwrap_or(i64::MAX),
            jitter_ms: 0,
        }
    }

    async fn send_once(&self, job: &NotificationJob) -> NotificationAttemptReport {
        let started = Instant::now();
        let body = match serde_json::to_vec(&NotificationPayload::from_event(&job.event)) {
            Ok(body) => body,
            Err(error) => {
                return NotificationAttemptReport {
                    outcome: NotificationDeliveryOutcome::Failed,
                    latency_ms: elapsed_ms(started),
                    retryable: false,
                    error: Some(format!("cannot encode notification payload: {error}")),
                };
            }
        };
        let mut request = self
            .client
            .post(job.endpoint.url())
            .header(USER_AGENT, USER_AGENT_VALUE)
            .header(CONTENT_TYPE, "application/json")
            .body(body);

        if let Some(token) = job.endpoint.token() {
            request = request.header(AUTHORIZATION, format!("Bearer {}", token.expose_secret()));
        }

        match request.send().await {
            Ok(response) if response.status().is_success() => {
                let latency_ms = elapsed_ms(started);
                self.metrics
                    .record_notification_request(ExternalOutcome::Succeeded, latency_ms);
                debug!("notification delivered");
                NotificationAttemptReport {
                    outcome: NotificationDeliveryOutcome::Succeeded,
                    latency_ms,
                    retryable: false,
                    error: None,
                }
            }
            Ok(response) => {
                let status = response.status();
                let latency_ms = elapsed_ms(started);
                self.metrics
                    .record_notification_request(ExternalOutcome::Failed, latency_ms);
                warn!(
                    status = status.as_u16(),
                    "notification endpoint returned non-OK status"
                );
                NotificationAttemptReport {
                    outcome: NotificationDeliveryOutcome::NonOk { status },
                    latency_ms,
                    retryable: retryable_status(status),
                    error: Some(format!(
                        "{} returned HTTP {status}",
                        job.endpoint.sanitized_url()
                    )),
                }
            }
            Err(error) => {
                let latency_ms = elapsed_ms(started);
                self.metrics
                    .record_notification_request(ExternalOutcome::Failed, latency_ms);
                let redacted_error = redact_error_message(&error.to_string());
                let outcome = if error.is_timeout() {
                    NotificationDeliveryOutcome::TimedOut
                } else {
                    NotificationDeliveryOutcome::Failed
                };
                warn!(error = %redacted_error, "notification request failed");
                NotificationAttemptReport {
                    outcome,
                    latency_ms,
                    retryable: true,
                    error: Some(format!(
                        "{} request failed: {}",
                        job.endpoint.sanitized_url(),
                        redacted_error
                    )),
                }
            }
        }
    }

    fn record_health(&self, endpoint: &NotificationEndpoint, attempt: &NotificationAttemptReport) {
        let name = endpoint.name.clone();
        match attempt.outcome {
            NotificationDeliveryOutcome::Succeeded => {
                self.health.set_healthy(
                    DependencyKind::Notification,
                    name,
                    crate::runtime::announce_worker::unix_time_ms(),
                );
            }
            NotificationDeliveryOutcome::NonOk { .. }
            | NotificationDeliveryOutcome::TimedOut
            | NotificationDeliveryOutcome::Failed => {
                let reason = attempt
                    .error
                    .as_deref()
                    .unwrap_or("notification delivery failed");
                let retry_after_ms = retry_after_deadline(self.retry);
                if let Some(reason) = reason_text(reason) {
                    self.health.set_degraded(
                        DependencyKind::Notification,
                        name,
                        reason,
                        Some(retry_after_ms),
                    );
                } else {
                    self.health.set_unknown(DependencyKind::Notification, name);
                }
            }
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NotificationDeliveryReport {
    pub attempts: Vec<NotificationAttemptReport>,
    pub final_outcome: NotificationDeliveryOutcome,
}

impl NotificationDeliveryReport {
    pub const fn succeeded(&self) -> bool {
        matches!(self.final_outcome, NotificationDeliveryOutcome::Succeeded)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NotificationAttemptReport {
    pub outcome: NotificationDeliveryOutcome,
    pub latency_ms: u64,
    pub retryable: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum NotificationDeliveryOutcome {
    Succeeded,
    NonOk { status: StatusCode },
    TimedOut,
    Failed,
}

#[derive(Debug, Serialize)]
struct NotificationPayload<'a> {
    title: &'a str,
    body: &'a str,
    extra: BTreeMap<&'a str, Value>,
}

impl<'a> NotificationPayload<'a> {
    fn from_event(event: &'a NotificationEvent) -> Self {
        let mut extra = event
            .extra
            .iter()
            .map(|(key, value)| (key.as_str(), value.clone()))
            .collect::<BTreeMap<_, _>>();
        extra.insert(
            "event",
            Value::String(match event.event {
                NotificationEventKind::Test => "TEST".to_owned(),
                NotificationEventKind::Results => "RESULTS".to_owned(),
            }),
        );
        Self {
            title: &event.title,
            body: &event.body,
            extra,
        }
    }
}

pub fn notification_queue(
    capacity: std::num::NonZeroUsize,
) -> (
    BoundedWorkQueue<NotificationJob>,
    WorkReceiver<NotificationJob>,
) {
    bounded_work_queue(QueueKind::Notification, capacity)
}

pub async fn run_notification_worker(
    worker: NotificationWorker,
    mut receiver: WorkReceiver<NotificationJob>,
    mut shutdown: ShutdownSignal,
) {
    loop {
        tokio::select! {
            biased;
            _state = shutdown.cancelled() => {
                receiver.close();
                release_queued_notifications(&mut receiver).await;
                break;
            }
            job = receiver.recv() => {
                let Some(job) = job else {
                    break;
                };
                let report = worker.deliver_until_shutdown(&job, Some(&shutdown)).await;
                if !report.succeeded() {
                    warn!(
                        endpoint = %job.endpoint.name,
                        url = %job.endpoint.sanitized_url(),
                        outcome = ?report.final_outcome,
                        attempts = report.attempts.len(),
                        "notification delivery failed after bounded retries"
                    );
                }
                receiver.mark_completed();
            }
        }
    }
}

async fn release_queued_notifications(receiver: &mut WorkReceiver<NotificationJob>) {
    while receiver.recv().await.is_some() {
        receiver.mark_cancelled();
    }
}

fn retryable_status(status: StatusCode) -> bool {
    status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS
}

fn retry_after_deadline(policy: NotificationRetryPolicy) -> i64 {
    let now = crate::runtime::announce_worker::unix_time_ms();
    let delay_ms = i64::try_from(policy.max_delay.as_millis()).unwrap_or(i64::MAX);
    fixed_retry_deadline_ms(now, delay_ms, None)
}

fn elapsed_ms(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn reason_text(value: &str) -> Option<ReasonText> {
    ReasonText::new(value)
        .ok()
        .or_else(|| ReasonText::new("notification delivery failed").ok())
}

fn redact_error_message(message: &str) -> String {
    sanitize_url_for_logging(message).to_string()
}

pub fn notification_dependency_key(endpoint: &NotificationEndpoint) -> DependencyKey {
    DependencyKey::new(DependencyKind::Notification, endpoint.name.clone())
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::num::NonZeroUsize;
    use std::sync::{Arc, Mutex};

    use axum::body::Bytes;
    use axum::extract::State;
    use axum::http::{HeaderMap, StatusCode};
    use axum::routing::post;
    use axum::{Router, response::IntoResponse};
    use serde_json::json;
    use tokio::net::TcpListener;

    use super::*;
    use crate::runtime::health::DependencySummary;
    use crate::runtime::shutdown::shutdown_channel;

    #[tokio::test]
    async fn delivery_posts_json_and_marks_endpoint_healthy() {
        let server = TestServer::new(TestBehavior::Ok).await;
        let health = HealthRegistry::new();
        let worker = test_worker(health.clone(), Duration::from_secs(2));
        let endpoint =
            endpoint(server.url()).with_token(NotificationToken::new("bearer-secret").unwrap());
        let job = NotificationJob::new(endpoint.clone(), NotificationEvent::test());

        let report = worker.deliver(&job).await;

        assert!(report.succeeded());
        assert_eq!(1, server.requests().len());
        let request = &server.requests()[0];
        assert_eq!(
            "application/json",
            request.content_type.as_deref().unwrap_or("")
        );
        assert_eq!(
            "Bearer bearer-secret",
            request.authorization.as_deref().unwrap_or("")
        );
        assert_eq!(json!("sporos"), request.body["title"]);
        assert_eq!(json!("TEST"), request.body["extra"]["event"]);
        assert_eq!(
            Some(DependencySummary::Healthy),
            health
                .snapshot()
                .summaries
                .get(&DependencyKind::Notification)
                .copied()
        );
    }

    #[tokio::test]
    async fn delivery_retries_retryable_non_ok_and_records_health() {
        let server = TestServer::new(TestBehavior::Status(StatusCode::SERVICE_UNAVAILABLE)).await;
        let health = HealthRegistry::new();
        let worker = test_worker(health.clone(), Duration::from_secs(2));
        let job = NotificationJob::new(endpoint(server.url()), NotificationEvent::test());

        let report = worker.deliver(&job).await;

        assert_eq!(
            NotificationDeliveryOutcome::NonOk {
                status: StatusCode::SERVICE_UNAVAILABLE
            },
            report.final_outcome
        );
        assert_eq!(2, report.attempts.len());
        assert_eq!(2, server.requests().len());
        let state = health.state(&notification_dependency_key(&job.endpoint));
        assert!(matches!(
            state,
            Some(crate::domain::DependencyState::Degraded { .. })
        ));
    }

    #[tokio::test]
    async fn delivery_does_not_retry_terminal_non_ok() {
        let server = TestServer::new(TestBehavior::Status(StatusCode::BAD_REQUEST)).await;
        let health = HealthRegistry::new();
        let worker = test_worker(health, Duration::from_secs(2));
        let job = NotificationJob::new(endpoint(server.url()), NotificationEvent::test());

        let report = worker.deliver(&job).await;

        assert_eq!(
            NotificationDeliveryOutcome::NonOk {
                status: StatusCode::BAD_REQUEST
            },
            report.final_outcome
        );
        assert_eq!(1, report.attempts.len());
        assert_eq!(1, server.requests().len());
    }

    #[tokio::test]
    async fn delivery_times_out_without_failing_worker_loop() {
        let server = TestServer::new(TestBehavior::Slow(Duration::from_millis(100))).await;
        let health = HealthRegistry::new();
        let worker = test_worker(health, Duration::from_millis(10));
        let (queue, receiver) =
            notification_queue(NonZeroUsize::new(1).unwrap_or(NonZeroUsize::MIN));
        let job = NotificationJob::new(endpoint(server.url()), NotificationEvent::test());
        queue.try_enqueue(job).unwrap();
        drop(queue);

        let (_controller, signal) = shutdown_channel();
        run_notification_worker(worker, receiver, signal).await;

        assert_eq!(2, server.requests().len());
    }

    #[tokio::test]
    async fn delivery_stops_retry_sleep_on_shutdown() {
        let server = TestServer::new(TestBehavior::Status(StatusCode::SERVICE_UNAVAILABLE)).await;
        let health = HealthRegistry::new();
        let worker = NotificationWorker::with_config(
            health,
            MetricsRegistry::new(),
            Duration::from_secs(2),
            NotificationRetryPolicy {
                max_attempts: NonZeroU8::new(2).unwrap_or(NonZeroU8::MIN),
                initial_delay: Duration::from_secs(60),
                max_delay: Duration::from_secs(60),
            },
        );
        let job = NotificationJob::new(endpoint(server.url()), NotificationEvent::test());
        let (controller, signal) = shutdown_channel();

        let handle =
            tokio::spawn(async move { worker.deliver_until_shutdown(&job, Some(&signal)).await });
        while server.requests().is_empty() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        controller.cancel_now("test shutdown").unwrap();
        let report = tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(1, report.attempts.len());
        assert_eq!(1, server.requests().len());
    }

    #[tokio::test]
    async fn worker_stops_retrying_delivery_on_shutdown() {
        let server = TestServer::new(TestBehavior::Status(StatusCode::SERVICE_UNAVAILABLE)).await;
        let health = HealthRegistry::new();
        let worker = NotificationWorker::with_config(
            health,
            MetricsRegistry::new(),
            Duration::from_secs(2),
            NotificationRetryPolicy {
                max_attempts: NonZeroU8::new(2).unwrap_or(NonZeroU8::MIN),
                initial_delay: Duration::from_secs(60),
                max_delay: Duration::from_secs(60),
            },
        );
        let (queue, receiver) =
            notification_queue(NonZeroUsize::new(2).unwrap_or(NonZeroUsize::MIN));
        let job = NotificationJob::new(endpoint(server.url()), NotificationEvent::test());
        queue.try_enqueue(job).unwrap();
        let (controller, signal) = shutdown_channel();

        let handle = tokio::spawn(run_notification_worker(worker, receiver, signal));
        while server.requests().is_empty() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        controller.cancel_now("test shutdown").unwrap();
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(1, server.requests().len());
        assert_eq!(0, queue.stats().depth);
        assert_eq!(1, queue.stats().completed);
    }

    #[test]
    fn debug_and_errors_redact_tokens_and_secret_urls() {
        let endpoint = endpoint(
            "https://user:password@example.invalid/hook?token=url-token&ok=true#fragment"
                .to_owned(),
        )
        .with_token(NotificationToken::new("bearer-secret").unwrap());
        let job = NotificationJob::new(endpoint.clone(), NotificationEvent::test());
        let rendered = format!("{job:?}");

        assert!(!rendered.contains("bearer-secret"));
        assert!(!rendered.contains("url-token"));
        assert!(!rendered.contains("password"));
        assert!(rendered.contains("[REDACTED]"));
    }

    fn endpoint(url: String) -> NotificationEndpoint {
        NotificationEndpoint::new(DependencyName::new("webhook-main").unwrap(), url)
    }

    fn test_worker(health: HealthRegistry, timeout: Duration) -> NotificationWorker {
        NotificationWorker::with_config(
            health,
            MetricsRegistry::new(),
            timeout,
            NotificationRetryPolicy {
                max_attempts: NonZeroU8::new(2).unwrap_or(NonZeroU8::MIN),
                initial_delay: Duration::ZERO,
                max_delay: Duration::from_millis(1),
            },
        )
    }

    #[derive(Debug, Clone)]
    enum TestBehavior {
        Ok,
        Status(StatusCode),
        Slow(Duration),
    }

    #[derive(Debug, Clone)]
    struct CapturedRequest {
        authorization: Option<String>,
        content_type: Option<String>,
        body: Value,
    }

    #[derive(Clone)]
    struct TestState {
        behavior: TestBehavior,
        requests: Arc<Mutex<Vec<CapturedRequest>>>,
    }

    struct TestServer {
        addr: SocketAddr,
        requests: Arc<Mutex<Vec<CapturedRequest>>>,
    }

    impl TestServer {
        async fn new(behavior: TestBehavior) -> Self {
            let requests = Arc::new(Mutex::new(Vec::new()));
            let state = TestState {
                behavior,
                requests: Arc::clone(&requests),
            };
            let app = Router::new()
                .route("/hook", post(capture))
                .with_state(state);
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            });

            Self { addr, requests }
        }

        fn url(&self) -> String {
            format!("http://{}/hook", self.addr)
        }

        fn requests(&self) -> Vec<CapturedRequest> {
            self.requests
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
        }
    }

    async fn capture(
        State(state): State<TestState>,
        headers: HeaderMap,
        body: Bytes,
    ) -> impl IntoResponse {
        let parsed = serde_json::from_slice(&body).unwrap_or_else(|_error| json!({}));
        state
            .requests
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(CapturedRequest {
                authorization: headers
                    .get(AUTHORIZATION)
                    .and_then(|value| value.to_str().ok())
                    .map(ToOwned::to_owned),
                content_type: headers
                    .get(CONTENT_TYPE)
                    .and_then(|value| value.to_str().ok())
                    .map(ToOwned::to_owned),
                body: parsed,
            });

        match state.behavior {
            TestBehavior::Ok => StatusCode::NO_CONTENT,
            TestBehavior::Status(status) => status,
            TestBehavior::Slow(delay) => {
                tokio::time::sleep(delay).await;
                StatusCode::NO_CONTENT
            }
        }
    }
}
