//! Push notification webhook payloads and delivery.

use std::{
    borrow::Cow,
    future::Future,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use reqwest::{Client, header::CONTENT_TYPE};
use serde::Serialize;
use serde_json::{Value, json};
use tokio::runtime::Builder;

use crate::{
    SporosError, VERSION,
    config::{NotificationPayloadDetail, RuntimeConfig},
    domain::{ActionResult, InjectionResult, SaveResult},
    retry::{
        RetryClass, RetryContext, RetryDecision, RetryError, RetryPolicy, classify_http_status,
        classify_reqwest_error, retry,
    },
    search::PipelineAttempt,
    startup::Redactor,
};

const NOTIFICATION_TIMEOUT: Duration = Duration::from_secs(5 * 60);

/// Summary of one notification delivery pass.
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
pub struct NotificationReport {
    /// Webhook URLs attempted.
    pub attempted: usize,
    /// Webhook URLs that returned a successful HTTP status.
    pub succeeded: usize,
    /// Webhook URLs that failed or returned an error status.
    pub failed: usize,
    /// Webhook URLs whose retry policy was exhausted.
    pub retry_exhausted: usize,
}

/// Webhook notification sender.
#[derive(Debug, Clone)]
pub struct NotificationSender {
    urls: Vec<String>,
    client: Client,
    redactor: Redactor,
    payload_detail: NotificationPayloadDetail,
}

impl NotificationSender {
    /// Build a sender from normalized runtime config.
    pub fn from_config(config: &RuntimeConfig, redactor: Redactor) -> crate::Result<Self> {
        Self::new_with_payload_detail(
            config.notification_webhook_urls.clone(),
            redactor,
            config.notification_payload_detail,
        )
    }

    /// Build a sender from normalized runtime config with an explicit timeout.
    pub(crate) fn from_config_with_timeout(
        config: &RuntimeConfig,
        redactor: Redactor,
        timeout: Duration,
    ) -> crate::Result<Self> {
        Self::new_with_timeout(
            config.notification_webhook_urls.clone(),
            redactor,
            timeout,
            config.notification_payload_detail,
        )
    }

    /// Build a sender from explicit URLs.
    pub fn new(urls: Vec<String>, redactor: Redactor) -> crate::Result<Self> {
        Self::new_with_payload_detail(urls, redactor, NotificationPayloadDetail::default())
    }

    pub(crate) fn new_with_payload_detail(
        urls: Vec<String>,
        redactor: Redactor,
        payload_detail: NotificationPayloadDetail,
    ) -> crate::Result<Self> {
        Self::new_with_timeout(urls, redactor, NOTIFICATION_TIMEOUT, payload_detail)
    }

    fn new_with_timeout(
        urls: Vec<String>,
        redactor: Redactor,
        timeout: Duration,
        payload_detail: NotificationPayloadDetail,
    ) -> crate::Result<Self> {
        let client = Client::builder()
            .timeout(timeout)
            .user_agent(format!("CrossSeed/{VERSION}"))
            .build()
            .map_err(|error| notification_error(format!("failed to build notifier: {error}")))?;
        Ok(Self {
            urls,
            client,
            redactor,
            payload_detail,
        })
    }

    /// Send the documented test notification payload.
    pub fn send_test(&self) -> NotificationReport {
        if self.urls.is_empty() {
            return NotificationReport::default();
        }
        block_on_notification(self.send_test_async()).unwrap_or_else(|error| {
            tracing::warn!(error = %error, "failed to run notification delivery");
            NotificationReport {
                attempted: self.urls.len(),
                succeeded: 0,
                failed: self.urls.len(),
                retry_exhausted: 0,
            }
        })
    }

    /// Validate configured notification webhooks during startup.
    pub fn validate_startup(&self) -> crate::Result<()> {
        let report = self.validate_startup_report();
        if report.failed == 0 {
            Ok(())
        } else {
            Err(notification_error(format!(
                "failed to validate {}/{} notification webhooks",
                report.failed, report.attempted
            )))
        }
    }

    /// Validate configured notification webhooks and return the delivery report.
    pub fn validate_startup_report(&self) -> NotificationReport {
        if self.urls.is_empty() {
            return NotificationReport::default();
        }
        block_on_notification(self.validate_startup_async()).unwrap_or_else(|error| {
            tracing::warn!(error = %error, "failed to run notification startup validation");
            NotificationReport {
                attempted: self.urls.len(),
                succeeded: 0,
                failed: self.urls.len(),
                retry_exhausted: 0,
            }
        })
    }

    /// Send the documented test notification payload asynchronously.
    pub async fn send_test_async(&self) -> NotificationReport {
        self.post_all(NotificationPayload {
            title: "cross-seed".to_owned(),
            body: "test notification".to_owned(),
            extra: json!({ "event": "TEST" }),
        })
        .await
    }

    async fn validate_startup_async(&self) -> NotificationReport {
        self.post_all(NotificationPayload {
            title: "cross-seed".to_owned(),
            body: "startup validation".to_owned(),
            extra: json!({ "event": "STARTUP_VALIDATION" }),
        })
        .await
    }

    /// Send a result notification when an attempt has a notifiable action result.
    pub fn send_result(&self, attempt: &PipelineAttempt) -> NotificationReport {
        if self.urls.is_empty() {
            return NotificationReport::default();
        }
        block_on_notification(self.send_result_async(attempt)).unwrap_or_else(|error| {
            tracing::warn!(error = %error, "failed to run notification delivery");
            NotificationReport {
                attempted: self.urls.len(),
                succeeded: 0,
                failed: self.urls.len(),
                retry_exhausted: 0,
            }
        })
    }

    /// Send a result notification asynchronously when an attempt has a notifiable action result.
    pub async fn send_result_async(&self, attempt: &PipelineAttempt) -> NotificationReport {
        let Some(payload) = result_payload(attempt, self.payload_detail) else {
            return NotificationReport::default();
        };
        self.post_all(payload).await
    }

    async fn post_all(&self, payload: NotificationPayload) -> NotificationReport {
        let mut report = NotificationReport {
            attempted: self.urls.len(),
            ..NotificationReport::default()
        };
        let body = match serde_json::to_vec(&payload) {
            Ok(body) => body,
            Err(error) => {
                tracing::warn!(error = %error, "failed to serialize notification payload");
                report.failed = report.attempted;
                return report;
            }
        };
        for url in &self.urls {
            match self.post_one(url, body.clone()).await {
                Ok(()) => {
                    report.succeeded += 1;
                }
                Err(error) => {
                    report.failed += 1;
                    if matches!(error, RetryError::Exhausted { .. }) {
                        report.retry_exhausted += 1;
                    }
                    tracing::warn!(
                        url = self.redactor.redact(url),
                        error = self.redactor.redact(&error.to_string()),
                        "notification webhook delivery failed"
                    );
                }
            }
        }
        report
    }

    async fn post_one(
        &self,
        url: &str,
        body: Vec<u8>,
    ) -> Result<(), RetryError<NotificationFailure>> {
        let client = self.client.clone();
        let url = url.to_owned();
        retry(
            RetryPolicy::idempotent(),
            RetryContext::new(
                "notification_webhook",
                tokio_util::sync::CancellationToken::new(),
            )
            .with_target(self.redactor.redact(&url)),
            move |_attempt| {
                let client = client.clone();
                let url = url.clone();
                let body = body.clone();
                async move {
                    match client
                        .post(&url)
                        .header(CONTENT_TYPE, "application/json")
                        .body(body)
                        .send()
                        .await
                    {
                        Ok(response) => classify_notification_response(response),
                        Err(error) => match classify_reqwest_error(&error) {
                            RetryClass::Retryable { retry_after } => RetryDecision::Retryable {
                                error: NotificationFailure {
                                    message: error.to_string(),
                                    status: error.status(),
                                    retry_after,
                                },
                                retry_after,
                            },
                            RetryClass::Fatal => RetryDecision::Fatal(NotificationFailure {
                                message: error.to_string(),
                                status: error.status(),
                                retry_after: None,
                            }),
                        },
                    }
                }
            },
        )
        .await
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct NotificationFailure {
    message: String,
    status: Option<reqwest::StatusCode>,
    retry_after: Option<Duration>,
}

impl std::fmt::Display for NotificationFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for NotificationFailure {}

fn classify_notification_response(
    response: reqwest::Response,
) -> RetryDecision<(), NotificationFailure> {
    let status = response.status();
    if status.is_success() {
        return RetryDecision::Success(());
    }
    match classify_http_status(status, response.headers(), current_time_millis()) {
        RetryClass::Retryable { retry_after } => RetryDecision::Retryable {
            error: NotificationFailure {
                message: format!("notification webhook returned HTTP {status}"),
                status: Some(status),
                retry_after,
            },
            retry_after,
        },
        RetryClass::Fatal => RetryDecision::Fatal(NotificationFailure {
            message: format!("notification webhook returned HTTP {status}"),
            status: Some(status),
            retry_after: None,
        }),
    }
}

#[derive(Debug, Serialize)]
struct NotificationPayload {
    title: String,
    body: String,
    extra: Value,
}

fn result_payload(
    attempt: &PipelineAttempt,
    detail: NotificationPayloadDetail,
) -> Option<NotificationPayload> {
    let result = notification_result(attempt.action_result?)?;
    let body = match detail {
        NotificationPayloadDetail::Redacted => format!("{result}: redacted"),
        NotificationPayloadDetail::Full => format!("{result}: {}", attempt.searchee_title),
    };
    let extra = match detail {
        NotificationPayloadDetail::Redacted => redacted_result_extra(attempt, result),
        NotificationPayloadDetail::Full => full_result_extra(attempt, result),
    };
    Some(NotificationPayload {
        title: "cross-seed".to_owned(),
        body,
        extra,
    })
}

fn redacted_result_extra(attempt: &PipelineAttempt, result: &str) -> Value {
    json!({
        "event": "RESULTS",
        "source": attempt.label.as_str(),
        "result": result,
        "paused": null,
        "decisions": [attempt.decision.as_str()],
        "redacted": true,
        "searchee": {
            "length": attempt.searchee_length,
            "sourceType": &attempt.searchee_source_type,
        }
    })
}

fn full_result_extra(attempt: &PipelineAttempt, result: &str) -> Value {
    json!({
        "event": "RESULTS",
        "name": &attempt.candidate_name,
        "infoHashes": &attempt.candidate_info_hashes,
        "trackers": &attempt.trackers,
        "source": attempt.label.as_str(),
        "result": result,
        "paused": null,
        "decisions": [attempt.decision.as_str()],
        "searchee": {
            "category": &attempt.searchee_category,
            "tags": &attempt.searchee_tags,
            "trackers": &attempt.searchee_trackers,
            "length": attempt.searchee_length,
            "clientHost": &attempt.searchee_client_host,
            "infoHash": &attempt.searchee_info_hash,
            "path": &attempt.searchee_path,
            "sourceType": &attempt.searchee_source_type,
        }
    })
}

fn notification_result(result: ActionResult) -> Option<&'static str> {
    match result {
        ActionResult::Save(SaveResult::Saved) => Some("SAVED"),
        ActionResult::Injection(InjectionResult::Injected) => Some("INJECTED"),
        ActionResult::Injection(InjectionResult::Failure) => Some("SAVED"),
        ActionResult::Injection(
            InjectionResult::AlreadyExists | InjectionResult::TorrentNotComplete,
        ) => None,
    }
}

fn notification_error(message: impl Into<Cow<'static, str>>) -> SporosError {
    SporosError::Integration {
        message: message.into(),
    }
}

fn current_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(duration_millis)
        .unwrap_or(0)
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn block_on_notification<F, T>(future: F) -> crate::Result<T>
where
    F: Future<Output = T>,
{
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| {
            notification_error(format!("failed to build notifier runtime: {error}"))
        })?;
    Ok(runtime.block_on(future))
}

#[cfg(test)]
mod tests {
    use super::{NotificationSender, result_payload};
    use crate::{
        config::NotificationPayloadDetail,
        domain::{ActionResult, Decision, InjectionResult, Label, SaveResult},
        search::PipelineAttempt,
        startup::Redactor,
    };
    use std::{
        io::{BufRead, BufReader, Read, Write},
        net::{TcpListener, TcpStream},
        thread,
    };

    #[test]
    fn test_notification_posts_json_payload() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let url = format!("http://{}", listener.local_addr().expect("addr"));
        let handle = std::thread::spawn(move || {
            let (mut stream, _remote) = listener.accept().expect("accept");
            let request = read_http_request(&mut stream);
            stream
                .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
                .expect("response");
            request
        });
        let sender = NotificationSender::new(vec![url], Redactor::default()).expect("sender");

        let report = sender.send_test();
        let request = handle.join().expect("server");

        assert_eq!(report.attempted, 1);
        assert_eq!(report.succeeded, 1);
        assert_eq!(report.retry_exhausted, 0);
        assert!(request.contains("user-agent: crossseed/"));
        assert!(request.contains(r#""event":"TEST""#));
    }

    #[test]
    fn notification_retries_transient_status() {
        let server = notification_server(vec![
            "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n",
            "HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n",
        ]);
        let sender =
            NotificationSender::new(vec![server.url.clone()], Redactor::default()).expect("sender");

        let report = sender.send_test();
        let requests = server.join();

        assert_eq!(report.attempted, 1);
        assert_eq!(report.succeeded, 1);
        assert_eq!(report.failed, 0);
        assert_eq!(report.retry_exhausted, 0);
        assert_eq!(requests.len(), 2);
    }

    #[test]
    fn notification_reports_retry_exhaustion() {
        let server = notification_server(vec![
            "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n",
            "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n",
            "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n",
        ]);
        let sender =
            NotificationSender::new(vec![server.url.clone()], Redactor::default()).expect("sender");

        let report = sender.send_test();
        let requests = server.join();

        assert_eq!(report.attempted, 1);
        assert_eq!(report.succeeded, 0);
        assert_eq!(report.failed, 1);
        assert_eq!(report.retry_exhausted, 1);
        assert_eq!(requests.len(), 3);
    }

    #[test]
    fn result_payload_contains_contract_fields_and_skips_non_results() {
        let mut attempt = attempt(ActionResult::Injection(InjectionResult::Injected));

        let payload = result_payload(&attempt, NotificationPayloadDetail::Full).expect("payload");

        assert_eq!(payload.title, "cross-seed");
        assert_eq!(payload.body, "INJECTED: Example Show");
        assert_eq!(payload.extra["event"], "RESULTS");
        assert_eq!(payload.extra["source"], "search");
        assert_eq!(payload.extra["result"], "INJECTED");
        assert_eq!(payload.extra["searchee"]["clientHost"], "client-a");

        attempt.action_result = Some(ActionResult::Injection(InjectionResult::AlreadyExists));
        assert!(result_payload(&attempt, NotificationPayloadDetail::Full).is_none());
    }

    #[test]
    fn result_payload_redacts_sensitive_fields_by_default() {
        let attempt = attempt(ActionResult::Save(SaveResult::Saved));

        let payload =
            result_payload(&attempt, NotificationPayloadDetail::Redacted).expect("payload");
        let serialized = serde_json::to_string(&payload).expect("payload json");

        assert_eq!(payload.body, "SAVED: redacted");
        assert_eq!(payload.extra["event"], "RESULTS");
        assert_eq!(payload.extra["source"], "search");
        assert_eq!(payload.extra["result"], "SAVED");
        assert_eq!(payload.extra["redacted"], true);
        assert_eq!(payload.extra["searchee"]["length"], 123);
        assert_eq!(payload.extra["searchee"]["sourceType"], "torrentClient");
        assert!(payload.extra.get("name").is_none());
        assert!(payload.extra.get("infoHashes").is_none());
        assert!(payload.extra.get("trackers").is_none());
        assert!(payload.extra["searchee"].get("clientHost").is_none());
        assert!(!serialized.contains("Example.Show.2024"));
        assert!(!serialized.contains("0123456789012345678901234567890123456789"));
        assert!(!serialized.contains("tracker.example"));
        assert!(!serialized.contains("client-a"));
        assert!(!serialized.contains("/data/Example Show"));
    }

    #[test]
    fn notification_failures_are_reported_not_returned() {
        let sender = NotificationSender::new(
            vec!["http://127.0.0.1:9/hook?token=secret".to_owned()],
            Redactor::default(),
        )
        .expect("sender");

        let report = sender.send_test();

        assert_eq!(report.attempted, 1);
        assert_eq!(report.succeeded, 0);
        assert_eq!(report.failed, 1);
    }

    fn attempt(result: ActionResult) -> PipelineAttempt {
        PipelineAttempt {
            label: Label::Search,
            searchee_title: "Example Show".to_owned(),
            candidate_name: "Example.Show.2024".to_owned(),
            candidate_guid: "guid".to_owned(),
            candidate_info_hashes: vec!["0123456789012345678901234567890123456789".to_owned()],
            trackers: vec!["tracker.example".to_owned()],
            decision: Decision::Match,
            action_result: Some(result),
            searchee_category: Some("tv".to_owned()),
            searchee_tags: vec!["cross-seed".to_owned()],
            searchee_trackers: vec!["local.tracker".to_owned()],
            searchee_length: 123,
            searchee_client_host: Some("client-a".to_owned()),
            searchee_info_hash: Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()),
            searchee_path: Some("/data/Example Show".to_owned()),
            searchee_source_type: "torrentClient".to_owned(),
        }
    }

    fn read_http_request(stream: &mut TcpStream) -> String {
        let mut reader = BufReader::new(stream);
        let mut request = String::new();
        let mut content_length = 0usize;
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).expect("line");
            if line.eq_ignore_ascii_case("\r\n") || line == "\n" {
                break;
            }
            if let Some((name, value)) = line.split_once(':') {
                if name.eq_ignore_ascii_case("content-length") {
                    content_length = value.trim().parse().expect("length");
                }
            }
            request.push_str(&line.to_ascii_lowercase());
        }
        let mut body = vec![0_u8; content_length];
        reader.read_exact(&mut body).expect("body");
        request.push_str(&String::from_utf8_lossy(&body));
        request
    }

    struct TestNotificationServer {
        url: String,
        handle: thread::JoinHandle<Vec<String>>,
    }

    impl TestNotificationServer {
        fn join(self) -> Vec<String> {
            self.handle.join().expect("server")
        }
    }

    fn notification_server(responses: Vec<&'static str>) -> TestNotificationServer {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let url = format!("http://{}", listener.local_addr().expect("addr"));
        let handle = thread::spawn(move || {
            let mut requests = Vec::new();
            for response in responses {
                let (mut stream, _remote) = listener.accept().expect("accept");
                requests.push(read_http_request(&mut stream));
                stream.write_all(response.as_bytes()).expect("response");
            }
            requests
        });
        TestNotificationServer { url, handle }
    }
}
