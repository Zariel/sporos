//! Push notification webhook payloads and delivery.

use std::{borrow::Cow, future::Future, time::Duration};

use reqwest::{Client, header::CONTENT_TYPE};
use serde::Serialize;
use serde_json::{Value, json};
use tokio::runtime::Builder;

use crate::{
    SporosError, VERSION,
    config::RuntimeConfig,
    domain::{ActionResult, InjectionResult, SaveResult},
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
}

/// Webhook notification sender.
#[derive(Debug, Clone)]
pub struct NotificationSender {
    urls: Vec<String>,
    client: Client,
    redactor: Redactor,
}

impl NotificationSender {
    /// Build a sender from normalized runtime config.
    pub fn from_config(config: &RuntimeConfig, redactor: Redactor) -> crate::Result<Self> {
        Self::new(config.notification_webhook_urls.clone(), redactor)
    }

    /// Build a sender from explicit URLs.
    pub fn new(urls: Vec<String>, redactor: Redactor) -> crate::Result<Self> {
        let client = Client::builder()
            .timeout(NOTIFICATION_TIMEOUT)
            .user_agent(format!("CrossSeed/{VERSION}"))
            .build()
            .map_err(|error| notification_error(format!("failed to build notifier: {error}")))?;
        Ok(Self {
            urls,
            client,
            redactor,
        })
    }

    /// Send the documented test notification payload.
    pub fn send_test(&self) -> NotificationReport {
        block_on_notification(self.send_test_async()).unwrap_or_else(|error| {
            tracing::warn!(error = %error, "failed to run notification delivery");
            NotificationReport {
                attempted: self.urls.len(),
                succeeded: 0,
                failed: self.urls.len(),
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

    /// Send a result notification when an attempt has a notifiable action result.
    pub fn send_result(&self, attempt: &PipelineAttempt) -> NotificationReport {
        block_on_notification(self.send_result_async(attempt)).unwrap_or_else(|error| {
            tracing::warn!(error = %error, "failed to run notification delivery");
            NotificationReport {
                attempted: self.urls.len(),
                succeeded: 0,
                failed: self.urls.len(),
            }
        })
    }

    /// Send a result notification asynchronously when an attempt has a notifiable action result.
    pub async fn send_result_async(&self, attempt: &PipelineAttempt) -> NotificationReport {
        let Some(payload) = result_payload(attempt) else {
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
            match self
                .client
                .post(url)
                .header(CONTENT_TYPE, "application/json")
                .body(body.clone())
                .send()
                .await
            {
                Ok(response) if response.status().is_success() => {
                    report.succeeded += 1;
                }
                Ok(response) => {
                    report.failed += 1;
                    tracing::warn!(
                        url = self.redactor.redact(url),
                        status = response.status().as_u16(),
                        "notification webhook returned an error status"
                    );
                }
                Err(error) => {
                    report.failed += 1;
                    tracing::warn!(
                        url = self.redactor.redact(url),
                        error = self.redactor.redact(&error.to_string()),
                        "notification webhook request failed"
                    );
                }
            }
        }
        report
    }
}

#[derive(Debug, Serialize)]
struct NotificationPayload {
    title: String,
    body: String,
    extra: Value,
}

fn result_payload(attempt: &PipelineAttempt) -> Option<NotificationPayload> {
    let result = notification_result(attempt.action_result?)?;
    let body = format!("{result}: {}", attempt.searchee_title);
    Some(NotificationPayload {
        title: "cross-seed".to_owned(),
        body,
        extra: json!({
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
        }),
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
        domain::{ActionResult, Decision, InjectionResult, Label},
        search::PipelineAttempt,
        startup::Redactor,
    };
    use std::{
        io::{BufRead, BufReader, Read, Write},
        net::{TcpListener, TcpStream},
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
        assert!(request.contains("user-agent: crossseed/"));
        assert!(request.contains(r#""event":"TEST""#));
    }

    #[test]
    fn result_payload_contains_contract_fields_and_skips_non_results() {
        let mut attempt = attempt(ActionResult::Injection(InjectionResult::Injected));

        let payload = result_payload(&attempt).expect("payload");

        assert_eq!(payload.title, "cross-seed");
        assert_eq!(payload.body, "INJECTED: Example Show");
        assert_eq!(payload.extra["event"], "RESULTS");
        assert_eq!(payload.extra["source"], "search");
        assert_eq!(payload.extra["result"], "INJECTED");
        assert_eq!(payload.extra["searchee"]["clientHost"], "client-a");

        attempt.action_result = Some(ActionResult::Injection(InjectionResult::AlreadyExists));
        assert!(result_payload(&attempt).is_none());
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
}
