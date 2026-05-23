use std::env;
use std::error::Error;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use reqwest::StatusCode;
use reqwest::header::CONTENT_TYPE;
use serde::Deserialize;
use serde_json::Value;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Debug)]
struct SystemContext {
    http_url: String,
    api_token: String,
    project: String,
    compose_file: String,
    compose_override: String,
    container_config: String,
}

#[derive(Debug, Deserialize)]
struct SystemSnapshot {
    local_items: i64,
    remote_candidates: i64,
    cached_candidates: i64,
    match_decisions: i64,
    enabled_indexers: i64,
}

#[tokio::test]
#[ignore = "requires scripts/system-test torrent-clients prepared compose context"]
async fn real_torrent_client_harness_uses_prepared_compose_stack() -> TestResult {
    let Some(context) = SystemContext::from_env()? else {
        eprintln!(
            "skipping real torrent-client system test: run scripts/system-test torrent-clients"
        );
        return Ok(());
    };

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;

    context.wait_for_sporos_ready(&client).await?;
    context.assert_qbittorrent_api().await?;
    context.assert_rtorrent_api().await?;

    eventually("seeded candidates and scanned local inventory", || async {
        let snapshot = context.snapshot().await?;
        Ok(snapshot.enabled_indexers >= 1
            && snapshot.remote_candidates >= 2
            && snapshot.cached_candidates >= 2
            && snapshot.local_items >= 2)
    })
    .await?;

    let accepted = client
        .post(context.url("/v1/searches"))
        .bearer_auth(&context.api_token)
        .header(CONTENT_TYPE, "application/json")
        .body(r#"{"query":"Sporos qBittorrent Fixture"}"#)
        .send()
        .await?;
    if accepted.status() != StatusCode::ACCEPTED {
        return Err(format!("search request returned {}", accepted.status()).into());
    }

    eventually("search workflow persisted a match decision", || async {
        let snapshot = context.snapshot().await?;
        Ok(snapshot.match_decisions >= 1)
    })
    .await?;

    let status = context.get_json(&client, "/v1/status").await?;
    if status.get("status").and_then(Value::as_str) != Some("ok") {
        return Err(format!("unexpected Sporos status response: {status}").into());
    }

    Ok(())
}

impl SystemContext {
    fn from_env() -> TestResult<Option<Self>> {
        let names = [
            "SPOROS_SYSTEM_HTTP_URL",
            "SPOROS_SYSTEM_API_TOKEN",
            "SPOROS_SYSTEM_PROJECT",
            "SPOROS_SYSTEM_COMPOSE_FILE",
            "SPOROS_SYSTEM_COMPOSE_OVERRIDE",
            "SPOROS_SYSTEM_CONTAINER_CONFIG",
        ];
        let mut values = Vec::with_capacity(names.len());
        let mut missing = Vec::new();
        for name in names {
            match env::var(name) {
                Ok(value) if !value.trim().is_empty() => values.push(value),
                Ok(_) | Err(env::VarError::NotPresent) => missing.push(name),
                Err(error) => return Err(format!("read {name}: {error}").into()),
            }
        }
        if missing.len() == names.len() {
            return Ok(None);
        }
        if !missing.is_empty() {
            return Err(format!(
                "missing system test context variables: {}",
                missing.join(", ")
            )
            .into());
        }

        let mut values = values.into_iter();
        let context = Self {
            http_url: values.next().ok_or("missing SPOROS_SYSTEM_HTTP_URL")?,
            api_token: values.next().ok_or("missing SPOROS_SYSTEM_API_TOKEN")?,
            project: values.next().ok_or("missing SPOROS_SYSTEM_PROJECT")?,
            compose_file: values.next().ok_or("missing SPOROS_SYSTEM_COMPOSE_FILE")?,
            compose_override: values
                .next()
                .ok_or("missing SPOROS_SYSTEM_COMPOSE_OVERRIDE")?,
            container_config: values
                .next()
                .ok_or("missing SPOROS_SYSTEM_CONTAINER_CONFIG")?,
        };
        Ok(Some(context))
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.http_url.trim_end_matches('/'), path)
    }

    async fn wait_for_sporos_ready(&self, client: &reqwest::Client) -> TestResult {
        eventually("Sporos readyz", || async {
            let response = client
                .get(self.url("/readyz"))
                .bearer_auth(&self.api_token)
                .send()
                .await?;
            Ok(response.status() == StatusCode::OK)
        })
        .await
    }

    async fn get_json(&self, client: &reqwest::Client, path: &str) -> TestResult<Value> {
        let response = client
            .get(self.url(path))
            .bearer_auth(&self.api_token)
            .send()
            .await?;
        let status = response.status();
        let body = response.text().await?;
        if !status.is_success() {
            return Err(format!("GET {path} returned {status}: {body}").into());
        }
        serde_json::from_str(&body).map_err(Into::into)
    }

    async fn snapshot(&self) -> TestResult<SystemSnapshot> {
        let output = self.compose_output(&[
            "exec",
            "-T",
            "sporos",
            "sporos",
            "system-test-snapshot",
            "--config",
            &self.container_config,
        ])?;
        serde_json::from_str(&output).map_err(Into::into)
    }

    async fn assert_qbittorrent_api(&self) -> TestResult {
        let output =
            self.compose_run_system_init("wget -qO- http://qbittorrent:8080/api/v2/app/version")?;
        let trimmed = output.trim();
        if trimmed.is_empty() {
            return Err("qBittorrent version response was empty".into());
        }
        Ok(())
    }

    async fn assert_rtorrent_api(&self) -> TestResult {
        let output = self.compose_run_system_init(
            "wget -qO- --header='Content-Type: text/xml' --post-data='<methodCall><methodName>download_list</methodName><params></params></methodCall>' http://rtorrent:8000/RPC2",
        )?;
        if !output.contains("<methodResponse>") {
            return Err("rTorrent XML-RPC response did not contain methodResponse".into());
        }
        Ok(())
    }

    fn compose_run_system_init(&self, script: &str) -> TestResult<String> {
        self.compose_output(&[
            "run",
            "--rm",
            "--no-deps",
            "system-init",
            "/bin/sh",
            "-eu",
            "-c",
            script,
        ])
    }

    fn compose_output(&self, args: &[&str]) -> TestResult<String> {
        let mut command = Command::new("docker");
        command
            .arg("compose")
            .arg("--project-name")
            .arg(&self.project)
            .arg("-f")
            .arg(&self.compose_file)
            .arg("-f")
            .arg(&self.compose_override)
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let output = output_with_timeout(command, Duration::from_secs(30))?;
        if !output.status.success() {
            return Err(format!(
                "docker compose {} failed with status {:?}\nstdout:\n{}\nstderr:\n{}",
                args.join(" "),
                output.status.code(),
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
        String::from_utf8(output.stdout).map_err(Into::into)
    }
}

fn output_with_timeout(
    mut command: Command,
    timeout: Duration,
) -> TestResult<std::process::Output> {
    let mut child = command.spawn()?;
    let deadline = Instant::now() + timeout;
    loop {
        if child.try_wait()?.is_some() {
            return child.wait_with_output().map_err(Into::into);
        }
        if Instant::now() >= deadline {
            let _kill_result = child.kill();
            let output = child.wait_with_output()?;
            return Err(format!(
                "command timed out after {timeout:?}\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
        thread::sleep(Duration::from_millis(100));
    }
}

async fn eventually<F, Fut>(label: &str, mut check: F) -> TestResult
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = TestResult<bool>>,
{
    let deadline = Instant::now() + Duration::from_secs(180);
    while Instant::now() < deadline {
        if check().await? {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    Err(format!("timed out waiting for {label}").into())
}
