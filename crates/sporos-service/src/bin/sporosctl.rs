use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand, ValueEnum};
use reqwest::StatusCode;
use serde_json::Value;
use sporos_service::config::Secret;

#[derive(Debug, Parser)]
#[command(name = "sporosctl", version, about = "Administer a Sporos service")]
struct Cli {
    #[arg(long, env = "SPOROS_URL", default_value = "http://127.0.0.1:8080")]
    url: String,
    #[arg(long, value_enum, default_value = "human")]
    output: Output,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Report liveness and readiness.
    Status,
    /// Inspect operator-facing task projections.
    Tasks {
        #[command(subcommand)]
        command: TaskCommand,
    },
    /// Inspect manual operations.
    Operations {
        #[command(subcommand)]
        command: OperationCommand,
    },
    /// Operate the qBittorrent inventory projection.
    Inventory {
        #[command(subcommand)]
        command: InventoryCommand,
    },
    /// Start a manual qBittorrent inventory search.
    Search {
        #[arg(long = "indexer")]
        indexers: Vec<i64>,
        #[arg(long)]
        force: bool,
        #[arg(long)]
        dry_run: bool,
    },
    /// Operate configured data roots.
    Data {
        #[command(subcommand)]
        command: DataCommand,
    },
}

#[derive(Debug, Subcommand)]
enum InventoryCommand {
    /// Request a durable full inventory reconciliation.
    Reconcile,
}

#[derive(Debug, Subcommand)]
enum TaskCommand {
    /// List recent tasks using keyset pagination.
    List {
        #[arg(long)]
        state: Option<String>,
        #[arg(long, default_value_t = 50, value_parser = clap::value_parser!(u16).range(1..=200))]
        limit: u16,
        #[arg(long)]
        cursor: Option<String>,
    },
    /// Show one task projection.
    Show { task_id: String },
    /// Show durable task evidence.
    Events { task_id: String },
    /// Retry a failed or cancelled task through a new durable start.
    Retry { task_id: String },
    /// Request cancellation of the authoritative workflow.
    Cancel { task_id: String },
}

#[derive(Debug, Subcommand)]
enum OperationCommand {
    /// List recent operations using keyset pagination.
    List {
        #[arg(long)]
        kind: Option<String>,
        #[arg(long, default_value_t = 50, value_parser = clap::value_parser!(u16).range(1..=200))]
        limit: u16,
        #[arg(long)]
        cursor: Option<String>,
    },
    /// Show one operation.
    Show { operation_id: String },
}

#[derive(Debug, Subcommand)]
enum DataCommand {
    /// Scan a configured data root.
    Scan {
        root: String,
        #[arg(long = "indexer")]
        indexers: Vec<i64>,
        #[arg(long)]
        force: bool,
        #[arg(long)]
        dry_run: bool,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Output {
    Human,
    Json,
}

#[tokio::main]
async fn main() -> ExitCode {
    match execute(Cli::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("sporosctl: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn execute(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    let base = reqwest::Url::parse(&cli.url)?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;
    match cli.command {
        Command::Status => status(&client, &base, cli.output).await,
        Command::Tasks { command } => {
            let api_key = api_key()?;
            match command {
                TaskCommand::List {
                    state,
                    limit,
                    cursor,
                } => {
                    let mut url = base.join("/api/v1/admin/tasks")?;
                    {
                        let mut query = url.query_pairs_mut();
                        query.append_pair("limit", &limit.to_string());
                        if let Some(state) = state {
                            query.append_pair("state", &state);
                        }
                        if let Some(cursor) = cursor {
                            query.append_pair("cursor", &cursor);
                        }
                    }
                    request_json(
                        &client,
                        reqwest::Method::GET,
                        url,
                        &api_key,
                        None,
                        cli.output,
                    )
                    .await
                }
                TaskCommand::Show { task_id } => {
                    let url = base.join(&format!("/api/v1/admin/tasks/{task_id}"))?;
                    request_json(
                        &client,
                        reqwest::Method::GET,
                        url,
                        &api_key,
                        None,
                        cli.output,
                    )
                    .await
                }
                TaskCommand::Events { task_id } => {
                    let url = base.join(&format!("/api/v1/admin/tasks/{task_id}/events"))?;
                    request_json(
                        &client,
                        reqwest::Method::GET,
                        url,
                        &api_key,
                        None,
                        cli.output,
                    )
                    .await
                }
                TaskCommand::Retry { task_id } => {
                    let url = base.join(&format!("/api/v1/admin/tasks/{task_id}/retry"))?;
                    request_json(
                        &client,
                        reqwest::Method::POST,
                        url,
                        &api_key,
                        Some(serde_json::json!({})),
                        cli.output,
                    )
                    .await
                }
                TaskCommand::Cancel { task_id } => {
                    let url = base.join(&format!("/api/v1/admin/tasks/{task_id}/cancel"))?;
                    request_json(
                        &client,
                        reqwest::Method::POST,
                        url,
                        &api_key,
                        Some(serde_json::json!({})),
                        cli.output,
                    )
                    .await
                }
            }
        }
        Command::Operations { command } => {
            let api_key = api_key()?;
            match command {
                OperationCommand::List {
                    kind,
                    limit,
                    cursor,
                } => {
                    let mut url = base.join("/api/v1/admin/operations")?;
                    {
                        let mut query = url.query_pairs_mut();
                        query.append_pair("limit", &limit.to_string());
                        if let Some(kind) = kind {
                            query.append_pair("kind", &kind);
                        }
                        if let Some(cursor) = cursor {
                            query.append_pair("cursor", &cursor);
                        }
                    }
                    request_json(
                        &client,
                        reqwest::Method::GET,
                        url,
                        &api_key,
                        None,
                        cli.output,
                    )
                    .await
                }
                OperationCommand::Show { operation_id } => {
                    let url = base.join(&format!("/api/v1/admin/operations/{operation_id}"))?;
                    request_json(
                        &client,
                        reqwest::Method::GET,
                        url,
                        &api_key,
                        None,
                        cli.output,
                    )
                    .await
                }
            }
        }
        Command::Inventory { command } => {
            let api_key = api_key()?;
            match command {
                InventoryCommand::Reconcile => {
                    let url = base.join("/api/v1/admin/inventory/reconcile")?;
                    request_json(
                        &client,
                        reqwest::Method::POST,
                        url,
                        &api_key,
                        Some(serde_json::json!({"full": true})),
                        cli.output,
                    )
                    .await
                }
            }
        }
        Command::Search {
            indexers,
            force,
            dry_run,
        } => {
            let api_key = api_key()?;
            let url = base.join("/api/v1/admin/searches")?;
            request_json(
                &client,
                reqwest::Method::POST,
                url,
                &api_key,
                Some(serde_json::json!({
                    "source": {
                        "kind": "qbittorrent",
                        "hashes": [],
                        "includeCategories": [],
                        "excludeCategories": [],
                        "includeTags": [],
                        "excludeTags": []
                    },
                    "indexerIds": indexers,
                    "force": force,
                    "dryRun": dry_run
                })),
                cli.output,
            )
            .await
        }
        Command::Data { command } => {
            let api_key = api_key()?;
            match command {
                DataCommand::Scan {
                    root,
                    indexers,
                    force,
                    dry_run,
                } => {
                    let url = base.join("/api/v1/admin/data-scans")?;
                    request_json(
                        &client,
                        reqwest::Method::POST,
                        url,
                        &api_key,
                        Some(serde_json::json!({
                            "root": root,
                            "indexerIds": indexers,
                            "force": force,
                            "dryRun": dry_run
                        })),
                        cli.output,
                    )
                    .await
                }
            }
        }
    }
}

async fn status(
    client: &reqwest::Client,
    base: &reqwest::Url,
    output: Output,
) -> Result<(), Box<dyn std::error::Error>> {
    let live = client.get(base.join("/livez")?).send().await?.status();
    let ready = client.get(base.join("/readyz")?).send().await?.status();
    match output {
        Output::Human => {
            println!("live: {}\nready: {}", health_word(live), health_word(ready));
        }
        Output::Json => println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "live": live.is_success(),
                "ready": ready.is_success(),
                "liveStatus": live.as_u16(),
                "readyStatus": ready.as_u16()
            }))?
        ),
    }
    if live.is_success() {
        Ok(())
    } else {
        Err("service is not live".into())
    }
}

fn health_word(status: StatusCode) -> &'static str {
    if status.is_success() {
        "ok"
    } else {
        "unavailable"
    }
}

async fn request_json(
    client: &reqwest::Client,
    method: reqwest::Method,
    url: reqwest::Url,
    api_key: &Option<Secret>,
    body: Option<Value>,
    output: Output,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut request = client.request(method, url);
    if let Some(api_key) = api_key {
        request = request.bearer_auth(api_key.expose());
    }
    if let Some(body) = body {
        request = request
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body.to_string());
    }
    let response = request.send().await?;
    let status = response.status();
    let body = response.text().await?;
    let value: Value = serde_json::from_str(&body)?;
    if !status.is_success() {
        let code = value
            .get("code")
            .and_then(Value::as_str)
            .unwrap_or("request_failed");
        return Err(format!("server returned {} ({code})", status.as_u16()).into());
    }
    match output {
        Output::Json => println!("{}", serde_json::to_string_pretty(&value)?),
        Output::Human => print_human(&value),
    }
    Ok(())
}

fn print_human(value: &Value) {
    if let Some(items) = value.get("items").and_then(Value::as_array) {
        println!("ID\tKIND\tSTATE\tUPDATED");
        for item in items {
            println!(
                "{}\t{}\t{}\t{}",
                field(item, "id"),
                field(item, "kind"),
                field(item, "state"),
                field(item, "updatedAt")
            );
        }
        if let Some(cursor) = value.get("nextCursor").and_then(Value::as_str) {
            println!("next cursor: {cursor}");
        }
    } else {
        println!(
            "{}",
            serde_json::to_string_pretty(value).unwrap_or_default()
        );
    }
}

fn field<'a>(value: &'a Value, name: &str) -> &'a str {
    value.get(name).and_then(Value::as_str).unwrap_or("-")
}

fn api_key() -> Result<Option<Secret>, Box<dyn std::error::Error>> {
    std::env::var("SPOROS__AUTH__API_KEY")
        .ok()
        .map(|value| Secret::parse("SPOROS__AUTH__API_KEY", value))
        .transpose()
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_supported_command_surface() {
        assert!(matches!(
            Cli::try_parse_from(["sporosctl", "status"])
                .expect("parse status")
                .command,
            Command::Status
        ));
        assert!(matches!(
            Cli::try_parse_from(["sporosctl", "tasks", "events", "00"])
                .expect("parse task events")
                .command,
            Command::Tasks {
                command: TaskCommand::Events { .. }
            }
        ));
        assert!(matches!(
            Cli::try_parse_from(["sporosctl", "operations", "list", "--kind", "data_scan"])
                .expect("parse operation list")
                .command,
            Command::Operations {
                command: OperationCommand::List { .. }
            }
        ));
        assert!(matches!(
            Cli::try_parse_from(["sporosctl", "search", "--indexer", "7", "--dry-run"])
                .expect("parse search")
                .command,
            Command::Search { dry_run: true, .. }
        ));
        assert!(matches!(
            Cli::try_parse_from(["sporosctl", "data", "scan", "media", "--force"])
                .expect("parse data scan")
                .command,
            Command::Data {
                command: DataCommand::Scan { force: true, .. }
            }
        ));
        assert!(matches!(
            Cli::try_parse_from([
                "sporosctl",
                "tasks",
                "list",
                "--state",
                "failed",
                "--limit",
                "25"
            ])
            .expect("parse task list")
            .command,
            Command::Tasks {
                command: TaskCommand::List { limit: 25, .. }
            }
        ));
        assert!(Cli::try_parse_from(["sporosctl", "tasks", "delete", "id"]).is_err());
        assert!(matches!(
            Cli::try_parse_from(["sporosctl", "inventory", "reconcile"])
                .expect("parse inventory reconciliation")
                .command,
            Command::Inventory {
                command: InventoryCommand::Reconcile
            }
        ));
    }
}
