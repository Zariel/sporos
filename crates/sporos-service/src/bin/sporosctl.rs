use std::path::PathBuf;
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
            let token = admin_token()?;
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
                    request_json(&client, url, &token, cli.output).await
                }
                TaskCommand::Show { task_id } => {
                    let url = base.join(&format!("/api/v1/admin/tasks/{task_id}"))?;
                    request_json(&client, url, &token, cli.output).await
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
    url: reqwest::Url,
    token: &Secret,
    output: Output,
) -> Result<(), Box<dyn std::error::Error>> {
    let response = client.get(url).bearer_auth(token.expose()).send().await?;
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

fn admin_token() -> Result<Secret, Box<dyn std::error::Error>> {
    let direct = std::env::var("SPOROS_ADMIN_TOKEN").ok();
    let file = std::env::var_os("SPOROS_ADMIN_TOKEN_FILE").map(PathBuf::from);
    Ok(Secret::resolve("SPOROS_ADMIN_TOKEN", direct, file)?)
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
    }
}
