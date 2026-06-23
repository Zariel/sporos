use std::ffi::OsString;
use std::path::PathBuf;

#[cfg(test)]
use clap::CommandFactory;
use clap::error::ErrorKind;
use clap::{Parser, Subcommand};

use crate::config::{CONFIG_SCHEMA, DEFAULT_CONFIG_PATH, SporosConfig, load_config};
use crate::runtime::app::validate_runtime_config;
use crate::runtime::daemon;

#[derive(Debug)]
pub struct CliError {
    message: String,
    exit_code: u8,
}

impl CliError {
    const FAILURE: u8 = 1;

    fn failure(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            exit_code: Self::FAILURE,
        }
    }

    fn usage(error: clap::Error) -> Self {
        Self {
            exit_code: u8::try_from(error.exit_code()).unwrap_or(Self::FAILURE),
            message: error.to_string(),
        }
    }

    pub fn exit_code(&self) -> u8 {
        self.exit_code
    }

    #[cfg(test)]
    fn contains(&self, needle: &str) -> bool {
        self.message.contains(needle)
    }
}

impl std::fmt::Display for CliError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for CliError {}

#[derive(Debug, Parser)]
#[command(name = "sporos", about = "Sporos torrent automation service")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Serve {
        #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
    },
    CheckConfig {
        #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
    },
    PrintConfigSchema,
}

pub fn run(args: impl IntoIterator<Item = OsString>) -> Result<String, CliError> {
    let cli = match Cli::try_parse_from(args) {
        Ok(cli) => cli,
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::DisplayHelp | ErrorKind::DisplayVersion
            ) =>
        {
            return Ok(error.to_string());
        }
        Err(error) => return Err(CliError::usage(error)),
    };

    match cli.command {
        Command::Serve { config } => {
            let loaded =
                load_config(&config).map_err(|error| CliError::failure(error.to_string()))?;
            let runtime = build_serve_runtime(&loaded)?;
            runtime
                .block_on(daemon::serve(loaded))
                .map_err(|error| CliError::failure(error.to_string()))?;
            Ok(String::new())
        }
        Command::CheckConfig { config } => {
            let loaded =
                load_config(&config).map_err(|error| CliError::failure(error.to_string()))?;
            validate_runtime_config(&loaded)
                .map_err(|error| CliError::failure(error.to_string()))?;
            Ok(format!("sporos config ok: {}", config.display()))
        }
        Command::PrintConfigSchema => Ok(CONFIG_SCHEMA.to_owned()),
    }
}

pub(crate) fn build_serve_runtime(
    config: &SporosConfig,
) -> Result<tokio::runtime::Runtime, CliError> {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    if let Some(worker_threads) = config.runtime.worker_threads {
        builder.worker_threads(worker_threads);
    }
    if let Some(max_blocking_threads) = config.runtime.max_blocking_threads {
        builder.max_blocking_threads(max_blocking_threads);
    }
    builder
        .build()
        .map_err(|error| CliError::failure(error.to_string()))
}

#[cfg(test)]
fn help_text() -> String {
    let mut bytes = Vec::new();
    Cli::command()
        .write_long_help(&mut bytes)
        .unwrap_or_default();
    String::from_utf8(bytes).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn check_config_loads_typed_toml() {
        let config_path = write_temp_config(
            r#"
            [server]
            bind = "127.0.0.1:2468"
            "#,
        );

        let output = run([
            OsString::from("sporos"),
            OsString::from("check-config"),
            OsString::from("--config"),
            config_path.clone().into_os_string(),
        ])
        .unwrap();

        assert!(output.contains("sporos config ok"));
        remove_temp_config(config_path);
    }

    #[test]
    fn check_config_rejects_runtime_intervals() {
        let config_path = write_temp_config(
            r#"
            [scheduling]
            client_inventory_interval = "0s"
            "#,
        );

        let error = run([
            OsString::from("sporos"),
            OsString::from("check-config"),
            OsString::from("--config"),
            config_path.clone().into_os_string(),
        ])
        .unwrap_err();

        assert!(error.contains("client inventory interval"));
        remove_temp_config(config_path);
    }

    #[test]
    fn print_config_schema_reports_supported_surface() {
        let output = run([
            OsString::from("sporos"),
            OsString::from("print-config-schema"),
        ])
        .unwrap();

        assert!(output.contains("[paths]"));
        assert!(output.contains("[runtime]"));
        assert!(output.contains("[scheduling]"));
    }

    #[test]
    fn help_is_successful_output() {
        let output = run([OsString::from("sporos"), OsString::from("--help")]).unwrap();

        assert!(output.contains("Usage: sporos <COMMAND>"));
    }

    #[test]
    fn invalid_usage_exits_with_clap_usage_code() {
        let error = run([OsString::from("sporos"), OsString::from("not-a-command")]).unwrap_err();

        assert_eq!(2, error.exit_code());
        assert!(error.contains("unrecognized subcommand"));
    }

    #[test]
    fn serve_runtime_uses_multi_thread_scheduler() {
        let mut config = SporosConfig::default();
        config.runtime.worker_threads = Some(2);
        config.runtime.max_blocking_threads = Some(2);

        let runtime = build_serve_runtime(&config).unwrap();
        let flavor = runtime.block_on(async { tokio::runtime::Handle::current().runtime_flavor() });

        assert_eq!(tokio::runtime::RuntimeFlavor::MultiThread, flavor);
    }

    #[test]
    fn production_help_excludes_system_test_commands() {
        let help = help_text();

        assert!(help.contains("serve"));
        assert!(help.contains("check-config"));
        assert!(!help.contains("system-test"));
        assert!(!help.contains("diagnostics"));
    }

    #[test]
    fn production_cli_rejects_system_test_commands() {
        let error = run([
            OsString::from("sporos"),
            OsString::from("system-test-diagnostics"),
        ])
        .unwrap_err();

        assert_eq!(2, error.exit_code());
        assert!(error.contains("unrecognized subcommand"));
    }

    fn write_temp_config(contents: &str) -> PathBuf {
        let root = unique_temp_root();
        fs::create_dir_all(&root).unwrap();
        let path = root.join("config.toml");
        let contents = format!(
            r#"
            [paths]
            database = "{}/state/sporos.db"
            torrent_cache_dir = "{}/cache/torrents"
            output_dir = "{}/output"

            {contents}
            "#,
            root.display(),
            root.display(),
            root.display()
        );
        fs::write(&path, contents).unwrap();
        path
    }

    fn remove_temp_config(path: PathBuf) {
        let root = path.parent().unwrap().to_path_buf();
        fs::remove_dir_all(root).unwrap();
    }

    fn unique_temp_root() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "sporos-cli-test-{nanos}-{}",
            TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
        ))
    }
}
