use std::ffi::OsString;
use std::path::PathBuf;

use clap::{Parser, Subcommand};

use crate::config::{CONFIG_SCHEMA, DEFAULT_CONFIG_PATH, load_config};

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

pub fn run(args: impl IntoIterator<Item = OsString>) -> Result<String, String> {
    let cli = Cli::try_parse_from(args).map_err(|error| error.to_string())?;

    match cli.command {
        Command::Serve { config } => {
            let loaded = load_config(&config).map_err(|error| error.to_string())?;
            Ok(format!(
                "sporos serve configuration loaded from {} for {} torrent client(s)",
                config.display(),
                loaded.torrent_clients.len()
            ))
        }
        Command::CheckConfig { config } => {
            load_config(&config).map_err(|error| error.to_string())?;
            Ok(format!("sporos config ok: {}", config.display()))
        }
        Command::PrintConfigSchema => Ok(CONFIG_SCHEMA.to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

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
        fs::remove_file(config_path).unwrap();
    }

    #[test]
    fn serve_loads_config_without_daemonizing() {
        let config_path = write_temp_config("");

        let output = run([
            OsString::from("sporos"),
            OsString::from("serve"),
            OsString::from("--config"),
            config_path.clone().into_os_string(),
        ])
        .unwrap();

        assert!(output.contains("sporos serve configuration loaded"));
        fs::remove_file(config_path).unwrap();
    }

    #[test]
    fn print_config_schema_reports_supported_surface() {
        let output = run([
            OsString::from("sporos"),
            OsString::from("print-config-schema"),
        ])
        .unwrap();

        assert!(output.contains("[paths]"));
        assert!(output.contains("[scheduling]"));
    }

    fn write_temp_config(contents: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("sporos-test-{nanos}.toml"));
        fs::write(&path, contents).unwrap();
        path
    }
}
