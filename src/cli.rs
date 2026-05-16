use std::ffi::OsString;
use std::path::PathBuf;

use clap::{Parser, Subcommand};

use crate::config::{CONFIG_SCHEMA, DEFAULT_CONFIG_PATH, load_config};
use crate::runtime::daemon;

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
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| error.to_string())?;
            runtime
                .block_on(daemon::serve(loaded))
                .map_err(|error| error.to_string())?;
            Ok(String::new())
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
    fn check_config_rejects_unsupported_keys() {
        let root = unique_temp_root();
        fs::create_dir_all(&root).unwrap();
        let config_path = root.join("config.toml");
        fs::write(
            &config_path,
            format!(
                r#"
                [paths]
                database = "{}/state/sporos.db"
                torrent_cache_dir = "{}/cache/torrents"
                output_dir = "{}/output"
                base_dir = "/data"
                "#,
                root.display(),
                root.display(),
                root.display()
            ),
        )
        .unwrap();

        let error = run([
            OsString::from("sporos"),
            OsString::from("check-config"),
            OsString::from("--config"),
            config_path.clone().into_os_string(),
        ])
        .unwrap_err();

        assert!(error.contains("unknown field"));
        assert!(error.contains("base_dir"));
        remove_temp_config(config_path);
    }

    #[test]
    fn check_config_rejects_missing_integration_api_keys() {
        let prowlarr_config = write_temp_config(
            r#"
            [indexers.prowlarr.main]
            url = "https://prowlarr.example"
            "#,
        );
        let arr_config = write_temp_config(
            r#"
            [indexers.arr.sonarr.main]
            url = "http://sonarr:8989"
            "#,
        );

        let prowlarr_error = run([
            OsString::from("sporos"),
            OsString::from("check-config"),
            OsString::from("--config"),
            prowlarr_config.clone().into_os_string(),
        ])
        .unwrap_err();
        let arr_error = run([
            OsString::from("sporos"),
            OsString::from("check-config"),
            OsString::from("--config"),
            arr_config.clone().into_os_string(),
        ])
        .unwrap_err();

        assert!(prowlarr_error.contains("indexers.prowlarr.api_key"));
        assert!(arr_error.contains("indexers.arr.sonarr.api_key"));
        remove_temp_config(prowlarr_config);
        remove_temp_config(arr_config);
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

    fn unique_temp_root() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "sporos-cli-test-{}-{nanos}-{counter}",
            std::process::id()
        ))
    }

    fn remove_temp_config(path: PathBuf) {
        let Some(root) = path.parent() else {
            return;
        };
        fs::remove_dir_all(root).unwrap();
    }
}
