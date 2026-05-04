//! Command-line entry points and top-level command dispatch.

use std::path::{Path, PathBuf};

use clap::{Arg, ArgAction, ArgMatches, Command, value_parser};

use crate::{
    SporosError,
    persistence::{AsyncDatabase, Database},
};

/// Run the command-line entry point.
pub async fn run() -> crate::Result<()> {
    run_from(std::env::args_os()).await
}

/// Run the command-line entry point from explicit arguments.
pub async fn run_from<I, T>(args: I) -> crate::Result<()>
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
{
    let matches = build_cli()
        .try_get_matches_from(args)
        .map_err(|error| crate::SporosError::configuration(error.to_string()))?;
    let config_file = crate::config::selected_config_file(config_arg(&matches))?;

    match matches.subcommand() {
        Some(("gen-config", _)) => {
            let path = crate::config::generate_config_file(&config_file.path)?;
            println!("{}", path.display());
        }
        Some(("update-torrent-cache-trackers", matches)) => {
            let config = load_runtime_config(&config_file)?;
            let result = crate::operations::update_torrent_cache_trackers(
                &config.state_dir,
                required_string(matches, "old-announce-url")?,
                required_string(matches, "new-announce-url")?,
            )?;
            println!(
                "updated {} of {} cached torrents",
                result.files_updated, result.files_seen
            );
        }
        Some(("diff", matches)) => {
            if let Some(diff) = crate::operations::diff_torrents(
                &required_path(matches, "searchee")?,
                &required_path(matches, "candidate")?,
            )? {
                println!("{diff}");
            }
        }
        Some(("tree", matches)) => {
            let tree = crate::operations::torrent_tree(&required_path(matches, "torrent")?)?;
            println!("name: {}", tree.name);
            println!("infoHash: {}", tree.info_hash);
            for (path, length) in tree.files {
                println!("{length}\t{path}");
            }
        }
        Some(("clear-indexer-failures", _)) => {
            let config = load_runtime_config(&config_file)?;
            let database = AsyncDatabase::open(&config.database_path).await?;
            let updated = crate::operations::clear_indexer_failures_async(&database).await?;
            println!("cleared {updated} indexer failures");
        }
        Some(("clear-cache", _)) => {
            let config = load_runtime_config(&config_file)?;
            let database = AsyncDatabase::open(&config.database_path).await?;
            let result = crate::operations::clear_cache_async(&database).await?;
            println!(
                "cleared {} decisions and {} timestamps",
                result.decisions_removed, result.timestamps_removed
            );
        }
        Some(("clear-client-cache", _)) => {
            let config = load_runtime_config(&config_file)?;
            let database = AsyncDatabase::open(&config.database_path).await?;
            let result = crate::operations::clear_client_cache_async(&database).await?;
            println!(
                "cleared {} torrents, {} client searchees, {} data rows, and {} ensemble rows",
                result.torrents_removed,
                result.client_searchees_removed,
                result.data_removed,
                result.ensemble_removed
            );
        }
        Some(("api-key", matches)) => {
            let raw_config = load_raw_config(&config_file)?;
            let config = normalize_config(&config_file, raw_config)?;
            let database = AsyncDatabase::open(&config.database_path).await?;
            let configured = matches
                .get_one::<String>("api-key")
                .map(String::as_str)
                .or(config.api_key.as_deref());
            println!(
                "{}",
                crate::operations::api_key_async(&database, configured).await?
            );
        }
        Some(("reset-api-key", _)) => {
            let config = load_runtime_config(&config_file)?;
            let database = AsyncDatabase::open(&config.database_path).await?;
            println!(
                "{}",
                crate::operations::reset_api_key_async(&database).await?
            );
        }
        Some((command @ ("daemon" | "serve"), matches)) => {
            let mut raw_config = load_raw_config(&config_file)?;
            apply_shared_options(matches, &mut raw_config)?;
            apply_daemon_options(matches, &mut raw_config)?;
            let config = normalize_config(&config_file, raw_config)?;
            let state_dir = config.state_dir.clone();
            let _runtime = crate::startup::full_runtime(
                state_dir.clone(),
                config.clone(),
                &crate::startup::RuntimeStartupHooks,
            )?;
            let database = Database::open(&config.database_path)?;
            let shutdown = crate::daemon::install_shutdown_handler();
            let run = crate::daemon::run_daemon(&state_dir, &config, &database, shutdown).await?;
            println!(
                "{} stopped: serving={}, jobs={}",
                command,
                run.listen_addr
                    .map(|address| address.to_string())
                    .unwrap_or_else(|| "disabled".to_owned()),
                run.jobs.len()
            );
        }
        Some(("search", matches)) => {
            let mut raw_config = load_raw_config(&config_file)?;
            apply_shared_options(matches, &mut raw_config)?;
            apply_search_options(matches, &mut raw_config)?;
            let config = normalize_config(&config_file, raw_config)?;
            let state_dir = config.state_dir.clone();
            let _runtime = crate::startup::full_runtime(
                state_dir.clone(),
                config.clone(),
                &crate::startup::RuntimeStartupHooks,
            )?;
            let database = Database::open(&config.database_path)?;
            let notifier = crate::notifications::NotificationSender::from_config(
                &config,
                crate::startup::Redactor::from_config(&config),
            )?;
            let result =
                crate::operations::run_search_workflow(&database, &state_dir, &config, &notifier)?;
            println!(
                "searched {} searchees across {} indexers: {} candidates, {} attempts",
                result.searchees,
                result.indexers,
                result.pipeline.candidates_assessed,
                result.pipeline.attempts_total
            );
        }
        Some(("rss", matches)) => {
            let mut raw_config = load_raw_config(&config_file)?;
            apply_shared_options(matches, &mut raw_config)?;
            let config = normalize_config(&config_file, raw_config)?;
            let state_dir = config.state_dir.clone();
            let _runtime = crate::startup::full_runtime(
                state_dir.clone(),
                config.clone(),
                &crate::startup::RuntimeStartupHooks,
            )?;
            let database = Database::open(&config.database_path)?;
            let notifier = crate::notifications::NotificationSender::from_config(
                &config,
                crate::startup::Redactor::from_config(&config),
            )?;
            let result =
                crate::operations::run_rss_workflow(&database, &state_dir, &config, &notifier)?;
            println!(
                "rss matched {} of {} candidates",
                result.attempts, result.candidates
            );
        }
        Some(("inject", matches)) => {
            let mut raw_config = load_raw_config(&config_file)?;
            apply_shared_options(matches, &mut raw_config)?;
            apply_inject_options(matches, &mut raw_config);
            let config = normalize_config(&config_file, raw_config)?;
            let state_dir = config.state_dir.clone();
            let _runtime = crate::startup::full_runtime(
                state_dir.clone(),
                config.clone(),
                &crate::startup::RuntimeStartupHooks,
            )?;
            let database = Database::open(&config.database_path)?;
            let result = crate::operations::run_inject_workflow(&database, &state_dir, &config)?;
            println!(
                "injected {} saved torrents, {} already existed, {} incomplete, {} failed",
                result.injected, result.already_exists, result.incomplete, result.failed
            );
        }
        Some(("restore", matches)) => {
            let mut raw_config = load_raw_config(&config_file)?;
            apply_shared_options(matches, &mut raw_config)?;
            let config = normalize_config(&config_file, raw_config)?;
            let state_dir = config.state_dir.clone();
            let _runtime = crate::startup::full_runtime(
                state_dir.clone(),
                config.clone(),
                &crate::startup::RuntimeStartupHooks,
            )?;
            let database = Database::open(&config.database_path)?;
            let result = crate::operations::run_restore_workflow(&database, &state_dir, &config)?;
            println!(
                "restored {} of {} cached torrents, failed {}",
                result.restored, result.scanned, result.failed
            );
        }
        Some(("test-notification", matches)) => {
            let mut raw_config = load_raw_config(&config_file)?;
            apply_shared_options(matches, &mut raw_config)?;
            let config = normalize_config(&config_file, raw_config)?;
            let state_dir = config.state_dir.clone();
            let _runtime = crate::startup::full_runtime(
                state_dir,
                config.clone(),
                &crate::startup::RuntimeStartupHooks,
            )?;
            let redactor = crate::startup::Redactor::from_config(&config);
            let sender = crate::notifications::NotificationSender::from_config(&config, redactor)?;
            let report = sender.send_test();
            println!(
                "sent {} of {} notification webhooks",
                report.succeeded, report.attempted
            );
        }
        Some((command, _)) => {
            println!("sporos {} {}", crate::VERSION, command);
        }
        None => {
            println!("sporos {}", crate::VERSION);
        }
    }

    Ok(())
}

fn config_arg(matches: &ArgMatches) -> Option<&Path> {
    matches.get_one::<String>("config").map(Path::new)
}

fn load_raw_config(
    config_file: &crate::config::ConfigFileTarget,
) -> crate::Result<crate::config::RawConfig> {
    let mut raw_config = crate::config::load_selected_raw_config(config_file)?;
    crate::config::apply_env_overrides(&mut raw_config)?;
    Ok(raw_config)
}

fn normalize_config(
    config_file: &crate::config::ConfigFileTarget,
    raw_config: crate::config::RawConfig,
) -> crate::Result<crate::config::RuntimeConfig> {
    let mut config = crate::config::RuntimeConfig::normalize(raw_config, &config_file.app_dir)?;
    config.config_path = config_file.path.clone();
    Ok(config)
}

fn load_runtime_config(
    config_file: &crate::config::ConfigFileTarget,
) -> crate::Result<crate::config::RuntimeConfig> {
    let raw_config = load_raw_config(config_file)?;
    normalize_config(config_file, raw_config)
}

fn required_string<'a>(matches: &'a ArgMatches, name: &str) -> crate::Result<&'a str> {
    matches
        .get_one::<String>(name)
        .map(String::as_str)
        .ok_or_else(|| SporosError::configuration(format!("missing required argument: {name}")))
}

fn required_path(matches: &ArgMatches, name: &str) -> crate::Result<PathBuf> {
    Ok(Path::new(required_string(matches, name)?).to_owned())
}

fn apply_shared_options(
    matches: &ArgMatches,
    raw_config: &mut crate::config::RawConfig,
) -> crate::Result<()> {
    if let Some(values) = string_values(matches, "torznab") {
        raw_config.torznab = values
            .iter()
            .map(|value| integration_from_query_url(value, "torznab"))
            .collect::<crate::Result<Vec<_>>>()?;
    }
    if matches.get_flag("use-client-torrents") {
        raw_config.use_client_torrents = Some(true);
    }
    if let Some(values) = path_values(matches, "data-dirs") {
        raw_config.data_dirs = values;
    }
    if let Some(value) = string_value(matches, "torrent-dir") {
        raw_config.torrent_dir = Some(PathBuf::from(value));
    }
    if let Some(value) = string_value(matches, "match-mode") {
        raw_config.match_mode = Some(value.clone());
    }
    if matches.get_flag("include-non-videos") {
        raw_config.include_non_videos = Some(true);
    }
    if matches.get_flag("include-single-episodes") {
        raw_config.include_single_episodes = Some(true);
    }
    if let Some(value) = string_value(matches, "season-from-episodes") {
        raw_config.season_from_episodes = Some(parse_ratio(value, "season-from-episodes")?);
    }
    if let Some(value) = matches.get_one::<f64>("fuzzy-size-threshold") {
        raw_config.fuzzy_size_threshold = Some(*value);
    }
    if let Some(value) = string_value(matches, "exclude-older") {
        raw_config.exclude_older = Some(crate::config::parse_duration_millis(value)?);
    }
    if let Some(value) = string_value(matches, "exclude-recent-search") {
        raw_config.exclude_recent_search = Some(crate::config::parse_duration_millis(value)?);
    }
    if let Some(value) = string_value(matches, "action") {
        raw_config.action = Some(value.clone());
    }
    if let Some(value) = string_value(matches, "output-dir") {
        raw_config.output_dir = Some(PathBuf::from(value));
    }
    if let Some(values) = string_values(matches, "torrent-clients") {
        raw_config.torrent_clients = values
            .iter()
            .map(|value| crate::config::TorrentClientConfig::parse(value))
            .collect::<crate::Result<Vec<_>>>()?;
    }
    if matches.get_flag("duplicate-categories") {
        raw_config.duplicate_categories = Some(true);
    }
    if let Some(value) = string_value(matches, "link-category") {
        raw_config.link_category = Some(value.clone());
    }
    if let Some(values) = path_values(matches, "link-dirs") {
        raw_config.link_dirs = values;
    }
    if let Some(value) = string_value(matches, "link-type") {
        raw_config.link_type = Some(value.clone());
    }
    if matches.get_flag("flat-linking") {
        raw_config.flat_linking = Some(true);
    }
    if let Some(value) = matches.get_one::<u32>("max-data-depth") {
        raw_config.max_data_depth = Some(*value);
    }
    if matches.get_flag("skip-recheck") {
        raw_config.skip_recheck = Some(true);
    }
    if let Some(value) = matches.get_one::<u64>("auto-resume-max-download") {
        raw_config.auto_resume_max_download = Some(*value);
    }
    if matches.get_flag("ignore-non-relevant-files-to-resume") {
        raw_config.ignore_non_relevant_files_to_resume = Some(true);
    }
    if let Some(value) = matches.get_one::<u64>("delay") {
        raw_config.delay = Some(*value);
    }
    if let Some(value) = string_value(matches, "snatch-timeout") {
        raw_config.snatch_timeout = Some(crate::config::parse_duration_millis(value)?);
    }
    if let Some(value) = matches.get_one::<u32>("snatch-retries") {
        raw_config.snatch_retries = Some(*value);
    }
    if let Some(value) = string_value(matches, "search-timeout") {
        raw_config.search_timeout = Some(crate::config::parse_duration_millis(value)?);
    }
    if let Some(value) = matches.get_one::<u32>("search-limit") {
        raw_config.search_limit = Some(*value);
    }
    if let Some(values) = string_values(matches, "notification-webhook-urls") {
        raw_config.notification_webhook_urls = values;
    }
    if let Some(values) = string_values(matches, "block-list") {
        raw_config.block_list = values;
    }
    if let Some(values) = string_values(matches, "sonarr") {
        raw_config.sonarr = values
            .iter()
            .map(|value| integration_from_query_url(value, "sonarr"))
            .collect::<crate::Result<Vec<_>>>()?;
    }
    if let Some(values) = string_values(matches, "radarr") {
        raw_config.radarr = values
            .iter()
            .map(|value| integration_from_query_url(value, "radarr"))
            .collect::<crate::Result<Vec<_>>>()?;
    }
    if matches.get_flag("verbose") {
        raw_config.verbose = Some(true);
    }
    Ok(())
}

fn apply_search_options(
    matches: &ArgMatches,
    raw_config: &mut crate::config::RawConfig,
) -> crate::Result<()> {
    if let Some(values) = path_values(matches, "torrents") {
        raw_config.torrents = Some(values);
    }
    if matches.get_flag("no-exclude-older") {
        raw_config.exclude_older = None;
    }
    if matches.get_flag("no-exclude-recent-search") {
        raw_config.exclude_recent_search = None;
    }
    Ok(())
}

fn apply_inject_options(matches: &ArgMatches, raw_config: &mut crate::config::RawConfig) {
    if let Some(value) = string_value(matches, "inject-dir") {
        raw_config.inject_dir = Some(PathBuf::from(value));
    }
    if matches.get_flag("ignore-titles") {
        raw_config.ignore_titles = Some(true);
    }
    if matches.get_flag("no-ignore-titles") {
        raw_config.ignore_titles = Some(false);
    }
}

fn apply_daemon_options(
    matches: &ArgMatches,
    raw_config: &mut crate::config::RawConfig,
) -> crate::Result<()> {
    if matches.get_flag("no-port") {
        raw_config.listen_port = Some(None);
    } else if let Some(port) = matches.get_one::<u16>("port") {
        raw_config.listen_port = Some(Some(*port));
    }
    if let Some(host) = string_value(matches, "host") {
        raw_config.listen_host = Some(host.parse().map_err(|error| {
            crate::SporosError::configuration(format!("invalid --host: {error}"))
        })?);
    }
    if let Some(search_cadence) = string_value(matches, "search-cadence") {
        raw_config.search_cadence = Some(crate::config::parse_duration_millis(search_cadence)?);
    }
    if let Some(rss_cadence) = string_value(matches, "rss-cadence") {
        raw_config.rss_cadence = Some(crate::config::parse_duration_millis(rss_cadence)?);
    }
    if let Some(api_key) = string_value(matches, "api-key") {
        raw_config.api_key = Some(api_key.clone());
    }
    Ok(())
}

fn string_value<'a>(matches: &'a ArgMatches, name: &str) -> Option<&'a String> {
    matches.get_one::<String>(name)
}

fn string_values(matches: &ArgMatches, name: &str) -> Option<Vec<String>> {
    matches
        .get_many::<String>(name)
        .map(|values| values.cloned().collect())
}

fn path_values(matches: &ArgMatches, name: &str) -> Option<Vec<PathBuf>> {
    string_values(matches, name).map(|values| values.into_iter().map(PathBuf::from).collect())
}

fn integration_from_query_url(
    value: &str,
    label: &str,
) -> crate::Result<crate::config::ApiIntegrationConfig> {
    let mut url = url::Url::parse(value)
        .map_err(|error| SporosError::configuration(format!("invalid {label} URL: {error}")))?;
    let api_key = url
        .query_pairs()
        .find_map(|(key, value)| (key == "apikey" || key == "api_key").then(|| value.into_owned()))
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            SporosError::configuration(format!(
                "{label} CLI URL must include apikey query parameter"
            ))
        })?;
    url.set_query(None);
    url.set_fragment(None);
    Ok(crate::config::ApiIntegrationConfig {
        url: url.to_string().trim_end_matches('/').to_owned(),
        api_key,
    })
}

fn parse_ratio(value: &str, name: &str) -> crate::Result<f64> {
    value
        .parse::<f64>()
        .map_err(|error| SporosError::configuration(format!("invalid --{name}: {error}")))
}

/// Build the compatibility command tree.
pub fn build_cli() -> Command {
    Command::new("cross-seed")
        .version(crate::VERSION)
        .about("Torrent cross-seeding automation")
        .subcommand_required(false)
        .arg_required_else_help(false)
        .arg(
            Arg::new("config")
                .long("config")
                .num_args(1)
                .global(true)
                .help("Read configuration from an explicit TOML file"),
        )
        .subcommand(Command::new("gen-config").about("Write a starter TOML configuration file"))
        .subcommand(
            Command::new("update-torrent-cache-trackers")
                .about("Repair cached torrent tracker URLs")
                .arg(Arg::new("old-announce-url").required(true))
                .arg(Arg::new("new-announce-url").required(true)),
        )
        .subcommand(
            Command::new("diff")
                .about("Inspect matching differences between two torrents")
                .arg(Arg::new("searchee").required(true))
                .arg(Arg::new("candidate").required(true)),
        )
        .subcommand(
            Command::new("tree")
                .about("Print a torrent file tree")
                .arg(Arg::new("torrent").required(true)),
        )
        .subcommand(Command::new("clear-indexer-failures").about("Clear indexer degradation state"))
        .subcommand(Command::new("clear-cache").about("Clear cached decisions and timestamps"))
        .subcommand(Command::new("clear-client-cache").about("Clear cached client inventory state"))
        .subcommand(
            Command::new("api-key")
                .about("Print or persist the service API key")
                .arg(Arg::new("api-key").long("api-key").num_args(1)),
        )
        .subcommand(Command::new("reset-api-key").about("Generate and store a new service API key"))
        .subcommand(add_daemon_options(add_shared_options(
            Command::new("serve").about("Run the single-writer service runtime"),
        )))
        .subcommand(add_daemon_options(add_shared_options(
            Command::new("daemon")
                .about("Deprecated compatibility alias for serve")
                .hide(true),
        )))
        .subcommand(
            add_shared_options(Command::new("rss"))
                .about("Run one administrative RSS processing pass"),
        )
        .subcommand(add_search_options(add_shared_options(
            Command::new("search").about("Run one administrative search pass"),
        )))
        .subcommand(
            add_inject_options(add_shared_options(Command::new("inject")))
                .about("Run one administrative saved-torrent injection pass"),
        )
        .subcommand(
            add_shared_options(Command::new("restore"))
                .about("Run one administrative torrent-cache restore pass"),
        )
        .subcommand(
            add_shared_options(Command::new("test-notification"))
                .about("Send an administrative notification test"),
        )
}

fn add_daemon_options(command: Command) -> Command {
    command
        .arg(
            Arg::new("port")
                .long("port")
                .num_args(1)
                .value_parser(value_parser!(u16)),
        )
        .arg(Arg::new("host").long("host").num_args(1))
        .arg(
            Arg::new("no-port")
                .long("no-port")
                .action(ArgAction::SetTrue),
        )
        .arg(
            Arg::new("search-cadence")
                .long("search-cadence")
                .num_args(1),
        )
        .arg(Arg::new("rss-cadence").long("rss-cadence").num_args(1))
        .arg(Arg::new("api-key").long("api-key").num_args(1))
}

fn add_search_options(command: Command) -> Command {
    command
        .arg(
            Arg::new("torrents")
                .long("torrents")
                .num_args(1..)
                .action(ArgAction::Append)
                .hide(true),
        )
        .arg(
            Arg::new("no-exclude-older")
                .long("no-exclude-older")
                .action(ArgAction::SetTrue),
        )
        .arg(
            Arg::new("no-exclude-recent-search")
                .long("no-exclude-recent-search")
                .action(ArgAction::SetTrue),
        )
}

fn add_inject_options(command: Command) -> Command {
    command
        .arg(Arg::new("inject-dir").long("inject-dir").num_args(1))
        .arg(
            Arg::new("ignore-titles")
                .long("ignore-titles")
                .action(ArgAction::SetTrue)
                .overrides_with("no-ignore-titles"),
        )
        .arg(
            Arg::new("no-ignore-titles")
                .long("no-ignore-titles")
                .action(ArgAction::SetTrue)
                .overrides_with("ignore-titles"),
        )
}

fn add_shared_options(command: Command) -> Command {
    command
        .arg(repeating("torznab", "torznab"))
        .arg(
            Arg::new("use-client-torrents")
                .long("use-client-torrents")
                .action(ArgAction::SetTrue),
        )
        .arg(repeating("data-dirs", "data-dirs"))
        .arg(Arg::new("torrent-dir").long("torrent-dir").num_args(1))
        .arg(Arg::new("match-mode").long("match-mode").num_args(1))
        .arg(
            Arg::new("include-non-videos")
                .long("include-non-videos")
                .action(ArgAction::SetTrue),
        )
        .arg(
            Arg::new("include-single-episodes")
                .long("include-single-episodes")
                .action(ArgAction::SetTrue),
        )
        .arg(
            Arg::new("season-from-episodes")
                .long("season-from-episodes")
                .num_args(1),
        )
        .arg(
            Arg::new("fuzzy-size-threshold")
                .long("fuzzy-size-threshold")
                .num_args(1)
                .value_parser(value_parser!(f64)),
        )
        .arg(Arg::new("exclude-older").long("exclude-older").num_args(1))
        .arg(
            Arg::new("exclude-recent-search")
                .long("exclude-recent-search")
                .num_args(1),
        )
        .arg(Arg::new("action").long("action").num_args(1))
        .arg(Arg::new("output-dir").long("output-dir").num_args(1))
        .arg(repeating("torrent-clients", "torrent-clients"))
        .arg(
            Arg::new("duplicate-categories")
                .long("duplicate-categories")
                .action(ArgAction::SetTrue),
        )
        .arg(Arg::new("link-category").long("link-category").num_args(1))
        .arg(repeating("link-dirs", "link-dirs"))
        .arg(Arg::new("link-type").long("link-type").num_args(1))
        .arg(
            Arg::new("flat-linking")
                .long("flat-linking")
                .action(ArgAction::SetTrue),
        )
        .arg(
            Arg::new("max-data-depth")
                .long("max-data-depth")
                .num_args(1)
                .value_parser(value_parser!(u32)),
        )
        .arg(
            Arg::new("skip-recheck")
                .long("skip-recheck")
                .action(ArgAction::SetTrue),
        )
        .arg(
            Arg::new("auto-resume-max-download")
                .long("auto-resume-max-download")
                .num_args(1)
                .value_parser(value_parser!(u64)),
        )
        .arg(
            Arg::new("ignore-non-relevant-files-to-resume")
                .long("ignore-non-relevant-files-to-resume")
                .action(ArgAction::SetTrue),
        )
        .arg(
            Arg::new("delay")
                .long("delay")
                .num_args(1)
                .value_parser(value_parser!(u64)),
        )
        .arg(
            Arg::new("snatch-timeout")
                .long("snatch-timeout")
                .num_args(1),
        )
        .arg(
            Arg::new("snatch-retries")
                .long("snatch-retries")
                .num_args(1)
                .value_parser(value_parser!(u32)),
        )
        .arg(
            Arg::new("search-timeout")
                .long("search-timeout")
                .num_args(1),
        )
        .arg(
            Arg::new("search-limit")
                .long("search-limit")
                .num_args(1)
                .value_parser(value_parser!(u32)),
        )
        .arg(repeating(
            "notification-webhook-urls",
            "notification-webhook-urls",
        ))
        .arg(repeating("block-list", "block-list"))
        .arg(repeating("sonarr", "sonarr"))
        .arg(repeating("radarr", "radarr"))
        .arg(
            Arg::new("verbose")
                .long("verbose")
                .short('v')
                .action(ArgAction::SetTrue),
        )
}

fn repeating(name: &'static str, long: &'static str) -> Arg {
    Arg::new(name)
        .long(long)
        .num_args(1..)
        .action(ArgAction::Append)
}

#[cfg(test)]
mod tests {
    use super::{
        apply_daemon_options, apply_inject_options, apply_search_options, apply_shared_options,
        build_cli,
    };
    use crate::config::{ApiIntegrationConfig, RawConfig, TorrentClientConfig};
    use std::path::Path;

    #[test]
    fn exposes_documented_command_names() {
        let cli = build_cli();
        let names = cli
            .get_subcommands()
            .map(clap::Command::get_name)
            .collect::<Vec<_>>();

        for expected in [
            "gen-config",
            "update-torrent-cache-trackers",
            "diff",
            "tree",
            "clear-indexer-failures",
            "clear-cache",
            "clear-client-cache",
            "api-key",
            "reset-api-key",
            "serve",
            "daemon",
            "rss",
            "search",
            "inject",
            "restore",
            "test-notification",
        ] {
            assert!(names.contains(&expected));
        }
    }

    #[test]
    fn reclassifies_workflow_commands_as_administrative() {
        let cli = build_cli();

        for (name, expected_about) in [
            ("serve", "Run the single-writer service runtime"),
            ("rss", "Run one administrative RSS processing pass"),
            ("search", "Run one administrative search pass"),
            (
                "inject",
                "Run one administrative saved-torrent injection pass",
            ),
            (
                "restore",
                "Run one administrative torrent-cache restore pass",
            ),
        ] {
            let command = cli
                .find_subcommand(name)
                .unwrap_or_else(|| panic!("missing {name} command"));

            assert_eq!(
                command.get_about().map(ToString::to_string).as_deref(),
                Some(expected_about)
            );
            assert!(!command.is_hide_set());
        }
    }

    #[test]
    fn daemon_is_hidden_compatibility_alias() {
        let cli = build_cli();
        let command = cli.find_subcommand("daemon").expect("daemon command");

        assert_eq!(
            command.get_about().map(ToString::to_string).as_deref(),
            Some("Deprecated compatibility alias for serve")
        );
        assert!(command.is_hide_set());
    }

    #[test]
    fn all_commands_have_explicit_roles() {
        let cli = build_cli();

        for command in cli.get_subcommands() {
            assert!(
                command.get_about().is_some(),
                "{} should describe its operational role",
                command.get_name()
            );
        }
    }

    #[test]
    fn parses_shared_workflow_options() {
        build_cli()
            .try_get_matches_from([
                "cross-seed",
                "search",
                "--torznab",
                "https://indexer.example/api?apikey=secret",
                "--use-client-torrents",
                "--match-mode",
                "partial",
                "--torrents",
                "/tmp/example.torrent",
                "--no-exclude-older",
            ])
            .expect("search command parses");
    }

    #[test]
    fn rejects_removed_hidden_cli_aliases() {
        for alias in [
            "--qbittorrent-url",
            "--rtorrent-rpc-url",
            "--transmission-rpc-url",
            "--deluge-rpc-url",
            "--link-dir",
            "--notification-webhook-url",
        ] {
            let result = build_cli().try_get_matches_from(["cross-seed", "search", alias, "value"]);

            assert!(result.is_err(), "{alias} should not parse");
        }
    }

    #[test]
    fn parses_daemon_and_inject_specific_options() {
        build_cli()
            .try_get_matches_from([
                "cross-seed",
                "--config",
                "/etc/sporos/config.toml",
                "daemon",
                "--port",
                "2468",
                "--host",
                "127.0.0.1",
                "--search-cadence",
                "1 day",
                "--rss-cadence",
                "30 minutes",
            ])
            .expect("daemon command parses");

        build_cli()
            .try_get_matches_from([
                "cross-seed",
                "serve",
                "--config",
                "/etc/sporos/config.toml",
                "--port",
                "9000",
                "--host",
                "0.0.0.0",
            ])
            .expect("serve command parses");

        build_cli()
            .try_get_matches_from([
                "cross-seed",
                "inject",
                "--config",
                "/etc/sporos/config.toml",
                "--inject-dir",
                "/output",
                "--ignore-titles",
            ])
            .expect("inject command parses");
    }

    #[test]
    fn applies_shared_cli_options_to_raw_config() {
        let matches = build_cli()
            .try_get_matches_from([
                "cross-seed",
                "search",
                "--torznab",
                "https://indexer.example/api?apikey=secret",
                "--use-client-torrents",
                "--data-dirs",
                "/data",
                "--torrent-dir",
                "/torrents",
                "--match-mode",
                "partial",
                "--include-non-videos",
                "--include-single-episodes",
                "--season-from-episodes",
                "0.5",
                "--fuzzy-size-threshold",
                "0.07",
                "--exclude-older",
                "2 days",
                "--exclude-recent-search",
                "12 hours",
                "--action",
                "inject",
                "--output-dir",
                "/output",
                "--torrent-clients",
                "qbittorrent:http://localhost:8080",
                "--duplicate-categories",
                "--link-category",
                "cross-seed",
                "--link-dirs",
                "/links",
                "--link-type",
                "symlink",
                "--flat-linking",
                "--max-data-depth",
                "3",
                "--skip-recheck",
                "--auto-resume-max-download",
                "1024",
                "--ignore-non-relevant-files-to-resume",
                "--delay",
                "60",
                "--snatch-timeout",
                "30s",
                "--snatch-retries",
                "4",
                "--search-timeout",
                "2 minutes",
                "--search-limit",
                "25",
                "--notification-webhook-urls",
                "https://notify.example/hook",
                "--block-list",
                "name:cam",
                "--sonarr",
                "http://sonarr.example/api?apikey=abc",
                "--radarr",
                "http://radarr.example/api?apikey=def",
                "--verbose",
                "--torrents",
                "/tmp/example.torrent",
                "--no-exclude-older",
            ])
            .expect("matches");
        let (_, matches) = matches.subcommand().expect("subcommand");
        let mut raw = RawConfig::default();

        apply_shared_options(matches, &mut raw).expect("shared");
        apply_search_options(matches, &mut raw).expect("search");

        assert_eq!(
            raw.torznab,
            vec![ApiIntegrationConfig {
                url: "https://indexer.example/api".to_owned(),
                api_key: "secret".to_owned(),
            }]
        );
        assert_eq!(raw.use_client_torrents, Some(true));
        assert_eq!(raw.data_dirs, vec![Path::new("/data")]);
        assert_eq!(raw.torrent_dir.as_deref(), Some(Path::new("/torrents")));
        assert_eq!(raw.match_mode.as_deref(), Some("partial"));
        assert_eq!(raw.include_non_videos, Some(true));
        assert_eq!(raw.include_single_episodes, Some(true));
        assert_eq!(raw.season_from_episodes, Some(0.5));
        assert_eq!(raw.fuzzy_size_threshold, Some(0.07));
        assert_eq!(raw.exclude_older, None);
        assert_eq!(raw.exclude_recent_search, Some(43_200_000));
        assert_eq!(raw.action.as_deref(), Some("inject"));
        assert_eq!(raw.output_dir.as_deref(), Some(Path::new("/output")));
        assert_eq!(
            raw.torrent_clients,
            vec![TorrentClientConfig::parse("qbittorrent:http://localhost:8080").expect("client")]
        );
        assert_eq!(raw.duplicate_categories, Some(true));
        assert_eq!(raw.link_category.as_deref(), Some("cross-seed"));
        assert_eq!(raw.link_dirs, vec![Path::new("/links")]);
        assert_eq!(raw.link_type.as_deref(), Some("symlink"));
        assert_eq!(raw.flat_linking, Some(true));
        assert_eq!(raw.max_data_depth, Some(3));
        assert_eq!(raw.skip_recheck, Some(true));
        assert_eq!(raw.auto_resume_max_download, Some(1024));
        assert_eq!(raw.ignore_non_relevant_files_to_resume, Some(true));
        assert_eq!(raw.delay, Some(60));
        assert_eq!(raw.snatch_timeout, Some(30_000));
        assert_eq!(raw.snatch_retries, Some(4));
        assert_eq!(raw.search_timeout, Some(120_000));
        assert_eq!(raw.search_limit, Some(25));
        assert_eq!(
            raw.notification_webhook_urls,
            vec!["https://notify.example/hook"]
        );
        assert_eq!(raw.block_list, vec!["name:cam"]);
        assert_eq!(
            raw.sonarr,
            vec![ApiIntegrationConfig {
                url: "http://sonarr.example/api".to_owned(),
                api_key: "abc".to_owned(),
            }]
        );
        assert_eq!(
            raw.radarr,
            vec![ApiIntegrationConfig {
                url: "http://radarr.example/api".to_owned(),
                api_key: "def".to_owned(),
            }]
        );
        assert_eq!(raw.verbose, Some(true));
        assert_eq!(
            raw.torrents.as_deref(),
            Some([Path::new("/tmp/example.torrent").to_path_buf()].as_slice())
        );
    }

    #[test]
    fn applies_inject_cli_options_to_raw_config() {
        let matches = build_cli()
            .try_get_matches_from([
                "cross-seed",
                "inject",
                "--inject-dir",
                "/saved",
                "--ignore-titles",
            ])
            .expect("matches");
        let (_, matches) = matches.subcommand().expect("subcommand");
        let mut raw = RawConfig::default();

        apply_shared_options(matches, &mut raw).expect("shared");
        apply_inject_options(matches, &mut raw);

        assert_eq!(raw.inject_dir.as_deref(), Some(Path::new("/saved")));
        assert_eq!(raw.ignore_titles, Some(true));
    }

    #[test]
    fn applies_command_flags_after_env_overrides() {
        let matches = build_cli()
            .try_get_matches_from([
                "cross-seed",
                "daemon",
                "--port",
                "3333",
                "--api-key",
                "cli-cli-cli-cli-cli-cli",
            ])
            .expect("matches");
        let (_, matches) = matches.subcommand().expect("subcommand");
        let mut raw = RawConfig {
            port: Some(Some(1111)),
            api_key: Some("file-file-file-file-file".to_owned()),
            ..RawConfig::default()
        };

        crate::config::apply_env_overrides_from(
            [
                ("SPOROS__LISTEN_PORT".to_owned(), "2222".to_owned()),
                (
                    "SPOROS__API_KEY".to_owned(),
                    "env-env-env-env-env-env".to_owned(),
                ),
            ],
            &mut raw,
        )
        .expect("env");
        apply_daemon_options(matches, &mut raw).expect("daemon");

        assert_eq!(raw.listen_port, Some(Some(3333)));
        assert_eq!(raw.api_key.as_deref(), Some("cli-cli-cli-cli-cli-cli"));
    }
}
