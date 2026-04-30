//! Command-line entry points and top-level command dispatch.

use std::path::{Path, PathBuf};

use clap::{Arg, ArgAction, ArgMatches, Command, value_parser};

use crate::{SporosError, persistence::Database};

/// Run the command-line entry point.
pub fn run() -> crate::Result<()> {
    run_from(std::env::args_os())
}

/// Run the command-line entry point from explicit arguments.
pub fn run_from<I, T>(args: I) -> crate::Result<()>
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
{
    let app_dir = crate::config::app_dir()?;
    let matches = build_cli()
        .try_get_matches_from(args)
        .map_err(|error| crate::SporosError::configuration(error.to_string()))?;

    match matches.subcommand() {
        Some(("gen-config", _)) => {
            let path = crate::config::generate_config(&app_dir)?;
            println!("{}", path.display());
        }
        Some(("update-torrent-cache-trackers", matches)) => {
            let result = crate::operations::update_torrent_cache_trackers(
                &app_dir,
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
            let database = Database::open_app_dir(&app_dir)?;
            let updated = crate::operations::clear_indexer_failures(&database)?;
            println!("cleared {updated} indexer failures");
        }
        Some(("clear-cache", _)) => {
            let database = Database::open_app_dir(&app_dir)?;
            let result = crate::operations::clear_cache(&database)?;
            println!(
                "cleared {} decisions and {} timestamps",
                result.decisions_removed, result.timestamps_removed
            );
        }
        Some(("clear-client-cache", _)) => {
            let database = Database::open_app_dir(&app_dir)?;
            let result = crate::operations::clear_client_cache(&database)?;
            println!(
                "cleared {} torrents, {} client searchees, {} data rows, and {} ensemble rows",
                result.torrents_removed,
                result.client_searchees_removed,
                result.data_removed,
                result.ensemble_removed
            );
        }
        Some(("api-key", matches)) => {
            let database = Database::open_app_dir(&app_dir)?;
            let raw_config = crate::config::load_file_raw_config(&app_dir)?;
            let configured = matches
                .get_one::<String>("api-key")
                .map(String::as_str)
                .or(raw_config.api_key.as_deref());
            println!("{}", crate::operations::api_key(&database, configured)?);
        }
        Some(("reset-api-key", _)) => {
            let database = Database::open_app_dir(&app_dir)?;
            println!("{}", crate::operations::reset_api_key(&database)?);
        }
        Some((command, _)) => {
            let _raw_config = crate::config::load_file_raw_config(&app_dir)?;
            println!("sporos {} {}", crate::VERSION, command);
        }
        None => {
            println!("sporos {}", crate::VERSION);
        }
    }

    Ok(())
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

/// Build the compatibility command tree.
pub fn build_cli() -> Command {
    Command::new("cross-seed")
        .version(crate::VERSION)
        .about("Rust compatibility rebuild of cross-seed behavior")
        .subcommand_required(false)
        .arg_required_else_help(false)
        .subcommand(Command::new("gen-config"))
        .subcommand(
            Command::new("update-torrent-cache-trackers")
                .arg(Arg::new("old-announce-url").required(true))
                .arg(Arg::new("new-announce-url").required(true)),
        )
        .subcommand(
            Command::new("diff")
                .arg(Arg::new("searchee").required(true))
                .arg(Arg::new("candidate").required(true)),
        )
        .subcommand(Command::new("tree").arg(Arg::new("torrent").required(true)))
        .subcommand(Command::new("clear-indexer-failures"))
        .subcommand(Command::new("clear-cache"))
        .subcommand(Command::new("clear-client-cache"))
        .subcommand(Command::new("api-key").arg(Arg::new("api-key").long("api-key").num_args(1)))
        .subcommand(Command::new("reset-api-key"))
        .subcommand(add_daemon_options(add_shared_options(Command::new(
            "daemon",
        ))))
        .subcommand(add_shared_options(Command::new("rss")))
        .subcommand(add_search_options(add_shared_options(Command::new(
            "search",
        ))))
        .subcommand(add_inject_options(add_shared_options(Command::new(
            "inject",
        ))))
        .subcommand(add_shared_options(Command::new("restore")))
        .subcommand(add_shared_options(Command::new("test-notification")))
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
            Arg::new("qbittorrent-url")
                .long("qbittorrent-url")
                .num_args(1)
                .hide(true),
        )
        .arg(
            Arg::new("rtorrent-rpc-url")
                .long("rtorrent-rpc-url")
                .num_args(1)
                .hide(true),
        )
        .arg(
            Arg::new("transmission-rpc-url")
                .long("transmission-rpc-url")
                .num_args(1)
                .hide(true),
        )
        .arg(
            Arg::new("deluge-rpc-url")
                .long("deluge-rpc-url")
                .num_args(1)
                .hide(true),
        )
        .arg(
            Arg::new("duplicate-categories")
                .long("duplicate-categories")
                .action(ArgAction::SetTrue),
        )
        .arg(Arg::new("link-category").long("link-category").num_args(1))
        .arg(repeating("link-dirs", "link-dirs"))
        .arg(Arg::new("link-dir").long("link-dir").num_args(1).hide(true))
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
        .arg(
            Arg::new("notification-webhook-url")
                .long("notification-webhook-url")
                .num_args(1)
                .hide(true),
        )
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
    use super::build_cli;

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
    fn parses_shared_workflow_options() {
        build_cli()
            .try_get_matches_from([
                "cross-seed",
                "search",
                "--torznab",
                "https://indexer.example/api",
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
    fn parses_daemon_and_inject_specific_options() {
        build_cli()
            .try_get_matches_from([
                "cross-seed",
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
                "inject",
                "--inject-dir",
                "/output",
                "--ignore-titles",
            ])
            .expect("inject command parses");
    }
}
