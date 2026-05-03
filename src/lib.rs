//! Core library for sporos torrent cross-seeding automation.

pub mod actions;
pub mod api;
pub mod cli;
pub mod clients;
pub mod config;
pub mod daemon;
pub mod domain;
pub mod error;
pub mod integrations;
pub mod matching;
pub mod memory;
pub mod notifications;
pub mod operations;
pub mod persistence;
pub mod runtime;
pub mod scheduler;
pub mod search;
pub mod startup;
pub mod torrent;

pub use error::{Result, SporosError};

/// Package version reported by the command-line entry point.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{
        SporosError, VERSION,
        config::{MatchMode, RawConfig, RuntimeConfig, TorrentClientConfig},
        domain::{Decision, File, InfoHash, MediaType, Metafile, Searchee},
        matching::{AssessmentOptions, assess_metafile},
        search::{Blocklist, VirtualSeasonOptions, create_virtual_season_searchees, parse_title},
        torrent::{
            SavedTorrentMetadata, parse_metadata_from_filename, parse_metafile,
            saved_torrent_filename,
        },
    };

    #[test]
    fn exposes_package_version() {
        assert_eq!(VERSION, env!("CARGO_PKG_VERSION"));
    }

    #[test]
    fn errors_keep_user_facing_context() {
        let error = SporosError::configuration("missing config.toml");

        assert_eq!(
            error.to_string(),
            "configuration error: missing config.toml"
        );
    }

    #[test]
    fn compatibility_config_title_blocklist_and_filename_contracts() {
        let root = std::env::temp_dir().join("sporos-compatibility-contracts");
        let config = RuntimeConfig::normalize(
            RawConfig {
                use_client_torrents: Some(true),
                torrent_clients: vec![
                    TorrentClientConfig::parse("qbittorrent:readonly:http://localhost:8080")
                        .expect("client"),
                ],
                season_from_episodes: Some(1.0),
                include_single_episodes: Some(true),
                ..RawConfig::default()
            },
            &root,
        )
        .expect("config");
        assert!(config.use_client_torrents);
        assert_eq!(config.torrent_clients[0].kind, "qbittorrent");

        let files = [File::new("Example.Show.S01E02.1080p.WEB-DL.mkv", 100)];
        let parsed = parse_title("Example.Show.S01E02.1080p.WEB-DL-GRP", &files, None)
            .expect("parsed title");
        assert_eq!(parsed.media_type, MediaType::Episode);
        assert_eq!(parsed.resolution.as_deref(), Some("1080p"));

        let blocklist = Blocklist::parse(&["name_regex:WEB-DL".to_owned()]).expect("blocklist");
        let searchee = Searchee::from_files(
            "Example.Show.S01E02.1080p.WEB-DL-GRP",
            parsed.title,
            files.to_vec(),
        );
        assert!(blocklist.matches_searchee(&searchee));

        let metadata = SavedTorrentMetadata::new(
            MediaType::Episode,
            "tracker/example",
            "Example: Show S01E02",
            InfoHash::from_validated("0123456789abcdef0123456789abcdef01234567"),
            true,
        );
        let filename = saved_torrent_filename(&metadata);
        let parsed_metadata = parse_metadata_from_filename(&filename).expect("metadata");
        assert_eq!(parsed_metadata.media_type, MediaType::Episode);
        assert_eq!(parsed_metadata.tracker, "tracker_example");
        assert_eq!(parsed_metadata.name, "Example_ Show S01E02");
        assert!(parsed_metadata.cached);
    }

    #[test]
    fn compatibility_torrent_normalization_and_matching_contracts() {
        let metafile = parse_metafile(
            b"d4:infod5:filesld6:lengthi100e4:pathl8:Season 16:02.mkveed6:lengthi100e4:pathl8:Season 16:01.mkveee4:name12:Example.Show12:piece lengthi16384e6:pieces20:aaaaaaaaaaaaaaaaaaaaee",
        )
        .expect("metafile");
        assert_eq!(metafile.info_hash.as_str().len(), 40);
        assert!(
            metafile
                .files
                .windows(2)
                .all(|window| window[0].path <= window[1].path)
        );
        assert_eq!(
            metafile.length,
            metafile.files.iter().map(|file| file.length).sum::<u64>()
        );

        let mut searchee = Searchee::from_files(
            metafile.name.to_string(),
            metafile.title.to_string(),
            metafile.files.clone(),
        );
        searchee.media_type = metafile.media_type;
        let blocklist = Blocklist::parse(&[]).expect("blocklist");
        let excluded = BTreeSet::new();
        let strict = AssessmentOptions {
            match_mode: MatchMode::Strict,
            fuzzy_size_threshold: 0.05,
            season_from_episodes: 1.0,
            include_single_episodes: true,
            info_hashes_to_exclude: &excluded,
            blocklist: &blocklist,
        };
        let assessment = assess_metafile(&metafile, &searchee, &strict, true, 0.05);
        assert_eq!(assessment.decision, Decision::Match);

        let candidate = Metafile::from_files(
            InfoHash::from_validated("1111111111111111111111111111111111111111"),
            "Example.Show.S01",
            "Example.Show.S01",
            100,
            vec![
                File::new("Example.Show.S01E01.mkv", 100),
                File::new("Example.Show.S01E02.mkv", 100),
            ],
        );
        let searchee = Searchee::from_files(
            "Example.Show.S01",
            "Example.Show.S01",
            vec![File::new("Example.Show.S01E01.mkv", 100)],
        );
        let partial = AssessmentOptions {
            match_mode: MatchMode::Partial,
            fuzzy_size_threshold: 0.05,
            season_from_episodes: 0.5,
            include_single_episodes: true,
            info_hashes_to_exclude: &excluded,
            blocklist: &blocklist,
        };
        let assessment = assess_metafile(&candidate, &searchee, &partial, false, 0.05);
        assert_eq!(assessment.decision, Decision::MatchPartial);
    }

    #[test]
    fn compatibility_virtual_season_contract() {
        let mut first = Searchee::from_files(
            "Example.Show.S01E01",
            "Example.Show.S01E01",
            vec![File::new("Example.Show.S01E01.mkv", 100)],
        );
        first.media_type = MediaType::Episode;
        first.mtime_millis = Some(1_000);
        let mut second = Searchee::from_files(
            "Example.Show.S01E02",
            "Example.Show.S01E02",
            vec![File::new("Example.Show.S01E02.mkv", 200)],
        );
        second.media_type = MediaType::Episode;
        second.mtime_millis = Some(2_000);

        let virtuals = create_virtual_season_searchees(
            &[first, second],
            VirtualSeasonOptions {
                season_from_episodes: 0.5,
                use_filters: false,
                now_millis: 10_000,
            },
        );

        assert_eq!(virtuals.len(), 1);
        assert_eq!(virtuals[0].media_type, MediaType::Pack);
        assert_eq!(virtuals[0].files.len(), 2);
        assert_eq!(virtuals[0].mtime_millis, Some(2_000));
    }
}
