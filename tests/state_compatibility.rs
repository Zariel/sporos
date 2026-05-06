use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
    fs,
    path::PathBuf,
    process::Command,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use sporos::{
    actions::{
        InjectionActionOptions, SavedInjectionOptions, inject_saved_torrents,
        restore_from_torrent_cache, save_torrent_with_metadata,
    },
    clients::{
        ClientErrorCode, ClientTorrent, DownloadDirOptions, InjectionOptions, NewTorrent,
        ResumeOptions, TorrentClient,
    },
    config::{
        ApiIntegrationConfig, LinkType, MatchMode, RawConfig, RuntimeConfig, TorrentClientConfig,
    },
    domain::{
        ClientLabel, Decision, File, InfoHash, InjectionResult, MediaType, Metafile, Searchee,
        TorrentClientKind, TorrentClientMetadata,
    },
    integrations::{cache_torrent_file, get_cached_torrent},
    matching::AssessmentOptions,
    persistence::{
        ClientSearcheeRecord, DataRootRecord, Database, DecisionRecord, EnsembleRecord, SqlValue,
    },
    search::Blocklist,
    torrent::{SavedTorrentMetadata, parse_metadata_from_filename},
};
use sqlx::Row;

#[test]
fn current_bootstrap_reopen_preserves_compatibility_state() {
    let root = temp_path("state-reopen");
    let app_dir = root.join("app");
    let output_dir = root.join("output");
    let retry_dir = root.join("retry");
    let restore_dir = root.join("restore");
    let data_dir = root.join("data");
    fs::create_dir_all(&app_dir).expect("app dir");
    fs::create_dir_all(&data_dir).expect("data dir");
    fs::write(data_dir.join("Example.Show.S01E01.mkv"), b"episode").expect("source file");

    let runtime = RuntimeConfig::normalize(
        RawConfig {
            torznab: vec![ApiIntegrationConfig {
                url: "https://indexer.example/api".to_owned(),
                api_key: "config-key".to_owned(),
            }],
            data_dirs: vec![data_dir.clone()],
            output_dir: Some(output_dir.clone()),
            inject_dir: Some(retry_dir.clone()),
            link_dirs: vec![root.join("links")],
            action: Some("inject".to_owned()),
            include_single_episodes: Some(true),
            fuzzy_size_threshold: Some(0.05),
            notification_webhook_urls: vec!["https://hooks.example/sporos".to_owned()],
            torrent_clients: vec![
                TorrentClientConfig::parse("qbittorrent:http://localhost:8080").expect("client"),
            ],
            ..RawConfig::default()
        },
        &app_dir,
    )
    .expect("config");
    assert_eq!(runtime.output_dir, output_dir);
    assert_eq!(runtime.inject_dir.as_deref(), Some(retry_dir.as_path()));
    assert_eq!(
        runtime.data_dirs.as_slice(),
        std::slice::from_ref(&data_dir)
    );
    assert_eq!(runtime.torrent_clients[0].kind, "qbittorrent");
    assert_eq!(
        runtime.notification_webhook_urls,
        ["https://hooks.example/sporos".to_owned()]
    );

    let bytes = torrent_bytes("Example.Show.S01E01.mkv", 7);
    let cached_info_hash = {
        let database = Database::open_app_dir(&app_dir).expect("database");
        let metafile = cache_torrent_file(&app_dir, &bytes).expect("cache");
        let info_hash = metafile.info_hash.clone().into_owned();
        seed_database_state(&database, info_hash.as_str());

        save_torrent_with_metadata(
            &output_dir,
            &SavedTorrentMetadata::new(
                MediaType::Episode,
                "tracker",
                "Example.Show.S01E01",
                info_hash.clone(),
                false,
            ),
            &bytes,
            false,
            |_| Ok(()),
        )
        .expect("output save");
        save_torrent_with_metadata(
            &retry_dir,
            &SavedTorrentMetadata::new(
                MediaType::Episode,
                "tracker",
                "Example.Show.S01E01",
                info_hash.clone(),
                false,
            ),
            &bytes,
            false,
            |_| Ok(()),
        )
        .expect("retry save");

        info_hash
    };

    let database = Database::open_app_dir(&app_dir).expect("reopen database");
    assert_database_state_survived(&database, cached_info_hash.as_str());

    let cached = get_cached_torrent(&app_dir, &cached_info_hash)
        .expect("cached torrent")
        .expect("cached torrent exists");
    assert_eq!(cached.info_hash, cached_info_hash);

    let restored =
        restore_from_torrent_cache(&database, &app_dir, &restore_dir, |_| Ok(())).expect("restore");
    assert_eq!(restored.scanned, 1);
    assert_eq!(restored.restored, 1);
    assert_eq!(torrent_file_count(&restore_dir), 1);

    let output_file = only_torrent_file(&output_dir);
    let metadata = parse_metadata_from_filename(
        output_file
            .file_name()
            .and_then(std::ffi::OsStr::to_str)
            .expect("output filename"),
    )
    .expect("output metadata");
    assert_eq!(metadata.info_hash, cached_info_hash);
    assert_eq!(fs::read(output_file).expect("output bytes"), bytes);

    let client = FakeClient::new();
    let clients: [&dyn TorrentClient; 1] = [&client];
    let blocklist = Blocklist::parse(&[]).expect("blocklist");
    let excluded = BTreeSet::new();
    let assessment = AssessmentOptions {
        match_mode: MatchMode::Strict,
        fuzzy_size_threshold: 0.05,
        season_from_episodes: 1.0,
        include_single_episodes: true,
        info_hashes_to_exclude: &excluded,
        blocklist: &blocklist,
    };
    let injection = InjectionActionOptions {
        clients: &clients,
        output_dir: Some(&output_dir),
        link_dirs: &[],
        link_type: LinkType::Symlink,
        flat_linking: false,
        unwrap_symlinks: false,
        skip_recheck: true,
        match_mode: MatchMode::Strict,
        auto_resume_max_download: 0,
        ignore_non_relevant_files_to_resume: false,
        category: Some(ClientLabel::new("tv")),
        tags: vec![ClientLabel::new("managed")],
        duplicate_categories: false,
    };
    let saved_options = SavedInjectionOptions {
        input_dir: &retry_dir,
        injection: &injection,
        assessment: &assessment,
        ignore_titles: false,
    };
    let injected =
        inject_saved_torrents(&saved_options, &[saved_retry_searchee(&data_dir)], |_| {
            Ok(())
        })
        .expect("saved retry");
    assert_eq!(injected.scanned, 1);
    assert_eq!(injected.injected, 1);
    assert_eq!(torrent_file_count(&retry_dir), 0);

    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn release_tags_require_schema_migration_fixtures() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let tags = release_tags(&manifest_dir);
    let migration_dir = manifest_dir.join("tests/fixtures/migrations");

    if tags.is_empty() {
        assert!(
            !migration_dir.exists()
                || fs::read_dir(&migration_dir).expect("migrations").count() == 0
        );
        return;
    }

    for tag in tags {
        let fixture = migration_dir.join(format!("{tag}.sql"));
        assert!(
            fixture.exists(),
            "release tag {tag} needs a schema migration fixture"
        );
    }
}

fn seed_database_state(database: &Database, info_hash: &str) {
    database
        .set_api_key("0123456789abcdef0123456789abcdef0123456789abcdef")
        .expect("api key");
    let searchee_id = database
        .get_or_insert_searchee("Example.Show.S01E01")
        .expect("searchee");
    database
        .upsert_decision(&DecisionRecord {
            searchee_id,
            guid: "guid-state",
            info_hash: Some(info_hash),
            decision: Decision::Match,
            first_seen: 100,
            last_seen: 200,
            fuzzy_size_factor: 0.05,
        })
        .expect("decision");
    database
        .execute_sql(
            "INSERT INTO indexer
                (id, name, url, apikey, trackers, active, status, retry_after,
                 search_cap, tv_search_cap, movie_search_cap, tv_id_caps, cat_caps, limits_caps)
             VALUES
                (1, 'Tracker', 'https://indexer.example/api', 'indexer-key',
                 '[\"tracker.example\"]', 1, 'rate-limited', 12345,
                 1, 1, 0, '[\"imdbid\"]', '[5000,5040]', '{\"default\":100}')",
            &[],
        )
        .expect("indexer");
    database
        .execute_sql(
            "INSERT INTO timestamp
                (searchee_id, indexer_id, first_searched, last_searched)
             VALUES (?1, 1, 111, 222)",
            &[SqlValue::I64(searchee_id)],
        )
        .expect("timestamp");
    database
        .execute_sql(
            "INSERT INTO rss (indexer_id, last_seen_guid) VALUES (1, 'rss-guid')",
            &[],
        )
        .expect("rss");
    let files = [File::new("Example.Show.S01E01.mkv", 7)];
    let tags = [ClientLabel::new("managed")];
    let trackers = [Cow::Borrowed("tracker.example")];
    database
        .refresh_client_searchees(
            "client",
            [ClientSearcheeRecord {
                client_host: "client",
                info_hash,
                name: "Example.Show.S01E01",
                title: "Example.Show.S01E01",
                files: &files,
                length: 7,
                save_path: "/downloads",
                category: Some("tv"),
                tags: &tags,
                trackers: &trackers,
                lookup: None,
            }],
        )
        .expect("client searchee");
    database
        .upsert_ensemble(&EnsembleRecord {
            client_host: Some("client"),
            path: "/downloads/Example.Show.S01E01.mkv",
            info_hash: Some(info_hash),
            ensemble: "example show s01",
            element: "1",
        })
        .expect("client ensemble");
    database
        .refresh_data_roots([DataRootRecord {
            path: "/data/example",
            title: "Example Show",
            lookup: None,
        }])
        .expect("data root");
    database
        .upsert_ensemble(&EnsembleRecord {
            client_host: None,
            path: "/data/example/Example.Show.S01E01.mkv",
            info_hash: None,
            ensemble: "example show s01",
            element: "1",
        })
        .expect("data ensemble");
}

fn assert_database_state_survived(database: &Database, info_hash: &str) {
    assert_eq!(
        database.get_api_key().expect("api key").as_deref(),
        Some("0123456789abcdef0123456789abcdef0123456789abcdef")
    );
    let guid_page = database.guid_info_hash_page(0, 10).expect("guid page");
    assert_eq!(guid_page.len(), 1);
    let guid = guid_page.first().expect("guid row");
    assert_eq!(guid.guid, "guid-state");
    assert_eq!(guid.info_hash, info_hash);

    let indexer_state: (String, String, String, i64, String) = database
        .query_row(
            "SELECT name, apikey, status, retry_after, trackers FROM indexer WHERE id = 1",
            &[],
            |row| (row.get(0), row.get(1), row.get(2), row.get(3), row.get(4)),
        )
        .expect("indexer state");
    assert_eq!(
        indexer_state,
        (
            "Tracker".to_owned(),
            "indexer-key".to_owned(),
            "rate-limited".to_owned(),
            12345,
            "[\"tracker.example\"]".to_owned()
        )
    );

    let rss_guid: String = database
        .query_scalar("SELECT last_seen_guid FROM rss WHERE indexer_id = 1", &[])
        .expect("rss");
    assert_eq!(rss_guid, "rss-guid");

    let timestamp_last: i64 = database
        .query_scalar(
            "SELECT last_searched FROM timestamp WHERE indexer_id = 1",
            &[],
        )
        .expect("timestamp");
    assert_eq!(timestamp_last, 222);

    let client_state: (String, String, String, String) = database
        .query_row(
            "SELECT category, tags, trackers, save_path FROM client_searchee WHERE client_host = 'client'",
            &[],
            |row| (row.get(0), row.get(1), row.get(2), row.get(3)),
        )
        .expect("client state");
    assert_eq!(
        client_state,
        (
            "tv".to_owned(),
            "[\"managed\"]".to_owned(),
            "[\"tracker.example\"]".to_owned(),
            "/downloads".to_owned()
        )
    );

    let ensemble_count: i64 = database
        .query_scalar(
            "SELECT
                (SELECT COUNT(*) FROM data_ensemble)
                + (SELECT COUNT(*) FROM client_ensemble)",
            &[],
        )
        .expect("ensemble count");
    assert_eq!(ensemble_count, 2);
}

fn saved_retry_searchee(data_dir: &std::path::Path) -> Searchee<'static> {
    let mut searchee = Searchee::from_files(
        "Example.Show.S01E01",
        "Example.Show.S01E01",
        vec![File::new("Example.Show.S01E01.mkv", 7)],
    );
    searchee.path = Some(Cow::Owned(data_dir.to_string_lossy().into_owned()));
    searchee.mtime_millis = file_mtime_millis(&data_dir.join("Example.Show.S01E01.mkv"));
    searchee.media_type = MediaType::Episode;
    searchee.into_owned()
}

fn file_mtime_millis(path: &std::path::Path) -> Option<u64> {
    let modified = fs::metadata(path).ok()?.modified().ok()?;
    let duration = modified.duration_since(UNIX_EPOCH).ok()?;
    Some(duration.as_millis().min(u128::from(u64::MAX)) as u64)
}

fn torrent_bytes(name: &str, length: u64) -> Vec<u8> {
    format!(
        "d4:infod6:lengthi{length}e4:name{}:{name}12:piece lengthi100e6:pieces20:aaaaaaaaaaaaaaaaaaaaee",
        name.len()
    )
    .into_bytes()
}

fn only_torrent_file(dir: &std::path::Path) -> PathBuf {
    fs::read_dir(dir)
        .expect("torrent dir")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| path.extension().and_then(std::ffi::OsStr::to_str) == Some("torrent"))
        .expect("torrent file")
}

fn torrent_file_count(dir: &std::path::Path) -> usize {
    fs::read_dir(dir)
        .map(|entries| {
            entries
                .filter_map(Result::ok)
                .filter(|entry| {
                    entry.path().extension().and_then(std::ffi::OsStr::to_str) == Some("torrent")
                })
                .count()
        })
        .unwrap_or(0)
}

fn release_tags(manifest_dir: &std::path::Path) -> Vec<String> {
    let Ok(output) = Command::new("git")
        .args(["tag", "--list", "v*"])
        .current_dir(manifest_dir)
        .output()
    else {
        return Vec::new();
    };
    if !output.status.success() {
        return Vec::new();
    }

    String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::trim)
        .filter(|tag| !tag.is_empty())
        .map(str::to_owned)
        .collect()
}

#[derive(Clone)]
struct FakeClient {
    metadata: TorrentClientMetadata<'static>,
    injections: Arc<Mutex<usize>>,
}

impl FakeClient {
    fn new() -> Self {
        Self {
            metadata: TorrentClientMetadata::new(
                "fake-client",
                0,
                TorrentClientKind::QBittorrent,
                false,
                "fake",
            ),
            injections: Arc::new(Mutex::new(0)),
        }
    }
}

impl TorrentClient for FakeClient {
    fn metadata(&self) -> &TorrentClientMetadata<'_> {
        &self.metadata
    }

    fn is_torrent_in_client(&self, _info_hash: &InfoHash<'_>) -> sporos::Result<bool> {
        Ok(false)
    }

    fn is_torrent_complete(&self, _info_hash: &InfoHash<'_>) -> sporos::Result<bool> {
        Ok(true)
    }

    fn is_torrent_checking(&self, _info_hash: &InfoHash<'_>) -> sporos::Result<bool> {
        Ok(false)
    }

    fn get_all_torrents(&self) -> sporos::Result<Vec<ClientTorrent<'static>>> {
        Ok(Vec::new())
    }

    fn get_download_dir(
        &self,
        _metafile: &Metafile<'_>,
        _options: DownloadDirOptions,
    ) -> sporos::Result<Result<PathBuf, ClientErrorCode>> {
        Ok(Ok(PathBuf::from("/downloads")))
    }

    fn get_all_download_dirs(&self) -> sporos::Result<BTreeMap<String, PathBuf>> {
        Ok(BTreeMap::new())
    }

    fn inject(
        &self,
        _new_torrent: &NewTorrent<'_>,
        _searchee: &Searchee<'_>,
        _decision: Decision,
        _options: &InjectionOptions,
    ) -> sporos::Result<InjectionResult> {
        if let Ok(mut injections) = self.injections.lock() {
            *injections += 1;
        }
        Ok(InjectionResult::Injected)
    }

    fn recheck_torrent(&self, _info_hash: &InfoHash<'_>) -> sporos::Result<()> {
        Ok(())
    }

    fn resume_injection(
        &self,
        _metafile: &Metafile<'_>,
        _decision: Decision,
        _options: ResumeOptions,
    ) -> sporos::Result<()> {
        Ok(())
    }

    fn validate_config(&self) -> sporos::Result<()> {
        Ok(())
    }
}

fn temp_path(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!(
        "sporos-state-{label}-{}-{nanos}",
        std::process::id()
    ))
}
