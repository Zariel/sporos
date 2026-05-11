use super::{
    api_key, api_key_async, cleanup_db, cleanup_db_with_clients, clear_cache, clear_cache_async,
    clear_client_cache, clear_client_cache_async, clear_indexer_failures,
    clear_indexer_failures_async, injection_options, refresh_workflow_client_searchees,
    reset_api_key, reset_api_key_async, rss_time_since_last_run, run_announce_match,
    run_webhook_search, update_torrent_cache_trackers, update_torrent_cache_trackers_in_dir,
    webhook_matches_request, webhook_targets_and_excluded,
};
use crate::{
    api::{WebhookPathSnapshot, WebhookRequest},
    clients::{
        ClientErrorCode, ClientTorrent, DownloadDirOptions, InjectionOptions, NewTorrent,
        ResumeOptions, TorrentClient,
    },
    config::{RawConfig, RuntimeConfig},
    domain::{
        Candidate, ClientLabel, Decision, File, InfoHash, InjectionResult, Metafile, Searchee,
        TorrentClientKind, TorrentClientMetadata,
    },
    notifications::NotificationSender,
    persistence::{
        AsyncDatabase, ClientSearcheeRecord, Database, DecisionRecord, EnsembleRecord, SqlValue,
    },
    startup::Redactor,
};
use std::{
    borrow::Cow,
    collections::BTreeMap,
    fs,
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[test]
fn api_key_prefers_config_then_db_then_generated() {
    let root = temp_path("api");
    fs::create_dir_all(&root).expect("temp dir");
    let database = Database::open_app_dir(&root).expect("database");

    assert_eq!(
        api_key(&database, Some("configured-api-key")).expect("configured"),
        "configured-api-key"
    );
    let generated = api_key(&database, None).expect("generated");
    assert_eq!(generated.len(), 48);
    assert_eq!(api_key(&database, None).expect("stored"), generated);
    let reset = reset_api_key(&database).expect("reset");
    assert_eq!(reset.len(), 48);
    assert_ne!(reset, generated);

    let _cleanup = fs::remove_dir_all(root);
}

#[tokio::test]
async fn async_api_key_prefers_config_then_db_then_generated() {
    let root = temp_path("async-api");
    fs::create_dir_all(&root).expect("temp dir");
    let database = AsyncDatabase::open_app_dir(&root).await.expect("database");

    assert_eq!(
        api_key_async(&database, Some("configured-api-key"))
            .await
            .expect("configured"),
        "configured-api-key"
    );
    let generated = api_key_async(&database, None).await.expect("generated");
    assert_eq!(generated.len(), 48);
    assert_eq!(
        api_key_async(&database, None).await.expect("stored"),
        generated
    );
    let reset = reset_api_key_async(&database).await.expect("reset");
    assert_eq!(reset.len(), 48);
    assert_ne!(reset, generated);

    database.close().await;
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn clears_cache_tables() {
    let root = temp_path("clear-cache");
    fs::create_dir_all(&root).expect("temp dir");
    let database = Database::open_app_dir(&root).expect("database");
    let searchee_id = database.get_or_insert_searchee("name").expect("searchee");
    database
        .upsert_decision(&DecisionRecord {
            searchee_id,
            guid: "guid",
            info_hash: None,
            decision: crate::domain::Decision::NoDownloadLink,
            first_seen: 1,
            last_seen: 1,
            fuzzy_size_factor: 0.05,
        })
        .expect("decision");

    let result = clear_cache(&database).expect("clear");

    assert_eq!(result.decisions_removed, 1);
    let _cleanup = fs::remove_dir_all(root);
}

#[tokio::test]
async fn async_clears_cache_tables_and_indexer_failures() {
    let root = temp_path("async-clear-cache");
    fs::create_dir_all(&root).expect("temp dir");
    let sync_database = Database::open_app_dir(&root).expect("database");
    let searchee_id = sync_database
        .get_or_insert_searchee("name")
        .expect("searchee");
    sync_database
        .upsert_decision(&DecisionRecord {
            searchee_id,
            guid: "guid",
            info_hash: None,
            decision: crate::domain::Decision::NoDownloadLink,
            first_seen: 1,
            last_seen: 1,
            fuzzy_size_factor: 0.05,
        })
        .expect("decision");
    sync_database
        .execute_sql(
            "INSERT INTO indexer (url, apikey, active, status, retry_after)
                 VALUES ('https://indexer.example', 'key', 1, 'RATE_LIMITED', 100)",
            &[],
        )
        .expect("indexer");
    drop(sync_database);

    let database = AsyncDatabase::open_app_dir(&root).await.expect("database");

    let cache = clear_cache_async(&database).await.expect("clear");
    let failures = clear_indexer_failures_async(&database)
        .await
        .expect("failures");
    let client = clear_client_cache_async(&database)
        .await
        .expect("client cache");

    assert_eq!(cache.decisions_removed, 1);
    assert_eq!(failures, 1);
    assert_eq!(client.torrents_removed, 0);

    database.close().await;
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn clears_client_cache_tables_and_indexer_failures() {
    let root = temp_path("client-cache");
    fs::create_dir_all(&root).expect("temp dir");
    let database = Database::open_app_dir(&root).expect("database");
    database
        .execute_sql(
            "INSERT INTO indexer (url, apikey, active, status, retry_after)
                 VALUES ('https://indexer.example', 'key', 1, 'RATE_LIMITED', 100)",
            &[],
        )
        .expect("indexer");

    let failures = clear_indexer_failures(&database).expect("failures");
    let client = clear_client_cache(&database).expect("client cache");

    assert_eq!(failures, 1);
    assert_eq!(client.torrents_removed, 0);
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn updates_cached_torrent_tracker_urls() {
    let root = temp_path("trackers");
    let cache_dir = root.join("torrent_cache");
    fs::create_dir_all(&cache_dir).expect("cache dir");
    let path = cache_dir.join("0123456789abcdef0123456789abcdef01234567.cached.torrent");
    fs::write(
            &path,
            b"d8:announce28:https://old.example/announce13:announce-listll28:https://old.example/announceeee",
        )
        .expect("write");

    let result = update_torrent_cache_trackers(
        &root,
        "https://old.example/announce",
        "https://longer-new.example/announce",
    )
    .expect("update");

    assert_eq!(result.files_seen, 1);
    assert_eq!(result.files_updated, 1);
    assert_eq!(
            fs::read(&path).expect("read"),
            b"d8:announce35:https://longer-new.example/announce13:announce-listll35:https://longer-new.example/announceeee"
        );
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn updates_cached_torrent_tracker_urls_in_configured_cache() {
    let root = temp_path("trackers-configured");
    let cache_dir = root.join("configured-cache");
    fs::create_dir_all(&cache_dir).expect("cache dir");
    let path = cache_dir.join("0123456789abcdef0123456789abcdef01234567.cached.torrent");
    fs::write(&path, b"d8:announce28:https://old.example/announcee").expect("write");

    let result = update_torrent_cache_trackers_in_dir(
        &cache_dir,
        "https://old.example/announce",
        "https://new.example/announce",
    )
    .expect("update");

    assert_eq!(result.files_seen, 1);
    assert_eq!(result.files_updated, 1);
    assert_eq!(
        fs::read(&path).expect("read"),
        b"d8:announce28:https://new.example/announcee"
    );
    assert!(!root.join("torrent_cache").exists());
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn rss_elapsed_time_uses_persisted_last_run_with_cadence_fallback() {
    let root = temp_path("rss-last-run");
    let app_dir = root.join("app");
    let data_dir = root.join("data");
    fs::create_dir_all(&app_dir).expect("app dir");
    fs::create_dir_all(&data_dir).expect("data dir");
    let database = Database::open_app_dir(&app_dir).expect("database");
    let config = RuntimeConfig::normalize(
        RawConfig {
            rss_cadence: Some(600_000),
            data_dirs: vec![data_dir],
            ..RawConfig::default()
        },
        &app_dir,
    )
    .expect("config");

    assert_eq!(
        rss_time_since_last_run(&database, &config, 1_000_000).expect("missing cursor"),
        Duration::from_millis(600_000)
    );
    database
        .execute_sql(
            "INSERT INTO job_log (name, last_run) VALUES ('rss', 100_000)",
            &[],
        )
        .expect("job log");

    assert_eq!(
        rss_time_since_last_run(&database, &config, 250_000).expect("elapsed"),
        Duration::from_millis(150_000)
    );
    database
        .execute_sql(
            "UPDATE job_log SET last_run = 300_000 WHERE name = 'rss'",
            &[],
        )
        .expect("future job log");

    assert_eq!(
        rss_time_since_last_run(&database, &config, 250_000).expect("current cursor"),
        Duration::from_millis(600_000)
    );
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn cleanup_prunes_cache_null_decisions_and_missing_paths() {
    let root = temp_path("cleanup");
    let cache_dir = root.join("configured-cache");
    fs::create_dir_all(&cache_dir).expect("cache dir");
    let database = Database::open_app_dir(&root).expect("database");
    let existing_data_path = root.join("data");
    fs::create_dir_all(&existing_data_path).expect("data dir");
    let missing_data = root.join("missing-data");
    let missing_ensemble = root.join("missing-episode.mkv");
    let existing_data = existing_data_path.to_string_lossy();
    let missing_data = missing_data.to_string_lossy();
    database
        .execute_sql(
            "INSERT INTO data (path, title) VALUES (?1, 'Existing'), (?2, 'Missing')",
            &[
                SqlValue::Text(Cow::Borrowed(existing_data.as_ref())),
                SqlValue::Text(Cow::Borrowed(missing_data.as_ref())),
            ],
        )
        .expect("data");
    let missing_ensemble = missing_ensemble.to_string_lossy();
    database
        .execute_sql(
            "INSERT INTO data_ensemble (data_root, path, info_hash, ensemble, element)
                 VALUES (?1, ?2, NULL, 'show s01', 'e01')",
            &[
                SqlValue::Text(Cow::Borrowed(existing_data.as_ref())),
                SqlValue::Text(Cow::Borrowed(missing_ensemble.as_ref())),
            ],
        )
        .expect("ensemble");
    let searchee_id = database.get_or_insert_searchee("name").expect("searchee");
    let old_hash = "0123456789012345678901234567890123456789";
    let recent_hash = "1111111111111111111111111111111111111111";
    let missing_hash = "2222222222222222222222222222222222222222";
    fs::write(cache_dir.join(format!("{old_hash}.cached.torrent")), b"old").expect("old cache");
    fs::write(
        cache_dir.join(format!("{recent_hash}.cached.torrent")),
        b"recent",
    )
    .expect("recent cache");
    let now = 800 * 86_400_000;
    insert_decision(&database, searchee_id, "old-guid", Some(old_hash), 1);
    insert_decision(
        &database,
        searchee_id,
        "recent-guid",
        Some(recent_hash),
        now,
    );
    insert_decision(
        &database,
        searchee_id,
        "missing-guid",
        Some(missing_hash),
        now,
    );
    insert_decision(&database, searchee_id, "null-guid", None, now);
    insert_announce_work(
        &database,
        "old-active",
        "old-active",
        "queued",
        now - 8 * 86_400_000,
    );
    let config = RuntimeConfig::normalize(
        RawConfig {
            data_dirs: vec![existing_data_path],
            torrent_cache_dir: Some(cache_dir.clone()),
            season_from_episodes: Some(1.0),
            ..RawConfig::default()
        },
        &root,
    )
    .expect("config");

    let result = cleanup_db(&database, &root, &config, now).expect("cleanup");

    assert_eq!(result.data_rows_removed, 1);
    assert_eq!(result.ensemble_rows_removed, 1);
    assert_eq!(result.torrent_cache_files_removed, 1);
    assert_eq!(result.null_decisions_removed, 1);
    assert_eq!(result.missing_cache_decisions_removed, 2);
    assert!(!result.catastrophic_decision_cleanup_skipped);
    assert_eq!(result.guid_info_hash_rows, 1);
    assert!(
        !cache_dir
            .join(format!("{old_hash}.cached.torrent"))
            .exists()
    );
    assert_eq!(
        database
            .query_scalar::<i64>("SELECT COUNT(*) FROM announce_work", &[])
            .expect("announce work count"),
        1
    );
    assert_eq!(
        database
            .query_scalar::<i64>(
                "SELECT COUNT(*) FROM announce_work WHERE status = 'queued'",
                &[],
            )
            .expect("active announce work count"),
        1
    );
    assert!(
        cache_dir
            .join(format!("{recent_hash}.cached.torrent"))
            .exists()
    );
    assert!(!root.join("torrent_cache").exists());
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn cleanup_skips_catastrophic_missing_decision_prune() {
    let root = temp_path("cleanup-guard");
    fs::create_dir_all(&root).expect("root");
    let database = Database::open_app_dir(&root).expect("database");
    let searchee_id = database.get_or_insert_searchee("name").expect("searchee");
    insert_decision(
        &database,
        searchee_id,
        "missing-guid",
        Some("0123456789012345678901234567890123456789"),
        2_000_000,
    );
    let config = RuntimeConfig::normalize(RawConfig::default(), &root).expect("config");

    let result = cleanup_db(&database, &root, &config, 2_000_000).expect("cleanup");

    assert!(result.catastrophic_decision_cleanup_skipped);
    assert_eq!(result.missing_cache_decisions_removed, 0);
    let remaining: i64 = database
        .query_scalar("SELECT COUNT(*) FROM decision", &[])
        .expect("count");
    assert_eq!(remaining, 1);
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn announce_match_uses_reverse_lookup_pipeline() {
    let root = temp_path("announce");
    let torrent_dir = root.join("torrents");
    fs::create_dir_all(&torrent_dir).expect("torrent dir");
    let database = Database::open_app_dir(&root).expect("database");
    database
        .upsert_client_searchee(&ClientSearcheeRecord {
            client_host: "client-a",
            info_hash: "0123456789abcdef0123456789abcdef01234567",
            name: "Example.Show.S01E01",
            title: "Example Show S01E01",
            files: &[File::new("Example.Show.S01E01.mkv", 10)],
            length: 10,
            save_path: "/downloads",
            category: None,
            tags: &[],
            trackers: &[Cow::Borrowed("tracker.example")],
            lookup: None,
        })
        .expect("client searchee");
    let searchee_id = database
        .get_or_insert_searchee("Example Show S01E01")
        .expect("searchee");
    database
        .upsert_decision(&DecisionRecord {
            searchee_id,
            guid: "https://tracker.example/download",
            info_hash: None,
            decision: Decision::MatchSizeOnly,
            first_seen: 1_000,
            last_seen: 1_000,
            fuzzy_size_factor: 0.05,
        })
        .expect("decision");
    let config = RuntimeConfig::normalize(
        RawConfig {
            torrent_dir: Some(torrent_dir),
            include_single_episodes: Some(true),
            ..RawConfig::default()
        },
        &root,
    )
    .expect("config");
    let notifier = NotificationSender::new(Vec::new(), Redactor::default()).expect("notifier");
    let candidate = Candidate::new(
        "Example.Show.S01E01",
        "https://tracker.example/download",
        Some("https://tracker.example/download"),
        "tracker",
    );

    let outcome = run_announce_match(&database, &root, &config, candidate, &notifier)
        .expect("announce")
        .expect("outcome");

    assert_eq!(outcome.decision, Decision::MatchSizeOnly);
    assert_eq!(outcome.action_result, None);
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn webhook_search_targets_requested_path() {
    let root = temp_path("webhook");
    let release = root.join("Example.Show.S01E01");
    fs::create_dir_all(&release).expect("release dir");
    fs::write(release.join("Example.Show.S01E01.mkv"), b"video").expect("video");
    let database = Database::open_app_dir(&root).expect("database");
    let config = RuntimeConfig::normalize(
        RawConfig {
            data_dirs: vec![release.clone()],
            include_single_episodes: Some(false),
            ..RawConfig::default()
        },
        &root,
    )
    .expect("config");
    let notifier = NotificationSender::new(Vec::new(), Redactor::default()).expect("notifier");

    let summary = run_webhook_search(
        &database,
        &root,
        &config,
        WebhookRequest {
            info_hash: None,
            path: Some(release.display().to_string()),
            path_snapshot: None,
            ignore_cross_seeds: false,
            ignore_exclude_recent_search: true,
            ignore_exclude_older: true,
            ignore_block_list: false,
            include_single_episodes: true,
            include_non_videos: false,
        },
        &notifier,
    )
    .expect("webhook search");

    assert_eq!(summary.searchees_seen, 1);
    assert_eq!(summary.indexer_searches, 0);
    let search_key: String = database
        .query_scalar("SELECT search_key FROM data", &[])
        .expect("data search key");
    let media_type: String = database
        .query_scalar("SELECT media_type FROM data", &[])
        .expect("data media type");
    let length: i64 = database
        .query_scalar("SELECT length FROM data", &[])
        .expect("data length");
    assert_eq!(search_key, "example.show.s01e01");
    assert_eq!(media_type, "episode");
    assert_eq!(length, 5);
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn webhook_match_canonicalizes_requested_path() {
    let root = temp_path("webhook-canonical");
    let data = root.join("data");
    fs::create_dir_all(&data).expect("data dir");
    let file = data.join("episode.mkv");
    fs::write(&file, b"video").expect("video");
    let mut searchee = Searchee::from_files(
        "Episode",
        "Episode",
        vec![File::new(file.display().to_string(), 5)],
    );
    searchee.path = Some(Cow::Owned(file.display().to_string()));
    let request = WebhookRequest {
        info_hash: None,
        path: Some(
            data.join("..")
                .join("data")
                .join("episode.mkv")
                .display()
                .to_string(),
        ),
        path_snapshot: None,
        ignore_cross_seeds: false,
        ignore_exclude_recent_search: false,
        ignore_exclude_older: false,
        ignore_block_list: false,
        include_single_episodes: false,
        include_non_videos: false,
    };

    assert!(webhook_matches_request(&searchee, &request));
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn webhook_search_revalidates_path_snapshot() {
    let root = temp_path("webhook-revalidate");
    fs::create_dir_all(&root).expect("root dir");
    let path = root.join("episode.mkv");
    fs::write(&path, b"video").expect("video");
    let database = Database::open_app_dir(&root).expect("database");
    let config = RuntimeConfig::normalize(RawConfig::default(), &root).expect("config");
    let notifier = NotificationSender::new(Vec::new(), Redactor::default()).expect("notifier");
    let snapshot = WebhookPathSnapshot::capture(&path.display().to_string()).expect("snapshot");
    let request = WebhookRequest {
        info_hash: None,
        path: Some(snapshot.canonical().to_owned()),
        path_snapshot: Some(snapshot),
        ignore_cross_seeds: false,
        ignore_exclude_recent_search: false,
        ignore_exclude_older: false,
        ignore_block_list: false,
        include_single_episodes: false,
        include_non_videos: false,
    };
    fs::write(&path, b"changed video").expect("changed video");

    let error = match run_webhook_search(&database, &root, &config, request, &notifier) {
        Ok(_) => panic!("webhook search should reject changed path"),
        Err(error) => error,
    };

    assert!(
        error
            .to_string()
            .contains("webhook path changed before search")
    );
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn webhook_search_excludes_all_local_hashes_not_only_targets() {
    let mut target = Searchee::from_files(
        "Target",
        "Target",
        vec![File::new("/downloads/target.mkv", 5)],
    );
    target.path = Some(Cow::Borrowed("/downloads/target.mkv"));
    let mut other = Searchee::from_files(
        "Existing",
        "Existing",
        vec![File::new("/downloads/existing.mkv", 5)],
    );
    other.info_hash = Some(InfoHash::from_validated(
        "0123456789abcdef0123456789abcdef01234567",
    ));
    let request = WebhookRequest {
        info_hash: None,
        path: Some("/downloads/target.mkv".to_owned()),
        path_snapshot: None,
        ignore_cross_seeds: false,
        ignore_exclude_recent_search: false,
        ignore_exclude_older: false,
        ignore_block_list: false,
        include_single_episodes: false,
        include_non_videos: false,
    };

    let (targets, excluded) =
        webhook_targets_and_excluded(vec![target.into_owned(), other.into_owned()], &request);

    assert_eq!(targets.len(), 1);
    assert!(excluded.contains("0123456789abcdef0123456789abcdef01234567"));
}

#[test]
fn cleanup_refreshes_client_searchees_and_rebuilds_ensemble() {
    let root = temp_path("cleanup-client-refresh");
    fs::create_dir_all(&root).expect("root");
    let database = Database::open_app_dir(&root).expect("database");
    let stale_files = [File::new("Old.Show.S01E01.mkv", 1)];
    database
        .refresh_client_searchees(
            "localhost",
            [
                ClientSearcheeRecord {
                    client_host: "localhost",
                    info_hash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    name: "Old.Show.S01E01",
                    title: "Old Show S01E01",
                    files: &stale_files,
                    length: 1,
                    save_path: "/downloads",
                    category: None,
                    tags: &[],
                    trackers: &[],
                    lookup: None,
                },
                ClientSearcheeRecord {
                    client_host: "localhost",
                    info_hash: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    name: "Example.Show.S01E01",
                    title: "Example Show S01E01",
                    files: &stale_files,
                    length: 1,
                    save_path: "/downloads",
                    category: None,
                    tags: &[],
                    trackers: &[],
                    lookup: None,
                },
            ],
        )
        .expect("seed client");
    database
        .upsert_ensemble(&EnsembleRecord {
            client_host: Some("localhost"),
            path: "/downloads/Old.Show.S01E01.mkv",
            info_hash: Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            ensemble: "old.show S01",
            element: "01",
        })
        .expect("stale ensemble");
    database
        .upsert_ensemble(&EnsembleRecord {
            client_host: Some("localhost"),
            path: "/downloads/Example.Show.S01E01.old.mkv",
            info_hash: Some("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
            ensemble: "example.show S01",
            element: "01",
        })
        .expect("stale same-hash ensemble");
    let client = FakeClient::new(vec![ClientTorrent {
        info_hash: InfoHash::new("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
            .expect("hash")
            .into_owned(),
        name: Cow::Borrowed("Example.Show.S01E01"),
        files: vec![File::new("Example.Show.S01E01.mkv", 42)],
        save_path: Cow::Borrowed("/downloads"),
        category: None,
        tags: Vec::new(),
        trackers: Vec::new(),
        complete: true,
        checking: false,
    }]);
    let config = RuntimeConfig::normalize(
        RawConfig {
            use_client_torrents: Some(true),
            season_from_episodes: Some(1.0),
            torrent_clients: vec![
                crate::config::TorrentClientConfig::parse("qbittorrent:http://localhost:8080")
                    .expect("client"),
            ],
            ..RawConfig::default()
        },
        &root,
    )
    .expect("config");

    let result =
        cleanup_db_with_clients(&database, &root, &config, 2_000_000, &[&client]).expect("cleanup");

    assert_eq!(result.client_searchees_refreshed, 1);
    assert_eq!(result.client_searchees_pruned, 1);
    assert_eq!(result.client_ensemble_rows_rebuilt, 1);
    let client_rows: i64 = database
        .query_scalar("SELECT COUNT(*) FROM client_searchee", &[])
        .expect("client count");
    let ensemble_path: String = database
        .query_scalar("SELECT path FROM client_ensemble", &[])
        .expect("ensemble path");
    let ensemble_rows: i64 = database
        .query_scalar("SELECT COUNT(*) FROM client_ensemble", &[])
        .expect("ensemble count");
    assert_eq!(client_rows, 1);
    assert_eq!(ensemble_rows, 1);
    assert_eq!(ensemble_path, "/downloads/Example.Show.S01E01.mkv");
    let search_key: String = database
        .query_scalar("SELECT search_key FROM client_searchee", &[])
        .expect("client search key");
    let media_type: String = database
        .query_scalar("SELECT media_type FROM client_searchee", &[])
        .expect("client media type");
    let video_bytes: i64 = database
        .query_scalar("SELECT video_bytes FROM client_searchee", &[])
        .expect("client video bytes");
    assert_eq!(search_key, "example.show.s01e01");
    assert_eq!(media_type, "episode");
    assert_eq!(video_bytes, 42);
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn workflow_client_refresh_uses_streaming_inventory() {
    let root = temp_path("workflow-client-cache");
    fs::create_dir_all(&root).expect("root");
    let database = Database::open_app_dir(&root).expect("database");
    let client = FakeClient::new(vec![ClientTorrent {
        info_hash: InfoHash::new("cccccccccccccccccccccccccccccccccccccccc")
            .expect("hash")
            .into_owned(),
        name: Cow::Borrowed("Streaming.Show.S01E01"),
        files: vec![File::new("Streaming.Show.S01E01.mkv", 42)],
        save_path: Cow::Borrowed("/downloads"),
        category: None,
        tags: Vec::new(),
        trackers: Vec::new(),
        complete: true,
        checking: false,
    }]);
    let config = RuntimeConfig::normalize(
        RawConfig {
            use_client_torrents: Some(true),
            torrent_clients: vec![
                crate::config::TorrentClientConfig::parse("qbittorrent:http://localhost:8080")
                    .expect("client"),
            ],
            ..RawConfig::default()
        },
        &root,
    )
    .expect("config");
    let clients: [&dyn TorrentClient; 1] = [&client];

    refresh_workflow_client_searchees(&database, &config, &clients).expect("refresh");

    let rows = database.client_searchee_rows().expect("rows");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].info_hash,
        "cccccccccccccccccccccccccccccccccccccccc"
    );
    let _cleanup = fs::remove_dir_all(root);
}

fn insert_decision(
    database: &Database,
    searchee_id: i64,
    guid: &str,
    info_hash: Option<&str>,
    last_seen: i64,
) {
    database
        .upsert_decision(&DecisionRecord {
            searchee_id,
            guid,
            info_hash,
            decision: Decision::Match,
            first_seen: last_seen,
            last_seen,
            fuzzy_size_factor: 0.05,
        })
        .expect("decision");
}

#[test]
fn injection_options_uses_configured_category_and_tags() {
    let root = temp_path("injection-label-config");
    let config = RuntimeConfig::normalize(
        RawConfig {
            injection_category: Some("managed".to_owned()),
            injection_tags: vec!["managed".to_owned(), "4k".to_owned()],
            ..RawConfig::default()
        },
        &root,
    )
    .expect("config");
    let clients: [&dyn TorrentClient; 0] = [];

    let options = injection_options(&config, &clients);

    assert_eq!(
        options.category.as_ref().map(ClientLabel::as_str),
        Some("managed")
    );
    assert_eq!(
        options
            .tags
            .iter()
            .map(ClientLabel::as_str)
            .collect::<Vec<_>>(),
        vec!["managed", "4k"]
    );
}

fn insert_announce_work(
    database: &Database,
    work_id: &str,
    dedupe_key: &str,
    status: &str,
    updated_at: i64,
) {
    database
        .execute_sql(
            "INSERT INTO announce_work
                (work_id, dedupe_key, name, guid, link, tracker, cookie, status,
                 attempts, created_at, updated_at, next_attempt_at, expires_at)
             VALUES (?1, ?2, 'Release', ?2, ?2, 'tracker', NULL, ?3,
                 0, ?4, ?4, ?4, ?5)",
            &[
                SqlValue::Text(Cow::Borrowed(work_id)),
                SqlValue::Text(Cow::Borrowed(dedupe_key)),
                SqlValue::Text(Cow::Borrowed(status)),
                SqlValue::I64(updated_at),
                SqlValue::I64(updated_at.saturating_add(86_400_000)),
            ],
        )
        .expect("announce work");
}

fn temp_path(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!("sporos-ops-{label}-{}-{nanos}", std::process::id()))
}

struct FakeClient {
    metadata: TorrentClientMetadata<'static>,
    torrents: Vec<ClientTorrent<'static>>,
}

impl FakeClient {
    fn new(torrents: Vec<ClientTorrent<'static>>) -> Self {
        Self {
            metadata: TorrentClientMetadata::new(
                "localhost",
                0,
                TorrentClientKind::QBittorrent,
                false,
                "fake",
            ),
            torrents,
        }
    }
}

impl TorrentClient for FakeClient {
    fn metadata(&self) -> &TorrentClientMetadata<'_> {
        &self.metadata
    }

    fn is_torrent_in_client(&self, _info_hash: &InfoHash<'_>) -> crate::Result<bool> {
        Ok(false)
    }

    fn is_torrent_complete(&self, _info_hash: &InfoHash<'_>) -> crate::Result<bool> {
        Ok(false)
    }

    fn is_torrent_checking(&self, _info_hash: &InfoHash<'_>) -> crate::Result<bool> {
        Ok(false)
    }

    fn get_all_torrents(&self) -> crate::Result<Vec<ClientTorrent<'static>>> {
        Err(super::operation_error(
            "operation workflows must use the streaming torrent visitor",
        ))
    }

    fn for_each_torrent(
        &self,
        visitor: &mut dyn FnMut(ClientTorrent<'static>) -> crate::Result<()>,
    ) -> crate::Result<()> {
        for torrent in self.torrents.iter().cloned() {
            visitor(torrent)?;
        }
        Ok(())
    }

    fn get_download_dir(
        &self,
        _metafile: &Metafile<'_>,
        _options: DownloadDirOptions,
    ) -> crate::Result<Result<PathBuf, ClientErrorCode>> {
        Ok(Err(ClientErrorCode::NotFound))
    }

    fn get_all_download_dirs(&self) -> crate::Result<BTreeMap<String, PathBuf>> {
        Ok(BTreeMap::new())
    }

    fn inject(
        &self,
        _new_torrent: &NewTorrent<'_>,
        _searchee: &Searchee<'_>,
        _decision: Decision,
        _options: &InjectionOptions,
    ) -> crate::Result<InjectionResult> {
        Ok(InjectionResult::Injected)
    }

    fn recheck_torrent(&self, _info_hash: &InfoHash<'_>) -> crate::Result<()> {
        Ok(())
    }

    fn resume_injection(
        &self,
        _metafile: &Metafile<'_>,
        _decision: Decision,
        _options: ResumeOptions,
    ) -> crate::Result<()> {
        Ok(())
    }

    fn validate_config(&self) -> crate::Result<()> {
        Ok(())
    }
}
