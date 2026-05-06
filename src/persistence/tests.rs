use super::{
    AnnounceWorkFinish, AnnounceWorkInsert, AnnounceWorkRetry, AnnounceWorkTerminalStatus,
    AsyncDatabase, ClientSearcheeRecord, DataRootRecord, Database, DecisionRecord,
    EndpointBreakerFailure, EnsembleRecord, ReverseLookupCriteria, SqlValue, bind_values,
    decision_guid_alias_lookup_sql, ensemble_client_sql, ensemble_data_sql,
    reverse_lookup_client_sql, reverse_lookup_data_sql, reverse_lookup_params, sqlx_error,
};
use crate::domain::{ClientLabel, Decision, File, LookupFields, MediaType};
use sqlx::Row;
use std::{
    borrow::Cow,
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

#[test]
fn initializes_schema_with_wal_and_documented_tables() {
    let root = temp_path("schema");
    fs::create_dir_all(&root).expect("temp dir");
    let database = Database::open_app_dir(&root).expect("database");

    let journal_mode: String = database
        .query_scalar("PRAGMA journal_mode", &[])
        .expect("journal mode");
    assert_eq!(journal_mode.to_ascii_lowercase(), "wal");
    let user_version: i64 = database
        .query_scalar("PRAGMA user_version", &[])
        .expect("schema version");
    assert_eq!(user_version, super::SCHEMA_VERSION);

    for table in [
        "searchee",
        "decision",
        "decision_guid_alias",
        "torrent",
        "job_log",
        "indexer",
        "indexer_tracker",
        "endpoint_breaker",
        "timestamp",
        "settings",
        "rss",
        "announce_work",
        "current_data_roots",
        "current_client_info_hashes",
        "current_client_ensemble_paths",
        "current_indexer_urls",
        "current_torrent_dir",
        "client_searchee",
        "data",
        "data_ensemble",
        "client_ensemble",
    ] {
        let count: i64 = database
            .query_scalar(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
                &[SqlValue::Text(Cow::Borrowed(table))],
            )
            .expect("table query");
        assert_eq!(count, 1, "{table}");
    }

    for column in [
        "search_key",
        "media_type",
        "season",
        "episode",
        "file_count",
        "video_bytes",
        "non_video_bytes",
    ] {
        assert_column(&database, "client_searchee", column);
    }
    for column in [
        "search_key",
        "media_type",
        "season",
        "episode",
        "length",
        "file_count",
        "video_bytes",
        "non_video_bytes",
    ] {
        assert_column(&database, "data", column);
    }
    for column in ["first_searched", "last_searched"] {
        assert_no_column(&database, "searchee", column);
    }
    for index in [
        "idx_client_searchee_lookup",
        "idx_decision_guid_alias_lookup",
        "idx_indexer_tracker_lookup",
        "idx_data_lookup",
        "idx_data_ensemble_root",
        "idx_data_ensemble_lookup",
        "idx_client_ensemble_lookup",
        "idx_announce_work_active_dedupe",
        "idx_announce_work_ready",
        "idx_announce_work_running_lease",
        "idx_announce_work_expiry",
        "idx_announce_work_status",
    ] {
        assert_index(&database, index);
    }

    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn schema_constraints_reject_invalid_cache_invariants() {
    let root = temp_path("schema-constraints");
    fs::create_dir_all(&root).expect("temp dir");
    let database = Database::open_app_dir(&root).expect("database");

    database
        .execute_sql("INSERT INTO searchee (name) VALUES ('Example')", &[])
        .expect("insert searchee");

    assert_sql_fails(
        &database,
        "INSERT INTO decision
             (searchee_id, guid, info_hash, decision, first_seen, last_seen, fuzzy_size_factor)
             VALUES (1, 'guid', NULL, 'UNKNOWN', 0, 0, 1.0)",
    );
    assert_sql_fails(
        &database,
        "INSERT INTO decision
             (searchee_id, guid, info_hash, decision, first_seen, last_seen, fuzzy_size_factor)
             VALUES (1, 'guid', NULL, 'MATCH', 20, 10, 1.0)",
    );
    assert_sql_fails(
        &database,
        "INSERT INTO indexer (url, apikey, active)
             VALUES ('http://indexer.test', 'secret', 2)",
    );
    assert_sql_fails(
        &database,
        "INSERT INTO job_log (name, last_run) VALUES ('rss', -1)",
    );

    database
        .execute_sql(
            "INSERT INTO indexer (url, apikey, active)
                 VALUES ('http://indexer.test', 'secret', 1)",
            &[],
        )
        .expect("insert indexer");
    assert_sql_fails(
        &database,
        "INSERT INTO timestamp
             (searchee_id, indexer_id, first_searched, last_searched)
             VALUES (1, 1, 10, 9)",
    );
    assert_sql_fails(
        &database,
        "INSERT INTO data
             (path, title, search_key, media_type, length)
             VALUES ('/data/show', 'Show', 'show', 'series', 1)",
    );
    assert_sql_fails(
        &database,
        "INSERT INTO client_searchee
             (client_host, info_hash, name, title, files, length, save_path, trackers, video_bytes)
             VALUES ('http://client', 'abc', 'Show', 'Show', '[]', 1, '/downloads', '[]', -1)",
    );
    assert_sql_fails(
        &database,
        "INSERT INTO settings (id, apikey) VALUES (1, 'secret')",
    );
    assert_sql_fails(
        &database,
        "INSERT INTO announce_work
             (work_id, dedupe_key, name, guid, link, tracker, status,
              attempts, created_at, updated_at, next_attempt_at, expires_at)
             VALUES ('work', 'dedupe', 'Name', 'guid', 'link', 'tracker',
              'running', 0, 10, 10, 10, 20)",
    );
    assert_sql_fails(
        &database,
        "INSERT INTO announce_work
             (work_id, dedupe_key, name, guid, link, tracker, status,
              attempts, created_at, updated_at, next_attempt_at, expires_at)
             VALUES ('work', 'dedupe', 'Name', 'guid', 'link', 'tracker',
              'queued', -1, 10, 10, 10, 20)",
    );

    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn announce_work_insert_dedupes_and_survives_reopen() {
    let root = temp_path("announce-work-insert");
    fs::create_dir_all(&root).expect("temp dir");
    let path = Database::path_for_app_dir(&root);
    let database = Database::open(&path).expect("database");

    let inserted = database
        .insert_or_dedupe_announce_work(&announce_insert("work-1", "dedupe-1", 1_000, 11_000))
        .expect("insert");
    assert!(inserted.inserted);
    assert_eq!(inserted.work.work_id, "work-1");
    assert_eq!(inserted.work.status, "queued");
    assert_eq!(inserted.work.cookie.as_deref(), Some("uid=1"));

    let deduped = database
        .insert_or_dedupe_announce_work(&AnnounceWorkInsert {
            work_id: "work-2",
            dedupe_key: "dedupe-1",
            name: "Other",
            guid: "https://tracker.example/other",
            link: "https://tracker.example/other",
            tracker: "tracker",
            cookie: None,
            now: 2_000,
            expires_at: 12_000,
        })
        .expect("dedupe");
    assert!(!deduped.inserted);
    assert_eq!(deduped.work.work_id, "work-1");

    drop(database);
    let reopened = Database::open(&path).expect("reopened");
    let claimed = reopened
        .claim_announce_work(1_000, "worker-a", 5_000, 10)
        .expect("claim");
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].work_id, "work-1");
    assert_eq!(claimed[0].attempts, 1);
    assert_eq!(claimed[0].lease_owner.as_deref(), Some("worker-a"));

    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn announce_work_claims_ready_rows_in_order() {
    let root = temp_path("announce-work-claim");
    fs::create_dir_all(&root).expect("temp dir");
    let database = Database::open_app_dir(&root).expect("database");

    database
        .insert_or_dedupe_announce_work(&announce_insert(
            "work-later",
            "dedupe-later",
            5_000,
            20_000,
        ))
        .expect("insert later");
    database
        .insert_or_dedupe_announce_work(&announce_insert("work-now", "dedupe-now", 1_000, 20_000))
        .expect("insert now");

    let claimed = database
        .claim_announce_work(2_000, "worker-a", 3_000, 10)
        .expect("claim");
    assert_eq!(
        claimed
            .iter()
            .map(|row| row.work_id.as_str())
            .collect::<Vec<_>>(),
        vec!["work-now"]
    );
    assert_eq!(claimed[0].status, "running");
    assert_eq!(claimed[0].lease_expires_at, Some(5_000));

    let released = database
        .release_stale_announce_leases(5_000, 6_000, 10)
        .expect("release lease");
    assert_eq!(released.len(), 1);
    assert_eq!(released[0].status, "retrying");
    assert_eq!(released[0].next_attempt_at, 6_000);

    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn announce_work_transitions_require_active_lease_owner() {
    let root = temp_path("announce-work-lease-fence");
    fs::create_dir_all(&root).expect("temp dir");
    let database = Database::open_app_dir(&root).expect("database");

    database
        .insert_or_dedupe_announce_work(&announce_insert("work", "dedupe", 1_000, 20_000))
        .expect("insert");
    let first = database
        .claim_announce_work(1_000, "worker-a", 1_000, 1)
        .expect("first claim");
    assert_eq!(first.len(), 1);
    let released = database
        .release_stale_announce_leases(2_000, 2_000, 1)
        .expect("release first lease");
    assert_eq!(released.len(), 1);
    let second = database
        .claim_announce_work(2_000, "worker-b", 5_000, 1)
        .expect("second claim");
    assert_eq!(second.len(), 1);

    let stale_retry = database
        .schedule_announce_retry(&AnnounceWorkRetry {
            work_id: &first[0].work_id,
            lease_owner: first[0].lease_owner.as_deref().expect("first lease owner"),
            now: 2_100,
            next_attempt_at: 3_000,
            error_class: Some("stale"),
            error_message: Some("stale retry"),
            outcome_context: Some("stale_worker"),
        })
        .expect("stale retry");
    assert!(!stale_retry);
    let stale_finish = database
        .finish_announce_work(&AnnounceWorkFinish {
            work_id: &first[0].work_id,
            lease_owner: first[0].lease_owner.as_deref().expect("first lease owner"),
            now: 2_200,
            status: AnnounceWorkTerminalStatus::Succeeded,
            error_class: None,
            error_message: None,
            outcome_context: Some("stale_worker"),
        })
        .expect("stale finish");
    assert!(!stale_finish);

    let active_finish = database
        .finish_announce_work(&AnnounceWorkFinish {
            work_id: &second[0].work_id,
            lease_owner: second[0]
                .lease_owner
                .as_deref()
                .expect("second lease owner"),
            now: 2_300,
            status: AnnounceWorkTerminalStatus::Succeeded,
            error_class: None,
            error_message: None,
            outcome_context: Some("active_worker"),
        })
        .expect("active finish");
    assert!(active_finish);

    let stats = database.announce_queue_stats(2_400).expect("stats");
    assert_eq!(stats.succeeded, 1);
    assert_eq!(stats.retry_scheduled, 0);

    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn announce_work_updates_terminal_expiry_and_stats() {
    let root = temp_path("announce-work-stats");
    fs::create_dir_all(&root).expect("temp dir");
    let database = Database::open_app_dir(&root).expect("database");

    database
        .insert_or_dedupe_announce_work(&announce_insert("work-ok", "dedupe-ok", 1_000, 20_000))
        .expect("insert ok");
    database
        .insert_or_dedupe_announce_work(&announce_insert("work-fail", "dedupe-fail", 1_000, 20_000))
        .expect("insert fail");
    database
        .insert_or_dedupe_announce_work(&announce_insert(
            "work-expire",
            "dedupe-expire",
            1_000,
            2_000,
        ))
        .expect("insert expire");

    let claimed = database
        .claim_announce_work(1_500, "worker-a", 5_000, 3)
        .expect("claim");
    assert_eq!(claimed.len(), 3);
    database
        .finish_announce_work(&AnnounceWorkFinish {
            work_id: &claimed[0].work_id,
            lease_owner: claimed[0].lease_owner.as_deref().expect("lease owner"),
            now: 1_600,
            status: AnnounceWorkTerminalStatus::Succeeded,
            error_class: None,
            error_message: None,
            outcome_context: Some("matched"),
        })
        .expect("finish ok");
    database
        .schedule_announce_retry(&AnnounceWorkRetry {
            work_id: &claimed[1].work_id,
            lease_owner: claimed[1].lease_owner.as_deref().expect("lease owner"),
            now: 1_700,
            next_attempt_at: 1_800,
            error_class: Some("terminal"),
            error_message: Some("retry once"),
            outcome_context: Some("retry_scheduled"),
        })
        .expect("retry failed");
    let retried = database
        .claim_announce_work(1_800, "worker-a", 5_000, 1)
        .expect("claim retry");
    assert_eq!(retried.len(), 1);
    assert_eq!(retried[0].attempts, 2);
    database
        .finish_announce_work(&AnnounceWorkFinish {
            work_id: &retried[0].work_id,
            lease_owner: retried[0].lease_owner.as_deref().expect("lease owner"),
            now: 1_900,
            status: AnnounceWorkTerminalStatus::TerminalFailed,
            error_class: Some("terminal"),
            error_message: Some("not matchable"),
            outcome_context: Some("file_tree_mismatch"),
        })
        .expect("finish failed");

    let expired = database.expire_announce_work(2_000, 10).expect("expire");
    assert_eq!(expired.len(), 1);
    assert_eq!(expired[0].status, "expired");

    let stats = database.announce_queue_stats(2_500).expect("stats");
    assert_eq!(stats.backlog, 0);
    assert_eq!(stats.running, 0);
    assert_eq!(stats.succeeded, 1);
    assert_eq!(stats.terminal_failed, 1);
    assert_eq!(stats.expired, 1);
    assert_eq!(stats.total_attempts, 4);
    assert_eq!(stats.retry_scheduled, 0);
    assert_eq!(stats.last_error_class.as_deref(), Some("terminal"));

    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn endpoint_breakers_open_half_open_and_close() {
    let root = temp_path("endpoint-breakers");
    fs::create_dir_all(&root).expect("temp dir");
    let database = Database::open_app_dir(&root).expect("database");

    let first = database
        .record_endpoint_breaker_failure(&EndpointBreakerFailure {
            endpoint_key: "https://indexer.example/api",
            operation: "torznab_search",
            now: 1_000,
            retry_after: None,
            error_class: "http_503",
            error_message: Some("service unavailable"),
        })
        .expect("first failure");
    assert_eq!(first.state, "closed");
    assert_eq!(first.failure_count, 1);
    assert!(
        database
            .open_endpoint_breaker("https://indexer.example/api", "torznab_search", 1_000)
            .expect("closed breaker")
            .is_none()
    );

    let second = database
        .record_endpoint_breaker_failure(&EndpointBreakerFailure {
            endpoint_key: "https://indexer.example/api",
            operation: "torznab_search",
            now: 2_000,
            retry_after: Some(7_000),
            error_class: "http_429",
            error_message: Some("rate limited"),
        })
        .expect("second failure");
    assert_eq!(second.state, "open");
    assert_eq!(second.failure_count, 2);
    assert_eq!(second.retry_after, Some(7_000));
    assert!(
        database
            .open_endpoint_breaker("https://indexer.example/api", "torznab_search", 3_000)
            .expect("open breaker")
            .is_some()
    );

    let open_stats = database.endpoint_breaker_stats(3_000).expect("open stats");
    assert_eq!(open_stats.open, 1);
    assert_eq!(open_stats.half_open, 0);
    assert_eq!(open_stats.next_retry_at, Some(7_000));
    assert_eq!(open_stats.last_error_class.as_deref(), Some("http_429"));

    let half_open = database
        .endpoint_breaker_stats(7_000)
        .expect("half-open stats");
    assert_eq!(half_open.open, 0);
    assert_eq!(half_open.half_open, 1);
    assert!(
        database
            .open_endpoint_breaker("https://indexer.example/api", "torznab_search", 7_000)
            .expect("half-open probe")
            .is_none()
    );

    database
        .close_endpoint_breaker("https://indexer.example/api", "torznab_search", 8_000)
        .expect("close breaker");
    let closed = database
        .endpoint_breaker_stats(8_000)
        .expect("closed stats");
    assert_eq!(closed.open, 0);
    assert_eq!(closed.half_open, 0);

    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn refresh_staging_tables_are_connection_independent() {
    let root = temp_path("refresh-staging");
    fs::create_dir_all(&root).expect("temp dir");
    let first = Database::open_app_dir(&root).expect("database");
    let second = Database::open(Database::path_for_app_dir(&root)).expect("second database");

    first.begin_data_root_refresh().expect("begin data");
    second
        .mark_refreshed_data_root("/media")
        .expect("mark data");
    assert_eq!(first.finish_data_root_refresh().expect("finish data"), 0);

    first.begin_client_searchee_refresh().expect("begin client");
    second
        .mark_refreshed_client_info_hash("abcdef")
        .expect("mark client hash");
    second
        .mark_refreshed_client_ensemble_path("/downloads/show")
        .expect("mark client path");
    assert_eq!(
        first
            .finish_client_searchee_refresh("http://client")
            .expect("finish client"),
        0
    );

    first.begin_torrent_dir_refresh().expect("begin torrent");
    second
        .mark_refreshed_torrent_path("/torrents/example.torrent")
        .expect("mark torrent");
    assert_eq!(
        first.finish_torrent_dir_refresh().expect("finish torrent"),
        0
    );

    first
        .sync_indexers([("http://indexer.test/torznab", "secret")])
        .expect("sync indexers");
    let temp_tables: i64 = first
        .query_scalar(
            "SELECT COUNT(*) FROM sqlite_temp_master
                 WHERE name IN (
                    'current_data_roots',
                    'current_client_info_hashes',
                    'current_client_ensemble_paths',
                    'current_indexer_urls',
                    'current_torrent_dir'
                 )",
            &[],
        )
        .expect("temp table query");
    assert_eq!(temp_tables, 0);

    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn lookup_query_plans_use_indexes() {
    let root = temp_path("lookup-indexes");
    fs::create_dir_all(&root).expect("temp dir");
    let database = Database::open_app_dir(&root).expect("database");
    assert_index_columns(
        &database,
        "idx_client_searchee_lookup",
        &["search_key", "media_type", "season", "episode", "length"],
    );
    assert_index_columns(
        &database,
        "idx_data_lookup",
        &["search_key", "media_type", "season", "episode", "length"],
    );

    let keys = vec!["example.show.s01e01".to_owned(), "example.show".to_owned()];
    for criteria in [
        ReverseLookupCriteria {
            search_keys: &keys,
            media_type: Some("episode"),
            season: Some(1),
            episode: Some(1),
            min_length: Some(1),
            max_length: Some(100),
        },
        ReverseLookupCriteria {
            search_keys: &keys,
            media_type: Some("episode"),
            season: Some(1),
            episode: None,
            min_length: None,
            max_length: None,
        },
        ReverseLookupCriteria {
            search_keys: &keys,
            media_type: None,
            season: None,
            episode: None,
            min_length: None,
            max_length: None,
        },
        ReverseLookupCriteria {
            search_keys: &keys,
            media_type: None,
            season: None,
            episode: None,
            min_length: Some(1),
            max_length: Some(100),
        },
    ] {
        let params = reverse_lookup_params(&criteria, 0, 100);
        let client_plan = explain_detail(
            &database,
            &format!(
                "EXPLAIN QUERY PLAN {}",
                reverse_lookup_client_sql(keys.len())
            ),
            &params,
        );
        let data_plan = explain_detail(
            &database,
            &format!("EXPLAIN QUERY PLAN {}", reverse_lookup_data_sql(keys.len())),
            &params,
        );

        assert!(
            client_plan.contains("idx_client_searchee_lookup"),
            "{client_plan}"
        );
        assert!(client_plan.contains("search_key=?"), "{client_plan}");
        assert!(data_plan.contains("idx_data_lookup"), "{data_plan}");
        assert!(data_plan.contains("search_key=?"), "{data_plan}");
    }
    for has_element in [true, false] {
        let params = if has_element {
            vec![
                SqlValue::Text(Cow::Borrowed("example show s01")),
                SqlValue::Text(Cow::Borrowed("01")),
            ]
        } else {
            vec![SqlValue::Text(Cow::Borrowed("example show s01"))]
        };
        let data_ensemble_plan = explain_detail(
            &database,
            &format!("EXPLAIN QUERY PLAN {}", ensemble_data_sql(has_element)),
            &params,
        );
        let client_ensemble_plan = explain_detail(
            &database,
            &format!("EXPLAIN QUERY PLAN {}", ensemble_client_sql(has_element)),
            &params,
        );

        assert!(
            data_ensemble_plan.contains("idx_data_ensemble_lookup"),
            "{data_ensemble_plan}"
        );
        assert!(
            client_ensemble_plan.contains("idx_client_ensemble_lookup"),
            "{client_ensemble_plan}"
        );
        if has_element {
            assert!(
                data_ensemble_plan.contains("ensemble=? AND element=?"),
                "{data_ensemble_plan}"
            );
            assert!(
                client_ensemble_plan.contains("ensemble=? AND element=?"),
                "{client_ensemble_plan}"
            );
        }
    }

    let _cleanup = fs::remove_dir_all(root);
}

#[tokio::test]
async fn async_database_opens_same_file_with_sqlx_pool() {
    let root = temp_path("async-boundary");
    fs::create_dir_all(&root).expect("temp dir");
    let expected_path = Database::path_for_app_dir(&root);

    let database = AsyncDatabase::open_app_dir(&root).await.expect("database");

    assert_eq!(database.path(), expected_path.as_path());
    let user_version: i64 = sqlx::query_scalar("PRAGMA user_version")
        .fetch_one(database.pool())
        .await
        .expect("schema version");
    assert_eq!(user_version, super::SCHEMA_VERSION);

    database.close().await;
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn upserts_searchee_decisions_and_pages_guid_map() {
    let root = temp_path("decision");
    fs::create_dir_all(&root).expect("temp dir");
    let database = Database::open_app_dir(&root).expect("database");
    let searchee_id = database
        .get_or_insert_searchee("Example Show S01")
        .expect("searchee");

    database
        .upsert_decision(&DecisionRecord {
            searchee_id,
            guid: "guid-1",
            info_hash: Some("0123456789abcdef0123456789abcdef01234567"),
            decision: Decision::Match,
            first_seen: 100,
            last_seen: 100,
            fuzzy_size_factor: 0.05,
        })
        .expect("decision");
    database
        .upsert_decision(&DecisionRecord {
            searchee_id,
            guid: "guid-1",
            info_hash: Some("fedcba9876543210fedcba9876543210fedcba98"),
            decision: Decision::MatchSizeOnly,
            first_seen: 100,
            last_seen: 200,
            fuzzy_size_factor: 0.1,
        })
        .expect("decision update");
    database
        .upsert_decision(&DecisionRecord {
            searchee_id,
            guid: "https://tracker.tv/torrent/123/group",
            info_hash: Some("abcdef0123456789abcdef0123456789abcdef01"),
            decision: Decision::Match,
            first_seen: 100,
            last_seen: 300,
            fuzzy_size_factor: 0.05,
        })
        .expect("alias decision");

    let page = database.guid_info_hash_page(0, 10).expect("page");

    assert_eq!(page.len(), 2);
    assert_eq!(page[0].guid, "guid-1");
    assert_eq!(
        page[0].info_hash,
        "fedcba9876543210fedcba9876543210fedcba98"
    );
    assert_eq!(
        database
            .decision_info_hash_by_tracker_id("123")
            .expect("alias")
            .as_deref(),
        Some("abcdef0123456789abcdef0123456789abcdef01")
    );
    let alias_plan = explain_detail(
        &database,
        &format!("EXPLAIN QUERY PLAN {}", decision_guid_alias_lookup_sql()),
        &[SqlValue::Text(Cow::Borrowed("torrent:123"))],
    );
    assert!(
        alias_plan.contains("idx_decision_guid_alias_lookup"),
        "{alias_plan}"
    );

    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn stores_generated_api_key_in_settings_row_zero() {
    let root = temp_path("settings");
    fs::create_dir_all(&root).expect("temp dir");
    let database = Database::open_app_dir(&root).expect("database");

    assert_eq!(database.get_api_key().expect("api key"), None);
    database
        .set_api_key("0123456789abcdef0123456789abcdef0123456789abcdef")
        .expect("set api key");

    assert_eq!(
        database.get_api_key().expect("api key"),
        Some("0123456789abcdef0123456789abcdef0123456789abcdef".to_owned())
    );

    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn mirrors_indexer_trackers_to_child_rows() {
    let root = temp_path("indexer-trackers");
    fs::create_dir_all(&root).expect("temp dir");
    let database = Database::open_app_dir(&root).expect("database");
    database
        .execute_sql(
            "INSERT INTO indexer (name, url, apikey, active)
                 VALUES ('TrackerName', 'https://indexer.example/api', 'secret', 1)",
            &[],
        )
        .expect("indexer");
    let indexer_id = database
        .indexer_id("https://indexer.example/api")
        .expect("indexer id");

    database
        .update_indexer_trackers_json(indexer_id, r#"["tracker.b","tracker.a","tracker.a"]"#)
        .expect("trackers");

    let child_count: i64 = database
        .query_scalar("SELECT COUNT(*) FROM indexer_tracker", &[])
        .expect("child count");
    assert_eq!(child_count, 2);
    let rows = database.indexer_tracker_rows().expect("tracker rows");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].name, "TrackerName");
    assert_eq!(rows[0].trackers, r#"["tracker.a","tracker.b"]"#);

    database
        .update_indexer_trackers_json(indexer_id, r#"["tracker.c","tracker.a"]"#)
        .expect("merged trackers");

    let child_count: i64 = database
        .query_scalar("SELECT COUNT(*) FROM indexer_tracker", &[])
        .expect("child count");
    assert_eq!(child_count, 3);
    let stored_json: String = database
        .query_scalar(
            "SELECT trackers FROM indexer WHERE id = ?1",
            &[SqlValue::I64(indexer_id)],
        )
        .expect("stored trackers");
    assert_eq!(stored_json, r#"["tracker.a","tracker.b","tracker.c"]"#);
    let rows = database.indexer_tracker_rows().expect("tracker rows");
    assert_eq!(rows[0].trackers, r#"["tracker.a","tracker.b","tracker.c"]"#);

    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn refreshes_data_roots_and_prunes_missing_rows() {
    let root = temp_path("data-roots");
    fs::create_dir_all(&root).expect("temp dir");
    let database = Database::open_app_dir(&root).expect("database");
    database
        .refresh_data_roots([
            DataRootRecord {
                path: "/data/one",
                title: "One",
                lookup: None,
            },
            DataRootRecord {
                path: "/data/two",
                title: "Two",
                lookup: None,
            },
        ])
        .expect("refresh");
    database
        .upsert_ensemble(&EnsembleRecord {
            client_host: None,
            path: "/data/two/file.mkv",
            info_hash: None,
            ensemble: "show s01",
            element: "1",
        })
        .expect("ensemble");

    let removed = database
        .refresh_data_roots([DataRootRecord {
            path: "/data/one",
            title: "One Updated",
            lookup: None,
        }])
        .expect("refresh");

    assert_eq!(removed, 1);
    let title: String = database
        .query_scalar("SELECT title FROM data WHERE path = '/data/one'", &[])
        .expect("title");
    assert_eq!(title, "One Updated");
    let ensemble_count: i64 = database
        .query_scalar("SELECT COUNT(*) FROM data_ensemble", &[])
        .expect("ensemble count");
    assert_eq!(ensemble_count, 0);
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn upserts_data_dir_ensemble_rows_with_data_source() {
    let root = temp_path("ensemble-data-source");
    fs::create_dir_all(&root).expect("temp dir");
    let database = Database::open_app_dir(&root).expect("database");
    database
        .refresh_data_roots([DataRootRecord {
            path: "/data/show",
            title: "Show",
            lookup: None,
        }])
        .expect("data root");

    database
        .upsert_ensemble(&EnsembleRecord {
            client_host: None,
            path: "/data/show/file.mkv",
            info_hash: None,
            ensemble: "old show s01",
            element: "1",
        })
        .expect("ensemble");
    database
        .upsert_ensemble(&EnsembleRecord {
            client_host: None,
            path: "/data/show/file.mkv",
            info_hash: Some("0123456789abcdef0123456789abcdef01234567"),
            ensemble: "new show s01",
            element: "2",
        })
        .expect("ensemble update");

    let row: (i64, Option<String>, String, String) = database
        .query_row(
            "SELECT COUNT(*), info_hash, ensemble, element FROM data_ensemble",
            &[],
            |row| (row.get(0), row.get(1), row.get(2), row.get(3)),
        )
        .expect("ensemble row");
    assert_eq!(
        row,
        (
            1,
            Some("0123456789abcdef0123456789abcdef01234567".to_owned()),
            "new show s01".to_owned(),
            "2".to_owned()
        )
    );

    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn reverse_lookup_selectors_page_compact_rows() {
    let root = temp_path("reverse-selectors");
    fs::create_dir_all(&root).expect("temp dir");
    let database = Database::open_app_dir(&root).expect("database");
    let files = [File::new("Example.Show.S01E01.mkv", 10)];
    let lookup = lookup_fields("example.show.s01e01", Some(1), Some(1), 10);
    let other_lookup = lookup_fields("other.show.s01e01", Some(1), Some(1), 10);
    for (info_hash, title, lookup) in [
        (
            "0123456789abcdef0123456789abcdef01234567",
            "Example Show S01E01",
            &lookup,
        ),
        (
            "fedcba9876543210fedcba9876543210fedcba98",
            "Example Show S01E01",
            &lookup,
        ),
        (
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "Other Show S01E01",
            &other_lookup,
        ),
    ] {
        database
            .upsert_client_searchee(&ClientSearcheeRecord {
                client_host: "client",
                info_hash,
                name: title,
                title,
                files: &files,
                length: 10,
                save_path: "/downloads",
                category: None,
                tags: &[],
                trackers: &[],
                lookup: Some(lookup),
            })
            .expect("client row");
    }
    for (path, title, lookup) in [
        ("/data/example-a", "Example Show S01E01", &lookup),
        ("/data/example-b", "Example Show S01E01", &lookup),
        ("/data/other", "Other Show S01E01", &other_lookup),
    ] {
        database
            .upsert_data_root(&DataRootRecord {
                path,
                title,
                lookup: Some(lookup),
            })
            .expect("data row");
    }
    let keys = vec!["example.show.s01e01".to_owned()];
    let criteria = ReverseLookupCriteria {
        search_keys: &keys,
        media_type: Some("episode"),
        season: Some(1),
        episode: Some(1),
        min_length: Some(1),
        max_length: Some(100),
    };

    let first_client = database
        .reverse_lookup_client_page(&criteria, 0, 1)
        .expect("client page");
    assert_eq!(first_client.len(), 1);
    assert_eq!(first_client[0].title, "Example Show S01E01");
    let second_client = database
        .reverse_lookup_client_page(&criteria, first_client[0].rowid, 10)
        .expect("client page");
    assert_eq!(second_client.len(), 1);
    assert_eq!(
        second_client[0].info_hash,
        "fedcba9876543210fedcba9876543210fedcba98"
    );

    let first_data = database
        .reverse_lookup_data_page(&criteria, 0, 1)
        .expect("data page");
    assert_eq!(first_data.len(), 1);
    assert_eq!(first_data[0].path, "/data/example-a");
    let second_data = database
        .reverse_lookup_data_page(&criteria, first_data[0].rowid, 10)
        .expect("data page");
    assert_eq!(second_data.len(), 1);
    assert_eq!(second_data[0].path, "/data/example-b");

    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn reverse_lookup_selectors_include_unindexed_rows() {
    let root = temp_path("reverse-selector-stale");
    fs::create_dir_all(&root).expect("temp dir");
    let database = Database::open_app_dir(&root).expect("database");
    let files = [File::new("Stale.Show.S01E01.mkv", 10)];
    database
        .upsert_client_searchee(&ClientSearcheeRecord {
            client_host: "client",
            info_hash: "0123456789abcdef0123456789abcdef01234567",
            name: "Stale Show S01E01",
            title: "Stale Show S01E01",
            files: &files,
            length: 10,
            save_path: "/downloads",
            category: None,
            tags: &[],
            trackers: &[],
            lookup: None,
        })
        .expect("client row");
    database
        .upsert_data_root(&DataRootRecord {
            path: "/data/stale",
            title: "Stale Show S01E01",
            lookup: None,
        })
        .expect("data row");
    let keys = vec!["example.show.s01e01".to_owned()];
    let criteria = ReverseLookupCriteria {
        search_keys: &keys,
        media_type: Some("episode"),
        season: Some(1),
        episode: Some(1),
        min_length: Some(1),
        max_length: Some(100),
    };

    let client_rows = database
        .reverse_lookup_client_page(&criteria, 0, 10)
        .expect("client page");
    let data_rows = database
        .reverse_lookup_data_page(&criteria, 0, 10)
        .expect("data page");

    assert_eq!(client_rows.len(), 1);
    assert_eq!(client_rows[0].title, "Stale Show S01E01");
    assert_eq!(data_rows.len(), 1);
    assert_eq!(data_rows[0].path, "/data/stale");

    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn stores_client_searchee_json_and_prunes_by_host() {
    let root = temp_path("client-searchees");
    fs::create_dir_all(&root).expect("temp dir");
    let database = Database::open_app_dir(&root).expect("database");
    let files = [File::new("Release/file.mkv", 42)];
    let tags = [ClientLabel::new("tag")];
    let trackers = [Cow::Borrowed("tracker.example")];
    database
        .refresh_client_searchees(
            "client",
            [ClientSearcheeRecord {
                client_host: "client",
                info_hash: "0123456789abcdef0123456789abcdef01234567",
                name: "Release",
                title: "Release",
                files: &files,
                length: 42,
                save_path: "/downloads",
                category: Some("tv"),
                tags: &tags,
                trackers: &trackers,
                lookup: None,
            }],
        )
        .expect("refresh");
    database
        .upsert_ensemble(&EnsembleRecord {
            client_host: Some("client"),
            path: "/downloads/file.mkv",
            info_hash: Some("0123456789abcdef0123456789abcdef01234567"),
            ensemble: "release",
            element: "1",
        })
        .expect("ensemble");

    let json: String = database
        .query_scalar("SELECT files FROM client_searchee", &[])
        .expect("files json");
    assert!(json.contains("Release/file.mkv"));

    let removed = database
        .refresh_client_searchees("client", [])
        .expect("prune");

    assert_eq!(removed, 1);
    let ensemble_count: i64 = database
        .query_scalar("SELECT COUNT(*) FROM client_ensemble", &[])
        .expect("ensemble count");
    assert_eq!(ensemble_count, 0);
    let _cleanup = fs::remove_dir_all(root);
}

fn temp_path(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!("sporos-db-{label}-{nanos}"))
}

fn announce_insert<'a>(
    work_id: &'a str,
    dedupe_key: &'a str,
    now: i64,
    expires_at: i64,
) -> AnnounceWorkInsert<'a> {
    AnnounceWorkInsert {
        work_id,
        dedupe_key,
        name: "Release",
        guid: "https://tracker.example/release",
        link: "https://tracker.example/release",
        tracker: "tracker",
        cookie: Some("uid=1"),
        now,
        expires_at,
    }
}

fn assert_sql_fails(database: &Database, sql: &str) {
    assert!(database.execute_sql(sql, &[]).is_err(), "{sql}");
}

fn assert_column(database: &Database, table: &str, column: &str) {
    let sql = format!("SELECT COUNT(*) FROM pragma_table_info('{table}') WHERE name = ?1");
    let count: i64 = database
        .query_scalar(&sql, &[SqlValue::Text(Cow::Borrowed(column))])
        .expect("column query");
    assert_eq!(count, 1, "{table}.{column}");
}

fn assert_no_column(database: &Database, table: &str, column: &str) {
    let sql = format!("SELECT COUNT(*) FROM pragma_table_info('{table}') WHERE name = ?1");
    let count: i64 = database
        .query_scalar(&sql, &[SqlValue::Text(Cow::Borrowed(column))])
        .expect("column query");
    assert_eq!(count, 0, "{table}.{column}");
}

fn assert_index(database: &Database, index: &str) {
    let count: i64 = database
        .query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?1",
            &[SqlValue::Text(Cow::Borrowed(index))],
        )
        .expect("index query");
    assert_eq!(count, 1, "{index}");
}

fn assert_index_columns(database: &Database, index: &str, expected: &[&str]) {
    let columns = database
        .block_on(async {
            sqlx::query("SELECT name FROM pragma_index_info(?1) ORDER BY seqno")
                .bind(index)
                .fetch_all(database.pool())
                .await
                .map(|rows| {
                    rows.into_iter()
                        .map(|row| row.get::<String, _>(0))
                        .collect::<Vec<_>>()
                })
                .map_err(sqlx_error)
        })
        .expect("index column query");
    assert_eq!(columns, expected, "{index}");
}

fn explain_detail(database: &Database, sql: &str, params: &[SqlValue<'_>]) -> String {
    database
        .block_on(async {
            bind_values(sqlx::query(sql), params)
                .fetch_all(database.pool())
                .await
                .map(|rows| {
                    rows.into_iter()
                        .map(|row| row.get::<String, _>(3))
                        .collect::<Vec<_>>()
                        .join("\n")
                })
                .map_err(sqlx_error)
        })
        .expect("query plan")
}

fn lookup_fields(
    search_key: &str,
    season: Option<u32>,
    episode: Option<u32>,
    length: u64,
) -> LookupFields {
    LookupFields {
        search_key: search_key.to_owned(),
        media_type: MediaType::Episode,
        season,
        episode,
        length,
        file_count: 1,
        video_bytes: length,
        non_video_bytes: 0,
    }
}
