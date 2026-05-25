use super::*;
use crate::announce::{
    AnnounceDedupeIdentity, AnnounceFetchMaterial, AnnounceLease, AnnounceReason, AnnounceStatus,
    AnnounceWorkId, AnnounceWorkItem,
};
use crate::domain::{
    CandidateGuid, ClientHost, DecisionReason, DisplayName, DownloadUrl, FileIndex, IndexerId,
    ItemTitle, MatchRatio, ReasonText, SourceKey, TrackerName,
};
use crate::indexers::{
    ApiKeySource, ConfiguredTorznabIndexer, ProwlarrIndexer, SanitizedTorznabUrl,
    parse_torznab_caps,
};
use crate::persistence::schema::{BUSY_TIMEOUT_MS, REQUIRED_TABLES};
use crate::secrets::{ApiKey, CookieSecret};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use std::fs;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::schema_setup::rebuild_indexers_table;

#[tokio::test]
async fn file_backed_repository_initializes_schema_and_connection_pragmas() {
    let root = unique_temp_dir("sqlite-pragmas");
    let database = root.join("sporos.db");
    let repository = Repository::connect(&database).await.unwrap();

    let foreign_keys: i64 = sqlx::query_scalar("PRAGMA foreign_keys;")
        .fetch_one(repository.pool())
        .await
        .unwrap();
    let busy_timeout: i64 = sqlx::query_scalar("PRAGMA busy_timeout;")
        .fetch_one(repository.pool())
        .await
        .unwrap();
    let journal_mode: String = sqlx::query_scalar("PRAGMA journal_mode;")
        .fetch_one(repository.pool())
        .await
        .unwrap();

    assert_eq!(1, foreign_keys);
    assert_eq!(i64::from(BUSY_TIMEOUT_MS), busy_timeout);
    assert_eq!("wal", journal_mode);

    for table in REQUIRED_TABLES {
        let exists: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
        )
        .bind(table)
        .fetch_one(repository.pool())
        .await
        .unwrap();
        assert_eq!(1, exists, "{table} should be initialized");
    }

    repository.pool().close().await;
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn readiness_helpers_probe_connection_and_schema() {
    let repository = Repository::connect_in_memory().await.unwrap();

    repository.check_connection().await.unwrap();
    assert!(repository.schema_initialized().await.unwrap());

    sqlx::query("DROP TABLE jobs")
        .execute(repository.pool())
        .await
        .unwrap();
    assert!(!repository.schema_initialized().await.unwrap());
}

#[tokio::test]
async fn remote_candidate_cache_material_reads_persisted_torrent_fields() {
    let repository = Repository::connect_in_memory().await.unwrap();
    let mut candidate = test_remote_candidate("guid-cache", "Example");
    candidate.torrent_cache_path = Some(PathBuf::from("/cache/example.torrent"));

    repository
        .upsert_remote_candidate(&candidate)
        .await
        .unwrap();
    let material = repository
        .remote_candidate_cache_material(&candidate.indexer_id, &candidate.guid)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        candidate.info_hash.as_ref().map(|hash| hash.as_str()),
        material.info_hash.as_deref()
    );
    assert_eq!(candidate.torrent_cache_path, material.torrent_cache_path);
}

#[tokio::test]
async fn remote_candidate_cache_material_leaves_cached_hash_unparsed() {
    let repository = Repository::connect_in_memory().await.unwrap();
    let candidate = test_remote_candidate("guid-invalid-cache", "Example");

    repository
        .upsert_remote_candidate(&candidate)
        .await
        .unwrap();
    sqlx::query("UPDATE remote_candidates SET info_hash = 'not-a-hash' WHERE guid = ?")
        .bind(candidate.guid.as_str())
        .execute(repository.pool())
        .await
        .unwrap();
    let material = repository
        .remote_candidate_cache_material(&candidate.indexer_id, &candidate.guid)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(Some("not-a-hash"), material.info_hash.as_deref());
}

#[tokio::test]
async fn announce_candidate_material_reads_typed_fetch_material() {
    let repository = Repository::connect_in_memory().await.unwrap();
    let mut work = test_announce_work("ann_material", "guid-material", 1);
    let download_url =
        DownloadUrl::new("https://tracker.example/download?passkey=supersecret").unwrap();
    work.fetch = Some(
        AnnounceFetchMaterial::new(
            &download_url,
            Some(CookieSecret::new("session=abc").unwrap()),
        )
        .unwrap(),
    );
    work.info_hash = Some(InfoHash::new("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap());
    work.attempt_count = 3;

    repository
        .insert_or_dedupe_announce_work(&work, 10)
        .await
        .unwrap();
    let material = repository
        .announce_candidate_material(&work.id)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(work.title, material.title);
    assert_eq!(work.tracker, material.tracker);
    assert_eq!(
        work.guid.as_ref().map(|guid| guid.as_str().to_owned()),
        material.guid
    );
    assert_eq!(work.info_hash, material.info_hash);
    assert_eq!(work.size, material.size);
    assert_eq!(Some(download_url), material.download_url);
    assert_eq!(Some("session=abc"), material.cookie.as_deref());
    assert_eq!(3, material.attempt_count);

    let debug = format!("{material:?}");
    assert!(!debug.contains("supersecret"));
    assert!(!debug.contains("session=abc"));
}

#[tokio::test]
async fn announce_candidate_material_treats_negative_size_as_absent() {
    let repository = Repository::connect_in_memory().await.unwrap();
    let work = test_announce_work("ann_negative_size", "guid-negative-size", 1);

    repository
        .insert_or_dedupe_announce_work(&work, 10)
        .await
        .unwrap();
    sqlx::query("UPDATE announce_work SET size = -1 WHERE id = ?")
        .bind(work.id.as_str())
        .execute(repository.pool())
        .await
        .unwrap();
    let material = repository
        .announce_candidate_material(&work.id)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(None, material.size);
}

#[tokio::test]
async fn system_test_snapshot_and_diagnostics_read_app_tables() {
    let repository = Repository::connect_in_memory().await.unwrap();
    let item = test_local_item("Diagnostic Example");
    let file = LocalFile::new(
        None,
        PathBuf::from("Diagnostic Example/file.mkv"),
        ByteSize::new(10),
        FileIndex::new(0),
    )
    .unwrap();
    repository
        .upsert_local_item_with_files(&item, &[file])
        .await
        .unwrap();
    repository
        .upsert_remote_candidate(&test_remote_candidate("guid-diag", "Diagnostic Candidate"))
        .await
        .unwrap();

    let snapshot = repository.system_test_snapshot(8).await.unwrap();
    let diagnostics = repository.system_test_diagnostics(8).await.unwrap();

    assert_eq!(1, snapshot.local_items);
    assert_eq!(1, snapshot.local_files);
    assert_eq!(1, snapshot.remote_candidates);
    assert_eq!(1, snapshot.client_items.len());
    assert_eq!("Diagnostic Example", diagnostics.local_items[0].title);
    assert_eq!("file.mkv", diagnostics.local_files[0].file_name);
    assert_eq!(
        "Diagnostic Candidate",
        diagnostics.remote_candidates[0].title
    );
}

#[tokio::test]
async fn file_backed_repository_reconciles_pre_prowlarr_schema() {
    let root = unique_temp_dir("sqlite-reconcile");
    let database = root.join("sporos.db");
    let setup_pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(&database)
                .create_if_missing(true),
        )
        .await
        .unwrap();
    for statement in [
        r#"
            CREATE TABLE indexers (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                url TEXT NOT NULL,
                api_key_source TEXT NOT NULL,
                enabled INTEGER NOT NULL,
                capabilities_json TEXT NOT NULL DEFAULT '{}',
                state TEXT NOT NULL,
                retry_after INTEGER,
                last_caps_refresh_at INTEGER,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                UNIQUE (name),
                UNIQUE (url)
            )
            "#,
        r#"
            CREATE TABLE dependency_health (
                dependency_type TEXT NOT NULL,
                dependency_name TEXT NOT NULL,
                state TEXT NOT NULL,
                reason TEXT,
                retry_after INTEGER,
                checked_at INTEGER NOT NULL,
                PRIMARY KEY (dependency_type, dependency_name)
            )
            "#,
        r#"
            INSERT INTO indexers (
                name,
                url,
                api_key_source,
                enabled,
                capabilities_json,
                state,
                retry_after,
                last_caps_refresh_at,
                created_at,
                updated_at
            )
            VALUES ('legacy', 'https://indexer.example/api', 'direct', 1, '{}',
                    'unknown', NULL, NULL, 10, 10)
            "#,
        r#"
            INSERT INTO dependency_health (
                dependency_type,
                dependency_name,
                state,
                reason,
                retry_after,
                checked_at
            )
            VALUES ('indexer', 'legacy', 'degraded', 'rate limited', 123, 20)
            "#,
    ] {
        sqlx::query(statement).execute(&setup_pool).await.unwrap();
    }
    setup_pool.close().await;

    let repository = Repository::connect(&database).await.unwrap();
    let rows = repository.indexer_registry_snapshot(10).await.unwrap();
    let legacy = rows
        .iter()
        .find(|indexer| indexer.name.as_str() == "legacy")
        .unwrap();
    let failure_count = repository
        .dependency_failure_count("indexer", &DependencyName::new("legacy").unwrap())
        .await
        .unwrap();

    assert_eq!("static", legacy.source_kind);
    assert_eq!("", legacy.source_name);
    assert_eq!("legacy", legacy.source_indexer_id);
    assert_eq!(0, failure_count);

    let renamed = repository
        .sync_torznab_indexers(
            &[test_indexer(
                "renamed",
                "https://indexer.example/api",
                ApiKeySource::Direct,
            )],
            100,
        )
        .await
        .unwrap();
    assert!(renamed.iter().any(|indexer| {
        indexer.name.as_str() == "renamed"
            && indexer.url == "https://indexer.example/api"
            && indexer.enabled
    }));

    repository.pool().close().await;
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn file_backed_repository_reconciles_partial_indexer_source_schema() {
    let root = unique_temp_dir("sqlite-reconcile-partial-source");
    let database = root.join("sporos.db");
    let setup_pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(&database)
                .create_if_missing(true),
        )
        .await
        .unwrap();
    for statement in [
        r#"
            CREATE TABLE indexers (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                url TEXT NOT NULL,
                source_kind TEXT NOT NULL DEFAULT 'static',
                api_key_source TEXT NOT NULL,
                enabled INTEGER NOT NULL,
                capabilities_json TEXT NOT NULL DEFAULT '{}',
                state TEXT NOT NULL,
                retry_after INTEGER,
                last_caps_refresh_at INTEGER,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                UNIQUE (name)
            )
            "#,
        r#"
            INSERT INTO indexers (
                name,
                url,
                source_kind,
                api_key_source,
                enabled,
                capabilities_json,
                state,
                retry_after,
                last_caps_refresh_at,
                created_at,
                updated_at
            )
            VALUES ('partial', 'https://partial.example/api', 'static', 'direct', 1,
                    '{}', 'unknown', NULL, NULL, 10, 10)
            "#,
    ] {
        sqlx::query(statement).execute(&setup_pool).await.unwrap();
    }
    setup_pool.close().await;

    let repository = Repository::connect(&database).await.unwrap();
    let rows = repository.indexer_registry_snapshot(10).await.unwrap();
    let partial = rows
        .iter()
        .find(|indexer| indexer.name.as_str() == "partial")
        .unwrap();

    assert_eq!("static", partial.source_kind);
    assert_eq!("", partial.source_name);
    assert_eq!("partial", partial.source_indexer_id);

    repository.pool().close().await;
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn indexer_schema_rebuild_restores_pragmas_after_failure() {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    sqlx::query("PRAGMA foreign_keys = ON")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        r#"
            CREATE TABLE indexers (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                url TEXT NOT NULL,
                source_kind TEXT NOT NULL DEFAULT 'static',
                source_name TEXT NOT NULL DEFAULT '',
                source_indexer_id TEXT NOT NULL DEFAULT '',
                enabled INTEGER NOT NULL,
                capabilities_json TEXT NOT NULL DEFAULT '{}',
                state TEXT NOT NULL,
                retry_after INTEGER,
                last_caps_refresh_at INTEGER,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                UNIQUE (name)
            )
            "#,
    )
    .execute(&pool)
    .await
    .unwrap();

    let error = rebuild_indexers_table(&pool).await.unwrap_err();
    let foreign_keys: i64 = sqlx::query_scalar("PRAGMA foreign_keys")
        .fetch_one(&pool)
        .await
        .unwrap();
    let legacy_alter_table: i64 = sqlx::query_scalar("PRAGMA legacy_alter_table")
        .fetch_one(&pool)
        .await
        .unwrap();

    assert!(error.to_string().contains("api_key_source"));
    assert_eq!(1, foreign_keys);
    assert_eq!(0, legacy_alter_table);
}

#[test]
fn source_key_prefix_range_uses_exclusive_text_bound() {
    assert_eq!(
        SourceKeyPrefixRange::new("12:qbit-a.local:".to_owned()),
        SourceKeyPrefixRange {
            start: "12:qbit-a.local:".to_owned(),
            end: Some("12:qbit-a.local;".to_owned()),
        }
    );
    assert_eq!(
        SourceKeyPrefixRange::new("rtorrent:5000:".to_owned()),
        SourceKeyPrefixRange {
            start: "rtorrent:5000:".to_owned(),
            end: Some("rtorrent:5000;".to_owned()),
        }
    );
}

#[tokio::test]
async fn client_source_key_range_queries_use_local_item_key_index() {
    let repository = Repository::connect_in_memory().await.unwrap();
    let range = SourceKeyPrefixRange::new(client_source_key_prefix(
        &ClientHost::new("qbit.local").unwrap(),
    ));
    let rows = sqlx::query(
        r#"
            EXPLAIN QUERY PLAN
            SELECT id
            FROM local_items
            WHERE source_type = 'client'
              AND source_key >= ?
              AND source_key < ?
            "#,
    )
    .bind(range.start)
    .bind(range.end.unwrap())
    .fetch_all(repository.pool())
    .await
    .unwrap();
    let details = rows
        .into_iter()
        .map(|row| row.get::<String, _>("detail"))
        .collect::<Vec<_>>()
        .join("\n");

    assert!(details.contains("USING COVERING INDEX") || details.contains("USING INDEX"));
    assert!(details.contains("source_type"));
    assert!(details.contains("source_key"));
}

#[tokio::test]
async fn local_item_keyset_queries_use_media_title_source_index() {
    let repository = Repository::connect_in_memory().await.unwrap();
    let rows = sqlx::query(
        r#"
            EXPLAIN QUERY PLAN
            SELECT id
            FROM local_items
            WHERE media_type = 'episode'
              AND (title, source_type, source_key) > (?, ?, ?)
            ORDER BY title, source_type, source_key
            LIMIT 512
            "#,
    )
    .bind("Paged Show 0511 S01E03")
    .bind("data_root")
    .bind("/media/paged-0511-e03.mkv")
    .fetch_all(repository.pool())
    .await
    .unwrap();
    let details = rows
        .into_iter()
        .map(|row| row.get::<String, _>("detail"))
        .collect::<Vec<_>>()
        .join("\n");

    assert!(details.contains("idx_local_items_media_title_source"));
    assert!(details.contains("media_type"));
    assert!(details.contains("title"));
}

#[tokio::test]
async fn staged_virtual_season_keyset_query_uses_primary_key_range() {
    let repository = Repository::connect_in_memory().await.unwrap();
    let mut replacement = repository
        .begin_local_inventory_replace_transaction(LocalInventoryScope::Virtual)
        .await
        .unwrap();
    replacement
        .initialize_virtual_season_candidate_stage()
        .await
        .unwrap();
    let rows = sqlx::query(
        r#"
            EXPLAIN QUERY PLAN
            SELECT state.title, state.season
            FROM staged_virtual_season_state state
            WHERE (state.title, state.season) > (?, ?)
            ORDER BY state.title, state.season
            LIMIT 512
            "#,
    )
    .bind("Paged Show 0511")
    .bind(1_i64)
    .fetch_all(&mut *replacement.transaction)
    .await
    .unwrap();
    let details = rows
        .into_iter()
        .map(|row| row.get::<String, _>("detail"))
        .collect::<Vec<_>>()
        .join("\n");

    assert!(details.contains("USING PRIMARY KEY"));
    assert!(details.contains("title"));
}

#[tokio::test]
async fn local_item_upsert_replaces_file_batch_in_transaction() {
    let repository = Repository::connect_in_memory().await.unwrap();
    let item = test_local_item("Example");
    let first_file = LocalFile::new(
        None,
        PathBuf::from("Example/file-a.mkv"),
        ByteSize::new(10),
        FileIndex::new(0),
    )
    .unwrap()
    .with_mtime_ms(Some(1_700_000_000_001));
    let second_file = LocalFile::new(
        None,
        PathBuf::from("Example/file-b.mkv"),
        ByteSize::new(20),
        FileIndex::new(1),
    )
    .unwrap()
    .with_mtime_ms(Some(1_700_000_000_002));

    let item_id = repository
        .upsert_local_item_with_files(&item, &[first_file.clone(), second_file])
        .await
        .unwrap();
    repository
        .upsert_local_item_with_files(&item, &[first_file])
        .await
        .unwrap();

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM local_files WHERE item_id = ?")
        .bind(i64_from_u64(item_id.get(), "local item id").unwrap())
        .fetch_one(repository.pool())
        .await
        .unwrap();

    assert_eq!(1, count);

    let files = repository.local_files_for_item(item_id, 10).await.unwrap();

    assert_eq!(1, files.len());
    assert_eq!(PathBuf::from("Example/file-a.mkv"), files[0].relative_path);
    assert_eq!("file-a.mkv", files[0].file_name);
    assert_eq!(ByteSize::new(10), files[0].size);
    assert_eq!(Some(1_700_000_000_001), files[0].mtime_ms);
    assert_eq!(FileIndex::new(0), files[0].file_index);
}

#[tokio::test]
async fn local_file_queries_find_duplicate_sizes_names_and_paths() {
    let repository = Repository::connect_in_memory().await.unwrap();
    let first_item = test_local_item("Example");
    let mut second_item = test_local_item("Other");
    second_item.source = LocalItemSource::Virtual {
        source_key: SourceKey::new("other-source").unwrap(),
    };
    second_item.info_hash = None;

    let first_file = LocalFile::new(
        None,
        PathBuf::from("Example/file-a.mkv"),
        ByteSize::new(10),
        FileIndex::new(0),
    )
    .unwrap()
    .with_mtime_ms(Some(1_700_000_000_001));
    let second_file = LocalFile::new(
        None,
        PathBuf::from("Other/file-a.mkv"),
        ByteSize::new(10),
        FileIndex::new(0),
    )
    .unwrap()
    .with_mtime_ms(Some(1_700_000_000_002));
    let third_file = LocalFile::new(
        None,
        PathBuf::from("Other/file-c.mkv"),
        ByteSize::new(20),
        FileIndex::new(1),
    )
    .unwrap();

    let first_item_id = repository
        .upsert_local_item_with_files(&first_item, &[first_file])
        .await
        .unwrap();
    let second_item_id = repository
        .upsert_local_item_with_files(&second_item, &[second_file, third_file])
        .await
        .unwrap();

    let size_matches = repository
        .local_files_by_size(ByteSize::new(10), 10)
        .await
        .unwrap();
    let name_matches = repository
        .local_files_by_size_and_name(ByteSize::new(10), "file-a.mkv", 10)
        .await
        .unwrap();
    let path_matches = repository
        .local_files_by_relative_path(&PathBuf::from("Other/file-a.mkv"), 10)
        .await
        .unwrap();

    assert_eq!(2, size_matches.len());
    assert_eq!(2, name_matches.len());
    assert_eq!(1, path_matches.len());
    assert_eq!(second_item_id, path_matches[0].item_id);
    assert_eq!(
        PathBuf::from("Other/file-a.mkv"),
        path_matches[0].relative_path
    );
    assert_eq!(Some(1_700_000_000_002), path_matches[0].mtime_ms);
    assert!(
        size_matches
            .iter()
            .any(|file| file.item_id == first_item_id)
    );
    assert!(
        size_matches
            .iter()
            .any(|file| file.item_id == second_item_id)
    );
}

#[tokio::test]
async fn local_item_with_largest_file_preserves_distinct_mtimes() {
    let repository = Repository::connect_in_memory().await.unwrap();
    let mut item = test_local_item("Example");
    item.media_type = MediaType::SeasonPack;
    item.mtime_ms = Some(1_700_000_000_111);
    let small_newer_file = LocalFile::new(
        None,
        PathBuf::from("Example/file-a.mkv"),
        ByteSize::new(10),
        FileIndex::new(0),
    )
    .unwrap()
    .with_mtime_ms(Some(1_700_000_000_999));
    let largest_older_file = LocalFile::new(
        None,
        PathBuf::from("Example/file-b.mkv"),
        ByteSize::new(20),
        FileIndex::new(1),
    )
    .unwrap()
    .with_mtime_ms(Some(1_700_000_000_222));
    repository
        .upsert_local_item_with_files(&item, &[small_newer_file, largest_older_file])
        .await
        .unwrap();

    let rows = repository
        .local_items_with_largest_file_by_media_type_page(MediaType::SeasonPack, 10, 0)
        .await
        .unwrap();

    assert_eq!(1, rows.len());
    assert_eq!(Some(1_700_000_000_111), rows[0].item.mtime_ms);
    assert_eq!("file-b.mkv", rows[0].file.file_name);
    assert_eq!(Some(1_700_000_000_222), rows[0].file.mtime_ms);
}

#[tokio::test]
async fn large_local_file_batches_commit_in_one_transaction() {
    let repository = Repository::connect_in_memory().await.unwrap();
    let item = test_local_item("Example");
    let files = (0..1_024)
        .map(|index| {
            LocalFile::new(
                None,
                PathBuf::from(format!("Example/file-{index:04}.mkv")),
                ByteSize::new(u64::from(index) + 1),
                FileIndex::new(index),
            )
            .unwrap()
            .with_mtime_ms(Some(1_700_000_000_000 + i64::from(index)))
        })
        .collect::<Vec<_>>();

    let item_id = repository
        .upsert_local_item_with_files(&item, &files)
        .await
        .unwrap();
    let stored = repository
        .local_files_for_item(item_id, 2_000)
        .await
        .unwrap();

    assert_eq!(files.len(), stored.len());
    assert_eq!(
        PathBuf::from("Example/file-0000.mkv"),
        stored[0].relative_path
    );
    assert_eq!(
        PathBuf::from("Example/file-1023.mkv"),
        stored[1023].relative_path
    );
    assert_eq!(Some(1_700_000_001_023), stored[1023].mtime_ms);
}

#[tokio::test]
async fn local_files_for_item_page_reports_truncation() {
    let repository = Repository::connect_in_memory().await.unwrap();
    let item = test_local_item("Example");
    let files = [
        LocalFile::new(
            None,
            PathBuf::from("Example/file-a.mkv"),
            ByteSize::new(10),
            FileIndex::new(0),
        )
        .unwrap(),
        LocalFile::new(
            None,
            PathBuf::from("Example/file-b.mkv"),
            ByteSize::new(20),
            FileIndex::new(1),
        )
        .unwrap(),
    ];
    let item_id = repository
        .upsert_local_item_with_files(&item, &files)
        .await
        .unwrap();

    let limited = repository
        .local_files_for_item_page(item_id, 1)
        .await
        .unwrap();
    let complete = repository
        .local_files_for_item_page(item_id, 2)
        .await
        .unwrap();

    assert!(limited.truncated);
    assert_eq!(1, limited.files.len());
    assert!(!complete.truncated);
    assert_eq!(2, complete.files.len());
}

#[tokio::test]
async fn failed_local_file_batch_rolls_back_item_and_file_replacement() {
    let repository = Repository::connect_in_memory().await.unwrap();
    let mut item = test_local_item("Original");
    let first_file = LocalFile::new(
        None,
        PathBuf::from("Example/file-a.mkv"),
        ByteSize::new(10),
        FileIndex::new(0),
    )
    .unwrap();
    let duplicate_a = LocalFile::new(
        None,
        PathBuf::from("Example/duplicate-a.mkv"),
        ByteSize::new(20),
        FileIndex::new(0),
    )
    .unwrap();
    let duplicate_b = LocalFile::new(
        None,
        PathBuf::from("Example/duplicate-b.mkv"),
        ByteSize::new(30),
        FileIndex::new(0),
    )
    .unwrap();

    let item_id = repository
        .upsert_local_item_with_files(&item, &[first_file])
        .await
        .unwrap();
    item.title = ItemTitle::new("Should Roll Back").unwrap();
    let result = repository
        .upsert_local_item_with_files(&item, &[duplicate_a, duplicate_b])
        .await;

    let title: String = sqlx::query_scalar("SELECT title FROM local_items WHERE id = ?")
        .bind(i64_from_u64(item_id.get(), "local item id").unwrap())
        .fetch_one(repository.pool())
        .await
        .unwrap();
    let file_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM local_files WHERE item_id = ?")
        .bind(i64_from_u64(item_id.get(), "local item id").unwrap())
        .fetch_one(repository.pool())
        .await
        .unwrap();

    assert!(matches!(result, Err(DatabaseError::QueryFailed { .. })));
    assert_eq!("Original", title);
    assert_eq!(1, file_count);
}

#[tokio::test]
async fn successful_owned_inventory_stream_clears_staged_rows() {
    let repository = Repository::connect_in_memory().await.unwrap();
    let mut item = test_local_item("Complete");
    item.source = LocalItemSource::DataRoot {
        path: PathBuf::from("/media/complete"),
    };
    item.info_hash = None;
    item.path = Some(PathBuf::from("/media/complete"));
    let file = LocalFile::new(
        None,
        PathBuf::from("complete.mkv"),
        ByteSize::new(20),
        FileIndex::new(0),
    )
    .unwrap();
    let (sender, receiver) = mpsc::channel(2);
    sender
        .send(OwnedLocalInventoryMessage::item(OwnedLocalItemFileBatch {
            item,
            files: vec![file],
        }))
        .await
        .unwrap();
    sender
        .send(OwnedLocalInventoryMessage::Finished)
        .await
        .unwrap();
    drop(sender);

    let summary = repository
        .replace_local_inventory_owned_receiver(LocalInventoryScope::DataRoot, receiver)
        .await
        .unwrap();
    let local_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM local_items")
        .fetch_one(repository.pool())
        .await
        .unwrap();
    let staged_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM temp.staged_local_inventory_items")
            .fetch_one(repository.pool())
            .await
            .unwrap();
    let staged_file_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM temp.staged_local_inventory_files")
            .fetch_one(repository.pool())
            .await
            .unwrap();

    assert_eq!(1, summary.upserted);
    assert_eq!(0, summary.pruned);
    assert_eq!(1, local_count);
    assert_eq!(0, staged_count);
    assert_eq!(0, staged_file_count);
}

#[tokio::test]
async fn stalled_owned_inventory_stream_does_not_starve_main_pool() {
    let root = unique_temp_dir("sqlite-inventory-staging-pool");
    let database = root.join("sporos.db");
    let repository = Repository::connect(&database).await.unwrap();
    repository
        .inventory_staging_pool
        .acquire()
        .await
        .unwrap()
        .close()
        .await
        .unwrap();
    let staging_foreign_keys: i64 = sqlx::query_scalar("PRAGMA foreign_keys;")
        .fetch_one(&repository.inventory_staging_pool)
        .await
        .unwrap();
    let staging_busy_timeout: i64 = sqlx::query_scalar("PRAGMA busy_timeout;")
        .fetch_one(&repository.inventory_staging_pool)
        .await
        .unwrap();
    let mut held_connections = Vec::new();
    for _ in 0..4 {
        held_connections.push(repository.pool().acquire().await.unwrap());
    }
    let (sender, receiver) = mpsc::channel(1);
    let staging_started = Arc::new(AtomicBool::new(false));
    let refresh_repository = repository.clone();
    let refresh_started = Arc::clone(&staging_started);
    let refresh_task = tokio::spawn(async move {
        refresh_repository
            .replace_local_inventory_owned_receiver_with_staging_signal(
                LocalInventoryScope::DataRoot,
                receiver,
                Some(refresh_started),
            )
            .await
    });

    tokio::time::timeout(Duration::from_secs(1), async {
        while !staging_started.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let still_available: i64 = tokio::time::timeout(Duration::from_millis(100), async {
        sqlx::query_scalar("SELECT 1")
            .fetch_one(repository.pool())
            .await
    })
    .await
    .unwrap()
    .unwrap();

    sender
        .send(OwnedLocalInventoryMessage::Finished)
        .await
        .unwrap();
    let summary = refresh_task.await.unwrap().unwrap();
    drop(held_connections);
    repository.inventory_staging_pool.close().await;
    repository.pool().close().await;
    fs::remove_dir_all(root).unwrap();

    assert_eq!(1, staging_foreign_keys);
    assert_eq!(i64::from(BUSY_TIMEOUT_MS), staging_busy_timeout);
    assert_eq!(1, still_available);
    assert_eq!(0, summary.upserted);
    assert_eq!(0, summary.pruned);
}

#[tokio::test]
async fn inventory_staging_pool_allows_parallel_staging_connections() {
    let root = unique_temp_dir("sqlite-inventory-parallel-staging-pool");
    let database = root.join("sporos.db");
    let repository = Repository::connect(&database).await.unwrap();

    let first = repository.inventory_staging_pool.acquire().await.unwrap();
    let second = tokio::time::timeout(
        Duration::from_millis(100),
        repository.inventory_staging_pool.acquire(),
    )
    .await
    .unwrap()
    .unwrap();

    drop(second);
    drop(first);
    repository.inventory_staging_pool.close().await;
    repository.pool().close().await;
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn invalid_owned_inventory_scope_clears_staged_rows() {
    let repository = Repository::connect_in_memory().await.unwrap();
    let mut valid = test_local_item("Valid");
    valid.source = LocalItemSource::DataRoot {
        path: PathBuf::from("/media/valid"),
    };
    valid.info_hash = None;
    valid.path = Some(PathBuf::from("/media/valid"));
    let valid_file = LocalFile::new(
        None,
        PathBuf::from("valid.mkv"),
        ByteSize::new(20),
        FileIndex::new(0),
    )
    .unwrap();
    let invalid = test_local_item("Invalid");
    let invalid_file = LocalFile::new(
        None,
        PathBuf::from("invalid.mkv"),
        ByteSize::new(30),
        FileIndex::new(0),
    )
    .unwrap();
    let (sender, receiver) = mpsc::channel(3);
    sender
        .send(OwnedLocalInventoryMessage::item(OwnedLocalItemFileBatch {
            item: valid,
            files: vec![valid_file],
        }))
        .await
        .unwrap();
    sender
        .send(OwnedLocalInventoryMessage::item(OwnedLocalItemFileBatch {
            item: invalid,
            files: vec![invalid_file],
        }))
        .await
        .unwrap();
    sender
        .send(OwnedLocalInventoryMessage::Finished)
        .await
        .unwrap();
    drop(sender);

    let result = repository
        .replace_local_inventory_owned_receiver(LocalInventoryScope::DataRoot, receiver)
        .await;
    let local_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM local_items")
        .fetch_one(repository.pool())
        .await
        .unwrap();
    let (staged_count, staged_file_count) = staged_inventory_counts(&repository).await;

    assert!(matches!(result, Err(DatabaseError::QueryFailed { .. })));
    assert_eq!(0, local_count);
    assert_eq!(0, staged_count);
    assert_eq!(0, staged_file_count);
}

#[tokio::test]
async fn unfinished_owned_inventory_stream_rolls_back_partial_refresh() {
    let repository = Repository::connect_in_memory().await.unwrap();
    let mut existing = test_local_item("Existing");
    existing.source = LocalItemSource::DataRoot {
        path: PathBuf::from("/media/existing"),
    };
    existing.info_hash = None;
    existing.path = Some(PathBuf::from("/media/existing"));
    let existing_file = LocalFile::new(
        None,
        PathBuf::from("existing.mkv"),
        ByteSize::new(10),
        FileIndex::new(0),
    )
    .unwrap();
    repository
        .upsert_local_item_with_files(&existing, &[existing_file])
        .await
        .unwrap();

    let mut partial = test_local_item("Partial");
    partial.source = LocalItemSource::DataRoot {
        path: PathBuf::from("/media/partial"),
    };
    partial.info_hash = None;
    partial.path = Some(PathBuf::from("/media/partial"));
    let partial_file = LocalFile::new(
        None,
        PathBuf::from("partial.mkv"),
        ByteSize::new(20),
        FileIndex::new(0),
    )
    .unwrap();
    let (sender, receiver) = mpsc::channel(1);
    sender
        .send(OwnedLocalInventoryMessage::item(OwnedLocalItemFileBatch {
            item: partial,
            files: vec![partial_file],
        }))
        .await
        .unwrap();
    drop(sender);

    let result = repository
        .replace_local_inventory_owned_receiver(LocalInventoryScope::DataRoot, receiver)
        .await;
    let existing_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM local_items WHERE source_key = ?")
            .bind("/media/existing")
            .fetch_one(repository.pool())
            .await
            .unwrap();
    let partial_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM local_items WHERE source_key = ?")
            .bind("/media/partial")
            .fetch_one(repository.pool())
            .await
            .unwrap();
    let staged_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM temp.staged_local_inventory_items")
            .fetch_one(repository.pool())
            .await
            .unwrap();
    let staged_file_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM temp.staged_local_inventory_files")
            .fetch_one(repository.pool())
            .await
            .unwrap();

    assert!(matches!(
        result,
        Err(DatabaseError::IncompleteStream { .. })
    ));
    assert_eq!(1, existing_count);
    assert_eq!(0, partial_count);
    assert_eq!(0, staged_count);
    assert_eq!(0, staged_file_count);
}

#[tokio::test]
async fn failed_staged_inventory_commit_rolls_back_item_files_and_prune() {
    let repository = Repository::connect_in_memory().await.unwrap();
    let mut existing = test_local_item("Original");
    existing.source = LocalItemSource::DataRoot {
        path: PathBuf::from("/media/existing"),
    };
    existing.info_hash = None;
    existing.path = Some(PathBuf::from("/media/existing"));
    let existing_file = LocalFile::new(
        None,
        PathBuf::from("existing.mkv"),
        ByteSize::new(10),
        FileIndex::new(0),
    )
    .unwrap();
    let item_id = repository
        .upsert_local_item_with_files(&existing, &[existing_file])
        .await
        .unwrap();

    let mut prunable = test_local_item("Prunable");
    prunable.source = LocalItemSource::DataRoot {
        path: PathBuf::from("/media/prunable"),
    };
    prunable.info_hash = None;
    prunable.path = Some(PathBuf::from("/media/prunable"));
    let prunable_file = LocalFile::new(
        None,
        PathBuf::from("prunable.mkv"),
        ByteSize::new(15),
        FileIndex::new(0),
    )
    .unwrap();
    repository
        .upsert_local_item_with_files(&prunable, &[prunable_file])
        .await
        .unwrap();

    let mut staged = existing.clone();
    staged.title = ItemTitle::new("Should Roll Back").unwrap();
    let duplicate_a = LocalFile::new(
        None,
        PathBuf::from("duplicate-a.mkv"),
        ByteSize::new(20),
        FileIndex::new(0),
    )
    .unwrap();
    let duplicate_b = LocalFile::new(
        None,
        PathBuf::from("duplicate-b.mkv"),
        ByteSize::new(30),
        FileIndex::new(0),
    )
    .unwrap();
    let (sender, receiver) = mpsc::channel(2);
    sender
        .send(OwnedLocalInventoryMessage::item(OwnedLocalItemFileBatch {
            item: staged,
            files: vec![duplicate_a, duplicate_b],
        }))
        .await
        .unwrap();
    sender
        .send(OwnedLocalInventoryMessage::Finished)
        .await
        .unwrap();
    drop(sender);

    let result = repository
        .replace_local_inventory_owned_receiver(LocalInventoryScope::DataRoot, receiver)
        .await;
    let title: String = sqlx::query_scalar("SELECT title FROM local_items WHERE id = ?")
        .bind(i64_from_u64(item_id.get(), "local item id").unwrap())
        .fetch_one(repository.pool())
        .await
        .unwrap();
    let existing_files = repository.local_files_for_item(item_id, 10).await.unwrap();
    let prunable_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM local_items WHERE source_key = ?")
            .bind("/media/prunable")
            .fetch_one(repository.pool())
            .await
            .unwrap();
    let (staged_count, staged_file_count) = staged_inventory_counts(&repository).await;

    assert!(matches!(result, Err(DatabaseError::QueryFailed { .. })));
    assert_eq!("Original", title);
    assert_eq!(1, existing_files.len());
    assert_eq!(
        PathBuf::from("existing.mkv"),
        existing_files[0].relative_path
    );
    assert_eq!(1, prunable_count);
    assert_eq!(0, staged_count);
    assert_eq!(0, staged_file_count);
}

#[tokio::test]
async fn failed_staged_inventory_prune_rolls_back_item_files_and_prune() {
    let repository = Repository::connect_in_memory().await.unwrap();
    let mut existing = test_local_item("Original");
    existing.source = LocalItemSource::DataRoot {
        path: PathBuf::from("/media/existing"),
    };
    existing.info_hash = None;
    existing.path = Some(PathBuf::from("/media/existing"));
    let existing_file = LocalFile::new(
        None,
        PathBuf::from("existing.mkv"),
        ByteSize::new(10),
        FileIndex::new(0),
    )
    .unwrap();
    let item_id = repository
        .upsert_local_item_with_files(&existing, &[existing_file])
        .await
        .unwrap();

    let mut prunable = test_local_item("Prunable");
    prunable.source = LocalItemSource::DataRoot {
        path: PathBuf::from("/media/prunable"),
    };
    prunable.info_hash = None;
    prunable.path = Some(PathBuf::from("/media/prunable"));
    let prunable_file = LocalFile::new(
        None,
        PathBuf::from("prunable.mkv"),
        ByteSize::new(15),
        FileIndex::new(0),
    )
    .unwrap();
    repository
        .upsert_local_item_with_files(&prunable, &[prunable_file])
        .await
        .unwrap();
    sqlx::query(
        r#"
            CREATE TRIGGER abort_prunable_local_item_delete
            BEFORE DELETE ON local_items
            WHEN old.source_key = '/media/prunable'
            BEGIN
                SELECT RAISE(ABORT, 'abort prunable delete');
            END
            "#,
    )
    .execute(repository.pool())
    .await
    .unwrap();

    let mut staged = existing.clone();
    staged.title = ItemTitle::new("Should Roll Back").unwrap();
    let staged_file = LocalFile::new(
        None,
        PathBuf::from("staged.mkv"),
        ByteSize::new(20),
        FileIndex::new(0),
    )
    .unwrap();
    let (sender, receiver) = mpsc::channel(2);
    sender
        .send(OwnedLocalInventoryMessage::item(OwnedLocalItemFileBatch {
            item: staged,
            files: vec![staged_file],
        }))
        .await
        .unwrap();
    sender
        .send(OwnedLocalInventoryMessage::Finished)
        .await
        .unwrap();
    drop(sender);

    let result = repository
        .replace_local_inventory_owned_receiver(LocalInventoryScope::DataRoot, receiver)
        .await;
    let title: String = sqlx::query_scalar("SELECT title FROM local_items WHERE id = ?")
        .bind(i64_from_u64(item_id.get(), "local item id").unwrap())
        .fetch_one(repository.pool())
        .await
        .unwrap();
    let existing_files = repository.local_files_for_item(item_id, 10).await.unwrap();
    let prunable_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM local_items WHERE source_key = ?")
            .bind("/media/prunable")
            .fetch_one(repository.pool())
            .await
            .unwrap();
    let (staged_count, staged_file_count) = staged_inventory_counts(&repository).await;

    assert!(matches!(result, Err(DatabaseError::QueryFailed { .. })));
    assert_eq!("Original", title);
    assert_eq!(1, existing_files.len());
    assert_eq!(
        PathBuf::from("existing.mkv"),
        existing_files[0].relative_path
    );
    assert_eq!(1, prunable_count);
    assert_eq!(0, staged_count);
    assert_eq!(0, staged_file_count);
}

#[tokio::test]
async fn remote_candidate_upsert_uses_indexer_guid_natural_key() {
    let repository = Repository::connect_in_memory().await.unwrap();
    let mut candidate = test_remote_candidate("guid-1", "Original");
    candidate.download_url = DownloadUrl::new(
            "https://user:password@indexer.example/download?id=1&authkey=secret&torrent_pass=other-secret",
        )
        .unwrap();

    let first_id = repository
        .upsert_remote_candidate(&candidate)
        .await
        .unwrap();
    candidate.title = ItemTitle::new("Updated").unwrap();
    candidate.torrent_cache_path = Some(PathBuf::from("/cache/fedcba.cached.torrent"));
    let second_id = repository
        .upsert_remote_candidate(&candidate)
        .await
        .unwrap();
    candidate.torrent_cache_path = None;
    let third_id = repository
        .upsert_remote_candidate(&candidate)
        .await
        .unwrap();

    let row =
        sqlx::query("SELECT title, redacted_download_url FROM remote_candidates WHERE id = ?")
            .bind(i64_from_u64(first_id.get(), "remote candidate id").unwrap())
            .fetch_one(repository.pool())
            .await
            .unwrap();
    let title: String = row.get("title");
    let redacted_download_url: String = row.get("redacted_download_url");
    let info_hash_matches = repository
        .remote_candidates_by_info_hash(candidate.info_hash.as_ref().unwrap(), 10)
        .await
        .unwrap();

    assert_eq!(first_id, second_id);
    assert_eq!(first_id, third_id);
    assert_eq!("Updated", title);
    assert_eq!(
        "https://[REDACTED]@indexer.example/download?id=1&authkey=[REDACTED]&torrent_pass=[REDACTED]",
        redacted_download_url
    );
    assert!(!redacted_download_url.contains("secret"));
    assert!(!redacted_download_url.contains("other-secret"));
    assert!(!redacted_download_url.contains("password"));
    assert_eq!(1, info_hash_matches.len());
    assert_eq!(first_id, info_hash_matches[0].id);
    assert_eq!(
        redacted_download_url,
        info_hash_matches[0].redacted_download_url
    );
    assert_eq!(
        Some(PathBuf::from("/cache/fedcba.cached.torrent")),
        info_hash_matches[0].torrent_cache_path
    );
}

#[tokio::test]
async fn sync_torznab_indexers_upserts_and_disables_removed_rows() {
    let repository = Repository::connect_in_memory().await.unwrap();
    let first = test_indexer(
        "main",
        "https://indexer.example/api",
        ApiKeySource::Env("INDEXER_KEY".to_owned()),
    );
    let second = test_indexer(
        "backup",
        "https://backup.example/api",
        ApiKeySource::File("/run/secrets/backup".to_owned()),
    );

    let synced = repository
        .sync_torznab_indexers(&[first.clone(), second.clone()], 100)
        .await
        .unwrap();
    let main = synced
        .iter()
        .find(|indexer| indexer.name.as_str() == "main")
        .unwrap();
    assert_eq!("https://indexer.example/api", main.url);
    assert_eq!("env:INDEXER_KEY", main.api_key_source);
    assert!(main.enabled);
    assert_eq!("unknown", main.state);

    let updated = test_indexer(
        "main",
        "https://indexer.example/prowlarr/api",
        ApiKeySource::Direct,
    );
    let resynced = repository
        .sync_torznab_indexers(&[updated], 200)
        .await
        .unwrap();
    let main = resynced
        .iter()
        .find(|indexer| indexer.name.as_str() == "main")
        .unwrap();
    let backup = resynced
        .iter()
        .find(|indexer| indexer.name.as_str() == "backup")
        .unwrap();

    assert_eq!("https://indexer.example/prowlarr/api", main.url);
    assert_eq!("direct", main.api_key_source);
    assert!(main.enabled);
    assert!(!backup.enabled);
    assert!(!format!("{resynced:?}").contains("secret-value"));
}

#[tokio::test]
async fn sync_prowlarr_indexers_updates_by_stable_source_identity() {
    let repository = Repository::connect_in_memory().await.unwrap();
    let source = DependencyName::new("main").unwrap();
    let first = test_prowlarr_indexer("main", 101, "Movies", "https://prowlarr.example/101/api");

    let synced = repository
        .sync_prowlarr_indexers(&source, &[first], ProwlarrRemovePolicy::Deactivate, 100)
        .await
        .unwrap();
    let row = synced
        .iter()
        .find(|indexer| indexer.source_indexer_id == "101")
        .unwrap();
    let row_id = row.id;
    assert_eq!("prowlarr", row.source_kind);
    assert_eq!("main", row.source_name);
    assert_eq!("main:Movies", row.name.as_str());
    assert_eq!("https://prowlarr.example/101/api", row.url);
    assert!(row.enabled);

    let renamed = test_prowlarr_indexer(
        "main",
        101,
        "Movies Renamed",
        "https://new-prowlarr.example/101/api",
    );
    let resynced = repository
        .sync_prowlarr_indexers(&source, &[renamed], ProwlarrRemovePolicy::Deactivate, 200)
        .await
        .unwrap();
    let row = resynced
        .iter()
        .find(|indexer| indexer.source_indexer_id == "101")
        .unwrap();

    assert_eq!(row_id, row.id);
    assert_eq!("main:Movies Renamed", row.name.as_str());
    assert_eq!("https://new-prowlarr.example/101/api", row.url);
    assert!(row.enabled);
}

#[tokio::test]
async fn sync_prowlarr_indexers_does_not_infer_source_name_changes() {
    let repository = Repository::connect_in_memory().await.unwrap();
    let source = DependencyName::new("main").unwrap();
    let renamed_source = DependencyName::new("renamed").unwrap();
    let first = test_prowlarr_indexer("main", 101, "Movies", "https://prowlarr.example/101/api");

    let synced = repository
        .sync_prowlarr_indexers(&source, &[first], ProwlarrRemovePolicy::Deactivate, 100)
        .await
        .unwrap();
    let row_id = synced
        .iter()
        .find(|indexer| indexer.source_indexer_id == "101")
        .unwrap()
        .id;
    let renamed =
        test_prowlarr_indexer("renamed", 101, "Movies", "https://prowlarr.example/101/api");

    let resynced = repository
        .sync_prowlarr_indexers(
            &renamed_source,
            &[renamed],
            ProwlarrRemovePolicy::Deactivate,
            200,
        )
        .await
        .unwrap();
    let row = resynced
        .iter()
        .find(|indexer| indexer.source_name == "main" && indexer.source_indexer_id == "101")
        .unwrap();

    assert_eq!(row_id, row.id);
    assert_eq!("main", row.source_name);
    assert!(row.enabled);
    assert!(
        resynced
            .iter()
            .all(|indexer| indexer.source_name != "renamed")
    );
}

#[tokio::test]
async fn sync_prowlarr_indexers_does_not_infer_source_name_and_url_changes() {
    let repository = Repository::connect_in_memory().await.unwrap();
    let source = DependencyName::new("main").unwrap();
    let renamed_source = DependencyName::new("renamed").unwrap();
    let first = test_prowlarr_indexer("main", 101, "Movies", "https://prowlarr.example/101/api");
    let synced = repository
        .sync_prowlarr_indexers(&source, &[first], ProwlarrRemovePolicy::Deactivate, 100)
        .await
        .unwrap();
    let row_id = synced
        .iter()
        .find(|indexer| indexer.source_indexer_id == "101")
        .unwrap()
        .id;
    let renamed = test_prowlarr_indexer(
        "renamed",
        101,
        "Movies",
        "https://new-prowlarr.example/101/api",
    );

    let resynced = repository
        .sync_prowlarr_indexers(
            &renamed_source,
            &[renamed],
            ProwlarrRemovePolicy::Deactivate,
            200,
        )
        .await
        .unwrap();
    let rows = resynced
        .iter()
        .filter(|indexer| indexer.source_indexer_id == "101")
        .collect::<Vec<_>>();

    assert_eq!(2, rows.len());
    assert!(rows.iter().any(|row| row.id == row_id
        && row.source_name == "main"
        && row.url == "https://prowlarr.example/101/api"
        && row.enabled));
    assert!(rows.iter().any(|row| row.id != row_id
        && row.source_name == "renamed"
        && row.url == "https://new-prowlarr.example/101/api"
        && row.enabled));
}

#[tokio::test]
async fn sync_prowlarr_indexers_keeps_ids_source_local() {
    let repository = Repository::connect_in_memory().await.unwrap();
    let main = DependencyName::new("main").unwrap();
    let backup = DependencyName::new("backup").unwrap();
    let main_indexer = test_prowlarr_indexer("main", 101, "Movies", "https://main.example/101/api");
    let backup_indexer =
        test_prowlarr_indexer("backup", 101, "Movies", "https://backup.example/101/api");

    repository
        .sync_prowlarr_indexers(
            &main,
            &[main_indexer],
            ProwlarrRemovePolicy::Deactivate,
            100,
        )
        .await
        .unwrap();
    let rows = repository
        .sync_prowlarr_indexers(
            &backup,
            &[backup_indexer],
            ProwlarrRemovePolicy::Deactivate,
            200,
        )
        .await
        .unwrap();
    let main_row = rows
        .iter()
        .find(|indexer| indexer.source_name == "main" && indexer.source_indexer_id == "101")
        .unwrap();
    let backup_row = rows
        .iter()
        .find(|indexer| indexer.source_name == "backup" && indexer.source_indexer_id == "101")
        .unwrap();

    assert_ne!(main_row.id, backup_row.id);
    assert_eq!("https://main.example/101/api", main_row.url);
    assert_eq!("https://backup.example/101/api", backup_row.url);
    assert!(main_row.enabled);
    assert!(backup_row.enabled);
}

#[tokio::test]
async fn sync_prowlarr_indexers_does_not_rewrite_same_url_source() {
    let repository = Repository::connect_in_memory().await.unwrap();
    let main = DependencyName::new("main").unwrap();
    let backup = DependencyName::new("backup").unwrap();
    let url = "https://prowlarr.example/shared/api";
    let main_indexer = test_prowlarr_indexer("main", 101, "Movies", url);
    let backup_indexer = test_prowlarr_indexer("backup", 101, "Movies", url);

    let synced = repository
        .sync_prowlarr_indexers(
            &main,
            &[main_indexer],
            ProwlarrRemovePolicy::Deactivate,
            100,
        )
        .await
        .unwrap();
    let main_id = synced
        .iter()
        .find(|indexer| indexer.source_name == "main" && indexer.source_indexer_id == "101")
        .unwrap()
        .id;
    let rows = repository
        .sync_prowlarr_indexers(
            &backup,
            &[backup_indexer],
            ProwlarrRemovePolicy::Deactivate,
            200,
        )
        .await
        .unwrap();
    let main_row = rows
        .iter()
        .find(|indexer| indexer.source_name == "main" && indexer.source_indexer_id == "101")
        .unwrap();
    let backup_row = rows
        .iter()
        .find(|indexer| indexer.source_name == "backup" && indexer.source_indexer_id == "101")
        .unwrap();

    assert_eq!(main_id, main_row.id);
    assert!(!main_row.enabled);
    assert_ne!(main_id, backup_row.id);
    assert!(backup_row.enabled);
}

#[tokio::test]
async fn sync_prowlarr_indexers_rejects_mismatched_source_rows() {
    let repository = Repository::connect_in_memory().await.unwrap();
    let source = DependencyName::new("main").unwrap();
    let other = test_prowlarr_indexer("other", 101, "Movies", "https://prowlarr.example/101/api");

    let error = repository
        .sync_prowlarr_indexers(&source, &[other], ProwlarrRemovePolicy::Deactivate, 100)
        .await
        .unwrap_err();

    assert!(error.to_string().contains("belongs to source `other`"));
}

#[tokio::test]
async fn sync_prowlarr_indexers_deactivates_removals_and_reactivates() {
    let repository = Repository::connect_in_memory().await.unwrap();
    let source = DependencyName::new("main").unwrap();
    let first = test_prowlarr_indexer("main", 101, "Movies", "https://prowlarr.example/101/api");
    let second = test_prowlarr_indexer("main", 102, "TV", "https://prowlarr.example/102/api");

    let initial = repository
        .sync_prowlarr_indexers_with_summary(
            &source,
            &[first.clone(), second.clone()],
            ProwlarrRemovePolicy::Deactivate,
            100,
        )
        .await
        .unwrap();
    assert_eq!(2, initial.imported);
    assert_eq!(0, initial.deactivated);
    let removed = repository
        .sync_prowlarr_indexers_with_summary(
            &source,
            std::slice::from_ref(&first),
            ProwlarrRemovePolicy::Deactivate,
            200,
        )
        .await
        .unwrap();
    assert_eq!(1, removed.imported);
    assert_eq!(1, removed.deactivated);
    let tv = removed
        .registry
        .iter()
        .find(|indexer| indexer.source_indexer_id == "102")
        .unwrap();
    assert!(!tv.enabled);

    let unchanged = repository
        .sync_prowlarr_indexers_with_summary(
            &source,
            std::slice::from_ref(&first),
            ProwlarrRemovePolicy::Deactivate,
            250,
        )
        .await
        .unwrap();
    assert_eq!(1, unchanged.imported);
    assert_eq!(0, unchanged.deactivated);

    let reactivated = repository
        .sync_prowlarr_indexers_with_summary(
            &source,
            &[first, second],
            ProwlarrRemovePolicy::Deactivate,
            300,
        )
        .await
        .unwrap();
    assert_eq!(2, reactivated.imported);
    assert_eq!(0, reactivated.deactivated);
    let tv = reactivated
        .registry
        .iter()
        .find(|indexer| indexer.source_indexer_id == "102")
        .unwrap();
    assert!(tv.enabled);
}

#[tokio::test]
async fn sync_prowlarr_indexers_can_ignore_removals() {
    let repository = Repository::connect_in_memory().await.unwrap();
    let source = DependencyName::new("main").unwrap();
    let indexer = test_prowlarr_indexer("main", 101, "Movies", "https://prowlarr.example/101/api");

    repository
        .sync_prowlarr_indexers(&source, &[indexer], ProwlarrRemovePolicy::Deactivate, 100)
        .await
        .unwrap();
    let rows = repository
        .sync_prowlarr_indexers(&source, &[], ProwlarrRemovePolicy::Ignore, 200)
        .await
        .unwrap();
    let row = rows
        .iter()
        .find(|indexer| indexer.source_indexer_id == "101")
        .unwrap();

    assert!(row.enabled);
}

#[tokio::test]
async fn sync_prowlarr_indexers_can_take_over_disabled_static_url() {
    let repository = Repository::connect_in_memory().await.unwrap();
    let source = DependencyName::new("main").unwrap();
    let url = "https://prowlarr.example/101/api";
    let imported = test_prowlarr_indexer("main", 101, "Movies", url);

    repository
        .sync_torznab_indexers(&[test_indexer("legacy", url, ApiKeySource::Direct)], 100)
        .await
        .unwrap();
    let disabled_static = repository.sync_torznab_indexers(&[], 200).await.unwrap();
    let static_row = disabled_static
        .iter()
        .find(|indexer| indexer.source_kind == "static")
        .unwrap();
    assert!(!static_row.enabled);

    let rows = repository
        .sync_prowlarr_indexers(&source, &[imported], ProwlarrRemovePolicy::Deactivate, 300)
        .await
        .unwrap();
    let prowlarr_row = rows
        .iter()
        .find(|indexer| indexer.source_kind == "prowlarr")
        .unwrap();

    assert_eq!(url, prowlarr_row.url);
    assert!(prowlarr_row.enabled);
}

#[tokio::test]
async fn concurrent_prowlarr_sync_keeps_one_active_url() {
    let root = unique_temp_dir("prowlarr-active-url");
    let database = root.join("sporos.db");
    let repository = Repository::connect(&database).await.unwrap();
    let main_repository = repository.clone();
    let backup_repository = repository.clone();
    let main = DependencyName::new("main").unwrap();
    let backup = DependencyName::new("backup").unwrap();
    let url = "https://prowlarr.example/shared/api";
    let main_indexer = test_prowlarr_indexer("main", 101, "Movies", url);
    let backup_indexer = test_prowlarr_indexer("backup", 202, "Movies", url);

    let (main_result, backup_result) = tokio::join!(
        async move {
            main_repository
                .sync_prowlarr_indexers(
                    &main,
                    &[main_indexer],
                    ProwlarrRemovePolicy::Deactivate,
                    100,
                )
                .await
        },
        async move {
            backup_repository
                .sync_prowlarr_indexers(
                    &backup,
                    &[backup_indexer],
                    ProwlarrRemovePolicy::Deactivate,
                    100,
                )
                .await
        }
    );

    main_result.unwrap();
    backup_result.unwrap();
    let active_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM indexers WHERE url = ? AND enabled != 0")
            .bind(url)
            .fetch_one(repository.pool())
            .await
            .unwrap();

    assert_eq!(1, active_count);
    let active_source: String =
        sqlx::query_scalar("SELECT source_name FROM indexers WHERE url = ? AND enabled != 0")
            .bind(url)
            .fetch_one(repository.pool())
            .await
            .unwrap();
    assert_eq!("backup", active_source);

    fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn sync_prowlarr_indexers_resolves_static_duplicates() {
    let repository = Repository::connect_in_memory().await.unwrap();
    repository
        .sync_torznab_indexers(
            &[test_indexer(
                "main:Movies",
                "https://prowlarr.example/101/api",
                ApiKeySource::Direct,
            )],
            100,
        )
        .await
        .unwrap();
    let source = DependencyName::new("main").unwrap();
    let duplicate =
        test_prowlarr_indexer("main", 101, "Movies", "https://prowlarr.example/101/api");
    let distinct = test_prowlarr_indexer("main", 102, "Movies", "https://prowlarr.example/102/api");

    let rows = repository
        .sync_prowlarr_indexers(
            &source,
            &[duplicate, distinct],
            ProwlarrRemovePolicy::Deactivate,
            200,
        )
        .await
        .unwrap();
    let static_row = rows
        .iter()
        .find(|indexer| indexer.source_kind == "static")
        .unwrap();
    let imported = rows
        .iter()
        .find(|indexer| indexer.source_indexer_id == "102")
        .unwrap();

    assert!(static_row.enabled);
    assert_eq!("main:Movies#102", imported.name.as_str());
    assert!(imported.enabled);
    assert!(
        rows.iter()
            .all(|indexer| indexer.source_indexer_id != "101" || !indexer.enabled)
    );
}

#[tokio::test]
async fn static_sync_displaces_existing_prowlarr_duplicates() {
    let repository = Repository::connect_in_memory().await.unwrap();
    let source = DependencyName::new("main").unwrap();
    let imported = test_prowlarr_indexer("main", 101, "Movies", "https://prowlarr.example/101/api");
    let imported_rows = repository
        .sync_prowlarr_indexers(
            &source,
            std::slice::from_ref(&imported),
            ProwlarrRemovePolicy::Deactivate,
            100,
        )
        .await
        .unwrap();
    let imported_id = imported_rows
        .iter()
        .find(|indexer| indexer.source_indexer_id == "101")
        .unwrap()
        .id;

    let rows = repository
        .sync_torznab_indexers(
            &[test_indexer(
                "main:Movies",
                "https://prowlarr.example/101/api",
                ApiKeySource::Direct,
            )],
            200,
        )
        .await
        .unwrap();
    let static_row = rows
        .iter()
        .find(|indexer| indexer.source_kind == "static")
        .unwrap();
    let imported_row = rows
        .iter()
        .find(|indexer| indexer.source_indexer_id == "101")
        .unwrap();

    assert_ne!(imported_id, static_row.id);
    assert_eq!("main:Movies", static_row.name.as_str());
    assert!(static_row.enabled);
    assert_eq!(imported_id, imported_row.id);
    assert_eq!("prowlarr", imported_row.source_kind);
    assert_eq!("main:Movies#101", imported_row.name.as_str());
    assert!(!imported_row.enabled);

    let resynced = repository
        .sync_prowlarr_indexers(&source, &[imported], ProwlarrRemovePolicy::Deactivate, 300)
        .await
        .unwrap();
    let imported_row = resynced
        .iter()
        .find(|indexer| indexer.source_indexer_id == "101")
        .unwrap();
    assert_eq!(imported_id, imported_row.id);
    assert!(!imported_row.enabled);
}

#[tokio::test]
async fn static_sync_renames_imported_name_conflicts_without_url_conflict() {
    let repository = Repository::connect_in_memory().await.unwrap();
    let source = DependencyName::new("main").unwrap();
    let imported = test_prowlarr_indexer("main", 101, "Movies", "https://prowlarr.example/101/api");
    repository
        .sync_prowlarr_indexers(
            &source,
            std::slice::from_ref(&imported),
            ProwlarrRemovePolicy::Deactivate,
            100,
        )
        .await
        .unwrap();

    let rows = repository
        .sync_torznab_indexers(
            &[test_indexer(
                "main:Movies",
                "https://static.example/api",
                ApiKeySource::Direct,
            )],
            200,
        )
        .await
        .unwrap();
    let imported_row = rows
        .iter()
        .find(|indexer| indexer.source_indexer_id == "101")
        .unwrap();
    assert_eq!("main:Movies#101", imported_row.name.as_str());
    assert!(!imported_row.enabled);

    let resynced = repository
        .sync_prowlarr_indexers(&source, &[imported], ProwlarrRemovePolicy::Deactivate, 300)
        .await
        .unwrap();
    let imported_row = resynced
        .iter()
        .find(|indexer| indexer.source_indexer_id == "101")
        .unwrap();
    assert_eq!("main:Movies#101", imported_row.name.as_str());
    assert!(imported_row.enabled);
}

#[tokio::test]
async fn indexer_caps_updates_registry_and_dependency_health() {
    let repository = Repository::connect_in_memory().await.unwrap();
    let indexer = test_indexer(
        "main",
        "https://indexer.example/api",
        ApiKeySource::Env("INDEXER_KEY".to_owned()),
    );
    repository
        .sync_torznab_indexers(&[indexer], 100)
        .await
        .unwrap();
    let name = DependencyName::new("main").unwrap();
    let caps = parse_torznab_caps(
        r#"
            <caps>
              <searching><search available="yes"/></searching>
              <categories><category id="5000" name="TV"/></categories>
            </caps>
            "#,
    )
    .unwrap();

    repository
        .record_indexer_caps_success(&name, &caps, 200)
        .await
        .unwrap();
    let rows = repository.indexer_registry_snapshot(10).await.unwrap();
    let health = repository.dependency_health_snapshot(10).await.unwrap();

    assert_eq!("healthy", rows[0].state);
    assert_eq!(Some(200), rows[0].last_caps_refresh_at_ms);
    assert_eq!("healthy", health[0].state);

    repository
        .record_indexer_caps_failure(
            &name,
            &ReasonText::new("bad caps").unwrap(),
            Some(5_000),
            300,
        )
        .await
        .unwrap();
    let rows = repository.indexer_registry_snapshot(10).await.unwrap();
    let health = repository.dependency_health_snapshot(10).await.unwrap();

    assert_eq!("degraded", rows[0].state);
    assert_eq!(Some(5_000), rows[0].retry_after_ms);
    assert_eq!("degraded", health[0].state);
    assert_eq!(Some("bad caps".to_owned()), health[0].reason);
}

#[tokio::test]
async fn indexer_request_backoff_updates_retry_and_health_state() {
    let repository = Repository::connect_in_memory().await.unwrap();
    let indexer = test_indexer(
        "main",
        "https://indexer.example/api",
        ApiKeySource::Env("INDEXER_KEY".to_owned()),
    );
    repository
        .sync_torznab_indexers(&[indexer], 100)
        .await
        .unwrap();
    let name = DependencyName::new("main").unwrap();

    repository
        .record_indexer_request_backoff(
            &name,
            &ReasonText::new("rate limited").unwrap(),
            10_000,
            200,
            false,
        )
        .await
        .unwrap();

    let rows = repository.indexer_registry_snapshot(10).await.unwrap();
    let health = repository.dependency_health_snapshot(10).await.unwrap();

    assert_eq!("degraded", rows[0].state);
    assert_eq!(Some(10_000), rows[0].retry_after_ms);
    assert_eq!("degraded", health[0].state);
    assert_eq!(1, health[0].failure_count);

    repository
        .record_indexer_request_backoff(
            &name,
            &ReasonText::new("network unavailable").unwrap(),
            20_000,
            300,
            true,
        )
        .await
        .unwrap();

    let rows = repository.indexer_registry_snapshot(10).await.unwrap();
    let health = repository.dependency_health_snapshot(10).await.unwrap();

    assert_eq!("unavailable", rows[0].state);
    assert_eq!(Some(20_000), rows[0].retry_after_ms);
    assert_eq!("unavailable", health[0].state);
    assert_eq!(Some("network unavailable".to_owned()), health[0].reason);
    assert_eq!(2, health[0].failure_count);

    repository
        .record_indexer_caps_success(&name, &TorznabCaps::default(), 400)
        .await
        .unwrap();
    let health = repository.dependency_health_snapshot(10).await.unwrap();
    assert_eq!("healthy", health[0].state);
    assert_eq!(0, health[0].failure_count);
}

#[tokio::test]
async fn search_history_updates_only_for_non_rate_limited_searches() {
    let repository = Repository::connect_in_memory().await.unwrap();
    let item_id = repository
        .upsert_local_item_with_files(&test_local_item("Example"), &[])
        .await
        .unwrap();
    repository
        .sync_torznab_indexers(
            &[test_indexer(
                "main",
                "https://indexer.example/api",
                ApiKeySource::Direct,
            )],
            100,
        )
        .await
        .unwrap();
    let indexer_id =
        IndexerId::new(repository.indexer_registry_snapshot(10).await.unwrap()[0].id).unwrap();

    repository
        .record_search_history(item_id, indexer_id, 200, false)
        .await
        .unwrap();
    repository
        .record_search_history(item_id, indexer_id, 300, false)
        .await
        .unwrap();
    repository
        .record_search_history(item_id, indexer_id, 150, false)
        .await
        .unwrap();
    repository
        .record_search_history(item_id, indexer_id, 400, true)
        .await
        .unwrap();

    let history = repository
        .search_history_for_item(item_id, 10)
        .await
        .unwrap();

    assert_eq!(
        vec![SearchHistoryRow {
            local_item_id: item_id,
            indexer_id,
            first_searched_at_ms: 150,
            last_searched_at_ms: 300,
        }],
        history
    );
}

#[tokio::test]
async fn match_decision_uses_foreign_keys_for_failure_paths() {
    let repository = Repository::connect_in_memory().await.unwrap();
    let assessment = CandidateAssessment {
        decision: MatchDecision::Exact,
        reason: DecisionReason::FileTreeMatched,
        matched_size: Some(ByteSize::new(42)),
        matched_ratio: Some(MatchRatio::new(1.0).unwrap()),
    };

    let result = repository
        .record_match_decision(
            LocalItemId::new(9_999).unwrap(),
            RemoteCandidateId::new(8_888).unwrap(),
            assessment,
            1_700_000_000_000,
        )
        .await;

    assert!(matches!(result, Err(DatabaseError::QueryFailed { .. })));
}

#[tokio::test]
async fn match_decision_records_and_reassesses_candidate_for_local_item() {
    let repository = Repository::connect_in_memory().await.unwrap();
    let item_id = repository
        .upsert_local_item_with_files(&test_local_item("Example"), &[])
        .await
        .unwrap();
    let candidate_id = repository
        .upsert_remote_candidate(&test_remote_candidate("guid-1", "Example"))
        .await
        .unwrap();

    repository
        .record_match_decision(
            item_id,
            candidate_id,
            CandidateAssessment {
                decision: MatchDecision::Exact,
                reason: DecisionReason::FileTreeMatched,
                matched_size: Some(ByteSize::new(42)),
                matched_ratio: Some(MatchRatio::new(1.0).unwrap()),
            },
            100,
        )
        .await
        .unwrap();
    repository
        .record_match_decision(
            item_id,
            candidate_id,
            CandidateAssessment {
                decision: MatchDecision::NoMatch,
                reason: DecisionReason::NameMismatch,
                matched_size: Some(ByteSize::new(1)),
                matched_ratio: Some(MatchRatio::new(0.1).unwrap()),
            },
            200,
        )
        .await
        .unwrap();

    let row =
        sqlx::query("SELECT decision, matched_size, reason_code, assessed_at FROM match_decisions")
            .fetch_one(repository.pool())
            .await
            .unwrap();
    let decision: String = row.get("decision");
    let matched_size: i64 = row.get("matched_size");
    let reason_code: String = row.get("reason_code");
    let assessed_at: i64 = row.get("assessed_at");
    let decision_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM match_decisions")
        .fetch_one(repository.pool())
        .await
        .unwrap();
    let decisions = repository
        .match_decisions_for_local_item(item_id, 10)
        .await
        .unwrap();

    assert_eq!(1, decision_count);
    assert_eq!("no_match", decision);
    assert_eq!(1, matched_size);
    assert_eq!("name_mismatch", reason_code);
    assert_eq!(200, assessed_at);
    assert_eq!(1, decisions.len());
    assert_eq!(candidate_id, decisions[0].candidate_id);
    assert_eq!("no_match", decisions[0].decision);
    assert_eq!(Some(1), decisions[0].matched_size);
    assert_eq!(Some(0.1), decisions[0].matched_ratio);
}

#[tokio::test]
async fn delete_cascades_remove_owned_files_and_match_decisions() {
    let repository = Repository::connect_in_memory().await.unwrap();
    let item_id = repository
        .upsert_local_item_with_files(
            &test_local_item("Example"),
            &[LocalFile::new(
                None,
                PathBuf::from("Example/file-a.mkv"),
                ByteSize::new(10),
                FileIndex::new(0),
            )
            .unwrap()],
        )
        .await
        .unwrap();
    let candidate_id = repository
        .upsert_remote_candidate(&test_remote_candidate("guid-1", "Example"))
        .await
        .unwrap();
    let assessment = CandidateAssessment {
        decision: MatchDecision::Exact,
        reason: DecisionReason::FileTreeMatched,
        matched_size: Some(ByteSize::new(10)),
        matched_ratio: Some(MatchRatio::new(1.0).unwrap()),
    };

    repository
        .record_match_decision(item_id, candidate_id, assessment, 100)
        .await
        .unwrap();
    sqlx::query("DELETE FROM local_items WHERE id = ?")
        .bind(i64_from_u64(item_id.get(), "local item id").unwrap())
        .execute(repository.pool())
        .await
        .unwrap();

    let file_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM local_files")
        .fetch_one(repository.pool())
        .await
        .unwrap();
    let decision_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM match_decisions")
        .fetch_one(repository.pool())
        .await
        .unwrap();
    assert_eq!(0, file_count);
    assert_eq!(0, decision_count);

    let next_item_id = repository
        .upsert_local_item_with_files(&test_local_item("Example 2"), &[])
        .await
        .unwrap();
    repository
        .record_match_decision(next_item_id, candidate_id, assessment, 200)
        .await
        .unwrap();
    sqlx::query("DELETE FROM remote_candidates WHERE id = ?")
        .bind(i64_from_u64(candidate_id.get(), "remote candidate id").unwrap())
        .execute(repository.pool())
        .await
        .unwrap();

    let decision_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM match_decisions")
        .fetch_one(repository.pool())
        .await
        .unwrap();
    assert_eq!(0, decision_count);
}

#[tokio::test]
async fn records_jobs_and_dependency_health() {
    let repository = Repository::connect_in_memory().await.unwrap();
    let dependency_name = DependencyName::new("indexer-main").unwrap();
    let dependency_state = DependencyState::Degraded {
        reason: ReasonText::new("rate limited").unwrap(),
        retry_after_ms: Some(123),
    };

    repository
        .record_job_status(
            &JobName::new("rss").unwrap(),
            JobStateUpdate {
                state: JobState::Running,
                last_started_at_ms: Some(100),
                last_finished_at_ms: None,
                next_run_at_ms: Some(456),
                last_error: None,
            },
        )
        .await
        .unwrap();
    repository
        .record_job_status(
            &JobName::new("rss").unwrap(),
            JobStateUpdate {
                state: JobState::Waiting,
                last_started_at_ms: None,
                last_finished_at_ms: Some(200),
                next_run_at_ms: Some(456),
                last_error: Some("rate limited"),
            },
        )
        .await
        .unwrap();
    repository
        .upsert_job_state(
            &JobName::new("cleanup").unwrap(),
            JobState::Pending,
            Some(10),
            None,
        )
        .await
        .unwrap();
    repository
        .upsert_job_state(
            &JobName::new("disabled").unwrap(),
            JobState::Disabled,
            Some(1),
            None,
        )
        .await
        .unwrap();
    repository
        .record_dependency_health("indexer", &dependency_name, &dependency_state, 789)
        .await
        .unwrap();
    repository
        .record_dependency_health(
            "client",
            &DependencyName::new("qbit").unwrap(),
            &DependencyState::Healthy { checked_at_ms: 900 },
            900,
        )
        .await
        .unwrap();

    let ready = repository.ready_jobs(10, 10).await.unwrap();
    let jobs = repository.job_status_snapshot(10).await.unwrap();
    let health = repository.dependency_health_snapshot(1).await.unwrap();

    assert_eq!(vec![JobName::new("cleanup").unwrap()], ready);
    assert_eq!(3, jobs.len());
    let rss = jobs.iter().find(|job| job.name.as_str() == "rss").unwrap();
    assert_eq!("waiting", rss.state);
    assert_eq!(Some(100), rss.last_started_at_ms);
    assert_eq!(Some(200), rss.last_finished_at_ms);
    assert_eq!(Some(456), rss.next_run_at_ms);
    assert_eq!(Some("rate limited".to_owned()), rss.last_error);
    assert_eq!(1, health.len());
    assert_eq!("client", health[0].dependency_type);
    assert_eq!("healthy", health[0].state);
}

#[tokio::test]
async fn announce_insert_deduplicates_and_enforces_capacity() {
    let repository = Repository::connect_in_memory().await.unwrap();
    let work = test_announce_work("ann_01", "guid-1", 1);
    let duplicate = test_announce_work("ann_02", "guid-1", 2);
    let other = test_announce_work("ann_03", "guid-2", 3);

    let inserted = repository
        .insert_or_dedupe_announce_work(&work, 1)
        .await
        .unwrap();
    let deduped = repository
        .insert_or_dedupe_announce_work(&duplicate, 1)
        .await
        .unwrap();
    let full = repository.insert_or_dedupe_announce_work(&other, 1).await;

    assert_eq!(
        AnnounceInsertResult::Inserted {
            id: AnnounceWorkId::new("ann_01").unwrap()
        },
        inserted
    );
    assert_eq!(
        AnnounceInsertResult::Deduplicated {
            id: AnnounceWorkId::new("ann_01").unwrap()
        },
        deduped
    );
    assert!(matches!(full, Err(DatabaseError::Busy { .. })));
}

#[tokio::test]
async fn concurrent_announce_inserts_enforce_capacity_atomically() {
    let root = unique_temp_dir("announce-atomic-insert");
    let database = root.join("sporos.db");
    let repository = Repository::connect(&database).await.unwrap();
    let first_work = test_announce_work("ann_atomic_insert_1", "guid-atomic-insert-1", 1);
    let second_work = test_announce_work("ann_atomic_insert_2", "guid-atomic-insert-2", 2);

    let barrier = Arc::new(Barrier::new(2));
    let first_repository = repository
        .clone()
        .with_announce_insert_barrier(barrier.clone());
    let second_repository = repository
        .clone()
        .with_announce_insert_barrier(barrier.clone());
    let (first, second) = tokio::join!(
        first_repository.insert_or_dedupe_announce_work(&first_work, 1),
        second_repository.insert_or_dedupe_announce_work(&second_work, 1)
    );

    let mut inserted = 0;
    let mut rejected = 0;
    for result in [first, second] {
        match result {
            Ok(AnnounceInsertResult::Inserted { .. }) => inserted += 1,
            Ok(AnnounceInsertResult::Deduplicated { id }) => {
                panic!("distinct announce work deduplicated unexpectedly: {id}");
            }
            Err(DatabaseError::Busy { operation, .. }) => {
                assert_eq!("accept announce work", operation);
                rejected += 1;
            }
            Err(error) => panic!("unexpected announce insert error: {error}"),
        }
    }
    let stored: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM announce_work")
        .fetch_one(repository.pool())
        .await
        .unwrap();

    assert_eq!(1, inserted);
    assert_eq!(1, rejected);
    assert_eq!(1, stored);
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn concurrent_duplicate_announce_inserts_dedupe() {
    let root = unique_temp_dir("announce-atomic-dedupe");
    let database = root.join("sporos.db");
    let repository = Repository::connect(&database).await.unwrap();
    let first_work = test_announce_work("ann_atomic_dedupe_1", "guid-atomic-dedupe", 1);
    let second_work = test_announce_work("ann_atomic_dedupe_2", "guid-atomic-dedupe", 2);

    let barrier = Arc::new(Barrier::new(2));
    let first_repository = repository
        .clone()
        .with_announce_insert_barrier(barrier.clone());
    let second_repository = repository
        .clone()
        .with_announce_insert_barrier(barrier.clone());
    let (first, second) = tokio::join!(
        first_repository.insert_or_dedupe_announce_work(&first_work, 1),
        second_repository.insert_or_dedupe_announce_work(&second_work, 1)
    );

    let mut inserted_id = None;
    let mut deduped_id = None;
    for result in [first, second] {
        match result {
            Ok(AnnounceInsertResult::Inserted { id }) => inserted_id = Some(id),
            Ok(AnnounceInsertResult::Deduplicated { id }) => deduped_id = Some(id),
            Err(error) => panic!("unexpected announce insert error: {error}"),
        }
    }
    let inserted_id = inserted_id.expect("one insert should win");
    let deduped_id = deduped_id.expect("one duplicate should dedupe");
    let stored: Vec<String> = sqlx::query_scalar("SELECT id FROM announce_work ORDER BY id")
        .fetch_all(repository.pool())
        .await
        .unwrap();

    assert_eq!(inserted_id, deduped_id);
    assert_eq!(vec![inserted_id.as_str().to_owned()], stored);
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn announce_claim_lease_retry_and_success_flow() {
    let repository = Repository::connect_in_memory().await.unwrap();
    let work = test_announce_work("ann_10", "guid-10", 1);
    repository
        .insert_or_dedupe_announce_work(&work, 10)
        .await
        .unwrap();

    let claimed = repository
        .claim_announce_work("worker-1", 10, 100, 5)
        .await
        .unwrap();
    assert_eq!(vec![AnnounceWorkId::new("ann_10").unwrap()], claimed);
    assert!(
        repository
            .renew_announce_lease(&claimed[0], "worker-1", 120, 20)
            .await
            .unwrap()
    );
    assert!(
        repository
            .mark_announce_retryable(
                &claimed[0],
                "worker-1",
                AnnounceRetryUpdate {
                    reason: AnnounceReason::RetryAfter,
                    next_attempt_at_ms: 50,
                    now_ms: 25,
                    error_class: "retryable_dependency",
                    redacted_message: "rate limited",
                },
            )
            .await
            .unwrap()
    );

    let early = repository
        .claim_announce_work("worker-1", 49, 150, 5)
        .await
        .unwrap();
    let ready = repository
        .claim_announce_work("worker-1", 50, 150, 5)
        .await
        .unwrap();
    assert!(early.is_empty());
    assert_eq!(vec![AnnounceWorkId::new("ann_10").unwrap()], ready);
    assert!(
        repository
            .mark_announce_succeeded(
                &ready[0],
                "worker-1",
                AnnounceReason::Injected,
                "injected",
                60
            )
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn announce_insert_persists_fetch_material() {
    let repository = Repository::connect_in_memory().await.unwrap();
    let mut work = test_announce_work("ann_fetch", "guid-fetch", 1);
    let download_url =
        DownloadUrl::new("https://tracker.example/download?id=1&apikey=secret").unwrap();
    work.fetch = Some(
        AnnounceFetchMaterial::new(
            &download_url,
            Some(CookieSecret::new("sid=secret-cookie").unwrap()),
        )
        .unwrap(),
    );

    repository
        .insert_or_dedupe_announce_work(&work, 10)
        .await
        .unwrap();
    let stored = repository
        .announce_fetch_material(&work.id)
        .await
        .unwrap()
        .unwrap();
    let redacted: String =
        sqlx::query_scalar("SELECT redacted_download_url FROM announce_work WHERE id = ?")
            .bind(work.id.as_str())
            .fetch_one(repository.pool())
            .await
            .unwrap();

    assert_eq!(download_url.as_str(), stored.expose_download_url());
    assert_eq!(
        "sid=secret-cookie",
        stored.cookie().unwrap().expose_secret()
    );
    assert!(redacted.contains("[REDACTED]"));
    assert!(!redacted.contains("secret"));
}

#[tokio::test]
async fn terminal_announce_transitions_clear_fetch_material() {
    let repository = Repository::connect_in_memory().await.unwrap();
    let mut work = test_announce_work("ann_fetch_clear", "guid-fetch-clear", 1);
    let download_url =
        DownloadUrl::new("https://tracker.example/download?id=1&apikey=secret").unwrap();
    work.fetch = Some(
        AnnounceFetchMaterial::new(
            &download_url,
            Some(CookieSecret::new("sid=secret-cookie").unwrap()),
        )
        .unwrap(),
    );
    repository
        .insert_or_dedupe_announce_work(&work, 10)
        .await
        .unwrap();
    let claimed = repository
        .claim_announce_work("worker-1", 1, 100, 1)
        .await
        .unwrap();

    assert!(
        repository
            .mark_announce_succeeded(
                &claimed[0],
                "worker-1",
                AnnounceReason::Injected,
                "injected",
                2,
            )
            .await
            .unwrap()
    );

    assert!(
        repository
            .announce_fetch_material(&work.id)
            .await
            .unwrap()
            .is_none()
    );
    assert_announce_fetch_columns_cleared(&repository, work.id.as_str()).await;
}

#[tokio::test]
async fn expired_announce_work_clears_fetch_material() {
    let repository = Repository::connect_in_memory().await.unwrap();
    let mut work = test_announce_work("ann_fetch_expired", "guid-fetch-expired", 1);
    let download_url =
        DownloadUrl::new("https://tracker.example/download?id=1&apikey=secret").unwrap();
    work.fetch = Some(
        AnnounceFetchMaterial::new(
            &download_url,
            Some(CookieSecret::new("sid=secret-cookie").unwrap()),
        )
        .unwrap(),
    );
    work.expires_at_ms = 10;
    repository
        .insert_or_dedupe_announce_work(&work, 10)
        .await
        .unwrap();

    assert_eq!(1, repository.expire_announce_work(10).await.unwrap());

    assert!(
        repository
            .announce_fetch_material(&work.id)
            .await
            .unwrap()
            .is_none()
    );
    assert_announce_fetch_columns_cleared(&repository, work.id.as_str()).await;
}

#[tokio::test]
async fn announce_expiry_and_lease_recovery_batches_are_bounded() {
    let repository = Repository::connect_in_memory().await.unwrap();
    for index in 0..3 {
        let mut expired = test_announce_work(
            &format!("ann_expired_{index}"),
            &format!("guid-expired-{index}"),
            index,
        );
        expired.expires_at_ms = 10;
        repository
            .insert_or_dedupe_announce_work(&expired, 10)
            .await
            .unwrap();

        let running = test_announce_work(
            &format!("ann_running_{index}"),
            &format!("guid-running-{index}"),
            index + 10,
        );
        repository
            .insert_or_dedupe_announce_work(&running, 10)
            .await
            .unwrap();
    }
    sqlx::query(
        r#"
            UPDATE announce_work
            SET status = 'running',
                lease_owner = 'worker-1',
                lease_until = 10,
                expires_at = CASE
                    WHEN id = 'ann_running_0' THEN 10
                    ELSE 20
                END
            WHERE id LIKE 'ann_running_%'
            "#,
    )
    .execute(repository.pool())
    .await
    .unwrap();

    assert_eq!(
        2,
        repository.expire_announce_work_batch(10, 2).await.unwrap()
    );
    assert_eq!(
        2,
        repository
            .recover_stale_announce_leases_batch(10, 2)
            .await
            .unwrap()
    );
    let status_counts =
        sqlx::query("SELECT status, COUNT(*) AS count FROM announce_work GROUP BY status")
            .fetch_all(repository.pool())
            .await
            .unwrap()
            .into_iter()
            .map(|row| (row.get::<String, _>("status"), row.get::<i64, _>("count")))
            .collect::<Vec<_>>();

    assert_eq!(
        Some(3),
        status_counts
            .iter()
            .find(|(status, _count)| status == "queued")
            .map(|(_status, count)| *count)
    );
    assert_eq!(
        Some(2),
        status_counts
            .iter()
            .find(|(status, _count)| status == "expired")
            .map(|(_status, count)| *count)
    );
    assert_eq!(
        Some(1),
        status_counts
            .iter()
            .find(|(status, _count)| status == "running")
            .map(|(_status, count)| *count)
    );
}

async fn assert_announce_fetch_columns_cleared(repository: &Repository, id: &str) {
    let (download_url, cookie): (Option<String>, Option<String>) =
        sqlx::query_as("SELECT download_url, cookie FROM announce_work WHERE id = ?")
            .bind(id)
            .fetch_one(repository.pool())
            .await
            .unwrap();

    assert!(download_url.is_none());
    assert!(cookie.is_none());
}

#[tokio::test]
async fn concurrent_announce_claims_do_not_steal_leases() {
    let root = unique_temp_dir("announce-atomic-claim");
    let database = root.join("sporos.db");
    let repository = Repository::connect(&database).await.unwrap();
    repository
        .insert_or_dedupe_announce_work(&test_announce_work("ann_atomic", "guid-atomic", 1), 10)
        .await
        .unwrap();

    let first_repository = repository.clone();
    let second_repository = repository.clone();
    let (first, second) = tokio::join!(
        first_repository.claim_announce_work("worker-1", 10, 100, 1),
        second_repository.claim_announce_work("worker-2", 10, 100, 1)
    );
    let first = claimed_or_busy_empty(first);
    let second = claimed_or_busy_empty(second);
    let total_claims = first.len() + second.len();
    let owner: String =
        sqlx::query_scalar("SELECT lease_owner FROM announce_work WHERE id = 'ann_atomic'")
            .fetch_one(repository.pool())
            .await
            .unwrap();

    assert_eq!(1, total_claims);
    assert!(
        (first.is_empty() && owner == "worker-2") || (second.is_empty() && owner == "worker-1")
    );

    fs::remove_dir_all(root).unwrap();
}

fn claimed_or_busy_empty(
    result: Result<Vec<AnnounceWorkId>, DatabaseError>,
) -> Vec<AnnounceWorkId> {
    match result {
        Ok(claimed) => claimed,
        Err(DatabaseError::Busy { .. }) => Vec::new(),
        Err(error) => panic!("unexpected claim error: {error}"),
    }
}

#[tokio::test]
async fn announce_expiry_recovery_and_status_snapshots_work() {
    let repository = Repository::connect_in_memory().await.unwrap();
    let mut expired = test_announce_work("ann_20", "guid-20", 1);
    expired.expires_at_ms = 5;
    let mut running = test_announce_work("ann_21", "guid-21", 1);
    running.status = AnnounceStatus::Running;
    running.lease = Some(AnnounceLease::new(ReasonText::new("worker-1").unwrap(), 5, 1).unwrap());

    repository
        .insert_or_dedupe_announce_work(&expired, 10)
        .await
        .unwrap();
    repository
        .insert_or_dedupe_announce_work(&running, 10)
        .await
        .unwrap();

    assert_eq!(1, repository.expire_announce_work(10).await.unwrap());
    assert_eq!(
        1,
        repository.recover_stale_announce_leases(10).await.unwrap()
    );

    let stats = repository.announce_status_counts(10).await.unwrap();

    assert!(
        stats.iter().all(|count| count.status != "expired"),
        "hot announce status counts should exclude retained terminal rows: {stats:?}"
    );
    assert!(
        stats
            .iter()
            .any(|count| count.status == "queued" && count.count == 1)
    );
}

#[tokio::test]
async fn terminal_announce_cleanup_uses_success_and_failure_retention() {
    let repository = Repository::connect_in_memory().await.unwrap();
    for (id, guid, status, finished_at_ms) in [
        (
            "ann_success_old",
            "guid-success-old",
            AnnounceStatus::Succeeded,
            999,
        ),
        (
            "ann_success_retained",
            "guid-success-retained",
            AnnounceStatus::Succeeded,
            1001,
        ),
        (
            "ann_failed_old",
            "guid-failed-old",
            AnnounceStatus::TerminalFailed,
            1999,
        ),
        (
            "ann_failed_retained",
            "guid-failed-retained",
            AnnounceStatus::TerminalFailed,
            2001,
        ),
        (
            "ann_expired_old",
            "guid-expired-old",
            AnnounceStatus::Expired,
            1999,
        ),
        (
            "ann_expired_retained",
            "guid-expired-retained",
            AnnounceStatus::Expired,
            2001,
        ),
    ] {
        let mut work = test_announce_work(id, guid, 1);
        work.status = status;
        work.finished_at_ms = Some(finished_at_ms);
        work.updated_at_ms = finished_at_ms;
        repository
            .insert_or_dedupe_announce_work(&work, 10)
            .await
            .unwrap();
    }
    repository
        .insert_or_dedupe_announce_work(&test_announce_work("ann_queued_old", "guid-queued", 1), 10)
        .await
        .unwrap();

    let deleted = repository
        .cleanup_terminal_announce_work(1_000, 2_000, 10)
        .await
        .unwrap();
    let remaining: Vec<String> = sqlx::query_scalar("SELECT id FROM announce_work ORDER BY id")
        .fetch_all(repository.pool())
        .await
        .unwrap();

    assert_eq!(3, deleted);
    assert_eq!(
        vec![
            "ann_expired_retained".to_owned(),
            "ann_failed_retained".to_owned(),
            "ann_queued_old".to_owned(),
            "ann_success_retained".to_owned(),
        ],
        remaining
    );
}

#[tokio::test]
async fn terminal_announce_cleanup_uses_retention_indexes() {
    let repository = Repository::connect_in_memory().await.unwrap();
    for (query, index_name) in [
        (
            r#"
            SELECT id
            FROM announce_work
            WHERE status = 'succeeded'
              AND finished_at IS NOT NULL
              AND finished_at <= ?
            ORDER BY finished_at, id
            LIMIT ?
            "#,
            "idx_announce_work_succeeded_retention",
        ),
        (
            r#"
            SELECT id
            FROM announce_work
            WHERE status = 'terminal_failed'
              AND finished_at IS NOT NULL
              AND finished_at <= ?
            ORDER BY finished_at, id
            LIMIT ?
            "#,
            "idx_announce_work_terminal_failed_retention",
        ),
        (
            r#"
            SELECT id
            FROM announce_work
            WHERE status = 'expired'
              AND finished_at IS NOT NULL
              AND finished_at <= ?
            ORDER BY finished_at, id
            LIMIT ?
            "#,
            "idx_announce_work_expired_retention",
        ),
    ] {
        let plan = explain_query_plan(&repository, query, 10_000, 100).await;

        assert!(
            !plan
                .iter()
                .any(|detail| detail.contains("SCAN announce_work")),
            "retention cleanup should not scan announce_work: {plan:?}"
        );
        assert!(
            !plan.iter().any(|detail| detail.contains("USE TEMP B-TREE")),
            "retention cleanup should not sort with a temp b-tree: {plan:?}"
        );
        assert!(
            plan.iter().any(|detail| detail.contains(index_name)),
            "retention cleanup should use a retention index: {plan:?}"
        );
    }
}

#[tokio::test]
async fn announce_queue_snapshot_queries_avoid_retained_table_scans() {
    let repository = Repository::connect_in_memory().await.unwrap();
    for (label, query) in [
        (
            "active queue summary",
            r#"
                SELECT
                    COUNT(*) AS active_count,
                    MIN(received_at) AS oldest_received_at,
                    MIN(CASE
                        WHEN status IN ('queued', 'retryable', 'waiting')
                        THEN next_attempt_at
                    END) AS next_attempt_at,
                    COALESCE(SUM(CASE
                        WHEN status = 'running' AND lease_owner IS NOT NULL
                        THEN 1 ELSE 0
                    END), 0) AS running_leases
                FROM announce_work
                WHERE status IN ('queued', 'running', 'waiting', 'retryable')
                "#,
        ),
        (
            "active status counts",
            r#"
                SELECT status, reason, COUNT(*) AS count
                FROM announce_work
                WHERE status = 'queued'
                GROUP BY status, reason
                ORDER BY status, reason
                LIMIT 100
                "#,
        ),
        (
            "active attempt rows",
            r#"
                SELECT last_error_class, last_action_outcome, reason, status, attempt_count
                FROM announce_work
                WHERE status = 'retryable'
                  AND attempt_count > 0
                "#,
        ),
        (
            "active dependency rows",
            r#"
                SELECT last_dependency_kind, last_dependency_name
                FROM announce_work
                WHERE status = 'waiting'
                  AND last_dependency_kind IS NOT NULL
                  AND last_dependency_name IS NOT NULL
                ORDER BY last_dependency_kind, last_dependency_name
                "#,
        ),
    ] {
        let plan = explain_query_plan_raw(&repository, query).await;

        assert!(
            !plan
                .iter()
                .any(|detail| detail.contains("SCAN announce_work")),
            "{label} should not scan retained announce_work rows: {plan:?}"
        );
        assert!(
            !plan.iter().any(|detail| detail.contains("USE TEMP B-TREE")),
            "{label} should not sort with a temp b-tree: {plan:?}"
        );
    }
}

#[tokio::test]
async fn announce_maintenance_queries_use_ordered_indexes() {
    let repository = Repository::connect_in_memory().await.unwrap();
    for (label, query, index_name) in [
        (
            "expiry batch",
            r#"
                SELECT id
                FROM announce_work INDEXED BY idx_announce_work_expires_at
                WHERE status IN ('queued', 'running', 'waiting', 'retryable')
                  AND expires_at <= 100
                ORDER BY expires_at, id
                LIMIT 100
                "#,
            "idx_announce_work_expires_at",
        ),
        (
            "stale lease recovery batch",
            r#"
                SELECT id
                FROM announce_work INDEXED BY idx_announce_work_lease_until
                WHERE status = 'running'
                  AND lease_until <= 100
                  AND expires_at > 100
                ORDER BY lease_until, id
                LIMIT 100
                "#,
            "idx_announce_work_lease_until",
        ),
        (
            "dependency scheduler",
            r#"
                SELECT work.id
                FROM announce_work AS work INDEXED BY idx_announce_work_dependency_schedule
                LEFT JOIN dependency_health health
                  ON health.dependency_type = work.last_dependency_kind
                 AND health.dependency_name = work.last_dependency_name
                WHERE work.status IN ('queued', 'retryable', 'waiting')
                  AND work.expires_at > 100
                  AND work.last_dependency_kind IS NOT NULL
                  AND work.last_dependency_name IS NOT NULL
                ORDER BY work.next_attempt_at, work.received_at
                LIMIT 100
                "#,
            "idx_announce_work_dependency_schedule",
        ),
        (
            "inventory wakeup",
            r#"
                SELECT id FROM announce_work INDEXED BY idx_announce_work_inventory_wakeup
                WHERE status = 'waiting'
                  AND expires_at > 100
                  AND reason IN ('source_incomplete', 'inventory_refreshing')
                  AND last_dependency_kind IS NULL
                  AND last_dependency_name IS NULL
                ORDER BY next_attempt_at, received_at
                LIMIT 100
                "#,
            "idx_announce_work_inventory_wakeup",
        ),
        (
            "due waiting wakeup",
            r#"
                SELECT id FROM announce_work INDEXED BY idx_announce_work_waiting_due
                WHERE status = 'waiting'
                  AND next_attempt_at <= 100
                  AND expires_at > 100
                  AND last_dependency_kind IS NULL
                  AND last_dependency_name IS NULL
                ORDER BY next_attempt_at, received_at
                LIMIT 100
                "#,
            "idx_announce_work_waiting_due",
        ),
        (
            "dependency wakeup",
            r#"
                SELECT id FROM announce_work INDEXED BY idx_announce_work_waiting_dependency_due
                WHERE status = 'waiting'
                  AND expires_at > 100
                  AND last_dependency_kind = 'indexer'
                  AND last_dependency_name = 'main'
                ORDER BY next_attempt_at, received_at
                LIMIT 100
                "#,
            "idx_announce_work_waiting_dependency_due",
        ),
    ] {
        let plan = explain_query_plan_raw(&repository, query).await;

        assert!(
            !plan.iter().any(|detail| detail.contains("USE TEMP B-TREE")),
            "{label} should not sort with a temp b-tree: {plan:?}"
        );
        assert!(
            plan.iter().any(|detail| detail.contains(index_name)),
            "{label} should use {index_name}: {plan:?}"
        );
        assert!(
            !plan
                .iter()
                .any(|detail| detail.contains("idx_announce_work_status_reason")),
            "{label} should not fall back to the broad status/reason index: {plan:?}"
        );
    }
}

#[tokio::test]
async fn scheduled_indexer_pages_use_targeted_indexes() {
    let repository = Repository::connect_in_memory().await.unwrap();
    for (label, query, index_name, expected_search) in [
        (
            "due caps first page",
            r#"
                SELECT id, name, retry_after, last_caps_refresh_at
                FROM indexers INDEXED BY idx_indexers_due_page
                WHERE enabled = 1
                  AND retry_after IS NULL
                ORDER BY name
                LIMIT 512
                "#,
            "idx_indexers_due_page",
            "enabled=? AND retry_after=?",
        ),
        (
            "due caps keyset page",
            r#"
                SELECT id, name, retry_after, last_caps_refresh_at
                FROM indexers INDEXED BY idx_indexers_due_page
                WHERE enabled = 1
                  AND retry_after IS NULL
                  AND name > 'main'
                ORDER BY name
                LIMIT 512
                "#,
            "idx_indexers_due_page",
            "enabled=? AND retry_after=? AND name>?",
        ),
        (
            "search-ready first page",
            r#"
                SELECT id, name, retry_after, capabilities_json
                FROM indexers INDEXED BY idx_indexers_search_ready_page
                WHERE enabled = 1
                  AND retry_after IS NULL
                  AND last_caps_refresh_at IS NOT NULL
                ORDER BY name
                LIMIT 512
                "#,
            "idx_indexers_search_ready_page",
            "enabled=? AND retry_after=?",
        ),
        (
            "search-ready keyset page",
            r#"
                SELECT id, name, retry_after, capabilities_json
                FROM indexers INDEXED BY idx_indexers_search_ready_page
                WHERE enabled = 1
                  AND retry_after IS NULL
                  AND name > 'main'
                  AND last_caps_refresh_at IS NOT NULL
                ORDER BY name
                LIMIT 512
                "#,
            "idx_indexers_search_ready_page",
            "enabled=? AND retry_after=? AND name>?",
        ),
    ] {
        let plan = explain_query_plan_raw(&repository, query).await;

        assert!(
            !plan.iter().any(|detail| detail.contains("USE TEMP B-TREE")),
            "{label} should not sort with a temp b-tree: {plan:?}"
        );
        assert!(
            plan.iter().any(|detail| detail.contains(index_name)),
            "{label} should use {index_name}: {plan:?}"
        );
        assert!(
            plan.iter().any(|detail| detail.contains(expected_search)),
            "{label} should constrain the targeted index search with {expected_search}: {plan:?}"
        );
        assert!(
            !plan
                .iter()
                .any(|detail| detail.contains("sqlite_autoindex_indexers")),
            "{label} should not scan the unique name index: {plan:?}"
        );
    }
}

#[tokio::test]
async fn candidate_and_decision_history_queries_use_ordered_indexes() {
    let repository = Repository::connect_in_memory().await.unwrap();
    for (label, query, index_name, expected_search) in [
        (
            "candidate info-hash history",
            r#"
                SELECT id, indexer_id, guid, redacted_download_url, title, info_hash, torrent_cache_path
                FROM remote_candidates
                WHERE info_hash = '0123456789abcdef0123456789abcdef01234567'
                ORDER BY last_seen_at DESC, id
                LIMIT 100
                "#,
            "idx_remote_candidates_info_hash_seen",
            "info_hash=?",
        ),
        (
            "local match-decision history",
            r#"
                SELECT candidate_id, decision, matched_size, matched_ratio, reason_code, assessed_at
                FROM match_decisions
                WHERE local_item_id = 1
                ORDER BY assessed_at DESC, candidate_id
                LIMIT 100
                "#,
            "idx_match_decisions_local_assessed",
            "local_item_id=?",
        ),
    ] {
        let plan = explain_query_plan_raw(&repository, query).await;

        assert!(
            !plan.iter().any(|detail| detail.contains("USE TEMP B-TREE")),
            "{label} should not sort with a temp b-tree: {plan:?}"
        );
        assert!(
            plan.iter().any(|detail| detail.contains(index_name)),
            "{label} should use {index_name}: {plan:?}"
        );
        assert!(
            plan.iter().any(|detail| detail.contains(expected_search)),
            "{label} should constrain the targeted index with {expected_search}: {plan:?}"
        );
    }
}

#[tokio::test]
async fn startup_indexer_health_query_uses_enabled_registry_index() {
    let repository = Repository::connect_in_memory().await.unwrap();
    let query = r#"
            SELECT
                health.dependency_type,
                health.dependency_name,
                health.state,
                health.reason,
                health.retry_after,
                health.failure_count,
                health.checked_at
            FROM indexers INDEXED BY idx_indexers_enabled_name
            INNER JOIN dependency_health AS health
                ON health.dependency_type = 'indexer'
               AND health.dependency_name = indexers.name
            WHERE indexers.enabled = 1
            ORDER BY indexers.name
            "#;
    let plan = explain_query_plan_raw(&repository, query).await;

    assert!(
        !plan
            .iter()
            .any(|detail| detail.contains("SCAN dependency_health")),
        "startup indexer health should not scan dependency_health: {plan:?}"
    );
    assert!(
        !plan.iter().any(|detail| detail.contains("USE TEMP B-TREE")),
        "startup indexer health should not sort with a temp b-tree: {plan:?}"
    );
    assert!(
        plan.iter()
            .any(|detail| detail.contains("idx_indexers_enabled_name")),
        "startup indexer health should start from enabled indexers: {plan:?}"
    );
    assert!(
        plan.iter()
            .any(|detail| detail.contains("sqlite_autoindex_dependency_health_1")),
        "startup indexer health should probe dependency_health by primary key: {plan:?}"
    );
}

#[tokio::test]
async fn active_announce_status_counts_keep_global_top_limit() {
    let repository = Repository::connect_in_memory().await.unwrap();
    for index in 0..3 {
        repository
            .insert_or_dedupe_announce_work(
                &test_announce_work(
                    &format!("ann_accepted_{index}"),
                    &format!("guid-a-{index}"),
                    1,
                ),
                10,
            )
            .await
            .unwrap();
    }
    for index in 0..5 {
        let mut work = test_announce_work(
            &format!("ann_waiting_{index}"),
            &format!("guid-z-{index}"),
            1,
        );
        work.reason = AnnounceReason::RetryAfter;
        repository
            .insert_or_dedupe_announce_work(&work, 10)
            .await
            .unwrap();
    }
    sqlx::query("UPDATE announce_work SET status = 'waiting' WHERE id LIKE 'ann_waiting_%'")
        .execute(repository.pool())
        .await
        .unwrap();

    let counts = repository.announce_status_counts(1).await.unwrap();

    assert_eq!(1, counts.len());
    assert_eq!("waiting", counts[0].status);
    assert_eq!("retry_after", counts[0].reason);
    assert_eq!(5, counts[0].count);
}

#[tokio::test]
async fn local_title_token_lookup_uses_title_gram_index_without_local_item_scan() {
    let repository = Repository::connect_in_memory().await.unwrap();
    let rows = sqlx::query(
        r#"
            EXPLAIN QUERY PLAN
            SELECT local_items.id
            FROM local_item_title_grams title_match
            INNER JOIN local_items
                ON local_items.id = title_match.item_id
            WHERE title_match.media_type = 'movie'
              AND title_match.gram = 'amp'
            ORDER BY title_match.title, title_match.source_type, title_match.source_key
            LIMIT 16
            "#,
    )
    .fetch_all(repository.pool())
    .await
    .unwrap();
    let plan = rows
        .into_iter()
        .map(|row| row.get::<String, _>("detail"))
        .collect::<Vec<_>>();

    assert!(
        !plan
            .iter()
            .any(|detail| detail.contains("SCAN local_items")),
        "title token lookup should not scan local_items: {plan:?}"
    );
    assert!(
        !plan.iter().any(|detail| detail.contains("USE TEMP B-TREE")),
        "title token lookup should not sort with a temp b-tree: {plan:?}"
    );
    assert!(
        plan.iter()
            .any(|detail| detail.contains("idx_local_item_title_grams_lookup")),
        "title token lookup should use the title gram index: {plan:?}"
    );
}

#[tokio::test]
async fn local_title_tokens_lookup_uses_title_gram_index_without_temp_sort() {
    let repository = Repository::connect_in_memory().await.unwrap();
    let rows = sqlx::query(
        r#"
            EXPLAIN QUERY PLAN
            SELECT local_items.id
            FROM local_item_title_grams title_match
            INNER JOIN local_items
                ON local_items.id = title_match.item_id
            WHERE title_match.media_type = 'movie'
              AND title_match.gram = 'exa'
              AND EXISTS (
                    SELECT 1
                    FROM local_item_title_grams required_match
                    WHERE required_match.item_id = title_match.item_id
                      AND required_match.gram = 'mmo'
                )
            ORDER BY title_match.title, title_match.source_type, title_match.source_key
            LIMIT 16
            "#,
    )
    .fetch_all(repository.pool())
    .await
    .unwrap();
    let plan = rows
        .into_iter()
        .map(|row| row.get::<String, _>("detail"))
        .collect::<Vec<_>>();

    assert!(
        !plan
            .iter()
            .any(|detail| detail.contains("SCAN local_items")),
        "multi-token lookup should not scan local_items: {plan:?}"
    );
    assert!(
        !plan.iter().any(|detail| detail.contains("USE TEMP B-TREE")),
        "multi-token lookup should not sort with a temp b-tree: {plan:?}"
    );
    assert!(
        plan.iter()
            .any(|detail| detail.contains("idx_local_item_title_grams_lookup")),
        "multi-token lookup should use the title gram index: {plan:?}"
    );
}

#[tokio::test]
async fn local_title_token_lookup_keeps_substring_candidates() {
    let repository = Repository::connect_in_memory().await.unwrap();
    let item = test_local_item("Examples Movie");
    repository
        .upsert_local_item_with_files(&item, &[])
        .await
        .unwrap();

    let found = repository
        .local_items_by_media_type_and_title_token(MediaType::Movie, "ample", 10)
        .await
        .unwrap();

    assert_eq!(1, found.len());
    assert_eq!("Examples Movie", found[0].title.as_str());
}

#[tokio::test]
async fn local_item_upsert_populates_title_grams_with_normalized_title() {
    let repository = Repository::connect_in_memory().await.unwrap();
    let item = test_local_item("Examples Movie 1080p");
    let item_id = repository
        .upsert_local_item_with_files(&item, &[])
        .await
        .unwrap();

    let rows = sqlx::query(
        r#"
            SELECT gram, normalized_title
            FROM local_item_title_grams
            WHERE item_id = ?
            ORDER BY gram
            "#,
    )
    .bind(i64_from_u64(item_id.get(), "local item id").unwrap())
    .fetch_all(repository.pool())
    .await
    .unwrap();
    let grams = rows
        .iter()
        .map(|row| row.get::<String, _>("gram"))
        .collect::<Vec<_>>();

    assert!(grams.contains(&"exa".to_owned()));
    assert!(grams.contains(&"ovi".to_owned()));
    assert!(!grams.contains(&"108".to_owned()));
    assert!(
        rows.iter()
            .all(|row| row.get::<String, _>("normalized_title") == "examples movie")
    );
}

#[tokio::test]
async fn announce_wakeups_make_matching_waits_claimable() {
    let repository = Repository::connect_in_memory().await.unwrap();
    for (id, guid) in [
        ("ann_30", "guid-inventory"),
        ("ann_31", "guid-refreshing"),
        ("ann_32", "guid-client"),
        ("ann_33", "guid-dependency"),
        ("ann_34", "guid-cache"),
        ("ann_35", "guid-scheduled"),
        ("ann_36", "guid-other-client"),
    ] {
        repository
            .insert_or_dedupe_announce_work(&test_announce_work(id, guid, 1), 10)
            .await
            .unwrap();
    }
    set_announce_waiting(
        &repository,
        "ann_30",
        AnnounceReason::SourceIncomplete,
        500,
        None,
    )
    .await;
    set_announce_waiting(
        &repository,
        "ann_31",
        AnnounceReason::InventoryRefreshing,
        500,
        None,
    )
    .await;
    set_announce_waiting(
        &repository,
        "ann_32",
        AnnounceReason::ClientChecking,
        500,
        Some(("client", "qbit.local")),
    )
    .await;
    set_announce_waiting(
        &repository,
        "ann_33",
        AnnounceReason::DependencyBackoff,
        500,
        Some(("indexer", "main")),
    )
    .await;
    set_announce_waiting(
        &repository,
        "ann_34",
        AnnounceReason::CandidateDownloading,
        500,
        Some(("candidate", "guid-cache")),
    )
    .await;
    set_announce_waiting(&repository, "ann_35", AnnounceReason::RetryAfter, 50, None).await;
    set_announce_waiting(
        &repository,
        "ann_36",
        AnnounceReason::ClientChecking,
        500,
        Some(("client", "other.local")),
    )
    .await;

    assert_eq!(
        2,
        repository
            .wake_announce_inventory_refresh(100, 10)
            .await
            .unwrap()
    );
    assert_eq!(
        1,
        repository
            .wake_announce_client_source_completion(
                &ClientHost::new("qbit.local").unwrap(),
                101,
                10
            )
            .await
            .unwrap()
    );
    assert_eq!(
        1,
        repository
            .wake_announce_dependency_recovery(
                "indexer",
                &DependencyName::new("main").unwrap(),
                102,
                10
            )
            .await
            .unwrap()
    );
    assert_eq!(
        1,
        repository
            .wake_announce_candidate_cache_completion(
                None,
                Some(&CandidateGuid::new("guid-cache").unwrap()),
                103,
                10
            )
            .await
            .unwrap()
    );
    assert_eq!(
        1,
        repository
            .wake_due_waiting_announce_work(104, 10)
            .await
            .unwrap()
    );

    let claimed = repository
        .claim_announce_work("worker-1", 104, 200, 10)
        .await
        .unwrap();

    assert_eq!(
        vec![
            AnnounceWorkId::new("ann_30").unwrap(),
            AnnounceWorkId::new("ann_31").unwrap(),
            AnnounceWorkId::new("ann_32").unwrap(),
            AnnounceWorkId::new("ann_33").unwrap(),
            AnnounceWorkId::new("ann_34").unwrap(),
            AnnounceWorkId::new("ann_35").unwrap(),
        ],
        claimed
    );
    assert_eq!(
        Some(("waiting".to_owned(), "client_checking".to_owned())),
        announce_status_reason(&repository, "ann_36").await
    );
}

#[tokio::test]
async fn healthy_dependency_record_wakes_matching_waits() {
    let repository = Repository::connect_in_memory().await.unwrap();
    repository
        .insert_or_dedupe_announce_work(&test_announce_work("ann_40", "guid-40", 1), 10)
        .await
        .unwrap();
    set_announce_waiting(
        &repository,
        "ann_40",
        AnnounceReason::DependencyBackoff,
        500,
        Some(("indexer", "main")),
    )
    .await;

    repository
        .record_dependency_health(
            "indexer",
            &DependencyName::new("main").unwrap(),
            &DependencyState::Healthy { checked_at_ms: 100 },
            100,
        )
        .await
        .unwrap();

    assert_eq!(
        Some(("queued".to_owned(), "dependency_backoff".to_owned())),
        announce_status_reason(&repository, "ann_40").await
    );
}

fn test_local_item(title: &str) -> LocalItem {
    LocalItem {
        id: None,
        source: LocalItemSource::Client {
            client_host: ClientHost::new("qbit.local").unwrap(),
            source_key: SourceKey::new("0123456789abcdef0123456789abcdef01234567").unwrap(),
        },
        title: ItemTitle::new(title).unwrap(),
        display_name: DisplayName::new(title).unwrap(),
        media_type: MediaType::Movie,
        info_hash: Some(InfoHash::new("0123456789abcdef0123456789abcdef01234567").unwrap()),
        path: None,
        save_path: Some(PathBuf::from("/downloads")),
        total_size: ByteSize::new(30),
        mtime_ms: Some(1_700_000_000_000),
    }
}

async fn staged_inventory_counts(repository: &Repository) -> (i64, i64) {
    let staged_count = sqlx::query_scalar("SELECT COUNT(*) FROM temp.staged_local_inventory_items")
        .fetch_one(repository.pool())
        .await
        .unwrap();
    let staged_file_count =
        sqlx::query_scalar("SELECT COUNT(*) FROM temp.staged_local_inventory_files")
            .fetch_one(repository.pool())
            .await
            .unwrap();

    (staged_count, staged_file_count)
}

fn test_remote_candidate(guid: &str, title: &str) -> RemoteCandidate {
    RemoteCandidate {
        id: None,
        indexer_id: IndexerId::new(1).unwrap(),
        guid: CandidateGuid::new(guid).unwrap(),
        download_url: DownloadUrl::new("https://indexer.example/download").unwrap(),
        title: ItemTitle::new(title).unwrap(),
        tracker: TrackerName::new("tracker.example").unwrap(),
        size: Some(ByteSize::new(42)),
        published_at_ms: Some(1_700_000_000_000),
        info_hash: Some(InfoHash::new("fedcba9876543210fedcba9876543210fedcba98").unwrap()),
        torrent_cache_path: None,
    }
}

fn test_indexer(name: &str, url: &str, api_key_source: ApiKeySource) -> ConfiguredTorznabIndexer {
    ConfiguredTorznabIndexer {
        name: DependencyName::new(name).unwrap(),
        url: SanitizedTorznabUrl::new(url).unwrap(),
        api_key: None,
        api_key_source,
        enabled: true,
    }
}

fn test_prowlarr_indexer(source: &str, prowlarr_id: i64, name: &str, url: &str) -> ProwlarrIndexer {
    ProwlarrIndexer {
        source: DependencyName::new(source).unwrap(),
        prowlarr_id,
        name: DependencyName::new(name).unwrap(),
        url: SanitizedTorznabUrl::new(url).unwrap(),
        api_key: Some(ApiKey::new("prowlarr-secret").unwrap()),
        api_key_source: ApiKeySource::Direct,
        tags: Vec::new(),
    }
}

fn test_announce_work(id: &str, guid: &str, received_at_ms: i64) -> AnnounceWorkItem {
    let tracker = TrackerName::new("tracker.example").unwrap();
    let guid = CandidateGuid::new(guid).unwrap();
    let dedupe_hash = AnnounceDedupeIdentity::Guid {
        tracker: tracker.clone(),
        guid: guid.clone(),
    }
    .hash();

    AnnounceWorkItem {
        id: AnnounceWorkId::new(id).unwrap(),
        status: AnnounceStatus::Queued,
        reason: AnnounceReason::Accepted,
        dedupe_hash,
        title: ItemTitle::new("Example").unwrap(),
        tracker,
        guid: Some(guid),
        info_hash: None,
        size: Some(ByteSize::new(42)),
        fetch: None,
        received_at_ms,
        updated_at_ms: received_at_ms,
        first_attempt_at_ms: None,
        finished_at_ms: None,
        attempt_count: 0,
        next_attempt_at_ms: received_at_ms,
        expires_at_ms: 1_000,
        lease: None,
        last_dependency_kind: None,
        last_dependency_name: None,
        last_error_class: None,
        last_redacted_message: None,
    }
}

async fn set_announce_waiting(
    repository: &Repository,
    id: &str,
    reason: AnnounceReason,
    next_attempt_at_ms: i64,
    dependency: Option<(&str, &str)>,
) {
    let (dependency_kind, dependency_name) = dependency.unwrap_or(("", ""));
    sqlx::query(
        r#"
            UPDATE announce_work
            SET status = 'waiting',
                reason = ?,
                next_attempt_at = ?,
                expires_at = 1_000,
                last_dependency_kind = NULLIF(?, ''),
                last_dependency_name = NULLIF(?, '')
            WHERE id = ?
            "#,
    )
    .bind(announce_reason_key(reason))
    .bind(next_attempt_at_ms)
    .bind(dependency_kind)
    .bind(dependency_name)
    .bind(id)
    .execute(repository.pool())
    .await
    .unwrap();
}

async fn announce_status_reason(repository: &Repository, id: &str) -> Option<(String, String)> {
    sqlx::query("SELECT status, reason FROM announce_work WHERE id = ?")
        .bind(id)
        .fetch_optional(repository.pool())
        .await
        .unwrap()
        .map(|row| (row.get("status"), row.get("reason")))
}

async fn explain_query_plan(
    repository: &Repository,
    query: &str,
    cutoff_ms: i64,
    limit: u16,
) -> Vec<String> {
    sqlx::query(&format!("EXPLAIN QUERY PLAN {query}"))
        .bind(cutoff_ms)
        .bind(i64::from(limit))
        .fetch_all(repository.pool())
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.get("detail"))
        .collect()
}

async fn explain_query_plan_raw(repository: &Repository, query: &str) -> Vec<String> {
    sqlx::query(&format!("EXPLAIN QUERY PLAN {query}"))
        .fetch_all(repository.pool())
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.get("detail"))
        .collect()
}

fn unique_temp_dir(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!("sporos-repository-test-{label}-{nanos}"));
    fs::create_dir_all(&path).unwrap();
    path
}
