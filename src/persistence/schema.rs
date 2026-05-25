pub const BUSY_TIMEOUT_MS: u32 = 5_000;

pub const CONNECTION_PRAGMAS: &[&str] = &[
    "PRAGMA foreign_keys = ON;",
    "PRAGMA journal_mode = WAL;",
    "PRAGMA busy_timeout = 5000;",
];

pub const INITIAL_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS local_items (
    id INTEGER PRIMARY KEY,
    source_type TEXT NOT NULL,
    source_key TEXT NOT NULL,
    title TEXT NOT NULL,
    display_name TEXT NOT NULL,
    media_type TEXT NOT NULL,
    info_hash TEXT,
    path TEXT,
    save_path TEXT,
    total_size INTEGER NOT NULL,
    mtime_ms INTEGER,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE (source_type, source_key)
);

CREATE INDEX IF NOT EXISTS idx_local_items_info_hash
    ON local_items (info_hash)
    WHERE info_hash IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_local_items_info_hash_media_type
    ON local_items (info_hash, media_type)
    WHERE info_hash IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_local_items_media_title
    ON local_items (media_type, title);
CREATE INDEX IF NOT EXISTS idx_local_items_media_title_source
    ON local_items (media_type, title, source_type, source_key);
CREATE INDEX IF NOT EXISTS idx_local_items_updated_at
    ON local_items (updated_at);

CREATE TABLE IF NOT EXISTS local_item_title_grams (
    item_id INTEGER NOT NULL REFERENCES local_items(id) ON DELETE CASCADE,
    media_type TEXT NOT NULL,
    gram TEXT NOT NULL,
    normalized_title TEXT NOT NULL,
    title TEXT NOT NULL,
    source_type TEXT NOT NULL,
    source_key TEXT NOT NULL,
    PRIMARY KEY (item_id, gram)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_local_item_title_grams_lookup
    ON local_item_title_grams (media_type, gram, title, source_type, source_key, normalized_title, item_id);
CREATE INDEX IF NOT EXISTS idx_local_item_title_grams_title_key
    ON local_item_title_grams (media_type, gram, normalized_title, title, source_type, source_key, item_id);

CREATE TABLE IF NOT EXISTS local_files (
    item_id INTEGER NOT NULL REFERENCES local_items(id) ON DELETE CASCADE,
    relative_path TEXT NOT NULL,
    file_name TEXT NOT NULL,
    size INTEGER NOT NULL,
    mtime_ms INTEGER,
    file_index INTEGER NOT NULL,
    PRIMARY KEY (item_id, file_index)
);

CREATE INDEX IF NOT EXISTS idx_local_files_item_size
    ON local_files (item_id, size);
CREATE INDEX IF NOT EXISTS idx_local_files_size_name
    ON local_files (size, file_name);
CREATE INDEX IF NOT EXISTS idx_local_files_relative_path
    ON local_files (relative_path);

CREATE TABLE IF NOT EXISTS remote_candidates (
    id INTEGER PRIMARY KEY,
    indexer_id INTEGER NOT NULL,
    guid TEXT NOT NULL,
    redacted_download_url TEXT NOT NULL,
    title TEXT NOT NULL,
    tracker TEXT NOT NULL,
    size INTEGER,
    published_at INTEGER,
    info_hash TEXT,
    torrent_cache_path TEXT,
    first_seen_at INTEGER NOT NULL,
    last_seen_at INTEGER NOT NULL,
    UNIQUE (indexer_id, guid)
);

CREATE INDEX IF NOT EXISTS idx_remote_candidates_info_hash
    ON remote_candidates (info_hash);
CREATE INDEX IF NOT EXISTS idx_remote_candidates_info_hash_seen
    ON remote_candidates (info_hash, last_seen_at DESC, id)
    WHERE info_hash IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_remote_candidates_last_seen_at
    ON remote_candidates (last_seen_at);
CREATE INDEX IF NOT EXISTS idx_remote_candidates_title
    ON remote_candidates (title);
CREATE INDEX IF NOT EXISTS idx_remote_candidates_published_at
    ON remote_candidates (published_at);

CREATE TABLE IF NOT EXISTS match_decisions (
    local_item_id INTEGER NOT NULL REFERENCES local_items(id) ON DELETE CASCADE,
    candidate_id INTEGER NOT NULL REFERENCES remote_candidates(id) ON DELETE CASCADE,
    decision TEXT NOT NULL,
    matched_size INTEGER,
    matched_ratio REAL,
    reason_code TEXT NOT NULL,
    assessed_at INTEGER NOT NULL,
    PRIMARY KEY (local_item_id, candidate_id)
);

CREATE INDEX IF NOT EXISTS idx_match_decisions_decision_assessed_at
    ON match_decisions (decision, assessed_at);
CREATE INDEX IF NOT EXISTS idx_match_decisions_candidate_id
    ON match_decisions (candidate_id);
CREATE INDEX IF NOT EXISTS idx_match_decisions_local_assessed
    ON match_decisions (local_item_id, assessed_at DESC, candidate_id);

CREATE TABLE IF NOT EXISTS indexers (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    url TEXT NOT NULL,
    source_kind TEXT NOT NULL DEFAULT 'static',
    source_name TEXT NOT NULL DEFAULT '',
    source_indexer_id TEXT NOT NULL DEFAULT '',
    api_key_source TEXT NOT NULL,
    enabled INTEGER NOT NULL,
    capabilities_json TEXT NOT NULL DEFAULT '{}',
    state TEXT NOT NULL,
    retry_after INTEGER,
    last_caps_refresh_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE (name),
    UNIQUE (source_kind, source_name, source_indexer_id)
);

CREATE INDEX IF NOT EXISTS idx_indexers_enabled_state_retry_after
    ON indexers (enabled, state, retry_after);
CREATE INDEX IF NOT EXISTS idx_indexers_enabled_name
    ON indexers (enabled, name);
CREATE INDEX IF NOT EXISTS idx_indexers_search_ready
    ON indexers (enabled, last_caps_refresh_at, retry_after, name);
CREATE INDEX IF NOT EXISTS idx_indexers_due_page
    ON indexers (enabled, retry_after, name);
CREATE INDEX IF NOT EXISTS idx_indexers_search_ready_page
    ON indexers (enabled, retry_after, name)
    WHERE last_caps_refresh_at IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_indexers_active_url_unique
    ON indexers (url)
    WHERE enabled != 0;
CREATE INDEX IF NOT EXISTS idx_indexers_url
    ON indexers (url);

CREATE TABLE IF NOT EXISTS search_history (
    local_item_id INTEGER NOT NULL REFERENCES local_items(id) ON DELETE CASCADE,
    indexer_id INTEGER NOT NULL REFERENCES indexers(id) ON DELETE CASCADE,
    first_searched_at INTEGER NOT NULL,
    last_searched_at INTEGER NOT NULL,
    PRIMARY KEY (local_item_id, indexer_id)
);

CREATE INDEX IF NOT EXISTS idx_search_history_indexer_last_searched_at
    ON search_history (indexer_id, last_searched_at);
CREATE INDEX IF NOT EXISTS idx_search_history_first_searched_at
    ON search_history (first_searched_at);

CREATE TABLE IF NOT EXISTS jobs (
    name TEXT PRIMARY KEY,
    state TEXT NOT NULL,
    last_started_at INTEGER,
    last_finished_at INTEGER,
    next_run_at INTEGER,
    last_error TEXT
);

CREATE INDEX IF NOT EXISTS idx_jobs_next_run_at
    ON jobs (next_run_at);
CREATE INDEX IF NOT EXISTS idx_jobs_state
    ON jobs (state);

CREATE TABLE IF NOT EXISTS dependency_health (
    dependency_type TEXT NOT NULL,
    dependency_name TEXT NOT NULL,
    state TEXT NOT NULL,
    reason TEXT,
    retry_after INTEGER,
    failure_count INTEGER NOT NULL DEFAULT 0,
    checked_at INTEGER NOT NULL,
    PRIMARY KEY (dependency_type, dependency_name)
);

CREATE INDEX IF NOT EXISTS idx_dependency_health_state_retry_after
    ON dependency_health (state, retry_after);

CREATE TABLE IF NOT EXISTS announce_work (
    id TEXT PRIMARY KEY,
    dedupe_hash TEXT NOT NULL,
    received_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    first_attempt_at INTEGER,
    finished_at INTEGER,
    tracker TEXT NOT NULL,
    guid TEXT,
    info_hash TEXT,
    title TEXT NOT NULL,
    category TEXT,
    size INTEGER,
    published_at INTEGER,
    download_url TEXT,
    redacted_download_url TEXT,
    cookie TEXT,
    status TEXT NOT NULL,
    reason TEXT NOT NULL,
    attempt_count INTEGER NOT NULL,
    next_attempt_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    lease_owner TEXT,
    lease_until INTEGER,
    last_dependency_kind TEXT,
    last_dependency_name TEXT,
    last_error_class TEXT,
    last_error_message TEXT,
    last_decision TEXT,
    last_action_outcome TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_announce_work_active_dedupe
    ON announce_work (dedupe_hash)
    WHERE status IN ('queued', 'running', 'waiting', 'retryable');
CREATE INDEX IF NOT EXISTS idx_announce_work_claimable
    ON announce_work (status, next_attempt_at)
    WHERE status IN ('queued', 'retryable');
CREATE INDEX IF NOT EXISTS idx_announce_work_expires_at
    ON announce_work (expires_at, id)
    WHERE status IN ('queued', 'running', 'waiting', 'retryable');
CREATE INDEX IF NOT EXISTS idx_announce_work_lease_until
    ON announce_work (lease_until, id)
    WHERE status = 'running';
CREATE INDEX IF NOT EXISTS idx_announce_work_status_reason
    ON announce_work (status, reason);
CREATE INDEX IF NOT EXISTS idx_announce_work_active_dependency
    ON announce_work (status, last_dependency_kind, last_dependency_name);
CREATE INDEX IF NOT EXISTS idx_announce_work_dependency_schedule
    ON announce_work (next_attempt_at, received_at)
    WHERE status IN ('queued', 'retryable', 'waiting')
      AND last_dependency_kind IS NOT NULL
      AND last_dependency_name IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_announce_work_waiting_due
    ON announce_work (next_attempt_at, received_at)
    WHERE status = 'waiting'
      AND last_dependency_kind IS NULL
      AND last_dependency_name IS NULL;
CREATE INDEX IF NOT EXISTS idx_announce_work_inventory_wakeup
    ON announce_work (next_attempt_at, received_at)
    WHERE status = 'waiting'
      AND reason IN ('source_incomplete', 'inventory_refreshing')
      AND last_dependency_kind IS NULL
      AND last_dependency_name IS NULL;
CREATE INDEX IF NOT EXISTS idx_announce_work_waiting_dependency_due
    ON announce_work (last_dependency_kind, last_dependency_name, next_attempt_at, received_at)
    WHERE status = 'waiting';
CREATE INDEX IF NOT EXISTS idx_announce_work_succeeded_retention
    ON announce_work (status, finished_at, id)
    WHERE status = 'succeeded'
      AND finished_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_announce_work_terminal_failed_retention
    ON announce_work (status, finished_at, id)
    WHERE status = 'terminal_failed'
      AND finished_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_announce_work_expired_retention
    ON announce_work (status, finished_at, id)
    WHERE status = 'expired'
      AND finished_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_announce_work_active_fetch_scrub
    ON announce_work (expires_at, id)
    WHERE status IN ('queued', 'running', 'waiting', 'retryable')
      AND (download_url IS NOT NULL OR cookie IS NOT NULL);
"#;

pub const REQUIRED_TABLES: &[&str] = &[
    "local_items",
    "local_item_title_grams",
    "local_files",
    "remote_candidates",
    "match_decisions",
    "indexers",
    "search_history",
    "jobs",
    "dependency_health",
    "announce_work",
];

pub fn initial_schema_statements() -> impl Iterator<Item = &'static str> {
    INITIAL_SCHEMA
        .split(';')
        .map(str::trim)
        .filter(|statement| !statement.is_empty())
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    #[test]
    fn connection_pragmas_enable_wal_busy_timeout_and_foreign_keys() {
        assert!(CONNECTION_PRAGMAS.contains(&"PRAGMA foreign_keys = ON;"));
        assert!(CONNECTION_PRAGMAS.contains(&"PRAGMA journal_mode = WAL;"));
        assert!(CONNECTION_PRAGMAS.contains(&"PRAGMA busy_timeout = 5000;"));
        assert_eq!(5_000, BUSY_TIMEOUT_MS);
    }

    #[test]
    fn schema_declares_required_tables() {
        for table in REQUIRED_TABLES {
            let expected = format!("CREATE TABLE IF NOT EXISTS {table}");
            assert!(
                INITIAL_SCHEMA.contains(&expected),
                "schema should create {table}"
            );
        }
    }

    #[test]
    fn schema_declares_required_keys_and_indexes() {
        for fragment in [
            "UNIQUE (source_type, source_key)",
            "WHERE info_hash IS NOT NULL",
            "PRIMARY KEY (item_id, file_index)",
            "UNIQUE (indexer_id, guid)",
            "redacted_download_url TEXT NOT NULL",
            "idx_remote_candidates_info_hash_seen",
            "PRIMARY KEY (local_item_id, candidate_id)",
            "idx_match_decisions_local_assessed",
            "UNIQUE (name)",
            "UNIQUE (source_kind, source_name, source_indexer_id)",
            "idx_indexers_enabled_name",
            "idx_indexers_search_ready",
            "idx_indexers_due_page",
            "idx_indexers_search_ready_page",
            "idx_indexers_active_url_unique",
            "idx_indexers_url",
            "api_key_source TEXT NOT NULL",
            "PRIMARY KEY (local_item_id, indexer_id)",
            "name TEXT PRIMARY KEY",
            "PRIMARY KEY (dependency_type, dependency_name)",
            "failure_count INTEGER NOT NULL DEFAULT 0",
            "idx_local_files_item_size",
            "idx_local_items_media_title_source",
            "idx_local_item_title_grams_lookup",
            "idx_local_item_title_grams_title_key",
            "idx_local_files_size_name",
            "idx_local_files_relative_path",
            "idx_jobs_next_run_at",
            "idx_dependency_health_state_retry_after",
            "idx_announce_work_active_dedupe",
            "idx_announce_work_claimable",
            "idx_announce_work_expires_at",
            "idx_announce_work_lease_until",
            "idx_announce_work_status_reason",
            "idx_announce_work_active_dependency",
            "idx_announce_work_dependency_schedule",
            "idx_announce_work_waiting_due",
            "idx_announce_work_inventory_wakeup",
            "idx_announce_work_waiting_dependency_due",
            "idx_announce_work_succeeded_retention",
            "idx_announce_work_terminal_failed_retention",
            "idx_announce_work_expired_retention",
            "idx_announce_work_active_fetch_scrub",
        ] {
            assert!(
                INITIAL_SCHEMA.contains(fragment),
                "schema should contain `{fragment}`"
            );
        }
    }

    #[test]
    fn schema_preserves_explicit_ownership_boundaries() {
        assert!(
            INITIAL_SCHEMA
                .contains("item_id INTEGER NOT NULL REFERENCES local_items(id) ON DELETE CASCADE")
        );
        assert!(INITIAL_SCHEMA.contains(
            "local_item_id INTEGER NOT NULL REFERENCES local_items(id) ON DELETE CASCADE"
        ));
        assert!(INITIAL_SCHEMA.contains(
            "candidate_id INTEGER NOT NULL REFERENCES remote_candidates(id) ON DELETE CASCADE"
        ));
        assert!(
            !INITIAL_SCHEMA.contains("remote_candidates(id) REFERENCES local_items"),
            "remote candidates must not be owned by local torrent identity"
        );
    }

    #[test]
    fn schema_is_inline_and_sporos_native() {
        assert!(!Path::new("migrations").exists());
        assert!(!Path::new("src/migrations").exists());
        let normalized = INITIAL_SCHEMA.to_ascii_lowercase();
        assert!(!normalized.contains(&["cross", "-seed"].concat()));
        assert!(!normalized.contains(&["cross", "seed"].concat()));
    }

    #[test]
    fn announce_work_schema_covers_durable_queue_state() {
        for fragment in [
            "id TEXT PRIMARY KEY",
            "dedupe_hash TEXT NOT NULL",
            "download_url TEXT",
            "redacted_download_url TEXT",
            "cookie TEXT",
            "status TEXT NOT NULL",
            "reason TEXT NOT NULL",
            "attempt_count INTEGER NOT NULL",
            "next_attempt_at INTEGER NOT NULL",
            "expires_at INTEGER NOT NULL",
            "lease_owner TEXT",
            "lease_until INTEGER",
            "last_dependency_kind TEXT",
            "last_error_class TEXT",
            "last_error_message TEXT",
            "last_decision TEXT",
            "last_action_outcome TEXT",
            "WHERE status IN ('queued', 'running', 'waiting', 'retryable')",
            "WHERE status IN ('queued', 'retryable')",
            "WHERE status = 'running'",
        ] {
            assert!(
                INITIAL_SCHEMA.contains(fragment),
                "announce_work schema should contain `{fragment}`"
            );
        }
    }

    #[test]
    fn schema_splits_into_statements_without_empty_entries() {
        let statements = initial_schema_statements().collect::<Vec<_>>();

        assert!(statements.len() > REQUIRED_TABLES.len());
        assert!(statements.iter().all(|statement| !statement.is_empty()));
    }
}
