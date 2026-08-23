CREATE TABLE sporos_operation (
    id BLOB PRIMARY KEY CHECK (length(id) = 16),
    kind TEXT NOT NULL,
    state TEXT NOT NULL,
    duroxide_instance_id TEXT NOT NULL,
    request_json TEXT NOT NULL CHECK (json_valid(request_json)),
    last_reported_cursor BLOB,
    produced_tasks INTEGER NOT NULL CHECK (produced_tasks >= 0),
    completed_tasks INTEGER NOT NULL CHECK (completed_tasks >= 0),
    failed_tasks INTEGER NOT NULL CHECK (failed_tasks >= 0),
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
) STRICT;

CREATE INDEX sporos_operation_state_idx
    ON sporos_operation (state, created_at, id);

CREATE TABLE sporos_blob (
    sha256 BLOB PRIMARY KEY CHECK (length(sha256) = 32),
    media_type TEXT NOT NULL,
    size INTEGER NOT NULL CHECK (size >= 0),
    data BLOB NOT NULL CHECK (length(data) = size),
    created_at INTEGER NOT NULL
) STRICT;

CREATE TABLE sporos_candidate (
    id BLOB PRIMARY KEY CHECK (length(id) = 16),
    blob_sha256 BLOB NOT NULL REFERENCES sporos_blob(sha256),
    v1_hash BLOB CHECK (v1_hash IS NULL OR length(v1_hash) = 20),
    v2_hash BLOB CHECK (v2_hash IS NULL OR length(v2_hash) = 32),
    manifest_digest BLOB NOT NULL CHECK (length(manifest_digest) = 32),
    display_name TEXT NOT NULL,
    release_json TEXT NOT NULL CHECK (json_valid(release_json)),
    state TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    CHECK (v1_hash IS NOT NULL OR v2_hash IS NOT NULL)
) STRICT;

CREATE INDEX sporos_candidate_v1_idx ON sporos_candidate (v1_hash)
    WHERE v1_hash IS NOT NULL;
CREATE INDEX sporos_candidate_v2_idx ON sporos_candidate (v2_hash)
    WHERE v2_hash IS NOT NULL;
CREATE INDEX sporos_candidate_state_idx
    ON sporos_candidate (state, created_at, id);

CREATE TABLE sporos_candidate_provenance (
    id INTEGER PRIMARY KEY,
    candidate_id BLOB NOT NULL REFERENCES sporos_candidate(id),
    trigger TEXT NOT NULL,
    indexer_id INTEGER,
    indexer_name TEXT,
    announcement_name TEXT,
    request_id TEXT,
    received_at INTEGER NOT NULL,
    detail_json TEXT CHECK (detail_json IS NULL OR json_valid(detail_json))
) STRICT;

CREATE INDEX sporos_candidate_provenance_candidate_idx
    ON sporos_candidate_provenance (candidate_id, received_at, id);

CREATE TABLE sporos_qbit_torrent (
    id BLOB PRIMARY KEY CHECK (length(id) = 16),
    v1_hash BLOB CHECK (v1_hash IS NULL OR length(v1_hash) = 20),
    v2_hash BLOB CHECK (v2_hash IS NULL OR length(v2_hash) = 32),
    name TEXT NOT NULL,
    total_size INTEGER NOT NULL CHECK (total_size >= 0),
    amount_left INTEGER NOT NULL CHECK (amount_left >= 0),
    progress_ppm INTEGER NOT NULL CHECK (progress_ppm BETWEEN 0 AND 1000000),
    state TEXT NOT NULL,
    save_path TEXT NOT NULL,
    content_path TEXT NOT NULL,
    category TEXT NOT NULL,
    tags_json TEXT NOT NULL CHECK (json_valid(tags_json)),
    is_complete INTEGER NOT NULL CHECK (is_complete IN (0, 1)),
    available INTEGER NOT NULL CHECK (available IN (0, 1)),
    file_manifest_version INTEGER NOT NULL CHECK (file_manifest_version >= 0),
    release_json TEXT CHECK (release_json IS NULL OR json_valid(release_json)),
    arr_identity_json TEXT CHECK (arr_identity_json IS NULL OR json_valid(arr_identity_json)),
    added_at INTEGER,
    completed_at INTEGER,
    last_seen_generation INTEGER NOT NULL CHECK (last_seen_generation >= 0),
    updated_at INTEGER NOT NULL,
    CHECK (v1_hash IS NOT NULL OR v2_hash IS NOT NULL)
) STRICT;

CREATE UNIQUE INDEX sporos_qbit_torrent_v1_idx ON sporos_qbit_torrent (v1_hash)
    WHERE v1_hash IS NOT NULL;
CREATE UNIQUE INDEX sporos_qbit_torrent_v2_idx ON sporos_qbit_torrent (v2_hash)
    WHERE v2_hash IS NOT NULL;
CREATE INDEX sporos_qbit_torrent_complete_idx
    ON sporos_qbit_torrent (is_complete, available, updated_at, id);
CREATE INDEX sporos_qbit_torrent_generation_idx
    ON sporos_qbit_torrent (last_seen_generation, id);

CREATE TABLE sporos_source_file (
    id INTEGER PRIMARY KEY,
    source_id BLOB NOT NULL CHECK (length(source_id) = 16),
    manifest_version INTEGER NOT NULL CHECK (manifest_version >= 0),
    relative_path BLOB NOT NULL,
    display_path TEXT NOT NULL,
    size INTEGER NOT NULL CHECK (size >= 0),
    file_kind TEXT NOT NULL,
    episode_key TEXT,
    local_path TEXT,
    device INTEGER,
    inode INTEGER,
    available INTEGER NOT NULL CHECK (available IN (0, 1)),
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    UNIQUE (source_id, manifest_version, ordinal)
) STRICT;

CREATE INDEX sporos_source_file_size_idx
    ON sporos_source_file (size, available, source_id);
CREATE INDEX sporos_source_file_episode_idx
    ON sporos_source_file (episode_key, available, source_id)
    WHERE episode_key IS NOT NULL;

CREATE TABLE sporos_data_source (
    id BLOB PRIMARY KEY CHECK (length(id) = 16),
    root_name TEXT NOT NULL,
    relative_path BLOB NOT NULL,
    kind TEXT NOT NULL,
    name TEXT NOT NULL,
    total_size INTEGER NOT NULL CHECK (total_size >= 0),
    release_json TEXT CHECK (release_json IS NULL OR json_valid(release_json)),
    device INTEGER,
    inode INTEGER,
    available INTEGER NOT NULL CHECK (available IN (0, 1)),
    last_seen_generation INTEGER NOT NULL CHECK (last_seen_generation >= 0),
    updated_at INTEGER NOT NULL,
    UNIQUE (root_name, relative_path)
) STRICT;

CREATE INDEX sporos_data_source_generation_idx
    ON sporos_data_source (root_name, last_seen_generation, id);

CREATE TABLE sporos_indexer (
    prowlarr_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    protocol TEXT NOT NULL,
    enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
    supports_search INTEGER NOT NULL CHECK (supports_search IN (0, 1)),
    redirect INTEGER NOT NULL CHECK (redirect IN (0, 1)),
    priority INTEGER NOT NULL,
    tags_json TEXT NOT NULL CHECK (json_valid(tags_json)),
    capabilities_json TEXT NOT NULL CHECK (json_valid(capabilities_json)),
    eligible INTEGER NOT NULL CHECK (eligible IN (0, 1)),
    ineligible_reason TEXT,
    status_json TEXT CHECK (status_json IS NULL OR json_valid(status_json)),
    refreshed_at INTEGER NOT NULL
) STRICT;

CREATE INDEX sporos_indexer_eligible_idx
    ON sporos_indexer (eligible, priority, prowlarr_id);

CREATE TABLE sporos_search_attempt (
    id BLOB PRIMARY KEY CHECK (length(id) = 16),
    source_id BLOB NOT NULL CHECK (length(source_id) = 16),
    indexer_id INTEGER NOT NULL REFERENCES sporos_indexer(prowlarr_id),
    query_fingerprint BLOB NOT NULL CHECK (length(query_fingerprint) = 32),
    policy_snapshot_id BLOB NOT NULL REFERENCES sporos_policy_snapshot(id),
    trigger TEXT NOT NULL,
    state TEXT NOT NULL,
    results_seen INTEGER NOT NULL CHECK (results_seen >= 0),
    results_downloaded INTEGER NOT NULL CHECK (results_downloaded >= 0),
    next_eligible_at INTEGER,
    reason_code TEXT,
    created_at INTEGER NOT NULL,
    completed_at INTEGER
) STRICT;

CREATE UNIQUE INDEX sporos_search_attempt_logical_idx
    ON sporos_search_attempt (
        source_id, indexer_id, query_fingerprint, policy_snapshot_id, trigger
    );
CREATE INDEX sporos_search_attempt_state_idx
    ON sporos_search_attempt (state, created_at, id);

CREATE TABLE sporos_match (
    id BLOB PRIMARY KEY CHECK (length(id) = 16),
    candidate_id BLOB NOT NULL REFERENCES sporos_candidate(id),
    matcher_version TEXT NOT NULL,
    mode TEXT,
    outcome TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    mapped_bytes INTEGER NOT NULL CHECK (mapped_bytes >= 0),
    missing_bytes INTEGER NOT NULL CHECK (missing_bytes >= 0),
    present_ratio_ppm INTEGER NOT NULL CHECK (present_ratio_ppm BETWEEN 0 AND 1000000),
    evidence_json TEXT NOT NULL CHECK (json_valid(evidence_json)),
    decision_digest BLOB NOT NULL CHECK (length(decision_digest) = 32),
    created_at INTEGER NOT NULL
) STRICT;

CREATE INDEX sporos_match_candidate_idx
    ON sporos_match (candidate_id, created_at, id);

CREATE TABLE sporos_file_mapping (
    match_id BLOB NOT NULL REFERENCES sporos_match(id),
    candidate_ordinal INTEGER NOT NULL CHECK (candidate_ordinal >= 0),
    source_id BLOB NOT NULL CHECK (length(source_id) = 16),
    source_file_id INTEGER NOT NULL REFERENCES sporos_source_file(id),
    candidate_path BLOB NOT NULL,
    size INTEGER NOT NULL CHECK (size >= 0),
    score INTEGER NOT NULL,
    PRIMARY KEY (match_id, candidate_ordinal)
) STRICT, WITHOUT ROWID;

CREATE TABLE sporos_injection_plan (
    id BLOB PRIMARY KEY CHECK (length(id) = 16),
    match_id BLOB NOT NULL REFERENCES sporos_match(id),
    candidate_id BLOB NOT NULL REFERENCES sporos_candidate(id),
    namespace_local TEXT NOT NULL,
    save_path_remote TEXT NOT NULL,
    category TEXT NOT NULL,
    tags_json TEXT NOT NULL CHECK (json_valid(tags_json)),
    resume_policy_json TEXT NOT NULL CHECK (json_valid(resume_policy_json)),
    plan_digest BLOB NOT NULL UNIQUE CHECK (length(plan_digest) = 32),
    state TEXT NOT NULL,
    created_at INTEGER NOT NULL
) STRICT;

CREATE TABLE sporos_link (
    id INTEGER PRIMARY KEY,
    plan_id BLOB NOT NULL REFERENCES sporos_injection_plan(id),
    candidate_ordinal INTEGER NOT NULL CHECK (candidate_ordinal >= 0),
    source_file_id INTEGER NOT NULL REFERENCES sporos_source_file(id),
    destination_relative BLOB NOT NULL,
    device INTEGER,
    inode INTEGER,
    size INTEGER NOT NULL CHECK (size >= 0),
    state TEXT NOT NULL,
    error_code TEXT,
    created_at INTEGER NOT NULL,
    verified_at INTEGER,
    UNIQUE (plan_id, candidate_ordinal)
) STRICT;

CREATE TABLE sporos_injection (
    id BLOB PRIMARY KEY CHECK (length(id) = 16),
    plan_id BLOB NOT NULL UNIQUE REFERENCES sporos_injection_plan(id),
    v1_hash BLOB CHECK (v1_hash IS NULL OR length(v1_hash) = 20),
    v2_hash BLOB CHECK (v2_hash IS NULL OR length(v2_hash) = 32),
    qbit_state TEXT,
    amount_left INTEGER CHECK (amount_left IS NULL OR amount_left >= 0),
    progress_ppm INTEGER CHECK (progress_ppm IS NULL OR progress_ppm BETWEEN 0 AND 1000000),
    integrity_safe INTEGER CHECK (integrity_safe IS NULL OR integrity_safe IN (0, 1)),
    resume_decision TEXT,
    state TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
) STRICT;

CREATE TABLE sporos_waiting_source (
    candidate_task_id BLOB NOT NULL REFERENCES sporos_task(id),
    source_id BLOB NOT NULL CHECK (length(source_id) = 16),
    created_at INTEGER NOT NULL,
    PRIMARY KEY (candidate_task_id, source_id)
) STRICT, WITHOUT ROWID;
