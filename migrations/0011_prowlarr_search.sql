ALTER TABLE sporos_search_attempt ADD COLUMN task_id BLOB
    REFERENCES sporos_task(id) CHECK (task_id IS NULL OR length(task_id) = 16);
ALTER TABLE sporos_search_attempt ADD COLUMN dependency_attempts INTEGER NOT NULL DEFAULT 0
    CHECK (dependency_attempts >= 0);

CREATE UNIQUE INDEX sporos_search_attempt_task_idx
    ON sporos_search_attempt (task_id) WHERE task_id IS NOT NULL;

CREATE TABLE sporos_indexer_rate_limit (
    indexer_id INTEGER PRIMARY KEY REFERENCES sporos_indexer(prowlarr_id),
    next_eligible_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
) STRICT;

CREATE TABLE sporos_search_result_summary (
    search_attempt_id BLOB NOT NULL REFERENCES sporos_search_attempt(id),
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    fingerprint BLOB NOT NULL CHECK (length(fingerprint) = 32),
    title TEXT NOT NULL,
    size INTEGER CHECK (size IS NULL OR size >= 0),
    state TEXT NOT NULL,
    candidate_id BLOB REFERENCES sporos_candidate(id),
    PRIMARY KEY (search_attempt_id, ordinal)
) STRICT, WITHOUT ROWID;

CREATE INDEX sporos_search_result_candidate_idx
    ON sporos_search_result_summary (candidate_id)
    WHERE candidate_id IS NOT NULL;
