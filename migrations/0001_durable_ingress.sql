CREATE TABLE sporos_schema_metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
) STRICT;

INSERT INTO sporos_schema_metadata (key, value)
VALUES ('application_schema', '1');

CREATE TABLE sporos_policy_snapshot (
    id BLOB PRIMARY KEY CHECK (length(id) = 16),
    config_hash BLOB NOT NULL CHECK (length(config_hash) = 32),
    matcher_version TEXT NOT NULL,
    payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
    created_at INTEGER NOT NULL
) STRICT;

CREATE TABLE sporos_task (
    id BLOB PRIMARY KEY CHECK (length(id) = 16),
    kind TEXT NOT NULL,
    state TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK (generation >= 0),
    operation_id BLOB CHECK (operation_id IS NULL OR length(operation_id) = 16),
    duroxide_instance_id TEXT,
    policy_snapshot_id BLOB NOT NULL REFERENCES sporos_policy_snapshot(id),
    reason_code TEXT,
    last_error_class TEXT,
    last_error_message TEXT,
    attempt_count INTEGER NOT NULL CHECK (attempt_count >= 0),
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    terminal_at INTEGER
) STRICT;

CREATE INDEX sporos_task_state_idx ON sporos_task (state, created_at, id);
CREATE INDEX sporos_task_operation_idx ON sporos_task (operation_id, id);
CREATE INDEX sporos_task_terminal_idx ON sporos_task (terminal_at, id);

CREATE TABLE sporos_task_event (
    id INTEGER PRIMARY KEY,
    task_id BLOB NOT NULL REFERENCES sporos_task(id),
    sequence INTEGER NOT NULL CHECK (sequence >= 0),
    state TEXT NOT NULL,
    reason_code TEXT,
    detail_json TEXT CHECK (detail_json IS NULL OR json_valid(detail_json)),
    created_at INTEGER NOT NULL,
    UNIQUE (task_id, sequence)
) STRICT;

CREATE TABLE sporos_outbox (
    id INTEGER PRIMARY KEY,
    task_id BLOB NOT NULL REFERENCES sporos_task(id),
    task_key BLOB NOT NULL UNIQUE CHECK (length(task_key) = 32),
    orchestration_name TEXT NOT NULL,
    orchestration_version TEXT NOT NULL,
    instance_id TEXT NOT NULL UNIQUE,
    input_json TEXT NOT NULL CHECK (json_valid(input_json)),
    visible_at INTEGER NOT NULL,
    attempt_count INTEGER NOT NULL CHECK (attempt_count >= 0),
    lease_token BLOB,
    lease_until INTEGER,
    dispatched_at INTEGER,
    last_error TEXT
) STRICT;

CREATE INDEX sporos_outbox_visible_idx
    ON sporos_outbox (dispatched_at, visible_at, id);
