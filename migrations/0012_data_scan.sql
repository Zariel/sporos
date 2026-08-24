CREATE TABLE sporos_data_scan_state (
    operation_id BLOB PRIMARY KEY REFERENCES sporos_operation(id)
        CHECK (length(operation_id) = 16),
    root_name TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK (generation >= 0),
    observed_releases INTEGER NOT NULL DEFAULT 0 CHECK (observed_releases >= 0),
    state TEXT NOT NULL,
    reason_code TEXT,
    updated_at INTEGER NOT NULL
) STRICT;

CREATE TABLE sporos_data_scan_directory (
    operation_id BLOB NOT NULL REFERENCES sporos_operation(id),
    relative_path BLOB NOT NULL,
    depth INTEGER NOT NULL CHECK (depth >= 0),
    cursor_name BLOB,
    state TEXT NOT NULL,
    PRIMARY KEY (operation_id, relative_path)
) STRICT, WITHOUT ROWID;

CREATE INDEX sporos_data_scan_directory_state_idx
    ON sporos_data_scan_directory (operation_id, state, relative_path);

ALTER TABLE sporos_data_source ADD COLUMN normalized_title TEXT;
ALTER TABLE sporos_data_source ADD COLUMN modified_at INTEGER;
ALTER TABLE sporos_source_file ADD COLUMN modified_at INTEGER;

CREATE INDEX sporos_data_source_identity_idx
    ON sporos_data_source (normalized_title, available, id);
