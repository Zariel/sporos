ALTER TABLE sporos_qbit_torrent ADD COLUMN qbit_id TEXT;
ALTER TABLE sporos_qbit_torrent ADD COLUMN content_fingerprint BLOB
    CHECK (content_fingerprint IS NULL OR length(content_fingerprint) = 32);
ALTER TABLE sporos_qbit_torrent ADD COLUMN file_manifest_state TEXT NOT NULL DEFAULT 'unloaded';
ALTER TABLE sporos_qbit_torrent ADD COLUMN file_manifest_loaded_at INTEGER;

CREATE UNIQUE INDEX sporos_qbit_torrent_qbit_id_idx
    ON sporos_qbit_torrent (qbit_id) WHERE qbit_id IS NOT NULL;
CREATE INDEX sporos_qbit_torrent_manifest_idx
    ON sporos_qbit_torrent (file_manifest_state, is_complete, available, id);

CREATE TABLE sporos_qbit_inventory_state (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    response_id INTEGER CHECK (response_id IS NULL OR response_id >= 0),
    generation INTEGER NOT NULL CHECK (generation >= 0),
    baseline_at INTEGER,
    last_success_at INTEGER,
    last_full_reconcile_at INTEGER,
    reconcile_requested_at INTEGER,
    application_version TEXT,
    web_api_version TEXT
) STRICT;

INSERT INTO sporos_qbit_inventory_state (singleton, generation) VALUES (1, 0);
