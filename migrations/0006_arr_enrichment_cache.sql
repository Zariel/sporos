CREATE TABLE sporos_arr_enrichment_cache (
    source_id BLOB NOT NULL CHECK (length(source_id) = 16),
    instance_kind TEXT NOT NULL CHECK (instance_kind IN ('sonarr', 'radarr')),
    instance_name TEXT NOT NULL,
    source_hash TEXT NOT NULL,
    identity_json TEXT CHECK (identity_json IS NULL OR json_valid(identity_json)),
    fetched_at INTEGER NOT NULL,
    negative_expires_at INTEGER,
    etag TEXT,
    PRIMARY KEY (source_id, instance_kind, instance_name),
    FOREIGN KEY (source_id) REFERENCES sporos_qbit_torrent(id)
) STRICT, WITHOUT ROWID;

CREATE INDEX sporos_arr_enrichment_cache_expiry_idx
    ON sporos_arr_enrichment_cache (negative_expires_at, source_id);
