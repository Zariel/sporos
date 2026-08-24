ALTER TABLE sporos_candidate ADD COLUMN manifest_json TEXT
    CHECK (manifest_json IS NULL OR json_valid(manifest_json));

CREATE TABLE sporos_candidate_task (
    candidate_id BLOB NOT NULL REFERENCES sporos_candidate(id),
    policy_snapshot_id BLOB NOT NULL REFERENCES sporos_policy_snapshot(id),
    task_id BLOB NOT NULL UNIQUE REFERENCES sporos_task(id),
    PRIMARY KEY (candidate_id, policy_snapshot_id)
) STRICT, WITHOUT ROWID;
