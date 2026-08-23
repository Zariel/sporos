CREATE TABLE sporos_qbit_completion (
    source_id BLOB NOT NULL CHECK (length(source_id) = 16),
    completed_at INTEGER NOT NULL,
    operation_id BLOB NOT NULL UNIQUE CHECK (length(operation_id) = 16),
    task_id BLOB NOT NULL UNIQUE CHECK (length(task_id) = 16),
    created_at INTEGER NOT NULL,
    PRIMARY KEY (source_id, completed_at),
    FOREIGN KEY (source_id) REFERENCES sporos_qbit_torrent(id),
    FOREIGN KEY (operation_id) REFERENCES sporos_operation(id),
    FOREIGN KEY (task_id) REFERENCES sporos_task(id)
) STRICT, WITHOUT ROWID;
