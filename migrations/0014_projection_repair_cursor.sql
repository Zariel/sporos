ALTER TABLE sporos_task ADD COLUMN projection_repair_checked_at INTEGER;

CREATE INDEX sporos_task_projection_repair_idx
    ON sporos_task (
        terminal_at,
        projection_repair_checked_at IS NOT NULL,
        projection_repair_checked_at,
        updated_at,
        id
    );
