ALTER TABLE sporos_task RENAME COLUMN generation TO projection_generation;
ALTER TABLE sporos_task RENAME COLUMN attempt_count TO observed_retry_count;
ALTER TABLE sporos_task ADD COLUMN duroxide_execution_id TEXT;
ALTER TABLE sporos_outbox RENAME COLUMN attempt_count TO start_delivery_attempt_count;
ALTER TABLE sporos_outbox ADD COLUMN permanent_failure_at INTEGER;

CREATE TRIGGER sporos_task_require_instance_insert
BEFORE INSERT ON sporos_task
WHEN NEW.duroxide_instance_id IS NULL
BEGIN
    SELECT RAISE(ABORT, 'duroxide_instance_id is required');
END;

CREATE TRIGGER sporos_task_require_instance_update
BEFORE UPDATE OF duroxide_instance_id ON sporos_task
WHEN NEW.duroxide_instance_id IS NULL
BEGIN
    SELECT RAISE(ABORT, 'duroxide_instance_id is required');
END;
