# Operations

`/livez` reports process liveness. `/readyz` becomes successful only after the
database, lock, migrations, runtime, and initial durable-start dispatch are
ready. `/metrics` is OpenMetrics text and includes bounded-route HTTP counts and
latency, outbox depth, task states, and inventory size/freshness.

Use `SPOROS_ADMIN_TOKEN` or `SPOROS_ADMIN_TOKEN_FILE` with `sporosctl`:

```console
sporosctl status
sporosctl inventory reconcile
sporosctl tasks list --state failed
sporosctl tasks show <task-id>
sporosctl tasks events <task-id>
sporosctl tasks retry <task-id>
sporosctl tasks cancel <task-id>
sporosctl operations list
sporosctl operations show <operation-id>
sporosctl search --indexer 7 --dry-run
sporosctl data scan media
```

Cancellation asks Duroxide to cancel the authoritative instance and starts a
durable reconciliation workflow. It never removes torrents, files, or links.
A retry is accepted only for a failed or cancelled supported task; it preserves
old task events and atomically enqueues a new version-pinned instance.

## Backup and restore

SQLite online backup is safe while the service runs, but a quiesced backup gives
the simplest operational proof:

1. Send `SIGTERM` and wait for graceful exit.
2. Run `sqlite3 /data/sporos.db '.backup /backup/sporos.db'`. If copying files
   instead, copy `sporos.db`, `sporos.db-wal`, and `sporos.db-shm` together.
3. Record the image version and configuration alongside the backup.
4. Run `sqlite3 /backup/sporos.db 'PRAGMA quick_check;'` and store the copy on a
   different failure domain.

Restore into an empty writable data directory, set directory mode `0700` and
database mode `0600`, then start the same or a schema-compatible newer image.
Readiness remains false until migrations and Duroxide recovery complete. Never
restore over a database owned by a running process.

## Upgrades and rollback

Stop the old container cleanly, take and validate a backup, then start the
immutable new version tag against the same volume. Compatible releases retain
previous workflow registrations so active histories replay with their pinned
implementation.

Schema migrations are forward-only. Rollback means stopping the new image and
restoring the pre-upgrade backup before starting the older image. Never point an
older binary at a migrated database. Unknown future migrations and changed
migration checksums are rejected at startup.

SQLite must live on a local POSIX/block-backed filesystem with reliable advisory
locking. NFS, SMB, and object-backed filesystem layers are unsupported.
