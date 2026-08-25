# Operations

The administrative and webhook HTTP contract is published as
[`openapi.yaml`](openapi.yaml). The repository checks its route set against the
service router and rejects cleanup or deletion endpoints in both surfaces.

`/livez` reports process liveness. `/readyz` becomes successful only after the
database, lock, migrations, runtime, and initial durable-start dispatch are
ready. `/metrics` is OpenMetrics text and includes bounded-route HTTP counts and
latency, outbox depth, task states, and inventory size/freshness.

When the service has `SPOROS__AUTH__API_KEY` configured, supply the same
environment variable to `sporosctl`. If the service has no API key,
`sporosctl` sends no authorization header:

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

## Failure diagnosis and retry timing

Runtime warning and error records include the complete causal chain in the
`error` field. Transient dependency and background-service failures retry with
exponential backoff from one second to five minutes and 20% bounded jitter.
Durable workflow jitter is derived from workflow identity so replay uses the
same timer. A valid longer Prowlarr `Retry-After` value takes precedence.

Healthy refresh intervals and state-observation timers remain periodic: they
are scheduling and reconciliation controls, not failed-operation retries.

## Structured decision logs

Sporos logs operator-relevant ingress and workflow decisions as structured
records. Correlate an Autobrr request through durable processing with
`request_id`, `candidate_id`, and `task_id`; matching and injection records add
`match_id` and `plan_id`. The `decision` and `reason` fields describe why an
announcement was accepted, rejected, deferred, matched, stopped, resumed, or
failed. Matching records also report source and mapping counts, mapped and
missing bytes, and the selected mode. Injection records summarize link reuse,
piece integrity, and the final qBittorrent state.

Expected negative decisions such as `no_plausible_source` are `INFO`. Failures
that need dependency, configuration, filesystem, or database attention are
`WARN` or `ERROR` and include the complete causal chain in `error`. Health and
metrics probes are excluded from HTTP access logs. Torrent payloads,
authorization headers, and API keys are never logged.

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
