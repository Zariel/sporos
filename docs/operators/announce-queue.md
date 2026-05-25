# Announce Queue Operations

The durable announce queue is enabled in the daemon runtime. A `202 Accepted`
response from `POST /v1/announcements` means the request was validated and
stored in SQLite, not that matching, saving, or client injection has already
finished.

## Accepted Versus Processed

`POST /v1/announcements` validates auth and the request body before creating
work. Valid work is persisted before the API returns. Duplicate active work
returns the existing work id with `deduplicated: true`; callers should treat
that as accepted work and should not retry just to get a new id.

Queued work is processed by daemon-owned workers. Workers claim small batches
with leases, process one item at a time, and release or renew the lease as the
workflow advances. On restart, stale running leases are recovered so work can be
claimed again from durable state.

## Queue Health

Readiness separates accepting work from immediate processing health:

- `readiness.accepting_work` is true when config, database, schema, and state
  paths are usable enough to durably accept announcements.
- `readiness.processing_ready` additionally requires workers to be running.

A degraded indexer or torrent client can delay processing without necessarily
making the service unable to accept work. Database unavailability, schema
failure, unwritable state paths, or stopped workers require operator attention.

`GET /v1/status` includes `announce_queue` when the durable queue is configured.
The snapshot reports active work, max pending capacity, worker capacity,
busy/idle workers, status and reason counts, retry delay, oldest active age,
running leases, attempt classes, and dependency waits. It intentionally omits
raw request bodies, cookies, API keys, passkeys, and full secret-bearing URLs.

## Metrics

`GET /metrics` exposes Prometheus text metrics. For announcements, watch:

- `sporos_workflow_enqueue_total{workflow="announcement",outcome=...}` for
  accepted, deduplicated, rejected, and invalid requests.
- `sporos_announce_work_total{status=...,reason=...}` for backlog shape.
- `sporos_announce_active_work` and `sporos_announce_oldest_active_age_seconds`
  for queue age and accumulation.
- `sporos_announce_next_retry_delay_seconds` for the nearest scheduled retry.
- `sporos_announce_running_leases` plus worker busy/idle/capacity gauges for
  worker utilization.
- `sporos_announce_attempts_total{outcome_class=...}` for retry and terminal
  outcome patterns.
- `sporos_announce_dependency_wait_count{dependency_kind=...,dependency_name=...}`
  for work blocked on safe dependency identifiers.

Metric labels are bounded to workflow, status, reason, dependency kind/name, and
outcome classes. Secret-bearing fields are not metric labels.

## TTL And Retention

`announce.default_ttl_secs` bounds how long non-terminal work can remain active.
Expired work moves to the `expired` terminal state and stops being claimable.
Queued, running, waiting, and retryable announce work may retain plaintext
`announce_work.download_url` and `announce_work.cookie` values in SQLite so the
daemon can recover and retry the workflow after restart. Those fields are
sensitive local state, not operator-facing diagnostics. Successful, terminal
failed, and expired transitions scrub the raw fetch material from retained rows.

Succeeded work is retained for `announce.success_retention_secs`; failed work is
retained for `announce.failure_retention_secs`. Retention is for operator
visibility and dedupe history. It is not a source of retry behavior after a
terminal state.

The scheduled `cleanup` job applies these bounds. It recovers stale running
leases whose lease deadline has passed, expires active work past its TTL, and
removes retained terminal work after the configured retention window. The job is
bounded per run so cleanup cannot monopolize the daemon. Configure its cadence
with `[scheduling].cleanup_interval`, which defaults to `24h`, or queue an
immediate run with `POST /v1/jobs/cleanup/runs`.

## Retry And Waiting Reasons

Workers distinguish waiting from retryable failures:

- `waiting` means Sporos is intentionally waiting for local or dependency state,
  such as source inventory, candidate download, or dependency recovery.
- `retryable` means an operation failed in a way that can be retried safely.
- `terminal_failed`, `expired`, and `succeeded` are terminal and are not claimed
  again.

Retry timing is daemon-owned. Operators should not ask announce callers to retry
source-incomplete work externally; the queue wakes or retries work from durable
state.

## Restarts

On startup, workers recover stale running leases whose `lease_until` has passed.
The queue source of truth is SQLite, not an in-memory channel, so accepted work
survives process restart as long as the state volume is intact.

If a shutdown occurs while work is running, leases are released or allowed to
expire into a claimable state. In-progress side effects must still be rechecked
by the workflow before any client mutation is attempted again.

## SQLite Single Writer

Sporos uses SQLite as the durable local state store. Run one writer instance for
the same database file. In Kubernetes, mount the state database on persistent
storage and avoid running multiple replicas against the same SQLite file.
