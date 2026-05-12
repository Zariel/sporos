# ADR 0001: Durable announce work queue

## Status

Accepted, not implemented.

## Date

2026-05-12

## Context

Sporos receives announce requests from automation that is bursty, time
sensitive, and dependent on external systems. A synchronous announce workflow
couples the caller's HTTP request to matching, torrent download, client
inspection, side effects, and retry timing. That makes the caller part of the
reliability boundary for cases the daemon is better placed to handle:

- the source torrent is still downloading and may become usable later;
- an indexer, tracker, Arr service, notification endpoint, or torrent client is
  temporarily unavailable;
- a pod restarts while announce work is in flight;
- a burst arrives faster than matching, downloading, or injection should run;
- dependency backoff or `Retry-After` should delay work without failing it.

Announces are not the source of truth for rebuilding system state. SQLite
state, local torrent cache, client inventory, and filesystem state remain the
operational state. Announce requests are durable, bounded work items that drive
matching and side effects.

## Decision

Sporos will use a durable inbox-style announce work queue.

The daemon accepts a valid announce, persists it as work, and acknowledges it
before processing. Processing is asynchronous, bounded, restart-safe, and owned
by the daemon. The queue owns retry timing, waiting, deduplication, expiry,
lease recovery, and terminal failure state.

The API contract for accepted announce work is:

- authenticate and validate synchronously;
- reject malformed, unauthorized, unsupported, or impossible requests
  synchronously;
- persist potentially actionable work before acknowledging it;
- return `202 Accepted` with a stable work id for queued or deduplicated work;
- process work through bounded daemon workers;
- retry or wait until success, terminal failure, or expiry;
- expose queue state through status, health, logs, and metrics.

This is a durable work queue, not event sourcing and not a generic workflow
engine. Queue wakeups are advisory signals; workers always re-read durable state
before making decisions.

## Goals

- Let callers hand off a valid announce once.
- Survive process and pod restarts after work is accepted.
- Absorb bursts with explicit capacity and backpressure.
- Make local source-incomplete and dependency-unavailable states visible and
  retryable inside the daemon.
- Avoid unsafe generic retries around non-idempotent torrent-client mutations.
- Keep state small, indexed, inspectable, and bounded by TTL and retention
  policy.

## Non-goals

- Full event sourcing.
- Multiple active writers against the same SQLite app state.
- A general-purpose task runner for unrelated workflows.
- Kubernetes manifests or Helm details.
- Compatibility with a synchronous final-result announce response before the
  first release.

## API Contract

The Sporos-native endpoint is:

```text
POST /v1/announcements
```

Accepted response:

```json
{
  "id": "ann_01h...",
  "status": "queued"
}
```

Repeated announces with the same dedupe key return `202 Accepted` and the
existing non-terminal work id. The response may include a `deduplicated` flag,
but must not expose raw dedupe material.

Immediate `4xx` responses are used for authentication failure, malformed JSON,
invalid URLs, unsupported request shapes, unsafe paths, blocklisted candidates,
or other cases that cannot become actionable by waiting. Immediate `5xx`
responses are reserved for cases where the daemon cannot durably accept work,
such as database unavailability. Capacity exhaustion should return a clear
retryable API error before work is accepted.

The accepted response is not a final matching or injection result. Operators and
automation that need progress can use status surfaces rather than making the
announce request carry the whole workflow.

## Work Item Model

An announce work item carries enough state to resume, debug, and bound work:

- stable work id;
- received, updated, first-attempt, and finished timestamps;
- dedupe hash built from stable candidate identity;
- sanitized candidate identity fields, such as tracker host, guid, info hash
  when known, title, category, size, and published time;
- secret-bearing fetch material only when needed, stored as operational state
  and excluded from logs, metrics, and support dumps;
- status;
- reason code;
- attempt count;
- next attempt timestamp;
- expiry timestamp;
- lease owner and lease expiry while running;
- last dependency kind and safe dependency name when waiting on one;
- last error class and redacted message;
- last decision or action outcome when available.

Status and reason are separate. Status drives scheduling; reason explains why a
work item is in that state.

Core statuses:

- `queued` - ready to claim when capacity allows;
- `running` - leased by a worker;
- `waiting` - blocked on known state that may change, such as source
  completion or dependency recovery;
- `retryable` - delayed after a transient failure;
- `succeeded` - workflow reached a successful terminal outcome;
- `terminal_failed` - no retry should change the outcome;
- `expired` - TTL elapsed before terminal success or failure.

## Storage

The initial unreleased SQLite schema should include an `announce_work` table or
equivalent queue-owned table. It should be part of the inline initial schema
until the first Rust release.

Required indexes:

- unique dedupe hash for non-expired active work;
- claimable work by status and `next_attempt_at`;
- expiry by `expires_at`;
- lease recovery by `lease_until`;
- status and reason snapshots for metrics/status.

Repository operations should be narrow and transactional:

- insert or return existing non-terminal work by dedupe hash;
- reject new work when durable capacity policy is exceeded;
- claim a bounded batch of ready work with a lease;
- renew or release a lease;
- mark waiting with next wake time and reason;
- mark retryable with backoff and reason;
- mark succeeded or terminal failed with outcome context;
- expire old non-terminal work;
- recover stale leases after restart;
- return bounded status and metric snapshots.

SQLite should run with WAL, foreign keys, and a busy timeout. Workers must not
hold database statements, transactions, mutex guards, or scarce resources across
unbounded `.await` points.

## Dedupe

Dedupe prevents IRC bursts and caller retries from creating duplicate work.

The dedupe key should prefer, in order:

- tracker or indexer identity plus info hash when known;
- tracker or indexer identity plus normalized guid;
- tracker or indexer identity plus normalized download link fingerprint;
- a conservative fallback built from sanitized title, size, and published time
  only when no stronger key exists.

Raw passkey URLs, cookies, API keys, and request bodies must not appear in
dedupe logs, metrics, or status JSON. If a secret-bearing URL is needed later
for torrent download, store it only as secret operational state and expose a
redacted form.

The queue returns the existing work id for duplicate active announces. Expired
or terminal retained work does not block a new announce unless the terminal
decision is explicitly safe to reuse.

## State Transitions

Normal flow:

```text
queued -> running -> succeeded
queued -> running -> waiting -> queued
queued -> running -> retryable -> queued
queued -> running -> terminal_failed
queued|waiting|retryable|running -> expired
running with stale lease -> queued
```

Workers may only transition work they currently lease, except for expiry and
stale lease recovery. Every transition records a reason code and updates
timestamps.

Wakeups are advisory. Inventory refresh, source download completion, dependency
health recovery, candidate torrent cache completion, or scheduled retry can wake
the queue. A woken item is still revalidated by the worker before any action.

## Retry And Waiting Semantics

Waiting is for a known condition that may change without the work itself
failing. Retryable is for a transient failed attempt.

Waiting examples:

- local source exists but is incomplete;
- local inventory is refreshing;
- a dependency has an active `retry_after`;
- a candidate torrent is being downloaded by another worker;
- a client reports a checking or transitional state.

Retryable examples:

- timeout, `429`, `5xx`, or honored `Retry-After` from indexers/trackers;
- temporary torrent-client read or health failure;
- temporary notification delivery failure if delivery is tied to the outcome;
- transient database or filesystem contention where retrying is safe.

Terminal examples:

- failed authentication or malformed request, normally rejected before insert;
- unsupported announce shape or invalid URL;
- blocklisted candidate or local item;
- same info hash or already-present terminal decision;
- unsafe filesystem path or invalid torrent metadata;
- invalid runtime configuration that cannot be fixed by waiting for another
  event.

No-match decisions require conservative handling. If local inventory or source
state could plausibly change during the TTL, mark the item waiting or retryable.
Only mark a no-match terminal when the decision is not expected to change before
expiry.

Retry delays use bounded jittered exponential backoff, preserve protocol
semantics such as `Retry-After`, and are capped. TTL is the primary guard
against infinite work. A max-attempt policy may be added only as a secondary
anti-spin guard.

## Side Effects And Idempotency

The announce queue may retry matching, reads, downloads, and status checks when
classified as safe. It must not blindly retry non-idempotent mutations.

Torrent injection, link creation, recheck, resume, and notification delivery
need operation-specific safety:

- make an action plan before mutation;
- re-check client and filesystem state before mutation;
- persist enough action context to know whether retry is safe;
- use client-specific existing-info-hash checks before injection;
- serialize client mutations where a client requires it;
- prefer saved-for-retry behavior over repeating ambiguous mutations;
- classify ambiguous side-effect failures as waiting for inspection or safe
  recovery, not as a generic retry loop.

## Dependency Health And Circuit Behavior

The queue consumes dependency health and backoff state. Full circuit breakers
are not required for the first implementation.

Endpoint-specific health should influence `next_attempt_at`. When a dependency
is unavailable or has `retry_after`, affected work waits instead of repeatedly
probing it. Recovery probes should be bounded and should update dependency
health. Later circuit-breaker behavior can build on the same dependency health
model if repeated failures show it is needed.

Dependencies that should feed queue scheduling include:

- Torznab indexers and trackers;
- Arr parse APIs;
- torrent-client read and health paths;
- notification webhooks;
- local filesystem and database health.

Torrent-client mutation paths remain special and are not governed by generic
dependency retries alone.

## Runtime

The runtime owns a bounded worker pool for announce work. A small in-memory
wake channel can reduce latency, but durable SQLite state is authoritative.

Workers:

- claim small batches from SQLite;
- process one work item at a time per worker;
- maintain or release leases explicitly;
- update durable state after each decision;
- observe cancellation on shutdown;
- avoid unbounded fan-out over clients, files, indexers, or candidates;
- page and stream large inventories instead of loading them into memory.

On startup, the daemon recovers stale `running` leases whose lease deadline has
passed, expires old work, and starts workers only after local state is usable.

On shutdown, the daemon stops accepting new work, cancels or drains workers
according to policy, records safe state for in-flight work, and exits without
depending on destructors for fallible cleanup.

## Capacity And Retention

Queue policy is explicit configuration:

- max pending non-terminal announce work;
- worker concurrency;
- claim batch size;
- lease duration and renewal cadence;
- default TTL;
- initial, max, and jittered retry delay;
- success and failure retention duration;
- optional per-source or per-tracker acceptance limits.

If durable capacity is exceeded before insert, reject the request. Once work is
accepted, it must remain durable until terminal state, expiry, or retention
cleanup.

## Observability

Logs, status, readiness, and metrics should make waiting understandable without
exposing secrets.

Metrics should include:

- accepted, deduplicated, rejected, expired, succeeded, and terminal-failed
  counts;
- backlog by status and reason with bounded labels;
- oldest work age;
- attempts by outcome class;
- retry delay and time-to-terminal histograms;
- leases claimed, renewed, released, and recovered;
- worker busy/idle counts;
- dependency wait counts by safe dependency kind and configured name.

Readiness should distinguish whether Sporos can accept durable work from
whether all dependencies are healthy enough to process it immediately. A
degraded indexer should not necessarily make the pod unable to accept work, but
database unavailability or a stopped worker pool should.

Status JSON should expose bounded queue snapshots, reason summaries, and safe
dependency state. It must not include passkeys, cookies, API keys, raw announce
bodies, or full secret URLs.

## Tests

Required coverage:

- API validation rejects invalid/auth failures before insert;
- accepted announces are durable before `202`;
- duplicate active announces return the existing id;
- capacity failure does not acknowledge work;
- claim ordering, lease renewal, stale lease recovery, and expiry;
- waiting versus retryable versus terminal classification;
- `Retry-After` and dependency health scheduling;
- source-incomplete work wakes after inventory/client state changes;
- worker shutdown leaves safe resumable state;
- side-effect ambiguity is not retried generically;
- metrics and status have bounded labels and redacted fields;
- restart integration test proves accepted work is resumed.

Use temporary SQLite databases, paused Tokio time where practical, fake
indexers/clients, and explicit fixtures for secret-bearing URLs.

## Implementation Order

1. Record this ADR and add the announce queue backlog.
2. Add announce queue domain types and configuration.
3. Add SQLite schema and repository operations.
4. Add the accept-only `POST /v1/announcements` contract.
5. Add durable worker claiming, leases, cancellation, and restart recovery.
6. Classify workflow outcomes into waiting, retryable, terminal, expired, and
   succeeded states.
7. Add event wakeups from inventory, dependency health, candidate downloads, and
   scheduled retry.
8. Add status, readiness, logs, and metrics.
9. Integrate matching, torrent download, and injection action paths.
10. Add end-to-end restart, burst, and degraded-dependency tests.

## Consequences

Positive consequences:

- callers can hand off valid work once;
- announce bursts are bounded by daemon policy;
- accepted work survives pod restarts;
- source-incomplete and dependency outage states become visible daemon state;
- retry behavior becomes consistent and testable.

Costs and risks:

- API semantics are asynchronous;
- persistence and scheduling complexity increases;
- TTL, dedupe, and retention must be strict to prevent stale backlog growth;
- classification errors can either drop useful work or retry unsafe work;
- SQLite remains a single-writer operational boundary;
- side-effect retry safety must be designed per action.

## Alternatives

The strongest alternative is a reconciler-first model: store candidates as
state and repeatedly reconcile desired torrent state against actual local,
client, and indexer state. That may become attractive if Sporos grows into a
broader desired-state controller, but it is heavier than the announce reliability
problem requires now.

External brokers such as Redis, NATS, RabbitMQ, or SQS add deployment and
operational complexity before Sporos needs multi-process queue ownership.

An in-memory queue would decouple HTTP latency but would still lose accepted
work on restart.

Keeping synchronous announce behavior leaves retry responsibility split between
callers and the daemon.

Full event sourcing is not aligned with the existing operational state model.

## Open Questions

- What should the default announce TTL be?
- What retention duration is useful after success, failure, and expiry?
- Which no-match decisions are safe to cache until expiry?
- Should the dedupe hash use a process-local secret or a configured stable
  secret?
- Should status lookup by work id be part of the first API slice?
- What is the first-release stance on multiple daemon replicas sharing one
  SQLite state directory?
