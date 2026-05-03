# Runtime Concurrency Design

This document defines the target daemon runtime for the queue migration. It is
the compatibility contract for replacing broad workflow `spawn_blocking`,
process-local locks, and ad hoc background tasks with explicit Tokio-owned
services.

The public CLI, TOML config, HTTP API, scheduler decisions, matching results,
saved torrent filenames, cache filenames, and torrent-client side effects do
not change as part of this migration.

## Current Runtime Map

The current daemon starts with `index_torrents_and_data_dirs`, then starts the
HTTP server when `port` is configured, then runs scheduler checks every minute.
HTTP handlers, scheduler checks, announce, webhook, RSS, search, inject,
cleanup, indexer caps refresh, and restore all eventually call workflow
functions in `src/operations.rs`.

The important current controls are:

| Current control | Current owner | Target owner |
| --- | --- | --- |
| `DaemonState.scheduler: Mutex<Scheduler>` | HTTP and scheduler loop | scheduler actor plus job queue |
| `Scheduler.check_jobs: Mutex<()>` | scheduler object | scheduler actor mailbox |
| raw webhook `tokio::spawn` | API request handler | background-work queue |
| `LOCAL_WORK_PERMITS` | whole workflow wrapper | named blocking executors |
| `CLIENT_INJECTION` | static action mutex | injection actor |
| `ReverseLookupGate` | per workflow runtime | shared reverse-lookup queue |
| filesystem indexing calls | daemon/API/job workflow callers | index refresh service |
| client/indexer HTTP calls | workflow helpers | async integration APIs and bounded request workers |

## Runtime Services

`RuntimeServices` is the daemon-owned service container. It is created during
daemon startup after config validation and database migration, and it is the
only object handed to HTTP handlers and the scheduler loop for background work.
It owns queue handles, cancellation tokens, worker join handles, and queue
capacity configuration.

The first implementation can use named constants for capacities and worker
counts. Later config exposure must be additive and must keep defaults compatible
with current behavior.

| Service | Shape | Default limit | Purpose |
| --- | --- | --- | --- |
| Scheduler actor | single consumer `mpsc` | 64 commands | Owns job state, eligibility, early-run requests, completion messages. |
| Job queue | bounded worker pool | 8 queued, 2 active | Runs accepted `rss`, `search`, `updateIndexerCaps`, `inject`, and `cleanup` jobs. |
| Background queue | bounded worker pool | 64 queued, 4 active | Runs validated webhook and API-triggered background work. |
| Reverse lookup queue | single consumer | 256 queued | Serializes RSS and announce reverse matching. |
| Injection actor | single consumer | 128 queued | Serializes client mutation: inject, recheck, resume, saved-retry side effects. |
| Index refresh service | single consumer with coalescing | 16 queued | Refreshes `torrent_dir`, `data_dirs`, and client searchee indexes. |
| Filesystem executor | bounded blocking workers | 64 queued, 4 active | Directory walks, torrent cache IO, link creation, cleanup file operations. |
| CPU executor | bounded blocking workers | 64 queued, 4 active | Torrent parsing, bencode hashing, heavy matching/fuzzy filtering. |
| External request workers | bounded async fan-out | per operation | Torznab, Arr, notifications, and torrent-client HTTP/XML-RPC calls. |

The named queues are intentionally small. The daemon handles long-running
inventory and network work; accepting unbounded work would make memory use and
shutdown latency unpredictable on large torrent inventories.

## Queue Ownership

### Scheduler Actor

The scheduler actor owns `Scheduler` and is the only task allowed to mutate job
state. API and timer callers submit typed commands:

- `CheckJobs { now, is_first_run }`;
- `RequestEarlyRun { name, now, overrides }`;
- `JobFinished { name, outcome }`;
- `Snapshot`.

The actor decides eligibility, marks jobs active, persists `job_log.last_run`,
and enqueues accepted job commands. It never awaits job body execution while
holding scheduler state. Job workers report completion back to the actor so
`is_active` is cleared on success, error, cancellation, or panic recovery.

Compatibility rules preserved by the actor:

- disabled jobs return `404`-compatible responses from `/api/job`;
- active jobs return `409`-compatible "already running" responses;
- a future `last_run` rejects duplicate early runs;
- early `search` and `rss` set `delay_next_run`;
- RSS activity blocks non-RSS scheduled work unless an early run explicitly
  bypasses the cadence path;
- cleanup does not start while another scheduled job is active.

### Job Queue

The job queue runs only commands accepted by the scheduler actor. Job workers
own the workflow future and its tracing span. They do not read or mutate
scheduler fields directly.

Queue-full behavior:

- periodic scheduler checks leave jobs inactive and log a warning;
- `/api/job` returns a `503` only after the scheduler has determined the job
  would otherwise be accepted but the job queue has no capacity;
- queued jobs are not duplicated by later timer checks because the scheduler
  marks them active before enqueue.

### Background Queue

The background queue owns work that should outlive the immediate API response.
The primary user is `/api/webhook`: validation still happens in the request
handler, and a valid webhook still returns `204 No Content`. After validation
the handler submits a background command and never runs targeted search inline.

Queue-full behavior for webhooks is compatibility-sensitive. A valid webhook
keeps returning `204`; the dropped command is logged with `warn` level,
including queue name and sanitized request fields. This preserves the current
"response already closed" behavior while making admission failure visible.

Announce does not use this queue because `/api/announce` response status depends
on the matching and action result.

### Reverse Lookup Queue

RSS and announce reverse matching share one runtime-owned queue. The worker
processes one reverse lookup at a time unless a later ticket records a
compatibility reason to raise the concurrency. This preserves the documented
one-at-a-time reverse-match behavior while preventing callers from bypassing
the gate by constructing a new workflow runtime.

Commands carry a caller label (`rss` or `announce`), candidate identity, and a
reply channel. If the queue is full:

- announce returns `503` because the response depends on the lookup;
- RSS logs the rejected candidate and continues the feed batch.

Cancellation of the caller drops the reply channel. The worker finishes any
already-started candidate when doing so is cheaper and safer than trying to
abort file or network side effects; otherwise it observes cancellation before
starting the next operation.

### Injection Actor

The injection actor replaces `CLIENT_INJECTION`. It receives typed commands for
candidate injection, saved-torrent retry, recheck, resume, and "already exists"
repair paths. It is the only runtime component allowed to mutate torrent
clients or linked files for injection side effects.

Callers await typed results that preserve the existing `InjectionResult`,
`SaveResult`, and `SavedInjectionSummary` behavior. A failed inject still saves
for retry when current behavior does. Link cleanup, saved torrent deletion, and
resume polling happen inside the actor or in actor-owned child work so ordering
with later client mutations remains explicit.

Queue-full behavior:

- pipeline actions that cannot enqueue injection save the candidate torrent for
  retry and return the current "saved for retry" compatible outcome;
- manual `inject` returns an operation error because the command is explicit
  user work and there is no already-open HTTP response to preserve.

### Index Refresh Service

The index refresh service owns `torrent_dir`, `data_dirs`, and client searchee
refresh orchestration. It coalesces concurrent full-refresh requests. A request
that arrives while an equivalent refresh is running attaches to the in-flight
result instead of starting a duplicate directory or client inventory scan.

Startup, scheduler jobs, announce, webhook, cleanup, and CLI workflows use the
same service boundary. CLI commands may create a short-lived service container
and wait for completion before exiting.

Queue-full behavior:

- daemon startup fails because startup indexing is required before serving;
- announce returns `503` when the required refresh cannot be admitted;
- webhook logs rejection after returning `204`;
- scheduled jobs log and skip the run so the next cadence can retry.

### Blocking Executors

Only concrete blocking operations enter blocking executors. Whole workflows do
not. The async orchestration layer remains visible to Tokio for cancellation,
backpressure, and shutdown.

Filesystem executor examples:

- walking `torrent_dir` and `data_dirs`;
- reading and writing torrent cache files;
- creating, probing, and removing links;
- cleanup scans and file deletion.

CPU executor examples:

- bencode parsing and info-hash computation;
- heavy file-tree matching;
- fuzzy title filtering over large batches.

Workers return typed operation errors and propagate panics as worker failures
with queue name and command kind in logs.

### External Requests

Torrent clients, Torznab, Arr, notifications, and snatch helpers expose async
APIs. Retry and rate-limit semantics stay with the integration layer because
those rules are protocol-specific. Runtime orchestration supplies cancellation,
bounded fan-out, and tracing context.

Non-idempotent operations such as torrent injection are not retried by generic
request workers. They are submitted through the injection actor so retry policy
can preserve client semantics.

## Shutdown And Cancellation

Daemon shutdown cancels intake first, then waits for workers.

1. Stop accepting HTTP requests and timer checks.
2. Close queue senders so no new commands are admitted.
3. Cancel queued commands that have not started.
4. Let started injection commands and filesystem mutations reach a safe point.
5. Let started external requests finish until their normal timeout, unless the
   operation is documented as abort-safe.
6. Report unfinished work with queue name, command kind, and elapsed time.
7. Close database pools and join worker tasks.

Manual CLI commands use the same service boundaries but wait only for the work
they submitted.

## Error Handling

Low-level operations return typed errors and do not decide queue policy.
Runtime services decide whether to reject, delay, drop, retry later, or convert
an error into an API response.

Errors are logged with these stable fields when available:

- `queue`;
- `command`;
- `job`;
- `label`;
- `info_hash`;
- `guid`;
- `indexer`;
- `client_host`;
- `elapsed_ms`;
- `queued_ms`.

High-volume candidate diagnostics use `debug` or `trace`. Queue rejection,
retry exhaustion, dropped webhook work, and safe cleanup failures use `warn`.
Failed API requests or worker crashes use `error`.

## Lock Order Rules

Awaiting while holding scheduler state, client mutation state, database
statements, or queue internals is forbidden unless the type is explicitly built
for that await boundary.

The allowed order is:

1. parse and validate request/config;
2. submit a typed command to a runtime service;
3. await the reply channel or return immediately for webhook;
4. let the service own any internal state mutation;
5. perform IO through async APIs or named executors.

No workflow may acquire scheduler state after starting job body work. No
workflow may call torrent-client mutation methods directly after the injection
actor exists. No service may hold an mpsc receiver borrow, database statement,
or mutex guard while calling an external service or running filesystem work.

## Observability

Every runtime service emits tracing events for:

- enqueue attempt and result;
- queue full or command dropped;
- worker start and finish;
- cancellation before start;
- cancellation during work;
- panic or worker task failure;
- elapsed runtime and time spent queued.

Queue metrics should expose depth, capacity, active workers, completed commands,
failed commands, cancelled commands, and rejected commands. Tests for later
migration tickets should exercise queue-full behavior, shutdown cancellation,
worker error propagation, and concurrent API/scheduler interactions.
