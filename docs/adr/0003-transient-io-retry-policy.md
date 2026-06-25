# ADR 0003: Transient IO retry policy

## Status

Accepted for implementation.

## Date

2026-06-25

## Context

Sporos is a long-running service that depends on torrent clients, indexers,
Arr/Prowlarr services, notification endpoints, SQLite, and local state/cache
files. Operators should expect transient failures such as connection resets,
timeouts, temporary database busy errors, and short-lived filesystem contention.

Some retry and backoff behavior already exists, including dependency health
backoff, `Retry-After` handling, saved torrent retry, inventory refresh retry,
and notification delivery retry. That coverage is not uniform. A single
transient transport failure while fetching client inventory files can fail a
whole refresh pass, and many HTTP or XML-RPC calls rely on outer scheduler
backoff rather than bounded per-operation retry.

Retries must improve reliability without hiding semantic failures or duplicating
side effects. A retry policy therefore needs to classify both the error and the
operation being retried.

## Decision

Sporos will use one shared transient IO retry policy for fallible IO paths.
Implementations may tune budgets per call site, but all retryable operations
must follow these rules:

- retries are bounded by attempt count and elapsed delay;
- retry sleeps use jittered exponential backoff or an explicit `Retry-After`;
- retries are shutdown-aware and stop promptly during daemon shutdown;
- attempts are only made for errors classified as transient;
- terminal errors fail without retry;
- mutation IO is retried only when the operation is idempotent or has an
  explicit duplicate-safety strategy;
- retry attempts, exhaustion, and partial dependency failures are observable in
  logs, health, metrics, or durable work state.

This policy applies to network IO, local filesystem IO, SQLite busy/unavailable
conditions, and background scheduler work. It is not specific to one torrent
client.

## Error Classification

Retryable network failures include:

- connection reset, refused, aborted, temporarily unavailable, or DNS lookup
  failures that may recover;
- request timeout before a response is received;
- HTTP 408, 429, 502, 503, and 504 for idempotent requests;
- protocol transport failures for XML-RPC and HTTP client adapters;
- dependency backoff windows that are explicitly retryable.

Terminal network failures include:

- authentication or authorization failure;
- malformed request, validation failure, unsupported capability, or bad
  endpoint configuration;
- HTTP 400, 401, 403, 404 where the endpoint contract says the object or
  permission cannot appear by retrying the same request;
- response schema changes, invalid response bodies, or semantic protocol errors
  unless a caller explicitly classifies the exact error as transient;
- cancellation and daemon shutdown.

Retryable local IO failures include:

- SQLite busy or locked errors after the configured busy timeout;
- temporary filesystem contention during safe reads, atomic writes, renames, or
  cleanup deletes;
- missing cleanup targets when the operation is already best-effort;
- interrupted local IO where retrying the same operation is safe.

Terminal local IO failures include:

- permission denied;
- unsafe path validation failures;
- corrupt input or invalid torrent data;
- missing required input files or directories after startup directory creation
  has failed;
- no space left on device, read-only filesystem, or quota exceeded;
- non-UTF-8 or platform path mismatches when the specific operation cannot
  safely proceed.

## Operation Classification

Idempotent read operations should use bounded retry when their error is
transient. This includes torrent client inventory reads, per-torrent file
listing, indexer search, candidate torrent downloads, Prowlarr and Arr reads,
and dependency health probes.

Local state operations should use bounded retry when repeating the operation
cannot corrupt state. Atomic file writes should retry the whole write cycle, not
just the final rename. Cleanup should tolerate races where another worker or
previous attempt already removed the target.

Mutation operations need a stronger contract before retry:

- torrent injection must be safe through existing-info-hash checks, an
  already-exists outcome, request identity, or post-failure verification;
- torrent client commands such as recheck and resume may retry only when repeat
  execution is accepted by the client contract;
- notification sends may retry only under the configured delivery policy and
  must document whether duplicate delivery is acceptable;
- ambiguous timeout-after-send cases must be resolved by verification or treated
  conservatively.

If an operation cannot prove duplicate safety, Sporos must fail or mark work
retryable at a higher durable workflow boundary instead of blindly replaying the
mutation.

## Backoff Interaction

Per-operation retry handles short transient failures inside one workflow
attempt. Dependency health backoff and queue retry handle broader dependency
outage or workflow retry timing.

`Retry-After` from an external dependency takes precedence over locally computed
delay when it is valid and within the operation's maximum delay cap. Existing
dependency backoff state must still be updated when the retry budget is
exhausted or a dependency explicitly rate-limits Sporos.

Retry helpers must avoid retry amplification. A caller that fans out over many
items still needs explicit concurrency limits, and retry sleeps count against
that call site's work budget.

## Observability

Retry behavior must be visible enough for an operator to answer whether Sporos
is retrying, waiting, degraded, or giving up.

At minimum, retry exhaustion should record:

- operation name;
- dependency kind and safe dependency name;
- attempt count;
- final error class and redacted message;
- next retry deadline when one exists;
- whether the failure was partial or made the whole workflow fail.

Logs must not include API keys, cookies, passkeys, bearer tokens, or raw signed
URLs.

## Consequences

The implementation work should first centralize retry helpers and typed
classification, then apply them to the highest-impact IO paths. Torrent client
inventory reads and external network reads should use the shared helper before
local filesystem/database IO and mutation-specific duplicate-safety work.

Tests should cover retryable versus terminal classification, `Retry-After`
precedence, shutdown during retry sleep, bounded concurrency during retry, and
ambiguous mutation outcomes.
