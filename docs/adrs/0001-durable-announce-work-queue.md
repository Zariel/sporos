# ADR 0001: Durable announce work queue

## Status

Proposed

## Date

2026-05-04

## Context

Announce requests usually come from IRC-driven automation. They are time
sensitive, bursty, and dependent on external systems that may be temporarily
unavailable. The current synchronous announce API couples the caller's HTTP
request to the whole matching and action workflow. That means the caller is also
part of the retry system for cases such as:

- the source torrent is still downloading and may become usable later;
- a tracker or indexer is rate limited or temporarily unavailable;
- a torrent client is unavailable during an otherwise valid announce;
- a Kubernetes pod restarts while work is in flight;
- a burst of announces arrives faster than matching, snatching, or injection can
  safely run.

The current synchronous response model has one useful property: callers can be
told immediately when the source is incomplete and should retry. That same
behavior can be handled more reliably inside the daemon if valid announces are
persisted as work and retried under daemon control.

This should not become an event-sourced architecture. The announce stream is not
the source of truth for rebuilding domain state. The persisted SQLite state,
torrent cache, client inventory, and filesystem remain the operational state.
Announces should be treated as durable, bounded work items that drive matching
and side effects.

## Decision

Move the project toward a durable announce work queue using an inbox-style
model.

The daemon should accept a valid announce, persist it as work, and process it
asynchronously through bounded workers. The queue should own retry timing,
deduplication, terminal failure state, and expiry. External callers should not
need to keep retrying solely because the daemon is waiting for local source
completion or a transient dependency recovery.

The target contract for accepted announce work is:

- authenticate and validate the request synchronously;
- reject malformed, unauthorized, or impossible requests synchronously;
- persist potentially actionable announce work before acknowledging it;
- return `202 Accepted` with a stable work identifier for queued work;
- process queued work with bounded concurrency and backpressure;
- retry transient failures until success, terminal failure, or expiry;
- expose queue state through health, status, logs, and Prometheus metrics.

The synchronous API contract may be kept temporarily for compatibility, but it
is not the north star for the daemon. Before a `0.1` release, the project should
choose whether `/api/announce` itself adopts queued semantics or whether a
separate compatibility path remains.

## North Star

The daemon should be the reliability boundary.

When a valid announce arrives, the caller should be able to hand it off once.
The daemon should then make progress when dependencies are healthy, delay work
when they are not, recover after pod restarts, and make every non-terminal
reason visible to operators.

The desired operator experience is:

- live and ready probes describe whether the pod can accept and process work;
- metrics show backlog, attempts, outcomes, retry delays, and circuit state;
- logs explain why work is waiting without exposing secrets;
- failed work has enough persisted context to debug without replaying IRC;
- stale work expires rather than accumulating forever.

## Scope

This ADR covers announce processing and the runtime direction it implies.

In scope:

- durable announce inbox table or equivalent persisted queue state;
- bounded announce workers owned by the daemon runtime;
- retry and expiry semantics for announce work;
- deduplication of repeated announces for the same remote candidate;
- circuit-breaker integration for tracker, indexer, notification, Arr, and
  client health where appropriate;
- status, health, logging, and Prometheus metric surfaces for queued work.

Out of scope:

- event sourcing as a system-wide persistence model;
- Kubernetes manifests or Helm charts;
- replacing all workflow orchestration in one change;
- changing conservative matching decisions;
- generic retries around non-idempotent torrent-client mutations without
  operation-specific safety rules.

## Work Item Model

Announce work should carry enough state to be durable and debuggable:

- stable work id;
- received timestamp;
- dedupe key based on candidate identity, such as tracker plus guid/link and
  available info hash;
- candidate fields from the accepted request;
- status such as queued, running, waiting, succeeded, terminal_failed, expired;
- attempt count;
- next attempt timestamp;
- expiry timestamp;
- last error class and message;
- last decision or action outcome when available.

The queue should be bounded by policy:

- dedupe repeated announces instead of adding duplicate work;
- expire work after a configured TTL;
- cap retry delay and attempt growth;
- expose backlog and expired counts;
- reject or shed accepted-but-not-yet-persisted work only when persistence or
  capacity policy cannot safely accept it.

## Retry Semantics

Retries should be explicit and classified.

Retryable cases include:

- local source incomplete but otherwise eligible;
- tracker/indexer timeout, `429`, `5xx`, or `Retry-After`;
- torrent client temporarily unavailable for reads or safe status checks;
- notification endpoint unavailable, if notification delivery is tied to the
  work outcome;
- transient database or filesystem contention where retrying is safe.

Terminal cases include:

- malformed announce body;
- failed authentication;
- invalid URL or unsupported request shape;
- impossible or invalid runtime config;
- blocklisted candidate or searchee;
- same info hash or already-present terminal decisions;
- matching decisions that are not expected to change before expiry;
- unsafe filesystem paths or invalid torrent metadata.

Non-idempotent operations must keep operation-specific handling. Torrent
injection, recheck, resume, and link creation should not be retried by a generic
HTTP-style retry loop. They should be coordinated through the injection action
path and saved-for-retry behavior where that is the safer outcome.

## Circuit Breakers

Circuit breakers should support the queue, not replace the queue.

Endpoint-specific breaker state should influence `next_attempt_at` for affected
work. When a breaker is open, queued work should wait instead of repeatedly
probing a broken dependency. When the cooldown expires, one or a small bounded
number of half-open attempts should probe recovery.

Circuit breakers should be considered for:

- Torznab trackers and indexers;
- notification webhooks;
- Arr parse APIs;
- torrent-client read and health paths.

Torrent-client mutation paths need special treatment because their side effects
are not uniformly idempotent.

Breaker state should be visible in logs, readiness/status, and metrics.

## API Direction

The target queued announce API should acknowledge accepted work rather than
returning final workflow outcomes synchronously.

Recommended response shape:

```text
202 Accepted
{
  "id": "<work-id>",
  "status": "queued"
}
```

Immediate `4xx` responses should remain for invalid requests and auth failures.
Immediate `5xx` responses should be reserved for cases where the daemon cannot
durably accept the work, such as database unavailability.

The current synchronous result mapping should be treated as a compatibility
contract until the project intentionally changes it before release or provides a
separate endpoint/mode.

## Incremental Plan

1. Add durable announce work storage and repository helpers.
2. Add queue state and worker orchestration to the daemon runtime.
3. Persist valid announces and process them asynchronously behind a feature or
   compatibility boundary.
4. Classify workflow outcomes into retryable, waiting, terminal, and succeeded
   states.
5. Add TTL, dedupe, bounded concurrency, and backpressure policy.
6. Integrate endpoint circuit state into retry scheduling.
7. Expose queue metrics, breaker state, and work outcome counters.
8. Decide the release API contract for `/api/announce` before `0.1`.

## Consequences

Positive consequences:

- the daemon can absorb IRC bursts with bounded work instead of request
  concurrency;
- source-incomplete retries become internal and durable;
- pod restarts no longer lose accepted announce work;
- transient dependency outages become delayed work rather than failed requests;
- operators can inspect backlog and failure reasons.

Costs and risks:

- API semantics change if `/api/announce` stops returning final outcomes;
- stale announce work can accumulate without strict TTL and dedupe;
- queue state adds persistence and scheduler complexity;
- retry classification must be conservative to avoid repeated unsafe side
  effects;
- multiple daemon replicas against the same app directory remain unsafe unless a
  separate single-writer or leader-election design is added.

## Alternatives Considered

### Keep synchronous announce processing

This keeps API compatibility simple but leaves retry responsibility split
between external callers and the daemon. It is weaker for Kubernetes restarts,
dependency outages, and IRC bursts.

### Full event sourcing

This would make an append-only event stream the primary model for rebuilding
state. It is unnecessary for the current domain and would add complexity without
solving the immediate reliability problem better than a durable work queue.

### In-memory queue only

This would reduce request coupling but would still lose accepted work on pod
restart and would not provide enough debugging state for operators.

## Open Questions

- What default TTL should announce work use?
- Should `/api/announce` switch to queued semantics before `0.1`, or should a
  separate queued endpoint/mode be introduced first?
- What is the exact dedupe key when a tracker does not provide a stable info
  hash before torrent download?
- Which terminal non-match decisions are safe to cache for the full TTL?
- How much per-work-item history should be retained after success or expiry?
