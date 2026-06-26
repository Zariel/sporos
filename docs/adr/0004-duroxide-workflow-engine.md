# ADR 0004: Duroxide workflow engine replacement

## Status

Accepted for testing-environment implementation.

## Date

2026-06-26

## Context

Sporos currently has several workflow mechanisms:

- durable announce work in `announce_work`, with lease-based polling,
  retryable/waiting states, and advisory wakeups;
- in-memory bounded queues for manual search, manual job runs, inventory
  refresh, notifications, and scheduler handoff;
- persisted scheduler job state for cleanup, media inventory, client inventory,
  and indexer capability refresh;
- saved torrent retry as a daemon maintenance loop with durable files and
  database state.

This is operationally reliable enough for phase one, but it spreads workflow
semantics across repository queries, queue workers, scheduler state, retry
helpers, and daemon loops. A single logical announce may be re-claimed many
times to rediscover that a dependency is still waiting. Recent announce
no-match work made this more visible: the desired behavior is a durable wait on
fresh media and client inventory, not periodic reprocessing until the relevant
inventory happens to be fresh.

Duroxide is an embedded Rust durable execution framework. Its public API
provides an in-process Tokio runtime, SQLite storage provider, activity and
orchestration registries, durable timers, external events, and replayed
deterministic orchestration functions. Duroxide documentation is available at
<https://docs.rs/duroxide/latest/duroxide/>. The implementation backlog should
also use the upstream orchestration guide as a source for deterministic
orchestration rules, activity boundaries, replay behavior, external events,
durable event queues, custom status, KV state, sub-orchestrations, retry
policies, cancellation, and continue-as-new:
<https://github.com/microsoft/duroxide/blob/main/docs/ORCHESTRATION-GUIDE.md>.

## Decision

Sporos will evaluate Duroxide as the full durable workflow engine replacement
in a testing environment.

The implementation will be planned as a full replacement, not as a compatibility
layer over the existing workflow mechanisms. During the evaluation, new test
state can start from an empty workflow database. There is no requirement to
migrate active `announce_work` rows, scheduler state, in-memory queues, or
saved retry loop state from an existing deployment.

Duroxide orchestration history becomes the durable execution source of truth
for workflows. Sporos SQLite domain state remains the source of truth for
application data such as candidates, local inventory, dependency health,
configuration-derived state, torrent cache records, and operator projections.

The first Duroxide integration target includes all workflow classes:

- announce workflows;
- manual search workflows;
- scheduled jobs;
- media and client inventory refresh workflows;
- saved torrent retry workflows;
- manual workflow trigger requests that currently enqueue in-memory work.

Side effects must run in Duroxide activities. Orchestrations must contain
deterministic coordination only: branching, fan-out/fan-in, durable timers,
external event waits, compensation decisions, and retry policy selection.

## Goals

- Replace polling/lease workflow loops with explicit durable workflows.
- Make waits first-class, especially inventory freshness waits for announce
  no-match decisions.
- Keep Kubernetes deployment simple: no Temporal/Cadence/Conductor service.
- Preserve the Sporos API contract that accepted announce work is durable and
  processed asynchronously.
- Make workflow state easier to inspect, test, and explain to operators.
- Keep side-effect idempotency explicit at workflow boundaries.

## Non-goals

- Migrating existing active workflow rows into Duroxide.
- Keeping both workflow engines active in production indefinitely.
- Replacing Sporos domain persistence, matching, torrent parsing, or torrent
  client adapters.
- Providing Kubernetes manifests or charts.
- Introducing an external workflow service dependency.

## Architecture

Add a `DuroxideWorkflowRuntime` owned by `AppRuntime`. It owns:

- a Duroxide SQLite provider;
- orchestration registry;
- activity registry;
- workflow client;
- startup/shutdown integration;
- projection and metrics adapters.

The Duroxide database should be a separate SQLite file under Sporos state:

```text
/app/state/sporos-workflows.db
```

Keeping the workflow database separate from the Sporos domain database reduces
lock coupling and makes it clear which state belongs to durable execution
history. Both databases remain part of the same state PVC backup set.

Workflows call Sporos activities for domain operations. Activities may use the
existing `Repository`, torrent clients, indexers, filesystem actions,
notification endpoints, and retry helpers. Activities must be idempotent or
must verify prior side effects before retrying.

## Workflow Model

Workflow instance IDs must be stable and deterministic:

- announces use a dedupe-derived instance ID;
- manual searches use a generated request ID;
- scheduled job supervisors use `job:{name}`;
- scheduled job runs use child instance IDs that include job name and scheduled
  time;
- inventory refreshes use coalesced IDs by refresh kind and scope;
- saved torrent retry uses a supervisor instance plus child work instances when
  a retry item needs isolated side-effect handling.

Required workflows:

- `AnnounceWorkflow`
  - validate persisted announce input;
  - wait for required inventory freshness;
  - perform reverse lookup;
  - download/cache candidate torrent if needed;
  - assess match;
  - prepare links;
  - inject, save, dry-run, or terminalize.
- `SearchWorkflow`
  - plan indexer searches;
  - fan out indexer requests with bounded concurrency;
  - stream/process candidates through bounded candidate download and matching
    activities;
  - persist and notify summary.
- `ScheduledJobWorkflow`
  - supervise cleanup, media inventory, client inventory, and indexer caps;
  - use durable timers for intervals and failure backoff;
  - coalesce manual triggers.
- `InventoryRefreshWorkflow`
  - handle full media, changed-path media, and client inventory refresh;
  - coalesce compatible refresh requests;
  - raise completion events for waiting workflows.
- `SavedTorrentRetryWorkflow`
  - scan saved retry files;
  - validate and prefetch bounded work;
  - perform duplicate-safe recheck/injection/link cleanup.

Notifications are activities unless evaluation shows notification delivery
requires its own durable workflow.

## Waiting And Events

Duroxide external events replace advisory queue wakeups where possible.

Announce no-match waits must be modeled as a barrier:

```text
required inventory freshness >= announce received_at
```

If both media and client inventory are required, the announce waits for both
freshness events before terminalizing no-match. It must not wake early because
one dependency returned a shorter retry deadline.

Events to model:

- `media_inventory_completed`;
- `client_inventory_completed`;
- `dependency_recovered`;
- `candidate_cache_completed`;
- `manual_job_requested`;
- `workflow_cancel_requested`;
- `shutdown` where Duroxide supports cooperative cancellation.

Durable timers model retry/backoff deadlines, TTL expiry, scheduled job
intervals, and candidate download wait timeouts.

## Idempotency And Side Effects

Activities that can mutate external state must use explicit duplicate-safety
contracts:

- torrent injection checks existing info hash before mutation and verifies
  ambiguous failures before retrying;
- recheck/resume actions are retried only when the torrent client contract makes
  repeat execution safe;
- link preparation uses deterministic destination roots and cleanup checkpoints;
- torrent cache writes use atomic files and deterministic cache keys;
- notification activities either tolerate duplicates or use deterministic
  delivery keys;
- cleanup treats already-missing files as successful cleanup.

Orchestrations must never perform direct database, filesystem, network, random,
clock, or environment reads. Those operations belong in activities.

## Operator Surfaces

The HTTP contract should remain stable:

- `POST /v1/announcements` returns `202 Accepted` after durable workflow start
  or dedupe;
- workflow endpoints still reject invalid/auth-failed input synchronously;
- `/v1/status`, `/readyz`, and `/metrics` continue to expose accepting-work,
  processing readiness, workflow backlog, attempts, waits, and terminal
  outcomes without exposing secrets.

The implementation may replace current status-query sources with projection
tables derived from Duroxide workflow state. Projection rows should be bounded
and safe for status/metrics labels.

## Testing And Evaluation

The evaluation must prove:

- workflow start, timer, activity, external event, restart, and completion on
  SQLite;
- no-match inventory waits behave as wait-all barriers;
- side-effect activities are idempotent across crash/retry;
- workflow projections preserve operator-facing status and metrics semantics;
- shutdown and restart do not lose accepted work;
- SQLite locking remains acceptable under realistic workflow concurrency.

The testing environment decides whether Duroxide is promoted to production.
If Duroxide fails the evaluation, Sporos should keep the existing workflow
implementation and record a follow-up ADR.

## Consequences

Positive consequences:

- less custom queue/scheduler/retry state-machine code;
- clearer workflow histories and waits;
- fewer artificial reclaims of waiting work;
- durable timers and external events replace ad hoc polling loops.

Costs and risks:

- Duroxide is a young dependency and must be validated under Sporos load;
- deterministic orchestration constraints require code discipline;
- workflow history growth must be managed with continue-as-new or bounded child
  workflows;
- projection code is still required for Sporos status and metrics;
- two SQLite databases must be backed up together.
