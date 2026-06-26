# Duroxide Workflow Engine Technical Design

## Summary

This design scopes a full replacement of Sporos workflow execution with
Duroxide in a testing environment. It covers announce, manual search, scheduled
jobs, inventory refresh, saved torrent retry, and manual workflow trigger paths.

The implementation is intentionally not backwards compatible with active
workflow state from the current engine. Evaluation starts from fresh test state.
Existing Sporos domain tables remain authoritative for domain data; Duroxide
owns workflow execution history, timers, waits, and activity scheduling.

## Runtime Shape

Add a workflow runtime owned by `AppRuntime`:

```text
DuroxideWorkflowRuntime
  provider: SQLite provider at /app/state/sporos-workflows.db
  client: Duroxide client
  orchestrations: registered Sporos workflows
  activities: registered Sporos side-effect activities
  projections: workflow state -> Sporos status/metrics rows
```

Runtime startup:

1. Open Sporos domain repository.
2. Open Duroxide SQLite provider.
3. Register activities.
4. Register orchestrations.
5. Start Duroxide runtime.
6. Seed supervisor workflows for scheduled jobs and saved retry.
7. Start filesystem watcher and other non-workflow event producers.

Runtime shutdown:

1. Stop accepting new workflow requests.
2. Stop event producers.
3. Let Duroxide stop dispatching new work.
4. Allow in-flight activities to finish or checkpoint according to shutdown
   budget.
5. Flush projection updates.

## Workflow Boundaries

### AnnounceWorkflow

Input:

- validated announce DTO;
- dedupe identity;
- sanitized candidate metadata;
- secret fetch material when needed;
- received timestamp and TTL.

Instance ID:

```text
announce:{dedupe_hash}
```

Flow:

1. Persist or dedupe public announce projection.
2. Wait for required inventory freshness after `received_at`.
3. Reverse lookup against local/client inventory.
4. If candidate material is needed, download/cache torrent.
5. Assess match and policy.
6. Prepare link plan.
7. Inject, save, dry-run, already-exists, or terminal-fail.
8. Scrub secret fetch material from projections on terminal state.

No-match rule:

- if media inventory is configured, wait for media freshness after announce;
- if torrent clients are configured, wait for client freshness after announce;
- terminal no-match is allowed only after all required freshness barriers pass.

### SearchWorkflow

Input:

- validated manual search request;
- media type and query terms;
- optional limits and source filters.

Flow:

1. Build search plan from indexers and Arr/Prowlarr context.
2. Fan out indexer searches with configured concurrency limits.
3. Stream candidates through bounded candidate processing.
4. Download/cache torrents only when needed.
5. Assess and execute requested action.
6. Persist summary and emit notification activities.

### ScheduledJobWorkflow

Supervisor instance IDs:

```text
job:cleanup
job:media_inventory
job:client_inventory
job:indexer_caps
```

Flow:

1. Wait for interval timer or manual trigger event.
2. Start a child run instance.
3. Execute the relevant activity/workflow.
4. Record success/failure projection.
5. Schedule next interval or failure backoff timer.

Manual trigger behavior:

- if a run is active, coalesce the trigger;
- if a failure backoff is active, keep the backoff unless the trigger is an
  operator-forced run;
- preserve current job names and operator terminology.

### InventoryRefreshWorkflow

Instance IDs:

```text
inventory:media:full
inventory:media:changed:{scope_hash}
inventory:client
```

Flow:

1. Coalesce compatible refresh requests.
2. Run media/client inventory activity.
3. Persist inventory state in Sporos domain DB.
4. Materialize virtual seasons.
5. Raise inventory completion events for waiting workflows.
6. Update status/metrics projections.

Changed-path media refresh:

- native/poll watcher remains an event producer;
- watcher raises or starts changed-path inventory workflows;
- polling fallback behavior remains outside Duroxide and only submits workflow
  events.

### SavedTorrentRetryWorkflow

Supervisor instance ID:

```text
saved-retry
```

Flow:

1. Wait for interval timer or startup event.
2. Scan saved torrent retry files.
3. Fan out bounded retry item child workflows.
4. Revalidate file safety and candidate metadata.
5. Link/recheck/inject/save according to current policy.
6. Delete, keep, or checkpoint retry files.

## Activities

Activities are the only place where Sporos performs IO or side effects.

Core activity groups:

- `repository.*`: reads/writes Sporos domain DB and projections;
- `inventory.*`: media scan, changed-path scan, client inventory refresh;
- `matching.*`: reverse lookup, candidate assessment, content filtering;
- `candidate.*`: torrent download, cache read/write, metadata parse;
- `torrent_client.*`: has-torrent, inject, pause, recheck, resume, file list;
- `actions.*`: link preparation, save torrent, cleanup;
- `scheduler.*`: projection updates and trigger normalization;
- `notifications.*`: notification delivery;
- `cleanup.*`: retained rows, stale cache files, saved retry cleanup.

Activity rules:

- input and output must be serializable and versioned;
- secrets must use existing secret/redaction types before serialization;
- retries must use the shared transient IO retry policy where safe;
- mutation retries require idempotency or post-failure verification;
- long-running activities must report enough progress through logs/projections
  to diagnose stalls.

## Data And Projections

Workflow database:

```text
/app/state/sporos-workflows.db
```

Domain database:

```text
/app/state/sporos.db
```

Projection tables in the domain database should expose bounded operator state:

- workflow public ID;
- workflow kind;
- public status and reason;
- received/started/finished/updated timestamps;
- next wake deadline;
- safe dependency kind/name;
- redacted error/action summary;
- terminal outcome;
- raw secret material count, never secret values.

The current `announce_work` table can be removed or replaced by announce
projection tables during the Duroxide branch. No active-row migration is
required for the evaluation environment.

## API And Operator Contract

Preserve accepted-work semantics:

- validation/auth failures remain synchronous;
- durable workflow start failure returns an immediate API error;
- successful start or active duplicate returns `202 Accepted`;
- accepted response is not a final workflow result.

Status and metrics must continue to answer:

- can Sporos accept work;
- are workflow workers running;
- how many workflows are running/waiting/retrying/terminal;
- which safe dependencies block work;
- what is the oldest active work age;
- are secret-bearing active records accumulating.

Logs should name workflow kind, public workflow ID, activity name, safe
dependency, and next action. Logs must not include raw fetch URLs, cookies,
tokens, passkeys, or API keys.

## Testing Strategy

### Spike Tests

- Start a Duroxide runtime with SQLite provider.
- Register one activity and one orchestration.
- Start an instance, wait on a timer, raise an external event, and complete.
- Stop and restart the runtime between wait and completion.
- Verify history survives restart and activity is not repeated unexpectedly.

### Announce Tests

- Duplicate active announces return the same public work ID.
- No-match with stale media inventory waits for media freshness.
- No-match with stale media and client inventory waits for both freshness
  events.
- No-match after fresh required inventory becomes terminal failed.
- Candidate download wait resumes after cache completion event.
- Restart while waiting does not rerun completed reverse lookup activity.
- Injection ambiguity is resolved by existing-info-hash verification.
- Terminal transitions scrub raw fetch material from projections.

### Search Tests

- Manual search starts a workflow and preserves accepted response semantics.
- Indexer fan-out obeys configured concurrency.
- Candidate processing is bounded and continues after one candidate failure.
- Search summary and notification projection match current operator semantics.

### Scheduled Job Tests

- Cleanup, media inventory, client inventory, and indexer caps supervisor
  workflows seed on startup.
- Interval timers create run child workflows.
- Manual trigger coalesces with an active run.
- Failure schedules backoff and later retry.
- Shutdown/restart resumes the supervisor without duplicate runs.

### Inventory Tests

- Full media refresh persists local inventory and wakes waiting announces.
- Changed-path refresh scans only affected roots and wakes waiters.
- Client inventory refresh persists freshness and wakes waiting announces.
- Failed refresh records degraded projection and retry deadline.

### Saved Retry Tests

- Startup/interval retry scans saved files.
- Corrupt files are retained or rejected according to current policy.
- Successful injection deletes completed retry file.
- Restart during item processing does not duplicate client mutation.

### Operator Tests

- `/v1/status` reports workflow projections without raw secrets.
- `/readyz` distinguishes accepting work from processing readiness.
- Prometheus metrics preserve bounded label cardinality.
- Logs include workflow/action context for matching, waiting, and linking.

## Implementation Phases

1. **Dependency spike**
   - Add Duroxide behind a short-lived evaluation branch.
   - Prove SQLite provider, runtime startup, timers, events, and restart.

2. **Runtime shell**
   - Add `DuroxideWorkflowRuntime`.
   - Register empty supervisor workflows and no-op activities.
   - Wire startup/shutdown and health.

3. **Announce workflow**
   - Move announce orchestration first.
   - Preserve HTTP accepted response and dedupe behavior.
   - Implement inventory freshness barrier.

4. **Scheduled jobs and inventory**
   - Replace `PersistedScheduler` execution with Duroxide supervisors.
   - Convert inventory refresh queue to workflow starts/events.

5. **Search and saved retry**
   - Convert manual search queue to `SearchWorkflow`.
   - Convert saved retry loop to supervisor/child workflows.

6. **Projection and operator parity**
   - Replace status and metrics queries with projection-backed equivalents.
   - Remove obsolete queue/scheduler/announce worker code only after parity
     tests pass.

7. **Testing environment evaluation**
   - Run race-oriented announce scenarios.
   - Run restart and shutdown fault tests.
   - Run large-inventory and burst tests.
   - Record promote/reject decision in a follow-up ADR.

## Acceptance Criteria

- No workflow class depends on the old in-memory queue or lease-polling engine.
- Announce no-match waits on all required inventory freshness barriers.
- Every external mutation activity has an idempotency or verification contract.
- Restart during every major wait point resumes without lost work.
- Operator APIs and metrics remain useful and secret-safe.
- Full gates pass:
  - `cargo fmt --check`
  - `cargo build`
  - `cargo check`
  - `cargo clippy --all-targets -- -D warnings`
  - `cargo test`

## Open Risks

- Duroxide is young and must prove SQLite locking behavior under Sporos load.
- Workflow history growth may require continue-as-new for supervisors and large
  fan-out workflows.
- Duroxide activity retry semantics must be reconciled with Sporos
  side-effect-specific retry policy.
- Projection lag must not make `/readyz` or `/v1/status` misleading.
