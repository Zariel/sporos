# ADR 0005: Duroxide testing evaluation

## Status

Accepted for the testing branch. Production promotion remains gated.

## Date

2026-06-27

## Context

ADR 0004 accepted Duroxide for a testing-environment implementation as the
durable workflow engine behind announce, search, scheduled job, inventory, and
saved torrent retry work.

The implementation follows the upstream Duroxide orchestration guide:
<https://github.com/microsoft/duroxide/blob/main/docs/ORCHESTRATION-GUIDE.md>.
That guide keeps orchestration functions deterministic and pushes IO and other
side effects into activities. It also defines the replay, external-event,
durable-timer, custom-status, sub-orchestration, and continue-as-new primitives
used by this branch.

## Decision

Promote Duroxide as the workflow engine for the testing branch and for the next
testing-environment deployment.

Do not declare the Duroxide path production-ready yet. Keep production promotion
gated on a cluster soak that measures:

- SQLite workflow-store lock behavior during burst announces, searches, and
  scheduled jobs;
- workflow history growth for long-running supervisors;
- projection lag under sustained activity.

The current evidence is strong enough to continue with Duroxide in the testing
path instead of returning to the lease-polling workflow engine. It is not strong
enough to remove those production gates.

## Evaluation Evidence

The test inventory for this branch lists 894 library tests, one Duroxide runtime
integration test, two system harness tests, and one system support test.

Representative Duroxide evidence:

- `tests/duroxide_runtime.rs` proves SQLite-backed runtime startup, activity
  execution, a durable timer, an external wait, restart while waiting, and
  completion without repeating the completed activity.
- `runtime::duroxide_workflow::tests::runtime_starts_seeds_supervisors_idempotently_and_shuts_down`
  proves runtime startup/shutdown and supervisor seeding.
- `runtime::duroxide_workflow::tests::announce_orchestration_waits_for_media_and_client_inventory_events`
  and `announce_orchestration_preserves_partial_inventory_wait_after_recheck`
  prove announce no-match inventory waits behave as wait-all barriers.
- `runtime::duroxide_workflow::tests::announce_orchestration_rechecks_when_inventory_completion_event_is_missed`
  covers missed inventory completion events with timer-backed recheck.
- `runtime::duroxide_workflow::tests::announce_orchestration_resumes_dependency_wait_after_file_backed_restart`
  covers restart while an announce waits on candidate cache recovery.
- `runtime::daemon::tests::announce_workflow_waits_for_scheduled_media_inventory_before_terminal_no_match`
  covers the real scheduled media inventory path before terminal no-match.
- `runtime::daemon::tests::search_workflow_resumes_blocked_candidate_after_file_backed_restart`
  covers restart while a search candidate download is in flight.
- `runtime::duroxide_workflow::tests::saved_retry_supervisor_runs_startup_and_interval_with_bounded_children`
  covers saved retry startup, interval execution, child workflows, and bounded
  child concurrency.
- `http::tests::announcement_acceptance_rejects_inserted_work_when_workflow_start_fails`
  covers API cleanup when workflow start fails after announce persistence.
- `http::tests::status_readyz_and_metrics_expose_workflow_projection_snapshots`
  covers projection-backed `/v1/status`, `/readyz`, `/metrics`, dependency
  blockers, bounded labels, and secret redaction.
- `runtime::workflow_contracts::tests::*` covers deterministic workflow naming,
  versioned inputs, side-effect activity contracts, and mutation verification
  requirements.

Large-inventory and burst-adjacent coverage exists at the workflow boundaries:

- qBittorrent and rTorrent client tests cover large inventory paging/chunking;
- client inventory refresh and saved torrent retry tests cover bounded
  concurrency;
- search workflow tests cover bounded candidate prefetch and continuation after
  candidate failure.

## Measured Risks

### SQLite Locking

The full test suite exercises multiple Duroxide runtimes and Sporos domain
SQLite repositories concurrently. The current gate run completed successfully,
but emitted retryable SQLite busy/deadlock warnings from Duroxide dispatchers
under test concurrency.

That result is acceptable for this testing-branch decision because the workflows
completed and the domain database is separate from `/app/state/sporos-workflows.db`.
It is not enough for production promotion because the suite is not a sustained
cluster load test.

### Workflow History Growth

Inventory refresh workflows use `continue_as_new_typed` after each refresh.
Saved retry item work runs in bounded child workflows.

Scheduled job supervisors and the announce coordination loop remain long-running
or retry-loop orchestration paths. They are acceptable for testing because the
current tests exercise finite waits, retries, restarts, and terminal outcomes.
They need longer-running history measurement before production promotion.

### Projection Lag

Projection writes are activity-side domain database writes and are exposed
through bounded status and metrics queries. Tests cover secret redaction, stale
projection update rejection, dependency blocker reporting, active counts, and
metrics labels.

The remaining production risk is lag under burst load, not projection semantics.

## Consequences

- The testing branch should keep Duroxide as the workflow engine and continue
  operator testing with the Duroxide workflow database under `/app/state`.
- The production decision is still conditional on cluster evidence for the three
  measured risks above.
- Any production promotion must include the exact cargo gates from `AGENTS.md`
  and a cluster soak summary.
