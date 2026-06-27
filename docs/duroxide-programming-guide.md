# Duroxide Programming Guide

This is the Sporos guide for writing and reviewing Duroxide workflow code. It
does not vendor the upstream guide. Use this file for the local contract and use
the upstream Duroxide guide for full API details:
<https://github.com/microsoft/duroxide/blob/main/docs/ORCHESTRATION-GUIDE.md>.

The design and decision records remain part of the contract:

- [Duroxide workflow engine design](design/duroxide-workflow-engine.md)
- [ADR 0004: Duroxide workflow engine replacement](adr/0004-duroxide-workflow-engine.md)
- [ADR 0005: Duroxide testing evaluation](adr/0005-duroxide-testing-evaluation.md)

## Core Rule

Orchestrations coordinate. Activities do work.

An orchestration may branch, loop, schedule activities, wait for events, wait on
durable timers, start child workflows, update custom status, and choose retry or
compensation paths. It must not read clocks, random values, files, databases,
environment variables, network services, or process-global mutable state except
through Duroxide replay-safe APIs.

An activity may perform IO and side effects, but it must have a focused purpose,
an explicit duplicate-safety story, bounded retry behavior, and cancellation
handling for long-running work.

## Orchestrations

Use only Duroxide primitives for durable coordination:

- `ctx.schedule_activity_typed` for side-effecting work.
- `ctx.schedule_timer` for delays and backoff. Do not use `tokio::time::sleep`
  in orchestration code.
- `ctx.select2` or `ctx.select3` for timeout and race patterns.
- `ctx.join`, `ctx.join2`, or child workflows for deterministic fan-out/fan-in.
- `ctx.dequeue_event_typed` for durable FIFO event queues.
- `ctx.schedule_wait_typed` only for one-shot positional events where that
  semantic is intentional.
- `ctx.utc_now()` for replay-safe time. Do not use `SystemTime::now()`.
- `ctx.new_guid()` for replay-safe identifiers. Do not use random UUIDs.
- `ctx.continue_as_new_typed` for long-running loops and actors.

Keep orchestration inputs and outputs serializable and versioned. Any change to
workflow name, activity name, activity order, tags, input shape, or branching
logic can affect replay. Long-lived workflow changes need a versioning plan, and
old registrations must remain available while old instances can replay.

## Activities

Activities should be small enough that their replay and retry boundaries are
obvious. Avoid processor-style activities that perform several unrelated
mutations before returning.

For every activity that mutates local state, the filesystem, a torrent client, or
an external service:

- define the stable idempotency key or verification read;
- make ambiguous post-side-effect failures safe before retrying;
- use atomic filesystem writes where possible;
- check existing torrent client state before repeating mutations;
- redact secrets before serializing, logging, or projecting state;
- return structured outputs that let the orchestration decide the next step.

Long-running activities must observe both application shutdown and Duroxide
activity cancellation. Pass `ActivityContext` into the activity implementation
and check `ctx.is_cancelled()` or use `ctx.cancelled()` with `tokio::select!`
around waits, polling, network calls, filesystem scans, and client operations.

## History And Payload Size

Treat Duroxide history as production state. It must stay bounded and useful.

- Use `continue_as_new_typed` for supervisor loops, actors, interval workflows,
  and repeated polling cycles.
- Prefer stable persisted references over large activity outputs when a workflow
  may handle many torrents, files, candidates, or saved retry items.
- Use paged database reads, bounded child workflows, or bounded fan-out batches
  instead of serializing large vectors into orchestration history.
- Add tests or measurements for paths that can scale with inventory size, search
  result count, saved retry count, or long-running supervisor cycles.

## Events, Timers, And Waits

Use durable timers for retry and dependency backoff. Preserve protocol semantics
such as `Retry-After` in activities, then return the chosen delay to the
orchestration.

Use durable event queues when an event may arrive after the workflow starts but
before the workflow reaches the wait. Queue messages before workflow start may be
dropped by providers, so start the workflow first and then enqueue.

When a workflow waits on a specialized worker, external event, inventory
freshness barrier, or dependency recovery, add a timer-backed recheck path unless
there is a documented reason it can wait forever.

## Status, Projections, And Logs

Custom status and projection rows are operator surfaces. Keep them bounded,
redacted, and stable.

- Include workflow kind, public id, state, reason, safe next action, and safe
  dependency labels.
- Never include raw URLs, passkeys, cookies, tokens, API keys, or request bodies
  that may contain secrets.
- Keep metric labels low-cardinality.
- Use `ctx.trace_*` inside orchestration code. Use `tracing` in activities.
- Projection writes belong in activities or other side-effecting boundaries, not
  directly in orchestrations.

## Testing Checklist

Any Duroxide workflow change should include focused coverage for the affected
durable behavior:

- activity scheduled and completed;
- timer or event wait behavior;
- restart while waiting or after an activity completed;
- no duplicate external mutation after replay or retry;
- cancellation or shutdown for long-running activities;
- `continue_as_new` rollover for long-running loops;
- projection/status updates and secret redaction;
- large or bounded batch behavior where applicable.

Run the repository cargo gates before merging Duroxide workflow changes:

1. `cargo fmt --check`
2. `cargo build`
3. `cargo check`
4. `cargo clippy --all-targets -- -D warnings`
5. `cargo test`

## Merge Readiness

Before promoting Duroxide workflow changes to mainline operation, resolve the
open merge-readiness beads under `sporos-fmlr`. In particular, the monolithic
workflow implementation must be split into a dedicated module tree, supervisor
history must be bounded, long-running activities must use `ActivityContext`
cancellation, and cluster evidence must cover SQLite contention and projection
lag.
