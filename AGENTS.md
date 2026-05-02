# Project Instructions for AI Agents

This file provides instructions and context for AI coding agents working on this project.

Never add more than is necessary.

<!-- BEGIN BEADS INTEGRATION -->
## Issue Tracking with bd (beads)

**IMPORTANT**: This project uses **bd (beads)** for ALL issue tracking. Do NOT use markdown TODOs, task lists, or other tracking methods.

### Why bd?

- Dependency-aware: Track blockers and relationships between issues
- Git-friendly: Dolt-powered version control with native sync
- Agent-optimized: JSON output, ready work detection, discovered-from links
- Prevents duplicate tracking systems and confusion

### Quick Start

**Check for ready work:**

```bash
bd ready --json
```

**Create new issues:**

```bash
bd create "Issue title" --description="Detailed context" -t bug|feature|task -p 0-4 --json
bd create "Issue title" --description="What this issue is about" -p 1 --deps discovered-from:bd-123 --json
```

**Claim and update:**

```bash
bd update <id> --claim --json
bd update bd-42 --priority 1 --json
```

**Complete work:**

```bash
bd close bd-42 --reason "Completed" --json
```

### Issue Types

- `bug` - Something broken
- `feature` - New functionality
- `task` - Work item (tests, docs, refactoring)
- `epic` - Large feature with subtasks
- `chore` - Maintenance (dependencies, tooling)

### Priorities

- `0` - Critical (security, data loss, broken builds)
- `1` - High (major features, important bugs)
- `2` - Medium (default, nice-to-have)
- `3` - Low (polish, optimization)
- `4` - Backlog (future ideas)

### Workflow for AI Agents

1. **Check ready work**: `bd ready` shows unblocked issues
2. **Claim your task atomically**: `bd update <id> --claim`
3. **Work on it**: Implement, test, document
4. **Discover new work?** Create linked issue:
   - `bd create "Found bug" --description="Details about what was found" -p 1 --deps discovered-from:<parent-id>`
5. **Complete**: `bd close <id> --reason "Done"`

### Auto-Sync

bd automatically syncs via Dolt:

- Each write auto-commits to Dolt history

### Important Rules

- ✅ Use bd for ALL task tracking
- ✅ Always use `--json` flag for programmatic use
- ✅ Link discovered work with `discovered-from` dependencies
- ✅ Check `bd ready` before asking "what should I work on?"
- ❌ Do NOT create markdown TODO lists
- ❌ Do NOT use external issue trackers
- ❌ Do NOT duplicate tracking systems

For more details, see README.md and docs/QUICKSTART.md.

## Landing the Plane (Session Completion)

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds

<!-- END BEADS INTEGRATION -->


## Build & Test

_Add your build and test commands here_

Pre-commit quality gates must run in this exact order and all must pass:

1. `cargo fmt --check`
2. `cargo build`
3. `cargo check`
4. `cargo test`

CI enforces the same cargo gate order.

All tests and clippy must pass with no warnings before the task is complete.

```bash
# Example:
# npm install
# npm test
```

## Architecture Overview

sporos is a Rust rebuild of the cross-seed behavior documented in
`docs/internal`. Treat those documents as the compatibility contract for CLI
flags, config semantics, HTTP API behavior, SQLite state, torrent cache and
output filenames, matching decisions, and torrent-client side effects.

## Conventions & Patterns

### Rust design
Preserve user-visible behavior before improving internals. When behavior is
unclear, add a compatibility test from `docs/internal` before changing it.

Memory efficiency is a primary design goal. The baseline scale is 10,000
torrents in a client, and the design should expect to handle 100,000. Any path
that lists, indexes, filters, matches, caches, injects, or cleans up torrents
must be written as large-inventory production code. Prefer streaming filesystem
walks, bounded queues, iterators, borrowed data, and paged database reads over
loading whole torrent inventories, RSS feeds, or directory trees into memory.
Avoid clone-heavy APIs, unbounded process-global caches, and retaining parsed
torrent metafiles longer than needed.

Keep module boundaries aligned with the documented runtime layers: CLI/config,
domain models, persistence, torrent parsing, search and matching, external
integrations, torrent-client adapters, actions, HTTP API, scheduler, and
operations. Add abstractions only when they reduce real duplication or protect a
compatibility boundary.

This is intended to be a long-running service. Production code must surface
errors up the stack so callers can decide whether to retry, degrade, skip one
item, return an API error, or shut down. Exiting the process is a serious error
condition and should be limited to startup/configuration failures or unrecoverable
runtime corruption. Logs must be sufficient to debug production issues and must
use appropriate levels: trace/debug for high-volume diagnostics, info for
lifecycle and user-visible progress, warn for recoverable anomalies, and error
for failed operations requiring attention.

External requests and fallible IO must handle transient failures gracefully with
bounded jittered exponential backoff where retrying is safe. Preserve explicit
protocol semantics such as `Retry-After`, avoid retrying non-idempotent actions
unless the operation is designed for it, and log retry exhaustion with enough
context to diagnose the dependency, operation, and final error.

Every feature that touches matching, injection, persistence, or public API
behavior needs focused tests. Memory-sensitive paths should include fixtures or
benchmarks that make peak allocation or resident memory regressions visible.

### Rust implementation practices
Use Rust 2024 for new crates unless a ticket documents a concrete compatibility
reason not to. Set `rust-version` explicitly in `Cargo.toml`, keep it aligned
with the pinned toolchain, and use the workspace resolver explicitly for virtual
workspaces. Prefer stable Rust; nightly features require an issue with the
reason, risk, and planned removal path.

Let `rustfmt` own formatting. Keep formatting config minimal and avoid local
style debates. Run Clippy with project warnings treated as errors; opt into
stricter Clippy groups one lint at a time, and never enable `restriction` as a
blanket group.

Use strong domain types for config, torrent metadata, decisions, client state,
and persisted rows. Keep external DTOs separate from internal models when doing
so prevents invalid states or accidental contract drift.

Keep names concise. Function, helper, variable, test, fixture, and module names
should identify the concept or behavior without restating what is already obvious
from the type signature, assertions, or surrounding module context.

Public and cross-module types should implement the common traits that make them
easy to inspect and test, such as `Debug`, `Clone`, `Eq`, `PartialEq`, `Hash`,
`Display`, and `Default` when those traits are semantically correct. Validate
inputs at system boundaries and prefer types that make invalid states
unrepresentable.

Production code should return `Result` with typed errors from library/domain
layers and add context at application boundaries. Do not use `unwrap`, `expect`,
or panics for recoverable runtime failures. If an invariant is truly impossible,
make that invariant explicit in the type or include a precise failure message.

Prefer borrowing in hot paths and clone only when ownership is actually needed.
Avoid cloning large `Vec`, `String`, file lists, torrent metadata, or response
bodies inside loops. Use compact structs, preallocated collections, and
streaming iterators when processing torrent inventories or filesystem trees.

Use bounded concurrency. Any async fan-out over torrents, indexers, files, or
clients must have an explicit limit, cancellation behavior, timeout behavior,
and backpressure. Do not block Tokio worker threads with expensive CPU work,
synchronous database calls, or large filesystem operations; isolate blocking
work with `spawn_blocking`, a dedicated worker, or a clearly bounded sync path.
Do not hold mutex guards, database statements, or other scarce resources across
`.await` unless the type is specifically designed for that and the scope is
bounded.

Use `Path` and `PathBuf` for filesystem paths and convert to strings only at
display, config, or API boundaries. Be careful with non-UTF-8 paths and with
client paths that may use different platform conventions than the host.

Keep dependencies deliberate. Prefer the standard library and already-selected
project crates before adding a new dependency. When adding a crate, disable
unneeded default features where practical and document why it belongs in the
ticket or PR. Keep feature flags additive and avoid exposing optional
dependencies through public APIs unless the compatibility cost is intentional.

Use `tracing` spans for operations that cross IO, database, scheduler, or client
adapter boundaries. Include stable identifiers such as info hash, indexer name,
client host, job name, and request label when safe, but never log secrets.

Tests should cover both success and failure behavior. Add regression tests for
compatibility bugs, property tests for parsers or filename round trips where
useful, and integration tests with fake services for network/client contracts.
Destructors must not perform fallible or blocking production cleanup; provide an
explicit close, flush, or shutdown method that returns `Result` when teardown can
fail.

### Git
Only commit touched filed, when commiting commit logical changes not just
the whole workspace. Commit messages should include a title and the body
should describe the change so that a reviewer can get an understanding. The
title should be all lower case and no longer than 50 characters, the body must
use prose, avoid lists and overly explainging things, be concise. Commit body
lines must be wrapped at 110 characters or less. Do not include literal `\n`
sequences in commit messages; use separate `git commit -m` arguments or an
editor so the message contains real line breaks.

### Release tags
When creating a tag, include a very brief changelog in the tag annotation using
one concise line per notable change item. Keep the annotation focused on the
externally meaningful behavior change, not implementation details. Do not list
docs, refactors, config renames, or internal plumbing unless they are
themselves the notable user-facing change.
