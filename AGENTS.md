# Project Instructions for AI Agents

Instructions for AI coding agents working in this repository. Never add more
than is necessary.

<!-- BEGIN BEADS INTEGRATION -->
## Issue Tracking with bd (beads)

**IMPORTANT**: This project uses **bd (beads)** for ALL issue tracking. Do NOT use markdown TODOs, task lists, or other tracking methods.

### Why bd

- Dependency-aware: track blockers and relationships between issues
- Agent-optimized: JSON output, ready work detection, discovered-from links
- Prevents duplicate tracking systems and confusion

### Quick Start

Common operations:

```bash
bd ready --json
bd create "Issue title" --description "Detailed context" -t bug|feature|task|epic|chore -p 0-4 --json
bd create "Found bug" --description "Details" -t bug -p 1 --deps discovered-from:<current-id> --json
bd update <id> --claim --json
bd update bd-42 --priority 1 --json
bd update bd-42 --status blocked --json
bd close bd-42 --reason "Completed" --json
bd show bd-42 --json
bd list --status open --json
bd list --parent <epic-id> --status open --json
bd children <epic-id> --json
bd graph --compact <id>
bd graph check --json
bd dep add <blocked-id> <blocker-id> --type blocks --json
bd dep add <issue-id> <parent-id> --type parent-child --json
```

`bd ready` is dependency-aware. `bd list --ready` only filters by status and is
not equivalent.

Dependency order matters. For `bd dep add A B`, `A` depends on `B`; `B` blocks
`A`. The `--blocked-by` and `--depends-on` flags mean the same thing.

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

1. **Check ready work**: `bd ready --json` shows unblocked work.
2. **Claim your task**: `bd update <id> --claim --json`.
3. **Work on it**: implement, test, document.
4. **Discover new work**: create a linked issue with
   `--deps discovered-from:<current-id>`.
5. **Complete**: run required quality gates, close the bead, then commit only
   the changed files for that ticket using the Git rules below.

### Important Rules

- Use bd for ALL task tracking.
- Always use `--json` for programmatic reads and writes.
- Link discovered work with `discovered-from` dependencies.
- Check `bd ready --json` before asking "what should I work on?"
- Do NOT create markdown TODO lists.
- Do NOT use external issue trackers.
- Do NOT duplicate tracking systems.
- Do NOT run `bd dolt push`, `bd dolt pull`, or other Dolt sync commands as
  routine agent cleanup. Normal bd commands are enough for local issue updates.

### Known Gotchas

- `bd dep add <blocked> <blocker>` is easy to reverse. Read it as
  "`blocked` waits for `blocker`."
- `bd link A B` has the same direction: `B` blocks `A`.
- `bd ready` excludes blocked/deferred/in-progress/hooked work. Prefer it over
  `bd list --ready` when choosing work.
- `bd create --id ... --parent ...` is not valid. Either let bd assign the ID
  when using `--parent`, or use an explicit child ID such as `<epic-id>.1`.
- Explicit child IDs such as `<epic-id>.1` are already treated as children of
  `<epic-id>` in this repository. Do not add a second `parent-child`
  dependency for the same pair; bd may reject it as a hierarchy deadlock.
- Use `bd graph check --json` or `bd dep cycles --json` after bulk dependency
  edits.
- Epics can appear in `bd ready`. When you want executable work, filter with
  `bd ready --type task --json`.

### Session Completion

Before ending a work session:

1. File bd issues for remaining follow-up work.
2. Run quality gates if code changed.
3. Close completed beads and update in-progress beads.
4. Stage only the files changed for the completed ticket and commit them using
   the commit format in the Git section.
5. Check `git status --short` and report any remaining uncommitted changes.
6. Do not push source or Dolt remotes unless the user explicitly asked for it.

<!-- END BEADS INTEGRATION -->


## Build & Test

Pre-commit quality gates must run in this exact order and all must pass:

1. `cargo fmt --check`
2. `cargo build`
3. `cargo check`
4. `cargo test`

CI enforces the same cargo gate order.

If clippy is run by CI, requested by the ticket, or useful for the change, it
must pass with no warnings.

## Architecture Overview

Sporos is in a phase-one Rust rewrite. During this phase, use
`docs/internal/10-sporos-rust-rewrite.md` as the controlling architecture guide.
The other `docs/internal` files are source-analysis research only; use them for
behavioral context and edge cases where they do not conflict with the Sporos
rewrite guide.

Treat `docs/internal` as read-only rewrite scaffolding. Do not edit, regenerate,
stage, or commit files under `docs/internal`; if durable project context is
needed, move it into Rust code, tests, ADRs, operator docs, beads issues, or
this file instead. After the rewrite phase, treat the current Rust code, tests,
ADRs, and beads issues as the active project contract, and do not rely on
removed internal research docs. The Rust-native TOML configuration and initial
Rust schema are the supported contract; do not add alternate configuration
loaders or schema compatibility layers unless a ticket explicitly changes that
contract.

SQLite schema changes are folded into the inline initial schema until the first
Rust release. Do not add migration files for unreleased schema changes. After
the first release, schema changes must use append-only numbered migrations and
compatibility tests from released schema fixtures.

Do not collapse every `info_hash` column into one canonical identity table.
Decision rows may describe external candidates that were never cached locally,
while torrent, client, and ensemble rows describe local state. Add foreign keys
only where the ownership boundary is explicit.

Keep the configured `client_host` value as the persisted client identity unless
a real client metadata table reduces repeated state or is needed for multi-client
ownership. Existing client-owned tables should key by `client_host` directly.

## Conventions & Patterns

### Rust design
Preserve user-visible behavior before improving internals. When behavior is
unclear, add a focused test around the current Rust behavior or an ADR-backed
change before modifying it.

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

Split large Rust files into submodules when the file mixes distinct
responsibilities or has become hard to review. Prefer ownership-oriented module
trees, such as separate torrent-client model, capability, error, registry, and
per-client adapter modules, over dumping unrelated helpers into one file. Do
not split files solely to satisfy a line count.

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
Only commit touched files, and commit logical changes rather than the whole
workspace. Commit titles must be lower case, no longer than 50 characters, and
formatted as `area: title`. The title after the area must complete the sentence
"when this change is merged it will ..."; for example,
`persistence: fix unique key violation`.

Commit bodies must use concise prose and give reviewers enough context. Avoid
lists unless the change genuinely needs them. When committing non-interactively,
pass the full commit message directly on stdin with `git commit -F-`. The
message must contain the title on the first line, a blank second line, and body
lines wrapped at 72 characters or less. Do not include literal `\n` sequences in
commit messages.

### Release tags
When creating a tag, include a very brief changelog in the tag annotation using
one concise line per notable change item. Keep the annotation focused on the
externally meaningful behavior change, not implementation details. Do not list
docs, refactors, config renames, or internal plumbing unless they are
themselves the notable user-facing change.
