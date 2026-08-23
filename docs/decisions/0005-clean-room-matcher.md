# Pure clean-room matcher

Status: Accepted for Phase 3.

Implementation review: Passed (Codex, 2026-08-24).

The matcher was implemented from the matching contract in `_private/design.md`
and the independently authored synthetic corpus under
`crates/sporos-matcher/tests/fixtures`. No cross-seed or qui implementation
files, tests, fixture data, or generated artifacts were inspected or copied.

The public input and output contract lives in `sporos-model`. The implementation
normalizes release descriptors, rejects unsafe manifests, checks existing and
media identities, and solves file mapping as a deterministic maximum-weight
bipartite assignment. Removing each selected edge and resolving the assignment
detects equal-best alternatives; the matcher rejects those rather than relying
on iteration order. Matching modes and reason codes are typed, serializable
values.

The normal dependency graph is intentionally limited to `sporos-model` and
`unicode-normalization` plus that crate's small Unicode tables:

```console
cargo tree -p sporos-matcher --edges normal
```

In particular, the crate has no service, workflow, database, HTTP, qBittorrent,
Arr, Prowlarr, or filesystem dependency. Serde supports the model's wire-stable
values; Serde JSON and proptest are development-only dependencies used to
execute the corpus and invariant tests.

The Phase 3 review checked:

- the approved synthetic corpus, including v1, v2, hybrid, path-safety, disc,
  Unicode, partial, and multi-source cases, passes;
- equal-best mappings reject with `ambiguous_file_mapping`;
- source and manifest file permutations produce identical decisions;
- every successful mapping is one-to-one and exact-size;
- the expected 20-file, eight-source benchmark completes without unbounded
  search or service access;
- all public production dependencies are Apache-2.0 compatible and no GPL or
  source-available implementation material is present.
