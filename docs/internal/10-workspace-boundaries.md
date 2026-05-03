# Workspace Boundaries

Workspace crates should enforce dependency direction, not only reduce file
size. Keep leaf behavior in the highest layer that owns the runtime contract,
and move only stable contracts or independently testable infrastructure into
lower crates.

Current crate boundaries:

- `sporos-core`: dependency-light domain models and error contracts.
- `sporos-retry`: bounded retry policy and HTTP retry classification.
- `sporos-config`: TOML and environment configuration normalization.
- `sporos-persistence`: SQLite schema, row DTOs, and query helpers.
- `sporos-runtime`: bounded queues and blocking task executors.

Do not move `clients`, `torrent`, or `search` into standalone crates until the
shared title/media parser is factored out. Those modules currently share
parsing semantics, and duplicating them would create compatibility drift.
