# Rust Binary Release Checklist

Use this checklist for the first Rust binary release and for every later tagged
release. Release readiness means users can keep their existing config, state,
cache, output directory, automation, and torrent-client side effects.

## Build Environment

Install Rust through rustup and let `rust-toolchain.toml` select the stable
toolchain and required components:

```text
rustup toolchain install stable
rustup component add rustfmt clippy
rustup show
```

The package declares `rust-version = "1.85"` in `Cargo.toml`; the active stable
toolchain must be at least that version. Build release artifacts from a clean
checkout:

```text
cargo fmt --check
cargo build
cargo check
cargo test
cargo clippy --all-targets --all-features -- -D warnings
cargo build --release
```

Package `target/release/sporos` as the `cross-seed` executable or install it
behind a `cross-seed` wrapper/symlink. Confirm the packaged command reports the
expected version and command surface before publishing.

## Compatibility Upgrade Notes

Before replacing a Node deployment, back up:

- `config.toml`;
- `cross-seed.db`, `cross-seed.db-wal`, and `cross-seed.db-shm`;
- `torrent_cache/`;
- configured `output_dir`;
- configured `inject_dir` or any saved retry `.torrent` files.

The first tagged Rust release is the SQLite migration cutoff. Before that tag,
the Rust schema is still unreleased and direct bootstrap compatibility tests are
the source of truth. After that tag, every schema change must include migration
coverage from each released schema fixture to the current schema before another
release is tagged.

The upgrade must preserve:

- config option names, defaults, and validation behavior;
- persisted API keys, indexer caps/health, RSS cursors, search timestamps,
  decisions, client searchee cache, data-dir roots, and ensemble rows;
- cached torrents named `<infoHash>.cached.torrent`;
- saved and restored output filenames;
- retryable saved torrents in `inject_dir` or `output_dir`.

## Operational Smoke Tests

Run smoke tests against a copied app directory first. Use real indexer and
torrent-client credentials only in an isolated test environment.

```text
cross-seed --help
cross-seed api-key --api-key <test-key>
cross-seed tree <known-good.torrent>
cross-seed diff <local-source.torrent> <candidate.torrent>
cross-seed restore --output-dir <temporary-output-dir>
cross-seed inject --inject-dir <copied-inject-dir> --ignore-titles
cross-seed daemon --no-port --verbose
```

For a production-like smoke test, run one of each workflow with temporary output
and link directories:

- search with `action: "save"`;
- search with `action: "inject"` and a writable test client;
- RSS with at least one enabled Torznab indexer;
- announce or webhook against the daemon API;
- cleanup after a client refresh.

Inspect logs for config validation, redacted secrets, rate-limit handling,
client mutation results, and cleanup counts. Verify no smoke test deletes
unexpected saved retry files or cached torrents.

## Known Non-Goals

Do not treat a release as an opportunity to broaden compatibility:

- no less-conservative matching or title-only matching expansion;
- no removal of saved-torrent retry behavior;
- no web framework requirement for the daemon API;
- no database engine change without an explicit migration or import path;
- no network requirement for local commands such as `diff` and `tree`;
- no user-visible default change without a release note and compatibility test.

## Rollback

If the Rust binary fails during an upgrade:

1. Stop the daemon or scheduled runner.
2. Preserve the failed run's logs, generated output, and saved retry torrents.
3. Restore the previous binary or Node deployment.
4. Restore the backed-up SQLite files if the Rust binary opened the database.
5. Restore `torrent_cache/`, `output_dir`, and `inject_dir` only when the failed
   run modified or deleted files unexpectedly.
6. Re-run `api-key`, `restore`, and a read-only `daemon --no-port` smoke test
   before returning to normal scheduled jobs.

Do not delete `torrent_cache/` as a rollback shortcut. It is part of the user's
decision cache and restore path.
