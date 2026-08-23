# Sporos patches

This directory is based on `duroxide` 0.1.30, published from upstream commit
`cfe0b8c957ef7ede43c6026ebba0052211de1a49`. The crates.io archive has SHA-256
`92b17ebe10f702644ad65a9c624b4060023955b4cd145c23ed4c6942b17fdac9`.

Sporos carries four SQLite-provider changes:

- expose a `NORMAL`/`FULL` synchronous-mode option, retaining `NORMAL` as the
  upstream-compatible default; and
- propagate file-backed schema migration failures instead of falling back to
  direct schema creation; and
- disable SQLx's default database drivers and use its Tokio runtime without a
  TLS backend because this provider only opens SQLite databases; and
- atomically reserve root orchestration identities with their queue entry so
  duplicate instance starts return a permanent constraint error.

These changes let Sporos require and inspect `synchronous=FULL`, and ensure an
unknown or incompatible migration cannot be hidden during startup. Keep the
patch minimal and review it separately whenever Duroxide is upgraded.
