# Sporos patches

This directory is based on `duroxide` 0.1.30, published from upstream commit
`cfe0b8c957ef7ede43c6026ebba0052211de1a49`. The crates.io archive has SHA-256
`92b17ebe10f702644ad65a9c624b4060023955b4cd145c23ed4c6942b17fdac9`.

Sporos carries seven focused changes:

- expose a `NORMAL`/`FULL` synchronous-mode option, retaining `NORMAL` as the
  upstream-compatible default; and
- expose the provider pool size, retaining five connections as the
  upstream-compatible default; and
- propagate file-backed schema migration failures instead of falling back to
  direct schema creation; and
- disable SQLx's default database drivers and use its Tokio runtime without a
  TLS backend because this provider only opens SQLite databases; and
- atomically reserve root orchestration identities with their queue entry so
  duplicate instance starts return a permanent constraint error; and
- expose a narrow root-start inspection operation that verifies queued or
  recorded start identity without exposing provider storage representation;
  and
- allow an activity retry policy to apply a deterministic error filter so
  callers can avoid retrying classified permanent application failures.

These changes let Sporos require and inspect `synchronous=FULL`, use one
connection per SQLite pool to avoid competing writers inside the single-active
service, ensure an unknown or incompatible migration cannot be hidden during
startup, reconcile transactional outbox delivery through the provider API, and
delegate classified retry timing to the durable runtime.
Keep the patch minimal and review it separately whenever Duroxide is upgraded.
