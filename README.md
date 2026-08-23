# Sporos

Sporos is a focused cross-seeding service for autobrr, qBittorrent, and
Prowlarr. It is currently in Phase 0: feasibility and correctness validation.
There is no runnable service yet.

The repository is a Rust workspace split by responsibility:

- `sporos-model` contains stable domain types and no I/O.
- `sporos-matcher` contains pure, deterministic matching.
- `sporos-service` owns external adapters and the `sporos` and `sporosctl`
  binaries.
- `sporos-testkit` contains development-only fakes and fault-injection support.

See [Development](docs/development.md) for setup and validation commands.

## Licence

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE).

