# Sporos

Sporos is a durable cross-seeding service for autobrr, qBittorrent 5.2+, and
Prowlarr. It inventories complete and downloading local data, accepts candidate
torrents idempotently, matches exact files across one or more sources, creates a
managed hardlink tree, and asks qBittorrent to verify before any torrent starts.

The service is deliberately conservative:

- SQLite and Duroxide make every acknowledged asynchronous request recoverable.
- qBittorrent, Prowlarr, Sonarr, and Radarr use bounded adapters.
- Candidate-native paths are materialised beneath one managed link root.
- Piece integrity is mandatory for partial matches.
- Sporos has no torrent, source-file, or link deletion operation.

The `sporos` binary runs the service. `sporosctl` provides status, inventory,
task, operation, search, and configured data-scan commands.

## Start here

- [Development setup](docs/development.md)
- [Configuration reference](docs/configuration.md)
- [Autobrr integration](docs/autobrr.md)
- [Matching and injection safety](docs/matching.md)
- [Operations, backup, and upgrades](docs/operations.md)
- [Container deployment](docs/container.md)
- [Security model](docs/security.md)

Nix is optional. `nix develop` supplies the complete tool set, while ordinary
Cargo commands work with rustup and a C compiler.

```console
cargo test --workspace --all-features
cargo run -p sporos-service --bin sporos
```

The OCI image runs as UID/GID `65532`, supports `linux/amd64` and `linux/arm64`,
and is intended for a read-only root filesystem with a writable `/data` volume.
No Kubernetes deployment manifests are shipped.

## Licence

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) and
[NOTICE](NOTICE).
