# Sporos

Sporos is a torrent automation service for announce/event-driven matching and
injection. External automation submits candidate events to Sporos, and the
daemon handles matching against local media, downloading candidate torrent
metadata, saving retryable work, and injecting matched torrents into a supported
torrent client.

## Features

- Durable `POST /v1/announcements` intake with deduplication, retry timing,
  TTL, retention, and status/metrics visibility.
- Matching against configured media directories and torrent-client inventory.
- Torznab indexer search with optional Prowlarr indexer import.
- qBittorrent and rTorrent client adapters.
- Scheduler-backed maintenance for cleanup and indexer capability refresh.
- Prometheus metrics plus liveness, readiness, and typed status endpoints.

## Run

Build the binary:

```bash
cargo build --release
```

Create a TOML config, validate it, then start the daemon:

```bash
sporos check-config --config /etc/sporos/config.toml
sporos serve --config /etc/sporos/config.toml
```

The default config path is `./config.toml`. Use
`sporos print-config-schema` to print the supported config surface.

## Configuration

Configuration is Rust-native TOML plus optional `SPOROS__` environment
overrides. At minimum, configure state paths, server bind/auth, at least one
torrent client, and the indexer or Prowlarr sources needed by your automation.
Production secrets should come from files or environment variables rather than
inline TOML values.

Start with [Configuration](docs/configuration.md). For day-two operations,
container notes, metrics, readiness, and queue details, see the
[Operator Guide](docs/operators/operator-guide.md) and
[Announce Queue Operations](docs/operators/announce-queue.md).

## HTTP Surface

Common endpoints are:

- `GET /livez`
- `GET /readyz`
- `GET /metrics`
- `GET /v1/status`
- `POST /v1/announcements`
- `POST /v1/searches`
- `POST /v1/jobs/{job_name}/runs`

Mutating workflow endpoints require `Authorization: Bearer <token>` when an API
token is configured. Externally reachable binds require an API token.
