# Sporos Operator Guide

This guide covers the supported Rust-native operator surface: TOML
configuration, `SPOROS__` environment overrides, local state paths, HTTP probes,
metrics, and day-two operation. It does not require compatibility with earlier
source layouts or configuration formats.

## Commands

Run Sporos with an explicit config file:

```bash
sporos serve --config /etc/sporos/config.toml
```

Validate the same typed config path before deployment:

```bash
sporos check-config --config /etc/sporos/config.toml
```

Print the supported config surface:

```bash
sporos print-config-schema
```

Startup failures are written to stderr. Runtime logs are expected on stdout or
stderr; do not rely on file logging or hidden state outside the configured
paths.

## Configuration

Use TOML as the source of truth. Operator-supplied filesystem paths must be
absolute. Local defaults are resolved to absolute paths during startup.

```toml
[paths]
database = "/data/state/sporos.db"
torrent_cache_dir = "/data/cache/torrents"
output_dir = "/data/output"
media_dirs = ["/media/movies", "/media/tv"]

[server]
bind = "0.0.0.0:2468"
api_token_file = "/var/run/secrets/sporos-api-token"

[torrent_clients.qbit_main]
kind = "qbittorrent"
url = "http://qbittorrent:8080"
username = "sporos"
password_file = "/var/run/secrets/qbit-password"
default_save_path = "/downloads"

[torrent_clients.rtorrent_archive]
kind = "rtorrent"
url = "http://rtorrent:5000/RPC2"
default_save_path = "/downloads/archive"
label_field = "custom1"

[indexers.default_timeouts]
search = "120s"
download = "30s"

[indexers.torznab.main]
url = "https://indexer.example/api"
api_key_file = "/var/run/secrets/indexer-api-key"

[indexers.prowlarr.main]
url = "https://prowlarr.example"
api_key_file = "/var/run/secrets/prowlarr-api-key"
update_interval = "24h"
tags = ["movies", "hd"]
tag_match = "any"
include_untagged = true
refresh_on_startup = true
required = false
remove_policy = "deactivate"

[matching]
mode = "partial"
fuzzy_size_threshold = 0.02
include_single_episodes = false
include_non_video = false
season_from_episodes = 1.0
recent_search_cooldown_secs = 259200
first_search_window_secs = 604800

[inventory]
media_scan_max_depth = 3

[scheduling]
rss_interval = "30m"
client_inventory_interval = "24h"
indexer_caps_interval = "24h"
cleanup_interval = "24h"

[announce]
max_pending = 1000
worker_concurrency = 2
claim_batch_size = 10
lease_duration_secs = 300
lease_renewal_secs = 120
default_ttl_secs = 86400
retry_initial_delay_secs = 30
retry_max_delay_secs = 3600
retry_jitter_ratio = 0.2
success_retention_secs = 604800
failure_retention_secs = 1209600
```

Supported torrent clients are qBittorrent and rTorrent. Transmission and
Deluge are outside the initial Rust rewrite scope.

Supported indexers are Torznab-compatible endpoints. Put API keys in
`api_key_file`, `api_key_env`, or development-only `api_key`; do not put API
keys in the indexer URL query string.

## Prowlarr Discovery

Prowlarr discovery is optional. Configure one or more named
`[indexers.prowlarr.<name>]` sources when Sporos should import Torznab endpoints
from Prowlarr instead of listing every indexer under `indexers.torznab`.

```toml
[indexers.prowlarr.main]
url = "https://prowlarr.example"
api_key_file = "/var/run/secrets/prowlarr-api-key"
update_interval = "24h"
tags = ["movies", "hd"]
tag_match = "any"
include_untagged = true
refresh_on_startup = true
required = false
remove_policy = "deactivate"
```

Use `url` for the Prowlarr address; `base_url` is accepted as an alias. The
value should be the Prowlarr base URL, without `/api/v1` and without an API key
query parameter. Sporos contacts `/api/v1/indexer`, reads tag labels from
`/api/v1/tag` when tag names need resolving, and builds imported Torznab proxy
URLs through the configured Prowlarr source.

Prowlarr API keys support `api_key_file`, `api_key_env`, and local-development
`api_key`, with the same one-source-only rule as direct Torznab keys. For
Kubernetes, mount the key as a secret file and point `api_key_file` at the
mounted path. If the key is provided through the process environment instead,
set the TOML field to `api_key_env = "PROWLARR_API_KEY"` or use an environment
override such as:

```bash
SPOROS__INDEXERS__PROWLARR__MAIN__API_KEY_ENV='"PROWLARR_API_KEY"'
```

`update_interval` controls the periodic refresh cadence. `refresh_on_startup`
performs an immediate refresh during startup when true; when false, the first
daemon refresh waits for the configured interval plus deterministic jitter.
`required = true` makes startup fail if the startup refresh fails. The default
`required = false` records the source as degraded and lets Sporos continue so
operators can fix Prowlarr without restarting the service.

Only enabled Prowlarr indexers whose protocol is `torrent` and which support RSS
or search are imported. Tag filters apply before import. With `tags = []`, all
tagged indexers are imported and `include_untagged` decides whether untagged
indexers are also imported. With configured tags, `tag_match = "any"` imports an
indexer that has at least one configured tag, while `tag_match = "all"` requires
every configured tag. Prowlarr may return numeric tag IDs; Sporos resolves tag
labels when needed, and numeric tag IDs can also be configured directly.

Imported indexers keep a stable source identity from the Prowlarr source name
and Prowlarr indexer id, so Prowlarr renames update the existing Sporos row
rather than creating a new one. `remove_policy = "deactivate"` deactivates
previously imported rows that disappear from Prowlarr, are disabled there, no
longer match tags, or no longer look Torznab-compatible. The `ignore` remove
policy leaves previously imported rows active when they are absent from the
current refresh result.

Prowlarr outages are dependency degradation unless the source is required at
startup. Failed refreshes update health and retry/backoff state; previously
imported indexers remain in their last synced state until a later successful
refresh applies additions, updates, or deactivations.

## Environment Overrides

Scalar config fields can be overridden with `SPOROS__` environment variables.
Double underscores separate TOML path segments, and segments are converted to
lowercase. Arrays are not settable through indexed environment variables, so
set `paths.media_dirs` in TOML.

```bash
SPOROS__SERVER__BIND='"0.0.0.0:2468"'
SPOROS__PATHS__DATABASE='"/data/state/sporos.db"'
SPOROS__MATCHING__FUZZY_SIZE_THRESHOLD='0.02'
SPOROS__TORRENT_CLIENTS__QBIT_MAIN__URL='"http://qbittorrent:8080"'
SPOROS__TORRENT_CLIENTS__QBIT_MAIN__PASSWORD_FILE='"/var/run/secrets/qbit-password"'
SPOROS__INDEXERS__TORZNAB__MAIN__API_KEY_FILE='"/var/run/secrets/indexer-api-key"'
SPOROS__INDEXERS__PROWLARR__MAIN__API_KEY_FILE='"/var/run/secrets/prowlarr-api-key"'
```

Override values are parsed as TOML scalars first. Quote string values when the
shell value should be interpreted as a TOML string.

## Secrets

HTTP workflow authentication uses `server.api_token`, `server.api_token_file`,
or `server.api_token_env`. A non-loopback bind requires one of these token
sources. Callers must send it as `Authorization: Bearer <token>` when using
mutating workflow endpoints.

Torrent client passwords support `password`, `password_file`, and
`password_env`. Torznab and Prowlarr indexer keys support `api_key`,
`api_key_file`, and `api_key_env`.

Use file or environment-backed secrets in production. Inline `password` and
`api_key` values are for local development. Secret wrappers redact debug and
display output, and operator endpoints intentionally avoid exposing request
cookies, API keys, passkeys, and secret-bearing URLs. Prowlarr API keys and the
keys attached to imported Prowlarr indexers are redacted from logs, metrics,
status, support output, and validation errors.

## Paths And State

`paths.database` stores SQLite state. `paths.torrent_cache_dir` stores cached
candidate torrents. `paths.output_dir` stores saved candidate torrents prepared
for client injection or retry. `paths.media_dirs` are read-only media inventory
roots.

On startup Sporos creates parent directories for the database, torrent cache,
and output paths, then checks that local state paths are writable. Media
directories must already exist and be readable; Sporos does not create media
roots.

Back up the SQLite database and any saved torrent/output directories together.
For a consistent SQLite backup, stop the writer or use SQLite backup tooling
against the mounted state volume. The torrent cache can be recreated from
indexers, but preserving it avoids unnecessary redownloads.

## HTTP Surface

The service exposes:

- `GET /livez`: process liveness only; independent of external dependencies.
- `GET /readyz`: local readiness for config, database, schema, writable paths,
  and workers, plus dependency summaries.
- `GET /metrics`: Prometheus text metrics.
- `GET /v1/status`: readiness plus durable announce queue status.
- `POST /v1/announcements`: accepts validated announcements as durable queued
  work when the announce queue is running.

Workflow endpoints require bearer auth when an API token is configured. Startup
rejects externally reachable binds without a configured token.

## Readiness And Degraded Dependencies

Readiness separates accepting work from processing health. The service can
remain able to accept durable announcements while an indexer, torrent client,
Arr instance, or notification endpoint is degraded. Database, schema, local
state, or worker failures are local service failures and should be treated as
operator-actionable.

Use `/readyz` for Kubernetes readiness. A degraded dependency can appear in
readiness and metrics without requiring a restart. Sporos records retry and
backoff state so workers can resume safely after dependency recovery.

rTorrent HTTP authentication is not supported in this release. Configure
authentication at a reverse proxy or use a private RPC endpoint; Sporos rejects
rTorrent `username`, `password`, `password_file`, and `password_env` settings so
credentials are not silently ignored.

## Metrics

Scrape `GET /metrics` as Prometheus text. Important metric families include:

- `sporos_http_requests_total` for HTTP request volume and status.
- `sporos_workflow_enqueue_total` for accepted, rejected, deduplicated, and
  invalid workflow submissions.
- `sporos_queue_depth` and related queue gauges for bounded in-memory queues.
- `sporos_dependency_health_state` for dependency summaries.
- `sporos_announce_*` metrics for durable announce backlog, retries, leases,
  worker capacity, and dependency waits when the announce workflow is enabled.
- `sporos_prowlarr_refresh_total`,
  `sporos_prowlarr_refresh_duration_seconds`,
  `sporos_prowlarr_refresh_imported_total`, and
  `sporos_prowlarr_refresh_deactivated_total` for Prowlarr source refresh
  outcomes, latency, and import/deactivation counts.
- `sporos_notification_requests_total` and notification latency metrics for
  webhook delivery.

Indexer and torrent-client request counters are planned but are not wired into
the daemon runtime in this release.

Labels are intentionally bounded. Do not expect raw titles, request bodies,
cookies, API keys, or full secret-bearing URLs in metrics.

## Announce Queue Operations

The durable announce API and worker are not enabled in the daemon runtime in
this release. `POST /v1/announcements` returns `503 Service Unavailable`
instead of accepting work until that workflow is wired into production.

See [Announce Queue Operations](announce-queue.md) for queue health, TTL,
retention, retry, restart, and single-writer details.

## Optional Diagnostics

Use diagnostics that do not mutate state first:

- `sporos check-config --config /etc/sporos/config.toml`
- `sporos print-config-schema`
- `GET /livez`
- `GET /readyz`
- `GET /v1/status`
- `GET /metrics`

For dependency issues, compare readiness dependency summaries with metric
outcome counters. Queued announcement diagnostics are unavailable in this
release because the daemon does not accept announcements; the durable queue
state and `sporos_announce_*` metrics apply only when that workflow is enabled.
