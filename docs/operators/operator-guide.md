# Sporos Operator Guide

This guide covers the supported operator surface: TOML configuration,
`SPOROS__` environment overrides, local state paths, HTTP probes, metrics, and
day-two operation.

## Commands

Run Sporos with an explicit config file:

```bash
sporos serve --config /app/config.toml
```

Validate the same typed config path before deployment:

```bash
sporos check-config --config /app/config.toml
```

Print the supported config surface:

```bash
sporos print-config-schema
```

Startup failures are written to stderr. Runtime logs are expected on stdout or
stderr; do not rely on file logging or hidden state outside the configured
paths.

## Container Image

Build the image with BuildKit enabled so dependency and registry cache mounts
are used:

```bash
DOCKER_BUILDKIT=1 docker build --pull -t sporos:local .
```

The Dockerfile uses a multi-stage Rust build. It copies `Cargo.toml` and
`Cargo.lock` before source files, runs `cargo fetch --locked`, compiles a dummy
crate to cache dependency artifacts, then builds the real `sporos` binary with
network access disabled. Release builds keep line-level debug information and
the runtime image enables Rust backtraces by default, so operator issue reports
can include useful stack frames. Runtime image contents are limited to the
binary, Debian CA certificates, a minimal init process, and the service user.

Run the container with operator-owned config, database, cache, output, and media
mounts. Provide secrets through environment variables supplied by the runtime:

```bash
docker run --rm \
  --name sporos \
  --stop-timeout 60 \
  -p 2468:2468 \
  -e SPOROS__SERVER__API_TOKEN \
  -e SPOROS__TORRENT_CLIENTS__QBIT_MAIN__PASSWORD \
  -e SPOROS__INDEXERS__PROWLARR__MAIN__API_KEY \
  -v ./config.toml:/app/config.toml:ro \
  -v sporos-state:/app/state \
  -v /srv/media:/media:ro \
  sporos:local
```

Mount a PVC at `/app/state` for the simple Kubernetes layout. The default
subdirectories are `/app/state/db`, `/app/state/cache`, and
`/app/state/output`, so SQLite's `sporos.db`, `sporos.db-wal`, and
`sporos.db-shm` files stay together while cache and output can still be mounted
separately by operators who want independent storage classes or backup policy.

The image runs as UID/GID `10001`. Mounted writable paths for
`paths.database`, `paths.torrent_cache_dir`, and `paths.output_dir` must be
writable by that identity, or by a runtime user override chosen by the operator.
Container defaults place those three paths under `/app` even when the mounted
config omits them. Set `server.bind = "0.0.0.0:2468"` in container and
Kubernetes configs so Services and probes can reach the Pod IP. Container
deployments must provide exactly one API token source, such as
`server.api_token_file` or the fixed Secret-backed
`SPOROS__SERVER__API_TOKEN` environment variable.
Use a stop timeout long enough for graceful shutdown to drain in-flight work; 60
seconds is a conservative starting point for Docker.
Deployment topology, orchestration, resource requests, and restart policy are
intentionally left to the operator.

## Configuration

Use TOML as the source of truth. Operator-supplied filesystem paths must be
absolute. Local defaults are resolved to absolute paths during startup.

```toml
[paths]
database = "/app/state/db/sporos.db"
torrent_cache_dir = "/app/state/cache"
output_dir = "/app/state/output"
media_dirs = ["/media/movies", "/media/tv"]

[server]
bind = "0.0.0.0:2468"

[torrent_clients.qbit_main]
kind = "qbittorrent"
url = "http://qbittorrent:8080"
username = "sporos"
default_save_path = "/downloads"
default_category = "cross-seed"
default_tags = ["cross-seed", "sporos"]

[torrent_clients.rtorrent_archive]
kind = "rtorrent"
url = "http://rtorrent:5000/RPC2"
default_save_path = "/downloads/archive"
default_label = "cross-seed"
label_field = "custom1"

[indexers.default_timeouts]
search = "120s"
download = "30s"

[indexers.torznab.main]
url = "https://indexer.example/api"

[indexers.prowlarr.main]
url = "https://prowlarr.example"
update_interval = "24h"
tags = ["movies", "hd"]
tag_match = "any"
include_untagged = true
refresh_on_startup = true
required = false
remove_policy = "deactivate"

[notifications.endpoints.ops]
url = "https://hooks.example/sporos"
timeout = "30s"
allow_duplicate_delivery = false
retry_max_attempts = 1
retry_initial_delay = "1s"
retry_max_delay = "30s"

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

[injection.recheck]
skip_recheck = false
max_remaining_bytes = 0
min_completion_percent = 85.0
max_remaining_percent = 15.0
ignore_non_relevant_files_to_resume = false
non_relevant_max_remaining_bytes = 209715200
piece_slack_multiplier = 2
poll_interval_ms = 5000
max_resume_wait_ms = 3600000
below_threshold_action = "inject_paused"

[scheduling]
client_inventory_interval = "24h"
indexer_caps_interval = "24h"
cleanup_interval = "24h"
saved_retry_interval = "30m"

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
remote_candidate_retention_secs = 2592000
```

Supported torrent clients are qBittorrent and rTorrent.

Injected torrent metadata is configurable per client. qBittorrent supports
`default_category` and `default_tags`; Sporos creates the configured category
and tags before the first injection and sends them with each add request. Omit
`default_category` to inject without a qBittorrent category. rTorrent supports
`default_label`, written to `custom1` through `load.raw*` and `d.custom1.set`;
`label_field` must remain `custom1`.

The built-in qBittorrent tag and rTorrent label default is `sporos`, keeping
Sporos-owned injections easy to distinguish. Configure `cross-seed` metadata
when matching an existing client layout or automation that expects
cross-seed-style labels. Client-specific fields are ignored by clients that do
not support that metadata type.

Auto-resume tuning lives under `[injection.recheck]`. Keep the defaults when
you want Sporos to add matches paused and only resume partial matches when there
is no remaining download. To accept lower-completion partial matches, set one
or more thresholds:

- `max_remaining_bytes` resumes when the client reports no more than that many
  missing bytes.
- `min_completion_percent` resumes when the candidate is at least that complete.
- `max_remaining_percent` resumes when the remaining bytes are at most that
  percentage of the candidate torrent size.

The byte and percentage checks are permissive: a partial, non-video-disc match
can resume when any configured threshold passes. Exact, size-only, and
video-disc matches keep stricter recheck behavior and do not use these partial
thresholds.

For partial matches that differ only by release extras, set
`ignore_non_relevant_files_to_resume = true`. Sporos then compares remaining
bytes with files such as samples, trailers, subtitles, `.nfo`, and `.srr`,
bounded by `non_relevant_max_remaining_bytes` and a piece-size allowance from
`piece_slack_multiplier`.

Use `below_threshold_action` for candidates that match but fail the auto-resume
thresholds. `inject_paused` adds the torrent paused and stops there.
`inject_and_start` adds it unpaused. `reject_without_injecting` avoids
torrent-client mutation and records a rejected search or terminal announcement
outcome, which is useful when external automation should handle low-completion
candidates outside Sporos.

Supported indexers are Torznab-compatible endpoints. Put API keys in
`api_key_file`, `SPOROS__...__API_KEY` environment overrides, or
development-only `api_key`; do not put API keys in the indexer URL query string.

## Prowlarr Discovery

Prowlarr discovery is optional. Configure one or more named
`[indexers.prowlarr.<name>]` sources when Sporos should import Torznab endpoints
from Prowlarr instead of listing every indexer under `indexers.torznab`.

```toml
[indexers.prowlarr.main]
url = "https://prowlarr.example"
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

Prowlarr API keys support `api_key_file`, the matching `SPOROS__...__API_KEY`
environment override, and local-development `api_key`, with the same
one-source-only rule as direct Torznab keys. The container examples use
environment-backed secrets for Kubernetes; for `[indexers.prowlarr.main]`,
provide:

```bash
SPOROS__INDEXERS__PROWLARR__MAIN__API_KEY
```

`update_interval` controls the periodic refresh cadence. `refresh_on_startup`
performs an immediate refresh during startup when true; when false, the first
daemon refresh waits for the configured interval plus deterministic jitter.
`required = true` makes startup fail if the startup refresh fails. The default
`required = false` records the source as degraded and lets Sporos continue so
operators can fix Prowlarr without restarting the service.

Only enabled Prowlarr indexers whose protocol is `torrent` and which support
search are imported. Tag filters apply before import. With `tags = []`, all
tagged indexers are imported and `include_untagged` decides whether untagged
indexers are also imported. With configured tags, `tag_match = "any"` imports
an indexer that has at least one configured tag, while `tag_match = "all"`
requires every configured tag. Prowlarr may return numeric tag IDs; Sporos
resolves tag labels when needed, and numeric tag IDs can also be configured
directly.

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

## Scheduling

The daemon persists supported scheduler jobs in SQLite and enqueues due runs
through bounded in-memory queues. Supported scheduled jobs are:

- `cleanup`: runs local maintenance for durable announce work and search state,
  including stale lease recovery, TTL expiry, retained terminal row cleanup, and
  stale remote candidate/torrent cache cleanup.
- `indexer_caps`: refreshes imported indexer capability metadata.

`[scheduling].cleanup_interval` controls how often the cleanup job is due. The
default is `24h`, which is usually enough for low-volume deployments. Shorten
it when operators need expired or retained announce work, stale remote
candidates, or canonical cached torrent files removed more quickly; lengthen it
when status and candidate history should remain visible longer and the queue is
not growing. `[announce].remote_candidate_retention_secs` controls stale remote
candidate retention. Candidates with recent match decisions may remain longer
so recent matching history stays available. Cleanup bounds row and canonical
cache-file growth, but SQLite database files may not shrink immediately after
deletes because freed pages can be reused by future writes.

Keep active announce TTLs short in production. Sporos accepts
`default_ttl_secs` values greater than `retry_max_delay_secs` and no more than
7 days; the 1 day default is the recommended production value for expiring
active fetch material. Terminal announce retention values, `success_retention_secs` and
`failure_retention_secs`, must be 1 second through 30 days. Remote candidate and
cached torrent retention must be 1 second through 90 days, with the 30 day
default usually enough for matching history while bounding local sensitive
state.

`[scheduling].indexer_caps_interval` controls periodic indexer capability
refresh. `client_inventory_interval` and `saved_retry_interval` control their
own daemon maintenance loops and are documented in the printed config schema.

Operators can queue an immediate supported job run with
`POST /v1/jobs/{job_name}/runs`, for example
`POST /v1/jobs/cleanup/runs`. A posted run updates durable job state and should
be treated as a mutating operation.

## Environment Overrides And Secrets

Any config value can be supplied by TOML or by a `SPOROS__` environment
override. Environment names are formed from the config path by uppercasing each
path segment and joining segments with double underscores. For example:

```bash
SPOROS__SERVER__BIND=0.0.0.0:2468
SPOROS__PATHS__DATABASE=/app/state/db/sporos.db
SPOROS__TORRENT_CLIENTS__QBIT_MAIN__PASSWORD=...
SPOROS__INDEXERS__PROWLARR__MAIN__API_KEY=...
SPOROS__NOTIFICATIONS__ENDPOINTS__OPS__TOKEN=...
```

HTTP workflow authentication uses `server.api_token`,
`server.api_token_file`, or `SPOROS__SERVER__API_TOKEN`. A non-loopback bind
requires one of these token sources. Callers must send it as
`Authorization: Bearer <token>` when using mutating workflow endpoints.

Known list fields use comma-separated environment values:
`paths.media_dirs`, `torrent_clients.<name>.default_tags`,
`indexers.prowlarr.<name>.tags`, and `injection.link_dirs`. Secret fields such
as `api_token`, `password`, `api_key`, and `token` are interpreted as raw
strings. Configure only one source for a secret: direct value, file path, or
environment override. Sporos does not read arbitrary env var names from config,
and `*_env` config fields are rejected.

Use environment-backed secrets in Kubernetes. File-backed secrets are still
supported when an operator intentionally mounts secret files. Inline `password`,
`api_key`, and notification `token` values are for local development. Secret
wrappers redact debug and display output, and operator endpoints intentionally
avoid exposing request cookies, API keys, passkeys, and secret-bearing URLs.
Prowlarr API keys and the keys attached to imported Prowlarr indexers are
redacted from logs, metrics, status, support output, and validation errors.

Sporos does not provide SQLite-at-rest encryption. Treat the database,
WAL/journal files, database backups, torrent cache, and output directory as
plaintext operator-owned sensitive state. While announce work is active, the
`announce_work.download_url` and `announce_work.cookie` columns can contain raw
tracker passkeys, signed URLs, or cookies so the daemon can retry after
restart. Redacted status, metrics, logs, and HTTP responses are not a guarantee
that local files are free of secrets.

Sensitive local state can include:

- raw active announce fetch URLs and cookies in SQLite;
- configured and imported indexer names, endpoint URLs, imported source
  identities, tracker hosts, capability metadata, API key source labels, and
  request health/backoff history;
- torrent-client hosts, save paths, categories, tags, labels, torrent hashes,
  file paths, tracker hosts, and progress state;
- media titles, virtual inventory paths, local source paths, match decisions,
  and rejection reasons;
- cached torrent files and saved torrent files prepared for injection or retry;
- SQLite WAL, journal, snapshots, diagnostics, and off-host backups.

## Paths And State

`paths.database` stores SQLite state. `paths.torrent_cache_dir` stores cached
candidate torrents. `paths.output_dir` stores saved candidate torrents prepared
for client injection or retry. `paths.media_dirs` are read-only media inventory
roots.

On startup Sporos creates parent directories for the database, torrent cache,
output paths, and configured injection link directories, then checks that local
state and link paths are writable. Media directories must already exist and be
readable; Sporos does not create media roots.

Back up the SQLite database and any saved torrent/output directories together.
For a consistent SQLite backup, stop the writer or use SQLite backup tooling
against the mounted database file. The default container paths are
`/app/state/db/sporos.db`, `/app/state/cache`, and `/app/state/output`, which
allows all local state to share one PVC by default while still allowing each
state class to be mounted and backed up separately in Kubernetes. The torrent
cache can be recreated from indexers, but preserving it avoids unnecessary
redownloads.
Protect backups with the same filesystem, host, and off-host access controls as
production secrets because they may include plaintext URLs, cookies, tracker
metadata, client paths, media titles, and cached torrent files.

## HTTP Surface

The service exposes:

- `GET /livez`: process liveness only; independent of external dependencies.
- `GET /readyz`: local readiness for config, database, schema, writable paths,
  and workers, plus dependency summaries.
- `GET /metrics`: Prometheus text metrics.
- `GET /v1/status`: readiness, dependency health, runtime queues, and durable
  announce queue status.
- `POST /v1/announcements`: accepts validated announcements as durable queued
  work.
- `POST /v1/searches`: queues an explicit search workflow.
- `POST /v1/jobs/{job_name}/runs`: queues a supported scheduler job run.
  Supported jobs are `cleanup` and `indexer_caps`.
- `POST /v1/notifications/test`: queues one test delivery for each configured
  notification endpoint.

Workflow endpoints require bearer auth when an API token is configured. Startup
rejects externally reachable binds without a configured token.

Representative `/v1/status` responses are checked in under
`docs/operators/status-examples/` and covered by HTTP tests so operator-facing
JSON changes are deliberate.

## Readiness And Degraded Dependencies

Readiness separates accepting work from processing health. The service can
remain able to accept durable announcements while an indexer, torrent client,
Arr instance, or notification endpoint is degraded. Database, schema, local
state, or worker failures are local service failures and should be treated as
operator-actionable.

Use `/readyz` for Kubernetes readiness. A degraded dependency can appear in
readiness and metrics without requiring a restart. Sporos records retry and
backoff state so workers can resume safely after dependency recovery.
`/v1/status.dependencies` lists dependency entries with stable `kind`, `name`,
`state`, retry, failure, and timestamp fields. `source` is `memory`,
`persisted`, or `memory_and_persisted`; `stale = true` means in-process health
and persisted health disagree, so operators should treat the in-process state as
current while preserving the persisted row for restart and audit context.
Notification endpoint delivery health is best-effort and memory-only. The
latest in-process delivery success or failure appears in `/v1/status` and
`sporos_dependency_health_state`, but configured endpoints return to `unknown`
after restart rather than preserving webhook delivery history in SQLite.

rTorrent HTTP authentication is not supported in this release. Configure
authentication at a reverse proxy or use a private RPC endpoint; Sporos rejects
rTorrent `username`, `password`, and `password_file` settings so
credentials are not silently ignored.

## Notification Operations

Notifications are optional webhook deliveries. Configure endpoints under
`[notifications.endpoints.<name>]`:

```toml
[notifications.endpoints.ops]
url = "https://hooks.example/sporos"
timeout = "30s"
allow_duplicate_delivery = false
retry_max_attempts = 1
retry_initial_delay = "1s"
retry_max_delay = "30s"
```

Use one token source per endpoint: `token_file`, the fixed
`SPOROS__NOTIFICATIONS__ENDPOINTS__<NAME>__TOKEN` environment variable, or
local-development `token`. The token is sent as a bearer token and is redacted
from debug output, errors, and metrics. Endpoint URLs must use HTTP(S) and must
not contain credentials, query parameters, or fragments.

`runtime.notification_queue_limit` bounds accepted notification jobs. When the
queue is full or closed, producers report rejected notification work instead of
blocking without bound. Queue depth, capacity, accepted, rejected, completed,
and cancelled counters are visible in `/v1/status` under the `notification`
runtime queue and in the `sporos_queue_*` metrics.

Each delivery uses the endpoint timeout and bounded retry policy. Notification
POSTs default to one attempt because a timeout after send can mean the webhook
already accepted the event. Set `allow_duplicate_delivery = true` before
setting `retry_max_attempts` above `1`. 2xx responses are success. 429 and 5xx
responses, request timeouts, and transport failures are retryable only when the
configured retry budget permits another attempt. Other non-2xx responses fail
without retry. Delivery health is best-effort and memory-only: `/v1/status` and
`sporos_dependency_health_state` expose the latest in-process endpoint state,
but notification health returns to `unknown` after restart.

Use `POST /v1/notifications/test` after changing endpoint config. The response
reports the number of endpoints, enqueued jobs, full-queue rejections, and
closed-queue rejections. Delivery attempts are observable through
`sporos_notification_requests_total` and
`sporos_notification_request_duration_seconds`.

## Metrics

Scrape `GET /metrics` as Prometheus text. Important metric families include:

- `sporos_http_requests_total` for HTTP request volume and status.
- `sporos_workflow_enqueue_total` for accepted, rejected, deduplicated, and
  invalid workflow submissions.
- `sporos_queue_depth` and related queue gauges for bounded in-memory queues.
- `sporos_dependency_health_state` for dependency summaries and
  `sporos_dependency_health_entries` for persisted dependency entry counts.
- `sporos_announce_*` metrics for durable announce backlog, retries, leases,
  worker capacity, and dependency waits.
- `sporos_search_attempts_total`, `sporos_decisions_total`, and
  `sporos_actions_total` for search, matching, and action outcomes.
- `sporos_indexer_requests_total`,
  `sporos_indexer_request_duration_seconds`,
  `sporos_client_requests_total`, and
  `sporos_client_request_duration_seconds` for external indexer and torrent
  client dependency calls.
- `sporos_job_duration_seconds`, `sporos_job_state`, and
  `sporos_job_last_duration_seconds` for scheduled and explicitly posted job
  runs.
- `sporos_prowlarr_refresh_total`,
  `sporos_prowlarr_refresh_duration_seconds`,
  `sporos_prowlarr_refresh_imported_total`, and
  `sporos_prowlarr_refresh_deactivated_total` for Prowlarr source refresh
  outcomes, latency, and import/deactivation counts.
- `sporos_notification_requests_total` and notification latency metrics for
  webhook delivery.

Labels are intentionally bounded. Do not expect raw titles, request bodies,
cookies, API keys, or full secret-bearing URLs in metrics.

## Announce Queue Operations

Sporos is centered on announce/event ingestion, matching, and injection.
External automation can submit candidate events through
`POST /v1/announcements`; the daemon then owns matching, retry timing, torrent
download, saving, and torrent-client injection.

The durable announce API and worker run in the daemon runtime.
`POST /v1/announcements` validates the request, persists accepted work in
SQLite, and returns `202 Accepted` before matching, saving, or client injection
has necessarily completed. If the durable queue is unavailable or at capacity,
the endpoint returns an error and records the rejected enqueue outcome.

See [Announce Queue Operations](announce-queue.md) for queue health, TTL,
retention, retry, restart, and single-writer details.

## Optional Diagnostics

Use read-only diagnostics first:

- `sporos print-config-schema`
- `GET /livez`
- `GET /readyz`
- `GET /v1/status`
- `GET /metrics`

Use side-effecting diagnostics when you want to validate writable state or
trigger daemon work:

- `sporos check-config --config /app/config.toml`: parses and validates
  config, creates required local state directories, and probes writable state
  paths.
- `POST /v1/jobs/indexer_caps/runs`: queues durable scheduler work and updates
  job/dependency state.
- `POST /v1/jobs/cleanup/runs`: queues durable cleanup work and updates job
  state while applying announce TTL, retention, and stale lease maintenance.

For dependency issues, compare readiness dependency summaries with metric
outcome counters. For queued announcement issues, compare `/v1/status`
`announce_queue` counts with `sporos_announce_*` metrics, especially active
work, retry delay, worker busy/idle gauges, attempt classes, and dependency
wait counts.
