# Sporos

Sporos provides reliable, efficient, and durable cross-seeding automation for
torrents. It accepts candidate releases from automation such as Autobrr, matches
them against local media and torrent-client inventory, prepares the matching
files, and injects the result into a supported torrent client.

Use Autobrr for fast tracker/indexer discovery and filtering. Use Sporos for the
stateful cross-seeding work: durable intake, matching, retry timing, safe file
preparation, torrent-client injection, and operator visibility.

## Features

- Durable announce intake: `POST /v1/announcements` stores accepted work before
  returning `202 Accepted`, deduplicates repeated announces, and survives daemon
  restarts.
- Cross-seed matching: compares candidate torrents with configured media roots
  and torrent-client inventory, including partial and size-aware matches.
- Safe injection flow: downloads candidate torrent metadata, prepares output
  files, injects into the chosen client, and handles recheck/resume behavior.
- Torrent-client support: qBittorrent and rTorrent.
- Indexer support: direct Torznab endpoints and optional Prowlarr import for
  Torznab-compatible torrent search endpoints.
- Durable retries and maintenance: bounded workers, retry/backoff state,
  retained outcomes, stale lease recovery, and scheduled cleanup.
- Operator visibility: `/livez`, `/readyz`, `/v1/status`, Prometheus metrics,
  dependency health, queue state, and explicit cleanup/indexer-cap jobs.
- Production configuration: TOML config, environment overrides, file/env-backed
  secrets, bounded queues, and redacted logs/metrics.

## Quick Start

Create a TOML config, validate it, then start the daemon:

```bash
sporos check-config --config /etc/sporos/config.toml
sporos serve --config /etc/sporos/config.toml
```

The default config path is `./config.toml`. Use
`sporos print-config-schema` to print the supported config surface.

## Sample Config

This is a compact starting point for qBittorrent plus Prowlarr-backed indexer
import. See [Configuration](docs/configuration.md) for the full config guide,
all supported fields, environment overrides, and secret handling.

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
default_category = "cross-seed"
default_tags = ["cross-seed"]

[indexers.prowlarr.main]
url = "http://prowlarr:9696"
api_key_file = "/var/run/secrets/prowlarr-api-key"
refresh_on_startup = true
required = false
remove_policy = "deactivate"

[matching]
mode = "partial"
fuzzy_size_threshold = 0.02
include_single_episodes = false
include_non_video = false

[scheduling]
client_inventory_interval = "24h"
indexer_caps_interval = "24h"
saved_retry_interval = "30m"
cleanup_interval = "24h"

[announce]
max_pending = 1000
worker_concurrency = 2
claim_batch_size = 10
default_ttl_secs = 86400
success_retention_secs = 604800
failure_retention_secs = 1209600
```

## Autobrr Setup

Autobrr should discover and filter candidate releases, then hand matching
releases to Sporos with a Webhook action. In Autobrr, add a Webhook action to
the filter that should feed Sporos.

Webhook URL:

```text
http://sporos:2468/v1/announcements
```

Method:

```text
POST
```

Headers:

```text
Authorization: Bearer <sporos api token>
Content-Type: application/json
```

Body:

```json
{
  "name": "{{ .TorrentName | js }}",
  "guid": "{{ .Indexer | js }}:{{ .TorrentID | js }}",
  "download_url": "{{ .TorrentUrl | js }}",
  "tracker": "{{ .IndexerName | js }}",
  "size": {{ .Size }}
}
```

Autobrr documents Webhook as an action type and supports template macros in
action fields. The important values for Sporos are the announced torrent name, a
stable tracker-scoped GUID, the torrent download URL, the tracker/indexer name,
and the size in bytes when available. See Autobrr's
[Actions](https://autobrr.com/filters/actions) and
[Macros](https://autobrr.com/filters/macros) docs for the full set of fields.

Do not also send the same Autobrr match directly to qBittorrent or rTorrent if
Sporos should own the cross-seed decision. Let Sporos decide whether the
candidate matches local data, prepare the files, and inject the torrent.

`202 Accepted` from Sporos means the announcement was validated and stored as
durable work. Matching and injection continue asynchronously. Check
`GET /v1/status` and `GET /metrics` for queue state, retry timing, dependency
health, and outcomes.

## Documentation

Start with [Configuration](docs/configuration.md). For operations, container
notes, metrics, readiness, scheduler jobs, and queue details, see the
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
