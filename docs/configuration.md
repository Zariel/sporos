# Sporos Configuration

Sporos reads TOML from `./config.toml` by default. Pass an explicit path with
`--config`:

```bash
sporos check-config --config /etc/sporos/config.toml
sporos serve --config /etc/sporos/config.toml
```

Use `sporos print-config-schema` to print the complete supported config shape.
`check-config` parses the file, applies environment overrides, validates typed
settings, creates required local state directories, and probes writable state
paths.

## Example

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

[indexers.torznab.main]
url = "https://indexer.example/api"
api_key_file = "/var/run/secrets/indexer-api-key"

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

## Paths

`paths.database` stores SQLite state. `paths.torrent_cache_dir` stores cached
candidate torrent files. `paths.output_dir` stores saved torrents prepared for
client injection or retry. These local state paths must be writable by the
Sporos process.

`paths.media_dirs` are read-only media inventory roots. They must already
exist and be readable; Sporos does not create media roots.

## Required And Optional Settings

For a network-facing daemon, configure:

- writable `paths.database`, `paths.torrent_cache_dir`, and `paths.output_dir`
- readable `paths.media_dirs`
- `server.bind` and one API token source
- at least one `[torrent_clients.<name>]`
- at least one direct Torznab indexer or one Prowlarr source that imports
  Torznab-compatible torrent search indexers

Matching, scheduling, announce queue limits, Arr services, notification
endpoints, and Prowlarr tag filters are optional tuning or integration settings.

## Server And Auth

`server.bind` defaults to `127.0.0.1:2468`. Use `server.api_token_file`,
`server.api_token_env`, or `server.api_token` to protect mutating workflow
endpoints. Non-loopback binds require one API token source.

Callers send the token as:

```text
Authorization: Bearer <token>
```

## Torrent Clients

Configure torrent clients under `[torrent_clients.<name>]`.

qBittorrent supports username/password authentication:

```toml
[torrent_clients.qbit_main]
kind = "qbittorrent"
url = "http://qbittorrent:8080"
username = "sporos"
password_file = "/var/run/secrets/qbit-password"
default_save_path = "/downloads"
```

rTorrent uses an HTTP RPC endpoint. Put authentication in a reverse proxy or
private network path:

```toml
[torrent_clients.rtorrent_archive]
kind = "rtorrent"
url = "http://rtorrent:5000/RPC2"
default_save_path = "/downloads/archive"
label_field = "custom1"
```

## Indexers

Direct Torznab indexers live under `[indexers.torznab.<name>]`:

```toml
[indexers.torznab.main]
url = "https://indexer.example/api"
api_key_file = "/var/run/secrets/indexer-api-key"
```

Prowlarr import is optional. Configure it when Sporos should import
Torznab-compatible torrent search endpoints from Prowlarr:

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

## Arr Services

Sonarr and Radarr instances are optional lookup helpers for title parsing and
search planning:

```toml
[indexers.arr.sonarr.main]
url = "http://sonarr:8989"
api_key_file = "/var/run/secrets/sonarr-api-key"

[indexers.arr.radarr.main]
url = "http://radarr:7878"
api_key_file = "/var/run/secrets/radarr-api-key"
```

## Environment Overrides

Scalar fields can be overridden with `SPOROS__` environment variables. Double
underscores separate TOML path segments, and values are parsed as TOML scalars:

```bash
SPOROS__SERVER__BIND='"0.0.0.0:2468"'
SPOROS__PATHS__DATABASE='"/data/state/sporos.db"'
SPOROS__TORRENT_CLIENTS__QBIT_MAIN__URL='"http://qbittorrent:8080"'
SPOROS__TORRENT_CLIENTS__QBIT_MAIN__PASSWORD_FILE='"/var/run/secrets/qbit-password"'
SPOROS__INDEXERS__TORZNAB__MAIN__API_KEY_FILE='"/var/run/secrets/indexer-api-key"'
```

Arrays such as `paths.media_dirs` should be set in TOML.

## Secrets

Secret-bearing fields generally support three forms:

- inline local-development value, such as `api_key` or `password`
- file path, such as `api_key_file` or `password_file`
- environment variable name, such as `api_key_env` or `password_env`

Use file or environment-backed secrets for production. Do not place API keys in
indexer URL query strings.

## Scheduling And Announcements

`[scheduling].cleanup_interval` controls the scheduler-backed cleanup job for
announce TTL expiry, retained terminal row cleanup, and stale lease recovery.
The default is `24h`.

External automation can submit candidates to `POST /v1/announcements`. Accepted
announcements are durable queued work: `202 Accepted` means the request was
validated and stored, not that matching or injection has already finished.

For operational detail, see the
[Operator Guide](operators/operator-guide.md) and
[Announce Queue Operations](operators/announce-queue.md).
