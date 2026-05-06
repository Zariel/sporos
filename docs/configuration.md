# Production Configuration

Sporos uses the Rust-native `config.toml` contract plus scalar `SPOROS__`
environment overrides. Generate a starter file with:

```sh
cross-seed --config /config/config.toml gen-config
```

In Kubernetes, mount one writable state volume at `/config` and use container
paths in the config. The default generated paths keep the SQLite database and
saved torrent output under that volume:

```toml
state_dir = "/config"
database_path = "/config/sporos.db"
output_dir = "/config/output"
```

## API

The service runtime listens on `listen_host` and `listen_port`. Use
`listen_host = "0.0.0.0"` inside a pod and expose it through a Service or
Ingress. Set an API key with at least 24 characters:

```toml
listen_host = "0.0.0.0"
listen_port = 9000
api_key = "replace-with-at-least-24-characters"
trusted_proxy_ips = ["10.0.0.1"]
```

Authenticated API requests must send `X-Api-Key`. Only configure
`trusted_proxy_ips` for proxies that are allowed to supply forwarded client
address headers.

## Sources

Use one local source mode for searchees:

```toml
use_client_torrents = true
data_dirs = []
```

or:

```toml
use_client_torrents = false
torrent_dir = "/torrents"
data_dirs = ["/media"]
```

Do not set `torrent_dir` and `use_client_torrents = true` together. Paths are
validated at startup and nested source, link, torrent, and output directories are
rejected.

## Clients

Torrent clients are structured TOML entries. Mark clients readonly when they are
only inventory sources:

```toml
[[torrent_clients]]
kind = "qbittorrent"
url = "http://qbittorrent:8080"
readonly = false
```

Supported `kind` values follow the Rust client adapters: `qbittorrent`,
`rtorrent`, `transmission`, and `deluge`.

## Indexers And Arr

Keep API keys separate from integration URLs:

```toml
[[torznab]]
url = "https://indexer.example/api"
api_key = "indexer-api-key"

[[sonarr]]
url = "http://sonarr:8989"
api_key = "sonarr-api-key"

[[radarr]]
url = "http://radarr:7878"
api_key = "radarr-api-key"
```

Do not include `apikey` or `api_key` query parameters in TOML integration URLs.
The CLI still accepts query-key URLs for administrative convenience and
normalizes them into structured config.

## Actions And Labels

Save mode writes matched torrents to `output_dir`:

```toml
action = "save"
```

Inject mode requires at least one non-readonly client. Use
`injection_category` and `injection_tags` for client labels, and configure
`link_dirs` when using linked injection:

```toml
action = "inject"
injection_category = "cross-seed"
injection_tags = ["managed"]
link_dirs = ["/links"]
link_type = "symlink"
```

`injection_category` overrides `link_category` when both are set.

## Scheduler

Leave cadences unset to disable scheduled loops. Enable them explicitly for the
service runtime:

```toml
search_cadence = "24 hours"
rss_cadence = "15 minutes"
```

Scheduled search and RSS require a local source and
`fuzzy_size_threshold <= 0.1`.

Daily cleanup also prunes terminal durable announce work after the configured
retention window:

```toml
[announce_queue]
terminal_retention = "7 days"
```

## Notifications

Configure webhook URLs and choose the result payload detail:

```toml
notification_webhook_urls = ["https://notify.example/hook"]
notification_payload_detail = "redacted"
```

Use `notification_payload_detail = "full"` only for trusted receivers. See
`docs/notifications.md` for the exact fields sent in each mode.

## Migration Constraints

Treat the current Rust TOML config and SQLite schema as the supported contract.
Do not rely on old TypeScript config names or compatibility loaders. For a fresh
Kubernetes deployment, start with a new writable `/config` volume, generate the
starter config, add clients/indexers/API auth, and let Sporos create the initial
SQLite database in `database_path`.
