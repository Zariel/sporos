# Configuration

Sporos reads TOML from `/config/sporos.toml`. Set `SPOROS_CONFIG` to require a
different file. Environment overrides use `SPOROS__`, double underscores for
nesting, and TOML values; for example:

```console
SPOROS__SERVER__BIND='"127.0.0.1:9000"'
SPOROS__INJECTION__DRY_RUN=true
```

Secrets may be set as `api_key` in TOML or through the corresponding environment
override. Secret-file indirection is not supported. API-key environment values
are raw strings rather than TOML expressions, so Kubernetes `Secret` values can
be exposed directly with `env` or `envFrom`.

`auth.api_key` is optional and, when set, is the single bearer key for both
autobrr and admin API routes. Leaving it unset disables API authentication.
`qbittorrent.api_key` is also optional; leaving it unset makes qBittorrent
requests without an `Authorization` header. Prowlarr and each configured Arr
instance require an API key.

```console
SPOROS__AUTH__API_KEY=sporos-secret
SPOROS__QBITTORRENT__API_KEY=qbt_example
SPOROS__PROWLARR__API_KEY=prowlarr-secret
SPOROS__ARR__SONARR__MAIN__API_KEY=sonarr-secret
SPOROS__ARR__RADARR__MAIN__API_KEY=radarr-secret
```

```toml
[server]
bind = "0.0.0.0:8080"
request_timeout = "30s"
shutdown_grace = "20s"
admin_body_limit_bytes = 1048576
autobrr_body_limit_bytes = 12582912

[runtime]
data_dir = "/data"
database_path = "/data/sporos.db"
lock_path = "/data/sporos.lock"
lock_wait = "0s"

[qbittorrent]
url = "http://qbittorrent:8080"
request_timeout = "15s"
sync_interval = "5s"
full_reconcile_interval = "30m"
inventory_stale_after = "30s"
inventory_batch_size = 500
database_batch_size = 200

[prowlarr]
url = "http://prowlarr:9696"
request_timeout = "15s"
refresh_interval = "5m"
include_tags = []
exclude_tags = []
require_proxy_downloads = true
max_results_per_query = 100

[arr.sonarr.main]
url = "http://sonarr:8989"
request_timeout = "15s"

[arr.radarr.main]
url = "http://radarr:7878"
request_timeout = "15s"

[data_scan.roots.media]
path = "/media/library"
max_depth = 8
max_releases = 100000
max_files_per_release = 100000

[paths]
link_root = "/media/.sporos-links"

[[paths.rewrite]]
name = "downloads"
remote = "/downloads"
local = "/media/downloads"
services = ["qbittorrent", "sonarr", "radarr"]

[sources]
include_categories = []
exclude_categories = []
include_tags = []
exclude_tags = ["no-sporos"]
exclude_sporos_managed = true

[matching]
mode = "partial"
season_from_episodes = true
preflight_size_tolerance = 0.02
max_torrent_bytes = 8388608
max_files_per_torrent = 100000
max_path_bytes = 4096
max_assignment_files = 4096
max_candidate_edges = 100000
max_assignment_component_files = 128
max_assignment_operations = 50000000
pending_source_timeout = "7d"
video_extensions = ["mkv", "mp4", "m4v", "avi", "ts", "m2ts", "mov", "wmv", "webm", "iso"]
optional_extensions = ["nfo", "txt", "srt", "ass", "ssa", "sub", "idx", "jpg", "jpeg", "png", "sfv"]

[injection]
dry_run = false
category_template = "sporos"
tag_templates = ["sporos", "sporos:{{ trigger }}", "sporos:indexer:{{ indexer_slug }}", "sporos:mode:{{ match_mode }}"]
inherit_source_category = false
inherit_source_tags = false

[injection.resume]
mode = "complete_only"
combine = "and"

[limits]
max_http_requests = 16
max_candidate_workflows = 8
max_uploads = 4
outbox_batch_size = 100
```

Only named data roots are accepted by the API. Root paths and the managed link
root must be absolute. Configured bounds are validated at startup; unknown keys,
unsafe names, missing required API keys, invalid URLs, and unbounded response
limits fail closed. Secret-file paths are not part of the TOML schema.

`matching.mode` is `strict`, `flexible`, or `partial`. Resume policy is
independent of matching. `complete_only` starts only a fully present candidate;
threshold mode can set maximum missing bytes and/or minimum present ratio. Piece
integrity is always checked and has no disable switch.
