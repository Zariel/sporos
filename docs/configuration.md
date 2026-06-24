# Sporos Configuration

Sporos reads TOML from `/app/config.toml` by default. Pass an explicit path with
`--config`:

```bash
sporos check-config --config /app/config.toml
sporos serve --config /app/config.toml
```

Use `sporos print-config-schema` to print the complete supported config shape.
`check-config` parses the file, applies environment overrides, validates typed
settings, creates required local state directories, and probes writable state
paths.

## Example

```toml
[paths]
database = "/app/state/db/sporos.db"
torrent_cache_dir = "/app/state/cache"
output_dir = "/app/state/output"
media_dirs = ["/media/movies", "/media/tv"]

[server]
bind = "0.0.0.0:2468"
api_token_env = "SPOROS_API_TOKEN"

[runtime]
worker_threads = 4
max_blocking_threads = 64
search_queue_limit = 100
indexing_queue_limit = 50
notification_queue_limit = 500
search_worker_concurrency = 4
manual_search_per_indexer_result_limit = 1000
manual_search_workflow_result_limit = 10000

[notifications.endpoints.ops]
url = "https://hooks.example/sporos"
token_env = "SPOROS_NOTIFICATION_TOKEN"
timeout = "30s"
retry_max_attempts = 3
retry_initial_delay = "1s"
retry_max_delay = "30s"

[torrent_clients.qbit_main]
kind = "qbittorrent"
url = "http://qbittorrent:8080"
username = "sporos"
password_env = "QBIT_PASSWORD"
default_save_path = "/downloads"
default_category = "cross-seed"
default_tags = ["cross-seed", "sporos"]

[indexers.torznab.main]
url = "https://indexer.example/api"
api_key_env = "INDEXER_API_KEY"

[matching]
mode = "partial"
fuzzy_size_threshold = 0.02
include_single_episodes = false
include_non_video = false

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
saved_retry_interval = "30m"
cleanup_interval = "24h"

[announce]
max_pending = 1000
worker_concurrency = 2
claim_batch_size = 10
default_ttl_secs = 86400
success_retention_secs = 604800
failure_retention_secs = 1209600
remote_candidate_retention_secs = 2592000
```

## Paths

`paths.database` stores SQLite state. `paths.torrent_cache_dir` stores cached
candidate torrent files. `paths.output_dir` stores saved torrents prepared for
client injection or retry. These local state paths must be writable by the
Sporos process.

These paths are plaintext operator-owned sensitive state. They can contain raw
active announce fetch URLs or cookies, tracker and indexer metadata, torrent
client hosts and paths, media titles, cached torrent files, and saved torrent
files. Protect database files, WAL/journal files, cache/output directories, and
backups with host, filesystem, and backup access controls.

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

## Runtime

`sporos serve` uses Tokio's multi-thread scheduler. Leave `[runtime]` unset to
use Tokio's default worker and blocking-thread policy, or set
`runtime.worker_threads` from 1 to 256 and `runtime.max_blocking_threads` from
1 to 512 when the deployment needs explicit CPU or blocking-IO caps.

The runtime queue limits bound accepted in-memory workflow requests before
backpressure is returned to callers. The defaults are intentionally conservative:
100 search requests, 50 indexing job requests, and 500 notification jobs.
`runtime.search_worker_concurrency` controls concurrent indexer search fan-out
for one search workflow. `runtime.manual_search_per_indexer_result_limit` caps
one indexer response, and `runtime.manual_search_workflow_result_limit` caps the
total candidates accepted for one manual search workflow. Announcement admission
is durable and is bounded by `announce.max_pending`.

`runtime.notification_queue_limit` bounds accepted notification jobs before
backpressure is reported. Leaving `[notifications.endpoints]` empty keeps
notification delivery disabled.

## Server And Auth

`server.bind` defaults to `127.0.0.1:2468` in the Rust config. The container
image sets `SPOROS__SERVER__BIND=0.0.0.0:2468` so Kubernetes Services and
probes can reach the Pod IP. Use `server.api_token_file`,
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
password_env = "QBIT_PASSWORD"
default_save_path = "/downloads"
default_category = "cross-seed"
default_tags = ["cross-seed", "sporos"]
```

rTorrent uses an HTTP RPC endpoint. Put authentication in a reverse proxy or
private network path:

```toml
[torrent_clients.rtorrent_archive]
kind = "rtorrent"
url = "http://rtorrent:5000/RPC2"
default_save_path = "/downloads/archive"
default_label = "cross-seed"
label_field = "custom1"
```

Injected torrents get client metadata from these optional fields:

- `default_category` is the qBittorrent category to create and send with new
  injections. Omit it to inject without a category.
- `default_tags` are qBittorrent tags to create and send with new injections.
  The default is `["sporos"]`, preserving the current Rust-native behavior.
- `default_label` is the rTorrent `custom1` value used in `load.raw*` and
  `d.custom1.set` calls. The default is `"sporos"`.

Use `cross-seed` values when matching an existing cross-seed-oriented client
layout. The built-in `sporos` defaults are intentionally conservative and keep
Sporos-owned injections easy to distinguish. qBittorrent uses category and
tags; rTorrent uses only `default_label` with `label_field = "custom1"`.

## Notifications

Configure optional webhook destinations under
`[notifications.endpoints.<name>]`. Endpoints are disabled when no entries are
configured. Each endpoint accepts a URL, one optional bearer token source, a
request timeout, and bounded retry policy:

```toml
[notifications.endpoints.ops]
url = "https://hooks.example/sporos"
token_env = "SPOROS_NOTIFICATION_TOKEN"
timeout = "30s"
retry_max_attempts = 3
retry_initial_delay = "1s"
retry_max_delay = "30s"
```

Use `token_file`, `token_env`, or local-development `token`. URLs must use
HTTP(S) and must not contain credentials, query parameters, or fragments.
Delivery health is best-effort and memory-only: `/v1/status` and metrics show
the latest in-process success or failure for each configured endpoint, and
endpoints return to `unknown` after restart.

## Injection

`[injection]` can define disabled-by-default dry-run and link preparation
policies. Set `dry_run = true` to run matching, download, and client preflight
while skipping torrent-client mutations, saved-torrent writes, prepared-link
creation, and saved-torrent deletion. Dry-run records and reports the action
Sporos would have taken, such as saving a candidate torrent or injecting into a
target client. Leave `link_type` unset to keep the current behavior of injecting
torrents directly into the client save path. When `link_type` is set,
`link_dirs` must contain at least one trusted operator-controlled directory.

```toml
[injection]
dry_run = false
link_type = "hardlink" # hardlink, symlink, reflink, or reflink_or_copy
link_dirs = ["/srv/sporos/links"]
flat_linking = false
```

`flat_linking = false` creates tracker-named subdirectories under the selected
link directory; `true` writes prepared links directly under the selected link
directory.

Prepared links use the saved-torrent retry file as their durable recovery
checkpoint. Controlled shutdown and client-side injection failures clean newly
created links before returning. If the process exits after links were created,
the next saved-torrent retry accepts existing matching prepared links, revalidates
them before client mutation, and keeps the retry file until the client confirms
that the torrent is complete or already owned. Replaced or unsafe prepared links
block client mutation; cleanup failures are reported as warnings with
`prepared_link_cleanup_incomplete` in the worker result.

## Injection Recheck And Auto-Resume

`[injection.recheck]` controls how Sporos adds a matched torrent, waits for the
client recheck, and decides whether to resume it. The defaults keep the current
conservative behavior: exact, size-only, partial, and video-disc matches are
added paused for recheck; partial matches are not auto-resumed unless a byte,
percentage, or non-relevant-file allowance says the remaining download is
acceptable.

```toml
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
```

`skip_recheck = true` skips the initial recheck for exact and size-only
matches, but partial matches and video-disc layouts still recheck. Auto-resume
thresholds apply only to partial, non-video-disc matches. Exact, size-only, and
video-disc matches keep their stricter behavior even when thresholds are set.

`max_remaining_bytes` allows auto-resume when the client reports at most that
many bytes still missing after recheck. `min_completion_percent` and
`max_remaining_percent` evaluate the same client-reported remaining bytes
against the candidate torrent's total size. The checks are permissive: a partial
match may resume when any configured byte or percentage threshold passes. Leave
the percentage fields unset to use byte-only behavior.

When `ignore_non_relevant_files_to_resume = true`, Sporos may also resume a
partial match when the remaining bytes can be explained by files such as
samples, trailers, subtitles, `.nfo`, `.srr`, or other explicitly non-relevant
release extras. `non_relevant_max_remaining_bytes` is a hard cap for this path,
and `piece_slack_multiplier` allows a small piece-size margin around the
non-relevant file total.

`poll_interval_ms` and `max_resume_wait_ms` bound how long Sporos waits for a
client recheck to finish before leaving the torrent paused and saving the
candidate for retry. Use a short poll interval only for test environments; in
normal operation the default five-second poll is intentionally quiet.

`below_threshold_action` decides what happens when a partial match is acceptable
but does not satisfy any auto-resume threshold:

- `inject_paused` adds the torrent paused and does not auto-resume it.
- `inject_and_start` adds the torrent unpaused so the client can start it even
  though the threshold did not pass.
- `reject_without_injecting` rejects the candidate before torrent-client
  mutation. Announcement workflows finish with a terminal rejected outcome, and
  search workflows count the candidate as rejected.

## Indexers

Direct Torznab indexers live under `[indexers.torznab.<name>]`:

```toml
[indexers.torznab.main]
url = "https://indexer.example/api"
api_key_env = "INDEXER_API_KEY"
```

Prowlarr import is optional. Configure it when Sporos should import
Torznab-compatible torrent search endpoints from Prowlarr:

```toml
[indexers.prowlarr.main]
url = "https://prowlarr.example"
api_key_env = "PROWLARR_API_KEY"
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
api_key_env = "SONARR_API_KEY"

[indexers.arr.radarr.main]
url = "http://radarr:7878"
api_key_env = "RADARR_API_KEY"
```

## Environment Overrides

Scalar fields can be overridden with `SPOROS__` environment variables. Double
underscores separate TOML path segments, and values are parsed as TOML scalars:

```bash
SPOROS__SERVER__BIND='"0.0.0.0:2468"'
SPOROS__PATHS__DATABASE='"/app/state/db/sporos.db"'
SPOROS__RUNTIME__WORKER_THREADS='4'
SPOROS__RUNTIME__MAX_BLOCKING_THREADS='64'
SPOROS__RUNTIME__SEARCH_QUEUE_LIMIT='100'
SPOROS__RUNTIME__INDEXING_QUEUE_LIMIT='50'
SPOROS__RUNTIME__NOTIFICATION_QUEUE_LIMIT='500'
SPOROS__RUNTIME__SEARCH_WORKER_CONCURRENCY='4'
SPOROS__RUNTIME__MANUAL_SEARCH_PER_INDEXER_RESULT_LIMIT='1000'
SPOROS__RUNTIME__MANUAL_SEARCH_WORKFLOW_RESULT_LIMIT='10000'
SPOROS__TORRENT_CLIENTS__QBIT_MAIN__URL='"http://qbittorrent:8080"'
SPOROS__TORRENT_CLIENTS__QBIT_MAIN__PASSWORD_ENV='"QBIT_PASSWORD"'
SPOROS__TORRENT_CLIENTS__QBIT_MAIN__DEFAULT_CATEGORY='"cross-seed"'
SPOROS__TORRENT_CLIENTS__QBIT_MAIN__DEFAULT_TAGS='"cross-seed,sporos"'
SPOROS__TORRENT_CLIENTS__RTORRENT_ARCHIVE__DEFAULT_LABEL='"cross-seed"'
SPOROS__INJECTION__RECHECK__MAX_REMAINING_PERCENT='15.0'
SPOROS__INJECTION__RECHECK__BELOW_THRESHOLD_ACTION='"inject_paused"'
SPOROS__INDEXERS__TORZNAB__MAIN__API_KEY_ENV='"INDEXER_API_KEY"'
SPOROS__NOTIFICATIONS__ENDPOINTS__OPS__TOKEN_ENV='"SPOROS_NOTIFICATION_TOKEN"'
```

Arrays such as `paths.media_dirs` should be set in TOML. `default_tags` can be
overridden as a comma-separated scalar environment value.

## Secrets

Secret-bearing fields generally support three forms:

- inline local-development value, such as `api_key`, `password`, or `token`
- file path, such as `api_key_file`, `password_file`, or `token_file`
- environment variable name, such as `api_key_env`, `password_env`, or
  `token_env`

Use file or environment-backed secrets for production. Do not place API keys in
indexer URL query strings.

## Scheduling And Announcements

`[scheduling].cleanup_interval` controls the scheduler-backed cleanup job for
announce TTL expiry, retained terminal row cleanup, stale lease recovery, and
stale remote candidate/torrent cache cleanup. The default is `24h`.
`[announce].default_ttl_secs` must be greater than
`retry_max_delay_secs` and no more than 7 days. It should usually stay at the 1
day default so active fetch material expires promptly.
`success_retention_secs` and `failure_retention_secs` must be between 1 second
and 30 days; defaults are 7 and 14 days respectively.
`[announce].remote_candidate_retention_secs` sets how long remote candidates and
their canonical cached torrent files are retained after they were last seen. It
must be between 1 second and 90 days, with a 30 day default. Candidates with
recent match decisions may remain longer so operator-visible matching history is
not removed early. Cleanup bounds row and cache-file counts, but SQLite database
files may not shrink immediately after deletes because SQLite can retain freed
pages for reuse.

External automation can submit candidates to `POST /v1/announcements`. Accepted
announcements are durable queued work: `202 Accepted` means the request was
validated and stored, not that matching or injection has already finished.

For operational detail, see the
[Operator Guide](operators/operator-guide.md) and
[Announce Queue Operations](operators/announce-queue.md).
