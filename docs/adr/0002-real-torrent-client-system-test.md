# ADR 0002: Real torrent client system test

## Status

Implemented.

## Date

2026-05-17

## Context

Sporos supports qBittorrent and rTorrent as first-class torrent clients. The
current test suite has broad unit and daemon coverage with fake HTTP and
XML-RPC services, but it does not run Sporos against real torrent client
processes. That leaves release risk in the places fake services are least able
to prove:

- qBittorrent login, cookie reuse, version behavior, multipart add, tags,
  recheck, resume, and inventory paging;
- rTorrent XML-RPC transport, `load.raw`, `load.raw_start`, `d.custom1`,
  `d.check_hash`, `d.resume`, inventory multicalls, file listing, and directory
  handling;
- daemon startup, TOML config, SQLite state, background workers, HTTP workflow
  queues, metrics, and shared filesystem paths working together;
- the production container image running with the same entrypoint and path
  layout operators use.

The normal pre-commit and CI cargo gates must stay fast and deterministic.
Real torrent clients add container startup time, version drift, path
permissions, and protocol-specific flake risk. That cost is acceptable before
releases and in targeted CI, but not as an implicit part of every `cargo test`
run.

Sporos also intentionally blocks candidate torrent downloads from internal
addresses in production code. A Docker fixture indexer is an internal address,
so the system test must not weaken SSRF protections just to make local
candidate downloads convenient.

## Decision

Sporos will add an opt-in Docker-backed system test that runs the production
`sporos serve` entry point against real qBittorrent and real rTorrent services.
The test is a release gate and a targeted CI job, separate from the standard
cargo gate.

The harness will use:

- the Sporos production image built from the repository `Dockerfile`;
- one pinned qBittorrent container;
- one pinned rTorrent container exposing XML-RPC on a private compose network;
- a small deterministic Torznab fixture service for capabilities and search
  responses;
- checked-in or generated tiny torrent fixtures with documented info hashes;
- per-run state, cache, output, client config, and download volumes;
- a single script entry point for local and CI execution.

The expected operator command is:

```bash
scripts/system-test torrent-clients
```

The expected Rust test entry point is an ignored integration test:

```bash
cargo test --test system_torrent_clients -- --ignored --nocapture
```

The script owns Docker Compose lifecycle, fixture generation, service waits,
diagnostic collection, and cleanup. The Rust ignored test may drive assertions
against Sporos HTTP, SQLite, qBittorrent, and rTorrent once the compose stack is
running.

## Goals

- Prove the production daemon can start from TOML config and serve HTTP probes.
- Prove Sporos can refresh inventory from real qBittorrent and rTorrent.
- Prove a search workflow can match cached candidate torrents and inject into
  the correct real client.
- Prove qBittorrent and rTorrent protocol side effects are visible in the real
  clients.
- Prove metrics and SQLite state reflect successful search, matching, and
  client mutation.
- Keep release failures diagnosable through logs, database snapshots, metrics,
  and direct client state.

## Non-goals

- Replacing unit, adapter, daemon, matching, or persistence tests.
- Testing every matching rule or every torrent-client feature branch.
- Testing public tracker connectivity, DHT, peer exchange, or real data
  transfer.
- Testing candidate torrent download from the fixture indexer.
- Adding alternate config loaders, schema compatibility layers, or migration
  files for the test harness.
- Running real client containers as part of unqualified `cargo test`.

## Test Topology

Use Docker Compose or an equivalent CI service topology:

```text
system-runner
  -> sporos:local
       /app/config.toml
       /app/sporos.db
       /app/cache
       /app/output
       /downloads
  -> qbittorrent:version-or-digest
       /downloads
  -> rtorrent:version-or-digest
       /downloads
  -> torznab-fixture
       /api
```

Only Sporos needs a host-published HTTP port for local debugging. In CI, the
runner should use the private compose network and service DNS names. qBittorrent
and rTorrent management ports should not be published to the host unless a
developer explicitly asks for an interactive debug run.

The harness must mount identical absolute download paths into Sporos and the
clients. Recheck and resume depend on the torrent clients seeing the same files
Sporos selected as source data. A path that exists only in the Sporos container
is a test bug.

rTorrent auth fields are not configured because Sporos currently rejects
rTorrent `username`, `password`, `password_file`, and `password_env` settings.
The XML-RPC endpoint is protected by the private test network.

## Harness Files

The implementation should add the following structure or a close equivalent:

```text
scripts/system-test
docker/system/compose.yml
docker/system/README.md
docker/system/config/sporos.toml.template
docker/system/fixtures/
tests/system_torrent_clients.rs
tests/system/
```

`scripts/system-test` is the stable CI and local entry point. It should:

1. create a unique run directory under a temp path;
2. generate secrets and TOML config into the run directory;
3. build or select the Sporos image;
4. start compose services with per-run project names;
5. wait for client APIs and Sporos probes with bounded polling;
6. preload source torrents into both clients;
7. seed Sporos cached candidate torrents and candidate rows;
8. run the ignored Rust integration test or equivalent assertions;
9. collect compose logs, Sporos metrics, and relevant SQLite snapshots on
   failure;
10. run `docker compose down -v --remove-orphans` unless diagnostics are being
    intentionally preserved.

Secrets are generated at runtime. The repository may contain templates and
placeholder directories, but not committed passwords, bearer tokens, cookies,
or session state.

## Fixtures

The test uses tiny deterministic torrents and matching media files:

- qBittorrent source torrent;
- qBittorrent candidate torrent;
- rTorrent source torrent;
- rTorrent candidate torrent.

For each client, the source and candidate torrents should have the same visible
file tree and file sizes, but different info hashes. The simplest fixture shape
is one small file per torrent. A generator should document expected info hashes
and write the matching file contents so failures can be reproduced.

The source torrents are preloaded into real clients before Sporos inventory is
asserted. Candidate torrents are copied into `paths.torrent_cache_dir` and
inserted into `remote_candidates` after Sporos has synced the fixture Torznab
indexer. This preseeded cache is deliberate: the system test is for real
torrent-client integration, while internal-address candidate download safety is
covered separately.

## Scenarios

### 1. Startup And Client Inventory

Start `sporos serve --config /app/config.toml` in the production image.
Wait for:

- `GET /livez` to return `200`;
- `GET /readyz` to return `200`;
- qBittorrent Web API login and version checks to succeed;
- rTorrent `download_list` over XML-RPC to succeed.

Preload one source torrent into qBittorrent and one into rTorrent. Wait for
Sporos's client inventory worker to persist both clients.

Required assertions:

- `local_items` has one `source_type = 'client'` row for each expected client
  host and info hash;
- `local_files` contains expected relative paths, file indexes, and sizes;
- save paths in SQLite match absolute paths visible inside the client
  containers;
- `/metrics` records successful client inventory requests.

### 2. Search To Real Injection

Configure the Torznab fixture as a static indexer. Run or wait for the
`indexer_caps` job so Sporos has searchable caps for the fixture indexer.

For each source item, seed a cached candidate torrent and a matching
`remote_candidates` row using the fixture indexer's database id and candidate
GUID. The fixture Torznab search response returns the same candidate GUID and
title.

Submit searches through the public Sporos API:

```http
POST /v1/searches
Authorization: Bearer <generated-token>
Content-Type: application/json

{"query":"<source title>"}
```

Required assertions:

- the search queue completes within the bounded timeout;
- `remote_candidates` keeps the expected `torrent_cache_path`;
- `match_decisions` records one accepted decision per candidate;
- qBittorrent contains the candidate info hash, the `sporos` tag, and the
  expected save path;
- rTorrent contains the candidate info hash, `d.custom1 = "sporos"`, and the
  expected directory;
- `/metrics` includes successful search, Torznab search, action, and client
  request counters;
- no unexpected saved retry torrent remains in `paths.output_dir` for the happy
  path.

All waits must be bounded and poll observable state. The test must not rely on
fixed sleeps except as a small backoff between polls.

## CI And Release Gate

The normal required gate remains:

```bash
cargo fmt --check
cargo build
cargo check
cargo test
```

The real-client system gate runs after the normal cargo gate in release CI and
in any targeted CI workflow that claims release readiness:

```bash
scripts/system-test torrent-clients
```

The system gate must fail the release if any required assertion fails. It may be
optional for every pull request until its runtime and flake rate are known, but
it must run on mainline, merge queue, nightly, or release-candidate workflows
often enough to catch drift before a release.

Use pinned client image versions or digests for the release-blocking job. Add a
separate scheduled compatibility job that can test newer client image tags and
file issues when upstream behavior changes. Do not let the release-blocking job
silently track `latest`.

## Diagnostics

On failure, the harness should preserve or print:

- Sporos container logs;
- qBittorrent and rTorrent logs;
- fixture Torznab logs;
- `GET /readyz`;
- `GET /v1/status`;
- `GET /metrics`;
- bounded SQLite snapshots for `indexers`, `local_items`, `local_files`,
  `remote_candidates`, `match_decisions`, `dependency_health`, and `jobs`;
- direct qBittorrent torrent info for relevant hashes;
- direct rTorrent inventory and file info for relevant hashes.

Diagnostic output must not include bearer tokens, qBittorrent cookies,
passkeys, raw secret-bearing URLs, or generated passwords. Logs uploaded by CI
must be treated as operator diagnostics and scrubbed or bounded accordingly.

## Operational Constraints

- Use unique compose project names and per-run volumes so concurrent CI jobs do
  not share state.
- Align or deliberately loosen test-only filesystem ownership so Sporos UID
  `10001` and client container users can read and write shared paths.
- Keep torrents and files small enough that XML-RPC body limits, qBittorrent
  recheck latency, and CI disk usage are not meaningful sources of failure.
- Disable or ignore peer-network features. The test should not require external
  trackers, peers, DHT, or internet access after images are available.
- Keep all generated config TOML within the supported Sporos config surface.
- Do not edit `docs/internal` or rely on internal rewrite research as the test
  contract.

## Risks

- Real client images can change behavior even when Sporos has not changed.
  Digest or version pins reduce this risk, and scheduled floating compatibility
  runs make drift visible.
- qBittorrent first-start configuration can be nondeterministic if credentials
  are not preseeded. The harness must own client config initialization.
- rTorrent XML-RPC exposure differs by image. The selected image and compose
  config must document the exact XML-RPC endpoint used by Sporos.
- Cached candidate seeding is coupled to the current pre-release schema. Until
  the first Rust release, schema changes are folded into the inline initial
  schema; after release, the harness must follow append-only migrations.
- `/readyz` alone does not prove external dependency behavior. The test must
  assert real inventory and real injection, not just probe success.

## Consequences

This ADR adds a release-quality confidence layer around the highest-risk
external integration surface without slowing normal local development. It keeps
SSRF protections intact, makes real client behavior observable, and creates a
clear place for future compatibility matrix work.

The implementation will require follow-up work to add the harness script,
compose topology, fixture generator, ignored Rust test, and CI job wiring.
