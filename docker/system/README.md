# Real Client System Topology

This directory contains the Docker Compose topology used by the real torrent
client system harness. Use the repository entry point:

```bash
scripts/system-test torrent-clients
```

Prerequisites:

- Docker with Compose v2.
- A working Rust toolchain matching `rust-toolchain.toml`.
- `openssl` on the host for generated per-run secrets.
- `perl` on the host for broad diagnostics redaction.
- Network access to pull the pinned Docker images on first run.

The harness builds the Sporos runtime image, starts private qBittorrent,
rTorrent, and Torznab fixture containers, seeds fixture torrents, then runs the
ignored Rust integration target:

```bash
cargo test --test system -- --ignored --nocapture
```

The ignored target is outside unqualified `cargo test`; normal local cargo gates
only validate the fixture files and compile the harness.

For debugging, preserve the run directory and compose stack:

```bash
scripts/system-test torrent-clients --preserve-diagnostics
```

On failure the runner writes a bounded, scrubbed diagnostics directory and
prints its path. The bundle includes service logs, Compose status, authenticated
Sporos probes, metrics, bounded database snapshots, and direct qBittorrent and
rTorrent state for the fixture candidate hashes. `--preserve-diagnostics` also
keeps the run directory and compose stack for manual inspection.

Expected runtime is a few minutes on a warm Docker cache and longer on the
first run because client images and the Sporos image must be built or pulled.
Most flakes are expected to be startup timing, Docker daemon stalls, or upstream
client image behavior changes. The runner uses bounded waits and collects
diagnostics before cleanup on failure.

## Covered Scenario

The harness covers the release-critical real-client path:

- Start Sporos with qBittorrent and rTorrent configured.
- Seed deterministic source torrents into each real client.
- Seed cached Torznab candidate torrents through the hidden system-test helper.
- Wait for client inventory refresh to persist local items and files.
- POST searches through the public Sporos API for both fixture titles.
- Assert cached candidates are matched and injected into qBittorrent and
  rTorrent with expected save paths, category/tags or label, file state, and
  metrics.
- Assert the happy path leaves no unexpected saved retry torrents.

Fixture metadata and expected info hashes are documented in
[`fixtures/README.md`](fixtures/README.md). Regenerate fixtures with:

```bash
cargo run --example generate_system_fixtures -- docker/system/fixtures
```

Then run the normal cargo gate and the system harness before committing fixture
changes.

## Services

- `sporos` builds the system-test Docker target from the repository
  `Dockerfile`, runs the production `sporos serve --config
  /etc/sporos/config.toml` command, and includes the
  `sporos-system-test-support` helper for harness-only setup and diagnostics.
  The production runtime image target still copies only the `sporos` binary.
- `qbittorrent` runs qBittorrent Web API on the private compose network only.
- `rtorrent` runs rTorrent with XML-RPC exposed only on the private compose
  network.
- `torznab-fixture` serves deterministic Torznab caps/search XML and the tiny
  generated torrent fixtures.
- `system-init` seeds qBittorrent config, fixture media, and test-safe volume
  ownership before the long-running services start.

The only published port is Sporos HTTP. The runner defaults
`SPOROS_SYSTEM_HTTP_PORT` to `0`, so Docker chooses an ephemeral host port unless
you set the variable explicitly. The resolved URL is exported to
`SPOROS_SYSTEM_HTTP_URL` in the run directory's `system-test.env`.
qBittorrent, rTorrent XML-RPC, and the Torznab fixture are reachable only from
containers on the private `system` network.

## Image Pins

Release-blocking harness runs should keep these images pinned. Use digest pins
where upstream tags are not stable enough for release gates:

| Service | Image |
| --- | --- |
| qBittorrent | `lscr.io/linuxserver/qbittorrent:5.2.0` |
| rTorrent | `ghcr.io/crazy-max/rtorrent-rutorrent@sha256:377bc208ec9d88c5fba6241e0c2ef08648ce8322307a6c687641a4182a7447e4` |
| Torznab fixture | `nginx:1.27.4-alpine` |
| Init | `alpine:3.21.3` |
| Sporos | local build from `Dockerfile` with Rust `1.95.0` and Debian `bookworm` |

The qBittorrent image uses LinuxServer.io's documented `WEBUI_PORT=8080`
setting. The rTorrent image uses the CrazyMax XML-RPC-over-nginx port
documented as `XMLRPC_PORT`, set here to `8000`.

The CI compatibility job can override the pinned client images with
`SPOROS_SYSTEM_QBITTORRENT_IMAGE` and `SPOROS_SYSTEM_RTORRENT_IMAGE`. The
defaults above remain the release-blocking pins.

Release CI runs `scripts/system-test torrent-clients` after the normal cargo
gate for version tags and manual dispatches. A separate scheduled/manual
compatibility job uses floating client tags to report upstream drift without
changing the release-blocking pins.

qBittorrent is preseeded with `qbittorrent/qBittorrent.conf`. Because
qBittorrent management ports are private to the Compose network and not
published to the host, the config enables Web UI auth bypass for the isolated
container subnet. Sporos still reads a placeholder qBittorrent password file so
the runtime config shape matches production deployments; the later runner can
replace this with a password-hash seeding path if it needs auth-on coverage.

## Volumes And Paths

Compose project names isolate runs. The runner sets a unique project name by
default, which gives each run its own named volumes:

- `sporos_state` -> `/app/state`
- `torrent_cache` -> `/app/cache/torrents`
- `output` -> `/app/output`
- `downloads` -> `/downloads`
- client config/session volumes for qBittorrent and rTorrent

The shared `/downloads` volume is mounted into Sporos, qBittorrent, and
rTorrent at the same absolute path so persisted save paths and client save
paths agree.

`system-init` copies checked-in fixture media into:

- `/downloads/qbittorrent`
- `/downloads/rtorrent`

It also gives the client UID/GID (`1000:1000`) ownership of client download and
config/session directories while leaving the downloaded files world-readable for
the Sporos runtime UID (`10001`).

After Sporos is live, the runner executes
`sporos-system-test-support system-test-seed` inside the Sporos container. The
helper reads the mounted fixture
manifest, copies candidate torrents into `paths.torrent_cache_dir` using the
normal cache filename format, and upserts matching `remote_candidates` rows
through the Rust repository. The Torznab fixture still advertises private
compose-network download URLs; the seed step lets later workflow tests use the
cached torrents without weakening production SSRF protections for candidate
downloads.

The runner writes `system-test.env` into its per-run directory and exports the
same values before invoking:

```bash
cargo test --test system -- --ignored --nocapture
```

The ignored Rust test uses that context to call Sporos on the host-published
HTTP port, inspect SQLite through the Sporos container, and assert the private
qBittorrent and rTorrent APIs from the compose network.

## Diagnostics

On failure, the runner copies a scrubbed diagnostics directory under
`${SPOROS_SYSTEM_RUN_ROOT:-${TMPDIR:-/tmp}}`. It includes:

- `compose-ps.txt`
- bounded tail of `compose-logs.txt`
- authenticated `/livez`, `/readyz`, `/v1/status`, and `/metrics` probes
- bounded SQLite snapshots from
  `sporos-system-test-support system-test-diagnostics`
- direct qBittorrent and rTorrent fixture hash state from
  `sporos-system-test-support system-test-client-state`

Generated bearer tokens, qBittorrent passwords, cookies, passkeys, and
secret-bearing URLs are redacted before diagnostics are archived or uploaded.
Set `SPOROS_SYSTEM_DIAGNOSTIC_LIMIT_BYTES` or
`SPOROS_SYSTEM_DIAGNOSTIC_TIMEOUT_SECONDS` to adjust per-file byte caps and
diagnostic command deadlines.

With `--preserve-diagnostics`, the runner keeps the run directory and Compose
stack. The script prints the run directory path and writes `system-test.env`
there; load or copy `SPOROS_SYSTEM_PROJECT` and
`SPOROS_SYSTEM_COMPOSE_OVERRIDE` from that file before manual inspection:

```bash
set -a
. /path/to/preserved-run/system-test.env
set +a
docker compose --project-name "$SPOROS_SYSTEM_PROJECT" \
  -f docker/system/compose.yml \
  -f "$SPOROS_SYSTEM_COMPOSE_OVERRIDE" ps
```

Clean up the preserved stack and volumes with:

```bash
docker compose --project-name "$SPOROS_SYSTEM_PROJECT" \
  -f docker/system/compose.yml \
  -f "$SPOROS_SYSTEM_COMPOSE_OVERRIDE" down -v --remove-orphans
```

## Templates

`config/sporos.toml.template` is a runnable Sporos config shape for the
topology. The runner copies it into a unique run directory and overlays compose
secrets with generated per-run values.

`secrets/*.template` contain placeholder values only. They are not production
secrets. They make `docker compose -f docker/system/compose.yml config` work
without the runner.
