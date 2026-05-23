# Real Client System Topology

This directory contains the Docker Compose topology used by the real torrent
client system harness. The stable entry point and lifecycle script are added by
later beads; for now this topology defines the services, network, volumes, and
templates they will render or mount.

## Services

- `sporos` builds the production image from the repository `Dockerfile` and
  runs `sporos serve --config /etc/sporos/config.toml`.
- `qbittorrent` runs qBittorrent Web API on the private compose network only.
- `rtorrent` runs rTorrent with XML-RPC exposed only on the private compose
  network.
- `torznab-fixture` serves deterministic Torznab caps/search XML and the tiny
  generated torrent fixtures.
- `system-init` seeds qBittorrent config, fixture media, and test-safe volume
  ownership before the long-running services start.

The only published port is Sporos HTTP on `127.0.0.1:${SPOROS_SYSTEM_HTTP_PORT:-2468}`.
qBittorrent, rTorrent XML-RPC, and the Torznab fixture are reachable only from
containers on the private `system` network.

## Image Pins

Release-blocking harness runs should keep these tags pinned until a later bead
switches to digest pins:

| Service | Image |
| --- | --- |
| qBittorrent | `lscr.io/linuxserver/qbittorrent:5.2.0` |
| rTorrent | `ghcr.io/crazy-max/rtorrent-rutorrent:5.2.10-0.16.7-r1` |
| Torznab fixture | `nginx:1.27.4-alpine` |
| Init | `alpine:3.21.3` |
| Sporos | local build from `Dockerfile` with Rust `1.95.0` and Debian `bookworm` |

The qBittorrent image uses LinuxServer.io's documented `WEBUI_PORT=8080`
setting. The rTorrent image uses the CrazyMax XML-RPC-over-nginx port
documented as `XMLRPC_PORT`, set here to `8000`.

qBittorrent is preseeded with `qbittorrent/qBittorrent.conf`. Because
qBittorrent management ports are private to the Compose network and not
published to the host, the config enables Web UI auth bypass for the isolated
container subnet. Sporos still reads a placeholder qBittorrent password file so
the runtime config shape matches production deployments; the later runner can
replace this with a password-hash seeding path if it needs auth-on coverage.

## Volumes And Paths

Compose project names isolate runs. The later runner should set a unique
project name, which gives each run its own named volumes:

- `sporos_state` -> `/data/state`
- `torrent_cache` -> `/data/cache/torrents`
- `output` -> `/data/output`
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

## Templates

`config/sporos.toml.template` is a runnable Sporos config shape for the
topology. It points at placeholder secret files and private service names. The
later runner may render a copy into the run directory if it needs unique tokens
or alternate ports.

`secrets/*.template` contain placeholder values only. They are not production
secrets. The later runner is responsible for generating per-run values and, for
qBittorrent, aligning the Web UI password in the client config with the
password file mounted into Sporos.
