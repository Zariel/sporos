# Reliability and release tests

`scripts/check` runs the local required gate. Real dependencies use disposable
Podman or Docker containers through `scripts/test-qbittorrent` and
`scripts/test-image`.

The automated fault matrix maps to these focused tests:

| Boundary | Coverage |
|---|---|
| HTTP commit and process death | candidate and fake-task subprocess crash probes |
| Outbox lease/start ambiguity | outbox reconciliation and collision tests |
| Activity persistence/replay | Duroxide interruption, timer, and pinned-version tests |
| SQLite contention/corruption | concurrent pool, ownership, poison-history, and migration tests |
| qBittorrent add/read-after-write | adapter fakes plus digest-pinned qBittorrent 5.2 test |
| Partial hardlinks/recheck | idempotent link, conflict, piece-range, and resume-policy tests |
| Prowlarr outage/429/redirect | durable rate-limit, origin, streaming, and malformed XML tests |
| Arr outage | independent-instance and advisory-enrichment tests |
| Filesystem disappearance/symlink | data scan availability and no-symlink tests |
| Cancellation/retry | authoritative cancellation reconciliation and evidence-preserving retry tests |
| N-1 upgrade/backup | active pinned workflow migration and online backup/restore tests |

`sporos-testkit::ScriptedHttpServer` supplies ordered status/body/header replies,
delays, dropped connections, and captured requests for new adapter scenarios.

`scripts/fuzz` runs bounded smoke campaigns for torrent, Torznab, release-name,
and matcher inputs. It requires nightly Rust plus `cargo-fuzz`; the optional
`nix develop .#fuzz` shell supplies both.

## Target-scale performance

```console
scripts/test-performance
```

The release-mode inventory gate projects 10,000 torrents and streams 60,000
files while enforcing a 512 MiB process peak. Matcher benchmarks record strict,
flexible, partial, and season inputs. Run the service container itself under a
1 GiB cgroup when collecting release measurements.

## Soak

```console
scripts/soak
```

The default duration is 86,400 seconds. The test continuously commits and reads
SQLite state, performs passive WAL checkpoints, verifies database integrity, and
rejects more than 256 MiB RSS growth. A short plumbing check may use
`SPOROS_SOAK_SECONDS=5`; it is not a substitute for the scheduled 24-hour gate.
