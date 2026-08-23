# Streaming inventory baseline

Status: Accepted for the Phase 0 parser gate.

The qBittorrent inventory decoder consumes a top-level JSON array through a
Serde sequence visitor and emits one owned torrent projection at a time. It
does not deserialize the array into a collection. A reader-level byte limit and
per-field limits apply before items reach the persistence callback.

The `phase0_inventory` release example generates 10,000 synthetic qBittorrent
torrents without retaining the generated response and clears a 250-item batch
at each simulated transaction boundary. On x86-64 Linux, its process-reported
peak RSS was 2,440 KiB:

```console
cargo run --release -p sporos-service --example phase0_inventory
torrents=10000 peak_rss_kib=2440
```

This establishes that the decoder and proposed batch size are comfortably
below the 512 MiB inventory-bootstrap target. It is not a measurement of the
complete service, SQLite page cache, or HTTP stack; those remain release-level
performance tests.
