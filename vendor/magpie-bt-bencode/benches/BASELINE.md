# magpie-bt-bencode — bench baseline (phase A)

Baseline captured 2026-04-13 on Darwin 25.3.0 (arm64), rustc 1.94.1, release profile (LTO thin, codegen-units=1).

| bench | input size | time (median) | throughput |
|---|---:|---:|---:|
| decode/small_dict | 24 B | 83 ns | 276 MiB/s |
| decode/flat_list/16 | ~92 B | 279 ns | 191 MiB/s |
| decode/flat_list/256 | ~1.5 KiB | 3.31 µs | 338 MiB/s |
| decode/flat_list/4096 | ~24 KiB | 55.3 µs | 405 MiB/s |
| decode/metainfo_like | 1.05 MiB | 7.83 µs | 125 GiB/s (see note) |

Note: `metainfo_like` throughput is dominated by the zero-copy borrow of the
long `pieces` byte string — the decoder only touches structural bytes, not the
pieces payload. This measures cost of the structural parse over a realistic
torrent shape, not payload copy cost.

## How to reproduce

```sh
cargo bench -p magpie-bt-bencode --bench decode -- \
  --warm-up-time 1 --measurement-time 2 --sample-size 10 \
  --save-baseline phase-a
```

## How to compare against baseline

```sh
cargo bench -p magpie-bt-bencode --bench decode -- \
  --baseline phase-a
```

Regression enforcement in CI (≥5 % on tracked benchmarks fails) is wired up in
Phase D per DISCIPLINES.md.
