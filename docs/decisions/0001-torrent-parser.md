# ADR 0001: Torrent metainfo parser

## Status

Accepted for Phase 0.

## Decision

Use exact-pinned `magpie-bt-metainfo` 0.1.3 and
`magpie-bt-bencode` 0.1.3.

The metainfo parser borrows byte strings from the input, retains the exact raw
`info` dictionary span, rejects non-canonical bencode and duplicate dictionary
keys, and has explicit v1, v2, and hybrid representations. This is a closer fit
than adopting a complete BitTorrent client crate and keeps protocol parsing
separate from network and storage machinery Sporos does not need.

The released bencode crate limits nesting but does not limit structural
allocation. Sporos vendors it with the upstream node-budget change from commit
`82107aac13e0d125aaa7e2378d510b4cb99e58c1` until that change is available in a
reviewed release.

The service adapter performs a bounded decode before typed parsing and then
projects only matching-relevant data. Its defaults enforce the design limits
for torrent size, nesting, structural nodes, file count, path components,
individual components, complete paths, and aggregate manifest paths. It also
rejects traversal, duplicate paths, file/directory prefix collisions, and v1
piece-count inconsistencies.

## Upgrade policy

Parser upgrades are reviewed separately from general dependency updates. An
upgrade must pass the v1/v2/hybrid corpus, malformed-input tests, path-safety
tests, and the `torrent` fuzz target before the exact pin changes.
