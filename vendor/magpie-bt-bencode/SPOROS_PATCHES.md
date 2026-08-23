# Sporos patches

This directory is based on `magpie-bt-bencode` 0.1.3, published from upstream
commit `4bb7a387b4ff4f261b115f5182159393c00a01e3`. The crates.io archive has SHA-256
`8aeac76f3229fe2bb78036cce1110b1e5924bd2b72da2c74449d1cc3e98d3bd2`.

Sporos carries the upstream node-budget change from Magpie commit
`82107aac13e0d125aaa7e2378d510b4cb99e58c1`. It adds a configurable limit to
the number of materialised bencode values, preventing a small torrent from
amplifying into an unbounded structural allocation.

Keep the patch separate and remove it when a reviewed Magpie release includes
the same behavior.
