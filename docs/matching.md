# Matching and injection safety

Matching is pure and deterministic. Every hardlink mapping requires exact byte
size equality and a one-to-one source file. Missing metadata lowers confidence;
conflicting media identity rejects the candidate.

- **Strict** requires every policy-required file and exact relative paths.
- **Flexible** permits candidate-native names and root layout when the complete
  exact-size mapping is unambiguous.
- **Partial** requires at least one primary video and allows missing candidate
  files. It imposes no built-in byte or percentage threshold.
- **Season from episodes** can assemble a season candidate from multiple complete
  qBittorrent torrents or configured data sources using episode identity and
  exact sizes.

Optional extensions and sample/proof/screenshot path classes may remain absent.
BDMV and VIDEO_TS are structured video layouts. Equal-best mappings to different
paths or inodes reject as ambiguous.

Injection is a separate safety stage:

1. qBittorrent receives the candidate stopped with automatic management off.
2. Sporos reads it back and verifies identity and save-path ownership.
3. Source size, device, and inode are rechecked beneath approved roots.
4. Hardlinks are created under a managed namespace without overwriting or
   unlinking any path.
5. qBittorrent rechecks the candidate.
6. Every missing piece is compared with linked file byte ranges.
7. The resume policy and mandatory integrity gate must both pass.

A partial candidate can be 90% missing when policy permits it, but it can never
start if a missing or failed piece intersects a hardlinked file. Dry-run mode
persists the decision and complete plan without external mutation.
