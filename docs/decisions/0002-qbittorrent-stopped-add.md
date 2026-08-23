# qBittorrent stopped-add contract

Status: Accepted for Phase 0.

## Decision

Sporos supports qBittorrent 5.2.0 or newer with Web API 2.14.1 or newer and
authenticates with `Authorization: Bearer <API key>`. Startup must validate both
versions before enabling mutations.

Every torrent upload uses these multipart fields:

- `stopped=true`
- `skip_checking=false`
- `contentLayout=Original`
- `autoTMM=false`
- an explicit `savepath`

The adapter accepts the legacy `Ok.` add acknowledgement and the structured add
receipt introduced by Web API 2.15. A structured receipt is valid only when the
single submitted torrent is either accepted or pending with no recorded
failure. The workflow still reconciles by infohash after submission.

After the torrent becomes visible, Sporos explicitly stops it and reads its
state again. A transient `checkingResumeData` state is expected before the stop
takes effect; an active transfer state is not.

## Experiment

`scripts/test-qbittorrent` runs the contract against qBittorrent 5.2.1 with Web
API 2.15.1 in a digest-pinned disposable container. It covers single- and
multi-file v1, v2, and hybrid torrents. The hybrid multi-file fixture includes
the BEP 52 padding needed to align its v1 representation.

For all six cases, qBittorrent:

- rejected an incorrect API key and accepted the configured key;
- reported the explicit save path;
- kept automatic torrent management disabled;
- preserved the requested tag;
- exposed single-file content as `<savepath>/<file>` and multi-file content as
  `<savepath>/<info name>` with `contentLayout=Original`;
- reached and remained in a stopped state after the explicit stop; and
- created no regular files below the download root before hardlink
  verification.

The production adapter contains no torrent deletion operation. Container and
volume removal belongs only to the test harness.
