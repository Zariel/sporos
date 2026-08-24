# Security model

Autobrr bodies, torrent metainfo, Torznab XML, qBittorrent paths, Arr responses,
and filesystem names are untrusted. Parsers and upstream responses have byte,
depth, count, and time bounds. Prowlarr downloads stay on the configured origin
and cross-origin redirects are rejected.

The image runs as fixed UID/GID `65532` with no elevated capabilities. Use a
read-only root filesystem, writable `/data`, read-only configuration, read-only
media mounts, and one writable managed link root. Source and link root must
share a filesystem.

Secrets come from configuration or environment overrides, are redacted from
debug output, and are never emitted in metrics. Secret-file indirection is not
supported. Do not place API keys, passkeys, announce URLs, cookies, raw torrent
bytes, or Prowlarr proxy URLs in log fields.

Production adapters expose no torrent deletion, source deletion, link cleanup,
shell execution, chmod, or chown operation. Destination creation uses beneath-
root, no-symlink resolution and never overwrites a different inode. Source size,
device, and inode are verified immediately before linking.

One optional bearer key protects both admin and webhook routes. When it is
unset those routes have no application-level authentication, so bind the API to
a trusted private network or enforce authentication at a reverse proxy.
Terminate TLS before sending bearer credentials. Health and metrics contain no
secrets but should still be network-restricted.

Keep the pinned base image and Rust dependencies refreshed, review vendored
Duroxide and bencode changes separately, and verify the release signature, SBOM,
provenance, and immutable digest before deployment.
