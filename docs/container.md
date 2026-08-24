# Container image

Sporos publishes `linux/amd64` and `linux/arm64` OCI images to
`ghcr.io/zariel/sporos`. Branch builds publish a moving, sanitized
branch tag (`main`, for example). A `vX.Y.Z` Git tag publishes `X.Y.Z`, then
creates the corresponding GitHub release. CI refuses to overwrite an existing
release image tag and does not create moving major, minor, or `latest` tags.
Use the published manifest digest when a deployment must be content-addressed.

The project does not publish Kubernetes, Compose, Helm, or other deployment
manifests.

The image runs as UID and GID `65532`, listens on port `8080`, and expects a
writable POSIX/block-backed volume at `/data`. Mount configuration read-only at
`/config/sporos.toml` and secrets as regular files under `/run/secrets`. Media
roots should be read-only; the configured link root must be writable and share
a filesystem with sources that will be hardlinked.

Base images and CI workflows are pinned immutably. Builds run natively on each
architecture and share a dependency-oriented BuildKit cache. Published images
include provenance, an SBOM, and keyless signatures. Pull requests exercise the
same multi-architecture build without publishing it.

Run the local smoke test with either Podman or Docker:

```console
scripts/test-image
```

The test uses a named volume, a read-only root filesystem, and a random
loopback port. It verifies liveness, readiness, the non-root user, and graceful
`SIGTERM` handling.
