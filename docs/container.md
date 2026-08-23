# Container image

Sporos publishes an OCI image for version tags. It does not publish Kubernetes,
Compose, Helm, or other deployment manifests.

The image runs as UID and GID `65532`, listens on port `8080`, and expects a
writable POSIX/block-backed volume at `/data`. Mount configuration read-only at
`/config/sporos.toml` and secrets as regular files under `/run/secrets`. Media
roots should be read-only; the configured link root must be writable and share
a filesystem with sources that will be hardlinked.

The image supports `linux/amd64` and `linux/arm64`. Base images and CI actions
are pinned immutably. Tagged builds receive semantic-version and revision OCI
labels and are pushed to GHCR. Pull-request and main builds exercise the same
multi-architecture build without publishing it.

Run the local smoke test with either Podman or Docker:

```console
scripts/test-image
```

The test uses a named volume, a read-only root filesystem, and a random
loopback port. It verifies liveness, readiness, the non-root user, and graceful
`SIGTERM` handling.
