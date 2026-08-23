# syntax=docker/dockerfile:1.7
FROM docker.io/library/rust:1.98-bookworm@sha256:e70e2eec3d495fd5c8e0be74adda86507dfac7f51a724fbf9813ff59b2b247c7 AS build

WORKDIR /src
COPY . .
RUN cargo build --locked --release --bins

FROM docker.io/library/debian:bookworm-slim@sha256:abd67ffcfa541b485a3dff59865ab629aa048a6c613e639d36e7456b0b229241

ARG VERSION=0.0.0
ARG REVISION=unknown
LABEL org.opencontainers.image.title="Sporos" \
      org.opencontainers.image.description="Durable cross-seed orchestration service" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.source="https://github.com/Zariel/sporos" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}"

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /src/target/release/sporos /usr/local/bin/sporos
COPY --from=build /src/target/release/sporosctl /usr/local/bin/sporosctl
RUN mkdir -p /data /config && chown 65532:65532 /data /config

USER 65532:65532
EXPOSE 8080
VOLUME ["/data"]
ENTRYPOINT ["/usr/local/bin/sporos"]
