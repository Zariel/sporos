# syntax=docker/dockerfile:1
FROM docker.io/library/rust:1.98-bookworm@sha256:e70e2eec3d495fd5c8e0be74adda86507dfac7f51a724fbf9813ff59b2b247c7 AS build-base

RUN cargo install --locked --version 0.1.78 cargo-chef

WORKDIR /src

FROM build-base AS plan

COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM build-base AS build

COPY --from=plan /src/recipe.json recipe.json
COPY vendor vendor
RUN cargo chef cook --locked --release --recipe-path recipe.json
COPY . .
RUN cargo build --locked --release --bins \
    && strip target/release/sporos target/release/sporosctl

FROM gcr.io/distroless/cc-debian12:nonroot@sha256:9dac0a79194e45a7da0158a9c6da57b217585af0786db3845d1f0ec1a0dd182f

ARG VERSION=0.0.0
ARG REVISION=unknown
LABEL org.opencontainers.image.title="Sporos" \
      org.opencontainers.image.description="Durable cross-seed orchestration service" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.source="https://github.com/Zariel/sporos" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}"

COPY --from=build --chown=65532:65532 /src/target/release/sporos /usr/local/bin/sporos
COPY --from=build --chown=65532:65532 /src/target/release/sporosctl /usr/local/bin/sporosctl

USER 65532:65532
EXPOSE 8080
VOLUME ["/data"]
ENTRYPOINT ["/usr/local/bin/sporos"]
