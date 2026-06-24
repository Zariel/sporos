# syntax=docker/dockerfile:1.7

ARG RUST_VERSION=1.95.0
ARG RUNTIME_DEBIAN_VERSION=bookworm

FROM rust:${RUST_VERSION}-bookworm AS build

ARG RUST_VERSION
ENV CARGO_TERM_COLOR=never \
    CARGO_PROFILE_RELEASE_DEBUG=1 \
    RUSTUP_TOOLCHAIN=${RUST_VERSION}

WORKDIR /workspace

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        gcc \
        libc6-dev \
        make \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY crates/sporos-system-test-support/Cargo.toml crates/sporos-system-test-support/Cargo.toml

RUN mkdir -p src crates/sporos-system-test-support/src \
    && printf 'pub fn placeholder() {}\n' > src/lib.rs \
    && printf 'fn main() {}\n' > src/main.rs \
    && printf 'fn main() {}\n' > crates/sporos-system-test-support/src/main.rs

RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    cargo fetch --locked

RUN --network=none \
    --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    CARGO_NET_OFFLINE=true cargo build --release --locked --bin sporos \
    && rm -rf src

COPY src ./src

RUN --network=none \
    --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    rm -rf \
        target/release/.fingerprint/sporos-* \
        target/release/deps/libsporos-* \
        target/release/deps/sporos-* \
        target/release/sporos \
        target/release/sporos.d \
    && CARGO_NET_OFFLINE=true cargo build --release --locked --bin sporos

FROM build AS system-test-build

COPY crates ./crates

RUN --network=none \
    --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    CARGO_NET_OFFLINE=true cargo build --release --locked -p sporos-system-test-support

FROM debian:${RUNTIME_DEBIAN_VERSION}-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tini \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 sporos \
    && useradd --system --uid 10001 --gid sporos --home-dir /app sporos \
    && mkdir -p /app/state /app/cache /app/output \
    && chown -R sporos:sporos /app

COPY --from=build /workspace/target/release/sporos /app/sporos

LABEL org.opencontainers.image.title="Sporos" \
      org.opencontainers.image.description="Torrent automation service" \
      org.opencontainers.image.licenses="MIT"

ENV PATH=/app:$PATH \
    RUST_BACKTRACE=1 \
    RUST_LIB_BACKTRACE=1 \
    SPOROS__SERVER__BIND=0.0.0.0:2468 \
    SPOROS__PATHS__DATABASE=/app/state/sporos.db \
    SPOROS__PATHS__TORRENT_CACHE_DIR=/app/cache \
    SPOROS__PATHS__OUTPUT_DIR=/app/output

USER 10001:10001
WORKDIR /app

EXPOSE 2468
VOLUME ["/app/state", "/app/cache", "/app/output"]

ENTRYPOINT ["/usr/bin/tini", "--", "/app/sporos"]
CMD ["serve"]

FROM runtime AS system-test-support

COPY --from=system-test-build /workspace/target/release/sporos-system-test-support /app/sporos-system-test-support

FROM runtime AS production
