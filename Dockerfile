# syntax=docker/dockerfile:1.7

FROM ghcr.io/rust-lang/rust:slim-bookworm AS chef

WORKDIR /workspace

RUN --mount=type=cache,id=cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=cargo-git,target=/usr/local/cargo/git \
    --mount=type=cache,id=cargo-target,target=/workspace/target \
    cargo install cargo-chef --locked --version 0.1.74

FROM chef AS planner

COPY rust-toolchain.toml Cargo.toml Cargo.lock ./

RUN mkdir -p src \
    && printf "" > src/lib.rs \
    && printf "fn main() {}\n" > src/main.rs \
    && mkdir -p benches \
    && printf "fn main() {}\n" > benches/memory_baseline.rs \
    && cargo chef prepare --recipe-path recipe.json

FROM chef AS builder

COPY rust-toolchain.toml Cargo.toml Cargo.lock ./
COPY --from=planner /workspace/recipe.json recipe.json
RUN --mount=type=cache,id=cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=cargo-git,target=/usr/local/cargo/git \
    --mount=type=cache,id=cargo-target,target=/workspace/target \
    cargo chef cook --release --locked --recipe-path recipe.json

COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY benches ./benches

RUN --mount=type=cache,id=cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=cargo-git,target=/usr/local/cargo/git \
    --mount=type=cache,id=cargo-target,target=/workspace/target \
    rm -f /workspace/target/release/sporos /workspace/target/release/deps/sporos* \
    && cargo build --release --locked -p sporos --bin sporos \
    && mkdir -p /workspace/artifacts \
    && cp /workspace/target/release/sporos /workspace/artifacts/sporos \
    && mkdir -p /workspace/data

FROM gcr.io/distroless/cc-debian12:nonroot

WORKDIR /app

COPY --from=builder /workspace/artifacts/sporos /app/sporos
COPY --from=builder --chown=nonroot:nonroot /workspace/data /data
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

ENV SPOROS__CONFIG_FILE=/config/config.toml
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt

VOLUME ["/config", "/data"]
EXPOSE 9000
USER nonroot:nonroot
ENTRYPOINT ["/app/sporos"]
CMD ["serve"]
