# Development

The project pins Rust in `rust-toolchain.toml`. Nix is optional: both setup
paths expose the same Rust toolchain, and all project commands are ordinary
Cargo commands.

## With rustup

Install [rustup](https://rustup.rs/) and a C build toolchain, then enter the
repository. The C compiler and linker build the bundled SQLite library; no
system SQLite installation is required. Rustup reads the toolchain file and
installs Rust 1.98.0 with rustfmt and Clippy when a Cargo command first runs.

```console
cargo test --workspace
```

The additional tools used by the full validation suite can be installed with
their normal Cargo installation methods. They are conveniences, not build
requirements.

## With Nix

With flakes enabled:

```console
nix develop
cargo test --workspace
```

The default shell includes the pinned Rust toolchain plus `cargo-nextest`,
`cargo-deny`, `cargo-audit`, Taplo, and the SQLite CLI.

## Fuzzing

`cargo-fuzz` requires nightly Rust and a C++ compiler, so fuzzing has a separate
optional shell:

```console
nix develop .#fuzz
cargo fuzz run <target>
```

Without Nix, install nightly through rustup and `cargo-fuzz` through Cargo,
then run the same target with `cargo +nightly fuzz`. Fuzz targets will be added
as the Phase 0 parser evaluations begin.

## Checks

The baseline checks require the pinned Rust toolchain and the C build tools
described above:

```console
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo test --workspace --doc
```

When `cargo-nextest` is available, it can replace the ordinary test command:

```console
cargo nextest run --workspace --all-features --no-tests pass
```

Dependency policy checks are available when their tools are installed:

```console
cargo deny check
cargo audit
```

Phase 0 pins Duroxide 0.1.30. Its SQLite provider is vendored with a small,
documented patch under `vendor/duroxide`; changes there must be reviewed
separately from routine dependency updates. A torrent metainfo parser remains
unselected until the bounded-memory and v1/v2/hybrid experiments are complete.
