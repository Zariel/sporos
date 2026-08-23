{
  description = "Sporos development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
    rust-overlay.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs =
    { nixpkgs, rust-overlay, ... }:
    let
      supportedSystems = [
        "aarch64-linux"
        "x86_64-linux"
      ];
      forAllSystems = nixpkgs.lib.genAttrs supportedSystems;
    in
    {
      devShells = forAllSystems (
        system:
        let
          pkgs = import nixpkgs {
            inherit system;
            overlays = [ (import rust-overlay) ];
          };
          rustToolchain = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
        in
        {
          default = pkgs.mkShell {
            packages = [
              rustToolchain
              pkgs.cargo-audit
              pkgs.cargo-deny
              pkgs.cargo-nextest
              pkgs.pkg-config
              pkgs.sqlite
              pkgs.taplo
            ];
          };

          fuzz = pkgs.mkShell {
            packages = [
              pkgs.rust-bin.nightly.latest.minimal
              pkgs.cargo-fuzz
              pkgs.clang
            ];
          };
        }
      );
    };
}
