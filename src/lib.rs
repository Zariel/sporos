//! Core library for the sporos Rust compatibility rebuild.

pub mod actions;
pub mod api;
pub mod cli;
pub mod clients;
pub mod config;
pub mod daemon;
pub mod domain;
pub mod error;
pub mod integrations;
pub mod matching;
pub mod memory;
pub mod notifications;
pub mod operations;
pub mod persistence;
pub mod scheduler;
pub mod search;
pub mod startup;
pub mod torrent;

pub use error::{Result, SporosError};

/// Package version reported by the command-line entry point.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests {
    use super::{SporosError, VERSION};

    #[test]
    fn exposes_package_version() {
        assert_eq!(VERSION, env!("CARGO_PKG_VERSION"));
    }

    #[test]
    fn errors_keep_user_facing_context() {
        let error = SporosError::configuration("missing config.js");

        assert_eq!(error.to_string(), "configuration error: missing config.js");
    }
}
