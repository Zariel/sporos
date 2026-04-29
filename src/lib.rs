//! Core library for the sporos Rust compatibility rebuild.

/// Package version reported by the command-line entry point.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests {
    use super::VERSION;

    #[test]
    fn exposes_package_version() {
        assert_eq!(VERSION, env!("CARGO_PKG_VERSION"));
    }
}
