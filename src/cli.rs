//! Command-line entry points and top-level command dispatch.

/// Run the command-line entry point.
pub fn run() -> crate::Result<()> {
    println!("sporos {}", crate::VERSION);
    Ok(())
}
