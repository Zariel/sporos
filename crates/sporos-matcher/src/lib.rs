//! Pure, deterministic matching for Sporos.

mod matcher;
mod release;

pub use matcher::{MatchRequest, Matcher, PureMatcher};
pub use release::{normalize_title, parse_release};
