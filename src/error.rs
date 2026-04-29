//! Error types returned by production library code.

use std::borrow::Cow;

/// Result type used by sporos library modules.
pub type Result<T> = std::result::Result<T, SporosError>;

/// Error type that preserves actionable diagnostics for the caller.
#[derive(Debug, thiserror::Error)]
pub enum SporosError {
    /// CLI or configuration validation failed.
    #[error("configuration error: {message}")]
    Configuration { message: Cow<'static, str> },

    /// Startup validation or singleton setup failed.
    #[error("startup error: {message}")]
    Startup { message: Cow<'static, str> },

    /// Domain model construction or validation failed.
    #[error("domain error: {message}")]
    Domain { message: Cow<'static, str> },

    /// Persistent state or cache access failed.
    #[error("persistence error: {message}")]
    Persistence { message: Cow<'static, str> },

    /// Torrent parsing or normalization failed.
    #[error("torrent error: {message}")]
    Torrent { message: Cow<'static, str> },

    /// Search orchestration failed.
    #[error("search error: {message}")]
    Search { message: Cow<'static, str> },

    /// Candidate matching or decision caching failed.
    #[error("matching error: {message}")]
    Matching { message: Cow<'static, str> },

    /// External indexer, Arr, or notification integration failed.
    #[error("integration error: {message}")]
    Integration { message: Cow<'static, str> },

    /// Torrent-client adapter operation failed.
    #[error("torrent client error: {message}")]
    TorrentClient { message: Cow<'static, str> },

    /// Save, inject, link, restore, or cleanup action failed.
    #[error("action error: {message}")]
    Action { message: Cow<'static, str> },

    /// HTTP API request handling failed.
    #[error("api error: {message}")]
    Api { message: Cow<'static, str> },

    /// Scheduler job coordination failed.
    #[error("scheduler error: {message}")]
    Scheduler { message: Cow<'static, str> },

    /// Operational task failed.
    #[error("operation error: {message}")]
    Operation { message: Cow<'static, str> },
}

impl SporosError {
    /// Build a configuration error.
    pub fn configuration(message: impl Into<Cow<'static, str>>) -> Self {
        Self::Configuration {
            message: message.into(),
        }
    }
}
