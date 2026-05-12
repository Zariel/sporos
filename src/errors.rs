use std::error::Error;
use std::fmt;
use std::path::PathBuf;

use crate::domain::{DomainError, JobName};
use crate::secrets::SecretFieldError;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum FailureClass {
    FatalLocal,
    RetryableDependency,
    BadRemoteData,
    UserActionRequired,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum HttpStatus {
    BadRequest,
    UnprocessableEntity,
    ServiceUnavailable,
    InternalServerError,
}

impl HttpStatus {
    pub const fn code(self) -> u16 {
        match self {
            Self::BadRequest => 400,
            Self::UnprocessableEntity => 422,
            Self::ServiceUnavailable => 503,
            Self::InternalServerError => 500,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum WorkerDisposition {
    ShutDown,
    Retry { retry_after_ms: Option<i64> },
    SkipItem,
    RequiresUserAction,
}

pub trait ClassifyFailure {
    fn failure_class(&self) -> FailureClass;
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ConfigError {
    MissingField { field: &'static str },
    InvalidField { field: &'static str, reason: String },
    InvalidDomain { source: DomainError },
    InvalidSecret { source: SecretFieldError },
    UnreadableFile { path: PathBuf, message: String },
}

impl ClassifyFailure for ConfigError {
    fn failure_class(&self) -> FailureClass {
        FailureClass::FatalLocal
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingField { field } => write!(formatter, "missing config field `{field}`"),
            Self::InvalidField { field, reason } => {
                write!(formatter, "invalid config field `{field}`: {reason}")
            }
            Self::InvalidDomain { source } => write!(formatter, "invalid domain value: {source}"),
            Self::InvalidSecret { source } => write!(formatter, "{source}"),
            Self::UnreadableFile { path, message } => {
                write!(
                    formatter,
                    "cannot read config file {}: {message}",
                    path.display()
                )
            }
        }
    }
}

impl Error for ConfigError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidDomain { source } => Some(source),
            Self::InvalidSecret { source } => Some(source),
            Self::MissingField { .. } | Self::InvalidField { .. } | Self::UnreadableFile { .. } => {
                None
            }
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DatabaseError {
    Busy {
        operation: String,
        retry_after_ms: Option<i64>,
    },
    QueryFailed {
        operation: String,
        message: String,
    },
    SchemaInitialization {
        message: String,
    },
    Unavailable {
        operation: String,
        message: String,
    },
}

impl ClassifyFailure for DatabaseError {
    fn failure_class(&self) -> FailureClass {
        match self {
            Self::Busy { .. } | Self::Unavailable { .. } => FailureClass::RetryableDependency,
            Self::QueryFailed { .. } | Self::SchemaInitialization { .. } => {
                FailureClass::FatalLocal
            }
        }
    }
}

impl DatabaseError {
    pub const fn retry_after_ms(&self) -> Option<i64> {
        match self {
            Self::Busy { retry_after_ms, .. } => *retry_after_ms,
            Self::QueryFailed { .. }
            | Self::SchemaInitialization { .. }
            | Self::Unavailable { .. } => None,
        }
    }
}

impl fmt::Display for DatabaseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Busy { operation, .. } => {
                write!(formatter, "database is busy during {operation}")
            }
            Self::QueryFailed { operation, message } => {
                write!(
                    formatter,
                    "database query failed during {operation}: {message}"
                )
            }
            Self::SchemaInitialization { message } => {
                write!(
                    formatter,
                    "database schema initialization failed: {message}"
                )
            }
            Self::Unavailable { operation, message } => {
                write!(
                    formatter,
                    "database unavailable during {operation}: {message}"
                )
            }
        }
    }
}

impl Error for DatabaseError {}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum IndexerError {
    BadResponse {
        indexer: String,
        message: String,
    },
    RateLimited {
        indexer: String,
        retry_after_ms: Option<i64>,
    },
    Unauthorized {
        indexer: String,
    },
    Unavailable {
        indexer: String,
        retry_after_ms: Option<i64>,
        message: String,
    },
}

impl ClassifyFailure for IndexerError {
    fn failure_class(&self) -> FailureClass {
        match self {
            Self::BadResponse { .. } => FailureClass::BadRemoteData,
            Self::RateLimited { .. } | Self::Unavailable { .. } => {
                FailureClass::RetryableDependency
            }
            Self::Unauthorized { .. } => FailureClass::UserActionRequired,
        }
    }
}

impl IndexerError {
    pub const fn retry_after_ms(&self) -> Option<i64> {
        match self {
            Self::RateLimited { retry_after_ms, .. } | Self::Unavailable { retry_after_ms, .. } => {
                *retry_after_ms
            }
            Self::BadResponse { .. } | Self::Unauthorized { .. } => None,
        }
    }
}

impl fmt::Display for IndexerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BadResponse { indexer, message } => {
                write!(
                    formatter,
                    "indexer `{indexer}` returned bad data: {message}"
                )
            }
            Self::RateLimited { indexer, .. } => {
                write!(formatter, "indexer `{indexer}` is rate limited")
            }
            Self::Unauthorized { indexer } => {
                write!(formatter, "indexer `{indexer}` rejected credentials")
            }
            Self::Unavailable {
                indexer, message, ..
            } => {
                write!(formatter, "indexer `{indexer}` unavailable: {message}")
            }
        }
    }
}

impl Error for IndexerError {}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum TorrentClientError {
    ApiChanged {
        client: String,
        message: String,
    },
    BadResponse {
        client: String,
        message: String,
    },
    Unauthorized {
        client: String,
    },
    Unavailable {
        client: String,
        retry_after_ms: Option<i64>,
        message: String,
    },
    UnsupportedCapability {
        client: String,
        capability: String,
    },
}

impl ClassifyFailure for TorrentClientError {
    fn failure_class(&self) -> FailureClass {
        match self {
            Self::BadResponse { .. } => FailureClass::BadRemoteData,
            Self::Unavailable { .. } => FailureClass::RetryableDependency,
            Self::ApiChanged { .. }
            | Self::Unauthorized { .. }
            | Self::UnsupportedCapability { .. } => FailureClass::UserActionRequired,
        }
    }
}

impl TorrentClientError {
    pub const fn retry_after_ms(&self) -> Option<i64> {
        match self {
            Self::Unavailable { retry_after_ms, .. } => *retry_after_ms,
            Self::ApiChanged { .. }
            | Self::BadResponse { .. }
            | Self::Unauthorized { .. }
            | Self::UnsupportedCapability { .. } => None,
        }
    }
}

impl fmt::Display for TorrentClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ApiChanged { client, message } => {
                write!(
                    formatter,
                    "torrent client `{client}` API changed: {message}"
                )
            }
            Self::BadResponse { client, message } => {
                write!(
                    formatter,
                    "torrent client `{client}` returned bad data: {message}"
                )
            }
            Self::Unauthorized { client } => {
                write!(formatter, "torrent client `{client}` rejected credentials")
            }
            Self::Unavailable {
                client, message, ..
            } => {
                write!(
                    formatter,
                    "torrent client `{client}` unavailable: {message}"
                )
            }
            Self::UnsupportedCapability { client, capability } => {
                write!(
                    formatter,
                    "torrent client `{client}` does not support {capability}"
                )
            }
        }
    }
}

impl Error for TorrentClientError {}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum TorrentParseError {
    InvalidBencode { message: String },
    InvalidInfoHash { source: DomainError },
    InvalidMetafile { source: DomainError },
    MissingInfoDictionary,
    UnsupportedLayout { message: String },
}

impl ClassifyFailure for TorrentParseError {
    fn failure_class(&self) -> FailureClass {
        FailureClass::BadRemoteData
    }
}

impl fmt::Display for TorrentParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidBencode { message } => {
                write!(formatter, "invalid torrent bencode: {message}")
            }
            Self::InvalidInfoHash { source } => {
                write!(formatter, "invalid torrent info hash: {source}")
            }
            Self::InvalidMetafile { source } => {
                write!(formatter, "invalid torrent metadata: {source}")
            }
            Self::MissingInfoDictionary => {
                write!(formatter, "torrent is missing an info dictionary")
            }
            Self::UnsupportedLayout { message } => {
                write!(formatter, "unsupported torrent layout: {message}")
            }
        }
    }
}

impl Error for TorrentParseError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidInfoHash { source } | Self::InvalidMetafile { source } => Some(source),
            Self::InvalidBencode { .. }
            | Self::MissingInfoDictionary
            | Self::UnsupportedLayout { .. } => None,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum MatchingError {
    InvalidTorrent { source: TorrentParseError },
    InsufficientLocalMetadata { item: String },
    PolicyRejected { reason: String },
    UnsupportedLayout { reason: String },
}

impl ClassifyFailure for MatchingError {
    fn failure_class(&self) -> FailureClass {
        match self {
            Self::InvalidTorrent { .. } => FailureClass::BadRemoteData,
            Self::InsufficientLocalMetadata { .. }
            | Self::PolicyRejected { .. }
            | Self::UnsupportedLayout { .. } => FailureClass::UserActionRequired,
        }
    }
}

impl fmt::Display for MatchingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTorrent { source } => {
                write!(formatter, "candidate torrent cannot be matched: {source}")
            }
            Self::InsufficientLocalMetadata { item } => {
                write!(formatter, "local item `{item}` has insufficient metadata")
            }
            Self::PolicyRejected { reason } => {
                write!(formatter, "candidate rejected by policy: {reason}")
            }
            Self::UnsupportedLayout { reason } => {
                write!(formatter, "unsupported matching layout: {reason}")
            }
        }
    }
}

impl Error for MatchingError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidTorrent { source } => Some(source),
            Self::InsufficientLocalMetadata { .. }
            | Self::PolicyRejected { .. }
            | Self::UnsupportedLayout { .. } => None,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ActionError {
    AlreadyExists { info_hash: String },
    InjectionFailed { source: TorrentClientError },
    SaveFailed { path: PathBuf, message: String },
    SourceIncomplete { info_hash: String },
}

impl ClassifyFailure for ActionError {
    fn failure_class(&self) -> FailureClass {
        match self {
            Self::AlreadyExists { .. } => FailureClass::BadRemoteData,
            Self::InjectionFailed { source } => source.failure_class(),
            Self::SaveFailed { .. } => FailureClass::FatalLocal,
            Self::SourceIncomplete { .. } => FailureClass::UserActionRequired,
        }
    }
}

impl ActionError {
    pub const fn retry_after_ms(&self) -> Option<i64> {
        match self {
            Self::InjectionFailed { source } => source.retry_after_ms(),
            Self::AlreadyExists { .. }
            | Self::SaveFailed { .. }
            | Self::SourceIncomplete { .. } => None,
        }
    }
}

impl fmt::Display for ActionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlreadyExists { info_hash } => {
                write!(formatter, "torrent `{info_hash}` already exists")
            }
            Self::InjectionFailed { source } => {
                write!(formatter, "torrent injection failed: {source}")
            }
            Self::SaveFailed { path, message } => {
                write!(
                    formatter,
                    "failed to save torrent to {}: {message}",
                    path.display()
                )
            }
            Self::SourceIncomplete { info_hash } => {
                write!(formatter, "source torrent `{info_hash}` is incomplete")
            }
        }
    }
}

impl Error for ActionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InjectionFailed { source } => Some(source),
            Self::AlreadyExists { .. }
            | Self::SaveFailed { .. }
            | Self::SourceIncomplete { .. } => None,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum WorkerError {
    DependencyUnavailable {
        dependency: String,
        retry_after_ms: Option<i64>,
        message: String,
    },
    JobFailed {
        job_name: JobName,
        class: FailureClass,
        message: String,
    },
    StartupFailed {
        message: String,
    },
}

impl ClassifyFailure for WorkerError {
    fn failure_class(&self) -> FailureClass {
        match self {
            Self::DependencyUnavailable { .. } => FailureClass::RetryableDependency,
            Self::JobFailed { class, .. } => *class,
            Self::StartupFailed { .. } => FailureClass::FatalLocal,
        }
    }
}

impl WorkerError {
    pub const fn retry_after_ms(&self) -> Option<i64> {
        match self {
            Self::DependencyUnavailable { retry_after_ms, .. } => *retry_after_ms,
            Self::JobFailed { .. } | Self::StartupFailed { .. } => None,
        }
    }

    pub fn disposition(&self) -> WorkerDisposition {
        match self.failure_class() {
            FailureClass::FatalLocal => WorkerDisposition::ShutDown,
            FailureClass::RetryableDependency => WorkerDisposition::Retry {
                retry_after_ms: self.retry_after_ms(),
            },
            FailureClass::BadRemoteData => WorkerDisposition::SkipItem,
            FailureClass::UserActionRequired => WorkerDisposition::RequiresUserAction,
        }
    }
}

impl fmt::Display for WorkerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DependencyUnavailable {
                dependency,
                message,
                ..
            } => {
                write!(
                    formatter,
                    "worker dependency `{dependency}` unavailable: {message}"
                )
            }
            Self::JobFailed {
                job_name, message, ..
            } => {
                write!(formatter, "worker job `{job_name}` failed: {message}")
            }
            Self::StartupFailed { message } => {
                write!(formatter, "worker startup failed: {message}")
            }
        }
    }
}

impl Error for WorkerError {}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ServiceError {
    Action(ActionError),
    Config(ConfigError),
    Database(DatabaseError),
    Indexer(IndexerError),
    Matching(MatchingError),
    TorrentClient(TorrentClientError),
    TorrentParse(TorrentParseError),
    Worker(WorkerError),
}

impl ClassifyFailure for ServiceError {
    fn failure_class(&self) -> FailureClass {
        match self {
            Self::Action(error) => error.failure_class(),
            Self::Config(error) => error.failure_class(),
            Self::Database(error) => error.failure_class(),
            Self::Indexer(error) => error.failure_class(),
            Self::Matching(error) => error.failure_class(),
            Self::TorrentClient(error) => error.failure_class(),
            Self::TorrentParse(error) => error.failure_class(),
            Self::Worker(error) => error.failure_class(),
        }
    }
}

impl ServiceError {
    pub const fn retry_after_ms(&self) -> Option<i64> {
        match self {
            Self::Action(error) => error.retry_after_ms(),
            Self::Database(error) => error.retry_after_ms(),
            Self::Indexer(error) => error.retry_after_ms(),
            Self::TorrentClient(error) => error.retry_after_ms(),
            Self::Worker(error) => error.retry_after_ms(),
            Self::Config(_) | Self::Matching(_) | Self::TorrentParse(_) => None,
        }
    }
}

impl fmt::Display for ServiceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Action(error) => write!(formatter, "{error}"),
            Self::Config(error) => write!(formatter, "{error}"),
            Self::Database(error) => write!(formatter, "{error}"),
            Self::Indexer(error) => write!(formatter, "{error}"),
            Self::Matching(error) => write!(formatter, "{error}"),
            Self::TorrentClient(error) => write!(formatter, "{error}"),
            Self::TorrentParse(error) => write!(formatter, "{error}"),
            Self::Worker(error) => write!(formatter, "{error}"),
        }
    }
}

impl Error for ServiceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Action(error) => Some(error),
            Self::Config(error) => Some(error),
            Self::Database(error) => Some(error),
            Self::Indexer(error) => Some(error),
            Self::Matching(error) => Some(error),
            Self::TorrentClient(error) => Some(error),
            Self::TorrentParse(error) => Some(error),
            Self::Worker(error) => Some(error),
        }
    }
}

impl From<ActionError> for ServiceError {
    fn from(error: ActionError) -> Self {
        Self::Action(error)
    }
}

impl From<ConfigError> for ServiceError {
    fn from(error: ConfigError) -> Self {
        Self::Config(error)
    }
}

impl From<DatabaseError> for ServiceError {
    fn from(error: DatabaseError) -> Self {
        Self::Database(error)
    }
}

impl From<IndexerError> for ServiceError {
    fn from(error: IndexerError) -> Self {
        Self::Indexer(error)
    }
}

impl From<MatchingError> for ServiceError {
    fn from(error: MatchingError) -> Self {
        Self::Matching(error)
    }
}

impl From<TorrentClientError> for ServiceError {
    fn from(error: TorrentClientError) -> Self {
        Self::TorrentClient(error)
    }
}

impl From<TorrentParseError> for ServiceError {
    fn from(error: TorrentParseError) -> Self {
        Self::TorrentParse(error)
    }
}

impl From<WorkerError> for ServiceError {
    fn from(error: WorkerError) -> Self {
        Self::Worker(error)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ErrorResponse {
    pub status: HttpStatus,
    pub code: &'static str,
    pub class: FailureClass,
    pub message: String,
    pub retry_after_ms: Option<i64>,
}

impl ErrorResponse {
    pub fn from_service_error(error: &ServiceError) -> Self {
        let class = error.failure_class();
        Self {
            status: http_status_for(class),
            code: error_code_for(class),
            class,
            message: error.to_string(),
            retry_after_ms: error.retry_after_ms(),
        }
    }
}

const fn http_status_for(class: FailureClass) -> HttpStatus {
    match class {
        FailureClass::FatalLocal => HttpStatus::InternalServerError,
        FailureClass::RetryableDependency => HttpStatus::ServiceUnavailable,
        FailureClass::BadRemoteData => HttpStatus::BadRequest,
        FailureClass::UserActionRequired => HttpStatus::UnprocessableEntity,
    }
}

const fn error_code_for(class: FailureClass) -> &'static str {
    match class {
        FailureClass::FatalLocal => "fatal_local_error",
        FailureClass::RetryableDependency => "retryable_dependency_error",
        FailureClass::BadRemoteData => "bad_remote_data",
        FailureClass::UserActionRequired => "user_action_required",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_errors_are_fatal_local_startup_failures() {
        let error = ServiceError::from(ConfigError::MissingField {
            field: "paths.database",
        });

        let response = ErrorResponse::from_service_error(&error);

        assert_eq!(FailureClass::FatalLocal, error.failure_class());
        assert_eq!(HttpStatus::InternalServerError, response.status);
        assert_eq!(500, response.status.code());
    }

    #[test]
    fn rate_limited_indexers_are_retryable_dependency_failures() {
        let error = ServiceError::from(IndexerError::RateLimited {
            indexer: "main".to_owned(),
            retry_after_ms: Some(10_000),
        });

        let response = ErrorResponse::from_service_error(&error);

        assert_eq!(FailureClass::RetryableDependency, error.failure_class());
        assert_eq!(HttpStatus::ServiceUnavailable, response.status);
        assert_eq!(Some(10_000), response.retry_after_ms);
    }

    #[test]
    fn torrent_parse_errors_are_bad_remote_data() {
        let error = ServiceError::from(TorrentParseError::MissingInfoDictionary);

        let response = ErrorResponse::from_service_error(&error);

        assert_eq!(FailureClass::BadRemoteData, error.failure_class());
        assert_eq!(HttpStatus::BadRequest, response.status);
        assert_eq!("bad_remote_data", response.code);
    }

    #[test]
    fn unsupported_client_capabilities_require_user_action() {
        let error = ServiceError::from(TorrentClientError::UnsupportedCapability {
            client: "archive".to_owned(),
            capability: "tags".to_owned(),
        });

        let response = ErrorResponse::from_service_error(&error);

        assert_eq!(FailureClass::UserActionRequired, error.failure_class());
        assert_eq!(HttpStatus::UnprocessableEntity, response.status);
        assert_eq!("user_action_required", response.code);
    }

    #[test]
    fn worker_disposition_follows_failure_class() {
        let retry = WorkerError::DependencyUnavailable {
            dependency: "indexer:main".to_owned(),
            retry_after_ms: Some(1_000),
            message: "timeout".to_owned(),
        };
        let startup = WorkerError::StartupFailed {
            message: "invalid config".to_owned(),
        };
        let user_action = WorkerError::JobFailed {
            job_name: JobName::new("rss").unwrap(),
            class: FailureClass::UserActionRequired,
            message: "bad credentials".to_owned(),
        };

        assert_eq!(
            WorkerDisposition::Retry {
                retry_after_ms: Some(1_000)
            },
            retry.disposition()
        );
        assert_eq!(WorkerDisposition::ShutDown, startup.disposition());
        assert_eq!(
            WorkerDisposition::RequiresUserAction,
            user_action.disposition()
        );
    }
}
