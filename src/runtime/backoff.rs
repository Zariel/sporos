use std::future::Future;
use std::io;
use std::time::Duration;

use crate::errors::{
    ClassifyFailure, DatabaseError, FailureClass, IndexerError, TorrentClientError,
};
use crate::runtime::shutdown::{ShutdownPhase, ShutdownSignal};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct JitteredBackoffPolicy {
    pub base_delay_ms: i64,
    pub max_delay_ms: i64,
    pub jitter_ms: i64,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct BackoffProbePolicy {
    pub retry: JitteredBackoffPolicy,
    pub recovery_probe_interval_ms: i64,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum RetryOutcome<T> {
    Completed(T),
    Exhausted,
    Shutdown,
}

pub const TRANSIENT_IO_RETRY_MAX_ATTEMPTS: u8 = 3;

pub const fn transient_io_retry_policy() -> JitteredBackoffPolicy {
    JitteredBackoffPolicy {
        base_delay_ms: 100,
        max_delay_ms: 1_000,
        jitter_ms: 100,
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum RetryErrorKind {
    TransientNetwork,
    Timeout,
    RateLimited,
    DependencyBackoff,
    DatabaseBusy,
    TransientLocalIo,
    Authentication,
    PermissionDenied,
    BadRequest,
    NotFound,
    Unsupported,
    InvalidResponse,
    InvalidInput,
    Cancelled,
    FatalLocal,
    NonIdempotent,
    Unknown,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum RetryDecision {
    Retry {
        kind: RetryErrorKind,
        retry_after: Option<Duration>,
    },
    DoNotRetry {
        kind: RetryErrorKind,
    },
}

impl RetryDecision {
    pub const fn retry(kind: RetryErrorKind) -> Self {
        Self::Retry {
            kind,
            retry_after: None,
        }
    }

    pub const fn retry_after(kind: RetryErrorKind, retry_after: Duration) -> Self {
        Self::Retry {
            kind,
            retry_after: Some(retry_after),
        }
    }

    pub const fn do_not_retry(kind: RetryErrorKind) -> Self {
        Self::DoNotRetry { kind }
    }

    pub const fn should_retry(self) -> bool {
        matches!(self, Self::Retry { .. })
    }

    pub const fn kind(self) -> RetryErrorKind {
        match self {
            Self::Retry { kind, .. } | Self::DoNotRetry { kind } => kind,
        }
    }

    pub const fn explicit_delay(self) -> Option<Duration> {
        match self {
            Self::Retry { retry_after, .. } => retry_after,
            Self::DoNotRetry { .. } => None,
        }
    }
}

pub trait RetryAfterSource {
    fn retry_after_delay(&self) -> Option<Duration>;
}

impl JitteredBackoffPolicy {
    pub fn retry_deadline_ms(
        self,
        now_ms: i64,
        consecutive_failures: u16,
        explicit_retry_after_ms: Option<i64>,
        jitter_key: &str,
    ) -> i64 {
        if let Some(retry_after_ms) = explicit_retry_after_ms {
            return retry_after_ms;
        }
        now_ms.saturating_add(self.delay_ms(consecutive_failures, jitter_key))
    }

    pub fn delay_ms(self, consecutive_failures: u16, jitter_key: &str) -> i64 {
        let shift = u32::from(consecutive_failures.min(6));
        let multiplier = 1_i64.checked_shl(shift).unwrap_or(i64::MAX);
        let delay = self.base_delay_ms.saturating_mul(multiplier);
        delay
            .saturating_add(stable_jitter_ms(
                jitter_key,
                consecutive_failures,
                self.jitter_ms,
            ))
            .min(self.max_delay_ms)
    }
}

impl BackoffProbePolicy {
    pub fn should_probe(
        self,
        now_ms: i64,
        retry_after_ms: Option<i64>,
        last_probe_ms: Option<i64>,
        explicit_retry_after: bool,
    ) -> bool {
        if retry_after_ms.is_some_and(|retry_after| retry_after > now_ms) {
            if explicit_retry_after {
                return false;
            }
            return last_probe_ms.is_none_or(|last_probe| {
                now_ms.saturating_sub(last_probe) >= self.recovery_probe_interval_ms
            });
        }
        true
    }
}

pub fn fixed_retry_deadline_ms(
    now_ms: i64,
    delay_ms: i64,
    explicit_retry_after_ms: Option<i64>,
) -> i64 {
    explicit_retry_after_ms
        .filter(|retry_after| *retry_after > now_ms)
        .unwrap_or_else(|| now_ms.saturating_add(delay_ms.max(1)))
}

pub fn bounded_exponential_delay(
    initial_delay: Duration,
    max_delay: Duration,
    attempt: u8,
) -> Duration {
    let multiplier = 1_u32
        .checked_shl(u32::from(attempt.saturating_sub(1)))
        .unwrap_or(u32::MAX);
    initial_delay.saturating_mul(multiplier).min(max_delay)
}

pub fn stable_jitter_ms(jitter_key: &str, attempt: u16, jitter_ms: i64) -> i64 {
    if jitter_ms <= 0 {
        return 0;
    }
    let seeded =
        stable_jitter_seed(jitter_key).saturating_add(u64::from(attempt).saturating_mul(97));
    i64::try_from(seeded % u64::try_from(jitter_ms).unwrap_or(1)).unwrap_or_default()
}

pub fn stable_jitter_seed(value: &str) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

pub fn classify_failure_class(error: &impl ClassifyFailure) -> RetryDecision {
    match error.failure_class() {
        FailureClass::RetryableDependency => {
            RetryDecision::retry(RetryErrorKind::DependencyBackoff)
        }
        FailureClass::BadRemoteData => RetryDecision::do_not_retry(RetryErrorKind::InvalidResponse),
        FailureClass::UserActionRequired => RetryDecision::do_not_retry(RetryErrorKind::BadRequest),
        FailureClass::FatalLocal => RetryDecision::do_not_retry(RetryErrorKind::FatalLocal),
    }
}

pub fn classify_failure_with_retry_after(
    error: &(impl ClassifyFailure + RetryAfterSource),
) -> RetryDecision {
    let decision = classify_failure_class(error);
    match (decision, error.retry_after_delay()) {
        (RetryDecision::Retry { kind, .. }, Some(retry_after)) => {
            RetryDecision::retry_after(kind, retry_after)
        }
        _ => decision,
    }
}

impl RetryAfterSource for DatabaseError {
    fn retry_after_delay(&self) -> Option<Duration> {
        self.retry_after_ms().map(duration_from_ms)
    }
}

impl RetryAfterSource for IndexerError {
    fn retry_after_delay(&self) -> Option<Duration> {
        self.retry_after_ms().map(duration_from_ms)
    }
}

impl RetryAfterSource for TorrentClientError {
    fn retry_after_delay(&self) -> Option<Duration> {
        self.retry_after_ms().map(duration_from_ms)
    }
}

pub fn classify_database_error(error: &DatabaseError) -> RetryDecision {
    match error {
        DatabaseError::Busy { retry_after_ms, .. } => {
            retry_with_optional_ms(RetryErrorKind::DatabaseBusy, *retry_after_ms)
        }
        DatabaseError::Unavailable { .. } => RetryDecision::retry(RetryErrorKind::DatabaseBusy),
        DatabaseError::QueryFailed { .. }
        | DatabaseError::IncompleteStream { .. }
        | DatabaseError::SchemaInitialization { .. } => {
            RetryDecision::do_not_retry(RetryErrorKind::FatalLocal)
        }
    }
}

pub fn classify_indexer_error(error: &IndexerError) -> RetryDecision {
    match error {
        IndexerError::RateLimited { retry_after_ms, .. } => {
            retry_with_optional_ms(RetryErrorKind::RateLimited, *retry_after_ms)
        }
        IndexerError::Unavailable { retry_after_ms, .. } => {
            retry_with_optional_ms(RetryErrorKind::TransientNetwork, *retry_after_ms)
        }
        IndexerError::BadResponse { .. } => {
            RetryDecision::do_not_retry(RetryErrorKind::InvalidResponse)
        }
        IndexerError::Unauthorized { .. } => {
            RetryDecision::do_not_retry(RetryErrorKind::Authentication)
        }
    }
}

pub fn classify_torrent_client_error(error: &TorrentClientError) -> RetryDecision {
    match error {
        TorrentClientError::Unavailable { retry_after_ms, .. } => {
            retry_with_optional_ms(RetryErrorKind::TransientNetwork, *retry_after_ms)
        }
        TorrentClientError::BadResponse { .. } | TorrentClientError::ApiChanged { .. } => {
            RetryDecision::do_not_retry(RetryErrorKind::InvalidResponse)
        }
        TorrentClientError::Unauthorized { .. } => {
            RetryDecision::do_not_retry(RetryErrorKind::Authentication)
        }
        TorrentClientError::Cancelled { .. } => {
            RetryDecision::do_not_retry(RetryErrorKind::Cancelled)
        }
        TorrentClientError::UnsupportedCapability { .. } => {
            RetryDecision::do_not_retry(RetryErrorKind::Unsupported)
        }
    }
}

pub fn classify_http_status(
    status: u16,
    retry_after: Option<Duration>,
    idempotent: bool,
) -> RetryDecision {
    if idempotent && matches!(status, 408 | 429 | 502 | 503 | 504) {
        let kind = if status == 429 {
            RetryErrorKind::RateLimited
        } else {
            RetryErrorKind::TransientNetwork
        };
        return retry_with_optional_duration(kind, retry_after);
    }

    match status {
        401 | 403 => RetryDecision::do_not_retry(RetryErrorKind::Authentication),
        400 | 422 => RetryDecision::do_not_retry(RetryErrorKind::BadRequest),
        404 => RetryDecision::do_not_retry(RetryErrorKind::NotFound),
        408 | 429 | 502 | 503 | 504 => RetryDecision::do_not_retry(RetryErrorKind::NonIdempotent),
        _ => RetryDecision::do_not_retry(RetryErrorKind::InvalidResponse),
    }
}

pub fn classify_reqwest_error(error: &reqwest::Error, idempotent: bool) -> RetryDecision {
    if error.is_timeout() {
        return RetryDecision::retry(RetryErrorKind::Timeout);
    }
    if let Some(status) = error.status() {
        return classify_http_status(status.as_u16(), None, idempotent);
    }
    if error.is_builder() {
        return RetryDecision::do_not_retry(RetryErrorKind::BadRequest);
    }
    if error.is_decode() {
        return RetryDecision::do_not_retry(RetryErrorKind::InvalidResponse);
    }
    if error.is_body() {
        return RetryDecision::retry(RetryErrorKind::TransientNetwork);
    }
    if error.is_connect() || error.is_request() {
        return RetryDecision::retry(RetryErrorKind::TransientNetwork);
    }
    RetryDecision::do_not_retry(RetryErrorKind::Unknown)
}

pub fn classify_io_error(error: &io::Error) -> RetryDecision {
    match error.kind() {
        io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut => {
            RetryDecision::retry(RetryErrorKind::TransientLocalIo)
        }
        io::ErrorKind::ConnectionAborted
        | io::ErrorKind::ConnectionRefused
        | io::ErrorKind::ConnectionReset
        | io::ErrorKind::NotConnected
        | io::ErrorKind::AddrInUse
        | io::ErrorKind::AddrNotAvailable
        | io::ErrorKind::BrokenPipe => RetryDecision::retry(RetryErrorKind::TransientNetwork),
        io::ErrorKind::PermissionDenied => {
            RetryDecision::do_not_retry(RetryErrorKind::PermissionDenied)
        }
        io::ErrorKind::NotFound => RetryDecision::do_not_retry(RetryErrorKind::NotFound),
        io::ErrorKind::InvalidInput => RetryDecision::do_not_retry(RetryErrorKind::InvalidInput),
        io::ErrorKind::InvalidData => RetryDecision::do_not_retry(RetryErrorKind::InvalidResponse),
        io::ErrorKind::Unsupported => RetryDecision::do_not_retry(RetryErrorKind::Unsupported),
        _ => RetryDecision::do_not_retry(RetryErrorKind::FatalLocal),
    }
}

pub async fn retry_with_classification<T, E, MakeFuture, Fut, Classify>(
    max_attempts: u8,
    policy: JitteredBackoffPolicy,
    jitter_key: &str,
    shutdown: Option<&ShutdownSignal>,
    mut make_future: MakeFuture,
    mut classify: Classify,
) -> RetryOutcome<Result<T, E>>
where
    MakeFuture: FnMut(u8) -> Fut,
    Fut: Future<Output = Result<T, E>>,
    Classify: FnMut(&E) -> RetryDecision,
{
    let attempts = max_attempts.max(1);
    for attempt in 1..=attempts {
        if shutdown.is_some_and(|signal| signal.state().phase != ShutdownPhase::Running) {
            return RetryOutcome::Shutdown;
        }
        let result = if let Some(shutdown) = shutdown {
            let mut attempt_shutdown = shutdown.clone();
            tokio::select! {
                biased;
                _state = attempt_shutdown.cancelled() => return RetryOutcome::Shutdown,
                result = make_future(attempt) => result,
            }
        } else {
            make_future(attempt).await
        };

        match result {
            Ok(value) => return RetryOutcome::Completed(Ok(value)),
            Err(error) => {
                let decision = classify(&error);
                if !decision.should_retry() || attempt == attempts {
                    return RetryOutcome::Completed(Err(error));
                }

                let delay = retry_delay_after_decision(policy, attempt, jitter_key, decision);
                if delay.is_zero() {
                    continue;
                }
                let Some(shutdown) = shutdown else {
                    tokio::time::sleep(delay).await;
                    continue;
                };
                let mut shutdown = shutdown.clone();
                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => return RetryOutcome::Shutdown,
                    () = tokio::time::sleep(delay) => {}
                }
            }
        }
    }
    RetryOutcome::Exhausted
}

pub async fn retry_transient_io<T, E, MakeFuture, Fut, Classify>(
    jitter_key: &str,
    mut make_future: MakeFuture,
    mut classify: Classify,
) -> Result<T, E>
where
    MakeFuture: FnMut(u8) -> Fut,
    Fut: Future<Output = Result<T, E>>,
    Classify: FnMut(&E) -> RetryDecision,
{
    let attempts = TRANSIENT_IO_RETRY_MAX_ATTEMPTS.max(1);
    let policy = transient_io_retry_policy();
    let mut attempt = 1;
    loop {
        match make_future(attempt).await {
            Ok(value) => return Ok(value),
            Err(error) => {
                let decision = classify(&error);
                if !decision.should_retry() || attempt == attempts {
                    return Err(error);
                }
                let delay = retry_delay_after_decision(policy, attempt, jitter_key, decision);
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
                attempt = attempt.saturating_add(1);
            }
        }
    }
}

pub fn retry_transient_io_blocking<T, E, Operation, Classify>(
    jitter_key: &str,
    mut operation: Operation,
    mut classify: Classify,
) -> Result<T, E>
where
    Operation: FnMut(u8) -> Result<T, E>,
    Classify: FnMut(&E) -> RetryDecision,
{
    let attempts = TRANSIENT_IO_RETRY_MAX_ATTEMPTS.max(1);
    let policy = transient_io_retry_policy();
    let mut attempt = 1;
    loop {
        match operation(attempt) {
            Ok(value) => return Ok(value),
            Err(error) => {
                let decision = classify(&error);
                if !decision.should_retry() || attempt == attempts {
                    return Err(error);
                }
                let delay = retry_delay_after_decision(policy, attempt, jitter_key, decision);
                if !delay.is_zero() {
                    std::thread::sleep(delay);
                }
                attempt = attempt.saturating_add(1);
            }
        }
    }
}

pub async fn retry_with_backoff<T, E, MakeFuture, Fut, ShouldRetry>(
    max_attempts: u8,
    policy: JitteredBackoffPolicy,
    jitter_key: &str,
    shutdown: Option<&ShutdownSignal>,
    mut make_future: MakeFuture,
    mut should_retry: ShouldRetry,
) -> RetryOutcome<Result<T, E>>
where
    MakeFuture: FnMut(u8) -> Fut,
    Fut: Future<Output = Result<T, E>>,
    ShouldRetry: FnMut(&E) -> bool,
{
    let attempts = max_attempts.max(1);
    for attempt in 1..=attempts {
        if shutdown.is_some_and(|signal| signal.state().phase != ShutdownPhase::Running) {
            return RetryOutcome::Shutdown;
        }
        let result = if let Some(shutdown) = shutdown {
            let mut attempt_shutdown = shutdown.clone();
            tokio::select! {
                biased;
                _state = attempt_shutdown.cancelled() => return RetryOutcome::Shutdown,
                result = make_future(attempt) => result,
            }
        } else {
            make_future(attempt).await
        };

        match result {
            Ok(value) => return RetryOutcome::Completed(Ok(value)),
            Err(error) if !should_retry(&error) || attempt == attempts => {
                return RetryOutcome::Completed(Err(error));
            }
            Err(_) => {
                let delay = retry_delay_after_attempt(policy, attempt, jitter_key);
                if delay.is_zero() {
                    continue;
                }
                let Some(shutdown) = shutdown else {
                    tokio::time::sleep(delay).await;
                    continue;
                };
                let mut shutdown = shutdown.clone();
                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => return RetryOutcome::Shutdown,
                    () = tokio::time::sleep(delay) => {}
                }
            }
        }
    }
    RetryOutcome::Exhausted
}

fn retry_delay_after_attempt(
    policy: JitteredBackoffPolicy,
    failed_attempt: u8,
    jitter_key: &str,
) -> Duration {
    Duration::from_millis(
        u64::try_from(
            policy
                .delay_ms(u16::from(failed_attempt.saturating_sub(1)), jitter_key)
                .max(0),
        )
        .unwrap_or_default(),
    )
}

fn retry_delay_after_decision(
    policy: JitteredBackoffPolicy,
    failed_attempt: u8,
    jitter_key: &str,
    decision: RetryDecision,
) -> Duration {
    decision
        .explicit_delay()
        .map(|delay| delay.min(duration_from_ms(policy.max_delay_ms)))
        .unwrap_or_else(|| retry_delay_after_attempt(policy, failed_attempt, jitter_key))
}

fn retry_with_optional_ms(kind: RetryErrorKind, retry_after_ms: Option<i64>) -> RetryDecision {
    retry_with_optional_duration(kind, retry_after_ms.map(duration_from_ms))
}

fn retry_with_optional_duration(
    kind: RetryErrorKind,
    retry_after: Option<Duration>,
) -> RetryDecision {
    RetryDecision::Retry { kind, retry_after }
}

fn duration_from_ms(milliseconds: i64) -> Duration {
    Duration::from_millis(u64::try_from(milliseconds.max(0)).unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::shutdown::shutdown_channel;

    #[test]
    fn jittered_backoff_honors_retry_after_jitter_and_cap() {
        let policy = JitteredBackoffPolicy {
            base_delay_ms: 1_000,
            max_delay_ms: 10_000,
            jitter_ms: 100,
        };

        assert_eq!(
            6_000,
            policy.retry_deadline_ms(1_000, 3, Some(6_000), "main")
        );
        let main = policy.retry_deadline_ms(1_000, 3, None, "main");
        let backup = policy.retry_deadline_ms(1_000, 3, None, "backup");

        assert_ne!(main, backup);
        assert!((9_000..=9_100).contains(&main));
        assert!((9_000..=9_100).contains(&backup));
        assert_eq!(11_000, policy.retry_deadline_ms(1_000, 20, None, "main"));
    }

    #[test]
    fn probe_policy_preserves_explicit_retry_after() {
        let policy = BackoffProbePolicy {
            retry: JitteredBackoffPolicy {
                base_delay_ms: 1_000,
                max_delay_ms: 10_000,
                jitter_ms: 0,
            },
            recovery_probe_interval_ms: 500,
        };

        assert!(!policy.should_probe(1_000, Some(2_000), None, true));
        assert!(policy.should_probe(1_000, Some(2_000), None, false));
        assert!(!policy.should_probe(1_000, Some(2_000), Some(750), false));
        assert!(policy.should_probe(1_000, Some(2_000), Some(400), false));
        assert!(policy.should_probe(2_001, Some(2_000), Some(1_999), true));
    }

    #[test]
    fn fixed_retry_deadline_prefers_future_explicit_deadline() {
        assert_eq!(5_000, fixed_retry_deadline_ms(1_000, 250, Some(5_000)));
        assert_eq!(1_250, fixed_retry_deadline_ms(1_000, 250, Some(999)));
        assert_eq!(1_001, fixed_retry_deadline_ms(1_000, 0, None));
    }

    #[test]
    fn typed_classifiers_preserve_retryability_and_retry_after() {
        let database_busy = DatabaseError::Busy {
            operation: "claim work".to_owned(),
            retry_after_ms: Some(250),
        };
        assert_eq!(
            RetryDecision::retry_after(RetryErrorKind::DatabaseBusy, Duration::from_millis(250)),
            classify_database_error(&database_busy)
        );

        let database_failed = DatabaseError::QueryFailed {
            operation: "read work".to_owned(),
            message: "syntax error".to_owned(),
        };
        assert_eq!(
            RetryDecision::do_not_retry(RetryErrorKind::FatalLocal),
            classify_database_error(&database_failed)
        );

        let client_unavailable = TorrentClientError::Unavailable {
            client: "qbit_main".to_owned(),
            retry_after_ms: Some(500),
            message: "connection reset".to_owned(),
        };
        assert_eq!(
            RetryDecision::retry_after(
                RetryErrorKind::TransientNetwork,
                Duration::from_millis(500)
            ),
            classify_torrent_client_error(&client_unavailable)
        );

        let client_auth = TorrentClientError::Unauthorized {
            client: "qbit_main".to_owned(),
        };
        assert_eq!(
            RetryDecision::do_not_retry(RetryErrorKind::Authentication),
            classify_torrent_client_error(&client_auth)
        );
    }

    #[test]
    fn generic_failure_classifier_preserves_retry_after_when_available() {
        let indexer_rate_limited = IndexerError::RateLimited {
            indexer: "main".to_owned(),
            retry_after_ms: Some(750),
        };

        assert_eq!(
            RetryDecision::retry_after(
                RetryErrorKind::DependencyBackoff,
                Duration::from_millis(750)
            ),
            classify_failure_with_retry_after(&indexer_rate_limited)
        );

        let indexer_bad_response = IndexerError::BadResponse {
            indexer: "main".to_owned(),
            message: "missing channel".to_owned(),
        };
        assert_eq!(
            RetryDecision::do_not_retry(RetryErrorKind::InvalidResponse),
            classify_failure_with_retry_after(&indexer_bad_response)
        );
    }

    #[test]
    fn http_status_classifier_separates_idempotent_and_mutation_retry() {
        assert_eq!(
            RetryDecision::retry_after(RetryErrorKind::RateLimited, Duration::from_millis(100)),
            classify_http_status(429, Some(Duration::from_millis(100)), true)
        );
        assert_eq!(
            RetryDecision::do_not_retry(RetryErrorKind::NonIdempotent),
            classify_http_status(503, None, false)
        );
        assert_eq!(
            RetryDecision::do_not_retry(RetryErrorKind::Authentication),
            classify_http_status(401, None, true)
        );
    }

    #[test]
    fn io_classifier_distinguishes_transient_and_terminal_failures() {
        assert_eq!(
            RetryDecision::retry(RetryErrorKind::TransientLocalIo),
            classify_io_error(&io::Error::from(io::ErrorKind::WouldBlock))
        );
        assert_eq!(
            RetryDecision::retry(RetryErrorKind::TransientNetwork),
            classify_io_error(&io::Error::from(io::ErrorKind::ConnectionReset))
        );
        assert_eq!(
            RetryDecision::do_not_retry(RetryErrorKind::PermissionDenied),
            classify_io_error(&io::Error::from(io::ErrorKind::PermissionDenied))
        );
    }

    #[test]
    fn retry_delay_after_decision_prefers_explicit_retry_after_and_caps() {
        let policy = JitteredBackoffPolicy {
            base_delay_ms: 50,
            max_delay_ms: 100,
            jitter_ms: 0,
        };

        assert_eq!(
            Duration::from_millis(100),
            retry_delay_after_decision(
                policy,
                1,
                "main",
                RetryDecision::retry_after(RetryErrorKind::RateLimited, Duration::from_millis(250)),
            )
        );
        assert_eq!(
            Duration::from_millis(50),
            retry_delay_after_decision(
                policy,
                1,
                "main",
                RetryDecision::retry(RetryErrorKind::TransientNetwork),
            )
        );
    }

    #[tokio::test]
    async fn retry_with_backoff_stops_on_shutdown_sleep() {
        let (controller, signal) = shutdown_channel();
        controller.cancel_now("test shutdown").unwrap();
        let policy = JitteredBackoffPolicy {
            base_delay_ms: 1_000,
            max_delay_ms: 1_000,
            jitter_ms: 0,
        };

        let result = retry_with_backoff(
            3,
            policy,
            "test",
            Some(&signal),
            |_attempt| async { Err::<(), _>("retry") },
            |_| true,
        )
        .await;

        assert_eq!(RetryOutcome::Shutdown, result);
    }

    #[tokio::test]
    async fn retry_with_backoff_stops_in_flight_attempt_on_shutdown() {
        let (controller, signal) = shutdown_channel();
        let (attempt_started, attempt_observed) = tokio::sync::oneshot::channel();
        let attempt_started = std::sync::Arc::new(std::sync::Mutex::new(Some(attempt_started)));
        let policy = JitteredBackoffPolicy {
            base_delay_ms: 1_000,
            max_delay_ms: 1_000,
            jitter_ms: 0,
        };

        let handle = tokio::spawn({
            let attempt_started = std::sync::Arc::clone(&attempt_started);
            async move {
                retry_with_backoff(
                    3,
                    policy,
                    "test",
                    Some(&signal),
                    move |_attempt| {
                        let attempt_started = std::sync::Arc::clone(&attempt_started);
                        async move {
                            if let Some(sender) = attempt_started
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner())
                                .take()
                            {
                                match sender.send(()) {
                                    Ok(()) | Err(()) => {}
                                }
                            }
                            std::future::pending::<Result<(), &str>>().await
                        }
                    },
                    |_| true,
                )
                .await
            }
        });

        attempt_observed.await.unwrap();
        controller.cancel_now("test shutdown").unwrap();
        let result = tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(RetryOutcome::Shutdown, result);
    }

    #[tokio::test]
    async fn retry_with_classification_retries_transient_and_stops_on_terminal() {
        let policy = JitteredBackoffPolicy {
            base_delay_ms: 0,
            max_delay_ms: 0,
            jitter_ms: 0,
        };
        let mut attempts = Vec::new();
        let result = retry_with_classification(
            3,
            policy,
            "test",
            None,
            |attempt| {
                attempts.push(attempt);
                async move {
                    if attempt == 1 {
                        Err::<u8, _>(RetryDecision::retry(RetryErrorKind::TransientNetwork))
                    } else {
                        Ok(42)
                    }
                }
            },
            |decision| *decision,
        )
        .await;

        assert_eq!(RetryOutcome::Completed(Ok(42)), result);
        assert_eq!(vec![1, 2], attempts);

        let mut terminal_attempts = Vec::new();
        let terminal = retry_with_classification(
            3,
            policy,
            "test",
            None,
            |attempt| {
                terminal_attempts.push(attempt);
                async { Err::<(), _>(RetryDecision::do_not_retry(RetryErrorKind::Authentication)) }
            },
            |decision| *decision,
        )
        .await;

        assert_eq!(
            RetryOutcome::Completed(Err(RetryDecision::do_not_retry(
                RetryErrorKind::Authentication
            ))),
            terminal
        );
        assert_eq!(vec![1], terminal_attempts);
    }

    #[tokio::test]
    async fn retry_with_classification_stops_during_retry_sleep() {
        let (controller, signal) = shutdown_channel();
        let (attempt_started, attempt_observed) = tokio::sync::oneshot::channel();
        let attempt_started = std::sync::Arc::new(std::sync::Mutex::new(Some(attempt_started)));
        let policy = JitteredBackoffPolicy {
            base_delay_ms: 10_000,
            max_delay_ms: 10_000,
            jitter_ms: 0,
        };

        let handle = tokio::spawn({
            let attempt_started = std::sync::Arc::clone(&attempt_started);
            async move {
                retry_with_classification(
                    3,
                    policy,
                    "test",
                    Some(&signal),
                    move |_attempt| {
                        let attempt_started = std::sync::Arc::clone(&attempt_started);
                        async move {
                            if let Some(sender) = attempt_started
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner())
                                .take()
                            {
                                match sender.send(()) {
                                    Ok(()) | Err(()) => {}
                                }
                            }
                            Err::<(), _>(RetryDecision::retry(RetryErrorKind::TransientNetwork))
                        }
                    },
                    |decision| *decision,
                )
                .await
            }
        });

        attempt_observed.await.unwrap();
        controller.cancel_now("test shutdown").unwrap();
        let result = tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(RetryOutcome::Shutdown, result);
    }

    #[test]
    fn retry_transient_io_blocking_retries_transient_failures() {
        let mut attempts = Vec::new();
        let result = retry_transient_io_blocking(
            "blocking-test",
            |attempt| {
                attempts.push(attempt);
                if attempt == 1 {
                    Err::<u8, _>(io::Error::from(io::ErrorKind::WouldBlock))
                } else {
                    Ok(42)
                }
            },
            |_| RetryDecision::retry_after(RetryErrorKind::TransientLocalIo, Duration::ZERO),
        );

        assert_eq!(42, result.unwrap());
        assert_eq!(vec![1, 2], attempts);
    }

    #[test]
    fn retry_transient_io_blocking_preserves_terminal_failures() {
        let mut attempts = Vec::new();
        let result = retry_transient_io_blocking(
            "blocking-test",
            |attempt| {
                attempts.push(attempt);
                Err::<(), _>(io::Error::from(io::ErrorKind::PermissionDenied))
            },
            classify_io_error,
        );

        assert_eq!(io::ErrorKind::PermissionDenied, result.unwrap_err().kind());
        assert_eq!(vec![1], attempts);
    }

    #[tokio::test]
    async fn retry_with_classification_stops_in_flight_attempt_on_shutdown() {
        let (controller, signal) = shutdown_channel();
        let (attempt_started, attempt_observed) = tokio::sync::oneshot::channel();
        let attempt_started = std::sync::Arc::new(std::sync::Mutex::new(Some(attempt_started)));
        let policy = JitteredBackoffPolicy {
            base_delay_ms: 1_000,
            max_delay_ms: 1_000,
            jitter_ms: 0,
        };

        let handle = tokio::spawn({
            let attempt_started = std::sync::Arc::clone(&attempt_started);
            async move {
                retry_with_classification(
                    3,
                    policy,
                    "test",
                    Some(&signal),
                    move |_attempt| {
                        let attempt_started = std::sync::Arc::clone(&attempt_started);
                        async move {
                            if let Some(sender) = attempt_started
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner())
                                .take()
                            {
                                match sender.send(()) {
                                    Ok(()) | Err(()) => {}
                                }
                            }
                            std::future::pending::<Result<(), RetryDecision>>().await
                        }
                    },
                    |decision| *decision,
                )
                .await
            }
        });

        attempt_observed.await.unwrap();
        controller.cancel_now("test shutdown").unwrap();
        let result = tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(RetryOutcome::Shutdown, result);
    }

    #[test]
    fn retry_delay_after_first_failure_uses_base_delay() {
        let policy = JitteredBackoffPolicy {
            base_delay_ms: 50,
            max_delay_ms: 1_000,
            jitter_ms: 0,
        };

        assert_eq!(
            Duration::from_millis(50),
            retry_delay_after_attempt(policy, 1, "notification-main")
        );
        assert_eq!(
            Duration::from_millis(100),
            retry_delay_after_attempt(policy, 2, "notification-main")
        );
    }
}
