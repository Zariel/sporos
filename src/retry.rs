//! Shared bounded retry helpers for safe external IO.

use std::{
    borrow::Cow,
    future::Future,
    time::{Duration, Instant},
};

use reqwest::{StatusCode, header::HeaderMap};
use tokio_util::sync::CancellationToken;

/// Whether an operation is safe to retry automatically.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum RetrySafety {
    /// Do not retry after the first attempt.
    Unsafe,
    /// The caller has verified that retrying is idempotent or explicitly safe.
    Safe,
}

/// Bounded retry policy for one external IO operation.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RetryPolicy {
    /// Maximum attempts including the initial attempt.
    pub max_attempts: u32,
    /// Maximum total elapsed retry window.
    pub max_elapsed: Duration,
    /// Initial delay before the first retry.
    pub base_delay: Duration,
    /// Maximum computed backoff delay.
    pub max_delay: Duration,
    /// Maximum random additive jitter.
    pub jitter: Duration,
    /// Whether the operation is safe to retry.
    pub safety: RetrySafety,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 1,
            max_elapsed: Duration::ZERO,
            base_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
            jitter: Duration::ZERO,
            safety: RetrySafety::Unsafe,
        }
    }
}

impl RetryPolicy {
    /// Conservative default for idempotent external reads and status checks.
    pub const fn idempotent() -> Self {
        Self {
            max_attempts: 3,
            max_elapsed: Duration::from_secs(30),
            base_delay: Duration::from_millis(250),
            max_delay: Duration::from_secs(5),
            jitter: Duration::from_millis(100),
            safety: RetrySafety::Safe,
        }
    }

    /// Return a copy with a caller-specific attempt bound.
    pub const fn with_max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    /// Return a copy with a caller-specific elapsed-time bound.
    pub const fn with_max_elapsed(mut self, max_elapsed: Duration) -> Self {
        self.max_elapsed = max_elapsed;
        self
    }

    /// Return a copy with caller-specific backoff bounds.
    pub const fn with_backoff(
        mut self,
        base_delay: Duration,
        max_delay: Duration,
        jitter: Duration,
    ) -> Self {
        self.base_delay = base_delay;
        self.max_delay = max_delay;
        self.jitter = jitter;
        self
    }

    fn allows_retry(&self) -> bool {
        self.safety == RetrySafety::Safe && self.max_attempts > 1
    }

    fn delay(&self, retry_number: u32) -> Duration {
        let exponent = retry_number.saturating_sub(1).min(31);
        let multiplier = 1_u32.checked_shl(exponent).unwrap_or(u32::MAX);
        let backoff = self.base_delay.saturating_mul(multiplier);
        let capped = backoff.min(self.max_delay);
        capped
            .saturating_add(random_jitter(self.jitter))
            .min(self.max_delay)
    }

    /// Compute the bounded policy delay for a one-based retry number.
    pub fn delay_for_retry(&self, retry_number: u32) -> Duration {
        self.delay(retry_number)
    }
}

/// Non-secret tracing context for retry attempts.
#[derive(Debug, Clone)]
pub struct RetryContext {
    /// Operation name, such as `torznab_search`.
    pub operation: &'static str,
    /// Sanitized target name or host. Do not pass API keys or full secret URLs.
    pub target: Option<Cow<'static, str>>,
    /// Cancellation token owned by runtime orchestration.
    pub cancellation: CancellationToken,
}

impl RetryContext {
    /// Build context for an operation without a public target label.
    pub fn new(operation: &'static str, cancellation: CancellationToken) -> Self {
        Self {
            operation,
            target: None,
            cancellation,
        }
    }

    /// Add a sanitized target label.
    pub fn with_target(mut self, target: impl Into<Cow<'static, str>>) -> Self {
        self.target = Some(target.into());
        self
    }
}

/// One retry attempt passed to the operation closure.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct RetryAttempt {
    /// One-based attempt number.
    pub number: u32,
    /// Maximum attempts allowed by the policy.
    pub max: u32,
}

/// Operation result that tells the retry helper whether another attempt is safe.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum RetryDecision<T, E> {
    /// The operation succeeded.
    Success(T),
    /// The operation failed and must not be retried.
    Fatal(E),
    /// The operation failed with a retryable condition.
    Retryable {
        /// Original error or status context.
        error: E,
        /// Server-specified retry delay, when present.
        retry_after: Option<Duration>,
    },
}

/// Retry helper failure.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum RetryError<E> {
    /// Cancellation was requested before another attempt could run.
    Cancelled {
        /// Attempts completed before cancellation.
        attempts: u32,
    },
    /// The operation failed with a non-retryable condition.
    Fatal {
        /// Attempts completed.
        attempts: u32,
        /// Original failure context.
        error: E,
    },
    /// Attempts or elapsed-time bounds were exhausted.
    Exhausted {
        /// Attempts completed.
        attempts: u32,
        /// Last retryable failure context.
        error: E,
    },
}

impl<E: std::fmt::Display> std::fmt::Display for RetryError<E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cancelled { attempts } => {
                write!(
                    formatter,
                    "retry operation cancelled after {attempts} attempts"
                )
            }
            Self::Fatal { attempts, error } => {
                write!(
                    formatter,
                    "retry operation failed after {attempts} attempts: {error}"
                )
            }
            Self::Exhausted { attempts, error } => {
                write!(
                    formatter,
                    "retry operation exhausted after {attempts} attempts: {error}"
                )
            }
        }
    }
}

impl<E> std::error::Error for RetryError<E> where E: std::error::Error + 'static {}

/// Run an operation with bounded retry/backoff.
pub async fn retry<T, E, F, Fut>(
    policy: RetryPolicy,
    context: RetryContext,
    mut operation: F,
) -> Result<T, RetryError<E>>
where
    F: FnMut(RetryAttempt) -> Fut,
    Fut: Future<Output = RetryDecision<T, E>>,
{
    let started_at = Instant::now();
    let max_attempts = policy.max_attempts.max(1);
    let mut attempt: u32 = 1;
    loop {
        if context.cancellation.is_cancelled() {
            return Err(RetryError::Cancelled {
                attempts: attempt.saturating_sub(1),
            });
        }
        match operation(RetryAttempt {
            number: attempt,
            max: max_attempts,
        })
        .await
        {
            RetryDecision::Success(value) => return Ok(value),
            RetryDecision::Fatal(error) => {
                return Err(RetryError::Fatal {
                    attempts: attempt,
                    error,
                });
            }
            RetryDecision::Retryable { error, retry_after } => {
                if !policy.allows_retry() || attempt >= max_attempts {
                    return Err(RetryError::Exhausted {
                        attempts: attempt,
                        error,
                    });
                }
                let retry_number = attempt;
                let delay = retry_after.unwrap_or_else(|| policy.delay(retry_number));
                if !within_elapsed(started_at, policy.max_elapsed, delay) {
                    return Err(RetryError::Exhausted {
                        attempts: attempt,
                        error,
                    });
                }
                tracing::debug!(
                    operation = context.operation,
                    target = context.target.as_deref(),
                    attempt,
                    max_attempts,
                    delay_ms = delay.as_millis(),
                    retry_after_ms = retry_after.map(|duration| duration.as_millis()),
                    "retrying external operation",
                );
                if !delay.is_zero() {
                    tokio::select! {
                        () = context.cancellation.cancelled() => {
                            return Err(RetryError::Cancelled { attempts: attempt });
                        }
                        () = tokio::time::sleep(delay) => {}
                    }
                }
                attempt = attempt.saturating_add(1);
            }
        }
    }
}

/// Retry classification for HTTP status and transport failures.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum RetryClass {
    /// The failure is safe to retry.
    Retryable {
        /// Server-requested delay, when provided.
        retry_after: Option<Duration>,
    },
    /// The failure should be surfaced immediately.
    Fatal,
}

/// Classify an HTTP status code from a safe request.
pub fn classify_http_status(
    status: StatusCode,
    headers: &HeaderMap,
    now_millis: u64,
) -> RetryClass {
    if is_retryable_status(status) {
        RetryClass::Retryable {
            retry_after: retry_after(headers, now_millis),
        }
    } else {
        RetryClass::Fatal
    }
}

/// Return whether a status commonly represents a transient server condition.
pub fn is_retryable_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::REQUEST_TIMEOUT
            | StatusCode::TOO_MANY_REQUESTS
            | StatusCode::INTERNAL_SERVER_ERROR
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT
    )
}

/// Classify a reqwest transport/status error from a safe request.
pub fn classify_reqwest_error(error: &reqwest::Error) -> RetryClass {
    if error.is_timeout() || error.is_connect() {
        return RetryClass::Retryable { retry_after: None };
    }
    if error.status().is_some_and(is_retryable_status) {
        return RetryClass::Retryable { retry_after: None };
    }
    RetryClass::Fatal
}

/// Parse a `Retry-After` header value as a delay.
pub fn retry_after(headers: &HeaderMap, now_millis: u64) -> Option<Duration> {
    headers
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| parse_retry_after(value, now_millis))
}

/// Parse `Retry-After` seconds or an HTTP-date value as a delay.
pub fn parse_retry_after(value: &str, now_millis: u64) -> Option<Duration> {
    if let Ok(seconds) = value.trim().parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    parse_http_date_millis(value)
        .map(|retry_at| Duration::from_millis(retry_at.saturating_sub(now_millis)))
}

fn within_elapsed(started_at: Instant, max_elapsed: Duration, delay: Duration) -> bool {
    max_elapsed.is_zero() || started_at.elapsed().saturating_add(delay) <= max_elapsed
}

fn random_jitter(bound: Duration) -> Duration {
    let max_millis = duration_millis(bound);
    if max_millis == 0 {
        return Duration::ZERO;
    }
    let mut bytes = [0_u8; 8];
    if getrandom::fill(&mut bytes).is_err() {
        return Duration::ZERO;
    }
    let value = u64::from_ne_bytes(bytes) % max_millis.saturating_add(1);
    Duration::from_millis(value)
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn parse_http_date_millis(value: &str) -> Option<u64> {
    let parts = value.split_whitespace().collect::<Vec<_>>();
    let start = if parts.first().is_some_and(|part| part.ends_with(',')) {
        1
    } else {
        0
    };
    let day = parts.get(start)?.parse::<i32>().ok()?;
    let month = month_number(parts.get(start + 1)?)?;
    let year = parts.get(start + 2)?.parse::<i32>().ok()?;
    let time = parts.get(start + 3)?;
    let zone = parts.get(start + 4).copied().unwrap_or("GMT");
    if !matches!(zone, "GMT" | "UTC" | "UT") {
        return None;
    }
    let mut time_parts = time.split(':');
    let hour = time_parts.next()?.parse::<i64>().ok()?;
    let minute = time_parts.next()?.parse::<i64>().ok()?;
    let second = time_parts.next()?.parse::<i64>().ok()?;
    let days = days_from_civil(year, month, day);
    let seconds = days
        .saturating_mul(86_400)
        .saturating_add(hour.saturating_mul(3600))
        .saturating_add(minute.saturating_mul(60))
        .saturating_add(second);
    u64::try_from(seconds)
        .ok()
        .map(|seconds| seconds.saturating_mul(1000))
}

fn month_number(value: &str) -> Option<i32> {
    match value {
        "Jan" => Some(1),
        "Feb" => Some(2),
        "Mar" => Some(3),
        "Apr" => Some(4),
        "May" => Some(5),
        "Jun" => Some(6),
        "Jul" => Some(7),
        "Aug" => Some(8),
        "Sep" => Some(9),
        "Oct" => Some(10),
        "Nov" => Some(11),
        "Dec" => Some(12),
        _ => None,
    }
}

fn days_from_civil(year: i32, month: i32, day: i32) -> i64 {
    let year = year - i32::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let month_adjusted = month + if month > 2 { -3 } else { 9 };
    let doy = (153 * month_adjusted + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    i64::from(era * 146_097 + doe - 719_468)
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicU32, Ordering},
        },
        time::Duration,
    };

    use reqwest::{StatusCode, header::HeaderMap};
    use tokio_util::sync::CancellationToken;

    use super::{
        RetryClass, RetryContext, RetryDecision, RetryError, RetryPolicy, classify_http_status,
        is_retryable_status, parse_retry_after, retry,
    };

    #[test]
    fn parses_retry_after_seconds_and_http_dates() {
        assert_eq!(parse_retry_after("2", 1_000), Some(Duration::from_secs(2)));
        assert_eq!(
            parse_retry_after("Thu, 01 Jan 1970 00:00:04 GMT", 1_000),
            Some(Duration::from_secs(3))
        );
        assert_eq!(
            parse_retry_after("Thu, 01 Jan 1970 00:00:00 GMT", 1_000),
            Some(Duration::ZERO)
        );
    }

    #[test]
    fn classifies_retryable_statuses() {
        assert!(is_retryable_status(StatusCode::TOO_MANY_REQUESTS));
        assert!(is_retryable_status(StatusCode::BAD_GATEWAY));
        assert!(!is_retryable_status(StatusCode::BAD_REQUEST));
        assert!(!is_retryable_status(StatusCode::UNAUTHORIZED));
    }

    #[test]
    fn classifies_status_retry_after() {
        let mut headers = HeaderMap::new();
        headers.insert(reqwest::header::RETRY_AFTER, "4".parse().expect("header"));

        assert_eq!(
            classify_http_status(StatusCode::TOO_MANY_REQUESTS, &headers, 1_000),
            RetryClass::Retryable {
                retry_after: Some(Duration::from_secs(4))
            }
        );
        assert_eq!(
            classify_http_status(StatusCode::NOT_FOUND, &headers, 1_000),
            RetryClass::Fatal
        );
    }

    #[tokio::test]
    async fn default_policy_does_not_retry() {
        let calls = Arc::new(AtomicU32::new(0));
        let observed = Arc::clone(&calls);
        let result = retry(
            RetryPolicy::default(),
            RetryContext::new("test", CancellationToken::new()),
            move |_attempt| {
                let observed = Arc::clone(&observed);
                async move {
                    observed.fetch_add(1, Ordering::Relaxed);
                    RetryDecision::<(), _>::Retryable {
                        error: "temporary",
                        retry_after: None,
                    }
                }
            },
        )
        .await;

        assert_eq!(
            result,
            Err(RetryError::Exhausted {
                attempts: 1,
                error: "temporary"
            })
        );
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn safe_policy_retries_until_success() {
        let calls = Arc::new(AtomicU32::new(0));
        let observed = Arc::clone(&calls);
        let policy = RetryPolicy::idempotent().with_max_attempts(3).with_backoff(
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
        );

        let result = retry(
            policy,
            RetryContext::new("test", CancellationToken::new()),
            move |_attempt| {
                let observed = Arc::clone(&observed);
                async move {
                    let call = observed.fetch_add(1, Ordering::Relaxed);
                    if call == 1 {
                        RetryDecision::Success("ok")
                    } else {
                        RetryDecision::Retryable {
                            error: "temporary",
                            retry_after: None,
                        }
                    }
                }
            },
        )
        .await;

        assert_eq!(result, Ok("ok"));
        assert_eq!(calls.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn retry_stops_when_elapsed_window_is_exhausted() {
        let policy = RetryPolicy::idempotent()
            .with_max_attempts(3)
            .with_max_elapsed(Duration::from_millis(1))
            .with_backoff(
                Duration::from_secs(1),
                Duration::from_secs(1),
                Duration::ZERO,
            );

        let result = retry(
            policy,
            RetryContext::new("test", CancellationToken::new()),
            |_attempt| async {
                RetryDecision::<(), _>::Retryable {
                    error: "temporary",
                    retry_after: None,
                }
            },
        )
        .await;

        assert_eq!(
            result,
            Err(RetryError::Exhausted {
                attempts: 1,
                error: "temporary"
            })
        );
    }

    #[tokio::test]
    async fn retry_sleep_honors_cancellation() {
        let cancellation = CancellationToken::new();
        let policy = RetryPolicy::idempotent()
            .with_max_attempts(3)
            .with_max_elapsed(Duration::ZERO)
            .with_backoff(
                Duration::from_secs(60),
                Duration::from_secs(60),
                Duration::ZERO,
            );
        let cancel_after_first = cancellation.clone();

        let result = retry(
            policy,
            RetryContext::new("test", cancellation),
            move |_attempt| {
                let cancel_after_first = cancel_after_first.clone();
                async move {
                    cancel_after_first.cancel();
                    RetryDecision::<(), _>::Retryable {
                        error: "temporary",
                        retry_after: None,
                    }
                }
            },
        )
        .await;

        assert_eq!(result, Err(RetryError::Cancelled { attempts: 1 }));
    }
}
