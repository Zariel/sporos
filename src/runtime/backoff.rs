#![cfg_attr(
    test,
    expect(
        clippy::let_underscore_must_use,
        reason = "test synchronization sends are best-effort and tracked for cleanup"
    )
)]

use std::future::Future;
use std::time::Duration;

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
                                let _ = sender.send(());
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
