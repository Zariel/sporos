// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Dispatcher implementations for Runtime
//!
//! This module contains the dispatcher logic split into separate concerns:
//! - `orchestration`: Orchestration dispatcher that processes orchestration turns
//! - `worker`: Worker dispatcher that executes activities

mod orchestration;
mod worker;

use std::time::Duration;

const PROVIDER_RETRY_JITTER_PERCENT: u8 = 20;

fn provider_retry_delay(base: Duration, max: Duration, attempt: u32, key: &[&[u8]]) -> Duration {
    let factor = 1_u32.checked_shl(attempt.saturating_sub(1).min(31)).unwrap_or(u32::MAX);
    let delay = base.saturating_mul(factor).min(max);
    jitter_delay(delay, attempt, key)
}

fn jitter_delay(delay: Duration, attempt: u32, key: &[&[u8]]) -> Duration {
    crate::jitter_delay(delay, PROVIDER_RETRY_JITTER_PERCENT, key, attempt)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_retry_is_bounded_and_separated() {
        let base = Duration::from_millis(100);
        let max = Duration::from_secs(3);
        let first = provider_retry_delay(base, max, 1, &[b"worker-a", b"fetch"]);

        assert!((Duration::from_millis(80)..=base).contains(&first));
        assert_ne!(first, provider_retry_delay(base, max, 1, &[b"worker-b", b"fetch"]));
        assert!((Duration::from_millis(2_400)..=max).contains(&provider_retry_delay(
            base,
            max,
            100,
            &[b"worker-a", b"fetch"]
        )));
    }
}
