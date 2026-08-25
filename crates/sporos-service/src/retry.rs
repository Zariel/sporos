use std::time::{Duration, SystemTime, UNIX_EPOCH};

const JITTER_PERCENT: u8 = 20;

pub(crate) struct Backoff {
    base: Duration,
    max: Duration,
    failures: u32,
    seed: u64,
}

impl Backoff {
    pub(crate) fn new(base: Duration, max: Duration) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let seed =
            u64::try_from(now.as_nanos()).unwrap_or(u64::MAX) ^ u64::from(std::process::id());
        Self::with_seed(base, max, seed)
    }

    fn with_seed(base: Duration, max: Duration, seed: u64) -> Self {
        assert!(!base.is_zero(), "backoff base must be positive");
        assert!(max >= base, "backoff maximum must not be below its base");
        Self {
            base,
            max,
            failures: 0,
            seed,
        }
    }

    pub(crate) fn fail(&mut self) -> Duration {
        self.failures = self.failures.saturating_add(1);
        deterministic(
            self.base,
            self.max,
            self.failures,
            &[&self.seed.to_le_bytes()],
        )
    }

    pub(crate) fn reset(&mut self) {
        self.failures = 0;
    }

    pub(crate) fn failures(&self) -> u32 {
        self.failures
    }
}

pub(crate) fn deterministic(
    base: Duration,
    max: Duration,
    attempt: u32,
    key: &[&[u8]],
) -> Duration {
    let factor = 1_u32
        .checked_shl(attempt.saturating_sub(1).min(31))
        .unwrap_or(u32::MAX);
    let delay = base.saturating_mul(factor).min(max);
    let mut seed = 0xcbf29ce484222325_u64;
    for part in key {
        for byte in part.iter().chain([0].iter()) {
            seed ^= u64::from(*byte);
            seed = seed.wrapping_mul(0x100000001b3);
        }
    }
    let random = mix(seed ^ u64::from(attempt));
    let delay_nanos = delay.as_nanos();
    let window = delay_nanos.saturating_mul(u128::from(JITTER_PERCENT)) / 100;
    let offset = window.saturating_mul(u128::from(random)) / u128::from(u64::MAX);
    let jittered = delay_nanos - window + offset;
    let seconds =
        u64::try_from(jittered / 1_000_000_000).expect("jitter cannot increase a duration");
    let nanoseconds =
        u32::try_from(jittered % 1_000_000_000).expect("nanosecond remainder fits u32");
    Duration::new(seconds, nanoseconds)
}

fn mix(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e3779b97f4a7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grows_resets_and_caps_with_bounded_jitter() {
        let mut backoff = Backoff::with_seed(Duration::from_secs(1), Duration::from_secs(5), 17);

        assert!((Duration::from_millis(800)..=Duration::from_secs(1)).contains(&backoff.fail()));
        assert!((Duration::from_millis(1_600)..=Duration::from_secs(2)).contains(&backoff.fail()));
        assert!((Duration::from_millis(3_200)..=Duration::from_secs(4)).contains(&backoff.fail()));
        for _ in 0..10 {
            assert!((Duration::from_secs(4)..=Duration::from_secs(5)).contains(&backoff.fail()));
        }
        backoff.reset();
        assert_eq!(backoff.failures(), 0);
        assert!((Duration::from_millis(800)..=Duration::from_secs(1)).contains(&backoff.fail()));
    }

    #[test]
    fn separates_retry_schedules() {
        let mut first = Backoff::with_seed(Duration::from_secs(1), Duration::from_secs(5), 1);
        let mut replay = Backoff::with_seed(Duration::from_secs(1), Duration::from_secs(5), 1);
        let mut other = Backoff::with_seed(Duration::from_secs(1), Duration::from_secs(5), 2);

        assert_eq!(first.fail(), replay.fail());
        assert_ne!(first.fail(), other.fail());
    }
}
