//! Retry delays for transactions that conflict.

use std::time::Duration;

/// Largest delay for a retry: `base * 2^attempt`, limited to `cap`.
pub fn backoff_window(attempt: u32, base: Duration, cap: Duration) -> Duration {
    let factor = 1u32.checked_shl(attempt).unwrap_or(u32::MAX);
    base.saturating_mul(factor).min(cap)
}

/// Delay before a retry, with full jitter: a uniform random time in
/// `0..=backoff_window(attempt, base, cap)`.
///
/// Transactions that abort together have the same attempt number. With a
/// fixed delay they retry together and conflict again. A random delay
/// spreads them over the window.
pub fn full_jitter_backoff(attempt: u32, base: Duration, cap: Duration) -> Duration {
    let window_us = u64::try_from(backoff_window(attempt, base, cap).as_micros()).unwrap_or(u64::MAX);
    if window_us == 0 {
        return Duration::ZERO;
    }
    let span = window_us.saturating_add(1);
    Duration::from_micros(rand::random::<u64>() % span)
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: Duration = Duration::from_millis(1);
    const CAP: Duration = Duration::from_millis(100);

    #[test]
    fn window_doubles_until_the_cap() {
        assert_eq!(backoff_window(0, BASE, CAP), Duration::from_millis(1));
        assert_eq!(backoff_window(1, BASE, CAP), Duration::from_millis(2));
        assert_eq!(backoff_window(6, BASE, CAP), Duration::from_millis(64));
        assert_eq!(backoff_window(7, BASE, CAP), CAP);
        assert_eq!(backoff_window(31, BASE, CAP), CAP);
        assert_eq!(backoff_window(u32::MAX, BASE, CAP), CAP);
    }

    #[test]
    fn jitter_stays_in_the_window_and_is_not_constant() {
        for attempt in [0u32, 3, 7, 40] {
            let window = backoff_window(attempt, BASE, CAP);
            let samples: Vec<Duration> = (0..256)
                .map(|_| full_jitter_backoff(attempt, BASE, CAP))
                .collect();
            assert!(samples.iter().all(|delay| *delay <= window));
            assert!(samples.iter().any(|delay| *delay != samples[0]));
        }
    }

    #[test]
    fn zero_window_gives_zero_delay() {
        assert_eq!(
            full_jitter_backoff(5, Duration::ZERO, CAP),
            Duration::ZERO
        );
    }
}
