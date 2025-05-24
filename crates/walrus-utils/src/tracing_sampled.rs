// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

pub use humantime::parse_duration;
pub use once_cell::sync::Lazy;
/// Logs the given statement at most once per specified interval for each call site.
/// Relies on a static variable per invocation location to track the last emission time.
/// Includes count of skipped events in the log message when an event is finally emitted.
///
/// # Arguments
///
/// * `$interval`: A `std::time::Duration` specifying the minimum interval between logs.
/// * `$level:ident`: The tracing level identifier (
///   e.g., `info`, `debug`, `warn`, `error`, `trace`).
/// * `$($fields_and_message:tt)*`: The remaining arguments passed directly to the underlying macro.
///
/// # Returns
///
/// Returns `true` if a log message was emitted by this specific call, `false` otherwise.
///
/// # Example
///
/// ```rust
/// use std::time::Duration;
/// use tracing;
/// use walrus_utils::tracing_sampled;
/// use std::thread;
///
/// fn process_item(item_id: u32) {
///     // Log processing start max once every 5 seconds for this function call site.
///     tracing_sampled::info!("5s", item_id, "Starting processing");
///     // Simulate work
///     thread::sleep(Duration::from_millis(100));
///     // Log completion max once every 30 seconds, including skipped count.
///     tracing_sampled::debug!("30s", item_id, "Finished processing item");
/// }
/// ```
#[macro_export]
macro_rules! log {
    ($interval:expr, $level:ident, $($fields_and_message:tt)*) => {
        {
            use std::sync::atomic::{AtomicU64, Ordering};
            use std::time::{Duration, SystemTime, UNIX_EPOCH};

            static LAST_LOGGED_MS: $crate::tracing_sampled::Lazy<AtomicU64> =
                $crate::tracing_sampled::Lazy::new(|| AtomicU64::new(0));
            static SKIPPED_COUNT: $crate::tracing_sampled::Lazy<AtomicU64> =
                $crate::tracing_sampled::Lazy::new(|| AtomicU64::new(0));

            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::from_secs(0))
                .as_millis() as u64;

            let last_ms = LAST_LOGGED_MS.load(Ordering::Relaxed);
            let interval_ms =
                $crate::tracing_sampled::parse_duration($interval).unwrap().as_millis() as u64;

            let should_log = last_ms == 0 || now_ms.saturating_sub(last_ms) >= interval_ms;

            let mut did_log = false;

            if should_log {
                match LAST_LOGGED_MS.compare_exchange(
                    last_ms,
                    now_ms,
                    Ordering::Relaxed,
                    Ordering::Relaxed
                ) {
                    Ok(_) => {
                        let skipped = SKIPPED_COUNT.swap(0, Ordering::Relaxed);

                        if skipped > 0 {
                            tracing::$level!(tracing_samples_skipped = skipped,
                                $($fields_and_message)*);
                        } else {
                            tracing::$level!($($fields_and_message)*);
                        }
                        did_log = true;
                    },
                    Err(_) => {
                        // CAS failed: Another thread updated LAST_LOGGED_MS
                    }
                }
            }

            if !did_log {
                SKIPPED_COUNT.fetch_add(1, Ordering::Relaxed);
            }

            did_log
        }
    };
}

#[macro_export]
macro_rules! info {
    ($interval:expr, $($fields_and_message:tt)*) => {
        $crate::tracing_sampled::log!($interval, info, $($fields_and_message)*)
    };
}

#[macro_export]
macro_rules! debug {
    ($interval:expr, $($fields_and_message:tt)*) => {
        $crate::tracing_sampled::log!($interval, debug, $($fields_and_message)*)
    };
}

#[macro_export]
macro_rules! warning {
    ($interval:expr, $($fields_and_message:tt)*) => {
        $crate::tracing_sampled::log!($interval, warn, $($fields_and_message)*)
    };
}

#[macro_export]
macro_rules! error {
    ($interval:expr, $($fields_and_message:tt)*) => {
        $crate::tracing_sampled::log!($interval, error, $($fields_and_message)*)
    };
}

#[macro_export]
macro_rules! trace {
    ($interval:expr, $($fields_and_message:tt)*) => {
        $crate::tracing_sampled::log!($interval, trace, $($fields_and_message)*)
    };
}

pub use crate::{debug, error, info, log, trace, warning};

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use tracing;

    use crate::tracing_sampled;

    #[tokio::test]
    async fn test_sampled_logging_new_macro() {
        let _ = tracing_subscriber::fmt::try_init();

        let start = Instant::now();
        let mut actual_log_count = 0;
        let mut total_calls = 0;

        tracing::info!("Starting new macro test loop");
        let test_duration = Duration::from_secs(3);

        while start.elapsed() < test_duration {
            total_calls += 1;

            if tracing_sampled::info!("1s", iteration = total_calls, "New macro info log") {
                actual_log_count += 1;
            }

            tracing_sampled::debug!("500ms", "Debug check: {}", total_calls);

            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        tracing::info!(
            "Finished new macro test loop after {} calls, {} logs emitted",
            total_calls,
            actual_log_count
        );

        let expected_min_logs = (test_duration.as_millis() as f64 / 1000.0).floor() as usize;
        let expected_max_logs = (test_duration.as_millis() as f64 / 1000.0).ceil() as usize;

        assert!(
            actual_log_count >= expected_min_logs && actual_log_count <= expected_max_logs + 1,
            "Expected roughly {} logs (between {} and {}), got {}",
            expected_min_logs,
            expected_min_logs,
            expected_max_logs + 1,
            actual_log_count
        );
    }
}
