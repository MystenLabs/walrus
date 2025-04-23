// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

#[cfg(feature = "backoff")]
pub mod backoff;

#[cfg(feature = "config")]
pub mod config;

#[cfg(feature = "metrics")]
pub mod metrics;

#[cfg(feature = "http")]
pub mod http;

#[cfg(feature = "test-utils")]
pub mod tests {
    use std::sync::OnceLock;

    use tokio::sync::Mutex;

    // Prevent tests running simultaneously to avoid interferences or race conditions.
    pub fn global_test_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(Mutex::default)
    }
}

/// A macro to print a crumb of information to the console. This is useful for debugging.
#[macro_export]
macro_rules! crumb {
    ($($arg:tt)+) => {{
        fn f() {}
        fn type_name_of<T>(_: T) -> &'static str {
            std::any::type_name::<T>()
        }
        let name = type_name_of(f);
        eprintln!(
            "{}:{}: crumb: [{:?}]: {}: {}",
            file!(),
            line!(),
            std::thread::current().id(),
            name.strip_suffix("::f").unwrap(),
            format!($($arg)+),
        );
    }};
    ($expr:literal) => {{
        $crate::crumb!("{}", $expr);
    }};
    () => {{
        $crate::crumb!("you left a crumb here.");
    }};
}
