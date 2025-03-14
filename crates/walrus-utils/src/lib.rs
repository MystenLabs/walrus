// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[cfg(feature = "backoff")]
pub mod backoff;

#[cfg(feature = "config")]
pub mod config;

#[cfg(feature = "metrics")]
pub mod metrics;

// TODO(jsmith): Move this out of walrus-utils, as it's an entire service as opposed to a util
#[cfg(feature = "checkpoints")]
pub mod checkpoint_downloader;

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
