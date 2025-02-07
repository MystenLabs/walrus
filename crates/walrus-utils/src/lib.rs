// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

pub mod backoff;
pub mod checkpoint_downloader;
pub mod metrics;

pub mod tests {
    use std::sync::OnceLock;

    use tokio::sync::Mutex;

    // Prevent tests running simultaneously to avoid interferences or race conditions.
    pub fn global_test_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(Mutex::default)
    }
}
