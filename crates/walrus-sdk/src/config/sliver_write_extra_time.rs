// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_with::{DurationMilliSeconds, serde_as};

/// The additional time allowed to sliver writes, to allow for more nodes to receive them.
#[serde_as]
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(default)]
pub struct SliverWriteExtraTime {
    /// The multiplication factor for the time it took to store n-f sliver.
    pub factor: f64,
    /// The minimum extra time.
    #[serde(rename = "base_millis")]
    #[serde_as(as = "DurationMilliSeconds")]
    pub base: Duration,
}

impl SliverWriteExtraTime {
    /// Returns the extra time for the given time.
    ///
    /// The extra time is computed as `store_time * factor + base`.
    pub fn extra_time(&self, store_time: Duration) -> Duration {
        #[allow(clippy::cast_possible_truncation)]
        let extra_time = Duration::from_nanos((store_time.as_nanos() as f64 * self.factor) as u64);
        self.base + extra_time
    }
}

impl Default for SliverWriteExtraTime {
    fn default() -> Self {
        Self {
            factor: 0.5,                      // 1/2 of the time it took to store n-f slivers.
            base: Duration::from_millis(500), // +0.5s every time.
        }
    }
}
