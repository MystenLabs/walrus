// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Bounded, process-local knowledge of lifetimes successfully submitted to Strata.

use std::collections::{HashMap, VecDeque};

use strata::BlobKey;
use walrus_core::Epoch;

// Keys are the fixed 33-byte sliver keys. Eviction only causes a redundant submission.
const CAPACITY: usize = 100_000;

#[derive(Debug)]
pub(super) struct LifetimeCache {
    entries: HashMap<BlobKey, Epoch>,
    insertion_order: VecDeque<BlobKey>,
    capacity: usize,
}

impl Default for LifetimeCache {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
            insertion_order: VecDeque::new(),
            capacity: CAPACITY,
        }
    }
}

impl LifetimeCache {
    pub(super) fn contains(&self, key: &BlobKey, end: Epoch) -> bool {
        self.entries.get(key) == Some(&end)
    }

    /// Called only after successful submission, under the blob's submission/exclusive lock.
    pub(super) fn record(&mut self, key: BlobKey, end: Epoch) {
        if !self.entries.contains_key(&key) {
            if self.entries.len() == self.capacity {
                let oldest = self.insertion_order.pop_front().expect("cache is full");
                self.entries.remove(&oldest);
            }
            self.insertion_order.push_back(key.clone());
        }
        self.entries.insert(key, end);
    }

    // Retain the FIFO slot until eviction, avoiding an O(capacity) removal on deletion.
    pub(super) fn invalidate(&mut self, key: &BlobKey) {
        if let Some(end) = self.entries.get_mut(key) {
            *end = 0; // A put never admits an expired lifetime.
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_cache_handles_changes_eviction_and_invalidation() {
        let mut cache = LifetimeCache {
            capacity: 2,
            ..Default::default()
        };
        let a = BlobKey::new([1]).unwrap();
        let b = BlobKey::new([2]).unwrap();
        let c = BlobKey::new([3]).unwrap();
        cache.record(a.clone(), 10);
        cache.record(a.clone(), 20);
        assert!(!cache.contains(&a, 10));
        assert!(cache.contains(&a, 20));
        cache.record(b.clone(), 10);
        cache.invalidate(&a);
        assert!(!cache.contains(&a, 20));
        cache.record(a.clone(), 20);
        cache.record(c.clone(), 10);
        assert!(!cache.contains(&a, 20));
        assert!(cache.contains(&b, 10));
        assert!(cache.contains(&c, 10));
        assert_eq!(cache.entries.len(), 2);
        assert_eq!(cache.insertion_order.len(), 2);
    }
}
