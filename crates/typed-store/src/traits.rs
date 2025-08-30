// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{borrow::Borrow, error::Error, ops::RangeBounds};

use serde::{Serialize, de::DeserializeOwned};

use crate::TypedStoreError;

/// The trait for the typed store to manage the map
pub trait Map<'a, K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    /// The error type for the map
    type Error: Error;
    /// The safe iterator type for the map
    type SafeIterator: Iterator<Item = Result<(K, V), TypedStoreError>> + SeekableIterator<K>;

    /// Returns true if the map contains a value for the specified key.
    fn contains_key(&self, key: &K) -> Result<bool, Self::Error>;

    /// Returns true if the map contains a value for the specified key.
    fn multi_contains_keys<J>(
        &self,
        keys: impl IntoIterator<Item = J>,
    ) -> Result<Vec<bool>, Self::Error>
    where
        J: Borrow<K>,
    {
        keys.into_iter()
            .map(|key| self.contains_key(key.borrow()))
            .collect()
    }

    /// Returns the value for the given key from the map, if it exists.
    fn get(&self, key: &K) -> Result<Option<V>, Self::Error>;

    /// Inserts the given key-value pair into the map.
    fn insert(&self, key: &K, value: &V) -> Result<(), Self::Error>;

    /// Removes the entry for the given key from the map.
    fn remove(&self, key: &K) -> Result<(), Self::Error>;

    /// Removes every key-value pair from the map.
    fn unsafe_clear(&self) -> Result<(), Self::Error>;

    /// Uses delete range on the entire key range when supported; otherwise
    /// deletes all entries individually as a fallback.
    fn schedule_delete_all(&self) -> Result<(), TypedStoreError>;

    /// Removes every key-value pair from the map individually
    fn delete_all_individually(&self) -> Result<(), TypedStoreError>;

    /// Returns true if the map is empty, otherwise false.
    fn is_empty(&self) -> bool;

    /// Same as `iter` but performs status check.
    fn safe_iter(&'a self) -> Result<Self::SafeIterator, Self::Error>;

    /// Same as `iter_with_bounds` but performs status check.
    fn safe_iter_with_bounds(
        &'a self,
        lower_bound: Option<K>,
        upper_bound: Option<K>,
    ) -> Result<Self::SafeIterator, Self::Error>;

    /// Same as `range_iter` but performs status check.
    fn safe_range_iter(
        &'a self,
        range: impl RangeBounds<K>,
    ) -> Result<Self::SafeIterator, Self::Error>;

    /// Returns a vector of values corresponding to the keys provided, non-atomically.
    fn multi_get<J>(&self, keys: impl IntoIterator<Item = J>) -> Result<Vec<Option<V>>, Self::Error>
    where
        J: Borrow<K>,
    {
        keys.into_iter().map(|key| self.get(key.borrow())).collect()
    }

    /// Inserts key-value pairs, non-atomically.
    fn multi_insert<J, U>(
        &self,
        key_val_pairs: impl IntoIterator<Item = (J, U)>,
    ) -> Result<(), Self::Error>
    where
        J: Borrow<K>,
        U: Borrow<V>,
    {
        key_val_pairs
            .into_iter()
            .try_for_each(|(key, value)| self.insert(key.borrow(), value.borrow()))
    }

    /// Removes keys, non-atomically.
    fn multi_remove<J>(&self, keys: impl IntoIterator<Item = J>) -> Result<(), Self::Error>
    where
        J: Borrow<K>,
    {
        keys.into_iter()
            .try_for_each(|key| self.remove(key.borrow()))
    }

    /// Try to catch up with primary when running as secondary
    fn try_catch_up_with_primary(&self) -> Result<(), Self::Error>;
}

/// The trait for seekable iterator.
pub trait SeekableIterator<K> {
    /// Seeks to the first key in the iterator.
    fn seek_to_first(&mut self);

    /// Seeks to the last key in the iterator.
    fn seek_to_last(&mut self);

    /// Seeks to the specified key. If the key does not exist, it will seek to the next key.
    fn seek(&mut self, key: &K) -> Result<(), TypedStoreError>;

    /// Seeks to the previous key. If the key does not exist, it will seek to the previous key.
    fn seek_to_prev(&mut self, key: &K) -> Result<(), TypedStoreError>;

    /// Returns the current key.
    fn key(&self) -> Result<Option<K>, TypedStoreError>;
}

/// The summary of the table
#[derive(Debug)]
pub struct TableSummary {
    /// The number of keys in the table
    pub num_keys: u64,
    /// The total number of key bytes in the table
    pub key_bytes_total: usize,
    /// The total number of value bytes in the table
    pub value_bytes_total: usize,
    /// The histogram of key sizes
    pub key_hist: hdrhistogram::Histogram<u64>,
    /// The histogram of value sizes
    pub value_hist: hdrhistogram::Histogram<u64>,
}
