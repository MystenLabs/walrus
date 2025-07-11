// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{fmt, marker::PhantomData, sync::Arc};

use bincode::Options;
use prometheus::{Histogram, HistogramTimer};
use rocksdb::Direction;
use serde::{Serialize, de::DeserializeOwned};

use super::{RocksDBRawIter, be_fix_int_ser};
use crate::{
    TypedStoreError,
    metrics::{DBMetrics, RocksDBPerfContext},
    rocks::errors::typed_store_err_from_bincode_err,
    traits::SeekableIterator,
};

/// An iterator over all key-value pairs in a data map.
pub struct SafeIter<'a, K, V> {
    cf_name: String,
    db_iter: RocksDBRawIter<'a>,
    _phantom: PhantomData<(K, V)>,
    direction: Direction,
    is_initialized: bool,
    _timer: Option<HistogramTimer>,
    _perf_ctx: Option<RocksDBPerfContext>,
    bytes_scanned: Option<Histogram>,
    keys_scanned: Option<Histogram>,
    db_metrics: Option<Arc<DBMetrics>>,
    bytes_scanned_counter: usize,
    keys_returned_counter: usize,
}

impl<K: DeserializeOwned, V: DeserializeOwned> fmt::Debug for SafeIter<'_, K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SafeIter")
    }
}

impl<'a, K: DeserializeOwned, V: DeserializeOwned> SafeIter<'a, K, V> {
    pub(super) fn new(
        cf_name: String,
        db_iter: RocksDBRawIter<'a>,
        _timer: Option<HistogramTimer>,
        _perf_ctx: Option<RocksDBPerfContext>,
        bytes_scanned: Option<Histogram>,
        keys_scanned: Option<Histogram>,
        db_metrics: Option<Arc<DBMetrics>>,
    ) -> Self {
        Self {
            cf_name,
            db_iter,
            _phantom: PhantomData,
            direction: Direction::Forward,
            is_initialized: false,
            _timer,
            _perf_ctx,
            bytes_scanned,
            keys_scanned,
            db_metrics,
            bytes_scanned_counter: 0,
            keys_returned_counter: 0,
        }
    }
}

impl<K: DeserializeOwned, V: DeserializeOwned> Iterator for SafeIter<'_, K, V> {
    type Item = Result<(K, V), TypedStoreError>;

    fn next(&mut self) -> Option<Self::Item> {
        // Implicitly set iterator to the first entry in the column
        // family if it hasn't been initialized used for backward
        // compatibility
        if !self.is_initialized {
            self.db_iter.seek_to_first();
            self.is_initialized = true;
        }
        if self.db_iter.valid() {
            let config = bincode::DefaultOptions::new()
                .with_big_endian()
                .with_fixint_encoding();
            let raw_key = self
                .db_iter
                .key()
                .expect("valid iterator should be able to get key");
            let raw_value = self
                .db_iter
                .value()
                .expect("valid iterator should be able to get value");
            self.bytes_scanned_counter += raw_key.len() + raw_value.len();
            self.keys_returned_counter += 1;
            let key = config.deserialize(raw_key).ok();
            let value = bcs::from_bytes(raw_value).ok();
            match self.direction {
                Direction::Forward => self.db_iter.next(),
                Direction::Reverse => self.db_iter.prev(),
            }
            key.and_then(|k| value.map(|v| Ok((k, v))))
        } else {
            match self.db_iter.status() {
                Ok(_) => None,
                Err(err) => Some(Err(TypedStoreError::RocksDBError(format!("{err}")))),
            }
        }
    }
}

impl<K, V> Drop for SafeIter<'_, K, V> {
    fn drop(&mut self) {
        if let Some(bytes_scanned) = self.bytes_scanned.take() {
            bytes_scanned.observe(self.bytes_scanned_counter as f64);
        }
        if let Some(keys_scanned) = self.keys_scanned.take() {
            keys_scanned.observe(self.keys_returned_counter as f64);
        }
        if let Some(db_metrics) = self.db_metrics.take() {
            db_metrics
                .read_perf_ctx_metrics
                .report_metrics(&self.cf_name);
        }
    }
}

impl<K: DeserializeOwned + Serialize, V> SeekableIterator<K> for SafeIter<'_, K, V> {
    fn seek_to_first(&mut self) {
        self.is_initialized = true;
        self.db_iter.seek_to_first();
    }

    fn seek_to_last(&mut self) {
        self.is_initialized = true;
        self.db_iter.seek_to_last();
    }

    fn seek(&mut self, key: &K) -> Result<(), TypedStoreError> {
        self.is_initialized = true;
        self.db_iter.seek(be_fix_int_ser(key)?);
        Ok(())
    }

    fn seek_to_prev(&mut self, key: &K) -> Result<(), TypedStoreError> {
        self.is_initialized = true;
        self.db_iter.seek_for_prev(be_fix_int_ser(key)?);
        Ok(())
    }

    fn key(&self) -> Result<Option<K>, TypedStoreError> {
        // Before getting the key, the caller must place the iterator at a valid position,
        // by either calling SeekableIterator APIs or Iterator APIs.
        if !self.is_initialized {
            return Err(TypedStoreError::IteratorNotInitialized);
        }

        let raw_key = self.db_iter.key();
        raw_key
            .map(|data| {
                bincode::DefaultOptions::new()
                    .with_big_endian()
                    .with_fixint_encoding()
                    .deserialize(data)
                    .map_err(typed_store_err_from_bincode_err)
            })
            .transpose()
    }
}
/// An iterator with a reverted direction to the original. The `RevIter`
/// is hosting an iteration which is consuming in the opposing direction.
/// It's not possible to do further manipulation (ex re-reverse) to the
/// iterator.
#[derive(Debug)]
pub struct SafeRevIter<'a, K: DeserializeOwned, V: DeserializeOwned> {
    iter: SafeIter<'a, K, V>,
}

impl<'a, K: DeserializeOwned, V: DeserializeOwned> SafeRevIter<'a, K, V> {
    pub(crate) fn new(mut iter: SafeIter<'a, K, V>, upper_bound: Option<Vec<u8>>) -> Self {
        iter.is_initialized = true;
        iter.direction = Direction::Reverse;
        match upper_bound {
            None => iter.db_iter.seek_to_last(),
            Some(key) => iter.db_iter.seek_for_prev(&key),
        }
        Self { iter }
    }
}

impl<K: DeserializeOwned, V: DeserializeOwned> Iterator for SafeRevIter<'_, K, V> {
    type Item = Result<(K, V), TypedStoreError>;

    /// Will give the next item backwards
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}
