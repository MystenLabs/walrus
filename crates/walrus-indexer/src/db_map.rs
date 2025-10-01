// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Custom DBMap wrapper with configurable key serialization.
//!
//! This module provides a wrapper around typed_store's DBMap that allows
//! for custom key serialization/deserialization strategies, enabling
//! optimized key encoding for specific use cases like the indexer.

use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use rocksdb::OptimisticTransactionDB;
use serde::{Deserialize, Serialize};
use typed_store::{
    TypedStoreError,
    rocks::{DBMap, MetricConf, ReadWriteOptions, RocksDB, open_cf_opts_optimistic},
    traits::Map,
};

/// Trait for key serialization and deserialization.
pub trait KeyCodec: Sized {
    /// Serialize the key to bytes.
    fn serialize(&self) -> Result<Vec<u8>, TypedStoreError>;

    /// Deserialize a key from bytes.
    fn deserialize(bytes: &[u8]) -> Result<Self, TypedStoreError>;
}

/// A wrapper around DBMap that allows raw key serialization/deserialization.
///
/// This struct provides a minimal interface for using DBMap with raw
/// key serialization/deserialization while keeping the default BCS serialization for values.
pub struct RawKeyDBMap<K, V>
where
    K: KeyCodec,
    V: Serialize + for<'de> Deserialize<'de> + Clone,
{
    inner: DBMap<Vec<u8>, V>,
    _phantom: PhantomData<K>,
}

impl<K, V> RawKeyDBMap<K, V>
where
    K: KeyCodec,
    V: Serialize + for<'de> Deserialize<'de> + Clone,
{
    /// Creates a new RawKeyDBMap wrapping an existing DBMap.
    pub fn new(inner: DBMap<Vec<u8>, V>) -> Result<Self, TypedStoreError> {
        Ok(RawKeyDBMap {
            inner,
            _phantom: PhantomData,
        })
    }

    /// Puts a key-value pair within a transaction.
    pub fn put_cf_with_txn(
        &self,
        txn: &rocksdb::Transaction<'_, OptimisticTransactionDB>,
        key: &K,
        value: &V,
    ) -> Result<(), TypedStoreError> {
        let key_bytes = key.serialize()?;
        let value_bytes = bcs::to_bytes(value)
            .map_err(|e| TypedStoreError::SerializationError(e.to_string()))?;
        let cf = self.inner.cf()?;

        txn.put_cf(&cf, key_bytes, value_bytes)
            .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))
    }

    /// Deletes a key within a transaction.
    pub fn delete_cf_with_txn(
        &self,
        txn: &rocksdb::Transaction<'_, OptimisticTransactionDB>,
        key: &K,
    ) -> Result<(), TypedStoreError> {
        let key_bytes = key.serialize()?;
        let cf = self.inner.cf()?;

        txn.delete_cf(&cf, key_bytes)
            .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))
    }

    /// Gets a value for update within a transaction (acquires lock).
    pub fn get_for_update_cf(
        &self,
        txn: &rocksdb::Transaction<'_, OptimisticTransactionDB>,
        key: &K,
    ) -> Result<Option<V>, TypedStoreError> {
        let key_bytes = key.serialize()?;
        let cf = self.inner.cf()?;

        let value_bytes = txn
            .get_for_update_cf(&cf, &key_bytes, true)
            .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;

        match value_bytes {
            Some(bytes) => {
                let value = bcs::from_bytes(&bytes)
                    .map_err(|e| TypedStoreError::SerializationError(e.to_string()))?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Gets a value within a transaction (without lock).
    pub fn get_cf_with_txn(
        &self,
        txn: &rocksdb::Transaction<'_, OptimisticTransactionDB>,
        key: &K,
    ) -> Result<Option<V>, TypedStoreError> {
        let key_bytes = key.serialize()?;
        let cf = self.inner.cf()?;

        let value_bytes = txn
            .get_cf(&cf, &key_bytes)
            .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;

        match value_bytes {
            Some(bytes) => {
                let value = bcs::from_bytes(&bytes)
                    .map_err(|e| TypedStoreError::SerializationError(e.to_string()))?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Reads a range of key-value pairs with a given prefix within a transaction.
    /// Returns an iterator over the key-value pairs that match the prefix.
    pub fn read_range_prefix<'a>(
        &self,
        txn: &'a rocksdb::Transaction<'_, OptimisticTransactionDB>,
        prefix: &K,
    ) -> Result<impl Iterator<Item = Result<(K, V), TypedStoreError>> + 'a, TypedStoreError> {
        let prefix_bytes = prefix.serialize()?;
        let cf = self.inner.cf()?;

        let iter = txn.prefix_iterator_cf(&cf, prefix_bytes);

        Ok(iter.map(|item| {
            match item {
                Ok((key_bytes, value_bytes)) => {
                    let key = K::deserialize(&key_bytes)?;
                    let value = bcs::from_bytes(&value_bytes)
                        .map_err(|e| TypedStoreError::SerializationError(e.to_string()))?;
                    Ok((key, value))
                }
                Err(e) => Err(TypedStoreError::RocksDBError(e.to_string())),
            }
        }))
    }

    /// Checks if the map contains a key.
    pub fn contains_key(&self, key: &K) -> Result<bool, TypedStoreError> {
        let key_bytes = key.serialize()?;
        self.inner.contains_key(&key_bytes)
    }

    /// Gets a value by key.
    pub fn get(&self, key: &K) -> Result<Option<V>, TypedStoreError> {
        let key_bytes = key.serialize()?;
        self.inner.get(&key_bytes)
    }

    /// Inserts a key-value pair.
    pub fn insert(&self, key: &K, value: &V) -> Result<(), TypedStoreError> {
        let key_bytes = key.serialize()?;
        self.inner.insert(&key_bytes, value)
    }

    /// Removes a key-value pair.
    pub fn remove(&self, key: &K) -> Result<(), TypedStoreError> {
        let key_bytes = key.serialize()?;
        self.inner.remove(&key_bytes)
    }

    /// Returns an iterator over all key-value pairs.
    /// Note: This uses safe_iter internally which may have different performance characteristics.
    pub fn safe_iter(
        &self,
    ) -> Result<impl Iterator<Item = Result<(K, V), TypedStoreError>> + '_, TypedStoreError> {
        Ok(self.inner.safe_iter()?.map(|result| {
            result.and_then(|(key_bytes, value)| {
                let key = K::deserialize(&key_bytes)?;
                Ok((key, value))
            })
        }))
    }

    /// Flushes the map to disk.
    pub fn flush(&self) -> Result<(), TypedStoreError> {
        self.inner.flush()
    }
}

impl<K, V> Clone for RawKeyDBMap<K, V>
where
    K: KeyCodec,
    V: Serialize + for<'de> Deserialize<'de> + Clone,
{
    fn clone(&self) -> Self {
        RawKeyDBMap {
            inner: self.inner.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<K, V> std::fmt::Debug for RawKeyDBMap<K, V>
where
    K: KeyCodec,
    V: Serialize + for<'de> Deserialize<'de> + Clone + std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RawKeyDBMap")
            .field("inner", &self.inner)
            .finish()
    }
}

/// Opens an optimistic transaction database with raw key serialization.
///
/// This function initializes a RocksDB optimistic transaction database and returns
/// RawKeyDBMaps for the specified column families with raw key serialization.
///
/// # Arguments
/// * `path` - The path where the database will be created or opened
/// * `db_options` - Optional RocksDB options for the database
/// * `metric_conf` - Metrics configuration for the database
/// * `cf_options` - Column family names with their specific options
///
/// # Returns
/// A tuple of (Arc<RocksDB>, Vec<RawKeyDBMap>) where each map corresponds to a column family.
pub fn open_cf_raw_key_opts_optimistic<P, K, V>(
    path: P,
    db_options: Option<rocksdb::Options>,
    metric_conf: MetricConf,
    cf_options: &[(&str, rocksdb::Options)],
) -> Result<(Arc<RocksDB>, Vec<RawKeyDBMap<K, V>>), TypedStoreError>
where
    P: AsRef<Path>,
    K: KeyCodec,
    V: Serialize + for<'de> Deserialize<'de> + Clone,
{
    // Open the optimistic transaction database using typed_store.
    let rocksdb = open_cf_opts_optimistic(path, db_options, metric_conf, cf_options)?;

    // Extract column family names.
    let cf_names: Vec<&str> = cf_options.iter().map(|(name, _)| *name).collect();

    // Create RawKeyDBMaps for each column family.
    let maps = create_raw_key_db_maps(&rocksdb, &cf_names, &ReadWriteOptions::default())?;

    Ok((rocksdb, maps))
}

/// Helper function to create RawKeyDBMaps from an existing optimistic transaction database.
///
/// This function takes an already opened RocksDB (from typed_store) and creates
/// RawKeyDBMaps for the specified column families with raw key serialization.
///
/// # Arguments
/// * `rocksdb` - The Arc<RocksDB> from typed_store (must be OptimisticTransactionDB variant)
/// * `cf_names` - Names of column families to wrap with RawKeyDBMap
/// * `opts` - Read/write options for the DBMaps
///
/// # Returns
/// A vector of RawKeyDBMaps, one for each column family.
pub fn create_raw_key_db_maps<K, V>(
    rocksdb: &Arc<RocksDB>,
    cf_names: &[&str],
    opts: &ReadWriteOptions,
) -> Result<Vec<RawKeyDBMap<K, V>>, TypedStoreError>
where
    K: KeyCodec,
    V: Serialize + for<'de> Deserialize<'de> + Clone,
{
    let mut maps = Vec::with_capacity(cf_names.len());

    for cf_name in cf_names {
        // Create a DBMap for this column family using the existing RocksDB
        let inner_map = DBMap::<Vec<u8>, V>::reopen(rocksdb, Some(cf_name), opts, false)?;

        // Wrap it in RawKeyDBMap
        let custom_map = RawKeyDBMap::new(inner_map)?;
        maps.push(custom_map);
    }

    Ok(maps)
}
