// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Custom DBMap wrapper with configurable key serialization.
//!
//! This module provides a wrapper around typed_store's DBMap that allows
//! for custom key serialization/deserialization strategies, enabling
//! optimized key encoding for specific use cases like the indexer.

use std::{marker::PhantomData, path::Path, sync::Arc};

use rocksdb::OptimisticTransactionDB;
use serde::{Deserialize, Serialize};

use crate::{
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
        let value_bytes =
            bcs::to_bytes(value).map_err(|e| TypedStoreError::SerializationError(e.to_string()))?;
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
    ///
    /// Note: The prefix parameter should serialize to the actual prefix bytes you want to match.
    pub fn read_range_prefix_bytes<'a>(
        &self,
        txn: &'a rocksdb::Transaction<'_, OptimisticTransactionDB>,
        prefix_bytes: Vec<u8>,
    ) -> Result<impl Iterator<Item = Result<(K, V), TypedStoreError>> + 'a, TypedStoreError> {
        let cf = self.inner.cf()?;

        let iter = txn.prefix_iterator_cf(&cf, prefix_bytes);

        Ok(iter.map(|item| match item {
            Ok((key_bytes, value_bytes)) => {
                let key = K::deserialize(&key_bytes)?;
                let value = bcs::from_bytes(&value_bytes)
                    .map_err(|e| TypedStoreError::SerializationError(e.to_string()))?;
                Ok((key, value))
            }
            Err(e) => Err(TypedStoreError::RocksDBError(e.to_string())),
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

/// Result type for opening a database with raw key maps.
/// Returns `(Arc<RocksDB>, Vec<RawKeyDBMap<K, V>>)` on success.
pub type OpenRawKeyDBResult<K, V> = Result<(Arc<RocksDB>, Vec<RawKeyDBMap<K, V>>), TypedStoreError>;

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
/// A tuple of `(Arc<RocksDB>, Vec<RawKeyDBMap>)` where each map corresponds to a column family.
pub fn open_cf_raw_key_opts_optimistic<P, K, V>(
    path: P,
    db_options: Option<rocksdb::Options>,
    metric_conf: MetricConf,
    cf_options: &[(&str, rocksdb::Options)],
) -> OpenRawKeyDBResult<K, V>
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
/// * `rocksdb` - The `Arc<RocksDB>` from typed_store (must be OptimisticTransactionDB variant)
/// * `cf_names` - Names of column families to wrap with `RawKeyDBMap`
/// * `opts` - Read/write options for the DBMaps
///
/// # Returns
/// A vector of `RawKeyDBMap` instances, one for each column family.
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

#[cfg(test)]
mod tests {
    use prometheus::Registry;
    use tempfile::TempDir;

    use super::*;

    // Test key type that implements KeyCodec
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestKey {
        prefix: String,
        id: u64,
    }

    impl KeyCodec for TestKey {
        fn serialize(&self) -> Result<Vec<u8>, TypedStoreError> {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(self.prefix.as_bytes());
            bytes.push(b'/');
            bytes.extend_from_slice(&self.id.to_be_bytes());
            Ok(bytes)
        }

        fn deserialize(bytes: &[u8]) -> Result<Self, TypedStoreError> {
            if bytes.len() < 9 {
                return Err(TypedStoreError::SerializationError(
                    "Key too short: minimum 9 bytes required".to_string(),
                ));
            }

            let id_bytes: [u8; 8] = bytes[bytes.len() - 8..]
                .try_into()
                .map_err(|_| TypedStoreError::SerializationError("Invalid id bytes".to_string()))?;
            let id = u64::from_be_bytes(id_bytes);

            if bytes[bytes.len() - 9] != b'/' {
                return Err(TypedStoreError::SerializationError(
                    "Missing '/' separator before u64".to_string(),
                ));
            }

            let prefix = String::from_utf8(bytes[..bytes.len() - 9].to_vec())
                .map_err(|e| TypedStoreError::SerializationError(e.to_string()))?;

            Ok(TestKey { prefix, id })
        }
    }

    // Test value type
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct TestValue {
        data: String,
        count: u32,
    }

    fn create_test_db() -> (TempDir, Arc<RocksDB>, Vec<RawKeyDBMap<TestKey, TestValue>>) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");

        let (db, maps) = open_cf_raw_key_opts_optimistic::<_, TestKey, TestValue>(
            db_path,
            None,
            MetricConf::default(),
            &[("test_cf", rocksdb::Options::default())],
        )
        .expect("Failed to create test database");

        (temp_dir, db, maps)
    }

    #[tokio::test]
    async fn test_basic_operations() {
        crate::metrics::DBMetrics::init(&Registry::default());
        let (_temp_dir, _db, maps) = create_test_db();

        let map = &maps[0];

        // Test insert and get
        let key1 = TestKey {
            prefix: "user".to_string(),
            id: 100,
        };
        let value1 = TestValue {
            data: "test data".to_string(),
            count: 42,
        };

        map.insert(&key1, &value1).expect("Failed to insert");

        let retrieved = map.get(&key1).expect("Failed to get");
        assert_eq!(retrieved, Some(value1.clone()));

        // Test contains_key
        assert!(map.contains_key(&key1).expect("Failed to check key"));

        // Test non-existent key
        let key2 = TestKey {
            prefix: "user".to_string(),
            id: 200,
        };
        assert!(!map.contains_key(&key2).expect("Failed to check key"));
        assert_eq!(map.get(&key2).expect("Failed to get"), None);

        // Test remove
        map.remove(&key1).expect("Failed to remove");
        assert!(!map.contains_key(&key1).expect("Failed to check key"));
    }

    #[tokio::test]
    async fn test_transaction_operations() {
        crate::metrics::DBMetrics::init(&Registry::default());
        let (_temp_dir, db, maps) = create_test_db();

        let map = &maps[0];

        // Get transaction handle
        let handle = db.as_optimistic().expect("Should be optimistic DB");

        // Test transaction insert and get
        let key = TestKey {
            prefix: "txn".to_string(),
            id: 1,
        };
        let value = TestValue {
            data: "transaction test".to_string(),
            count: 99,
        };

        // Successful transaction
        {
            let txn = handle.transaction();

            map.put_cf_with_txn(&txn, &key, &value)
                .expect("Failed to put in transaction");
            txn.commit().expect("Failed to commit transaction");
        }

        {
            let txn = handle.transaction();

            let retrieved = map
                .get_cf_with_txn(&txn, &key)
                .expect("Failed to get in transaction");
            assert_eq!(retrieved, Some(value.clone()));
            txn.commit().expect("Failed to commit transaction");
        }
    }
}
