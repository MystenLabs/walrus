// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Column family handle with configurable key serialization/deserialization.
//!
//! This module provides a handle to a RocksDB ColumnFamily that allows
//! for custom key serialization/deserialization strategies, enabling
//! optimized key encoding for specific use cases, like to preserve alphanumeric order.

use std::{marker::PhantomData, sync::Arc};

use rocksdb::OptimisticTransactionDB;
use serde::{Deserialize, Serialize};

use crate::{
    TypedStoreError,
    metrics::{DBMetrics, spawn_db_metrics_reporter},
    rocks::RocksDB,
};

/// Trait for key serialization and deserialization.
pub trait KeyCodec: Sized {
    /// Serialize the key to bytes.
    fn serialize(&self) -> Result<Vec<u8>, TypedStoreError>;

    /// Deserialize a key from bytes.
    fn deserialize(bytes: &[u8]) -> Result<Self, TypedStoreError>;
}

/// A handle to a RocksDB ColumnFamily with custom key serialization/deserialization.
///
/// This struct provides a transactional interface for a specific RocksDB column family with custom
/// key serialization/deserialization while keeping the default BCS serialization for values.
/// All operations are transaction-based for consistency.
pub struct ColumnFamilyHandle<K, V>
where
    K: KeyCodec,
    V: Serialize + for<'de> Deserialize<'de> + Clone,
{
    /// The RocksDB instance.
    db: Arc<RocksDB>,
    /// Column family name.
    cf_name: String,
    /// Database metrics.
    db_metrics: Arc<DBMetrics>,
    /// Phantom data to enforce type constraints.
    _phantom: PhantomData<(K, V)>,
    /// Shutdown signal for metrics task.
    _metrics_shutdown: tokio::sync::oneshot::Sender<()>,
}

impl<K, V> ColumnFamilyHandle<K, V>
where
    K: KeyCodec,
    V: Serialize + for<'de> Deserialize<'de> + Clone,
{
    /// Creates a new ColumnFamilyHandle with direct RocksDB and ColumnFamily access.
    pub fn new(
        db: Arc<RocksDB>,
        cf_name: &str,
        is_deprecated: bool,
    ) -> Result<Self, TypedStoreError> {
        // Verify the column family exists
        db.cf_handle(cf_name).ok_or_else(|| {
            TypedStoreError::RocksDBError(format!("column family '{}' not found", cf_name))
        })?;

        // Get metrics instance
        let db_metrics = DBMetrics::get().clone();

        // Start metrics reporting task
        let (sender, receiver) = tokio::sync::oneshot::channel();
        if !is_deprecated {
            spawn_db_metrics_reporter(
                db.clone(),
                vec![cf_name.to_string()],
                db_metrics.clone(),
                receiver,
            );
        }

        Ok(ColumnFamilyHandle {
            db,
            cf_name: cf_name.to_string(),
            db_metrics,
            _phantom: PhantomData,
            _metrics_shutdown: sender,
        })
    }

    /// Get the column family handle.
    fn cf(&self) -> Result<Arc<rocksdb::BoundColumnFamily<'_>>, TypedStoreError> {
        self.db.cf_handle(&self.cf_name).ok_or_else(|| {
            TypedStoreError::RocksDBError(format!("column family '{}' not found", self.cf_name))
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
        let cf = self.cf()?;

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
        let cf = self.cf()?;

        txn.delete_cf(&cf, key_bytes)
            .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))
    }

    /// Gets a value for update within a transaction, the commit will fail if the value is updated
    /// outside the transaction.
    pub fn get_for_update_cf(
        &self,
        txn: &rocksdb::Transaction<'_, OptimisticTransactionDB>,
        key: &K,
    ) -> Result<Option<V>, TypedStoreError> {
        let key_bytes = key.serialize()?;
        let cf = self.cf()?;

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

    /// Gets a value within a transaction.
    pub fn get_cf_with_txn(
        &self,
        txn: &rocksdb::Transaction<'_, OptimisticTransactionDB>,
        key: &K,
    ) -> Result<Option<V>, TypedStoreError> {
        let key_bytes = key.serialize()?;
        let cf = self.cf()?;

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

    /// Reads a range of key-value pairs within a transaction using efficient range bounds.
    /// Returns key-value pairs in the range [begin, end).
    ///
    /// This is more efficient than prefix-based iteration for range queries.
    pub fn read_range(
        &self,
        txn: &rocksdb::Transaction<'_, OptimisticTransactionDB>,
        begin: Vec<u8>,
        end: Vec<u8>,
        row_limit: usize,
    ) -> Result<Vec<(K, V)>, TypedStoreError> {
        let mut read_opts = rocksdb::ReadOptions::default();
        read_opts.set_iterate_lower_bound(begin);
        read_opts.set_iterate_upper_bound(end);

        let cf = self.cf()?;
        let iter = txn.iterator_cf_opt(&cf, read_opts, rocksdb::IteratorMode::Start);

        let mut results = Vec::new();

        for item in iter.take(row_limit) {
            match item {
                Ok((key_bytes, value_bytes)) => {
                    match (|| -> Result<(K, V), TypedStoreError> {
                        let key = K::deserialize(&key_bytes)?;
                        let value = bcs::from_bytes(&value_bytes)
                            .map_err(|e| TypedStoreError::SerializationError(e.to_string()))?;
                        Ok((key, value))
                    })() {
                        Ok(kv_pair) => results.push(kv_pair),
                        Err(e) => {
                            if results.is_empty() {
                                return Err(e);
                            } else {
                                break; // Return partial results
                            }
                        }
                    }
                }
                Err(e) => {
                    if results.is_empty() {
                        return Err(TypedStoreError::RocksDBError(e.to_string()));
                    } else {
                        break; // Return partial results
                    }
                }
            }

            if results.len() >= row_limit {
                break;
            }
        }

        Ok(results)
    }
}

impl<K, V> Clone for ColumnFamilyHandle<K, V>
where
    K: KeyCodec,
    V: Serialize + for<'de> Deserialize<'de> + Clone,
{
    fn clone(&self) -> Self {
        // Start metrics reporting for the cloned instance
        let (sender, receiver) = tokio::sync::oneshot::channel();
        spawn_db_metrics_reporter(
            self.db.clone(),
            vec![self.cf_name.clone()],
            self.db_metrics.clone(),
            receiver,
        );

        ColumnFamilyHandle {
            db: self.db.clone(),
            cf_name: self.cf_name.clone(),
            db_metrics: self.db_metrics.clone(),
            _phantom: PhantomData,
            _metrics_shutdown: sender,
        }
    }
}

impl<K, V> std::fmt::Debug for ColumnFamilyHandle<K, V>
where
    K: KeyCodec,
    V: Serialize + for<'de> Deserialize<'de> + Clone + std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ColumnFamilyHandle")
            .field("db", &self.db)
            .field("cf_name", &self.cf_name)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::{path::Path, sync::Arc};

    use prometheus::Registry;
    use tempfile::TempDir;

    use super::*;
    use crate::rocks::{MetricConf, ReadWriteOptions, RocksDB, open_cf_opts_optimistic};

    /// Result type for opening a database with column family handles.
    /// Returns `(Arc<RocksDB>, Vec<ColumnFamilyHandle<K, V>>)` on success.
    type OpenCFHandleResult<K, V> =
        Result<(Arc<RocksDB>, Vec<ColumnFamilyHandle<K, V>>), TypedStoreError>;

    /// Opens an optimistic transaction database with column family handles.
    ///
    /// This function initializes a RocksDB optimistic transaction database and returns
    /// ColumnFamilyHandles for the specified column families with custom key
    /// serialization.
    ///
    /// # Arguments
    /// * `path` - The path where the database will be created or opened
    /// * `db_options` - Optional RocksDB options for the database
    /// * `metric_conf` - Metrics configuration for the database
    /// * `cf_options` - Column family names with their specific options
    ///
    /// # Returns
    /// A tuple of `(Arc<RocksDB>, Vec<ColumnFamilyHandle>)` where each handle
    /// corresponds to a column family.
    fn open_cf_handle_opts_optimistic<P, K, V>(
        path: P,
        db_options: Option<rocksdb::Options>,
        metric_conf: MetricConf,
        cf_options: &[(&str, rocksdb::Options)],
    ) -> OpenCFHandleResult<K, V>
    where
        P: AsRef<Path>,
        K: KeyCodec,
        V: Serialize + for<'de> Deserialize<'de> + Clone,
    {
        // Open the optimistic transaction database using typed_store.
        let rocksdb = open_cf_opts_optimistic(path, db_options, metric_conf, cf_options)?;

        // Extract column family names.
        let cf_names: Vec<&str> = cf_options.iter().map(|(name, _)| *name).collect();

        // Create ColumnFamilyHandles for each column family.
        let handles = create_cf_handles(&rocksdb, &cf_names, &ReadWriteOptions::default())?;

        Ok((rocksdb, handles))
    }

    /// Helper function to create ColumnFamilyHandles from an existing optimistic
    /// transaction database.
    ///
    /// This function takes an already opened RocksDB (from typed_store) and creates
    /// ColumnFamilyHandles for the specified column families with custom key
    /// serialization.
    ///
    /// # Arguments
    /// * `rocksdb` - The `Arc<RocksDB>` from typed_store (must be
    ///   OptimisticTransactionDB variant)
    /// * `cf_names` - Names of column families to wrap with
    ///   `ColumnFamilyHandle`
    /// * `_opts` - Read/write options (unused for transactional operations)
    ///
    /// # Returns
    /// A vector of `ColumnFamilyHandle` instances, one for each column family.
    fn create_cf_handles<K, V>(
        rocksdb: &Arc<RocksDB>,
        cf_names: &[&str],
        _opts: &ReadWriteOptions,
    ) -> Result<Vec<ColumnFamilyHandle<K, V>>, TypedStoreError>
    where
        K: KeyCodec,
        V: Serialize + for<'de> Deserialize<'de> + Clone,
    {
        let mut maps = Vec::with_capacity(cf_names.len());

        for cf_name in cf_names {
            // Create ColumnFamilyHandle directly with the RocksDB and column family name
            let custom_map = ColumnFamilyHandle::new(rocksdb.clone(), cf_name, false)?;
            maps.push(custom_map);
        }

        Ok(maps)
    }

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

    impl TestKey {
        fn new(prefix: String, id: u64) -> Self {
            Self { prefix, id }
        }
    }

    // Test value type
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct TestValue {
        data: String,
        count: u32,
    }

    impl TestValue {
        fn new(data: String, count: u32) -> Self {
            Self { data, count }
        }
    }

    fn create_test_db() -> (
        TempDir,
        Arc<RocksDB>,
        Vec<ColumnFamilyHandle<TestKey, TestValue>>,
    ) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");

        let (db, maps) = open_cf_handle_opts_optimistic::<_, TestKey, TestValue>(
            db_path,
            None,
            MetricConf::default(),
            &[("test_cf", rocksdb::Options::default())],
        )
        .expect("failed to create test database");

        (temp_dir, db, maps)
    }

    #[tokio::test]
    async fn test_transaction_operations() {
        crate::metrics::DBMetrics::init(&Registry::default());
        let (_temp_dir, db, maps) = create_test_db();

        let map = &maps[0];

        // Get transaction handle
        let handle = db.as_optimistic().expect("should be optimistic DB");

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
                .expect("failed to put in transaction");
            txn.commit().expect("failed to commit transaction");
        }

        {
            let txn = handle.transaction();

            let retrieved = map
                .get_cf_with_txn(&txn, &key)
                .expect("failed to get in transaction");
            assert_eq!(retrieved, Some(value.clone()));
            txn.commit().expect("failed to commit transaction");
        }
    }

    #[tokio::test]
    async fn test_transaction_range_operations() {
        crate::metrics::DBMetrics::init(&Registry::default());
        let (_temp_dir, db, maps) = create_test_db();

        let map = &maps[0];
        let handle = db.as_optimistic().expect("should be optimistic DB");

        let prefix = "range_test";
        let mut kvs = Vec::new();
        for i in 1..=5 {
            let key = TestKey::new(prefix.to_string(), i);
            let value = TestValue::new(format!("value_{}", i), i as u32 * 10);
            kvs.push((key, value));
        }

        {
            let txn = handle.transaction();

            for (key, value) in kvs.iter() {
                map.put_cf_with_txn(&txn, key, value)
                    .expect("failed to put in transaction");
            }

            let prefix2 = "range_tesu";
            for i in 1..=3 {
                let key = TestKey::new(prefix2.to_string(), i);
                let value = TestValue::new(format!("other_{}", i), i as u32 * 100);
                map.put_cf_with_txn(&txn, &key, &value)
                    .expect("failed to put in transaction");
            }

            let prefix3 = "range_tess";
            for i in 1..=3 {
                let key = TestKey::new(prefix3.to_string(), i);
                let value = TestValue::new(format!("other_{}", i), i as u32 * 100);
                map.put_cf_with_txn(&txn, &key, &value)
                    .expect("failed to put in transaction");
            }

            txn.commit().expect("failed to commit transaction");
        }

        // Create prefix bytes for "range_test/" to match our key serialization format
        let mut begin = prefix.as_bytes().to_vec();
        begin.push(b'/');
        let mut end = prefix.as_bytes().to_vec();
        end.push(b'0');

        {
            let txn = handle.transaction();

            let results_with_limit: Vec<(TestKey, TestValue)> = map
                .read_range(&txn, begin.clone(), end.clone(), 3)
                .expect("failed to read range");

            assert_eq!(results_with_limit.len(), 3);

            let results_without_limit: Vec<(TestKey, TestValue)> = map
                .read_range(&txn, begin.clone(), end.clone(), 100)
                .expect("failed to read range");

            assert_eq!(results_without_limit.len(), kvs.len());
            for (i, (key, value)) in results_without_limit.iter().enumerate() {
                assert_eq!(key, &kvs[i].0);
                assert_eq!(value, &kvs[i].1);
            }
        }

        {
            let txn = handle.transaction();
            assert!(map.delete_cf_with_txn(&txn, &kvs[2].0).is_ok());
            txn.commit().expect("failed to commit transaction");

            let txn = handle.transaction();
            let results_without_limit: Vec<(TestKey, TestValue)> = map
                .read_range(&txn, begin, end, 100)
                .expect("failed to read range");

            kvs.remove(2);
            assert_eq!(results_without_limit.len(), kvs.len());
            for (i, (key, value)) in results_without_limit.iter().enumerate() {
                assert_eq!(key, &kvs[i].0);
                assert_eq!(value, &kvs[i].1);
            }
        }
    }
}
