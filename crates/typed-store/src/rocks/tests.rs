// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::sync::{Mutex, MutexGuard, OnceLock};

use rstest::rstest;

use super::*;
use crate::{
    retry_transaction,
    rocks::safe_iter::{SafeIter, SafeRevIter},
    traits::SeekableIterator,
};

fn temp_dir() -> std::path::PathBuf {
    tempfile::tempdir()
        .expect("Failed to open temporary directory")
        .keep()
}

// Prevent tests running simultaneously to avoid interferences with metric registration.
fn global_test_lock() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(Mutex::default).lock().unwrap()
}

enum TestIteratorWrapper<'a, K, V> {
    SafeIter(SafeIter<'a, K, V>),
}

// Implement Iterator for TestIteratorWrapper that returns the same type
// result for different types of Iterator.
// For non-safe Iterator, it returns the key value pair. For SafeIterator,
// it consumes the result (assuming no error),
// and return they key value pairs.
impl<K: DeserializeOwned, V: DeserializeOwned> Iterator for TestIteratorWrapper<'_, K, V> {
    type Item = (K, V);
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            TestIteratorWrapper::SafeIter(iter) => iter.next().map(|result| result.unwrap()),
        }
    }
}

impl<K: DeserializeOwned + Serialize, V: DeserializeOwned> SeekableIterator<K>
    for TestIteratorWrapper<'_, K, V>
{
    fn seek_to_first(&mut self) {
        match self {
            TestIteratorWrapper::SafeIter(iter) => iter.seek_to_first(),
        }
    }

    fn seek_to_last(&mut self) {
        match self {
            TestIteratorWrapper::SafeIter(iter) => iter.seek_to_last(),
        }
    }

    fn seek(&mut self, key: &K) -> Result<(), TypedStoreError> {
        match self {
            TestIteratorWrapper::SafeIter(iter) => iter.seek(key),
        }
    }

    fn seek_to_prev(&mut self, key: &K) -> Result<(), TypedStoreError> {
        match self {
            TestIteratorWrapper::SafeIter(iter) => iter.seek_to_prev(key),
        }
    }

    fn key(&self) -> Result<Option<K>, TypedStoreError> {
        match self {
            TestIteratorWrapper::SafeIter(iter) => iter.key(),
        }
    }
}

fn get_iter<K, V>(db: &DBMap<K, V>) -> TestIteratorWrapper<'_, K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    TestIteratorWrapper::SafeIter(db.safe_iter().expect("failed to get iterator"))
}

fn get_reverse_iter<K, V>(
    db: &DBMap<K, V>,
    lower_bound: Option<K>,
    upper_bound: Option<K>,
) -> SafeRevIter<'_, K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    db.reversed_safe_iter_with_bounds(lower_bound, upper_bound)
        .unwrap()
}

fn get_iter_with_bounds<K, V>(
    db: &DBMap<K, V>,
    lower_bound: Option<K>,
    upper_bound: Option<K>,
) -> TestIteratorWrapper<'_, K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    TestIteratorWrapper::SafeIter(
        db.safe_iter_with_bounds(lower_bound, upper_bound)
            .expect("failed to get iterator"),
    )
}

fn get_range_iter<K, V>(
    db: &DBMap<K, V>,
    range: impl RangeBounds<K>,
) -> TestIteratorWrapper<'_, K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    TestIteratorWrapper::SafeIter(db.safe_range_iter(range).expect("failed to get iterator"))
}

#[tokio::test]
async fn test_open() {
    let _db = open_map::<_, u32, String>(temp_dir(), None);
}

#[tokio::test]
async fn test_reopen() {
    let arc = {
        let db = open_map::<_, u32, String>(temp_dir(), None);
        db.insert(&123456789, &"123456789".to_string())
            .expect("Failed to insert");
        db
    };
    let db = DBMap::<u32, String>::reopen(&arc.rocksdb, None, &ReadWriteOptions::default(), false)
        .expect("Failed to re-open storage");
    assert!(
        db.contains_key(&123456789)
            .expect("Failed to retrieve item in storage")
    );
}

#[tokio::test]
async fn test_wrong_reopen() {
    let rocks = open_rocksdb(temp_dir(), &["foo", "bar", "baz"]);
    let db = DBMap::<u8, u8>::reopen(&rocks, Some("quux"), &ReadWriteOptions::default(), false);
    assert!(db.is_err());
}

#[tokio::test]
async fn test_contains_key() {
    let db = open_map(temp_dir(), None);

    db.insert(&123456789, &"123456789".to_string())
        .expect("Failed to insert");
    assert!(
        db.contains_key(&123456789)
            .expect("Failed to call contains key")
    );
    assert!(
        !db.contains_key(&000000000)
            .expect("Failed to call contains key")
    );
}

#[tokio::test]
async fn test_multi_contain() {
    let db = open_map(temp_dir(), None);

    db.insert(&123, &"123".to_string())
        .expect("Failed to insert");
    db.insert(&456, &"456".to_string())
        .expect("Failed to insert");
    db.insert(&789, &"789".to_string())
        .expect("Failed to insert");

    let result = db
        .multi_contains_keys([123, 456])
        .expect("Failed to check multi keys existence");

    assert_eq!(result.len(), 2);
    assert!(result[0]);
    assert!(result[1]);

    let result = db
        .multi_contains_keys([123, 987, 789])
        .expect("Failed to check multi keys existence");

    assert_eq!(result.len(), 3);
    assert!(result[0]);
    assert!(!result[1]);
    assert!(result[2]);
}

#[tokio::test]
async fn test_get() {
    let db = open_map(temp_dir(), None);

    db.insert(&123456789, &"123456789".to_string())
        .expect("Failed to insert");
    assert_eq!(
        Some("123456789".to_string()),
        db.get(&123456789).expect("Failed to get")
    );
    assert_eq!(None, db.get(&000000000).expect("Failed to get"));
}

#[tokio::test]
async fn test_multi_get() {
    let db = open_map(temp_dir(), None);

    db.insert(&123, &"123".to_string())
        .expect("Failed to insert");
    db.insert(&456, &"456".to_string())
        .expect("Failed to insert");

    let result = db.multi_get([123, 456, 789]).expect("Failed to multi get");

    assert_eq!(result.len(), 3);
    assert_eq!(result[0], Some("123".to_string()));
    assert_eq!(result[1], Some("456".to_string()));
    assert_eq!(result[2], None);
}

#[tokio::test]
async fn test_skip() {
    let db = open_map(temp_dir(), None);

    db.insert(&123, &"123".to_string())
        .expect("Failed to insert");
    db.insert(&456, &"456".to_string())
        .expect("Failed to insert");
    db.insert(&789, &"789".to_string())
        .expect("Failed to insert");

    // Skip all smaller
    let key_vals: Vec<_> = get_iter_with_bounds(&db, Some(456), None).collect();
    assert_eq!(key_vals.len(), 2);
    assert_eq!(key_vals[0], (456, "456".to_string()));
    assert_eq!(key_vals[1], (789, "789".to_string()));

    // Skip to the end
    assert_eq!(get_iter_with_bounds(&db, Some(999), None).count(), 0);

    // Skip to last
    assert_eq!(
        get_reverse_iter(&db, None, None).next(),
        Some(Ok((789, "789".to_string()))),
    );

    // Skip to successor of first value
    assert_eq!(get_iter_with_bounds(&db, Some(000), None).count(), 3);
    assert_eq!(get_iter_with_bounds(&db, Some(000), None).count(), 3);
}

#[tokio::test]
async fn test_reverse_iter_with_bounds() {
    let db = open_map(temp_dir(), None);
    db.insert(&123, &"123".to_string())
        .expect("Failed to insert");
    db.insert(&456, &"456".to_string())
        .expect("Failed to insert");
    db.insert(&789, &"789".to_string())
        .expect("Failed to insert");

    let mut iter = get_reverse_iter(&db, None, Some(999));
    assert_eq!(iter.next().unwrap(), Ok((789, "789".to_string())));

    db.insert(&999, &"999".to_string())
        .expect("Failed to insert");
    let mut iter = get_reverse_iter(&db, None, Some(999));
    assert_eq!(iter.next().unwrap(), Ok((999, "999".to_string())));

    let mut iter = get_reverse_iter(&db, None, None);
    assert_eq!(iter.next().unwrap(), Ok((999, "999".to_string())));
}

#[tokio::test]
async fn test_remove() {
    let db = open_map(temp_dir(), None);

    db.insert(&123456789, &"123456789".to_string())
        .expect("Failed to insert");
    assert!(db.get(&123456789).expect("Failed to get").is_some());

    db.remove(&123456789).expect("Failed to remove");
    assert!(db.get(&123456789).expect("Failed to get").is_none());
}

#[tokio::test]
async fn test_iter() {
    let db = open_map(temp_dir(), None);
    db.insert(&123456789, &"123456789".to_string())
        .expect("Failed to insert");
    db.insert(&987654321, &"987654321".to_string())
        .expect("Failed to insert");

    let mut iter = get_iter(&db);

    assert_eq!(Some((123456789, "123456789".to_string())), iter.next());
    assert_eq!(Some((987654321, "987654321".to_string())), iter.next());
    assert_eq!(None, iter.next());
}

#[tokio::test]
async fn test_iter_reverse() {
    let db = open_map(temp_dir(), None);

    db.insert(&1, &"1".to_string()).expect("Failed to insert");
    db.insert(&2, &"2".to_string()).expect("Failed to insert");
    db.insert(&3, &"3".to_string()).expect("Failed to insert");

    let mut iter = get_reverse_iter(&db, None, None);
    assert_eq!(Some(Ok((3, "3".to_string()))), iter.next());
    assert_eq!(Some(Ok((2, "2".to_string()))), iter.next());
    assert_eq!(Some(Ok((1, "1".to_string()))), iter.next());
    assert_eq!(None, iter.next());

    let mut iter = get_iter_with_bounds(&db, Some(1), None);
    assert_eq!(Some((1, "1".to_string())), iter.next());
    assert_eq!(Some((2, "2".to_string())), iter.next());
}

#[tokio::test]
async fn test_insert_batch() {
    let db = open_map(temp_dir(), None);
    let keys_vals = (1..100).map(|i| (i, i.to_string()));
    let mut insert_batch = db.batch();
    insert_batch
        .insert_batch(&db, keys_vals.clone())
        .expect("Failed to batch insert");
    insert_batch.write().expect("Failed to execute batch");
    for (k, v) in keys_vals {
        let val = db.get(&k).expect("Failed to get inserted key");
        assert_eq!(Some(v), val);
    }
}

#[tokio::test]
async fn test_insert_batch_across_cf() {
    let rocks = open_rocksdb(temp_dir(), &["First_CF", "Second_CF"]);

    let db_cf_1 = DBMap::reopen(
        &rocks,
        Some("First_CF"),
        &ReadWriteOptions::default(),
        false,
    )
    .expect("Failed to open storage");
    let keys_vals_1 = (1..100).map(|i| (i, i.to_string()));

    let db_cf_2 = DBMap::reopen(
        &rocks,
        Some("Second_CF"),
        &ReadWriteOptions::default(),
        false,
    )
    .expect("Failed to open storage");
    let keys_vals_2 = (1000..1100).map(|i| (i, i.to_string()));

    let mut batch = db_cf_1.batch();
    batch
        .insert_batch(&db_cf_1, keys_vals_1.clone())
        .expect("Failed to batch insert")
        .insert_batch(&db_cf_2, keys_vals_2.clone())
        .expect("Failed to batch insert");

    batch.write().expect("Failed to execute batch");
    for (k, v) in keys_vals_1 {
        let val = db_cf_1.get(&k).expect("Failed to get inserted key");
        assert_eq!(Some(v), val);
    }

    for (k, v) in keys_vals_2 {
        let val = db_cf_2.get(&k).expect("Failed to get inserted key");
        assert_eq!(Some(v), val);
    }
}

#[tokio::test]
async fn test_insert_batch_across_different_db() {
    let rocks = open_rocksdb(temp_dir(), &["First_CF", "Second_CF"]);
    let rocks2 = open_rocksdb(temp_dir(), &["First_CF", "Second_CF"]);

    let db_cf_1: DBMap<i32, String> = DBMap::reopen(
        &rocks,
        Some("First_CF"),
        &ReadWriteOptions::default(),
        false,
    )
    .expect("Failed to open storage");
    let keys_vals_1 = (1..100).map(|i| (i, i.to_string()));

    let db_cf_2: DBMap<i32, String> = DBMap::reopen(
        &rocks2,
        Some("Second_CF"),
        &ReadWriteOptions::default(),
        false,
    )
    .expect("Failed to open storage");
    let keys_vals_2 = (1000..1100).map(|i| (i, i.to_string()));

    assert!(
        db_cf_1
            .batch()
            .insert_batch(&db_cf_1, keys_vals_1)
            .expect("Failed to batch insert")
            .insert_batch(&db_cf_2, keys_vals_2)
            .is_err()
    );
}

#[tokio::test]
async fn test_delete_batch() {
    let db = {
        let _lock = global_test_lock();
        DBMap::<i32, String>::open(
            temp_dir(),
            MetricConf::default(),
            None,
            None,
            &ReadWriteOptions::default(),
        )
        .expect("Failed to open storage")
    };

    let keys_vals = (1..100).map(|i| (i, i.to_string()));
    let mut batch = db.batch();
    batch
        .insert_batch(&db, keys_vals)
        .expect("Failed to batch insert");

    // delete the odd-index keys
    let deletion_keys = (1..100).step_by(2);
    batch
        .delete_batch(&db, deletion_keys)
        .expect("Failed to batch delete");

    batch.write().expect("Failed to execute batch");

    for (k, _) in get_iter(&db) {
        assert_eq!(k % 2, 0);
    }
}

#[tokio::test]
async fn test_delete_range() {
    let db: DBMap<i32, String> = {
        let _lock = global_test_lock();
        DBMap::open(
            temp_dir(),
            MetricConf::default(),
            None,
            None,
            &ReadWriteOptions::default().set_ignore_range_deletions(false),
        )
        .expect("Failed to open storage")
    };

    // Note that the last element is (100, "100".to_owned()) here
    let keys_vals = (0..101).map(|i| (i, i.to_string()));
    let mut batch = db.batch();
    batch
        .insert_batch(&db, keys_vals)
        .expect("Failed to batch insert");

    let cap = db
        .rocksdb
        .as_range_delete()
        .expect("range delete supported");
    batch
        .schedule_delete_range(&db, &50, &100, &cap)
        .expect("Failed to delete range");

    batch.write().expect("Failed to execute batch");

    for k in 0..50 {
        assert!(db.contains_key(&k).expect("Failed to query legal key"),);
    }
    for k in 50..100 {
        assert!(!db.contains_key(&k).expect("Failed to query legal key"));
    }

    // range operator is not inclusive of to
    assert!(db.contains_key(&100).expect("Failed to query legal key"));
}

#[tokio::test]
async fn test_clear() {
    let db = {
        let _lock = global_test_lock();
        DBMap::<i32, String>::open(
            temp_dir(),
            MetricConf::default(),
            None,
            Some("table"),
            &ReadWriteOptions::default(),
        )
        .expect("Failed to open storage")
    };
    // Test clear of empty map
    let _ = db.unsafe_clear();

    let keys_vals = (0..101).map(|i| (i, i.to_string()));
    let mut insert_batch = db.batch();
    insert_batch
        .insert_batch(&db, keys_vals)
        .expect("Failed to batch insert");

    insert_batch.write().expect("Failed to execute batch");

    // Check we have multiple entries
    assert!(db.safe_iter().expect("failed to get iterator").count() > 1);
    let _ = db.unsafe_clear();
    assert_eq!(db.safe_iter().expect("failed to get iterator").count(), 0);
    // Clear again to ensure safety when clearing empty map
    let _ = db.unsafe_clear();
    assert_eq!(db.safe_iter().expect("failed to get iterator").count(), 0);
    // Clear with one item
    let _ = db.insert(&1, &"e".to_string());
    assert_eq!(db.safe_iter().expect("failed to get iterator").count(), 1);
    let _ = db.unsafe_clear();
    assert_eq!(db.safe_iter().expect("failed to get iterator").count(), 0);
}

#[tokio::test]
async fn test_iter_with_bounds() {
    let db = open_map(temp_dir(), None);

    // Add [1, 50) and (50, 100) in the db
    for i in 1..100 {
        if i != 50 {
            db.insert(&i, &i.to_string()).unwrap();
        }
    }

    // Tests basic bounded scan.
    let db_iter = get_iter_with_bounds(&db, Some(20), Some(90));
    assert_eq!(
        (20..50)
            .chain(51..90)
            .map(|i| (i, i.to_string()))
            .collect::<Vec<_>>(),
        db_iter.collect::<Vec<_>>()
    );

    // Don't specify upper bound.
    let db_iter = get_iter_with_bounds(&db, Some(20), None);
    assert_eq!(
        (20..50)
            .chain(51..100)
            .map(|i| (i, i.to_string()))
            .collect::<Vec<_>>(),
        db_iter.collect::<Vec<_>>()
    );

    // Don't specify lower bound.
    let db_iter = get_iter_with_bounds(&db, None, Some(90));
    assert_eq!(
        (1..50)
            .chain(51..90)
            .map(|i| (i, i.to_string()))
            .collect::<Vec<_>>(),
        db_iter.collect::<Vec<_>>()
    );

    // Don't specify any bounds.
    let db_iter = get_iter_with_bounds(&db, None, None);
    assert_eq!(
        (1..50)
            .chain(51..100)
            .map(|i| (i, i.to_string()))
            .collect::<Vec<_>>(),
        db_iter.collect::<Vec<_>>()
    );

    // Specify a bound outside of dataset.
    let db_iter = db
        .safe_iter_with_bounds(Some(200), Some(300))
        .expect("failed to get iterator");
    assert!(db_iter.collect::<Vec<_>>().is_empty());

    // Skip to first key in the bound (bound is [1, 50))
    let db_iter = get_iter_with_bounds(&db, Some(1), Some(50));
    assert_eq!(
        (1..50).map(|i| (i, i.to_string())).collect::<Vec<_>>(),
        db_iter.collect::<Vec<_>>()
    );
}

#[rstest]
#[tokio::test]
async fn test_range_iter() {
    let db = open_map(temp_dir(), None);

    // Add [1, 50) and (50, 100) in the db
    for i in 1..100 {
        if i != 50 {
            db.insert(&i, &i.to_string()).unwrap();
        }
    }

    // Tests basic range iterating with inclusive end.
    let db_iter = get_range_iter(&db, 10..=20);
    assert_eq!(
        (10..21).map(|i| (i, i.to_string())).collect::<Vec<_>>(),
        db_iter.collect::<Vec<_>>()
    );

    // Tests range with min start and exclusive end.
    let db_iter = get_range_iter(&db, ..20);
    assert_eq!(
        (1..20).map(|i| (i, i.to_string())).collect::<Vec<_>>(),
        db_iter.collect::<Vec<_>>()
    );

    // Tests range with max end.
    let db_iter = get_range_iter(&db, 60..);
    assert_eq!(
        (60..100).map(|i| (i, i.to_string())).collect::<Vec<_>>(),
        db_iter.collect::<Vec<_>>()
    );

    // Skip to first key in the bound (bound is [1, 49))
    let db_iter = get_range_iter(&db, 1..49);
    assert_eq!(
        (1..49).map(|i| (i, i.to_string())).collect::<Vec<_>>(),
        db_iter.collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn test_is_empty() {
    let db = {
        let _lock = global_test_lock();
        DBMap::<i32, String>::open(
            temp_dir(),
            MetricConf::default(),
            None,
            Some("table"),
            &ReadWriteOptions::default(),
        )
        .expect("Failed to open storage")
    };

    // Test empty map is truly empty
    assert!(db.is_empty());
    let _ = db.unsafe_clear();
    assert!(db.is_empty());

    let keys_vals = (0..101).map(|i| (i, i.to_string()));
    let mut insert_batch = db.batch();
    insert_batch
        .insert_batch(&db, keys_vals)
        .expect("Failed to batch insert");

    insert_batch.write().expect("Failed to execute batch");

    // Check we have multiple entries and not empty
    assert!(db.safe_iter().expect("failed to get iterator").count() > 1);
    assert!(!db.is_empty());

    // Clear again to ensure empty works after clearing
    let _ = db.unsafe_clear();
    assert_eq!(db.safe_iter().expect("failed to get iterator").count(), 0);
    assert!(db.is_empty());
}

#[tokio::test]
async fn test_multi_insert() {
    // Init a DB
    let db: DBMap<i32, String> = open_map(temp_dir(), Some("table"));
    // Create kv pairs
    let keys_vals = (0..101).map(|i| (i, i.to_string()));

    db.multi_insert(keys_vals.clone())
        .expect("Failed to multi-insert");

    for (k, v) in keys_vals {
        let val = db.get(&k).expect("Failed to get inserted key");
        assert_eq!(Some(v), val);
    }
}

#[tokio::test]
async fn test_checkpoint() {
    let path_prefix = temp_dir();
    let db_path = path_prefix.join("db");
    let db: DBMap<i32, String> = open_map(db_path, Some("table"));
    // Create kv pairs
    let keys_vals = (0..101).map(|i| (i, i.to_string()));

    db.multi_insert(keys_vals.clone())
        .expect("Failed to multi-insert");
    let checkpointed_path = path_prefix.join("checkpointed_db");
    db.rocksdb
        .checkpoint(&checkpointed_path)
        .expect("Failed to create db checkpoint");
    // Create more kv pairs
    let new_keys_vals = (101..201).map(|i| (i, i.to_string()));
    db.multi_insert(new_keys_vals.clone())
        .expect("Failed to multi-insert");
    // Verify checkpoint
    let checkpointed_db: DBMap<i32, String> = open_map(checkpointed_path, Some("table"));
    // Ensure keys inserted before checkpoint are present in original and checkpointed db
    for (k, v) in keys_vals {
        let val = db.get(&k).expect("Failed to get inserted key");
        assert_eq!(Some(v.clone()), val);
        let val = checkpointed_db.get(&k).expect("Failed to get inserted key");
        assert_eq!(Some(v), val);
    }
    // Ensure keys inserted after checkpoint are only present in original db but not
    // in checkpointed db
    for (k, v) in new_keys_vals {
        let val = db.get(&k).expect("Failed to get inserted key");
        assert_eq!(Some(v.clone()), val);
        let val = checkpointed_db.get(&k).expect("Failed to get inserted key");
        assert_eq!(None, val);
    }
}

#[tokio::test]
async fn test_multi_remove() {
    // Init a DB
    let db: DBMap<i32, String> = open_map(temp_dir(), Some("table"));

    // Create kv pairs
    let keys_vals = (0..101).map(|i| (i, i.to_string()));

    db.multi_insert(keys_vals.clone())
        .expect("Failed to multi-insert");

    // Check insertion
    for (k, v) in keys_vals.clone() {
        let val = db.get(&k).expect("Failed to get inserted key");
        assert_eq!(Some(v), val);
    }

    // Remove 50 items
    db.multi_remove(keys_vals.clone().map(|kv| kv.0).take(50))
        .expect("Failed to multi-remove");
    assert_eq!(
        db.safe_iter().expect("failed to get iterator").count(),
        101 - 50
    );

    // Check that the remaining are present
    for (k, v) in keys_vals.skip(50) {
        let val = db.get(&k).expect("Failed to get inserted key");
        assert_eq!(Some(v), val);
    }
}

fn open_map<P: AsRef<Path>, K, V>(path: P, opt_cf: Option<&str>) -> DBMap<K, V> {
    let _lock = global_test_lock();

    DBMap::<K, V>::open(
        path,
        MetricConf::default(),
        None,
        opt_cf,
        &ReadWriteOptions::default(),
    )
    .expect("failed to open rocksdb")
}

fn open_rocksdb<P: AsRef<Path>>(path: P, opt_cfs: &[&str]) -> Arc<RocksDB> {
    open_cf(path, None, MetricConf::default(), opt_cfs).expect("failed to open rocksdb")
}

#[tokio::test]
async fn test_sampling() {
    let sampling_interval = SamplingInterval::new(Duration::ZERO, 10);
    for _i in 0..10 {
        assert!(!sampling_interval.sample());
    }
    assert!(sampling_interval.sample());
    for _i in 0..10 {
        assert!(!sampling_interval.sample());
    }
    assert!(sampling_interval.sample());
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_sampling_time() {
    let sampling_interval = SamplingInterval::new(Duration::from_secs(1), 10);
    for _i in 0..10 {
        assert!(!sampling_interval.sample());
    }
    assert!(!sampling_interval.sample());
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    assert!(sampling_interval.sample());
    for _i in 0..10 {
        assert!(!sampling_interval.sample());
    }
    assert!(!sampling_interval.sample());
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    assert!(sampling_interval.sample());
}

#[tokio::test]
async fn test_iterator_seek() {
    let db: DBMap<u32, String> = open_map(temp_dir(), None);

    db.insert(&123, &"123".to_string())
        .expect("Failed to insert");
    db.insert(&456, &"456".to_string())
        .expect("Failed to insert");
    db.insert(&789, &"789".to_string())
        .expect("Failed to insert");

    let mut iter = get_iter(&db);

    assert!(matches!(
        iter.key().unwrap_err(),
        TypedStoreError::IteratorNotInitialized
    ));

    iter.seek(&0).unwrap();
    assert_eq!(iter.key().unwrap(), Some(123));
    assert_eq!(
        iter.by_ref().collect::<Vec<_>>(),
        vec![
            (123, "123".to_string()),
            (456, "456".to_string()),
            (789, "789".to_string())
        ]
    );

    iter.seek(&123).unwrap();
    assert_eq!(iter.key().unwrap(), Some(123));

    iter.seek(&234).unwrap();
    assert_eq!(iter.key().unwrap(), Some(456));
    assert_eq!(
        iter.by_ref().collect::<Vec<_>>(),
        vec![(456, "456".to_string()), (789, "789".to_string())]
    );
    assert_eq!(iter.key().unwrap(), None);

    iter.seek(&567).unwrap();
    assert_eq!(iter.key().unwrap(), Some(789));

    iter.seek_to_prev(&234).unwrap();
    assert_eq!(iter.key().unwrap(), Some(123));

    iter.seek_to_prev(&123).unwrap();
    assert_eq!(iter.key().unwrap(), Some(123));

    iter.seek_to_prev(&122).unwrap();
    assert_eq!(iter.key().unwrap(), None);

    iter.seek(&789).unwrap();
    assert_eq!(iter.key().unwrap(), Some(789));

    iter.seek(&890).unwrap();
    assert_eq!(iter.key().unwrap(), None);

    iter.seek_to_last();
    assert_eq!(iter.key().unwrap(), Some(789));

    iter.seek_to_first();
    assert_eq!(iter.key().unwrap(), Some(123));

    iter.seek_to_last();
    assert_eq!(iter.key().unwrap(), Some(789));
}

#[tokio::test]
async fn test_dbmap_ticker_statistics() {
    use rocksdb::statistics::Ticker;

    // Create a temporary directory for the test
    let path = temp_dir();

    // Create database options with statistics enabled
    let mut db_options = default_db_options().options;
    db_options.enable_statistics();
    db_options.set_statistics_level(rocksdb::statistics::StatsLevel::All);

    // Create metric configuration
    let metric_conf = MetricConf::new("test_db");

    // Open the database with multiple column families
    let rocks = {
        let _lock = global_test_lock();
        open_cf_opts(
            &path,
            Some(db_options),
            metric_conf,
            &[
                ("cf1", default_db_options().options),
                ("cf2", default_db_options().options),
            ],
        )
        .expect("Failed to open database")
    };

    // Create DBMaps for different column families
    let db_cf1: DBMap<i32, String> =
        DBMap::reopen(&rocks, Some("cf1"), &ReadWriteOptions::default(), false)
            .expect("Failed to open cf1");

    let db_cf2: DBMap<i32, String> =
        DBMap::reopen(&rocks, Some("cf2"), &ReadWriteOptions::default(), false)
            .expect("Failed to open cf2");

    // Get initial ticker counts
    let initial_bytes_written = rocks.db_options().get_ticker_count(Ticker::BytesWritten);
    let initial_keys_written = rocks
        .db_options()
        .get_ticker_count(Ticker::NumberKeysWritten);
    let initial_bytes_read = rocks.db_options().get_ticker_count(Ticker::BytesRead);
    let initial_keys_read = rocks.db_options().get_ticker_count(Ticker::NumberKeysRead);

    // Perform operations on first column family
    db_cf1
        .insert(&1, &"value1".to_string())
        .expect("Failed to insert");
    db_cf1
        .insert(&2, &"value2".to_string())
        .expect("Failed to insert");
    db_cf1
        .insert(&3, &"value3".to_string())
        .expect("Failed to insert");

    // Perform operations on second column family
    db_cf2
        .insert(&10, &"value10".to_string())
        .expect("Failed to insert");
    db_cf2
        .insert(&20, &"value20".to_string())
        .expect("Failed to insert");

    // Flush both column families to ensure data is written
    db_cf1.flush().expect("Failed to flush cf1");
    db_cf2.flush().expect("Failed to flush cf2");

    // Read some data to generate read statistics
    let _val1 = db_cf1.get(&1).expect("Failed to get from cf1");
    let _val2 = db_cf1.get(&2).expect("Failed to get from cf1");
    let _val10 = db_cf2.get(&10).expect("Failed to get from cf2");

    // Get ticker counts after operations
    let final_bytes_written = rocks.db_options().get_ticker_count(Ticker::BytesWritten);
    let final_keys_written = rocks
        .db_options()
        .get_ticker_count(Ticker::NumberKeysWritten);
    let final_bytes_read = rocks.db_options().get_ticker_count(Ticker::BytesRead);
    let final_keys_read = rocks.db_options().get_ticker_count(Ticker::NumberKeysRead);

    // Verify that ticker values have increased
    assert!(
        final_bytes_written > initial_bytes_written,
        "BytesWritten should have increased: {initial_bytes_written} -> {final_bytes_written}"
    );

    assert!(
        final_keys_written > initial_keys_written,
        "NumberKeysWritten should have increased: {initial_keys_written} -> {final_keys_written}"
    );

    assert!(
        final_bytes_read > initial_bytes_read,
        "BytesRead should have increased: {initial_bytes_read} -> {final_bytes_read}"
    );

    assert!(
        final_keys_read > initial_keys_read,
        "NumberKeysRead should have increased: {initial_keys_read} -> {final_keys_read}"
    );

    // Verify that we wrote the expected number of keys
    assert_eq!(
        final_keys_written - initial_keys_written,
        5,
        "Should have written exactly 5 keys"
    );

    // Verify that we read the expected number of keys
    assert_eq!(
        final_keys_read - initial_keys_read,
        3,
        "Should have read exactly 3 keys"
    );

    // Test histogram data as well
    let histogram_data = rocks
        .db_options()
        .get_histogram_data(rocksdb::statistics::Histogram::DbWrite);
    assert!(
        histogram_data.count() > 0,
        "Write histogram should have data"
    );
    assert!(
        histogram_data.max().is_normal(),
        "Write histogram max should be a normal number"
    );

    let read_histogram_data = rocks
        .db_options()
        .get_histogram_data(rocksdb::statistics::Histogram::DbGet);
    assert!(
        read_histogram_data.count() > 0,
        "Read histogram should have data"
    );
    assert!(
        read_histogram_data.max().is_normal(),
        "Read histogram max should be a normal number"
    );
}

fn open_optimistic_rocksdb<P: AsRef<Path>>(path: P, opt_cfs: &[&str]) -> Arc<RocksDB> {
    open_cf_optimistic(path, None, MetricConf::default(), opt_cfs)
        .expect("failed to open optimistic rocksdb")
}

fn open_optimistic_map<P: AsRef<Path>, K, V>(path: P, cf: &str) -> DBMap<K, V> {
    let rocks = open_optimistic_rocksdb(path, &[cf]);
    DBMap::<K, V>::reopen(&rocks, Some(cf), &ReadWriteOptions::default(), false)
        .expect("failed to reopen optimistic cf")
}

#[tokio::test]
async fn test_optimistic_transaction_commit() {
    let db: DBMap<i32, String> = open_optimistic_map(temp_dir(), "cf");

    // Seed initial value
    db.insert(&1, &"v1".to_string()).expect("insert");

    // Start a transaction with snapshot
    let tx = db
        .rocksdb
        .as_optimistic()
        .expect("optimistic db")
        .transaction();
    let cf = db.cf().expect("cf handle");

    // Read existing value via tx, then update and commit
    let key_buf = be_fix_int_ser(&1).unwrap();
    let new_val = bcs::to_bytes(&"v2".to_string()).unwrap();
    {
        let _existing = tx
            .get_pinned_cf_opt(&cf, &key_buf, &rocksdb::ReadOptions::default())
            .expect("tx get");
        // drop pinned slice before commit
    }
    tx.put_cf(&cf, &key_buf, &new_val).expect("tx put");
    tx.commit().expect("tx commit");

    assert_eq!(db.get(&1).unwrap(), Some("v2".to_string()));
}

#[tokio::test]
async fn test_optimistic_transaction_rollback() {
    let db: DBMap<i32, String> = open_optimistic_map(temp_dir(), "cf");

    let tx = db
        .rocksdb
        .as_optimistic()
        .expect("optimistic db")
        .transaction();
    let cf = db.cf().expect("cf handle");

    let key_buf = be_fix_int_ser(&42).unwrap();
    let val = bcs::to_bytes(&"temp".to_string()).unwrap();
    tx.put_cf(&cf, &key_buf, &val).expect("tx put");

    // Rollback and ensure value is not visible
    tx.rollback().expect("tx rollback");
    assert_eq!(db.get(&42).unwrap(), None);
}

#[tokio::test]
async fn test_optimistic_transaction_conflict_and_retry() {
    let db: DBMap<i32, String> = open_optimistic_map(temp_dir(), "cf");

    // Seed initial value
    db.insert(&1, &"base".to_string()).expect("insert");

    // Two concurrent transactions with snapshots
    let handle = db.rocksdb.as_optimistic().expect("optimistic db");
    let tx1 = handle.transaction();
    let tx2 = handle.transaction();
    let cf = db.cf().expect("cf handle");

    let key_buf = be_fix_int_ser(&1).unwrap();

    // tx1 updates and commits first
    let val_tx1 = bcs::to_bytes(&"tx1".to_string()).unwrap();
    tx1.put_cf(&cf, &key_buf, &val_tx1).expect("tx1 put");
    tx1.commit().expect("tx1 commit");

    // tx2 attempts to commit a conflicting write
    let val_tx2 = bcs::to_bytes(&"tx2".to_string()).unwrap();
    tx2.put_cf(&cf, &key_buf, &val_tx2).expect("tx2 put");
    let commit_err = tx2.commit();
    assert!(commit_err.is_err(), "expected a write-conflict error");

    // Now exercise retry_transaction! by retrying until success
    let _ = retry_transaction!({
        let tx = db
            .rocksdb
            .as_optimistic()
            .expect("optimistic db")
            .transaction();
        let cf = db.cf().expect("cf handle");
        let key_buf = be_fix_int_ser(&1).unwrap();
        let final_val = bcs::to_bytes(&"final".to_string()).unwrap();
        if let Err(_e) = tx.put_cf(&cf, &key_buf, &final_val) {
            Err(TypedStoreError::RetryableTransactionError)
        } else {
            match tx.commit() {
                Ok(()) => Ok(()),
                Err(_e) => Err(TypedStoreError::RetryableTransactionError),
            }
        }
    });

    assert_eq!(db.get(&1).unwrap(), Some("final".to_string()));
}
