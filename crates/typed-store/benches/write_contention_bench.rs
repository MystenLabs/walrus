// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! This benchmark is designed to demonstrate the blocking behavior of large synchronous writes
//! on the Tokio runtime. It compares three scenarios:
//! 1.  **Synchronous writes:** `db.insert()` is called directly on the Tokio core thread.
//! 2.  **Asynchronous writes:** `db.insert_async()` is called which does serialization
//!     in the tokio thread and offloads the write to a blocking thread.
//! 3.  **Asynchronous writes:** `db.insert_full_async()` is called which offloads
//!     both serialization and the write to a blocking thread.
//!
//! How it works:
//! A background "heartbeat" task is run, which attempts to increment
//! a counter every millisecond. If the runtime is blocked by a long-running synchronous
//! operation, the heartbeat task will be starved, and the counter will not increment as expected.
//! The primary endpoint we are measuring is:
//!
//! - The number of "heartbeats" missed during the writes, which is a direct measure of
//!   how long the Tokio runtime was blocked and unable to process other tasks.
//!
//! Expected results:
//! - For small blob sizes, both methods should perform similarly with minimal blocking.
//! - For large blob sizes (e.g., >1MB), the synchronous method will show a significant
//!   increase in execution time and a high number of missed heartbeats, proving that it
//!   is blocking the runtime.
//! - The `spawn_blocking` method will show consistent performance and minimal missed
//!   heartbeats across all blob sizes, demonstrating that it correctly offloads the
//!   blocking work and keeps the runtime responsive.
//! How to run:
//! ```
//! cargo bench --bench write_contention_bench -- --verbose
//! ```

#![allow(clippy::unwrap_used)]

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use once_cell::sync::Lazy;
use rand::{Rng, prelude::*};
use tempfile::tempdir;
use tokio::runtime::Runtime;
use typed_store::{
    rocks::{DBMap, MetricConf, ReadWriteOptions, open_cf_opts},
    traits::Map,
};

const BLOB_SIZES: [usize; 5] = [
    1 * 1024,
    128 * 1024,
    1 * 1024 * 1024,
    4 * 1024 * 1024,
    10 * 1024 * 1024,
];
const NUM_WRITES: usize = 10;
const KEY_SIZE: usize = 32;

static RUNTIME: Lazy<Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
});

struct DB {
    write_contention: DBMap<Vec<u8>, Vec<u8>>,
}

/// Creates a temporary RocksDB instance for benchmarking.
async fn setup_db() -> (DB, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let mut db_opts = rocksdb::Options::default();
    db_opts.create_missing_column_families(true);
    db_opts.create_if_missing(true);
    let rocks_db = open_cf_opts(
        dir.path(),
        Some(db_opts.clone()),
        MetricConf::new("write_contention_bench"),
        &[("write_contention_bench", db_opts)],
    )
    .unwrap();

    let write_contention = DBMap::reopen(
        &rocks_db,
        Some("write_contention_bench"),
        &ReadWriteOptions::default(),
        false,
    )
    .unwrap();

    let db = DB { write_contention };

    (db, dir)
}

fn generate_kv(rng: &mut StdRng, value_size: usize) -> (Vec<u8>, Vec<u8>) {
    let key: Vec<u8> = (0..KEY_SIZE).map(|_| rng.r#gen::<u8>()).collect();
    let value: Vec<u8> = (0..value_size).map(|_| rng.r#gen::<u8>()).collect();
    (key, value)
}

/// A simple async task that increments a counter every millisecond.
/// Used to detect if the Tokio runtime is being blocked.
async fn heartbeat_task(counter: Arc<AtomicU64>, stop: Arc<AtomicU64>) {
    let mut interval = tokio::time::interval(Duration::from_millis(1));
    loop {
        tokio::select! {
            _ = interval.tick() => {
                counter.fetch_add(1, Ordering::Relaxed);
            }
            _ = async {
                while stop.load(Ordering::Relaxed) == 0 {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            } => {
                break;
            }
        }
    }
}

fn bench_sync_vs_async_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_contention");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));

    for &blob_size in &BLOB_SIZES {
        group.bench_function(
            BenchmarkId::new("insert_sync", format!("{}B", blob_size)),
            |b| {
                b.to_async(&*RUNTIME).iter_custom(|iters| async move {
                    let (db, _dir) = setup_db().await;
                    let mut rng = StdRng::seed_from_u64(42);
                    let kvs: Vec<_> = (0..NUM_WRITES)
                        .map(|_| generate_kv(&mut rng, blob_size))
                        .collect();

                    let counter = Arc::new(AtomicU64::new(0));
                    let stop = Arc::new(AtomicU64::new(0));
                    let heartbeat = tokio::spawn(heartbeat_task(counter.clone(), stop.clone()));

                    let start = Instant::now();
                    for _ in 0..iters {
                        for (key, value) in kvs.iter() {
                            db.write_contention.insert(key, value).unwrap();
                            tokio::task::yield_now().await;
                        }
                    }
                    let elapsed = start.elapsed();

                    stop.store(1, Ordering::Relaxed);
                    heartbeat.await.unwrap();

                    let expected_heartbeats = elapsed.as_millis() as u64;
                    let actual_heartbeats = counter.load(Ordering::Relaxed);
                    let missed_heartbeats = expected_heartbeats.saturating_sub(actual_heartbeats);
                    let miss_percentage = if expected_heartbeats > 0 {
                        (missed_heartbeats as f64 / expected_heartbeats as f64) * 100.0
                    } else {
                        0.0
                    };

                    println!(
                        "[sync_write {}KB iter={:?}]: elapsed: {:?},
                        missed heartbeats: {} / {} ({:.1}%)",
                        blob_size / 1024,
                        iters,
                        elapsed,
                        missed_heartbeats,
                        expected_heartbeats,
                        miss_percentage
                    );
                    elapsed
                })
            },
        );

        group.bench_function(
            BenchmarkId::new("insert_async", format!("{}B", blob_size)),
            |b| {
                b.to_async(&*RUNTIME).iter_custom(|iters| async move {
                    let (db, _dir) = setup_db().await;
                    let mut rng = StdRng::seed_from_u64(42);
                    let kvs: Vec<_> = (0..NUM_WRITES)
                        .map(|_| generate_kv(&mut rng, blob_size))
                        .collect();

                    let counter = Arc::new(AtomicU64::new(0));
                    let stop = Arc::new(AtomicU64::new(0));
                    let heartbeat = tokio::spawn(heartbeat_task(counter.clone(), stop.clone()));

                    let start = Instant::now();
                    for _ in 0..iters {
                        for (key, value) in kvs.iter() {
                            let db_map_clone = db.write_contention.clone();
                            let key_clone = key.clone();
                            let value_clone = value.clone();
                            db_map_clone
                                .insert_async(&key_clone, &value_clone)
                                .await
                                .unwrap();
                            tokio::task::yield_now().await;
                        }
                    }
                    let elapsed = start.elapsed();

                    stop.store(1, Ordering::Relaxed);
                    heartbeat.await.unwrap();

                    let expected_heartbeats = elapsed.as_millis() as u64;
                    let actual_heartbeats = counter.load(Ordering::Relaxed);
                    let missed_heartbeats = expected_heartbeats.saturating_sub(actual_heartbeats);
                    let miss_percentage = if expected_heartbeats > 0 {
                        (missed_heartbeats as f64 / expected_heartbeats as f64) * 100.0
                    } else {
                        0.0
                    };

                    println!(
                        "[sync_write {}KB iter={:?}]: elapsed: {:?},
                        missed heartbeats: {} / {} ({:.1}%)",
                        blob_size / 1024,
                        iters,
                        elapsed,
                        missed_heartbeats,
                        expected_heartbeats,
                        miss_percentage
                    );
                    elapsed
                })
            },
        );

        group.bench_function(
            BenchmarkId::new("insert_async full", format!("{}B", blob_size)),
            |b| {
                b.to_async(&*RUNTIME).iter_custom(|iters| async move {
                    let (db, _dir) = setup_db().await;
                    let mut rng = StdRng::seed_from_u64(42);
                    let kvs: Vec<_> = (0..NUM_WRITES)
                        .map(|_| generate_kv(&mut rng, blob_size))
                        .collect();

                    let counter = Arc::new(AtomicU64::new(0));
                    let stop = Arc::new(AtomicU64::new(0));
                    let heartbeat = tokio::spawn(heartbeat_task(counter.clone(), stop.clone()));

                    let start = Instant::now();
                    for _ in 0..iters {
                        for (key, value) in kvs.iter() {
                            let db_map_clone = db.write_contention.clone();
                            let key_clone = key.clone();
                            let value_clone = value.clone();
                            db_map_clone
                                .insert_full_async(key_clone, value_clone)
                                .await
                                .unwrap();
                            tokio::task::yield_now().await;
                        }
                    }
                    let elapsed = start.elapsed();

                    stop.store(1, Ordering::Relaxed);
                    heartbeat.await.unwrap();

                    let expected_heartbeats = elapsed.as_millis() as u64;
                    let actual_heartbeats = counter.load(Ordering::Relaxed);
                    let missed_heartbeats = expected_heartbeats.saturating_sub(actual_heartbeats);

                    let miss_percentage = if expected_heartbeats > 0 {
                        (missed_heartbeats as f64 / expected_heartbeats as f64) * 100.0
                    } else {
                        0.0
                    };

                    println!(
                        "[sync_write {}KB iter={:?}]: elapsed: {:?},
                        missed heartbeats: {} / {} ({:.1}%)",
                        blob_size / 1024,
                        iters,
                        elapsed,
                        missed_heartbeats,
                        expected_heartbeats,
                        miss_percentage
                    );
                    elapsed
                })
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_sync_vs_async_writes);
criterion_main!(benches);
