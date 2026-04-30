// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the backup module's pooled-blob support.
//!
//! These tests exercise the SQL paths that maintain `pool_ref_count` and the pool-expiry
//! GC. They require a real Postgres instance (the production code uses Postgres-specific
//! syntax like `ON CONFLICT … DO UPDATE`, `WITH … RETURNING`, partial indexes), so each
//! test connects to `WALRUS_BACKUP_TEST_DATABASE_URL` and creates a unique schema for
//! isolation; no `TEST_DATABASE_URL` set means the test is skipped (returns Ok).
//!
//! Run with:
//!     WALRUS_BACKUP_TEST_DATABASE_URL=postgres://… \
//!         cargo test -p walrus-service --features backup backup::tests -- --ignored

use std::{
    env,
    sync::atomic::{AtomicU64, Ordering},
};

use diesel::{
    Connection as _,
    QueryableByName,
    sql_types::{Bytea, Int4, Int8, Text},
};
use diesel_async::{AsyncConnection as _, AsyncPgConnection, RunQueryDsl as _};
use diesel_migrations::MigrationHarness;
use sui_types::base_types::ObjectID;
use walrus_core::{BlobId, Epoch};
use walrus_sui::{
    test_utils::{event_id_for_testing, fixed_event_id_for_testing},
    types::{
        BlobCertified,
        BlobEvent,
        ContractEvent,
        EpochChangeEvent,
        EpochChangeStart,
        PooledBlobCertified,
        PooledBlobDeleted,
        StoragePoolCreatedEvent,
        StoragePoolEvent,
        StoragePoolExtendedEvent,
    },
};

use super::{
    garbage_collector::{cleanup_drained_pools, drain_expired_pool_refs_step},
    service::{MIGRATIONS, dispatch_contract_event},
};

const TEST_VERSION: &str = "test";
const ENV_VAR: &str = "WALRUS_BACKUP_TEST_DATABASE_URL";

/// Test-local unique ObjectID generator. The shared `object_id_for_testing()` helper in
/// `walrus_sui::test_utils` returns a fixed value every call, which collapses distinct
/// pools/objects to the same key in our schema.
fn unique_object_id() -> ObjectID {
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut bytes = [0u8; ObjectID::LENGTH];
    bytes[ObjectID::LENGTH - 8..].copy_from_slice(&n.to_be_bytes());
    ObjectID::new(bytes)
}

// --- Test harness ---

/// Each test gets its own schema in the shared test database. Schema names are unique
/// per test invocation, so tests can run concurrently without colliding on table names.
struct TestDb {
    base_url: String,
    schema: String,
}

impl TestDb {
    fn new() -> Option<Self> {
        let base_url = env::var(ENV_VAR).ok()?;
        // Schema name combines pid + nanosecond timestamp + a process-local counter so it's
        // unique even when tests run in parallel across multiple processes (nextest) or hit
        // the same nanosecond from different threads.
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let schema = format!(
            "walrus_backup_test_{pid}_{nanos}_{n}",
            pid = std::process::id(),
            nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos(),
            n = COUNTER.fetch_add(1, Ordering::Relaxed),
        );
        Some(Self { base_url, schema })
    }

    async fn connect(&self) -> AsyncPgConnection {
        let mut conn = AsyncPgConnection::establish(&self.base_url)
            .await
            .expect("connect to test database");
        // Drop and recreate the per-test schema, then point session search_path at it.
        diesel::sql_query(format!(
            "DROP SCHEMA IF EXISTS {schema} CASCADE",
            schema = self.schema
        ))
        .execute(&mut conn)
        .await
        .expect("drop schema");
        diesel::sql_query(format!("CREATE SCHEMA {schema}", schema = self.schema))
            .execute(&mut conn)
            .await
            .expect("create schema");
        diesel::sql_query(format!("SET search_path TO {schema}", schema = self.schema))
            .execute(&mut conn)
            .await
            .expect("set search_path");
        conn
    }

    /// Apply the embedded migrations to the test schema using a sync connection (Diesel's
    /// migration harness is sync-only). The sync and async connections share the schema.
    fn run_migrations(&self) {
        let mut sync_conn =
            diesel::PgConnection::establish(&self.base_url).expect("connect for migration");
        diesel::RunQueryDsl::execute(
            diesel::sql_query(format!("SET search_path TO {schema}", schema = self.schema)),
            &mut sync_conn,
        )
        .expect("set migration search_path");
        sync_conn
            .run_pending_migrations(MIGRATIONS)
            .expect("run migrations");
    }
}

impl Drop for TestDb {
    fn drop(&mut self) {
        let base_url = self.base_url.clone();
        let schema = self.schema.clone();
        // Use a sync connection to clean up; we're in a Drop so we can't .await.
        if let Ok(mut conn) = diesel::PgConnection::establish(&base_url) {
            let _ = diesel::RunQueryDsl::execute(
                diesel::sql_query(format!("DROP SCHEMA IF EXISTS {schema} CASCADE")),
                &mut conn,
            );
        }
    }
}

async fn setup() -> Option<(TestDb, AsyncPgConnection)> {
    let db = TestDb::new()?;
    // Order matters: connect (which creates the per-test schema) before running
    // migrations into it.
    let conn = db.connect().await;
    db.run_migrations();
    Some((db, conn))
}

// --- Event factories ---

fn pool_event(storage_pool_id: ObjectID, start_epoch: Epoch, end_epoch: Epoch) -> ContractEvent {
    ContractEvent::StoragePoolEvent(StoragePoolEvent::StoragePoolCreated(
        StoragePoolCreatedEvent {
            epoch: start_epoch,
            storage_pool_id,
            reserved_encoded_capacity_bytes: 0,
            start_epoch,
            end_epoch,
            event_id: event_id_for_testing(),
        },
    ))
}

fn pool_extend_event(storage_pool_id: ObjectID, new_end_epoch: Epoch) -> ContractEvent {
    ContractEvent::StoragePoolEvent(StoragePoolEvent::StoragePoolExtended(
        StoragePoolExtendedEvent {
            epoch: 1,
            storage_pool_id,
            new_end_epoch,
            event_id: event_id_for_testing(),
        },
    ))
}

fn pool_certify(blob_id: BlobId, storage_pool_id: ObjectID) -> ContractEvent {
    ContractEvent::BlobEvent(BlobEvent::PooledBlobCertified(PooledBlobCertified {
        epoch: 1,
        blob_id,
        deletable: false,
        object_id: unique_object_id(),
        storage_pool_id,
        event_id: event_id_for_testing(),
    }))
}

fn pool_delete(blob_id: BlobId, storage_pool_id: ObjectID) -> ContractEvent {
    ContractEvent::BlobEvent(BlobEvent::PooledBlobDeleted(PooledBlobDeleted {
        epoch: 1,
        blob_id,
        object_id: unique_object_id(),
        was_certified: true,
        storage_pool_id,
        event_id: event_id_for_testing(),
    }))
}

fn regular_certify(blob_id: BlobId, end_epoch: Epoch) -> ContractEvent {
    ContractEvent::BlobEvent(BlobEvent::Certified(BlobCertified {
        epoch: 1,
        blob_id,
        end_epoch,
        deletable: false,
        is_extension: false,
        object_id: unique_object_id(),
        event_id: fixed_event_id_for_testing(0),
    }))
}

fn epoch_change(epoch: Epoch) -> ContractEvent {
    ContractEvent::EpochChangeEvent(EpochChangeEvent::EpochChangeStart(EpochChangeStart {
        epoch,
        event_id: event_id_for_testing(),
    }))
}

// --- Inspection helpers ---

#[derive(QueryableByName)]
struct PoolRefCount {
    #[diesel(sql_type = Int4)]
    pool_ref_count: i32,
}

#[derive(QueryableByName)]
struct State {
    #[diesel(sql_type = Text)]
    state: String,
}

#[derive(QueryableByName)]
struct Count {
    #[diesel(sql_type = Int8)]
    n: i64,
}

async fn pool_ref_count(conn: &mut AsyncPgConnection, blob_id: BlobId) -> Option<i32> {
    diesel::sql_query("SELECT pool_ref_count FROM blob_state WHERE blob_id = $1")
        .bind::<Bytea, _>(blob_id.0.to_vec())
        .get_result::<PoolRefCount>(conn)
        .await
        .ok()
        .map(|r| r.pool_ref_count)
}

async fn pooled_ref_count(conn: &mut AsyncPgConnection, blob_id: BlobId) -> i64 {
    diesel::sql_query("SELECT COUNT(*)::bigint AS n FROM pooled_blob_ref WHERE blob_id = $1")
        .bind::<Bytea, _>(blob_id.0.to_vec())
        .get_result::<Count>(conn)
        .await
        .expect("query pooled_blob_ref")
        .n
}

async fn pool_state_count(conn: &mut AsyncPgConnection, pool_id: ObjectID) -> i64 {
    diesel::sql_query(
        "SELECT COUNT(*)::bigint AS n FROM storage_pool_state WHERE storage_pool_id = $1",
    )
    .bind::<Bytea, _>(pool_id.to_vec())
    .get_result::<Count>(conn)
    .await
    .expect("query storage_pool_state")
    .n
}

async fn blob_state_state(conn: &mut AsyncPgConnection, blob_id: BlobId) -> Option<String> {
    diesel::sql_query("SELECT state FROM blob_state WHERE blob_id = $1")
        .bind::<Bytea, _>(blob_id.0.to_vec())
        .get_result::<State>(conn)
        .await
        .ok()
        .map(|r| r.state)
}

/// Forces `blob_state.state` to 'archived' so we can exercise the GC eligibility
/// query without having to plumb through the fetcher's actual upload path.
async fn force_archived(conn: &mut AsyncPgConnection, blob_id: BlobId) {
    diesel::sql_query(
        "
            UPDATE blob_state
            SET state = 'archived', backup_url = 'test://', initiate_fetch_after = NULL,
                retry_count = NULL
            WHERE blob_id = $1
        ",
    )
    .bind::<Bytea, _>(blob_id.0.to_vec())
    .execute(conn)
    .await
    .expect("force archived");
}

const GC_ELIGIBLE_QUERY: &str = "
    SELECT blob_id FROM blob_state
    WHERE state = 'archived'
        AND pool_ref_count = 0
        AND (end_epoch IS NULL
            OR end_epoch + $1
                <= COALESCE((SELECT MAX(epoch) FROM epoch_change_start_event), 0))
        AND (initiate_gc_after IS NULL OR initiate_gc_after < NOW())
";

#[derive(QueryableByName)]
struct BlobIdRow {
    #[diesel(sql_type = Bytea)]
    blob_id: Vec<u8>,
}

async fn gc_eligible_blob_ids(conn: &mut AsyncPgConnection, offset: i64) -> Vec<BlobId> {
    diesel::sql_query(GC_ELIGIBLE_QUERY)
        .bind::<Int8, _>(offset)
        .get_results::<BlobIdRow>(conn)
        .await
        .expect("eligibility query")
        .into_iter()
        .map(|r| BlobId::try_from(r.blob_id.as_slice()).expect("blob_id"))
        .collect()
}

async fn dispatch(conn: &mut AsyncPgConnection, event: &ContractEvent) {
    dispatch_contract_event(TEST_VERSION, event, conn)
        .await
        .expect("dispatch");
}

// --- Tests ---

/// `PooledBlobCertified` inserts the ref row and bumps `pool_ref_count` by exactly 1.
/// Replaying the same event leaves both invariant.
#[tokio::test]
#[ignore = "requires WALRUS_BACKUP_TEST_DATABASE_URL"]
async fn pool_certify_increments_counter_idempotently() {
    let Some((_db, mut conn)) = setup().await else {
        return;
    };
    let blob_id = walrus_core::test_utils::blob_id_from_u64(1);
    let pool_id = unique_object_id();

    dispatch(&mut conn, &pool_event(pool_id, 1, 100)).await;
    dispatch(&mut conn, &pool_certify(blob_id, pool_id)).await;

    assert_eq!(pool_ref_count(&mut conn, blob_id).await, Some(1));
    assert_eq!(pooled_ref_count(&mut conn, blob_id).await, 1);

    // Replay: same (pool, blob) → ON CONFLICT DO NOTHING; counter unchanged.
    dispatch(&mut conn, &pool_certify(blob_id, pool_id)).await;
    assert_eq!(pool_ref_count(&mut conn, blob_id).await, Some(1));
    assert_eq!(pooled_ref_count(&mut conn, blob_id).await, 1);
}

/// A blob in two distinct pools has `pool_ref_count = 2`. Each `PooledBlobDeleted`
/// decrements by exactly 1, and the row vanishes from `pooled_blob_ref`.
#[tokio::test]
#[ignore = "requires WALRUS_BACKUP_TEST_DATABASE_URL"]
async fn pool_certify_and_delete_two_pools_round_trip() {
    let Some((_db, mut conn)) = setup().await else {
        return;
    };
    let blob_id = walrus_core::test_utils::blob_id_from_u64(2);
    let pool_a = unique_object_id();
    let pool_b = unique_object_id();

    dispatch(&mut conn, &pool_event(pool_a, 1, 100)).await;
    dispatch(&mut conn, &pool_event(pool_b, 1, 100)).await;
    dispatch(&mut conn, &pool_certify(blob_id, pool_a)).await;
    dispatch(&mut conn, &pool_certify(blob_id, pool_b)).await;
    assert_eq!(pool_ref_count(&mut conn, blob_id).await, Some(2));

    dispatch(&mut conn, &pool_delete(blob_id, pool_a)).await;
    assert_eq!(pool_ref_count(&mut conn, blob_id).await, Some(1));
    assert_eq!(pooled_ref_count(&mut conn, blob_id).await, 1);

    // Replay of the same delete is a no-op (ref already gone, counter not decremented).
    dispatch(&mut conn, &pool_delete(blob_id, pool_a)).await;
    assert_eq!(pool_ref_count(&mut conn, blob_id).await, Some(1));

    dispatch(&mut conn, &pool_delete(blob_id, pool_b)).await;
    assert_eq!(pool_ref_count(&mut conn, blob_id).await, Some(0));
    assert_eq!(pooled_ref_count(&mut conn, blob_id).await, 0);
}

/// `StoragePoolExtended` keeps `end_epoch` monotonic via `GREATEST`. A lower extension
/// is ignored.
#[tokio::test]
#[ignore = "requires WALRUS_BACKUP_TEST_DATABASE_URL"]
async fn pool_extend_is_monotonic() {
    let Some((_db, mut conn)) = setup().await else {
        return;
    };
    let pool_id = unique_object_id();

    dispatch(&mut conn, &pool_event(pool_id, 1, 100)).await;
    dispatch(&mut conn, &pool_extend_event(pool_id, 200)).await;
    dispatch(&mut conn, &pool_extend_event(pool_id, 50)).await;

    #[derive(QueryableByName)]
    struct EndEpoch {
        #[diesel(sql_type = Int8)]
        end_epoch: i64,
    }
    let row =
        diesel::sql_query("SELECT end_epoch FROM storage_pool_state WHERE storage_pool_id = $1")
            .bind::<Bytea, _>(pool_id.to_vec())
            .get_result::<EndEpoch>(&mut conn)
            .await
            .expect("query pool");
    assert_eq!(row.end_epoch, 200);
}

/// Pool-expiry GC drains all refs of an expired pool across multiple steps,
/// decrementing the corresponding blobs' counters by exactly the number of refs each
/// owned in that pool. After the last ref is drained, the pool's `storage_pool_state`
/// row is removed by the cleanup step.
#[tokio::test]
#[ignore = "requires WALRUS_BACKUP_TEST_DATABASE_URL"]
async fn drain_expired_pool_refs_step_decrements_and_cleans_up() {
    let Some((_db, mut conn)) = setup().await else {
        return;
    };
    let pool_id = unique_object_id();

    // Pool with end_epoch=10. Three blobs in it.
    dispatch(&mut conn, &pool_event(pool_id, 1, 10)).await;
    let blob_ids: Vec<_> = (10..13)
        .map(walrus_core::test_utils::blob_id_from_u64)
        .collect();
    for &b in &blob_ids {
        dispatch(&mut conn, &pool_certify(b, pool_id)).await;
    }
    for &b in &blob_ids {
        assert_eq!(pool_ref_count(&mut conn, b).await, Some(1));
    }

    // Advance the observed epoch so `end_epoch + offset (=0) <= current_epoch`.
    dispatch(&mut conn, &epoch_change(20)).await;

    // First drain step removes all three refs (well within the 200-row batch cap).
    let drained = drain_expired_pool_refs_step(&mut conn, 0)
        .await
        .expect("drain");
    assert_eq!(drained, 3);
    for &b in &blob_ids {
        assert_eq!(pool_ref_count(&mut conn, b).await, Some(0));
    }
    assert_eq!(pooled_ref_count(&mut conn, blob_ids[0]).await, 0);

    // After cleanup, the pool's row is gone.
    cleanup_drained_pools(&mut conn, 0).await.expect("cleanup");
    assert_eq!(pool_state_count(&mut conn, pool_id).await, 0);

    // A second drain step is a no-op.
    let drained_again = drain_expired_pool_refs_step(&mut conn, 0)
        .await
        .expect("drain again");
    assert_eq!(drained_again, 0);
}

/// The pool-expiry sweep is bounded by the batch size: a pool with more refs than the
/// batch size needs multiple ticks to fully drain, and the pool row stays in
/// `storage_pool_state` until the last ref is gone. This is the crash-recovery
/// invariant — restarting mid-drain finds the same pool and resumes.
#[tokio::test]
#[ignore = "requires WALRUS_BACKUP_TEST_DATABASE_URL"]
async fn drain_pool_resumes_across_batches() {
    let Some((_db, mut conn)) = setup().await else {
        return;
    };
    let pool_id = unique_object_id();
    dispatch(&mut conn, &pool_event(pool_id, 1, 10)).await;
    // 250 blobs > batch size of 200 → at least two drain steps needed.
    for i in 1000..1250 {
        let b = walrus_core::test_utils::blob_id_from_u64(i);
        dispatch(&mut conn, &pool_certify(b, pool_id)).await;
    }
    dispatch(&mut conn, &epoch_change(20)).await;

    let first = drain_expired_pool_refs_step(&mut conn, 0)
        .await
        .expect("step 1");
    assert_eq!(first, 200);

    // Pool row still present because refs remain.
    cleanup_drained_pools(&mut conn, 0)
        .await
        .expect("cleanup 1");
    assert_eq!(pool_state_count(&mut conn, pool_id).await, 1);

    let second = drain_expired_pool_refs_step(&mut conn, 0)
        .await
        .expect("step 2");
    assert_eq!(second, 50);

    cleanup_drained_pools(&mut conn, 0)
        .await
        .expect("cleanup 2");
    assert_eq!(pool_state_count(&mut conn, pool_id).await, 0);
}

/// A blob with both regular and pool sources is GC-eligible only when **both** are
/// expired. The eligibility query mirrors the GC's pre-filter.
#[tokio::test]
#[ignore = "requires WALRUS_BACKUP_TEST_DATABASE_URL"]
async fn gc_eligibility_requires_all_sources_expired() {
    let Some((_db, mut conn)) = setup().await else {
        return;
    };
    let blob_only_regular_live = walrus_core::test_utils::blob_id_from_u64(20);
    let blob_only_regular_dead = walrus_core::test_utils::blob_id_from_u64(21);
    let blob_dual_pool_live = walrus_core::test_utils::blob_id_from_u64(22);
    let blob_dual_dead = walrus_core::test_utils::blob_id_from_u64(23);
    let pool_id = unique_object_id();

    // Live regular cert (end_epoch in the future).
    dispatch(&mut conn, &regular_certify(blob_only_regular_live, 100)).await;
    // Dead regular cert (end_epoch already past once we advance the epoch).
    dispatch(&mut conn, &regular_certify(blob_only_regular_dead, 5)).await;
    // Dual: regular dead, pool live (pool's end_epoch beyond current).
    dispatch(&mut conn, &pool_event(pool_id, 1, 50)).await;
    dispatch(&mut conn, &regular_certify(blob_dual_pool_live, 5)).await;
    dispatch(&mut conn, &pool_certify(blob_dual_pool_live, pool_id)).await;
    // Dual: regular dead, pool also expired-and-drained.
    dispatch(&mut conn, &regular_certify(blob_dual_dead, 5)).await;
    let dead_pool = unique_object_id();
    dispatch(&mut conn, &pool_event(dead_pool, 1, 5)).await;
    dispatch(&mut conn, &pool_certify(blob_dual_dead, dead_pool)).await;

    // Pretend we finished archiving all four.
    for b in [
        blob_only_regular_live,
        blob_only_regular_dead,
        blob_dual_pool_live,
        blob_dual_dead,
    ] {
        force_archived(&mut conn, b).await;
    }

    // Advance epoch beyond regular `end_epoch + offset(=0)` for the dead ones; pool
    // `dead_pool` is also past now.
    dispatch(&mut conn, &epoch_change(20)).await;

    // Drain the expired pool so its blobs' counters drop to 0.
    let _ = drain_expired_pool_refs_step(&mut conn, 0)
        .await
        .expect("drain");
    cleanup_drained_pools(&mut conn, 0).await.expect("cleanup");

    let eligible = gc_eligible_blob_ids(&mut conn, 0).await;
    // Only `blob_only_regular_dead` and `blob_dual_dead` should be picked.
    // `blob_only_regular_live` has end_epoch=100 > 20 → live.
    // `blob_dual_pool_live` has pool_ref_count = 1 (its pool not expired) → live.
    let eligible_set: std::collections::HashSet<BlobId> = eligible.into_iter().collect();
    assert!(eligible_set.contains(&blob_only_regular_dead));
    assert!(eligible_set.contains(&blob_dual_dead));
    assert!(!eligible_set.contains(&blob_only_regular_live));
    assert!(!eligible_set.contains(&blob_dual_pool_live));
}

/// `BlobCertified` arriving after `PooledBlobCertified` for the same blob_id keeps the
/// pool ref count intact and adds the regular-source `end_epoch`. Confirms the pool-cert
/// upsert's `DO UPDATE SET` deliberately leaves `end_epoch` alone.
#[tokio::test]
#[ignore = "requires WALRUS_BACKUP_TEST_DATABASE_URL"]
async fn regular_cert_after_pool_cert_keeps_pool_count() {
    let Some((_db, mut conn)) = setup().await else {
        return;
    };
    let blob_id = walrus_core::test_utils::blob_id_from_u64(30);
    let pool_id = unique_object_id();

    dispatch(&mut conn, &pool_event(pool_id, 1, 100)).await;
    dispatch(&mut conn, &pool_certify(blob_id, pool_id)).await;
    assert_eq!(pool_ref_count(&mut conn, blob_id).await, Some(1));

    dispatch(&mut conn, &regular_certify(blob_id, 50)).await;
    assert_eq!(pool_ref_count(&mut conn, blob_id).await, Some(1));

    #[derive(QueryableByName)]
    struct EndEpoch {
        #[diesel(sql_type = diesel::sql_types::Nullable<Int8>)]
        end_epoch: Option<i64>,
    }
    let row = diesel::sql_query("SELECT end_epoch FROM blob_state WHERE blob_id = $1")
        .bind::<Bytea, _>(blob_id.0.to_vec())
        .get_result::<EndEpoch>(&mut conn)
        .await
        .expect("query");
    assert_eq!(row.end_epoch, Some(50));
}

/// Ensure the `CHECK (pool_ref_count >= 0)` constraint is in force: a stray decrement
/// that would drive the counter below zero must be rejected by Postgres rather than
/// silently allowed. Defends against future bugs in the decrement logic.
#[tokio::test]
#[ignore = "requires WALRUS_BACKUP_TEST_DATABASE_URL"]
async fn negative_pool_ref_count_rejected() {
    let Some((_db, mut conn)) = setup().await else {
        return;
    };
    let blob_id = walrus_core::test_utils::blob_id_from_u64(40);
    let pool_id = unique_object_id();

    dispatch(&mut conn, &pool_event(pool_id, 1, 100)).await;
    dispatch(&mut conn, &pool_certify(blob_id, pool_id)).await;
    dispatch(&mut conn, &pool_delete(blob_id, pool_id)).await;
    assert_eq!(pool_ref_count(&mut conn, blob_id).await, Some(0));

    let res = diesel::sql_query(
        "UPDATE blob_state SET pool_ref_count = pool_ref_count - 1 WHERE blob_id = $1",
    )
    .bind::<Bytea, _>(blob_id.0.to_vec())
    .execute(&mut conn)
    .await;
    assert!(res.is_err(), "expected CHECK violation on negative counter");
}

/// `PooledBlobCertified` for a blob in 'archived' state keeps it archived (it's already
/// backed up); the upsert's `DO UPDATE SET` only resets state-machine columns when the
/// row was not already archived. Confirms we don't re-fetch already-archived blobs on
/// each new pool ref.
#[tokio::test]
#[ignore = "requires WALRUS_BACKUP_TEST_DATABASE_URL"]
async fn pool_certify_does_not_disturb_archived_state() {
    let Some((_db, mut conn)) = setup().await else {
        return;
    };
    let blob_id = walrus_core::test_utils::blob_id_from_u64(50);
    let pool_id = unique_object_id();
    let pool_id_2 = unique_object_id();

    dispatch(&mut conn, &pool_event(pool_id, 1, 100)).await;
    dispatch(&mut conn, &pool_event(pool_id_2, 1, 100)).await;
    dispatch(&mut conn, &pool_certify(blob_id, pool_id)).await;
    force_archived(&mut conn, blob_id).await;
    assert_eq!(
        blob_state_state(&mut conn, blob_id).await.as_deref(),
        Some("archived")
    );

    // A new pool ref arrives. Counter bumps; state stays 'archived'.
    dispatch(&mut conn, &pool_certify(blob_id, pool_id_2)).await;
    assert_eq!(pool_ref_count(&mut conn, blob_id).await, Some(2));
    assert_eq!(
        blob_state_state(&mut conn, blob_id).await.as_deref(),
        Some("archived")
    );
}
