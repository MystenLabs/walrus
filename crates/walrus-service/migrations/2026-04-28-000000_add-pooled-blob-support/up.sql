-- Pooled blob support in the backup service.
--
-- A blob_id can be referenced by regular Blob objects (BlobCertified) and/or
-- by storage pools (PooledBlobCertified). The archive must live until every
-- source has expired.
--
-- Pool lifetimes are kept in `storage_pool_state`. Pool membership is kept in
-- `pooled_blob_ref`, keyed on (storage_pool_id, blob_id) — the on-chain
-- contract guarantees that a given (pool, blob_id) pair is unique, so we
-- collapse to that granularity rather than tracking individual PooledBlob
-- objects.
--
-- `blob_state.pool_ref_count` is a denormalized counter of live pool refs for
-- each blob. Pool-related event handlers maintain it transactionally with
-- inserts/deletes on `pooled_blob_ref`. A blob is GC-eligible iff its
-- regular `end_epoch + offset` has lapsed (or is NULL) AND `pool_ref_count`
-- has reached zero. The counter design lets the GC's hot path read a single
-- row to decide eligibility — no JOIN, no classifier, no deferral.
--
-- A separate pool-expiry GC job drains expired pools' refs in bounded batches,
-- atomically deleting each ref and decrementing its blob's counter. Once a
-- pool's last ref is gone, its `storage_pool_state` row is removed.

CREATE TABLE storage_pool_state (
    -- Sui object ID of the storage pool.
    storage_pool_id      BYTEA                    NOT NULL,
    -- Inclusive epoch in which the pool was created.
    start_epoch          BIGINT                   NOT NULL,
    -- Exclusive end epoch. Monotonically increased by StoragePoolExtended.
    end_epoch            BIGINT                   NOT NULL,
    -- When this pool was first observed by the orchestrator.
    created_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- Version of the orchestrator that last touched this row.
    orchestrator_version TEXT                     NOT NULL,
    PRIMARY KEY (storage_pool_id),
    CONSTRAINT valid_storage_pool_id     CHECK (LENGTH(storage_pool_id) = 32),
    CONSTRAINT valid_storage_pool_epochs CHECK (end_epoch >= start_epoch)
);

-- Pool-expiry GC scans this index to find pools to drain.
CREATE INDEX storage_pool_state_end_epoch ON storage_pool_state (end_epoch);

-- One row per (storage_pool, blob_id) pair. The on-chain contract guarantees
-- this pair is unique, so we don't need a per-object_id row.
CREATE TABLE pooled_blob_ref (
    storage_pool_id BYTEA                    NOT NULL,
    blob_id         BYTEA                    NOT NULL,
    certified_at    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (storage_pool_id, blob_id),
    CONSTRAINT valid_pooled_blob_id         CHECK (LENGTH(blob_id) = 32),
    CONSTRAINT valid_pooled_storage_pool_id CHECK (LENGTH(storage_pool_id) = 32)
    -- No FK to storage_pool_state: Sui checkpoint ordering already guarantees
    -- the pool row exists first; we don't want a hard dependency that blocks
    -- event replay if state ever drifts.
);

-- Pool-expiry GC scans by storage_pool_id to drain refs of expired pools.
-- The PK already orders (storage_pool_id, blob_id), so this index is the
-- left prefix of the PK and is satisfied by the PK itself — no separate
-- index needed.

-- end_epoch is now nullable. NULL means "no regular-source lifetime — pool
-- refs are authoritative for this blob_id."
ALTER TABLE blob_state ALTER COLUMN end_epoch DROP NOT NULL;

-- Live pool ref count for this blob. Maintained transactionally by the
-- PooledBlobCertified, PooledBlobDeleted, and pool-expiry GC code paths.
ALTER TABLE blob_state ADD COLUMN pool_ref_count INT NOT NULL DEFAULT 0;
ALTER TABLE blob_state
    ADD CONSTRAINT pool_ref_count_non_negative CHECK (pool_ref_count >= 0);

-- Replace the old end_epoch-keyed GC index with one that narrows to the
-- actually-eligible set: archived AND no pool refs. Within those rows, the
-- GC's WHERE clause filters by end_epoch + offset against current_epoch
-- and by initiate_gc_after for retry backoff. Live pool-only blobs have
-- pool_ref_count > 0 and never appear here — no daily re-check.
DROP INDEX IF EXISTS blob_state_garbage_collection;

CREATE INDEX blob_state_gc_eligible
    ON blob_state (end_epoch NULLS FIRST)
    WHERE state = 'archived' AND pool_ref_count = 0;
