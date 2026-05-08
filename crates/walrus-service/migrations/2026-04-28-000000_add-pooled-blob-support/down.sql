-- Rolling back implies abandoning pool support. Pool-only rows can't coexist
-- with a NOT NULL end_epoch, so backfill with 0 first.
UPDATE blob_state SET end_epoch = 0 WHERE end_epoch IS NULL;
ALTER TABLE blob_state ALTER COLUMN end_epoch SET NOT NULL;

ALTER TABLE blob_state DROP CONSTRAINT IF EXISTS pool_ref_count_non_negative;
ALTER TABLE blob_state DROP COLUMN IF EXISTS pool_ref_count;

DROP INDEX IF EXISTS blob_state_gc_eligible;

CREATE INDEX blob_state_garbage_collection
    ON blob_state (end_epoch)
    WHERE state = 'archived';

DROP TABLE IF EXISTS pooled_blob_ref;
DROP TABLE IF EXISTS storage_pool_state;
