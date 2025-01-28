-- Initialize backup-related tables.

CREATE TABLE stream_event (
    -- Sequence number of the checkpoint.
    checkpoint_sequence_number BIGINT  NOT NULL,
    -- Offset within the checkpoint
    counter                    BIGINT  NOT NULL,
    -- Digest of the transaction.
    transaction_digest         BYTEA   NOT NULL,
    -- Index since genesis event.
    event_index                BIGINT  NOT NULL,
    -- Index since first Walrus event.
    element_index              BIGINT  NOT NULL,
    -- Contents of the event.
    element                    JSONB   NOT NULL,
    PRIMARY KEY (element_index)
);

CREATE TABLE blob_state (
    -- The Walrus Blob ID.
    blob_id              BYTEA     NOT NULL,
    -- Maximum epoch this blob is valid for (for later GC.)
    end_epoch            BIGINT    NOT NULL,
    -- State of the blob.
    state                TEXT      NOT NULL,
    -- Storage location (ie: gs://some-bucket/blobs/blob_id)
    backup_url           TEXT          NULL,
    -- When was the blob created?
    created_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- When should a worker pick up this blob fetch task?
    initiate_fetch_after TIMESTAMP WITH TIME ZONE     NULL DEFAULT CURRENT_TIMESTAMP,
    -- How many times have we tried to fetch this blob?
    fetch_attempts       INT            NULL,
    PRIMARY KEY (blob_id),
    CONSTRAINT valid_blob_state
    CHECK (
        (state = 'archived'
            AND initiate_fetch_after IS NULL
            AND backup_url IS NOT NULL
            AND fetch_attempts IS NULL)
        OR (state = 'waiting'
            AND initiate_fetch_after IS NOT NULL
            AND backup_url IS NULL
            AND fetch_attempts IS NOT NULL)
        OR (state = 'deleted'
            AND initiate_fetch_after IS NULL
            AND backup_url IS NULL
            AND fetch_attempts IS NULL))
);

CREATE INDEX blob_state_delegate_after
    ON blob_state (initiate_fetch_after)
    INCLUDE (fetch_attempts)
    WHERE state = 'waiting';

CREATE INDEX blob_state_garbage_collection
    ON blob_state (end_epoch)
    WHERE state = 'archived';

CREATE TABLE epoch_change_start_event (
    -- The epoch number.
    epoch BIGINT NOT NULL,
    -- When the epoch change start event was noticed.
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (epoch)
);
