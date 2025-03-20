// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use walrus_core::ShardIndex;

/// Column family names used in RocksDB.
const EVENT_STORE: &str = "event_store";
const AGGREGATE_BLOB_INFO_COLUMN_FAMILY_NAME: &str = "aggregate_blob_info";
const PER_OBJECT_BLOB_INFO_COLUMN_FAMILY_NAME: &str = "per_object_blob_info";
const NODE_STATUS_COLUMN_FAMILY_NAME: &str = "node_status";
const METADATA_COLUMN_FAMILY_NAME: &str = "metadata";
const EVENT_INDEX_COLUMN_FAMILY_NAME: &str = "latest_handled_event_index";
const BASE_COLUMN_FAMILY_NAME: &str = "shard";
const PRIMARY_SLIVERS_COLUMN_FAMILY_NAME: &str = "primary-slivers";
const SECONDARY_SLIVERS_COLUMN_FAMILY_NAME: &str = "secondary-slivers";
const STATUS_COLUMN_FAMILY_NAME: &str = "status";
const SYNC_PROGRESS_COLUMN_FAMILY_NAME: &str = "sync-progress";
const PENDING_RECOVER_SLIVERS_COLUMN_FAMILY_NAME: &str = "pending-recover-slivers";

/// Returns the base column family name for a shard.
fn base_column_family_name(id: ShardIndex) -> String {
    format!("{}-{}", BASE_COLUMN_FAMILY_NAME, id.0)
}

/// Returns the name of the event store column family.
pub fn event_store_cf_name() -> &'static str {
    EVENT_STORE
}

/// Returns the name of the aggregate blob info column family.
pub fn aggregate_blob_info_cf_name() -> &'static str {
    AGGREGATE_BLOB_INFO_COLUMN_FAMILY_NAME
}

/// Returns the name of the per-object blob info column family.
pub fn per_object_blob_info_cf_name() -> &'static str {
    PER_OBJECT_BLOB_INFO_COLUMN_FAMILY_NAME
}

/// Returns the name of the node status column family.
pub fn node_status_cf_name() -> &'static str {
    NODE_STATUS_COLUMN_FAMILY_NAME
}

/// Returns the name of the metadata column family.
pub fn metadata_cf_name() -> &'static str {
    METADATA_COLUMN_FAMILY_NAME
}

/// Returns the name of the event index column family.
pub fn event_index_cf_name() -> &'static str {
    EVENT_INDEX_COLUMN_FAMILY_NAME
}

/// Returns the name of the primary slivers column family for the given shard.
pub fn primary_slivers_column_family_name(id: ShardIndex) -> String {
    format!(
        "{}/{}",
        base_column_family_name(id),
        PRIMARY_SLIVERS_COLUMN_FAMILY_NAME
    )
}

/// Returns the name of the secondary slivers column family for the given shard.
pub fn secondary_slivers_column_family_name(id: ShardIndex) -> String {
    format!(
        "{}/{}",
        base_column_family_name(id),
        SECONDARY_SLIVERS_COLUMN_FAMILY_NAME
    )
}

/// Returns the name of the shard status column family for the given shard.
pub fn shard_status_column_family_name(id: ShardIndex) -> String {
    format!(
        "{}/{}",
        base_column_family_name(id),
        STATUS_COLUMN_FAMILY_NAME
    )
}

/// Returns the name of the shard sync progress column family for the given shard.
pub fn shard_sync_progress_column_family_name(id: ShardIndex) -> String {
    format!(
        "{}/{}",
        base_column_family_name(id),
        SYNC_PROGRESS_COLUMN_FAMILY_NAME
    )
}

/// Returns the name of the pending recover slivers column family for the given shard.
pub fn pending_recover_slivers_column_family_name(id: ShardIndex) -> String {
    format!(
        "{}/{}",
        base_column_family_name(id),
        PENDING_RECOVER_SLIVERS_COLUMN_FAMILY_NAME
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_family_names() {
        assert_eq!(metadata_cf_name(), "metadata");
        assert_eq!(event_store_cf_name(), "event_store");
        assert_eq!(aggregate_blob_info_cf_name(), "aggregate_blob_info");
        assert_eq!(per_object_blob_info_cf_name(), "per_object_blob_info");
        assert_eq!(node_status_cf_name(), "node_status");
        assert_eq!(event_index_cf_name(), "latest_handled_event_index");

        let shard = ShardIndex(900);
        assert_eq!(base_column_family_name(shard), "shard-900");
        assert_eq!(shard_status_column_family_name(shard), "shard-900/status");
        assert_eq!(
            shard_sync_progress_column_family_name(shard),
            "shard-900/sync-progress"
        );
        assert_eq!(
            pending_recover_slivers_column_family_name(shard),
            "shard-900/pending-recover-slivers"
        );
        assert_eq!(
            primary_slivers_column_family_name(shard),
            "shard-900/primary-slivers"
        );
        assert_eq!(
            secondary_slivers_column_family_name(shard),
            "shard-900/secondary-slivers"
        );
    }
}
