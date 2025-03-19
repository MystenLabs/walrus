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
const PRIMARY_SLIVERS_COLUMN_FAMILY_NAME: &str = "primary-slivers";
const SECONDARY_SLIVERS_COLUMN_FAMILY_NAME: &str = "secondary-slivers";
const BASE_COLUMN_FAMILY_NAME: &str = "shard";
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
    base_column_family_name(id) + PRIMARY_SLIVERS_COLUMN_FAMILY_NAME
}

/// Returns the name of the secondary slivers column family for the given shard.
pub fn secondary_slivers_column_family_name(id: ShardIndex) -> String {
    base_column_family_name(id) + SECONDARY_SLIVERS_COLUMN_FAMILY_NAME
}

/// Returns the name of the shard status column family for the given shard.
pub fn shard_status_column_family_name(id: ShardIndex) -> String {
    base_column_family_name(id) + STATUS_COLUMN_FAMILY_NAME
}

/// Returns the name of the shard sync progress column family for the given shard.
pub fn shard_sync_progress_column_family_name(id: ShardIndex) -> String {
    base_column_family_name(id) + SYNC_PROGRESS_COLUMN_FAMILY_NAME
}

/// Returns the name of the pending recover slivers column family for the given shard.
pub fn pending_recover_slivers_column_family_name(id: ShardIndex) -> String {
    base_column_family_name(id) + PENDING_RECOVER_SLIVERS_COLUMN_FAMILY_NAME
}
