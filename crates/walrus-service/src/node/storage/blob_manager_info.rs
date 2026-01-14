// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Storage for UnifiedStorage metadata in the database.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sui_types::base_types::ObjectID;
use typed_store::{
    TypedStoreError,
    rocks::{DBMap, ReadWriteOptions, RocksDB},
    traits::Map,
};
use walrus_core::Epoch;

use super::constants;

/// Information about a UnifiedStorage stored in the database.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredUnifiedStorageInfo {
    /// The end epoch for this UnifiedStorage.
    pub end_epoch: Epoch,
    /// The BlobManager ID that owns this storage, if any.
    pub blob_manager_id: Option<ObjectID>,
}

impl StoredUnifiedStorageInfo {
    /// Creates a new StoredUnifiedStorageInfo.
    pub fn new(end_epoch: Epoch, blob_manager_id: Option<ObjectID>) -> Self {
        Self {
            end_epoch,
            blob_manager_id,
        }
    }

    /// Returns the epoch at which blobs in this storage become eligible for GC.
    /// With no grace period, GC is eligible at end_epoch.
    pub fn gc_eligible_epoch(&self) -> Epoch {
        self.end_epoch
    }

    /// Returns true if the given epoch is valid for this storage.
    #[allow(dead_code)]
    pub fn is_epoch_valid(&self, epoch: Epoch) -> bool {
        epoch < self.end_epoch
    }

    /// Returns true if data can be garbage collected at the given epoch.
    #[allow(dead_code)]
    pub fn can_gc_at(&self, current_epoch: Epoch) -> bool {
        current_epoch >= self.gc_eligible_epoch()
    }
}

/// Table for storing UnifiedStorage information.
#[derive(Debug, Clone)]
pub struct UnifiedStorageTable {
    storages: DBMap<ObjectID, StoredUnifiedStorageInfo>,
    // Secondary index: blob_manager_id -> storage_id
    blob_manager_index: DBMap<ObjectID, ObjectID>,
}

impl UnifiedStorageTable {
    /// Reopens an existing UnifiedStorageTable from the database.
    pub fn reopen(database: &Arc<RocksDB>) -> Result<Self, TypedStoreError> {
        let storages = DBMap::reopen(
            database,
            Some(constants::blob_managers_cf_name()), // Reusing existing CF name
            &ReadWriteOptions::default(),
            false,
        )?;
        let blob_manager_index = DBMap::reopen(
            database,
            Some(constants::blob_manager_index_cf_name()),
            &ReadWriteOptions::default(),
            false,
        )?;

        Ok(Self {
            storages,
            blob_manager_index,
        })
    }

    /// Gets the info for a specific UnifiedStorage.
    pub fn get(
        &self,
        storage_id: &ObjectID,
    ) -> Result<Option<StoredUnifiedStorageInfo>, TypedStoreError> {
        self.storages.get(storage_id)
    }

    /// Inserts or updates a UnifiedStorage's info.
    pub fn insert(
        &self,
        storage_id: ObjectID,
        info: StoredUnifiedStorageInfo,
    ) -> Result<(), TypedStoreError> {
        let mut batch = self.storages.batch();
        batch.insert_batch(&self.storages, [(&storage_id, &info)])?;

        // Update secondary index if blob_manager_id is present.
        if let Some(manager_id) = info.blob_manager_id {
            batch.insert_batch(&self.blob_manager_index, [(&manager_id, &storage_id)])?;
        }

        batch.write()
    }

    /// Gets the storage_id for a given blob_manager_id.
    pub fn get_storage_id(
        &self,
        blob_manager_id: &ObjectID,
    ) -> Result<Option<ObjectID>, TypedStoreError> {
        self.blob_manager_index.get(blob_manager_id)
    }

    /// Updates the end_epoch and potentially the blob_manager_id for a UnifiedStorage.
    pub fn update_storage_info(
        &self,
        storage_id: &ObjectID,
        new_end_epoch: Epoch,
        blob_manager_id: Option<ObjectID>,
    ) -> Result<(), TypedStoreError> {
        let info = StoredUnifiedStorageInfo::new(new_end_epoch, blob_manager_id);
        self.storages.insert(storage_id, &info)
    }

    /// Removes a UnifiedStorage from the table.
    #[allow(dead_code)]
    pub fn remove(&self, storage_id: &ObjectID) -> Result<(), TypedStoreError> {
        self.storages.remove(storage_id)
    }

    /// Checks if a UnifiedStorage exists.
    #[allow(dead_code)]
    pub fn contains(&self, storage_id: &ObjectID) -> Result<bool, TypedStoreError> {
        self.storages.contains_key(storage_id)
    }

    /// Returns the number of UnifiedStorage instances stored.
    #[allow(dead_code)]
    pub fn count(&self) -> Result<usize, TypedStoreError> {
        Ok(self.storages.safe_iter()?.count())
    }
}
