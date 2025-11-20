// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Storage for BlobManager metadata in the database.

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

/// Information about a BlobManager stored in the database.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredBlobManagerInfo {
    /// The end epoch for this BlobManager's storage.
    pub end_epoch: Epoch,
}

impl StoredBlobManagerInfo {
    /// Creates a new StoredBlobManagerInfo.
    pub fn new(end_epoch: Epoch) -> Self {
        Self { end_epoch }
    }

    /// Returns true if the given epoch is valid for this BlobManager.
    pub fn is_epoch_valid(&self, epoch: Epoch) -> bool {
        epoch < self.end_epoch
    }
}

/// Table for storing BlobManager information.
#[derive(Debug, Clone)]
pub struct BlobManagerTable {
    blob_managers: DBMap<ObjectID, StoredBlobManagerInfo>,
}

impl BlobManagerTable {
    /// Reopens an existing BlobManagerTable from the database.
    pub fn reopen(database: &Arc<RocksDB>) -> Result<Self, TypedStoreError> {
        let blob_managers = DBMap::reopen(
            database,
            Some(constants::blob_managers_cf_name()),
            &ReadWriteOptions::default(),
            false,
        )?;

        Ok(Self { blob_managers })
    }

    /// Gets the info for a specific BlobManager.
    pub fn get(
        &self,
        manager_id: &ObjectID,
    ) -> Result<Option<StoredBlobManagerInfo>, TypedStoreError> {
        self.blob_managers.get(manager_id)
    }

    /// Inserts or updates a BlobManager's info.
    pub fn insert(
        &self,
        manager_id: ObjectID,
        info: StoredBlobManagerInfo,
    ) -> Result<(), TypedStoreError> {
        self.blob_managers.insert(&manager_id, &info)
    }

    /// Updates the end_epoch for a BlobManager.
    pub fn update_end_epoch(
        &self,
        manager_id: &ObjectID,
        new_end_epoch: Epoch,
    ) -> Result<(), TypedStoreError> {
        let info = StoredBlobManagerInfo::new(new_end_epoch);
        self.blob_managers.insert(manager_id, &info)
    }

    /// Removes a BlobManager from the table.
    #[allow(dead_code)]
    pub fn remove(&self, manager_id: &ObjectID) -> Result<(), TypedStoreError> {
        self.blob_managers.remove(manager_id)
    }

    /// Checks if a BlobManager exists.
    #[allow(dead_code)]
    pub fn contains(&self, manager_id: &ObjectID) -> Result<bool, TypedStoreError> {
        self.blob_managers.contains_key(manager_id)
    }

    /// Returns the number of BlobManagers stored.
    #[allow(dead_code)]
    pub fn count(&self) -> Result<usize, TypedStoreError> {
        Ok(self.blob_managers.safe_iter()?.count())
    }
}
