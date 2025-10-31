// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Client for managing blobs through BlobManager.

use sui_types::base_types::ObjectID;
use walrus_core::EpochCount;
use walrus_sui::client::{BlobObjectMetadata, BlobPersistence, SuiContractClient};

use crate::{client::WalrusNodeClient, error::ClientResult};

/// A facade for interacting with Walrus BlobManager.
#[derive(Debug)]
pub struct BlobManagerClient<'a, T> {
    client: &'a mut WalrusNodeClient<T>,
    manager_id: ObjectID,
    manager_cap: ObjectID,
}

impl<'a, T> BlobManagerClient<'a, T> {
    /// Creates a new BlobManagerClient.
    pub fn new(
        client: &'a mut WalrusNodeClient<T>,
        manager_id: ObjectID,
        manager_cap: ObjectID,
    ) -> Self {
        Self {
            client,
            manager_id,
            manager_cap,
        }
    }

    /// Get the BlobManager object ID.
    pub fn manager_id(&self) -> ObjectID {
        self.manager_id
    }

    /// Get the BlobManagerCap object ID.
    pub fn manager_cap(&self) -> ObjectID {
        self.manager_cap
    }
}

impl BlobManagerClient<'_, SuiContractClient> {
    /// Reserve storage and register multiple blobs in the BlobManager.
    ///
    /// This function:
    /// 1. Reserves the total storage needed for all blobs
    /// 2. Splits the storage for each individual blob
    /// 3. Registers each blob with the BlobManager
    ///
    /// Returns a list of blob object IDs.
    pub async fn reserve_and_register_blobs(
        &mut self,
        epochs_ahead: EpochCount,
        blob_metadata_list: Vec<BlobObjectMetadata>,
        persistence: BlobPersistence,
    ) -> ClientResult<Vec<ObjectID>> {
        self.client
            .sui_client()
            .reserve_and_register_blobs_in_blobmanager(
                self.manager_id,
                self.manager_cap.into(),
                epochs_ahead,
                blob_metadata_list,
                persistence,
            )
            .await
            .map_err(|e| e.into())
    }
}
