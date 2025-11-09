// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Client for managing blobs through BlobManager.

use std::fmt::Debug;

use sui_types::{SUI_FRAMEWORK_ADDRESS, TypeTag, base_types::ObjectID};
use walrus_core::{BlobId, EpochCount};
use walrus_sui::{
    client::{BlobObjectMetadata, BlobPersistence, SuiContractClient},
    types::move_structs::ManagedBlob,
};

use crate::{
    client::{
        WalrusNodeClient,
        WalrusStoreBlob,
        client_types::WalrusStoreBlobApi,
        resource::{RegisterBlobOp, StoreOp},
    },
    error::ClientResult,
};

/// A facade for interacting with Walrus BlobManager.
#[derive(Debug)]
pub struct BlobManagerClient<'a, T> {
    client: &'a WalrusNodeClient<T>,
    manager_id: ObjectID,
    manager_cap: ObjectID,
    /// Table ID for blob_id -> vector<ObjectID> mapping.
    blob_id_to_objects_table_id: ObjectID,
    /// Table ID for ObjectID -> ManagedBlob mapping.
    blobs_by_object_id_table_id: ObjectID,
}

impl<'a> BlobManagerClient<'a, SuiContractClient> {
    /// Helper function to create a ClientError from a message string.
    fn error(msg: impl Into<String>) -> crate::error::ClientError {
        use anyhow::anyhow;
        crate::error::ClientError::from(crate::error::ClientErrorKind::Other(
            anyhow!(msg.into()).into(),
        ))
    }

    /// Creates a new BlobManagerClient by fetching and caching the table IDs.
    pub async fn new(
        client: &'a WalrusNodeClient<SuiContractClient>,
        manager_id: ObjectID,
        manager_cap: ObjectID,
    ) -> ClientResult<Self> {
        // Fetch the table IDs from the BlobManager structure.
        let (blob_id_to_objects_table_id, blobs_by_object_id_table_id) =
            Self::extract_table_ids(client, manager_id).await?;

        Ok(Self {
            client,
            manager_id,
            manager_cap,
            blob_id_to_objects_table_id,
            blobs_by_object_id_table_id,
        })
    }

    /// Extracts a Table ObjectID from the BlobManager structure.
    ///
    /// The BlobManager has the following nested structure:
    /// - BlobManager { blob_stash: BlobStash }
    ///   - BlobStash::ObjectBased(BlobStashByObject) (enum variant stored as "pos0")
    ///     - BlobStashByObject { blob_id_to_objects: Table, blobs_by_object_id: Table }
    ///       - Table { id: UID }
    ///
    /// This function navigates through this structure to extract the ObjectID of a specific Table
    /// (either "blob_id_to_objects" or "blobs_by_object_id") by its name.
    fn extract_table_id_from_blob_manager(
        blob_manager_response: &sui_sdk::rpc_types::SuiObjectData,
        name: &str,
    ) -> ClientResult<ObjectID> {
        use sui_sdk::rpc_types::{SuiMoveStruct, SuiMoveValue, SuiParsedData};

        // Step 1: Get the parsed content from the BlobManager SuiObjectData.
        // The content field contains the Move object's parsed data structure.
        let content = blob_manager_response
            .content
            .as_ref()
            .ok_or_else(|| Self::error("BlobManager content not found"))?;

        // Step 2: Extract the MoveObject from the parsed data.
        // Sui objects are represented as MoveObject when they are Move structs.
        let SuiParsedData::MoveObject(obj) = content else {
            return Err(Self::error("BlobManager is not a MoveObject"));
        };

        // Step 3: Get the fields map from the MoveObject.
        // WithFields variant contains a BTreeMap of field names to values.
        let SuiMoveStruct::WithFields(fields) = &obj.fields else {
            return Err(Self::error("BlobManager fields not found"));
        };

        // Step 4: Extract the "blob_stash" field from BlobManager.
        // This field contains the BlobStash enum, which wraps BlobStashByObject.
        let stash_value = fields
            .get("blob_stash")
            .ok_or_else(|| Self::error("blob_stash field not found"))?;

        // Verify that blob_stash is a Struct value (Move enums are represented as Struct).
        let SuiMoveValue::Struct(stash_struct) = stash_value else {
            return Err(Self::error("blob_stash is not a Struct"));
        };

        // Step 5: Extract the "pos0" field from the BlobStash enum.
        // Move enums are serialized with variant fields named "pos0", "pos1", etc.
        // For BlobStash::ObjectBased, the BlobStashByObject is stored in "pos0".
        let SuiMoveStruct::WithTypes {
            fields: stash_fields,
            ..
        } = stash_struct
        else {
            return Err(Self::error("blob_stash struct has no fields"));
        };

        let pos0_value = stash_fields
            .get("pos0")
            .ok_or_else(|| Self::error("pos0 field not found in blob_stash"))?;

        // Verify that pos0 is a Struct value (the BlobStashByObject struct).
        let SuiMoveValue::Struct(pos0_struct) = pos0_value else {
            return Err(Self::error("pos0 is not a Struct"));
        };

        // Step 6: Extract the Table field by name from BlobStashByObject.
        // BlobStashByObject contains two Table fields: "blob_id_to_objects" and
        // "blobs_by_object_id".
        // The name parameter specifies which Table to extract.
        let SuiMoveStruct::WithTypes {
            fields: pos0_fields,
            ..
        } = pos0_struct
        else {
            return Err(Self::error("pos0 struct has no fields"));
        };

        let table_value = pos0_fields.get(name).ok_or_else(|| {
            Self::error(format!("Table '{}' not found in BlobStashByObject", name))
        })?;

        // Verify that the table field is a Struct value (Table is a Move struct).
        let SuiMoveValue::Struct(table_struct) = table_value else {
            return Err(Self::error(format!("Table '{}' is not a Struct", name)));
        };

        // Step 7: Extract the "id" field from the Table struct.
        // Sui Tables store their ObjectID in an "id" field of type UID.
        // This ObjectID is used to access the Table's dynamic fields.
        let SuiMoveStruct::WithTypes {
            fields: table_fields,
            ..
        } = table_struct
        else {
            return Err(Self::error(format!(
                "Table '{}' struct has no fields",
                name
            )));
        };

        let id_value = table_fields
            .get("id")
            .ok_or_else(|| Self::error(format!("Table '{}' has no 'id' field", name)))?;

        // Extract the UID value, which contains the ObjectID we need.
        let SuiMoveValue::UID { id } = id_value else {
            return Err(Self::error(format!(
                "Table '{}' id field is not a UID",
                name
            )));
        };

        Ok(*id)
    }

    /// Extracts the two table IDs from the BlobManager structure.
    async fn extract_table_ids(
        client: &WalrusNodeClient<SuiContractClient>,
        manager_id: ObjectID,
    ) -> ClientResult<(ObjectID, ObjectID)> {
        use sui_sdk::rpc_types::SuiObjectDataOptions;

        // Fetch the BlobManager with parsed content using the retriable sui client.
        let blob_manager_response = client
            .sui_client()
            .retriable_sui_client()
            .get_object_with_options(manager_id, SuiObjectDataOptions::new().with_content())
            .await?;

        let blob_manager_data = blob_manager_response
            .data
            .as_ref()
            .ok_or_else(|| Self::error("BlobManager object data not found"))?;

        Ok((
            Self::extract_table_id_from_blob_manager(blob_manager_data, "blob_id_to_objects")?,
            Self::extract_table_id_from_blob_manager(blob_manager_data, "blobs_by_object_id")?,
        ))
    }

    /// Get the BlobManager object ID.
    pub fn manager_id(&self) -> ObjectID {
        self.manager_id
    }

    /// Get the BlobManagerCap object ID.
    pub fn manager_cap(&self) -> ObjectID {
        self.manager_cap
    }

    /// Gets a ManagedBlob by blob_id and deletable flag.
    pub async fn get_managed_blob(
        &self,
        blob_id: BlobId,
        deletable: bool,
    ) -> ClientResult<ManagedBlob> {
        use walrus_sui::client::retry_client::RetriableSuiClient;

        // Step 1: Get the ObjectID from blob_id_to_objects table.
        let blob_id_bytes: [u8; 32] = {
            let slice = blob_id.as_ref();
            let mut bytes = [0u8; 32];
            bytes.copy_from_slice(slice);
            bytes
        };

        // Access the RetriableSuiClient to use get_dynamic_field.
        let sui_client: &RetriableSuiClient = self.client.sui_client().retriable_sui_client();

        let object_ids: Vec<ObjectID> = sui_client
            .get_dynamic_field(
                self.blob_id_to_objects_table_id,
                TypeTag::U256,
                blob_id_bytes,
            )
            .await?;

        // Step 2: For each ObjectID, read the ManagedBlob from blobs_by_object_id table.
        // The table stores ManagedBlob as dynamic fields, so we use get_dynamic_field.
        let object_id_type_tag = TypeTag::Struct(Box::new(
            sui_types::parse_sui_struct_tag(&format!("{}::object::ID", SUI_FRAMEWORK_ADDRESS))
                .map_err(|e| {
                    Self::error(format!("Failed to parse TypeTag for object::ID: {}", e))
                })?,
        ));

        for object_id in object_ids {
            let managed_blob: ManagedBlob = sui_client
                .get_dynamic_field(
                    self.blobs_by_object_id_table_id,
                    object_id_type_tag.clone(),
                    object_id,
                )
                .await?;

            if managed_blob.deletable == deletable {
                return Ok(managed_blob);
            }
        }

        Err(Self::error(format!(
            "No ManagedBlob found with blob_id {} and deletable={}",
            blob_id, deletable
        )))
    }
}

impl BlobManagerClient<'_, SuiContractClient> {
    /// Reserve storage and register multiple blobs in the BlobManager.
    ///
    /// Returns the ObjectIDs of registered blobs (blobs stay in BlobManager's table).
    #[allow(dead_code)]
    pub async fn reserve_and_register_blobs(
        &self,
        epochs_ahead: EpochCount,
        blob_metadata_list: Vec<BlobObjectMetadata>,
        persistence: BlobPersistence,
    ) -> ClientResult<Vec<ObjectID>> {
        tracing::info!(
            "BlobManager reserve_and_register: manager_id={:?}, manager_cap={:?}, num_blobs={}",
            self.manager_id,
            self.manager_cap,
            blob_metadata_list.len()
        );

        self.client
            .sui_client()
            .reserve_and_register_managed_blobs(
                self.manager_id,
                self.manager_cap,
                epochs_ahead,
                blob_metadata_list,
                persistence,
            )
            .await
            .map_err(crate::error::ClientError::from)?;

        // Note: register_blob doesn't return ManagedBlob objects - they are owned by BlobManager
        // We can't extract ObjectIDs here. Callers should query BlobManager directly if needed.
        // Return empty vec for now - this function may need to be refactored.
        Ok(vec![])
    }

    /// Registers blobs with the BlobManager and returns WalrusStoreBlob results.
    ///
    /// DEPRECATED: This function is outdated and uses the old flow.
    /// Use the ResourceManager::register_walrus_store_blobs() with blob_manager_id/cap instead.
    #[allow(dead_code, deprecated, unused_variables, unreachable_code)]
    #[deprecated]
    pub async fn register_blobmanager_store_blobs<'a, T: Debug + Clone + Send + Sync>(
        &self,
        encoded_blobs_with_status: Vec<WalrusStoreBlob<'a, T>>,
        epochs_ahead: EpochCount,
        persistence: BlobPersistence,
    ) -> ClientResult<Vec<WalrusStoreBlob<'a, T>>> {
        // This function is deprecated and no longer works with the new ObjectID-based flow
        unimplemented!("Deprecated. Use ResourceManager with blob_manager_id/cap.");
    }

    /// Certifies and completes blobs that were registered with the BlobManager.
    ///
    /// Takes blobs with StoreOp::RegisteredInBlobManager, certifies them,
    /// and returns completed blobs with BlobStoreResult::ManagedByBlobManager.
    ///
    /// This is the main certification workflow for BlobManager-registered blobs.
    pub async fn certify_and_complete_blobs<'a, T: Debug + Clone + Send + Sync>(
        &self,
        blobs_to_certify: Vec<WalrusStoreBlob<'a, T>>,
    ) -> ClientResult<Vec<WalrusStoreBlob<'a, T>>> {
        use walrus_sui::client::ArgumentOrOwnedObject;

        use crate::client::responses::BlobStoreResult;

        if blobs_to_certify.is_empty() {
            return Ok(vec![]);
        }

        // Extract Blob objects, operations and certificates from blobs
        let mut blob_info: Vec<_> = Vec::new();
        let mut certs_with_blobs: Vec<_> = Vec::new();

        for blob in &blobs_to_certify {
            if let WalrusStoreBlob::WithCertificate(inner) = blob
                && let StoreOp::RegisteredInBlobManager {
                    blob: registered_blob,
                    operation,
                } = &inner.operation
            {
                blob_info.push((registered_blob.clone(), operation.clone()));
                certs_with_blobs.push((registered_blob, &inner.certificate));
            }
        }

        if blob_info.is_empty() {
            return Ok(vec![]);
        }

        tracing::info!(
            "BlobManager certify_and_complete_blobs: manager_id={:?}, num_blobs={}",
            self.manager_id,
            blob_info.len()
        );

        // Certify all blobs in a single batch transaction
        self.client
            .sui_client()
            .certify_blobs_in_blobmanager(
                self.manager_id,
                ArgumentOrOwnedObject::Object(self.manager_cap),
                &certs_with_blobs,
            )
            .await?;

        tracing::info!(
            "Successfully certified {} blobs in BlobManager",
            certs_with_blobs.len()
        );

        // Get price computation and current epoch for cost/end_epoch calculation
        let price_computation = self.client.get_price_computation().await?;
        let current_epoch = self.client.get_committees().await?.epoch();

        // Complete the blobs by creating BlobStoreResult::ManagedByBlobManager
        let mut completed_blobs = Vec::new();
        for (blob, (registered_blob, operation)) in
            blobs_to_certify.into_iter().zip(blob_info.iter())
        {
            // Calculate cost from the operation
            let cost = price_computation.operation_cost(operation);

            // Calculate end_epoch from epochs_ahead in the operation
            let end_epoch = match operation {
                RegisterBlobOp::RegisterFromScratch { epochs_ahead, .. } => {
                    current_epoch + *epochs_ahead
                }
                RegisterBlobOp::ReuseStorage { .. } | RegisterBlobOp::ReuseRegistration { .. } => {
                    // For reuse operations, we don't extend epochs, so end_epoch stays the same
                    // But we don't have the original blob's end_epoch here
                    // Use current_epoch as a fallback (this is conservative)
                    current_epoch
                }
                RegisterBlobOp::ReuseAndExtend {
                    epochs_extended, ..
                }
                | RegisterBlobOp::ReuseAndExtendNonCertified {
                    epochs_extended, ..
                } => {
                    // Extend from current epoch
                    current_epoch + *epochs_extended
                }
            };

            let result = BlobStoreResult::ManagedByBlobManager {
                blob_id: registered_blob.blob_id,
                blob_object_id: registered_blob.id, // Use ObjectID from the Blob object
                resource_operation: operation.clone(),
                cost,
                end_epoch,
            };

            let completed = blob.complete_with(result);
            completed_blobs.push(completed);
        }

        Ok(completed_blobs)
    }
}
