// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Client for managing blobs through BlobManager.

use std::fmt::Debug;

use sui_types::{SUI_FRAMEWORK_ADDRESS, TypeTag, base_types::ObjectID};
use walrus_core::{BlobId, metadata::BlobMetadataApi as _};
use walrus_sui::{
    client::{BlobPersistence, SuiContractClient},
    types::move_structs::ManagedBlob,
};

use crate::{
    client::{
        WalrusNodeClient,
        WalrusStoreBlobMaybeFinished,
        WalrusStoreBlobUnfinished,
        client_types::{
            BlobObject,
            BlobPendingCertifyAndExtend,
            BlobWithStatus,
            RegisteredBlob,
            WalrusStoreBlobFinished,
        },
        resource::{PriceComputation, RegisterBlobOp},
    },
    error::ClientResult,
};

/// A facade for interacting with Walrus BlobManager.
#[derive(Debug)]
pub struct BlobManagerClient<'a, T> {
    client: &'a WalrusNodeClient<T>,
    manager_id: ObjectID,
    manager_cap: ObjectID,
    /// Table ID for `blob_id -> vector<ObjectID>` mapping.
    blob_id_to_objects_table_id: ObjectID,
    /// Table ID for `ObjectID -> ManagedBlob` mapping.
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

    /// Creates a new BlobManagerClient from a BlobManagerCap object ID.
    /// Extracts the manager_id from the cap and initializes the client.
    pub async fn from_cap(
        client: &'a WalrusNodeClient<SuiContractClient>,
        cap_id: ObjectID,
    ) -> ClientResult<Self> {
        use sui_sdk::rpc_types::{
            SuiMoveStruct,
            SuiMoveValue,
            SuiObjectDataOptions,
            SuiParsedData,
        };

        // Read the BlobManagerCap to get the manager_id.
        let cap_response = client
            .sui_client()
            .retriable_sui_client()
            .get_object_with_options(cap_id, SuiObjectDataOptions::new().with_content())
            .await
            .map_err(|e| Self::error(format!("Failed to read BlobManagerCap: {}", e)))?;

        let cap_data = cap_response
            .data
            .ok_or_else(|| Self::error("BlobManagerCap object not found"))?;

        let content = cap_data
            .content
            .ok_or_else(|| Self::error("BlobManagerCap content not found"))?;

        let SuiParsedData::MoveObject(move_obj) = content else {
            return Err(Self::error("BlobManagerCap is not a Move object"));
        };

        // Extract manager_id field directly from the Move object.
        let SuiMoveStruct::WithFields(fields) = &move_obj.fields else {
            return Err(Self::error("BlobManagerCap has no fields"));
        };

        let manager_id_value = fields
            .get("manager_id")
            .ok_or_else(|| Self::error("manager_id field not found"))?;

        let SuiMoveValue::Address(manager_id) = manager_id_value else {
            return Err(Self::error("manager_id is not an Address"));
        };

        // Create the BlobManagerClient using the manager_id from the cap.
        Self::new(client, (*manager_id).into(), cap_id).await
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

        // Query returns a single ObjectID (one blob per blob_id).
        let object_id: ObjectID = sui_client
            .get_dynamic_field(
                self.blob_id_to_objects_table_id,
                TypeTag::U256,
                blob_id_bytes,
            )
            .await?;

        // Step 2: Read the ManagedBlob from blobs_by_object_id table using the ObjectID.
        // The table stores ManagedBlob as dynamic fields, so we use get_dynamic_field.
        let object_id_type_tag = TypeTag::Struct(Box::new(
            sui_types::parse_sui_struct_tag(&format!("{}::object::ID", SUI_FRAMEWORK_ADDRESS))
                .map_err(|e| {
                    Self::error(format!("Failed to parse TypeTag for object::ID: {}", e))
                })?,
        ));

        let managed_blob: ManagedBlob = sui_client
            .get_dynamic_field(
                self.blobs_by_object_id_table_id,
                object_id_type_tag,
                object_id,
            )
            .await?;

        // Step 3: Verify the deletable flag matches (contract enforces only one blob per blob_id).
        if managed_blob.deletable != deletable {
            return Err(Self::error(format!(
                "Blob permanency conflict: blob_id {} exists with deletable={}, requested \
                deletable={}",
                blob_id, managed_blob.deletable, deletable
            )));
        }

        Ok(managed_blob)
    }
}

impl BlobManagerClient<'_, SuiContractClient> {
    /// Reserve storage and register multiple blobs in the BlobManager.
    ///
    /// Returns WalrusStoreBlobMaybeFinished with RegisteredBlob state containing
    /// BlobObject::Managed.
    pub async fn register_blobs(
        &self,
        blobs_with_status: Vec<WalrusStoreBlobMaybeFinished<BlobWithStatus>>,
        persistence: BlobPersistence,
    ) -> ClientResult<Vec<WalrusStoreBlobMaybeFinished<RegisteredBlob>>> {
        use walrus_sui::client::BlobObjectMetadata;

        let blobs_count = blobs_with_status.len();
        let mut results = Vec::with_capacity(blobs_count);
        let mut to_be_processed = Vec::new();

        // Separate already finished blobs from those that need processing.
        for blob in blobs_with_status {
            match blob.try_finish() {
                Ok(blob) => results.push(blob),
                Err(blob) => to_be_processed.push(blob),
            }
        }

        // If there are no blobs to be processed, return early.
        if to_be_processed.is_empty() {
            return Ok(results);
        }

        // Extract BlobObjectMetadata from EncodedBlob for unfinished blobs.
        let blob_metadata_list: Vec<BlobObjectMetadata> = to_be_processed
            .iter()
            .map(|blob| {
                BlobObjectMetadata::try_from(blob.state.encoded_blob.metadata.as_ref())
                    .map_err(crate::error::ClientError::from)
            })
            .collect::<Result<Vec<_>, _>>()?;

        tracing::info!(
            "BlobManager reserve_and_register: manager_id={:?}, manager_cap={:?}, num_blobs={}",
            self.manager_id,
            self.manager_cap,
            blob_metadata_list.len()
        );

        // Register blobs in BlobManager.
        self.client
            .sui_client()
            .reserve_and_register_managed_blobs(
                self.manager_id,
                self.manager_cap,
                blob_metadata_list.clone(),
                persistence,
            )
            .await
            .map_err(crate::error::ClientError::from)?;

        let deletable = match persistence {
            BlobPersistence::Deletable => true,
            BlobPersistence::Permanent => false,
        };

        // Construct MaybeFinished<RegisteredBlob> for each blob.
        let registered_blobs: Vec<WalrusStoreBlobMaybeFinished<RegisteredBlob>> = to_be_processed
            .into_iter()
            .zip(blob_metadata_list.iter())
            .map(|(blob, metadata)| {
                blob.map_infallible(
                    |blob_with_status| RegisteredBlob {
                        encoded_blob: blob_with_status.encoded_blob.clone(),
                        status: blob_with_status.status,
                        blob_object: BlobObject::Managed {
                            manager_id: self.manager_id,
                            blob_id: metadata.blob_id,
                            deletable,
                            encoded_size: blob_with_status
                                .encoded_blob
                                .metadata
                                .metadata()
                                .encoded_size()
                                .expect("encoded blob should have valid encoded size"),
                        },
                        operation: RegisterBlobOp::RegisteredInBlobManager {
                            encoded_length: blob_with_status
                                .encoded_blob
                                .metadata
                                .metadata()
                                .encoded_size()
                                .expect("encoded blob should have valid encoded size"),
                            manager_id: self.manager_id,
                        },
                    },
                    "register_managed_blob",
                )
                .into_maybe_finished()
            })
            .collect();

        results.extend(registered_blobs.into_iter());
        Ok(results)
    }

    /// Certifies and completes blobs that were registered with the BlobManager.
    /// Certifies managed blobs in the BlobManager.
    ///
    /// Takes blobs that have been uploaded to storage nodes with certificates,
    /// and certifies them in the BlobManager.
    pub async fn certify_blobs(
        &self,
        blobs_to_certify: Vec<WalrusStoreBlobUnfinished<BlobPendingCertifyAndExtend>>,
        price_computation: PriceComputation,
    ) -> ClientResult<Vec<WalrusStoreBlobFinished>> {
        if blobs_to_certify.is_empty() {
            return Ok(vec![]);
        }

        let cert_params: Vec<_> = blobs_to_certify
            .iter()
            .filter_map(|blob| match &blob.state.blob_object {
                BlobObject::Managed {
                    blob_id, deletable, ..
                } => blob
                    .state
                    .certificate
                    .as_ref()
                    .map(|cert| (*blob_id, *deletable, cert.as_ref())),
                BlobObject::Regular(_) => {
                    panic!("BlobManagerClient should only certify managed blobs")
                }
            })
            .collect();

        self.client
            .sui_client()
            .certify_managed_blobs_in_blobmanager(self.manager_id, self.manager_cap, &cert_params)
            .await
            .map_err(crate::error::ClientError::from)?;

        // Convert all blobs to finished state with ManagedByBlobManager result
        // For managed blobs, we don't need certify_and_extend_result or price_computation
        let results = blobs_to_certify
            .into_iter()
            .map(|blob| {
                blob.map_infallible(
                    |blob_state| {
                        blob_state.with_certify_and_extend_result(
                            None, // Not needed for managed blobs
                            &price_computation,
                        )
                    },
                    "certify_managed_blob",
                )
            })
            .collect();

        Ok(results)
    }
}
