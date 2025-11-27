// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Client for managing blobs through BlobManager.

use std::fmt::Debug;

use sui_types::{TypeTag, base_types::ObjectID};
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

/// BlobManager storage information.
#[derive(Debug, Clone)]
pub struct BlobManagerStorageInfo {
    /// Total storage capacity in bytes.
    pub total_capacity: u64,
    /// Used storage capacity in bytes.
    pub used_capacity: u64,
    /// Available storage capacity in bytes.
    pub available_capacity: u64,
    /// The epoch when the storage expires.
    pub end_epoch: u32,
}

/// BlobManager coin stash balances.
#[derive(Debug, Clone)]
pub struct BlobManagerCoinStashBalances {
    /// WAL token balance in the coin stash.
    pub wal_balance: u64,
    /// SUI token balance in the coin stash.
    pub sui_balance: u64,
}

/// A facade for interacting with Walrus BlobManager.
#[derive(Debug)]
pub struct BlobManagerClient<'a, T> {
    client: &'a WalrusNodeClient<T>,
    manager_id: ObjectID,
    manager_cap: ObjectID,
    /// Table ID for `blob_id -> ManagedBlob` mapping (simplified single-table design).
    blobs_table_id: ObjectID,
}

impl<'a> BlobManagerClient<'a, SuiContractClient> {
    /// Helper function to create a ClientError from a message string.
    fn error(msg: impl Into<String>) -> crate::error::ClientError {
        use anyhow::anyhow;
        crate::error::ClientError::from(crate::error::ClientErrorKind::Other(
            anyhow!(msg.into()).into(),
        ))
    }

    /// Creates a new BlobManagerClient by fetching and caching the table ID.
    pub async fn new(
        client: &'a WalrusNodeClient<SuiContractClient>,
        manager_id: ObjectID,
        manager_cap: ObjectID,
    ) -> ClientResult<Self> {
        // Fetch the table ID from the BlobManager structure.
        let blobs_table_id = Self::extract_blobs_table_id(client, manager_id).await?;

        Ok(Self {
            client,
            manager_id,
            manager_cap,
            blobs_table_id,
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

    /// Extracts the blobs Table ObjectID from the BlobManager structure.
    ///
    /// The BlobManager has the following nested structure (simplified single-table design):
    /// - BlobManager { blob_stash: BlobStash }
    ///   - BlobStash::BlobIdBased(BlobStashByBlobId) (enum variant stored as "pos0")
    ///     - BlobStashByBlobId { blobs: Table<u256, ManagedBlob> }
    ///       - Table { id: UID }
    ///
    /// This function navigates through this structure to extract the ObjectID of the blobs Table.
    fn extract_blobs_table_id_from_blob_manager(
        blob_manager_response: &sui_sdk::rpc_types::SuiObjectData,
    ) -> ClientResult<ObjectID> {
        use sui_sdk::rpc_types::{SuiMoveStruct, SuiMoveValue, SuiParsedData};

        // Step 1: Get the parsed content from the BlobManager SuiObjectData.
        let content = blob_manager_response
            .content
            .as_ref()
            .ok_or_else(|| Self::error("BlobManager content not found"))?;

        // Step 2: Extract the MoveObject from the parsed data.
        let SuiParsedData::MoveObject(obj) = content else {
            return Err(Self::error("BlobManager is not a MoveObject"));
        };

        // Step 3: Get the fields map from the MoveObject.
        let SuiMoveStruct::WithFields(fields) = &obj.fields else {
            return Err(Self::error("BlobManager fields not found"));
        };

        // Step 4: Extract the "blob_stash" field from BlobManager.
        let stash_value = fields
            .get("blob_stash")
            .ok_or_else(|| Self::error("blob_stash field not found"))?;

        let SuiMoveValue::Struct(stash_struct) = stash_value else {
            return Err(Self::error("blob_stash is not a Struct"));
        };

        // Step 5: Extract the "pos0" field from the BlobStash enum.
        // For BlobStash::BlobIdBased, the BlobStashByBlobId is stored in "pos0".
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

        let SuiMoveValue::Struct(pos0_struct) = pos0_value else {
            return Err(Self::error("pos0 is not a Struct"));
        };

        // Step 6: Extract the "blobs" Table field from BlobStashByBlobId.
        let SuiMoveStruct::WithTypes {
            fields: pos0_fields,
            ..
        } = pos0_struct
        else {
            return Err(Self::error("pos0 struct has no fields"));
        };

        let table_value = pos0_fields
            .get("blobs")
            .ok_or_else(|| Self::error("blobs table not found in BlobStashByBlobId"))?;

        let SuiMoveValue::Struct(table_struct) = table_value else {
            return Err(Self::error("blobs table is not a Struct"));
        };

        // Step 7: Extract the "id" field from the Table struct.
        let SuiMoveStruct::WithTypes {
            fields: table_fields,
            ..
        } = table_struct
        else {
            return Err(Self::error("blobs table struct has no fields"));
        };

        let id_value = table_fields
            .get("id")
            .ok_or_else(|| Self::error("blobs table has no 'id' field"))?;

        let SuiMoveValue::UID { id } = id_value else {
            return Err(Self::error("blobs table id field is not a UID"));
        };

        Ok(*id)
    }

    /// Extracts the blobs table ID from the BlobManager structure.
    async fn extract_blobs_table_id(
        client: &WalrusNodeClient<SuiContractClient>,
        manager_id: ObjectID,
    ) -> ClientResult<ObjectID> {
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

        Self::extract_blobs_table_id_from_blob_manager(blob_manager_data)
    }

    /// Get the BlobManager object ID.
    pub fn manager_id(&self) -> ObjectID {
        self.manager_id
    }

    /// Get the BlobManagerCap object ID.
    pub fn manager_cap(&self) -> ObjectID {
        self.manager_cap
    }

    /// Fetches BlobManager info including storage and coin stash balances.
    ///
    /// This makes a single RPC call to fetch all BlobManager state at once,
    /// which is more efficient than calling `get_storage_info` and
    /// `get_coin_stash_balances` separately.
    pub async fn get_blob_manager_info(
        &self,
    ) -> ClientResult<(BlobManagerStorageInfo, BlobManagerCoinStashBalances)> {
        use sui_sdk::rpc_types::SuiObjectDataOptions;

        // Fetch the BlobManager with parsed content.
        let blob_manager_response = self
            .client
            .sui_client()
            .retriable_sui_client()
            .get_object_with_options(self.manager_id, SuiObjectDataOptions::new().with_content())
            .await?;

        let blob_manager_data = blob_manager_response
            .data
            .as_ref()
            .ok_or_else(|| Self::error("BlobManager object data not found"))?;

        let storage_info = Self::extract_storage_info_from_blob_manager(blob_manager_data)?;
        let coin_stash_balances =
            Self::extract_coin_stash_balances_from_blob_manager(blob_manager_data)?;

        Ok((storage_info, coin_stash_balances))
    }

    /// Gets the storage information from the BlobManager.
    ///
    /// Returns storage capacity (total, used, available) and the end epoch.
    pub async fn get_storage_info(&self) -> ClientResult<BlobManagerStorageInfo> {
        use sui_sdk::rpc_types::SuiObjectDataOptions;

        // Fetch the BlobManager with parsed content.
        let blob_manager_response = self
            .client
            .sui_client()
            .retriable_sui_client()
            .get_object_with_options(self.manager_id, SuiObjectDataOptions::new().with_content())
            .await?;

        let blob_manager_data = blob_manager_response
            .data
            .as_ref()
            .ok_or_else(|| Self::error("BlobManager object data not found"))?;

        Self::extract_storage_info_from_blob_manager(blob_manager_data)
    }

    /// Gets the coin stash balances from the BlobManager.
    ///
    /// Returns the WAL and SUI balances in the coin stash.
    pub async fn get_coin_stash_balances(&self) -> ClientResult<BlobManagerCoinStashBalances> {
        use sui_sdk::rpc_types::SuiObjectDataOptions;

        // Fetch the BlobManager with parsed content.
        let blob_manager_response = self
            .client
            .sui_client()
            .retriable_sui_client()
            .get_object_with_options(self.manager_id, SuiObjectDataOptions::new().with_content())
            .await?;

        let blob_manager_data = blob_manager_response
            .data
            .as_ref()
            .ok_or_else(|| Self::error("BlobManager object data not found"))?;

        Self::extract_coin_stash_balances_from_blob_manager(blob_manager_data)
    }

    /// Extracts storage info from a fetched BlobManager object.
    fn extract_storage_info_from_blob_manager(
        blob_manager_data: &sui_sdk::rpc_types::SuiObjectData,
    ) -> ClientResult<BlobManagerStorageInfo> {
        use sui_sdk::rpc_types::{SuiMoveStruct, SuiMoveValue, SuiParsedData};

        let content = blob_manager_data
            .content
            .as_ref()
            .ok_or_else(|| Self::error("BlobManager content not found"))?;

        let SuiParsedData::MoveObject(obj) = content else {
            return Err(Self::error("BlobManager is not a MoveObject"));
        };

        // BlobManager struct can be represented as WithTypes or WithFields.
        let fields = match &obj.fields {
            SuiMoveStruct::WithTypes { fields, .. } => fields,
            SuiMoveStruct::WithFields(fields) => fields,
            _ => {
                return Err(Self::error("BlobManager fields not found"));
            }
        };

        // Navigate to BlobManager.storage (BlobStorage enum).
        let storage_value = fields
            .get("storage")
            .ok_or_else(|| Self::error("storage field not found"))?;

        let SuiMoveValue::Struct(storage_struct) = storage_value else {
            return Err(Self::error("storage is not a Struct"));
        };

        // Extract the UnifiedStorage from the enum's pos0 field.
        // Storage struct can be represented as WithTypes or WithFields.
        let storage_fields = match storage_struct {
            SuiMoveStruct::WithTypes { fields, .. } => fields,
            SuiMoveStruct::WithFields(fields) => fields,
            _ => {
                return Err(Self::error("storage struct has unexpected format"));
            }
        };

        let pos0_value = storage_fields
            .get("pos0")
            .ok_or_else(|| Self::error("pos0 field not found in storage"))?;

        let SuiMoveValue::Struct(unified_struct) = pos0_value else {
            return Err(Self::error("storage pos0 is not a Struct"));
        };

        // UnifiedStorage struct can be represented as WithTypes or WithFields.
        let unified_fields = match unified_struct {
            SuiMoveStruct::WithTypes { fields, .. } => fields,
            SuiMoveStruct::WithFields(fields) => fields,
            _ => {
                return Err(Self::error("UnifiedStorage has unexpected format"));
            }
        };

        // Extract the values from UnifiedStorage.
        let total_capacity = Self::extract_u64_field(unified_fields, "total_capacity")?;
        let available_storage = Self::extract_u64_field(unified_fields, "available_storage")?;
        let end_epoch = Self::extract_u32_field(unified_fields, "end_epoch")?;

        Ok(BlobManagerStorageInfo {
            total_capacity,
            used_capacity: total_capacity - available_storage,
            available_capacity: available_storage,
            end_epoch,
        })
    }

    /// Extracts coin stash balances from a fetched BlobManager object.
    fn extract_coin_stash_balances_from_blob_manager(
        blob_manager_data: &sui_sdk::rpc_types::SuiObjectData,
    ) -> ClientResult<BlobManagerCoinStashBalances> {
        use sui_sdk::rpc_types::{SuiMoveStruct, SuiMoveValue, SuiParsedData};

        let content = blob_manager_data
            .content
            .as_ref()
            .ok_or_else(|| Self::error("BlobManager content not found"))?;

        let SuiParsedData::MoveObject(obj) = content else {
            return Err(Self::error("BlobManager is not a MoveObject"));
        };

        // BlobManager struct can be represented as WithTypes or WithFields.
        let fields = match &obj.fields {
            SuiMoveStruct::WithTypes { fields, .. } => fields,
            SuiMoveStruct::WithFields(fields) => fields,
            _ => {
                return Err(Self::error("BlobManager fields not found"));
            }
        };

        // Navigate to BlobManager.coin_stash (BlobManagerCoinStash).
        let coin_stash_value = fields
            .get("coin_stash")
            .ok_or_else(|| Self::error("coin_stash field not found"))?;

        let SuiMoveValue::Struct(coin_stash_struct) = coin_stash_value else {
            return Err(Self::error("coin_stash is not a Struct"));
        };

        // coin_stash struct can be represented as WithTypes or WithFields.
        let coin_stash_fields = match coin_stash_struct {
            SuiMoveStruct::WithTypes { fields, .. } => fields,
            SuiMoveStruct::WithFields(fields) => fields,
            _ => {
                return Err(Self::error("coin_stash struct has unexpected format"));
            }
        };

        // Extract WAL balance from wal_balance field (Balance struct has a value field).
        let wal_balance = Self::extract_balance_value(coin_stash_fields, "wal_balance")?;
        let sui_balance = Self::extract_balance_value(coin_stash_fields, "sui_balance")?;

        Ok(BlobManagerCoinStashBalances {
            wal_balance,
            sui_balance,
        })
    }

    /// Helper to extract a u64 value from a field map.
    fn extract_u64_field(
        fields: &std::collections::BTreeMap<String, sui_sdk::rpc_types::SuiMoveValue>,
        field_name: &str,
    ) -> ClientResult<u64> {
        use sui_sdk::rpc_types::SuiMoveValue;

        let value = fields
            .get(field_name)
            .ok_or_else(|| Self::error(format!("{} field not found", field_name)))?;

        match value {
            SuiMoveValue::String(s) => s
                .parse::<u64>()
                .map_err(|e| Self::error(format!("Failed to parse {} as u64: {}", field_name, e))),
            SuiMoveValue::Number(n) => Ok(u64::from(*n)),
            _ => Err(Self::error(format!(
                "{} is not a number or string",
                field_name
            ))),
        }
    }

    /// Helper to extract a u32 value from a field map.
    fn extract_u32_field(
        fields: &std::collections::BTreeMap<String, sui_sdk::rpc_types::SuiMoveValue>,
        field_name: &str,
    ) -> ClientResult<u32> {
        use sui_sdk::rpc_types::SuiMoveValue;

        let value = fields
            .get(field_name)
            .ok_or_else(|| Self::error(format!("{} field not found", field_name)))?;

        match value {
            SuiMoveValue::String(s) => s
                .parse::<u32>()
                .map_err(|e| Self::error(format!("Failed to parse {} as u32: {}", field_name, e))),
            SuiMoveValue::Number(n) => Ok(*n),
            _ => Err(Self::error(format!(
                "{} is not a number or string",
                field_name
            ))),
        }
    }

    /// Helper to extract a Balance value.
    /// Balance<T> is serialized as a String representing the u64 value.
    fn extract_balance_value(
        fields: &std::collections::BTreeMap<String, sui_sdk::rpc_types::SuiMoveValue>,
        field_name: &str,
    ) -> ClientResult<u64> {
        use sui_sdk::rpc_types::SuiMoveValue;

        let balance_value = fields
            .get(field_name)
            .ok_or_else(|| Self::error(format!("{} field not found", field_name)))?;

        let SuiMoveValue::String(s) = balance_value else {
            return Err(Self::error(format!(
                "{} is not a String, got {:?}",
                field_name, balance_value
            )));
        };

        s.parse::<u64>()
            .map_err(|e| Self::error(format!("Failed to parse {} as u64: {}", field_name, e)))
    }

    /// Gets a ManagedBlob by blob_id and deletable flag.
    ///
    /// Uses the simplified single-table design where ManagedBlobs are stored
    /// directly in a Table<u256, ManagedBlob> keyed by blob_id.
    pub async fn get_managed_blob(
        &self,
        blob_id: BlobId,
        deletable: bool,
    ) -> ClientResult<ManagedBlob> {
        use walrus_sui::client::retry_client::RetriableSuiClient;

        // Convert blob_id to bytes for table lookup.
        let blob_id_bytes: [u8; 32] = {
            let slice = blob_id.as_ref();
            let mut bytes = [0u8; 32];
            bytes.copy_from_slice(slice);
            bytes
        };

        // Access the RetriableSuiClient to use get_dynamic_field.
        let sui_client: &RetriableSuiClient = self.client.sui_client().retriable_sui_client();

        // Single-step lookup: blob_id -> ManagedBlob directly from the blobs table.
        let managed_blob: ManagedBlob = sui_client
            .get_dynamic_field(self.blobs_table_id, TypeTag::U256, blob_id_bytes)
            .await?;

        // Verify the deletable flag matches (contract enforces only one blob per blob_id).
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

    /// Deletes a deletable managed blob from the BlobManager.
    ///
    /// This removes the blob from the BlobManager's storage tracking and returns
    /// the allocated storage back to the storage pool.
    ///
    /// # Arguments
    /// * `blob_id` - The ID of the blob to delete.
    ///
    /// # Errors
    /// Returns an error if:
    /// - The blob is not deletable (only deletable blobs can be deleted).
    /// - The blob is not registered in this BlobManager.
    /// - The transaction fails.
    pub async fn delete_blob(&self, blob_id: BlobId) -> ClientResult<()> {
        tracing::info!(
            "BlobManager delete_blob: manager_id={:?}, manager_cap={:?}, blob_id={}",
            self.manager_id,
            self.manager_cap,
            blob_id
        );

        // Call the Sui contract to delete the blob.
        // The contract will handle all validation:
        // - Check if the blob exists
        // - Verify it's deletable (not permanent)
        // - Ensure it belongs to this BlobManager
        self.client
            .sui_client()
            .delete_managed_blob(self.manager_id, self.manager_cap, blob_id, true)
            .await
            .map_err(crate::error::ClientError::from)?;

        Ok(())
    }

    // ===== Coin Stash Management Methods =====

    /// Deposits WAL tokens to the BlobManager's coin stash.
    ///
    /// Anyone can deposit WAL tokens to the coin stash, which can then be used
    /// by anyone for storage operations (buy storage, extend storage).
    ///
    /// # Arguments
    ///
    /// * `wal_amount` - The amount of WAL tokens to deposit (in MIST units).
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the deposit was successful.
    /// * `Err(ClientError)` if the transaction fails.
    pub async fn deposit_wal_to_coin_stash(&self, wal_amount: u64) -> ClientResult<()> {
        tracing::info!(
            "BlobManager deposit_wal: manager_id={:?}, amount={}",
            self.manager_id,
            wal_amount
        );

        self.client
            .sui_client()
            .deposit_wal_to_blob_manager(self.manager_id, wal_amount)
            .await
            .map_err(crate::error::ClientError::from)?;

        Ok(())
    }

    /// Deposits SUI tokens to the BlobManager's coin stash.
    ///
    /// Anyone can deposit SUI tokens to the coin stash, which can then be used
    /// by anyone for gas operations.
    ///
    /// # Arguments
    ///
    /// * `sui_amount` - The amount of SUI tokens to deposit (in MIST units).
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the deposit was successful.
    /// * `Err(ClientError)` if the transaction fails.
    pub async fn deposit_sui_to_coin_stash(&self, sui_amount: u64) -> ClientResult<()> {
        tracing::info!(
            "BlobManager deposit_sui: manager_id={:?}, amount={}",
            self.manager_id,
            sui_amount
        );

        self.client
            .sui_client()
            .deposit_sui_to_blob_manager(self.manager_id, sui_amount)
            .await
            .map_err(crate::error::ClientError::from)?;

        Ok(())
    }

    /// Buys additional storage capacity using funds from the coin stash.
    ///
    /// This increases the BlobManager's storage capacity by purchasing more storage.
    /// The funds are taken from the coin stash's WAL balance.
    ///
    /// # Arguments
    ///
    /// * `storage_amount` - The amount of storage to buy (in bytes).
    /// * `epochs_ahead` - The number of epochs ahead to reserve the storage for.
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the storage was successfully purchased.
    /// * `Err(ClientError)` if:
    ///   - The coin stash has insufficient WAL balance.
    ///   - The transaction fails.
    pub async fn buy_storage_from_stash(
        &self,
        storage_amount: u64,
        epochs_ahead: u32,
    ) -> ClientResult<()> {
        tracing::info!(
            "BlobManager buy_storage: manager_id={:?}, cap={:?}, amount={}, epochs={}",
            self.manager_id,
            self.manager_cap,
            storage_amount,
            epochs_ahead
        );

        self.client
            .sui_client()
            .buy_storage_from_stash(
                self.manager_id,
                self.manager_cap,
                storage_amount,
                epochs_ahead,
            )
            .await
            .map_err(crate::error::ClientError::from)?;

        Ok(())
    }

    /// Extends the storage period using funds from the coin stash.
    ///
    /// This extends the validity period of the BlobManager's existing storage.
    /// The funds are taken from the coin stash's WAL balance.
    ///
    /// # Arguments
    ///
    /// * `extension_epochs` - The number of epochs to extend the storage for.
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the storage period was successfully extended.
    /// * `Err(ClientError)` if:
    ///   - The coin stash has insufficient WAL balance.
    ///   - The transaction fails.
    pub async fn extend_storage_from_stash(&self, extension_epochs: u32) -> ClientResult<()> {
        tracing::info!(
            "BlobManager extend_storage: manager_id={:?}, epochs={}",
            self.manager_id,
            extension_epochs
        );

        self.client
            .sui_client()
            .extend_storage_from_stash(self.manager_id, extension_epochs)
            .await
            .map_err(crate::error::ClientError::from)?;

        Ok(())
    }

    /// Withdraws all WAL funds from the coin stash.
    ///
    /// This method can only be called by someone with a fund_manager or admin capability.
    /// It withdraws all available WAL tokens from the coin stash.
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the withdrawal was successful.
    /// * `Err(ClientError)` if:
    ///   - The caller doesn't have fund_manager or admin permission.
    ///   - The transaction fails.
    pub async fn withdraw_all_wal(&self) -> ClientResult<()> {
        tracing::info!(
            "BlobManager withdraw_all_wal: manager_id={:?}, cap={:?}",
            self.manager_id,
            self.manager_cap
        );

        self.client
            .sui_client()
            .withdraw_all_wal_from_blob_manager(self.manager_id, self.manager_cap)
            .await
            .map_err(crate::error::ClientError::from)?;

        Ok(())
    }

    /// Withdraws all SUI funds from the coin stash.
    ///
    /// This method can only be called by someone with a fund_manager or admin capability.
    /// It withdraws all available SUI tokens from the coin stash.
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the withdrawal was successful.
    /// * `Err(ClientError)` if:
    ///   - The caller doesn't have fund_manager or admin permission.
    ///   - The transaction fails.
    pub async fn withdraw_all_sui(&self) -> ClientResult<()> {
        tracing::info!(
            "BlobManager withdraw_all_sui: manager_id={:?}, cap={:?}",
            self.manager_id,
            self.manager_cap
        );

        self.client
            .sui_client()
            .withdraw_all_sui_from_blob_manager(self.manager_id, self.manager_cap)
            .await
            .map_err(crate::error::ClientError::from)?;

        Ok(())
    }

    /// Extends the storage period using funds from the coin stash (fund manager version).
    ///
    /// This extends the validity period of the BlobManager's existing storage.
    /// Unlike `extend_storage_from_stash`, this method bypasses policy time/amount constraints.
    /// It requires a capability with fund_manager permission.
    ///
    /// # Arguments
    ///
    /// * `extension_epochs` - The number of epochs to extend the storage for.
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the storage period was successfully extended.
    /// * `Err(ClientError)` if:
    ///   - The caller doesn't have fund_manager permission.
    ///   - The coin stash has insufficient WAL balance.
    ///   - The transaction fails.
    pub async fn extend_storage_from_stash_fund_manager(
        &self,
        extension_epochs: u32,
    ) -> ClientResult<()> {
        tracing::info!(
            "BlobManager extend_storage_fund_manager: manager_id={:?}, cap={:?}, epochs={}",
            self.manager_id,
            self.manager_cap,
            extension_epochs
        );

        self.client
            .sui_client()
            .extend_storage_from_stash_fund_manager(
                self.manager_id,
                self.manager_cap,
                extension_epochs,
            )
            .await
            .map_err(crate::error::ClientError::from)?;

        Ok(())
    }

    // ===== Extension Policy Management Methods =====

    /// Sets the extension policy to disabled.
    ///
    /// When disabled, no one (including fund managers) can extend storage.
    /// This method requires a capability with fund_manager permission.
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the policy was successfully set.
    /// * `Err(ClientError)` if:
    ///   - The caller doesn't have fund_manager permission.
    ///   - The transaction fails.
    pub async fn set_extension_policy_disabled(&self) -> ClientResult<()> {
        tracing::info!(
            "BlobManager set_extension_policy_disabled: manager_id={:?}, cap={:?}",
            self.manager_id,
            self.manager_cap
        );

        self.client
            .sui_client()
            .set_extension_policy_disabled(self.manager_id, self.manager_cap)
            .await
            .map_err(crate::error::ClientError::from)?;

        Ok(())
    }

    /// Sets the extension policy to fund_manager only.
    ///
    /// When set to fund_manager only, only accounts with fund_manager capability
    /// can extend storage using `extend_storage_from_stash_fund_manager`.
    /// Public extensions via `extend_storage_from_stash` will be rejected.
    /// This method requires a capability with fund_manager permission.
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the policy was successfully set.
    /// * `Err(ClientError)` if:
    ///   - The caller doesn't have fund_manager permission.
    ///   - The transaction fails.
    pub async fn set_extension_policy_fund_manager_only(&self) -> ClientResult<()> {
        tracing::info!(
            "BlobManager set_extension_policy_fund_manager_only: manager_id={:?}, cap={:?}",
            self.manager_id,
            self.manager_cap
        );

        self.client
            .sui_client()
            .set_extension_policy_fund_manager_only(self.manager_id, self.manager_cap)
            .await
            .map_err(crate::error::ClientError::from)?;

        Ok(())
    }

    /// Sets the extension policy to constrained with the given parameters.
    ///
    /// A constrained policy allows anyone to extend storage, subject to:
    /// - Time constraint: Extension only allowed when within `expiry_threshold_epochs` of expiry.
    /// - Amount constraint: Maximum epochs capped at `max_extension_epochs`.
    ///
    /// Fund managers can bypass these constraints using `extend_storage_from_stash_fund_manager`.
    /// This method requires a capability with fund_manager permission.
    ///
    /// # Arguments
    ///
    /// * `expiry_threshold_epochs` - Extension only allowed when within this many epochs of expiry.
    /// * `max_extension_epochs` - Maximum epochs that can be extended in a single call.
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the policy was successfully set.
    /// * `Err(ClientError)` if:
    ///   - The caller doesn't have fund_manager permission.
    ///   - The transaction fails.
    pub async fn set_extension_policy_constrained(
        &self,
        expiry_threshold_epochs: u32,
        max_extension_epochs: u32,
    ) -> ClientResult<()> {
        tracing::info!(
            "BlobManager set_extension_policy_constrained: manager_id={:?}, cap={:?}, \
            expiry_threshold={}, max_extension={}",
            self.manager_id,
            self.manager_cap,
            expiry_threshold_epochs,
            max_extension_epochs
        );

        self.client
            .sui_client()
            .set_extension_policy_constrained(
                self.manager_id,
                self.manager_cap,
                expiry_threshold_epochs,
                max_extension_epochs,
            )
            .await
            .map_err(crate::error::ClientError::from)?;

        Ok(())
    }

    // ===== Capability Management Methods =====

    /// Creates a new capability for this BlobManager.
    ///
    /// Only accounts with admin capabilities can create new capabilities.
    /// The new capability will be transferred to the transaction sender.
    ///
    /// # Arguments
    ///
    /// * `is_admin` - Whether the new capability should have admin permissions.
    /// * `fund_manager` - Whether the new capability should have fund manager permissions.
    ///   Note: Only capabilities with fund_manager permission can create new fund_manager caps.
    ///
    /// # Returns
    ///
    /// * `Ok(ObjectID)` - The ObjectID of the newly created capability.
    /// * `Err(ClientError)` if:
    ///   - The caller doesn't have admin permission.
    ///   - The caller doesn't have fund_manager permission but tries to create a fund_manager cap.
    ///   - The transaction fails.
    pub async fn create_cap(&self, is_admin: bool, fund_manager: bool) -> ClientResult<ObjectID> {
        tracing::info!(
            "BlobManager create_cap: manager_id={:?}, cap={:?}, is_admin={}, fund_manager={}",
            self.manager_id,
            self.manager_cap,
            is_admin,
            fund_manager
        );

        let new_cap_id = self
            .client
            .sui_client()
            .create_blob_manager_cap(self.manager_id, self.manager_cap, is_admin, fund_manager)
            .await
            .map_err(crate::error::ClientError::from)?;

        Ok(new_cap_id)
    }

    // ===== Blob Attribute Management Methods =====

    /// Sets an attribute on a managed blob.
    ///
    /// This adds or updates a key-value attribute on the specified blob.
    /// If the key already exists, the value is updated.
    ///
    /// # Arguments
    ///
    /// * `blob_id` - The ID of the blob to set the attribute on.
    /// * `key` - The attribute key (max 1024 bytes).
    /// * `value` - The attribute value (max 1024 bytes).
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the attribute was successfully set.
    /// * `Err(ClientError)` if:
    ///   - The blob is not found in this BlobManager.
    ///   - The key or value exceeds size limits.
    ///   - Adding a new key would exceed the max attributes limit (100).
    ///   - The transaction fails.
    pub async fn set_attribute(
        &self,
        blob_id: BlobId,
        key: String,
        value: String,
    ) -> ClientResult<()> {
        tracing::info!(
            "BlobManager set_attribute: manager_id={:?}, blob_id={}, key={}",
            self.manager_id,
            blob_id,
            key
        );

        self.client
            .sui_client()
            .set_managed_blob_attribute(self.manager_id, self.manager_cap, blob_id, key, value)
            .await
            .map_err(crate::error::ClientError::from)?;

        Ok(())
    }

    /// Removes an attribute from a managed blob.
    ///
    /// This removes a key-value attribute from the specified blob.
    ///
    /// # Arguments
    ///
    /// * `blob_id` - The ID of the blob to remove the attribute from.
    /// * `key` - The attribute key to remove.
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the attribute was successfully removed.
    /// * `Err(ClientError)` if:
    ///   - The blob is not found in this BlobManager.
    ///   - The key does not exist on the blob.
    ///   - The transaction fails.
    pub async fn remove_attribute(&self, blob_id: BlobId, key: String) -> ClientResult<()> {
        tracing::info!(
            "BlobManager remove_attribute: manager_id={:?}, blob_id={}, key={}",
            self.manager_id,
            blob_id,
            key
        );

        self.client
            .sui_client()
            .remove_managed_blob_attribute(self.manager_id, self.manager_cap, blob_id, key)
            .await
            .map_err(crate::error::ClientError::from)?;

        Ok(())
    }

    /// Clears all attributes from a managed blob.
    ///
    /// This removes all key-value attributes from the specified blob.
    ///
    /// # Arguments
    ///
    /// * `blob_id` - The ID of the blob to clear attributes from.
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the attributes were successfully cleared.
    /// * `Err(ClientError)` if:
    ///   - The blob is not found in this BlobManager.
    ///   - The transaction fails.
    pub async fn clear_attributes(&self, blob_id: BlobId) -> ClientResult<()> {
        tracing::info!(
            "BlobManager clear_attributes: manager_id={:?}, blob_id={}",
            self.manager_id,
            blob_id
        );

        self.client
            .sui_client()
            .clear_managed_blob_attributes(self.manager_id, self.manager_cap, blob_id)
            .await
            .map_err(crate::error::ClientError::from)?;

        Ok(())
    }
}
