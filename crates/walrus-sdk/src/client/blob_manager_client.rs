// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Client for managing blobs through BlobManager.

use std::{fmt::Debug, sync::Arc};

use sui_types::{TypeTag, base_types::ObjectID};
use walrus_core::{BlobId, metadata::BlobMetadataApi as _};
use walrus_sui::{
    client::{BlobPersistence, SuiContractClient},
    types::move_structs::{BlobManager, BlobManagerCap, ManagedBlob},
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

/// Cached BlobManager data for efficient reuse across operations.
///
/// This struct stores the essential data needed to create a `BlobManagerClient`:
/// - The `BlobManagerCap` capability object.
/// - The `blobs_table_id` for looking up managed blobs.
///
/// By caching this data in `WalrusNodeClient`, we avoid re-fetching it from the chain
/// on each store operation.
#[derive(Debug, Clone)]
pub struct BlobManagerData {
    /// The BlobManagerCap capability object.
    cap: BlobManagerCap,
    /// Table ID for `blob_id -> ManagedBlob` mapping.
    blobs_table_id: ObjectID,
}

impl BlobManagerData {
    /// Returns a reference to the BlobManagerCap.
    pub fn cap(&self) -> &BlobManagerCap {
        &self.cap
    }

    /// Returns the capability ID.
    pub fn cap_id(&self) -> ObjectID {
        self.cap.id
    }

    /// Returns the manager ID.
    pub fn manager_id(&self) -> ObjectID {
        self.cap.manager_id
    }

    /// Returns the blobs table ID.
    pub fn blobs_table_id(&self) -> ObjectID {
        self.blobs_table_id
    }

    /// Returns true if the capability has admin permissions.
    pub fn is_admin(&self) -> bool {
        self.cap.is_admin
    }

    /// Returns true if the capability has fund manager permissions.
    pub fn is_fund_manager(&self) -> bool {
        self.cap.fund_manager
    }

    /// Creates BlobManagerData by fetching the cap and table ID from the chain.
    pub async fn from_cap_id(
        client: &WalrusNodeClient<SuiContractClient>,
        cap_id: ObjectID,
    ) -> ClientResult<Self> {
        let cap = BlobManagerClient::fetch_blob_manager_cap(client, cap_id).await?;
        tracing::debug!(?cap, "Fetched blob manager cap.");
        let blobs_table_id =
            BlobManagerClient::extract_blobs_table_id(client, cap.manager_id).await?;
        tracing::debug!(
            ?blobs_table_id,
            ?cap.manager_id,
            "Fetched blob manager table id."
        );

        Ok(Self {
            cap,
            blobs_table_id,
        })
    }
}

/// A facade for interacting with Walrus BlobManager.
#[derive(Debug)]
pub struct BlobManagerClient<'a, T> {
    client: &'a WalrusNodeClient<T>,
    /// The cached BlobManager data (shared via Arc for efficiency).
    data: Arc<BlobManagerData>,
}

impl<'a> BlobManagerClient<'a, SuiContractClient> {
    /// Helper function to create a ClientError from a message string.
    fn error(msg: impl Into<String>) -> crate::error::ClientError {
        use anyhow::anyhow;
        crate::error::ClientError::from(crate::error::ClientErrorKind::Other(
            anyhow!(msg.into()).into(),
        ))
    }

    /// Creates a new BlobManagerClient from a BlobManagerCap object ID.
    ///
    /// This method fetches the BlobManagerCap and table ID from the chain.
    pub async fn from_cap_id(
        client: &'a WalrusNodeClient<SuiContractClient>,
        cap_id: ObjectID,
    ) -> ClientResult<Self> {
        let data = BlobManagerData::from_cap_id(client, cap_id).await?;
        Ok(Self {
            client,
            data: Arc::new(data),
        })
    }

    /// Creates a new BlobManagerClient from cached BlobManagerData.
    ///
    /// This is the most efficient constructor as it uses pre-fetched data.
    pub fn from_data(
        client: &'a WalrusNodeClient<SuiContractClient>,
        data: Arc<BlobManagerData>,
    ) -> Self {
        Self { client, data }
    }

    /// Fetches a BlobManagerCap from the chain by its object ID.
    ///
    /// Uses BCS deserialization via the `AssociatedContractStruct` trait for type-safe parsing.
    pub async fn fetch_blob_manager_cap(
        client: &WalrusNodeClient<SuiContractClient>,
        cap_id: ObjectID,
    ) -> ClientResult<BlobManagerCap> {
        client
            .sui_client()
            .retriable_sui_client()
            .get_sui_object::<BlobManagerCap>(cap_id)
            .await
            .map_err(|e| Self::error(format!("failed to read BlobManagerCap: {}", e)))
    }

    /// Fetches the BlobManager object from the chain.
    ///
    /// Uses BCS deserialization via the `AssociatedContractStruct` trait.
    async fn fetch_blob_manager(
        client: &WalrusNodeClient<SuiContractClient>,
        manager_id: ObjectID,
    ) -> ClientResult<BlobManager> {
        client
            .sui_client()
            .retriable_sui_client()
            .get_sui_object::<BlobManager>(manager_id)
            .await
            .map_err(|e| Self::error(format!("failed to read BlobManager: {}", e)))
    }

    /// Extracts the blobs table ID from the BlobManager structure.
    async fn extract_blobs_table_id(
        client: &WalrusNodeClient<SuiContractClient>,
        manager_id: ObjectID,
    ) -> ClientResult<ObjectID> {
        let blob_manager = Self::fetch_blob_manager(client, manager_id).await?;
        Ok(blob_manager.storage.blobs.id)
    }

    /// Get the BlobManager object ID.
    pub fn manager_id(&self) -> ObjectID {
        self.data.manager_id()
    }

    /// Get the BlobManagerCap object ID.
    pub fn cap_id(&self) -> ObjectID {
        self.data.cap_id()
    }

    /// Get the BlobManagerCap.
    pub fn cap(&self) -> &BlobManagerCap {
        self.data.cap()
    }

    /// Get the cached BlobManagerData.
    pub fn data(&self) -> &BlobManagerData {
        &self.data
    }

    /// Returns true if this client has admin permissions.
    pub fn is_admin(&self) -> bool {
        self.data.is_admin()
    }

    /// Returns true if this client has fund manager permissions.
    pub fn is_fund_manager(&self) -> bool {
        self.data.is_fund_manager()
    }

    /// Fetches BlobManager info including storage and coin stash balances.
    ///
    /// This makes a single RPC call to fetch all BlobManager state at once,
    /// which is more efficient than calling `get_storage_info` and
    /// `get_coin_stash_balances` separately.
    pub async fn get_blob_manager_info(
        &self,
    ) -> ClientResult<(BlobManagerStorageInfo, BlobManagerCoinStashBalances)> {
        let blob_manager = Self::fetch_blob_manager(self.client, self.data.manager_id()).await?;

        let storage = &blob_manager.storage;
        let storage_info = BlobManagerStorageInfo {
            total_capacity: storage.available_storage + storage.used_storage,
            used_capacity: storage.used_storage,
            available_capacity: storage.available_storage,
            end_epoch: storage.end_epoch,
        };

        let coin_stash_balances = BlobManagerCoinStashBalances {
            wal_balance: blob_manager.coin_stash.wal_balance,
            sui_balance: blob_manager.coin_stash.sui_balance,
        };

        Ok((storage_info, coin_stash_balances))
    }

    /// Gets the storage information from the BlobManager.
    ///
    /// Returns storage capacity (total, used, available) and the end epoch.
    pub async fn get_storage_info(&self) -> ClientResult<BlobManagerStorageInfo> {
        let blob_manager = Self::fetch_blob_manager(self.client, self.data.manager_id()).await?;
        let storage = &blob_manager.storage;

        Ok(BlobManagerStorageInfo {
            total_capacity: storage.available_storage + storage.used_storage,
            used_capacity: storage.used_storage,
            available_capacity: storage.available_storage,
            end_epoch: storage.end_epoch,
        })
    }

    /// Gets the coin stash balances from the BlobManager.
    ///
    /// Returns the WAL and SUI balances in the coin stash.
    pub async fn get_coin_stash_balances(&self) -> ClientResult<BlobManagerCoinStashBalances> {
        let blob_manager = Self::fetch_blob_manager(self.client, self.data.manager_id()).await?;

        Ok(BlobManagerCoinStashBalances {
            wal_balance: blob_manager.coin_stash.wal_balance,
            sui_balance: blob_manager.coin_stash.sui_balance,
        })
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
            .get_dynamic_field(self.data.blobs_table_id(), TypeTag::U256, blob_id_bytes)
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

    /// Gets the object ID of a ManagedBlob by blob_id.
    ///
    /// This is a convenience method that queries the on-chain ManagedBlob and returns
    /// just its object ID. This is useful for constructing `BlobPersistenceType::Deletable`
    /// when using upload relay for managed blobs.
    ///
    /// # Arguments
    ///
    /// * `blob_id` - The blob ID to look up.
    /// * `deletable` - Whether the blob should be deletable (for permanency conflict check).
    ///
    /// # Returns
    ///
    /// The `ObjectID` of the ManagedBlob on-chain.
    pub async fn get_managed_blob_object_id(
        &self,
        blob_id: BlobId,
        deletable: bool,
    ) -> ClientResult<ObjectID> {
        let managed_blob = self.get_managed_blob(blob_id, deletable).await?;
        Ok(managed_blob.id)
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
            self.data.manager_id(),
            self.data.cap_id(),
            blob_metadata_list.len()
        );

        // Register blobs in BlobManager.
        self.client
            .sui_client()
            .reserve_and_register_managed_blobs(
                self.data.manager_id(),
                self.data.cap_id(),
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
                            manager_id: self.data.manager_id(),
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
                            manager_id: self.data.manager_id(),
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
            .certify_managed_blobs_in_blobmanager(
                self.data.manager_id(),
                self.data.cap_id(),
                &cert_params,
            )
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
            self.data.manager_id(),
            self.data.cap_id(),
            blob_id
        );

        // Call the Sui contract to delete the blob.
        // The contract will handle all validation:
        // - Check if the blob exists
        // - Verify it's deletable (not permanent)
        // - Ensure it belongs to this BlobManager
        self.client
            .sui_client()
            .delete_managed_blob(self.data.manager_id(), self.data.cap_id(), blob_id)
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
            self.data.manager_id(),
            wal_amount
        );

        self.client
            .sui_client()
            .deposit_wal_to_blob_manager(self.data.manager_id(), wal_amount)
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
            self.data.manager_id(),
            sui_amount
        );

        self.client
            .sui_client()
            .deposit_sui_to_blob_manager(self.data.manager_id(), sui_amount)
            .await
            .map_err(crate::error::ClientError::from)?;

        Ok(())
    }

    /// Buys additional storage capacity using funds from the coin stash.
    ///
    /// This increases the BlobManager's storage capacity by purchasing more storage.
    /// The new storage uses the same epoch range as the existing storage.
    /// The funds are taken from the coin stash's WAL balance.
    ///
    /// # Arguments
    ///
    /// * `storage_amount` - The amount of storage to buy (in bytes).
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the storage was successfully purchased.
    /// * `Err(ClientError)` if:
    ///   - The coin stash has insufficient WAL balance.
    ///   - The transaction fails.
    pub async fn buy_storage_from_stash(&self, storage_amount: u64) -> ClientResult<()> {
        tracing::info!(
            "BlobManager buy_storage: manager_id={:?}, cap={:?}, amount={}",
            self.data.manager_id(),
            self.data.cap_id(),
            storage_amount,
        );

        self.client
            .sui_client()
            .buy_storage_from_stash(self.data.manager_id(), self.data.cap_id(), storage_amount)
            .await
            .map_err(crate::error::ClientError::from)?;

        Ok(())
    }

    /// Withdraws a specific amount of WAL funds from the coin stash.
    ///
    /// This method can only be called by someone with a fund_manager or admin capability.
    /// It withdraws the specified amount of WAL tokens from the coin stash.
    ///
    /// # Arguments
    ///
    /// * `amount` - The amount of WAL tokens to withdraw.
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the withdrawal was successful.
    /// * `Err(ClientError)` if:
    ///   - The caller doesn't have fund_manager or admin permission.
    ///   - The amount exceeds the available balance.
    ///   - The transaction fails.
    pub async fn withdraw_wal(&self, amount: u64) -> ClientResult<()> {
        tracing::info!(
            "BlobManager withdraw_wal: manager_id={:?}, cap={:?}, amount={}",
            self.data.manager_id(),
            self.data.cap_id(),
            amount
        );

        self.client
            .sui_client()
            .withdraw_wal_from_blob_manager(self.data.manager_id(), self.data.cap_id(), amount)
            .await
            .map_err(crate::error::ClientError::from)?;

        Ok(())
    }

    /// Withdraws a specific amount of SUI funds from the coin stash.
    ///
    /// This method can only be called by someone with a fund_manager or admin capability.
    /// It withdraws the specified amount of SUI tokens from the coin stash.
    ///
    /// # Arguments
    ///
    /// * `amount` - The amount of SUI tokens to withdraw.
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the withdrawal was successful.
    /// * `Err(ClientError)` if:
    ///   - The caller doesn't have fund_manager or admin permission.
    ///   - The amount exceeds the available balance.
    ///   - The transaction fails.
    pub async fn withdraw_sui(&self, amount: u64) -> ClientResult<()> {
        tracing::info!(
            "BlobManager withdraw_sui: manager_id={:?}, cap={:?}, amount={}",
            self.data.manager_id(),
            self.data.cap_id(),
            amount
        );

        self.client
            .sui_client()
            .withdraw_sui_from_blob_manager(self.data.manager_id(), self.data.cap_id(), amount)
            .await
            .map_err(crate::error::ClientError::from)?;

        Ok(())
    }

    // ===== Extension Policy Management Methods =====

    /// Sets the extension policy to disabled.
    ///
    /// When disabled, no one can extend storage. All extension attempts will be rejected.
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
            self.data.manager_id(),
            self.data.cap_id()
        );

        self.client
            .sui_client()
            .set_extension_policy_disabled(self.data.manager_id(), self.data.cap_id())
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
            self.data.manager_id(),
            self.data.cap_id(),
            expiry_threshold_epochs,
            max_extension_epochs
        );

        self.client
            .sui_client()
            .set_extension_policy_constrained(
                self.data.manager_id(),
                self.data.cap_id(),
                expiry_threshold_epochs,
                max_extension_epochs,
            )
            .await
            .map_err(crate::error::ClientError::from)?;

        Ok(())
    }

    /// Set a fixed tip amount for transaction senders.
    ///
    /// Sets the amount of SUI (in MIST) that will be tipped to users who help
    /// execute storage operations like extensions. Tips are paid from the
    /// BlobManager's coin stash.
    ///
    /// This method requires a capability with fund_manager permission.
    ///
    /// # Arguments
    ///
    /// * `tip_amount` - The tip amount in MIST (1 SUI = 1,000,000,000 MIST).
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the policy was successfully set.
    /// * `Err(ClientError)` if:
    ///   - The caller doesn't have fund_manager permission.
    ///   - The transaction fails.
    pub async fn set_tip_policy_fixed_amount(&self, tip_amount: u64) -> ClientResult<()> {
        tracing::info!(
            "BlobManager set_tip_policy_fixed_amount: manager_id={:?}, cap={:?}, tip_amount={}",
            self.data.manager_id(),
            self.data.cap_id(),
            tip_amount
        );

        self.client
            .sui_client()
            .set_tip_policy_fixed_amount(self.data.manager_id(), self.data.cap_id(), tip_amount)
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
            self.data.manager_id(),
            self.data.cap_id(),
            is_admin,
            fund_manager
        );

        let new_cap_id = self
            .client
            .sui_client()
            .create_blob_manager_cap(
                self.data.manager_id(),
                self.data.cap_id(),
                is_admin,
                fund_manager,
            )
            .await
            .map_err(crate::error::ClientError::from)?;

        Ok(new_cap_id)
    }

    /// Revokes a capability, preventing it from being used for any future operations.
    /// Only an admin can revoke a capability.
    pub async fn revoke_cap(&self, cap_to_revoke_id: ObjectID) -> ClientResult<()> {
        tracing::info!(
            "BlobManager revoke_cap: manager_id={:?}, admin_cap={:?}, cap_to_revoke_id={}",
            self.data.manager_id(),
            self.data.cap_id(),
            cap_to_revoke_id
        );

        self.client
            .sui_client()
            .revoke_blob_manager_cap(self.data.manager_id(), self.data.cap_id(), cap_to_revoke_id)
            .await
            .map_err(crate::error::ClientError::from)?;

        Ok(())
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
            self.data.manager_id(),
            blob_id,
            key
        );

        self.client
            .sui_client()
            .set_managed_blob_attribute(
                self.data.manager_id(),
                self.data.cap_id(),
                blob_id,
                key,
                value,
            )
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
            self.data.manager_id(),
            blob_id,
            key
        );

        self.client
            .sui_client()
            .remove_managed_blob_attribute(self.data.manager_id(), self.data.cap_id(), blob_id, key)
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
            self.data.manager_id(),
            blob_id
        );

        self.client
            .sui_client()
            .clear_managed_blob_attributes(self.data.manager_id(), self.data.cap_id(), blob_id)
            .await
            .map_err(crate::error::ClientError::from)?;

        Ok(())
    }
}
