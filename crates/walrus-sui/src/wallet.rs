// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Wallet Context wrapper.

use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    sync::Arc,
};

use sui_package_management::LockCommand;
use sui_sdk::{
    rpc_types::{SuiObjectData, SuiTransactionBlockResponse},
    sui_client_config::SuiEnv,
    wallet_context::WalletContext,
};
use sui_types::{
    base_types::{ObjectID, ObjectRef, SuiAddress},
    crypto::EmptySignInfo,
    message_envelope::Envelope,
    transaction::{SenderSignedData, Transaction, TransactionData},
};

/// The `Wallet` struct wraps the `WalletContext` from the Sui SDK. This allows us to
/// reduce the scope of the `WalletContext` to only the methods we need.
#[derive(Clone)]
pub struct Wallet {
    active_address: SuiAddress,
    active_env: SuiEnv,
    config_path: PathBuf,
    wallet_context: Arc<WalletContext>,
}

impl std::fmt::Debug for Wallet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Wallet")
            .field("active_address", &self.active_address)
            .field("active_env", &self.active_env)
            .finish()
    }
}

/// The error type for the `Wallet` struct. Currently there are no custom errors, so we defer to
/// anyhow::Error.
pub type WalletError = anyhow::Error;

impl Wallet {
    /// Create a new Wallet.
    pub fn new(mut wallet_context: WalletContext) -> Result<Self, WalletError> {
        Ok(Self {
            active_address: wallet_context.active_address()?,
            active_env: wallet_context.config.get_active_env()?.clone(),
            config_path: wallet_context.config.path().to_path_buf(),
            wallet_context: Arc::new(wallet_context),
        })
    }

    /// Get the active address.
    pub fn active_address(&self) -> SuiAddress {
        self.active_address
    }

    /// Passes through to the `WalletContext` to sign a transaction.
    pub async fn sign_transaction(&self, transaction_data: &TransactionData) -> Transaction {
        self.wallet_context.sign_transaction(transaction_data).await
    }

    // TODO: WAL-820 move callsites to the RetriableSuiClient.
    /// Passes through to the `WalletContext` to get the reference gas price.
    #[deprecated(note = "Avoid this method. Use the RetriableSuiClient instead.")]
    pub async fn get_reference_gas_price(&self) -> Result<u64, WalletError> {
        self.wallet_context.get_reference_gas_price().await
    }

    // TODO: WAL-820 move callsites to the RetriableSuiClient.
    /// Passes through to the `WalletContext` to get an [`sui_types::base_types::ObjectRef`].
    #[deprecated(note = "Avoid this method. Use the RetriableSuiClient instead.")]
    pub async fn get_object_ref(&self, object_id: ObjectID) -> Result<ObjectRef, WalletError> {
        self.wallet_context.get_object_ref(object_id).await
    }

    // TODO: WAL-820 move callsites to the RetriableSuiClient.
    /// Passes through to the `WalletContext` to call execute_transaction_may_fail.
    #[deprecated(note = "Avoid this method. Use the RetriableSuiClient instead.")]
    pub async fn execute_transaction_may_fail(
        &self,
        signed_transaction: Envelope<SenderSignedData, EmptySignInfo>,
    ) -> Result<SuiTransactionBlockResponse, WalletError> {
        self.wallet_context
            .execute_transaction_may_fail(signed_transaction)
            .await
    }

    // TODO: WAL-820 move callsites to the RetriableSuiClient.
    /// Find a gas object which fits the budget
    #[deprecated(note = "Avoid this method. Use the RetriableSuiClient instead.")]
    pub async fn gas_for_owner_budget(
        &self,
        address: SuiAddress,
        budget: u64,
        forbidden_gas_objects: BTreeSet<ObjectID>,
    ) -> Result<(u64, SuiObjectData), WalletError> {
        self.wallet_context
            .gas_for_owner_budget(address, budget, forbidden_gas_objects)
            .await
    }

    /// Update the `Move.lock` file with automated address management info. See
    /// [`sui_package_management::update_lock_file`] for details.
    // TODO: WAL-821 After we bring in Sui v1.50, we should remove this method in favor of
    // update_lock_file_for_chain_env.
    pub async fn update_lock_file(
        &self,
        lock_command: LockCommand,
        install_dir: Option<PathBuf>,
        lock_file: Option<PathBuf>,
        response: &SuiTransactionBlockResponse,
    ) -> Result<(), WalletError> {
        sui_package_management::update_lock_file(
            &self.wallet_context,
            lock_command,
            install_dir,
            lock_file,
            response,
        )
        .await
    }

    /// Get the rpc_url for the active environment.
    pub fn get_rpc_url(&self) -> &str {
        &self.active_env.rpc
    }

    /// Get the path to the wallet configuration file.
    pub fn get_config_path(&self) -> &Path {
        &self.config_path
    }

    /// Get the active environment.
    pub fn get_active_env(&self) -> &SuiEnv {
        &self.active_env
    }
}
