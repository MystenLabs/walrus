// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A transaction builder for programmable transactions containing Walrus-related calls.

use std::{
    collections::{BTreeSet, HashSet},
    fmt::Debug,
    str::FromStr,
    sync::Arc,
};

use fastcrypto::traits::ToFromBytes;
use sui_sdk::rpc_types::SuiObjectDataOptions;
use sui_types::{
    base_types::{ObjectID, ObjectType, SuiAddress},
    programmable_transaction_builder::ProgrammableTransactionBuilder,
    transaction::{Argument, Command, ObjectArg, ProgrammableTransaction},
    Identifier,
    SUI_CLOCK_OBJECT_ID,
    SUI_CLOCK_OBJECT_SHARED_VERSION,
};
use tokio::sync::OnceCell;
use tracing::instrument;
use walrus_core::{
    messages::{ConfirmationCertificate, InvalidBlobCertificate, ProofOfPossession},
    Epoch,
    EpochCount,
    NetworkPublicKey,
    BlobId,
};

use super::{
    read_client::Mutability,
    BlobObjectMetadata,
    BlobPersistence,
    CoinType,
    ReadClient,
    SuiClientError,
    SuiClientResult,
    SuiReadClient,
};
use crate::{
    contracts::{self, FunctionTag},
    types::{
        move_structs::{Authorized, BlobAttribute, WalExchange},
        NetworkAddress,
        NodeMetadata,
        NodeRegistrationParams,
        NodeUpdateParams,
        SystemObject,
        UpdatePublicKeyParams,
    },
    utils::{price_for_encoded_length, write_price_for_encoded_length},
};

const CLOCK_OBJECT_ARG: ObjectArg = ObjectArg::SharedObject {
    id: SUI_CLOCK_OBJECT_ID,
    initial_shared_version: SUI_CLOCK_OBJECT_SHARED_VERSION,
    mutable: false,
};

/// The maximum number of blobs that can be burned in a single PTB.
/// This number is chosen just below the maximum number of commands in a PTB (1024).
// NB: this should be kept in sync with the maximum number of commands in the Sui `ProtocolConfig`.
pub const MAX_BURNS_PER_PTB: usize = 1000;

#[derive(Debug, Clone, Copy)]
/// A wrapper around an [`Argument`] or an [`ObjectID`] for use in [`WalrusPtbBuilder`].
pub enum ArgumentOrOwnedObject {
    /// An [`Argument`].
    Argument(Argument),
    /// An [`ObjectID`].
    Object(ObjectID),
}

impl From<Argument> for ArgumentOrOwnedObject {
    fn from(arg: Argument) -> Self {
        Self::Argument(arg)
    }
}

impl From<&Argument> for ArgumentOrOwnedObject {
    fn from(arg: &Argument) -> Self {
        Self::Argument(*arg)
    }
}

impl From<ObjectID> for ArgumentOrOwnedObject {
    fn from(obj: ObjectID) -> Self {
        Self::Object(obj)
    }
}

impl From<&ObjectID> for ArgumentOrOwnedObject {
    fn from(obj: &ObjectID) -> Self {
        Self::Object(*obj)
    }
}

/// A PTB builder for Walrus transactions.
pub struct WalrusPtbBuilder {
    pt_builder: ProgrammableTransactionBuilder,
    read_client: Arc<SuiReadClient>,
    tx_wal_balance: u64,
    tx_sui_cost: u64,
    used_wal_coins: BTreeSet<ObjectID>,
    wal_coin_arg: Option<Argument>,
    sender_address: SuiAddress,
    args_to_consume: HashSet<Argument>,
    // TODO(WAL-512): revisit caching system/staking objects in the read client
    // TODO(WAL-514): potentially remove if no longer needed
    /// Caches the system object to allow reading information about e.g. the committee size.
    /// Since the Ptb builder is not long-lived (i.e. transactions may anyway fail across epoch
    /// boundaries), we can cache it for the builder's lifetime.
    system_object: OnceCell<SystemObject>,
}

impl Debug for WalrusPtbBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WalrusPtbBuilder")
            .field("read_client", &self.read_client)
            .field("tx_wal_balance", &self.tx_wal_balance)
            .field("tx_sui_cost", &self.tx_sui_cost)
            .field("used_wal_coins", &self.used_wal_coins)
            .field("wal_coin_arg", &self.wal_coin_arg)
            .field("sender_address", &self.sender_address)
            .field("args_to_consume", &self.args_to_consume)
            .finish()
    }
}

impl WalrusPtbBuilder {
    /// Constructor for [`WalrusPtbBuilder`].
    pub fn new(read_client: Arc<SuiReadClient>, sender_address: SuiAddress) -> Self {
        Self {
            pt_builder: ProgrammableTransactionBuilder::new(),
            read_client,
            tx_wal_balance: 0,
            tx_sui_cost: 0,
            used_wal_coins: BTreeSet::new(),
            wal_coin_arg: None,
            sender_address,
            args_to_consume: HashSet::new(),
            system_object: OnceCell::new(),
        }
    }

    /// Fills up the WAL coin argument of the PTB to at least `min_balance`.
    ///
    /// This function merges additional coins if necessary and is a no-op if the current available
    /// balance (that has already been added to the PTB and hasn't been consumed yet) is larger than
    /// `min_balance`.
    ///
    /// # Errors
    ///
    /// Returns a [`SuiClientError::NoCompatibleWalCoins`] if no WAL coins with sufficient balance
    /// can be found.
    pub async fn fill_wal_balance(&mut self, min_balance: u64) -> SuiClientResult<()> {
        if min_balance <= self.tx_wal_balance {
            return Ok(());
        }
        let additional_balance = min_balance - self.tx_wal_balance;
        let mut coins = self
            .read_client
            .get_coins_with_total_balance(
                self.sender_address,
                CoinType::Wal,
                additional_balance,
                self.used_wal_coins.iter().cloned().collect(),
            )
            .await?;
        let mut added_balance = 0;
        let main_coin = if let Some(coin_arg) = self.wal_coin_arg {
            coin_arg
        } else {
            let coin = coins
                .pop()
                .ok_or_else(|| SuiClientError::NoCompatibleWalCoins)?;
            // Make sure that we don't select the same coin later again.
            self.used_wal_coins.insert(coin.coin_object_id);
            added_balance += coin.balance;
            let coin_arg = self.pt_builder.input(coin.object_ref().into())?;
            self.wal_coin_arg = Some(coin_arg);
            coin_arg
        };
        if !coins.is_empty() {
            let coin_args = coins
                .into_iter()
                .map(|coin| {
                    // Make sure that we don't select the same coin later again.
                    self.used_wal_coins.insert(coin.coin_object_id);
                    added_balance += coin.balance;
                    self.pt_builder.input(coin.object_ref().into())
                })
                .collect::<Result<Vec<_>, _>>()?;
            self.pt_builder
                .command(Command::MergeCoins(main_coin, coin_args));
        }
        self.tx_wal_balance += added_balance;
        Ok(())
    }

    fn reduce_wal_balance(&mut self, amount: u64) -> SuiClientResult<()> {
        if amount > self.tx_wal_balance {
            return Err(SuiClientError::Internal(anyhow::anyhow!(
                "trying to reduce WAL balance below 0"
            )));
        }
        self.tx_wal_balance -= amount;
        Ok(())
    }

    /// Adds a move call to a function in the Walrus package to the PTB.
    ///
    /// Always returns an [`Argument::Result`] if no error is returned.
    pub(crate) fn walrus_move_call(
        &mut self,
        function: FunctionTag<'_>,
        arguments: Vec<Argument>,
    ) -> SuiClientResult<Argument> {
        self.move_call(
            self.read_client.get_system_package_id(),
            function,
            arguments,
        )
    }

    /// Adds a move call to the PTB.
    ///
    /// Always returns an [`Argument::Result`] if no error is returned.
    pub(crate) fn move_call(
        &mut self,
        package_id: ObjectID,
        function: FunctionTag<'_>,
        arguments: Vec<Argument>,
    ) -> SuiClientResult<Argument> {
        tracing::info!("package_id: {:?}", package_id);
        tracing::info!("function: {:?}", function);
        tracing::info!("arguments: {:?}", arguments);
        Ok(self.pt_builder.programmable_move_call(
            package_id,
            Identifier::from_str(function.module)?,
            Identifier::from_str(function.name)?,
            function.type_params,
            arguments,
        ))
    }

    /// Adds a call to `reserve_space` to the `pt_builder` and returns the result [`Argument`].
    pub async fn reserve_space(
        &mut self,
        encoded_size: u64,
        epochs_ahead: EpochCount,
    ) -> SuiClientResult<Argument> {
        let price = self
            .storage_price_for_encoded_length(encoded_size, epochs_ahead)
            .await?;
        self.fill_wal_balance(price).await?;

        let reserve_arguments = vec![
            self.system_arg(Mutability::Mutable).await?,
            self.pt_builder.pure(encoded_size)?,
            self.pt_builder.pure(epochs_ahead)?,
            self.wal_coin_arg()?,
        ];
        let result_arg =
            self.walrus_move_call(contracts::system::reserve_space, reserve_arguments)?;
        self.reduce_wal_balance(price)?;
        self.add_result_to_be_consumed(result_arg);
        Ok(result_arg)
    }

    /// Adds a call to `register_blob` to the `pt_builder` and returns the result [`Argument`].
    pub async fn register_blob(
        &mut self,
        storage_resource: ArgumentOrOwnedObject,
        blob_metadata: BlobObjectMetadata,
        persistence: BlobPersistence,
    ) -> SuiClientResult<Argument> {
        let price = self
            .write_price_for_encoded_length(blob_metadata.encoded_size)
            .await?;
        self.fill_wal_balance(price).await?;

        let storage_resource_arg = self.argument_from_arg_or_obj(storage_resource).await?;

        let register_arguments = vec![
            self.system_arg(Mutability::Mutable).await?,
            storage_resource_arg,
            self.pt_builder.pure(blob_metadata.blob_id)?,
            self.pt_builder.pure(blob_metadata.root_hash.bytes())?,
            self.pt_builder.pure(blob_metadata.unencoded_size)?,
            self.pt_builder
                .pure(u8::from(blob_metadata.encoding_type))?,
            self.pt_builder.pure(persistence.is_deletable())?,
            self.wal_coin_arg()?,
        ];
        let result_arg =
            self.walrus_move_call(contracts::system::register_blob, register_arguments)?;
        self.reduce_wal_balance(price)?;
        self.mark_arg_as_consumed(&storage_resource_arg);
        self.add_result_to_be_consumed(result_arg);
        Ok(result_arg)
    }

    /// Adds a call to `certify_blob` to the `pt_builder`.
    pub async fn certify_blob(
        &mut self,
        blob_object: ArgumentOrOwnedObject,
        certificate: &ConfirmationCertificate,
    ) -> SuiClientResult<()> {
        let signers = self.signers_to_bitmap(&certificate.signers).await?;

        let certify_args = vec![
            self.system_arg(Mutability::Immutable).await?,
            self.argument_from_arg_or_obj(blob_object).await?,
            self.pt_builder.pure(certificate.signature.as_bytes())?,
            self.pt_builder.pure(&signers)?,
            self.pt_builder.pure(&certificate.serialized_message)?,
        ];
        self.walrus_move_call(contracts::system::certify_blob, certify_args)?;
        Ok(())
    }

    // TODO(WAL-514): simplify and remove rpc call
    async fn signers_to_bitmap(&self, signers: &[u16]) -> SuiClientResult<Vec<u8>> {
        let committee_size = self.system_object().await?.committee_size() as usize;
        let mut bitmap = vec![0; committee_size.div_ceil(8)];
        for signer in signers {
            let byte_index = signer / 8;
            let bit_index = signer % 8;
            bitmap[byte_index as usize] |= 1 << bit_index;
        }
        Ok(bitmap)
    }

    /// Adds a call to `certify_event_blob` to the `pt_builder`.
    pub async fn certify_event_blob(
        &mut self,
        blob_metadata: BlobObjectMetadata,
        storage_node_cap: ArgumentOrOwnedObject,
        ending_checkpoint_seq_num: u64,
        epoch: u32,
    ) -> SuiClientResult<()> {
        let arguments = vec![
            self.system_arg(Mutability::Mutable).await?,
            self.argument_from_arg_or_obj(storage_node_cap).await?,
            self.pt_builder.pure(blob_metadata.blob_id)?,
            self.pt_builder.pure(blob_metadata.root_hash.bytes())?,
            self.pt_builder.pure(blob_metadata.unencoded_size)?,
            self.pt_builder
                .pure(u8::from(blob_metadata.encoding_type))?,
            self.pt_builder.pure(ending_checkpoint_seq_num)?,
            self.pt_builder.pure(epoch)?,
        ];
        self.walrus_move_call(contracts::system::certify_event_blob, arguments)?;
        Ok(())
    }

    /// Adds a call to `delete_blob` to the `pt_builder` and returns the result [`Argument`].
    pub async fn delete_blob(
        &mut self,
        blob_object: ArgumentOrOwnedObject,
    ) -> SuiClientResult<Argument> {
        let blob_arg = self.argument_from_arg_or_obj(blob_object).await?;
        let delete_arguments = vec![self.system_arg(Mutability::Mutable).await?, blob_arg];
        let result_arg = self.walrus_move_call(contracts::system::delete_blob, delete_arguments)?;
        self.mark_arg_as_consumed(&blob_arg);
        self.add_result_to_be_consumed(result_arg);
        Ok(result_arg)
    }

    /// Adds a call to `burn` the blob to the `pt_builder`.
    pub async fn burn_blob(&mut self, blob_object: ArgumentOrOwnedObject) -> SuiClientResult<()> {
        let blob_arg = self.argument_from_arg_or_obj(blob_object).await?;
        self.walrus_move_call(contracts::blob::burn, vec![blob_arg])?;
        self.mark_arg_as_consumed(&blob_arg);
        Ok(())
    }

    /// Adds a call to create a new instance of Metadata and returns the result [`Argument`].
    pub async fn new_metadata(&mut self) -> SuiClientResult<Argument> {
        let result_arg = self.walrus_move_call(contracts::metadata::new, vec![])?;
        self.add_result_to_be_consumed(result_arg);
        Ok(result_arg)
    }

    /// Adds a call to insert or update a key-value pair in a Metadata object.
    pub async fn insert_or_update_blob_attribute(
        &mut self,
        blob_attribute: ArgumentOrOwnedObject,
        key: String,
        value: String,
    ) -> SuiClientResult<()> {
        let metadata_arg = self.argument_from_arg_or_obj(blob_attribute).await?;
        let key_arg = self.pt_builder.pure(key)?;
        let value_arg = self.pt_builder.pure(value)?;
        self.walrus_move_call(
            contracts::metadata::insert_or_update,
            vec![metadata_arg, key_arg, value_arg],
        )?;
        Ok(())
    }

    /// Adds a call to add metadata to a blob.
    pub async fn add_blob_attribute(
        &mut self,
        blob_object: ArgumentOrOwnedObject,
        blob_attribute: BlobAttribute,
    ) -> SuiClientResult<()> {
        // Create a new metadata object
        let metadata_arg = self.new_metadata().await?;

        // Iterate through the passed-in metadata and populate the move metadata
        for (key, value) in blob_attribute.iter() {
            self.insert_or_update_blob_attribute(metadata_arg.into(), key.clone(), value.clone())
                .await?;
        }
        let blob_arg = self.argument_from_arg_or_obj(blob_object).await?;
        self.walrus_move_call(contracts::blob::add_metadata, vec![blob_arg, metadata_arg])?;
        self.mark_arg_as_consumed(&metadata_arg);
        Ok(())
    }

    /// Adds a call to remove metadata dynamic field from a blob and returns the
    /// result [`Argument`].
    ///
    /// Note the [`BlobAttribute`] corresponds to the `metadata::Metadata` in the contract.
    pub async fn remove_blob_attribute(
        &mut self,
        blob_object: ArgumentOrOwnedObject,
    ) -> SuiClientResult<Argument> {
        let blob_arg = self.argument_from_arg_or_obj(blob_object).await?;
        let result_arg = self.walrus_move_call(contracts::blob::take_metadata, vec![blob_arg])?;
        Ok(result_arg)
    }

    /// Adds calls to insert or update multiple metadata key-value pairs in a blob.
    pub async fn insert_or_update_blob_attribute_pairs<I, T>(
        &mut self,
        blob_object: ArgumentOrOwnedObject,
        pairs: I,
    ) -> SuiClientResult<()>
    where
        I: IntoIterator<Item = (T, T)>,
        T: Into<String>,
    {
        let blob_arg = self.argument_from_arg_or_obj(blob_object).await?;

        for (key, value) in pairs {
            let key_arg = self.pt_builder.pure(key.into())?;
            let value_arg = self.pt_builder.pure(value.into())?;
            self.walrus_move_call(
                contracts::blob::insert_or_update_metadata_pair,
                vec![blob_arg, key_arg, value_arg],
            )?;
        }
        Ok(())
    }

    /// Adds calls to remove multiple metadata key-value pairs from a blob.
    pub async fn remove_blob_attribute_pairs<I, K>(
        &mut self,
        blob_object: ArgumentOrOwnedObject,
        keys: I,
    ) -> SuiClientResult<()>
    where
        I: IntoIterator<Item = K>,
        K: AsRef<str>,
    {
        let blob_arg = self.argument_from_arg_or_obj(blob_object).await?;

        for key in keys {
            let key_arg = self.pt_builder.pure(key.as_ref().to_string())?;
            self.walrus_move_call(
                contracts::blob::remove_metadata_pair,
                vec![blob_arg, key_arg],
            )?;
        }
        Ok(())
    }

    /// Adds a call to create a new shared blob from the blob.
    pub async fn new_shared_blob(
        &mut self,
        blob_object: ArgumentOrOwnedObject,
    ) -> SuiClientResult<()> {
        let blob_arg = self.argument_from_arg_or_obj(blob_object).await?;
        self.walrus_move_call(contracts::shared_blob::new, vec![blob_arg])?;
        self.mark_arg_as_consumed(&blob_arg);
        Ok(())
    }

    /// Adds a call to create a new shared blob and fund it.
    pub async fn new_funded_shared_blob(
        &mut self,
        blob_object: ArgumentOrOwnedObject,
        amount: u64,
    ) -> SuiClientResult<()> {
        let blob_arg = self.argument_from_arg_or_obj(blob_object).await?;
        // Split the amount from the main WAL coin.
        self.fill_wal_balance(amount).await?;
        let split_main_coin_arg = self.wal_coin_arg()?;
        let split_amount_arg = self.pt_builder.pure(amount)?;
        let split_coin = self.pt_builder.command(Command::SplitCoins(
            split_main_coin_arg,
            vec![split_amount_arg],
        ));
        self.walrus_move_call(
            contracts::shared_blob::new_funded,
            vec![blob_arg, split_coin],
        )?;
        self.mark_arg_as_consumed(&blob_arg);
        self.reduce_wal_balance(amount)?;
        Ok(())
    }

    /// Adds a call to fund a shared blob.
    pub async fn fund_shared_blob(
        &mut self,
        shared_blob_object_id: ObjectID,
        amount: u64,
    ) -> SuiClientResult<()> {
        let shared_blob_arg = self.pt_builder.obj(
            self.read_client
                .object_arg_for_shared_obj(shared_blob_object_id, Mutability::Mutable)
                .await?,
        )?;
        // Split the amount from the main WAL coin.
        self.fill_wal_balance(amount).await?;
        let split_main_coin_arg = self.wal_coin_arg()?;
        let split_amount_arg = self.pt_builder.pure(amount)?;
        let split_coin = self.pt_builder.command(Command::SplitCoins(
            split_main_coin_arg,
            vec![split_amount_arg],
        ));

        let args = vec![shared_blob_arg, split_coin];
        self.walrus_move_call(contracts::shared_blob::fund, args)?;
        self.reduce_wal_balance(amount)?;
        Ok(())
    }

    /// Adds a call to extend a shared blob.
    pub async fn extend_shared_blob(
        &mut self,
        shared_blob_object_id: ObjectID,
        epochs_ahead: Epoch,
    ) -> SuiClientResult<()> {
        let shared_blob_arg = self.pt_builder.obj(
            self.read_client
                .object_arg_for_shared_obj(shared_blob_object_id, Mutability::Mutable)
                .await?,
        )?;
        let args = vec![
            shared_blob_arg,
            self.system_arg(Mutability::Mutable).await?,
            self.pt_builder.pure(epochs_ahead)?,
        ];
        self.walrus_move_call(contracts::shared_blob::extend, args)?;
        Ok(())
    }

    /// Adds a call to extend an owned blob.
    pub async fn extend_blob(
        &mut self,
        blob_object: ArgumentOrOwnedObject,
        epochs_ahead: EpochCount,
        encoded_size: u64,
    ) -> SuiClientResult<()> {
        let price = self
            .storage_price_for_encoded_length(encoded_size, epochs_ahead)
            .await?;

        self.fill_wal_balance(price).await?;

        let args = vec![
            self.system_arg(Mutability::Mutable).await?,
            self.argument_from_arg_or_obj(blob_object).await?,
            self.pt_builder.pure(epochs_ahead)?,
            self.wal_coin_arg()?,
        ];
        self.walrus_move_call(contracts::system::extend_blob, args)?;
        self.reduce_wal_balance(price)?;
        Ok(())
    }
    /// Adds a transfer to the PTB. If the recipient is `None`, the sender address is used.
    pub async fn transfer<I: IntoIterator<Item = ArgumentOrOwnedObject>>(
        &mut self,
        recipient: Option<SuiAddress>,
        to_transfer: I,
    ) -> SuiClientResult<()> {
        let mut args = vec![];
        for arg_or_obj in to_transfer {
            args.push(self.argument_from_arg_or_obj(arg_or_obj).await?);
        }
        args.iter().for_each(|arg| self.mark_arg_as_consumed(arg));
        self.pt_builder
            .transfer_args(recipient.unwrap_or(self.sender_address), args);
        Ok(())
    }

    /// Transfers all outputs that have not been consumed yet by another command in the PTB.
    ///
    /// If the recipient is `None`, the sender address is used.
    pub async fn transfer_remaining_outputs(
        &mut self,
        recipient: Option<SuiAddress>,
    ) -> SuiClientResult<()> {
        if self.args_to_consume.is_empty() {
            return Ok(());
        }
        let args: Vec<_> = self.args_to_consume.iter().map(|arg| arg.into()).collect();
        self.transfer(recipient, args).await
    }

    /// Splits off `amount` from the gas coin, adds a call to `exchange_all_for_wal` to the PTB
    /// and merges the WAL coins into the payment coin of the PTB.
    pub async fn exchange_sui_for_wal(
        &mut self,
        exchange_id: ObjectID,
        amount: u64,
    ) -> SuiClientResult<()> {
        let exchange: WalExchange = self
            .read_client
            .sui_client()
            .get_sui_object(exchange_id)
            .await?;
        // We can get the package ID from the exchange object because we only use it in testnet
        // and the exchange is currently not designed for upgrades.
        let exchange_package = self
            .read_client
            .sui_client()
            .get_package_id_from_object(exchange_id)
            .await?;
        let exchange_arg = self.pt_builder.obj(
            self.read_client
                .object_arg_for_shared_obj(exchange_id, Mutability::Mutable)
                .await?,
        )?;
        self.tx_sui_cost += amount;
        let amount_arg = self.pt_builder.pure(amount)?;

        let split_coin = self
            .pt_builder
            .command(Command::SplitCoins(Argument::GasCoin, vec![amount_arg]));

        let result_arg = self.move_call(
            exchange_package,
            contracts::wal_exchange::exchange_all_for_wal,
            vec![exchange_arg, split_coin],
        )?;
        let wal_amount = exchange.exchange_rate.sui_to_wal(amount);
        self.tx_wal_balance += wal_amount;
        match self.wal_coin_arg {
            Some(wal_coin_arg) => {
                self.pt_builder
                    .command(Command::MergeCoins(wal_coin_arg, vec![result_arg]));
            }
            None => {
                // This coin needs to be consumed by another function or transferred at the end.
                self.add_result_to_be_consumed(result_arg);
                self.wal_coin_arg = Some(result_arg);
            }
        }
        Ok(())
    }

    /// Adds a call to create a new exchange, funded with `amount` WAL, to the PTB.
    pub async fn create_and_fund_exchange(
        &mut self,
        exchange_package: ObjectID,
        amount: u64,
    ) -> SuiClientResult<Argument> {
        self.fill_wal_balance(amount).await?;
        let args = vec![self.wal_coin_arg()?, self.pt_builder.pure(amount)?];
        let result_arg =
            self.move_call(exchange_package, contracts::wal_exchange::new_funded, args)?;
        self.reduce_wal_balance(amount)?;
        self.add_result_to_be_consumed(result_arg);
        Ok(result_arg)
    }

    /// Adds a call to `invalidate_blob_id` to the PTB.
    pub async fn invalidate_blob_id(
        &mut self,
        certificate: &InvalidBlobCertificate,
    ) -> SuiClientResult<()> {
        let signers = self.signers_to_bitmap(&certificate.signers).await?;

        let invalidate_args = vec![
            self.system_arg(Mutability::Immutable).await?,
            self.pt_builder.pure(certificate.signature.as_bytes())?,
            self.pt_builder.pure(&signers)?,
            self.pt_builder.pure(&certificate.serialized_message)?,
        ];
        self.walrus_move_call(contracts::system::invalidate_blob_id, invalidate_args)?;
        Ok(())
    }

    /// Adds a call to `epoch_sync_done` to the PTB.
    pub async fn epoch_sync_done(
        &mut self,
        storage_node_cap: ArgumentOrOwnedObject,
        epoch: Epoch,
    ) -> SuiClientResult<()> {
        let args = vec![
            self.staking_arg(Mutability::Mutable).await?,
            self.argument_from_arg_or_obj(storage_node_cap).await?,
            self.pt_builder.pure(epoch)?,
            self.pt_builder.obj(CLOCK_OBJECT_ARG)?,
        ];
        self.walrus_move_call(contracts::staking::epoch_sync_done, args)?;
        Ok(())
    }

    /// Adds a call to initiate epoch change to the PTB.
    pub async fn initiate_epoch_change(&mut self) -> SuiClientResult<()> {
        let args = vec![
            self.staking_arg(Mutability::Mutable).await?,
            self.system_arg(Mutability::Mutable).await?,
            self.pt_builder.obj(CLOCK_OBJECT_ARG)?,
        ];
        self.walrus_move_call(contracts::staking::initiate_epoch_change, args)?;
        Ok(())
    }

    /// Adds a call to `voting_end` to the PTB.
    pub async fn voting_end(&mut self) -> SuiClientResult<()> {
        let args = vec![
            self.staking_arg(Mutability::Mutable).await?,
            self.pt_builder.obj(CLOCK_OBJECT_ARG)?,
        ];
        self.walrus_move_call(contracts::staking::voting_end, args)?;
        Ok(())
    }

    /// Adds a call to `stake_with_pool` to the PTB.
    pub async fn stake_with_pool(
        &mut self,
        amount: u64,
        node_id: ObjectID,
    ) -> SuiClientResult<Argument> {
        self.fill_wal_balance(amount).await?;

        // Split the amount to stake from the main WAL coin.
        let split_main_coin_arg = self.wal_coin_arg()?;
        let split_amount_arg = self.pt_builder.pure(amount)?;
        let split_coin = self.pt_builder.command(Command::SplitCoins(
            split_main_coin_arg,
            vec![split_amount_arg],
        ));

        // Stake the split coin.
        let staking_args = vec![
            self.staking_arg(Mutability::Mutable).await?,
            split_coin,
            self.pt_builder.pure(node_id)?,
        ];
        let result_arg =
            self.walrus_move_call(contracts::staking::stake_with_pool, staking_args)?;
        self.reduce_wal_balance(amount)?;
        self.add_result_to_be_consumed(result_arg);
        Ok(result_arg)
    }

    /// Adds a call to `register_candidate` to the PTB.
    pub async fn register_candidate(
        &mut self,
        node_parameters: &NodeRegistrationParams,
        proof_of_possession: ProofOfPossession,
    ) -> SuiClientResult<Argument> {
        let node_metadata_arg = self.create_node_metadata(&node_parameters.metadata).await?;
        let args = vec![
            self.staking_arg(Mutability::Mutable).await?,
            self.pt_builder.pure(&node_parameters.name)?,
            self.pt_builder
                .pure(node_parameters.network_address.to_string())?,
            node_metadata_arg,
            self.pt_builder
                .pure(node_parameters.public_key.as_bytes())?,
            self.pt_builder
                .pure(node_parameters.network_public_key.as_bytes())?,
            self.pt_builder
                .pure(proof_of_possession.signature.as_bytes())?,
            self.pt_builder.pure(node_parameters.commission_rate)?,
            self.pt_builder.pure(node_parameters.storage_price)?,
            self.pt_builder.pure(node_parameters.write_price)?,
            self.pt_builder.pure(node_parameters.node_capacity)?,
        ];
        let result_arg = self.walrus_move_call(contracts::staking::register_candidate, args)?;
        self.add_result_to_be_consumed(result_arg);
        Ok(result_arg)
    }

    /// Adds a call to `create_node_metadata` to the PTB and returns the result [`Argument`].
    pub async fn create_node_metadata(
        &mut self,
        node_metadata: &NodeMetadata,
    ) -> SuiClientResult<Argument> {
        let args = vec![
            self.pt_builder.pure(&node_metadata.image_url)?,
            self.pt_builder
                .pure(node_metadata.project_url.to_string())?,
            self.pt_builder
                .pure(node_metadata.description.to_string())?,
        ];
        let result_arg = self.walrus_move_call(contracts::node_metadata::new, args)?;
        Ok(result_arg)
    }

    /// Adds a call to update the node metadata by first creating the metadata and then calling
    /// `set_node_metadata` with the metadata argument.
    pub async fn set_node_metadata(
        &mut self,
        storage_node_cap: ArgumentOrOwnedObject,
        node_metadata: &NodeMetadata,
    ) -> SuiClientResult<()> {
        let metadata_arg = self.create_node_metadata(node_metadata).await?;
        let args = vec![
            self.staking_arg(Mutability::Mutable).await?,
            self.argument_from_arg_or_obj(storage_node_cap).await?,
            metadata_arg,
        ];
        self.walrus_move_call(contracts::staking::set_node_metadata, args)?;
        Ok(())
    }

    /// Sends `amount` WAL to `recipient`.
    pub async fn pay_wal(&mut self, recipient: SuiAddress, amount: u64) -> SuiClientResult<()> {
        self.fill_wal_balance(amount).await?;
        let amount_arg = self.pt_builder.pure(amount)?;
        let wal_coin_arg = self.wal_coin_arg()?;
        let split_coin = self
            .pt_builder
            .command(Command::SplitCoins(wal_coin_arg, vec![amount_arg]));
        self.transfer(Some(recipient), vec![split_coin.into()])
            .await?;
        self.reduce_wal_balance(amount)?;
        Ok(())
    }

    /// Authenticates the sender address. Returns an `Authenticated` Move type as result argument.
    pub fn authenticate_sender(&mut self) -> SuiClientResult<Argument> {
        let result_arg = self.walrus_move_call(contracts::auth::authenticate_sender, vec![])?;
        Ok(result_arg)
    }

    /// Authenticates using an object as capability. Returns an `Authenticated` Move type as result
    /// argument.
    ///
    /// Since the move call is generic, we need the object ID here to determine the correct type
    /// argument (instead of allowing an `ArgumentOrOwnedObject`).
    pub async fn authenticate_with_object(
        &mut self,
        object: ObjectID,
    ) -> SuiClientResult<Argument> {
        let object_data = self
            .read_client
            .sui_client()
            .get_object_with_options(object, SuiObjectDataOptions::new().with_type())
            .await?
            .data
            .ok_or_else(|| anyhow::anyhow!("no object data returned"))?;
        let ObjectType::Struct(object_type) = object_data.object_type()? else {
            return Err(anyhow::anyhow!("object is not a struct").into());
        };
        let object_ref = object_data.object_ref();
        let object_arg = self
            .pt_builder
            .obj(ObjectArg::ImmOrOwnedObject(object_ref))?;
        let result_arg = self.walrus_move_call(
            contracts::auth::authenticate_with_object.with_type_params(&[object_type.into()]),
            vec![object_arg],
        )?;
        Ok(result_arg)
    }

    /// Creates an `Authorized` Move type for the given address and returns it as result argument.
    pub fn authorized_address(&mut self, address: SuiAddress) -> SuiClientResult<Argument> {
        let address_arg = self.pt_builder.pure(address)?;
        let result_arg =
            self.walrus_move_call(contracts::auth::authorized_address, vec![address_arg])?;
        Ok(result_arg)
    }

    /// Creates an `Authorized` Move type for the given object and returns it as result argument.
    pub fn authorized_object(&mut self, object_id: ObjectID) -> SuiClientResult<Argument> {
        let object_id_arg = self.pt_builder.pure(object_id)?;
        let result_arg =
            self.walrus_move_call(contracts::auth::authorized_object, vec![object_id_arg])?;
        Ok(result_arg)
    }

    #[instrument(err, skip(self))]
    /// Creates an `Authorized` Move type for the given address or object and returns it as result
    /// argument.
    pub fn authorized_address_or_object(
        &mut self,
        authorized: Authorized,
    ) -> SuiClientResult<Argument> {
        match authorized {
            Authorized::Address(address) => self.authorized_address(address),
            Authorized::Object(object_id) => self.authorized_object(object_id),
        }
    }

    #[instrument(err, skip(self))]
    /// Sets the commission receiver for the node.
    pub async fn set_commission_receiver(
        &mut self,
        node_id: ObjectID,
        authenticated: Argument,
        receiver: Argument,
    ) -> SuiClientResult<()> {
        let args = vec![
            self.staking_arg(Mutability::Mutable).await?,
            self.pt_builder.pure(node_id)?,
            authenticated,
            receiver,
        ];
        self.walrus_move_call(contracts::staking::set_commission_receiver, args)?;
        Ok(())
    }

    #[instrument(err, skip(self))]
    /// Sets the governance authorized object for the pool.
    pub async fn set_governance_authorized(
        &mut self,
        node_id: ObjectID,
        authenticated: Argument,
        authorized: Argument,
    ) -> SuiClientResult<()> {
        let args = vec![
            self.staking_arg(Mutability::Mutable).await?,
            self.pt_builder.pure(node_id)?,
            authenticated,
            authorized,
        ];
        self.walrus_move_call(contracts::staking::set_governance_authorized, args)?;
        Ok(())
    }

    /// Updates node parameters based on the provided NodeUpdateParams
    pub async fn update_node_params(
        &mut self,
        storage_node_cap: ArgumentOrOwnedObject,
        params: NodeUpdateParams,
    ) -> SuiClientResult<()> {
        if let Some(name) = params.name {
            self.update_node_name(&storage_node_cap, name).await?;
        }

        if let Some(update_public_key) = params.update_public_key {
            self.update_next_public_key(&storage_node_cap, update_public_key)
                .await?;
        }

        if let Some(network_public_key) = params.network_public_key {
            self.update_network_public_key(&storage_node_cap, network_public_key)
                .await?;
        }

        if let Some(network_address) = params.network_address {
            self.update_network_address(&storage_node_cap, network_address)
                .await?;
        }

        if let Some(storage_price) = params.storage_price {
            self.update_storage_price(&storage_node_cap, storage_price)
                .await?;
        }

        if let Some(write_price) = params.write_price {
            self.update_write_price(&storage_node_cap, write_price)
                .await?;
        }

        if let Some(node_capacity) = params.node_capacity {
            self.update_node_capacity(&storage_node_cap, node_capacity)
                .await?;
        }

        Ok(())
    }

    /// Updates the node name.
    pub async fn update_node_name(
        &mut self,
        storage_node_cap: &ArgumentOrOwnedObject,
        name: String,
    ) -> SuiClientResult<()> {
        let args = vec![
            self.staking_arg(Mutability::Mutable).await?,
            self.argument_from_arg_or_obj(*storage_node_cap).await?,
            self.pt_builder.pure(name)?,
        ];
        self.walrus_move_call(contracts::staking::set_name, args)?;
        Ok(())
    }

    /// Updates the next public key of the node.
    pub async fn update_next_public_key(
        &mut self,
        storage_node_cap: &ArgumentOrOwnedObject,
        params: UpdatePublicKeyParams,
    ) -> SuiClientResult<()> {
        let args = vec![
            self.staking_arg(Mutability::Mutable).await?,
            self.argument_from_arg_or_obj(*storage_node_cap).await?,
            self.pt_builder.pure(params.next_public_key.as_bytes())?,
            self.pt_builder
                .pure(params.proof_of_possession.signature.as_bytes())?,
        ];
        self.walrus_move_call(contracts::staking::set_next_public_key, args)?;
        Ok(())
    }

    /// Updates the network public key of the node.
    pub async fn update_network_public_key(
        &mut self,
        storage_node_cap: &ArgumentOrOwnedObject,
        network_public_key: NetworkPublicKey,
    ) -> SuiClientResult<()> {
        let args = vec![
            self.staking_arg(Mutability::Mutable).await?,
            self.argument_from_arg_or_obj(*storage_node_cap).await?,
            self.pt_builder.pure(network_public_key.as_bytes())?,
        ];
        self.walrus_move_call(contracts::staking::set_network_public_key, args)?;
        Ok(())
    }

    /// Updates the network address of the node.
    pub async fn update_network_address(
        &mut self,
        storage_node_cap: &ArgumentOrOwnedObject,
        network_address: NetworkAddress,
    ) -> SuiClientResult<()> {
        let args = vec![
            self.staking_arg(Mutability::Mutable).await?,
            self.argument_from_arg_or_obj(*storage_node_cap).await?,
            self.pt_builder.pure(network_address.to_string())?,
        ];
        self.walrus_move_call(contracts::staking::set_network_address, args)?;
        Ok(())
    }

    /// Updates the storage price of the node.
    pub async fn update_storage_price(
        &mut self,
        storage_node_cap: &ArgumentOrOwnedObject,
        storage_price: u64,
    ) -> SuiClientResult<()> {
        let args = vec![
            self.staking_arg(Mutability::Mutable).await?,
            self.argument_from_arg_or_obj(*storage_node_cap).await?,
            self.pt_builder.pure(storage_price)?,
        ];
        self.walrus_move_call(contracts::staking::set_storage_price_vote, args)?;
        Ok(())
    }

    /// Updates the write price of the node.
    pub async fn update_write_price(
        &mut self,
        storage_node_cap: &ArgumentOrOwnedObject,
        write_price: u64,
    ) -> SuiClientResult<()> {
        let args = vec![
            self.staking_arg(Mutability::Mutable).await?,
            self.argument_from_arg_or_obj(*storage_node_cap).await?,
            self.pt_builder.pure(write_price)?,
        ];
        self.walrus_move_call(contracts::staking::set_write_price_vote, args)?;
        Ok(())
    }

    /// Updates the node capacity of the node.
    pub async fn update_node_capacity(
        &mut self,
        storage_node_cap: &ArgumentOrOwnedObject,
        node_capacity: u64,
    ) -> SuiClientResult<()> {
        let args = vec![
            self.staking_arg(Mutability::Mutable).await?,
            self.argument_from_arg_or_obj(*storage_node_cap).await?,
            self.pt_builder.pure(node_capacity)?,
        ];
        self.walrus_move_call(contracts::staking::set_node_capacity_vote, args)?;
        Ok(())
    }

    /// Adds a call to `add_quilt_task` to the PTB.
    pub async fn add_quilt_task(
        &mut self,
        storage_node_cap: ArgumentOrOwnedObject,
        task_id: ObjectID,
    ) -> SuiClientResult<()> {
        let args = vec![
            self.system_arg(Mutability::Mutable).await?,
            self.argument_from_arg_or_obj(storage_node_cap).await?,
            self.pt_builder.pure(task_id)?,
        ];
        self.walrus_move_call(contracts::system::add_quilt_task, args)?;
        Ok(())
    }

    /// Adds a call to `update_quilt_task_state` to the PTB.
    pub async fn update_quilt_task_state(
        &mut self,
        storage_node_cap: ArgumentOrOwnedObject,
        task_id: ObjectID,
        new_state: u8,
    ) -> SuiClientResult<()> {
        let args = vec![
            self.system_arg(Mutability::Mutable).await?,
            self.argument_from_arg_or_obj(storage_node_cap).await?,
            self.pt_builder.pure(task_id)?,
            self.pt_builder.pure(new_state)?,
        ];
        self.walrus_move_call(contracts::system::update_quilt_task_state, args)?;
        Ok(())
    }

    /// Transfers all remaining outputs and returns the PTB and the SUI balance needed in addition
    /// to the gas cost that needs to be covered by the gas coin.
    pub async fn finish(mut self) -> SuiClientResult<(ProgrammableTransaction, u64)> {
        self.transfer_remaining_outputs(None).await?;
        let sui_cost = self.tx_sui_cost;
        Ok((self.pt_builder.finish(), sui_cost))
    }

    async fn storage_price_for_encoded_length(
        &self,
        encoded_size: u64,
        epochs_ahead: EpochCount,
    ) -> SuiClientResult<u64> {
        Ok(price_for_encoded_length(
            encoded_size,
            self.read_client.storage_price_per_unit_size().await?,
            epochs_ahead,
        ))
    }

    async fn write_price_for_encoded_length(&self, encoded_size: u64) -> SuiClientResult<u64> {
        Ok(write_price_for_encoded_length(
            encoded_size,
            self.read_client.write_price_per_unit_size().await?,
        ))
    }

    async fn argument_from_arg_or_obj(
        &mut self,
        arg_or_obj: ArgumentOrOwnedObject,
    ) -> SuiClientResult<Argument> {
        match arg_or_obj {
            ArgumentOrOwnedObject::Argument(arg) => Ok(arg),
            ArgumentOrOwnedObject::Object(obj) => Ok(self
                .pt_builder
                .obj(self.read_client.object_arg_for_object(obj).await?)?),
        }
    }

    async fn system_arg(&mut self, mutable: Mutability) -> SuiClientResult<Argument> {
        Ok(self
            .pt_builder
            .obj(self.read_client.object_arg_for_system_obj(mutable).await?)?)
    }

    async fn staking_arg(&mut self, mutable: Mutability) -> SuiClientResult<Argument> {
        Ok(self
            .pt_builder
            .obj(self.read_client.object_arg_for_staking_obj(mutable).await?)?)
    }

    fn wal_coin_arg(&mut self) -> SuiClientResult<Argument> {
        self.wal_coin_arg
            .ok_or_else(|| SuiClientError::NoCompatibleWalCoins)
    }

    fn mark_arg_as_consumed(&mut self, arg: &Argument) {
        self.args_to_consume.remove(arg);
    }

    fn add_result_to_be_consumed(&mut self, arg: Argument) {
        self.args_to_consume.insert(arg);
    }

    async fn system_object(&self) -> SuiClientResult<&SystemObject> {
        self.system_object
            .get_or_try_init(|| self.read_client.get_system_object())
            .await
    }
}
