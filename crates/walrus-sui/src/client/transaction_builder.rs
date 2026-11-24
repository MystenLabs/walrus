// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! A transaction builder for programmable transactions containing Walrus-related calls.

use std::{collections::HashSet, fmt::Debug, str::FromStr, sync::Arc};

use fastcrypto::traits::ToFromBytes;
use sui_move_build::CompiledPackage;
use sui_types::{
    Identifier,
    SUI_CLOCK_OBJECT_ID,
    SUI_CLOCK_OBJECT_SHARED_VERSION,
    SUI_FRAMEWORK_PACKAGE_ID,
    TypeTag,
    base_types::{ObjectID, ObjectRef, SuiAddress},
    gas_coin::GAS,
    programmable_transaction_builder::ProgrammableTransactionBuilder,
    transaction::{
        Argument,
        CallArg,
        Command,
        FundsWithdrawalArg,
        ObjectArg,
        ProgrammableTransaction,
        SharedObjectMutability,
        TransactionData,
        TransactionKind,
    },
};
use tracing::Level;
use walrus_core::{
    Epoch,
    EpochCount,
    NetworkPublicKey,
    ensure,
    messages::{ConfirmationCertificate, InvalidBlobCertificate, ProofOfPossession},
};

use super::{
    BlobObjectMetadata,
    BlobPersistence,
    CoinType,
    PoolOperationWithAuthorization,
    ReadClient,
    SuiClientError,
    SuiClientResult,
    SuiReadClient,
    UpgradeType,
};
use crate::{
    client::retry_client::{RetriableSuiClient, retriable_sui_client::GasBudgetAndPrice},
    coin::Coin,
    contracts::{self, FunctionTag},
    types::{
        NetworkAddress,
        NodeRegistrationParams,
        NodeUpdateParams,
        SystemObject,
        UpdatePublicKeyParams,
        move_structs::{Authorized, EmergencyUpgradeCap, NodeMetadata, WalExchange},
    },
    utils::{TEN_THOUSAND_BASIS_POINTS, price_for_encoded_length, write_price_for_encoded_length},
};

mod owned_blob_ops;
mod pooled_blob_ops;
mod shared_blob_ops;

const CLOCK_OBJECT_ARG: ObjectArg = ObjectArg::SharedObject {
    id: SUI_CLOCK_OBJECT_ID,
    initial_shared_version: SUI_CLOCK_OBJECT_SHARED_VERSION,
    mutability: SharedObjectMutability::Immutable,
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
    /// An [`ObjectArg`].
    ObjectArg(ObjectArg),
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

impl From<ObjectArg> for ArgumentOrOwnedObject {
    fn from(obj: ObjectArg) -> Self {
        Self::ObjectArg(obj)
    }
}

impl From<&ObjectArg> for ArgumentOrOwnedObject {
    fn from(obj: &ObjectArg) -> Self {
        Self::ObjectArg(*obj)
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
///
/// All SUI and WAL the PTB spends is withdrawn from the sender's address balances, using at most
/// one withdrawal per coin type, and gas is paid from the sender's SUI address balance. Remaining
/// WAL, and WAL received in the PTB, is returned to the sender's address balance.
pub struct WalrusPtbBuilder {
    pt_builder: ProgrammableTransactionBuilder,
    read_client: Arc<SuiReadClient>,
    tx_wal_balance: u64,
    wal_coin_arg: Option<Argument>,
    wal_withdrawal: Option<PendingWithdrawal>,
    sui_withdrawal: Option<PendingWithdrawal>,
    sender_address: SuiAddress,
    args_to_consume: HashSet<Argument>,
}

/// A withdrawal from the sender's address balance, redeemed as a coin at the point of creation.
///
/// The amount of the withdrawal input is only set when the PTB is finished, so that a PTB needs at
/// most one withdrawal per coin type, no matter how many payments it contains.
#[derive(Debug, Clone, Copy)]
struct PendingWithdrawal {
    /// The `FundsWithdrawal` input of the PTB.
    input: Argument,
    /// The coin redeemed from the withdrawal.
    coin: Argument,
    /// The total amount to withdraw.
    amount: u64,
}

/// The amounts that a finished PTB withdraws from the sender's address balances.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct AddressBalanceWithdrawals {
    /// The amount of MIST withdrawn, in addition to the gas budget.
    sui: u64,
    /// The amount of FROST withdrawn.
    wal: u64,
}

impl Debug for WalrusPtbBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WalrusPtbBuilder")
            .field("read_client", &self.read_client)
            .field("tx_wal_balance", &self.tx_wal_balance)
            .field("wal_coin_arg", &self.wal_coin_arg)
            .field("wal_withdrawal", &self.wal_withdrawal)
            .field("sui_withdrawal", &self.sui_withdrawal)
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
            wal_coin_arg: None,
            wal_withdrawal: None,
            sui_withdrawal: None,
            sender_address,
            args_to_consume: HashSet::new(),
        }
    }

    /// Fills up the WAL coin argument of the PTB to at least `min_balance`.
    ///
    /// The missing amount is withdrawn from the sender's WAL address balance. This is a no-op if
    /// the current available balance (that has already been added to the PTB and hasn't been
    /// consumed yet) is at least `min_balance`.
    pub fn fill_wal_balance(&mut self, min_balance: u64) -> SuiClientResult<()> {
        if min_balance <= self.tx_wal_balance && self.wal_coin_arg.is_some() {
            return Ok(());
        }
        let additional_balance = min_balance - self.tx_wal_balance;
        if let Some(withdrawal) = self.wal_withdrawal.as_mut() {
            withdrawal.amount += additional_balance;
        } else if additional_balance == 0 {
            let wal_type_tag = self.wal_type_tag()?;
            let zero_coin = self.move_call(
                SUI_FRAMEWORK_PACKAGE_ID,
                contracts::coin::zero.with_type_params(&[wal_type_tag]),
                vec![],
            )?;
            self.wal_coin_arg = Some(zero_coin);
        } else {
            let wal_type_tag = self.wal_type_tag()?;
            let withdrawal =
                self.withdraw_from_address_balance(wal_type_tag, additional_balance)?;
            self.wal_withdrawal = Some(withdrawal);
            match self.wal_coin_arg {
                Some(wal_coin_arg) => {
                    self.pt_builder
                        .command(Command::MergeCoins(wal_coin_arg, vec![withdrawal.coin]));
                }
                None => self.wal_coin_arg = Some(withdrawal.coin),
            }
        }
        self.tx_wal_balance += additional_balance;
        Ok(())
    }

    /// Returns a coin with `amount` MIST withdrawn from the sender's SUI address balance.
    fn sui_coin(&mut self, amount: u64) -> SuiClientResult<Argument> {
        if amount == 0 && self.sui_withdrawal.is_none() {
            // Withdrawals of zero are rejected by Sui.
            return self.move_call(
                SUI_FRAMEWORK_PACKAGE_ID,
                contracts::coin::zero.with_type_params(&[GAS::type_tag()]),
                vec![],
            );
        }
        let source_coin = if let Some(withdrawal) = self.sui_withdrawal.as_mut() {
            withdrawal.amount += amount;
            withdrawal.coin
        } else {
            let withdrawal = self.withdraw_from_address_balance(GAS::type_tag(), amount)?;
            self.sui_withdrawal = Some(withdrawal);
            withdrawal.coin
        };
        let amount_arg = self.pt_builder.pure(amount)?;
        Ok(self
            .pt_builder
            .command(Command::SplitCoins(source_coin, vec![amount_arg])))
    }

    /// Adds a withdrawal of `amount` of `coin_type` from the sender's address balance and redeems
    /// it as a coin. The amount can be increased until the PTB is finished.
    fn withdraw_from_address_balance(
        &mut self,
        coin_type: TypeTag,
        amount: u64,
    ) -> SuiClientResult<PendingWithdrawal> {
        let input = self
            .pt_builder
            .funds_withdrawal(FundsWithdrawalArg::balance_from_sender(
                amount,
                coin_type.clone(),
            ))?;
        let coin = self.move_call(
            SUI_FRAMEWORK_PACKAGE_ID,
            contracts::coin::redeem_funds.with_type_params(&[coin_type]),
            vec![input],
        )?;
        Ok(PendingWithdrawal {
            input,
            coin,
            amount,
        })
    }

    /// Sends `coin` to the address balance of `recipient`.
    fn send_funds(
        &mut self,
        coin_type: TypeTag,
        coin: Argument,
        recipient: SuiAddress,
    ) -> SuiClientResult<()> {
        let recipient_arg = self.pt_builder.pure(recipient)?;
        self.move_call(
            SUI_FRAMEWORK_PACKAGE_ID,
            contracts::coin::send_funds.with_type_params(&[coin_type]),
            vec![coin, recipient_arg],
        )?;
        self.mark_arg_as_consumed(&coin);
        Ok(())
    }

    fn wal_type_tag(&self) -> SuiClientResult<TypeTag> {
        Ok(TypeTag::from_str(self.read_client.wal_coin_type())?)
    }

    /// Returns the remaining WAL to the sender's address balance, closes the SUI withdrawal, and
    /// returns the finished PTB together with the amounts it withdraws from the address balances.
    async fn finish_with_withdrawals(
        mut self,
    ) -> SuiClientResult<(ProgrammableTransaction, AddressBalanceWithdrawals)> {
        if let Some(wal_coin_arg) = self.wal_coin_arg.take() {
            let wal_type_tag = self.wal_type_tag()?;
            self.send_funds(wal_type_tag, wal_coin_arg, self.sender_address)?;
            self.tx_wal_balance = 0;
        }
        if let Some(withdrawal) = self.sui_withdrawal {
            // Every withdrawn MIST has been split off, so the redeemed coin is empty.
            self.move_call(
                SUI_FRAMEWORK_PACKAGE_ID,
                contracts::coin::destroy_zero.with_type_params(&[GAS::type_tag()]),
                vec![withdrawal.coin],
            )?;
        }
        self.transfer_remaining_outputs(None).await?;

        let mut programmable_transaction = self.pt_builder.finish();
        let mut withdrawals = AddressBalanceWithdrawals::default();
        if let Some(withdrawal) = self.wal_withdrawal {
            set_withdrawal_amount(
                &mut programmable_transaction,
                withdrawal,
                TypeTag::from_str(self.read_client.wal_coin_type())?,
            )?;
            withdrawals.wal = withdrawal.amount;
        }
        if let Some(withdrawal) = self.sui_withdrawal {
            set_withdrawal_amount(&mut programmable_transaction, withdrawal, GAS::type_tag())?;
            withdrawals.sui = withdrawal.amount;
        }
        Ok((programmable_transaction, withdrawals))
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
        self.move_call(self.read_client.walrus_package_id(), function, arguments)
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
        tracing::trace!(
            package_id = package_id.to_canonical_string(true),
            ?function,
            ?arguments,
            "move call"
        );
        Ok(self.pt_builder.programmable_move_call(
            package_id,
            Identifier::from_str(function.module)?,
            Identifier::from_str(function.name)?,
            function.type_params,
            arguments,
        ))
    }

    /// Adds a transfer to the PTB. If the recipient is `None`, the sender address is used.
    #[tracing::instrument(level = Level::DEBUG, skip_all)]
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
    #[tracing::instrument(level = Level::DEBUG, skip_all)]
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

    /// Withdraws `amount` MIST from the sender's address balance, adds a call to
    /// `exchange_all_for_wal` to the PTB and merges the WAL coins into the payment coin of the PTB.
    pub async fn exchange_sui_for_wal(
        &mut self,
        exchange_id: ObjectID,
        amount: u64,
    ) -> SuiClientResult<()> {
        let exchange: WalExchange = self
            .read_client
            .retriable_sui_client()
            .get_sui_object(exchange_id)
            .await?;
        // We can get the package ID from the exchange object because we only use it in testnet
        // and the exchange is currently not designed for upgrades.
        let exchange_package = self
            .read_client
            .retriable_sui_client()
            .get_package_id_from_object(exchange_id)
            .await?;
        let exchange_arg = self.pt_builder.obj(
            self.read_client
                .object_arg_for_shared_obj(exchange_id, SharedObjectMutability::Mutable)
                .await?,
        )?;
        let sui_coin = self.sui_coin(amount)?;
        let result_arg = self.move_call(
            exchange_package,
            contracts::wal_exchange::exchange_all_for_wal,
            vec![exchange_arg, sui_coin],
        )?;
        let wal_amount = exchange.exchange_rate.sui_to_wal(amount);
        self.add_wal_coin_result_arg(result_arg, wal_amount);
        Ok(())
    }

    /// Adds a call to create a new exchange, funded with `amount` WAL, to the PTB.
    pub fn create_and_fund_exchange(
        &mut self,
        exchange_package: ObjectID,
        amount: u64,
    ) -> SuiClientResult<Argument> {
        self.fill_wal_balance(amount)?;
        let args = vec![self.wal_coin_arg()?, self.pt_builder.pure(amount)?];
        let result_arg =
            self.move_call(exchange_package, contracts::wal_exchange::new_funded, args)?;
        self.reduce_wal_balance(amount)?;
        self.add_result_to_be_consumed(result_arg);
        Ok(result_arg)
    }

    /// Adds a call to create a new credits object (`subsidies::Subsidies` in Move), funded with
    /// `amount` WAL, to the PTB.
    pub fn create_and_fund_credits(
        &mut self,
        package_id: ObjectID,
        initial_buyer_subsidy_rate: u16,
        initial_system_subsidy_rate: u16,
        amount: u64,
    ) -> SuiClientResult<Argument> {
        self.fill_wal_balance(amount)?;
        let split_main_coin_arg = self.wal_coin_arg()?;
        let split_amount_arg = self.pt_builder.pure(amount)?;
        let split_coin = self.pt_builder.command(Command::SplitCoins(
            split_main_coin_arg,
            vec![split_amount_arg],
        ));
        let args = vec![
            self.pt_builder.pure(package_id)?,
            self.pt_builder.pure(initial_buyer_subsidy_rate)?,
            self.pt_builder.pure(initial_system_subsidy_rate)?,
            split_coin,
        ];
        let result_arg = self.move_call(
            package_id,
            contracts::credits::new_with_initial_rates_and_funds,
            args,
        )?;
        self.reduce_wal_balance(amount)?;
        self.add_result_to_be_consumed(result_arg);
        Ok(result_arg)
    }

    /// Adds a call to create a new walrus subsidies object
    /// ([`contracts::walrus_subsidies::WalrusSubsidies`]) to the PTB.
    pub fn create_walrus_subsidies(
        &mut self,
        package_id: ObjectID,
        system_subsidy_rate: u32,
        base_subsidy: u64,
        subsidy_per_shard: u64,
    ) -> SuiClientResult<Argument> {
        let args = vec![
            self.system_arg(SharedObjectMutability::Immutable)?,
            self.staking_arg(SharedObjectMutability::Immutable)?,
            self.pt_builder.pure(system_subsidy_rate)?,
            self.pt_builder.pure(base_subsidy)?,
            self.pt_builder.pure(subsidy_per_shard)?,
        ];
        let result_arg = self.move_call(package_id, contracts::walrus_subsidies::new, args)?;
        self.add_result_to_be_consumed(result_arg);
        Ok(result_arg)
    }

    /// Adds a call to fund the walrus subsidies object
    /// ([`contracts::walrus_subsidies::WalrusSubsidies`]) to the PTB, if a walrus subsidies object
    /// is configured.
    pub fn fund_walrus_subsidies(&mut self, amount: u64) -> SuiClientResult<()> {
        let Some(walrus_subsidies_pkg_id) = self.read_client.walrus_subsidies_package_id() else {
            return Err(SuiClientError::WalrusSubsidiesNotConfigured);
        };
        self.fill_wal_balance(amount)?;
        let split_main_coin_arg = self.wal_coin_arg()?;
        let split_amount_arg = self.pt_builder.pure(amount)?;
        let split_coin = self.pt_builder.command(Command::SplitCoins(
            split_main_coin_arg,
            vec![split_amount_arg],
        ));
        let args = vec![
            self.pt_builder.obj(
                self.read_client
                    .object_arg_for_walrus_subsidies_obj(SharedObjectMutability::Mutable)?,
            )?,
            split_coin,
        ];
        self.move_call(
            walrus_subsidies_pkg_id,
            contracts::walrus_subsidies::add_coin,
            args,
        )?;
        self.reduce_wal_balance(amount)?;
        Ok(())
    }

    /// Adds a call to `invalidate_blob_id` to the PTB.
    pub async fn invalidate_blob_id(
        &mut self,
        certificate: &InvalidBlobCertificate,
    ) -> SuiClientResult<()> {
        let signers = self.signers_to_bitmap(&certificate.signers).await?;

        let invalidate_args = vec![
            self.system_arg(SharedObjectMutability::Immutable)?,
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
            self.staking_arg(SharedObjectMutability::Mutable)?,
            self.argument_from_arg_or_obj(storage_node_cap).await?,
            self.pt_builder.pure(epoch)?,
            self.pt_builder.obj(CLOCK_OBJECT_ARG)?,
        ];
        self.walrus_move_call(contracts::staking::epoch_sync_done, args)?;
        Ok(())
    }

    /// Adds a call to initiate epoch change to the PTB.
    pub fn initiate_epoch_change(&mut self) -> SuiClientResult<()> {
        let args = vec![
            self.staking_arg(SharedObjectMutability::Mutable)?,
            self.system_arg(SharedObjectMutability::Mutable)?,
            self.pt_builder.obj(CLOCK_OBJECT_ARG)?,
        ];
        self.walrus_move_call(contracts::staking::initiate_epoch_change, args)?;
        Ok(())
    }

    /// Adds a call to `initiate_epoch_change_v2` to the PTB.
    pub fn initiate_epoch_change_v2(&mut self) -> SuiClientResult<()> {
        let args = vec![
            self.staking_arg(SharedObjectMutability::Mutable)?,
            self.system_arg(SharedObjectMutability::Mutable)?,
            self.treasury_arg(SharedObjectMutability::Mutable)?,
            self.pt_builder.obj(CLOCK_OBJECT_ARG)?,
        ];
        self.walrus_move_call(contracts::staking::initiate_epoch_change_v2, args)?;
        Ok(())
    }

    /// Adds a call to `voting_end` to the PTB.
    pub fn voting_end(&mut self) -> SuiClientResult<()> {
        let args = vec![
            self.staking_arg(SharedObjectMutability::Mutable)?,
            self.pt_builder.obj(CLOCK_OBJECT_ARG)?,
        ];
        self.walrus_move_call(contracts::staking::voting_end, args)?;
        Ok(())
    }

    /// Adds a call to `walrus_subsidies::process_subsidies` to the PTB.
    pub fn process_subsidies(&mut self) -> SuiClientResult<()> {
        let args = vec![
            self.walrus_subsidies_arg(SharedObjectMutability::Mutable)?,
            self.staking_arg(SharedObjectMutability::Mutable)?,
            self.system_arg(SharedObjectMutability::Mutable)?,
            self.pt_builder.obj(CLOCK_OBJECT_ARG)?,
        ];
        let Some(walrus_subsidies_package_id) = self.read_client.walrus_subsidies_package_id()
        else {
            return Err(SuiClientError::CreditsNotEnabled);
        };
        self.move_call(
            walrus_subsidies_package_id,
            contracts::walrus_subsidies::process_subsidies,
            args,
        )?;
        Ok(())
    }

    /// Adds a call to `request_withdraw_stake` to the PTB.
    pub async fn request_withdraw_stake(&mut self, staked_wal_id: ObjectID) -> SuiClientResult<()> {
        let request_withdraw_stake_args = vec![
            self.staking_arg(SharedObjectMutability::Mutable)?,
            self.pt_builder.obj(
                self.read_client
                    .object_arg_for_object(staked_wal_id)
                    .await?,
            )?,
        ];
        self.walrus_move_call(
            contracts::staking::request_withdraw_stake,
            request_withdraw_stake_args,
        )?;
        Ok(())
    }

    /// Adds a call to `withdraw_stake` to the PTB.
    pub async fn withdraw_stake(&mut self, staked_wal_id: ObjectID) -> SuiClientResult<()> {
        let withdraw_stake_args = vec![
            self.staking_arg(SharedObjectMutability::Mutable)?,
            self.argument_from_arg_or_obj(ArgumentOrOwnedObject::from(staked_wal_id))
                .await?,
        ];
        let wal = self.walrus_move_call(contracts::staking::withdraw_stake, withdraw_stake_args)?;
        // The withdrawn amount is not known in advance, so it does not count as available.
        self.add_wal_coin_result_arg(wal, 0);
        Ok(())
    }

    /// Adds a call to `stake_with_pool` to the PTB.
    pub fn stake_with_pool(&mut self, amount: u64, node_id: ObjectID) -> SuiClientResult<Argument> {
        self.fill_wal_balance(amount)?;

        // Split the amount to stake from the main WAL coin.
        let split_main_coin_arg = self.wal_coin_arg()?;
        let split_amount_arg = self.pt_builder.pure(amount)?;
        let split_coin = self.pt_builder.command(Command::SplitCoins(
            split_main_coin_arg,
            vec![split_amount_arg],
        ));

        // Stake the split coin.
        let staking_args = vec![
            self.staking_arg(SharedObjectMutability::Mutable)?,
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
    pub fn register_candidate(
        &mut self,
        node_parameters: &NodeRegistrationParams,
        proof_of_possession: ProofOfPossession,
    ) -> SuiClientResult<Argument> {
        let node_metadata_arg = self.create_node_metadata(&node_parameters.metadata)?;
        let args = vec![
            self.staking_arg(SharedObjectMutability::Mutable)?,
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
    pub fn create_node_metadata(
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
        storage_node_cap: &ArgumentOrOwnedObject,
        node_metadata: &NodeMetadata,
    ) -> SuiClientResult<()> {
        let metadata_arg = self.create_node_metadata(node_metadata)?;
        let args = vec![
            self.staking_arg(SharedObjectMutability::Mutable)?,
            self.argument_from_arg_or_obj(*storage_node_cap).await?,
            metadata_arg,
        ];
        self.walrus_move_call(contracts::staking::set_node_metadata, args)?;
        Ok(())
    }

    /// Sends `amount` WAL from the sender's address balance to the address balance of
    /// `recipient`.
    pub fn pay_wal(&mut self, recipient: SuiAddress, amount: u64) -> SuiClientResult<()> {
        self.fill_wal_balance(amount)?;
        let amount_arg = self.pt_builder.pure(amount)?;
        let wal_coin_arg = self.wal_coin_arg()?;
        let split_coin = self
            .pt_builder
            .command(Command::SplitCoins(wal_coin_arg, vec![amount_arg]));
        let wal_type_tag = self.wal_type_tag()?;
        self.send_funds(wal_type_tag, split_coin, recipient)?;
        self.reduce_wal_balance(amount)?;
        Ok(())
    }

    /// Sends `amount` MIST from the sender's address balance to the address balance of
    /// `recipient`.
    pub fn pay_sui(&mut self, recipient: SuiAddress, amount: u64) -> SuiClientResult<()> {
        let sui_coin = self.sui_coin(amount)?;
        self.send_funds(GAS::type_tag(), sui_coin, recipient)
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
        object_id: ObjectID,
    ) -> SuiClientResult<Argument> {
        let (object_ref, type_tag): (ObjectRef, TypeTag) = self
            .read_client
            .retriable_sui_client()
            .get_object_ref_and_type_tag(object_id)
            .await?;
        let object_arg = self
            .pt_builder
            .obj(ObjectArg::ImmOrOwnedObject(object_ref))?;
        let result_arg = self.walrus_move_call(
            contracts::auth::authenticate_with_object.with_type_params(&[type_tag]),
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

    #[tracing::instrument(err, skip(self))]
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

    /// Sets the commission receiver for the node.
    pub async fn set_commission_receiver(
        &mut self,
        node_id: ObjectID,
        receiver: Argument,
    ) -> SuiClientResult<()> {
        let authenticated = self
            .get_authenticated_arg_for_pool(node_id, PoolOperationWithAuthorization::Commission)
            .await?;
        let args = vec![
            self.staking_arg(SharedObjectMutability::Mutable)?,
            self.pt_builder.pure(node_id)?,
            authenticated,
            receiver,
        ];
        self.walrus_move_call(contracts::staking::set_commission_receiver, args)?;
        Ok(())
    }

    /// Sets the commission rate for the node.
    pub async fn set_commission_rate(
        &mut self,
        storage_node_cap: &ArgumentOrOwnedObject,
        rate: u16,
    ) -> SuiClientResult<()> {
        let args = vec![
            self.staking_arg(SharedObjectMutability::Mutable)?,
            self.argument_from_arg_or_obj(*storage_node_cap).await?,
            self.pt_builder.pure(rate)?,
        ];
        self.walrus_move_call(contracts::staking::set_next_commission, args)?;
        Ok(())
    }

    /// Sets the governance authorized object for the pool.
    pub async fn set_governance_authorized(
        &mut self,
        node_id: ObjectID,
        authorized: Argument,
    ) -> SuiClientResult<()> {
        let authenticated = self
            .get_authenticated_arg_for_pool(node_id, PoolOperationWithAuthorization::Governance)
            .await?;
        let args = vec![
            self.staking_arg(SharedObjectMutability::Mutable)?,
            self.pt_builder.pure(node_id)?,
            authenticated,
            authorized,
        ];
        self.walrus_move_call(contracts::staking::set_governance_authorized, args)?;
        Ok(())
    }

    /// Collects the commission for the node.
    pub async fn collect_commission(&mut self, node_id: ObjectID) -> SuiClientResult<()> {
        let authenticated = self
            .get_authenticated_arg_for_pool(node_id, PoolOperationWithAuthorization::Commission)
            .await?;
        let args = vec![
            self.staking_arg(SharedObjectMutability::Mutable)?,
            self.pt_builder.pure(node_id)?,
            authenticated,
        ];
        let result_arg = self.walrus_move_call(contracts::staking::collect_commission, args)?;
        // We can get the value of the commission up to now. If there is an epoch change before the
        // transaction is executed, this may increase, but not decrease.
        let commission_value = self.read_client.get_staking_pool(node_id).await?.commission;
        self.add_wal_coin_result_arg(result_arg, commission_value);
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

        let mut price_votes_updated = false;
        if let Some(storage_price) = params.storage_price {
            self.update_storage_price(&storage_node_cap, storage_price)
                .await?;
            price_votes_updated = true;
        }

        if let Some(write_price) = params.write_price {
            self.update_write_price(&storage_node_cap, write_price)
                .await?;
            price_votes_updated = true;
        }

        if price_votes_updated && params.update_price_immediately {
            // We are using live stable pricing, so we need to call `update_prices` to apply the
            // new prices.
            // TODO(WAL-804): create a test to test updating prices immediately.
            self.apply_system_prices()?;
        }

        if let Some(node_capacity) = params.node_capacity {
            self.update_node_capacity(&storage_node_cap, node_capacity)
                .await?;
        }

        if let Some(metadata) = params.metadata {
            self.set_node_metadata(&storage_node_cap, &metadata).await?;
        }

        if let Some(commission_rate) = params.commission_rate {
            self.set_commission_rate(&storage_node_cap, commission_rate)
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
            self.staking_arg(SharedObjectMutability::Mutable)?,
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
            self.staking_arg(SharedObjectMutability::Mutable)?,
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
            self.staking_arg(SharedObjectMutability::Mutable)?,
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
            self.staking_arg(SharedObjectMutability::Mutable)?,
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
            self.staking_arg(SharedObjectMutability::Mutable)?,
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
            self.staking_arg(SharedObjectMutability::Mutable)?,
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
            self.staking_arg(SharedObjectMutability::Mutable)?,
            self.argument_from_arg_or_obj(*storage_node_cap).await?,
            self.pt_builder.pure(node_capacity)?,
        ];
        self.walrus_move_call(contracts::staking::set_node_capacity_vote, args)?;
        Ok(())
    }

    /// Recalculates and updates the system storage and write prices based on the current
    /// committee's price votes.
    pub fn apply_system_prices(&mut self) -> SuiClientResult<()> {
        let args = vec![
            self.staking_arg(SharedObjectMutability::Mutable)?,
            self.system_arg(SharedObjectMutability::Mutable)?,
        ];
        self.walrus_move_call(contracts::staking::update_prices, args)?;
        Ok(())
    }

    /// Votes for an upgrade of the system contract from the Node with `node_id`.
    pub async fn vote_for_upgrade(
        &mut self,
        upgrade_manager: ObjectID,
        node_id: ObjectID,
        digest: &[u8],
    ) -> SuiClientResult<()> {
        let args = vec![
            self.pt_builder.obj(
                self.read_client
                    .object_arg_for_shared_obj(upgrade_manager, SharedObjectMutability::Mutable)
                    .await?,
            )?,
            self.staking_arg(SharedObjectMutability::Mutable)?,
            self.get_authenticated_arg_for_pool(
                node_id,
                PoolOperationWithAuthorization::Governance,
            )
            .await?,
            self.pt_builder.pure(node_id)?,
            self.pt_builder.pure(digest)?,
        ];
        self.walrus_move_call(contracts::upgrade::vote_for_upgrade, args)?;
        Ok(())
    }

    /// Authorizes an upgrade that has reached a quorum of the votes from the storage nodes.
    ///
    /// Returns the `UpgradeTicket` as result argument.
    pub async fn authorize_upgrade(
        &mut self,
        upgrade_manager: ObjectID,
        digest: &[u8],
    ) -> SuiClientResult<Argument> {
        let args = vec![
            self.pt_builder.obj(
                self.read_client
                    .object_arg_for_shared_obj(upgrade_manager, SharedObjectMutability::Mutable)
                    .await?,
            )?,
            self.staking_arg(SharedObjectMutability::Immutable)?,
            self.pt_builder.pure(digest)?,
        ];
        self.walrus_move_call(contracts::upgrade::authorize_upgrade, args)
    }

    /// Authorizes an emergency upgrade using the emergency upgrade cap.
    pub async fn authorize_emergency_upgrade(
        &mut self,
        upgrade_manager: ObjectID,
        emergency_upgrade_cap: ArgumentOrOwnedObject,
        digest: &[u8],
    ) -> SuiClientResult<Argument> {
        let args = vec![
            self.pt_builder.obj(
                self.read_client
                    .object_arg_for_shared_obj(upgrade_manager, SharedObjectMutability::Mutable)
                    .await?,
            )?,
            self.argument_from_arg_or_obj(emergency_upgrade_cap).await?,
            self.pt_builder.pure(digest)?,
        ];
        self.walrus_move_call(contracts::upgrade::authorize_emergency_upgrade, args)
    }

    /// Commits a contract upgrade.
    pub(crate) async fn commit_upgrade(
        &mut self,
        upgrade_manager: ObjectID,
        upgrade_receipt: Argument,
    ) -> SuiClientResult<()> {
        let args = vec![
            self.pt_builder.obj(
                self.read_client
                    .object_arg_for_shared_obj(upgrade_manager, SharedObjectMutability::Mutable)
                    .await?,
            )?,
            self.staking_arg(SharedObjectMutability::Mutable)?,
            self.system_arg(SharedObjectMutability::Mutable)?,
            upgrade_receipt,
        ];
        self.walrus_move_call(contracts::upgrade::commit_upgrade, args)?;
        Ok(())
    }

    /// Builds a transaction to upgrade the walrus contract using the custom upgrade policy.
    ///
    /// This includes the authorization, the upgrade itself, and committing the upgrade.
    pub async fn custom_walrus_upgrade(
        &mut self,
        upgrade_manager: ObjectID,
        compiled_package: CompiledPackage,
        upgrade_type: UpgradeType,
    ) -> SuiClientResult<()> {
        let digest = compiled_package.get_package_digest(false);

        let upgrade_ticket_arg = if upgrade_type.is_emergency_upgrade() {
            let emergency_upgrade_cap: EmergencyUpgradeCap = self
                .read_client
                .get_owned_objects(self.sender_address, &[])
                .await?
                .next()
                .ok_or_else(|| anyhow::anyhow!("no emergency upgrade capability found"))?;

            // Authorize the upgrade.
            self.authorize_emergency_upgrade(
                upgrade_manager,
                emergency_upgrade_cap.id.into(),
                &digest,
            )
            .await?
        } else {
            self.authorize_upgrade(upgrade_manager, &digest).await?
        };

        // Execute the upgrade.
        let modules = compiled_package.get_package_bytes(false);
        let upgrade_receipt_arg = self.pt_builder.upgrade(
            self.read_client.walrus_package_id(),
            upgrade_ticket_arg,
            compiled_package
                .dependency_ids
                .published
                .into_values()
                .map(|dep| dep.published_at)
                .collect(),
            modules,
        );

        // Commit the upgrade
        self.commit_upgrade(upgrade_manager, upgrade_receipt_arg)
            .await
    }

    /// Sets the migration epoch on the staking object to the following epoch.
    pub fn set_migration_epoch(&mut self, new_package_id: ObjectID) -> SuiClientResult<()> {
        let args = vec![self.staking_arg(SharedObjectMutability::Mutable)?];
        self.move_call(
            new_package_id,
            contracts::staking::set_migration_epoch,
            args,
        )?;
        Ok(())
    }

    /// Migrates the staking and system contracts to the new package ID.
    pub fn migrate_contracts(&mut self, new_package_id: ObjectID) -> SuiClientResult<()> {
        let args = vec![
            self.staking_arg(SharedObjectMutability::Mutable)?,
            self.system_arg(SharedObjectMutability::Mutable)?,
        ];
        self.move_call(new_package_id, contracts::init::migrate_v2, args)?;
        Ok(())
    }

    /// Finishes the PTB and returns the [`TransactionData`] containing the unsigned transaction,
    /// which pays for gas from the sender's SUI address balance.
    ///
    /// Remaining WAL is returned to the sender's address balance and all other remaining outputs
    /// are transferred to the sender. If no `gas_budget` is provided, the budget will be
    /// estimated.
    #[tracing::instrument(skip_all, level = Level::DEBUG)]
    pub async fn build_transaction_data(
        self,
        gas_budget: Option<u64>,
    ) -> SuiClientResult<TransactionData> {
        let read_client = self.read_client.clone();
        let sender_address = self.sender_address;
        let (programmable_transaction, withdrawals) = self.finish_with_withdrawals().await?;
        if withdrawals.wal > 0 {
            let wal_balance = read_client
                .address_balance(sender_address, CoinType::Wal)
                .await?;
            ensure!(
                wal_balance >= withdrawals.wal,
                SuiClientError::NoCompatibleWalCoins
            );
        }
        build_transaction_data_with_address_balance_gas(
            programmable_transaction,
            read_client.retriable_sui_client(),
            sender_address,
            gas_budget,
            withdrawals.sui,
        )
        .await
    }

    /// Given the node ID, checks if the sender is authorized to perform the operation (either as
    /// sender or by owning the corresponding object) and returns an `Authenticated` Move type as
    /// result argument.
    async fn get_authenticated_arg_for_pool(
        &mut self,
        node_id: ObjectID,
        operation: PoolOperationWithAuthorization,
    ) -> SuiClientResult<Argument> {
        let pool = self.read_client.get_staking_pool(node_id).await?;
        let authorized = match operation {
            PoolOperationWithAuthorization::Commission => pool.commission_receiver,
            PoolOperationWithAuthorization::Governance => pool.governance_authorized,
        };
        match authorized {
            Authorized::Address(receiver) => {
                ensure!(
                    receiver == self.sender_address,
                    SuiClientError::NotAuthorizedForPool(node_id)
                );
                self.authenticate_sender()
            }
            Authorized::Object(receiver) => {
                let owner_address = self
                    .read_client
                    .retriable_sui_client()
                    .get_object_owner_address(receiver)
                    .await?;
                ensure!(
                    owner_address == Some(self.sender_address),
                    SuiClientError::NotAuthorizedForPool(node_id)
                );
                self.authenticate_with_object(receiver).await
            }
        }
    }

    async fn storage_price_for_encoded_length(
        &self,
        encoded_size: u64,
        epochs_ahead: EpochCount,
        with_credits: bool,
    ) -> SuiClientResult<u64> {
        let full_price = price_for_encoded_length(
            encoded_size,
            self.read_client.storage_price_per_unit_size().await?,
            epochs_ahead,
        );
        let buyer_pays = match self.read_client.credits_object_id() {
            Some(credits_object_id) if with_credits => {
                let credits_object = self
                    .read_client
                    .retriable_sui_client()
                    .get_credits_object(credits_object_id)
                    .await?;
                let subsidy = full_price * u64::from(credits_object.buyer_subsidy_rate)
                    / TEN_THOUSAND_BASIS_POINTS;
                full_price - subsidy
            }
            Some(_) => full_price,
            None => full_price,
        };

        Ok(buyer_pays)
    }

    async fn write_price_for_encoded_length(
        &self,
        encoded_size: u64,
        with_credits: bool,
    ) -> SuiClientResult<u64> {
        let full_price = write_price_for_encoded_length(
            encoded_size,
            self.read_client.write_price_per_unit_size().await?,
        );
        let buyer_pays = match self.read_client.credits_object_id() {
            Some(credits_object_id) if with_credits => {
                let buyer_subsidy_rate = self
                    .read_client
                    .retriable_sui_client()
                    .get_credits_object(credits_object_id)
                    .await?
                    .buyer_subsidy_rate;
                let subsidy =
                    full_price * u64::from(buyer_subsidy_rate) / TEN_THOUSAND_BASIS_POINTS;
                full_price - subsidy
            }
            Some(_) => full_price,
            None => full_price,
        };

        Ok(buyer_pays)
    }

    #[tracing::instrument(skip_all, level = Level::DEBUG)]
    async fn argument_from_arg_or_obj(
        &mut self,
        arg_or_obj: ArgumentOrOwnedObject,
    ) -> SuiClientResult<Argument> {
        match arg_or_obj {
            ArgumentOrOwnedObject::Argument(arg) => Ok(arg),
            ArgumentOrOwnedObject::ObjectArg(obj_arg) => Ok(self.pt_builder.obj(obj_arg)?),
            ArgumentOrOwnedObject::Object(obj) => Ok(self
                .pt_builder
                .obj(self.read_client.object_arg_for_object(obj).await?)?),
        }
    }

    fn system_arg(&mut self, mutable: SharedObjectMutability) -> SuiClientResult<Argument> {
        Ok(self
            .pt_builder
            .obj(self.read_client.object_arg_for_system_obj(mutable))?)
    }

    fn staking_arg(&mut self, mutable: SharedObjectMutability) -> SuiClientResult<Argument> {
        Ok(self
            .pt_builder
            .obj(self.read_client.object_arg_for_staking_obj(mutable))?)
    }

    fn credits_arg(&mut self, mutable: SharedObjectMutability) -> SuiClientResult<Argument> {
        Ok(self
            .pt_builder
            .obj(self.read_client.object_arg_for_credits_obj(mutable)?)?)
    }

    fn treasury_arg(&mut self, mutable: SharedObjectMutability) -> SuiClientResult<Argument> {
        Ok(self
            .pt_builder
            .obj(self.read_client.object_arg_for_treasury_obj(mutable)?)?)
    }

    fn walrus_subsidies_arg(
        &mut self,
        mutable: SharedObjectMutability,
    ) -> SuiClientResult<Argument> {
        Ok(self.pt_builder.obj(
            self.read_client
                .object_arg_for_walrus_subsidies_obj(mutable)?,
        )?)
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

    async fn system_object(&self) -> SuiClientResult<SystemObject> {
        self.read_client.get_system_object().await
    }

    /// Adds a wal coin resulting from a command to the main WAL coin argument.
    fn add_wal_coin_result_arg(&mut self, arg: Argument, value: u64) {
        self.tx_wal_balance += value;
        match self.wal_coin_arg {
            Some(wal_coin_arg) => {
                self.pt_builder
                    .command(Command::MergeCoins(wal_coin_arg, vec![arg]));
            }
            // This coin is returned to the sender's address balance when the PTB is finished.
            None => self.wal_coin_arg = Some(arg),
        }
    }
}

/// Returns the [`TransactionData`] for `programmable_transaction`, paying for gas from the SUI
/// address balance of `sender_address`.
///
/// `sui_withdrawn` is the amount of MIST the PTB itself withdraws from the sender's address
/// balance; the address balance must cover it in addition to the gas budget. If no `gas_budget` is
/// provided, the budget will be estimated.
///
/// # Errors
///
/// Returns a [`SuiClientError::NoCompatibleGasCoins`] error if the sender's SUI address balance
/// does not cover the gas budget and `sui_withdrawn`.
pub async fn build_transaction_data_with_address_balance_gas(
    programmable_transaction: ProgrammableTransaction,
    sui_client: &RetriableSuiClient,
    sender_address: SuiAddress,
    gas_budget: Option<u64>,
    sui_withdrawn: u64,
) -> SuiClientResult<TransactionData> {
    let sui_balance = sui_client
        .get_address_balance(sender_address, Coin::SUI)
        .await?;
    // Check the PTB's own withdrawal before the gas estimation, which fails if it is not covered.
    ensure!(
        sui_balance >= sui_withdrawn,
        SuiClientError::NoCompatibleGasCoins(Some(sui_withdrawn.into()))
    );
    let GasBudgetAndPrice {
        gas_budget,
        gas_price,
    } = sui_client
        .gas_budget_and_price(
            gas_budget,
            sender_address,
            TransactionKind::ProgrammableTransaction(programmable_transaction.clone()),
        )
        .await?;
    let required_balance = u128::from(gas_budget) + u128::from(sui_withdrawn);
    ensure!(
        u128::from(sui_balance) >= required_balance,
        SuiClientError::NoCompatibleGasCoins(Some(required_balance))
    );
    let (chain_identifier, epoch) = sui_client.get_chain_identifier_and_epoch().await?;
    // Without gas payment objects, the transaction may not contain any owned object that protects
    // it against replays. It is therefore only valid in the current and the following epoch on
    // this chain, and the nonce distinguishes otherwise identical transactions.
    Ok(TransactionData::new_programmable_with_address_balance_gas(
        sender_address,
        programmable_transaction,
        gas_budget,
        gas_price,
        chain_identifier,
        epoch,
        rand::random(),
    ))
}

/// Sets the amount of the `FundsWithdrawal` input of `withdrawal` in `programmable_transaction`.
fn set_withdrawal_amount(
    programmable_transaction: &mut ProgrammableTransaction,
    withdrawal: PendingWithdrawal,
    coin_type: TypeTag,
) -> SuiClientResult<()> {
    let Argument::Input(index) = withdrawal.input else {
        return Err(SuiClientError::Internal(anyhow::anyhow!(
            "a withdrawal must be a PTB input"
        )));
    };
    let input = programmable_transaction
        .inputs
        .get_mut(usize::from(index))
        .ok_or_else(|| SuiClientError::Internal(anyhow::anyhow!("missing withdrawal input")))?;
    ensure!(
        matches!(input, CallArg::FundsWithdrawal(_)),
        SuiClientError::Internal(anyhow::anyhow!("the input is not a withdrawal"))
    );
    *input = CallArg::FundsWithdrawal(FundsWithdrawalArg::balance_from_sender(
        withdrawal.amount,
        coin_type,
    ));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_withdrawal_amount_replaces_the_withdrawal_input() -> SuiClientResult<()> {
        let mut pt_builder = ProgrammableTransactionBuilder::new();
        let pure = pt_builder.pure(1u64)?;
        let input = pt_builder
            .funds_withdrawal(FundsWithdrawalArg::balance_from_sender(1, GAS::type_tag()))?;
        let mut programmable_transaction = pt_builder.finish();

        let withdrawal = PendingWithdrawal {
            input,
            coin: Argument::Result(0),
            amount: 1_234,
        };
        set_withdrawal_amount(&mut programmable_transaction, withdrawal, GAS::type_tag())?;
        assert_eq!(
            programmable_transaction.inputs[1],
            CallArg::FundsWithdrawal(FundsWithdrawalArg::balance_from_sender(
                1_234,
                GAS::type_tag()
            ))
        );

        let not_a_withdrawal = PendingWithdrawal {
            input: pure,
            ..withdrawal
        };
        assert!(
            set_withdrawal_amount(
                &mut programmable_transaction,
                not_a_withdrawal,
                GAS::type_tag()
            )
            .is_err()
        );
        Ok(())
    }
}
