// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Helper functions for the crate.

use std::{
    cmp::Reverse,
    collections::HashSet,
    num::NonZeroU16,
    path::Path,
    str::FromStr,
    time::Duration,
};

use anyhow::{Context as _, Result, anyhow};
use move_core_types::language_storage::StructTag;
use serde::{Deserialize, Serialize};
use sui_config::{Config, SUI_KEYSTORE_FILENAME, sui_config_dir};
use sui_keys::keystore::{
    AccountKeystore as _,
    FileBasedKeystore,
    GenerateOptions,
    GeneratedKey,
    Keystore,
};
use sui_sdk::{
    sui_client_config::{SuiClientConfig, SuiEnv},
    types::base_types::ObjectID,
};
use sui_types::{
    Identifier,
    SUI_FRAMEWORK_PACKAGE_ID,
    TypeTag,
    base_types::{ObjectRef, SuiAddress},
    digests::TransactionDigest,
    gas_coin::GAS,
    programmable_transaction_builder::ProgrammableTransactionBuilder,
    transaction::{
        Argument,
        Command,
        FundsWithdrawalArg,
        ObjectArg,
        ProgrammableTransaction,
        TransactionData,
        TransactionKind,
    },
};
use walrus_core::{
    EncodingType,
    Epoch,
    EpochCount,
    encoding::encoded_blob_length_for_n_shards,
    ensure,
    keys::ProtocolKeyPair,
    messages::{ProofOfPossessionMsg, SignedMessage},
};

use crate::{
    client::{
        SuiClientError,
        SuiClientResult,
        SuiContractClient,
        dual_client::DEFAULT_CHECKPOINT_WAIT_TIMEOUT,
        retry_client::{
            RetriableSuiClient,
            retriable_sui_client::{GasBudgetAndPrice, MAX_GAS_PAYMENT_OBJECTS},
        },
        transaction_builder::build_transaction_data_with_address_balance_gas,
    },
    coin::Coin,
    config::load_wallet_context_from_path,
    contracts::{self, AssociatedContractStruct, FunctionTag, MoveConversionError},
    types::{ExecuteTransactionResponse, ObjectChangeEntry, TransactionEffectsStatus},
    wallet::Wallet,
};

/// 10,000 basis points is 100%.
pub const TEN_THOUSAND_BASIS_POINTS: u64 = 10_000;

// Keep in sync with the same constant in `contracts/walrus/sources/system/system_state_inner.move`.
// The storage unit is used in doc comments for CLI arguments in the files
// `crates/walrus-service/bin/deploy.rs` and `crates/walrus-service/bin/node.rs`.
// Change the unit there if it changes.
/// The number of bytes per storage unit.
pub const BYTES_PER_UNIT_SIZE: u64 = 1_024 * 1_024; // 1 MiB

// Keep in sync with the same function in `contracts/walrus/sources/system/system_state_inner.move`.
/// Calculates the number of storage units required to store a blob with the
/// given encoded size.
pub fn storage_units_from_size(encoded_size: u64) -> u64 {
    encoded_size.div_ceil(BYTES_PER_UNIT_SIZE)
}

/// Computes the price given the unencoded blob size.
pub fn price_for_unencoded_length(
    unencoded_length: u64,
    n_shards: NonZeroU16,
    price_per_unit_size: u64,
    epochs: EpochCount,
    encoding_type: EncodingType,
) -> Option<u64> {
    encoded_blob_length_for_n_shards(n_shards, unencoded_length, encoding_type)
        .map(|encoded_length| price_for_encoded_length(encoded_length, price_per_unit_size, epochs))
}

/// Computes the price given the encoded blob size.
pub fn price_for_encoded_length(
    encoded_length: u64,
    price_per_unit_size: u64,
    epochs: EpochCount,
) -> u64 {
    storage_units_from_size(encoded_length) * price_per_unit_size * u64::from(epochs)
}

/// Computes the price given the encoded blob size.
pub fn write_price_for_encoded_length(encoded_length: u64, price_per_unit_size: u64) -> u64 {
    storage_units_from_size(encoded_length) * price_per_unit_size
}

/// Gets the package address from an object.
///
/// Note: This returns the package address from the object type, not the newest package ID.
pub(crate) fn get_package_id_from_object(object: &sui_types::object::Object) -> Result<ObjectID> {
    Ok(object
        .struct_tag()
        .context("object does not have a struct tag")?
        .address
        .into())
}

/// Gets the objects of the given type that were created in a transaction.
///
/// All the object ids of the objects created in the transaction, and of type represented by the
/// `struct_tag`, are taken from the [`ExecuteTransactionResponse`].
pub(crate) fn get_created_sui_object_ids_by_type(
    response: &ExecuteTransactionResponse,
    struct_tag: &StructTag,
) -> Result<Vec<ObjectID>> {
    match response.object_changes.as_ref() {
        Some(changes) => Ok(changes
            .iter()
            .filter_map(|changed| {
                if let ObjectChangeEntry::Created {
                    object_type,
                    object_id,
                    ..
                } = changed
                {
                    if object_type == struct_tag {
                        Some(*object_id)
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect()),
        None => Err(anyhow!(
            "no object changes in transaction response for digest {}",
            response.digest
        )),
    }
}

pub(crate) fn get_sui_object_from_bcs<U>(
    bcs_data: &[u8],
    struct_tag: &StructTag,
) -> SuiClientResult<U>
where
    U: AssociatedContractStruct,
{
    ensure!(
        struct_tag.name.as_str() == U::CONTRACT_STRUCT.name
            && struct_tag.module.as_str() == U::CONTRACT_STRUCT.module,
        MoveConversionError::TypeMismatch {
            expected: U::CONTRACT_STRUCT.to_string(),
            actual: format!(
                "{}::{} ({})",
                struct_tag.module.as_str(),
                struct_tag.name.as_str(),
                struct_tag
            ),
        }
        .into()
    );

    Ok(bcs::from_bytes::<U>(bcs_data).with_context(|| {
        format!(
            "could not convert object to expected type {}",
            U::CONTRACT_STRUCT
        )
    })?)
}

// Wallet setup

// Faucets
const LOCALNET_FAUCET: &str = "http://127.0.0.1:9123/gas";
const DEVNET_FAUCET: &str = "https://faucet.devnet.sui.io/v2/gas";
const TESTNET_FAUCET: &str = "https://faucet.testnet.sui.io/v2/gas";

/// Enum for the different sui networks.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SuiNetwork {
    /// Local sui network.
    Localnet,
    /// Sui Devnet.
    Devnet,
    /// Sui Testnet.
    Testnet,
    /// Sui Mainnet.
    Mainnet,
    /// A custom Sui network.
    Custom {
        /// The RPC endpoint for the network.
        rpc: String,
        /// The faucet for the network.
        faucet: Option<String>,
    },
}

impl FromStr for SuiNetwork {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "localnet" => Ok(SuiNetwork::Localnet),
            "devnet" => Ok(SuiNetwork::Devnet),
            "testnet" => Ok(SuiNetwork::Testnet),
            "mainnet" => Ok(SuiNetwork::Mainnet),
            _ => {
                let parts = s.split(';').collect::<Vec<_>>();
                if parts.len() == 1 {
                    Ok(SuiNetwork::Custom {
                        rpc: parts[0].to_owned(),
                        faucet: None,
                    })
                } else if parts.len() == 2 {
                    Ok(SuiNetwork::Custom {
                        rpc: parts[0].to_owned(),
                        faucet: Some(parts[1].to_owned()),
                    })
                } else {
                    Err(anyhow!(
                        "network must be 'localnet', 'devnet', 'testnet', 'mainnet', \
                        or a custom string in the form 'rpc_url(;faucet_url)?'"
                    ))
                }
            }
        }
    }
}

impl SuiNetwork {
    fn faucet(&self) -> Option<&str> {
        match self {
            SuiNetwork::Localnet => Some(LOCALNET_FAUCET),
            SuiNetwork::Devnet => Some(DEVNET_FAUCET),
            SuiNetwork::Testnet => Some(TESTNET_FAUCET),
            SuiNetwork::Mainnet => None,
            SuiNetwork::Custom { faucet, .. } => faucet.as_deref(),
        }
    }

    /// Returns the [`SuiEnv`] associated with `self`.
    pub fn env(&self) -> SuiEnv {
        match self {
            SuiNetwork::Localnet => SuiEnv::localnet(),
            SuiNetwork::Devnet => SuiEnv::devnet(),
            SuiNetwork::Testnet => SuiEnv::testnet(),
            SuiNetwork::Mainnet => SuiEnv {
                alias: "mainnet".to_string(),
                rpc: sui_sdk::SUI_MAINNET_URL.into(),
                ws: None,
                basic_auth: None,
                chain_id: None,
            },
            SuiNetwork::Custom { rpc, .. } => SuiEnv {
                alias: "custom".to_owned(),
                rpc: rpc.to_string(),
                ws: None,
                basic_auth: None,
                chain_id: None,
            },
        }
    }

    /// Returns the string representation of the network.
    pub fn r#type(&self) -> String {
        match self {
            SuiNetwork::Localnet => "localnet".to_owned(),
            SuiNetwork::Devnet => "devnet".to_owned(),
            SuiNetwork::Testnet => "testnet".to_owned(),
            SuiNetwork::Mainnet => "mainnet".to_owned(),
            SuiNetwork::Custom { rpc, faucet } => format!(
                "{rpc}{}",
                faucet.as_ref().map(|f| format!(";{f}")).unwrap_or_default()
            ),
        }
    }
}

impl std::fmt::Display for SuiNetwork {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.r#type())
    }
}

/// Creates a wallet on `network` and stores its config at `config_path`.
///
/// The keystore will be stored in the same directory as the wallet config and named
/// `keystore_filename` (if provided) and `sui.keystore` otherwise.  Returns the created Wallet.
///
/// `request_time` is a configuration that is set in the wallet, and automatically populated to
/// the SuiClient created from the wallet.
pub async fn create_wallet(
    config_path: &Path,
    sui_env: SuiEnv,
    keystore_filename: Option<&str>,
    request_timeout: Option<Duration>,
) -> Result<Wallet> {
    let keystore_path = config_path
        .parent()
        .map_or_else(sui_config_dir, |path| Ok(path.to_path_buf()))?
        .join(keystore_filename.unwrap_or(SUI_KEYSTORE_FILENAME));

    let mut keystore = FileBasedKeystore::load_or_create(&keystore_path)?;
    let GeneratedKey {
        address: new_address,
        public_key: _,
        scheme: _,
        mnemonic: _,
    } = keystore.generate(None, GenerateOptions::default()).await?;

    let keystore = Keystore::from(keystore);

    let alias = sui_env.alias.clone();
    SuiClientConfig {
        keystore,
        external_keys: None,
        envs: vec![sui_env],
        active_address: Some(new_address),
        active_env: Some(alias),
    }
    .persisted(config_path)
    .save()?;
    load_wallet_context_from_path(Some(config_path), request_timeout)
}

/// Sends a request to the faucet to request coins for `address`.
pub async fn send_faucet_request(address: SuiAddress, network: &SuiNetwork) -> Result<()> {
    // send the request to the faucet
    let client = reqwest::Client::new();
    let data_raw = format!("{{\"FixedAmountRequest\": {{ \"recipient\": \"{address}\" }} }} ");
    let Some(faucet) = network.faucet() else {
        return Err(anyhow!("faucet not available for {network}"));
    };
    let _result = client
        .post(faucet)
        .header("Content-Type", "application/json")
        .body(data_raw)
        .send()
        .await?;
    Ok(())
}

/// Returns the SUI coin objects and the SUI address balance of `address`, to detect funds from
/// the faucet, which may arrive in either form.
async fn sui_funds(
    retriable_sui_client: &RetriableSuiClient,
    address: SuiAddress,
) -> Result<(HashSet<ObjectID>, u64)> {
    let coins = retriable_sui_client
        .select_all_coins(address, Coin::SUI)
        .await?
        .into_iter()
        .map(|coin| coin.coin_object_id)
        .collect();
    let address_balance = retriable_sui_client
        .get_address_balance(address, Coin::SUI)
        .await?;
    Ok((coins, address_balance))
}

/// Requests SUI for `address` on `network` from a faucet.
///
/// The faucet may send coin objects; the owner of `address` can move them to its address balance
/// with [`deposit_coins_to_address_balance`].
#[tracing::instrument(skip(network, sui_client))]
pub async fn request_sui_from_faucet(
    address: SuiAddress,
    network: &SuiNetwork,
    sui_client: &RetriableSuiClient,
) -> Result<()> {
    let mut backoff = Duration::from_millis(100);
    let max_backoff = Duration::from_mins(5);
    // Allows checking if we have received funds from the faucet.
    let funds = sui_funds(sui_client, address).await?;

    let mut successful_response = false;

    loop {
        // Send a request to the faucet if either the previous response did not return "ok"
        // or if we waited for at least 2 seconds after the previous request.
        successful_response = if !successful_response {
            send_faucet_request(address, network)
                .await
                .inspect_err(|error| tracing::warn!(?error, "faucet request failed, retrying"))
                .inspect(|_| tracing::debug!("waiting to receive tokens from faucet"))
                .is_ok()
        } else {
            backoff <= Duration::from_secs(2)
        };
        tracing::debug!("sleeping for {backoff:?}");
        tokio::time::sleep(backoff).await;
        if sui_funds(sui_client, address).await? != funds {
            break;
        }
        backoff = backoff.saturating_mul(2).min(max_backoff);
    }
    tracing::debug!("received tokens from faucet");
    Ok(())
}

/// Sends `sui_amount` MIST to the address balance of `address` from the address balance of the
/// provided wallet if the wallet has at least 2 SUI more than that amount, otherwise requests SUI
/// from the faucet (generally provides 1 SUI).
///
/// The faucet may send coin objects; the owner of `address` can move them to its address balance
/// with [`deposit_coins_to_address_balance`].
pub async fn get_sui_from_wallet_or_faucet(
    address: SuiAddress,
    wallet: &mut Wallet,
    network: &SuiNetwork,
    sui_amount: u64,
) -> Result<()> {
    let one_sui = 1_000_000_000;
    let min_balance = sui_amount + 2 * one_sui;
    let sender = wallet.active_address();
    let rpc_urls = &[wallet.get_rpc_url()];
    let client = RetriableSuiClient::new_for_rpc_urls(rpc_urls, Default::default())?;
    let balance = client.get_address_balance(sender, Coin::SUI).await?;
    if balance >= min_balance {
        let mut ptb = ProgrammableTransactionBuilder::new();
        pay_sui_from_address_balance(&mut ptb, address, sui_amount)?;
        let transaction = build_transaction_data_with_address_balance_gas(
            ptb.finish(),
            &client,
            sender,
            Some(one_sui / 2),
            sui_amount,
        )
        .await?;
        sign_and_send_transaction(wallet, &client, transaction, "send_sui").await?;
        Ok(())
    } else {
        request_sui_from_faucet(address, network, &client).await?;
        Ok(())
    }
}

/// Adds commands to `ptb` that send `amount` MIST from the sender's address balance to the address
/// balance of `recipient`.
///
/// The transaction must pay for gas from the sender's SUI address balance, which has to cover
/// `amount` in addition to the gas budget.
pub fn pay_sui_from_address_balance(
    ptb: &mut ProgrammableTransactionBuilder,
    recipient: SuiAddress,
    amount: u64,
) -> Result<()> {
    let coin = if amount == 0 {
        // Withdrawals of zero are rejected by Sui.
        sui_framework_call(
            ptb,
            contracts::coin::zero.with_type_params(&[GAS::type_tag()]),
            vec![],
        )?
    } else {
        let withdrawal = ptb.funds_withdrawal(FundsWithdrawalArg::balance_from_sender(
            amount,
            GAS::type_tag(),
        ))?;
        sui_framework_call(
            ptb,
            contracts::coin::redeem_funds.with_type_params(&[GAS::type_tag()]),
            vec![withdrawal],
        )?
    };
    send_funds(ptb, GAS::type_tag(), coin, recipient)
}

/// Adds a command to `ptb` that sends `coin` of `coin_type` to the address balance of `recipient`.
pub(crate) fn send_funds(
    ptb: &mut ProgrammableTransactionBuilder,
    coin_type: TypeTag,
    coin: Argument,
    recipient: SuiAddress,
) -> Result<()> {
    let recipient_arg = ptb.pure(recipient)?;
    sui_framework_call(
        ptb,
        contracts::coin::send_funds.with_type_params(&[coin_type]),
        vec![coin, recipient_arg],
    )?;
    Ok(())
}

/// Adds a call to `function` of the Sui framework to `ptb`.
fn sui_framework_call(
    ptb: &mut ProgrammableTransactionBuilder,
    function: FunctionTag<'_>,
    arguments: Vec<Argument>,
) -> Result<Argument> {
    Ok(ptb.programmable_move_call(
        SUI_FRAMEWORK_PACKAGE_ID,
        Identifier::from_str(function.module)?,
        Identifier::from_str(function.name)?,
        function.type_params,
        arguments,
    ))
}

/// The maximum number of coins to deposit into the address balance in a single transaction.
const MAX_COINS_PER_DEPOSIT: usize = 500;

/// Moves all coin objects of the given `coin_types` owned by the wallet's active address into its
/// address balances, and returns the digests of the executed transactions.
///
/// The transactions pay for gas from the SUI address balance. If that does not cover the gas, the
/// SUI coins are first used as gas payment to move their value, except for the gas budget, into
/// the address balance; the remainder of that gas coin is moved in a second transaction.
pub async fn deposit_coins_to_address_balance(
    wallet: &Wallet,
    sui_client: &RetriableSuiClient,
    coin_types: &[&str],
    gas_budget: Option<u64>,
) -> SuiClientResult<Vec<TransactionDigest>> {
    let owner = wallet.active_address();
    let mut digests = Vec::new();
    let mut seeded_address_balance = false;
    loop {
        let mut coins = Vec::new();
        for coin_type in coin_types {
            let coin_refs: Vec<_> = sui_client
                .select_all_coins(owner, coin_type)
                .await?
                .iter()
                .map(Coin::object_ref)
                .collect();
            if !coin_refs.is_empty() {
                coins.push((TypeTag::from_str(coin_type)?, coin_refs));
            }
        }
        if coins.is_empty() {
            return Ok(digests);
        }
        let mut coins_in_batch = 0;
        let batch: Vec<_> = coins
            .into_iter()
            .map_while(|(coin_type, mut coin_refs)| {
                let remaining = MAX_COINS_PER_DEPOSIT.checked_sub(coins_in_batch)?;
                coin_refs.truncate(remaining);
                coins_in_batch += coin_refs.len();
                (!coin_refs.is_empty()).then_some((coin_type, coin_refs))
            })
            .collect();
        let transaction = match build_transaction_data_with_address_balance_gas(
            deposit_coins_ptb(owner, &batch)?,
            sui_client,
            owner,
            gas_budget,
            0,
        )
        .await
        {
            Ok(transaction) => transaction,
            Err(SuiClientError::NoCompatibleGasCoins(_)) if !seeded_address_balance => {
                seeded_address_balance = true;
                seed_address_balance_with_gas_coins(owner, sui_client, gas_budget).await?
            }
            Err(error) => return Err(error),
        };
        let response =
            sign_and_send_transaction(wallet, sui_client, transaction, "deposit_coins").await?;
        tracing::info!(digest = %response.digest, %owner, "moved coins to the address balance");
        digests.push(response.digest);
    }
}

/// Returns a transaction that pays for gas with the SUI coins of `owner` and moves their value,
/// except for the gas budget, into the SUI address balance of `owner`.
async fn seed_address_balance_with_gas_coins(
    owner: SuiAddress,
    sui_client: &RetriableSuiClient,
    gas_budget: Option<u64>,
) -> SuiClientResult<TransactionData> {
    let mut gas_coins = sui_client.select_all_coins(owner, Coin::SUI).await?;
    // Use the largest coins if there are more than fit into the gas payment.
    gas_coins.sort_unstable_by_key(|coin| Reverse(coin.balance));
    gas_coins.truncate(MAX_GAS_PAYMENT_OBJECTS);
    let gas_coin_balance: u64 = gas_coins.iter().map(|coin| coin.balance).sum();
    // The amount does not affect the gas cost, so estimate the budget with a placeholder.
    let GasBudgetAndPrice {
        gas_budget,
        gas_price,
    } = sui_client
        .gas_budget_and_price(
            gas_budget,
            owner,
            TransactionKind::ProgrammableTransaction(deposit_gas_coin_ptb(owner, 1)?),
        )
        .await?;
    let amount = gas_coin_balance
        .checked_sub(gas_budget)
        .filter(|amount| *amount > 0)
        .ok_or(SuiClientError::NoCompatibleGasCoins(Some(
            gas_budget.into(),
        )))?;
    Ok(TransactionData::new_programmable(
        owner,
        gas_coins.iter().map(Coin::object_ref).collect(),
        deposit_gas_coin_ptb(owner, amount)?,
        gas_budget,
        gas_price,
    ))
}

/// Returns a PTB that merges the given coins per coin type and sends them to the address balance
/// of `owner`.
fn deposit_coins_ptb(
    owner: SuiAddress,
    coins: &[(TypeTag, Vec<ObjectRef>)],
) -> Result<ProgrammableTransaction> {
    let mut ptb = ProgrammableTransactionBuilder::new();
    for (coin_type, coin_refs) in coins {
        let coin_args = coin_refs
            .iter()
            .map(|coin_ref| ptb.obj(ObjectArg::ImmOrOwnedObject(*coin_ref)))
            .collect::<Result<Vec<_>>>()?;
        let Some((&coin, other_coins)) = coin_args.split_first() else {
            continue;
        };
        if !other_coins.is_empty() {
            ptb.command(Command::MergeCoins(coin, other_coins.to_vec()));
        }
        send_funds(&mut ptb, coin_type.clone(), coin, owner)?;
    }
    Ok(ptb.finish())
}

/// Returns a PTB that splits `amount` off the gas coin and sends it to the address balance of
/// `owner`.
///
/// This is the only use of the gas coin: it moves SUI into the address balance of a wallet that
/// can not yet pay for gas from its address balance.
fn deposit_gas_coin_ptb(owner: SuiAddress, amount: u64) -> Result<ProgrammableTransaction> {
    let mut ptb = ProgrammableTransactionBuilder::new();
    let amount_arg = ptb.pure(amount)?;
    let coin = ptb.command(Command::SplitCoins(Argument::GasCoin, vec![amount_arg]));
    send_funds(&mut ptb, GAS::type_tag(), coin, owner)?;
    Ok(ptb.finish())
}

/// Signs `transaction` with `wallet`, executes it, and checks that it succeeded.
async fn sign_and_send_transaction(
    wallet: &Wallet,
    sui_client: &RetriableSuiClient,
    transaction: TransactionData,
    method: &'static str,
) -> SuiClientResult<ExecuteTransactionResponse> {
    let signed_transaction = wallet.sign_transaction(&transaction).await;
    let response = sui_client
        .execute_transaction(signed_transaction, method, DEFAULT_CHECKPOINT_WAIT_TIMEOUT)
        .await?;
    match &response.effects_status {
        TransactionEffectsStatus::Success => Ok(response),
        TransactionEffectsStatus::Failure { error } => Err(
            SuiClientError::TransactionExecutionError(error.as_str().into()),
        ),
    }
}

/// Generate a proof of possession of node private key for a storage node.
pub fn generate_proof_of_possession(
    bls_sk: &ProtocolKeyPair,
    contract_client: &SuiContractClient,
    current_epoch: Epoch,
) -> SignedMessage<ProofOfPossessionMsg> {
    generate_proof_of_possession_for_address(bls_sk, contract_client.address(), current_epoch)
}

/// Generate a proof of possession of node private key for a storage node with an explicitly
/// specified Sui address.
pub fn generate_proof_of_possession_for_address(
    bls_sk: &ProtocolKeyPair,
    sui_address: SuiAddress,
    current_epoch: Epoch,
) -> SignedMessage<ProofOfPossessionMsg> {
    bls_sk.sign_message(&ProofOfPossessionMsg::new(
        current_epoch,
        sui_address.to_inner(),
        bls_sk.public().clone(),
    ))
}

#[cfg(test)]
mod tests {
    use sui_types::{
        base_types::{ObjectDigest, SequenceNumber},
        transaction::{CallArg, ProgrammableMoveCall},
    };

    use super::*;

    fn random_object_ref() -> ObjectRef {
        (
            ObjectID::random(),
            SequenceNumber::new(),
            ObjectDigest::random(),
        )
    }

    fn assert_send_funds(command: &Command, coin_type: &TypeTag) {
        let Command::MoveCall(call) = command else {
            panic!("expected a move call, got {command:?}");
        };
        let ProgrammableMoveCall {
            package,
            module,
            function,
            type_arguments,
            ..
        } = call.as_ref();
        assert_eq!(*package, SUI_FRAMEWORK_PACKAGE_ID);
        assert_eq!((module.as_str(), function.as_str()), ("coin", "send_funds"));
        assert_eq!(type_arguments, &[coin_type.clone().into()]);
    }

    #[test]
    fn deposit_coins_ptb_merges_and_sends_each_coin_type() -> Result<()> {
        let owner = SuiAddress::random_for_testing_only();
        let wal_type = TypeTag::from_str("0x1234::wal::WAL")?;
        let sui_coins = vec![random_object_ref()];
        let wal_coins = vec![
            random_object_ref(),
            random_object_ref(),
            random_object_ref(),
        ];

        let ptb = deposit_coins_ptb(
            owner,
            &[
                (GAS::type_tag(), sui_coins.clone()),
                (wal_type.clone(), wal_coins.clone()),
            ],
        )?;

        let coin_inputs: Vec<_> = ptb
            .inputs
            .iter()
            .filter_map(|input| match input {
                CallArg::Object(ObjectArg::ImmOrOwnedObject(object_ref)) => Some(*object_ref),
                _ => None,
            })
            .collect();
        assert_eq!(coin_inputs, [sui_coins, wal_coins].concat());
        // A single SUI coin is sent as is; the WAL coins are merged first.
        assert_eq!(ptb.commands.len(), 3);
        assert_send_funds(&ptb.commands[0], &GAS::type_tag());
        assert!(matches!(
            &ptb.commands[1],
            Command::MergeCoins(Argument::Input(2), other_coins) if other_coins.len() == 2
        ));
        assert_send_funds(&ptb.commands[2], &wal_type);
        assert!(!ptb.commands.iter().any(Command::is_gas_coin_used));
        Ok(())
    }

    #[test]
    fn deposit_gas_coin_ptb_sends_split_to_address_balance() -> Result<()> {
        let owner = SuiAddress::random_for_testing_only();
        let ptb = deposit_gas_coin_ptb(owner, 42)?;
        assert_eq!(ptb.commands.len(), 2);
        assert!(matches!(
            &ptb.commands[0],
            Command::SplitCoins(Argument::GasCoin, amounts) if amounts.len() == 1
        ));
        assert_send_funds(&ptb.commands[1], &GAS::type_tag());
        assert!(ptb.inputs.contains(&CallArg::Pure(bcs::to_bytes(&42u64)?)));
        assert!(ptb.inputs.contains(&CallArg::Pure(bcs::to_bytes(&owner)?)));
        Ok(())
    }

    #[test]
    fn pay_sui_from_address_balance_withdraws_and_sends() -> Result<()> {
        let recipient = SuiAddress::random_for_testing_only();
        let mut ptb = ProgrammableTransactionBuilder::new();
        pay_sui_from_address_balance(&mut ptb, recipient, 7)?;
        let ptb = ptb.finish();
        assert_eq!(
            ptb.inputs[0],
            CallArg::FundsWithdrawal(FundsWithdrawalArg::balance_from_sender(7, GAS::type_tag()))
        );
        assert_eq!(ptb.commands.len(), 2);
        assert_send_funds(&ptb.commands[1], &GAS::type_tag());
        assert!(!ptb.commands.iter().any(Command::is_gas_coin_used));
        Ok(())
    }

    #[test]
    fn pay_sui_from_address_balance_does_not_withdraw_zero() -> Result<()> {
        let recipient = SuiAddress::random_for_testing_only();
        let mut ptb = ProgrammableTransactionBuilder::new();
        pay_sui_from_address_balance(&mut ptb, recipient, 0)?;
        let ptb = ptb.finish();
        assert!(
            !ptb.inputs
                .iter()
                .any(|input| matches!(input, CallArg::FundsWithdrawal(_)))
        );
        assert_eq!(ptb.commands.len(), 2);
        assert_send_funds(&ptb.commands[1], &GAS::type_tag());
        Ok(())
    }
}
