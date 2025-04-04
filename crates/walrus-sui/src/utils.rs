// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Helper functions for the crate.

use std::{
    collections::HashSet,
    future::Future,
    num::NonZeroU16,
    path::Path,
    str::FromStr,
    time::Duration,
};

use anyhow::{anyhow, Result};
use move_core_types::language_storage::StructTag as MoveStructTag;
use move_package::{source_package::layout::SourcePackageLayout, BuildConfig as MoveBuildConfig};
use serde::{Deserialize, Serialize};
use sui_config::{sui_config_dir, Config, SUI_KEYSTORE_FILENAME};
use sui_keys::keystore::{AccountKeystore, FileBasedKeystore, Keystore};
use sui_sdk::{
    rpc_types::{ObjectChange, Page, SuiObjectResponse, SuiTransactionBlockResponse},
    sui_client_config::{SuiClientConfig, SuiEnv},
    types::base_types::ObjectID,
    wallet_context::WalletContext,
    SuiClient,
};
use sui_types::{
    base_types::SuiAddress,
    crypto::SignatureScheme,
    programmable_transaction_builder::ProgrammableTransactionBuilder,
    transaction::TransactionData,
};
use walrus_core::{
    encoding::encoded_blob_length_for_n_shards,
    keys::ProtocolKeyPair,
    messages::{ProofOfPossessionMsg, SignedMessage},
    EncodingType,
    Epoch,
    EpochCount,
};

use crate::{
    client::{SuiClientResult, SuiContractClient},
    config::load_wallet_context_from_path,
    contracts::AssociatedContractStruct,
};

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
    storage_units_from_size(encoded_length) * price_per_unit_size * (epochs as u64)
}

/// Computes the price given the encoded blob size.
pub fn write_price_for_encoded_length(encoded_length: u64, price_per_unit_size: u64) -> u64 {
    storage_units_from_size(encoded_length) * price_per_unit_size
}

/// Gets the package address from an object response.
///
/// Note: This returns the package address from the object type, not the newest package ID.
pub(crate) fn get_package_id_from_object_response(
    object_response: &SuiObjectResponse,
) -> Result<ObjectID> {
    let sui_types::base_types::ObjectType::Struct(move_object_type) =
        object_response.object()?.object_type()?
    else {
        anyhow::bail!("response does not contain a move struct object");
    };
    Ok(move_object_type.address().into())
}

/// Gets the objects of the given type that were created in a transaction.
///
/// All the object ids of the objects created in the transaction, and of type represented by the
/// `struct_tag`, are taken from the [`SuiTransactionBlockResponse`].
pub(crate) fn get_created_sui_object_ids_by_type(
    response: &SuiTransactionBlockResponse,
    struct_tag: &MoveStructTag,
) -> Result<Vec<ObjectID>> {
    match response.object_changes.as_ref() {
        Some(changes) => Ok(changes
            .iter()
            .filter_map(|changed| {
                if let ObjectChange::Created {
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
            "No object changes in transaction response: {:?}",
            response.errors
        )),
    }
}

pub(crate) fn get_sui_object_from_object_response<U>(
    object_response: &SuiObjectResponse,
) -> SuiClientResult<U>
where
    U: AssociatedContractStruct,
{
    U::try_from_object_data(object_response.data.as_ref().ok_or_else(|| {
        anyhow!(
            "response does not contain object data [err={:?}]",
            object_response.error
        )
    })?)
    .map_err(|_e| {
        anyhow!(
            "could not convert object to expected type {}",
            U::CONTRACT_STRUCT
        )
        .into()
    })
}

pub(crate) async fn handle_pagination<F, T, C, Fut>(
    closure: F,
) -> Result<impl Iterator<Item = T>, sui_sdk::error::Error>
where
    F: FnMut(Option<C>) -> Fut,
    T: 'static,
    Fut: Future<Output = Result<Page<T, C>, sui_sdk::error::Error>>,
{
    handle_pagination_with_cursor(closure, None).await
}

pub(crate) async fn handle_pagination_with_cursor<F, T, C, Fut>(
    mut closure: F,
    mut cursor: Option<C>,
) -> Result<impl Iterator<Item = T>, sui_sdk::error::Error>
where
    F: FnMut(Option<C>) -> Fut,
    T: 'static,
    Fut: Future<Output = Result<Page<T, C>, sui_sdk::error::Error>>,
{
    let mut cont = true;
    let mut iterators = vec![];
    while cont {
        let page = closure(cursor).await?;
        cont = page.has_next_page;
        cursor = page.next_cursor;
        iterators.push(page.data.into_iter());
    }
    Ok(iterators.into_iter().flatten())
}

// Wallet setup

// Faucets
const LOCALNET_FAUCET: &str = "http://127.0.0.1:9123/gas";
const DEVNET_FAUCET: &str = "https://faucet.devnet.sui.io/v1/gas";
const TESTNET_FAUCET: &str = "https://faucet.testnet.sui.io/v1/gas";

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
            },
            SuiNetwork::Custom { rpc, .. } => SuiEnv {
                alias: "custom".to_owned(),
                rpc: rpc.to_string(),
                ws: None,
                basic_auth: None,
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
pub fn create_wallet(
    config_path: &Path,
    sui_env: SuiEnv,
    keystore_filename: Option<&str>,
) -> Result<WalletContext> {
    let keystore_path = config_path
        .parent()
        .map_or_else(sui_config_dir, |path| Ok(path.to_path_buf()))?
        .join(keystore_filename.unwrap_or(SUI_KEYSTORE_FILENAME));

    let mut keystore = FileBasedKeystore::new(&keystore_path)?;
    let (new_address, _phrase, _scheme) =
        keystore.generate_and_add_new_key(SignatureScheme::ED25519, None, None, None)?;

    let keystore = Keystore::from(keystore);

    let alias = sui_env.alias.clone();
    SuiClientConfig {
        keystore,
        envs: vec![sui_env],
        active_address: Some(new_address),
        active_env: Some(alias),
    }
    .persisted(config_path)
    .save()?;
    load_wallet_context_from_path(Some(config_path))
}

/// Sends a request to the faucet to request coins for `address`.
pub async fn send_faucet_request(address: SuiAddress, network: &SuiNetwork) -> Result<()> {
    // send the request to the faucet
    let client = reqwest::Client::new();
    let data_raw = format!(
        "{{\"FixedAmountRequest\": {{ \"recipient\": \"{}\" }} }} ",
        address
    );
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

async fn sui_coin_set(sui_client: &SuiClient, address: SuiAddress) -> Result<HashSet<ObjectID>> {
    Ok(handle_pagination(|cursor| {
        sui_client
            .coin_read_api()
            .get_coins(address.to_owned(), None, cursor, None)
    })
    .await?
    .map(|coin| coin.coin_object_id)
    .collect())
}

/// Requests SUI coins for `address` on `network` from a faucet.
#[tracing::instrument(skip(network, sui_client))]
pub async fn request_sui_from_faucet(
    address: SuiAddress,
    network: &SuiNetwork,
    sui_client: &SuiClient,
) -> Result<()> {
    let mut backoff = Duration::from_millis(100);
    let max_backoff = Duration::from_secs(300);
    // Set of coins to allow checking if we have received a new coin from the faucet
    let coins = sui_coin_set(sui_client, address).await?;

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
        if sui_coin_set(sui_client, address).await? != coins {
            break;
        }
        backoff = backoff.saturating_mul(2).min(max_backoff);
    }
    tracing::debug!("received tokens from faucet");
    Ok(())
}

/// Gets `sui_amount` MIST for `address` from the provided wallet if the wallet has at least 2 SUI
/// more than that amount, otherwise request SUI from the faucet (generally provides 1 SUI).
pub async fn get_sui_from_wallet_or_faucet(
    address: SuiAddress,
    wallet: &mut WalletContext,
    network: &SuiNetwork,
    sui_amount: u64,
) -> Result<()> {
    let one_sui = 1_000_000_000;
    let min_balance = sui_amount + 2 * one_sui;
    let sender = wallet.active_address()?;
    let balance = wallet
        .get_client()
        .await?
        .coin_read_api()
        .get_balance(sender, None)
        .await?;
    tracing::debug!(
        "funding {address} with {sui_amount} SUI, balance: {balance:?}, min_balance: {min_balance:?}",
        balance = balance.total_balance,
        min_balance = min_balance
    );
    if balance.total_balance >= u128::from(min_balance) {
        tracing::debug!("funding from admin wallet");
        let mut ptb = ProgrammableTransactionBuilder::new();
        ptb.transfer_sui(address, Some(sui_amount));
        let ptb = ptb.finish();
        let gas_budget = one_sui / 2;
        let gas_coins = wallet
            .get_client()
            .await?
            .coin_read_api()
            .select_coins(sender, None, (gas_budget + one_sui) as u128, vec![])
            .await?
            .iter()
            .map(|coin| coin.object_ref())
            .collect();

        let transaction = TransactionData::new_programmable(
            sender,
            gas_coins,
            ptb,
            gas_budget,
            wallet.get_reference_gas_price().await?,
        );
        wallet
            .execute_transaction_may_fail(wallet.sign_transaction(&transaction))
            .await?;
        Ok(())
    } else {
        tracing::debug!("funding from faucet");
        request_sui_from_faucet(address, network, &wallet.get_client().await?).await?;
        Ok(())
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

/// Resolve Move.lock file path in package directory (where Move.toml is).
/// Taken with small modifications (no rerooting/changing current directory) from
/// `sui_move::manage_package::resolve_lock_file_path` to avoid adding a dependency.
pub(crate) fn resolve_lock_file_path(
    mut build_config: MoveBuildConfig,
    package_path: &Path,
) -> Result<MoveBuildConfig, anyhow::Error> {
    if build_config.lock_file.is_none() {
        let lock_file_path = package_path.join(SourcePackageLayout::Lock.path());
        build_config.lock_file = Some(lock_file_path);
    }
    Ok(build_config)
}
