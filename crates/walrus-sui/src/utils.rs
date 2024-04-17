// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper functions for the crate.
//!

use std::{
    collections::{BTreeSet, HashSet},
    future::Future,
    path::{Path, PathBuf},
    str::FromStr,
};

use anyhow::{anyhow, Result};
use move_core_types::{language_storage::StructTag as MoveStructTag, u256::U256};
use sui_config::{sui_config_dir, Config, SUI_CLIENT_CONFIG, SUI_KEYSTORE_FILENAME};
use sui_keys::keystore::{AccountKeystore, FileBasedKeystore, Keystore};
use sui_sdk::{
    rpc_types::{
        ObjectChange,
        Page,
        SuiMoveStruct,
        SuiMoveValue,
        SuiObjectDataOptions,
        SuiObjectResponse,
        SuiParsedData,
        SuiTransactionBlockResponse,
    },
    sui_client_config::{SuiClientConfig, SuiEnv},
    types::base_types::ObjectID,
    wallet_context::WalletContext,
    SuiClient,
};
use sui_types::{
    base_types::{ObjectType, SuiAddress},
    crypto::SignatureScheme,
    transaction::CallArg,
    TypeTag,
};
use tracing::instrument;
use walrus_core::BlobId;

use crate::{client::SuiClientResult, contracts::AssociatedContractStruct};

pub(crate) fn get_struct_from_object_response(
    object_response: &SuiObjectResponse,
) -> Result<SuiMoveStruct> {
    match object_response {
        SuiObjectResponse {
            data: Some(data),
            error: None,
        } => match &data.content {
            Some(SuiParsedData::MoveObject(parsed_object)) => Ok(parsed_object.fields.clone()),
            _ => Err(anyhow!("Unexpected data in ObjectResponse: {:?}", data)),
        },
        SuiObjectResponse {
            error: Some(error), ..
        } => Err(anyhow!("Error in ObjectResponse: {:?}", error)),
        SuiObjectResponse { .. } => Err(anyhow!(
            "ObjectResponse contains data and error: {:?}",
            object_response
        )),
    }
}

pub(crate) async fn get_type_parameters(
    sui: &SuiClient,
    object_id: ObjectID,
) -> Result<Vec<TypeTag>> {
    match sui
        .read_api()
        .get_object_with_options(object_id, SuiObjectDataOptions::new().with_type())
        .await?
        .into_object()?
        .object_type()?
    {
        ObjectType::Struct(move_obj_type) => Ok(move_obj_type.type_params()),
        ObjectType::Package => Err(anyhow!(
            "Object ID points to a package instead of a Move Struct"
        )),
    }
}

/// Retrieves the objects of the given type that were created in a transaction
/// from a [`SuiTransactionBlockResponse`]
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

pub(crate) async fn get_sui_object<U>(
    sui_client: &SuiClient,
    object_id: ObjectID,
) -> SuiClientResult<U>
where
    U: AssociatedContractStruct,
{
    let obj_struct = get_struct_from_object_response(
        &sui_client
            .read_api()
            .get_object_with_options(object_id, SuiObjectDataOptions::new().with_content())
            .await?,
    )?;
    U::try_from(obj_struct).map_err(|_e| {
        anyhow!(
            "could not convert object with id {} to expected type",
            object_id
        )
        .into()
    })
}

/// Attempts to convert a vector of SuiMoveValues to a vector of numeric rust types
pub(crate) fn sui_move_convert_numeric_vec<T>(sui_move_vec: Vec<SuiMoveValue>) -> Result<Vec<T>>
where
    T: TryFrom<u32> + FromStr,
{
    sui_move_vec
        .into_iter()
        .map(|e| match e {
            SuiMoveValue::Number(n) => T::try_from(n).map_err(|_| anyhow!("conversion failed")),
            SuiMoveValue::String(s) => s.parse().map_err(|_| anyhow!("conversion failed")),
            other => Err(anyhow!("unexpected value in Move vector: {:?}", other)),
        })
        .collect()
}

/// Attempts to convert a vector of SuiMoveValues to a vector of type T
pub(crate) fn sui_move_convert_struct_vec<T>(sui_move_vec: Vec<SuiMoveValue>) -> Result<Vec<T>>
where
    T: AssociatedContractStruct,
{
    sui_move_vec
        .into_iter()
        .map(|e| match e {
            SuiMoveValue::Struct(move_struct) => {
                T::try_from(move_struct).map_err(|_| anyhow!("conversion failed"))
            }
            other => Err(anyhow!("unexpected value in Move vector: {:?}", other)),
        })
        .collect()
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

pub(crate) fn call_args_to_object_ids<T>(call_args: T) -> BTreeSet<ObjectID>
where
    T: IntoIterator<Item = CallArg>,
{
    call_args
        .into_iter()
        .filter_map(|arg| match arg {
            CallArg::Object(sui_sdk::types::transaction::ObjectArg::ImmOrOwnedObject(obj)) => {
                Some(obj.0.to_owned())
            }
            _ => None,
        })
        .collect()
}

pub(crate) fn blob_id_from_u256(input: U256) -> BlobId {
    BlobId(input.to_le_bytes())
}

// Wallet setup

// Faucets
const LOCALNET_FAUCET: &str = "http://127.0.0.1:9123/gas";
const DEVNET_FAUCET: &str = "https://faucet.devnet.sui.io/v1/gas";
const TESTNET_FAUCET: &str = "https://faucet.testnet.sui.io/v1/gas";

/// Enum for the different sui networks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SuiNetwork {
    /// Local sui network
    Localnet,
    /// Sui Devnet
    Devnet,
    /// Sui Testnet
    Testnet,
}

impl FromStr for SuiNetwork {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "localnet" => Ok(SuiNetwork::Localnet),
            "devnet" => Ok(SuiNetwork::Devnet),
            "testnet" => Ok(SuiNetwork::Testnet),
            _ => Err(anyhow!(
                "network must be 'localnet', 'devnet', or 'testnet'"
            )),
        }
    }
}

impl SuiNetwork {
    fn faucet(&self) -> &str {
        match self {
            SuiNetwork::Localnet => LOCALNET_FAUCET,
            SuiNetwork::Devnet => DEVNET_FAUCET,
            SuiNetwork::Testnet => TESTNET_FAUCET,
        }
    }

    fn env(&self) -> SuiEnv {
        match self {
            SuiNetwork::Localnet => SuiEnv::localnet(),
            SuiNetwork::Devnet => SuiEnv::devnet(),
            SuiNetwork::Testnet => SuiEnv::testnet(),
        }
    }
}

/// Loads a sui wallet from `config_path`.
pub fn load_wallet(config_path: Option<PathBuf>) -> Result<WalletContext> {
    let config_path = config_path.unwrap_or(sui_config_dir()?.join(SUI_CLIENT_CONFIG));
    WalletContext::new(&config_path, None, None)
}

/// Creates a wallet on `network` and stores its config at `config_path`. The keystore will
/// be stored in the same directory as the wallet config and named `keystore_filename`
/// (if provided) and `sui.keystore` otherwise.
/// Returns the created Wallet.
pub fn create_wallet(
    config_path: &Path,
    network: &SuiNetwork,
    keystore_filename: Option<&str>,
) -> Result<WalletContext> {
    let env = network.env();
    let keystore_path = config_path
        .parent()
        .unwrap_or(&sui_config_dir()?)
        .join(keystore_filename.unwrap_or(SUI_KEYSTORE_FILENAME));
    let mut keystore = Keystore::from(FileBasedKeystore::new(&keystore_path)?);

    let (new_address, _phrase, _scheme) =
        keystore.generate_and_add_new_key(SignatureScheme::ED25519, None, None, None)?;
    let alias = env.alias.clone();
    SuiClientConfig {
        keystore,
        envs: vec![env],
        active_address: Some(new_address),
        active_env: Some(alias),
    }
    .persisted(config_path)
    .save()?;
    load_wallet(Some(config_path.to_owned()))
}

/// Requests sui for `address` on `network` from a faucet.
#[instrument(skip(network, sui_client))]
pub async fn request_sui_from_faucet(
    address: SuiAddress,
    network: SuiNetwork,
    sui_client: &SuiClient,
) -> Result<()> {
    // Set of coins to allow checking if we have received a new coin from the faucet
    let coins: HashSet<_> = handle_pagination(|cursor| {
        sui_client
            .coin_read_api()
            .get_coins(address.to_owned(), None, cursor, None)
    })
    .await?
    .map(|coin| coin.coin_object_id)
    .collect();

    // send the request to the faucet
    let client = reqwest::Client::new();
    let data_raw = format!(
        "{{\"FixedAmountRequest\": {{ \"recipient\": \"{}\" }} }} ",
        address
    );
    let _result = client
        .post(network.faucet())
        .header("Content-Type", "application/json")
        .body(data_raw)
        .send()
        .await?;

    tracing::info!("waiting to receive tokens from faucet");
    // check if we have received a new coin from the faucet
    while handle_pagination(|cursor| {
        sui_client
            .coin_read_api()
            .get_coins(address.to_owned(), None, cursor, None)
    })
    .await?
    .all(|coin| coins.contains(&coin.coin_object_id))
    {
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    }
    tracing::info!("received tokens from faucet");
    Ok(())
}

// Macros

macro_rules! call_arg_pure {
    ($value:expr) => {
        CallArg::Pure(bcs::to_bytes($value).map_err(|e| anyhow!("bcs conversion failed: {e:?}"))?)
    };
}

macro_rules! match_for_correct_type {
    ($value:expr, $field_type:path) => {
        match $value {
            Some($field_type(x)) => Some(x),
            _ => None,
        }
    };
    ($value:expr, $field_type:path { $var:ident }) => {
        match $value {
            Some($field_type { $var }) => Some($var),
            _ => None,
        }
    };
}

macro_rules! get_dynamic_field {
    ($struct:expr, $field_name:expr, $field_type:path $({ $var:ident })*) => {
        match_for_correct_type!(
            $struct.read_dynamic_field_value($field_name),
            $field_type $({ $var })*
        ).ok_or(anyhow!(
            "SuiMoveStruct does not contain field {} with expected type {}: {:?}",
            $field_name,
            stringify!($field_type),
            $struct,
        ))
    };
}

macro_rules! get_dynamic_objectid_field {
    ($struct:expr) => {
        get_dynamic_field!($struct, "id", SuiMoveValue::UID { id })
    };
}

macro_rules! get_dynamic_u64_field {
    ($struct:expr, $field_name:expr) => {
        get_dynamic_field!($struct, $field_name, SuiMoveValue::String)?.parse()
    };
}

macro_rules! get_field_from_event {
    ($event_object:expr, $field_name:expr, $field_type:path) => {
        match_for_correct_type!($event_object.get($field_name), $field_type).ok_or(anyhow!(
            "Event does not contain field {} with expected type {}: {:?}",
            $field_name,
            stringify!($field_type),
            $event_object
        ))
    };
}

macro_rules! get_u64_field_from_event {
    ($struct: expr, $field_name: expr) => {
        get_field_from_event!($struct, $field_name, Value::String)?.parse()
    };
}

pub(crate) use get_dynamic_field;
#[allow(unused)]
pub(crate) use get_field_from_event;
