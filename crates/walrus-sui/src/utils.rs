// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper functions for the crate.
//!

use std::{collections::BTreeSet, future::Future, str::FromStr};

use anyhow::{anyhow, Result};
use move_core_types::{language_storage::StructTag as MoveStructTag, u256::U256};
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
    types::base_types::ObjectID,
    SuiClient,
};
use sui_types::{base_types::ObjectType, transaction::CallArg, TypeTag};
use walrus_core::BlobId;

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
pub fn get_created_sui_object_ids_by_type(
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

pub async fn get_sui_object<U>(sui_client: &SuiClient, object_id: ObjectID) -> SuiClientResult<U>
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

/// Returns an error if the condition evaluates to false.
/// If a message is provided instead of an error, the message is turned into
/// an error using [`anyhow!`] and then cast to the expected type
macro_rules! ensure {
    ($cond:expr, $msg:literal $(,)?) => {
        if !$cond {
            return Err(anyhow!($msg).into());
        }
    };
    ($cond:expr, $fmt:expr, $($arg:tt)*) => {
        if !$cond {
            return Err(anyhow!($fmt, $($arg)*).into());
        }
    };
    ($cond:expr, $err:expr $(,)?) => {
        if !$cond {
            return Err($err);
        }
    };
}

pub(crate) use get_dynamic_field;
#[allow(unused)]
pub(crate) use get_field_from_event;

use crate::{client::SuiClientResult, contracts::AssociatedContractStruct};
