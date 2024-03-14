// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper functions for the crate.
//!

use std::collections::BTreeSet;

use anyhow::{anyhow, Result};
use futures::Future;
use move_core_types::language_storage::StructTag as MoveStructTag;
use sui_sdk::{
    rpc_types::{
        ObjectChange,
        Page,
        SuiMoveStruct,
        SuiObjectDataOptions,
        SuiObjectResponse,
        SuiParsedData,
        SuiTransactionBlockResponse,
    },
    types::base_types::ObjectID,
    SuiClient,
};
use sui_types::{base_types::ObjectType, transaction::CallArg, TypeTag};

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

pub(crate) fn get_created_object_ids_by_type(
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

macro_rules! match_for_correct_type {
    ($value: expr, $field_type: path) => {
        match $value {
            Some($field_type(x)) => Some(x),
            _ => None,
        }
    };
    ($value: expr, $field_type: path { $var: ident }) => {
        match $value {
            Some($field_type { $var }) => Some($var),
            _ => None,
        }
    };
}

macro_rules! get_dynamic_field {
    ($struct: expr, $field_name: expr, $field_type: path $({ $var: ident })*) => {
        match_for_correct_type!(
            $struct.read_dynamic_field_value($field_name),
            $field_type $({ $var })*
        ).ok_or(anyhow!(
            "SuiMoveStruct does not contain field {} with expected type {}: {:?}",
            $field_name,
            stringify!($field_type),
            $struct,
        ))?
    };
}

#[allow(unused)]
macro_rules! get_field_from_event {
    ($event_object: expr, $field_name: expr, $field_type: path) => {
        match_for_correct_type!($event_object.get($field_name), $field_type).ok_or(anyhow!(
            "Event does not contain field {} with expected type {}: {:?}",
            $field_name,
            stringify!($field_type),
            $event_object
        ))?
    };
}

pub(crate) use get_dynamic_field;
#[allow(unused)]
pub(crate) use get_field_from_event;
