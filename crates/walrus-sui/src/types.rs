// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Walrus move type bindings. Replicates the move types in Rust.
//!

use anyhow::{anyhow, Ok};
use sui_sdk::rpc_types::{SuiMoveStruct, SuiMoveValue};
use sui_types::base_types::ObjectID;

use crate::{
    contracts,
    contracts::{AssociatedContractStruct, StructTag},
    utils::get_dynamic_field,
};

/// Sui object for storage resources
#[derive(Debug)]
pub struct StorageResource {
    pub id: ObjectID,
    pub start_epoch: u64,
    pub end_epoch: u64,
    pub storage_size: u64,
}

impl TryFrom<&SuiMoveStruct> for StorageResource {
    type Error = anyhow::Error;

    fn try_from(sui_move_struct: &SuiMoveStruct) -> Result<Self, Self::Error> {
        let id = get_dynamic_field!(sui_move_struct, "id", SuiMoveValue::UID { id });
        let start_epoch =
            get_dynamic_field!(sui_move_struct, "start_epoch", SuiMoveValue::String).parse()?;
        let end_epoch =
            get_dynamic_field!(sui_move_struct, "end_epoch", SuiMoveValue::String).parse()?;
        let storage_size =
            get_dynamic_field!(sui_move_struct, "storage_size", SuiMoveValue::String).parse()?;
        Ok(Self {
            id,
            start_epoch,
            end_epoch,
            storage_size,
        })
    }
}

impl TryFrom<SuiMoveStruct> for StorageResource {
    type Error = anyhow::Error;

    fn try_from(sui_move_struct: SuiMoveStruct) -> Result<Self, Self::Error> {
        Self::try_from(&sui_move_struct)
    }
}

impl AssociatedContractStruct for StorageResource {
    const CONTRACT_STRUCT: StructTag<'static> = contracts::storage_resource::Storage;
}
