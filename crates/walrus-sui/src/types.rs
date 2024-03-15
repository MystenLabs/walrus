// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Walrus move type bindings. Replicates the move types in Rust.
//!

use anyhow::anyhow;
use move_core_types::u256::U256;
use sui_sdk::rpc_types::{SuiMoveStruct, SuiMoveValue};
use sui_types::base_types::ObjectID;
use walrus_core::{BlobId, EncodingType};

use crate::{
    contracts::{self, AssociatedContractStruct, StructTag},
    utils::{blob_id_from_u256, get_dynamic_field},
};
/// Sui object for storage resources
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct StorageResource {
    /// Object id of the Sui object
    pub id: ObjectID,
    /// The start epoch of the resource (inclusive)
    pub start_epoch: u64,
    /// The end epoch of the resource (exclusive)
    pub end_epoch: u64,
    /// The total amount of reserved storage
    pub storage_size: u64,
}

impl TryFrom<&SuiMoveStruct> for StorageResource {
    type Error = anyhow::Error;

    fn try_from(sui_move_struct: &SuiMoveStruct) -> Result<Self, Self::Error> {
        let id = get_dynamic_objectid_field!(sui_move_struct)?;
        let start_epoch = get_dynamic_u64_field!(sui_move_struct, "start_epoch")?;
        let end_epoch = get_dynamic_u64_field!(sui_move_struct, "end_epoch")?;
        let storage_size = get_dynamic_u64_field!(sui_move_struct, "storage_size")?;
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

/// Sui object for a blob
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Blob {
    /// Object id of the Sui object
    pub id: ObjectID,
    /// The epoch in which the blob has been registered
    pub stored_epoch: u64,
    /// The blob Id
    pub blob_id: BlobId,
    /// The total encoded size of the blob
    pub encoded_size: u64,
    /// The erasure coding type used for the blob
    pub erasure_code_type: EncodingType,
    /// The epoch in which the blob was first certified, `None` if the blob is uncertified
    pub certified: Option<u64>,
    /// The [`StorageResource`] used to store the blob
    pub storage: StorageResource,
}

impl TryFrom<&SuiMoveStruct> for Blob {
    type Error = anyhow::Error;

    fn try_from(sui_move_struct: &SuiMoveStruct) -> Result<Self, Self::Error> {
        let id = get_dynamic_objectid_field!(sui_move_struct)?;

        let stored_epoch = get_dynamic_u64_field!(sui_move_struct, "stored_epoch")?;
        let blob_id = blob_id_from_u256(
            get_dynamic_field!(sui_move_struct, "blob_id", SuiMoveValue::String)?
                .parse::<U256>()?,
        );
        let encoded_size = get_dynamic_u64_field!(sui_move_struct, "size")?;
        let erasure_code_type = EncodingType::try_from(u8::try_from(get_dynamic_field!(
            sui_move_struct,
            "erasure_code_type",
            SuiMoveValue::Number
        )?)?)?;
        // `SuiMoveValue::Option` seems to be replaced with `SuiMoveValue::String` directly
        // if it is not `None`.
        let certified = get_dynamic_field!(sui_move_struct, "certified", SuiMoveValue::String)
            .and_then(|s| Ok(Some(s.parse()?)))
            .or_else(|_| {
                match get_dynamic_field!(sui_move_struct, "certified", SuiMoveValue::Option)?
                    .as_ref()
                {
                    None => Ok(None),
                    // Below would be the expected behaviour in the not-None-case, so we capture
                    // this nevertheless
                    Some(SuiMoveValue::String(s)) => Ok(Some(s.parse()?)),
                    Some(smv) => Err(anyhow!("unexpected type for field `certified`: {}", smv)),
                }
            })?;
        let storage = StorageResource::try_from(get_dynamic_field!(
            sui_move_struct,
            "storage",
            SuiMoveValue::Struct
        )?)?;
        Ok(Self {
            id,
            stored_epoch,
            blob_id,
            encoded_size,
            erasure_code_type,
            certified,
            storage,
        })
    }
}

impl TryFrom<SuiMoveStruct> for Blob {
    type Error = anyhow::Error;

    fn try_from(sui_move_struct: SuiMoveStruct) -> Result<Self, Self::Error> {
        Self::try_from(&sui_move_struct)
    }
}

impl AssociatedContractStruct for Blob {
    const CONTRACT_STRUCT: StructTag<'static> = contracts::blob::Blob;
}
