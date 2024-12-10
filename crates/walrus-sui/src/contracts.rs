// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Walrus contract bindings. Provides an interface for looking up contract function,
//! modules, and type names.

use core::fmt;

use anyhow::{Context, Result};
use move_core_types::{identifier::Identifier, language_storage::StructTag as MoveStructTag};
use serde::de::DeserializeOwned;
use sui_sdk::{
    rpc_types::{SuiData, SuiEvent, SuiObjectData},
    types::base_types::ObjectID,
};
use sui_types::TypeTag;
use thiserror::Error;
use walrus_core::ensure;

/// Error returned when converting a Sui object or event to a rust struct.
#[derive(Debug, Error)]
pub enum MoveConversionError {
    #[error("object data does not contain bcs")]
    /// Error if the object data we are trying to convert does not contain bcs.
    NoBcs,
    #[error("not a move object")]
    /// Error if the object data is not a move object.
    NotMoveObject,
    /// Error resulting if the object or event does not have the expected type.
    #[error("the move struct {actual} does not match the expected type {expected}")]
    TypeMismatch {
        /// Expected type of the struct.
        expected: String,
        /// Actual type of the struct.
        actual: String,
    },
    #[error(transparent)]
    /// Error during BCS deserialization.
    Bcs(#[from] bcs::Error),
}

/// A trait for types that correspond to a contract type.
///
/// Implementors of this trait are convertible from [SuiObjectData]s and can
/// identify their associated contract type.
pub trait AssociatedContractStruct: DeserializeOwned {
    /// [`StructTag`] corresponding to the Move struct associated type.
    const CONTRACT_STRUCT: StructTag<'static>;

    /// Converts a [`SuiObjectData`] to [`Self`].
    #[tracing::instrument(err, skip_all)]
    fn try_from_object_data(sui_object_data: &SuiObjectData) -> Result<Self, MoveConversionError> {
        tracing::debug!(
            "converting move object to rust struct {:?}",
            Self::CONTRACT_STRUCT,
        );
        let raw = sui_object_data
            .bcs
            .as_ref()
            .ok_or(MoveConversionError::NoBcs)?;
        let raw = raw
            .try_as_move()
            .ok_or(MoveConversionError::NotMoveObject)?;
        ensure!(
            raw.type_.name.as_str() == Self::CONTRACT_STRUCT.name
                && raw.type_.module.as_str() == Self::CONTRACT_STRUCT.module,
            MoveConversionError::TypeMismatch {
                expected: Self::CONTRACT_STRUCT.to_string(),
                actual: format!("{}::{}", raw.type_.module.as_str(), raw.type_.name.as_str()),
            }
        );
        Ok(bcs::from_bytes(&raw.bcs_bytes)?)
    }
}

/// A trait for types that correspond to a Sui event.
///
/// Implementors of this trait are convertible from [SuiEvent]s and can
/// identify their associated contract type.
pub trait AssociatedSuiEvent: TryFrom<SuiEvent> {
    /// [`StructTag`] corresponding to the Move struct of the associated event.
    const EVENT_STRUCT: StructTag<'static>;
}

/// Tag identifying contract functions based on their name and module.
#[derive(Debug)]
pub struct FunctionTag<'a> {
    /// Move function name.
    pub name: &'a str,
    /// Move module of the function.
    pub module: &'a str,
    /// Type parameters of the function.
    pub type_params: Vec<TypeTag>,
    /// Number of Sui objects that are outputs of the function.
    pub n_object_outputs: u16,
}

impl<'a> FunctionTag<'a> {
    /// Return a new [FunctionTag] with the provided type parameters.
    pub fn with_type_params(&self, type_params: &[TypeTag]) -> Self {
        Self {
            type_params: type_params.to_vec(),
            ..*self
        }
    }
}

/// Tag identifying contract structs based on their name and module.
#[derive(Debug, PartialEq, Eq)]
pub struct StructTag<'a> {
    /// Move struct name.
    pub name: &'a str,
    /// Move module of the struct.
    pub module: &'a str,
}

impl<'a> StructTag<'a> {
    /// Returns a Move StructTag for the identified struct, within the published contract module.
    pub fn to_move_struct_tag(
        &self,
        package: ObjectID,
        type_params: &[TypeTag],
    ) -> Result<MoveStructTag> {
        Ok(MoveStructTag {
            address: package.into(),
            module: Identifier::new(self.module).with_context(|| {
                format!("Struct module is not a valid identifier: {}", self.module)
            })?,
            name: Identifier::new(self.name).with_context(|| {
                format!("Struct name is not a valid identifier: {}", self.module)
            })?,
            type_params: type_params.into(),
        })
    }
}

impl<'a> From<&'a MoveStructTag> for StructTag<'a> {
    fn from(value: &'a MoveStructTag) -> Self {
        Self {
            name: value.name.as_str(),
            module: value.module.as_str(),
        }
    }
}

impl<'a> fmt::Display for StructTag<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}::{}", self.module, self.name)
    }
}

macro_rules! contract_ident {
    (struct $modname:ident::$itemname:ident) => {
        #[allow(non_upper_case_globals)]
        #[doc=stringify!([StructTag] for the Move struct $modname::$itemname)]
        pub const $itemname: StructTag = StructTag {
            module: stringify!($modname),
            name: stringify!($itemname),
        };
    };
    (fn $modname:ident::$itemname:ident) => {
        contract_ident!(fn $modname::$itemname, 0);
    };
    (fn $modname:ident::$itemname:ident, $n_out:expr) => {
        #[allow(non_upper_case_globals)]
        #[doc=stringify!([FunctionTag] for the Move function $modname::$itemname)]
        pub const $itemname: FunctionTag = FunctionTag {
            module: stringify!($modname),
            name: stringify!($itemname),
            type_params: vec![],
            n_object_outputs: $n_out,
        };
    };
}

/// Module for tags corresponding to the Move module `storage_resource`.
pub mod storage_resource {
    use super::*;

    contract_ident!(fn storage_resource::split_by_epoch, 1);
    contract_ident!(fn storage_resource::split_by_size, 1);
    contract_ident!(fn storage_resource::fuse_periods);
    contract_ident!(fn storage_resource::fuse_amount);
    contract_ident!(fn storage_resource::fuse);
    contract_ident!(struct storage_resource::Storage);
}

/// Module for tags corresponding to the Move module `system`.
pub mod system {
    use super::*;

    contract_ident!(struct system::System);
    contract_ident!(fn system::reserve_space, 1);
    contract_ident!(fn system::register_blob, 1);
    contract_ident!(fn system::certify_blob);
    contract_ident!(fn system::invalidate_blob_id);
    contract_ident!(fn system::delete_blob, 1);
}

/// Module for tags corresponding to the Move module `system_state_inner`.
pub mod system_state_inner {
    use super::*;

    contract_ident!(struct system_state_inner::SystemStateInnerV1);
}

/// Module for tags corresponding to the Move module `staking_pool`.
pub mod staking_pool {
    use super::*;

    contract_ident!(struct staking_pool::StakingPool);
}

/// Module for tags corresponding to the Move module `staking`.
pub mod staking {
    use super::*;

    contract_ident!(struct staking::Staking);
    contract_ident!(fn staking::register_candidate, 1);
    contract_ident!(fn staking::stake_with_pool, 1);
    contract_ident!(fn staking::voting_end);
    contract_ident!(fn staking::initiate_epoch_change);
    contract_ident!(fn staking::epoch_sync_done);
}

/// Module for tags corresponding to the Move module `staking_inner`.
pub mod staking_inner {
    use super::*;

    contract_ident!(struct staking_inner::StakingInnerV1);
}

/// Module for tags corresponding to the Move module `init`.
pub mod init {
    use super::*;

    contract_ident!(fn init::initialize_walrus);
}

/// Module for tags corresponding to the Move module `staked_wal`.
pub mod staked_wal {
    use super::*;

    contract_ident!(struct staked_wal::StakedWal);
}

/// Module for tags corresponding to the Move module `committee`.
pub mod committee {
    use super::*;

    contract_ident!(struct committee::Committee);
}

/// Module for tags corresponding to the Move module `storage_node`.
pub mod storage_node {
    use super::*;

    contract_ident!(struct storage_node::StorageNodeInfo);
    contract_ident!(struct storage_node::StorageNodeCap);
    contract_ident!(fn storage_node::create_storage_node_info, 1);
}

/// Module for tags corresponding to the Move module `blob`.
pub mod blob {
    use super::*;

    contract_ident!(struct blob::Blob);
}

/// Module for tags corresponding to the Move module `blob_events`.
pub mod events {
    use super::*;

    contract_ident!(struct events::BlobCertified);
    contract_ident!(struct events::BlobRegistered);
    contract_ident!(struct events::BlobDeleted);
    contract_ident!(struct events::InvalidBlobID);
    contract_ident!(struct events::EpochParametersSelected);
    contract_ident!(struct events::EpochChangeStart);
    contract_ident!(struct events::EpochChangeDone);
    contract_ident!(struct events::ShardsReceived);
    contract_ident!(struct events::ShardRecoveryStart);
}

/// Module for tags corresponding to the Move module `wal_exchange`.
pub mod wal_exchange {
    use super::*;

    contract_ident!(struct wal_exchange::AdminCap);
    contract_ident!(struct wal_exchange::Exchange);
    contract_ident!(struct wal_exchange::ExchangeRate);
    contract_ident!(fn wal_exchange::new_exchange_rate, 1);
    contract_ident!(fn wal_exchange::new, 1);
    contract_ident!(fn wal_exchange::new_funded, 1);
    contract_ident!(fn wal_exchange::add_wal);
    contract_ident!(fn wal_exchange::add_sui);
    contract_ident!(fn wal_exchange::add_all_wal);
    contract_ident!(fn wal_exchange::add_all_sui);
    contract_ident!(fn wal_exchange::withdraw_wal, 1);
    contract_ident!(fn wal_exchange::withdraw_sui, 1);
    contract_ident!(fn wal_exchange::set_exchange_rate);
    contract_ident!(fn wal_exchange::exchange_all_for_wal, 1);
    contract_ident!(fn wal_exchange::exchange_for_wal, 1);
    contract_ident!(fn wal_exchange::exchange_all_for_sui, 1);
    contract_ident!(fn wal_exchange::exchange_for_sui, 1);
}

/// Module for tags corresponding to the Move module `dynamic_field` from the `sui` package.
pub mod dynamic_field {
    use super::*;

    contract_ident!(struct dynamic_field::Field);
}
