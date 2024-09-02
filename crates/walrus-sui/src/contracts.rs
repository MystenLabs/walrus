// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Walrus contract bindings. Provides an interface for looking up contract function,
//! modules, and type names.

use anyhow::{anyhow, Context, Result};
use move_core_types::{identifier::Identifier, language_storage::StructTag as MoveStructTag};
use serde::de::DeserializeOwned;
use sui_sdk::{
    rpc_types::{SuiData, SuiEvent, SuiObjectData},
    types::base_types::ObjectID,
};
use sui_types::TypeTag;
use tracing::instrument;
use walrus_core::ensure;

/// A trait for types that correspond to a contract type.
///
/// Implementors of this trait are convertible from [SuiObjectData]s and can
/// identify their associated contract type.
pub trait AssociatedContractStruct: DeserializeOwned {
    /// [`StructTag`] corresponding to the Move struct associated type.
    const CONTRACT_STRUCT: StructTag<'static>;

    /// Converts a [`SuiObjectData`] to [`Self`].
    #[instrument(err, skip_all)]
    fn try_from_object_data(sui_object_data: &SuiObjectData) -> Result<Self, anyhow::Error> {
        tracing::debug!(
            "converting move object to rust struct {:?}",
            Self::CONTRACT_STRUCT,
        );
        let raw = sui_object_data
            .bcs
            .as_ref()
            .ok_or_else(|| anyhow!("object data does not contain bcs"))?;
        let raw = raw
            .try_as_move()
            .ok_or_else(|| anyhow!("object data is a package, not a move object"))?;
        ensure!(
            raw.type_.name.as_str() == Self::CONTRACT_STRUCT.name
                && raw.type_.module.as_str() == Self::CONTRACT_STRUCT.module,
            "object is not of type {:?}",
            Self::CONTRACT_STRUCT
        );
        raw.deserialize()
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
    contract_ident!(fn system::reserve_space, 2);
    contract_ident!(fn system::share_new);
    contract_ident!(fn system::invalidate_blob_id);
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
    contract_ident!(fn storage_node::create_storage_node_info, 1);
}

/// Module for tags corresponding to the Move module `blob`.
pub mod blob {
    use super::*;

    contract_ident!(fn blob::register, 1);
    contract_ident!(fn blob::certify);
    contract_ident!(struct blob::Blob);
}

/// Module for tags corresponding to the Move module `blob_events`.
pub mod blob_events {
    use super::*;

    contract_ident!(struct blob_events::BlobCertified);
    contract_ident!(struct blob_events::BlobRegistered);
    contract_ident!(struct blob_events::InvalidBlobID);
}
