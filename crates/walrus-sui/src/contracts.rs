// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Walrus contract bindings. Provides an interface for looking up contract function,
//! modules, and type names.
//!

use anyhow::{Context, Result};
use move_core_types::{identifier::Identifier, language_storage::StructTag as MoveStructTag};
use sui_sdk::{rpc_types::SuiMoveStruct, types::base_types::ObjectID};
use sui_types::TypeTag;

/// A trait for types that correspond to a contract type.
///
/// Implementors of this trait are convertible from [SuiMoveStruct]s and can
/// identify their associated contract type.
pub trait AssociatedContractStruct: TryFrom<SuiMoveStruct> {
    const CONTRACT_STRUCT: StructTag<'static>;
}

/// Tag identifying contract functions based on their name and module.
pub struct FunctionTag<'a> {
    pub name: &'a str,
    pub module: &'a str,
    pub type_params: Vec<TypeTag>,
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
#[derive(Debug)]
pub struct StructTag<'a> {
    pub name: &'a str,
    pub module: &'a str,
}

impl<'a> StructTag<'a> {
    /// Return a Move StructTag for the identified struct, within the published contract module.
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

macro_rules! contract_ident {
    (struct $modname:ident::$itemname:ident) => {
        #[allow(non_upper_case_globals)]
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
        pub const $itemname: FunctionTag = FunctionTag {
            module: stringify!($modname),
            name: stringify!($itemname),
            type_params: vec![],
            n_object_outputs: $n_out,
        };
    };
}

#[allow(unused)]
pub mod storage_resource {
    use super::*;

    contract_ident!(fn storage_resource::split_by_epoch, 1);
    contract_ident!(fn storage_resource::split_by_size, 1);
    contract_ident!(fn storage_resource::fuse_periods);
    contract_ident!(fn storage_resource::fuse_amount);
    contract_ident!(fn storage_resource::fuse);
    contract_ident!(struct storage_resource::Storage);
}

pub mod system {
    use super::*;

    contract_ident!(struct system::System);
    contract_ident!(fn system::reserve_space, 2);
}

pub mod committee {
    use super::*;

    contract_ident!(struct committee::Committee);
}

pub mod storage_node {
    use super::*;

    contract_ident!(struct storage_node::StorageNodeInfo);
}

#[allow(unused)]
pub mod blob {
    use super::*;

    contract_ident!(fn blob::register, 1);
    contract_ident!(fn blob::certify);
    contract_ident!(struct blob::BlobCertified);
    contract_ident!(struct blob::BlobRegistered);
    contract_ident!(struct blob::Blob);
}
