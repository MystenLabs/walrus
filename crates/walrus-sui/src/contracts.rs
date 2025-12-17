// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Walrus contract bindings. Provides an interface for looking up contract function,
//! modules, and type names.

use core::fmt;
use std::collections::BTreeMap;

use anyhow::{Context, Result};
use move_core_types::{identifier::Identifier, language_storage::StructTag as MoveStructTag};
use serde::de::DeserializeOwned;
use sui_sdk::{
    rpc_types::{SuiData, SuiEvent, SuiObjectData},
    types::base_types::ObjectID,
};
use sui_types::TypeTag;
use thiserror::Error;
use tracing::Level;
use walrus_core::ensure;

/// Error returned when converting a Sui object or event to a rust struct.
#[derive(Debug, Error)]
pub enum MoveConversionError {
    #[error("object data does not contain bcs")]
    /// Error if the object data we are trying to convert does not contain BCS.
    NoBcs,
    #[error("not a Move object")]
    /// Error if the object data is not a Move object.
    NotMoveObject,
    /// Error resulting if the object or event does not have the expected type.
    #[error("the Move struct {actual} does not match the expected type {expected}")]
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
    #[tracing::instrument(
        err(Debug, level = Level::DEBUG),
        skip_all,
        fields(object_id = %sui_object_data.object_id),
    )]
    fn try_from_object_data(sui_object_data: &SuiObjectData) -> Result<Self, MoveConversionError> {
        tracing::trace!(
            target_struct = %Self::CONTRACT_STRUCT,
            "converting Move object to Rust struct",
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

/// A trait for types that store the latest package ID. This is helpful to enable some generic
/// functions.
pub(crate) trait AssociatedContractStructWithPkgId: AssociatedContractStruct {
    /// Returns the package ID of the contract.
    fn package_id(&self) -> ObjectID;
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
}

impl FunctionTag<'_> {
    /// Return a new [FunctionTag] with the provided type parameters.
    pub fn with_type_params(&self, type_params: &[TypeTag]) -> Self {
        Self {
            type_params: type_params.to_vec(),
            ..*self
        }
    }
}

pub(crate) type TypeOriginMap = BTreeMap<(String, String), ObjectID>;

/// Tag identifying contract structs based on their name and module.
#[derive(Debug, PartialEq, Eq)]
pub struct StructTag<'a> {
    /// Move struct name.
    pub name: &'a str,
    /// Move module of the struct.
    pub module: &'a str,
}

impl StructTag<'_> {
    /// Returns a [`MoveStructTag`] for the identified struct with the given package ID.
    ///
    /// Use [`Self::to_move_struct_tag_with_type_map`] if the type origin map is available.
    pub(crate) fn to_move_struct_tag_with_package(
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

    /// Converts a [`StructTag`] to a [`MoveStructTag`] using the matching package ID from the given
    /// type origin map.
    pub(crate) fn to_move_struct_tag_with_type_map(
        &self,
        type_origin_map: &TypeOriginMap,
        type_params: &[TypeTag],
    ) -> Result<MoveStructTag> {
        let package_id = type_origin_map
            .get(&(self.module.to_string(), self.name.to_string()))
            .ok_or(anyhow::anyhow!("type origin not found"))?;
        self.to_move_struct_tag_with_package(*package_id, type_params)
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

impl fmt::Display for StructTag<'_> {
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
        #[allow(non_upper_case_globals)]
        #[doc=stringify!([FunctionTag] for the Move function $modname::$itemname)]
        pub const $itemname: FunctionTag = FunctionTag {
            module: stringify!($modname),
            name: stringify!($itemname),
            type_params: vec![],
        };
    };
}

/// Module for tags corresponding to the Move module `storage_resource`.
pub mod storage_resource {
    use super::*;

    contract_ident!(fn storage_resource::split_by_epoch);
    contract_ident!(fn storage_resource::split_by_size);
    contract_ident!(fn storage_resource::fuse_periods);
    contract_ident!(fn storage_resource::fuse_amount);
    contract_ident!(fn storage_resource::fuse);
    contract_ident!(struct storage_resource::Storage);
}

/// Module for tags corresponding to the Move module `system`.
pub mod system {
    use super::*;

    contract_ident!(struct system::System);
    contract_ident!(fn system::reserve_space);
    contract_ident!(fn system::register_blob);
    contract_ident!(fn system::certify_blob);
    contract_ident!(fn system::invalidate_blob_id);
    contract_ident!(fn system::delete_blob);
    contract_ident!(fn system::certify_event_blob);
    contract_ident!(fn system::extend_blob);
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
    contract_ident!(fn staking::register_candidate);
    contract_ident!(fn staking::stake_with_pool);
    contract_ident!(fn staking::request_withdraw_stake);
    contract_ident!(fn staking::withdraw_stake);
    contract_ident!(fn staking::voting_end);
    contract_ident!(fn staking::initiate_epoch_change);
    contract_ident!(fn staking::epoch_sync_done);
    contract_ident!(fn staking::set_node_metadata);
    contract_ident!(fn staking::set_name);
    contract_ident!(fn staking::set_network_address);
    contract_ident!(fn staking::set_network_public_key);
    contract_ident!(fn staking::set_next_public_key);
    contract_ident!(fn staking::reset_next_public_key);
    contract_ident!(fn staking::set_commission_receiver);
    contract_ident!(fn staking::set_governance_authorized);
    contract_ident!(fn staking::set_storage_price_vote);
    contract_ident!(fn staking::set_write_price_vote);
    contract_ident!(fn staking::set_node_capacity_vote);
    contract_ident!(fn staking::collect_commission);
    contract_ident!(fn staking::set_next_commission);
    contract_ident!(fn staking::set_migration_epoch);
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
    contract_ident!(fn init::migrate);
}

/// Module for tags corresponding to the Move module `upgrade`.
pub mod upgrade {
    use super::*;

    contract_ident!(struct upgrade::EmergencyUpgradeCap);
    contract_ident!(struct upgrade::UpgradeManager);
    contract_ident!(fn upgrade::vote_for_upgrade);
    contract_ident!(fn upgrade::authorize_upgrade);
    contract_ident!(fn upgrade::authorize_emergency_upgrade);
    contract_ident!(fn upgrade::commit_upgrade);
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
    contract_ident!(fn storage_node::create_storage_node_info);
}
/// Module for tags corresponding to the Move module `metadata`.
pub mod metadata {
    use super::*;

    contract_ident!(struct metadata::Metadata);
    contract_ident!(fn metadata::new);
    contract_ident!(fn metadata::insert_or_update);
}

/// Module for tags corresponding to the Move module `blob`.
pub mod blob {
    use super::*;

    contract_ident!(struct blob::Blob);
    contract_ident!(fn blob::burn);
    contract_ident!(fn blob::add_metadata);
    contract_ident!(fn blob::take_metadata);
    contract_ident!(fn blob::insert_or_update_metadata_pair);
    contract_ident!(fn blob::remove_metadata_pair);
}

/// Module for tags corresponding to the Move module `shared_blob`.
pub mod shared_blob {
    use super::*;

    contract_ident!(struct shared_blob::SharedBlob);
    contract_ident!(fn shared_blob::new);
    contract_ident!(fn shared_blob::new_funded);
    contract_ident!(fn shared_blob::fund);
    contract_ident!(fn shared_blob::extend);
}

/// Module for tags corresponding to the Move module `blob_events`.
pub mod events {
    use super::*;

    contract_ident!(struct events::BlobCertified);
    contract_ident!(struct events::BlobRegistered);
    contract_ident!(struct events::BlobDeleted);
    contract_ident!(struct events::ManagedBlobRegistered);
    contract_ident!(struct events::ManagedBlobCertified);
    contract_ident!(struct events::ManagedBlobDeleted);
    contract_ident!(struct events::ManagedBlobMadePermanent);
    contract_ident!(struct events::BlobMovedIntoBlobManager);
    contract_ident!(struct events::BlobManagerUpdated);
    contract_ident!(struct events::BlobManagerCreated);
    contract_ident!(struct events::InvalidBlobID);
    contract_ident!(struct events::EpochParametersSelected);
    contract_ident!(struct events::EpochChangeStart);
    contract_ident!(struct events::EpochChangeDone);
    contract_ident!(struct events::ShardsReceived);
    contract_ident!(struct events::ShardRecoveryStart);
    contract_ident!(struct events::ContractUpgraded);
    contract_ident!(struct events::RegisterDenyListUpdate);
    contract_ident!(struct events::DenyListUpdate);
    contract_ident!(struct events::DenyListBlobDeleted);
    contract_ident!(struct events::ContractUpgradeProposed);
    contract_ident!(struct events::ContractUpgradeQuorumReached);
    contract_ident!(struct events::ProtocolVersionUpdated);
}

/// Module for tags corresponding to the Move module `auth`.
pub mod auth {
    use super::*;

    contract_ident!(fn auth::authenticate_sender);
    contract_ident!(fn auth::authenticate_with_object);
    contract_ident!(fn auth::authorized_address);
    contract_ident!(fn auth::authorized_object);
}

/// Module for tags corresponding to the Move module `extended_field`.
pub mod extended_field {
    use super::*;

    contract_ident!(struct extended_field::Key);
}

/// Module for tags corresponding to the Move module `node_metadata`.
pub mod node_metadata {
    use super::*;

    contract_ident!(fn node_metadata::new);
    contract_ident!(struct node_metadata::NodeMetadata);
}

/// Module for tags corresponding to the Move module `wal_exchange`.
pub mod wal_exchange {
    use super::*;

    contract_ident!(struct wal_exchange::AdminCap);
    contract_ident!(struct wal_exchange::Exchange);
    contract_ident!(struct wal_exchange::ExchangeRate);
    contract_ident!(fn wal_exchange::new_exchange_rate);
    contract_ident!(fn wal_exchange::new);
    contract_ident!(fn wal_exchange::new_funded);
    contract_ident!(fn wal_exchange::add_wal);
    contract_ident!(fn wal_exchange::add_sui);
    contract_ident!(fn wal_exchange::add_all_wal);
    contract_ident!(fn wal_exchange::add_all_sui);
    contract_ident!(fn wal_exchange::withdraw_wal);
    contract_ident!(fn wal_exchange::withdraw_sui);
    contract_ident!(fn wal_exchange::set_exchange_rate);
    contract_ident!(fn wal_exchange::exchange_all_for_wal);
    contract_ident!(fn wal_exchange::exchange_for_wal);
    contract_ident!(fn wal_exchange::exchange_all_for_sui);
    contract_ident!(fn wal_exchange::exchange_for_sui);
}

/// Module for tags corresponding to the Move module `subsidies`.
///
/// This module is only used for Walrus credits. Callsites to this module have been updated to be
/// named "credits" in the rust codebase to avoid confusion with the `walrus_subsidies` module.
pub mod credits {
    use super::*;

    contract_ident!(struct subsidies::Subsidies);
    contract_ident!(struct subsidies::AdminCap);
    contract_ident!(fn subsidies::new);
    contract_ident!(fn subsidies::new_with_initial_rates_and_funds);
    contract_ident!(fn subsidies::add_funds);
    contract_ident!(fn subsidies::set_buyer_subsidy_rate);
    contract_ident!(fn subsidies::set_system_subsidy_rate);
    contract_ident!(fn subsidies::extend_blob);
    contract_ident!(fn subsidies::reserve_space);
    contract_ident!(fn subsidies::register_blob);
}

/// Module for tags corresponding to the Move module `walrus_subsidies`.
pub mod walrus_subsidies {
    use super::*;

    contract_ident!(struct walrus_subsidies::WalrusSubsidies);
    contract_ident!(struct walrus_subsidies::WalrusSubsidiesInnerV1);
    contract_ident!(struct walrus_subsidies::SubsidiesInnerKey);
    contract_ident!(struct walrus_subsidies::AdminCap);
    contract_ident!(fn walrus_subsidies::new);
    contract_ident!(fn walrus_subsidies::add_coin);
    contract_ident!(fn walrus_subsidies::process_subsidies);
}

/// Module for tags corresponding to the Move module `dynamic_field` from the `sui` package.
pub mod dynamic_field {
    use super::*;

    contract_ident!(struct dynamic_field::Field);
}

/// Module for tags corresponding to the Move module `package` from the `sui` package.
pub mod package {
    use super::*;

    contract_ident!(struct package::UpgradeCap);
}

/// Module for tags corresponding to the Move module `blob_stash`.
pub mod blob_stash {}

/// Module for tags corresponding to the Move module `managed_blob`.
pub mod managed_blob {
    use super::*;

    contract_ident!(struct managed_blob::ManagedBlob);
}

/// Module for tags corresponding to the Move module `blobmanager`.
pub mod blobmanager {
    use super::*;

    contract_ident!(struct blobmanager::BlobManager);
    contract_ident!(struct blobmanager::BlobManagerCap);
    contract_ident!(fn blobmanager::new_with_unified_storage);
    contract_ident!(fn blobmanager::register_blob);
    contract_ident!(fn blobmanager::certify_blob);
    contract_ident!(fn blobmanager::make_blob_permanent);
    contract_ident!(fn blobmanager::delete_blob);
    contract_ident!(fn blobmanager::move_blob_into_manager);
    contract_ident!(fn blobmanager::cap_manager_id);
    contract_ident!(fn blobmanager::manager_id);
    contract_ident!(fn blobmanager::capacity_info);
    contract_ident!(fn blobmanager::storage_epochs);
    contract_ident!(fn blobmanager::blob_count);
    contract_ident!(fn blobmanager::total_blob_size);
    contract_ident!(fn blobmanager::has_blob);
    contract_ident!(fn blobmanager::get_blob_object_ids);
    contract_ident!(fn blobmanager::get_blob_info);
    // Coin stash operations.
    contract_ident!(fn blobmanager::deposit_wal_to_coin_stash);
    contract_ident!(fn blobmanager::deposit_sui_to_coin_stash);
    contract_ident!(fn blobmanager::buy_storage_from_stash);
    contract_ident!(fn blobmanager::extend_storage_from_stash);
    contract_ident!(fn blobmanager::withdraw_wal);
    contract_ident!(fn blobmanager::withdraw_sui);
    contract_ident!(fn blobmanager::coin_stash_balances);
    // Capability management.
    contract_ident!(fn blobmanager::create_cap);
    contract_ident!(fn blobmanager::revoke_cap);
    contract_ident!(fn blobmanager::can_delegate);
    contract_ident!(fn blobmanager::can_withdraw_funds);
    // Blob attribute operations.
    contract_ident!(fn blobmanager::set_blob_attribute);
    contract_ident!(fn blobmanager::remove_blob_attribute);
    contract_ident!(fn blobmanager::clear_blob_attributes);
    // Extension policy operations.
    contract_ident!(fn blobmanager::set_extension_params);
    // Storage adjustment operations.
    contract_ident!(fn blobmanager::adjust_storage);
}

/// Module for tags corresponding to the Move module `blob_manager_inner_v1`.
pub mod blob_manager_inner_v1 {
    use super::*;

    contract_ident!(struct blob_manager_inner_v1::BlobManagerInnerV1);
    contract_ident!(struct blob_manager_inner_v1::CapInfo);
}
