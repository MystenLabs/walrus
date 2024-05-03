// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Utilities to publish the walrus contracts and deploy a system object for testing.

use std::{path::PathBuf, str::FromStr};

use anyhow::Result;
use fastcrypto::{bls12381::min_pk::BLS12381PublicKey, traits::ToFromBytes};
use sui_sdk::{types::base_types::ObjectID, wallet_context::WalletContext};
use walrus_core::ShardIndex;

use crate::{
    system_setup::{create_system_object, publish_package, SystemParameters},
    types::{Committee, NetworkAddress, StorageNode},
};

const DEFAULT_GAS_BUDGET: u64 = 1_000_000_000;
const DEFAULT_CAPACITY: u64 = 1_000_000_000;
const DEFAULT_PRICE: u64 = 10;

/// Provides the default contract path for testing for the package with name `package`.
pub fn contract_path_for_testing(package: &str) -> anyhow::Result<PathBuf> {
    Ok(PathBuf::from_str(env!("CARGO_MANIFEST_DIR"))?
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("contracts")
        .join(package))
}

/// Publishes the package with a default system object.
///
/// The system object has the default e2e test setup (compatible with the current tests), and
/// returns the IDs of the package and the system object. The default test setup currently uses a
/// single storage node with sk = 117.
pub async fn publish_with_default_system(
    wallet: &mut WalletContext,
) -> Result<(ObjectID, ObjectID)> {
    let (pkg_id, cap_id) = publish_package(
        wallet,
        contract_path_for_testing("blob_store")?,
        DEFAULT_GAS_BUDGET,
    )
    .await?;

    // Default system config, compatible with current tests

    // Pk corresponding to secret key scalar(117)
    let pubkey_bytes = [
        149, 234, 204, 58, 220, 9, 200, 39, 89, 63, 88, 30, 142, 45, 224, 104, 191, 76, 245, 208,
        192, 235, 41, 229, 55, 47, 13, 35, 54, 71, 136, 238, 15, 155, 235, 17, 44, 138, 126, 156,
        47, 12, 114, 4, 51, 112, 92, 240,
    ];
    let node = StorageNode {
        name: "Test0".to_owned(),
        network_address: NetworkAddress::from_str("127.0.0.1:8080")?,
        public_key: BLS12381PublicKey::from_bytes(&pubkey_bytes)?,
        shard_ids: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
            .into_iter()
            .map(ShardIndex)
            .collect(),
    };
    let committee = Committee::new(vec![node], 0)?;
    let system_params = SystemParameters::new_with_sui(committee, DEFAULT_CAPACITY, DEFAULT_PRICE);

    // Create system object
    let system_object_id =
        create_system_object(wallet, pkg_id, cap_id, &system_params, DEFAULT_GAS_BUDGET).await?;

    Ok((pkg_id, system_object_id))
}
