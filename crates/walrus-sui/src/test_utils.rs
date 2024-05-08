// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Test utilities for `walrus-sui`.

mod mock_clients;
pub mod system_setup;

use std::collections::BTreeSet;

use anyhow::bail;
use fastcrypto::{
    bls12381::min_pk::{BLS12381AggregateSignature, BLS12381PrivateKey},
    traits::{Signer, ToFromBytes},
};
pub use mock_clients::{MockContractClient, MockSuiReadClient};
use sui_sdk::wallet_context::WalletContext;
use sui_types::{
    base_types::SuiAddress,
    digests::TransactionDigest,
    event::EventID,
    programmable_transaction_builder::ProgrammableTransactionBuilder,
};
use test_cluster::{TestCluster, TestClusterBuilder};
use walrus_core::{
    messages::{Confirmation, ConfirmationCertificate, InvalidBlobCertificate, InvalidBlobIdMsg},
    BlobId,
    EncodingType,
    Epoch,
};
use walrus_test_utils::WithTempDir;

use crate::{
    types::{BlobCertified, BlobRegistered},
    utils::{create_wallet, sign_and_send_ptb},
};

const DEFAULT_GAS_BUDGET: u64 = 500_000_000;

/// Returns a random `EventID` for testing.
pub fn event_id_for_testing() -> EventID {
    EventID {
        tx_digest: TransactionDigest::random(),
        event_seq: 0,
    }
}

/// Returns a certificate on the provided `blob_id` from the default test committee.
///
/// The default test committee is currently a single storage node with sk = 117.
pub fn get_default_blob_certificate(blob_id: BlobId, epoch: Epoch) -> ConfirmationCertificate {
    let confirmation = bcs::to_bytes(&Confirmation::new(epoch, blob_id)).unwrap();
    let signature = sign_with_default_committee(&confirmation);
    ConfirmationCertificate::new(vec![0], confirmation, signature)
}

/// Returns a certificate from the default test committee that marks `blob_id` as invalid.
///
/// The default test committee is currently a single storage node with sk = 117.
pub fn get_default_invalid_certificate(blob_id: BlobId, epoch: Epoch) -> InvalidBlobCertificate {
    let invalid_blob_id_msg = bcs::to_bytes(&InvalidBlobIdMsg::new(epoch, blob_id)).unwrap();
    let signature = sign_with_default_committee(&invalid_blob_id_msg);
    InvalidBlobCertificate::new(vec![0], invalid_blob_id_msg, signature)
}

/// Creates and returns a sui test cluster.
pub async fn sui_test_cluster() -> TestCluster {
    TestClusterBuilder::new().build().await
}

/// Creates a wallet for testing in the same network as `funding_wallet`, funded by
/// `funding_wallet` by transferring at least two gas objects.
pub async fn wallet_for_testing(
    funding_wallet: &WalletContext,
) -> anyhow::Result<WithTempDir<WalletContext>> {
    let temp_dir = tempfile::tempdir().expect("temporary directory creation must succeed");

    let mut wallet = create_wallet(
        &temp_dir.path().join("wallet_config.yaml"),
        funding_wallet.config.get_active_env()?.to_owned(),
        None,
    )?;
    fund_address(funding_wallet, wallet.active_address()?).await?;
    Ok(WithTempDir {
        inner: wallet,
        temp_dir,
    })
}

/// Funds `recipient` with at least two gas objects from `funding_wallet` that do not
/// belong to the wallets active address.
async fn fund_address(funding_wallet: &WalletContext, recipient: SuiAddress) -> anyhow::Result<()> {
    // Get an address and a list of at least 3 gas objects owned by that address, s.t.
    // we can send two gas objects to the recipient and use the 3rd for funding the
    // transaction.
    let Some((sender, mut objects)) = funding_wallet
        .get_all_accounts_and_gas_objects()
        .await?
        .into_iter()
        .find(|(_sender, objects)| objects.len() >= 3)
    else {
        bail!("no address in funding wallet with at least 3 gas objects")
    };

    // Get a gas coin with enough budget
    let gas_coin = funding_wallet
        .gas_for_owner_budget(sender, DEFAULT_GAS_BUDGET, BTreeSet::new())
        .await?
        .1
        .object_ref();

    // Exclude the gas coin used for the transaction from the objects that we are sending to the
    // recipient address.
    if let Some((coin_index, _)) = objects
        .iter()
        .enumerate()
        .find(|(_, &coin)| coin == gas_coin)
    {
        objects.swap_remove(coin_index);
    }

    let mut ptb = ProgrammableTransactionBuilder::new();
    for object_ref in objects {
        ptb.transfer_object(recipient, object_ref)?;
    }

    sign_and_send_ptb(
        sender,
        funding_wallet,
        ptb.finish(),
        gas_coin,
        DEFAULT_GAS_BUDGET,
    )
    .await?;

    Ok(())
}

fn sign_with_default_committee(msg: &[u8]) -> BLS12381AggregateSignature {
    let mut sk = [0; 32];
    sk[31] = 117;
    let sk = BLS12381PrivateKey::from_bytes(&sk).unwrap();
    BLS12381AggregateSignature::from(sk.sign(msg))
}

/// Trait to provide an event with the specified `blob_id` for testing.
pub trait EventForTesting {
    /// Returns an event with the specified `blob_id` for testing.
    fn for_testing(blob_id: BlobId) -> Self;
}

impl EventForTesting for BlobRegistered {
    fn for_testing(blob_id: BlobId) -> Self {
        Self {
            epoch: 0,
            blob_id,
            size: 10000,
            erasure_code_type: EncodingType::RedStuff,
            end_epoch: 42,
            event_id: event_id_for_testing(),
        }
    }
}

impl EventForTesting for BlobCertified {
    fn for_testing(blob_id: BlobId) -> Self {
        Self {
            epoch: 0,
            blob_id,
            end_epoch: 42,
            event_id: event_id_for_testing(),
        }
    }
}
