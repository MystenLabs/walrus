// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Test utilities for `walrus-sui`.

mod mock_clients;
pub mod system_setup;

use std::{
    collections::BTreeSet,
    path::PathBuf,
    sync::{mpsc, OnceLock},
    thread,
};

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
use tokio::{
    runtime::{Builder, Runtime},
    sync::Mutex,
};
use walrus_core::{
    messages::{Confirmation, ConfirmationCertificate, InvalidBlobCertificate, InvalidBlobIdMsg},
    BlobId,
    EncodingType,
    Epoch,
};
use walrus_test_utils::WithTempDir;

use crate::{
    types::{BlobCertified, BlobRegistered, InvalidBlobID},
    utils::{create_wallet, sign_and_send_ptb},
};

const DEFAULT_GAS_BUDGET: u64 = 500_000_000;
const DEFAULT_FUNDING_PER_COIN: u64 = 10_000_000_000;

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

/// Returns the global instance of a Sui test cluster and the path to the wallet config.
///
/// Initialises the test cluster it if it doesn't exist yet.
fn global_sui_test_cluster() -> &'static Mutex<(PathBuf, TestCluster, Runtime)> {
    static CLUSTER: OnceLock<Mutex<(PathBuf, TestCluster, Runtime)>> = OnceLock::new();
    CLUSTER.get_or_init(|| {
        tracing::debug!("building global sui test cluster");
        let (tx, rx) = mpsc::channel();
        let runtime = thread::spawn(move || {
            let runtime = Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("should be able to build runtime");
            runtime.spawn(async move {
                let mut test_cluster = sui_test_cluster().await;
                let wallet_path = test_cluster.wallet().config.path().to_path_buf();
                tx.send((test_cluster, wallet_path))
                    .expect("can send test cluster");
            });
            runtime
        })
        .join()
        .expect("should be able to wait for thread to finish");
        let (cluster, wallet_path) = rx.recv().expect("should receive test_cluster");
        Mutex::new((wallet_path, cluster, runtime))
    })
}

/// Returns a wallet on the global sui test cluster.
///
/// Initialises the test cluster it if it doesn't exist yet.
pub async fn new_wallet_on_global_test_cluster() -> anyhow::Result<WithTempDir<WalletContext>> {
    let guard = global_sui_test_cluster().lock().await;
    // Load the cluster's wallet from file instead of using the wallet stored in the cluster.
    // This prevents tasks from being spawned in the current runtime that are expected by
    // the wallet to continue running.
    let mut cluster_wallet = WalletContext::new(&guard.0, None, None)?;
    wallet_for_testing(&mut cluster_wallet).await
}

/// Creates and returns a sui test cluster.
pub async fn sui_test_cluster() -> TestCluster {
    TestClusterBuilder::new().build().await
}

/// Creates a wallet for testing in the same network as `funding_wallet`, funded by
/// `funding_wallet` by transferring at least two gas objects.
pub async fn wallet_for_testing(
    funding_wallet: &mut WalletContext,
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

/// Funds `recipient` with two gas objects with 10 Sui each.
async fn fund_address(
    funding_wallet: &mut WalletContext,
    recipient: SuiAddress,
) -> anyhow::Result<()> {
    let sender = funding_wallet.active_address()?;

    let gas_coin = funding_wallet
        .gas_for_owner_budget(sender, DEFAULT_GAS_BUDGET, BTreeSet::new())
        .await?
        .1
        .object_ref();

    let mut ptb = ProgrammableTransactionBuilder::new();

    ptb.pay_sui(
        vec![recipient, recipient],
        vec![DEFAULT_FUNDING_PER_COIN, DEFAULT_FUNDING_PER_COIN],
    )?;

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

impl EventForTesting for InvalidBlobID {
    fn for_testing(blob_id: BlobId) -> Self {
        Self {
            epoch: 0,
            blob_id,
            event_id: event_id_for_testing(),
        }
    }
}
