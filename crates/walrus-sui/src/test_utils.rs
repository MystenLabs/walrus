// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Test utilities for `walrus-sui`.

pub mod system_setup;

#[cfg(not(msim))]
use std::sync::mpsc;
use std::{collections::BTreeSet, path::PathBuf, sync::Arc};

use fastcrypto::{
    bls12381::min_pk::{BLS12381AggregateSignature, BLS12381PrivateKey},
    traits::{Signer, ToFromBytes},
};
#[cfg(msim)]
use sui_config::local_ip_utils;
use sui_sdk::{sui_client_config::SuiEnv, wallet_context::WalletContext};
#[cfg(msim)]
use sui_simulator::runtime::NodeHandle;
use sui_types::{
    base_types::{ObjectID, SuiAddress},
    digests::TransactionDigest,
    event::EventID,
    programmable_transaction_builder::ProgrammableTransactionBuilder,
};
use test_cluster::{TestCluster, TestClusterBuilder};
#[cfg(not(msim))]
use tokio::runtime::Runtime;
#[cfg(msim)]
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use walrus_core::{
    messages::{Confirmation, ConfirmationCertificate, InvalidBlobCertificate, InvalidBlobIdMsg},
    BlobId,
    EncodingType,
    Epoch,
};
use walrus_test_utils::WithTempDir;

use crate::{
    types::{BlobCertified, BlobRegistered, InvalidBlobId},
    utils::{create_wallet, request_sui_from_faucet, sign_and_send_ptb, SuiNetwork},
};

/// Default gas budget for transactions in tests and benchmarks.
pub const DEFAULT_GAS_BUDGET: u64 = 500_000_000;
const DEFAULT_FUNDING_PER_COIN: u64 = 10_000_000_000;

/// Returns a random `EventID` for testing.
pub fn event_id_for_testing() -> EventID {
    EventID {
        tx_digest: TransactionDigest::random(),
        event_seq: 0,
    }
}

/// Returns an arbitrary (fixed) `EventID` for testing.
pub fn fixed_event_id_for_testing() -> EventID {
    EventID {
        tx_digest: TransactionDigest::new([42; 32]),
        event_seq: 314,
    }
}

/// Returns an arbitrary (fixed) `ObjectID` for testing.
pub fn object_id_for_testing() -> ObjectID {
    ObjectID::from_single_byte(42)
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

/// Handle for the global Sui test cluster.
#[allow(missing_debug_implementations)]
pub struct TestClusterHandle {
    wallet_path: Mutex<PathBuf>,
    _cluster: TestCluster,

    #[cfg(msim)]
    _node_handle: NodeHandle,
}

impl TestClusterHandle {
    // Creates a test Sui cluster using tokio runtime.
    #[cfg(not(msim))]
    fn new(runtime: &Runtime) -> Self {
        tracing::debug!("building global Sui test cluster");
        let (tx, rx) = mpsc::channel();
        runtime.spawn(async move {
            let mut test_cluster = sui_test_cluster().await;
            let wallet_path = test_cluster.wallet().config.path().to_path_buf();
            tx.send((test_cluster, wallet_path))
                .expect("can send test cluster");
        });
        let (cluster, wallet_path) = rx.recv().expect("should receive test_cluster");
        Self {
            wallet_path: Mutex::new(wallet_path),
            _cluster: cluster,
        }
    }

    // Creates a test Sui cluster using deterministic MSIM runtime.
    #[cfg(msim)]
    async fn new() -> Self {
        let (tx, mut rx) = mpsc::channel(10);
        let handle = sui_simulator::runtime::Handle::current();
        let builder = handle.create_node();
        let node_handle = builder
            .ip(local_ip_utils::get_new_ip().parse().unwrap())
            .init(move || {
                let tx = tx.clone();
                async move {
                    let mut test_cluster = sui_test_cluster().await;
                    let wallet_path = test_cluster.wallet().config.path().to_path_buf();
                    tx.send((test_cluster, wallet_path))
                        .await
                        .expect("Notifying cluster creation must succeed");
                }
            })
            .build();
        let Some((cluster, wallet_path)) = rx.recv().await else {
            panic!("Unexpected end of channel");
        };
        Self {
            wallet_path: Mutex::new(wallet_path),
            _cluster: cluster,
            _node_handle: node_handle,
        }
    }
}

/// Handler for the global Sui test cluster using the tokio runtime.
#[cfg(not(msim))]
pub mod using_tokio {
    use std::{
        sync::{Arc, OnceLock, Weak},
        thread,
    };

    use tokio::runtime::{Builder, Runtime};

    use super::TestClusterHandle;

    struct GlobalTestClusterHandler {
        inner: Weak<TestClusterHandle>,
        runtime: Runtime,
    }

    impl GlobalTestClusterHandler {
        fn new() -> Self {
            let runtime = thread::spawn(move || {
                Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .expect("should be able to build runtime")
            })
            .join()
            .expect("should be able to wait for thread to finish");
            Self {
                inner: Weak::new(),
                runtime,
            }
        }

        fn get_test_cluster_handle(&mut self) -> Arc<TestClusterHandle> {
            if let Some(handle) = self.inner.upgrade() {
                handle
            } else {
                let handle = Arc::new(TestClusterHandle::new(&self.runtime));
                self.inner = Arc::downgrade(&handle);
                handle
            }
        }
    }

    /// Returns a handle to the global instance of a Sui test cluster and the wallet config path.
    ///
    /// Initializes the test cluster if it doesn't exist yet.
    pub fn global_sui_test_cluster() -> Arc<TestClusterHandle> {
        static CLUSTER: OnceLock<std::sync::Mutex<GlobalTestClusterHandler>> = OnceLock::new();
        CLUSTER
            .get_or_init(|| std::sync::Mutex::new(GlobalTestClusterHandler::new()))
            .lock()
            .unwrap()
            .get_test_cluster_handle()
    }
}

/// Creates a wallet for testing funded with 2 coins by the faucet of the provided network.
pub async fn wallet_for_testing_from_faucet(
    network: &SuiNetwork,
) -> anyhow::Result<WithTempDir<WalletContext>> {
    let mut wallet = temp_dir_wallet(network.env())?;
    let address = wallet.as_mut().active_address()?;
    let sui_client = wallet.as_ref().get_client().await?;
    request_sui_from_faucet(address, network, &sui_client).await?;
    request_sui_from_faucet(address, network, &sui_client).await?;
    Ok(wallet)
}

/// Creates a wallet for testing in a temporary directory.
pub fn temp_dir_wallet(env: SuiEnv) -> anyhow::Result<WithTempDir<WalletContext>> {
    let temp_dir = tempfile::tempdir().expect("temporary directory creation must succeed");
    let wallet = create_wallet(&temp_dir.path().join("wallet_config.yaml"), env, None)?;

    Ok(WithTempDir {
        inner: wallet,
        temp_dir,
    })
}

/// Handler for the global Sui test cluster using the deterministic msim runtime.
#[cfg(msim)]
pub mod using_msim {
    use std::sync::Arc;

    use super::TestClusterHandle;

    /// Returns a handle to a newly created global instance of a Sui test cluster and the wallet
    /// config path.
    pub async fn global_sui_test_cluster() -> Arc<TestClusterHandle> {
        Arc::new(TestClusterHandle::new().await)
    }
}

/// Returns a wallet on the global Sui test cluster as well as a handle to the cluster.
///
/// Initializes the test cluster if it doesn't exist yet. The cluster handle (or at least one
/// copy if the function is called multiple times) must be kept alive while the wallet is active.
pub async fn new_wallet_on_sui_test_cluster(
    sui_cluster: Arc<TestClusterHandle>,
) -> anyhow::Result<WithTempDir<WalletContext>> {
    let path_guard = sui_cluster.wallet_path.lock().await;
    // Load the cluster's wallet from file instead of using the wallet stored in the cluster.
    // This prevents tasks from being spawned in the current runtime that are expected by
    // the wallet to continue running.
    let mut cluster_wallet = WalletContext::new(&path_guard, None, None)?;
    let wallet = wallet_for_testing(&mut cluster_wallet).await?;
    drop(path_guard);
    Ok(wallet)
}

/// Creates and returns a Sui test cluster.
pub async fn sui_test_cluster() -> TestCluster {
    TestClusterBuilder::new()
        .with_num_validators(2)
        .build()
        .await
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
        vec![gas_coin],
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

impl EventForTesting for InvalidBlobId {
    fn for_testing(blob_id: BlobId) -> Self {
        Self {
            epoch: 0,
            blob_id,
            event_id: event_id_for_testing(),
        }
    }
}
