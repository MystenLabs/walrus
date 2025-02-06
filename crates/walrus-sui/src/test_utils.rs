// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Test utilities for `walrus-sui`.

pub mod system_setup;

#[cfg(not(msim))]
use std::sync::mpsc;
use std::{
    collections::BTreeSet,
    fmt::{self, Debug, Formatter},
    path::PathBuf,
    sync::Arc,
};

use fastcrypto::{
    bls12381::min_pk::{BLS12381AggregateSignature, BLS12381KeyPair, BLS12381PrivateKey},
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
    transaction::TransactionData,
};
use test_cluster::{TestCluster, TestClusterBuilder};
#[cfg(not(msim))]
use tokio::runtime::Runtime;
#[cfg(msim)]
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use walrus_core::{
    keys::ProtocolKeyPair,
    messages::{
        BlobPersistenceType,
        Confirmation,
        ConfirmationCertificate,
        InvalidBlobCertificate,
        InvalidBlobIdMsg,
    },
    BlobId,
    EncodingType,
    Epoch,
};
use walrus_test_utils::WithTempDir;

use crate::{
    client::SuiContractClient,
    types::{BlobCertified, BlobDeleted, BlobRegistered, InvalidBlobId},
    utils::{create_wallet, request_sui_from_faucet, SuiNetwork},
};

/// Default gas budget for some transactions in tests and benchmarks.
const DEFAULT_GAS_BUDGET: u64 = 500_000_000;
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
    let confirmation = bcs::to_bytes(&Confirmation::new(
        epoch,
        blob_id,
        BlobPersistenceType::Permanent,
    ))
    .unwrap();
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

/// Represents a test cluster running within this process or as a separate process.
pub enum LocalOrExternalTestCluster {
    /// A test cluster running within this process.
    Local {
        /// The local test cluster.
        cluster: TestCluster,
    },
    /// A test running in another process.
    External {
        /// The RPC URL of the external test cluster.
        rpc_url: String,
    },
}
impl Debug for LocalOrExternalTestCluster {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Local { .. } => f.debug_struct("Local").finish(),
            Self::External { rpc_url } => f
                .debug_struct("External")
                .field("rpc_url", rpc_url)
                .finish(),
        }
    }
}

impl LocalOrExternalTestCluster {
    /// Returns the URL of the RPC node.
    pub fn rpc_url(&self) -> String {
        match self {
            LocalOrExternalTestCluster::Local { cluster } => {
                cluster.fullnode_handle.rpc_url.clone()
            }
            LocalOrExternalTestCluster::External { rpc_url, .. } => rpc_url.clone(),
        }
    }
}

/// Handle for the global Sui test cluster.
pub struct TestClusterHandle {
    wallet_path: Mutex<PathBuf>,
    cluster: LocalOrExternalTestCluster,

    #[cfg(msim)]
    node_handle: NodeHandle,
}

impl Debug for TestClusterHandle {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("TestClusterHandle").finish()
    }
}

impl TestClusterHandle {
    // Creates a test Sui cluster using tokio runtime.
    #[cfg(not(msim))]
    fn new(runtime: &Runtime) -> Self {
        Self::from_env().unwrap_or_else(|| Self::new_on_runtime(runtime))
    }

    // Creates a test Sui cluster using tokio runtime.
    #[cfg(not(msim))]
    fn new_on_runtime(runtime: &Runtime) -> Self {
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
            cluster: LocalOrExternalTestCluster::Local { cluster },
        }
    }

    /// Attempts to construct a handle to an externally running sui cluster.
    ///
    /// If the environment variable `SUI_TEST_CONFIG_DIR` is defined, then the wallet and network
    /// configuration information taken from the the associated Sui files in the specified
    /// directory.
    ///
    /// Returns None if the environment variable is not set.
    #[cfg(not(msim))]
    fn from_env() -> Option<Self> {
        let config_path = std::env::var("SUI_TEST_CONFIG_DIR").ok()?;
        tracing::debug!("using external Sui test cluster");
        let wallet_path = std::path::Path::new(&config_path)
            .join("client.yaml")
            .into();
        let rpc_url = "http://127.0.0.1:9000".into();
        Some(Self {
            cluster: LocalOrExternalTestCluster::External { rpc_url },
            wallet_path,
        })
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
            cluster: LocalOrExternalTestCluster::Local { cluster },
            node_handle,
        }
    }

    /// Returns the path to the wallet config file.
    pub async fn wallet_path(&self) -> PathBuf {
        self.wallet_path.lock().await.clone()
    }

    /// Returns the test cluster reference.
    #[cfg(not(msim))]
    pub fn cluster(&self) -> &LocalOrExternalTestCluster {
        &self.cluster
    }

    /// Returns the local test cluster reference for simtests.
    #[cfg(msim)]
    pub fn cluster(&self) -> &TestCluster {
        let LocalOrExternalTestCluster::Local { ref cluster } = self.cluster else {
            unreachable!("always use a local test cluster in simtests")
        };
        cluster
    }

    /// Returns the URL of the RPC node.
    pub fn rpc_url(&self) -> String {
        self.cluster.rpc_url()
    }

    /// Returns the simulator node handle for the Sui test cluster.
    #[cfg(msim)]
    pub fn sim_node_handle(&self) -> &NodeHandle {
        &self.node_handle
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

/// Returns a new wallet on the global Sui test cluster.
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

/// Returns a new `SuiContractClient` on the global Sui test cluster.
pub async fn new_contract_client_on_sui_test_cluster(
    sui_cluster_handle: Arc<TestClusterHandle>,
    existing_client: &SuiContractClient,
) -> anyhow::Result<WithTempDir<SuiContractClient>> {
    let contract_config = existing_client.read_client().contract_config();
    let walrus_client = new_wallet_on_sui_test_cluster(sui_cluster_handle)
        .await?
        .and_then_async(|wallet| {
            SuiContractClient::new(
                wallet,
                &contract_config,
                existing_client.read_client().backoff_config().clone(),
                None,
            )
        })
        .await?;
    Ok(walrus_client)
}

/// Creates and returns a Sui test cluster.
pub async fn sui_test_cluster() -> TestCluster {
    TestClusterBuilder::new()
        .with_num_validators(1)
        .disable_fullnode_pruning()
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

    fund_addresses(funding_wallet, vec![wallet.active_address()?]).await?;

    Ok(WithTempDir {
        inner: wallet,
        temp_dir,
    })
}

/// Funds the `recipients` with gas objects with `DEFAULT_FUNDING_PER_COIN` Sui each.
async fn fund_addresses(
    funding_wallet: &mut WalletContext,
    recipients: Vec<SuiAddress>,
) -> anyhow::Result<()> {
    let sender = funding_wallet.active_address()?;

    let gas_coin = funding_wallet
        .gas_for_owner_budget(sender, DEFAULT_GAS_BUDGET, BTreeSet::new())
        .await?
        .1
        .object_ref();

    let mut ptb = ProgrammableTransactionBuilder::new();

    let amounts = vec![DEFAULT_FUNDING_PER_COIN; recipients.len()];
    ptb.pay_sui(recipients, amounts)?;

    let transaction = TransactionData::new_programmable(
        sender,
        vec![gas_coin],
        ptb.finish(),
        DEFAULT_GAS_BUDGET,
        funding_wallet.get_reference_gas_price().await?,
    );
    funding_wallet
        .execute_transaction_may_fail(funding_wallet.sign_transaction(&transaction))
        .await?;

    Ok(())
}

fn sign_with_default_committee(msg: &[u8]) -> BLS12381AggregateSignature {
    default_protocol_keypair().as_ref().sign(msg).into()
}

fn default_protocol_keypair() -> ProtocolKeyPair {
    let mut sk = [0; 32];
    sk[31] = 117;
    let sk = BLS12381PrivateKey::from_bytes(&sk).unwrap();
    BLS12381KeyPair::from(sk).into()
}

/// Trait to provide an event with the specified `blob_id` for testing.
pub trait EventForTesting {
    /// Returns an event with the specified `blob_id` for testing.
    fn for_testing(blob_id: BlobId) -> Self;
}

impl EventForTesting for BlobRegistered {
    fn for_testing(blob_id: BlobId) -> Self {
        Self {
            epoch: 1,
            blob_id,
            size: 10000,
            encoding_type: EncodingType::RedStuff,
            end_epoch: 42,
            deletable: false,
            object_id: ObjectID::random(),
            event_id: event_id_for_testing(),
        }
    }
}

impl EventForTesting for BlobCertified {
    fn for_testing(blob_id: BlobId) -> Self {
        Self {
            epoch: 1,
            blob_id,
            end_epoch: 42,
            deletable: false,
            object_id: ObjectID::random(),
            is_extension: false,
            event_id: event_id_for_testing(),
        }
    }
}

impl EventForTesting for BlobDeleted {
    fn for_testing(blob_id: BlobId) -> Self {
        Self {
            epoch: 1,
            blob_id,
            end_epoch: 42,
            object_id: ObjectID::random(),
            was_certified: true,
            event_id: event_id_for_testing(),
        }
    }
}

impl EventForTesting for InvalidBlobId {
    fn for_testing(blob_id: BlobId) -> Self {
        Self {
            epoch: 1,
            blob_id,
            event_id: event_id_for_testing(),
        }
    }
}
